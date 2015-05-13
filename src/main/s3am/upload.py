from __future__ import print_function

from contextlib import closing, contextmanager
from operator import itemgetter
import os
import pycurl
import multiprocessing
import itertools
from io import BytesIO
import signal
import traceback
from urlparse import urlparse

from boto.s3.connection import S3Connection, OrdinaryCallingFormat
from boto.s3.multipart import MultiPartUpload

from s3am import me, log, UserError, WorkerException
from s3am.humanize import bytes2human, human2bytes




# http://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
#
max_part_per_page = 1000
max_uploads_per_page = 1000
min_part_size = human2bytes( "5M" )
max_part_size = human2bytes( "5G" )
max_parts_per_upload = 10000

# The multiprocessing module needs semaphores to be declared at the top level. I'm assuming this
# applies to events, too. The semaphore's initial value depends on a command line option so we
# need to instantiate it later, in stream(). The second reason for initializing them later is
# that we want stream() to be called multiple times per program invocation, e.g. for unit tests.
#
download_slots_semaphore = None
done_event = None
error_event = None

num_cores = multiprocessing.cpu_count( )


class Upload( object ):
    def __init__( self, bucket_name, key_name ):
        super( Upload, self ).__init__( )
        self.bucket_name = bucket_name
        self.key_name = key_name

    def cancel( self, allow_prefix ):
        with self._open_bucket( ) as bucket:
            for upload in self._get_uploads( bucket, allow_prefix=allow_prefix ):
                upload.cancel_upload( )

    @contextmanager
    def _open_bucket( self ):
        """
        A context manager for buckets.
        """
        # Due to the fact that this code is using multiple processes, it is safer to fetch the
        # bucket from a fresh connection rather than caching it or the connection.
        #
        with closing( S3Connection( calling_format=OrdinaryCallingFormat() ) ) as s3:
            yield s3.get_bucket( self.bucket_name )

    def _get_uploads( self, bucket, limit=max_uploads_per_page, allow_prefix=False ):
        """
        Get all open multipart uploads for the user-specified key
        """
        if limit > max_uploads_per_page:
            raise ValueError( "Limit must not exceed %i" % max_uploads_per_page )
        uploads = bucket.get_all_multipart_uploads( prefix=self.key_name, max_uploads=limit )
        if len( uploads ) == max_uploads_per_page:
            raise RuntimeError( "Can't handle more than %i uploads" % max_uploads_per_page )

        if not allow_prefix:
            uploads = [ upload for upload in uploads if upload.key_name == self.key_name ]

        return uploads

    def _get_upload( self, bucket, upload_id ):
        """
        Returns a MultiPartObject representing the given upload in the given bucket
        """
        # There is no way to just get a multipart object by itself and without its children part
        # objects. There is either ListParts or ListMultiPartUploads. We can, however, fake that
        # call by creating an empty MultiPartObject and fill in the attributes we know are needed
        # for the method calls on that object.
        #
        upload = MultiPartUpload( bucket )
        upload.id = upload_id
        upload.key_name = self.key_name
        return upload


class StreamingUpload( Upload ):
    """
    Represents one upload from a URL to S3.
    """

    def __init__( self, url, bucket_name, key_name=None, resume=False, part_size=min_part_size,
                  download_slots=num_cores, upload_slots=num_cores ):
        if key_name:
            self.key_name = key_name
        else:
            key_name = os.path.basename( urlparse( url ).path )
        super( StreamingUpload, self ).__init__( bucket_name, key_name )
        self.url = url
        self.part_size = part_size
        self.resume = resume
        self.download_slots = download_slots
        self.upload_slots = upload_slots

    def upload( self ):
        """
        Stream a URL to a key in an S3 bucket using a parallelized multi-part upload.
        """
        global download_slots_semaphore, done_event, error_event

        log.info( 'Streaming %s' % self.url )
        download_slots_semaphore = multiprocessing.Semaphore( self.download_slots )
        done_event = multiprocessing.Event( )
        error_event = multiprocessing.Event( )

        upload_id, completed_parts = self._prepare_upload( )
        part_nums = itertools.count( )
        num_workers = self.download_slots + self.upload_slots
        workers = multiprocessing.Pool( num_workers, _init_worker )

        def complete_part( ( part_num_, part_size ) ):
            if part_size > 0 or part_num_ == 0:
                assert part_num_ not in completed_parts
                completed_parts[ part_num_ ] = part_size

        try:
            while not done_event.is_set( ):
                if error_event.is_set( ):
                    raise WorkerException( )
                part_num = next( part_nums )
                if part_num in completed_parts:
                    assert self.resume
                    log.info( 'part %i: exists', part_num )
                else:
                    download_slots_semaphore.acquire( )
                    log.info( 'part %i: dispatching', part_num )
                    workers.apply_async( self._stream_part,
                                         args=[ upload_id, part_num ],
                                         callback=complete_part )
            workers.close( )
            workers.join( )
            self._sanity_check( completed_parts )
            with self._open_bucket( ) as bucket:
                self._get_upload( bucket, upload_id ).complete_upload( )
            log.info( 'Completed %s' % self.url )
        except WorkerException:
            workers.close( )
            workers.join( )
            raise
        except ( Exception, KeyboardInterrupt ):
            workers.close( )
            workers.terminate( )
            raise

    def _prepare_upload( self ):
        """
        Prepare a new multipart upload or resume a previously interrupted one. Returns the upload ID
        and a dictionary mapping the 0-based index of a part to its size.
        """
        completed_parts = { }
        with self._open_bucket( ) as bucket:
            uploads = self._get_uploads( bucket )
            if len( uploads ) == 0:
                if self.resume:
                    raise UserError( "Transfer failed. There is no pending upload to be resumed." )
                else:
                    upload_id = bucket.initiate_multipart_upload( key_name=self.key_name ).id
            elif len( uploads ) == 1:
                if self.resume:
                    upload = uploads[ 0 ]
                    upload_id = upload.id
                    for part in upload:
                        completed_parts[ part.part_number - 1 ] = part.size
                    # If there is an upload but no parts we can use whatever part size we want,
                    # otherwise we need to ensure that we use the same part size as before.
                    if len( completed_parts ) > 0:
                        previous_part_size = self._guess_part_size( completed_parts )
                        if self.part_size != previous_part_size:
                            raise UserError(
                                "Transfer failed. The part size appears to have changed from %i "
                                "to %i. Either resume the upload with the old part size or cancel "
                                "the upload and restart with the new part size. "
                                % ( self.part_size, previous_part_size) )
                else:
                    raise UserError(
                        "Transfer failed. There is a pending upload. If you would like to resume "
                        "that upload, run {me} again with --resume. If you would like to cancel "
                        "the upload, use '{me} {bucket_name} cancel {key_name}'. Note that "
                        "pending uploads incur storage fees.".format( me=me, **vars( self ) ) )
            else:
                raise RuntimeError(
                    "Transfer failed. Detected more than one pending multipart upload. Consider "
                    "using '{me} {bucket_name} cancel {key_name}' to delete all of them before "
                    "trying the transfer again. Note that pending uploads incur storage "
                    "fees.".format( me=me, **vars( self ) ) )
            return upload_id, completed_parts

    def _guess_part_size( self, completed_parts ):
        size_groups = self._part_size_histogram( completed_parts )
        assert len( size_groups ) > 0
        # We can't handle more than two different sizes (first term) and if we have two different
        # sizes, the smaller one should only occur once (second term).
        if len( size_groups ) > 2 \
                or len( size_groups ) == 2 and size_groups[ 1 ][ 1 ] != 1:
            raise RuntimeError(
                "Can't reliably determine previously used part size for this upload. "
                "You should probably cancel it and start over." )
        return size_groups[ 0 ][ 0 ]

    def _stream_part( self, upload_id, part_num ):
        """
        Download o part from the source URL, buffer it in memory and then upload it to S3.
        """
        try:
            try:
                log.info( 'part %i: downloading', part_num )
                buf = self._download_part( part_num )
            finally:
                download_slots_semaphore.release( )

            download_size = buf.tell( )
            log.info( 'part %i: downloaded %sB', part_num, bytes2human( download_size ) )
            if download_size > self.part_size:
                assert False
            elif download_size < self.part_size:
                done_event.set( )
            else:
                pass
            if error_event.is_set(): raise BailoutException()
            if download_size > 0 or part_num == 0:
                log.info( 'part %i: uploading', part_num )
                buf.seek( 0 )
                self._upload_part( upload_id, part_num, buf )
                upload_size = buf.tell( )
                assert download_size == upload_size
                log.info( 'part %i: uploaded %sB', part_num, bytes2human( upload_size ) )
            return part_num, download_size
        except BailoutException:
            raise
        except BaseException:
            error_event.set( )
            log.error( traceback.format_exc( ) )
            raise

    def _download_part( self, part_num ):
        """
        Download a part from the source URL. Returns a BytesIO buffer. The buffer's tell() method
        will return the size of the downloaded part, which may be less than the requested part
        size if the part is the last one for the URL.
        """
        buf = BytesIO( )
        with closing( pycurl.Curl( ) ) as c:
            c.setopt( c.URL, self.url )
            c.setopt( c.WRITEDATA, buf )
            c.setopt( c.FAILONERROR, 1 )
            start = part_num * self.part_size
            end = start + self.part_size - 1
            c.setopt( c.RANGE, "%i-%i" % ( start, end ) )
            try:
                c.perform( )
            except pycurl.error as e:
                error_code, message = e
                if error_code == c.E_BAD_DOWNLOAD_RESUME: # bad range for FTP
                    pass
                elif error_code == c.E_HTTP_RETURNED_ERROR:
                    # http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html#sec10.4.17
                    if c.getinfo(c.RESPONSE_CODE) == 416:
                        pass
                    else:
                        raise
                else:
                    raise
                buf.truncate(0)
                buf.seek(0)
        return buf

    def _upload_part( self, upload_id, part_num, buf ):
        """
        Upload a part to S3. The given buffer's tell() is assumed to point at the first byte to
        be uploaded. When this method returns, the tell() method points at the end of the buffer.
        """
        with self._open_bucket( ) as bucket:
            self._get_upload( bucket, upload_id ).upload_part_from_file( buf, part_num + 1 )

    def _sanity_check( self, completed_parts ):
        """
        Verify that all parts are present and valid.
        """

        # Check uniqueness property (no duplicate part_nums)
        assert isinstance( completed_parts, dict )

        # At least one part
        assert len( completed_parts ) > 0

        # Check completeness (no missing part nums)
        assert max( completed_parts ) >= len( completed_parts ) - 1

        # The parts should belong to one or two distinct size groups. With n >= 5MB being the
        # configured part size, s = 0 being the size of a sentinel part for empty files (S3 needs
        # at least one part per upload) and l being the size of the last part with 0 < l < <= n
        # we should have either [s] or [n*,l]. For example, we could have [s], [n, l] or [n,n,l]
        # but not [ ], [n,l, l] or [n,l1,l2].
        #
        groups = self._part_size_histogram( completed_parts )

        def s( part_size, num_parts ):
            return part_size == 0 and num_parts == 1

        def n( part_size, num_parts ):
            return part_size == self.part_size and num_parts > 0

        def l( part_size, num_parts ):
            return 0 < part_size <= self.part_size and num_parts == 1

        if len( groups ) == 1:
            assert s( *groups[ 0 ] ) or l( *groups[ 0 ] )
        elif len( groups ) == 2:
            assert n( *groups[ 0 ] ) and l( *groups[ 1 ] )
        else:
            assert False

    def _part_size_histogram( self, completed_parts ):
        """
        Group input parts by size and return the length of each group.

        :param completed_parts: a dictionary mapping part numbers to part sizes

        :return: A list of (part_size, num_parts) tuples where part_size is the size of a part and
        num_parts the number of parts of that size in the input. The returned list is sorted by
        descending part size. Note that there can't be any empty groups in the result, so the second
        member of each tuple is guaranteed to be non-zero.
        """

        by_part_size = itemgetter( 1 )
        # Convert to list of ( part_num, part_size ) pairs, sorted by descending size
        completed_parts = sorted( completed_parts.items( ), key=by_part_size, reverse=True )

        def ilen( it ):
            """Count # of elements in itereator"""
            return sum( 1 for _ in it )

        return [ ( part_size, ilen( group ) ) for part_size, group in
            itertools.groupby( completed_parts, by_part_size ) ]


class BailoutException( RuntimeError ):
    pass

def _init_worker( ):
    """
    Hacks around weirdness with Ctrl-C and multiprocessing
    """
    signal.signal( signal.SIGINT, signal.SIG_IGN )


# Allow instance methods to be used with multiprocessing
#
# Stolen from https://gist.github.com/fiatmoney/1086393

def _pickle_method( method ):
    func_name = method.im_func.__name__
    obj = method.im_self
    cls = method.im_class
    if func_name.startswith( '__' ) and not func_name.endswith( '__' ):  # deal with mangled names
        cls_name = cls.__name__.lstrip( '_' )
        func_name += '_' + cls_name
    return _unpickle_method, (func_name, obj, cls)


def _unpickle_method( func_name, obj, cls ):
    for cls in cls.__mro__:
        try:
            func = cls.__dict__[ func_name ]
        except KeyError:
            pass
        else:
            return func.__get__( obj, cls )
    assert False


import copy_reg
import types

copy_reg.pickle( types.MethodType, _pickle_method, _unpickle_method )
