# Copyright (C) 2015 UCSC Computational Genomics Lab
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function, absolute_import
import base64
from contextlib import closing, contextmanager
import hashlib
from operator import itemgetter
import os
import pycurl
import multiprocessing
import itertools
from io import BytesIO
import signal
import traceback
from urlparse import urlparse
from StringIO import StringIO

import boto.s3

from boto.s3.connection import S3Connection

from boto.s3.multipart import MultiPartUpload, Part

from s3am import me, log, UserError, WorkerException
from s3am.boto_utils import work_around_dots_in_bucket_names
from s3am.humanize import bytes2human, human2bytes

max_part_per_page = 1000  # http://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
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


class Operation( object ):
    """
    A base class for the cancel and upload operations
    """

    def __init__( self, bucket_name, key_name ):
        super( Operation, self ).__init__( )
        self.bucket_name = bucket_name
        self.key_name = key_name
        work_around_dots_in_bucket_names( )
        with closing( S3Connection( ) ) as s3:
            bucket = s3.get_bucket( self.bucket_name )
            self.bucket_location = bucket.get_location( )

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

    def _get_upload( self, bucket, upload_id, parts=None ):
        return MultiPartUploadPlus( bucket, self.key_name, upload_id, parts=parts )

    def _part_for_key( self, bucket, part_num, key ):
        """
        Most of boto's multipart-related methods return a Key object rather than a part object.
        This method converts the former to the latter. Ultimately, we need a MultiPartUpload with
        a list of Part objects in order to complete a multi-part upload.

        :rtype : Part
        """
        part = Part( )
        part.bucket = bucket
        part.part_number = part_num + 1
        part.size = key.size
        part.etag = key.etag
        part.last_modified = key.last_modified
        return part


class MultiPartUploadPlus( MultiPartUpload ):
    """
    There is no built-in way to just get a MultiPartUpload object without its children part
    objects. There is either ListParts or ListMultiPartUploads. We can, however, fake one
    by creating an empty MultiPartObject and fill in the attributes we know are needed for the
    method calls we perform on that object.
    """

    def __init__( self, bucket, key_name, upload_id, parts=None ):
        """
        Constructs a multi-part object and optionally pass a list of parts. If the parts are
        supplied, the resulting object can be used to complete the upload via its
        complete_upload() method.

        :type parts: list[Part]
        """
        super( MultiPartUploadPlus, self ).__init__( bucket )
        self.id = upload_id
        self.key_name = key_name
        self._parts = parts

    def __iter__( self ):
        if self._parts is None:
            return super( MultiPartUploadPlus, self ).__iter__( )
        else:
            return iter( self._parts )


class Cancel( Operation ):
    """
    Cancel a pending upload
    """
    def __init__( self, bucket_name, key_name, allow_prefix ):
        super( Cancel, self ).__init__( bucket_name, key_name )
        self.allow_prefix = allow_prefix

    def run( self ):
        with closing( boto.s3.connect_to_region( self.bucket_location ) ) as s3:
            bucket = s3.get_bucket( self.bucket_name )
            for upload in self._get_uploads( bucket, allow_prefix=self.allow_prefix ):
                upload.cancel_upload( )


class Upload( Operation ):
    """
    Perform or resume an upload
    """

    def __init__( self, url, bucket_name,
                  key_name=None, resume=False, part_size=min_part_size,
                  download_slots=num_cores, upload_slots=num_cores,
                  sse_key=None, src_sse_key=None ):
        if key_name:
            self.key_name = key_name
        else:
            key_name = os.path.basename( urlparse( url ).path )
        super( Upload, self ).__init__( bucket_name, key_name )
        self.url = url
        self.part_size = part_size
        self.resume = resume
        self.download_slots = download_slots
        self.upload_slots = upload_slots
        self.sse_key = sse_key
        self.src_sse_key = src_sse_key

    def run( self ):
        """
        Stream a URL to a key in an S3 bucket using a parallelized multi-part upload.
        """
        global download_slots_semaphore, done_event, error_event

        download_slots_semaphore = multiprocessing.Semaphore( self.download_slots )
        done_event = multiprocessing.Event( )
        error_event = multiprocessing.Event( )

        upload_id, completed_parts = self._prepare_upload( )
        part_nums = itertools.count( )
        num_workers = self.download_slots + self.upload_slots
        workers = multiprocessing.Pool( num_workers, _init_worker )

        def complete_part( ( part_num_, part ) ):
            if part is not None:
                assert part_num_ not in completed_parts
                completed_parts[ part_num_ ] = part

        if self.url.startswith( 's3:' ):
            log.info( 'Copying %s' % self.url )
            url = urlparse( self.url )
            assert url.scheme == 's3'
            assert url.path.startswith( '/' )
            with closing( boto.s3.connect_to_region( self.bucket_location ) ) as s3:
                src_bucket = s3.get_bucket( url.netloc )
                headers = { }
                if self.src_sse_key:
                    self._add_encryption_headers( self.src_sse_key, headers )
                src_key = src_bucket.get_key( url.path[ 1: ], headers=headers )
            worker_func = self._copy_part
            kwargs = dict(
                src_bucket_name=src_bucket.name,
                src_key_name=src_key.name,
                size=src_key.size )
        else:
            log.info( 'Streaming %s' % self.url )
            kwargs = { }
            worker_func = self._stream_part

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
                    workers.apply_async( func=worker_func,
                                         args=[ upload_id, part_num ],
                                         kwds=kwargs,
                                         callback=complete_part )
            workers.close( )
            workers.join( )
            if error_event.is_set( ):
                raise WorkerException( )
            self._sanity_check( completed_parts )
            with closing( boto.s3.connect_to_region( self.bucket_location ) ) as s3:
                bucket = s3.get_bucket( self.bucket_name )
                upload = self._get_upload( bucket, upload_id, parts=completed_parts.values( ) )
                upload.complete_upload( )
            log.info( 'Completed %s' % self.url )
        except WorkerException:
            workers.close( )
            workers.join( )
            raise
        except (Exception, KeyboardInterrupt):
            workers.close( )
            workers.terminate( )
            raise

    def _prepare_upload( self ):
        """
        Prepare a new multipart upload or resume a previously interrupted one. Returns the upload ID
        and a dictionary mapping the 0-based index of a part to its size.
        """
        completed_parts = { }
        with closing( boto.s3.connect_to_region( self.bucket_location ) ) as s3:
            bucket = s3.get_bucket( self.bucket_name )
            uploads = self._get_uploads( bucket )
            if len( uploads ) == 0:
                if self.resume:
                    raise UserError( "Transfer failed. There is no pending upload to be resumed." )
                else:
                    headers = { }
                    self.__add_encryption_headers( headers )
                    upload_id = bucket.initiate_multipart_upload( key_name=self.key_name,
                                                                  headers=headers ).id
            elif len( uploads ) == 1:
                if self.resume:
                    upload = uploads[ 0 ]
                    upload_id = upload.id
                    for part in upload:
                        completed_parts[ part.part_number - 1 ] = part
                    # If there is an upload but no parts we can use whatever part size we want,
                    # otherwise we need to ensure that we use the same part size as before.
                    if len( completed_parts ) > 0:
                        previous_part_size = self._guess_part_size( completed_parts )
                        if self.part_size != previous_part_size:
                            raise UserError(
                                "Transfer failed. The part size appears to have changed from %i "
                                "to %i. Either resume the upload with the old part size or cancel "
                                "the upload and restart with the new part size. "
                                % (self.part_size, previous_part_size) )
                else:
                    raise UserError(
                        "Transfer failed. There is a pending upload. If you would like to resume "
                        "that upload, run {me} again with --resume. If you would like to cancel "
                        "the upload, use '{me} cancel {bucket_name} {key_name}'. Note that "
                        "pending uploads incur storage fees.".format( me=me, **vars( self ) ) )
            else:
                raise RuntimeError(
                    "Transfer failed. Detected more than one pending multipart upload. Consider "
                    "using '{me} cancel {bucket_name} {key_name}' to delete all of them before "
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

    @contextmanager
    def _propagate_worker_exception( self ):
        """
        A context manager that sets the error_event if any exception occurs in the body. It can
        be safely assumed that this context manager does not cause any exceptions before it
        yields to the enclosed block.
        """
        try:
            yield
        except BailoutException:
            raise
        except BaseException:
            error_event.set( )
            log.error( traceback.format_exc( ) )
            raise

    def _stream_part( self, upload_id, part_num ):
        """
        Download o part from the source URL, buffer it in memory and then upload it to S3.
        """
        with self._propagate_worker_exception( ):
            try:
                log.info( 'part %i: downloading', part_num )
                buf = self._download_part( part_num )
            finally:
                download_slots_semaphore.release( )
            download_size = buf.tell( )
            self._log_progress( part_num, 'downloaded', download_size )
            if download_size > self.part_size:
                assert False
            elif download_size < self.part_size:
                done_event.set( )
            else:
                pass
            if error_event.is_set( ): raise BailoutException( )
            if download_size > 0 or part_num == 0:
                log.info( 'part %i: uploading', part_num )
                buf.seek( 0 )
                part = self._upload_part( upload_id, part_num, buf )
                upload_size = buf.tell( )
                assert download_size == upload_size == part.size
                self._log_progress( part_num, 'uploaded', upload_size )
            else:
                part = None
            return part_num, part

    def _log_progress( self, part_num, task, download_size ):
        log.info( 'part %i: %s %sB (%i bytes)',
                  part_num, task, bytes2human( download_size ), download_size )

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
            start, end = self._get_part_range( part_num )
            c.setopt( c.RANGE, "%i-%i" % (start, end - 1) )
            try:
                c.perform( )
            except pycurl.error as e:
                error_code, message = e
                if error_code == c.E_BAD_DOWNLOAD_RESUME:  # bad range for FTP
                    pass
                elif error_code == c.E_HTTP_RETURNED_ERROR:
                    # http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html#sec10.4.17
                    if c.getinfo( c.RESPONSE_CODE ) == 416:
                        pass
                    else:
                        raise
                else:
                    raise
                buf.truncate( 0 )
                buf.seek( 0 )
        return buf

    def _get_part_range( self, part_num ):
        """
        Returns a tuple (start,end) with start being the offset of the first byte to copy and end
        being the offset of the byte after the last byte to copy.

        Invariant:  end - start == self.part_size
        """
        start = part_num * self.part_size
        end = start + self.part_size
        return start, end

    def _upload_part( self, upload_id, part_num, buf ):
        """
        Upload a part to S3. The given buffer's tell() is assumed to point at the first byte to
        be uploaded. When this method returns, the tell() method points at the end of the buffer.

        :return: the part object representing the uploaded part
        :rtype: Part
        """
        with closing( boto.s3.connect_to_region( self.bucket_location ) ) as s3:
            bucket = s3.get_bucket( self.bucket_name )
            headers = { }
            self.__add_encryption_headers( headers )
            upload = self._get_upload( bucket, upload_id )
            key = upload.upload_part_from_file( buf, part_num + 1, headers=headers )
            return self._part_for_key( bucket, part_num, key )

    def _copy_part( self, upload_id, part_num, src_bucket_name, src_key_name, size ):
        with self._propagate_worker_exception( ):
            try:
                if error_event.is_set( ): raise BailoutException( )
                log.info( 'part %i: copying', part_num )
                start, end = self._get_part_range( part_num )
                end = min( end, size )
                part_size = end - start
                if part_size > 0 or part_num == 0:
                    url = urlparse( self.url )
                    assert url.scheme == 's3'
                    assert url.path.startswith( '/' )
                    with closing( boto.s3.connect_to_region( self.bucket_location ) ) as s3:
                        bucket = s3.get_bucket( self.bucket_name )
                        upload = self._get_upload( bucket, upload_id )
                        headers = { }
                        if self.sse_key:
                            self._add_encryption_headers( self.sse_key, headers )
                        if part_size == 0:
                            # Since copy_part_from_key doesn't allow empty ranges, we handle that
                            # case by uploading an empty part.
                            assert part_num == 0
                            # noinspection PyTypeChecker
                            key = upload.upload_part_from_file(
                                StringIO( ), part_num + 1,
                                headers=headers )
                        else:
                            if self.src_sse_key:
                                self._add_encryption_headers( self.src_sse_key, headers,
                                                              for_copy=True )
                            key = upload.copy_part_from_key(
                                src_bucket_name=src_bucket_name,
                                src_key_name=src_key_name,
                                part_num=part_num + 1,
                                start=start,
                                end=end - 1,
                                headers=headers )
                            # somehow copy_part_from_key doesn't set the key size
                            key.size = part_size
                    assert key.size == part_size
                    self._log_progress( part_num, 'copied', part_size )
                    return part_num, self._part_for_key( bucket, part_num, key )
                else:
                    done_event.set( )
                    return part_num, None
            finally:
                download_slots_semaphore.release( )

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
        # but not [ ], [n,l,l] or [n,l1,l2].
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
        :type completed_parts: dict[int,boto.s3.key.Key]

        :return: A list of (part_size, num_parts) tuples where part_size is the size of a part and
        num_parts the number of parts of that size in the input. The returned list is sorted by
        descending part size. Note that there can't be any empty groups in the result, so the second
        member of each tuple is guaranteed to be non-zero.
        """

        by_part_size = itemgetter( 1 )

        # Convert to list of ( part_num, part_size ) pairs, sorted by descending size
        completed_parts = [ (part_num, part.size) for part_num, part in
            completed_parts.iteritems( ) ]
        completed_parts.sort( key=by_part_size, reverse=True )

        def ilen( it ):
            """Count # of elements in itereator"""
            return sum( 1 for _ in it )

        return [ (part_size, ilen( group )) for part_size, group in
            itertools.groupby( completed_parts, by_part_size ) ]

    def __add_encryption_headers( self, headers ):
        if self.sse_key is not None:
            self._add_encryption_headers( self.sse_key, headers )

    @staticmethod
    def _add_encryption_headers( sse_key, headers, for_copy=False ):
        assert len( sse_key ) == 32
        encoded_sse_key = base64.b64encode( sse_key )
        encoded_sse_key_md5 = base64.b64encode( hashlib.md5( sse_key ).digest( ) )
        prefix = 'x-amz%s-server-side-encryption-customer-' % ('-copy-source' if for_copy else '')
        headers[ prefix + 'algorithm' ] = 'AES256'
        headers[ prefix + 'key' ] = encoded_sse_key
        headers[ prefix + 'key-md5' ] = encoded_sse_key_md5


class BailoutException( RuntimeError ):
    pass


def _init_worker( ):
    """
    Hacks around weirdness with the multiprocessing module and Ctrl-C
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
