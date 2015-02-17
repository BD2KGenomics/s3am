#!/usr/bin/env python2.7

from __future__ import print_function

from contextlib import closing, contextmanager
import logging
from operator import itemgetter
import os
import pycurl
import multiprocessing
import itertools
from io import BytesIO
import signal
import traceback
import sys
from urlparse import urlparse
import argparse

from boto.s3.connection import S3Connection
from boto.s3.multipart import MultiPartUpload

from humanize import human2bytes, bytes2human

log = logging.getLogger( __name__ )
logging.basicConfig( level=logging.WARN,
                     format="%(asctime)-15s %(module)s(%(process)d) %(message)s" )

# http://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
#
max_part_per_page = 1000
max_uploads_per_page = 1000
min_part_size = human2bytes( "5M" )
max_part_size = human2bytes( "5G" )
max_parts_per_upload = 10000

# FIXME: doesn't handle (hangs) if file is larger than max_parts_per_upload * min_part_size

# The multiprocessing module needs semaphores to be declared at the top level. I'm assuming this
# applies to events, too. The semaphore's initial value depends on a command line option so we
# need to instantiate it later, in stream(). The second reason for initializing them later is
# that we want stream() to be called multiple times per program invocation, e.g. for unit tests.
#
download_slots = None
done = None
error = None

# Normally we would encapsulate all of this module-level state in a class but the multiprocessing
# package can only run module-level functions, not class methods. This is why you will see a few
# globals here.
#
options = None


class UserError( Exception ):
    """
    An exception that doesn't cause a stack trace to be printed.
    """
    pass


class WorkerException( Exception ):
    """
    An exception where we let other workers finish
    """
    pass


def main( args=sys.argv[ 1: ] ):
    """
    Main entry point. Neither thread-safe nor reentrant but can be called repeatedly.
    """
    try:
        parse_args( args )
        if options.verbose:
            logging.getLogger( ).setLevel( logging.INFO )
        if options.mode == 'stream':
            stream( )
        elif options.mode == 'cancel':
            cancel( )
    except UserError as e:
        print( e.message, file=sys.stderr )
        sys.exit( 1 )


def parse_args( args ):
    """
    Parse command line arguments set global option variable with parse result
    """
    global options
    num_cores = multiprocessing.cpu_count( )
    p = argparse.ArgumentParser( description='Stream content from HTTP or FTP servers to S3' )
    p.add_argument( '--verbose', action='store_true', help="Print INFO level log messages" )
    p.add_argument( 'bucket_name', metavar='BUCKET' )
    sps = p.add_subparsers( dest='mode' )

    sp = sps.add_parser( 'stream', help="Perform an upload" )
    sp.add_argument( '--resume', action='store_true',
                     help="Attempt to resume a previously interrupted upload. Only works if there "
                          "is exactly one open upload. Already uploaded pieces will be skipped." )
    sp.add_argument( '--download-slots', type=int, metavar="NUM", default=num_cores )
    sp.add_argument( '--upload-slots', type=int, metavar="NUM", default=num_cores )

    def parse_part_size( s ):
        i = human2bytes( s )
        if i < min_part_size:
            raise argparse.ArgumentError( "Part size must be at least %i" % min_part_size )
        if i > max_part_size:
            raise argparse.ArgumentError( "Part size must not exceed %i" % max_part_size )
        return i

    sp.add_argument( '--part-size', default=min_part_size, type=parse_part_size,
                     help="The number of bytes in each part. This parameter must be at least "
                          "{min} and no more than {max}. The default is {min}. Note that S3 allows "
                          "no more than {max_parts} per upload and this program does not "
                          "currently ensure that this parameter is large enough to stream the "
                          "source URL's content in its entirety using those {max_parts} "
                          "parts.".format( min=min_part_size,
                                           max=max_part_size,
                                           max_parts=max_parts_per_upload ) )
    sp.add_argument( 'url', metavar='URL', help="The source URL." )
    sp.add_argument( 'key_name', nargs='?', metavar='KEY' )

    sp = sps.add_parser( 'cancel',
                         help="Cancel open uploads for some or all keys in a bucket" )
    sp.add_argument( 'key_name', metavar='KEY_PREFIX' )
    options = p.parse_args( args )


def stream( ):
    """
    Stream a URL to a key in an S3 bucket using a parallelized multi-part upload. URL, bucket and
    key are read from the global options variable.
    """
    global download_slots, done, error

    log.info( 'Streaming %s' % options.url )
    download_slots = multiprocessing.Semaphore( options.download_slots )
    done = multiprocessing.Event( )
    error = multiprocessing.Event( )

    upload_id, completed_parts = prepare_upload( )
    part_nums = itertools.count( )
    num_workers = options.download_slots + options.upload_slots
    workers = multiprocessing.Pool( num_workers, init_worker )

    def complete_part( ( part_num, part_size ) ):
        if part_size > 0 or part_num == 0:
            assert part_num not in completed_parts
            completed_parts[ part_num ] = part_size

    try:
        while not done.is_set( ):
            if error.is_set( ):
                raise WorkerException( )
            part_num = next( part_nums )
            if part_num in completed_parts:
                assert options.resume
                log.info( 'part %i: exists', part_num )
            else:
                download_slots.acquire( )
                log.info( 'part %i: dispatching', part_num )
                workers.apply_async( stream_part,
                                     args=[ upload_id, part_num ],
                                     callback=complete_part )
        workers.close( )
        workers.join( )
        sanity_check( completed_parts )
        with open_bucket( ) as bucket:
            get_upload( bucket, upload_id ).complete_upload( )
        log.info( 'Completed %s' % options.url )
    except WorkerException:
        workers.close( )
        workers.join( )
        raise
    except ( Exception, KeyboardInterrupt ):
        workers.close( )
        workers.terminate( )
        raise


def prepare_upload( ):
    """
    Prepare a new multipart upload or resume a previously interrupted one. Returns the upload ID
    and a dictionary mapping the 0-based index of a part to its size.
    """
    if not options.key_name:
        options.key_name = os.path.basename( urlparse( options.url ).path )
    part_nums = { }
    with open_bucket( ) as bucket:
        uploads = get_uploads( bucket )
        if len( uploads ) == 0:
            upload = bucket.initiate_multipart_upload( key_name=options.key_name )
            upload_id = upload.id
        elif len( uploads ) == 1:
            if options.resume:
                upload = uploads[ 0 ]
                upload_id = upload.id
                for part in upload:
                    part_nums[ part.part_number - 1 ] = part.size
            else:
                raise UserError(
                    "There is one pending upload. Either pass --resume to resume it or use "
                    "'cancel' to delete it and then try again." )
        else:
            raise UserError(
                "More than one active multipart upload. Consider using 'cancel' to delete all of "
                "them and start from scratch." )
    return upload_id, part_nums


def stream_part( upload_id, part_num ):
    """
    Download o part from the source URL, buffer it in memory and then upload it to S3.
    """
    try:
        try:
            log.info( 'part %i: downloading', part_num )
            buf = download_part( part_num )
        finally:
            download_slots.release( )

        download_size = buf.tell( )
        log.info( 'part %i: downloaded %sB', part_num, bytes2human( download_size ) )
        if download_size > options.part_size:
            assert False
        elif download_size < options.part_size:
            done.set( )
        else:
            pass
        if download_size > 0 or part_num == 0:
            log.info( 'part %i: uploading', part_num )
            buf.seek( 0 )
            upload_part( upload_id, part_num, buf )
            upload_size = buf.tell( )
            assert download_size == upload_size
            log.info( 'part %i: uploaded %sB', part_num, bytes2human( upload_size ) )
        return part_num, download_size
    except BaseException as e:
        error.set( )
        log.error( traceback.format_exc( ) )
        raise e


def download_part( part_num ):
    """
    Download a part from the source URL. Returns a BytesIO buffer. The buffer's tell() method
    will return the size of the downloaded part, which may be less than the requested part size
    if the part is the last one for the URL.
    """
    buf = BytesIO( )
    with closing( pycurl.Curl( ) ) as c:
        c.setopt( c.URL, options.url )
        c.setopt( c.WRITEDATA, buf )
        start = part_num * options.part_size
        end = start + options.part_size - 1
        c.setopt( c.RANGE, "%i-%i" % ( start, end ) )
        try:
            c.perform( )
        except pycurl.error as e:
            error_code, message = e
            if error_code == 36:
                pass
            else:
                raise
    return buf


def upload_part( upload_id, part_num, buf ):
    """
    Upload a part to S3. The given buffer's tell() is assumed to point at the first byte to
    be uploaded. When this method returns, the tell() method points at the end of the buffer.
    """
    with open_bucket( ) as bucket:
        get_upload( bucket, upload_id ).upload_part_from_file( buf, part_num + 1 )


def sanity_check( completed_parts ):
    """
    Verify that all parts are present and valid.
    """

    # Check uniqueness property (no duplicate part_nums)
    assert isinstance( completed_parts, dict )

    # At least one part
    assert len( completed_parts ) > 0

    # Check completeness (no missing part nums)
    assert max( completed_parts ) >= len( completed_parts ) - 1

    by_part_size = itemgetter( 1 )

    # Convert to list of ( part_num, part_size ) pairs, sorted by descending size
    completed_parts = sorted( completed_parts.items( ), key=by_part_size, reverse=True )

    def ilen( it ):
        """Count # of elements in itereator"""
        return sum( 1 for _ in it )

    # We should now have parts belonging to one or two distinct size groups. With N >= 5MB being
    # the configured part size, S = 0 being the size of a sentinel part for empty files (S3 needs
    # at least one part per upload) and L being the size of the last part with 0 < L < <= N we
    # should have either [S] or [N*,L]. For example, we could have [S], [N,L] or [N,N,L] but we
    # can't get [ ], [N,L, L] or [N,L1,L2].
    #
    # First, group by part size and count the number of parts in each size group. Note that there
    # can't be any empty groups, so the second member of each tuple is guaranteed to be non-zero.
    #
    groups = [ ( part_size, ilen( group ) ) for part_size, group in
        itertools.groupby( completed_parts, by_part_size ) ]

    def S( part_size, num_parts ):
        return part_size == 0 and num_parts == 1

    def N( part_size, num_parts ):
        return part_size == options.part_size and num_parts > 0

    def L( part_size, num_parts ):
        return 0 < part_size <= options.part_size and num_parts == 1

    if len( groups ) == 1:
        assert S( *groups[ 0 ] ) or L( *groups[ 0 ] )
    elif len( groups ) == 2:
        assert N( *groups[ 0 ] ) and L( *groups[ 1 ] )
    else:
        assert False


def cancel( ):
    with open_bucket( ) as bucket:
        for upload in get_uploads( bucket ):
            upload.cancel_upload( )


@contextmanager
def open_bucket( ):
    """
    A context manager for buckets.
    """
    # Due to the fact that this code is using multiple processes, it is safer to fetch the bucket
    # from a fresh connection rather than caching it or the connection.
    #
    with closing( S3Connection( ) ) as s3:
        yield s3.get_bucket( options.bucket_name )


def get_uploads( bucket, limit=max_uploads_per_page ):
    """
    Get all open multipart uploads for the user-specified key
    """
    # FIXME: Use bucket.list_multipart_uploads() once https://github.com/boto/boto/pull/2920 has
    # been merged such that we can pass a prefix to that method
    #
    if limit > max_uploads_per_page:
        raise ValueError( "Limit must not exceed %i" % max_uploads_per_page )
    uploads = bucket.get_all_multipart_uploads( prefix=options.key_name, max_uploads=limit )
    if len( uploads ) == max_uploads_per_page:
        raise RuntimeError( "Can't handle more than %i uploads" % max_uploads_per_page )
    return uploads


def get_upload( bucket, upload_id ):
    """
    Returns a MultiPartObject representing the given upload in the given bucket
    """
    # There is no way to just get a multipart object by itself and without its children part
    # objects. There is either ListParts or ListMultiPartUploads. We can, however, fake that call
    # by creating an empty MultiPartObject and fill in the attributes we know are needed for the
    # method calls on that object.
    #
    upload = MultiPartUpload( bucket )
    upload.id = upload_id
    upload.key_name = options.key_name
    return upload


def init_worker( ):
    """
    Hacks around weirdness with Ctrl-C and multiprocessing
    """
    signal.signal( signal.SIGINT, signal.SIG_IGN )


if __name__ == "__main__":
    main( )


# bucket_name = 'bd2k-test-data'

# part_size = 50 * 1024 * 1024
# key_name = 'NA12878.mapped.ILLUMINA.bwa.CEU.high_coverage_pcr_free.20130906.bam'
# url = 'ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/data/NA12878/high_coverage_alignment/NA12878.mapped.ILLUMINA.bwa.CEU.high_coverage_pcr_free.20130906.bam'

# key_name = 'ERR001268.filt.fastq.gz'
# url = 'ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/data/NA12878/sequence_read/ERR001268.filt.fastq.gz'
