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

import hashlib
import logging
import shutil
import socket
import time
import unittest
from contextlib import closing
from tempfile import mkdtemp
from threading import Lock

import math

import errno

import itertools

import FTPd
import boto.exception
import boto.s3
import os
import pyftpdlib.handlers
import s3am
import s3am.boto_utils
import s3am.cli
import s3am.operations

# The dot in the domain name makes sure that boto.work_around_dots_in_bucket_names() is covered
from bd2k.util.iterables import concat

test_bucket_name_prefix = 's3am-unit-tests.foo'
test_bucket_region = 'us-west-1'
copy_bucket_region = 'us-east-1'  # using us-east-1 so we get exposed to its quirks

host = "127.0.0.1"
port = 21212
part_size = s3am.operations.min_part_size
two_and_a_half_parts = int( part_size * 2.5 )
two_parts = 10 * 1024 * 1024
test_sizes = [ 0, 1, part_size - 1, part_size, part_size + 1, two_parts, two_and_a_half_parts ]

verbose = ('--verbose',)  # ('--debug',)

num_slots = 4  # how many download and upload slots to use

log = logging.getLogger( __name__ )

slots = ('--download-slots', str( num_slots ), '--upload-slots', str( num_slots ))
one_slot = ('--download-slots', '1', '--upload-slots', '0')


def md5( contents ):
    return hashlib.md5( contents ).hexdigest( )


class TestFile( object ):
    def __init__( self, ftp_root, size ):
        self.ftp_root = ftp_root
        self.size = size
        buf = bytearray( os.urandom( size ) )
        self.md5 = md5( buf )
        with open( self.path, 'w' ) as f:
            f.write( buf )

    @property
    def name( self ):
        return 'test-%i.bin' % self.size

    @property
    def path( self ):
        return os.path.join( self.ftp_root, self.name )


class OperationsTests( unittest.TestCase ):
    @classmethod
    def setUpClass( cls ):
        super( OperationsTests, cls ).setUpClass( )
        s3am.boto_utils.work_around_dots_in_bucket_names( )

    def setUp( self ):
        super( OperationsTests, self ).setUp( )
        self.netloc = '%s:%s' % (host, port)
        self.ftp_root_url = 'ftp://%s/' % self.netloc
        self.s3 = s3am.boto_utils.s3_connect_to_region( test_bucket_region )
        self.test_bucket_name = '%s-%i' % (test_bucket_name_prefix, int( time.time( ) ))
        test_bucket_location = s3am.boto_utils.region_to_bucket_location( test_bucket_region )
        self.bucket = self.s3.create_bucket( self.test_bucket_name, location=test_bucket_location )
        self._clean_bucket( self.bucket )
        self.ftp_root = mkdtemp( prefix=__name__ )
        self.test_files = { size: TestFile( self.ftp_root, size ) for size in test_sizes }
        self.ftpd = FTPd.FTPd( self.ftp_root, address=(host, port), dtp_handler=UnreliableHandler )
        logging.getLogger( 'pyftpdlib' ).setLevel( logging.WARN )
        self.ftpd.start( )

    def _clean_bucket( self, bucket ):
        for upload in bucket.list_multipart_uploads( ):
            upload.cancel_upload( )
        for key in bucket.list( ):
            key.delete( )

    def tearDown( self ):
        self.ftpd.stop( )
        self._clean_bucket( self.bucket )
        self.bucket.delete( )
        self.s3.close( )
        for test_file in self.test_files.itervalues( ):
            os.unlink( test_file.path )
        try:
            os.rmdir( self.ftp_root )
        except OSError as e:
            if e.errno == errno.ENOTEMPTY:
                # We could call shutil.rmtree right away but I think it's worth knowing whether
                # left-over files existed or not.
                log.warning( 'FTP root directory has left-over files. '
                             'See trace below for details.', exc_info=True )
                shutil.rmtree( self.ftp_root )
            else:
                raise

    def _assert_key( self, test_file, sse_key=None, is_master=False ):
        headers = { }
        if sse_key is not None:
            sse_key = s3am.operations.SSEKey( binary=sse_key, is_master=is_master )
            sse_key = sse_key.resolve( bucket_location=self.bucket.get_location( ),
                                       bucket_name=self.bucket.name,
                                       key_name=test_file.name )
            s3am.operations.Upload._add_encryption_headers( sse_key, headers )
        key = self.bucket.get_key( test_file.name, headers=headers )
        self.assertEquals( key.size, test_file.size )
        self.assertEquals( md5( key.get_contents_as_string( headers=headers ) ), test_file.md5 )

    def test_file_urls( self ):
        test_file = self.test_files[ 1 ]
        for url_prefix in 'file:', 'file://', 'file://localhost':
            s3am.cli.main( concat(
                'upload', '--exists=overwrite', verbose, slots,
                url_prefix + test_file.path, self.s3_url( ) ) )
            self._assert_key( test_file )

    def test_invalid_file_urls( self ):
        test_file = self.test_files[ 1 ]
        for url_prefix in ('file:/',):
            self.assertRaises( s3am.InvalidSourceURLError, s3am.cli.main, concat(
                'upload', verbose, slots,
                url_prefix + test_file.path, self.s3_url( ) ) )

    def test_file_path( self ):
        test_file = self.test_files[ 1 ]
        for path in test_file.path, os.path.relpath( test_file.path ):
            s3am.cli.main( concat(
                'upload', '--exists=overwrite', verbose, slots,
                path, self.s3_url( ) ) )
            self._assert_key( test_file )

    def test_upload( self ):
        for test_file in self.test_files.itervalues( ):
            s3am.cli.main( concat(
                'upload', verbose, slots,
                self.ftp_url( test_file ), self.s3_url( ) ) )
            self._assert_key( test_file )

    def test_existence_handling( self ):
        test_file = self.test_files[ two_and_a_half_parts ]
        # upload a file to the remote bucket so that another upload call will require overwrite or
        # skip to be passed to --exists
        s3am.cli.main( concat(
            'upload', verbose, slots,
            self.ftp_url( test_file ), self.s3_url( ) ) )

        # Upload to a key that already exists in the bucket without --overwrite should fail
        self.assertRaises( s3am.ObjectExistsError,
                           s3am.cli.main,
                           concat( 'upload', verbose, slots,
                                   self.ftp_url( test_file ), self.s3_url( ) ) )
        # Now try with --exists=skip. This should raise a SystemExit
        try:
            s3am.cli.main( concat(
                'upload', verbose, slots, '--exists=skip',
                self.ftp_url( test_file ), self.s3_url( ) ) )
        except SystemExit as err:
            self.assertEquals( err.code, 0 )
        else:
            self.fail( )
        # Now try with --existst=overwrite. This should pass.
        s3am.cli.main( concat(
            'upload', verbose, slots, '--exists=overwrite',
            self.ftp_url( test_file ), self.s3_url( ) ) )

    def s3_url( self, test_file=None, bucket_name=None, file_name=None ):
        """
        :param TestFile test_file:
        :param str file_name:
        :param str bucket_name:
        :rtype: str
        """
        self.assertTrue( file_name is None or test_file is None )
        if test_file is not None:
            file_name = test_file.name
        return 's3://%s/%s' % (bucket_name or self.test_bucket_name, file_name or '')

    def ftp_url( self, test_file ):
        return self.ftp_root_url + test_file.name

    def test_encryption( self ):
        for is_master in False, True:
            try:
                test_file = self.test_files[ two_and_a_half_parts ]
                src_url = self.ftp_url( test_file )
                sse_key = '-0123456789012345678901234567890'
                args = concat(
                    '--sse-key=' + sse_key,
                    [ '--sse-key-is-master' ] if is_master else [ ] )
                s3am.cli.main( concat(
                    'upload', verbose, slots, src_url, self.s3_url( ),
                    '--exists=overwrite',
                    args ) )
                self._assert_key( test_file, sse_key=sse_key, is_master=is_master )
                # Ensure that we can't actually retrieve the object without specifying an encryption key
                try:
                    self._assert_key( test_file )
                except boto.exception.S3ResponseError as e:
                    self.assertEquals( e.status, 400 )
                else:
                    self.fail( 'S3ResponseError(400) should have been raised' )
                # If a per-file was used ...
                if is_master:
                    # ... ensure that we can't retrieve the object with the master key.
                    try:
                        self._assert_key( test_file, sse_key=sse_key, is_master=False )
                    except boto.exception.S3ResponseError as e:
                        self.assertEquals( e.status, 403 )
                    else:
                        self.fail( 'S3ResponseError(403) should have been raised' )
                self._test_download( test_file, args=args )
            finally:
                self._clean_bucket( self.bucket )

    def test_resume( self ):
        test_file = self.test_files[ two_and_a_half_parts ]
        src_url = self.ftp_url( test_file )

        # Run with a simulated download failure
        UnreliableHandler.setup_for_failure_at( int( 0.75 * test_file.size ) )
        self.assertRaises( s3am.WorkerException,
                           s3am.cli.main,
                           concat( 'upload', verbose, one_slot, src_url, self.s3_url( ) ) )

        # Retrying without --resume should fail
        self.assertRaises( s3am.UploadExistsError,
                           s3am.cli.main,
                           concat( 'upload', verbose, slots, src_url, self.s3_url( ) ) )

        # Retrying with --resume and different part size should fail
        self.assertRaises( s3am.IncompatiblePartSizeError,
                           s3am.cli.main,
                           concat( 'upload', verbose, slots, src_url, self.s3_url( ),
                                   '--resume', '--part-size', str( 2 * part_size ) ) )

        # Retry
        s3am.cli.main( concat( 'upload', verbose, slots, src_url, self.s3_url( ), '--resume' ) )

        # FIMXE: We should assert that the resume skips existing parts
        self._assert_key( test_file )

        self._test_force_resume_overwrites( force_or_resume='resume', test_file=test_file,
                                            src_url=src_url )

    def test_multiple_uploads( self ):
        test_file = self.test_files[ two_and_a_half_parts ]
        src_url = self.ftp_url( test_file )
        upload1 = self.bucket.initiate_multipart_upload( test_file.name )
        try:
            upload2 = self.bucket.initiate_multipart_upload( test_file.name )
            try:
                self.assertRaises( s3am.MultipleUploadsExistError,
                                   s3am.cli.main,
                                   concat( 'upload', src_url, self.s3_url( ) ) )
                self.assertRaises( s3am.MultipleUploadsExistError,
                                   s3am.cli.main,
                                   concat( 'upload', '--resume', src_url, self.s3_url( ) ) )
            finally:
                upload2.cancel_upload( )
        finally:
            upload1.cancel_upload( )
        s3am.cli.main( concat( 'upload', '--force', verbose, src_url, self.s3_url( ) ) )
        self._assert_key( test_file )

    def _test_force_resume_overwrites( self, force_or_resume, test_file, src_url ):
        assert force_or_resume in ('force', 'resume')
        force_or_resume = '--' + force_or_resume
        # Run with a simulated download failure to create an unfinished upload
        UnreliableHandler.setup_for_failure_at( int( 0.75 * test_file.size ) )
        self.assertRaises( s3am.WorkerException,
                           s3am.cli.main,
                           concat( 'upload', '--exists=overwrite', verbose, one_slot,
                                   src_url, self.s3_url( ) ) )
        # Running without --force/--resume and --exists=overwrite should fail
        self.assertRaises( s3am.ObjectExistsError,
                           s3am.cli.main,
                           concat( 'upload', verbose, slots, src_url, self.s3_url( ) ) )
        # Running without --resume/--force but with --exists=overwrite should fail
        self.assertRaises( s3am.UploadExistsError,
                           s3am.cli.main,
                           concat( 'upload', verbose, slots, src_url, self.s3_url( ),
                                   '--exists=overwrite' ) )
        # Running with --force/--resume and --exists=overwrite should pass
        s3am.cli.main( concat(
            'upload', verbose, slots, src_url, self.s3_url( ),
            force_or_resume, '--exists=overwrite' ) )

    def test_force( self ):
        test_file = self.test_files[ two_and_a_half_parts ]
        src_url = self.ftp_url( test_file )

        # Run with a simulated download failure
        UnreliableHandler.setup_for_failure_at( int( 0.75 * test_file.size ) )
        try:
            s3am.cli.main( concat( 'upload', verbose, one_slot, src_url, self.s3_url( ) ) )
        except s3am.WorkerException:
            pass
        else:
            self.fail( )

        # Retrying without --force should fail
        self.assertRaises( s3am.UploadExistsError,
                           s3am.cli.main,
                           concat( 'upload', verbose, slots, src_url, self.s3_url( ) ) )

        # Retrying with --force should succeed. We use a different part size to ensure that
        # transfer is indeed started from scratch, not resumed.
        s3am.cli.main( concat(
            'upload', verbose, slots, src_url, self.s3_url( ),
            '--force', '--part-size', str( 2 * part_size ) ) )

        # Test force with uploading a key that already exists in the bucket
        self._test_force_resume_overwrites( force_or_resume='force', test_file=test_file,
                                            src_url=src_url )

    def test_cancel( self ):
        test_file = self.test_files[ two_and_a_half_parts ]
        src_url = self.ftp_url( test_file )

        # Run with a simulated download failure
        UnreliableHandler.setup_for_failure_at( int( 0.75 * test_file.size ) )
        self.assertRaises( s3am.WorkerException,
                           s3am.cli.main,
                           concat( 'upload', verbose, one_slot, src_url, self.s3_url( ) ) )

        # A retry without --resume should fail.
        self.assertRaises( s3am.UploadExistsError,
                           s3am.cli.main,
                           concat( 'upload', verbose, slots, src_url, self.s3_url( ) ) )

        # Cancel
        s3am.cli.main( concat( 'cancel', verbose, self.s3_url( test_file ) ) )

        # Retry, should succeed
        s3am.cli.main( concat( 'upload', verbose, slots, src_url, self.s3_url( ) ) )

    def test_copy( self ):
        # setup already created the destination bucket
        dst_bucket_name = self.test_bucket_name
        src_bucket_name = dst_bucket_name + '-src'
        with closing( s3am.boto_utils.s3_connect_to_region( copy_bucket_region ) ) as s3:
            src_location = s3am.boto_utils.region_to_bucket_location( copy_bucket_region )
            src_bucket = s3.create_bucket( src_bucket_name, location=src_location )
            try:
                self._clean_bucket( src_bucket )
                for test_file in self.test_files.itervalues( ):
                    src_url = self.ftp_url( test_file )
                    src_sse_key = '-0123456789012345678901234567890'
                    dst_sse_key = 'skdjfh9q4rusidfjs9fjsdr9vkfdh833'
                    dst_url = self.s3_url( test_file, src_bucket_name )
                    s3am.cli.main( concat(
                        'upload', verbose, slots, src_url, dst_url,
                        '--sse-key=' + src_sse_key, '--sse-key-is-master' ) )
                    src_url = dst_url
                    dst_url = self.s3_url( )
                    s3am.cli.main( concat(
                        'upload', verbose, slots, src_url, dst_url,
                        '--src-sse-key=' + src_sse_key, '--src-sse-key-is-master',
                        '--sse-key=' + dst_sse_key ) )
                    self._assert_key( test_file, dst_sse_key )
            finally:
                self._clean_bucket( src_bucket )
                src_bucket.delete( )

    def test_verify( self ):
        for test_file in self.test_files.itervalues( ):
            s3am.cli.main( concat(
                'upload', verbose, slots,
                self.ftp_url( test_file ), self.s3_url( ) ) )
            self._assert_key( test_file )
            buffer_size = s3am.operations.verify_buffer_size
            for verify_part_size in { buffer_size, part_size }:
                md5 = s3am.cli.main( concat(
                    'verify',
                    '--part-size', str( verify_part_size ),
                    self.s3_url( test_file ) ) )
                self.assertEquals( test_file.md5, md5 )

    def test_generate_key( self ):
        test_dir = mkdtemp( 'test_genkey' )
        key_file = os.path.join( test_dir, 'test.key' )

        def entropy( string ):
            """
            Calculates the Shannon entropy of a string
            http://stackoverflow.com/questions/2979174/how-do-i-compute-the-approximate-entropy-of
                                                                                       -a-bit-string

            :param str string: The string for which entropy must be calculated
            """
            # Get probability of chars in string
            prob = [ float( string.count( c ) ) / len( string )
                for c in dict.fromkeys( list( string ) ) ]
            # Calculate the entropy
            entropy = - sum( [ p * math.log( p ) / math.log( 2.0 ) for p in prob ] )
            return entropy

        try:
            for i in xrange( 0, 10 ):
                s3am.cli.main( concat( 'generate-sse-key', key_file ) )
                self.assertTrue( os.path.exists( key_file ) )
                self.assertEqual( os.stat( key_file ).st_size, 32 )
                with open( key_file ) as k_f:
                    self.assertGreater( entropy( k_f.read( ) ), 0 )
                os.remove( key_file )
        finally:
            shutil.rmtree( test_dir )

    def test_download( self ):
        for test_file in self.test_files.itervalues( ):
            s3am.cli.main( concat(
                'upload', verbose, slots,
                self.ftp_url( test_file ), self.s3_url( ) ) )
            self._assert_key( test_file )
            self._test_download( test_file )

    def _test_download( self, test_file, cleanup=True, args=() ):
        """
        :param bool|None cleanup: True, False or None to clean up always, never or only on success
        """
        dl_slots = ('--download-slots', str( num_slots ), '--checksum-slots', str( num_slots ))
        dst_path = test_file.path + '.downloaded'

        def _cleanup( ):
            for suffix in ('', '.partial', '.progress'):
                try:
                    os.unlink( dst_path + suffix )
                except OSError as e:
                    if e.errno == errno.ENOENT:
                        pass
                    else:
                        raise

        try:
            s3am.cli.main( concat(
                'download', verbose, dl_slots,
                '--part-size', str( part_size ),
                args,
                self.s3_url( test_file ),
                'file://' + dst_path ) )
            self._assert_file( test_file, dst_path )
        except:
            if cleanup:
                _cleanup( )
            raise
        else:
            if cleanup is not False:
                _cleanup( )

    def test_download_resume( self ):
        test_file = self.test_files[ two_and_a_half_parts ]
        s3am.cli.main( concat(
            'upload', verbose, slots,
            self.ftp_url( test_file ), self.s3_url( ) ) )
        self._assert_key( test_file )
        s3am.operations.Download.simulate_error( rate=1.0 )
        try:
            self.assertRaises( s3am.WorkerException, self._test_download, test_file, cleanup=None )
            # Change etag without actually changing the content (etag = f(part_size,content))
            s3am.cli.main( concat(
                'upload', verbose, slots,
                '--exists=overwrite', '--part-size', str( part_size * 2 ),
                self.ftp_url( test_file ), self.s3_url( ) ) )
            s3am.operations.Download.simulate_error( rate=0.75 )
            while True:
                try:
                    self._test_download( test_file, cleanup=None,
                                         args=[ '--download-exists=resume' ] )
                except s3am.WorkerException:
                    pass
                else:
                    break
        finally:
            s3am.operations.Download.simulate_error( rate=None )

    def _assert_file( self, test_file, path ):
        with open( path ) as f:
            self.assertEquals( test_file.md5, md5( f.read( ) ) )
        for suffix in ('.partial', '.progress'):
            self.assertFalse( os.path.exists( path + suffix ) )

    def test_inaccessible_location( self ):
        s3am.cli.main( [ 'download',
                           's3://cgl-toil-tests-disallow-getbucketlocation/README',
                           'file://%s/README' % self.ftp_root ] )
        os.unlink(os.path.join(self.ftp_root, 'README'))


class UnreliableHandler( pyftpdlib.handlers.DTPHandler ):
    """
    Lets us trigger an IO error during the download
    """

    def send( self, data ):
        self._simulate_error( data )
        return pyftpdlib.handlers.DTPHandler.send( self, data )

    lock = Lock( )
    error_at_byte = None
    sent_bytes = 0

    @classmethod
    def _simulate_error( cls, data ):
        with cls.lock:
            if cls.error_at_byte is not None:
                cls.sent_bytes += len( data )
                if cls.sent_bytes > cls.error_at_byte:
                    log.info( 'Simulating error at %i', cls.sent_bytes )
                    cls.error_at_byte = None
                    cls.sent_bytes = 0
                    raise socket.error( )
                else:
                    log.info( 'Not simulating error at %i', cls.sent_bytes )

    @classmethod
    def setup_for_failure_at( cls, offset ):
        with cls.lock:
            cls.error_at_byte = offset
            cls.sent_bytes = 0
