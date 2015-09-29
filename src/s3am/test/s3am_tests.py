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
import os
import socket
from tempfile import mkdtemp
from threading import Lock
import unittest
import logging
import time

from boto.exception import S3ResponseError
from pyftpdlib.handlers import DTPHandler
import boto.s3

from s3am.boto_utils import work_around_dots_in_bucket_names
import s3am.operations
import s3am.cli
from FTPd import FTPd


# The dot in the domain name makes sure that boto.work_around_dots_in_bucket_names() is covered
test_bucket_name_prefix = 's3am-unit-tests.foo'
test_bucket_location = 'us-west-1'

host = "127.0.0.1"
port = 21212
part_size = s3am.operations.min_part_size
two_and_a_half_parts = int( part_size * 2.5 )
two_parts = 10 * 1024 * 1024
test_sizes = [ 0, 1, part_size - 1, part_size, part_size + 1, two_parts, two_and_a_half_parts ]

verbose = '--verbose'  # '--debug'


def md5( contents ):
    return hashlib.md5( contents ).digest( )


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


class CoreTests( unittest.TestCase ):
    @classmethod
    def setUpClass( cls ):
        super( CoreTests, cls ).setUpClass( )
        work_around_dots_in_bucket_names( )

    def setUp( self ):
        super( CoreTests, self ).setUp( )
        self.netloc = '%s:%s' % (host, port)
        self.url = 'ftp://%s/' % self.netloc
        self.s3 = boto.s3.connect_to_region( test_bucket_location )
        self.test_bucket_name = '%s-%i' % (test_bucket_name_prefix, int( time.time( ) ))
        self.bucket = self.s3.create_bucket( self.test_bucket_name, location=test_bucket_location )
        self._clean_bucket( self.bucket )
        self.ftp_root = mkdtemp( prefix=__name__ )
        self.test_files = { size: TestFile( self.ftp_root, size ) for size in test_sizes }
        self.ftpd = FTPd( self.ftp_root, address=(host, port), dtp_handler=UnreliableHandler )
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
        os.rmdir( self.ftp_root )

    def _assert_key( self, test_file, sse_key=None ):
        headers = { }
        if sse_key is not None:
            s3am.operations.Upload._add_encryption_headers( sse_key, headers )
        key = self.bucket.get_key( test_file.name, headers=headers )
        self.assertEquals( key.size, test_file.size )
        self.assertEquals( md5( key.get_contents_as_string( headers=headers ) ), test_file.md5 )

    def test_upload( self ):
        for test_file in self.test_files.itervalues( ):
            s3am.cli.main(
                [ 'upload', ('%s' % verbose), self.url + test_file.name, self.test_bucket_name ] )
            self._assert_key( test_file )

    def test_encryption( self ):
        test_file = self.test_files[ two_and_a_half_parts ]
        url = self.url + test_file.name
        sse_key = '-0123456789012345678901234567890'
        s3am.cli.main(
            [ 'upload', verbose, '--sse-key=' + sse_key, url, self.test_bucket_name ] )
        self._assert_key( test_file, sse_key=sse_key )
        # Ensure that we can't actually retrieve the object without specifying an encryption key
        try:
            self._assert_key( test_file )
        except S3ResponseError as e:
            self.assertEquals( e.status, 400 )
        else:
            self.fail( )

    def test_resume( self ):
        test_file = self.test_files[ two_and_a_half_parts ]
        url = self.url + test_file.name

        # Resume with nothing to resume
        try:
            s3am.cli.main( [ 'upload', verbose, url, self.test_bucket_name, '--resume' ] )
            self.fail( )
        except s3am.UserError as e:
            self.assertIn( "no pending upload to be resumed", e.message )

        # Run with a simulated download failure
        UnreliableHandler.setup_for_failure_at( int( 0.9 * test_file.size ) )
        try:
            s3am.cli.main( [
                'upload', verbose, url, self.test_bucket_name,
                '--download-slots', '1', '--upload-slots', '0' ] )
            self.fail( )
        except s3am.WorkerException:
            pass

        # Retry without resume
        try:
            s3am.cli.main( [ 'upload', verbose, url, self.test_bucket_name ] )
            self.fail( )
        except s3am.UserError as e:
            self.assertIn( "There is a pending upload", e.message )

        # Retry with inconsistent part size
        try:
            s3am.cli.main( [
                'upload', verbose, url, self.test_bucket_name,
                '--resume', '--part-size', str( 2 * part_size ) ] )
            self.fail( )
        except s3am.UserError as e:
            self.assertIn( "part size appears to have changed", e.message )

        # Retry
        s3am.cli.main( [ 'upload', verbose, url, self.test_bucket_name, '--resume' ] )

        # FIMXE: We should assert that the resume skips existing parts

        self._assert_key( test_file )

    def test_cancel( self ):
        test_file = self.test_files[ two_and_a_half_parts ]
        url = self.url + test_file.name

        # Run with a simulated download failure
        UnreliableHandler.setup_for_failure_at( int( 0.9 * test_file.size ) )

        try:
            s3am.cli.main( [
                'upload', verbose, url, self.test_bucket_name,
                '--download-slots', '1', '--upload-slots', '0' ] )
            self.fail( )
        except s3am.WorkerException:
            pass

        # Cancel
        s3am.cli.main( [ 'cancel', verbose, self.test_bucket_name, test_file.name ] )

        # Retry, should fail
        try:
            s3am.cli.main( [ 'upload', verbose, url, self.test_bucket_name, '--resume' ] )
            self.fail( )
        except s3am.UserError as e:
            self.assertIn( "no pending upload to be resumed", e.message )

    def test_copy( self ):
        dst_bucket_name = self.test_bucket_name
        src_bucket_name = dst_bucket_name + '-src'
        src_bucket = self.s3.create_bucket( src_bucket_name, location=test_bucket_location )
        try:
            self._clean_bucket( src_bucket )
            for test_file in self.test_files.itervalues():
                url = self.url + test_file.name
                src_sse_key = '-0123456789012345678901234567890'
                dst_sse_key = 'skdjfh9q4rusidfjs9fjsdr9vkfdh833'
                s3am.cli.main(
                    [ 'upload', verbose, '--sse-key=' + src_sse_key, url, src_bucket_name ] )
                url = '/'.join( ('s3:/', src_bucket_name, test_file.name) )
                s3am.cli.main( [
                    'upload', verbose,
                    '--src-sse-key=' + src_sse_key,
                    '--sse-key=' + dst_sse_key,
                    url, dst_bucket_name ] )
                self._assert_key( test_file, dst_sse_key )
        finally:
            self._clean_bucket( src_bucket )
            src_bucket.delete( )


class UnreliableHandler( DTPHandler ):
    """
    Lets us trigger an IO error during the download
    """

    def send( self, data ):
        self._simulate_error( data )
        return DTPHandler.send( self, data )

    lock = Lock( )
    error_at_byte = None
    sent_bytes = 0

    @classmethod
    def _simulate_error( cls, data ):
        with cls.lock:
            if cls.error_at_byte is not None:
                cls.sent_bytes += len( data )
                if cls.sent_bytes > cls.error_at_byte:
                    cls.error_at_byte = None
                    cls.sent_bytes = 0
                    raise socket.error( )

    @classmethod
    def setup_for_failure_at( cls, offset ):
        with cls.lock:
            cls.error_at_byte = offset
            cls.sent_bytes = 0
