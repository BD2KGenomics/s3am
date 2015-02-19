import hashlib
import os
import socket
from tempfile import mkdtemp
import unittest
import logging
import time

from pyftpdlib.handlers import DTPHandler
from pyftpdlib.ioloop import AsyncChat
from boto.s3.connection import S3Connection

import s3am
from FTPd import FTPd

test_bucket_name_prefix = 's3am-unit-tests'

host = "127.0.0.1"
port = 21212
part_size = s3am.min_part_size
test_sizes = (0, 1, part_size - 1, part_size, part_size + 1, int( part_size * 2.5 ) )


def md5( contents ):
    return hashlib.md5( contents ).digest( )


class TestFile( ):
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
    def setUp( self ):
        super( CoreTests, self ).setUp( )
        self.netloc = '%s:%s' % (host, port)
        self.url = 'ftp://%s/' % self.netloc
        self.s3 = S3Connection( )
        self.test_bucket_name = '%s-%i' % ( test_bucket_name_prefix, int( time.time( ) ) )
        self.bucket = self.s3.create_bucket( self.test_bucket_name )
        self._clean_bucket( )
        self.ftp_root = mkdtemp( prefix=__name__ )
        self.test_files = [ TestFile( self.ftp_root, size ) for size in test_sizes ]
        self.ftpd = FTPd( self.ftp_root, address=(host, port), dtp_handler=UnreliableHandler )
        logging.getLogger( 'pyftpdlib' ).setLevel( logging.WARN )
        self.ftpd.start( )

    def _clean_bucket( self ):
        for upload in self.bucket.list_multipart_uploads( ):
            upload.cancel_upload( )
        for key in self.bucket.list( ):
            key.delete( )

    def tearDown( self ):
        self.ftpd.stop( )
        self._clean_bucket( )
        self.bucket.delete( )
        self.s3.close( )
        for test_file in self.test_files:
            os.unlink( test_file.path )
        os.rmdir( self.ftp_root )

    def assert_key( self, test_file ):
        key = self.bucket.get_key( test_file.name )
        self.assertEquals( key.size, test_file.size )
        self.assertEquals( md5( key.get_contents_as_string( ) ), test_file.md5 )

    def test_streams( self ):
        for test_file in self.test_files[ :-1 ]:
            s3am.main( [ '--verbose', self.test_bucket_name, 'stream', self.url + test_file.name ] )
            self.assert_key( test_file )

    def test_resume( self ):
        global error_at_byte
        test_file = self.test_files[ -1 ]
        error_at_byte = int( 0.9 * test_file.size )
        url = self.url + test_file.name
        try:
            s3am.main( [
                '--verbose', self.test_bucket_name, 'stream', url,
                '--resume' ] )
            self.fail( "s3am should have failed with nothing to resume" )
        except SystemExit:
            pass

        try:
            s3am.main( [
                '--verbose', self.test_bucket_name, 'stream', url,
                '--download-slots', '1', '--upload-slots', '1' ] )
            self.fail( "s3am should have failed with WorkerException" )
        except s3am.WorkerException as e:
            pass
        try:
            s3am.main( [
                '--verbose', self.test_bucket_name, 'stream', url ] )
            self.fail( "s3am should have failed" )
        except SystemExit:
            pass
        # FIMXE: We should assert that the resume skips existing parts
        s3am.main( [
            '--verbose', self.test_bucket_name, 'stream', url,
            '--resume' ] )
        self.assert_key( test_file )

error_at_byte = None
sent_bytes = 0

class UnreliableHandler( DTPHandler ):
    """
    Lets us trigger an IO error during the download
    """

    def send( self, data ):
        global error_at_byte, sent_bytes
        if error_at_byte is not None:
            sent_bytes += len( data )
            if sent_bytes > error_at_byte:
                error_at_byte = None
                sent_bytes = 0
                raise socket.error( )
        return AsyncChat.send( self, data )
