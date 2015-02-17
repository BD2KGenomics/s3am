import threading

from pyftpdlib.authorizers import DummyAuthorizer

from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer


class FTPd( threading.Thread ):
    def __init__( self, root_dir, address=None, timeout=0.001, dtp_handler=None):
        threading.Thread.__init__( self )
        self.__flag = threading.Event( )
        self.__timeout = timeout
        authorizer = DummyAuthorizer( )
        authorizer.add_anonymous( root_dir )
        handler = FTPHandler
        handler.authorizer = authorizer
        if dtp_handler is not None:
            handler.dtp_handler = dtp_handler
        self.server = FTPServer( address, handler )

    def start( self ):
        self.__flag.clear( )
        threading.Thread.start( self )
        self.__flag.wait( )

    def run( self ):
        self.__flag.set( )
        while self.__flag.is_set( ):
            self.server.serve_forever( timeout=self.__timeout, blocking=False )
        self.server.close_all( )
        self.server.close()

    def stop( self ):
        self.__flag.clear( )
        self.join( )
