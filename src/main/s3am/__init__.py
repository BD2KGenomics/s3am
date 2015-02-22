import logging
import os
import sys

me = os.path.basename( sys.argv[ 0 ] )

log = logging.getLogger( __name__ )

logging.basicConfig( level=logging.WARN,
                     format="%(asctime)-15s %(module)s(%(process)d) %(message)s" )


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


