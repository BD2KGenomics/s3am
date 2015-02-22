#!/usr/bin/env python2.7

import os

import sys
from s3am.ui import try_main

sys.path.append( os.path.join( os.path.dirname( __file__ ), 'src', 'main' ) )

if __name__ == "__main__":
    try_main( )
