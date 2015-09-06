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

import base64
import logging
import sys
import inspect
import argparse

from s3am import UserError
from s3am.humanize import human2bytes
from s3am.operations import min_part_size, max_part_size, max_parts_per_upload, Upload, Cancel


def try_main( args=sys.argv[ 1: ] ):
    """
    Main entry point. Neither thread-safe nor reentrant but can be called repeatedly.
    """
    try:
        main( args )
    except UserError as e:
        sys.stderr.write( "error: %s\n" % e.message )
        sys.exit( 2 )


def main( args ):
    o = parse_args( args )
    if o.verbose:
        logging.getLogger( ).setLevel( logging.INFO )
    if o.debug:
        logging.getLogger( ).setLevel( logging.DEBUG )
    if o.mode == 'upload':
        operation = Upload(
            url=o.url,
            bucket_name=o.bucket_name,
            key_name=o.key_name,
            resume=o.resume,
            part_size=o.part_size,
            download_slots=o.download_slots,
            upload_slots=o.upload_slots,
            sse_key=o.sse_key or o.sse_key_file or o.sse_key_base64,
            src_sse_key=o.src_sse_key or o.src_sse_key_file or o.src_sse_key_base64 )
    elif o.mode == 'cancel':
        operation = Cancel( bucket_name=o.bucket_name,
                            key_name=o.key_name,
                            allow_prefix=o.allow_prefix )
    else:
        assert False
    operation.run(  )


def default_args( function ):
    """
    Determine the default values for keyword arguments in the given function. Returns a
    dictionary mapping argument names to default values.

    >>> def f(foo=123): pass
    >>> default_args( f )['foo']
    123
    """
    spec = inspect.getargspec( function )
    return dict( zip( spec.args[ -len( spec.defaults ): ], spec.defaults ) )


def parse_args( args ):
    """
    Parse command line arguments and set global option variable with parse result
    """
    p = argparse.ArgumentParser( add_help=False,
                                 description="Stream content from HTTP or FTP servers to S3.",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter )

    def add_common_arguments( sp ):
        sp.add_argument( '--verbose', action='store_true',
                         help="Print informational log messages." )
        sp.add_argument( '--debug', action='store_true',
                         help="Print debug log messages. WARNING: This will leak encryption keys!" )
        sp.add_argument( 'bucket_name', metavar='BUCKET',
                         help="Name of the destination S3 bucket." )

    p.add_argument( '--help', action=ArgParseOverallHelpAction, help="Show this help and exit." )

    sps = p.add_subparsers( dest='mode' )

    upload_sp = sps.add_parser( 'upload', add_help=False, help="Perform an upload.",
                                description="Download the contents of the given URL and upload it "
                                            "to the specified key and bucket in S3 using multiple "
                                            "processes in parallel.",
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter )

    upload_sp.add_argument( '--resume', action='store_true',
                            help="Attempt to resume an unfinished upload. Only works if there is "
                                 "exactly one open upload. Already uploaded pieces will be "
                                 "skipped." )

    defaults = default_args( Upload.__init__ )

    upload_sp.add_argument( '--download-slots', type=int, metavar='NUM',
                            default=defaults[ 'download_slots' ],
                            help="The number of processes that will concurrently upload to S3." )
    upload_sp.add_argument( '--upload-slots', type=int, metavar='NUM',
                            default=defaults[ 'download_slots' ],
                            help="The number of processes that will concurrently download from "
                                 "the source URL." )

    def parse_part_size( s ):
        i = human2bytes( s )
        if i < min_part_size:
            raise argparse.ArgumentTypeError( "Part size must be at least %i" % min_part_size )
        if i > max_part_size:
            raise argparse.ArgumentTypeError( "Part size must not exceed %i" % max_part_size )
        return i

    upload_sp.add_argument( '--part-size', metavar='NUM',
                            default=defaults[ 'part_size' ], type=parse_part_size,
                            help="The number of bytes in each part. This parameter must be at "
                                 "least {min} and no more than {max}. The default is {min}. Note "
                                 "that S3 allows no more than {max_parts} per upload and this "
                                 "program does not currently ensure that this parameter is large "
                                 "enough to stream the source URL's content in its entirety using "
                                 "those {max_parts} parts.".format( min=min_part_size,
                                                                    max=max_part_size,
                                                                    max_parts=max_parts_per_upload ) )

    def parse_sse_key( s ):
        if len( s ) != 32:
            raise argparse.ArgumentTypeError( "SSE-C key must be exactly 32 bytes long" )
        return s

    def parse_sse_key_file( s ):
        with open( s ) as f:
            return parse_sse_key( f.read( ) )

    def parse_sse_key_base64( s ):
        return parse_sse_key( base64.b64decode( s ) )

    sse_helps = {
        '--sse-key': "binary 32-byte key to use for server-side encryption with customer-provided "
                     "keys (SSE-C). The given key will be used to encrypt the uploaded content at "
                     "rest in S3. Subsequent downloads of the object will require the same key",
        '--src-sse-key': "binary 32-byte key to use for copying an S3 object that is encrypted "
                         "with server-side encryption using customer-provided keys (SSE-C). "
                         "This option is only applicable if the source URL starts with s3://" }

    for prefix, sse_help in sse_helps.iteritems( ):
        sse_key_gr = upload_sp.add_mutually_exclusive_group( )
        sse_key_gr.add_argument( prefix, metavar='KEY', type=parse_sse_key,
                                 help="The %s. If the key starts with a - (dash) character, "
                                      "the --sse-key=... form of this option must be used." %
                                      sse_help )
        sse_key_gr.add_argument( prefix + '-file', metavar='PATH', type=parse_sse_key_file,
                                 help="The path to a file containing the %s." % sse_help )
        sse_key_gr.add_argument( prefix + '-base64', metavar='KEY', type=parse_sse_key_base64,
                                 help="The base64 encoding of the %s" % sse_help )

    upload_sp.add_argument( 'url', metavar='URL', help="The URL to download from." )

    add_common_arguments( upload_sp )

    upload_sp.add_argument( 'key_name', nargs='?', metavar='KEY',
                            help="The key to upload to. If KEY is omitted, the last component of "
                                 "the source URL's path will be used instead." )

    cancel_sp = sps.add_parser( 'cancel', add_help=False, help="Cancel unfinished uploads.",
                                description="Cancel multipart uploads that were not completed.",
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter )

    add_common_arguments( cancel_sp )

    cancel_sp.add_argument( 'key_name', metavar='KEY',
                            help="The key, or, if --prefix is specified, the key prefix for which "
                                 "to delete all pending uploads." )

    cancel_sp.add_argument( '--prefix', dest='allow_prefix', action='store_true',
                            help="Treat KEY as a prefix, i.e. cancel uploads for all objects "
                                 "whose key starts with the given value. By default only the "
                                 "object whose key is an exact match with KEY will be deleted. In "
                                 "order to delete all uploads for all keys in a bucket, "
                                 "use --prefix with an empty string '' for KEY." )

    return p.parse_args( args )


# noinspection PyProtectedMember
class ArgParseOverallHelpAction( argparse._HelpAction ):
    def __call__( self, parser, namespace, values, option_string=None ):
        parser.print_help( )

        # retrieve subparsers from parser
        subparsers_actions = [
            action for action in parser._actions
            if isinstance( action, argparse._SubParsersAction ) ]
        # there will probably only be one subparser_action,
        # but better save than sorry
        for subparsers_action in subparsers_actions:
            # get all subparsers and print help
            for choice, subparser in subparsers_action.choices.items( ):
                sys.stderr.write( "\n\n\n".format( choice ) )
                sys.stderr.write( subparser.format_help( ) )

        parser.exit( )
