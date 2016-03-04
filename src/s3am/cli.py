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
import hashlib
import logging
import sys
import inspect
import argparse

from s3am import UserError
from s3am.humanize import human2bytes
from s3am.operations import (min_part_size,
                             max_part_size,
                             max_parts_per_upload,
                             Upload,
                             Cancel,
                             Verify)


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
    kwargs = dict( requester_pays=o.requester_pays )
    if o.mode == 'upload':
        operation = Upload(
            src_url=o.src_url,
            dst_url=o.dst_url,
            resume=o.resume,
            force=o.force,
            part_size=o.part_size,
            download_slots=o.download_slots,
            upload_slots=o.upload_slots,
            sse_key=o.sse_key or o.sse_key_file or o.sse_key_base64,
            src_sse_key=o.src_sse_key or o.src_sse_key_file or o.src_sse_key_base64,
            **kwargs )
    elif o.mode == 'cancel':
        operation = Cancel(
            dst_url=o.dst_url,
            allow_prefix=o.allow_prefix,
            **kwargs )
    elif o.mode == 'verify':
        try:
            checksum = hashlib.new( o.checksum )
        except ValueError:
            raise UserError( "Checksum algorithm '%s' does not exist" % o.checksum )
        operation = Verify(
            url=o.url,
            checksum=checksum,
            sse_key=o.sse_key or o.sse_key_file or o.sse_key_base64,
            part_size=o.part_size,
            **kwargs )
    else:
        assert False
    result = operation.run( )
    if result is not None:
        print result
    return result


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
        sp.add_argument( '--requester-pays', action='store_true',
                         help="Agree to be charged for requests against buckets not owned by you. "
                              "For details refer to http://docs.aws.amazon.com/AmazonS3/latest/dev"
                              "/RequesterPaysBuckets.html" )

    p.add_argument( '--help', action=ArgParseOverallHelpAction, help="Show this help and exit." )

    sps = p.add_subparsers( dest='mode' )

    upload_sp = sps.add_parser( 'upload', add_help=False, help="Perform an upload.",
                                description="Download the contents of the given URL and upload it "
                                            "to the specified key and bucket in S3 using multiple "
                                            "processes in parallel.",
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter )

    gr = upload_sp.add_mutually_exclusive_group( )
    gr.add_argument( '--resume', action='store_true',
                     help="Resume a previously interrupted and therefore unfinished upload "
                          "for the given object if such an upload exists. When resuming an "
                          "upload, already uploaded parts will be skipped. Be advised that "
                          "unfinished uploads are more or less hidden objects that "
                          "nevertheless incur storage fees, just like regular objects. Use "
                          "'s3am cancel' to remove unfinished uploads for a given object." )

    gr.add_argument( '--force', action='store_true',
                     help="Delete all previously interrupted and therefore unfinished "
                          "uploads for the given object before beginning a new upload. "
                          "Without this flag, s3am will err on the side of caution and "
                          "exit with an error if it detects unfinished uploads." )

    defaults = default_args( Upload.__init__ )

    upload_sp.add_argument( '--download-slots', type=int, metavar='NUM',
                            default=defaults[ 'download_slots' ],
                            help="The number of processes that will concurrently upload to S3." )
    upload_sp.add_argument( '--upload-slots', type=int, metavar='NUM',
                            default=defaults[ 'download_slots' ],
                            help="The number of processes that will concurrently download from "
                                 "the SRC_URL." )

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
                                 "enough to stream the SRC_URL's content in its entirety using "
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

    def add_sse_opts( sp, helps ):
        for prefix, sse_help in helps.iteritems( ):
            sse_key_gr = sp.add_mutually_exclusive_group( )
            sse_key_gr.add_argument( prefix, metavar='KEY', type=parse_sse_key,
                                     help="The %s. If the key starts with a - (dash) character, "
                                          "the --sse-key=... form of this option must be used." %
                                          sse_help )
            sse_key_gr.add_argument( prefix + '-file', metavar='PATH', type=parse_sse_key_file,
                                     help="The path to a file containing the %s." % sse_help )
            sse_key_gr.add_argument( prefix + '-base64', metavar='KEY', type=parse_sse_key_base64,
                                     help="The base64 encoding of the %s" % sse_help )

    def sse_help( purpose='' ):
        return ("binary 32-byte key to use for " + purpose + "server-side encryption with "
                                                             "customer-provided keys (SSE-C).")

    add_sse_opts( upload_sp, {
        '--sse-key': sse_help( ) + " " +
                     "The given key will be used to encrypt the uploaded content at rest in S3. "
                     "Subsequent downloads of the object will require the same key",
        '--src-sse-key': sse_help( purpose="copying an S3 object that uses " ) + " " +
                         "This option is only applicable if SRC_URL refers starts with s3://." } )

    add_common_arguments( upload_sp )

    upload_sp.add_argument( 'src_url', metavar='SRC_URL', help="The URL to download from." )

    upload_sp.add_argument( 'dst_url', metavar='DST_URL',
                            help="The S3 URL to upload to. Must be of the form s3://BUCKET/KEY. "
                                 "If DST_URL ends in a slash, the last path component from "
                                 "SRC_URL will be appended to DST_URL." )

    cancel_sp = sps.add_parser( 'cancel', add_help=False, help="Cancel unfinished uploads.",
                                description="Cancel multipart uploads that were not completed.",
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter )

    add_common_arguments( cancel_sp )

    cancel_sp.add_argument( 'dst_url', metavar='URL',
                            help="The S3 URL for which to delete pending uploads. Must be of the "
                                 "form s3://BUCKET/KEY. URL must not end in a slash unless "
                                 "--prefix is passed." )

    cancel_sp.add_argument( '--prefix', dest='allow_prefix', action='store_true',
                            help="Treat URL as a prefix, i.e. cancel pending uploads for all "
                                 "objects whose URL starts with the given URL. By default only "
                                 "the object whose URL is an exact match will be deleted. In "
                                 "order to delete all uploads for all keys in a bucket, "
                                 "use --prefix and a URL of the form s3://BUCKET/." )

    verify_sp = sps.add_parser( 'verify', add_help=False, help="Verify the contents of a URL.",
                                description="Compute a checksum of an object at a given URL.",
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter )

    add_common_arguments( verify_sp )

    verify_sp.add_argument( 'url', metavar='URL',
                            help="" )

    # algorithms_available was introduced in 2.7.9
    algorithms = getattr( hashlib, 'algorithms_available', None ) or hashlib.algorithms
    verify_sp.add_argument( '--checksum', metavar='TYPE',
                            choices=algorithms, default='md5',
                            help="The checksum algorithm to use for verification. Valid choices "
                                 "are %s." % ', '.join( algorithms ) )

    add_sse_opts( verify_sp, {
        '--sse-key': "binary 32-byte key to use for verifying an S3 object that is encrypted with "
                     "server-side encryption using customer-provided keys (SSE-C)." } )

    def parse_verify_part_size( s ):
        i = human2bytes( s )
        if i < 1:
            raise argparse.ArgumentTypeError( "Part size must be at least 1" )
        return i

    verify_sp.add_argument( '--part-size', metavar='NUM',
                            default=defaults[ 'part_size' ], type=parse_verify_part_size,
                            help="The number of bytes in each part to verify. Verification is "
                                 "broken into parts for increased robustness." )
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
