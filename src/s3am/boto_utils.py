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

from __future__ import absolute_import
import logging

import boto.s3
from boto.s3.connection import S3Connection

log = logging.getLogger( __name__ )

old_match_hostname = None


def work_around_dots_in_bucket_names( ):
    """
    https://github.com/boto/boto/issues/2836
    """
    global old_match_hostname
    if old_match_hostname is None:
        import re
        import ssl
        try:
            old_match_hostname = ssl.match_hostname
        except AttributeError:
            log.warn( 'Failed to install workaround for dots in bucket names.' )
        else:
            hostname_re = re.compile( r'^(.*?)(\.s3(?:-[^.]+)?\.amazonaws\.com)$' )

            def new_match_hostname( cert, hostname ):
                match = hostname_re.match( hostname )
                if match:
                    hostname = match.group( 1 ).replace( '.', '' ) + match.group( 2 )
                return old_match_hostname( cert, hostname )

            ssl.match_hostname = new_match_hostname


def region_to_bucket_location( region ):
    if region == 'us-east-1':
        return ''
    else:
        return region


def bucket_location_to_region( location ):
    if location == '':
        return 'us-east-1'
    else:
        return location


def bucket_location_to_http_url( location ):
    if location:
        return 'https://s3-' + location + '.amazonaws.com'
    else:
        return 'https://s3.amazonaws.com'


def s3_connect_to_region( region ):
    """
    :param str region: the region name
    :rtype: S3Connection
    """
    s3 = boto.s3.connect_to_region( region )
    if s3 is None:
        raise RuntimeError( "The region name '%s' appears to be invalid.", region )
    else:
        return s3


def modify_metadata_retry( ):
    """
    https://github.com/BD2KGenomics/s3am/issues/16
    """
    from boto import config

    def inject_default( name, default ):
        section = 'Boto'
        value = config.get( section, name )

        if value != default:
            if not config.has_section( section ):
                config.add_section( section )
            config.set( section, name, default )

    inject_default( 'metadata_service_timeout', '5.0' )
    inject_default( 'metadata_service_num_attempts', '3' )
