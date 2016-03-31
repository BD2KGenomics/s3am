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
import base64
import functools
import random
from collections import namedtuple
from contextlib import closing, contextmanager
import hashlib
from operator import itemgetter
from bd2k.util.ec2.credentials import enable_metadata_credential_caching

import time
import sys

import abc
import os
import pycurl
import multiprocessing
import itertools
from io import BytesIO
import signal
import traceback
from urlparse import urlparse
from StringIO import StringIO

from boto import handler
from boto.resultset import ResultSet
from boto.s3.connection import S3Connection, xml

from boto.s3.multipart import MultiPartUpload, Part

from boto.exception import S3ResponseError

from s3am import (me,
                  log,
                  ObjectExistsError,
                  UploadExistsError,
                  InvalidSourceURLError,
                  InvalidDestinationURLError,
                  InvalidS3URLError,
                  InvalidPartSizeError,
                  InvalidEncryptionKeyError,
                  WorkerException)

from s3am.boto_utils import (work_around_dots_in_bucket_names,
                             s3_connect_to_region,
                             bucket_location_to_region,
                             bucket_location_to_http_url)
from s3am.humanize import bytes2human, human2bytes

max_part_per_page = 1000  # http://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
max_uploads_per_page = 1000
min_part_size = human2bytes( "5M" )
max_part_size = human2bytes( "5G" )
max_parts_per_upload = 10000

verify_buffer_size = 1024 * 1024

# The multiprocessing module needs semaphores to be declared at the top level. I'm assuming this
# applies to events, too. The semaphore's initial value depends on a command line option so we
# need to instantiate it later, in stream(). The second reason for initializing them later is
# that we want stream() to be called multiple times per program invocation, e.g. for unit tests.
#
download_slots_semaphore = None
done_event = None
error_event = None

num_cores = multiprocessing.cpu_count( )


class SSEKey( namedtuple( '_SSEKey', 'binary is_master' ) ):
    """
    A namedtuple of the binary SSE-C key and its attributes.

    The ``binary`` attribute contains the 32-byte binary SSE-C key.

    The ``is_master`` attribute specifies whether the key is a master key and should therefore
    not be used directly. The actual encryption key that is used to encrypt the object is derived
    from the master key the object's URL.
    """

    def __nonzero__( self ):
        return bool( self.binary )

    def __str__( self ):
        return self.binary

    def resolve( self, bucket_location, bucket_name, key_name ):
        if self.is_master:
            base_url = bucket_location_to_http_url( bucket_location )
            url = '/'.join( [ base_url, bucket_name, key_name ] )
            binary = hashlib.sha256( self.binary + str( url ) ).digest( )
            assert len( binary ) == 32
            return SSEKey( binary, False )
        else:
            return self


class Operation( object ):
    """
    A S3AM operation
    """

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def run( self ):
        raise NotImplementedError( )

    def __init__( self, requester_pays=False ):
        """
        :param bool requester_pays: whether to opt-in to the requester pays feature on the bucket
        """
        super( Operation, self ).__init__( )
        self.requester_pays = requester_pays
        work_around_dots_in_bucket_names( )
        enable_metadata_credential_caching( )

    @staticmethod
    def _add_encryption_headers( sse_key, headers, for_copy=False ):
        """
        :param SSEKey sse_key: the 32-byte binary key
        :param dict headers: the S3 request headers to populate
        :param bool for_copy: whether to populate the headers for the source of a copy request
        """
        # ensure key has been resolved
        assert not sse_key.is_master
        sse_key = sse_key.binary
        assert len( sse_key ) == 32
        encoded_sse_key = base64.b64encode( sse_key )
        encoded_sse_key_md5 = base64.b64encode( hashlib.md5( sse_key ).digest( ) )
        prefix = 'x-amz%s-server-side-encryption-customer-' % ('-copy-source' if for_copy else '')
        headers[ prefix + 'algorithm' ] = 'AES256'
        headers[ prefix + 'key' ] = encoded_sse_key
        headers[ prefix + 'key-md5' ] = encoded_sse_key_md5

    def _get_default_headers( self ):
        headers = { }
        if self.requester_pays:
            headers[ 'x-amz-request-payer' ] = 'requester'
        return headers

    # Work around bucket.get_location() not supporting the headers keyword argument

    def get_bucket_location( self, bucket, headers=None ):
        response = bucket.connection.make_request( 'GET', bucket.name,
                                                   query_args='location', headers=headers )
        body = response.read( )
        if response.status == 200:
            rs = ResultSet( bucket )
            h = handler.XmlHandler( rs, bucket )
            if not isinstance( body, bytes ):
                body = body.encode( 'utf-8' )
            xml.sax.parseString( body, h )
            # noinspection PyUnresolvedReferences
            return rs.LocationConstraint
        else:
            raise bucket.connection.provider.storage_response_error(
                response.status, response.reason, body )


class BucketModification( Operation ):
    """
    An operation that modifies a bucket
    """

    __metaclass__ = abc.ABCMeta

    def __init__( self, dst_url, **kwargs ):
        """
        :param str dst_url: URL of object in S3 to upload to
        :param kwargs: keyword arguments to be passed to the super constructor
        """
        super( BucketModification, self ).__init__( **kwargs )
        dst_url = urlparse( dst_url )
        if dst_url.scheme == 's3' and dst_url.netloc and dst_url.path.startswith( '/' ):
            self.bucket_name = dst_url.netloc
            assert dst_url.path.startswith( '/' )
            self.key_name = dst_url.path[ 1: ]
            with closing( S3Connection( ) ) as s3:
                headers = self._get_default_headers( )
                bucket = s3.get_bucket( self.bucket_name, headers=headers )
                self.bucket_location = self.get_bucket_location( bucket, headers=headers )
        else:
            raise InvalidS3URLError( "Invalid S3 URL for destination (%s). Must be of the form "
                                     "s3://BUCKET/ or s3://BUCKET/KEY." % dst_url )

    @property
    def bucket_region( self ):
        return bucket_location_to_region( self.bucket_location )

    def _get_uploads( self, bucket, limit=max_uploads_per_page, allow_prefix=False ):
        """
        Get all open multipart uploads for the user-specified key
        """
        if limit > max_uploads_per_page:
            raise ValueError( "Limit must not exceed %i" % max_uploads_per_page )
        headers = self._get_default_headers( )
        uploads = bucket.get_all_multipart_uploads( prefix=self.key_name,
                                                    max_uploads=limit,
                                                    headers=headers )
        if len( uploads ) == max_uploads_per_page:
            raise RuntimeError( "Can't handle more than %i uploads" % max_uploads_per_page )

        uploads = [ MultiPartUploadPlus( bucket=bucket,
                                         key_name=upload.key_name,
                                         upload_id=upload.id,
                                         headers=headers )
            for upload in uploads if allow_prefix or upload.key_name == self.key_name ]

        return uploads


# noinspection PyIncorrectDocstring
class MultiPartUploadPlus( MultiPartUpload ):
    """
    There is no built-in way to just get a MultiPartUpload object without its children part
    objects. There is either ListParts or ListMultiPartUploads. We can, however, fake one
    by creating an empty MultiPartObject and fill in the attributes we know are needed for the
    method calls we perform on that object.
    """

    def __init__( self, bucket, key_name, upload_id, parts=None, headers=None ):
        """
        Constructs a multi-part object and optionally pass a list of parts. If the parts are
        supplied, the resulting object can be used to complete the upload via its
        complete_upload() method.

        :type parts: list[Part]
        """
        super( MultiPartUploadPlus, self ).__init__( bucket )
        self.id = upload_id
        self.key_name = key_name
        self._parts = parts
        self._headers = headers

    def __iter__( self ):
        if self._parts is None:
            return super( MultiPartUploadPlus, self ).__iter__( )
        else:
            return iter( self._parts )

    # Work around the fact that Boto's get_all_parts() and __iter__() do not support the headers 
    # keyword argument and the fact that get_all_parts() returns None on error rather than raising 
    # an exception

    def get_all_parts( self, max_parts=None, part_number_marker=None,
                       encoding_type=None ):
        """
        Return the uploaded parts of this MultiPart Upload.  This is
        a lower-level method that requires you to manually page through
        results.  To simplify this process, you can just use the
        object itself as an iterator and it will automatically handle
        all of the paging with S3.
        """
        self._parts = [ ]
        query_args = 'uploadId=%s' % self.id
        if max_parts:
            query_args += '&max-parts=%d' % max_parts
        if part_number_marker:
            query_args += '&part-number-marker=%s' % part_number_marker
        if encoding_type:
            query_args += '&encoding-type=%s' % encoding_type
        conn = self.bucket.connection
        response = conn.make_request( 'GET', self.bucket.name,
                                      self.key_name,
                                      query_args=query_args,
                                      headers=self._headers )
        body = response.read( )
        if response.status == 200:
            h = handler.XmlHandler( self, self )
            xml.sax.parseString( body, h )
            return self._parts
        else:
            raise conn.provider.storage_response_error( response.status, response.reason, body )

    # Work around lack of headers suport in complete_upload()

    def complete_upload( self, headers=None ):
        """
        Complete the MultiPart Upload operation.  This method should
        be called when all parts of the file have been successfully
        uploaded to S3.

        :rtype: :class:`boto.s3.multipart.CompleteMultiPartUpload`
        :returns: An object representing the completed upload.
        """
        return self.bucket.complete_multipart_upload( self.key_name,
                                                      self.id,
                                                      self.to_xml( ),
                                                      headers=headers )

    # Work around lack of headers suport in cancel_upload()

    def cancel_upload( self, headers=None ):
        """
        Cancels a MultiPart Upload operation.  The storage consumed by
        any previously uploaded parts will be freed. However, if any
        part uploads are currently in progress, those part uploads
        might or might not succeed. As a result, it might be necessary
        to abort a given multipart upload multiple times in order to
        completely free all storage consumed by all parts.
        """
        self.bucket.cancel_multipart_upload( self.key_name, self.id, headers=headers )


class Cancel( BucketModification ):
    """
    Cancel an unfinished upload
    """

    def __init__( self, dst_url, allow_prefix, **kwargs ):
        super( Cancel, self ).__init__( dst_url, **kwargs )
        if (self.key_name.endswith( '/' ) or self.key_name == '') and not allow_prefix:
            raise InvalidDestinationURLError(
                "Make sure the destination URL does not end in / or pass --prefix if you really "
                "intend to delete uploads for all objects whose key starts with '%s'." %
                self.key_name )
        self.allow_prefix = allow_prefix

    def run( self ):
        with closing( s3_connect_to_region( self.bucket_region ) ) as s3:
            headers = self._get_default_headers( )
            bucket = s3.get_bucket( self.bucket_name, headers=headers )
            uploads = self._get_uploads( bucket, allow_prefix=self.allow_prefix )
            for upload in uploads:
                upload.cancel_upload( headers=headers )


class Upload( BucketModification ):
    """
    Perform or resume an upload
    """

    def __init__( self, src_url, dst_url,
                  resume=False, force=False, exists=None, part_size=min_part_size,
                  download_slots=num_cores, upload_slots=num_cores,
                  sse_key=None, src_sse_key=None, **kwargs ):
        """
        :param str src_url: URL or path to upload from
        :param str dst_url: URL of object in S3 to upload to
        :param bool resume: whether to resume an incomplete multipart upload
        :param bool force: whether to remove any incomplete multipart uploads before proceeding
        :param str exists: whether to 'overwrite', 'skip' or fail (None) if destination exists
        :param int part_size: the size in bytes of each part in the multipart upload
        :param download_slots: the number concurrent requests to download parts
        :param upload_slots: the number concurrent requests to upload parts
        :param SSEKey sse_key: the SSE-C key for encrypting the destination object
        :param SSEKey src_sse_key: the SSE-C key for decrypting the source object (if in S3)
        :param kwargs: keyword arguments to be passed to the super constructor
        """
        super( Upload, self ).__init__( dst_url, **kwargs )
        # Neither dot nor slash occur in a valid URL's scheme part so we can use those to detect
        # a path, even if that path contains a colon. Also anything without a colon can't be a
        # URL and we'll assume it is a path.
        if src_url[ 0 ] in './' or ':' not in src_url:
            src_url = 'file://' + os.path.abspath( src_url )
        parsed_src_url = urlparse( src_url )
        if parsed_src_url.scheme == 'file' and parsed_src_url.netloc not in ('', 'localhost'):
            raise InvalidSourceURLError(
                "The URL '%s' is not a valid file:// URL. For absolute paths use "
                "file:/ABSOLUTE/PATH/TO/FILE, file:///ABSOLUTE/PATH/TO/FILE or just "
                "/ABSOLUTE/PATH/TO/FILE. For relative paths use RELATIVE/PATH/TO/FILE. To refer "
                "to a file called FILE in the current working directory, use FILE." % src_url )
        src_url = parsed_src_url
        if self.key_name.endswith( '/' ) or self.key_name == '':
            self.key_name += os.path.basename( src_url.path )
        src_url = src_url.geturl( )
        self.url = src_url
        self.part_size = part_size
        self.resume = resume
        self.force = force
        self.exists = exists
        self.download_slots = download_slots
        self.upload_slots = upload_slots
        self.sse_key = sse_key
        self.src_sse_key = src_sse_key

    def run( self ):
        """
        Stream a URL to a key in an S3 bucket using a parallelized multi-part upload.
        """
        global download_slots_semaphore, done_event, error_event

        download_slots_semaphore = multiprocessing.Semaphore( self.download_slots )
        done_event = multiprocessing.Event( )
        error_event = multiprocessing.Event( )

        upload_id, completed_parts = self._prepare_upload( )
        part_nums = itertools.count( )
        num_workers = self.download_slots + self.upload_slots
        workers = multiprocessing.Pool( num_workers, _init_worker )

        # Stagger the workers a bit, to avoid swamping the metadata service
        # See https://github.com/BD2KGenomics/s3am/issues/16 for details
        for _ in range( num_workers ):
            workers.apply_async( functools.partial( time.sleep, random.random( ) + .5 ) )

        def complete_part( ( part_num_, part ) ):
            if part is not None:
                assert part_num_ not in completed_parts
                completed_parts[ part_num_ ] = part

        if self.url.startswith( 's3:' ):
            log.info( 'Copying %s' % self.url )
            url = urlparse( self.url )
            assert url.scheme == 's3'
            assert url.path.startswith( '/' )
            with closing( S3Connection( ) ) as s3:
                headers = self._get_default_headers( )
                src_bucket = s3.get_bucket( url.netloc, headers=headers )
                src_key = url.path[ 1: ]
                if self.src_sse_key:
                    self.src_sse_key = self.src_sse_key.resolve(
                        bucket_location=self.get_bucket_location( src_bucket, headers=headers ),
                        bucket_name=src_bucket.name,
                        key_name=src_key )
                    self._add_encryption_headers( self.src_sse_key, headers )
                src_key = src_bucket.get_key( src_key, headers=headers )
            worker_func = self._copy_part
            kwargs = dict(
                src_bucket_name=src_bucket.name,
                src_key_name=src_key.name,
                size=src_key.size )
        else:
            log.info( 'Streaming %s' % self.url )
            kwargs = { }
            worker_func = self._stream_part

        try:
            while not done_event.is_set( ):
                if error_event.is_set( ):
                    raise WorkerException( )
                part_num = next( part_nums )
                if part_num in completed_parts:
                    assert self.resume
                    log.info( 'part %i: exists', part_num )
                else:
                    download_slots_semaphore.acquire( )
                    log.info( 'part %i: dispatching', part_num )
                    workers.apply_async( func=worker_func,
                                         args=[ upload_id, part_num ],
                                         kwds=kwargs,
                                         callback=complete_part )
            workers.close( )
            workers.join( )
            if error_event.is_set( ):
                raise WorkerException( )
            self._sanity_check( completed_parts )
            with closing( s3_connect_to_region( self.bucket_region ) ) as s3:
                headers = self._get_default_headers( )
                bucket = s3.get_bucket( self.bucket_name, headers=headers )
                upload = MultiPartUploadPlus( bucket=bucket,
                                              key_name=self.key_name,
                                              upload_id=upload_id,
                                              parts=completed_parts.values( ) )
                upload.complete_upload( headers=headers )
            log.info( 'Completed %s' % self.url )
        except WorkerException:
            workers.close( )
            workers.join( )
            raise
        except (Exception, KeyboardInterrupt):
            workers.close( )
            workers.terminate( )
            raise

    def _prepare_upload( self ):
        """
        Prepare a new multipart upload or resume a previously interrupted one. Returns the upload
        ID and a dictionary mapping the 0-based index of a part to its size.
        """
        with closing( s3_connect_to_region( self.bucket_region ) ) as s3:
            headers = self._get_default_headers( )
            bucket = s3.get_bucket( self.bucket_name, headers=headers )

            # Poppulate encryption headers if necessary
            if self.sse_key:
                self.sse_key = self.sse_key.resolve( bucket_location=self.bucket_location,
                                                     bucket_name=self.bucket_name,
                                                     key_name=self.key_name )
                self._add_encryption_headers( self.sse_key, headers=headers )

            # Check if the object exists at the destination before continuing.  If the user has
            # not specified that they want to overwrite the existing data, then we exit before we
            # modify any unfinished uploads.
            try:
                if bucket.get_key( self.key_name, headers=headers ):
                    if self.exists == 'overwrite':
                        log.warn( 'Key (%s) already exists in bucket (%s). Overwriting the '
                                  'data.', self.key_name, self.bucket_name )
                        # Continue with the rest of the script
                    elif self.exists == 'skip':
                        # Silently exit
                        log.warn( 'Key (%s) already exists in bucket (%s). Skipping.',
                                  self.key_name,
                                  self.bucket_name )
                        sys.exit( 0 )
                    else:
                        # If we don't want to overwrite, then we need to quit at this point
                        raise ObjectExistsError( 'Object %s already exists in bucket %s' %
                                                 (self.key_name, self.bucket_name) )
            except S3ResponseError as err:
                if err.status == 403:
                    if self.sse_key:
                        raise InvalidEncryptionKeyError(
                            'The destination URL exists and the input key could not be used to '
                            'access it (got a 403 error from S3).  This usually means the key '
                            'differs from the one used for encrypting the object when it was '
                            'uploaded.' )
                    else:
                        raise InvalidEncryptionKeyError(
                            'The destination URL exists but could not be accessed (got a 403 '
                            'error from S3).  This suggests that the remote file is encrypted. '
                            'This operation cannot be completed without the encryption key that '
                            'was used when the object was uploaded.' )
                else:
                    raise

            # Collect the pending uploads prefixed with key_name
            uploads = self._get_uploads( bucket )

            while True:
                if len( uploads ) == 0:
                    # Necessary when uploader and bucket owner are differnt AWS accounts. Without
                    # this, only an ACL for the uploader will be created. With this header,
                    # an ACL for the uploader and one for the bucket owner will be created.
                    headers[ 'x-amz-acl' ] = 'bucket-owner-full-control'
                    upload_id = bucket.initiate_multipart_upload( key_name=self.key_name,
                                                                  headers=headers ).id
                    return upload_id, { }
                elif self.resume:
                    if len( uploads ) == 1:
                        upload = uploads[ 0 ]
                        completed_parts = { part.part_number - 1: part for part in upload }
                        # If there is an upload but no parts we can use whatever part size we want,
                        # otherwise we need to ensure that we use the same part size as before.
                        if len( completed_parts ) > 0:
                            previous_part_size = self._guess_part_size( completed_parts )
                            if self.part_size != previous_part_size:
                                raise InvalidPartSizeError(
                                    "Transfer failed. The part size appears to have changed from "
                                    "%i to %i. Either resume the upload with the previous part "
                                    "size or cancel it before using the new part size." % (
                                        self.part_size, previous_part_size) )
                        return upload.id, completed_parts
                    else:
                        raise RuntimeError(
                            "Transfer failed. There are multiple unfinished uploads, so there is "
                            "ambiguity as to which one of them should be resumed. Consider using "
                            "'{me} cancel s3://{bucket_name}/{key_name}' to delete all of them "
                            "before trying the transfer again. Note that unfinished uploads incur "
                            "storage fees.".format( me=me, **vars( self ) ) )
                elif self.force:
                    log.warn( 'Cancelling all unfinished uploads before proceeding.' )
                    for upload in uploads:
                        bucket.cancel_multipart_upload( upload.key_name,
                                                        upload.id,
                                                        headers=self._get_default_headers( ) )
                    uploads = [ ]
                else:
                    if len( uploads ) == 1:
                        raise UploadExistsError(
                            "Transfer failed. There is an unfinished upload. To resume that "
                            "upload, run {me} again with --resume. To cancel it, use '{me} cancel "
                            "s3://{bucket_name}/{key_name}'. Note that unfinished uploads incur "
                            "storage fees.".format( me=me, **vars( self ) ) )
                    else:
                        raise RuntimeError(
                            "Transfer failed. Detected unfinished multipart uploads. Consider "
                            "using '{me} cancel s3://{bucket_name}/{key_name}' to delete all of "
                            "them before trying the transfer again. Note that pending uploads "
                            "incur storage fees.".format( me=me, **vars( self ) ) )

    def _guess_part_size( self, completed_parts ):
        size_groups = self._part_size_histogram( completed_parts )
        assert len( size_groups ) > 0
        # We can't handle more than two different sizes (first term) and if we have two different
        # sizes, the smaller one should only occur once (second term).
        if len( size_groups ) > 2 \
            or len( size_groups ) == 2 and size_groups[ 1 ][ 1 ] != 1:
            raise RuntimeError(
                "Can't reliably determine previously used part size for this upload. "
                "You should probably cancel it and start over." )
        return size_groups[ 0 ][ 0 ]

    @contextmanager
    def _propagate_worker_exception( self ):
        """
        A context manager that sets the error_event if any exception occurs in the body. It can
        be safely assumed that this context manager does not cause any exceptions before it
        yields to the enclosed block.
        """
        try:
            yield
        except BailoutException:
            raise
        except BaseException:
            error_event.set( )
            log.error( traceback.format_exc( ) )
            raise

    def _stream_part( self, upload_id, part_num ):
        """
        Download o part from the source URL, buffer it in memory and then upload it to S3.
        """
        with self._propagate_worker_exception( ):
            try:
                log.info( 'part %i: downloading', part_num )
                buf = self._download_part( part_num )
            finally:
                download_slots_semaphore.release( )
            download_size = buf.tell( )
            self._log_progress( part_num, 'downloaded', download_size )
            if download_size > self.part_size:
                assert False
            elif download_size < self.part_size:
                done_event.set( )
            else:
                pass
            if error_event.is_set( ): raise BailoutException( )
            if download_size > 0 or part_num == 0:
                log.info( 'part %i: uploading', part_num )
                buf.seek( 0 )
                part = self._upload_part( upload_id, part_num, buf )
                upload_size = buf.tell( )
                assert download_size == upload_size == part.size
                self._log_progress( part_num, 'uploaded', upload_size )
            else:
                part = None
            return part_num, part

    def _log_progress( self, part_num, task, download_size ):
        log.info( 'part %i: %s %sB (%i bytes)',
                  part_num, task, bytes2human( download_size ), download_size )

    def _download_part( self, part_num ):
        """
        Download a part from the source URL. Returns a BytesIO buffer. The buffer's tell() method
        will return the size of the downloaded part, which may be less than the requested part
        size if the part is the last one for the URL.
        """
        buf = BytesIO( )
        with closing( pycurl.Curl( ) ) as c:
            c.setopt( c.URL, self.url )
            c.setopt( c.WRITEDATA, buf )
            c.setopt( c.FAILONERROR, 1 )
            start, end = self._get_part_range( part_num )
            c.setopt( c.RANGE, "%i-%i" % (start, end - 1) )
            try:
                c.perform( )
            except pycurl.error as e:
                error_code, message = e
                if error_code == c.E_BAD_DOWNLOAD_RESUME:  # bad range for FTP
                    pass
                elif error_code == c.E_HTTP_RETURNED_ERROR:
                    # http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html#sec10.4.17
                    if c.getinfo( c.RESPONSE_CODE ) == 416:
                        pass
                    else:
                        raise
                else:
                    raise
                buf.truncate( 0 )
                buf.seek( 0 )
        return buf

    def _get_part_range( self, part_num ):
        """
        Returns a tuple (start,end) with start being the offset of the first byte to copy and end
        being the offset of the byte after the last byte to copy.

        Invariant:  end - start == self.part_size
        """
        start = part_num * self.part_size
        end = start + self.part_size
        return start, end

    def _upload_part( self, upload_id, part_num, buf ):
        """
        Upload a part to S3. The given buffer's tell() is assumed to point at the first byte to
        be uploaded. When this method returns, the tell() method points at the end of the buffer.

        :return: the part object representing the uploaded part
        :rtype: Part
        """
        with closing( s3_connect_to_region( self.bucket_region ) ) as s3:
            headers = self._get_default_headers( )
            bucket = s3.get_bucket( self.bucket_name, headers=headers )
            upload = MultiPartUploadPlus( bucket=bucket,
                                          key_name=self.key_name,
                                          upload_id=upload_id,
                                          headers=headers )
            if self.sse_key:
                self._add_encryption_headers( self.sse_key, headers )
            key = upload.upload_part_from_file( buf, part_num + 1, headers=headers )
            return self._part_for_key( bucket, part_num, key )

    def _copy_part( self, upload_id, part_num, src_bucket_name, src_key_name, size ):
        with self._propagate_worker_exception( ):
            try:
                if error_event.is_set( ): raise BailoutException( )
                log.info( 'part %i: copying', part_num )
                start, end = self._get_part_range( part_num )
                end = min( end, size )
                part_size = end - start
                if part_size > 0 or part_num == 0:
                    url = urlparse( self.url )
                    assert url.scheme == 's3'
                    assert url.path.startswith( '/' )
                    with closing( s3_connect_to_region( self.bucket_region ) ) as s3:
                        headers = self._get_default_headers( )
                        bucket = s3.get_bucket( self.bucket_name, headers=headers )
                        upload = MultiPartUploadPlus( bucket=bucket,
                                                      key_name=self.key_name,
                                                      upload_id=upload_id,
                                                      headers=headers )
                        if self.sse_key:
                            self._add_encryption_headers( self.sse_key, headers )
                        if part_size == 0:
                            # Since copy_part_from_key doesn't allow empty ranges, we handle that
                            # case by uploading an empty part.
                            assert part_num == 0
                            # noinspection PyTypeChecker
                            key = upload.upload_part_from_file(
                                StringIO( ), part_num + 1,
                                headers=headers )
                        else:
                            if self.src_sse_key:
                                self._add_encryption_headers( self.src_sse_key, headers,
                                                              for_copy=True )
                            key = upload.copy_part_from_key(
                                src_bucket_name=src_bucket_name,
                                src_key_name=src_key_name,
                                part_num=part_num + 1,
                                start=start,
                                end=end - 1,
                                headers=headers )
                            # somehow copy_part_from_key doesn't set the key size
                            key.size = part_size
                    assert key.size == part_size
                    self._log_progress( part_num, 'copied', part_size )
                    return part_num, self._part_for_key( bucket, part_num, key )
                else:
                    done_event.set( )
                    return part_num, None
            finally:
                download_slots_semaphore.release( )

    def _part_for_key( self, bucket, part_num, key ):
        """
        Most of boto's multipart-related methods return a Key object rather than a part object.
        This method converts the former to the latter. Ultimately, we need a MultiPartUpload with
        a list of Part objects in order to complete a multi-part upload.

        :rtype : Part
        """
        part = Part( )
        part.bucket = bucket
        part.part_number = part_num + 1
        part.size = key.size
        part.etag = key.etag
        part.last_modified = key.last_modified
        return part

    def _sanity_check( self, completed_parts ):
        """
        Verify that all parts are present and valid.
        """

        # Check uniqueness property (no duplicate part_nums)
        assert isinstance( completed_parts, dict )

        # At least one part
        assert len( completed_parts ) > 0

        # Check completeness (no missing part nums)
        assert max( completed_parts ) >= len( completed_parts ) - 1

        # The parts should belong to exaclty one or two distinct size groups.
        groups = self._part_size_histogram( completed_parts )

        def sentinel( part_size, num_parts ):
            # S3 needs at least one part per upload so for empty files we need a sentinel part
            return part_size == 0 and num_parts == 1

        def one_or_more_full_parts( part_size, num_parts ):
            return part_size == self.part_size and num_parts > 0

        def one_last_part( part_size, num_parts ):
            return 0 < part_size < self.part_size and num_parts == 1

        def xor( *bools ):
            return bools.count( True ) == 1

        if len( groups ) == 1:
            assert xor( sentinel( *groups[ 0 ] ),
                        one_or_more_full_parts( *groups[ 0 ] ),
                        one_last_part( *groups[ 0 ] ) )
        elif len( groups ) == 2:
            assert one_or_more_full_parts( *groups[ 0 ] ) and one_last_part( *groups[ 1 ] )
        else:
            assert False

    def _part_size_histogram( self, completed_parts ):
        """
        Group input parts by size and return the length of each group.

        :param completed_parts: a dictionary mapping part numbers to part sizes

        :type completed_parts: dict[int,boto.s3.key.Key]

        :return: A list of (part_size, num_parts) tuples where part_size is the size of a part and
        num_parts the number of parts of that size in the input. The returned list is sorted by
        descending part size. Note that there can't be any empty groups in the result, so the second
        member of each tuple is guaranteed to be greater than zero.
        """

        by_part_size = itemgetter( 1 )

        # Convert to list of ( part_num, part_size ) pairs, sorted by descending size
        completed_parts = [ (part_num, part.size) for part_num, part in
            completed_parts.iteritems( ) ]
        completed_parts.sort( key=by_part_size, reverse=True )

        # Count # of elements in iterator
        def ilen( it ):
            return sum( 1 for _ in it )

        return [ (part_size, ilen( group )) for part_size, group in
            itertools.groupby( completed_parts, by_part_size ) ]


class BailoutException( RuntimeError ):
    pass


def _init_worker( ):
    """
    Hacks around weirdness with the multiprocessing module and Ctrl-C
    """
    signal.signal( signal.SIGINT, signal.SIG_IGN )


# Allow instance methods to be used with multiprocessing
#
# Stolen from https://gist.github.com/fiatmoney/1086393

def _pickle_method( method ):
    func_name = method.im_func.__name__
    obj = method.im_self
    cls = method.im_class
    if func_name.startswith( '__' ) and not func_name.endswith( '__' ):  # deal with mangled names
        cls_name = cls.__name__.lstrip( '_' )
        func_name += '_' + cls_name
    return _unpickle_method, (func_name, obj, cls)


def _unpickle_method( func_name, obj, cls ):
    for cls in cls.__mro__:
        try:
            func = cls.__dict__[ func_name ]
        except KeyError:
            pass
        else:
            return func.__get__( obj, cls )
    assert False


import copy_reg
import types

copy_reg.pickle( types.MethodType, _pickle_method, _unpickle_method )


class Verify( Operation ):
    """
    Compute a checksum of the content at a given URL.
    """

    def __init__( self, url, checksum, sse_key, part_size, **kwargs ):
        super( Verify, self ).__init__( **kwargs )
        url = urlparse( url )
        if url.scheme != 's3':
            raise NotImplementedError(
                "Only 's3://' URLs are currently supported, not '%s://'." % url.scheme )
        if not url.netloc or not url.path.startswith( '/' ):
            raise InvalidS3URLError( "Invalid S3 URL for destination (%s). Must be of the form "
                                     "s3://BUCKET/ or s3://BUCKET/KEY." % url )
        self.url = url
        self.checksum = checksum
        self.sse_key = sse_key
        self.part_size = part_size

    def run( self ):
        assert self.url.scheme == 's3' and self.url.netloc and self.url.path.startswith( '/' )
        with closing( S3Connection( ) ) as s3:
            headers = self._get_default_headers( )
            bucket = s3.get_bucket( self.url.netloc, headers=headers )
            key = bucket.get_key( self.url.path[ 1: ], headers=headers )
            if self.sse_key:
                self._add_encryption_headers( self.sse_key, headers )
            start, size = 0, key.size
            while start < size:
                end = min( start + self.part_size, size )
                key.open_read( headers=dict( headers,
                                             Range='bytes=%i-%i' % (start, end - 1) ) )
                try:
                    while True:
                        buf = key.read( verify_buffer_size )
                        if not buf: break
                        self.checksum.update( buf )
                finally:
                    key.close( )
                start = end
        return self.checksum.hexdigest( )


class GenerateSSEKey( object ):
    """
    Generate a random 32-byte key for file encryption using SSE-C.
    """
    def __init__(self, key_file):
        """
        :param str key_file: A path to a file that will be used to store the generated key
        """
        self.key_file = key_file

    def run( self ):
        assert os.path.exists(os.path.dirname(self.key_file))
        log.info( 'Writing key to (%s)' % self.key_file)
        with open(self.key_file, 'w') as fH:
            fH.write( os.urandom( 32 ) )
