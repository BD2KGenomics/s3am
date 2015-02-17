S3AM is a fast, parallel, streaming multipart uploader for S3. Streams content
from any URL for which the locally installed libcurl and the remote server
supports byte range requests, e.g. file://, ftp:// (many servers) and http://
servers. It uses the pycurl bindings for libcurl and Python's multiprocessing
module to work around lock contention in the Python interpreter and to avoid
potential thread-safety issues with libcurl.

Prerequisites
=============

Python 2.7, libcurl and pip.

Installation
============

sudo pip install git+ssh://git@github.com:BD2KGenomics/s3am.git

Usage
=====

TODO

Caveats
=======

Does not implement back-to-back checksumming. An MD5 is computed for every part
uploaded to S3 but there is no code in place to compare those with the source
side. I think S3 exposes the MD5 of all part MD5's concatenated. So if we could
get libcurl and the sending server to support the Content-MD5 HTTP header we
could use that. But that would not be as strong a guarantee as verifying the
MD5 over the file in its entirety.
