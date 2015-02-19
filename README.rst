S3AM is a fast, parallel, streaming multipart uploader for S3. It streams
content from any URL for which the locally installed libcurl and the remote
server support byte range requests, e.g. ``file://``, ``ftp://`` (many servers)
and ``http://`` (some servers). It uses the PyCurl bindings for libcurl and
Python's multiprocessing module to work around lock contention in the Python
interpreter and to avoid potential thread-safety issues with libcurl.

Prerequisites
=============

Python 2.7.x, libcurl and pip.


Installation
============

::

   sudo pip install git+https://github.com/BD2KGenomics/s3am.git

On OS X systems with a HomeBrew-ed Python, you should omit the sudo. You can
find out if yo have a HomeBrew-ed Python by running ``which python``. If that
prints ``/usr/local/bin/python`` you are most likely using a HomeBrew-ed Python
and you should omit ``sudo``. If it prints ``/usr/bin/python`` you need to run
``pip`` with ``sudo``.


Configuration
=============

Obtain an access and secret key for AWS. Create ``~/.boto`` with the following
contents::

   [Credentials]
   aws_access_key_id = PASTE YOUR ACCESS KEY ID HERE
   aws_secret_access_key = PASTE YOUR SECRET ACCESS KEY HERE


Usage
=====

::

   s3am --help
   s3am stream --help
   s3am cancel --help

For example::

   s3am bd2k-test-data stream \
        ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/data/NA12878/sequence_read/ERR001268.filt.fastq.gz

If an upload was interrupted you can resume it by rerunning the command with the
``--resume`` option.


Caveats
=======

S3AM uses at 5M per process for a buffer that hold an upload part. There are as
many processes as the sum of the number of download and upload slots, which
defaults to twice the number of cores. The reason for the big buffer is that S3
doesn't support chunked transfer coding and therefore must have the full part
when starting the upload of each part. To be precise, it only needs to know its
size and MD5. All but the last part have a know size but as S3AM is currently
implemented it doesn't know which part is the last part until it has fully
downloaded it. If S3AM knew the size of the source file in advance, it could
compute all of that. It could do a FTP ls or an HTTP HEAD request to determine
that. But might still need to hack around in boto in order to work around the
lack of chunked coding support in S3. Don't know what to do about MD5 not being
known.

S3AM does not implement back-to-back checksumming. An MD5 is computed for every
part uploaded to S3 but there is no code in place to compare those with the
source side. I think S3 exposes the MD5 of all part MD5's concatenated. So if
we could get libcurl and the sending server to support the Content-MD5 HTTP
header we could use that. But that would not be as strong a guarantee as
verifying the MD5 over the file in its entirety.
