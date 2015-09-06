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

import sys

from setuptools import setup, find_packages

assert sys.version_info >= (2, 7)

kwargs = dict(
    name='s3am',
    version='1.0a1',
    author='Hannes Schmidt',
    author_email='hannes@ucsc.edu',
    url='https://github.com/BD2KGenomics/cgcloud',
    description='Efficiently transfer large amounts of data to S3',
    package_dir={ '': 'src' },
    packages=find_packages( 'src' ),
    entry_points={
        'console_scripts': [
            's3am = s3am.cli:try_main'
        ]
    },
    install_requires=[ 'pycurl', 'boto' ],
    tests_require=[ 'pytest==2.7.2', 'pyftpdlib' ],
    test_suite='toil' )

from setuptools.command.test import test as TestCommand


class PyTest( TestCommand ):
    user_options = [ ('pytest-args=', 'a', "Arguments to pass to py.test") ]

    def initialize_options( self ):
        TestCommand.initialize_options( self )
        self.pytest_args = [ ]

    def finalize_options( self ):
        TestCommand.finalize_options( self )
        self.test_args = [ ]
        self.test_suite = True

    def run_tests( self ):
        import pytest
        # Sanitize command line arguments to avoid confusing Toil code attempting to parse them
        sys.argv[ 1: ] = [ ]
        errno = pytest.main( self.pytest_args )
        sys.exit( errno )


kwargs[ 'cmdclass' ] = { 'test': PyTest }

setup( **kwargs )
