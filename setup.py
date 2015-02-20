import sys

from setuptools import setup, find_packages

assert sys.version_info >= (2, 7)

setup(
    name="s3am",
    version="0.1.dev1",
    package_dir={ '': 'src/main' },
    packages=find_packages( 'src/main' ),
    entry_points={
        'console_scripts': [
            's3am = s3am:try_main'
        ]
    },
    install_requires=[ 'pycurl', 'boto' ],
    tests_require=[ 'pyftpdlib' ] )
