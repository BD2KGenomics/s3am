import sys

from setuptools import setup

assert sys.version_info >= (2, 7)

setup(
    name="s3am",
    version="0.1.dev1",
    entry_points={
        'console_scripts': [
            's3am = s3am:try_main'
        ]
    },
    py_modules=[ 's3am', 'humanize' ],
    install_requires=[ 'pycurl', 'boto' ],
    tests_require=[ 'pyftpdlib' ]
)
