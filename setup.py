from setuptools import setup, find_packages

setup(
    name="s3am",
    version="0.1.dev1",
    entry_points={
        'console_scripts': [
            's3am = s3am:main'
        ]
    },
    py_modules = [ 's3am', 'humanize' ],
    install_requires=[ 'pycurl', 'boto' ],
    tests_require=[ 'pyftpdlib' ]
)
