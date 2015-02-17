from setuptools import setup, find_packages

setup(
    name="s3am",
    version="0.1.dev1",
    entry_points={
        'console_scripts': [
            's3am = s3am:main'
        ]
    },
    packages=find_packages( ),
    install_requires=[ 'pycurl', 'boto', 'pyftpdlib' ]
)
