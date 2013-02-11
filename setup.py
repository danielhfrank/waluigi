import os
from distutils.core import setup

# also update version in __init__.py
version = '0.1'

setup(
    name="waluigi",
    version=version,
    keywords=["luigi", "mrjob"],
    long_description=open(os.path.join(os.path.dirname(__file__), "README.md"), "r").read(),
    description="Luigi adapter for MRJob jobs",
    author="Dan Frank",
    author_email="df@bit.ly",
    url="http://github.com/danielhfrank/waluigi",
    license="Apache Software License",
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
    ],
    packages=['waluigi'],
    install_requires=['luigi'],
    requires=['luigi'],
    download_url="https://github.com/danielhfrank/waluigi/archive/%s.tar.gz" % version,
)
