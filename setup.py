import os
import sys

from setuptools import setup
from setuptools import find_packages

sys.path.append('src')
from nlogn import metadata


setup(
    name=metadata.project,
    version=metadata.version,
    description=metadata.description,
    author=metadata.authors,
    author_email=metadata.emails,
    url=metadata.url,
    packages=[
        'nlogn.agent'
    ]
)
#os.path.join('src', 'nlogn/')
