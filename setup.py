import os
import sys

from setuptools import setup

sys.path.append('src')
from nlogn import metadata


setup(
    name=metadata.package,
    version=metadata.version,
    description=metadata.description,
    author=metadata.authors,
    author_email=metadata.emails,
    url=metadata.url,
    packages=[
        'nlogn',
        'nlogn.agent',
        'nlogn.database',
        'nlogn.loggers',
        'nlogn.pipeline',
        'nlogn.relay'
        ],
    package_dir={
        'nlogn': os.path.join('src', 'nlogn')
    },
    entry_points={
        'console_scripts': [
            'nlogn-agent=nlogn.agent.app:main',
            'nlogn-relay=nlogn.relay.flask_app_wrapper:main',
        ],
    }
)
