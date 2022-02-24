import os
import sys
from pathlib import Path
from setuptools import setup, find_packages

if sys.version_info.major != 3:
    raise RuntimeError("This package requires Python 3+")

version = '0.1.13'
pkg_name = 'lazycls'
gitrepo = 'trisongz/lazycls'
root = Path(__file__).parent

requirements = [
    'anyio',
    'aiofile',
    #'aiopath', # remove deps as 3.10 vs 3.9 is different
    'dill',
    'fsspec',
    'orjson',
    'pydantic[dotenv]',
    'pylogz',
    'pysimdjson',
    'pyyaml',
    'sniffio',
    'typer',
    'universal_pathlib',
    'PyJWT'
]

extras = {
    'gcs': ['gcsfs'],
    's3': ['s3fs'], 
    'cloudfs': ['gcsfs', 's3fs'],
}

args = {
    'packages': find_packages(include = ['lazycls', 'lazycls.*', 'lazy', 'lazy.*']),
    'install_requires': requirements,
    'include_package_data': True,
    'long_description': root.joinpath('README.md').read_text(encoding='utf-8'),
    'entry_points': {
        "console_scripts": [
            "lazy = lazy.cli.apps:baseCli",
        ]
    }
}

setup(
    name=pkg_name,
    version=version,
    url=f'https://github.com/{gitrepo}',
    license='MIT Style',
    description='Python Utilities for the Super Lazy',
    author='Tri Songz',
    author_email='ts@growthengineai.com',
    long_description_content_type="text/markdown",
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.7',
        'Topic :: Software Development :: Libraries',
    ],
    **args
)