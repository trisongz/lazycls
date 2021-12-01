import os
import sys
from pathlib import Path
from setuptools import setup, find_packages

if sys.version_info.major != 3:
    raise RuntimeError("This package requires Python 3+")

version = '0.0.1'
pkg_name = 'lazycls'
gitrepo = 'trisongz/lazycls'
root = Path(__file__).parent

args = {
    'packages': find_packages(include = ['lazycls', 'lazycls.*']),
    'requirements': [],
    'entry_points': {}
}

with root.joinpath('requirements.txt').open('r') as r:
    reqs = [line.split('=', 1)[0] for line in r]
    args['requirements'].extend(reqs)


with root.joinpath('README.md').open('r') as f:
    long_description = f.read()

setup(
    name=pkg_name,
    version=version,
    url=f'https://github.com/{gitrepo}',
    license='MIT Style',
    description='Dynamic Dataclasses for the Super Lazy',
    author='Tri Songz',
    author_email='ts@growthengineai.com',
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.7',
        'Topic :: Software Development :: Libraries',
    ],
    **args
)