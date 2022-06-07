import codecs
import os
import re

from setuptools import find_packages, setup

# Project version approach from https://packaging.python.org/guides/single-sourcing-package-version/#single-sourcing-the-version
# Version specified in mpt.__init__.py
here = os.path.abspath(os.path.dirname(__file__))


def read(*parts):
    with codecs.open(os.path.join(here, *parts), 'r') as fp:
        return fp.read()


def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


setup(
    name='bl-mpt',
    version=find_version('mpt', '__init__.py'),
    packages=find_packages(),
    url='http://github.com/britishlibrary/mpt',
    license='Apache License 2.0',
    author='The British Library',
    author_email='digital.preservation@bl.uk',
    description='Tools for creating and validating checksums',
    entry_points={
        'console_scripts': [
            'mpt = mpt.__main__:main',
            'mptreport = mptreport.__main__:main'
        ]
    },
    install_requires=[
        'tqdm>=4.32',
    ],
    classifiers=[
        'Development Status :: 5 - Production/Stable',

        # Indicate who your project is intended for
        'Intended Audience :: End Users/Desktop',
        'Topic :: Utilities',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: Apache Software License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 3.6',
    ]
)
