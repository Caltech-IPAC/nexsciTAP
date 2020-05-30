#!/bin/sh

# This script is for building the Python 2.7/3.6 versions of
# the package wheels for Mac OSX.  It assumes you have already
# installed 'python2' and 'python3' and have 'Cython', 'jinja2'
# and 'wheel' installed as Python packages in both.

# If our OSX version has gotten ahead of the version used to
# build Cython, we can get a bunch of version mismatch warning
# messages from the loader.  This can be avoided with the following:

export MACOSX_DEPLOYMENT_TARGET=10.9


# Python 3.6

rm -rf build

python3.6 setup.py bdist_wheel


# Python 3.7

rm -rf build

python3.7 setup.py bdist_wheel


# Python 3.8

rm -rf build

python3.8 setup.py bdist_wheel


rm -rf TAP.egg-info build
