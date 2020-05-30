import os

from setuptools import setup
from distutils.extension import Extension

setup(name='TAP', 
    version='1.0.1', 
    author='Mihseh Kong', 
    author_email='mihseh@ipac.caltech.edu', 
    license='LICENSE.txt', 
    keywords='astronomy, database, web-service', 
    url = 'https://github.com/Caltech-IPAC/TAP',
    description='VO Table Access Protocol (TAP) web service', 
    ext_modules=[Extension('writerecs', ['writerecsmodule.c'])],
    install_requires=['ADQL', 'spatial_index'])
