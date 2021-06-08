import os

from setuptools import setup
from distutils.extension import Extension

setup(name='nexsciTAP', 
    version='1.2.3', 
    author='Mihseh Kong', 
    author_email='mihseh@ipac.caltech.edu', 
    license='LICENSE', 
    keywords='astronomy database ADQL web-service', 
    url = 'https://github.com/Caltech-IPAC/nexsciTAP',
    description='NExScI VO Table Access Protocol (TAP) web service', 
    long_description=open('README.md').read(),
    ext_modules=[Extension('TAP/writerecs', ['TAP/writerecsmodule.c'])],
    install_requires=['ADQL', 'spatial_index', 'configobj'],
    packages=['TAP']
)
