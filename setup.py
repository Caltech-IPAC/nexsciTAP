
from distutils.extension import Extension

from setuptools import setup

setup(name='nexsciTAP',
    version='3.0.0',
    author='John Good',
    author_email='jcg@caltech.edu',
    license='LICENSE',
    keywords='astronomy database ADQL web-service',
    url = 'https://github.com/Caltech-IPAC/nexsciTAP',
    description='NExScI VO Table Access Protocol (TAP) web service',
    long_description=open('README.md').read(),
    ext_modules=[Extension('TAP/writerecs', ['TAP/writerecsmodule.c'])],
    install_requires=['ADQL', 'spatial_index', 'configobj'],
    packages=['TAP']
)
