
from distutils.extension import Extension

from setuptools import setup

setup(name='nexsciTAP',
    version='3.1.0',
    author='John Good',
    author_email='jcg@caltech.edu',
    license='LICENSE',
    keywords='astronomy database ADQL web-service',
    url = 'https://github.com/Caltech-IPAC/nexsciTAP',
    description='NExScI VO Table Access Protocol (TAP) web service',
    long_description=open('README.md').read(),
    ext_modules=[Extension('TAP/writerecs', ['TAP/writerecsmodule.c'])],
    # Database drivers (cx_Oracle, psycopg2, mysql-connector) are not listed:
    # each deployment installs the one for its DBMS.  lxml is not imported,
    # but BeautifulSoup is asked for it by name to read UWS job documents.
    install_requires=['ADQL', 'spatial_index', 'configobj', 'sqlparse',
                      'astropy', 'beautifulsoup4', 'lxml', 'xmltodict'],
    packages=['TAP']
)
