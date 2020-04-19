# TAP
NExScI Python Table Access Protocol (TAP) Server

TAP (Table Access Protocol) is a proposed standard by the IVOA (International Virtual Astronomy Alliance).  It defines a web service for searching tables in relational databases using a dialect of SQL called ADQL (Astronomical Data Query Language).  ADQL includes functions that support spatial constraints (<i>e.g.,</i> all records with a degree on the sky of specified coordinates).

This TAP implementation is written as a Python package and will shortly be installable through PyPI ("pip install") as well as through this source.  

The first release of the TAP server supports Oracle, though we plan to extend this soon.  It runs on LINUX and expects a fully installed Oracle server, a web server like Apache, and a C compiler.

The package builds with a simple Makefile, installs as a /cgi-bin service, and is configured through a simple configuration file (pointed to by an environment variable).

This version of the code is aimed at internal consumption within NExScI at IPAC and is missing any real build/install/configure instructions.
