# nexsciTAP

##### [Documentation](https://caltech-ipac.github.io/nexsciTAP)

## NASA ExoplaNExScI Python Table Access Protocol (TAP) Server   Version 1.0

The TAP (Table Access Protocol) is a standard recommended by the International Virtual Observatory Alliance (http://www.ivoa.net).  It defines a web service for searching tables in relational databases using a dialect of SQL called ADQL (Astronomical Data Query Language) (http://www.ivoa.net/documents/latest/ADQL.html).  ADQL includes functions that support spatial constraints (<i>e.g.,</i> all records with a degree on the sky of specified coordinates). 

This TAP implementation is written as a Python package and can be installed through PyPI ("pip install") or built from the source in GitHub.  The distribution includes a module dedicated to translating ADQL to SQL and a spatial indexing package that improves the performance of spatial searches. The code base is compact. It consists of 10 KLOC of Python, and 15 KLOC of spatial indexing code, written in C and deployed as a Python binary extension package. 

Because there are differences in the implementation of the SQL standard from DBMS to DBMS, we document how to use DB API 2.0 to implement the variants of SQL in common use. Version 1.0 of the TAP server supports Oracle and SQLite3.  It runs on LINUX and requires a a web server such as Apache, a C compiler, and deployment of either an Oracle server or the SQLite3 library.

Several AQDL geometry functions are not deployed in Version 1.0. These are 

INTERSECTS;  AREA and CENTROID;  COORD1, COORD2, and COORDSYS; and REGION.

The documentation at https://caltech-ipac.github.io/nexsciTAP  provides installation and configuration instructions.
