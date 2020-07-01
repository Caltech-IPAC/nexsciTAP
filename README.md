# nexsciTAP

##### [Documentation](https://caltech-ipac.github.io/nexsciTAP)

NExScI Python Table Access Protocol (TAP) Server

The TAP (Table Access Protocol) is a standard recommended by the [International Virtual Astronomy Alliance] [ivoa]

[id]: http://www.ivoa.net

  It defines a web service for searching tables in relational databases using a dialect of SQL called ADQL (Astronomical Data Query Language).  ADQL includes functions that support spatial constraints (<i>e.g.,</i> all records with a degree on the sky of specified coordinates).

This TAP implementation is written as a Python package and will shortly be installable through PyPI ("pip install") as well as through this source.  

The first release of the TAP server supports Oracle, though we plan to extend this soon and the documentation describe how.  It runs on LINUX and expects a fully installed Oracle server, a web server like Apache, and a C compiler.

This version is complete.  See the documentation (https://caltech-ipac.github.io/nexsciTAP) for installation and configuration instructions.
