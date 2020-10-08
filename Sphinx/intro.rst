
The NExScI TAP Service
======================

The primary expected audience for this document are services providers, usually those 
with existing relational DBMSs containing tables of astronomical data.  A secondary
audience would be end users of the service as we explain the details of connecting
to the service and submitting queries.  For example, the URL

https://exoplanetarchive.ipac.caltech.edu/TAP/sync?query=select+pl_name,avg(ra),avg(dec)+from+PS+group+by+pl_name

is a complete synchronous TAP query that returns a list of all known exoplanets and
their coordinates.


What is NExScI TAP?
-------------------
Tabular data in astronomy (and other sciences) is commonly housed in relational
databases.  This may be final reduced data like astronomical catalogs or searchable
metadata for image or spectra or anything else.  Research projects very often start 
with database searches of these tables, often followed by the download of associated 
data files.

The International Virtual Astronomy Alliance (IVOA) Table Access Protocol (TAP)
provides a standard interface specification for such queries.  TAP defines how
to compose the request using ADQL (a dialect of the standard Structured Query
Language SQL), how to submit the request to a remote server (either in foreground
or, for slow queries, in background), and how to retrieve results when they are 
ready.

The NExScI TAP service is a Python implementation of the protocol using open-source
and extensible code.  It is easy to install and configure and easy to extend to
other DBMSs.  NExScI TAP uses Python DB-API 2.0 to connect to DBMS servers.  
Currently connections to Oracle and SQLite3 have been tested with PostgresSQL and
MySQL planned in the near future.  If you have a particular DBMS you are interested
in, please contact us.

Because queries against spatial regions on the sky are so common in astronomy, 
ADQL extends SQL with standard spatial constraint functions.  In order to support
these extensions, NExScI TAP includes a formalism for fast spatially-indexed
searches which doesn't require modification of the underlying DBMS.


Structure of this Documentation
-------------------------------
NExScI TAP is aimed at operations of all scales, from a single table that a researcher
wishes to share to the largest mission archives.

This documentation focuses initially on installation and configuration, then overviews 
TAP operations.  There are several appendices covering the structure of the service, 
and delving into the details of the spatial indexing and the translation of ADQL into
the local DBMS SQL.

For those who prefer to understand how things work before they start running them,
most of the appendices have been written to make sense as stand-alone documents.

|

*The NASA Exoplanet Science Institute is operated by the California Institute of Technology,
under contract with the National Aeronautics and Space Administration under the
Exoplanet Exploration Program.*
