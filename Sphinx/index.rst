
The NExScI TAP Service
======================

.. toctree::
   :maxdepth: 2
   :caption: Getting "Up and Running"

   install
   configure
   operations

.. toctree::
   :maxdepth: 2
   :caption: Appendices

   tap_architecture
   tap_schema
   spatial_index
   loading
   ADQL_translation
   extending
   coordinates

.. toctree::
   :maxdepth: 2
   :caption: The Code

   spatial_index_docs
   adql_docs
   tap_docs


What is NExScI TAP?
-------------------
Tabular data in astronomy (and other sciences) is commonly housed in relational
databases.  This may be final reduced data like astronomical catalogs or searchable
metadata for image or spectra.  Research projects very often start with database 
searches of these tables, often followed by the download of associated data files.

The International Virtual Astronomy Alliance (IVOA) Table Access Protocol (TAP)
provides a standard interface specification for such queries.  TAP defines how
to compose the request using ADQL (a dialect of the standard Structured Query
Language SQL), how to submit the request to a remote server (either in foreground
or, for slow queries, in background), and how to retrieve results when they are 
ready.

The NExScI TAP service is a Python implementation of the protocol using open-source
and extensible code.  It is easy to install and configure and easy to extend to
other DBMSs.

Because queries against spatial regions on the sky is so common in astronomy, 
ADQL extends SQL with standard spatial constraint functions.  In order to support
these extensions, NExScI TAP includes a formalism for fast spatially-indexed
searches which doesn't require modification of the underlying DBMS.


Structure of this Documentation
-------------------------------
NExScI TAP is aimed at operations of all scales, from a single table that a researcher
wishes to share to the largest mission archives.

In order to get these archives on-line quickly, this documentation focuses initially
on installation and configuration, then overviews TAP operations and the structure
of the service, and finally delves into the details of the spatial indexing and the 
translation of ADQL into the local DBMS SQL.

For those who prefer to understand how things work before they start running them,
the latter sections have been written to make sense as stand-alone documents.

