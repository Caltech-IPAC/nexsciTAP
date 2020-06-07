
Extending the Service
=====================
All of the stuff in this section is in the "future work" category
though none of it is particularly difficult.

Support for Different DBMSs
---------------------------
NExScI TAP was designed from the start with multiple DBMS support
in mind.  Python PEP 249 defines a generic interface for talking to
databases and it has been implemented for most systems.  We have so
far only fully integrated the Oracle packages (cx_Oracle) as that is
what we use operationally but we have investigated several others
and plan to incorporate them as time permits (or hopefully as others
do it for us).


Python Database API (DB-API) 2.0 (PEP 249)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The API defines a straightforward interface to databases in general.
The initial connect() method takes a variable set of arguments,
depending on the DBMS.  This returns a "conection", which in turn
can be asked for a "cursor" (connection.cursor()).  Cursors can
be given a query (cursor.executeSQL(sql)) and then asked to step
through the results.

Oracle
~~~~~~
For Oracle,
the initialization takes the form::
conn = cx_Oracle.connect (dbuser, dbpassword, dbserver)

PostgreSQL
~~~~~~~~~~
The most popular PostgreSQL database driver is psycopg2.  Initialization
is similar to Oracle::
conn = psycopg2.connect(host='localhost',database='exodev', user='exo_dbuser', password='XXXXXXXX')

and the same sort of INI block can be used (see the Configuration
section)::
[pgdb]
host=localhost
database=exodev
user=exo_dbuser
password=XXXXXXXX

SQLite3
~~~~~~~
SQLite3 has the simplist of initializations.  Since it works directly
on files with no server/login, it can be initialized with just::
conn = sqlite3.connect('exodev.db')

where 'example.db' is any SQLite3 file.  This is an intrinsic
part of the SQLite3 Python package.

MySQL
~~~~~
For MySQL,
we use the MySQLdb package::
conn = MySQLdb.connect('localhost', 'dbuser', 'XXXXXXX', 'exodb')


Table Upload
------------
The TAP specification defines an (optional) UPLOAD capability, which 
nexsciTAP does not currently support.  Uploaded tables are loaded into
a separate database schema (visible to the user as "TAP_UPLOAD") and
tables in it are generally meant to support chaining queries together
across databases, though there is no such constraint on how they are 
used.

There are two situations where uploaded tables could be particularly
useful.  If the uploaded table contains extended inforamation on records
in a archive table record (and the "ID" of that record), then a simple
join of the two would produce an augmented catalog.

Perhaps the most alluring possibility involves positional cross-comparison
as the first step in actual catalog cross-matching.  This is a 
complicated process (the nearest positional match between two tables 
is frequently not the best match).

NExScI TAP does not currently support UPLOAD, though it is a fairly
straightforward thing to add.  To date, none of the projects we support
have express a need for the above.


Dealing with Proprietary Data
-----------------------------
NExScI TAP has been extended to support access to proprietary data for
two projects.  In general, this involves two complications.  The first
is the requirement to authenticate the users.  Our projects are using
a simple approach of having a separate login service and temporary 
authorization cookies but there are a wide array of more advanced 
mechanisms.

THe second complication involves modifying the user query before 
submission to the database to include joins with additional tables
which identify which projects a user belongs and which data records
those projects currently have access to.

A simpler security setup (*e.g.* one where you had the luxury of identifying
data records directly with users) could be done in simpler manners.
However, since there is no one way to do this, we can't distribute a
single solution as part of NExScI TAP.  We would be happy to share the
code we do have if your needs are similar.


Dealing with Extended Objects (Images in Particular)
----------------------------------------------------
Tesselation-based spatial index works great for point-like data like 
astronomical catalogs and can even be used with small extended objects.
For instance, if you have metadata for images that are all small you
can index the image center coordinates and then pad the queries by the
maximum size of the images.

This falls apart for truly extended objects (image sets where some of
the images are ten degrees across; region specifications like constellation
outlines; and so on).  For this kind of data there are more effective 
approaches, most notably R-Trees, which work by creating a heirarchy of 
object clusters.  Building the tree is a laborious process since it 
involves intense rethinking of how best to split, group, and reorganize
things so that the most effective organization emerges.  

The objects and the heirarchy of clusters are defined by their bounding
boxes and at all levels it is perfectly OK for these boxes to overlap;
we are just trying to get to a state where we can quickly exclude large
subsets from consideration.

Unlike the tesselation approach, where you can leverage the basic 
DBMS B-Tree indexing, doing this right requires a different internal
indexing approach.  Most DBMSs now support some form of this but often
at an extra cost and pretty much alway in a way that requires additional
database software installation and configuration.

If you are interested in just spatial searching of image metadata and
don't need relational constraints (or are willing to apply these as
post-filters) there is a stand-alone tool that comes as part of the
Montage package that contains an optimized R-Tree implementation on
file-based data.

If you truly want to fold R-Tree processing into the DBMS framework,
the best idea is to adopt a DBMS where this has been done well 
(*e.g.* PostgreSQL) and update NExScI TAP to translate ADQL to the
PostgreSQL-based formalism.  We haven't had time to explore this
fully but plan to unless someone else does it first.
