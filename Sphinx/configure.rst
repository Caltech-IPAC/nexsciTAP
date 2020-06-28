Configuration Instructions
==========================

Here we have a little bit of a chicken-and-egg problem.  A few of the parameters
in the configuration file presumes some understanding of the spatial indexing.
We will describe the parameters as we encounter them but for a real understanding
please refer to later sections.

As we say in the previous section, starting the NExScI TAP service does not require
any parameters but of course there are a number of things it needs before it can
really do anything.  Many of these parameters (such as where to put workspace files
and the password to log into the DBMS) cannot be known to the end users.

To consolidate this all in one place, the service reads a number of configuration
parameters from a file, found by referring to an environment variable "TAP_CONFIG".
This file is read using the fairly standard ConfigObj package and uses the common
INI format, originally from Windows.  The data is is simple line-based keyword/value
format and has (optionally) section labels (in square brackets).  It can be 
hierarchical, though we don't need or use that complexity.

We will illustrate our TAP.conf file through a complete example.  Note that a 
number of these parameters can be defaulted, though it is generally a good idea
to be explicit for clarity::

   [webserver]
   DBMS=oracle
   # DBMS=sqlite3
   TAP_WORKDIR=/work
   TAP_WORKURL=/workspace
   HTTP_URL=http://tapdb.ipac.caltech.edu
   HTTP_PORT=80
   CGI_PGM=/TAP


   # Parameters for cx_Oracle interface
   #
   [oracle]
   ServerName=tapdb
   UserID=tap_user
   Password=XXXXXXXXXXXX


   # Parameters for sqlite3 interface
   #
   [sqlite3]
   DB=/work/jcg/ps.db
   TAP_SCHEMA=/work/jcg/tap_schema.db


   # Spatial indexing settings
   #
   ADQL_MODE=HTM
   ADQL_LEVEL=20
   ADQL_XCOL=x
   ADQL_YCOL=y
   ADQL_ZCOL=z
   ADQL_COLNAME=htm20
   ADQL_ENCODING=BASE10


In order, here is what each parameter means, starting with the ones describing
the machine and service configuration:

- **DBMS** This TAP server can support a range of DBMSs.  This parameter specifies the
  DBMS currently in use and also selects which of the parameter blocks (below) will be
  used to implement the connection.  Here we have selected "oracle" but also show an
  example (inactive) of an SQLite3 connection.  

- **TAP_WORKDIR** This is a the absolute path to the disk space NExScI TAP is supposed
  to use as working space.  It will create a "TAP" subdirectory here and then a collection
  of temporary, uniquely-named session subdirectories of that.

- **TAP_WORKURL** The above disk space needs to be URL-accessible (and you have to 
  arrange with your web server to make it so).  This is the base URL to the same space.

- **HTTP_URL** A TAP session can involve multiple HTTP connections for various bits
  of information.  So the machine address needs to be folded in in several places.

- **HTTP_PORT** And the port number as well (often the standard 80).

- **CGI_PGM** Similarly, we need to know how to address the NExScI TAP service 
  executable.  By default, this would be something like "/cgi-bin/nph-tap.py" but
  you may want to use an alias to streamline the address (we do in this example).
  Setting that up is again a matter on web server configuration.


For Oracle there are three parameters needed to make a connection.  These are 
well-known quantities you can get from your DBA:

- **ServerName** The core information for making a connection is the "server name"
  as configured in Oracle.

- **UserID** Once you initiate a connection, you will need a valid user iD.

- **Password** And password.  These three parameters are the arguments to the 
  cx_Oracle Python package initiation.  cx_Oracle is conformant to the Python DB
  API 2.0 spec.  Other databases will have different detailed connection parameters.

For SQLite3 you need to point as the database files (SQLite3 works using files rather
than a daemon process.  Since the TAP standard insists on having the TAP_SCHEMA tables
in a different schema (in SQLite3 a different file), we have a second parameter
for that:

- **DB** SQLite3 database file containing the tables to be served.

- **TAP_SCHEMA** SQLite3 database file containing the five "TAP_SCHEMA" tables.


Spatial Indexing Settings.  Refer to the indexing documentation for a better 
understanding.  Here we will be terse:

- **ADQL_MODE** We support two sky tesselations for our spatial indexing. Here we
  are using HTM (Heirarchical Triangular Mesh) as opposed to HPX (HEALPix).

- **ADQL_LEVEL** Any tesselation recurse arbitrarily deeply.  The level specify
  how deep this is and correspond to a smallest "cell" size (here HTM 20 cells are
  about 0.3 arcseconds on a side).

- **ADQL_COLNAME** For our custom spatial indexing to work, tables need to include
  the index value column explicitly.  The obvious name here would be "htm20" (it is
  for us) but you may have some reason to use another name.

- **ADQL_ENCODING** This one is arcane and only really applies to HTM in practice.
  Since tesselations are based on recursive decomposition of spatial cells into 
  four, sometimes (and mostly for debugging) it is nice to have database values
  be base 4 numbers.  But this increasing the number of digits by a lot and normally
  regular base 10 numbers are easier to store.

- **ADQL_XCOL** The spatial indexing cell numbers allow very quick subsetting but
  the results are only approximate (*i.e.* some of the records in a "matching" cell
  are outside the exact geometry defined (like a cone on the sky).  Including the
  (x,y,z) geometric tree-vector coordinates of the location allow us to perform an
  exact secondary filter of the data.  Again, you may not want to use the name 'x'
  for this x-column so we let you set the name explicitly here.

- **ADQL_YCOL** Ditto for the y-column name.

- **ADQL_ZCOL** And the z-column name.


