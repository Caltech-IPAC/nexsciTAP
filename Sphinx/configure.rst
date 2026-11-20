Configuration Instructions
==========================

Here we have a little bit of a chicken-and-egg problem.  A few of the parameters
in the configuration file presume some understanding of the spatial indexing.
We will describe the parameters as we encounter them but for a real understanding
please refer to later sections.

As we say in the previous section, starting the NExScI TAP service does not require
any parameters but of course there are a number of things it needs before it can
really do anything.  Many of these parameters (such as where to put workspace files
and the password to log into the DBMS) cannot be known to the end users.

To consolidate this all in one place, the service reads a number of configuration
parameters from a file, found by referring to an environment variable ``TAP_CONF``.
This file is read using the fairly standard ConfigObj package and uses the common
INI format, originally from Windows.  The data is in simple line-based keyword/value
format and has (optionally) section labels (in square brackets).  It can be 
hierarchical, though we don't need or use that complexity.

We will illustrate our TAP.conf file through a complete example.  Note that a 
number of these parameters can be defaulted, though it is generally a good idea
to be explicit for clarity.  Be very careful with this file and its permissions;
it contains the database password for the TAP user.  Risk can be minimized by
allowing this user limited access (*e.g.,* read-only to just the TAP-related tables).
Still, the file itself should also be read-only access and just from an admin 
account.  Below is a complete example::

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
   DB=/work/sqlite/ps.db
   TAP_SCHEMA=/work/sqlite/tap_schema.db


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

- **DBMS** This TAP server potentially supports a wide range of DBMSs (though currently 
  just  "oracle" and "sqlite3" are implemented.  This parameter specifies the DBMS 
  currently in use and also selects which of the parameter blocks (below) will be used 
  to implement the connection.  Here we have selected "oracle" but also show an example 
  (inactive) of an SQLite3 connection.  

- **TAP_WORKDIR** This is a the absolute path to the disk space NExScI TAP is supposed
  to use as working space.  It will create a "TAP" subdirectory here and then a collection
  of temporary, uniquely-named session subdirectories.

- **TAP_WORKURL** The above disk space needs to be URL-accessible (and you have to 
  arrange with your web server to make it so).  This is the base URL to the same space.

- **HTTP_URL** A TAP session can involve multiple HTTP connections for various bits
  of information.  So we need the machine address to construct the path to the 
  job status and to the returned data (as well as being part of the original request
  submission).

- **HTTP_PORT** And the port number as well (often the standard 80).

- **CGI_PGM** Similarly, we need to know how to address the NExScI TAP service 
  executable.  By default, this would be something like "/cgi-bin/TAP/nph-tap.py" but
  you may want to use an alias to streamline the address (we do in this example).
  Setting that up is again a matter of web server configuration.


For Oracle there are three parameters needed to make a connection.  These are 
well-known quantities you can get from your DBA:

- **ServerName** The core information for making a connection is the "server name"
  as configured in Oracle.

- **UserID** Once you initiate a connection, you will need a valid user iD.

- **Password** And password.  These three parameters are the arguments to the 
  cx_Oracle Python package initialization.  cx_Oracle is conformant with the Python DB
  API 2.0 spec.  Other databases will have different detailed connection parameters.

For SQLite3 you need to point to the database files (SQLite3 works using files rather
than a daemon process).  Since the TAP standard insists on having the TAP_SCHEMA tables
in a different schema (in SQLite3 a different file), we have a second parameter
for that:

- **DB** SQLite3 database file containing the tables to be served.

- **TAP_SCHEMA** SQLite3 database file containing the five "TAP_SCHEMA" tables.

These files can be stored anywhere you like.


Spatial Indexing Settings.  Refer to the indexing documentation for a better 
understanding.  Here we will be terse:

- **ADQL_MODE** We support two sky tesselations for our spatial indexing. Here we
  are using HTM (Heirarchical Triangular Mesh) as opposed to HPX (HEALPix).

- **ADQL_LEVEL** Any tesselation recurses arbitrarily deeply.  The level specifies
  how deep this is and corresponds to a smallest "cell" size (here HTM 20 cells are
  about 0.3 arcseconds on a side).

- **ADQL_COLNAME** For our custom spatial indexing to work, tables need to include
  the index value column explicitly.  The obvious name here would be "htm20" (it is
  for us) but you may have some reason to use another name.

- **ADQL_ENCODING** This one is arcane and only really applies to HTM in practice.
  Since tesselations are based on recursive decomposition of spatial cells into 
  four, sometimes (and mostly for debugging) it is nice to have database values
  be base 4 numbers.  But this increases the number of digits by a fair bit and normally
  regular base 10 numbers are easier to store.  Supported values are therefore 
  "BASE10" and "BASE4".

- **ADQL_XCOL** The spatial indexing cell numbers allow very quick subsetting but
  the results are only approximate (*i.e.* some of the records in a "matching" cell
  are outside the exact geometry defined; like a cone on the sky).  Including the
  (x,y,z) geometric tree-vector coordinates of the location allow us to perform an
  exact secondary filter of the data.  Again, you may not want to use the name 'x'
  for this x-column so we let you set the name explicitly here.

- **ADQL_YCOL** Ditto for the y-column name.

- **ADQL_ZCOL** And the z-column name.


Debug Logging
-------------

Debug logging is **off by default** and controlled by the operator only —
it cannot be enabled by a web request.  To enable it, add ``DEBUG = 1``
to the ``[WEB]`` section of your ``TAP.conf``::

   [WEB]
   DEBUG = 1

If your ``TAP.conf`` still uses the 1.x ``[webserver]`` layout shown above,
put ``DEBUG = 1`` under ``[webserver]`` instead — it is translated to
``[WEB]`` automatically.  Do **not** add a ``[WEB]`` section to a 1.x file:
its presence makes the file read as 3.x layout and every request will fail
with a configuration error.

This writes per-request debug logs to ``/tmp/tap_<pid>.debug``.
Set ``DEBUG = 0`` or remove the key when done.  Treat the log as
sensitive and do not leave it enabled in production.

To diagnose failures that occur before the config file loads (e.g. a
missing or unparseable ``TAP.conf``), set the server environment
variable ``TAP_DEBUG=1``.  When set, any request that fails before
``TAP.conf`` loads writes bootstrap records to the **web server error log**
(stderr / Apache ``error_log``) — not to the per-archive ``/tmp`` debug file.
This keeps per-archive debug policy under the exclusive control of each
archive's ``TAP.conf``; ``TAP_DEBUG`` is a server-admin diagnostic tool, not
an archive setting.  Unset it when done.

.. versionchanged:: 3.1.1
   In 3.0.2–3.1.0, ``TAP_DEBUG=1`` wrote per-request debug records to
   ``/tmp/tap_<pid>.debug`` for every request.  From 3.1.1 onward:

   - Per-archive debug requires ``DEBUG = 1`` in that archive's ``TAP.conf``;
     add it to the ``[WEB]`` block of the archive that needs debugging.
   - ``TAP_DEBUG=1`` writes only startup diagnostics to the web server error
     log and never creates a ``/tmp`` file.

   Deployments that relied on ``TAP_DEBUG=1`` for per-request traces must
   add ``DEBUG = 1`` to the relevant archive's ``TAP.conf``.

