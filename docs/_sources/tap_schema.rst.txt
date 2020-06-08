
Setting up Your TAP_SCHEMA Tables
=================================

The TAP specification requires that you maintain in your database a set of tables
describing your holdings.  Pretty much everything about these tables is fixed:

- They have fixed names (like TABLES and COLUMNS)

- They contain a minimum set of columns with fixed names (like COLUMN_NAME)

- They must reside in a separate DBMS "schema" named TAP_SCHEMA

All of this is in aid of data mining; a user can issue a set of canned queries
against your database to determine your holdings and they can find columns 
by their function (*e.g.,* coordinates).

The NExScI TAP service makes use of the TAP_SCHEMA tables to inform the 
presentation of data.  In particular, if you want fine control over the 
formatting of output, you can add a FORMAT column to the COLUMNS table
and populate it with C/Python format strings like "6d" to get six-digit
integers or "20s" for twenty-character strings.
