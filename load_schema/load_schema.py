#!/bin/env python

import sqlite3
import pprint

pp = pprint.PrettyPrinter()

conn = sqlite3.connect('/data/sqlite3/tap_schema.db', detect_types=True)

cursor = conn.cursor()


# POPULATE TAP_SCHEMA TABLES

# This programs populate all the TAP_SCHEMA tables for a simple TAP server.
# To use it with another DBMS, just change the "sqlite3" DB-API 2.0 specified
# above to whatever DBMS is desired and the path to the database file.  The
# modify the "schemas", "tables" and "columns" data below as appropriate.
# The annoying bits associated with the "keys" and "key_columns" tables are
# standard (unless you want to add join keys for your own tables).

# We only have one data table so most of this is straightforward enough:
# Document that table (one record each in the schemas and "tables" tables)
# and describe all the table columns for the "columns" table).

# Then things gets a bit recursive, since by setting up these table we now have
# more tables to document (e.g. the "columns" table needs to list out the
# columns in the "columns" table itself).  But all this stuff just needs to be
# worked out once; from here on if we were to add tables we would just need
# to add one "tables" record and its "columns" records.

# Finally, there are two more TAP_SCHEMA tables describing how foreign keys 
# in our tables relate the tables to each other (i.e., allow JOINS).  Our one
# real data table has no such keys but all these TAP_SCHEMA tables themselves
# do have such relationships (and so must be included).  However, unless we add
# new data tables which can join with each other, we never need to touch these 
# two tables again.



# SCHEMA(S)   ==================================================

print('\n\nSCHEMAS table:')

sql = 'DROP TABLE schemas;'

try:
    cursor.execute(sql)
except sqlite3.Error as e:
    print('An error occurred in DROP TABLE schemas:', e.args[0])

# --------------------------------------------------------------

sql = """
CREATE TABLE IF NOT EXISTS schemas (
    schema_name  TEXT NOT NULL,
    utype        TEXT,
    description  TEXT,
    schema_index INTEGER,
    PRIMARY KEY (schema_name)
) WITHOUT ROWID;
"""

try:
    cursor.execute(sql)
except sqlite3.Error as e:
    print('An error occurred in "schemas" CREATE TABLE:', e.args[0])

# --------------------------------------------------------------

sql = """
INSERT INTO schemas(schema_name, description, schema_index)
VALUES(?,?,?);
"""

schemadata = [
    ('navostats',  'The NAVO ServiceMon statistics database.', 1),
    ('TAP_SCHEMA', 'Standard TAP schema description tables.',  2)
]

for i in range(len(schemadata)):

    values = schemadata[i]

    print(values)

    try:
        cursor = conn.cursor()

        cursor.execute(sql,values)

        conn.commit()

        print('inserted')

    except sqlite3.Error as e:

        print('An error occurred in INSERT INTO schemas:', e.args[0])



# TABLE(S) =====================================================

print('\nTABLES table:')

sql = 'DROP TABLE tables;'

try:
    cursor.execute(sql)
except sqlite3.Error as e:
    print('An error occurred in DROP TABLE tables:', e.args[0])

# --------------------------------------------------------------

sql = """
CREATE TABLE IF NOT EXISTS tables (
    schema_name TEXT NOT NULL,
    table_name  TEXT NOT NULL,
    table_type  TEXT NOT NULL,
    utype       TEXT,
    description TEXT,
    table_index INTEGER,
    PRIMARY KEY (table_index)
) WITHOUT ROWID;
"""

try:
    cursor.execute(sql)
except sqlite3.Error as e:
    print('An error occurred in "tables" CREATE TABLE:', e.args[0])

# --------------------------------------------------------------

sql = """
INSERT INTO tables(schema_name,table_name,table_type,description,table_index)
VALUES(?,?,?,?,?);
"""

tbldata = [
    ('navostats',  'navostats',  'table', 'NAVO ServiceMon stats table.',             1),
    ('TAP_SCHEMA', 'schemas',    'table', 'List of database schemas.',                2),
    ('TAP_SCHEMA', 'tables',     'table', 'List fo database tables.',                 3),
    ('TAP_SCHEMA', 'columns',    'table', 'List of table columns.',                   4),
    ('TAP_SCHEMA', 'keys',       'table', 'List of foreign keys for joining tables.', 5),
    ('TAP_SCHEMA', 'key_columns','table', 'List of key columns for joining tables',   6)
]

for i in range(len(tbldata)):

    values = tbldata[i]

    print(values)

    try:
        cursor = conn.cursor()

        cursor.execute(sql,values)

        conn.commit()

        print('inserted')

    except sqlite3.Error as e:

        print('An error occurred in INSERT INTO tables:', e.args[0])



# COLUMNS ======================================================

print('\n\nCOLUMNS table:')

sql = 'DROP TABLE columns;'

try:
    cursor.execute(sql)
except sqlite3.Error as e:
    print('An error occurred in DROP TABLE columns:', e.args[0])

# --------------------------------------------------------------

sql = """
CREATE TABLE IF NOT EXISTS columns (
    table_name   TEXT NOT NULL,
    column_name  TEXT NOT NULL,
    datatype     TEXT NOT NULL,
    arraysize    TEXT,
    xtype        TEXT,
    "size"       INTEGER,
    description  TEXT,
    utype        TEXT,
    unit         TEXT,
    ucd          TEXT,
    indexed      INTEGER NOT NULL,
    principal    INTEGER NOT NULL,
    std          INTEGER NOT NULL,
    format       TEXT NOT NULL,
    column_index INTEGER,
    CONSTRAINT pkcolumns PRIMARY KEY (table_name,column_name)
) WITHOUT ROWID;
"""

try:
    cursor.execute(sql)
except sqlite3.Error as e:
    print('An error occurred in "columns" CREATE TABLE:', e.args[0])

# --------------------------------------------------------------

sql = """
INSERT INTO columns(table_name,column_name,datatype,description,unit,indexed,principal,std,format)
VALUES(?,?,?,?,?,?,?,?,?);
"""

coldata = [
    ('navostats', 'location',      'char',    'Location of the monitoring service (e.g, AWS region)', '',       0, 1, 1, '80s'  ),
    ('navostats', 'start_time',    'char',    'Date/time the query started.',                         '',       0, 1, 1, '30s'  ),
    ('navostats', 'end_time',      'char',    'Date/time the query ended.',                           '',       0, 1, 1, '30s'  ),
    ('navostats', 'int0_desc',     'char',    'Description of the int0 parameter.',                   '',       0, 1, 1, '20s'  ),
    ('navostats', 'int0_duration', 'double',  'Query time.',                                          'sec',    0, 1, 1, '20.6f'),
    ('navostats', 'int1_desc',     'char',    'Description of the int1 parameter.',                   '',       0, 1, 1, '20s'  ),
    ('navostats', 'int1_duration', 'double',  'Download time.',                                       '',       0, 1, 1, '20.6f'),
    ('navostats', 'int2_desc',     'char',    'Description of the int2 parameter.',                   '',       0, 1, 1, '20s'  ),
    ('navostats', 'int2_duration', 'double',  'Total time.',                                          '',       0, 1, 1, '20.6f'),
    ('navostats', 'base_name',     'char',    'Dataset name.',                                        '',       0, 1, 1, '20s'  ),
    ('navostats', 'service_type',  'char',    'Service type (TAP or cone search).',                   '',       0, 1, 1, '20s'  ),
    ('navostats', 'ra',            'double',  'Search region Right Ascension.',                       'degree', 0, 1, 1, '20.6f'),
    ('navostats', 'dec',           'double',  'Search region Declination.',                           'degree', 0, 1, 1, '20.6f'),
    ('navostats', 'sr',            'double',  'Search region radius.',                                'degree', 0, 1, 1, '20.6f'),
    ('navostats', 'adql',          'char',    'Query base ADQL.',                                     '',       0, 1, 1, '300s' ),
    ('navostats', 'access_url',    'char',    'Base TAP URL.',                                        '',       0, 1, 1, '300s' ),
    ('navostats', 'size',          'integer', 'Size of return table.',                                'byte',   0, 1, 1, '10d'  ),
    ('navostats', 'num_rows',      'integer', 'Number of rows in return table.',                      '',       0, 1, 1, '9d'   ),
    ('navostats', 'num_columns',   'integer', 'Number of columns in return table.',                   '',       0, 1, 1, '9d'   ),

    ('TAP_SCHEMA.schemas', 'schema_name',  'char',    '', '', 0, 1, 1, ''),
    ('TAP_SCHEMA.schemas', 'utype',        'char',    '', '', 0, 1, 1, ''),
    ('TAP_SCHEMA.schemas', 'description',  'char',    '', '', 0, 1, 1, ''),
    ('TAP_SCHEMA.schemas', 'schema_index', 'integer', '', '', 0, 1, 1, ''),

    ('TAP_SCHEMA.tables', 'schema_name', 'char',    '', '', 0, 1, 1, ''),
    ('TAP_SCHEMA.tables', 'table_name',  'char',    '', '', 0, 1, 1, ''),
    ('TAP_SCHEMA.tables', 'table_type',  'char',    '', '', 0, 1, 1, ''),
    ('TAP_SCHEMA.tables', 'utype',       'char',    '', '', 0, 1, 1, ''),
    ('TAP_SCHEMA.tables', 'description', 'char',    '', '', 0, 1, 1, ''),
    ('TAP_SCHEMA.tables', 'table_index', 'integer', '', '', 0, 1, 1, ''),

    ('TAP_SCHEMA.columns', 'table_name',   'char',    '', '', 0, 1, 1, ''),
    ('TAP_SCHEMA.columns', 'column_name',  'char',    '', '', 0, 1, 1, ''),
    ('TAP_SCHEMA.columns', 'datatype',     'char',    '', '', 0, 1, 1, ''),
    ('TAP_SCHEMA.columns', 'arraysize',    'char',    '', '', 0, 1, 1, ''),
    ('TAP_SCHEMA.columns', 'xtype',        'char',    '', '', 0, 1, 1, ''),
    ('TAP_SCHEMA.columns', '"size"',       'integer', '', '', 0, 1, 1, ''),
    ('TAP_SCHEMA.columns', 'description',  'char',    '', '', 0, 1, 1, ''),
    ('TAP_SCHEMA.columns', 'utype',        'char',    '', '', 0, 1, 1, ''),
    ('TAP_SCHEMA.columns', 'unit',         'char',    '', '', 0, 1, 1, ''),
    ('TAP_SCHEMA.columns', 'ucd',          'char',    '', '', 0, 1, 1, ''),
    ('TAP_SCHEMA.columns', 'indexed',      'integer', '', '', 0, 1, 1, ''),
    ('TAP_SCHEMA.columns', 'principal',    'integer', '', '', 0, 1, 1, ''),
    ('TAP_SCHEMA.columns', 'std',          'integer', '', '', 0, 1, 1, ''),
    ('TAP_SCHEMA.columns', 'column_index', 'integer', '', '', 0, 1, 1, ''),

    ('TAP_SCHEMA.keys', 'key_id',       'char', '', '', 0, 1, 1, ''),
    ('TAP_SCHEMA.keys', 'from_table',   'char', '', '', 0, 1, 1, ''),
    ('TAP_SCHEMA.keys', 'target_table', 'char', '', '', 0, 1, 1, ''),
    ('TAP_SCHEMA.keys', 'description',  'char', '', '', 0, 1, 1, ''),
    ('TAP_SCHEMA.keys', 'utype',        'char', '', '', 0, 1, 1, ''),

    ('TAP_SCHEMA.key_columns', 'key_id',        'char', '', '', 0, 1, 1, ''),
    ('TAP_SCHEMA.key_columns', 'from_column',   'char', '', '', 0, 1, 1, ''),
    ('TAP_SCHEMA.key_columns', 'target_column', 'char', '', '', 0, 1, 1, '')
]

for i in range(len(coldata)):

    values = coldata[i]

    print(values)

    try:
        cursor = conn.cursor()

        cursor.execute(sql,values)

        conn.commit()

        print('inserted')

    except sqlite3.Error as e:

        print('An error occurred in INSERT INTO columns:', e.args[0])



# KEYS =========================================================

print('\n\nKEYS table:')

sql = 'DROP TABLE keys;'

try:
    cursor.execute(sql)
except sqlite3.Error as e:
    print('An error occurred in DROP TABLE keys:', e.args[0])

# --------------------------------------------------------------

sql = """
CREATE TABLE IF NOT EXISTS keys (
    key_id       TEXT NOT NULL,
    from_table   TEXT NOT NULL,
    target_table TEXT NOT NULL,
    description  TEXT,
    utype        TEXT,
    PRIMARY KEY (key_id)
) WITHOUT ROWID;
"""

try:
    cursor.execute(sql)
except sqlite3.Error as e:
    print('An error occurred in "keys" CREATE TABLE:', e.args[0])

# --------------------------------------------------------------

sql = """
INSERT INTO keys(key_id,from_table,target_table,description,utype)
VALUES(?,?,?,?,?);
"""

keydata = [
    ('1', 'TAP_SCHEMA.tables',      'TAP_SCHEMA.schemas', 'Link "tables" table to "schemas" table.',             ''),
    ('2', 'TAP_SCHEMA.columns',     'TAP_SCHEMA.tables',  'Link "columns" table to "tables" table.',             ''),
    ('3', 'TAP_SCHEMA.keys',        'TAP_SCHEMA.tables',  'Link "keys" table to "tables" table.',                ''),
    ('4', 'TAP_SCHEMA.keys',        'TAP_SCHEMA.tables',  'Also needed to link "keys" table to "tables" table.', ''),
    ('5', 'TAP_SCHEMA.key_columns', 'TAP_SCHEMA.keys',    'Link "key_columns" table to "keys" table.',           '')
]

for i in range(len(keydata)):

    values = keydata[i]

    print(values)

    try:
        cursor = conn.cursor()

        cursor.execute(sql,values)

        conn.commit()

        print('inserted')

    except sqlite3.Error as e:

        print('An error occurred in INSERT INTO keys:', e.args[0])



# KEY_COLUMNS ==================================================

print('\n\nKEY_COLUMNS table:')

sql = 'DROP TABLE key_columns;'

try:
    cursor.execute(sql)
except sqlite3.Error as e:
    print('An error occurred in DROP TABLE key_columns:', e.args[0])

# --------------------------------------------------------------

sql = """
CREATE TABLE IF NOT EXISTS key_columns (
    key_id        TEXT NOT NULL,
    from_column   TEXT NOT NULL,
    target_column TEXT NOT NULL,
    PRIMARY KEY (key_id)
) WITHOUT ROWID;
"""

try:
    cursor.execute(sql)
except sqlite3.Error as e:
    print('An error occurred in key_columns CREATE TABLE:', e.args[0])

# --------------------------------------------------------------

sql = """
INSERT INTO key_columns(key_id,from_column,target_column)
VALUES(?,?,?);
"""

keycoldata = [
    ('1', 'schema_name',  'schema_name'),
    ('2', 'table_name',   'table_name' ),
    ('3', 'from_table',   'table_name' ),
    ('4', 'target_table', 'table_name' ),
    ('5', 'key_id',       'key_id'     )
]

for i in range(len(keycoldata)):

    values = keycoldata[i]

    print(values)

    try:
        cursor = conn.cursor()

        cursor.execute(sql,values)

        conn.commit()

        print('inserted')

    except sqlite3.Error as e:

        print('An error occurred in INSERT INTO key_columns:', e.args[0])
