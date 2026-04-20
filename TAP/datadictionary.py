# Copyright (c) 2020, Caltech IPAC.
# This code is released with a BSD 3-clause license. License information is at
#   https://github.com/Caltech-IPAC/nexsciTAP/blob/master/LICENSE


import os
import logging
import re
from astropy.io import ascii


class dataDictionary:

    pid = os.getpid()

    debug = 0

    ddtbl = None

    status = ''
    msg = ''
    returnMsg = ""

    conn = None
    dbtable = ''

    #
    # DD columns
    #

    ncols = 0
    colname = {}

    colfmt   = {}
    coltype  = {}
    coldesc  = {}
    colunits = {}
    colwidth = {}

    connectInfo = None

    nfetch = 1000


    @staticmethod
    def get_date_column_types(connectInfo, debug=False):
        """Query TAP_SCHEMA for columns with xtype 'date' or 'timestamp'.

        Returns a dict mapping column_name (lowercase) to xtype string,
        covering all tables in TAP_SCHEMA.  This is used by the ADQL
        translator to wrap date string literals in DBMS-appropriate cast
        functions (TO_DATE, TO_TIMESTAMP) before the query reaches the
        database.

        Opens and closes its own connection.  The query is lightweight
        (small system table, typically under 100 rows with xtype set)
        and adds negligible overhead compared to the actual data query.

        Returns an empty dict on any failure so that ADQL translation
        proceeds without date rewriting rather than raising an error.
        """

        column_types = {}

        try:
            dbms = connectInfo['dbms'].lower()
            tap_schema = connectInfo.get('tap_schema', 'TAP_SCHEMA')
            columns_table = connectInfo.get('columns_table', 'columns')

            sql = (
                "SELECT column_name, xtype FROM "
                + tap_schema + "." + columns_table
                + " WHERE xtype IS NOT NULL"
            )

            conn = None

            if dbms == 'oracle':
                import cx_Oracle
                conn = cx_Oracle.connect(
                    connectInfo['userid'],
                    connectInfo['password'],
                    connectInfo['dbserver']
                )

            elif dbms == 'sqlite3':
                import sqlite3
                conn = sqlite3.connect(connectInfo['db'])

            elif dbms == 'mysql':
                import mysql.connector
                if connectInfo.get('dbserver') is not None:
                    conn = mysql.connector.connect(
                        user=connectInfo['userid'],
                        password=connectInfo['password'],
                        host=connectInfo['dbserver'],
                        port=int(connectInfo.get('port', 3306)),
                        db=connectInfo.get('dbschema', '')
                    )
                elif connectInfo.get('socket') is not None:
                    conn = mysql.connector.connect(
                        user=connectInfo['userid'],
                        password=connectInfo['password'],
                        unix_socket=connectInfo['socket'],
                        db=connectInfo.get('dbschema', '')
                    )

            elif dbms == 'postgresql':
                import psycopg2
                conn = psycopg2.connect(
                    user=connectInfo['userid'],
                    password=connectInfo['password'],
                    host=connectInfo['dbserver'],
                    port=int(connectInfo.get('port', 5432)),
                    dbname=connectInfo.get('db', '')
                )

            if conn is None:
                return column_types

            cursor = conn.cursor()
            cursor.execute(sql)

            for row in cursor.fetchall():
                col_name = str(row[0]).strip().lower()
                xtype = str(row[1]).strip().lower()
                if xtype in ('date', 'timestamp'):
                    column_types[col_name] = xtype

            cursor.close()
            conn.close()

            if debug:
                logging.debug(
                    f'get_date_column_types: {len(column_types)} '
                    f'date/timestamp columns found'
                )

        except Exception as e:
            if debug:
                logging.debug(
                    f'get_date_column_types failed: {str(e)}; '
                    f'proceeding without date rewriting'
                )
            column_types = {}

        return column_types


    def __init__(self, conn, table, connectInfo, **kwargs):

        """
        A dataDictionary specifies the following properites of each column in
        a database table; it is used for re-formating the output IPAC ASCII
        table.

        colname(char):       column name in DB table,
        datatype(char):      data type(char, integer, double, date),
        description(char):   description,
        units(char):         unit,
        format(char):        output format(20s, 20.10f, 10d, etc...),


        Required Input:

            conn:              database connection handle,

            table(char):      database table name,

            connectInfo:      DBMS connection info,


        Usage:

          dd = dataDictionary(conn, table, connectInfo)

        """

        self.connectInfo = connectInfo

        if('debug' in kwargs):
            self.debug = kwargs['debug']

        if self.debug:
            logging.debug('Enter dataDictionary.init')

        self.ddtbl = None
        if('ddtbl' in kwargs):
            self.ddtbl = kwargs['ddtbl']

        self.ddfile = None
        if('ddfile' in kwargs):
            self.ddfile = kwargs['ddfile']

        self.conn = conn
        self.dbtable = table

        if self.debug:
            logging.debug(f'dbtable = {self.dbtable:s}')
            logging.debug(f'ddtbl   = ' + str(self.ddtbl))
            logging.debug(f'ddfile  = ' + str(self.ddfile))


        #
        # If we have a DD table file, scan it instead of querying the TAP_SCHEMA table
        #

        if self.ddfile != None:

            if self.debug:
                logging.debug(f'Reading: {self.ddfile:s}')

            tdata = ascii.read(self.ddfile, format='ipac')
            tcolnames = tdata.colnames

            if self.debug:
                logging.debug('colnames: ' + str(tcolnames))

            i = 0 

            for trow in tdata:
                    
                dbname = ''
                desc   = ''
                if 'name' in tcolnames:
                    dbname = trow['name'].lower()
                    desc   = dbname
                    
                type = ''
                if 'intype' in tcolnames:
                    type = trow['intype']
                    
                unit = ''
                if 'units' in tcolnames:
                    unit = trow['units']

                format = ''
                if 'format' in tcolnames:
                    format = trow['format']


                width = 0
                if 'width' in tcolnames:
                    widthstr = trow['width']
                else:
                    widthstr = format

                substr = re.search(r'\d+', widthstr)

                if substr != None:
                    width = int(substr.group())

                if len(dbname) > width:
                    width = len(dbname)
                  

                self.colname[i] = dbname
                i = i+1

                self.coltype [dbname] = type
                self.coldesc [dbname] = desc
                self.colunits[dbname] = unit
                self.colfmt  [dbname] = format
                self.colwidth[dbname] = width

            return

        #
        # Construct and submit data dictionary query
        #

        cursor = self.conn.cursor()
        if self.debug:
            logging.debug('')
            logging.debug('Created DD query cursor.')

        if self.ddtbl == None:

            sql = "select * from " + self.connectInfo["tap_schema"] + "." + self.connectInfo["columns_table"] + " where lower(table_name) = " + \
                "'" + self.dbtable + "'"

            if self.debug:
                logging.debug('')
                logging.debug(f'TAP_SCHEMA sql = {sql:s}')

        else:

            sql = "select name as column_name, description as desc, units as unit, intype as datatype, format from " + self.ddtbl

            if self.debug:
                logging.debug('')
                logging.debug(f'Internal DD table sql = {sql:s}')

        try:
            cursor.execute(sql)

        except Exception as e:

            self.status = 'error'
            self.msg = 'Failed to execute [' + sql + ']'

            if self.debug:
                logging.debug('')
                logging.debug(f'errmsg = {self.msg:s}')
                logging.debug(f'str(e) = {str(e):s}')

            raise Exception(self.msg)

        if self.debug:
            logging.debug('')
            logging.debug('select TAP_SCHEMA/DD statement executed')

        #
        # { Extract column index
        #

        if self.debug:
            logging.debug('')
            logging.debug('TAP_SCHEMA/DD cursor description:')
            logging.debug('------------------------------------------------')
            logging.debug(cursor.description)
            logging.debug('------------------------------------------------')

        ncols = len(cursor.description)

        if self.debug:
            logging.debug(f'ncols = {ncols:d}\n')

        ind_colname = -1
        ind_datatype = -1
        ind_desc = -1
        ind_unit = -1
        ind_format = -1
        ind_xtype = -1

        i = 0
        for col in cursor.description:

            name = str(col[0]).lower()

            if self.debug:
                logging.debug(f'processing name = {name:s}')


            if(name == 'column_name'):
                ind_colname = i

            if(name == 'datatype'):
                ind_datatype = i

            if(name == 'description'):
                ind_desc = i

            if(name == 'unit'):
                ind_unit = i

            if(name == 'format'):
                ind_format = i

            if(name == 'xtype'):
                ind_xtype = i

            i = i + 1

        if self.debug:
            logging.debug('')
            logging.debug(f'ind_colname  = {ind_colname:d}')
            logging.debug(f'ind_datatype = {ind_datatype:d}')
            logging.debug(f'ind_desc     = {ind_desc:d}')
            logging.debug(f'ind_unit     = {ind_unit:d}')
            logging.debug(f'ind_format   = {ind_format:d}')
            logging.debug(f'ind_xtype    = {ind_xtype:d}')
        #
        # } end extract column index

        #
        # Retrieve column name, dtype, format, units from
        # TAP_SCHEMA.columns table.
        #
        # ind_colname: colname(column_name),
        # ind_datatype: datatype(datatype),
        # ind_desc: description(all nulls),
        # ind_unit: unit(all nulls),
        # ind_format: format(format: in the form of 11d, 12.6f, 25.25s etc.
        #

        cursor.arraysize = self.nfetch

        self.colfmt   = {}
        self.coltype  = {}
        self.coldesc  = {}
        self.colunits = {}
        self.colwidth = {}
        self.colxtype = {}
        self.ncols = 0

        while True:
            #
            # { while loop
            #
            
            #rows = cursor.fetchmany(self.nfetch)
            rows = cursor.fetchall()

            self.ncols = self.ncols + len(rows)

            if self.debug:
                logging.debug('')
                logging.debug(f'ncols= {self.ncols:d}')

            i = 0
            for row in rows:
            
                #
                # { for loop: each row in the file represents
                #             a column in data dictionary
                #
                if self.debug:
                    logging.debug('')
                    logging.debug(f'i = {i:d}')

                col_str = str(row[ind_colname]).strip().lower()

                if col_str.startswith('"') and col_str.endswith('"'):
                    col_str = col_str[1:-1]

                self.colname[i] = col_str

                #
                # Datatype
                #

                if(row[ind_datatype] is None):
                    self.coltype[col_str] = ''
                else:
                    self.coltype[col_str] = \
                        str(row[ind_datatype]).strip().lower()

                if(self.coltype[col_str] == 'integer'):
                    self.coltype[col_str] = 'int'

                #
                # Xtype (TAP_SCHEMA xtype column, e.g. 'date', 'timestamp').
                # Previously discarded despite being in the SELECT * result.
                # Now stored so the ADQL translator can wrap date literals
                # in DBMS-appropriate cast functions (TO_DATE, TO_TIMESTAMP).
                #

                if(ind_xtype >= 0 and row[ind_xtype] is not None):
                    self.colxtype[col_str] = \
                        str(row[ind_xtype]).strip().lower()
                else:
                    self.colxtype[col_str] = ''

                #
                # Format
                #

                if(row[ind_format] is None):
                    self.colfmt[col_str] = ''
                else:
                    self.colfmt[col_str] = \
                        str(row[ind_format]).strip().replace('i', 'd')
                    self.colfmt[col_str] = self.colfmt[col_str].lower()

                #
                # Unit
                #

                if(row[ind_unit] is None):
                    self.colunits[col_str] = ''
                else:
                    self.colunits[col_str] = str(row[ind_unit]).strip()

                #
                # Desc
                #

                if(row[ind_desc] is None):
                    self.coldesc[col_str] = ''
                else:
                    self.coldesc[col_str] = str(row[ind_desc]).strip()

                if self.debug:
                    logging.debug('')
                    logging.debug(f'      col_str  = {col_str:s}')
                    logging.debug(f'      coldesc  = {self.coldesc[col_str]:s}')
                    logging.debug(f'      coltype  = {self.coltype[col_str]:s}')
                    logging.debug(f'      colunits = {self.colunits[col_str]:s}')
                    logging.debug(f'      colfmt   = {self.colfmt[col_str]:s}')

                #
                # Determine width and fmt from colfmt arrary:
                #

                fmtstr = ''
                width = 0

                if((self.coltype[col_str] == 'char') or
                   (self.coltype[col_str] == 'date')):

                    #
                    # { coltype = char, date
                    #

                    if(self.colfmt[col_str] != 'null'):
                        #
                        # { extract width from colfmt
                        #

                        fmtstr = self.colfmt[col_str]

                        if((self.coltype[col_str] == 'date') or
                           (self.coltype[col_str] == 'char')):

                            ind = fmtstr.find('s')

                            if(ind != -1):
                                widthstr = fmtstr[0:ind]

                                width = int(widthstr)

                        #
                        # } end extract width from colfmt
                        #

                    #
                    # Failed to extract width from colfmt
                    #

                    if(width == 0):
                        width = 80

                    if(len(col_str) > width):
                        width = len(col_str)
                    if(len(self.coltype[col_str]) > width):
                        width = len(self.coltype[col_str])
                    if(len(self.colunits[col_str]) > width):
                        width = len(self.colunits[col_str])

                    fmtstr = str(width) + 's'

                    if self.debug:
                        logging.debug(f'      width    = [{width:d}] [date]')

                    #
                    # } end coltype == 'char'/'date'
                    #

                elif((self.coltype[col_str] == 'integer') or
                     (self.coltype[col_str] == 'int') or
                     (self.coltype[col_str] == 'short')):

                    #
                    # { coltype == 'int/short': extract width from fmt
                    #

                    width = 12
                    if(self.colfmt[col_str] != 'null'):

                        #
                        # { extract width from colfmt
                        #

                        fmtstr = self.colfmt[col_str]

                        ind = fmtstr.find('d')

                        if(ind != -1):
                            widthstr = fmtstr[0:ind]
                            width = int(widthstr)

                        #
                        # } end extract width from colfmt
                        #

                    if(len(col_str) > width):
                        width = len(col_str)
                    if(len(self.coltype[col_str]) > width):
                        width = len(self.coltype[col_str])
                    if(len(self.colunits[col_str]) > width):
                        width = len(self.colunits[col_str])
                    fmtstr = str(width) + 'd'

                    if self.debug:
                        logging.debug(f'      fmtstr   = {fmtstr:s} [int]')

                    #
                    # } end coltype == 'int/short'
                    #

                elif(self.coltype[col_str] == 'long'):

                    #
                    # { coltype == 'long': default 20
                    #

                    width = 20
                    if(self.colfmt[col_str] != 'null'):

                        fmtstr = self.colfmt[col_str]

                        ind = fmtstr.find('d')

                        if(ind != -1):
                            widthstr = fmtstr[0:ind]
                            width = int(widthstr)

                    if(len(col_str) > width):
                        width = len(col_str)
                    if(len(self.coltype[col_str]) > width):
                        width = len(self.coltype[col_str])
                    if(len(self.colunits[col_str]) > width):
                        width = len(self.colunits[col_str])
                    fmtstr = str(width) + 'd'

                    if self.debug:
                        logging.debug(f'      width    = {width:d} [long]')

                    #
                    #  } end coltype == 'long'
                    #

                elif((self.coltype[col_str] == 'double') or
                     (self.coltype[col_str] == 'float')):

                    #
                    #  { coltype == 'double/float'
                    #

                    width = 0
                    if(self.colfmt[col_str] != 'null'):

                        fmtstr = self.colfmt[col_str]

                        ind = fmtstr.find('.')

                        if(ind != -1):
                            widthstr = fmtstr[0:ind]
                            width = int(widthstr)

                            remstr = fmtstr[ind+1:]

                        else:
                            width = 20
                            remstr = '11e'

                    else:
                        width = 20
                        remstr = '11e'

                    if(len(col_str) > width):
                        width = len(col_str)
                    if(len(self.coltype[col_str]) > width):
                        width = len(self.coltype[col_str])
                    if(len(self.colunits[col_str]) > width):
                        width = len(self.colunits[col_str])

                    fmtstr = str(width) + '.' + remstr

                    if self.debug:
                        logging.debug(f'      width    = {width:d} [double]')

                    #
                    # } end coltype == 'double/float'
                    #

                self.colwidth[col_str] = width
                self.colfmt[col_str] = fmtstr

                if self.debug:
                    logging.debug(f'      colfmt   = {self.colfmt[col_str]:s}')

                i = i + 1

                #
                # }  end for loop
                #

            if len(rows) < cursor.arraysize:
                break

            #
            # } end while loop
            #

        return
