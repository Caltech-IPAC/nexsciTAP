# Copyright (c) 2020, Caltech IPAC.
# This code is released with a BSD 3-clause license. License information is at
#   https://github.com/Caltech-IPAC/nexsciTAP/blob/master/LICENSE

import sys
import os
import logging

import datetime

import argparse
import configobj

from TAP.datadictionary import dataDictionary
from TAP.writeresult    import writeResult
from TAP.tablenames     import TableNames
from TAP.configparam    import configParam

from ADQL.adql import ADQL

from spatial_index import SpatialIndex


class tapQuery:

    pid = os.getpid()

    debug = 0

    status = ''
    msg = ''
    returnMsg = ''

    #
    # DD columns
    #

    dd = None

    conn = None
    dbtable = None

    sql = None
    racol = 'ra'
    deccol = 'dec'

    nfetch = 1000

    outpath = None
    userworkdir = None
    ntot = 0

    ddtbl = None

    format = 'votable'
    maxrec = -1
    coldesc = 0


    def __init__(self, **kwargs):

        #
        # {
        #

        """
        tapQuery provides a basic search interface for database tables.

        Given an SQL statement and database table name, tapQuery contacts
        the DBMS server, submits the query and return the data in IPAC ASCII
        format.

        Required keyword input parameters:

            connectInfo:       Dictionary containing the info needed
                               to make a "connection".  These parameters
                               are different depending on the DBMS.
            conn:              We will need the above connectInfo for it's
                               formatting info but we may have already made
                               the DBMS connection elsewhere (for more involved
                               processing scenarios).  In that case we can 
                               optionally pass in the connection itself 
                               instead of creating it here.
            query(char):       the sql query to be executed,
            workdir(char):     user work directory

        Optional keyword input parameters:

            outpath(char):     output file path,
            racol(char):       decimal RA column name,
            deccol(char):      decimal DEC column name,
            maxrec(int):       number of records to return(default: all)
            format(char):      return table format(default: votable)

        Usage:

          tapquery = tapQuery(connectInfo=connectInfo,
                              query=query,
                              workdir=userworkdir,
                              filename=filename,
                              maxrec=maxrec,
                              ddtbl=ddtbl,  
                              format=format,
                              racol=racol,
                              deccol=deccol)
        """

        if('debug' in kwargs):
            self.debug = kwargs['debug']

        self.ddtbl = None
        if('ddtbl' in kwargs):
            self.ddtbl = kwargs['ddtbl']

        if self.debug:
            logging.debug(f'Enter tapQuery')
            logging.debug(f'self.debug = {self.debug:d}')


        self.arraysize = 10000
        if('arraysize' in kwargs):
            self.arraysize = kwargs['arraysize']

        if('connectInfo' in kwargs):
            self.connectInfo = kwargs['connectInfo']
            self.dbms = self.connectInfo['dbms']

            self.tap_schema = 'None'
            if 'tap_schema' in self.connectInfo :
                self.tap_schema = self.connectInfo['tap_schema']

            if 'tap_schema_file' in self.connectInfo :
                self.tap_schema_file = self.connectInfo['tap_schema_file']


        # If the DBMS connection has already been made:

        if('conn' in kwargs):
            self.conn = kwargs['conn']


        # Otherwise collect the info for making the connection and make it.

        else:
            
            # Collect DBMS-specific keyword parameters
            #
            # There are two use modes for getting these parameters.  Most of the time we let this code do everything;
            # getting the connection paramters and making the actual connection ('conn' parameter) to whichever DBMS
            # we are using.  But sometimes we have a more complicated scenario (like using and populating temporary
            # tables as part of the processing).  The we will have run tapUtil() to do this same setup and used the 
            # database connection for these other steps before we get here.  The all of the code here and the section
            # below where we connect to the database will have already been executed there. 


            # ORACLE

            if(self.dbms.lower() == 'oracle'):

                import cx_Oracle

                self.dbserver = None 
                if('dbserver' in self.connectInfo):
                    self.dbserver = self.connectInfo['dbserver']

                if (self.dbserver is None):
                    self.msg = 'Failed to retrieve required input parameter'\
                               ' [dbserver]'
                    self.status = 'error'
                    raise Exception(self.msg)

                self.userid = None 
                if ('userid' in self.connectInfo):
                    self.userid = self.connectInfo['userid']

                if (self.userid is None):
                    self.msg = 'Failed to retrieve required input parameter'\
                               ' [userid]'
                    self.status = 'error'
                    raise Exception(self.msg)

                self.password =  None
                if ('password' in self.connectInfo):
                    self.password = self.connectInfo['password']

                if (self.password is None):
                    self.msg = 'Failed to retrieve required input parameter'\
                               ' [password]'
                    self.status = 'error'
                    raise Exception(self.msg)

                if self.debug:
                    logging.debug(f'userid   = {self.userid:s}')
                    logging.debug(f'password = {self.password:s}')
                    logging.debug(f'dbserver = {self.dbserver:s}')


            # SQLITE3

            elif self.dbms.lower() == 'sqlite3':

                import sqlite3

                self.db = None 
                if ('db' in self.connectInfo):
                    self.db = self.connectInfo['db']

                if (self.db is None):
                    self.msg = 'Failed to retrieve required input parameter'\
                               ' [db]'
                    self.status = 'error'
                    raise Exception(self.msg)

                self.tap_schema = None 
                if('tap_schema' in self.connectInfo):
                    self.tap_schema = self.connectInfo['tap_schema']

                if (self.tap_schema is None):
                    self.msg = 'Failed to retrieve required input parameter'\
                               ' [tap_schema]'
                    self.status = 'error'
                    raise Exception(self.msg)

                if self.debug:
                    logging.debug(f'db= {self.db:s}')
                    logging.debug(f'tap_schema= {self.tap_schema:s}')

            
            # MYSQL

            elif self.dbms.lower() == 'mysql':

                import mysql.connector
            
                self.dbserver = None 
                self.port     = 3306
                self.socket   = None
                self.db       = None
                self.userid   = None
                self.password = None
                
                if ('dbserver' in self.connectInfo):
                    self.dbserver = self.connectInfo['dbserver']

                if (self.dbserver is None):
                    self.socket = self.connectInfo['socket']
                else:
                    port = None
                    port = self.connectInfo['port']
                    if (port is not None):
                        self.port = int(port)


                if ((self.dbserver is None) and \
                    (self.socket is None)):
                    
                    self.msg = 'Failed to retrieve required input DB server ' \
                        'parameter [dbserver] or [socket]'
                    
                    self.status = 'error'
                    raise Exception(self.msg)

                if('userid' in self.connectInfo):
                    self.userid = self.connectInfo['userid']

                if(self.userid is  None):
                    self.msg = 'Failed to retrieve required input parameter'\
                               ' [userid]'
                    self.status = 'error'
                    raise Exception(self.msg)

                if('password' in self.connectInfo):
                    self.password = self.connectInfo['password']

                if(self.password is None):
                    self.msg = 'Failed to retrieve required input parameter'\
                               ' [password]'
                    self.status = 'error'
                    raise Exception(self.msg)

                if('dbschema' in self.connectInfo):
                    self.db = self.connectInfo['dbschema']

                if self.debug:
                    logging.debug('dbserver=')
                    logging.debug(self.dbserver)
                    logging.debug('port=')
                    logging.debug(self.port)
                    logging.debug('socket=')
                    logging.debug(self.socket)
                    logging.debug(f'db   = {self.db:s}')
                    logging.debug(f'userid   = {self.userid:s}')
                    logging.debug(f'password = {self.password:s}')


            # POSTGRESQL

            elif self.dbms.lower() == 'pgsql':

                import psycopg2

                self.hostname = None 
                if('hostname' in self.connectInfo):
                    self.hostname = self.connectInfo['hostname']

                if (self.hostname is None):
                    self.msg = 'Failed to retrieve required input parameter'\
                               ' [hostname]'
                    self.status = 'error'
                    raise Exception(self.msg)

                self.database = None 
                if('database' in self.connectInfo):
                    self.database = self.connectInfo['database']

                if (self.database is None):
                    self.msg = 'Failed to retrieve required input parameter'\
                               ' [database]'
                    self.status = 'error'
                    raise Exception(self.msg)

                self.username = None 
                if('username' in self.connectInfo):
                    self.username = self.connectInfo['username']

                if (self.username is None):
                    self.msg = 'Failed to retrieve required input parameter'\
                               ' [username]'
                    self.status = 'error'
                    raise Exception(self.msg)

                self.password = None 
                if('password' in self.connectInfo):
                    self.password = self.connectInfo['password']

                if (self.password is None):
                    self.msg = 'Failed to retrieve required input parameter'\
                               ' [password]'
                    self.status = 'error'
                    raise Exception(self.msg)

                if self.debug:
                    logging.debug(f'hostname = {self.hostname:s}')
                    logging.debug(f'database = {self.database:s}')
                    logging.debug(f'username = {self.username:s}')
                    logging.debug(f'password = {self.password:s}')




            # BAD (OR NO) DBMS

            else:
                self.status = 'error'
                self.msg = 'Invalid DBMS'

                raise Exception(self.msg)



        # Connect to the DBMS

            # ORACLE

            if self.dbms.lower() == 'oracle':

                try:
                    self.conn = cx_Oracle.connect(self.userid,
                                                  self.password,
                                                  self.dbserver)

                    if self.debug:
                        logging.debug('connected to Oracle, DB ' + self.dbserver)

                except Exception as e:

                    self.status = 'error'
                    self.msg = 'Failed to connect to cx_Oracle'

                    raise Exception(self.msg)

          
            # SQLITE3

            elif self.dbms.lower() == 'sqlite3':

                try:
                    self.conn = sqlite3.connect(self.db)

                    if self.debug:
                        logging.debug('connected to SQLite3, database ' + self.db)

                    cmd = 'ATTACH DATABASE ? AS ' + self.tap_schema_file

                    dbspec = (self.tap_schema,)

                    if self.debug:
                        logging.debug('cmd: ' + cmd + '(' + self.tap_schema + ')')

                    cursor = self.conn.cursor()

                    cursor.execute(cmd, dbspec)

                    if self.debug:
                        logging.debug('TAP_SCHEMA attached')

                except Exception as e:

                    self.status = 'error'
                    self.msg = 'Failed to connect to SQLite3 databases'

                    raise Exception(self.msg)


            # MYSQL

            elif self.dbms.lower() == 'mysql':
           
                try:
                    if (self.dbserver is not None):

                        self.conn = mysql.connector.connect (
                            user=self.userid, \
                            password=self.password, \
                            host=self.dbserver, \
                            port=self.port, \
                            db=self.db
                        )
                    
                    elif (self.socket is not None):

                        self.conn = mysql.connector.connect (
                            user=self.userid, \
                            password=self.password, \
                            unix_socket=self.socket, \
                            db=self.db
                        )
                    
                    else:
                        self.status = 'error'
                        self.msg = 'Failed to connect to mysql databases'
                        raise Exception(self.msg)

                    if self.debug:
                        logging.debug('mysql connected')

                except Exception as e:

                    self.status = 'error'
                    self.msg = 'Failed to connect to mysql databases'

                    raise Exception(self.msg)

                if self.debug:
                    logging.debug('here0')
               

            # POSTGRESQL

            elif self.dbms.lower() == 'pgsql':

                try:
                    self.conn = psycopg2.connect (
                        host=self.hostname, \
                        database=self.database, \
                        user=self.username, \
                        password=self.password
                    )

                    if self.debug:
                        logging.debug('connected to postgresql DB ' + self.hostname)

                except Exception as e:

                    self.status = 'error'
                    self.msg = 'Failed to connect to pgsql: ' + str(e)

                    raise Exception(self.msg)


 
        # Get the query and query processing parameters (format, workspace, coordinate columns, etc.)

        self.sql = None 
        if('query' in kwargs):
            self.sql = kwargs['query']

        if(self.sql is None):
            self.msg = 'Failed to retrieve required input parameter [query]'
            self.status = 'error'
            raise Exception(self.msg)

        self.userworkdir =  None
        if('workdir' in kwargs):
            self.userworkdir = kwargs['workdir']

        if (self.userworkdir is None):
            self.userworkdir = '.'

        self.filename = None
        if('filename' in kwargs):
            self.filename = kwargs['filename']

        if self.debug:
            logging.debug(f'userworkdir= {self.userworkdir:s}')

            if('filename' in kwargs):
                logging.debug(f'filename= {self.filename:s}')
            else:
                logging.debug(f'filename= None')

            logging.debug(f'sql= {self.sql:s}')


        self.racol =  'ra'
        if('racol' in kwargs):
            self.racol = kwargs['racol']

        self.deccol = 'dec'
        if('deccol' in kwargs):
            self.deccol = kwargs['deccol']

        if self.debug:
            logging.debug(f'racol= {self.racol:s}')
            logging.debug(f'deccol= {self.deccol:s}')

        if('format' in kwargs):
            self.format = kwargs['format']

        if('maxrec' in kwargs):
            maxrecstr = kwargs['maxrec']

            try:
                self.maxrec = int(maxrecstr)

            except Exception as e:

                self.msg = "Failed to convert input maxrec value [" + \
                    maxrecstr + "] to integer."
                raise Exception(self.msg)

        if self.debug:
            #  if self.ddtbl != None:
            #      logging.debug(f'ddtbl= {self.ddtbl:s}')

            logging.debug(f'format= {self.format:s}')
            logging.debug(f'maxrec= {self.maxrec:d}')


        # Extract DB table name from query

        self.dbtable = None 

        tn = TableNames()
        tables = tn.extract_tables(self.sql)

        if len(tables) > 0:
            self.dbtable = tables[0]

        if self.debug:
            logging.debug(f'dbtable= [{self.dbtable:s}]')


        #
        # Retrieve dd table
        #

        self.dd = None

        if self.tap_schema.lower() == 'none':
            if self.debug:
                logging.debug('No DD; all formats defaulting.')

        else:
            try:
                self.dd = dataDictionary(self.conn, self.dbtable, self.connectInfo, ddtbl=self.ddtbl, debug=self.debug)

                if self.debug:
                    logging.debug('DD successfully retrieved.')

            except Exception as e:

                if self.debug:
                    logging.debug('dataDictionary retrieval failure.')

                self.msg = f'dataDictionary retrieval failure.'

                raise Exception(self.msg)

            if self.debug:
                logging.debug('Done DD retrieval')

        #
        # Submit database query with user input SQL
        #

        cursor = self.conn.cursor()
        
        if self.debug:
            logging.debug(f'sql = {self.sql:s}')
            logging.debug('call execut sql')

        self.msg = self.__executeSql__(cursor, self.sql, debug=1)
    
        if self.debug:
            logging.debug('returned executeSql')

        if len(self.msg) > 0:
            raise Exception(self.msg)
            

    #
    # Call writeResult
    #

        try:
            wresult = writeResult(cursor,
                                  self.userworkdir,
                                  self.dd,
                                  filename=self.filename,
                                  format=self.format,
                                  maxrec=self.maxrec,
                                  arraysize=self.arraysize,
                                  coldesc=self.coldesc,
                                  racol=self.racol,
                                  deccol=self.deccol,
                                  dbms=self.dbms, \
                                  debug=self.debug)

        except Exception as e:

            if self.debug:
                logging.debug(f'writeResult exception: {str(e):s}')

            raise Exception(str(e))

        self.stat    = 'OK'
        self.outpath = wresult.outpath
        self.ntot    = wresult.ntot

        if self.debug:
            logging.debug('Return:')
            logging.debug('stat    = ' + str(self.stat))
            logging.debug('outpath = ' + str(self.outpath))
            logging.debug('ntot    = ' + str(self.ntot))

        #
        # } end of init def
        #


    def __executeSql__(self, cursor, sql, **kwargs):

        #
        # {
        #

        debug = 0

        if('debug' in kwargs):
            debug = kwargs['debug']

        if self.debug:
            logging.debug('Enter executeSql')
            logging.debug(f'sql:= {sql:s}')

        #
        # The actual database query is the cursor.execute() statement below.
        # If the query fails, we get an exception, which we will save and then
        # try a rollback() (to avoid memory issues), whether or not the query
        # was successful.

        # Then if the query was successful we will return an empty string and
        # if it failed we will return the exception message.

        return_message = ''

        try:
            cursor.execute(sql)
            
        except Exception as e:
            return_message = str(e).replace('"', "'")


        # Rollback just to be safe.

        try:
            self.conn.rollback()
        
        except Exception as e:
            pass
            
        return return_message

        #
        # } end of executeSql def
        #



# Note that if we use this main method, we implicitly need to create the DBMS connection implicitly.
# So there is not 'conn' parameter included.  That is for other use cases where this main is not used
# (e.g., where we use tapUtil() to create the connections or where we are running the nexsciTAP web
# service).

def main():

    pid = os.getpid()

    sys.tracebacklimit = 0

    debugfname = '/tmp/tap_' + str(pid) + '.debug'

    parser = argparse.ArgumentParser(description='tapQuery')

    parser.add_argument('--configpath',    help='Configuration file specifying database userid, passwd, and server.')
    parser.add_argument('--instance',      help='Configuration instance (defaults to using predefined config).')
    parser.add_argument('--sql',           help='ADQL (SQL) SELECT statement.')
    parser.add_argument('--filename',      help='Output filename.', default='results.tbl')
    parser.add_argument('--ddtbl',         help='Format of output table.', default=None)
    parser.add_argument('--format',        help='Format of output table.', default='ipac')
    parser.add_argument('--maxrec',        help='Maximum number of records on output (default: all).', default='-1')
    parser.add_argument('--debug',         help='Debug flag: 1/0 (default: 0).', default='0')

    args = parser.parse_args()

    adql_string   = args.sql
    configpath    = args.configpath
    instance      = args.instance
    format        = args.format
    ddtbl         = args.ddtbl
    maxrec        = args.maxrec
    filename      = args.filename
    debug         = int(args.debug)

    if debug:

        logging.basicConfig(filename=debugfname,
                            format='%(levelname)-8s %(relativeCreated)d>  '
                            '%(filename)s %(lineno)d  '
                            '(%(funcName)s):   %(message)s',
                            level=logging.DEBUG)

        logging.debug('TAP version:  3Apr2024 with Oracle, PostgreSQL, SQLite and MySQL.')

    arraysize  = 10000

    if configpath == None:
        if 'TAP_CONF' in os.environ:
            configpath = os.environ['TAP_CONF']
        else:
            print('[struct stat="ERROR", msg="No config file path given or in TAP_CONF environment variable."]')
            exit(0)

    if adql_string == None:
            print('[struct stat="ERROR", msg="Need at least the SQL string (and usually an ouput filname).  Run again with --help for details."]')
            exit(0)

    config = None

    try:
        # ADQL/spatial-index config

        config = configParam(configpath, instance=instance, debug=debug)

        if debug:
            logging.debug('config:')
            logging.debug('%s', config)

        dbms    = config.dbms
        xcol    = config.adqlparam['xcol']
        ycol    = config.adqlparam['ycol']
        zcol    = config.adqlparam['zcol']
        racol   = config.racol
        deccol  = config.deccol

        level   = config.adqlparam['level']
        colname = config.adqlparam['colname']

        encoding = SpatialIndex.BASE4
        if(config.adqlparam['encoding'] == 'BASE10'):
            encoding = SpatialIndex.BASE10

        mode = SpatialIndex.HTM
        if(config.adqlparam['mode'] == 'HPX'):
            mode = SpatialIndex.HPX


        # Set up ADQL translator

        adql = ADQL(dbms=dbms, mode=mode, level=level, indxcol=colname,
                    encoding=encoding, racol=racol, deccol=deccol,
                    xcol=xcol, ycol=ycol, zcol=zcol)

    except Exception as e:

        if debug:
            logging.debug(f'config exception: {str(e):s}')

        print('[struct stat="ERROR", msg="Config exception: could not find section/parameter ' + str(e) + '"]')
        exit(0)

    try:
        sql_string = adql.sql(adql_string)

        if debug:
            logging.debug(f'ADQL string: {adql_string:s}')
            logging.debug(f' SQL string: {sql_string:s}')

        query = tapQuery(connectInfo=config.connectInfo,
                         query=sql_string,
                         filename=filename,
                         ddtbl=ddtbl,
                         format=format,
                         maxrec=maxrec,
                         arraysize=arraysize,
                         racol=config.racol,
                         deccol=config.deccol,
                         debug=debug)

        status  = query.stat.upper()
        outfile = query.outpath
        nrec    = query.ntot
        msg     = query.returnMsg

        if status == 'OK':
            print('[struct stat="' + status + '", filename="' + outfile + '", nrec=' + str(nrec) + ']')
        else:
            print('[struct stat="' + status + '", msg="' + msg + '"]')

    except Exception as e:
        print('[struct stat="ERROR", msg="Query failed: ' + str(e) + '"]')
        exit(0)


if __name__ == "__main__":
    main()
