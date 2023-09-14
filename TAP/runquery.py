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
from TAP.writeresult import writeResult
from TAP.tablenames import TableNames

class runQuery:

    pid = os.getpid()

    debug = 0

    debugfname = './runquery.debug'
    status = ''
    msg = ''
    returnMsg = ""

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

    format = 'votable'
    maxrec = -1
    coldesc = 0


    def __init__(self, **kwargs):

        #
        # {
        #

        """
        runQuery provides a basic search interface for database tables.

        Given an SQL statement and database table name, runQuery contacts
        the DBMS server, submits the query and return the data in IPAC ASCII
        format.

        Required keyword input parameters:

            connectInfo:        Dictionary containing the info needed
                                to make a "connection".  These parameters
                                are different depending on the DBMS.
            query(char):       the sql query to be executed,
            workdir(char):     user work directory

        Optional keyword input parameters:

            outpath(char):     output file path,
            racol(char):       decimal RA column name,
            deccol(char):      decimal DEC column name,
            maxrec(int):       number of records to return(default: all)
            format(char):      return table format(default: votable)

        Usage:

          runquery = runQuery(connectInfo=connectInfo,
                              query=query,
                              workdir=userworkdir,
                              maxrec=maxrec,
                              format=format,
                              racol=racol,
                              deccol=deccol)
        """

        if('debug' in kwargs):
            self.debug = kwargs['debug']

        if self.debug:
            logging.debug('')
            logging.debug(f'Enter runQuery')
            logging.debug(f'self.debug = {self.debug:d}')


        self.arraysize = 10000
        if('arraysize' in kwargs):
            self.arraysize = kwargs['arraysize']

        #
        # Get keyword parameters
        #

        if('connectInfo' in kwargs):

            self.connectInfo = kwargs['connectInfo']

            self.dbms = self.connectInfo['dbms']

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
                    logging.debug('')
                    logging.debug(f'userid   = {self.userid:s}')
                    logging.debug(f'password = {self.password:s}')
                    logging.debug(f'dbserver = {self.dbserver:s}')


            if(self.dbms.lower() == 'sqlite3'):

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
                    logging.debug('')
                    logging.debug(f'db= {self.db:s}')
                    logging.debug(f'tap_schema= {self.tap_schema:s}')

            
            if(self.dbms.lower() == 'mysql'):

                import mysql.connector
            
                self.dbserver = None 
                self.port = 3306
                self.socket =  None
                self.db =  None
                self.userid =  None
                self.password =  None
                
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

                if(self.db is None):
                    self.msg = 'Failed to retrieve required input parameter'\
                               ' [dbschema]'
                    self.status = 'error'
                    raise Exception(self.msg)

                if self.debug:
                    logging.debug('')
                    logging.debug('dbserver=')
                    logging.debug(self.dbserver)
                    logging.debug('port=')
                    logging.debug(self.port)
                    logging.debug('socket=')
                    logging.debug(self.socket)
                    logging.debug(f'db   = {self.db:s}')
                    logging.debug(f'userid   = {self.userid:s}')
                    logging.debug(f'password = {self.password:s}')
        
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
            self.msg = \
                'Failed to retrieve required input parameter [userworkdir]'
            self.status = 'error'
            raise Exception(self.msg)

        self.filename =  None
        if('filename' in kwargs):
            self.filename = kwargs['filename']


        if self.debug:
            logging.debug('')
            logging.debug(f'userworkdir= {self.userworkdir:s}')
            logging.debug(f'filename= {self.filename:s}')
            logging.debug(f'sql= {self.sql:s}')


        self.racol =  'ra'
        if('racol' in kwargs):
            self.racol = kwargs['racol']

        self.deccol = 'dec'
        if('deccol' in kwargs):
            self.deccol = kwargs['deccol']

        if self.debug:
            logging.debug('')
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
            logging.debug('')
            logging.debug(f'format= {self.format:s}')
            logging.debug(f'maxrec= {self.maxrec:d}')


        #
        # Extract DB table name from query
        #

        self.dbtable = None 

        tn = TableNames()
        tables = tn.extract_tables(self.sql)

        if len(tables) > 0:
            self.dbtable = tables[0]

        if self.debug:
            logging.debug('')
            logging.debug(f'dbtable= [{self.dbtable:s}]')

        #
        # Connect to DBMS
        #

        if(self.dbms.lower() == 'oracle'):

            try:
                self.conn = cx_Oracle.connect(self.userid,
                                              self.password,
                                              self.dbserver)

                if self.debug:
                    logging.debug('')
                    logging.debug('connected to Oracle, DB ' + self.dbserver)

            except Exception as e:

                self.status = 'error'
                self.msg = 'Failed to connect to cx_Oracle'

                raise Exception(self.msg)

        elif(self.dbms.lower() == 'sqlite3'):

            try:
                self.conn = sqlite3.connect(self.db)

                if self.debug:
                    logging.debug('')
                    logging.debug('connected to SQLite3, database ' + self.db)

                cmd = 'ATTACH DATABASE ? AS TAP_SCHEMA'

                dbspec = (self.tap_schema,)

                if self.debug:
                    logging.debug('')
                    logging.debug('cmd: ' + cmd + '(' + self.tap_schema + ')')

                cursor = self.conn.cursor()

                cursor.execute(cmd, dbspec)

                if self.debug:
                    logging.debug('')
                    logging.debug('TAP_SCHEMA attached')

            except Exception as e:

                self.status = 'error'
                self.msg = 'Failed to connect to SQLite3 databases'

                raise Exception(self.msg)

        elif (self.dbms.lower() == 'mysql'):
       
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
                    logging.debug('')
                    logging.debug('mysql connected')

            except Exception as e:

                self.status = 'error'
                self.msg = 'Failed to connect to mysql databases'

                raise Exception(self.msg)

            if self.debug:
                logging.debug('')
                logging.debug('here0')
           
        else:
            self.status = 'error'
            self.msg = 'Invalid DBMS'

            raise Exception(self.msg)

        #
        # Retrieve dd table
        #

        self.dd = None

        try:
            self.dd = dataDictionary(self.conn, self.dbtable, debug=self.debug)

            if self.debug:
                logging.debug('')
                logging.debug('DD successfully retrieved')

        except Exception as e:

            if self.debug:
                logging.debug('')
                logging.debug('dataDictionary exception: {str(e)}')

            self.msg = f'dataDictionary retrieval exception: {str(e)}'

            #raise Exception(self.msg)

        if self.debug:
            logging.debug('')
            logging.debug('Done DD retrieval')

        #
        # Submit database query of user input sql
        #

        cursor = self.conn.cursor()
        
        if self.debug:
            logging.debug('')
            logging.debug(f'sql = {self.sql:s}')
            logging.debug('call execute sql')

        try:
            self.__executeSql__(cursor, self.sql, debug=1)
        
            if self.debug:
                logging.debug('')
                logging.debug('returned executeSql')

        except Exception as e:

            if self.debug:
                logging.debug('')
                logging.debug(f'executeSql exception: {str(e):s}')

            raise Exception(str(e))

        if self.debug:
            logging.debug('')
            logging.debug('returned executeSql')


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
                logging.debug('')
                logging.debug(f'writeResult exception: {str(e):s}')

            raise Exception(str(e))

        self.stat = 'ok'
        self.outpath = wresult.outpath
        self.ntot = wresult.ntot
        self.returnMsg = \
            f'runQuery completed: query result path: {self.outpath:s}'

        if self.debug:
            logging.debug('')
            logging.debug(f'outpath = {self.outpath:s}')

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
            logging.debug('')
            logging.debug('Enter executeSql')
            logging.debug(f'sql:= {sql:s}')

        try:
            cursor.execute(sql)
        except Exception as e:

            msg = str(e).replace('"', "'")

            if self.debug:
                logging.debug('')
                logging.debug(f'executeSql exception: {str(msg):s}')

            # raise Exception(str(e))
            raise Exception(msg)

        #
        # } end of executeSql def
        #


def main():

    sys.tracebacklimit = 0

    parser = argparse.ArgumentParser(description='runQuery')

    parser.add_argument('--dsn', required=True,
                        help='Oracle database(DSN) to use.')

    parser.add_argument('--table', required=True,
                        help='Database table to query.')

    parser.add_argument('--sql', required=True,
                        help='SQL SELECT statement.')

    parser.add_argument('--configpath', required=True,
                        help='Configuration file specifying DSN'
                             ' userid, passwd, and server.')

    parser.add_argument('--workdir', required=True,
                        help='workdir for the result.')

    parser.add_argument('--debug', required=False,
                        help='debugflag: 1/0.')

    args = parser.parse_args()

    dsn_in = ''
    table = ''
    sql = ''
    configpath = ''
    workdir = ''
    debug = 0

    dsn_in = args.dsn
    table = args.table
    sql = args.sql
    configpath = args.configpath
    workdir = args.workdir
    debug = args.debug

    confobj = configobj.ConfigObj(configpath)

    dsn = ''
    if dsn_in.upper() in confobj:
        dsn = dsn_in.upper()
    elif dsn_in.lower() in confobj:
        dsn = dsn_in.lower()

    userid = ''
    if('UserID' in confobj[dsn]):
        userid = confobj[dsn]['UserID']

    password = ''
    if('Password' in confobj[dsn]):
        password = confobj[dsn]['Password']

    if('ServerName' in confobj[dsn]):
        dbserver = confobj[dsn]['ServerName']

    racol = ''
    if('RACOL' in confobj['webserver']):
        racol = confobj['webserver']['RACOL']

    deccol = ''
    if('DECCOL' in confobj['webserver']):
        deccol = confobj['webserver']['DECCOL']

    query = None
    try:

        query = runQuery(dbserver=dbserver,
                         userid=userid,
                         password=password,
                         dbtable=table,
                         query=sql,
                         workdir=workdir,
                         racol=racol,
                         deccol=deccol,
                         format='ipac',
                         maxrec='10000',
                         debug=debug)

        print(query.returnMsg)

    except Exception as e:

        print(f'runQuery failed: {str(e):s}')


if __name__ == "__main__":
    main()
