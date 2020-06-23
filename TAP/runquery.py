import sys
import os
import io
import logging
import re

import time
import datetime

#import itertools
import cx_Oracle
import sqlite3
import argparse
import configobj

from TAP.datadictionary import dataDictionary
from TAP.writeresult import writeResult 
from TAP.tablenames import TableNames


class runQuery:

#    sys.tracebacklimit = 0

    pid = os.getpid()
    
    debug = 0
    debugtime = 0
    
    debugfname = './runquery.debug'
    status = ''
    msg = ''
    returnMsg = ""

#
#    dd columns
#
    dd = None

    conn = None
    dbtable = ''
    ddtable = ''
    
    sql = ''
    racol = ''
    deccol = ''

    nfetch = 1000

    querytbl = 'query.tbl'
    querypath = ''

    outpath = ''
    userworkdir = ''
    ntot = 0

    format = 'votable'
    maxrec = -1
    coldesc = 0

    
    def __init__(self, **kwargs):
#
#{
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
	    query (char):       the sql query to be executed,
	    workdir (char):     user work directory  
	    
	Optional keyword input parameters:

            outpath (char):     output file path, 
            racol (char):       decimal RA column name,
            deccol (char):      decimal DEC column name,
            maxrec (int):       number of records to return (default: all)
            format (char):      return table format (default: votable)

	Usage:

          runquery = runQuery (connectInfo=connectInfo,
	                      query=query, 
			      workdir=userworkdir,
			      maxrec=maxrec,
			      format=format,
			      racol=racol,
			      deccol=deccol)
        """

        if ('debug' in kwargs):
            self.debug = kwargs['debug']

        if ('debugtime' in kwargs):
            self.debugtime = kwargs['debugtime']

        if  (self.debug or self.debugtime):

            if (len(self.debugfname) > 0):
      
                logging.basicConfig (filename=self.debugfname, \
		    level=logging.DEBUG)
    
                with open (self.debugfname, 'w') as fdebug:
                    pass

            logging.debug ('')
            logging.debug (f'Enter runQuery.init')

       
        if self.debugtime:
            time0 = datetime.datetime.now()
            logging.debug ('')
            logging.debug (f'Enter runQuery.init')

#
#    get keyword parameters
#
        if ('connectInfo' in kwargs):

            self.connectInfo = kwargs['connectInfo']

            self.dbms = self.connectInfo['dbms']

            if(self.dbms.lower() == 'oracle'):

                self.dbserver = ''
                if ('dbserver' in self.connectInfo):
                    self.dbserver  = self.connectInfo['dbserver']

                if (len(self.dbserver) == 0):
                    self.msg = 'Failed to retrieve required input parameter [dbserver]'
                    self.status = 'error'
                    raise Exception (self.msg) 

                self.dbuser = ''
                if ('userid' in self.connectInfo):
                    self.dbuser  = self.connectInfo['userid']

                if (len(self.dbuser) == 0):
                    self.msg = 'Failed to retrieve required input parameter [userid]'
                    self.status = 'error'
                    raise Exception (self.msg) 

                self.dbpassword = ''
                if ('password' in self.connectInfo):
                    self.dbpassword  = self.connectInfo['password']

                if (len(self.dbpassword) == 0):
                    self.msg = 'Failed to retrieve required input parameter [password]'
                    self.status = 'error'
                    raise Exception (self.msg) 
                
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'dbuser= {self.dbuser:s}')
                    logging.debug (f'dbpassword= {self.dbpassword:s}')
                    logging.debug (f'dbserver= {self.dbserver:s}')


            if(self.dbms.lower() == 'sqlite3'):

                self.db = ''
                if ('db' in self.connectInfo):
                    self.db  = self.connectInfo['db']

                if (len(self.db) == 0):
                    self.msg = 'Failed to retrieve required input parameter [db]'
                    self.status = 'error'
                    raise Exception (self.msg) 

                self.tap_schema = ''
                if ('userid' in self.connectInfo):
                    self.tap_schema  = self.connectInfo['userid']

                if (len(self.tap_schema) == 0):
                    self.msg = 'Failed to retrieve required input parameter [userid]'
                    self.status = 'error'
                    raise Exception (self.msg) 

                if self.debug:
                    logging.debug ('')
                    logging.debug (f'tap_schema= {self.db:s}')
                    logging.debug (f'db= {self.tap_schema:s}')
        

        self.sql = ''
        if ('query' in kwargs):
            self.sql  = kwargs['query']

        if (len(self.sql) == 0):
            self.msg = 'Failed to retrieve required input parameter [query]'
            self.status = 'error'
            raise Exception (self.msg) 

        self.userworkdir = ''
        if ('workdir' in kwargs):
            self.userworkdir  = kwargs['workdir']

        if (len(self.userworkdir) == 0):
            self.msg = \
	        'Failed to retrieve required input parameter [userworkdir]'
            self.status = 'error'
            raise Exception (self.msg) 

        if self.debug:
            logging.debug ('')
            logging.debug (f'userworkdir= {self.userworkdir:s}')
            logging.debug (f'sql= {self.sql:s}')


        self.racol = ''
        if ('racol' in kwargs):
            self.racol  = kwargs['racol']

        self.deccol = ''
        if ('deccol' in kwargs):
            self.deccol  = kwargs['deccol']

        if self.debug:
            logging.debug ('')
            logging.debug (f'racol= {self.racol:s}')
            logging.debug (f'deccol= {self.deccol:s}')

         
        if ('format' in kwargs):
            self.format = kwargs['format']

        if ('maxrec' in kwargs):
            maxrecstr = kwargs['maxrec']
            
            try:
                self.maxrec = int(maxrecstr)
            
            except Exception as e:
                
                self.msg = "Failed to convert input maxrec value [" + \
                    maxrecstr + "] to integer."
                raise Exception (msg)

        if self.debug:
            logging.debug ('')
            logging.debug (f'format= {self.format:s}')
            logging.debug (f'maxrec= {self.maxrec:d}')
           

#
#    extract DB table name from query
#
        self.dbtable = ''
        self.ddtable = ''

        tn = TableNames()
        tables = tn.extract_tables(self.sql)

        if len(tables) > 0:
            self.dbtable = tables[0]

        if (len(self.dbtable) > 0):
            self.ddtable = self.dbtable + '_dd'

        if self.debug:
            logging.debug ('')
            logging.debug (f'dbtable= [{self.dbtable:s}]')
            logging.debug (f'ddtable= {self.ddtable:s}')

        
        if self.debugtime:
            time1 = datetime.datetime.now()
            delt = (time1 - time0).total_seconds()
            logging.debug ('')
            logging.debug (f'time (init--retrieve param): {delt:f}')


#        
#    Connect to DBMS
#
        if self.debugtime:
            time0 = datetime.datetime.now()
        
        if(self.dbms.lower() == 'oracle'):

            try:
                self.conn = cx_Oracle.connect (\
                    self.dbuser, \
                    self.dbpassword, \
                    self.dbserver)

            except Exception as e:
                  
                self.status = 'error'
                self.msg = 'Failed to connect to cx_Oracle'
            
                raise Exception (self.msg) 

        
        elif(self.dbms.lower() == 'sqlite3'):

            try:
                self.conn = sqlite3.connect (\
                    self.db, self.tap_schema)

            except Exception as e:
                  
                self.status = 'error'
                self.msg = 'Failed to connect to SQLite3'
            
                raise Exception (self.msg) 

        else:
            self.status = 'error'
            self.msg = 'Invalid DBMS'
        
            raise Exception (self.msg) 
        

        if self.debug:
            logging.debug ('')
            logging.debug ('connected to DBMS')

        if self.debugtime:
            time1 = datetime.datetime.now()
            delt = (time1 - time0).total_seconds()
            logging.debug ('')
            logging.debug (f'time (connect to DBMS): {delt:f}')
            
#
#    retrieve dd table
#
        if self.debug:
            logging.debug ('')
            logging.debug (f'self.ddtable= {self.ddtable:s}')
            logging.debug ('call dataDictionry')

        if self.debugtime:
            time0 = datetime.datetime.now()
        
        self.dd = None 
        try:
            if self.debug:
                self.dd = dataDictionary (self.conn, self.ddtable, debug=1)   
            else:
                self.dd = dataDictionary (self.conn, self.ddtable)   


            if self.debug:
                logging.debug ('')
                logging.debug ('returned dataDictionary')
            
        except Exception as e:

            if self.debug:
                logging.debug ('')
                logging.debug ('dataDictionary exception: {str(e)}')
            
            self.msg = f'dataDictionary retrieval exception: {str(e)}'
           
            raise Exception (self.msg)

        if self.debug:
            logging.debug ('')
            logging.debug ('dd successfully retrieved')
        
        if self.debugtime:
            time1 = datetime.datetime.now()
            delt = (time1 - time0).total_seconds()
            logging.debug ('')
            logging.debug (f'time (retrieve DD: {delt:f}')

#
#    setup querypath
#
        self.querypath = self.userworkdir + '/' + self.querytbl

#        
# Submit database query of user input sql
#        
        if self.debug:
            logging.debug ('')
            logging.debug (f'sql = {self.sql:s}')
            logging.debug ('call execute sql')
                
        if self.debugtime:
            time0 = datetime.datetime.now()
        
        cursor = self.conn.cursor()
      
        try:
            self.__executeSql__ (cursor, self.sql)
        
        except Exception as e:

            if self.debug:
                logging.debug ('')
                logging.debug (f'executeSql exception: {str(e):s}')

            raise Exception (str(e))

        if self.debug:
            logging.debug ('')
            logging.debug ('returned executeSql')

        if self.debugtime:
            time1 = datetime.datetime.now()
            delt = (time1 - time0).total_seconds()
            logging.debug ('')
            logging.debug (f'time (execute query): {delt:f}')
            

#        
# open querypath for output
#       
        if self.debug:
            logging.debug ('')
            logging.debug (f'querypath= {self.querypath:s}')

        if self.debugtime:
            time0 = datetime.datetime.now()
        
        try:
            if (self.debug and self.debugtime):
            
                wresult = writeResult (cursor, \
                    self.userworkdir, \
                    self.dd, \
                    format=self.format, \
                    maxrec=self.maxrec, \
                    coldesc=self.coldesc, \
                    racol=self.racol, \
                    deccol=self.deccol, \
                    debugtime=1, \
                    debug=1)

            elif self.debugtime:
            
                wresult = writeResult (cursor, \
                    self.userworkdir, \
                    self.dd, \
                    format=self.format, \
                    maxrec=self.maxrec, \
                    coldesc=self.coldesc, \
                    racol=self.racol, \
                    deccol=self.deccol, \
                    debugtime=1)

            elif self.debug:
            
                wresult = writeResult (cursor, \
                    self.userworkdir, \
                    self.dd, \
                    format=self.format, \
                    maxrec=self.maxrec, \
                    coldesc=self.coldesc, \
                    racol=self.racol, \
                    deccol=self.deccol, \
                    debug=1)

            else:
                wresult = writeResult (cursor, \
                    self.userworkdir, \
                    self.dd, \
                    format=self.format, \
                    maxrec=self.maxrec, \
                    coldesc=self.coldesc, \
                    racol=self.racol, \
                    deccol=self.deccol)

        
        except Exception as e:

            if self.debug:
                logging.debug ('')
                logging.debug (f'writeResult exception: {str(e):s}')

            raise Exception (str(e))

        if self.debug:
            logging.debug ('')
            logging.debug ('returned writeResultfile')

        if self.debugtime:
            time1 = datetime.datetime.now()
            delt = (time1 - time0).total_seconds()
            logging.debug ('')
            logging.debug (f'time (write output): {delt:f}')
            
        self.stat = 'ok'
        self.outpath = wresult.outpath
        self.ntot = wresult.ntot
        self.returnMsg = \
            f'runQuery completed: query result path: {self.outpath:s}'
       
        if self.debug:
            logging.debug ('')
            logging.debug (f'here0: outpath= {self.outpath:s}')
        
#
#} end of init def 
#
    
    def __executeSql__ (self, cursor, sql, **kwargs):
#
#{
#
        debug = 0

        if ('debug' in kwargs):
            debug = kwargs['debug']

        if debug:
            logging.debug ('')
            logging.debug ('Enter executeSql')
            logging.debug (f'sql:= {sql:s}')

        try:
            cursor.execute (sql)
        except Exception as e:
            
            msg = str(e).replace('"', "'")

            if debug:
                logging.debug ('')
                logging.debug (f'create table exception: {str(msg):s}')
            
            # raise Exception (str(e)) 
            raise Exception (msg) 

#
#} end of executeSql def 
#


def main():

    sys.tracebacklimit = 0

    parser = argparse.ArgumentParser(description='runQuery')

    parser.add_argument('--dsn',   required=True, \
        help='Oracle database (DSN) to use.')
    parser.add_argument('--table', required=True, \
        help='Database table to query.')
    parser.add_argument('--sql',   required=True, \
        help='SQL SELECT statement.')
    parser.add_argument('--configpath', required=True, \
        help='Configuration file specifying DSN userid, passwd, and server.')
    parser.add_argument('--workdir', required=True, \
        help='workdir for the result.')
    parser.add_argument('--debug', required=False, \
        help='debugflag: 1/0.')

    args = parser.parse_args()

    dsn_in = ''
    table = ''
    sql   = ''
    configpath = ''
    workdir = ''
    debug = 0

    dsn_in = args.dsn
    table = args.table
    sql   = args.sql
    configpath = args.configpath
    workdir = args.workdir
    debug = args.debug

#    print (f'dsn_in= {dsn_in:s}')
#    print (f'table= {table:s}')
#    print (f'sql= {sql:s}')
#    print (f'configpath= {configpath:s}')

#    if (debug is not None):
#        print (f'debug= {debug:s}')
#    else:
#        print ('debug is None')

    confobj = configobj.ConfigObj (configpath)

    dsn = ''
    if dsn_in.upper() in confobj:
        dsn = dsn_in.upper()
    elif dsn_in.lower() in confobj:
        dsn = dsn_in.lower()

#    print (f'dsn= {dsn:s}')

    userid = ''
    if ('UserID' in confobj[dsn]):
        userid = confobj[dsn]['UserID']
#    print (f'userid= {userid:s}')
    
    password = ''
    if ('Password' in confobj[dsn]):
        password = confobj[dsn]['Password']
#    print (f'password= {password:s}')
    
    dbserver = ''
    if ('ServerName' in confobj[dsn]):
        dbserver = confobj[dsn]['ServerName']
#    print (f'dbserver= {dbserver:s}')

    racol = ''
    if ('RACOL' in confobj['webserver']):
        racol = confobj['webserver']['RACOL']
#    print (f'racol= {racol:s}')

    deccol = ''
    if ('DECCOL' in confobj['webserver']):
        deccol = confobj['webserver']['DECCOL']
#    print (f'deccol= {deccol:s}')
    
    query = None 
    try:
#        query = runQuery (dbserver=dbserver, \
#            userid=userid, \
#            password=password, \
#            dbtable=table, \
#            query=sql, \
#            workdir=workdir, \
#            debug=debug)
        
        query = runQuery (dbserver=dbserver, \
            userid=userid, \
            password=password, \
            dbtable=table, \
            query=sql, \
            workdir=workdir, \
            racol=racol, \
            deccol=deccol, \
            format='ipac', \
            maxrec='10000', \
            debug=debug)
        
        print (query.returnMsg)

    except Exception as e:
            
        print (f'runQuery failed: {str(e):s}')



if __name__ == "__main__":
    main()




