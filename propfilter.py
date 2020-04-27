import sys
import os
import io
import logging

import time
import datetime

import itertools
import cx_Oracle
import argparse
import configobj

from writeresult import writeResult
from datadictionary import dataDictionary
from tablenames import TableNames


class propFilter:

    pid = os.getpid()
    
    debug = 0
    debugtime = 0

    status = ''
    msg = ''

    cookiestr = ''
    userid = ''
    encodedpass = ''

    cookiename = ''
    accesstbl = ''
    usertbl = ''
    
    propfilter = ''

    conn = None
    dd = None

    dbtable = ''
    ddtable = ''
    instrument = ''
    datalevel = ''

    nfetch = 1000
    ninsert = 1000

    racol = ''
    deccol = ''

    userworkdir = ''
    outpath = ''
    inpath = ''

    format = 'votable'
    maxrec = -1
    coldesc = 0

    nrec = 0
    ntot = 0

    nrows_in = 0
    ncols_in = 0
    colnames_in = None
    query_in = ''
    query = ''
    
    selectstr = ''
    wherestr = ''
    orderbystr = ''
    groupbystr = ''
    
    selectcols = []
    orderbycols = []
    groupbycols = []

    time0 = None 
    time1 = None
    delt = 0.0
    
    def __init__(self, **kwargs):
#
#{
#
        """
        propFilter filters out the proprietary data from the input tabe.
	
	Required keyword parameters:

            dbserver:   database server,
            
            dbuser:     database server's userid,
            
            password:   database server's password,
          
            query (char): user query 
            
            workdir (char): user work directory  
	
	Optional keyword parameters (for accessing proprietary data):
   
	    cookiename (char):  cookie name for the HTTP server,
	    
	    cookiestr (char):   cookie string extracted from input HTTP cookie 
	                        containing KOA userid and encoded password,

            usertbl (char):     DB table containing the userid and 
	                        encoded password
	
            accesstbl (char):   DB table containing the user access info 
	
            accessid (char):   column name in accesstbl represent the user 
	                       access info  
	
            fileid (char):     column name in result represent the unique
	                       filename 

            format (char):     output format (default votable),

            maxrec (int):      default -1 meaning return all records,

            racol (char):      RA column name,
            
            deccol (char):     Dec column name, 

	Usage:

            pfilter = propFilter (dbserver=config.dbserver, \
                                  userid=config.dbuser, \
                                  password=config.dbpassword, \
		                  query=query,
		                  workdir= userworkdir, 
                                  racol=config.racol, \
                                  deccol=config.deccol, \
				  cookiename=cookiename, 
		                  cookiestr= cookiestr, 
	                          usertbl=usertbl, 
		                  accesstbl=accesstbl, 
	                          fileid=fileid, 
		                  accessid=accessid,
				  format=format,
                                  maxrec=maxrec)

        """

        if ('debug' in kwargs):
            self.debug = kwargs['debug']
        
        if ('debugtime' in kwargs):
            self.debugtime = kwargs['debugtime']
        
        if self.debug:
            logging.debug ('')
            logging.debug (f'Enter propFilter.init')
        
        if self.debugtime:
            self.time0 = datetime.datetime.now()
            logging.debug (f'Enter propFilter.init')

#
#    get input parameters
#
        self.dbserver = ''
        if ('dbserver' in kwargs):
            self.dbserver  = kwargs['dbserver']

        if (len(self.dbserver) == 0):
            self.msg = 'Failed to retrieve required input parameter [dbserver]'
            self.status = 'error'
            raise Exception (self.msg) 

        self.dbuser = ''
        if ('userid' in kwargs):
            self.dbuser  = kwargs['userid']

        if (len(self.dbuser) == 0):
            self.msg = 'Failed to retrieve required input parameter [userid]'
            self.status = 'error'
            raise Exception (self.msg) 

        self.dbpassword = ''
        if ('password' in kwargs):
            self.dbpassword  = kwargs['password']

        if (len(self.dbpassword) == 0):
            self.msg = 'Failed to retrieve required input parameter [password]'
            self.status = 'error'
            raise Exception (self.msg) 
        
        if self.debug:
            logging.debug ('')
            logging.debug (f'dbuser= {self.dbuser:s}')
            logging.debug (f'dbpassword= {self.dbpassword:s}')
            logging.debug (f'dbserver= {self.dbserver:s}')

#        self.dbtable = ''
#        if ('dbtable' in kwargs):
#            self.dbtable  = kwargs['dbtable']

#        if (len(self.dbtable) == 0):
#            self.msg = 'Failed to retrieve required input parameter [dbtable]'
#            self.status = 'error'
#            raise Exception (self.msg) 

#        self.ddtable = ''
#        if ('ddtable' in kwargs):
#            self.ddtable  = kwargs['ddtable']

#        if (len(self.ddtable) == 0):
#            self.ddtable = self.dbtable + '_dd'

        self.userWorkdir = ''
        if ('workdir' in kwargs):
            self.userworkdir  = kwargs['workdir']

        if (len(self.userworkdir) == 0):
            self.msg = \
	        'Failed to retrieve required input parameter [userworkdir]'
            raise Exception (self.msg) 

        if self.debug:
            logging.debug ('')
#            logging.debug (f'dbtable= {self.dbtable:s}')
#            logging.debug (f'ddtable= {self.ddtable:s}')
            logging.debug (f'userworkdir= {self.userworkdir:s}')
        
#        self.outpath = ''
#        if ('outpath' in kwargs):
#            self.outpath  = kwargs['outpath']

#        if (len(self.outpath) == 0):
#            self.outpath = self.userworkdir + '/query.tbl'

#        if self.debug:
#            logging.debug ('')
#            logging.debug (f'outpath= {self.outpath:s}')


        self.cookiename = ''
        if ('cookiename' in kwargs):
            self.cookiename  = kwargs['cookiename']

        self.cookiestr = ''
        if ('cookiestr' in kwargs):
            self.cookiestr  = kwargs['cookiestr']

        self.propfilter = ''
        if ('propfilter' in kwargs):
            self.propfilter  = kwargs['propfilter']

        self.usertbl = ''
        if ('usertbl' in kwargs):
            self.usertbl  = kwargs['usertbl']

        self.accesstbl = ''
        if ('accesstbl' in kwargs):
            self.accesstbl  = kwargs['accesstbl']

        self.usertbl = ''
        if ('usertbl' in kwargs):
            self.usertbl  = kwargs['usertbl']

        self.accessid = ''
        if ('accessid' in kwargs):
            self.accessid  = kwargs['accessid']

        self.fileid = ''
        if ('fileid' in kwargs):
            self.fileid  = kwargs['fileid']

        self.fileid_allowed = self.fileid + '_allowed'

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

        self.query_in = ''
        if ('query' in kwargs):
            self.query_in  = kwargs['query']

        if (len(self.query_in) == 0):
            self.msg = 'Failed to retrieve required input parameter [query]'
            raise Exception (self.msg) 

        if self.debug:
            logging.debug ('')
            logging.debug (f'cookiestr= {self.cookiestr:s}')
            logging.debug (f'usertbl= {self.usertbl:s}')
            logging.debug (f'accesstbl= {self.accesstbl:s}')
#            logging.debug (f'dbtable= {self.dbtable:s}')
            logging.debug (f'fileid= {self.fileid:s}')
            logging.debug (f'fileid_allowed= {self.fileid_allowed:s}')
            logging.debug (f'accessid= {self.accessid:s}')
            logging.debug (f'propfilter= {self.propfilter:s}')

        if ('format' in kwargs):
            self.format = kwargs['format']

        if ('maxrec' in kwargs):
            maxrecstr = kwargs['maxrec']
            
            try:
                self.maxrec = int(maxrecstr)
            
            except Exception as e:
                
                self.msg = "Failed to convert input maxrec value [" + \
                    maxrecstr + "] to integer."
                raise Exception (self.msg)

        if self.debug:
            logging.debug ('')
            logging.debug (f'format= {self.format:s}')
            logging.debug (f'maxrec= {self.maxrec:d}')

       
#        self.query = self.query_in.lower()
        self.query = self.query_in


        if self.debugtime:
            self.time1 = datetime.datetime.now()
            self.delt = (self.time1 - self.time0).total_seconds()
            self.time0 = self.time1
            logging.debug ('')
            logging.debug (f'time (retrieve input params): {self.delt:f}')

#        
#    Connect to Oracle
#
        if self.debugtime:
            self.time0 = datetime.datetime.now()
        
        try:
            self.conn = cx_Oracle.connect (\
                self.dbuser, self.dbpassword, self.dbserver)
        
        except Exception as e:
              
            self.status = 'error'
            self.msg = 'Failed to connect to cx_Oracle'
        
            raise Exception (self.msg) 

        if self.debug:
            logging.debug ('')
            logging.debug ('connected to Oracle')

        if self.debugtime:
            self.time1 = datetime.datetime.now()
            self.delt = (self.time1 - self.time0).total_seconds()
            logging.debug ('')
            logging.debug (f'time (connect to oracle server): {self.delt:f}')

#
#    Use Oracle function to check query syntax 
#
        if self.debugtime:
            self.time0 = datetime.datetime.now()
        
        try:
            if self.debug:
                logging.debug ('')
                logging.debug (f'call parseSql:')
           
            
            cursor = self.conn.cursor()

            self.__parseSql__ (cursor, self.query)

            if self.debug:
                logging.debug ('')
                logging.debug (f'returned parseSql:')
            
        except Exception as e:
                
            errmsg = f'Input query [{self.query:s}] syntax error: {str(e):s}'
            
            if self.debug:
                logging.debug ('')
                logging.debug (f'Exception __parseSql: {str(e):s}')
          
            self.__encodeSqlerrmsg__ (errmsg)

            raise Exception (self.msg) 

#
#    get dbtable name
#
        if self.debug:
            logging.debug ('')
            logging.debug (f'extract dbtable from TableNames class')

        self.dbtable = ''
        self.ddtable = ''

        tn = TableNames()
        tables = tn.extract_tables(self.query)

        if len(tables) > 0:
            self.dbtable = tables[0]

        if (len(self.dbtable) > 0):
            self.ddtable = self.dbtable + '_dd'

        if self.debug:
            logging.debug ('')
            logging.debug (f'dbtable= [{self.dbtable:s}]')
            logging.debug (f'ddtable= {self.ddtable:s}')

        
#
#    parse query: to extract query pieces for propfilter  
#

        if self.debugtime:
            self.time0 = datetime.datetime.now()
       
        if self.debug:
            self.__parseQuery__ (self.query, debug=1)
        else:
            self.__parseQuery__ (self.query)

        if self.debug:
            logging.debug ('')
            logging.debug (f'returned parseQuery:')
            logging.debug (f'orderbystr= {self.orderbystr:s}')
            logging.debug (f'groupbystr= {self.groupbystr:s}')
            logging.debug (f'wherestr= {self.wherestr:s}')
            logging.debug (f'selectstr= {self.selectstr:s}')

        if self.debugtime:
            self.time1 = datetime.datetime.now()
            self.delt = (self.time1 - self.time0).total_seconds()
            logging.debug ('')
            logging.debug (f'time (parseQuery): {self.delt:f}')

#
#    validate user
#
        ind = -1
        if ((len(self.cookiestr) > 0) and (len(self.cookiename) > 0)):
            ind = self.cookiestr.find (self.cookiename)
	
        if (ind != -1):
            
            if self.debugtime:
                self.time0 = datetime.datetime.now()
            
            try:
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'call validateUser')
	   
                if self.debug:
                    self.__validateUser__ (self.cookiename, self.cookiestr, \
	      	        self.propfilter, self.usertbl, debug=1)
                else:
                    self.__validateUser__ (self.cookiename, self.cookiestr, \
                        self.propfilter, self.usertbl)

        
                if self.debug:
                    logging.debug ('')
                    logging.debug ('returned validateUser')
                    logging.debug (f'userid= {self.userid:s}')
	    
            except Exception as e:
        
                self.msg = 'Failed to validate user: ' + str(e)
            
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'{self.msg:s}')
	    
                raise Exception (self.msg)

            if self.debugtime:
                self.time1 = datetime.datetime.now()
                self.delt = (self.time1 - self.time0).total_seconds()
                logging.debug ('')
                logging.debug (f'time (validateUser): {self.delt:f}')

        if self.debug:
            logging.debug ('')
            logging.debug (f'final userid= {self.userid:s}')
	    

#
#    retrieve data dictionary
#
        if self.debug:
            logging.debug ('')
            logging.debug (f'self.ddtable= {self.ddtable:s}')
            logging.debug ('call dataDictionry')

        if self.debugtime:
            self.time0 = datetime.datetime.now()
        
        self.dd = None 
        try:
            if self.debug:
                self.dd = dataDictionary (self.conn, self.dbtable, debug=1)   
            else:
                self.dd = dataDictionary (self.conn, self.dbtable)   

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
            self.time1 = datetime.datetime.now()
            self.delt = (self.time1 - self.time0).total_seconds()
            logging.debug ('')
            logging.debug (f'time (retrieve DD: {self.delt:f}')


#
#    create tmp_accessiddbtbl
#
        if self.debug:
            logging.debug ('')
            logging.debug (f'userid= {self.userid:s}')

        tmp_accessiddbtbl = 'tmp_' + self.accessid + str(os.getpid())
#        tmp_accessiddbtbl = 'tmp_' + self.accessid
            
        if self.debug:
            logging.debug ('')
            logging.debug (f'tmp_accessiddbtbl= {tmp_accessiddbtbl:s}')
            
        if (len(self.userid) > 0):
#
# { 
#
            if self.debugtime:
                self.time0 = datetime.datetime.now()

            try:
                if self.debug:
                    logging.debug ('')
                    logging.debug ('call createTmpAccessiddb')
           
                if self.debug:
                    self.__createTmpAccessiddb__ (tmp_accessiddbtbl, \
                        self.userid, self.accessid, self.accesstbl, debug=1)
                else:
                    self.__createTmpAccessiddb__ (tmp_accessiddbtbl, \
                        self.userid, self.accessid, self.accesstbl)
        
                if self.debug:
                    logging.debug ('')
                    logging.debug ('returned createTmpAccessiddb')
                    logging.debug ('here0: returned createTmpAccessiddb')
	    
	    
            except Exception as e:
        
                self.msg = 'Failed to create tmp_accessiddbtbl: ' + str(e)
            
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'{self.msg:s}')
	    
                raise Exception (self.msg)

            if self.debugtime:
                self.time1 = datetime.datetime.now()
                self.delt = (self.time1 - self.time0).total_seconds()
                logging.debug ('')
                logging.debug (f'time (createTmpAccessiddb): {self.delt:f}')
    
            if self.debug:
                logging.debug ('')
                logging.debug ('{tmp_accessiddbtbl created}')
#
#}   End create tmp_accessiddbtbl block: only used if userid exists 
#

#
#    create tmp_fileidAlloweddbtbl
#
        tmp_fileidAlloweddbtbl = 'tmp_fileidallowed' + str(os.getpid())
        if self.debug:
            logging.debug ('')
            logging.debug (\
		f'tmp_fileidAllowddbtbl= {tmp_fileidAlloweddbtbl:s}')
    
        if self.debugtime:
            self.time0 = datetime.datetime.now()
        
        try:
            if self.debug:
                logging.debug ('')
                logging.debug ('call createTmpFileiddb')
                logging.debug (f'wherestr= {self.wherestr:s}')
	   
            if self.debug:
                self.__createTmpFileiddb__ (tmp_fileidAlloweddbtbl, \
                    self.fileid, self.fileid_allowed, self.dbtable, \
                    self.wherestr, self.accessid, tmp_accessiddbtbl, \
                    debug=1)
            else:
                self.__createTmpFileiddb__ (tmp_fileidAlloweddbtbl, \
                    self.fileid, self.fileid_allowed, self.dbtable, \
                    self.wherestr, self.accessid, tmp_accessiddbtbl)
                
            if self.debug:
                logging.debug ('')
                logging.debug ('returned createTmpFileiddb: tmp_fileidAlloweddbtbl')
	    
        except Exception as e:
        
            self.msg = str(e)
            
            if self.debug:
                logging.debug ('')
                logging.debug (f'{self.msg:s}')
	    
            raise Exception (self.msg)

        if self.debugtime:
            self.time1 = datetime.datetime.now()
            self.delt = (self.time1 - self.time0).total_seconds()
            logging.debug ('')
            logging.debug (f'time (createTmpFileiddb): {self.delt:f}')

        if self.debug:
            logging.debug ('')
            logging.debug ('{tmp_fileidAlloweddbtbl created}')


        if self.debugtime:
            self.time0 = datetime.datetime.now()

#
#    construct the final select statement for table joint
#

#        selectstr = 'select '
#        len_selectcols = len (self.selectcols)

#        for i in range (0, len_selectcols):
#            selectstr = selectstr + 'a.' + self.selectcols[i] + ', '

#        selectstr = selectstr + 'b.' + self.fileid_allowed

#        if self.debug:
#            logging.debug ('')
#            logging.debug (f'selectstr= {selectstr:s}')
       
#        sql = selectstr + " from " + self.dbtable + " a, " + \
#            tmp_fileidAlloweddbtbl + " b where a." + self.fileid + \
#            "=b." + self.fileid_allowed  


        sql = self.selectstr + " from " + self.dbtable + \
             " where " + self.fileid + " in (select " + self.fileid_allowed + \
             " from " + tmp_fileidAlloweddbtbl + ")"

        if self.debug:
            logging.debug ('')
            logging.debug (
                f'select (before adding groupby and orderby: sql= {sql:s}')
            logging.debug ( f'groupbystr= {self.groupbystr:s}')
            logging.debug ( f'orderbystr= {self.orderbystr:s}')

#
#    add group by clause
#
        if (len(self.groupbystr) > 0):
            
            groupbystr = 'group by '
            len_groupbycols = len (self.groupbycols)

            for i in range (0, len_groupbycols):
                
                if (i == len_groupbycols-1):

#                    groupbystr = groupbystr + 'a.' + \
#                        self.groupbycols[i]
                    
                    groupbystr = groupbystr + self.groupbycols[i]
                else:
#                    groupbystr = groupbystr + 'a.' + \
#                        self.groupbycols[i] + ', '
                    
                    groupbystr = groupbystr + self.groupbycols[i] + ', '

            sql = sql + " " + groupbystr

        if self.debug:
            logging.debug ('')
            logging.debug (f'groupby added: sql= {sql:s}')
       
#
#    add order by clause
#
        if (len(self.orderbystr) > 0):
            
            orderbystr = 'order by '
            len_orderbycols = len (self.orderbycols)

            for i in range (0, len_orderbycols):
                
                if (i == len_orderbycols-1):

#                    orderbystr = orderbystr + 'a.' + \
#                        self.orderbycols[i]

                     orderbystr = orderbystr + self.orderbycols[i]
                else:
#                    orderbystr = orderbystr + 'a.' + \
#                        self.orderbycols[i] + ', '

                    orderbystr = orderbystr + self.orderbycols[i] + ', '

            sql = sql + " " + orderbystr

        if self.debug:
            logging.debug ('')
            logging.debug (f'orderby added: sql= {sql:s}')
       
        cursor = self.conn.cursor()

        try:
            if self.debug:
                self.__executeSql__ (cursor, sql, debug=1)
            else:
                self.__executeSql__ (cursor, sql)

        except Exception as e:
        
            self.msg = 'Failed to execute [' + sql + ']: ' + str(e)
            
            if self.debug:
                logging.debug ('')
                logging.debug (f'{self.msg:s}')
	   
            raise Exception (self.msg) 

        if self.debugtime:
            self.time1 = datetime.datetime.now()
            self.delt = (self.time1 - self.time0).total_seconds()
            logging.debug ('')
            logging.debug (f'time (perform joint statement): {self.delt:f}')

        if self.debug:
            logging.debug ('')
            logging.debug ('join select statement executed')


        ncol = len (cursor.description)
        exclcol = ncol - 1

        if self.debug:
            logging.debug ('')
            logging.debug (f'ncol= {ncol:d} exclcol= {exclcol:d}')
   
#
#    write result to filteredpath 
#
        if self.debug:
            logging.debug ('')
            logging.debug ('call writeResultfile')
        
        if self.debugtime:
            self.time0 = datetime.datetime.now()

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

            if self.debug:
                logging.debug ('')
                logging.debug ('returned writeResultfile')
            
        except Exception as e:
        
            if self.debug:
                logging.debug ('')
                logging.debug (f'writeResult exception: {str(e):s}')
	    
            raise Exception (str(e))

        self.outpath = wresult.outpath
        self.ntot = wresult.ntot

#
#    drop all tmp DB tables
#
        try:
            self.__dropDbtbl__ (tmp_fileidalloweddbtbl)
        except Exception as e:
            pass

        if self.debug:
            logging.debug ('')
            logging.debug ('tmp_fileidalloweddbtbl dropped')

        if (len(self.userid) > 0):
            
            try:
                self.__dropDbtbl__ (tmp_accessiddbtbl)
            except Exception as e:
                pass

            if self.debug:
                logging.debug ('')
                logging.debug ('tmp_accessiddbtbl dropped')

        if self.debug:
            logging.debug ('')
            logging.debug ('done propFilter')
	   
        if self.debugtime:
            self.time1 = datetime.datetime.now()
            self.delt = (self.time1 - self.time0).total_seconds()
            logging.debug ('')
            logging.debug (f'time (writeResult joined table): {self.delt:f}')
            logging.debug (f'ntot= {self.ntot:d}')

        return

#
#} end of init def 
#


    def __parseQuery__ (self, query, **kwargs):
#
#    Assume that group by and order by cannot be in the same query
#
#{ 
#
        debug = 0 

        if ('debug' in kwargs):
            debug = kwargs['debug']

        if debug:
            logging.debug ('')
            logging.debug (f'Enter parseQuery: query= [{query:s}]')
            logging.debug (f'dbtable= [{self.dbtable:s}]')

        instrume = ["hires", \
                    "nirspec", \
                    "nirc2", \
                    "lris", \
                    "deimos", \
                    "mosfire", \
                    "osiris", \
                    "lws", \
                    "esi", \
                    "nirc", \
                    "kcwi", \
                    "nires", \
                    "nirc2"]

        level = ["l0", \
                 "l1", \
                 "l2", \
                 "eng"]

        selectstr = ''
        wherestr = '' 
        orderbystr = ''
        groupbystr = ''

#
#    extract select list from query
#
        ind = query.lower().find(' from ')

        substr = ''
        if (ind != -1):
            selectstr = query[0:ind]

        if debug:
            logging.debug ('')
            logging.debug (f'selectstr= [{selectstr:s}]')

#
#    retrieve instrument and datalevel from dbtable name: 
#    this only works for KOA and NEID because of their dbtable name 
#
        if (self.propfilter.lower() == 'koa'):
#
#{    retrieve instrument
# 
            ninst = len(instrume)

            if debug:
                logging.debug ('')
                logging.debug (f'ninst= {ninst:d}')

            self.instrument = ''
            for i in range (ninst):

                dbtable_lower = self.dbtable.lower()
	
                ind = dbtable_lower.find (instrume[i])
	
                if (ind != -1):
                    self.instrument = instrume[i]
                    break
    
            if debug:
                logging.debug ('')
                logging.debug (f'instrument= {self.instrument:s}')
#
#} end  retrieve instrument
# 
            ninst = len(instrume)

        
        if (self.propfilter.lower() == 'neid'):
#
#{    retrieve datalevel and modify fileid 
# 
            nlevel = len(level)

            if debug:
                logging.debug ('')
                logging.debug (f'nlevel= {nlevel:d}')

            self.datalevel = ''
            for i in range (nlevel):

                dbtable_lower = self.dbtable.lower()
	
                ind = dbtable_lower.find (level[i])
	
                if (ind != -1):
                    self.datalevel = level[i]
                    break
    
            if debug:
                logging.debug ('')
                logging.debug (f'datalevel= {self.datalevel:s}')

            if (self.datalevel == 'l0' or self.datalevel == 'eng'):
                
                self.fileid = 'l0' + self.fileid
                self.fileid_allowed = 'l0' + self.fileid_allowed
            else:
                self.fileid = self.datalevel.lower() + self.fileid
                self.fileid_allowed = self.datalevel.lower() + \
                    self.fileid_allowed
            
            if debug:
                logging.debug ('')
                logging.debug (f'fileid= {self.fileid:s}')
                logging.debug (f'fileid_allowed= {self.fileid_allowed:s}')
#
#}
#
#
#    extract where constraint, order by, and group by clause
#
        substr = ''
        ind = query.lower().find('where')
    
        if (ind != -1):
            substr = query[ind:]

        if debug:
            logging.debug ('')
            logging.debug (f'substr= [{substr:s}]')

     
        if (len(substr) > 0):
#
#{  where clause exists
#
            if debug:
                logging.debug ('')
                logging.debug ('xxx1')
       
            ind1 = -1
            ind2 = -1

            ind1 = substr.lower().find('order by')
            ind2 = substr.lower().find('group by')

            if debug:
                logging.debug ('')
                logging.debug (f'xxx1-1: ind1= {ind1:d} ind2= {ind2:d}')
        
            if ((ind1 >= 0) and (ind2 >= 0)):

                if debug:
                    logging.debug ('')
                    logging.debug ('xxx1-2')
        
                if (ind1 < ind2):
                    
                    if debug:
                        logging.debug ('')
                        logging.debug ('xxx1-3')
        
                    wherestr = substr[0:ind1]
                    orderbystr = substr[ind1:ind2]
                    groupbystr = substr[ind2:]
                else:
                    if debug:
                        logging.debug ('')
                        logging.debug ('xxx1-4')
        
                    wherestr = substr[0:ind2]
                    groupbystr = substr[ind2:ind1]
                    orderbystr = substr[ind1:]

            elif (ind1 >= 0):
                
                if debug:
                    logging.debug ('')
                    logging.debug ('xxx1-5')
            
                wherestr = substr[0:ind1]
                orderbystr = substr[ind1:]
            
            elif (ind2 >= 0):
            
                if debug:
                    logging.debug ('')
                    logging.debug ('xxx1-6')

                wherestr = substr[0:ind2]
                groupbystr = substr[ind2:]

            else:
                wherestr = substr

#
#}
#
        else:
#
#{  where clause doesn't exists
#
            ind1 = -1
            ind2 = -1
            ind1 = query.lower().find('order by')
            ind2 = query.lower().find('group by')

            if debug:
                logging.debug ('')
                logging.debug ('xxx2')
        
            ind = -1
            if ((ind1 >= 0) and (ind2 >= 0)):

                if debug:
                    logging.debug ('')
                    logging.debug ('xxx2-1')
        
                if (ind1 < ind2):
                    
                    if debug:
                        logging.debug ('')
                        logging.debug ('xxx2-2')
        
                    orderbystr = query[ind1:ind2]
                    groupbystr = query[ind2:]
                
                else:
                    if debug:
                        logging.debug ('')
                        logging.debug ('xxx2-3')
        
                    groupbystr = query[ind2:ind1]
                    orderbystr = query[ind1:]

            elif (ind1 >= 0):
            
                if debug:
                    logging.debug ('')
                    logging.debug ('xxx 2-4')
            
                orderbystr = query[ind1:]
            
            elif (ind2 >= 0):
        
                if debug:
                    logging.debug ('')
                    logging.debug ('xxx 2-5')

                groupbystr = query[ind2:]
            
#
#} end where clause doesn't exist
#
        self.selectstr = selectstr
        self.wherestr = wherestr
        self.orderbystr = orderbystr
        self.groupbystr = groupbystr

        if debug:
            logging.debug ('')
            logging.debug (f'self.selectstr= {self.selectstr:s}')
            logging.debug (f'self.wherestr= {self.wherestr:s}')
            logging.debug (f'self.orderby= {self.orderbystr:s}')
            logging.debug (f'self.groupby= {self.groupbystr:s}')

#
#    retrieve selectcols
#
        """
        if debug:
            logging.debug ('')
            logging.debug (f'retrieve selectcols')
            logging.debug (f'selectstr= {selectstr:s}')

        str1 = selectstr[7:].strip()
        
        if debug:
            logging.debug ('')
            logging.debug (f'str1= {str1:s}')

        while (len(str1) > 0):
        
            ind = str1.find (',')

            col = ''
            if (ind != -1):
                col = str1[0:ind].strip()
                str1 = str1[ind+1:] 
            else:
                col = str1.strip()
                str1 = ''
        
            if debug:
                logging.debug ('')
                logging.debug (f'col= {col:s}')
                logging.debug (f'str1= {str1:s}')
            
            self.selectcols.append (col)

            if (len(str1) == 0):
                break

        if debug:
            logging.debug ('')
            logging.debug (f'len(selectcols)= {len(self.selectcols):d}')
            logging.debug (self.selectcols)
        """

#
#    retrieve groupbycols
#
        if debug:
            logging.debug ('')
            logging.debug (f'retrieve groupbycols')
            logging.debug (f'groupbystr= {groupbystr:s}')

        ind = groupbystr.lower().find('by ')
        str1 = groupbystr[ind+3:].strip()
       
        while (len(str1) > 0):
        
            ind = str1.find (',')
            col = ''
            if (ind != -1):
                col = str1[0:ind].strip()
                str1 = str1[ind+1:] 
            else:
                col = str1.strip()
                str1 = ''
        
            if debug:
                logging.debug ('')
                logging.debug (f'col= {col:s}')
                logging.debug (f'str1= {str1:s}')
                
            self.groupbycols.append (col)
            
            if (len(str1) == 0):
                break

        if debug:
            logging.debug ('')
            logging.debug (f'len(groupbycols)= {len(self.groupbycols):d}')
            logging.debug (self.groupbycols)

#
#    retrieve orderbycols
#
        if debug:
            logging.debug ('')
            logging.debug (f'retrieve orderbycols')
            logging.debug (f'orderbystr= {orderbystr:s}')

        ind = orderbystr.lower().find('by ')
        str1 = orderbystr[ind+3:].strip()
       
        while (len(str1) > 0):
        
            ind = str1.find (',')
            col = ''
            if (ind != -1):
                col = str1[0:ind].strip()
                str1 = str1[ind+1:] 
            else:
                col = str1.strip()
                str1 = ''
        
            if debug:
                logging.debug ('')
                logging.debug (f'col= {col:s}')
                logging.debug (f'str1= {str1:s}')
                
            self.orderbycols.append (col)
            
            if (len(str1) == 0):
                break

        if debug:
            logging.debug ('')
            logging.debug (f'len(orderbycols)= {len(self.orderbycols):d}')
            logging.debug (self.orderbycols)

        return
#
#} end of parseQuery def 
#


    def __validateUser__ (self, cookiename, cookiestr, propfilter, \
        usertbl, **kwargs):
#
#{ 
#
        debug = 0 

        if ('debug' in kwargs):
            debug = kwargs['debug']

        if debug:
            logging.debug ('')
            logging.debug (f'Enter validateUser: cookiename= [{cookiename:s}]')
            logging.debug (f'cookiestr= {cookiestr:s}')
            logging.debug (f'usertbl= {usertbl:s}')

#
#    if cookiestr exists: validate userid/encodedpass 
#
        msg = ''
        name = ''
        ind = cookiestr.find (cookiename)
        
        if debug:
            logging.debug ('')
            logging.debug (f'ind= {ind:d}')

        substr = ''
        if (ind != -1):
            substr = cookiestr[ind:]
        else:
            msg = 'Failed to find cookiename: [{cookiename:s}] in cookiestr'
            raise Exception (msg) 

        if debug:
            logging.debug ('')
            logging.debug (f'substr= {substr:s}')

#
#    separate cookiestr from cookiename
#
        substr1 = ''
        ind = substr.find ('=')
        if (ind != -1):
            substr1 = substr[ind+1:]
            
        if debug:
            logging.debug ('')
            logging.debug (f'substr1= {substr1:s}')

#
#    empty cookie str is OK, treat it as anonymous user
#
        if (len(substr1) == 0):
            return    

        arr = substr1.split ('|')
        narr = len (arr)

        if debug:
            logging.debug ('')
            logging.debug (f'narr= {narr:d}')

        for i in range (0, narr):

            if debug:
                logging.debug ('')
                logging.debug (f'= {i:d} arr= {arr[i]:s}')

        self.userid = arr[0] 
        self.encodedpass = arr[1] 

        if debug:
            logging.debug ('')
            logging.debug (f'userid= {self.userid:s}')
            logging.debug (f'encodedpass= {self.encodedpass:s}')

        if (self.userid == 'anon'):
            self.userid = ''
            return    

#
#    validate userid/encodedpass with the data in usertbl
#
        cursor = self.conn.cursor()

        sql = ''
        if (propfilter == 'koa'):

            sql = "select passwd from " + usertbl + \
                " where userid='" + self.userid + "'"  
        
        elif (propfilter == 'neid'):
            sql = "select password from " + usertbl + \
                " where userid='" + self.userid + "'"  

        if debug:
            logging.debug ('')
            logging.debug (f'sql= {sql:s}')

        try:
            self.__executeSql__ (cursor, sql)

        except Exception as e:
        
            self.msg = 'Failed to execute [' + sql + ']: ' + str(e)
            
            if debug:
                logging.debug ('')
                logging.debug (f'{self.msg:s}')
	    
            raise Exception (self.msg)

        if debug:
            logging.debug ('')
            logging.debug ('returned executeSql')
            logging.debug ('call singleValueResult')

        password = ''       
        try:

            if (propfilter == 'koa'):
                password = self.__singleValueResult__ (cursor, 'passwd')

            elif (propfilter == 'neid'):
        
                if debug:
                    logging.debug ('')
                    logging.debug ('neid: call singleValueResult:')
               
                if debug:
                    password = self.__singleValueResult__ (cursor, 'password', \
                        debug=1)
                else:
                    password = self.__singleValueResult__ (cursor, 'password')

        
                if debug:
                    logging.debug ('')
                    logging.debug ('returned singleValueResult')
                    logging.debug (f'password = {password:s}')

        except Exception as e:
        
            self.msg = 'Failed to retrieve singleValueResult: ' + str(e)
            
            if debug:
                logging.debug ('')
                logging.debug (f'{self.msg:s}')
	    
            raise Exception (self.msg)

        if debug:
            logging.debug ('')
            logging.debug (f'password = {password:s}')
        
        if (len(password) == 0):
            
            self.msg = 'Failed to validate user: cannot find userid: ' \
	        + self.userid  + ' in user table.'
            raise Exception (self.msg)
        
        if (password != self.encodedpass):
            
            self.msg = 'Incorrect password for the user: ' + self.userid
            raise Exception (self.msg)

        self.status = 'ok'
        return

#
#} end of validateUser def 
#


    def __singleValueResult__ (self, cursor, keyword, **kwargs):
#
#{ 
#
        debug = 0 

        if ('debug' in kwargs):
            debug = kwargs['debug']

        if debug:
            logging.debug ('')
            logging.debug (f'Enter singleValueResult: keyword= {keyword:s}')

#
#    retrieve single row data containing single column password from cursor
#
        ncol = len (cursor.description)

        if debug:
            logging.debug ('')
            logging.debug (f'ncol= {ncol:d}')

        
        rows = cursor.fetchmany()
        nrec = len(rows)

        if debug:
            logging.debug ('')
            logging.debug (f'nrec= {nrec:d}')
            logging.debug (f'arraysize= {cursor.arraysize:d}')

        keyval = ''
        row = 0
        for col in range(ncol):
                    
            colname = cursor.description[col][0]
            
            if debug:
                logging.debug ('')
                logging.debug (f'colname= {colname:s}')
        
            val = rows[row][col]
                    
            valstr = ''
            if val is None:
                valstr = ''
            else:
                valstr = str(val)
       
            if debug:
                logging.debug ('')
                logging.debug (f'colname= {colname:s}')
                logging.debug (f'valstr= {valstr:s}')
        
            if (colname.lower() == keyword):
                keyval = valstr

        if debug:
            logging.debug ('')
            logging.debug (f'keyval= {keyval:s}')
        
        return (keyval)

#
#} end of singleValueResult def 
#


    def __writeSinglecolResult__ (self, cursor, outpath, colwidth, **kwargs):
#
#{ 
#
        debug = 0
    
        if ('debug' in kwargs):
            debug = kwargs['debug']

        if debug:
            logging.debug ('')
            logging.debug ('Enter writeSinglecolResult')
            logging.debug (f'outpath= {outpath:s}')
            logging.debug (f'colwidth= {colwidth:d}')
            
#
#    open outpath for write 
#
        fp = None
        try:
            fp = open (outpath, 'w')
        except Exception as e:

            msg = 'Failed to open output file [' + outpath + ']'
	    
            if debug:
                logging.debug ('')
                logging.debug (f'errmsg= {msg:s}')
                logging.debug (f'str(e)= {str(e):s}')
           
            raise Exception (msg)

        if debug:
            logging.debug ('')
            logging.debug (f'{outpath:s} opened for write')

#
#    write header line
#
        fmt = str(colwidth) + 's'  
        
        line = ''
        name = '' 
        for col in cursor.description:
            name = col[0].lower()

        line = f'|{name:{fmt}}|'
        line = line + '\n'
        fp.write(line)
        fp.flush()

        dtype = 'char'
        line = f'|{dtype:{fmt}}|'
        line = line + '\n'
        fp.write(line)
        fp.flush()

        unit = ' '
        line = f'|{unit:{fmt}}|'
        line = line + '\n'
        fp.write(line)
        fp.flush()

        nuls = 'null'
        line = f'|{nuls:{fmt}}|'
        line = line + '\n'
        fp.write(line)
        fp.flush()

#
#    retrieve single column data from cursor
#
        ncol = len (cursor.description)
        cursor.arraysize = 10000 

        if debug:
            logging.debug ('')
            logging.debug (f'ncol= {ncol:d}')
            logging.debug (f'arraysize= {cursor.arraysize:d}')

        ntot = 0
        passwd = ''

        while True:
        
            rows = cursor.fetchmany()
        
            nrec = len(rows)

            ntot = ntot + nrec
        
            if debug:
                logging.debug ('')
                logging.debug (f'nrec= {nrec:d}')
                logging.debug (f'ntot= {ntot:d}')

            i = 0
            for row in range(nrec):
        
                for col in range(ncol):
                    
#                    colname = cursor.description[col][0]
                    
                    val = rows[row][col]
                    
                    valstr = ''
                    if val is None:
                        valstr = ''
                    else:
                        valstr = str(val)
       
                    line = f' {valstr:{fmt}} '
                    line = line + '\n'
                    fp.write(line)
                    fp.flush()

            if len(rows) < cursor.arraysize:
                break
         
        fp.close() 

        if debug:
            logging.debug ('')
            logging.debug ('Done writeSinglecolResult')
        return
#
#} end of writeSinglecolResult def 
#


    def __createTmpAccessiddb__ (self, tmp_accessiddbtbl, userid, accessid, \
        accesstbl, **kwargs):
#
#{ 
#
        debug = 0

        if ('debug' in kwargs):
            debug = kwargs['debug']

        if debug:
            logging.debug ('')
            logging.debug ('Enter createTmpAccessiddb')
            logging.debug (f'tmp_accessiddbtbl= {tmp_accessiddbtbl:s}')
            logging.debug (f'userid= {userid:s}')
            logging.debug (f'accesstbl= {accesstbl:s}')

#
#    create tmp_accessiddbtbl, but first drop tmp_accessiddbtbl just in case 
#    it might already existed
#
        if self.debugtime:
            self.time0 = datetime.datetime.now()

        try: 
            self.__dropDbtbl__ (tmp_accessiddbtbl)

        except Exception as e:
        
            self.msg = 'Failed to create tmp_accessiddbtbl: ' + str(e)
            if debug:
                logging.debug ('')
                logging.debug (f'{self.msg:s}')
            pass

        
        sql = "create global temporary table " + tmp_accessiddbtbl + \
            " (" + accessid + " varchar(22)) on commit preserve rows"
        
        if debug:
            logging.debug ('')
            logging.debug (f'sql= {sql:s}')

        cursor = self.conn.cursor()
        try: 
            self.__executeSql__ (cursor, sql)
	    
            if debug:
                logging.debug ('')
                logging.debug (f'returned from executeSql: {sql:s}')

        except Exception as e:
        
            self.msg = 'Failed to create tmp_accessiddbtbl: ' + str(e)
            if debug:
                logging.debug ('')
                logging.debug (f'{self.msg:s}')
            raise Exception (self.msg)

        if debug:
            logging.debug ('')
            logging.debug ('tmp_accessiddbtbl created')
         
#        if self.debugtime:
#            self.time1 = datetime.datetime.now()
#            self.delt = (self.time1 - self.time0).total_seconds()
#            self.time0 = self.time1
#            if debug:
#                logging.debug ('')
#                logging.debug (f'time (createTmpAccessiddb): {self.delt:f}')


#
#    insert into tmp_accessiddbtbl: select accessid allowed by userid: accessidtbl 
#
        sql = "insert into " + tmp_accessiddbtbl + \
            " (select lower(" + accessid + ") as " + accessid + \
	    " from " + accesstbl + " where userid = '" + userid + "')"
        
        if debug:
            logging.debug ('')
            logging.debug (f'sql= {sql:s}')
        
        cursor = self.conn.cursor()
        try: 
            self.__executeSql__ (cursor, sql)
	    
            if debug:
                logging.debug ('')
                logging.debug (f'returned from executeSql: {sql:s}')

        except Exception as e:
        
            self.msg = 'Failed to insert data toe tmp_accessiddbtbl: ' + str(e)
            if debug:
                logging.debug ('')
                logging.debug (f'{self.msg:s}')
	    
            raise Exception (self.msg)

        if debug:
            logging.debug ('')
            logging.debug ('insert tmp_accessiddbtbl statement executed')

#        if self.debugtime:
#            self.time1 = datetime.datetime.now()
#            self.delt = (self.time1 - self.time0).total_seconds()
#            self.time0 = self.time1
#            if debug:
#                logging.debug ('')
#                logging.debug (f'time (insert tmpAccessiddb): {self.delt:f}')

#
#    select accessid from tmp_iddbtbl: just to verify
#
        """    
        sql = "select * from " + tmp_accessiddbtbl
        
        if debug:
            logging.debug ('')
            logging.debug (f'sql= {sql:s}')
        
        cursor = self.conn.cursor()
        try: 
            self.__executeSql__ (cursor, sql)
	    
            if debug:
                logging.debug ('')
                logging.debug (f'returned from executeSql: {sql:s}')

        except Exception as e:
        
            self.msg = 'Failed to select from tmp_accessiddbtbl: ' + str(e)
            if debug:
                logging.debug ('')
                logging.debug (f'{self.msg:s}')
	    
            raise Exception (self.msg)

        if debug:
            logging.debug ('')
            logging.debug ('select id statement executed')

        accessidpath = self.userworkdir + '/accessid.tbl'
        
        if debug:
            logging.debug ('')
            logging.debug (f'accessidpath= {accessidpath:s}')

        try: 
            self.__writeSinglecolResult__ (cursor, accessidpath, 22)
	    
            if debug:
                logging.debug ('')
                logging.debug ('returned from writeSinglecolResult: {sql:s}')

        except Exception as e:
        
            self.msg = 'Failed to write result to accessidtbl: ' + str(e)
            if debug:
                logging.debug ('')
                logging.debug (f'{self.msg:s}')
	    
            raise Exception (self.msg)
        """    
        
        return

#
#} end of createTmpAccessiddb def 
#


    def __createTmpFileiddb__ (self, tmp_fileiddbtbl, fileid, fileid_allowed, \
        dbtable, wherestr, accessid, tmp_accessiddbtbl, **kwargs):
#
#{ 
#
        debug = 0 

        if ('debug' in kwargs):
            debug = kwargs['debug']

        if debug:
            logging.debug ('')
            logging.debug ('Enter createTmpFileiddb')
            logging.debug (f'tmp_fileiddbtbl= {tmp_fileiddbtbl:s}')
            logging.debug (f'fileid= {fileid:s}')
            logging.debug (f'fileid_allowed= {fileid_allowed:s}')
            logging.debug (f'dbtable= {dbtable:s}')
            logging.debug (f'accessid= {accessid:s}')
            logging.debug (f'tmp_accessiddbtbl= {tmp_accessiddbtbl:s}')
            logging.debug (f'wherestr= {wherestr:s}')
       
#
#    create tmp_fileiddbtbl, but first drop tmp_fileiddbtbl just in case 
#    it might already existed
#
        if self.debugtime:
            self.time0 = datetime.datetime.now()

        try: 
            self.__dropDbtbl__ (tmp_fileiddbtbl)

        except Exception as e:
        
            self.msg = 'Failed to create tmp_fileiddbtbl: ' + str(e)
            if debug:
                logging.debug ('')
                logging.debug (f'{self.msg:s}')
            pass

        
        sql = "create global temporary table " + tmp_fileiddbtbl + \
            " (" + fileid_allowed + " varchar(35)) on commit preserve rows"
        
        if debug:
            logging.debug ('')
            logging.debug (f'sql= {sql:s}')

        cursor = self.conn.cursor()
        try: 
            self.__executeSql__ (cursor, sql)
	    
            if debug:
                logging.debug ('')
                logging.debug (f'returned from executeSql: {sql:s}')

        except Exception as e:
        
            self.msg = 'Failed to create tmp_accessiddbtbl: ' + str(e)
            if debug:
                logging.debug ('')
                logging.debug (f'{self.msg:s}')
            raise Exception (self.msg)

        if debug:
            logging.debug ('')
            logging.debug ('tmp_fileididdbtbl created')
         
#        if self.debugtime:
#            self.time1 = datetime.datetime.now()
#            self.delt = (self.time1 - self.time0).total_seconds()
#            self.time0 = self.time1
#            if debug:
#                logging.debug ('')
#                logging.debug (f'time (create tmp_fileiddbtbl): {self.delt:f}')


#
#    insert into tmp_fileiddbtbl: select koaid_allowed from dbtable with
#    input where condition and the accessid constraint
#
#    KOA access_constraint 
#
        access_constraint = ''
        if (self.propfilter == 'koa'): 
                
            if debug:
                logging.debug ('')
                logging.debug ('construct koa access_constraint')
                logging.debug (f'instrument= {self.instrument:s}')
         
            if (self.instrument.lower() == 'hires'): 
                
                if (len(self.userid) > 0):
                
                    access_constraint = \
                        "((current_date > add_months (date_obs, propmin))" + \
                        " or (lower(" + accessid + ") in (select " + \
                        accessid + " from " + tmp_accessiddbtbl + ")))"

                else:
                    access_constraint = \
                        "(current_date > add_months(date_obs, propmin))"
            else:
                if (len(self.userid) > 0):
                
                    access_constraint = \
                        "((current_date > add_months (date_obs, propint))" + \
                        " or (lower(" + accessid + ") in (select " + \
                        accessid + " from " + tmp_accessiddbtbl + ")))"

                else:
                    access_constraint = \
                        "(current_date > add_months(date_obs, propint))"
            
            if debug:
                logging.debug ('')
                logging.debug (
                    f'koa: access_constraint= {access_constraint:s}')

#
#    NEID fileidallowed sql
#
        elif (self.propfilter == 'neid'):
            
            if debug:
                logging.debug ('')
                logging.debug ('construct neid access_constraint')
                logging.debug (f'datalevel= {self.datalevel:s}')
         
         
            if ((self.datalevel.lower() == 'l0') or \
                (self.datalevel.lower() == 'eng')): 
            
                if (len(self.userid) > 0):
                
                    access_constraint = \
                        "((current_date > add_months (obsdate, l0propint))" + \
                        " or (lower(" + accessid + ") in (select " + \
                        accessid + " from " + tmp_accessiddbtbl + ")))"
                else:
                    access_constraint = \
                        "(current_date > add_months(obsdate, l0propint))"
            
            elif (self.datalevel.lower() == 'l1'): 
            
                if (len(self.userid) > 0):
                
                    access_constraint = \
                        "((current_date > add_months (obsdate, l1propint))" + \
                        " or (lower(" + accessid + ") in (select " + \
                        accessid + " from " + tmp_accessiddbtbl + ")))"
                else:
                    access_constraint = \
                        "(current_date > add_months(obsdate, l1propint))"

            elif (self.datalevel.lower() == 'l2'): 
            
                if (len(self.userid) > 0):
                
                    access_constraint = \
                        "((current_date > add_months (obsdate, l2propint))" + \
                        " or (lower(" + accessid + ") in (select " + \
                        accessid + " from " + tmp_accessiddbtbl + ")))"
                else:
                    access_constraint = \
                        "(current_date > add_months(obsdate, l2propint))"
            
            if debug:
                logging.debug ('')
                logging.debug (
                    f'neid: access_constraint= {access_constraint:s}')

        if debug:
            logging.debug ('')
            logging.debug (f'access_constraint= {access_constraint:s}')


        selectstr = ''
        if (len(wherestr) > 0):
#            selectstr = "select " + fileid + " as " + fileid_allowed + \
#                " from " + dbtable  + " " + wherestr + " and " + \
#                access_constraint

            selectstr = "select " + fileid + " from " + dbtable + " " + \
                wherestr + " and " + access_constraint
        else:
#            selectstr = "select " + fileid + " as " + fileid_allowed + \
#                " from " + dbtable  + " where " + access_constraint
            
            selectstr = "select " + fileid + " from " + dbtable  + \
                " where " + access_constraint

        if debug:
            logging.debug ('')
            logging.debug (f'selectstr= {selectstr:s}')

#
#    test the selectstr
#
        cursor = self.conn.cursor()
        try: 
            self.__executeSql__ (cursor, selectstr)
	    
            if debug:
                logging.debug ('')
                logging.debug (f'returned from executeSql: {selectstr:s}')

#            if self.debugtime:
#                self.time1 = datetime.datetime.now()
#                self.delt = (self.time1 - self.time0).total_seconds()
#                self.time0 = self.time1
#                if debug:
#                    logging.debug ('')
#                    logging.debug (f'time (select): {self.delt:f}')
            
        except Exception as e:
        
            self.msg = f'Failed to execute select statement [{selectstr:s}]: ' \
                + str(e)
            if debug:
                logging.debug ('')
                logging.debug (f'{self.msg:s}')
	    

#            if self.debugtime:
#                self.time1 = datetime.datetime.now()
#                self.delt = (self.time1 - self.time0).total_seconds()
#                self.time0 = self.time1
#                if debug:
#                    logging.debug ('')
#                    logging.debug (f'time (select): {self.delt:f}')
            
            raise Exception (self.msg)

        if debug:
            logging.debug ('')
            logging.debug ('select statement executed successfully')


        sql = "insert into " + tmp_fileiddbtbl + \
            " (" + selectstr + ")"

        if debug:
            logging.debug ('')
            logging.debug (f'sql= {sql:s}')


        if self.debugtime:
            self.time0 = datetime.datetime.now()
        
        cursor = self.conn.cursor()
        try: 
            self.__executeSql__ (cursor, sql)
	    
            if debug:
                logging.debug ('')
                logging.debug (f'returned from executeSql: {sql:s}')

        except Exception as e:
        
            self.msg = 'Failed to insert data toe tmp_fileiddbtbl: ' + str(e)
            if debug:
                logging.debug ('')
                logging.debug (f'{self.msg:s}')
	    
            raise Exception (self.msg)

        if debug:
            logging.debug ('')
            logging.debug ('insert tmp_fileiddbtbl statement executed')

#        if self.debugtime:
#            self.time1 = datetime.datetime.now()
#            self.delt = (self.time1 - self.time0).total_seconds()
#            self.time0 = self.time1
#            if debug:
#                logging.debug ('')
#                logging.debug (f'time (insert tmp_fileiddb): {self.delt:f}')

#
#    select accessid from tmp_fileiddbtbl: just to verify
#
               
        sql = "select * from " + tmp_fileiddbtbl
        
        if debug:
            logging.debug ('')
            logging.debug (f'sql= {sql:s}')
        
        cursor = self.conn.cursor()
        try: 
            self.__executeSql__ (cursor, sql)
	    
            if debug:
                logging.debug ('')
                logging.debug (f'returned from executeSql: {sql:s}')

        except Exception as e:
        
            self.msg = 'Failed to select from tmp_fileiddbtbl: ' + str(e)
            if debug:
                logging.debug ('')
                logging.debug (f'{self.msg:s}')
	    
            raise Exception (self.msg)

        if debug:
            logging.debug ('')
            logging.debug ('select id statement executed')
    
        fileidpath = self.userworkdir + '/' + tmp_fileiddbtbl + '.tbl'
        
        if debug:
            logging.debug ('')
            logging.debug (f'fileidpath= {fileidpath:s}')

        try: 
            self.__writeSinglecolResult__ (cursor, fileidpath, 35)
	    
            if debug:
                logging.debug ('')
                logging.debug ('returned from writeSinglecolResult: {sql:s}')

        except Exception as e:
        
            self.msg = 'Failed to write result to fileidtbl: ' + str(e)
            if debug:
                logging.debug ('')
                logging.debug (f'{self.msg:s}')
	    
            raise Exception (self.msg)
             

        return
#
#} end of createTmpFileiddb def 
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
            
            if debug:
                logging.debug ('')
                logging.debug (f'Failed to executeSql: {str(e):s}')
            
#            self.msg = 'Failed to executeSql: ' + str(e).replace('"', "'")
            
            self. __encodeSqlerrmsg__ (str(e))

            if self.debug:
                logging.debug ('')
                logging.debug (
                    f'returned encodeSqlerrmsg: self.msg= {self.msg:s}')
           
            raise Exception (self.msg) 

#
#} end of executeSql def 
#

    
    def __parseSql__ (self, cursor, sql, **kwargs):
#
#{ 
#
        debug =0 

        if ('debug' in kwargs):
            debug = kwargs['debug']

        if debug:
            logging.debug ('')
            logging.debug ('Enter parseSql')
            logging.debug (f'sql:= {sql:s}')

        try:
            cursor.parse (sql)
        
        except Exception as e:
            
            if debug:
                logging.debug ('')
                logging.debug (f'Failed to parseSql: {str(e):s}')
           
            
#            self. __encodeSqlerrmsg__ (str(e))

#            if debug:
#                logging.debug ('')
#                logging.debug (f'self.msg= {self.msg:s}')
           
            raise Exception (str(e))

#
#} end of parseSql def 
#

    def __encodeSqlerrmsg__ (self, errmsg, **kwargs):
#
#{ 
#
        debug = 0

        if ('debug' in kwargs):
            debug = kwargs['debug']

        if debug:
            logging.debug ('')
            logging.debug ('Enter encodeSqlerrmsg')
            logging.debug (f'errmsg= {errmsg:s}')

#
#
#    Oracle error might contain characters that needs fixing for xml structure
#
#    replace " with '
#
        errmsg = errmsg.replace('"', "'")
        
        if debug:
            logging.debug ('')
            logging.debug (f'here1: errmsg= {errmsg:s}')

#
#    replace < with &lt;
#
        errmsg = errmsg.replace('<', '&lt;')

        if debug:
            logging.debug ('')
            logging.debug (f'here1: errmsg= {errmsg:s}')

#
#    replace > with &gt;
#
        errmsg = errmsg.replace('>', '&gt;')

        if debug:
            logging.debug ('')
            logging.debug (f'here1: errmsg= {errmsg:s}')

        self.msg = errmsg

        return

#
#} end of encodeSqlerrmsg def 
#


    def __dropDbtbl__ (self, dbtable, **kwargs):
#
#{ 
#
        debug = 0

        if ('debug' in kwargs):
            debug = kwargs['debug']

        if debug:
            logging.debug ('')
            logging.debug ('Enter dropDbtbl')

        cursor_drop = self.conn.cursor()
        dropsql = 'drop table ' + dbtable
        
        if debug:
            logging.debug ('')
            logging.debug (f'dropsql:= {dropsql:s}')

        try:
            cursor_drop.execute (dropsql)
        
            if debug:
                logging.debug ('')
                logging.debug (f'table {dbtable:s} successfully dropped')
        
        except Exception as e:
            pass
            
            if debug:
                logging.debug ('')
                logging.debug (f'drop table exception: {str(e):s}')
        return

#
#} end of dropDbtbl def 
#


