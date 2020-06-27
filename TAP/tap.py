import os
import sys
import fcntl
import re

import os
import sys
import fcntl
import io
import logging

import datetime
import time
import signal

import cgi
import tempfile 
import configobj
import cx_Oracle

import xmltodict
from bs4 import BeautifulSoup

import numpy as np 
from astropy.io import ascii 
from astropy.table import Table, Column

from ADQL.adql import ADQL

from spatial_index import SpatialIndex

from TAP.datadictionary import dataDictionary
from TAP.runquery import runQuery
from TAP.configparam import configParam
from TAP.propfilter import propFilter
from TAP.tablenames import TableNames 


class Tap:
    
    """
    This class is the main program to process the TAP query submitted by client,
    it performs the following functionality:
 
    1.  extract input parameters,

    2.  read parameters from TAP configuration file (TAP.ini), the path of the 
        config file is specified by the environment variable 'TAP_CONF',

        'TAP.ini' contains web server info, database server info, spatial 
        index setting, and special column names for filtering the proprietary
        data.

    3.  convert ADQL query to ORACLE query,

    4.  retrieve metadata from database, applies proprietary filter if
        it is specified by the project.


    Input TAP parameters:
    ---------------------

        query (char):  an ADQL query (required)
   
        phase (char): the phase it input is either PENDING or RUN, 
                      if not specified, set to PENDING.

        format (char): output metadata table format: 
                       votable, ipac, cvs, or tvs; default is votable.
    
        maxrec (int): integer number of records to be returned; 
                      if not specified, all records are returned.

    
    Date: February 05, 2019 (Mihseh Kong)
    """       

#
# {  class tap 
#
    
    pid = os.getpid()
    form = cgi.FieldStorage()
   
    debugtime = 0
    time00 = None

    debug = 0
    debugfname = ''

    debugfname = '/tmp/tap_' + str(pid) + '.debug'
    
    sql = ''
    servername = ''
    dbtable = ''
    ddtable = ''
    dd = None
  
    param = dict()
    statdict = dict()


    errmsg = ''

    propflag = -1 
    datalevel = ''
    instrument = ''

    overflow = 0
    maxrec = -1 
    maxrecstr = ''

    ntot = 0

    status = ''
    msg = ''
    
    tapcontext = ''
    getstatus = 0
    setstatus = 0
    id = ''
    statuskey = ''

    configpath = ''
    pathinfo = ''
    
    cookiestr = ''
    cookiename = ''

    workdir = ''
    workurl = ''
    httpurl = ''
    cgipgm  = ''

    userWorkdir = ''
    workspace = ''

    statustbl = ''
    statuspath = ''
    statusurl = ''

    resulttbl = ''
    resultpath = ''
    resulturl = ''

    query = ''


    def __init__ (self, **kwargs):
#
# {  tap.init() 
#
        
        if ('debug' in self.form):
            self.debug = 1
   
        if ('debugtime' in self.form):
            self.debugtime = 1
#
#    set debug = 1 for debugging client
#
#        self.debug =1

        if ((self.debug or self.debugtime) and (len(self.debugfname) > 0)):
      
            logging.basicConfig (filename=self.debugfname, level=logging.DEBUG)
    
            with open (self.debugfname, 'w') as fdebug:
                pass

        if self.debug:
            logging.debug ('')
            logging.debug (f'Enter Tap.init(): pid= {self.pid:d}')

#
#    print all environ keys, retrieve async/sync spec from PATH_INFO
#    environ variable
#
        if self.debug:
            logging.debug ('')
            logging.debug ('Environment parameters:')
            logging.debug ('')

            for key in os.environ.keys():
                logging.debug (f'{key:s}: {os.environ[key]:20s}')
  
        if self.debug:
            logging.debug ('')
            logging.debug ('got here 0')
            logging.debug ('')

        if self.debugtime:
            self.time00 = datetime.datetime.now()
            time0 = datetime.datetime.now()
            logging.debug ('')
            logging.debug ('TAP service start:')

#
#    default values; initialize phase to PENDING
#
    
        self.param['lang'] = 'ADQL'
        self.param['phase'] = ''
        self.param['request'] = 'doQuery'
        self.param['query'] = ''
        self.param['format'] = 'votable'
        self.param['maxrec'] = -1 
    
  
        if self.debug:
            logging.debug ('')
            logging.debug (f'here0')
   

        self.querykey = 0
        for key in self.form:
        
            if self.debug:
                logging.debug ('')
                logging.debug (f'key= {key:s} val= {self.form[key].value:s}')
    
            if (key.lower() == 'propflag'):
            
                self.propflag = int(self.form[key].value)
            
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'input propflag= [{self.propflag:d}]')

            if self.debug:
                logging.debug ('')
                logging.debug (f'propflag= {self.propflag:d}')

            if (key.lower() == 'lang'):
                self.param['lang'] = self.form[key].value

            if (key.lower() == 'request'):
                self.param['request'] = self.form[key].value

            if (key.lower() == 'phase'):
                self.param['phase'] = self.form[key].value.strip()
        
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'phase= {self.param["phase"]:s}')

            if (key.lower() == 'query'):
                self.param['query'] = self.form[key].value.strip()
                self.querykey = 1

            if (key.lower() == 'format'):
                self.param['format'] = self.form[key].value.strip()

            if (key.lower() == 'responseformat'):
                self.param['format'] = self.form[key].value.strip()

            if (key.lower() == 'maxrec'):
            
                self.maxrecstr = self.form[key].value
            
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'maxrecstr= {self.maxrecstr:s}')
           
        self.nparam = len(self.param)
    
        if self.debug:
            logging.debug ('')
            logging.debug (f"param['format']= {self.param['format']:s}")

        self.format = self.param['format'].lower()
    
        if ((self.format != 'votable') and \
            (self.format != 'ipac') and \
            (self.format != 'csv') and \
            (self.format != 'tsv')):

            if self.debug:
                logging.debug ('')
                logging.debug ('format error detected')
      
            self.msg = 'Response format (' + self.format + \
	        ') must be: votable, ipac, csv, or tsv'
        
            self.__printError__ ('votable', self.msg)
  

        self.maxrec = -1
        if (len(self.maxrecstr) > 0):
        
            try:
                maxrec_dbl = float(self.maxrecstr)
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'maxrec_dbl= [{maxrec_dbl:f}]')
       
                self.maxrec = int(maxrec_dbl)
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'maxrec= [{self.maxrec:d}]')
            
            except Exception as e:
                
                self.msg = "Failed to convert input maxrec value [" + \
                    self.param['maxrec'] + "] to integer."
                self.__printError__ (self.format, self.msg)


        if self.debug:
            logging.debug ('')
            logging.debug (f'format= {self.format:s}')
            logging.debug (f'maxrec= {self.maxrec:d}')
    
        self.param['maxrec'] = self.maxrec

        if self.debug:
            logging.debug ('')
            logging.debug (f'nparam= {self.nparam:d}')
            
            for key in self.param:
                if (key == 'maxrec'):
                    logging.debug (f'key= {key:s} value= {self.param[key]:d}')
                else:
                    logging.debug (f'key= {key:s} value= {self.param[key]:s}')

    
        if ("PATH_INFO" in os.environ):
            self.pathinfo = os.environ["PATH_INFO"]

        if (len(self.pathinfo) == 0):
            self.msg = 'Failed to find PATH_INFO (e.g. sync, async) in URL.'
            self.__printError__ (self.format, self.msg)

        if self.debug:
            logging.debug ('')
            logging.debug (f'pathinfo= {self.pathinfo:s}')
    
        if (self.pathinfo[0] == '/'):
            self.pathinfo = self.pathinfo[1:]

        if self.debug:
            logging.debug ('')
            logging.debug (f'pathinfo= {self.pathinfo:s}')
    
        arr = self.pathinfo.split ('/')
        narr = len(arr)

        if self.debug:
            logging.debug ('')
            logging.debug (f'narr= {narr:d}')
            for i in range (narr):
                logging.debug (f'i= {i:d} arr= {arr[i]:s}')
         
        if (arr[0] == "async"):
            self.tapcontext = 'async'
        elif (arr[0] == "sync"):
            self.tapcontext = 'sync'
 
        if (narr > 1):
        
            self.getstatus = 1
            self.id = arr[1]
        
            if (len(self.id) == 0):
                self.msg = 'Failed to find jobid for retrieving job status.'
                self.__printError__ (self.format, self.msg)


            len_id = len(self.id)
            ind = self.pathinfo.find(self.id)
            i = ind+len_id + 1
            self.statuskey = self.pathinfo[i:]

            if (self.param['phase'] == 'RUN'):
                self.getstatus = 0
                self.setstatus = 1

        if self.debug:
            logging.debug ('')
            logging.debug (f'statuskey= {self.statuskey:s}')
            logging.debug (f'tapcontext= {self.tapcontext:s}')
            logging.debug (f'getstatus= {self.getstatus:d}')
            logging.debug (f'setstatus= {self.setstatus:d}')
            logging.debug (f'id= {self.id:s}')

        
#        if ((self.getstatus == 0) and (self.querykey == 0)):
#            msg = "TAP keyword: 'query' not found."
#            self.__printError__ (param['format'], msg)

#
#    retrieve cookiestr
#
        self.cookiestr = os.getenv ('HTTP_COOKIE', default='')

        if self.debug:
            logging.debug ('')
            logging.debug (f'cookiestr= {self.cookiestr:s}') 

#
#    extract configfile name from TAP_CONF environment variable
#    Note: make sure TAP_CONF env var is set
#
        if ('TAP_CONF' in os.environ):
            self.configpath = os.environ['TAP_CONF']
        else:
            if self.debug:
                logging.debug ('')
                logging.debug ('Failed to find TAP_CONF environment variable.')
        
            self.msg = 'Failed to find TAP_CONF environment variable.'
            self.__printError__ (self.format, self.msg)

        if self.debug:
            logging.debug ('')
            logging.debug (f'configpath= {self.configpath:s}')

#
#    retrieve config variables
#
        self.config = None
        try:
            self.config = configParam (self.configpath, debug=1)
#            self.config = configParam (self.configpath)
    
            if self.debug:
                logging.debug ('')
                logging.debug ('returned config')

        except Exception as e:

            if self.debug:
                logging.debug ('')
                logging.debug (f'config exception: {str(e):s}') 
        
            self.__printError__ (self.format, str(e))

        self.workdir = self.config.workdir
        self.workurl = self.config.workurl
        self.httpurl = self.config.httpurl
        self.cgipgm  = self.config.cgipgm 
        self.cookiename = self.config.cookiename
   
        if self.debug:
            logging.debug ('')
            logging.debug (f'workdir= {self.workdir:s}') 
            logging.debug (f'workurl= {self.workurl:s}') 
            logging.debug (f'httpurl= {self.httpurl:s}') 
            logging.debug (f'cgipgm= {self.cgipgm:s}') 
            logging.debug (f'cookiename= {self.cookiename:s}') 
            logging.debug (f'fileid= {self.config.fileid:s}') 
            logging.debug (f'accessid= {self.config.accessid:s}') 
            logging.debug (f'racol= {self.config.racol:s}') 
            logging.debug (f'deccol= {self.config.deccol:s}') 
            logging.debug (f'propfilter= {self.config.propfilter:s}') 
            logging.debug (f'phase= {self.param["phase"]:s}') 


#
#    initialize statdict dict
#
        self.statdict['process_id'] = self.pid 
        self.statdict['owneridlabel'] = 'ownerId xsi:nil="true"'
        self.statdict['quotelabel'] = 'quote xsi:nil="true"'
        self.statdict['errmsg'] = ''
    
        self.statdict['phase'] = self.param['phase']
        if self.debug:
            logging.debug ('')
            logging.debug (f'param[phase]= {self.param["phase"]:s}') 
            logging.debug (f'statdict[phase]= {self.statdict["phase"]:s}') 

        
        self.statdict['jobid'] = '' 
    
        self.statdict['starttime'] = '' 
        self.statdict['destruction'] =  '' 
        self.statdict['endtime'] = '' 
        self.statdict['duration'] = '0' 

        self.statdict['resulturl'] = ''

#
#    sync or async without input workspace id: make workspace, 
#    otherwise retrieve workspace from getstatus id
#
        if ((self.tapcontext == 'sync') or \
            ((self.tapcontext == 'async') and \
            (self.getstatus == 0) and \
            (self.setstatus == 0))):
#
#{   make workspace:
#    make TAP subdir if it doesn't exist, and 
#    make a workspace name with unique id
#
            if self.debug:
                logging.debug ('')
                logging.debug (f'Async without workspace id: make workspace')

            tapdir = self.workdir + '/TAP'

            try:
                os.makedirs (tapdir, exist_ok=True)
                os.chmod (tapdir, 0o775)
    
            except Exception as e:
                self.msg = 'Failed to create ' + tapdir + ': ' + str(e)
                self.__printError__ (self.format , self.msg) 

            if self.debug:
                logging.debug ('')
                logging.debug (f'tapdir: {tapdir:s} created')

            
            try:
                self.userWorkdir = tempfile.mkdtemp (prefix='tap_', dir=tapdir)
    
            except Exception as e:
                self.msg = 'tempfile.mkdtemp exception: ' + str(e)
                self.__printError__ (self.format, self.msg) 

            if self.debug:
                logging.debug ('')
                logging.debug (
                    f'returned tempfile.mkdtemp {self.userWorkdir:s}')
   
            ind = self.userWorkdir.rfind ('/') 
            if (ind > 0):
                self.workspace = self.userWorkdir[ind+1:]

            if self.debug:
                logging.debug ('')
                logging.debug (f'workspace= {self.workspace:s}')
   
   
            try:
                os.makedirs(self.userWorkdir, exist_ok=True)
                os.chmod (self.userWorkdir, 0o775)
    
            except Exception as e:
        
                self.msg = 'os.makedir exception: ' + str(e)
                self.__printError__ (self.format, self.msg) 

            if self.debug:
                logging.debug ('')
                logging.debug (f'userWorkdir {self.userWorkdir:s} created')
#
#} end of make workspace
#
        else:
#
#{ retrieve workspace from id 
#
            self.workspace = self.id
            self.userWorkdir = self.workdir + '/TAP/' + self.workspace
        
#
#} end of retrieve workspace 
#
        if self.debug:
            logging.debug ('')
            logging.debug (f'workspace= {self.workspace:s}')
            logging.debug (f'userWorkdir= {self.userWorkdir:s}')

#
#   make up status and result table names
#
        self.statustbl = 'status.xml'
        self.statuspath = self.userWorkdir + '/' + self.statustbl
        self.statusurl = self.httpurl + '/' + self.cgipgm + \
            '/' + self.tapcontext + '/' + self.workspace

        if self.debug:
            logging.debug ('')
            logging.debug (f'statuspath= {self.statuspath:s}')
            logging.debug (f'statusurl= {self.statusurl:s}')
  

        self.resulttbl = ''
        if (self.format == 'votable'):
            self.resulttbl = 'result.xml'
        elif (self.format == 'ipac'):
            self.resulttbl = 'result.tbl'
        elif (self.format == 'csv'):
            self.resulttbl = 'result.csv'
        elif (self.format == 'tsv'):
            self.resulttbl = 'result.tsv'


        self.resultpath = self.userWorkdir + '/' + self.resulttbl
        self.resulturl = self.httpurl + self.workurl + '/TAP/' + \
            self.workspace + '/' + self.resulttbl 

        if self.debug:
            logging.debug ('')
            logging.debug (f'resultpath= {self.resultpath:s}')
            logging.debug (f'resulturl= {self.resulturl:s}')

#
#    if async and phase == PENDING: send 303 with statusurl and exit
#
        if self.debug:
            logging.debug ('')
            logging.debug ('before setting phase to PENDING')
            logging.debug (f'tapcontext= {self.tapcontext:s}')
            logging.debug (f'getstatus= {self.getstatus:d}')
            logging.debug (f'setstatus= {self.setstatus:d}')
            logging.debug (f'param[phase]= {self.param["phase"]:s}')


        if ((self.tapcontext == 'async') and \
            (self.getstatus == 0) and \
            (self.setstatus == 0) and \
            (len(self.param['phase']) == 0)):
    
#
#{  if phase not specified: set to PENDING and exit
#
            self.statdict['phase'] = 'PENDING'
            self.statdict['jobid'] = self.workspace
    
            if self.debug:
                logging.debug ('')
                logging.debug ('call writeStatusMessage:')
                logging.debug (f'maxrec= {self.maxrec:d}')

            if self.debug:
                self.__writeStatusMsg__ (self.statuspath, self.statdict, self.param, \
                    debug=1)    
            else:
                self.__writeStatusMsg__ (self.statuspath, self.statdict, self.param)    
    
            if self.debug:
                logging.debug ('')
                logging.debug ('returned writeStatusMessage')
                logging.debug (f'statusurl= {self.statusurl:s}')

        
            print ("HTTP/1.1 303 See Other\r")
            print (f"Location: %s\r\n\r" % self.statusurl)
            print (f"Redirect Location: %s" % self.statusurl)
            sys.stdout.flush()
            sys.exit()
#
#} end of PENDING case
#
        if self.debug:
            logging.debug ('')
            logging.debug (f'here1')

#
#    getstatus case: call getStatus mothod which reads status file: 
#    printStatus or error messages, then exit.
#
    
        if (self.getstatus == 1):
#
#{
#
            if self.debug:
                logging.debug ('')
                logging.debug (f'Case getstatus')

            try:
                self.__getStatus__ (self.workdir, self.id, self.statuskey, \
                    self.param)

#                self.__getStatus__ (self.workdir, self.id, self.statuskey, \
#                    self.param, debug=1)
        
            except Exception as e:

                if self.debug:
                    logging.debug ('')
                    logging.debug (f'getStatus error: {str(e):s}') 
            
                self.__printError__ (self.format , str(e)) 

#
#    getStatus will exit when done
#
#}
#
        if self.debug:
            logging.debug ('')
            logging.debug (f'here2')

#
#    setstatus to RUN case:
#    parse status file to get parameters
#
        if ((self.tapcontext == 'async') and \
            (self.param['phase'] == 'RUN')):
#
#{
#
            if self.debug:
                logging.debug ('')
                logging.debug (f'Case set phase RUN')
                logging.debug ('')
                logging.debug ('param: (from input)')
                logging.debug (f'format= {self.param["format"]:s}')
                logging.debug (f'lang= {self.param["lang"]:s}')
                logging.debug (f'maxrec= {self.param["maxrec"]:d}')
                logging.debug (f'query= {self.param["query"]:s}')
                logging.debug (f'phase= {self.param["phase"]:s}')
            
#
#    parse statuspath to retrieve parameters
#
            if (self.setstatus == 1):
#
#{    setstatus = 1
#

                if self.debug:
                    logging.debug ('')
                    logging.debug (f'case setstatus=1')
                    logging.debug (f'statuspath= {self.statuspath:s}')

                doc = None
                with open (self.statuspath, 'r') as fp:
                    doc = fp.read()
        
                if self.debug:
                    logging.debug ('')
                    logging.debug ('doc=')
                    logging.debug (doc)
        
                soup = BeautifulSoup (doc, 'lxml')

                parameters = soup.find ('uws:parameters')

                if self.debug:
                    logging.debug ('')
                    logging.debug ('parameters=')
                    logging.debug (parameters)
       
                parameter = parameters.find (id='query')
                self.param['query'] = parameter.string
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'query= {self.param["query"]:s}')

                parameter = parameters.find (id='format')
                self.param['format'] = parameter.string
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'format= {self.param["format"]:s}')

                parameter = parameters.find (id='maxrec')
                self.maxrecstr = parameter.string
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'maxrecstr= {self.maxrecstr:s}')

                self.param['maxrec'] = int(parameter.string)
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'maxrecstr= {self.param["maxrec"]:d}')

                parameter = parameters.find (id='lang')
                self.param['lang'] = parameter.string
        
#
#} end  setstatus = 1
#

#
#    rewrite statustbl
#   
            if self.debug:
                logging.debug ('')
                logging.debug (f'xxx0-0: write statustbl')
            
            self.statdict['process_id'] = self.pid 
            self.statdict['jobid'] = self.workspace

            self.statdict['phase'] = 'EXECUTING'

            stime = datetime.datetime.now()
            destructtime = stime + datetime.timedelta (days=4)

            if self.debug:
                logging.debug ('')
                logging.debug (f'got here: statusurl= {self.statusurl:s}')
                logging.debug (f"process_id= {self.statdict['process_id']:d}")
                logging.debug (f"jobid= {self.statdict['jobid']:s}")
                logging.debug (f'stime:')
                logging.debug (stime)
                logging.debug ('destructtime:')
                logging.debug (destructtime)

            starttime = stime.strftime ('%Y-%m-%dT%H:%M:%S.%f')[:-4]
            destruction = destructtime.strftime ('%Y-%m-%dT%H:%M:%S.%f')[:-4]
            if self.debug:
                logging.debug ('')
                logging.debug (f'starttime: {starttime:s}')
                logging.debug (f'destruction= {destruction:s}')
                logging.debug (f'xxx0')
            
   
            self.statdict['stime'] = stime
            if self.debug:
                logging.debug ('')
                logging.debug (f'xxx0-0-0')
            
            self.statdict['starttime'] = starttime
            if self.debug:
                logging.debug ('')
                logging.debug (f'xxx0-1')
            
            self.statdict['destruction'] = destruction 
            if self.debug:
                logging.debug ('')
                logging.debug (f'xxx0-2')
            
            self.statdict['endtime'] = '' 
            if self.debug:
                logging.debug ('')
                logging.debug (f'xxx0-3')
            
            self.statdict['duration'] = '0' 
       
            if self.debug:
                logging.debug ('')
                logging.debug (f'xxx0-4')
            
            self.statdict['resulturl'] = self.resulturl

            if self.debug:
                logging.debug ('')
                logging.debug ('call writeStatusMessage:')
                logging.debug (f'phase= {self.statdict["phase"]:s}')
       
            if self.debug:
                self.__writeStatusMsg__ (self.statuspath, self.statdict, \
                    self.param, debug=1)    
            else: 
                self.__writeStatusMsg__ (self.statuspath, self.statdict, \
                    self.param)    
    
            if self.debug:
                logging.debug ('')
                logging.debug ('returned writeStatusMessage')

#
#    generate return response and terminate parent process
#    before proceed to run the search program 
#
            if self.debug:
                logging.debug ('')
                logging.debug ('call printAsyncResponse')
   
            self.__printAsyncResponse__ (self.statusurl)

#            self.__printAsyncResponse__ (self.statusurl, debug=1)

            if self.debug:
                logging.debug ('')
                logging.debug ('returned printAsyncResponse')
#
#}
#
        if self.debug:
            logging.debug ('')
            logging.debug (f'here3')


#
#{    run the query for both async and sync cases:
#
#   if query is blank, return error
#
        if (len(self.param['query']) == 0):

            self.msg = "Input 'query' is blank."

            if (self.tapcontext == 'async'):
            
                self.__writeAsyncError__ (self.msg, self.statuspath, \
                    self.statdict, self.param)
            else: 
                self.__printError__ (self.format, self.msg)

#
#  convert ADQL query to ORACLE query (this may be moved to the up so pending
#  case can reject bad adql query)
#
        if self.debug:
            logging.debug ('')
            logging.debug (f'here 4')

        query_adql = self.param['query']
    
        if self.debug:
            logging.debug ('')
            logging.debug (f'query_adql= {query_adql:s}')
   
        try:
            if self.debug:
                logging.debug ('')
                logging.debug (f'here 4-0')

            mode = SpatialIndex.HTM
            if self.debug:
                logging.debug ('')
                logging.debug (f'here 4-1')

            if (self.config.adqlparam['mode'] == 'HPX'):
                mode = SpatialIndex.HPX

            if self.debug:
                logging.debug ('')
                logging.debug (f'mode= {mode:d}')

            level   = int(self.config.adqlparam['level'])
            colname = self.config.adqlparam['colname']

            if self.debug:
                logging.debug ('')
                logging.debug (f'level= {level:d}')
                logging.debug (f'colname= {colname:s}')

            encoding = SpatialIndex.BASE4
            if(self.config.adqlparam['encoding'] == 'BASE10'):
                encoding = SpatialIndex.BASE10

            if self.debug:
                logging.debug ('')
                logging.debug (f'encoding= {encoding:d}')

            racol = self.config.racol
            deccol = self.config.deccol
            
            xcol = self.config.adqlparam['xcol']
            ycol = self.config.adqlparam['ycol']
            zcol = self.config.adqlparam['zcol']

            if self.debug:
                logging.debug ('')
                logging.debug (f'racol= {racol:s}')
                logging.debug (f'deccol= {deccol:s}')
                logging.debug (f'xcol= {xcol:s}')
                logging.debug (f'ycol= {ycol:s}')
                logging.debug (f'zcol= {zcol:s}')


            dbms = self.config.connectInfo['dbms']
            
            if self.debug:
                logging.debug ('')
                logging.debug (f'dbms= {dbms:s}')


            adql = ADQL (dbms=dbms, mode=mode, level=level, indxcol=colname, \
                encoding=encoding, racol=racol, deccol=deccol, \
                xcol=xcol, ycol=ycol, zcol=zcol)

            self.query = adql.sql (query_adql)
    
            if self.debug:
                logging.debug ('')
                logging.debug (f'final query= {self.query:s}')

        except Exception as e:

            if self.debug:
                logging.debug ('')
                logging.debug (f'exception: {str(e):s}')
       
            if (self.tapcontext == 'async'):
            
                self.__writeAsyncError__ (str(e), self.statuspath, \
                    self.statdict, self.param)
            else: 
                self.__printError__ (self.format, str(e))

#
#    extract DB table name from query (This will be replaced with a library
#    parser
#
        self.dbtable = ''
        try:
            tn = TableNames()
            tables = tn.extract_tables(self.query)
            self.dbtable = tables[0]
        except:
        
            if self.debug:
                logging.debug ('')
                logging.debug (f'TableName exception')

            pass

        if len(self.dbtable) == 0:

            self.msg = 'No table name found ADQL in query.'
            self.__printError__ (self.format, self.msg);

        self.ddtable = self.dbtable + '_dd'
    
        if self.debug:
            logging.debug ('')
            logging.debug (f'dbtable= [{self.dbtable:s}]')
            logging.debug (f'ddtable= {self.ddtable:s}')

        self.datalevel = self.__getDatalevel__ (self.dbtable)

        if self.debug:
            logging.debug ('')
            logging.debug (f'datalevel= [{self.datalevel:s}]')

#
#    determine whether to use runQuery or propFilter to execute sql
#
        if (self.propflag == -1): 

            if self.debug:
                logging.debug ('')
                logging.debug (f'No input propflag:')

            if ((self.config.propfilter.lower() == 'koa') or \
                ((self.config.propfilter.lower() == 'neid') and \
                    (self.datalevel != 'l0'))):
                self.propflag = 1
            else:
                self.propflag = 0

        if self.debug:
            logging.debug ('')
            logging.debug (f'propflag= [{self.propflag:d}]')


#
#    check if dbtable is tap_schema tables
#
        ind = self.dbtable.lower().find ('tap_schema') 

        if (ind != -1):
            self.propflag = 0
        
            if self.debug:
                logging.debug ('')
                logging.debug (f'tap_schema table queries: set propflag to 0')

        if self.debug:
            logging.debug ('')
            logging.debug (f'propflag= [{self.propflag:d}]')

        if self.debugtime:
            time1 = datetime.datetime.now()
            delt = (time1 - time0).total_seconds()
            time0 = time1
            logging.debug ('')
            logging.debug (\
                f'time (initialization including adql translation): {delt:f}s')

        dbquery = None
        propfilter = None
   
#
#    force proflag = 0 for debugging
#
#        self.propflag = 0


        if (self.propflag == 0):
#
#{   execute 'runQuery': 
#    return the result if sync, or 
#    update the status file if async 
#
            try:
                if self.debug:
                    logging.debug ('')
                    logging.debug ('will call runQuery')
                    logging.debug (
                        f'maxrec= {self.maxrec:d} format= {self.format:s}')

                if (self.debug and self.debugtime):

                    dbquery = runQuery ( \
                        connectInfo=self.config.connectInfo, \
                        query=self.query, \
                        workdir=self.userWorkdir, \
                        format=self.format, \
                        maxrec=self.maxrec, \
                        racol=self.config.racol, \
                        deccol=self.config.deccol, \
                        debug=1, \
                        debugtime=1)
               
                elif self.debug:
            
                    dbquery = runQuery ( \
                        connectInfo=self.config.connectInfo, \
                        query=self.query, \
                        workdir=self.userWorkdir, \
                        format=self.format, \
                        maxrec=self.maxrec, \
                        racol=self.config.racol, \
                        deccol=self.config.deccol, \
                        debug=1)
                    
                elif self.debugtime:
            
                    dbquery = runQuery ( \
                        connectInfo=self.config.connectInfo, \
                        query=self.query, \
                        workdir=self.userWorkdir, \
                        format=self.format, \
                        maxrec=self.maxrec, \
                        racol=self.config.racol, \
                        deccol=self.config.deccol, \
                        debugtime=1)
               
                else:
                    dbquery = runQuery ( \
                        connectInfo=self.config.connectInfo, \
                        query=self.query, \
                        workdir=self.userWorkdir, \
                        format=self.format, \
                        maxrec=self.maxrec, \
                        racol=self.config.racol, \
                        deccol=self.config.deccol)

                self.phase = 'COMPLETED'
                self.ntot = dbquery.ntot

                if self.debug:
                    logging.debug ('')
                    logging.debug ('returned runQuery')

            except Exception as e:
   
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'runQuery exception: {str(e):s}')

                self.phase = 'ERROR'
#                self.errmsg = str(e)
            
                if (self.tapcontext == 'async'):
            
                    self.__writeAsyncError__ (str(e), self.statuspath, \
                        self.statdict, self.param)
                else: 
                    self.__printError__ (self.format, str(e))

            if self.debug:
                logging.debug ('')
                logging.debug (f'Done runQuery: outpath= {dbquery.outpath:s}')

            if self.debugtime:
                time1 = datetime.datetime.now()
                delt = (time1 - time0).total_seconds()
                time0 = time1
                logging.debug ('')
                logging.debug (f'time (runquery): {delt:f}s')

#
#}    end runquery
#
        else:
#
# {   run propFilter
#
    
            propfilter = None
            try:
                if self.debug:
                    logging.debug ('')
                    logging.debug ('will call propFilter')

                if (self.debug and self.debugtime):

                    propfilter = propFilter ( \
                        connectInfo=self.config.connectInfo, \
                        query=self.query, \
                        workdir=self.userWorkdir, \
                        racol=self.config.racol, \
                        deccol=self.config.deccol, \
                        cookiename=self.config.cookiename, \
                        cookiestr=self.cookiestr, \
                        propfilter=self.config.propfilter.lower(), \
                        usertbl=self.config.usertbl, \
                        accesstbl=self.config.accesstbl, \
                        fileid=self.config.fileid, \
                        accessid=self.config.accessid, \
                        format=self.format, \
                        maxrec=self.maxrec, \
                        debugtime=1, \
                        debug=1)

                elif self.debug:

                    propfilter = propFilter ( \
                        connectInfo=self.config.connectInfo, \
                        query=self.query, \
                        workdir=self.userWorkdir, \
                        racol=self.config.racol, \
                        deccol=self.config.deccol, \
                        cookiename=self.config.cookiename, \
                        cookiestr=self.cookiestr, \
                        propfilter=self.config.propfilter.lower(), \
                        usertbl=self.config.usertbl, \
                        accesstbl=self.config.accesstbl, \
                        fileid=self.config.fileid, \
                        accessid=self.config.accessid, \
                        format=self.format, \
                        maxrec=self.maxrec, \
                        debug=1)

                elif self.debugtime:

                    propfilter = propFilter ( \
                        connectInfo=self.config.connectInfo, \
                        query=self.query, \
                        workdir=self.userWorkdir, \
                        racol=self.config.racol, \
                        deccol=self.config.deccol, \
                        cookiename=self.config.cookiename, \
                        cookiestr=self.cookiestr, \
                        propfilter=self.config.propfilter.lower(), \
                        usertbl=self.config.usertbl, \
                        accesstbl=self.config.accesstbl, \
                        fileid=self.config.fileid, \
                        accessid=self.config.accessid, \
                        format=self.format, \
                        maxrec=self.maxrec, \
                        debugtime=1)

                else:
                    propfilter = propFilter ( \
                        connectInfo=self.config.connectInfo, \
                        query=self.query, \
                        workdir=self.userWorkdir, \
                        racol=self.config.racol, \
                        deccol=self.config.deccol, \
                        cookiename=self.config.cookiename, \
                        cookiestr=self.cookiestr, \
                        propfilter=self.config.propfilter.lower(), \
                        usertbl=self.config.usertbl, \
                        accesstbl=self.config.accesstbl, \
                        fileid=self.config.fileid, \
                        accessid=self.config.accessid, \
                        format=self.format, \
                        maxrec=self.maxrec)

                
                self.phase = 'COMPLETED'
                self.ntot = propfilter.ntot

                if self.debug:
                    logging.debug ('')
                    logging.debug ('returned propFilter: phase= {self.phase:s}')
                    logging.debug (f'ntot= {self.ntot:d}')

            except Exception as e:
   
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'propFilter exception: {str(e):s}')

                self.phase = 'ERROR'
                self.errmsg = str(e)

                if (self.tapcontext == 'async'):
                
                    if self.debug:
                        logging.debug ('')
                        logging.debug (f'call writeAsyncError')
                        for key in self.param:
                            if (key == 'maxrec'):
                                logging.debug (
                                    f'key= {key:s} val= {self.param[key]:d}')
                            else:
                                logging.debug (
                                    f'key= {key:s} val= {self.param[key]:s}')
                
                    self.__writeAsyncError__ (str(e), self.statuspath, \
                        self.statdict, self.param)
            
                    if self.debug:
                        logging.debug ('')
                        logging.debug (f'returned writeAsyncError')

                else: 
                    if self.debug:
                        logging.debug ('')
                        logging.debug (f'call printError')

                    self.__printError__ (self.format, str(e))

            if self.debug:
                logging.debug ('')
                logging.debug (
                    f'Done propfilter: outpath= {propfilter.outpath:s}')

            if self.debugtime:
                time1 = datetime.datetime.now()
                delt = (time1 - time0).total_seconds()
                logging.debug ('')
                logging.debug (f'time (propfilter): {delt:f}s')
        
#
# }   end propfilter 
#
#
#} finished run query  
#

#
#    async: write complete status  message 
#
        if (self.tapcontext == 'async'):
                
            if self.debug:
                logging.debug ('')
                logging.debug (f'case async')
       
            if self.debugtime:
                time0 = datetime.datetime.now()
        
            if self.debug:
                logging.debug ('')
                logging.debug (f'xxx0-0')
       
            etime = datetime.datetime.now()
            endtime = etime.strftime ('%Y-%m-%dT%H:%M:%S.%f')[:-4]
        
            if self.debug:
                logging.debug ('')
                logging.debug (f'xxx0-1')
       
            durationtime = etime - self.statdict['stime']
            duration = str(durationtime.total_seconds())[:4] 

            if self.debug:
                logging.debug ('')
                logging.debug (f'xxx0-2')
       
            self.statdict['endtime'] = endtime 
            self.statdict['duration'] = duration 
	
            if self.debug:
                logging.debug ('')
                logging.debug (f'xxx0-3')
       
            if self.debug:
                logging.debug ('')
                logging.debug (f'phase= {self.phase:s}')
                logging.debug (f'errmsg= {self.errmsg:s}')
      
            self.statdict['phase'] = self.phase
            self.statdict['errmsg'] = self.errmsg 
        
            if self.debug:
                logging.debug ('')
                logging.debug ('call writeStatusMsg')
                logging.debug (f'phase= {self.statdict["phase"]:s}')
       
       
#            time.sleep (2.0)
            self.__writeStatusMsg__ (self.statuspath, self.statdict, self.param)    
        
            if self.debug:
                logging.debug ('')
                logging.debug (f'returned writeStatusMsg')
       
            if self.debugtime:
                time1 = datetime.datetime.now()
                delt = (time1 - time0).total_seconds()
                logging.debug ('')
                logging.debug (f'time (async: writeStatusMsg): {delt:f}s')

                delt = (time1 - self.time00).total_seconds()
                logging.debug ('')
                logging.debug (
                    f'time (total TAP service completion): {delt:f}s')

        else:
            if self.debug:
                logging.debug ('')
                logging.debug (f'case sync')
       
            if self.debug:
                logging.debug ('')
                logging.debug (f'call printSyncResult')
       
            if self.debugtime:
                time0 = datetime.datetime.now()
        
            self.__printSyncResult__ (self.resultpath, self.format)

            if self.debug:
                logging.debug ('')
                logging.debug (f'returned printSyncResult')
      
            if self.debugtime:
                time1 = datetime.datetime.now()
                delt = (time1 - time0).total_seconds()
                logging.debug ('')
                logging.debug (f'time (sync: print return): {delt:f}s')

                delt = (time1 - self.time00).total_seconds()
                logging.debug ('')
                logging.debug (
                    f'time (total TAP service completion): {delt:f}s')

        if self.debug:
            logging.debug ('')
            logging.debug ('TAP service done')

        sys.exit()

#
# } end tap.init()
#



    def __getStatusJob__ (self, data, **kwargs):
#
# { 
#
        debug = 0

        if ('debug' in kwargs):
            debug = kwargs['debug']

        if debug:
            logging.debug ('')
            logging.debug ('Enter getStatusJob')
            logging.debug ('data=')
            logging.debug (data)

#
#    parse data to extract inparam
#
        doc = None
        try:
            doc = xmltodict.parse(data)
    
        except Exception as e:

            msg = 'Exception xmltodict.parse: ' + str(e)
            raise Exception (msg)
    
        if debug:
            logging.debug ('')
            logging.debug ('doc:')
            logging.debug (doc)

        job = None 
        try:
            job = doc['uws:job']
        except Exception as e:
        
            msg = 'Exception retrieving job from status file: ' + str(e)
            raise Exception (msg)

        return (job)
#
# }  end of getStatusJob
#


    def __getStatusData__ (self, statuspath, **kwargs):
#
# { 
#
        debug = 0 

        if ('debug' in kwargs):
            debug = kwargs['debug']

        if debug:
            logging.debug ('')
            logging.debug ('Enter getStatusData')
            logging.debug (f'statuspath= {statuspath:s}]')

        data = ''
        opened = False 
        nopen = 0


#
#    In case multiple processes are trying to access status file, 
#    allow attempts to open file three times before raise exception
#
        while (not opened):
    
            try: 
                with open (statuspath, 'r') as fp:
      
                    opened = True
                    data = fp.read()
     
                    if debug:
                        logging.debug ('')
                        logging.debug ('data=')
                        logging.debug (data)
    
            except Exception as e:
                msg = 'Error reading status file: ' + str(e)
                pass

            if debug:
                logging.debug ('')
                logging.debug ('here1: opened:')
                logging.debug (opened)

            if (opened):
                break

            time.sleep (1.0)
            nopen = nopen + 1

            if (nopened > 2):
                break

        if debug:
            logging.debug ('')
            logging.debug ('here2: opened:')
            logging.debug (opened)
	
        if (not opened):
            raise Exception (msg)

        return (data)
#
# }  end of getStatusData
#



    def __getPhase__ (self, statuspath, **kwargs):
#
# { 
#

        debug = 0

        if ('debug' in kwargs):
            debug = kwargs['debug']

        if debug:
            logging.debug ('')
            logging.debug ('Enter getPhase')
            logging.debug (f'statuspath= {statuspath:s}]')

        try:
            data = self.__getStatusData__ (statuspath)
        
            if debug:
                logging.debug ('')
                logging.debug ('data=')
                logging.debug (data)

        except Exception as e:
        
            msg = 'Error reading status file: ' + str(e)
            raise Exception (msg)

        job = None 
        try:
            job = self.__getStatusJob__ (data)
        except Exception as e:
            msg = 'Exception retrieving job from status file: ' + str(e)
            raise Exception (msg)

        keystr = 'uws:phase'
        retval = job['uws:phase']
    
        if debug:
            logging.debug ('')
            logging.debug (f'retval= {retval:s}')

        return (retval)
#
# }  end of getPhase
#



    def __printStatus__ (self, key, retval, outtype, **kwargs):
#
# { 
#
        debug = 0

        if ('debug' in kwargs):
            debug = kwargs['debug']

        if debug:
            logging.debug ('')
            logging.debug ('Enter printStatus')
            logging.debug (f'key= {key:s}')
            logging.debug (f'retval= {retval:s}')

#
#    header
#
        print ("HTTP/1.1 200 OK\r")

        if (outtype == 'xml'): 
        
            print("Content-type: text/xml\r")
            print("\r")
        
            print ('<?xml version="1.0" encoding="UTF-8"?>')
            print ('<uws:job xmlns:uws="http://www.ivoa.net/xml/UWS/v1.0" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.ivoa.net/xml/UWS/v1.0 http://www.ivoa.net.xml/UWS/v1.0">')
    
            if ((key == 'errorSummary') or \
                (key == 'errmsg') or \
                (key == 'error')):

                if (len(retval) == 0):
                    print ('    <uws:errorSummary></uws:errorSummary>')
                else:
                    print ('    <uws:errorSummary>')
                    print (retval)
                    print ('    </uws:errorSummary>')

            elif (key == 'parameters'):
        
                print (retval)
        
            elif ((key == 'results') or \
                (key == 'results/resulturl')):

                print ('    <uws:results>')
                print (retval)
                print ('    </uws:results>')
        
            print ('</uws:job>')
            sys.stdout.flush()
        
        else:
            print("Content-type: text/plain\r")
            print("\r")
        
            print (retval)
            sys.stdout.flush()
        
        sys.stdout.flush()
        return
#
# }  end of printStatus
#


    def __getStatus__ (self, workdir, workspace, key, param, **kwargs):
#
# { 
#
        debug = 1 

        if ('debug' in kwargs):
            debug = kwargs['debug']

        if debug:
            logging.debug ('')
            logging.debug ('Enter getStatus')
            logging.debug (f'workdir= {workdir:s}]')
            logging.debug (f'workspace= {workspace:s}]')
            logging.debug (f'key= {key:s}')

        statuspath = workdir + '/TAP/' + workspace + '/status.xml'

        if debug:
            logging.debug ('')
            logging.debug (f'statuspath= {statuspath:s}]')

        isExist = os.path.exists (statuspath)
    
        if (isExist == 0):
            msg = 'Status file: status.tbl does not exit.'
            raise Exception (msg)

        if debug:
            logging.debug ('')
            logging.debug (f'statuspath exists')

        data = '' 
        try:
            data = self.__getStatusData__ (statuspath)

        except Exception as e:
        
            msg = 'Error getStatusData: ' + str(e)
            raise Exception (msg)

        if debug:
            logging.debug ('')
            logging.debug ('returned getStatusData: data=')
            logging.debug (data)

#
#    no key: return the whole status file
#
        if (len(key) == 0):
        
            print ("HTTP/1.1 200 OK\r")
            print("Content-type: text/xml\r")
            print("\r")
            print (data);
            sys.exit()
#
#    extract format
#
        soup = None
        try:
            soup = BeautifulSoup (data, 'lxml')
	
        except Exception as e:
            self.__printError__ ('votable', str(e))

        
        format = 'votable'
        try:
            format = soup.parameters.findAll ( \
                'parameter', {'id':'format'})[0].get_text()
    
        except Exception as e:
        
            self.__printError__ ('votable', str(e))
    
        if debug:
            logging.debug ('')
            logging.debug (f'format= {format:s}')

        job = None 
        try:
            job = self.__getStatusJob__ (data)
        
        except Exception as e:
            msg = 'Exception retrieving job from status file: ' + str(e)
            raise Exception (msg)
   

        if (key == 'parameters'):

            parameters = None
            try:
                parameters = soup.find('uws:parameters')
            except Exception as e:
                self.__printError__ (format, str(e))
   
            if debug:
                logging.debug ('')
                logging.debug ('parameters:')
                logging.debug (parameters)
        
            self.__printStatus__ ('parameters', parameters, 'xml')
            sys.exit()

#
#    parse data to extract inparam
#
        job = None 
        try:
            job = self.__getStatusJob__ (data)
    
        except Exception as e:
            msg = 'Exception retrieving job from status file: ' + str(e)
            raise Exception (msg)

        if ((key == 'phase') or \
            (key == 'startTime') or \
            (key == 'endTime') or \
            (key == 'executionDuration') or \
            (key == 'destruction') or \
            (key == 'jobId') or \
            (key == 'runId') or \
            (key == 'ownerId') or \
            (key == 'quote')):
#
# {  single value return
#
            retval = 'None'
            keystr = 'uws:' + key
            outstr = ''

            if ((key == 'phase') or \
                (key == 'startTime') or \
                (key == 'endTime') or \
                (key == 'executionDuration') or \
                (key == 'destruction') or \
                (key == 'jobId') or \
                (key == 'runId')):

                retval = job[keystr]
                outstr = retval
    
#                outstr = f'    <{keystr:s}>{retval:s}</{keystr:s}>'
    
            elif ((key == 'ownerId') or \
                (key == 'quote')):

#                outstr = f'    <{keystr:s} xsi:nil="true"/>'
                retval = '' 
   
            if debug:
                logging.debug ('')
                logging.debug (f'retval= {retval:s}')
                logging.debug (f'keystr= {keystr:s}')
                logging.debug (f'outstr= {outstr:s}')
    
            self.__printStatus__ (key, retval, 'plain')
            sys.exit()

#
# }  end single value return
#
            
#
# {   key: return error 
#
        phase = job['uws:phase']
   
        if ((key == 'errorSummary') or \
            (key == 'errmsg') or \
            (key == 'error')):
            
            retval = ''
            errorSummary = ''
            errmsg = ''
        
            if (phase.lower() == 'error'): 

                try:
                    errorSummary = job['uws:errorSummary']
                    errmsg = job['uws:errorSummary']['uws:message']
                except:
                    pass
    
            if debug:
                logging.debug ('')
                logging.debug (f'errmsg: {errmsg:s}')

            if (len (errmsg) > 0):
                outstr = f'        <uws:message>{errmsg:s}</uws:message>'
            else:
                outstr = ''

            self.__printStatus__ ('errorSummary', outstr, 'xml')
            sys.exit()

#
# }  end return error 
#

#
# {  input key: result, results, resulturl 
#

        results = 'None'
        result = 'None'
        resulturl = 'None'

        if (key == 'resulturl'):
            key = 'results/resulturl'
        if (key == 'result'):
            key = 'results/result'

        if ((key == 'results') or \
            (key == 'results/result') or \
            (key == 'results/resulturl')):

            try:
                results = job['uws:results']
                result = job['uws:results']['uws:result']

                if (phase.lower() == 'completed'):
                    resulturl = job['uws:results']['uws:result']['@xlink:href']
	
            except:
                if debug:
                    logging.debug ('')
                    logging.debug ('error retrieving result')
                pass
        
        if debug:
            logging.debug ('')
            logging.debug (f'resulturl: {resulturl:s}')
            logging.debug (f'result:')
            logging.debug (result)
            logging.debug (f'results:')
            logging.debug (results)
      
         
        if ((key == 'results') or \
            (key == 'results/resulturl')):

            if debug:
                logging.debug ('')
                logging.debug (f'case1: results/resulturl')
            
            outstr = f'        <uws:result id="result" xlink:type="simple" xlink:href="{resulturl:s}"/>'
        
            self.__printStatus__ (key, outstr, 'xml')
            sys.exit()

        if debug:
            logging.debug ('')
            logging.debug (f'case2: result')
            
        if (len(resulturl) == 0):
            msg = 'resulturl not found.'
            self.__printError__ (format, msg)

#
# last case: 'results/result' -- return result table 
#
                
        indx = resulturl.find(workspace)
        substr = resulturl[indx:]

        resultpath = workdir + '/TAP/' + substr
        if debug:
            logging.debug ('')
            logging.debug (f'resultpath= {resultpath:s}')

        fp = None
        try:
            fp = open (resultpath, 'r')
        except Exception as e:
            msg = 'Failed to open result file: ' + resultpath
            self.__printError__ (format, msg)

        print ("HTTP/1.1 200 OK\r")

        if (format == 'json'):
            print("Content-type: application/json\r")
        elif (format == 'votable'):
            print("Content-type: text/xml\r")
        else:
            print("Content-type: text/plain\r")
        print("\r")

        try:
            while True:

                line = fp.readline()

                if not line:
                    break
        
                sys.stdout.write (line)
                sys.stdout.flush()

        except Exception as e:
            self.__printError__ (format, str(e))
        
        fp.close()
        sys.exit()
#
# }  end return result 
#

#
# }  end of getStatus
#



    def __printError__ (self, fmt, errmsg):
#
# { 
#
        debug = 0
    
        if debug:
            logging.debug ('')
            logging.debug (f'Enter printError: fmt= {fmt:s}')

        print ("HTTP/1.1 200 OK\r")
    
        if (fmt == 'votable'):
        
            if debug:
                logging.debug ('')
                logging.debug (f'xxx1')

            print("Content-type: text/xml\r")
            print("\r")

            print ('<?xml version="1.0" encoding="UTF-8"?>')
            print ('<VOTABLE version="1.4" xmlns="http://www.ivoa.net/xml/VOTable/v1.3">')
            print ('<RESOURCE type="results">')
            print ('<INFO name="QUERY_STATUS" value="ERROR">')
        
            print (errmsg)
        
            print ('</INFO>')
            print ('</RESOURCE>')
            print ('</VOTABLE>')

        else:
            if debug:
                logging.debug ('')
                logging.debug ('xxx2')

            print("Content-type: application/json\r")
            print("\r")
    
            print ("{")
            print ('    "status": "error",');
            print ('    "msg": "%s"' % errmsg);
            print ("}")
    
        sys.stdout.flush()
        sys.exit()
#
# }  end of printError
#



    def __writeAsyncError__ (self, errmsg, statuspath, statdict, param, **kwargs):
#
# { 
#
        debug = 1 

        if ('debug' in kwargs):
            debug = kwargs['debug']

            if debug:
                logging.debug ('')
                logging.debug (f'debug= {debug:d}')
    
        if debug:
            logging.debug ('')
            logging.debug ('Enter writeAsyncError')
            logging.debug (f'statuspath= {statuspath:s}')
            logging.debug (f'errmsg= {errmsg:s}')
            logging.debug (f'format= {param["format"]:s}')
            logging.debug (f'phase= {statdict["phase"]:s}')
            for key in param:
                if (key == 'maxrec'):
                    logging.debug (f'key= {key:s} val= {param[key]:d}')
                else:
                    logging.debug (f'key= {key:s} val= {param[key]:s}')


#
#    set status parameters
#
        etime = datetime.datetime.now()
        endtime = etime.strftime ('%Y-%m-%dT%H:%M:%S.%f')[:-4]
        
        durationtime = etime - statdict['stime']
        duration = str(durationtime.total_seconds())[:4] 

        statdict['endtime'] = endtime 
        statdict['duration'] = duration 
	
        statdict['phase'] = 'ERROR'
        statdict['errmsg'] = errmsg 
        
        if debug:
            logging.debug ('')
            logging.debug ('call writeStatusMsg')
            logging.debug(f'statuspath= {statuspath:s}')
            logging.debug(f'format= {param["format"]:s}')
            logging.debug(f'maxrec= {param["maxrec"]:d}')

        if debug:
            self.__writeStatusMsg__ (statuspath, statdict, param, debug=1)    
        else:
            self.__writeStatusMsg__ (statuspath, statdict, param)    

        if debug:
            logging.debug ('')
            logging.debug ('returned writeStatusMsg')

        sys.exit()
#
# }  end of writeAsyncError
#



    def __printSyncResult__ (self, resultpath, format, **kwargs):
#
# { 
#
        debug = 0 

        if ('debug' in kwargs):
            debug = kwargs['debug']

        if debug:
            logging.debug ('')
            logging.debug ('Enter printSyncResult')
            logging.debug (f'resultpath= {resultpath:s}')
            logging.debug (f'format= [{format:s}]')

        print ("HTTP/1.1 200 OK\r")

        fp = None
        try:
            fp = open (resultpath, 'r')
        except IOERROR:
            msg = 'Failed to open result file.'
            self.__printError__ (msg)

        if (format == 'json'):
            print("Content-type: application/json\r")
        elif (format == 'votable'):
#            print("Content-type: application/xml\r")
            print("Content-type: text/xml\r")
        else:
            print("Content-type: text/plain\r")
        print("\r")
    
        while True:

            line = fp.readline()
        
            if debug:
                logging.debug ('')
                logging.debug ('here3-1')
                logging.debug (f'line= [{line:s}]')

            if not line:
                break

            sys.stdout.write (line)
            sys.stdout.flush()

        fp.close()
        return

#
# }  end of printSyncResult
#



#
#   place holder for re-direct sync response case
#
    def __printSyncResponse__ (self, status, msg, resulturl, format, **kwargs):
#
# {
#
        debug = 0

        if ('debug' in kwargs):
            debug = kwargs['debug']

        if debug:
            logging.debug ('')
            logging.debug ('Enter printSyncResponse')
            logging.debug (f'resulturl= {resulturl:s}')

        if (status == 'error'):

            if debug:
                logging.debug ('')
                logging.debug ('xxx1')

            print ("HTTP/1.1 200 OK\r")
            print("Content-type: application/json\r")
            print("\r")
    
            print ("{")
            print ('    "status": "error",');
            print ('    "msg": "%s"' % errmsg);
            print ("}")

        else:
            if debug:
                logging.debug ('')
                logging.debug ('xxx2')

            print ("HTTP/1.1 303 See Other\r")
            print (f"Location: %s\r\n\r" % resulturl)
            print (f"Redirect Location: %s" % resulturl)
    
        sys.stdout.flush()
        if debug:
            logging.debug ('')
            logging.debug ('done')

        return
#
#}
#


#
#    async: return statusurl and kill the parent process
#
    def __printAsyncResponse__ (self, statusurl, **kwargs):

        debug = 0 

        if ('debug' in kwargs):
            debug = kwargs['debug']

        if debug:
            logging.debug ('')
            logging.debug ('Enter printAsyncResponse')
            logging.debug (f'statusurl= {statusurl:s}')

        print ("HTTP/1.1 303 See Other\r")
        print (f"Location: %s\r\n\r" % statusurl)
        print (f"Redirect Location: %s" % statusurl)
        sys.stdout.flush()
    
        time.sleep(2.0)

#        if debug:
#            logging.debug ('')
#            logging.debug ('sleep 2 msec before terminating parent process')

#
#  shut down parent program
#
        os.kill (os.getppid(), signal.SIGKILL)

        if debug:
            logging.debug ('')
            logging.debug ('parent process killed')

        return


#
#    TAP status result always written in xml format
#
    def __writeStatusMsg__ (self, statuspath, statdict, param, **kwargs):
#
# { end writeStatusMsg
#
        debug = 1

        if ('debug' in kwargs):
            debug = kwargs['debug']

        if debug:
            logging.debug ('')
            logging.debug ('Enter writeStatusMsg')
            logging.debug (f"statuspath= {statuspath:s}")
            logging.debug (f"phase= {statdict['phase']:s}")
            logging.debug (f"errmsg= {statdict['errmsg']:s}")
        
            for key in param:
                if (key == 'maxrec'):
                    logging.debug (f'key= {key:s} val= {param[key]:d}')
                else:
                    logging.debug (f'key= {key:s} val= {param[key]:s}')


        format = param['format'].lower()
        maxrec = param['maxrec']

        if debug:
            logging.debug ('')
            logging.debug (f"format= {format:s}")
            logging.debug (f"maxrec= {maxrec:d}")

        fp = None
        try:
            fp = open (statuspath, 'w+')
            os.chmod(statuspath, 0o664)
        
        except Exception as e:
            msg = 'Failed to open/create status file.'
            self.__printError__ (format, msg)

        if debug:
            logging.debug ('')
            logging.debug ('status file opened')

        try:
            fcntl.lockf (fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except Exception as e:
        
            msg = 'Cannot lock status file.'
            if debug:
                logging.debug ('')
                logging.debug (f'{msg:s}: {str(e):s}')

            self.__printError__ (format, msg)

        if debug:
            logging.debug ('')
            logging.debug ('status file locked for write')

        phase = statdict['phase']

        resulturl = statdict['resulturl']
        errmsg = statdict['errmsg']
    
        if debug:
            logging.debug ('')
            logging.debug (f'phase= [{phase:s}]')
            logging.debug (f'resulturl= [{resulturl:s}]')
            logging.debug (f'errmsg= [{errmsg:s}]')

        fp.write ('<?xml version="1.0" encoding="UTF-8"?>\n')
        
        fp.write ('<uws:job xmlns:uws="http://www.ivoa.net/xml/UWS/v1.0" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.ivoa.net/xml/UWS/v1.0 http://www.ivoa.net.xml/UWS/v1.0">\n')
        
        fp.write (f"    <uws:jobId>{statdict['jobid']:s}</uws:jobId>\n")
        fp.write (f"    <uws:runId>{statdict['process_id']:d}</uws:runId>\n")
        fp.write ('    <uws:ownerId xsi:nil="true"/>\n')
        fp.write (f"    <uws:phase>{statdict['phase'].upper():s}</uws:phase>\n")
        fp.write ('    <uws:quote xsi:nil="true"/>\n')
    
        fp.write (f"    <uws:startTime>{statdict['starttime']:s}</uws:startTime>\n") 
	
        fp.write (f"    <uws:endTime>{statdict['endtime']:s}</uws:endTime>\n") 
        
        fp.write (f"    <uws:executionDuration>{statdict['duration']:s}</uws:executionDuration>\n")

        fp.write (f"    <uws:destruction>{statdict['destruction']:s}</uws:destruction>\n")
    
        fp.write ('    <uws:parameters>\n')
       
        format = param['format']

        fp.write (f'        <uws:parameter id="format">{format:s}</uws:parameter>\n') 
        
        lang = param['lang']

        fp.write (f'        <uws:parameter id="lang">{lang:s}</uws:parameter>\n') 

        fp.write (f'        <uws:parameter id="maxrec">{maxrec:d}</uws:parameter>\n') 

#
#    encode query to escape '<' and '>'
#
        str1 = param['query']
      
        ind = str1.find ('<')
        while (ind >= 0):

            substr1 = str1[0:ind]
            substr2 = str1[ind+1:]

            str1 = substr1 + '&lt;' + substr2
            ind = str1.find ('<')
       
        if debug:
            logging.debug ('')
            logging.debug (f'str1= [{str1:s}]')

        ind = str1.find ('>')
        while (ind >= 0):

            substr1 = str1[0:ind]
            substr2 = str1[ind+1:]

            str1 = substr1 + '&gt;' + substr2
            ind = str1.find ('>')

        if debug:
            logging.debug ('')
            logging.debug (f'final query str1= [{str1:s}]')
         

        fp.write (f'        <uws:parameter id="query">{str1:s}')
        fp.write ('        </uws:parameter>\n')
    
        fp.write ('    </uws:parameters>\n')
  
        if (phase.lower() == 'completed'):

            fp.write ('    <uws:results>\n')
            fp.write (f'        <uws:result id="result" xlink:type="simple" xlink:href="{resulturl:s}"/>\n')
            fp.write ('    </uws:results>\n')
  
        elif (phase.lower() == 'error'):

            fp.write ('    <uws:errorSummary type="transient" hasDetail="true">\n')
            fp.write (f'        <uws:message>{errmsg:s}</uws:message>\n')
            fp.write ('    </uws:errorSummary>\n')
  
        fp.write ('</uws:job>\n')
	
        fp.flush()
        fp.close()

#
#    Note: closing file automatically released the lock
#

        if debug:
            logging.debug ('')
            logging.debug ('done writeStatusMsg')

        return
#
# } end writeStatusMsg
#


    def __getDatalevel__ (self, dbtable, **kwargs):

        debug = 0 

        if ('debug' in kwargs):
            debug = kwargs['debug']

        if debug:
            logging.debug ('')
            logging.debug (f'dbtable= {dbtable:s}')

        level = ["l0", \
                 "l1", \
                 "l2", \
                 "eng"]

        nlevel = len(level)

        if debug:
            logging.debug ('')
            logging.debug (f'nlevel= {nlevel:d}')

        datalevel = ''
        for i in range (nlevel):

            dbtable_lower = dbtable.lower()
	
            ind = dbtable_lower.find (level[i])
	
            if (ind != -1):
                datalevel = level[i]
                break
    
        if debug:
            logging.debug ('')
            logging.debug (f'ind= {ind:d}')
            logging.debug (f'datalevel= {datalevel:s}')

        return (datalevel)

#
# } end tap class
#


if __name__=="__main__":
    
    import sys
    main()
    
