#!/usr/bin/env python
#
#
#  nph-tap.py 
#
#  This is the cgi program to process the TAP query submitted by client  
# 
#    1.  extract input CGI parameters,
#
#    2.  extract parameters from TAP config file (configparam.py), 
#        the config file name is TAP.ini and its path is specified by 
#        the environment variable 'TAP_CONF',
#
#        config parameters include web server and database server parameters,
#
#    3.  convert ADQL query to ORACLE query (adql.py),
#
#    4.  perform database retrieval (runquery.py), or
#        database retrieval with proprietary filtering (propfilter.py)
#
#
#  Inputs parameters:
#
#    query (char):  an ADQL query
#    format (char): votable, ipac, cvs, or tvs format (alternatively 'responseformat')
#    maxrec (int): integer number of records to be returned
#
#
#  Default inputs:
#
#        REQUEST:  doQuery,
#        LANG:     ADQL,
#        PHASE:    RUN,
#        FORMAT:   votable,
#        MAXREC:   0 (i.e. unlimited)
#
#  Date: February 05, 2019 (Mihseh Kong)
#
#      Database retrieval module is in C
#
#
#  Revision: June 03, 2019 (Mihseh Kong)
#
#      Consolidate all config parameters to Tap.ini in IBM .ini format
#
#
#  Revision: July 19, 2019 (Mihseh Kong)
#
#      Write database retrieval modules in python, added/modified:
#
#      nph-tap.py,
#      configparam.py,
#      datadictionary.py,
#      runquery.py,
#      propfilter.py,
#      writeresult.py
#
#
#  Revision: August 1, 2019 (Mihseh Kong)
#
#      Add two input columns: fileid and accessid in TAP.ini so different 
#      projects (KOA and NEID) may share the same propfilter.py module.
#
#  Revision: August 8, 2019 (Mihseh Kong)
#
#      make up output format for columns not in DD based on cx_Oracle
#      cursor description array, and added them to DD
#       
#      Add racol, deccol in config file
#
#  Revision: December 01, 2019 (Mihseh Kong)
#
#      write writerecsmodule.c to do the IO in C for speed 
#       

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

from adql import ADQL
from spatial_index import SpatialIndex
from datadictionary import dataDictionary
from runquery import runQuery
from configparam import configParam
from propfilter import propFilter

from tablenames import TableNames 


def main():
#
# { 
#
    pid = os.getpid()
   
    debugtime = 0
    debug = 0
    debugfname = ''

#    debugfname = '/neid/cm/ws/mihseh/base/src/wsvc/TAP/tap_' +  \
#        str(pid) + '.debug'
#    debugfname = '/koa/cm/ws/mihseh/base/src/wsvc/TAP/tap_' +  \
#        str(pid) + '.debug'
#    debugfname = '/home/mihseh/tap/server/tap_' +  str(pid) + '.debug'

    debugfname = '/tmp/tap_' + str(pid) + '.debug'
    
    form = cgi.FieldStorage()
  
    if ('debug' in form):
        debug = 1
    
    if ('debugtime' in form):
        debugtime = 1
   
    if ((debug or debugtime) and (len(debugfname) > 0)):
      
        logging.basicConfig (filename=debugfname, level=logging.DEBUG)
    
        with open (debugfname, 'w') as fdebug:
            pass

    if debug:
        logging.debug ('')
        logging.debug (f'Enter nph-tap: pid= {pid:d}')

#
#    print all environ keys, retrieve async/sync spec from PATH_INFO
#    environ variable
#
    if debug:
        logging.debug ('')
        logging.debug ('Environment parameters:')
        logging.debug ('')

        for key in os.environ.keys():
            logging.debug (f'{key:s}: {os.environ[key]:20s}')
  
    if debug:
        logging.debug ('')
        logging.debug ('got here 0')
        logging.debug ('')

    if debugtime:
        time00 = datetime.datetime.now()
        time0 = datetime.datetime.now()
        logging.debug ('')
        logging.debug ('nph-tap start:')

    sql = ''
    servername = ''
    dbtable = ''
    ddtable = ''
    
    dd = None
    
#
#    default values; initialize phase to PENDING
#
    param = dict()
    
    param['lang'] = 'ADQL'
    param['phase'] = ''
    param['request'] = 'doQuery'
    param['query'] = ''
    param['format'] = 'votable'
    param['maxrec'] = ''
   
    phase = ''
    errmsg = ''

    propflag = -1 
    datalevel = ''
    overflow = 0
    maxrec = -1 

    ntot = 0

#
#    retrieve input parameters
#
    status = ''
    msg = ''
    
#    form = cgi.FieldStorage()
  
    if debug:
        logging.debug ('')
        logging.debug (f'here0')
   

    querykey = 0
    for key in form:
        
        if debug:
            logging.debug ('')
            logging.debug (f'key= {key:s} val= {form[key].value:s}')
    
        if (key.lower() == 'propflag'):
            
            propflag = int(form[key].value)
            
            if debug:
                logging.debug ('')
                logging.debug (
                    f'input propflag= [{propflag:d}]')

        if debug:
            logging.debug ('')
            logging.debug (f'propflag= {propflag:d}')

        if (key.lower() == 'lang'):
            param['lang'] = form[key].value

        if (key.lower() == 'request'):
            param['request'] = form[key].value

        if (key.lower() == 'phase'):
            param['phase'] = form[key].value
        
            if debug:
                logging.debug ('')
                logging.debug (f'phase= {param["phase"]:s}')

        if (key.lower() == 'query'):
            param['query'] = form[key].value.strip()
            querykey = 1

        if (key.lower() == 'format'):
            param['format'] = form[key].value

        if (key.lower() == 'responseformat'):
            param['format'] = form[key].value

        if (key.lower() == 'maxrec'):
            
            param['maxrec'] = form[key].value
            
            if debug:
                logging.debug ('')
                logging.debug (f"param['maxrec']= {param['maxrec']:s}")
           
    nparam = len(param)

    if debug:
        logging.debug ('')
        logging.debug (f"param['format']= {param['format']:s}")

    if (len(param['maxrec']) > 0):
                
        try:
            maxrec = int(param['maxrec'])
            
        except Exception as e:
                
            msg = "Failed to convert input maxrec value [" + \
                param['maxrec'] + "] to integer."
            printError (param['format'], msg)
    else:
        param['maxrec'] = '-1'

    if debug:
        logging.debug ('')
        logging.debug (f'nparam= {nparam:d}')
        for key in param:
            logging.debug (f'key= {key:s} value= {param[key]:s}')

    format = param['format'].lower()
    
    if debug:
        logging.debug ('')
        logging.debug (f'format= {format:s}')
        logging.debug (f'maxrec= {maxrec:d}')
    
    tapcontext = ''
    getstatus = 0
    setstatus = 0
    id = ''
    statuskey = ''

    pathinfo = ''
    if ("PATH_INFO" in os.environ):
        pathinfo = os.environ["PATH_INFO"]

    if (len(pathinfo) == 0):
        msg = 'Failed to find PATH_INFO (e.g. sync, async) in URL.'
        printError (param['format'], msg)

    if debug:
        logging.debug ('')
        logging.debug (f'pathinfo= {pathinfo:s}')
    
    if (pathinfo[0] == '/'):
        pathinfo = pathinfo[1:]

    if debug:
        logging.debug ('')
        logging.debug (f'pathinfo= {pathinfo:s}')
    
    arr = pathinfo.split ('/')
    narr = len(arr)

    if debug:
        logging.debug ('')
        logging.debug (f'narr= {narr:d}')
        for i in range (narr):
            logging.debug (f'i= {i:d} arr= {arr[i]:s}')
         
    if (arr[0] == "async"):
        tapcontext = 'async'
    elif (arr[0] == "sync"):
        tapcontext = 'sync'
 
    if (narr > 1):
        
        getstatus = 1
        id = arr[1]
        
        if (len(id) == 0):
            msg = 'Failed to find jobid for retrieving job status.'
            printError (param['format'], msg)


        len_id = len(id)
        ind = pathinfo.find(id)
        i = ind+len_id + 1
        statuskey = pathinfo[i:]

        if (param['phase'] == 'RUN'):
            getstatus = 0
            setstatus = 1

    if debug:
        logging.debug ('')
        logging.debug (f'statuskey= {statuskey:s}')
        logging.debug (f'tapcontext= {tapcontext:s}')
        logging.debug (f'getstatus= {getstatus:d}')
        logging.debug (f'setstatus= {setstatus:d}')
        logging.debug (f'id= {id:s}')

    if ((format != 'votable') and \
        (format != 'ipac') and \
        (format != 'csv') and \
        (format != 'tsv')):

        if debug:
            logging.debug ('')
            logging.debug ('format error detected')
      
        msg = 'Response format (' + format + \
	    ') must be: votable, ipac, csv, or tsv'
        printError (param['format'], msg)
   
        
#    if ((getstatus == 0) and (querykey == 0)):
#        msg = "TAP keyword: 'query' not found."
#        printError (param['format'], msg)

#
#    retrieve cookiestr
#
    cookiestr = ''
    cookiestr = os.getenv ('HTTP_COOKIE', default='')

    if debug:
        logging.debug ('')
        logging.debug (f'cookiestr= {cookiestr:s}') 

#
#    extract configfile name from TAP_CONF environment variable
#    Note: make sure TAP_CONF env var is set
#
    configpath = ''
    if ('TAP_CONF' in os.environ):
        configpath = os.environ['TAP_CONF']
    else:
        if debug:
            logging.debug ('')
            logging.debug ('Failed to find TAP_CONF environment variable.')
        
        msg = 'Failed to find TAP_CONF environment variable.'
        printError (format, msg)

    if debug:
        logging.debug ('')
        logging.debug (f'configpath= {configpath:s}')

#
#    retrieve config variables
#
    config = None
    try:
#        config = configParam (configpath, debug=1)
        config = configParam (configpath)
    
        if debug:
            logging.debug ('')
            logging.debug ('returned config')

    except Exception as e:

        if debug:
            logging.debug ('')
            logging.debug (f'config exception: {str(e):s}') 
        
        printError (format, str(e))

    workdir = config.workdir
    workurl = config.workurl
    httpurl = config.httpurl
    cgipgm  = config.cgipgm 
    cookiename = config.cookiename
   
    if debug:
        logging.debug ('')
        logging.debug (f'workdir= {workdir:s}') 
        logging.debug (f'workurl= {workurl:s}') 
        logging.debug (f'httpurl= {httpurl:s}') 
        logging.debug (f'cgipgm= {cgipgm:s}') 
        logging.debug (f'cookiename= {cookiename:s}') 
        logging.debug (f'fileid= {config.fileid:s}') 
        logging.debug (f'accessid= {config.accessid:s}') 
        logging.debug (f'racol= {config.racol:s}') 
        logging.debug (f'deccol= {config.deccol:s}') 
        logging.debug (f'propfilter= {config.propfilter:s}') 

#
#    initialize statdict dict
#
    statdict = dict()

        
    statdict['process_id'] = pid 
    statdict['owneridlabel'] = 'ownerId xsi:nil="true"'
    statdict['quotelabel'] = 'quote xsi:nil="true"'
    statdict['errmsg'] = ''
    
    statdict['phase'] = ''
    statdict['jobid'] = '' 
    
    statdict['starttime'] = '' 
    statdict['destruction'] =  '' 
    statdict['endtime'] = '' 
    statdict['duration'] = '0' 

    statdict['resulturl'] = ''

#
#    sync or async without input workspace id: make workspace, 
#    otherwise retrieve workspace from getstatus id
#
    userWorkdir = ''
    workspace = ''
    
    if ((tapcontext == 'sync') or \
        ((tapcontext == 'async') and \
        (getstatus == 0) and \
        (setstatus == 0))):
#
#{  make workspace
#
        if debug:
            logging.debug ('')
            logging.debug (f'Async without workspace id: make workspace')

#
#    make TAP subdir if it doesn't exist 
#
        tapdir = workdir + '/TAP'

        try:
            os.makedirs (tapdir, exist_ok=True)
            os.chmod (tapdir, 0o775)
    
        except Exception as e:
            msg = 'Failed to create ' + tapdir + ': ' + str(e)
            printError (format , msg) 

        if debug:
            logging.debug ('')
            logging.debug (f'tapdir: {tapdir:s} created')

#
#    make a workspace name with unique id
#
        try:
            userWorkdir = tempfile.mkdtemp (prefix='tap_', dir=tapdir)
    
        except Exception as e:
            msg = 'tempfile.mkdtemp exception: ' + str(e)
            printError (format, msg) 

        if debug:
            logging.debug ('')
            logging.debug (f'returned tempfile.mkdtemp {userWorkdir:s}')
   
        ind = userWorkdir.rfind ('/') 
        if (ind > 0):
            workspace = userWorkdir[ind+1:]

        if debug:
            logging.debug ('')
            logging.debug (f'workspace= {workspace:s}')
   
   
        try:
            os.makedirs(userWorkdir, exist_ok=True)
            os.chmod (userWorkdir, 0o775)
    
        except Exception as e:
        
            msg = 'os.makedir exception: ' + str(e)
            printError (format, msg) 

        if debug:
            logging.debug ('')
            logging.debug (f'userWorkdir {userWorkdir:s} created')
#
#} end of make workspace
#
    else:
#
#{ retrieve workspace from id 
#
        workspace = id
        userWorkdir = workdir + '/TAP/' + workspace
        
#
#} end of retrieve workspace 
#
    if debug:
        logging.debug ('')
        logging.debug (f'workspace= {workspace:s}')
        logging.debug (f'userWorkdir= {userWorkdir:s}')

#
#   make up status and result table names
#
    statustbl = 'status.xml'
    statuspath = userWorkdir + '/' + statustbl
    statusurl = httpurl + '/' + cgipgm + \
        '/' + tapcontext + '/' + workspace

    if debug:
        logging.debug ('')
        logging.debug (f'statuspath= {statuspath:s}')
        logging.debug (f'statusurl= {statusurl:s}')
  

    resulttbl = ''
    if (format == 'json'):
        resulttbl = 'result.json'
    elif (format == 'votable'):
        resulttbl = 'result.xml'
    elif (format == 'ipac'):
        resulttbl = 'result.tbl'
    elif (format == 'csv'):
        resulttbl = 'result.csv'
    elif (format == 'tsv'):
        resulttbl = 'result.tsv'
    elif (format == 'html'):
        resulttbl = 'result.html'

    resultpath = userWorkdir + '/' + resulttbl
    resulturl = httpurl + workurl + '/TAP/' + workspace + '/' \
        + resulttbl 

    if debug:
        logging.debug ('')
        logging.debug (f'resultpath= {resultpath:s}')
        logging.debug (f'resulturl= {resulturl:s}')

#
#    if async and phase == PENDING: send 303 with statusurl and exit
#
    if ((tapcontext == 'async') and \
        (getstatus == 0) and \
        (setstatus == 0) and \
        (len(param['phase']) == 0)):
    
#
#{  if phase not specified: set to PENDING and exit
#

        statdict['phase'] = 'PENDING'
        statdict['jobid'] = workspace
    
        if debug:
            logging.debug ('')
            logging.debug ('call writeStatusMessage:')

#        writeStatusMsg (statuspath, statdict, param)    
        writeStatusMsg (statuspath, statdict, param, debug=1)    
    
    
        if debug:
            logging.debug ('')
            logging.debug ('returned writeStatusMessage')


        if debug:
            logging.debug ('')
            logging.debug (f'statusurl= {statusurl:s}')

        print ("HTTP/1.1 303 See Other\r")
        print (f"Location: %s\r\n\r" % statusurl)
        print (f"Redirect Location: %s" % statusurl)
        sys.stdout.flush()
    
        sys.exit()
#
#} end of PENDING case
#
    if debug:
        logging.debug ('')
        logging.debug (f'here1')

#
#    getstatus case: call getStatus mothod which reads status file: 
#    printStatus or error messages, then exit.
#
    if (getstatus == 1):
#
#{
#
        if debug:
            logging.debug ('')
            logging.debug (f'Case getstatus')

        try:
#            getStatus (workdir, id, statuskey, param)

            getStatus (workdir, id, statuskey, param, debug=1)
        
        except Exception as e:

            if debug:
                logging.debug ('')
                logging.debug (f'getStatus error: {str(e):s}') 
            
            printError (format , str(e)) 

#
#    getStatus will exit when done
#
#
#}
#
    if debug:
        logging.debug ('')
        logging.debug (f'here2')

#
#    setstatus to RUN case:
#    parse status file to get parameters
#
    if ((tapcontext == 'async') and \
#        (setstatus == 1) and \
        (param['phase'] == 'RUN')):
#
#{
#
        if debug:
            logging.debug ('')
            logging.debug (f'Case set phase RUN')

#        workspace = id
#        userWorkdir = workdir + '/TAP/' + workspace
        
#        statustbl = 'status.xml'
#        statuspath = userWorkdir + '/' + statustbl
#        statusurl = httpurl + '/cgi-bin/TAP/nph-tap.py/' + \
#            tapcontext + '/' + workspace

        if debug:
            logging.debug ('')
            logging.debug (f'workspace= {workspace:s}')
            logging.debug (f'userWorkdir= {userWorkdir:s}')
            logging.debug (f'statuspath= {statuspath:s}')
            logging.debug (f'statusurl= {statusurl:s}')
   
        if debug:
            logging.debug ('')
            logging.debug ('param: (from input)')
            logging.debug (f'format= {param["format"]:s}')
            logging.debug (f'lang= {param["lang"]:s}')
            logging.debug (f'maxrec= {param["maxrec"]:s}')
            logging.debug (f'query= {param["query"]:s}')
            logging.debug (f'phase= {param["phase"]:s}')
            
   
#
#    parse statuspath to retrieve parameters
#
        if (setstatus == 1):
        
            if debug:
                logging.debug ('')
                logging.debug (f'case setstatus=1')

            doc = None
            with open (statuspath, 'r') as fp:
                doc = fp.read()
        
            if debug:
                logging.debug ('')
                logging.debug ('doc=')
                logging.debug (doc)
        
            soup = BeautifulSoup (doc, 'lxml')

            parameters = soup.find ('uws:parameters')

            if debug:
                logging.debug ('')
                logging.debug ('parameters=')
                logging.debug (parameters)
       
            parameter = parameters.find (id='query')
            param['query'] = parameter.string

            parameter = parameters.find (id='format')
            param['format'] = parameter.string

            parameter = parameters.find (id='maxrec')
            param['maxrec'] = parameter.string

            parameter = parameters.find (id='lang')
            param['lang'] = parameter.string
        

            if debug:
                logging.debug ('')
                logging.debug ('param: (after retrieved from status.xml')
                logging.debug (f'format= {param["format"]:s}')
                logging.debug (f'lang= {param["lang"]:s}')
                logging.debug (f'maxrec= {param["maxrec"]:s}')
                logging.debug (f'query= {param["query"]:s}')
                logging.debug (f'phase= {param["phase"]:s}')
            
#
#    rewrite statustbl
#   
        statdict['process_id'] = pid 
        statdict['jobid'] = workspace

        statdict['phase'] = 'EXECUTING'

        stime = datetime.datetime.now()
        destructtime = stime + datetime.timedelta (days=4)

        if debug:
            logging.debug ('')
            logging.debug (f'got here: statusurl= {statusurl:s}')
            logging.debug (f"process_id= {statdict['process_id']:d}")
            logging.debug (f"jobid= {statdict['jobid']:s}")
            logging.debug (f'stime:')
            logging.debug (stime)
            logging.debug ('destructtime:')
            logging.debug (destructtime)

        starttime = stime.strftime ('%Y-%m-%dT%H:%M:%S.%f')[:-4]
        destruction = destructtime.strftime ('%Y-%m-%dT%H:%M:%S.%f')[:-4]
        if debug:
            logging.debug ('')
            logging.debug (f'starttime: {starttime:s}')
            logging.debug (f'destruction= {destruction:s}')
   
        statdict['stime'] = stime
        statdict['starttime'] = starttime
        statdict['destruction'] = destruction 
        statdict['endtime'] = '' 
        statdict['duration'] = '0' 
       
        statdict['resulturl'] = resulturl

        if debug:
            logging.debug ('')
            logging.debug ('call writeStatusMessage:')
            logging.debug (f'phase= {statdict["phase"]:s}')
       

#        writeStatusMsg (statuspath, statdict, param)    
    
        writeStatusMsg (statuspath, statdict, param, debug=1)    
    
        if debug:
            logging.debug ('')
            logging.debug ('returned writeStatusMessage')

#
#    generate return response and terminate parent process
#    before proceed to run the search program 
#
        if debug:
            logging.debug ('')
            logging.debug ('call printAsyncResponse')
   
#        printAsyncResponse (statusurl)

        printAsyncResponse (statusurl, debug=1)

        if debug:
            logging.debug ('')
            logging.debug ('returned printAsyncResponse')
#
#}
#
    if debug:
        logging.debug ('')
        logging.debug (f'here3')


#
#    run the query for both async and sync cases:
#    if query is blank, return error
#
    if (len(param['query']) == 0):

        msg = "Input 'query' is blank."

        if (tapcontext == 'async'):
            
            writeAsyncError (msg, statuspath, statdict, param)
        else: 
            printError (format, msg)

#
#  convert ADQL query to ORACLE query (this may be moved to the up so pending
#  case can reject bad adql query)
#
    if debug:
        logging.debug ('')
        logging.debug (f'here 4')

    query_adql = param['query']
    
    if debug:
        logging.debug ('')
        logging.debug (f'query_adql= {query_adql:s}')
   
    try:
        mode = SpatialIndex.HTM
        if(config.adqlparam['mode'] == 'HPX'):
            mode = SpatialIndex.HPX

        level   = int(config.adqlparam['level'])
        colname = config.adqlparam['colname']

        encoding = SpatialIndex.BASE4
        if(config.adqlparam['encoding'] == 'BASE10'):
            encoding = SpatialIndex.BASE10

        xcol = config.adqlparam['xcol']
        ycol = config.adqlparam['ycol']
        zcol = config.adqlparam['zcol']

        adql = ADQL(mode=mode, level=level, colname=colname, \
            encoding=encoding, xcol=xcol, ycol=ycol, zcol=zcol)

        query = adql.sql (query_adql)
    
        if debug:
            logging.debug ('')
            logging.debug (f'final query= {query:s}')

    except Exception as e:

        if debug:
            logging.debug ('')
            logging.debug (f'adql exception: {str(e):s}')
       
        if (tapcontext == 'async'):
            
            writeAsyncError (str(e), statuspath, statdict, param)
        else: 
            printError (format, str(e))

#
#    extract DB table name from query (This will be replaced with a library
#    parser
#
    """
    dbtable = ''
    try:
        query_lower = query.lower()

        ind = query_lower.find ('from ')
        
        if (ind != -1):
            substr = query[ind+5:]

        if debug:
            logging.debug ('')
            logging.debug (f'substr= [{substr:s}]')

        ind = substr.find(' ')
       
        if (ind != -1):
            dbtable = substr[0:ind]
        else:
            dbtable = substr
    except:
        pass
    """

    dbtable = ''
    try:
        tn = TableNames()

        tables = tn.extract_tables(query)

        dbtable = tables[0]
    except:
        
        if debug:
            logging.debug ('')
            logging.debug (f'TableName exception')

        pass

    if len(dbtable) == 0:

        msg = 'No table name found ADQL in query.'
        printError(format, msg);


    ddtable = dbtable + '_dd'
    
    if debug:
        logging.debug ('')
        logging.debug (f'dbtable= [{dbtable:s}]')
        logging.debug (f'ddtable= {ddtable:s}')

    """
    instrument = getInstrument (dbtable)

    if debug:
        logging.debug ('')
        logging.debug (f'instrument= [{instrument:s}]')
    """

    datalevel = getDatalevel (dbtable)

    if debug:
        logging.debug ('')
        logging.debug (f'datalevel= [{datalevel:s}]')

#
#    determine whether to use runQuery or propFilter to execute sql
#
    if (propflag == -1): 

        if debug:
            logging.debug ('')
            logging.debug (f'No input propflag:')

        if ((config.propfilter.lower() == 'koa') or \
            ((config.propfilter.lower() == 'neid') and (datalevel != 'l0'))):
            propflag = 1
        else:
            propflag = 0

    if debug:
        logging.debug ('')
        logging.debug (f'propflag= [{propflag:d}]')

    if debugtime:
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
#    propflag = 0

    maxrec = int (param['maxrec'])
    if debug:
        logging.debug ('')
        logging.debug (f'maxrec= {maxrec:d}')
            

    if (propflag == 0):
#
#    execute 'runQuery': 
#    return the result if sync, or 
#    update the status file if async 
#
        try:
            if debug:
                logging.debug ('')
                logging.debug ('will call runQuery')
            

            if (debug and debugtime):

                dbquery = runQuery (dbserver=config.dbserver, \
                    userid=config.dbuser, \
                    password=config.dbpassword, \
                    query=query, \
                    workdir=userWorkdir, \
                    format=format, \
                    maxrec=maxrec, \
                    racol=config.racol, \
                    deccol=config.deccol, \
                    debug=1, \
                    debugtime=1)
               
            elif debug:
            
                dbquery = runQuery (dbserver=config.dbserver, \
                    userid=config.dbuser, \
                    password=config.dbpassword, \
                    query=query, \
                    workdir=userWorkdir, \
                    format=format, \
                    maxrec=maxrec, \
                    racol=config.racol, \
                    deccol=config.deccol, \
                    debug=1)
                    
            elif debugtime:
            
                dbquery = runQuery (dbserver=config.dbserver, \
                    userid=config.dbuser, \
                    password=config.dbpassword, \
                    query=query, \
                    workdir=userWorkdir, \
                    format=format, \
                    maxrec=maxrec, \
                    racol=config.racol, \
                    deccol=config.deccol, \
                    debugtime=1)
               
            else:
                dbquery = runQuery (dbserver=config.dbserver, \
                    userid=config.dbuser, \
                    password=config.dbpassword, \
                    query=query, \
                    workdir=userWorkdir, \
                    format=format, \
                    maxrec=maxrec, \
                    racol=config.racol, \
                    deccol=config.deccol)

            phase = 'COMPLETED'
            ntot = dbquery.ntot

            if debug:
                logging.debug ('')
                logging.debug ('returned runQuery')

        except Exception as e:
   
            if debug:
                logging.debug ('')
                logging.debug (f'runQuery exception: {str(e):s}')

            phase = 'ERROR'
            errmsg = str(e)
            
            if (tapcontext == 'async'):
            
                writeAsyncError (str(e), statuspath, statdict, param)
            else: 
                printError (format, str(e))

        if debug:
            logging.debug ('')
            logging.debug (f'Done runQuery: outpath= {dbquery.outpath:s}')

#
#    end runquery
#
        if debugtime:
            time1 = datetime.datetime.now()
            delt = (time1 - time0).total_seconds()
            time0 = time1
            logging.debug ('')
            logging.debug (f'time (runquery): {delt:f}s')

    else:
#
#    run propFilter
#
    
        propfilter = None
        try:
            if debug:
                logging.debug ('')
                logging.debug ('will call propFilter')

            if (debug and debugtime):

                propfilter = propFilter (dbserver=config.dbserver, \
                    userid=config.dbuser, \
                    password=config.dbpassword, \
                    query=query, \
                    workdir=userWorkdir, \
                    racol=config.racol, \
                    deccol=config.deccol, \
                    cookiename=config.cookiename, \
                    cookiestr=cookiestr, \
                    propfilter=config.propfilter.lower(), \
                    usertbl=config.usertbl, \
                    accesstbl=config.accesstbl, \
                    fileid=config.fileid, \
                    accessid=config.accessid, \
                    format=format, \
                    maxrec=maxrec, \
                    debugtime=1, \
                    debug=1)

            elif debug:

                propfilter = propFilter (dbserver=config.dbserver, \
                    userid=config.dbuser, \
                    password=config.dbpassword, \
                    query=query, \
                    workdir=userWorkdir, \
                    racol=config.racol, \
                    deccol=config.deccol, \
                    cookiename=config.cookiename, \
                    cookiestr=cookiestr, \
                    propfilter=config.propfilter.lower(), \
                    usertbl=config.usertbl, \
                    accesstbl=config.accesstbl, \
                    fileid=config.fileid, \
                    accessid=config.accessid, \
                    format=format, \
                    maxrec=maxrec, \
                    debug=1)

            elif debugtime:

                propfilter = propFilter (dbserver=config.dbserver, \
                    userid=config.dbuser, \
                    password=config.dbpassword, \
                    query=query, \
                    workdir=userWorkdir, \
                    racol=config.racol, \
                    deccol=config.deccol, \
                    cookiename=config.cookiename, \
                    cookiestr=cookiestr, \
                    propfilter=config.propfilter.lower(), \
                    usertbl=config.usertbl, \
                    accesstbl=config.accesstbl, \
                    fileid=config.fileid, \
                    accessid=config.accessid, \
                    format=format, \
                    maxrec=maxrec, \
                    debugtime=1)

            else:
                propfilter = propFilter (dbserver=config.dbserver, \
                    userid=config.dbuser, \
                    password=config.dbpassword, \
                    query=query, \
                    workdir=userWorkdir, \
                    racol=config.racol, \
                    deccol=config.deccol, \
                    cookiename=config.cookiename, \
                    cookiestr=cookiestr, \
                    propfilter=config.propfilter.lower(), \
                    usertbl=config.usertbl, \
                    accesstbl=config.accesstbl, \
                    fileid=config.fileid, \
                    accessid=config.accessid, \
                    format=format, \
                    maxrec=maxrec)

                
            phase = 'COMPLETED'
            ntot = propfilter.ntot

            if debug:
                logging.debug ('')
                logging.debug ('returned propFilter: phase= {phase:s}')
                logging.debug (f'ntot= {ntot:d}')

        except Exception as e:
   
            if debug:
                logging.debug ('')
                logging.debug (f'propFilter exception: {str(e):s}')

            phase = 'ERROR'
            errmsg = str(e)

            if (tapcontext == 'async'):
                
                if debug:
                    logging.debug ('')
                    logging.debug (f'call writeAsyncError')

                writeAsyncError (str(e), statuspath, statdict, param)
            
                if debug:
                    logging.debug ('')
                    logging.debug (f'returned writeAsyncError')

            else: 
                if debug:
                    logging.debug ('')
                    logging.debug (f'call printError')

                printError (format, str(e))

#
#    end propfilter 
#
        if debug:
            logging.debug ('')
            logging.debug (f'Done propfilter: outpath= {propfilter.outpath:s}')

        if debugtime:
            time1 = datetime.datetime.now()
            delt = (time1 - time0).total_seconds()
            logging.debug ('')
            logging.debug (f'time (propfilter): {delt:f}s')
        
#
#    job done
#

#
#    async: write complete status  message 
#
    if (tapcontext == 'async'):
                
        if debug:
            logging.debug ('')
            logging.debug (f'case async')
       
        if debugtime:
            time0 = datetime.datetime.now()
        
        etime = datetime.datetime.now()
        endtime = etime.strftime ('%Y-%m-%dT%H:%M:%S.%f')[:-4]
        
        durationtime = etime - statdict['stime']
        duration = str(durationtime.total_seconds())[:4] 

        statdict['endtime'] = endtime 
        statdict['duration'] = duration 
	
        if debug:
            logging.debug ('')
            logging.debug (f'phase= {phase:s}')
            logging.debug (f'errmsg= {errmsg:s}')
      
        statdict['phase'] = phase
        statdict['errmsg'] = errmsg 
        
        if debug:
            logging.debug ('')
            logging.debug ('call writeStatusMsg')
            logging.debug (f'phase= {statdict["phase"]:s}')
       
       
#        time.sleep (2.0)
        writeStatusMsg (statuspath, statdict, param)    
        
        if debug:
            logging.debug ('')
            logging.debug (f'returned writeStatusMsg')
       
        if debugtime:
            time1 = datetime.datetime.now()
            delt = (time1 - time0).total_seconds()
            logging.debug ('')
            logging.debug (f'time (async: writeStatusMsg): {delt:f}s')

            delt = (time1 - time00).total_seconds()
            logging.debug ('')
            logging.debug (f'time (total nph-tap completion): {delt:f}s')

    else:
        if debug:
            logging.debug ('')
            logging.debug (f'case sync')
       
        if debug:
            logging.debug ('')
            logging.debug (f'call printSyncResult')
       
        if debugtime:
            time0 = datetime.datetime.now()
        
        printSyncResult (resultpath, format)

        if debug:
            logging.debug ('')
            logging.debug (f'returned printSyncResult')
      
        if debugtime:
            time1 = datetime.datetime.now()
            delt = (time1 - time0).total_seconds()
            logging.debug ('')
            logging.debug (f'time (sync: print return): {delt:f}s')

            delt = (time1 - time00).total_seconds()
            logging.debug ('')
            logging.debug (
                f'time (total nph-tap completion): {delt:f}s, ntot= {ntot:d}')

    if debug:
        logging.debug ('')
        logging.debug ('nph-tap done')

    sys.exit()

#
# }  end of main
#



def getStatusJob (data, **kwargs):
#
# { 
#
    pid = os.getpid()
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


def getStatusData (statuspath, **kwargs):
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



def getPhase (statuspath, **kwargs):
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
        data = getStatusData (statuspath)
        
        if debug:
            logging.debug ('')
            logging.debug ('data=')
            logging.debug (data)

    except Exception as e:
        
        msg = 'Error reading status file: ' + str(e)
        raise Exception (msg)

    job = None 
    try:
        job = getStatusJob (data)
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



def printStatus (key, retval, outtype, **kwargs):
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


def getStatus (workdir, workspace, key, param, **kwargs):
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
        data = getStatusData (statuspath)

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
        soup = BeautifulSoup (data, 'xml')
	
    except Exception as e:
        printError (format, str(e))

    format = 'votable'
    try:
        format = soup.parameters.findAll ('parameter', {'id':'format'})[0].get_text()
    except Exception as e:
        printError (format, str(e))
    
    if debug:
        logging.debug ('')
        logging.debug (f'format= {format:s}')

    job = None 
    try:
        job = getStatusJob (data)
    except Exception as e:
        msg = 'Exception retrieving job from status file: ' + str(e)
        raise Exception (msg)
   

    if (key == 'parameters'):

        parameters = None
        try:
            parameters = soup.find('uws:parameters')
        except Exception as e:
            printError (format, str(e))
   
        if debug:
            logging.debug ('')
            logging.debug ('parameters:')
            logging.debug (parameters)
        
        printStatus ('parameters', parameters, 'xml')
        sys.exit()

#
#    parse data to extract inparam
#
    job = None 
    try:
        job = getStatusJob (data)
    except Exception as e:
        msg = 'Exception retrieving job from status file: ' + str(e)
        raise Exception (msg)

    if ((key == 'phase') or \
        (key == 'startTime') or \
        (key == 'endTime') or \
        (key == 'executionDuration') or \
        (key == 'destruction') or \
        (key == 'jobId') or \
        (key == 'processId') or \
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
            (key == 'processId')):

            retval = job[keystr]
            outstr = retval
    
#            outstr = f'    <{keystr:s}>{retval:s}</{keystr:s}>'
    
        elif ((key == 'ownerId') or \
            (key == 'quote')):

#            outstr = f'    <{keystr:s} xsi:nil="true"/>'
            retval = '' 
   
        if debug:
            logging.debug ('')
            logging.debug (f'retval= {retval:s}')
            logging.debug (f'keystr= {keystr:s}')
            logging.debug (f'outstr= {outstr:s}')
    
        printStatus (key, retval, 'plain')
        sys.exit()

#
# }  end single value return
#
        retval = 'None'
#
#    key: error or result
#
    phase = job['uws:phase']
   
    if ((key == 'errorSummary') or \
        (key == 'errmsg') or \
        (key == 'error')):
#
# {  error or result
#
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
            logging.debug ('here2')
            logging.debug (f'errmsg: {errmsg:s}')

        if (len (errmsg) > 0):
            outstr = f'        <uws:message>{errmsg:s}</uws:message>'
        else:
            outstr = ''

        printStatus ('errorSummary', outstr, 'xml')
        sys.exit()

#
# }  end error or result 
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
        
        printStatus (key, outstr, 'xml')
        sys.exit()

#
# last case: return result table 
#
    if debug:
        logging.debug ('')
        logging.debug (f'case2: result')
            
    if (len(resulturl) == 0):
        msg = 'resulturl not found.'
        printError (format, msg)

                
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
        printError (format, msg)

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
        printError (format, str(e))
        
    fp.close()
    sys.exit()
#
# }  end of getStatus
#



def printError (fmt, errmsg):
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



def writeAsyncError (errmsg, statuspath, statdict, param, **kwargs):
#
# { 
#

    debug = 0 

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

    writeStatusMsg (statuspath, statdict, param)    
#    writeStatusMsg (statuspath, statdict, param, debug=1)    
    
    if debug:
        logging.debug ('')
        logging.debug ('returned writeStatusMsg')

#    return
    sys.exit()
#
# }  end of writeAsyncError
#



def printSyncResult (resultpath, format, **kwargs):
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
        printError (msg)

    if (format == 'json'):
        print("Content-type: application/json\r")
    elif (format == 'votable'):
#        print("Content-type: application/xml\r")
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
def printSyncResponse (status, msg, resulturl, format, **kwargs):

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
#    async: return statusurl and kill the parent process
#
def printAsyncResponse (statusurl, **kwargs):

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

#    if debug:
#        logging.debug ('')
#        logging.debug ('sleep 2 msec before terminating parent process')

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
def writeStatusMsg (statuspath, statdict, param, **kwargs):

    debug = 0

    if ('debug' in kwargs):
        debug = kwargs['debug']

    if debug:
        logging.debug ('')
        logging.debug ('Enter writeStatusMsg')
        logging.debug (f"statuspath= {statuspath:s}")
        logging.debug (f"phase= {statdict['phase']:s}")
        logging.debug (f"errmsg= {statdict['errmsg']:s}")
        logging.debug (f"format= {param['format']:s}")
        logging.debug (f"maxrec= {param['maxrec']:s}")

    fp = None
    try:
        fp = open (statuspath, 'w+')
        os.chmod(statuspath, 0o664)
    except Exception as e:
        msg = 'Failed to open/create status file.'
        printError (msg)

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

        printError (msg)

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
    fp.write (f"    <uws:processId>{statdict['process_id']:d}</uws:processId>\n")
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

    if (len(param['maxrec']) == 0):
        maxrec = -1
    else:
        maxrec = int (param['maxrec'])

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
         

    fp.write (f'        <uws:parameter id="query">{str1:s}\n')
    fp.write ('        </uws:parameter>\n')
    
    fp.write ('    </uws:parameters>\n')
  
    if (phase.lower() == 'completed'):

        fp.write ('    <uws:results>\n')
        fp.write (f'        <uws:result id="result" xlink:type="simple" xlink:href="{resulturl:s}"/>\n')
        fp.write ('    </uws:results>\n')
  
    elif (phase.lower() == 'error'):

        fp.write ('    <uws:errorSummary>\n')
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



def getDatalevel (dbtable, **kwargs):

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


if __name__=="__main__":
    import sys

    main()
    
