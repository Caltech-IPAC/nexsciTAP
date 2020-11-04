# Copyright (c) 2020, Caltech IPAC.
# This code is released with a BSD 3-clause license. License information is at
#   https://github.com/Caltech-IPAC/nexsciTAP/blob/master/LICENSE


import os
import sys
import fcntl

import logging

import datetime
import time
import signal

import cgi
import tempfile

import xmltodict
from bs4 import BeautifulSoup

from ADQL.adql import ADQL

from spatial_index import SpatialIndex

from TAP.runquery import runQuery
from TAP.configparam import configParam
from TAP.propfilter import propFilter
from TAP.tablenames import TableNames


class Tap:

    """
    This class is the main program to process the TAP query submitted by
    a web client, it performs the following functionality:

    1.  extract input parameters;

    2.  read parameters from TAP configuration file(TAP.ini), the path
        of the config file is specified by the environment variable
        'TAP_CONF',

        'TAP.ini' contains web server info, database server info,
        spatial index setting, and special column names for filtering
        the proprietary data;

    3.  convert ADQL query to local DBMS query (currently Oracle or SQLite3,
        with others to follow); and

    4.  retrieve metadata from database, applies proprietary filter if
        it is specified by the project.


    Input TAP parameters:
    ---------------------

        query(char):  an ADQL query(required)

        phase(char): the phase it input is either PENDING or RUN,
                      if not specified, set to PENDING.

        format(char): output metadata table format:
                       votable, ipac, cvs, or tvs; default is votable.

        maxrec(int): integer number of records to be returned;
                      if not specified, all records are returned.


    Date: February 05, 2019(Mihseh Kong)
    """

    #
    # { class tap
    #

    pid = os.getpid()
    form = cgi.FieldStorage()


    debug = 0

    debugfname = '/tmp/tap_' + str(pid) + '.debug'

    sql = ''
    servername = ''
    dbtable = ''
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


    def __init__(self, **kwargs):

        #
        # { tap.init()
        #

        if('debug' in self.form):
            self.debug = 1

        if(self.debug):
            logging.basicConfig(filename=self.debugfname,
                                format='%(levelname)-8s %(relativeCreated)d>  '
                                '%(filename)s %(lineno)d  '
                                '(%(funcName)s):   %(message)s',
                                level=logging.DEBUG)

        if self.debug:
            logging.debug(f'Enter Tap.init(): pid= {self.pid:d}')

        #
        # Print all environ keys, retrieve async/sync spec from PATH_INFO
        # environ variable
        #

        if self.debug:
            logging.debug('')
            logging.debug('Environment parameters:')
            logging.debug('')

            for key in os.environ.keys():
                logging.debug(f'      {key:s}: {os.environ[key]:20s}')


        #
        #  Default values; initialize phase to PENDING
        #

        self.param['lang'] = 'ADQL'
        self.param['phase'] = ''
        self.param['request'] = 'doQuery'
        self.param['query'] = ''
        self.param['format'] = 'votable'
        self.param['maxrec'] = -1

        self.querykey = 0

        if self.debug:
            logging.debug('')
            logging.debug('HTTP request keywords:\n')

        for key in self.form:

            if self.debug:
                logging.debug(f'      key: {key:<15}   val: {self.form[key].value:s}')

            if(key.lower() == 'propflag'):
                self.propflag = int(self.form[key].value)

            if(key.lower() == 'lang'):
                self.param['lang'] = self.form[key].value

            if(key.lower() == 'request'):
                self.param['request'] = self.form[key].value

            if(key.lower() == 'phase'):
                self.param['phase'] = self.form[key].value.strip()

            if(key.lower() == 'query'):
                self.param['query'] = self.form[key].value.strip()
                self.querykey = 1

            if(key.lower() == 'format'):
                self.param['format'] = self.form[key].value.strip()

            if(key.lower() == 'responseformat'):
                self.param['format'] = self.form[key].value.strip()

            if(key.lower() == 'maxrec'):

                self.maxrecstr = self.form[key].value

        self.nparam = len(self.param)

        self.format = self.param['format'].lower()

        if((self.format != 'votable')
           and (self.format != 'ipac')
           and (self.format != 'csv')
           and (self.format != 'tsv')):

            if self.debug:
                logging.debug('')
                logging.debug('format error detected')

            self.msg = 'Response format(' + self.format + \
                ') must be: votable, ipac, csv, or tsv'

            self.__printError__('votable', self.msg)


        self.maxrec = -1
        if(len(self.maxrecstr) > 0):

            try:
                maxrec_dbl = float(self.maxrecstr)

                self.maxrec = int(maxrec_dbl)

            except Exception as e:

                self.msg = "Failed to convert input maxrec value [" + \
                    self.param['maxrec'] + "] to integer."
                self.__printError__(self.format, self.msg)


        if self.debug:
            logging.debug('')
            logging.debug(f'format = {self.format:s}')
            logging.debug(f'maxrec = {self.maxrec:d}')

        self.param['maxrec'] = self.maxrec

        if self.debug:
            logging.debug('')
            logging.debug(f'nparam = {self.nparam:d}')

            for key in self.param:
                if(key == 'maxrec'):
                    logging.debug(f'key = {key:<15} value = {self.param[key]:d}')
                else:
                    logging.debug(f'key = {key:<15} value = {self.param[key]:s}')


        if("PATH_INFO" in os.environ):
            self.pathinfo = os.environ["PATH_INFO"]

        if(len(self.pathinfo) == 0):
            self.msg = 'Failed to find PATH_INFO(e.g. sync, async) in URL.'
            self.__printError__(self.format, self.msg)

        if(self.pathinfo[0] == '/'):
            self.pathinfo = self.pathinfo[1:]

        if self.debug:
            logging.debug('')
            logging.debug(f'pathinfo = {self.pathinfo:s}')

        arr = self.pathinfo.split('/')
        narr = len(arr)

        if(arr[0] == "async"):
            self.tapcontext = 'async'
        elif(arr[0] == "sync"):
            self.tapcontext = 'sync'

        if(narr > 1):

            self.getstatus = 1
            self.id = arr[1]

            if(len(self.id) == 0):
                
                self.msg = 'Failed to find jobid for retrieving job status.'
                self.__printError__(self.format, self.msg)

            len_id = len(self.id)
            ind = self.pathinfo.find(self.id)
            i = ind + len_id + 1
            self.statuskey = self.pathinfo[i:]

            if(self.param['phase'] == 'RUN'):
                self.getstatus = 0
                self.setstatus = 1

        if self.debug:
            logging.debug('')
            logging.debug(f'statuskey  = {self.statuskey:s}')
            logging.debug(f'tapcontext = {self.tapcontext:s}')
            logging.debug(f'getstatus  = {self.getstatus:d}')
            logging.debug(f'setstatus  = {self.setstatus:d}')
            logging.debug(f'id         = {self.id:s}')

        #
        # Retrieve cookiestr
        #

        self.cookiestr = os.getenv('HTTP_COOKIE', default='')

        if self.debug:
            logging.debug('')
            logging.debug(f'cookiestr = {self.cookiestr:s}')

        #
        #  Extract configfile name from TAP_CONF environment variable
        #  Note: make sure TAP_CONF env var is set
        #

        if('TAP_CONF' in os.environ):
            self.configpath = os.environ['TAP_CONF']
        else:
            if self.debug:
                logging.debug('')
                logging.debug('Failed to find TAP_CONF environment variable.')

            self.msg = 'Failed to find TAP_CONF environment variable.'
            self.__printError__(self.format, self.msg)

        if self.debug:
            logging.debug('')
            logging.debug(f'configpath = {self.configpath:s}')

        #
        # Retrieve config variables
        #

        self.config = None
        try:
            self.config = configParam(self.configpath, debug=self.debug)

        except Exception as e:

            if self.debug:
                logging.debug('')
                logging.debug(f'config exception: {str(e):s}')

            self.__printError__(self.format, str(e))

        self.workdir = self.config.workdir
        self.workurl = self.config.workurl
        self.httpurl = self.config.httpurl
        self.cgipgm  = self.config.cgipgm

        self.arraysize = self.config.arraysize

        self.cookiename = self.config.cookiename

        if self.debug:
            logging.debug('')
            logging.debug(f'workdir    = {self.workdir:s}')
            logging.debug(f'workurl    = {self.workurl:s}')
            logging.debug(f'httpurl    = {self.httpurl:s}')
            logging.debug(f'cgipgm     = {self.cgipgm:s}')
            logging.debug(f'arraysize  = {self.arraysize:d}')
            logging.debug(f'cookiename = {self.cookiename:s}')
            logging.debug(f'fileid     = {self.config.fileid:s}')
            logging.debug(f'accessid   = {self.config.accessid:s}')
            logging.debug(f'racol      = {self.config.racol:s}')
            logging.debug(f'deccol     = {self.config.deccol:s}')
            logging.debug(f'propfilter = {self.config.propfilter:s}')
            logging.debug(f'phase      = {self.param["phase"]:s}')

        #
        # Initialize statdict dict
        #

        self.statdict['process_id'] = self.pid
        self.statdict['owneridlabel'] = 'ownerId xsi:nil="true"'
        self.statdict['quotelabel'] = 'quote xsi:nil="true"'
        self.statdict['errmsg'] = ''

        self.statdict['phase'] = self.param['phase']
        if self.debug:
            logging.debug('')
            logging.debug(f'param[phase]= {self.param["phase"]:s}')
            logging.debug(f'statdict[phase]= {self.statdict["phase"]:s}')


        self.statdict['jobid'] = ''

        self.statdict['starttime'] = ''
        self.statdict['destruction'] = ''
        self.statdict['endtime'] = ''
        self.statdict['duration'] = '0'

        self.statdict['resulturl'] = ''

        #
        # sync or async without input workspace id: make workspace,
        # otherwise retrieve workspace from getstatus id
        #

        if((self.tapcontext == 'sync')
            or ((self.tapcontext == 'async')
                and (self.getstatus == 0)
                and (self.setstatus == 0))):

            #
            # {  Make workspace:
            #    make TAP subdir if it doesn't exist, and
            #    make a workspace name with unique id
            #

            if self.debug:
                logging.debug('')
                logging.debug('Async without workspace id: make workspace')

            tapdir = self.workdir + '/TAP'

            try:
                os.makedirs(tapdir, exist_ok=True)
                os.chmod(tapdir, 0o775)

            except Exception as e:
                self.msg = 'Failed to create ' + tapdir + ': ' + str(e)
                self.__printError__(self.format, self.msg)

            if self.debug:
                logging.debug('')
                logging.debug(f'tapdir: {tapdir:s} created')


            try:
                self.userWorkdir = tempfile.mkdtemp(prefix='tap_', dir=tapdir)

            except Exception as e:
                self.msg = 'tempfile.mkdtemp exception: ' + str(e)
                self.__printError__(self.format, self.msg)

            ind = self.userWorkdir.rfind('/')
            if(ind > 0):
                self.workspace = self.userWorkdir[ind + 1:]

            try:
                os.makedirs(self.userWorkdir, exist_ok=True)
                os.chmod(self.userWorkdir, 0o775)

            except Exception as e:

                self.msg = 'os.makedir exception: ' + str(e)
                self.__printError__(self.format, self.msg)

            if self.debug:
                logging.debug(f'userWorkdir: {self.userWorkdir:s} created')
            #
            # } end of make workspace
            #

        else:

            #
            # { Retrieve workspace from id
            #

            self.workspace = self.id
            self.userWorkdir = self.workdir + '/TAP/' + self.workspace

            #
            # } end of retrieve workspace
            #

        if self.debug:
            logging.debug('')
            logging.debug(f'workspace   = {self.workspace:s}')
            logging.debug(f'userWorkdir = {self.userWorkdir:s}')

        #
        # Make status and result table names
        #

        self.statustbl = 'status.xml'
        self.statuspath = self.userWorkdir + '/' + self.statustbl
        self.statusurl = self.httpurl + '/' + self.cgipgm + \
            '/' + self.tapcontext + '/' + self.workspace

        if self.debug:
            logging.debug('')
            logging.debug(f'statuspath  = {self.statuspath:s}')
            logging.debug(f'statusurl   = {self.statusurl:s}')


        self.resulttbl = ''
        if(self.format == 'votable'):
            self.resulttbl = 'result.xml'
        elif(self.format == 'ipac'):
            self.resulttbl = 'result.tbl'
        elif(self.format == 'csv'):
            self.resulttbl = 'result.csv'
        elif(self.format == 'tsv'):
            self.resulttbl = 'result.tsv'

        self.resultpath = self.userWorkdir + '/' + self.resulttbl
        self.resulturl = self.httpurl + self.workurl + '/TAP/' + \
            self.workspace + '/' + self.resulttbl

        if self.debug:
            logging.debug('')
            logging.debug(f'resultpath  = {self.resultpath:s}')
            logging.debug(f'resulturl   = {self.resulturl:s}')

        #
        # If async and phase == PENDING: send 303 with statusurl and exit
        #

        if self.debug:
            logging.debug('')
            logging.debug('Before setting phase to PENDING:\n')
            logging.debug(f'      tapcontext   = {self.tapcontext:s}')
            logging.debug(f'      getstatus    = {self.getstatus:d}')
            logging.debug(f'      setstatus    = {self.setstatus:d}')
            logging.debug(f'      param[phase] = {self.param["phase"]:s}')


        if((self.tapcontext == 'async')
           and (self.getstatus == 0)
           and (self.setstatus == 0)
           and (len(self.param['phase']) == 0)):

            #
            # { If phase not specified: set to PENDING and exit
            #

            self.statdict['phase'] = 'PENDING'
            self.statdict['jobid'] = self.workspace

            self.__writeStatusMsg__(self.statuspath, self.statdict,
                                    self.param)


            print("HTTP/1.1 303 See Other\r")
            print("Location: %s\r\n\r" % self.statusurl)
            print("Redirect Location: %s" % self.statusurl)

            sys.stdout.flush()

            if self.debug:
                logging.debug('')
                logging.debug('Return HTTP redirect to status.xml and exit.')

            sys.exit()

            #
            # } end of PENDING case
            #

        #
        # getStatus case: call getStatus method which reads status file:
        # printStatus or error messages, then exit.
        #

        if(self.getstatus == 1):

            #
            # {
            #

            try:
                self.__getStatus__(self.workdir, self.id, self.statuskey,
                                   self.param)

            except Exception as e:
                if(self.tapcontext == 'async'):
                
                    self.phase = 'ERROR'
                    self.__writeAsyncError__(str(e), self.statuspath,
                                         self.statdict, self.param)
                else: 
                    self.__printError__(self.format, str(e))

            #
            # getStatus will exit when done
            #
            # }
        #
        # setstatus to RUN case:
        # parse status file to get parameters
        #

        if((self.tapcontext == 'async') and (self.param['phase'] == 'RUN')):

            #
            # {
            #

            if self.debug:
                logging.debug('')
                logging.debug('Case set phase RUN')
                logging.debug('')
                logging.debug('param:(from input):\n')
                logging.debug(f'      format= {self.param["format"]:s}')
                logging.debug(f'      lang= {self.param["lang"]:s}')
                logging.debug(f'      maxrec= {self.param["maxrec"]:d}')
                logging.debug(f'      query= {self.param["query"]:s}')
                logging.debug(f'      phase= {self.param["phase"]:s}')

            #
            # Parse statuspath to retrieve parameters
            #

            if(self.setstatus == 1):

                #
                # {    setstatus = 1
                #

                doc = None
                with open(self.statuspath, 'r') as fp:
                    doc = fp.read()

                if self.debug:
                    logging.debug('')
                    logging.debug('XML doc:')
                    logging.debug('------------------------------------------')
                    logging.debug(doc)
                    logging.debug('------------------------------------------')

                soup = BeautifulSoup(doc, 'lxml')

                parameters = soup.find('uws:parameters')

                parameter = parameters.find(id='query')
                self.param['query'] = parameter.string
                if self.debug:
                    logging.debug('XML status parameters:\n')
                    logging.debug(f'      query = {self.param["query"]:s}')

                parameter = parameters.find(id='format')
                self.param['format'] = parameter.string
                self.format = parameter.string.lower()
                
                if self.debug:
                    logging.debug('')
                    logging.debug(f'      format = {self.format:s}')

#
#    rename resulttbl for async PENDING-->RUN case
#
                if(self.format == 'votable'):
                    self.resulttbl = 'result.xml'
                elif(self.format == 'ipac'):
                    self.resulttbl = 'result.tbl'
                elif(self.format == 'csv'):
                    self.resulttbl = 'result.csv'
                elif(self.format == 'tsv'):
                    self.resulttbl = 'result.tsv'

                self.resultpath = self.userWorkdir + '/' + self.resulttbl
                self.resulturl = self.httpurl + self.workurl + '/TAP/' + \
                    self.workspace + '/' + self.resulttbl

                if self.debug:
                    logging.debug('')
                    logging.debug(f'resultpath  = {self.resultpath:s}')
                    logging.debug(f'resulturl   = {self.resulturl:s}')


                parameter = parameters.find(id='maxrec')
                self.maxrecstr = parameter.string
 
                self.param['maxrec'] = int(parameter.string)
                self.maxrec = int(parameter.string)
                
                if self.debug:
                    logging.debug('')
                    logging.debug(f'      self.maxrec = {self.maxrec:d}')

                parameter = parameters.find(id ='lang')
                self.param['lang'] = parameter.string
                if self.debug:
                    logging.debug('')
                    logging.debug(f'      lang = {self.param["lang"]:s}\n')

                #
                # } end setstatus = 1
                #

            #
            # Rewrite statustbl
            #

            self.statdict['process_id'] = self.pid
            self.statdict['jobid'] = self.workspace

            self.statdict['phase'] = 'EXECUTING'

            stime = datetime.datetime.now()
            destructtime = stime + datetime.timedelta(days=4)

            if self.debug:
                logging.debug(f'statusurl = {self.statusurl:s}')
                logging.debug(f"process_id = {self.statdict['process_id']:d}")
                logging.debug(f"jobid = {self.statdict['jobid']:s}")
                logging.debug('stime:')
                logging.debug(stime)
                logging.debug('destructtime:')
                logging.debug(destructtime)

            starttime = stime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-4]
            destruction = destructtime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-4]

            if self.debug:
                logging.debug('')
                logging.debug(f'starttime: {starttime:s}')
                logging.debug(f'destruction= {destruction:s}')


            self.statdict['stime'] = stime
            self.statdict['starttime'] = starttime
            self.statdict['destruction'] = destruction
            self.statdict['endtime'] = ''
            self.statdict['duration'] = '0'
            self.statdict['resulturl'] = self.resulturl

            self.__writeStatusMsg__(self.statuspath, self.statdict,
                                    self.param)

            #
            # Generate return response and terminate parent process
            # before proceed to run the search program
            #

            self.__printAsyncResponse__(self.statusurl)
            #
            # }
            #
        #
        # { Run the query for both async and sync cases:
        #
        #   if query is blank, return error
        #

        if(len(self.param['query']) == 0):

            self.msg = "Input 'query' is blank."

            if(self.tapcontext == 'async'):

                self.phase = 'ERROR'

                self.__writeAsyncError__(self.msg, self.statuspath,
                                         self.statdict, self.param)
            else:
                self.__printError__(self.format, self.msg)

        #
        # Convert ADQL query to a local database query
        #

        query_adql = self.param['query']

        if self.debug:
            logging.debug('')
            logging.debug(f'ADQL query: {query_adql:s}\n')

        try:
            mode = SpatialIndex.HTM

            if(self.config.adqlparam['mode'] == 'HPX'):
                mode = SpatialIndex.HPX

            level   = int(self.config.adqlparam['level'])
            colname = self.config.adqlparam['colname']

            encoding = SpatialIndex.BASE4
            if(self.config.adqlparam['encoding'] == 'BASE10'):
                encoding = SpatialIndex.BASE10

            racol = self.config.racol
            deccol = self.config.deccol

            xcol = self.config.adqlparam['xcol']
            ycol = self.config.adqlparam['ycol']
            zcol = self.config.adqlparam['zcol']

            if self.debug:
                logging.debug(f'mode     = {mode:d}')
                logging.debug(f'level    = {level:d}')
                logging.debug(f'colname  = {colname:s}')
                logging.debug(f'encoding = {encoding:d}')
                logging.debug(f'racol    = {racol:s}')
                logging.debug(f'deccol   = {deccol:s}')
                logging.debug(f'xcol     = {xcol:s}')
                logging.debug(f'ycol     = {ycol:s}')
                logging.debug(f'zcol     = {zcol:s}')


            dbms = self.config.connectInfo['dbms']

            if self.debug:
                logging.debug('')
                logging.debug(f'dbms = {dbms:s}')


            adql = ADQL(dbms=dbms, mode=mode, level=level, indxcol=colname,
                        encoding=encoding, racol=racol, deccol=deccol,
                        xcol=xcol, ycol=ycol, zcol=zcol)

            if self.debug:
                logging.debug('')
                logging.debug(f'ADQL initialized')


            self.query = adql.sql(query_adql)

            if self.debug:
                logging.debug('')
                logging.debug(f'Query to DBMS: {self.query:s}')

        except Exception as e:

            if self.debug:
                logging.debug('')
                logging.debug(f'ADQL exception: {str(e):s}')

            if(self.tapcontext == 'async'):

                self.phase = 'ERROR'
                self.__writeAsyncError__(str(e), self.statuspath,
                                         self.statdict, self.param)
            else:
                self.__printError__(self.format, str(e))

        #
        # Extract DB table name from query(This will be replaced with a library
        # parser
        #

        self.dbtable = ''
        try:
            tn = TableNames()
            tables = tn.extract_tables(self.query)
            self.dbtable = tables[0]

        except Exception as e:
            if self.debug:
                logging.debug('')
                logging.debug('TableName exception')
            pass

        if len(self.dbtable) == 0:

            self.msg = 'No table name found in ADQL query.'
            
            if(self.tapcontext == 'async'):
                
                self.phase = 'ERROR'
                self.__writeAsyncError__(self.msg, self.statuspath,
                                         self.statdict, self.param)
            else:
                self.__printError__(self.format, self.msg)

        if self.debug:
            logging.debug('')
            logging.debug(f'dbtable = [{self.dbtable:s}]')

        self.datalevel = self.__getDatalevel__(self.dbtable)

        if self.debug:
            logging.debug('')
            logging.debug(f'datalevel = [{self.datalevel:s}]')

        #
        # Determine whether to use runQuery or propFilter to execute SQL
        #

        if(self.propflag == -1):

            if((self.config.propfilter.lower() == 'koa')
                or ((self.config.propfilter.lower() == 'neid')
                    and (self.datalevel != 'l0'))):
                self.propflag = 1
            else:
                self.propflag = 0

        #
        # Check if dbtable is tap_schema tables
        #

        ind = self.dbtable.lower().find('tap_schema')

        if(ind != -1):
            self.propflag = 0

            if self.debug:
                logging.debug('')
                logging.debug('tap_schema table queries: set propflag to 0')

        if self.debug:
            logging.debug('')
            logging.debug(f'propflag = [{self.propflag:d}]')

        dbquery = None
        propfilter = None

        #
        # Force proflag = 0 for debugging
        #

        if(self.propflag == 0):

            #
            # { Execute 'runQuery':
            #   return the result if sync, or
            #   update the status file if async
            #

            try:

                dbquery = runQuery(connectInfo=self.config.connectInfo,
                                   query=self.query,
                                   workdir=self.userWorkdir,
                                   format=self.format,
                                   maxrec=self.maxrec,
                                   arraysize=self.arraysize,
                                   racol=self.config.racol,
                                   deccol=self.config.deccol,
                                   debug=self.debug)

                self.phase = 'COMPLETED'
                self.ntot = dbquery.ntot

            except Exception as e:

                if self.debug:
                    logging.debug('')
                    logging.debug(f'runQuery exception: {str(e):s}')

                self.phase = 'ERROR'

                if(self.tapcontext == 'async'):

                    self.__writeAsyncError__(str(e), self.statuspath,
                                             self.statdict, self.param)

                else:
                    self.__printError__(self.format, str(e))
            #
            # } end runquery
            #

        else:

            #
            # { Run propFilter
            #

            propfilter = None
            try:
                propfilter = propFilter(connectInfo=self.config \
                                                        .connectInfo,
                                        query=self.query,
                                        workdir=self.userWorkdir,
                                        racol=self.config.racol,
                                        deccol=self.config.deccol,
                                        cookiename=self.config.cookiename,
                                        cookiestr=self.cookiestr,
                                        propfilter=self.config.propfilter \
                                                              .lower(),
                                        usertbl=self.config.usertbl,
                                        accesstbl=self.config.accesstbl,
                                        fileid=self.config.fileid,
                                        accessid=self.config.accessid,
                                        format=self.format,
                                        maxrec=self.maxrec,
                                        arraysize=self.arraysize,
                                        debug=self.debug)


                self.phase = 'COMPLETED'
                self.ntot = propfilter.ntot

            except Exception as e:

                if self.debug:
                    logging.debug('')
                    logging.debug(f'propFilter exception: {str(e):s}')

                self.phase = 'ERROR'
                self.errmsg = str(e)

                if(self.tapcontext == 'async'):

                    self.__writeAsyncError__(str(e), self.statuspath,
                                             self.statdict, self.param)
                else:
                    self.__printError__(self.format, str(e))
            #
            # } end propfilter
            #

        #
        # } finished run query
        #

        #
        # async: write complete status  message
        #

        if(self.tapcontext == 'async'):

            if self.debug:
                logging.debug('')
                logging.debug('Case: async')

            etime = datetime.datetime.now()
            endtime = etime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-4]

            durationtime = etime - self.statdict['stime']
            duration = str(durationtime.total_seconds())[:4]

            self.statdict['endtime'] = endtime
            self.statdict['duration'] = duration

            if self.debug:
                logging.debug('')
                logging.debug(f'phase= {self.phase:s}')
                logging.debug(f'errmsg= {self.errmsg:s}')

            self.statdict['phase'] = self.phase
            self.statdict['errmsg'] = self.errmsg

            self.__writeStatusMsg__(self.statuspath, self.statdict, self.param)

        else:
            if self.debug:
                logging.debug('')
                logging.debug('Case: sync')

            self.__printSyncResult__(self.resultpath, self.format)

        if self.debug:
            logging.debug('')
            logging.debug('TAP service done. Return data or status and exit.')

        sys.exit()

        #
        # } end tap.init()
        #


    def __getStatusJob__(self, data, **kwargs):

        #
        # {
        #
        # Parse data to extract inparam
        #

        doc = None

        try:
            doc = xmltodict.parse(data)

        except Exception as e:

            msg = 'Exception xmltodict.parse: ' + str(e)
            raise Exception(msg)

        if self.debug:
            logging.debug('')
            logging.debug('doc:')
            logging.debug('----------------------------------------------')
            logging.debug(doc)
            logging.debug('----------------------------------------------')

        job = None
        try:
            job = doc['uws:job']
        except Exception as e:

            msg = 'Exception retrieving job from status file: ' + str(e)
            raise Exception(msg)

        return(job)
#
# }  end of getStatusJob
#


    def __getStatusData__(self, statuspath, **kwargs):

        #
        # {
        #

        data = ''
        opened = False
        nopen = 0

        #
        # In case multiple processes are trying to access status file,
        # allow attempts to open file three times before raise exception
        #

        while(not opened):

            try:
                with open(statuspath, 'r') as fp:

                    opened = True
                    data = fp.read()

                    if self.debug:
                        logging.debug('')
                        logging.debug('data=')
                        logging.debug('-------------------------------------')
                        logging.debug(data)
                        logging.debug('-------------------------------------')

            except Exception as e:
                msg = 'Error reading status file: ' + str(e)
                pass

            if self.debug:
                logging.debug('')
                logging.debug('opened:' + str(opened))

            if(opened):
                break

            time.sleep(1.0)
            nopen = nopen + 1

            if(nopen > 2):
                break

        if(not opened):
            raise Exception(msg)

        return(data)

        #
        # }  end of getStatusData
        #


    def __getPhase__(self, statuspath, **kwargs):

        #
        # {
        #

        try:
            data = self.__getStatusData__(statuspath)

        except Exception as e:

            msg = 'Error reading status file: ' + str(e)
            raise Exception(msg)

        job = None
        try:
            job = self.__getStatusJob__(data)
        except Exception as e:
            msg = 'Exception retrieving job from status file: ' + str(e)
            raise Exception(msg)

        retval = job['uws:phase']

        return(retval)

        #
        # }  end of getPhase
        #



    def __printStatus__(self, key, retval, outtype, **kwargs):

        #
        # {
        #

        #
        # Header
        #

        print("HTTP/1.1 200 OK\r")

        if(outtype == 'xml'):

            print("Content-type: text/xml\r")
            print("\r")

            print('<?xml version="1.0" encoding="UTF-8"?>')
            print('<uws:job xmlns:uws="http://www.ivoa.net/xml/UWS/v1.0"'
                  ' xmlns:xlink="http://www.w3.org/1999/xlink"'
                  ' xmlns:xs="http://www.w3.org/2001/XMLSchema"'
                  ' xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"'
                  ' xsi:schemaLocation="http://www.ivoa.net/xml/UWS/v1.0">')

            if((key == 'errorSummary')
                    or (key == 'errmsg')
                    or (key == 'error')):

                if(len(retval) == 0):
                    print('    <uws:errorSummary></uws:errorSummary>')
                else:
                    print('    <uws:errorSummary>')
                    print(retval)
                    print('    </uws:errorSummary>')

            elif(key == 'parameters'):

                print(retval)

            elif((key == 'results') or (key == 'results/resulturl')):

                print('    <uws:results>')
                print(retval)
                print('    </uws:results>')

            print('</uws:job>')
            sys.stdout.flush()

        else:
            print("Content-type: text/plain\r")
            print("\r")

            print(retval)
            sys.stdout.flush()

        sys.stdout.flush()

        if self.debug:
            logging.debug('Write status to user and exit.')

        return

        #
        # } end of printStatus
        #


    def __getStatus__(self, workdir, workspace, key, param, **kwargs):

        #
        # {
        #

        isExist = os.path.exists(self.statuspath)

        if(isExist == 0):
            msg = 'Status file: status.xml does not exist.'
            raise Exception(msg)

        if self.debug:
            logging.debug('')
            logging.debug('statuspath exists')

        data = ''
        try:
            data = self.__getStatusData__(self.statuspath)

        except Exception as e:

            msg = 'Error getStatusData: ' + str(e)
            raise Exception(msg)

        #
        # No key: return the whole status file
        #

        if self.debug:
            logging.debug('')
            logging.debug('Return status.xml to user and exit.')

        if(len(key) == 0):

            print("HTTP/1.1 200 OK\r")
            print("Content-type: text/xml\r")
            print("\r")
            print(data)
            sys.exit()

        #
        # Extract format
        #

        soup = None
        try:
            soup = BeautifulSoup(data, 'lxml')

        except Exception as e:
            if(self.tapcontext == 'async'):
                
                self.phase = 'ERROR'
                self.__writeAsyncError__(str(e), self.statuspath,
                                         self.statdict, self.param)
            else: 
                self.__printError__('votable', str(e))

        format = 'votable'

        try:
            parameters = soup.find('uws:parameters')
            parameter = parameters.find(id='format')

            format = parameter.string

        except Exception as e:

            if(self.tapcontext == 'async'):
                
                self.phase = 'ERROR'
                self.__writeAsyncError__(str(e), self.statuspath,
                                         self.statdict, self.param)
            else: 
                self.__printError__('votable', str(e))

        if self.debug:
            logging.debug('')
            logging.debug(f'format= {format:s}')

        job = None
        try:
            job = self.__getStatusJob__(data)

        except Exception as e:
            msg = 'Exception retrieving job from status file: ' + str(e)
            raise Exception(msg)


        if(key == 'parameters'):

            parameters = None
            try:
                parameters = soup.find('uws:parameters')
            except Exception as e:
                if(self.tapcontext == 'async'):
                
                    self.phase = 'ERROR'
                    self.__writeAsyncError__(str(e), self.statuspath,
                                         self.statdict, self.param)
                else: 
                    self.__printError__(format, str(e))

            if self.debug:
                logging.debug('')
                logging.debug('parameters:')
                logging.debug(parameters)

            self.__printStatus__('parameters', parameters, 'xml')
            sys.exit()

        #
        # Parse data to extract inparam
        #

        job = None
        try:
            job = self.__getStatusJob__(data)

        except Exception as e:
            msg = 'Exception retrieving job from status file: ' + str(e)
            raise Exception(msg)

        if((key == 'phase')
                or (key == 'startTime')
                or (key == 'endTime')
                or (key == 'executionDuration')
                or (key == 'destruction')
                or (key == 'jobId')
                or (key == 'runId')
                or (key == 'ownerId')
                or (key == 'quote')):

            #
            # { Single value return
            #

            retval = 'None'
            keystr = 'uws:' + key
            outstr = ''

            if((key == 'phase')
                    or (key == 'startTime')
                    or (key == 'endTime')
                    or (key == 'executionDuration')
                    or (key == 'destruction')
                    or (key == 'jobId')
                    or (key == 'runId')):

                retval = job[keystr]
                outstr = retval

            elif((key == 'ownerId') or (key == 'quote')):

                retval = ''

            if self.debug:
                logging.debug('')
                logging.debug(f'retval= {retval:s}')
                logging.debug(f'keystr= {keystr:s}')
                logging.debug(f'outstr= {outstr:s}')

            self.__printStatus__(key, retval, 'plain')
            sys.exit()

            #
            # } end single value return
            #

        #
        # { Key: return error
        #

        phase = job['uws:phase']

        if((key == 'errorSummary')
                or (key == 'errmsg')
                or (key == 'error')):

            retval = ''
            errmsg = ''

            if(phase.lower() == 'error'):

                try:
                    errmsg = job['uws:errorSummary']['uws:message']
                except Exception as e:
                    pass

            if self.debug:
                logging.debug('')
                logging.debug(f'errmsg: {errmsg:s}')

            if(len(errmsg) > 0):
                outstr = f'        <uws:message>{errmsg:s}</uws:message>'
            else:
                outstr = ''

            self.__printStatus__('errorSummary', outstr, 'xml')
            sys.exit()

        #
        # } end return error
        #

        #
        # { Input key: result, results, resulturl
        #

        result = 'None'
        resulturl = 'None'

        if(key == 'resulturl'):
            key = 'results/resulturl'
        if(key == 'result'):
            key = 'results/result'

        if((key == 'results')
                or (key == 'results/result')
                or (key == 'results/resulturl')):

            try:
                result = job['uws:results']['uws:result']

                if(phase.lower() == 'completed'):
                    resulturl = job['uws:results']['uws:result']['@xlink:href']

            except Exception as e:
                if self.debug:
                    logging.debug('')
                    logging.debug('error retrieving result')
                pass

        if self.debug:
            logging.debug('')
            logging.debug(f'resulturl: {resulturl:s}')
            logging.debug('result:')
            logging.debug(result)


        if((key == 'results') or (key == 'results/resulturl')):

            if self.debug:
                logging.debug('')
                logging.debug('case1: results/resulturl')

            outstr = '        <uws:result id="result" xlink:type="simple"' \
                     f' xlink:href="{resulturl:s}"/>'

            self.__printStatus__(key, outstr, 'xml')
            sys.exit()

        if(len(resulturl) == 0):
            msg = 'resulturl not found.'
            if(self.tapcontext == 'async'):
                
                self.phase = 'ERROR'
                self.__writeAsyncError__(msg, self.statuspath,
                                         self.statdict, self.param)
            else: 
                self.__printError__(format, msg)

        #
        # Last case: 'results/result' -- return result table
        #

        indx = resulturl.find(workspace)
        substr = resulturl[indx:]

        resultpath = workdir + '/TAP/' + substr
        if self.debug:
            logging.debug('')
            logging.debug(f'resultpath = {resultpath:s}')

        fp = None
        try:
            fp = open(resultpath, 'r')
        except Exception as e:
            msg = 'Failed to open result file: ' + resultpath
            if(self.tapcontext == 'async'):
                
                self.phase = 'ERROR'
                self.__writeAsyncError__(msg, self.statuspath,
                                         self.statdict, self.param)
            else: 
                self.__printError__(format, msg)

        print("HTTP/1.1 200 OK\r")

        if(format == 'json'):
            print("Content-type: application/json\r")
        elif(format == 'votable'):
            print("Content-type: text/xml\r")
        else:
            print("Content-type: text/plain\r")
        print("\r")

        try:
            while True:

                line = fp.readline()

                if not line:
                    break

                sys.stdout.write(line)
                sys.stdout.flush()

        except Exception as e:
            if(self.tapcontext == 'async'):
                
                self.phase = 'ERROR'
                self.__writeAsyncError__(str(e), self.statuspath,
                                         self.statdict, self.param)
            else: 
                self.__printError__(format, str(e))

        fp.close()
        sys.exit()

        #
        # } end return result
        #

        #
        # } end of getStatus
        #


    def __printError__(self, fmt, errmsg):

        #
        # {
        #

        print("HTTP/1.1 200 OK\r")

        if(fmt == 'votable'):

            print("Content-type: text/xml\r")
            print("\r")

            print('<?xml version="1.0" encoding="UTF-8"?>')
            print('<VOTABLE version="1.4"'
                  ' xmlns="http://www.ivoa.net/xml/VOTable/v1.3">')
            print('<RESOURCE type="results">')
            print('<INFO name="QUERY_STATUS" value="ERROR">')

            print(errmsg)

            print('</INFO>')
            print('</RESOURCE>')
            print('</VOTABLE>')

        else:
            print("Content-type: application/json\r")
            print("\r")

            print("{")
            print('    "status": "error",')
            print('    "msg": "%s"' % errmsg)
            print("}")

        sys.stdout.flush()
        sys.exit()

        #
        # }  end of printError
        #



    def __writeAsyncError__(self, errmsg, statuspath, statdict, param, **kwargs):

        #
        # {
        #

        #
        # Set status parameters
        #
        
        if self.debug:
            logging.debug('')
            logging.debug(f'From writeAsyncError')
     

        etime = datetime.datetime.now()
        endtime = etime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-4]

        durationtime = etime - statdict['stime']
        duration = str(durationtime.total_seconds())[:4]

        statdict['endtime'] = endtime
        statdict['duration'] = duration

        statdict['phase'] = 'ERROR'
        statdict['errmsg'] = errmsg

        self.__writeStatusMsg__(statuspath, statdict, param)

        sys.exit()

        #
        # }  end of writeAsyncError
        #


    def __printSyncResult__(self, resultpath, format, **kwargs):

        #
        # {
        #

        if self.debug:
            logging.debug('')
            logging.debug(f'resultpath = {resultpath:s}')
            logging.debug(f'format     = [{format:s}]\n')

            logging.debug('Output lines:')
            logging.debug('-------------------------------------------------')

        print("HTTP/1.1 200 OK\r")

        fp = None
        try:
            fp = open(resultpath, 'r')
        except IOError:
            msg = 'Failed to open result file.'
            self.__printError__(msg)

        if(format == 'json'):
            print("Content-type: application/json\r")
        elif(format == 'votable'):
            print("Content-type: text/xml\r")
        else:
            print("Content-type: text/plain\r")
        print("\r")

        while True:

            line = fp.readline()

            if self.debug:
                logging.debug(f'[{line:s}]')

            if not line:
                break

            sys.stdout.write(line)
            sys.stdout.flush()

        if self.debug:
            logging.debug('-------------------------------------------------')

        fp.close()
        return

        #
        # }  end of printSyncResult
        #


    def __printSyncResponse__(self, status, msg, resulturl, format, **kwargs):

        #
        # { Place holder for re-direct sync response case
        #

        if(status == 'error'):

            print("HTTP/1.1 200 OK\r")
            print("Content-type: application/json\r")
            print("\r")

            print("{")
            print('    "status": "error",')
            print('    "msg": "%s"' % msg)
            print("}")

        else:
            print("HTTP/1.1 303 See Other\r")
            print("Location: %s\r\n\r" % resulturl)
            print("Redirect Location: %s" % resulturl)

        sys.stdout.flush()

        return

        #
        # }
        #


    def __printAsyncResponse__(self, statusurl, **kwargs):

        #
        # async: return statusurl and kill the parent process
        #

        print("HTTP/1.1 303 See Other\r")
        print("Location: %s\r\n\r" % statusurl)
        print("Redirect Location: %s" % statusurl)
        sys.stdout.flush()

        time.sleep(2.0)

        #
        # Shut down parent program
        #

        os.kill(os.getppid(), signal.SIGKILL)

        if self.debug:
            logging.debug('')
            logging.debug('parent process killed')

        return


    def __writeStatusMsg__(self, statuspath, statdict, param, **kwargs):

        if self.debug:
            logging.debug('')
            logging.debug(f'Enter writeStatusMsg')
        
        #
        # { TAP status result always written in XML format
        #

        format = param['format'].lower()
        maxrec = param['maxrec']

        fp = None
        try:
            fp = open(statuspath, 'w+')
            os.chmod(statuspath, 0o664)

        except Exception as e:
            msg = 'Failed to open/create status file.'
            self.__printError__(format, msg)

        try:
            fcntl.lockf(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except Exception as e:

            msg = 'Cannot lock status file.'

            if self.debug:
                logging.debug('')
                logging.debug(f'{msg:s}: {str(e):s}')

            self.__printError__(format, msg)

        if self.debug:
            logging.debug('')
            logging.debug('status file locked for write')

        phase = statdict['phase']

        if self.debug:
            logging.debug('')
            logging.debug(f'phase= {phase:s}')

        resulturl = statdict['resulturl']
        errmsg = statdict['errmsg']

        fp.write('<?xml version="1.0" encoding="UTF-8"?>\n')

        fp.write('<uws:job xmlns:uws="http://www.ivoa.net/xml/UWS/v1.0"'
                 '   xmlns:xlink="http://www.w3.org/1999/xlink"'
                 '   xmlns:xs="http://www.w3.org/2001/XMLSchema"'
                 '   xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"'
                 '   xsi:schemaLocation="http://www.ivoa.net/xml/UWS/v1.0">\n')

        fp.write(f"    <uws:jobId>{statdict['jobid']:s}</uws:jobId>\n")
        fp.write(f"    <uws:runId>{statdict['process_id']:d}</uws:runId>\n")
        fp.write('    <uws:ownerId xsi:nil="true"/>\n')
        fp.write(f"    <uws:phase>{statdict['phase'].upper():s}</uws:phase>\n")
        fp.write('    <uws:quote xsi:nil="true"/>\n')

        fp.write(f"    <uws:startTime>{statdict['starttime']:s}</uws:startTime>\n")

        fp.write(f"    <uws:endTime>{statdict['endtime']:s}</uws:endTime>\n")

        fp.write(f"    <uws:executionDuration>{statdict['duration']:s}"
                 "</uws:executionDuration>\n")

        fp.write(f"    <uws:destruction>{statdict['destruction']:s}"
                 "</uws:destruction>\n")

        fp.write('    <uws:parameters>\n')

        format = param['format']

        fp.write(f'        <uws:parameter id="format">{format:s}</uws:parameter>\n')

        lang = param['lang']

        fp.write(f'        <uws:parameter id="lang">{lang:s}</uws:parameter>\n')

        fp.write(f'        <uws:parameter id="maxrec">{maxrec:d}</uws:parameter>\n')

        #
        # Encode query to escape '<' and '>'
        #

        str1 = param['query']

        ind = str1.find('<')
        while(ind >= 0):

            substr1 = str1[0:ind]
            substr2 = str1[ind + 1:]

            str1 = substr1 + '&lt;' + substr2
            ind = str1.find('<')

        ind = str1.find('>')
        while(ind >= 0):

            substr1 = str1[0:ind]
            substr2 = str1[ind + 1:]

            str1 = substr1 + '&gt;' + substr2
            ind = str1.find('>')

        fp.write(f'        <uws:parameter id="query">{str1:s}')
        fp.write('        </uws:parameter>\n')

        fp.write('    </uws:parameters>\n')

        if(phase.lower() == 'completed'):

            fp.write('    <uws:results>\n')
            fp.write('        <uws:result id="result" xlink:type="simple"'
                     f' xlink:href="{resulturl:s}"/>\n')
            fp.write('    </uws:results>\n')

        elif(phase.lower() == 'error'):

            fp.write('    <uws:errorSummary type="transient" hasDetail="true">\n')
            fp.write(f'        <uws:message>{errmsg:s}</uws:message>\n')
            fp.write('    </uws:errorSummary>\n')

        fp.write('</uws:job>\n')

        fp.flush()
        fp.close()

        #
        # Note: closing file automatically released the lock
        #

        return

        #
        # } end writeStatusMsg
        #


    def __getDatalevel__(self, dbtable, **kwargs):

        if self.debug:
            logging.debug(f'dbtable   = {dbtable:s}')

        level = ["l0",
                 "l1",
                 "l2",
                 "eng"]

        nlevel = len(level)

        if self.debug:
            logging.debug(f'nlevel    = {nlevel:d}')

        datalevel = ''
        for i in range(nlevel):

            dbtable_lower = dbtable.lower()

            ind = dbtable_lower.find(level[i])

            if(ind != -1):
                datalevel = level[i]
                break

        if self.debug:
            logging.debug(f'ind       = {ind:d}')
            logging.debug(f'datalevel = {datalevel:s}')

        return(datalevel)

    #
    # } end tap class
    #
