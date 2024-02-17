#Copyright (c) 2020, Caltech IPAC.
# This code is released with a BSD 3-clause license. License information is at
#   https://github.com/Caltech-IPAC/nexsciTAP/blob/master/LICENSE


import os
import logging
import configobj
import pprint


class configParam:

    """
    configParam class retrieves and stores all the configuration parameters
    for nph-tap.py and dbquery.py from a file.

    Required input:

        path:  full path of the config file

    optional input:

        debugloggername: a keyword input(i.e., debugloggername=loggername)
        where 'loggername' is defined in the calling program

        instance:  configuration instance name for searching the config file
    """

    debug = 0

    def __init__(self, path, **kwargs):

        pp = pprint.PrettyPrinter(indent=3)



        # Initialize from kwargs

        if('debug' in kwargs):
            self.debug = kwargs['debug']

        self.instance = None
        if 'instance' in kwargs:
            self.instance = kwargs['instance']

        if self.debug:
            logging.debug('')
            logging.debug('      path:')
            logging.debug('      %s', path)

        if self.debug:
            logging.debug('')
            logging.debug('      kwargs:')
            logging.debug('      %s', kwargs)


        # Open config file

        self.configpath = path

        if self.debug:
            logging.debug('')
            logging.debug('Enter configParam: path= %s' % path)

        isExist = os.path.exists(path)

        if(isExist == 0):
            self.msg = 'Cannot find config file: ' + path
            raise Exception(self.msg)

        confobj = configobj.ConfigObj(self.configpath)

        if self.debug:
            logging.debug('')
            logging.debug('      confobj:')
            logging.debug('      %s', confobj)

        if self.debug:
            logging.debug('')
            logging.debug('ConfigObj instantiated successfully')

        
        ### Web server config parameters ############

        self.web = 'WEB'
        if self.debug:
            logging.debug('')
            logging.debug(f'      server = {self.web:s}')


        # TAP_WORKDIR

        self.workdir =  None
        if('TAP_WORKDIR' in confobj[self.web]):
            self.workdir = confobj[self.web]['TAP_WORKDIR']

        if (self.workdir is None):
            self.status = 'error'
            self.msg = 'Failed to find TAP_WORKDIR in config_file'
            raise Exception(self.msg)


        # TAP_WORKURL

        self.workurl =  None
        if('TAP_WORKURL' in confobj[self.web]):
            self.workurl = confobj[self.web]['TAP_WORKURL']

        if (self.workurl is None):
            self.status = 'error'
            self.msg = 'Failed to find TAP_WORKURL in config_file'
            raise Exception(self.msg)


        # HTTP_URL  (includingt HTTP_PORT)

        self.httpurl = None 
        if ('HTTP_URL' in confobj[self.web]):
            self.httpurl = confobj[self.web]['HTTP_URL']

        if (self.httpurl is None):
            self.status = 'error'
            self.msg = 'Failed to find HTTP_URL in config_file'
            raise Exception(self.msg)

        self.port = None 
        if('HTTP_PORT' in confobj[self.web]):
            self.port = confobj[self.web]['HTTP_PORT']

            if ((self.port != '80') and (self.port != '443')):
                self.httpurl = self.httpurl + ':' + self.port


        # CGIPGM

        self.cgipgm = None 
        if ('CGI_PGM' in confobj[self.web]):
            self.cgipgm = confobj[self.web]['CGI_PGM']


        # ARRAYSIZE

        self.arraysize = 10000 
        if ('ArraySize' in confobj[self.web]):
            self.arraysize = confobj[self.web]['ArraySize']


        # INFOMSG

        self.infomsg = ''                                                      
        if('INFOMSG' in confobj[self.web]):                                 
            self.infomsg = confobj[self.web]['INFOMSG']                     


        
        ### Configuration ###########################

        # If there was an input configuration 'instance', read 'db_connection' 
        # and 'sptind_config' section names from there.  Otherwise use the
        # following defaults:

        self.db_connection = 'DBMS'
        self.sptind_config = 'SPTIND'


        if self.instance:  # Set by kwargs above

            self.db_connection =  None
            if('DB_CONNECTION' in confobj[self.instance]):
                self.db_connection = confobj[self.instance]['DB_CONNECTION']

            if (self.db_connection is None):
                self.status = 'error'
                self.msg = 'Failed to find DB_CONNECTION in config_file'
                raise Exception(self.msg)
           

            self.sptind_config =  None
            if('SPTIND_CONFIG' in confobj[self.instance]):
                self.sptind_config = confobj[self.instance]['SPTIND_CONFIG']



        ### Database Connection #####################


        self.dbms              = None
        self.dbserver          = None
        self.userid            = None
        self.password          = None
        self.db                = None
        self.tap_schema        = 'TAP_SCHEMA'
        self.tap_schema_file   = 'TAP_SCHEMA.db'
        self.schemas_table     = 'schemas'
        self.tables_table      = 'tables'
        self.columns_table     = 'columns'
        self.keys_table        = 'keys'
        self.key_columns_table = 'key_columns'
        self.dbport            = None
        self.socket            = None
        self.dbschema          = None

        self.cookiename        = '' 
        self.accesstbl         = '' 
        self.usertbl           = '' 
        self.propfilter        = ''
        self.fileid            = ''
        self.accessid          = ''
        self.racol             = 'ra'
        self.deccol            = 'dec'


        if self.db_connection:

            # DBMS

            if 'DBMS' in confobj[self.db_connection]:
                self.dbms = confobj[self.db_connection]['DBMS']

            if (self.dbms is None):
                self.status = 'error'
                self.msg = 'Failed to find DBMS in config_file'
                raise Exception(self.msg)
           

            # ORACLE Connection

            if self.dbms == 'oracle':

                # SERVERNAME

                if 'ServerName' in confobj[self.db_connection]:
                    self.dbserver = confobj[self.db_connection]['ServerName']

                if (self.dbserver is None):
                    self.status = 'error'
                    self.msg = 'Failed to find Oracle ServerName in config_file'
                    raise Exception(self.msg)


                # USERID

                if 'UserID' in confobj[self.db_connection]:
                    self.userid = confobj[self.db_connection]['UserID']

                if (self.userid is None):
                    self.status = 'error'
                    self.msg = 'Failed to find Oracle UserID in config_file'
                    raise Exception(self.msg)


                # PASSWORD

                if 'Password' in confobj[self.db_connection]:
                    self.password = confobj[self.db_connection]['Password']

                if (self.password is None):
                    self.status = 'error'
                    self.msg = 'Failed to find Oracle Password in config_file'
                    raise Exception(self.msg)


                # TAP_SCHEMA, schemas, tables, columns

                if 'TAP_SCHEMA' in confobj[self.db_connection]:
                    self.tap_schema = confobj[self.db_connection]['TAP_SCHEMA']

                if 'SchemasTable' in confobj[self.db_connection]:
                    self.schemas_table = confobj[self.db_connection]['SchemasTable']

                if 'TablesTable' in confobj[self.db_connection]:
                    self.tables_table = confobj[self.db_connection]['TablesTable']

                if 'ColumnsTable' in confobj[self.db_connection]:
                    self.columns_table = confobj[self.db_connection]['ColumnsTable']

                if 'KeysTable' in confobj[self.db_connection]:
                    self.keys_table = confobj[self.db_connection]['KeysTable']

                if 'KeyColumnsTable' in confobj[self.db_connection]:
                    self.key_columns_table = confobj[self.db_connection]['KeyColumnsTable']


                # KOA Proprietary Access parameters

                self.cookiename = '' 
                if('COOKIENAME' in confobj[self.db_connection]):
                    self.cookiename = confobj[self.db_connection]['COOKIENAME']

                self.accesstbl = '' 
                if('ACCESS_TBL' in confobj[self.db_connection]):
                    self.accesstbl = confobj[self.db_connection]['ACCESS_TBL']

                self.usertbl = '' 
                if('USERS_TBL' in confobj[self.db_connection]):
                    self.usertbl = confobj[self.db_connection]['USERS_TBL']

                self.propfilter =  ''
                if('PROPFILTER' in confobj[self.db_connection]):
                    self.propfilter = confobj[self.db_connection]['PROPFILTER']

                self.fileid =  ''
                if('FILEID' in confobj[self.db_connection]):
                    self.fileid = confobj[self.db_connection]['FILEID']

                self.accessid = ''
                if('ACCESSID' in confobj[self.db_connection]):
                    self.accessid = confobj[self.db_connection]['ACCESSID']

                self.racol = 'ra'
                if('RACOL' in confobj[self.db_connection]):
                    self.racol = confobj[self.db_connection]['RACOL']

                self.deccol = 'dec'
                if('DECCOL' in confobj[self.db_connection]):
                    self.deccol = confobj[self.db_connection]['DECCOL']


            # SQLITE Connection

            if self.dbms == 'sqlite3':

                # DB

                if 'DB' in confobj[self.db_connection]:
                    self.db = confobj[self.db_connection]['DB']

                if (self.db is None):
                    self.status = 'error'
                    self.msg = 'Failed to find SQLite DB in config_file'
                    raise Exception(self.msg)


                # TAP_SCHEMA, schemas, tables, columns

                if 'TAP_SCHEMA_FILE' in confobj[self.db_connection]:
                    self.tap_schema_file = confobj[self.db_connection]['TAP_SCHEMA_FILE']

                if (self.tap_schema_file is None):
                    self.status = 'error'
                    self.msg = 'Failed to find SQLite TAP_SCHEMA_FILE in config_file'
                    raise Exception(self.msg)

                if 'TAP_SCHEMA' in confobj[self.db_connection]:
                    self.tap_schema = confobj[self.db_connection]['TAP_SCHEMA']

                if 'schemas_table' in confobj[self.db_connection]:
                    self.schemas_table = confobj[self.db_connection]['schemas_table']

                if 'tables_table' in confobj[self.db_connection]:
                    self.tables_table = confobj[self.db_connection]['tables_table']

                if 'columns_table' in confobj[self.db_connection]:
                    self.columns_table = confobj[self.db_connection]['columns_table']

                if 'keys_table' in confobj[self.db_connection]:
                    self.columns_table = confobj[self.db_connection]['keys_table']

                if 'key_columns_table' in confobj[self.db_connection]:
                    self.columns_table = confobj[self.db_connection]['key_columns_table']



            # MYSQL Connection

            if(self.dbms == 'mysql'):

                # DBSERVER

                if('ServerName' in confobj[self.db_connection]):
                    self.dbserver = confobj[self.db_connection]['ServerName']


                # PORT

                if('port' in confobj[self.db_connection]):
                    self.dbport = confobj[self.db_connection]['port']


                # SOCKET

                if ('socket' in confobj[self.db_connection]):
                    self.socket = confobj[self.db_connection]['socket']


                if ((self.dbserver is None) and \
                    (self.socket is None)):

                    self.status = 'error'
                    self.msg = \
                        'Failed to MySQL db server OR socket info in config_file'
                    raise Exception(self.msg)


                # USERID

                if('UserID' in confobj[self.db_connection]):
                    self.userid = confobj[self.db_connection]['UserID']

                if (self.userid is None):
                    self.status = 'error'
                    self.msg = 'Failed to MySQL DBMS user ID in config_file'
                    raise Exception(self.msg)

 
                # PASSWORD

                if ('Password' in confobj[self.db_connection]):
                    self.password = confobj[self.db_connection]['Password']

                if (self.password is None):
                    self.status = 'error'
                    self.msg = 'Failed to MySQL DB password in config_file'
                    raise Exception(self.msg)


                # DBSCHEMA

                if('dbschema' in confobj[self.db_connection]):
                    self.dbschema = confobj[self.db_connection]['dbschema']

                if (self.dbschema is None):
                    self.status = 'error'
                    self.msg = 'Failed to MySQL DB schema in config_file'
                    raise Exception(self.msg)


                # TAP_SCHEMA, schemas, tables, columns

                if 'TAP_SCHEMA' in confobj[self.db_connection]:
                    self.tap_schema = confobj[self.db_connection]['TAP_SCHEMA']

                if 'schemas_table' in confobj[self.db_connection]:
                    self.schemas_table = confobj[self.db_connection]['schemas_table']

                if 'tables_table' in confobj[self.db_connection]:
                    self.tables_table = confobj[self.db_connection]['tables_table']

                if 'columns_table' in confobj[self.db_connection]:
                    self.columns_table = confobj[self.db_connection]['columns_table']

                if 'keys_table' in confobj[self.db_connection]:
                    self.columns_table = confobj[self.db_connection]['keys_table']

                if 'key_columns_table' in confobj[self.db_connection]:
                    self.columns_table = confobj[self.db_connection]['key_columns_table']



        ### Spatial Index Configuration #############

        self.adqlparam = {}

        self.adqlparam['mode']     = 'HTM'
        self.adqlparam['level']    =  7
        self.adqlparam['xcol']     = 'x'
        self.adqlparam['ycol']     = 'y'
        self.adqlparam['zcol']     = 'z'
        self.adqlparam['colname']  = 'spt_ind'
        self.adqlparam['encoding'] = 'BASE4'

        if self.sptind_config:

            if('MODE' in confobj[self.sptind_config]):
                self.adqlparam['mode'] = confobj[self.sptind_config]['MODE']

            if('LEVEL' in confobj[self.sptind_config]):
                self.adqlparam['level'] = confobj[self.sptind_config]['LEVEL']

            if(self.adqlparam['mode'] == 'HTM' and self.adqlparam['level'] != '7'):
                self.adqlparam['colname'] = 'htm' + str(self.adqlparam['level'])
                self.adqlparam['encoding'] = 'BASE10'

            if('COLNAME' in confobj[self.sptind_config]):
                self.adqlparam['colname'] = confobj[self.sptind_config]['COLNAME']

            if('XCOL' in confobj[self.sptind_config]):
                self.adqlparam['xcol'] = confobj[self.sptind_config]['XCOL']

            if('YCOL' in confobj[self.sptind_config]):
                self.adqlparam['ycol'] = confobj[self.sptind_config]['YCOL']

            if('ZCOL' in confobj[self.sptind_config]):
                self.adqlparam['zcol'] = confobj[self.sptind_config]['ZCOL']

            if('ENCODING' in confobj[self.sptind_config]):
                self.adqlparam['encoding'] = confobj[self.sptind_config]['ENCODING']


        if self.debug:
            logging.debug('')
            logging.debug('adqlparam:')
            logging.debug('%s', self.adqlparam)


        self.connectInfo = {}

        self.connectInfo['dbms']              = self.dbms
        self.connectInfo['dbserver']          = self.dbserver
        self.connectInfo['userid']            = self.userid
        self.connectInfo['password']          = self.password
        self.connectInfo['db']                = self.db
        self.connectInfo['tap_schema']        = self.tap_schema
        self.connectInfo['tap_schema_file']   = self.tap_schema_file
        self.connectInfo['schemas_table']     = self.schemas_table
        self.connectInfo['tables_table']      = self.tables_table
        self.connectInfo['columns_table']     = self.columns_table
        self.connectInfo['keys_table']        = self.keys_table
        self.connectInfo['key_columns_table'] = self.key_columns_table
        self.connectInfo['port']              = self.dbport
        self.connectInfo['socket']            = self.socket
        self.connectInfo['dbschema']          = self.dbschema

        if self.debug:
            logging.debug('')
            logging.debug('connectInfo:')
            logging.debug('%s', self.connectInfo)


        if self.debug:
            logging.debug('')
            logging.debug('      workdir    = ' + str(self.workdir))
            logging.debug('      workurl    = ' + str(self.workurl))
            logging.debug('      httpurl    = ' + str(self.httpurl))
            logging.debug('      cgipgm     = ' + str(self.cgipgm))
            logging.debug('      port       = ' + str(self.dbport))
            logging.debug('      cookiename = ' + str(self.cookiename))
            logging.debug('      usertbl    = ' + str(self.usertbl))
            logging.debug('      accesstbl  = ' + str(self.accesstbl))
            logging.debug('      propfilter = ' + str(self.propfilter))
            logging.debug('      fileid     = ' + str(self.fileid))
            logging.debug('      accessid   = ' + str(self.accessid))
            logging.debug('      racol      = ' + str(self.racol))
            logging.debug('      deccol     = ' + str(self.deccol))

        return
