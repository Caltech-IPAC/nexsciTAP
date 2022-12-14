#Copyright (c) 2020, Caltech IPAC.
# This code is released with a BSD 3-clause license. License information is at
#   https://github.com/Caltech-IPAC/nexsciTAP/blob/master/LICENSE


import os
import logging
import configobj


class configParam:

    """
    configParam class retrieves and stores all the configuration parameters
    for nph-tap.py and dbquery.py from a file.

    Required input:

        path:  full path of the config file

    optional input:

        debugloggername: a keyword input(i.e., debugloggername=loggername)
        where 'loggername' is defined in the calling program
    """

    debug = 0

    def __init__(self, path, **kwargs):

        if('debug' in kwargs):
            self.debug = kwargs['debug']

        self.configpath = path

        if self.debug:
            logging.debug('')
            logging.debug('Enter configParam: path= %s' % path)

        isExist = os.path.exists(path)

        if(isExist == 0):
            self.msg = 'Cannot find config file: ' + path
            raise Exception(self.msg)

        #
        # Instantiate configobj class
        #

        confobj = configobj.ConfigObj(self.configpath)

        if self.debug:
            logging.debug('')
            logging.debug('ConfigObj instantiated successfully')

        #
        # Extract config parameters of input server
        #

        self.server = 'webserver'
        if self.debug:
            logging.debug('')
            logging.debug(f'      server = {self.server:s}')

        dbms = None 
        if('DBMS' in confobj[self.server]):
            dbms = confobj[self.server]['DBMS']
        self.dbms = dbms

        if self.debug:
            logging.debug('dbms=')
            logging.debug(self.dbms)


        if (self.dbms is None):
            self.status = 'error'
            self.msg = 'Failed to find database server name in config_file'
            raise Exception(self.msg)


        arraysize = 10000

        if('ArraySize' in confobj[self.server]):
            try:
                arraysize = int(confobj[self.server]['ArraySize'])
            except Exception as e:
                arraysize = 10000

        self.arraysize = arraysize


        self.connectInfo = {}

        self.connectInfo['dbms'] = dbms

        if(dbms == 'oracle'):

            self.connectInfo['dbserver'] = None 
            if('ServerName' in confobj[dbms]):
                self.connectInfo['dbserver'] = confobj[dbms]['ServerName']

            if (self.connectInfo['dbserver'] is None):
                self.status = 'error'
                self.msg = 'Failed to find db server name in config_file'
                raise Exception(self.msg)

            self.connectInfo['userid'] = None 
            if('UserID' in confobj[dbms]):
                self.connectInfo['userid'] = confobj[dbms]['UserID']

            if (self.connectInfo['userid'] is None):
                self.status = 'error'
                self.msg = 'Failed to find DBMS user ID in config_file'
                raise Exception(self.msg)

            self.connectInfo['password'] = None 
            if('Password' in confobj[dbms]):
                self.connectInfo['password'] = confobj[dbms]['Password']

            if (self.connectInfo['password'] is None):
                self.status = 'error'
                self.msg = 'Failed to find db password in config_file'
                raise Exception(self.msg)

            if self.debug:
                logging.debug ('')
                logging.debug ("dbserver=")
                logging.debug (self.connectInfo['dbserver'])
                logging.debug ("dbuser= [Not shown for security reasons.]")
                logging.debug ("password= [Not shown for security reasons.]")
           
            #
            #   Change to below (temporarily) to debug login info.
            # 
            #   logging.debug ("dbuser=")
            #   logging.debug (self.connectInfo['userid'])
            #   logging.debug ("password=")
            #   logging.debug (self.connectInfo['password'])


        if (dbms == 'sqlite3'):

            self.connectInfo['db'] = None 
            if ('DB' in confobj[dbms]):
                self.connectInfo['db'] = confobj[dbms]['DB']

            if (self.connectInfo['db'] is None):
                self.status = 'error'
                self.msg = 'Failed to find DB in config_file'
                raise Exception(self.msg)

            self.connectInfo['tap_schema'] =  None
            if ('TAP_SCHEMA' in confobj[dbms]):
                self.connectInfo['tap_schema'] = confobj[dbms]['TAP_SCHEMA']

            if (self.connectInfo['tap_schema'] is None):
                self.status = 'error'
                self.msg = 'Failed to find TAP_SCHEMA password in config_file'
                raise Exception(self.msg)

            if self.debug:
                logging.debug ('')
                logging.debug ("db=")
                logging.debug (self.connectInfo['db'])
                logging.debug ("tap_schema=")
                logging.debug (self.connectInfo['tap_schema'])


        if(dbms == 'mysql'):

            self.connectInfo['dbserver'] = None 
            if('ServerName' in confobj[dbms]):
                self.connectInfo['dbserver'] = confobj[dbms]['ServerName']

            if self.debug:
                logging.debug('')
                logging.debug(f'dbserver= ')
                logging.debug(self.connectInfo['dbserver'])
            
            self.connectInfo['port'] =  None
            if('port' in confobj[dbms]):
                self.connectInfo['port'] = confobj[dbms]['port']

            if self.debug:
                logging.debug('')
                logging.debug(f'port= ')
                logging.debug(self.connectInfo['port'])
            
            self.connectInfo['socket'] = None
            if ('socket' in confobj[dbms]):
                self.connectInfo['socket'] = confobj[dbms]['socket']
            
            if self.debug:
                logging.debug('')
                logging.debug(f'socket= ')
                logging.debug(self.connectInfo['socket'])
            
                
            if ((self.connectInfo['dbserver'] is None) and \
                (self.connectInfo['socket'] is None)):
                
                self.status = 'error'
                self.msg = \
                    'Failed to find db server OR socket info in config_file'
                raise Exception(self.msg)

            self.connectInfo['userid'] = None 
            if('UserID' in confobj[dbms]):
                self.connectInfo['userid'] = confobj[dbms]['UserID']

            if (self.connectInfo['userid'] is None):
                self.status = 'error'
                self.msg = 'Failed to find DBMS user ID in config_file'
                raise Exception(self.msg)

            self.connectInfo['password'] = None 
            if ('Password' in confobj[dbms]):
                self.connectInfo['password'] = confobj[dbms]['Password']

            if (self.connectInfo['password'] is None):
                self.status = 'error'
                self.msg = 'Failed to find db password in config_file'
                raise Exception(self.msg)

            self.connectInfo['dbschema'] = None 
            if('dbschema' in confobj[dbms]):
                self.connectInfo['dbschema'] = confobj[dbms]['dbschema']

            if (self.connectInfo['dbschema'] is None):
                self.status = 'error'
                self.msg = 'Failed to find DB schema in config_file'
                raise Exception(self.msg)

            if self.debug:
                logging.debug('')
                if (self.connectInfo['dbserver'] is not None):
                    logging.debug (\
                        f"dbserver= {self.connectInfo['dbserver']:s}")
                if (self.connectInfo['socket'] is not None):
                    logging.debug (f"socket= {self.connectInfo['socket']:s}")
                if (self.connectInfo['dbschema'] is not None):
                    logging.debug (f"db= {self.connectInfo['dbschema']:s}")
                logging.debug ("userid= [Not shown for security reasons.]")
                logging.debug ("password= [Not shown for security reasons.]")
                
                
        if(dbms == 'postgresql'):

            self.connectInfo['hostname'] = None 
            if('HOSTNAME' in confobj[dbms]):
                self.connectInfo['hostname'] = confobj[dbms]['HOSTNAME']

            if self.debug:
                logging.debug('')
                logging.debug(f'hostname= ')
                logging.debug(self.connectInfo['hostname'])
            
            self.connectInfo['database'] =  None
            if('DATABASE' in confobj[dbms]):
                self.connectInfo['database'] = confobj[dbms]['DATABASE']

            if self.debug:
                logging.debug('')
                logging.debug(f'database= ')
                logging.debug(self.connectInfo['database'])
            
                
            if (self.connectInfo['hostname'] is None): \
                self.status = 'error'
                self.msg = \
                    'Failed to find PostgrSQL HOSTNAME in config_file'
                raise Exception(self.msg)

            if (self.connectInfo['database'] is None):
                
                self.status = 'error'
                self.msg = \
                    'Failed to find PostgrSQL DATABASE keyword in config_file'
                raise Exception(self.msg)

            self.connectInfo['userid'] = None 
            if('USERNAME' in confobj[dbms]):
                self.connectInfo['username'] = confobj[dbms]['USERNAME']

            if (self.connectInfo['username'] is None):
                self.status = 'error'
                self.msg = 'Failed to find DBMS username in config_file'
                raise Exception(self.msg)

            self.connectInfo['password'] = None 
            if ('Password' in confobj[dbms]):
                self.connectInfo['password'] = confobj[dbms]['Password']

            if (self.connectInfo['password'] is None):
                self.status = 'error'
                self.msg = 'Failed to find db password in config_file'
                raise Exception(self.msg)

            if self.debug:
                logging.debug('')
                logging.debug ('hostname= ')
                logging.debug (hostname)
                logging.debug('')
                logging.debug ('database= ')
                logging.debug (database)
                logging.debug('')
                logging.debug ('username= ')
                logging.debug (username)
                logging.debug('')
                logging.debug ('password= ')
                logging.debug (password)
                
        self.adqlparam = {}

        #
        # Default values
        #

        self.adqlparam['mode']     = 'HTM'
        self.adqlparam['level']    =  7
        self.adqlparam['xcol']     = 'x'
        self.adqlparam['ycol']     = 'y'
        self.adqlparam['zcol']     = 'z'
        self.adqlparam['colname']  = 'spt_ind'
        self.adqlparam['encoding'] = 'BASE4'

        if('ADQL_MODE' in confobj[self.server]):
            self.adqlparam['mode'] = confobj[self.server]['ADQL_MODE']

        if('ADQL_LEVEL' in confobj[self.server]):
            self.adqlparam['level'] = confobj[self.server]['ADQL_LEVEL']

        if(self.adqlparam['mode'] == 'HTM' and self.adqlparam['level'] != '7'):
            self.adqlparam['colname'] = 'htm' + str(self.adqlparam['level'])
            self.adqlparam['encoding'] = 'BASE10'

        if('ADQL_XCOL' in confobj[self.server]):
            self.adqlparam['xcol'] = confobj[self.server]['ADQL_XCOL']

        if('ADQL_YCOL' in confobj[self.server]):
            self.adqlparam['ycol'] = confobj[self.server]['ADQL_YCOL']

        if('ADQL_ZCOL' in confobj[self.server]):
            self.adqlparam['zcol'] = confobj[self.server]['ADQL_ZCOL']

        if('ADQL_ENCODING' in confobj[self.server]):
            self.adqlparam['encoding'] = confobj[self.server]['ADQL_ENCODING']


        self.workdir = None 
        if('TAP_WORKDIR' in confobj[self.server]):
            self.workdir = confobj[self.server]['TAP_WORKDIR']

        if (self.workdir is None):
            self.status = 'error'
            self.msg = 'Failed to find TAP_WORKDIR in config_file'
            raise Exception(self.msg)

        self.workurl =  None
        if('TAP_WORKURL' in confobj[self.server]):
            self.workurl = confobj[self.server]['TAP_WORKURL']

        if (self.workurl is None):
            self.status = 'error'
            self.msg = 'Failed to find TAP_WORKURL in config_file'
            raise Exception(self.msg)

        self.httpurl = None 
        if ('HTTP_URL' in confobj[self.server]):
            self.httpurl = confobj[self.server]['HTTP_URL']

        if (self.httpurl is None):
            self.status = 'error'
            self.msg = 'Failed to find HTTP_URL in config_file'
            raise Exception(self.msg)

        self.port = None 
        if('HTTP_PORT' in confobj[self.server]):
            self.port = confobj[self.server]['HTTP_PORT']

        if (self.port is None):
            self.status = 'error'
            self.msg = 'Failed to find HTTP_PORT in config_file'
            raise Exception(self.msg)

        if ((self.port != '80') and (self.port != '443')):
            self.httpurl = self.httpurl + ':' + self.port

        self.cgipgm = None 
        if ('CGI_PGM' in confobj[self.server]):
            self.cgipgm = confobj[self.server]['CGI_PGM']

        if (self.cgipgm is None):
            self.status = 'error'
            self.msg = 'Failed to find CGI_PGM in config_file'
            raise Exception(self.msg)

        self.cookiename = '' 
        if('COOKIENAME' in confobj[self.server]):
            self.cookiename = confobj[self.server]['COOKIENAME']

        self.accesstbl = '' 
        if('ACCESS_TBL' in confobj[self.server]):
            self.accesstbl = confobj[self.server]['ACCESS_TBL']

        self.usertbl = '' 
        if('USERS_TBL' in confobj[self.server]):
            self.usertbl = confobj[self.server]['USERS_TBL']

        self.propfilter =  ''
        if('PROPFILTER' in confobj[self.server]):
            self.propfilter = confobj[self.server]['PROPFILTER']

        self.fileid =  ''
        if('FILEID' in confobj[self.server]):
            self.fileid = confobj[self.server]['FILEID']

        self.accessid = ''
        if('ACCESSID' in confobj[self.server]):
            self.accessid = confobj[self.server]['ACCESSID']

        self.racol = 'ra'
        if('RACOL' in confobj[self.server]):
            self.racol = confobj[self.server]['RACOL']

        self.deccol = 'dec'
        if('DECCOL' in confobj[self.server]):
            self.deccol = confobj[self.server]['DECCOL']

        if self.debug:
            logging.debug('')
            logging.debug(f'      workdir    = {self.workdir:s}')
            logging.debug(f'      workurl    = {self.workurl:s}')
            logging.debug(f'      httpurl    = {self.httpurl:s}')
            logging.debug(f'      cgipgm     = {self.cgipgm:s}')
            logging.debug(f'      port       = {self.port:s}')
            logging.debug(f'      cookiename = {self.cookiename:s}')
            logging.debug(f'      usertbl    = {self.usertbl:s}')
            logging.debug(f'      accesstbl  = {self.accesstbl:s}')
            logging.debug(f'      propfilter = {self.propfilter:s}')
            logging.debug(f'      fileid     = {self.fileid:s}')
            logging.debug(f'      accessid   = {self.accessid:s}')
            logging.debug(f'      racol      = {self.racol:s}')
            logging.debug(f'      deccol     = {self.deccol:s}')

        return
