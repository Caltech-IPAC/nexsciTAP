import os
import sys
import logging
import configobj


class configParam:

    """
    configParam class retrieves and stores all the configuration variable 
    for nph-tap.py and dbquery.py  
    
    Required input:

        path:  full path of the config file

    optional input:

        debugloggername: a keyowrd input (i.e. debugloggername=loggername)
	where 'loggername' is defined in the calling program
    """

    path = ''
    status = 'ok'
    errmsg = ''
    
    debug = 0 
    debugfname = ''

    def __init__ (self, path, **kwargs):

        self.configpath = path

        if ('debug' in kwargs):
            self.debug = kwargs['debug']

        if self.debug:
            logging.debug ('')
            logging.debug ('Enter configParam: path= %s' % path)

        isExist = os.path.exists (path)

        if self.debug:
            logging.debug ('')
            logging.debug ('isExist= %d' % isExist)

        if (isExist == 0):
            status = 'error'
            errmsg = 'Cannot find config file: ' + path
            raise Exception (errmsg)

       
#
#    instantiate configobj class
#
        confobj = configobj.ConfigObj (self.configpath)

        if self.debug:
            logging.debug ('')
            logging.debug ('config instantiated')
       
#
#    extract config parameters of input server
#
        self.server = 'webserver'
        if self.debug:
            logging.debug ('')
            logging.debug (f'server= {self.server:s}')

        dsn = ''
        if ('DSN' in confobj[self.server]):
            dsn = confobj[self.server]['DSN']
        self.dsn = dsn

        if self.debug:
            logging.debug (f'dsn= {self.dsn:s}')

        if (len(self.dsn) == 0):
            self.status = 'error'
            self.msg = 'Failed to find database server name in config_file'
            raise Exception (self.msg) 


        self.adqlparam = {}

        self.adqlparam['mode']     = 'HTM'
        self.adqlparam['level']    =  7
        self.adqlparam['xcol']     = 'x'
        self.adqlparam['ycol']     = 'y'
        self.adqlparam['zcol']     = 'z'
        self.adqlparam['colname']  = 'spt_ind'
        self.adqlparam['encoding'] = 'BASE4'

        if ('ADQL_MODE' in confobj[self.server]):
            self.adqlparam['mode'] = confobj[self.server]['ADQL_MODE']

        if ('ADQL_LEVEL' in confobj[self.server]):
            self.adqlparam['level'] = confobj[self.server]['ADQL_LEVEL']

        if(self.adqlparam['mode'] == 'HTM' and self.adqlparam['level'] != '7'):
            self.adqlparam['colname'] = 'htm' + str(self.adqlparam['level'])
            self.adqlparam['encoding'] = 'BASE10'

        if ('ADQL_XCOL' in confobj[self.server]):
            self.adqlparam['xcol'] = confobj[self.server]['ADQL_XCOL']

        if ('ADQL_YCOL' in confobj[self.server]):
            self.adqlparam['ycol'] = confobj[self.server]['ADQL_YCOL']

        if ('ADQL_ZCOL' in confobj[self.server]):
            self.adqlparam['zcol'] = confobj[self.server]['ADQL_ZCOL']

        if ('ADQL_ENCODING' in confobj[self.server]):
            self.adqlparam['encoding'] = confobj[self.server]['ADQL_ENCODING']


        self.dbuser = ''
        if ('UserID' in confobj[dsn]):
            self.dbuser = confobj[dsn]['UserID']

        if (len(self.dbuser) == 0):
            self.status = 'error'
            self.msg = 'Failed to find db user in config_file'
            raise Exception (self.msg) 
        
        self.dbpassword = ''
        if ('Password' in confobj[dsn]):
            self.dbpassword = confobj[dsn]['Password']

        if (len(self.dbpassword) == 0):
            self.status = 'error'
            self.msg = 'Failed to find db password in config_file'
            raise Exception (self.msg) 
	
        self.dbserver = ''
        if ('ServerName' in confobj[dsn]):
            self.dbserver = confobj[dsn]['ServerName']

        if (len(self.dbserver) == 0):
            self.status = 'error'
            self.msg = 'Failed to find db server name in config_file'
            raise Exception (self.msg) 

        if self.debug:
            logging.debug ('')
            logging.debug (f'dbuser= {self.dbuser:s}')
            logging.debug (f'dbpassword= {self.dbpassword:s}')
            logging.debug (f'dbserver= {self.dbserver:s}')

        self.workdir = ''
        if ('TAP_WORKDIR' in confobj[self.server]):
            self.workdir = confobj[self.server]['TAP_WORKDIR']

        if (len(self.workdir) == 0):
            self.status = 'error'
            self.msg = 'Failed to find TAP_WORKDIR in config_file'
            raise Exception (self.msg) 
    
        self.workurl = ''
        if ('TAP_WORKURL' in confobj[self.server]):
            self.workurl = confobj[self.server]['TAP_WORKURL']

        if (len(self.workurl) == 0):
            self.status = 'error'
            self.msg = 'Failed to find TAP_WORKURL in config_file'
            raise Exception (self.msg) 
    
        self.httpurl = ''
        if ('HTTP_URL' in confobj[self.server]):
            self.httpurl = confobj[self.server]['HTTP_URL']

        if (len(self.httpurl) == 0):
            self.status = 'error'
            self.msg = 'Failed to find HTTP_URL in config_file'
            raise Exception (self.msg) 
    
        self.port = ''
        if ('HTTP_PORT' in confobj[self.server]):
            self.port = confobj[self.server]['HTTP_PORT']

        if (len(self.port) == 0):
            self.status = 'error'
            self.msg = 'Failed to find HTTP_PORT in config_file'
            raise Exception (self.msg) 
   
        self.httpurl = self.httpurl + ':' + self.port
	    
        self.cgipgm = ''
        if ('CGI_PGM' in confobj[self.server]):
            self.cgipgm = confobj[self.server]['CGI_PGM']

        if (len(self.cgipgm) == 0):
            self.status = 'error'
            self.msg = 'Failed to find CGI_PGM in config_file'
            raise Exception (self.msg) 
    
        if self.debug:
            logging.debug ('')
            logging.debug (f'workdir= {self.workdir:s}') 
            logging.debug (f'workurl= {self.workurl:s}') 
            logging.debug (f'httpurl= {self.httpurl:s}') 
            logging.debug (f'cgipgm= {self.cgipgm:s}') 
            logging.debug (f'port= {self.port:s}') 

        self.cookiename = ''
        if ('COOKIENAME' in confobj[self.server]):
            if self.debug:
                logging.debug ('')
                logging.debug ('xxx1') 
            self.cookiename = confobj[self.server]['COOKIENAME']

#        if (len(self.cookiename) == 0):
#            self.status = 'error'
#            self.msg = 'Failed to find COOKIENAME in config_file'
#            raise Exception (self.msg) 
    
        if self.debug:
            logging.debug ('')
            logging.debug (f'cookiename= {self.cookiename:s}') 
        
        self.accesstbl = ''
        if ('ACCESS_TBL' in confobj[self.server]):
            self.accesstbl = confobj[self.server]['ACCESS_TBL']

#        if (len(self.accesstbl) == 0):
#            self.status = 'error'
#            self.msg = 'Failed to find ACCESS_TBL in config_file'
#            raise Exception (self.msg) 
    
        self.usertbl = ''
        if ('USERS_TBL' in confobj[self.server]):
            self.usertbl = confobj[self.server]['USERS_TBL']

#        if (len(self.usertbl) == 0):
#            self.status = 'error'
#            self.msg = 'Failed to find USERS_TBL in config_file'
#            raise Exception (self.msg) 
    
        if self.debug:
            logging.debug ('')
            logging.debug (f'usertbl= {self.usertbl:s}') 
            logging.debug (f'accesstbl= {self.accesstbl:s}') 
        
        self.propfilter = ''
        if ('PROPFILTER' in confobj[self.server]):
            self.propfilter = confobj[self.server]['PROPFILTER']

        if self.debug:
            logging.debug ('')
            logging.debug (f'propfilter= {self.propfilter:s}') 
        
        self.fileid = ''
        if ('FILEID' in confobj[self.server]):
            self.fileid = confobj[self.server]['FILEID']

        self.accessid = ''
        if ('ACCESSID' in confobj[self.server]):
            self.accessid = confobj[self.server]['ACCESSID']

        self.racol = ''
        if ('RACOL' in confobj[self.server]):
            self.racol = confobj[self.server]['RACOL']

        self.deccol = ''
        if ('DECCOL' in confobj[self.server]):
            self.deccol = confobj[self.server]['DECCOL']

        return

