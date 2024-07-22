# Copyright (c) 2020, Caltech IPAC.
# This code is released with a BSD 3-clause license. License information is at
#   https://github.com/Caltech-IPAC/nexsciTAP/blob/master/LICENSE

import sys
import os
import logging

import pprint

import configobj

from TAP.configparam import configParam
from ADQL.adql       import ADQL
from spatial_index   import SpatialIndex

class tapUtil:

    def __init__(self, **kwargs):

        self.conn = None

        pid = os.getpid()

        sys.tracebacklimit = 0

        debug = False
        if 'debug' in kwargs:
            debug = kwargs['debug']

        if debug:
            logging.debug('')
            logging.debug('In tapUtil().')

        store = None
        if 'store' in kwargs:
            store = kwargs['store']

        configpath = None
        if 'configpath' in kwargs:
            configpath = kwargs['configpath']

        instance = None
        if 'instance' in kwargs:
            instance = kwargs['instance']

        if configpath == None:
            if 'TAP_CONF' in os.environ:
                configpath = os.environ['TAP_CONF']
            else:
                raise Exception('No config file path given or in TAP_CONF environment variable.')


        # ADQL/spatial-index config

        config = None

        try:
            config = configParam(configpath, instance=instance, debug=debug)

            if store != None:
                store.setConfig(config)

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

            self.adql = ADQL(dbms=dbms, mode=mode, level=level, indxcol=colname,
                             encoding=encoding, racol=racol, deccol=deccol,
                             xcol=xcol, ycol=ycol, zcol=zcol)

            if store != None:
                store.setADQL(self.adql)



            # Get DB connection info

            connectInfo = config.connectInfo

            if ('dbserver' in connectInfo):
                dbserver = connectInfo['dbserver']

            if ('hostname' in connectInfo):
                hostname = connectInfo['hostname']

            socket = None
            if ('socket' in connectInfo):
                socket = connectInfo['socket']
            
            port = None
            if ('port' in connectInfo):
                port = connectInfo['port']

                if (port is not None):
                    port = int(port)

            if (dbserver is None and hostname is None and socket is None):

                msg = 'Failed to retrieve required input DB server ' \
                    'parameter [dbserver] or [socket]'
                raise Exception(msg)

            if('userid' in connectInfo):
                userid = connectInfo['userid']

            if(userid == None and 'username' in connectInfo):
                userid = connectInfo['username']

            if(userid is  None):
                msg = 'Failed to retrieve required input parameter'\
                           ' [userid/username]'
                raise Exception(msg)

            if('password' in connectInfo):
                password = connectInfo['password']

            if(password is None):
                msg = 'Failed to retrieve required input parameter'\
                               ' [password]'
                raise Exception(msg)

            if('dbschema' in connectInfo):
                db = connectInfo['dbschema']

            if('database' in connectInfo):
                database = connectInfo['database']

            if debug:
                logging.debug('')
                logging.debug('dbserver = ' + str(dbserver))
                logging.debug('hostname = ' + str(hostname))
                logging.debug('port     = ' + str(port))
                logging.debug('socket   = ' + str(socket))
                logging.debug('db       = ' + str(db))
                logging.debug('database = ' + str(database))
                logging.debug('userid   = ' + str(userid))
                logging.debug('password = ' + str(password))

        except Exception as e:
            raise Exception(e)


        #
        # Connect to DBMS
        #

        # ORACLE

        if(dbms.lower() == 'oracle'):

            import cx_Oracle

            try:
                self.conn = cx_Oracle.connect(userid,
                                              password,
                                              dbserver)

                if debug:
                    logging.debug('')
                    logging.debug('connected to Oracle, DB ' + dbserver)

            except Exception as e:
                msg = 'Failed to connect to cx_Oracle'
                raise Exception(msg)


        # POSTGRESQL

        elif(dbms.lower() == 'pgsql'):

            import psycopg2

            try:
                self.conn = psycopg2.connect (
                    host=hostname, \
                    database=database, \
                    user=userid, \
                    password=password
                )

                if debug:
                    logging.debug('')
                    logging.debug('connected to postgresql DB ' + hostname)

            except Exception as e:

                self.status = 'error'
                self.msg = 'Failed to connect to pgsql: ' + str(e)

                raise Exception(self.msg)


        # SQLITE3

        elif(dbms.lower() == 'sqlite3'):

            import sqlite3

            try:
                self.conn = sqlite3.connect(db)

                if debug:
                    logging.debug('')
                    logging.debug('connected to SQLite3, database ' + db)

                cmd = 'ATTACH DATABASE ? AS ' + tap_schema_file

                dbspec = (tap_schema,)

                if debug:
                    logging.debug('')
                    logging.debug('cmd: ' + cmd + '(' + tap_schema + ')')

                cursor = self.conn.cursor()

                cursor.execute(cmd, dbspec)

                if debug:
                    logging.debug('')
                    logging.debug('TAP_SCHEMA attached')

            except Exception as e:
                msg = 'Failed to connect to SQLite3 databases'
                raise Exception(msg)


        # MYSQL

        elif (dbms.lower() == 'mysql'):
   
            import msql.connector
       
            try:
                if (dbserver is not None):

                    self.conn = mysql.connector.connect (
                            user=userid, \
                            password=password, \
                            host=dbserver, \
                            port=port, \
                            db=db
                    )
                
                elif (socket is not None):

                    self.conn = mysql.connector.connect (
                            user=userid, \
                            password=password, \
                            unix_socket=socket, \
                            db=db
                    )
                
                else:
                    msg = 'Failed to connect to mysql databases'
                    raise Exception(msg)

                if debug:
                    logging.debug('')
                    logging.debug('mysql connected')

            except Exception as e:
                msg = 'Failed to connect to mysql databases'
                raise Exception(msg)

            if debug:
                logging.debug('')
                logging.debug('here0')
           
        else:
            msg = 'Invalid DBMS'
            raise Exception(msg)

        if store != None:
            store.setConn(self.conn)


    def ADQL(self, adqlstr):

        try:
            sqlstr = self.adql.sql(adqlstr)
        except Exception as e:
            raise Exception(e)

        return sqlstr
