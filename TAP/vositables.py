# Copyright (c) 2020, Caltech IPAC.
# This code is released with a BSD 3-clause license. License information is at
#   https://github.com/Caltech-IPAC/nexsciTAP/blob/master/LICENSE


import sys
import os
import logging

import datetime

import argparse


class vosiTables:
#
# { class vosiTables
#

    pid = os.getpid()

    debug = 0

    debugfname = './vosi.debug'
    status = ''
    msg = ''
    returnMsg = ""

    arraysize = 10000
    
    #
    # DD columns
    #

    dd = None

    conn = None
    dbtable = ''

    sql = ''
    racol = ''
    deccol = ''

    nfetch = 1000

    outpath = ''

    #ntot = 0

    format = 'votable'
    maxrec = -1
    coldesc = 0

    nschema = 0
    
    schema_namearr = []
    schema_descarr = []
    
    
    def __init__(self, **kwargs):
    
    #
    # { vosiTables.init method
    #

        """
        vosiTables class generates the VOSI compatible XML outputs for TAP 
        query with endpoint TAP/tables 

        Required keyword input parameters:

            connectInfo (python dictionary):    
                a python dictionary containing info needed to make 
                a "connection" to a DBMS.  
            
            ourpath(char):    output table file path 

        Usage:

            vosi = vosiTables (\
                 dbms=dbms,
                 dbserver=dbserver, \
                 userid=userid, \
                 password=password, \
                 outpath=outpath, \
                 debug=debug)
        """

        self.dbms = '' 
        self.dbserver = ''
        self.userid = ''
        self.password = ''
        self.outpath = ''
        self.debug = 0 
   
        self.db = ''
        self.tap_schema = ''

        if('debug' in kwargs):
            self.debug = kwargs['debug']

        if self.debug:
            logging.debug('')
            logging.debug('Enter vosiTables.init')

        if('dbms' in kwargs):
            self.dbms = kwargs['dbms']

        if('dbserver' in kwargs):
            self.dbserver = kwargs['dbserver']

        if('userid' in kwargs):
            self.userid = kwargs['userid']

        if('password' in kwargs):
            self.password = kwargs['password']

        if('outpath' in kwargs):
            self.outpath = kwargs['outpath']

        if('db' in kwargs):
            self.db = kwargs['db']

        if('tap_schema' in kwargs):
            self.tap_schema = kwargs['tap_schema']

        #
        # Get keyword parameters
        #
          
        if ((self.dbms is None) or (len(self.dbms) == 0)):
            self.msg = 'Database name missing.'
            self.status = 'error'
            #self.__printVosiError__ (self.msg)
            
            raise Exception (self.msg)

        if self.debug:
            logging.debug('')
            logging.debug(f'dbms= {self.dbms:s}')
   
   
        if (self.dbms.lower() == 'oracle'):

            if ((self.dbserver is None) or (len(self.dbserver) == 0)):
                self.msg = 'Database server name missing.'
                self.status = 'error'
                #self.__printVosiError__ (self.msg)
                raise Exception (self.msg)

     
            if self.debug:
                logging.debug('')
                logging.debug(f'dbserver= {self.dbserver:s}')
        
            if ((self.userid is None) or (len(self.userid) == 0)):
                self.msg = 'Database userid missing.'
                self.status = 'error'
                #self.__printVosiError__ (self.msg)
                raise Exception (self.msg)

     
            if self.debug:
                logging.debug('')
                logging.debug(f'userid= {self.userid:s}')
        
            if ((self.password is None) or (len(self.password) == 0)):
                self.msg = 'Database password missing.'
                self.status = 'error'
                #self.__printVosiError__ (self.msg)
                raise Exception (self.msg)

     
            if self.debug:
                logging.debug('')
                logging.debug(f'password= {self.password:s}')
        
        elif (self.dbms.lower() == 'sqlite3'):

            if ((self.db is None) or (len(self.db) == 0)):
                self.msg = 'Database name missing.'
                self.status = 'error'
                #self.__printVosiError__ (self.msg)
                raise Exception (self.msg)

     
            if self.debug:
                logging.debug('')
                logging.debug(f'db= {self.db:s}')
        
            if ((self.tap_schema is None) or (len(self.tap_schema) == 0)):
                
                self.msg = 'Database table name missing.'
                self.status = 'error'
                #self.__printVosiError__ (self.msg)
                raise Exception (self.msg)

     
            if self.debug:
                logging.debug('')
                logging.debug(f'tap_schema= {self.tap_schema:s}')
      

        if ((len(self.outpath) == 0) or (self.outpath is None)):
            self.msg = 'Required output path is missing.'
            #self.__printVosiError__ (self.msg)
            raise Exception (self.msg)

     
        if self.debug:
            logging.debug('')
            logging.debug(f'outpath= {self.outpath:s}')

        #
        # { Connect to DBMS
        #
        if(self.dbms.lower() == 'oracle'):

            import cx_Oracle
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

                #self.__printVosiError__ (self.msg)
                raise Exception (self.msg)



        elif (self.dbms.lower() == 'sqlite3'):

            import sqlite3

            self.db = ''
            if('db' in self.connectInfo):
                self.db = self.connectInfo['db']

            if(len(self.db) == 0):
                self.msg = 'Failed to retrieve required input parameter'\
                    ' [db]'
                self.status = 'error'
                #self.__printVosiError__ (self.msg)
                raise Exception (self.msg)



            self.tap_schema = ''
            if('tap_schema' in self.connectInfo):
                self.tap_schema = self.connectInfo['tap_schema']

            if(len(self.tap_schema) == 0):
                self.msg = 'Failed to retrieve required input parameter'\
                    ' [tap_schema]'
                self.status = 'error'
                #self.__printVosiError__ (self.msg)
                raise Exception (self.msg)


            if self.debug:
                logging.debug('')
                logging.debug(f'db= {self.db:s}')
                logging.debug(f'tap_schema= {self.tap_schema:s}')

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
                #self.__printVosiError__ (self.msg)
                raise Exception (self.msg)


        else:
            self.msg = f'Invalid DBMS: {self.dbms:s}'
            self.status = 'error'
            #self.__printVosiError__ (self.msg)
            raise Exception (self.msg)


        #
        # } end Connect to DBMS
        #
        
        #
        # start loop for constructing output:
        # 
        #   -- vosi header: tableset tag,
        #   -- query for schemas: schema tag,
        #   -- for each schema, query tables: table tag,
        #   -- for each table, query columns: column tag,
        #   -- end column tag,
        #   -- for each table, query foreignkey: foreignkey tag,
        #   -- end foreignkey tag
        #   -- end table tag,
        #   -- end schema tag
        #   -- end tableset tag
        
        #
        #   open ouput file in workdir for vosi table output
        #
        #vositblpath = self.workdir + '/vositable.xml'
        vositblpath = self.outpath
        
        if self.debug:
            logging.debug('')
            logging.debug(f'vositblpath = {vositblpath:s}')

          
        fp = None
        try:
            fp = open (vositblpath, 'w')
            os.chmod(vositblpath, 0o664)

        except Exception as e:
        
            if self.debug:
                logging.debug('')
                logging.debug(f'open voditblpath exception: {str(e):s}')

            self.msg = 'Failed to open/create vositbl file.'
            #self.__printVosiError__(self.msg)
            raise Exception (self.msg)


        #
        # write vositable header
        #
        fp.write ("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
        fp.write("<vosi:tableset\n")
        fp.write("   xmlns:vosi=\"http://www.ivoa.net/xml/VOSITables/v1.0\" \n")
        fp.write("   xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \n")
        fp.write("   xmlns:vod=\"http://www.ivoa.net/xml/VODataService/v1.1\" > \n")
        fp.flush()

        #
        # Submit database query for TAP_SCHEMA.schemas
        #

        self.sql = "select schema_name, description from TAP_SCHEMA.schemas order by schema_index"

        if self.debug:
            logging.debug('')
            logging.debug(f'sql = {self.sql:s}')
            logging.debug('call execute sql')

        cursor = self.conn.cursor()

        try:
            self.__executeSql__(cursor, self.sql)

        except Exception as e:

            if self.debug:
                logging.debug('')
                logging.debug(f'executeSql exception: {str(e):s}')
            
            #self.__printVosiError__ (str(e))
            raise Exception (str(e))


        if self.debug:
            logging.debug('')
            logging.debug('returned executeSql')


        try:
            if self.debug:
                logging.debug('')
                logging.debug('call getSchemaQueryArr__:')

            self.__getSchemaQueryArr__ (cursor, \
                                    debug=self.debug)
        
            if self.debug:
                logging.debug('')
                logging.debug('returned getSchemaDesc')

        except Exception as e:

            if self.debug:
                logging.debug('')
                logging.debug(f'writeResult exception: {str(e):s}')

            #self.__printVosiError__ (str(e))
            raise Exception (str(e))

       
        self.nschema = len (self.schema_namearr)
        
        if self.debug:
            logging.debug('')
            logging.debug(f'nschema= {self.nschema:d}')
            logging.debug('')
            logging.debug('schema name and desc')

            for i in range (0,self.nschema):
                logging.debug(f'{i:d} schema_name={self.schema_namearr[i]:s}')
                logging.debug(f'schema_description={self.schema_descarr[i]:s}')

        for ischema in range (0, self.nschema): 
        #
        # { ischema loop
        #
        #   for each schema:
        #   1. write header block for each schema,
        #
            fp.write("    <schema>\n")
            fp.write("        <name>" + \
                self.schema_namearr[ischema] + "</name>\n")
            fp.write("        <description>" + \
                self.schema_descarr[ischema] + "</description>\n")
            fp.flush() 
    
        
        #
        #   2. execute table sql
        #
            self.sql = "select table_name, description, table_type " + \
                "from TAP_SCHEMA.tables where schema_name='" + \
                self.schema_namearr[ischema] + "' order by table_index"

            if self.debug:
                logging.debug('')
                logging.debug(f'ischema= {ischema:d}')
                logging.debug(f'schema_name= {self.schema_namearr[i]:s}')
                logging.debug('')
                logging.debug(f'sql = {self.sql:s}')
                logging.debug('call execute sql')

            cursor = self.conn.cursor()

            try:
                self.__executeSql__(cursor, self.sql)

            except Exception as e:

                if self.debug:
                    logging.debug('')
                    logging.debug(f'executeSql exception: {str(e):s}')
            
                #self.__printVosiError__ (str(e))
                raise Exception (str(e))


            if self.debug:
                logging.debug('')
                logging.debug('returned executeSql')
                logging.debug('')
                logging.debug('call getTableQueryArr')

        # 
        #   3. getSqlresult of table sql 
        #      re-initialize arrays
        #

            table_namearr = []
            table_descarr = []
            table_typearr = []
    
            ntable = 0
            try:
                ntable = self.__getTableQueryArr__(cursor, table_namearr, \
                    table_descarr, table_typearr, debug=1)

            except Exception as e:

                if self.debug:
                    logging.debug('')
                    logging.debug(f'getTableQueryArr exception: {str(e):s}')
            
                #self.__printVosiError__ (str(e))
                raise Exception (str(e))


            if self.debug:
                logging.debug('')
                logging.debug('returned getTableQueryArr')
                logging.debug(f'ntable= {ntable:d}')
          
          
                for itable in range (0, ntable): 
                    logging.debug ('')
                    logging.debug (f'itable= {itable:d}')
                    logging.debug (
                        f'table_name= {table_namearr[itable]:s}')
                    logging.debug (
                        f'table_desc= {table_descarr[itable]:s}')
                    logging.debug (
                        f'table_type= {table_typearr[itable]:s}')
    
            #
            #    for each table:
            #

            for itable in range (0, ntable): 
            #
            # { itable loop
            #   
            #    1. write header block for each table,
            #
                fp.write("        <table type=\"" + \
                    table_typearr[itable] + "\">\n")
                fp.write("            <name>" + \
                    table_namearr[itable] + "</name>\n")
                fp.write("             <description>" + \
                    table_descarr[itable] + "</description>\n")


            #
            #    2. execute column sql
            #
                self.sql = "select column_name, description, unit, ucd, "  + \
                    "datatype, principal, indexed from TAP_SCHEMA.columns " + \
                    "where table_name='" + table_namearr[itable] + \
                    "' order by column_index"
                
                if self.debug:
                    logging.debug('')
                    logging.debug(f'sql = {self.sql:s}')
                    logging.debug('call execute sql')

                cursor = self.conn.cursor()

                try:
                    self.__executeSql__(cursor, self.sql)

                except Exception as e:

                    if self.debug:
                        logging.debug('')
                        logging.debug(f'executeSql exception: {str(e):s}')
            
                    #self.__printVosiError__ (str(e))
                    raise Exception (str(e))


                if self.debug:
                    logging.debug('')
                    logging.debug('returned executeSql')

            #
            #    3. getSqlresult of column sql and write column result
            #
                try:
                    self.__writeOneTableResult__ (cursor, fp, **kwargs)

                except Exception as e:

                    if self.debug:
                        logging.debug('')
                        logging.debug(
                            f'writeOneTableResult exception: {str(e):s}')
            
                    #self.__printVosiError__ (str(e))
                    raise Exception (str(e))


                if self.debug:
                    logging.debug('')
                    logging.debug('returned writeOneTableResult')

            #
            #    4. execute foreignkey sq
            #    5. getSqlresult of foreignkey sql 
            #    -- write foreignkey result

                self.sql = "select key_id as foreignKey, " + \
                    "target_table as targetTable,  " + \
                    "from_column as fromColumn, " + \
                    "target_column as targetColumn, description "  + \
                    "from TAP_SCHEMA.keys " + \
                    "NATURAL JOIN TAP_SCHEMA.key_columns " + \
                    "where from_table='" + table_namearr[itable] + "'"
                
                if self.debug:
                    logging.debug('')
                    logging.debug(f'foreign key sql:');
                    logging.debug(f'sql= {self.sql:s}');
                    logging.debug('call execute sql')

                cursor = self.conn.cursor()

                try:
                    self.__executeSql__(cursor, self.sql)

                except Exception as e:

                    if self.debug:
                        logging.debug('')
                        logging.debug(f'executeSql exception: {str(e):s}')
            
                    #self.__printVosiError__ (str(e))
                    raise Exception (str(e))


                if self.debug:
                    logging.debug('')
                    logging.debug('returned executeSql')

            #
            #    5. getSqlresult of foreignkey sql and write foreignkey result
            #
                try:
                    self.__writeForeignKey__ (cursor, fp, **kwargs)

                except Exception as e:

                    if self.debug:
                        logging.debug('')
                        logging.debug(
                            f'writeForeignKey exception: {str(e):s}')
            
                    #self.__printVosiError__ (str(e))
                    raise Exception (str(e))



                if self.debug:
                    logging.debug('')
                    logging.debug('returned writeForeignKey')


            #
            #    6. write end block for each table
            #
                fp.write("        </table>\n")
                fp.flush() 
            
            #
            # } end itable loop
            #
            
            #
            #    2. write end block for each schema
            #
            fp.write("    </schema>\n")
            fp.flush() 

        #
        # } end ischema loop and write end block for xml file
        #
        fp.write("</vosi:tableset>\n")
        fp.flush() 
        
    #
    # } end vosiTables.init
    #


    def __getTableQueryArr__ (self, cursor, table_namearr, table_descarr, \
        table_typearr, **kwargs):
    #
    # { vosiTable.getTableQueryArr
    #
    #   this query contains only three char columns:
    #   table_name, description, and table_type;
    #

        if self.debug:
            logging.debug('')
            logging.debug('Enter getTableQueryArr')

        size = 0

        table_colname = []
        table_dbtype = []

        nfetch = self.arraysize
        cursor.arraysize = nfetch

        if self.debug:
            logging.debug('')
            logging.debug(f'nfetch = {nfetch:d}')


        i = 0
        for col in cursor.description:

        #
        # { For loop for cursor description:
        #   cursor description is a list with ncols tuples:
        #   each tuple
        #   contains each output colname's name, datatype,
        #   dbdatatype, size, etc...
        #

            if self.debug:
                logging.debug('')
                logging.debug('get cursor description:')
                logging.debug(f'i = {i:d} col = ' + str(col))

            #
            # Extract colname, dbdatatype, size, precision, scale from
            # col array
            #
            colname = str(col[0]).lower()
            table_colname.append(colname)

            if self.debug:
                logging.debug('')
                logging.debug(f'colname(lower) = {colname:s}')


            dbtype = ''
            size = None

            dbdatatypestr = str(col[1])

            if self.debug:
                logging.debug('')
                logging.debug(f'dbdatatypestr = {dbdatatypestr:s}')

            ind = dbdatatypestr.find("VARCHAR")
            if(ind != -1):
                dbtype = 'STRING'

            ind = dbdatatypestr.find("STRING")
            if(ind != -1):
                dbtype = 'STRING'

            ind = dbdatatypestr.find("NUMBER")
            if(ind != -1):
                dbtype = 'NUMBER'

            if self.debug:
                logging.debug(f'dbtype    = {dbtype:s}')

            table_dbtype.append(dbtype)

            i = i + 1

        #
        # } end of for loop for analysing cursor description
        #

        #
        # Start fetching data lines
        #

        if self.debug:
            logging.debug('')
            logging.debug('Start fetching data:')

        if self.debug:
            logging.debug('')
            logging.debug(f'nfetch= {nfetch:d}')

        irow = 0
        ntot = 0

        while True:

        #
        # { start of while loop for fetching data lines;
        #   max 10000 lines at a time
        #

            rows = cursor.fetchmany()

            nrec = len(rows)

            if self.debug:
                logging.debug(f'nrec = {nrec:d}')
                logging.debug('')

            
            for ll in range(0, nrec):

            #
            # { Beginning ll loop: one row
            #

                row = rows[ll]
                
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'len(row)= {len(row):d}')
                    logging.debug ('')
                    logging.debug ('row:')
                    logging.debug(row)

                for i in range(0, len(row)):

                #
                # { Beginning i loop: one col
                #
                    data = ''

                    if (table_dbtype[i] != 'STRING'):

                        data = str(row[i])
                    else:
                        data = row[i]

                    if self.debug:
                        logging.debug (f'i= {i:d}, data= {data:s}')
                        logging.debug (f'table_colname= {table_colname[i]:s}')
                    
                    if (table_colname[i].lower() == 'table_name'):
                        table_namearr.append (data)
                    
                    elif (table_colname[i].lower() == 'description'):
                        table_descarr.append (data)
                    
                    elif (table_colname[i].lower() == 'table_type'):
                        table_typearr.append (data)

                #
                # } end of i loop
                #

                irow = irow + 1

            #
            # } end of ll loop
            #      
            if self.debug:
                logging.debug('')
                logging.debug(table_namearr)
                logging.debug(table_descarr)
                logging.debug(table_typearr)
                logging.debug('----------------------------------------')

            
            ntot = ntot + nrec

            if(len(rows) < cursor.arraysize):
                break

            if self.debug:
                logging.debug ('')
                logging.debug (f'ntot= {ntot:d}')
        #
        # } end while loop for fetching data lines
        #

        return (ntot)

    #
    # } end vosiTables.getTableQueryArr
    #


   
    def __getSchemaQueryArr__ (self, cursor,  **kwargs):
    #
    # { vosiTables.getSchemaQueryArr
    #   
    #   this query contains only two columns:
    #   schema_name and schema_description
    #
        if self.debug:
            logging.debug('')
            logging.debug(f'Enter __getSchemaQueryArr__\n')

        size = 0
        nfetch = self.arraysize
        cursor.arraysize = nfetch
        
        if self.debug:
            logging.debug('')
            logging.debug(f'nfetch= {nfetch:d}')


        i = 0
        for col in cursor.description:

        #
        # { For loop for cursor description:
        #   cursor description is a list with ncols tuples:
        #   each tuple #   contains each output colname's name, datatype,
        #   dbdatatype, size, etc...
        #

            if self.debug:
                logging.debug('')
                logging.debug('----------------------------------------------')
                logging.debug(f'i = {i:d} col = ' + str(col))

            #
            # Extract colname, dbdatatype, size, precision, scale from
            # col array
            #

            colname = str(col[0]).lower()
  
            if self.debug:
                logging.debug('')
                logging.debug(f'colname(lower) = {colname:s}')

            #
            # { Extract dbdatatype, display_size, precision, scale from
            # col_array:
            #   col[1], col[2], col[4], and col[5]
            #

            dbdatatype = ''
            size = None
            precision = None
            scale = None

            dbdatatypestr = str(col[1])

            if self.debug:
                logging.debug('')
                logging.debug(f'dbdatatypestr = {dbdatatypestr:s}')

            ind = dbdatatypestr.find("VARCHAR")
            if(ind != -1):
                dbdatatype = 'VARCHAR'

            ind = dbdatatypestr.find("STRING")
            if(ind != -1):
                dbdatatype = 'STRING'

            size = None
            if(col[2] is not None):

                size = col[2]
                if(len(colname) > size):
                    size = len(colname)

                if self.debug:
                    logging.debug(f'size      = {size:d}')

            #
            # } end  extract dbdatatype, display_size, precision,
            #   scale from
            #   col_array: col[1], col[2], col[4], and col[5]


            dbtype = ''
            datatype = ''
            fmt = ''
            width = 0

            if(dbdatatype == 'STRING') or (dbdatatype == 'VARCHAR'):

                #
                # { dbdatatype == string
                #

                width = 0
                if(size is not None):
                    width = size

                if(len(colname) > width):
                    width = len(colname)

                datatype = 'char'
                dbtype = dbdatatype
                fmt = str(width) + 's'

                #
                # } end dbdatatype == STRING
                #

            else:
                #
                # { unknown type: default to char
                #

                datatype = 'char'
                dbtype = dbdatatype

                width = 200
                if(len(colname) > width):
                    width = len(colname)
                fmt = str(width) + 's'

                #
                # } end unknown type
                #

            if self.debug:
                logging.debug('')
                logging.debug(f'      width   = {width:d}')
                logging.debug(f'      dbtype  = {dbtype:s}')
                logging.debug(f'      datatype = {datatype:s}')
                logging.debug(f'      fmt     = {fmt:s}')

            i = i + 1

        #
        # } end of for loop for analysing cursor description
        #

        if self.debug:
            logging.debug('')
            logging.debug('Start fetching data')
            logging.debug('----------------------------------------')

        #
        # Start fetching data lines
        #

        nfetch = self.arraysize

        cursor.arraysize = nfetch

        irow = 0
        ntot = 0

        while True:

        #
        # { start of while loop for fetching data lines;
        #   max 10000 lines at a time
        #

            rows = cursor.fetchmany()

            nrec = len(rows)

            if self.debug:
                logging.debug(f'nrec = {nrec:d}')
                logging.debug('')


            for ll in range(0, nrec):

            #
            # { Beginning ll loop: one row
            #

                row = rows[ll]
                
                if self.debug:
                    logging.debug(row)
                    logging.debug(f'row[0]= {row[0]:s}')
                    logging.debug(f'row[1]= {row[1]:s}')


                self.schema_namearr.append(row[0])
                self.schema_descarr.append(row[1])
                
                if self.debug:
                    logging.debug(row)
                    logging.debug(self.schema_namearr)
                    logging.debug(self.schema_descarr)

                irow = irow + 1

            #
            # } end of ll loop
            #

            if self.debug:
                logging.debug('----------------------------------------')
                logging.debug(f'irow= {irow:d}')

            ntot = ntot + nrec

            if(len(rows) < cursor.arraysize):
                break

        #
        # } end while loop for fetching data lines
        #

        return

    #
    # } end vosiTables.getSchemaQueryArr
    #


    def __writeOneTableResult__ (self, cursor, fp,  **kwargs):

    #
    # {vosiTables.writeOneTableResult
    #
        if self.debug:
            logging.debug('')
            logging.debug('Enter __writeOneTableResult')

        cursor.arraysize = self.arraysize

        dbtype = []

        i = 0
        for col in cursor.description:

            #
            # { For loop for cursor description:
            #   cursor description is a list with ncols tuples:
            #   each tuple
            #   contains each output colname's name, datatype,
            #   dbdatatype, size, etc...
            #

            if self.debug:
                logging.debug('')
                logging.debug(f'i = {i:d} col = ' + str(col))

            #
            # Extract colname, dbdatatype, size, precision, scale from
            # col array
            #

            colname = str(col[0]).lower()

            if self.debug:
                logging.debug('')
                logging.debug(f'colname(lower) = {colname:s}')

            #
            # { Extract dbdatatype, display_size, precision, scale from
            # col_array:
            #   col[1], col[2], col[4], and col[5]
            #

            dbdatatype = ''
            size = None

            dbdatatypestr = str(col[1])

            if self.debug:
                logging.debug('')
                logging.debug(f'dbdatatypestr = {dbdatatypestr:s}')

            ind = dbdatatypestr.find("VARCHAR")
            if(ind != -1):
                dbdatatype = 'STRING'

            ind = dbdatatypestr.find("STRING")
            if(ind != -1):
                dbdatatype = 'STRING'

            if self.debug:
                logging.debug(f'dbdatatype    = {dbdatatype:s}')

            dbtype.append(dbdatatype)
            #
            # } end  extract dbdatatype from col_array: col[1]
            #
  
            i = i + 1

        #
        # } end of for loop for analysing dd's cursor description
        #

        #
        # Start fetching data lines
        #

        nfetch = self.arraysize
        cursor.arraysize = nfetch

        irow = 0
        ntot = 0

        while True:

        #
        # { start of while loop for fetching data lines;
        #   max 10000 lines at a time
        #

            rows = cursor.fetchmany()
            nrec = len(rows)

            if self.debug:
                logging.debug(f'nrec = {nrec:d}')
                logging.debug('')

            for ll in range(0, nrec):


                #
                # { Beginning ll loop: one row
                #

                row = rows[ll]
    
                col_name = ''
                col_desc = ''
                col_unit = ''
                col_ucd = ''
                col_datatype = ''
                col_principal = ''
                col_indexed = ''

                data = '' 

                for i in range(0, len(row)):
                
                    if (row[i] is None):
                        data = 'None'
                    elif (dbtype[i] == 'STRING'):
                        data = row[i]
                    else:
                        data = str(row[i])

                    if self.debug:
                        logging.debug('')
                        logging.debug(f'i= {i:d}, data= {data:s}')

                    if (i == 0):
                        col_name = data
                    elif (i == 1):
                        col_desc = data
                    elif (i == 2):
                        col_unit = data
                    elif (i == 3):
                        col_ucd = data
                    elif (i == 4):
                        col_datatype = data
                    elif (i == 5):
                        col_principal = data
                    elif (i == 6):
                        col_indexed = data

                irow = irow + 1

                if col_datatype == 'timestamp':
                    col_datatype = 'char'


                #
                #    for row, write column block
                #
                fp.write("            <column>\n")
                fp.write("                <name>" + col_name + "</name>\n")
                fp.write("                <description>" + \
                    col_desc + "</description>\n")
        
                if (col_unit != 'None'):
                    fp.write("                <unit>" + col_unit + "</unit>\n")
       
                if (col_ucd != 'None'):
                    fp.write ("                <ucd>" + col_ucd + "</ucd>\n")
                
                fp.write ("                " + \
                        "<dataType xsi:type=\"vod:VOTableType\">" + \
                        col_datatype + "</dataType>\n")
           
                if (col_principal != 'None' and col_principal != 0 \
                        and col_principal != '0'):
                    fp.write("                " + \
                        "<flag>principal</flag>\n")
           
                if (col_indexed != 'None' and col_indexed != 0 \
                        and col_indexed != '0'):
                    fp.write("                " + "<flag>indexed</flag>\n")
        
                fp.write("            </column>\n")
       

                #
                # } end of ll loop
                #

            ntot = ntot + nrec

            if(len(rows) < cursor.arraysize):
                break 

            if self.debug:
                logging.debug('')
                logging.debug(f'ntot= {ntot:d}')

        #
        # } end of while loop for fetching data lines
        #

        return

    #
    # }end vosiTables.writeOneTableResult
    #


    def __writeForeignKey__ (self, cursor, fp,  **kwargs):

    #
    # {vosiTables.writeForeignKey
    #
        if self.debug:
            logging.debug('')
            logging.debug('Enter __writeForeignKey')

        cursor.arraysize = self.arraysize

        dbtype = []

        i = 0
        for col in cursor.description:

            #
            # { For loop for cursor description:
            #   cursor description is a list with ncols tuples:
            #   each tuple
            #   contains each output colname's name, datatype,
            #   dbdatatype, size, etc...
            #

            if self.debug:
                logging.debug('')
                logging.debug(f'i = {i:d} col = ' + str(col))

            #
            # Extract colname, dbdatatype, size, precision, scale from
            # col array
            #

            colname = str(col[0]).lower()

            if self.debug:
                logging.debug('')
                logging.debug(f'colname(lower) = {colname:s}')

            #
            # { Extract dbdatatype, display_size, precision, scale from
            # col_array:
            #   col[1], col[2], col[4], and col[5]
            #

            dbdatatype = ''
            size = None

            dbdatatypestr = str(col[1])

            if self.debug:
                logging.debug('')
                logging.debug(f'dbdatatypestr = {dbdatatypestr:s}')

            ind = dbdatatypestr.find("VARCHAR")
            if(ind != -1):
                dbdatatype = 'STRING'

            ind = dbdatatypestr.find("STRING")
            if(ind != -1):
                dbdatatype = 'STRING'

            if self.debug:
                logging.debug(f'dbdatatype    = {dbdatatype:s}')

            dbtype.append(dbdatatype)
            #
            # } end  extract dbdatatype from col_array: col[1]
            #
  
            i = i + 1

        #
        # } end of for loop for analysing dd's cursor description
        #

        #
        # Start fetching data lines
        #

        nfetch = self.arraysize
        cursor.arraysize = nfetch

        irow = 0
        ntot = 0

        while True:

        #
        # { start of while loop for fetching data lines;
        #   max 10000 lines at a time
        #

            rows = cursor.fetchmany()
            nrec = len(rows)

            if self.debug:
                logging.debug(f'nrec = {nrec:d}')
                logging.debug('')

            for ll in range(0, nrec):


                #
                # { Beginning ll loop: one row
                #

                row = rows[ll]
    
                keyid = ''
                targettbl = ''
                fromcol = ''
                targetcol= ''
                desc = ''

                data = '' 
                for i in range(0, len(row)):
                
                    if (row[i] is None):
                        data = 'None'
                    elif (dbtype[i] == 'STRING'):
                        data = row[i]
                    else:
                        data = str(row[i])

                    if self.debug:
                        logging.debug('')
                        logging.debug(f'i= {i:d}, data= {data:s}')

                    if (i == 0):
                        keyid = data
                    elif (i == 1):
                        targettbl = data
                    elif (i == 2):
                        fromcol = data
                    elif (i == 3):
                        targetcol = data
                    elif (i == 4):
                        desc = data

                irow = irow + 1
            
                #
                #    for row, write column block
                #
                fp.write("            <foreignKey>\n")
                fp.write("                <targetTable>" + targettbl + \
                    "</targetTable>\n")
                
                fp.write("                <fkColumn>\n")
                fp.write("                    <fromColumn>" + fromcol + \
                    "</fromColumn>\n")
                fp.write("                    <targetColumn>" + targetcol + \
                    "</targetColumn>\n")
                fp.write("                </fkColumn>\n")

                fp.write("                <description>" + \
                    desc + "</description>\n")
                fp.write("            </foreignKey>\n")

                #
                # } end of ll loop
                #

            ntot = ntot + nrec

            if(len(rows) < cursor.arraysize):
                break 

            if self.debug:
                logging.debug('')
                logging.debug(f'ntot= {ntot:d}')

        #
        # } end of while loop for fetching data lines
        #

        return

    #
    # }end vosiTables.writeOneTableResult
    #










    def __executeSql__(self, cursor, sql, **kwargs):
    #
    # { vosiTables.executeSql
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
                logging.debug(f'create table exception: {str(msg):s}')

            # raise Exception(str(e))
            raise Exception(msg)

    #
    # } end vosiTables.executeSql
    #


    def __printVosiError__(self, errmsg):

        #
        # {
        #

        httphdr = "HTTP/1.1 500 ERROR\r"

        #print("HTTP/1.1 200 OK\r")
        
        print(httphdr)

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

        sys.stdout.flush()
        sys.exit()

        #
        # }  end of printError
        #


#
# } end class vosiTables:
#

def main():

    print ('Enter main')

    sys.tracebacklimit = 0

    parser = argparse.ArgumentParser(description='vosiTables')

    parser.add_argument('--dbms', required=True,
                        help='database (e.g. oracle, sqlite, etc..) to use.')

    parser.add_argument('--dbserver', required=False,
                        help='Oracle database server (dbserver) to use.')
    
    parser.add_argument('--userid', required=False,
                        help='Oracle dbserver userid (userid) to use.')
    
    parser.add_argument('--password', required=False,
                        help='Oracle dbserver (password) to use.')
    
    parser.add_argument('--db', required=False,
                        help='Sqlite database name to use.')
    
    parser.add_argument('--tap_schema', required=False,
                        help='Sqlite database table name to use.')
    
    parser.add_argument('--outpath', required=True,
                        help='outpath for the result.')

    parser.add_argument('--debug', required=False,
                        help='debugflag: 1/0.')

    args = parser.parse_args()

    dbms = ''
    dbserver = ''
    userid = ''
    password = ''
    outpath = ''
    debug = 0

    db = ''
    tap_schema = ''

    if (args.dbms is not None):
        dbms = args.dbms
    print (f'dbms= {dbms:s}')
    
    if (args.dbserver is not None):
        dbserver = args.dbserver
    print (f'dbserver= {dbserver:s}')
    
    if (args.userid is not None):
        userid = args.userid
    print (f'userid= {userid:s}')
    
    
    if (args.password is not None):
        password = args.password
    print (f'password= {password:s}')
    
    
    if (args.outpath is not None):
        outpath = args.outpath
    print (f'outpath= {outpath:s}')
    
    
    if (args.debug is not None):
        debug = args.debug
    print (f'debug= {debug:s}')
    
    if (args.db is not None):
        db = args.db
    print (f'db= {db:s}')
    
    if (args.tap_schema is not None):
        tap_schema = args.tap_schema
    print (f'tap_schema= {tap_schema:s}')
    
    
    if debug:
        debugfname = './vosi.debug'

        logging.basicConfig(filename=debugfname, \
            level=logging.DEBUG)

        print (f'debug turned on')
        
        logging.debug('')
        logging.debug('debug turned on')
        logging.debug(f'dbms= {dbms:s}')
        logging.debug(f'dbserver= {dbserver:s}')
        logging.debug(f'userid= {userid:s}')
        logging.debug(f'password= {password:s}')
        logging.debug(f'outpath= {outpath:s}')
        logging.debug(f'debug= {debug:s}')


    vositbl = None
    try:
        if debug:
            logging.debug('')
            logging.debug('call vosiTables')
        
        vositbl = vosiTables (dbms=dbms, \
            dbserver=dbserver, \
            userid=userid, \
            password=password, \
            outpath=outpath, \
            debug=debug)

        if debug:
            logging.debug('')
            logging.debug('vosiTables initialized')

        print('vosiTables initialized')

    except Exception as e:

        if debug:
            logging.debug('')
            logging.debug(f'vosiTable.init failed: {str(e):s}')

        print(f'vosiTable.init failed: {str(e):s}')


if __name__ == "__main__":
    main()
