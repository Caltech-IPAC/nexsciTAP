import sys
import os
import io
import logging

import time
import itertools
import cx_Oracle


class dataDictionary:

    pid = os.getpid()
    
    debug = 0 

    status = ''
    msg = ''
    returnMsg = ""

    conn = None
    dbtable = ''

#
#    dd columns
#
    ncols = 0
    colname = {}

    colfmt   = {}
    coltype  = {}
    coldesc  = {}
    colunits = {}
    colwidth = {}

    nfetch = 1000
   
    def __init__(self, conn, table, **kwargs):

        """
        A dataDictionary specifies the following properites of each column in
	a database table; it is used for re-formating the output IPAC ASCII 
	table.

        colname (char):       column name in DB table,
        datatype (char):      data type (char, integer, double, date),
        description (char):   description,
        units (char):         unit,
        format (char):        output format (20s, 20.10f, 10d, etc...),
        
       
	Required Input:

	    conn:              database connection handle,
	
	    table (char):      database table name,
        
         	
	Usage:

          dd = dataDictionary (conn, table)

        """

        if ('debug' in kwargs):
            self.debug = kwargs['debug']

        if self.debug:
            logging.debug ('')
            logging.debug (f'Enter dataDictionary.init')

        self.conn = conn
        self.dbtable = table

        if self.debug:
            logging.debug ('')
            logging.debug (f'dbtable= {self.dbtable:s}')
       

#
# construct and submit data dictionary query
#        
        cursor = self.conn.cursor()
        if self.debug:
            logging.debug ('')
            logging.debug ('cursor:')
            logging.debug (cursor)
        

#        sql = 'select * from ' + self.ddtable + ' order by cntr'

        sql = "select * from TAP_SCHEMA.columns where table_name = " + \
            "'" + self.dbtable + "'"
        
        if self.debug:
            logging.debug ('')
            logging.debug (f'sql= {sql:s}')
        
        try:
            cursor.execute(sql)
        
        except Exception as e:
              
            self.status = 'error'
            self.msg = 'Failed to execute [' + sql + ']'
	    
            if self.debug:
                logging.debug ('')
                logging.debug (f'errmsg= {self.msg:s}')
                logging.debug (f'str(e)= {str(e):s}')
            
            raise Exception (self.msg) 

        if self.debug:
            logging.debug ('')
            logging.debug ('select dd statement executed')
        
#        
#    Retrieve column name, dtype, format, units from 
#    TAP_SCHEMA.columns table.
#
#    col0: table_name (not used),
#    col1: colname (column_name),
#    col2: datatype (datatype),
#    col6: description (all nulls),
#    col8: unit (all nulls),
#    col14: format (format: in the form of 11d, 12.6f, 25.25s etc..,
#        
        cursor.arraysize = self.nfetch
        
        self.colfmt   = {}
        self.coltype  = {}
        self.coldesc  = {}
        self.colunits = {}
        self.colwidth = {}
        self.ncols = 0
        
        while True:
#
#{ while loop
#
            rows = cursor.fetchmany()
      
            if self.debug:
                logging.debug ('')
                logging.debug ('rows:')
                logging.debug (rows)

            self.ncols = self.ncols + len(rows)
        
            if self.debug:
                logging.debug ('')
                logging.debug (f'ncols= {self.ncols:d}')

            i = 0
            for row in rows:
#
#{ for loop: each row in the file represent a column in data dictionary
#
                col_str = str(row[1]).strip().upper()

#                if (col_str.lower() == 'ra2000'):
#                    col_str = 'RA'
		
#                if (col_str.lower() == 'dec2000'):
#                    col_str = 'DEC'

                self.colname[i] = col_str

                if self.debug:
                    logging.debug ('')
                    logging.debug (f'col_str= {col_str:s}')
                    logging.debug (\
		        f'i=[{i:d}] colname= {self.colname[i]:s}')
                
#
#    datatype 
#
                if (row[2] is None):
                    self.coltype[col_str] = ''
                else:
                    self.coltype[col_str] = str(row[2]).strip()

                if self.debug:
                    logging.debug ('')
                    logging.debug (f'coltype= {self.coltype[col_str]:s}')
                
                if (self.coltype[col_str] == 'integer'):
                    self.coltype[col_str] = 'int'
                
#
#    format 
#
                if (row[14] is None):
                    self.colfmt[col_str] = ''
                else:
                    self.colfmt[col_str] = str(row[14]).strip().replace('i', 'd')
                 
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'colfmt= {self.colfmt[col_str]:s}')
                
#
#    unit 
#
                if (row[8] is None):
                    self.colunits[col_str] = ''
                else:
                    self.colunits[col_str] = str(row[8]).strip()
                 
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'colunits= {self.colunits[col_str]:s}')
                
#
#    desc 
#
                if (row[6] is None):
                    self.coldesc[col_str] = ''
                else:
                    self.coldesc[col_str] = str(row[6]).strip()
                 
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'coldesc= {self.coldesc[col_str]:s}')
                
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'col_str= {col_str:s}')
                    logging.debug (f'coldesc= {self.coldesc[col_str]:s}')
                    logging.debug (f'coltype= {self.coltype[col_str]:s}')
                    logging.debug (f'colunits= {self.colunits[col_str]:s}')
                    logging.debug (f'colfmt= {self.colfmt[col_str]:s}')

#
#    determine width and fmt from colfmt arrary:
#
                fmtstr = ''
                width = 0

                if ((self.coltype[col_str] == 'char') or \
                    (self.coltype[col_str] == 'date')):
#
#{ coltype = char, date
#

                    if (self.colfmt[col_str] != 'null'):
#
# { extract width from colfmt
#   
                        fmtstr = self.colfmt[col_str]
                    
                        if self.debug:
                            logging.debug ('')
                            logging.debug ('extract width from fmtstr')
                            logging.debug (f'fmtstr= {fmtstr:s}')
			    
                        if ((self.coltype[col_str] == 'date') or \
                            (self.coltype[col_str] == 'char')):

                            ind= fmtstr.find('s')
                       
                            if (ind != -1):
                                widthstr = fmtstr[0:ind]
                                if self.debug:
                                    logging.debug ('')
                                    logging.debug (f'widthstr= [{widthstr:s}]')

                                width = int (widthstr)
                        
#
# } end extract width from colfmt
#   
                        
                    if self.debug:
                        logging.debug ('')
                        logging.debug (f'width= [{width:d}]')

#
#    failed to extract width from colfmt
#
                    if (width == 0):
                       width = 80

                    if (len(col_str) > width):
                        width = len(col_str)
                    if (len(self.coltype[col_str]) > width):
                        width = len(self.coltype[col_str])
                    if (len(self.colunits[col_str]) > width):
                        width = len(self.colunits[col_str])
                        
                    fmtstr = str(width) + 's'
                        
                    if self.debug:
                        logging.debug ('')
                        logging.debug (f'width= [{width:d}]')
                        logging.debug (f'fmtstr= [{fmtstr:s}]')

#
# } end coltype == 'char'/'date'
#
                        
                elif ((self.coltype[col_str] == 'integer') or \
                    (self.coltype[col_str] == 'int') or \
                    (self.coltype[col_str] == 'short')):
#
#{ coltype == 'int/short': extract width from fmt
#
                    width = 12
                    if (self.colfmt[col_str] != 'null'):
#
# { extract width from colfmt
#   
                        fmtstr = self.colfmt[col_str]
                    
                        if self.debug:
                            logging.debug ('')
                            logging.debug ('extract width from fmtstr')
                            logging.debug (f'fmtstr= {fmtstr:s}')
                        
                        ind = fmtstr.find ('d')
                        
                        if (ind != -1):
                            widthstr = fmtstr[0:ind]
                            width = int (widthstr)
#
# } end extract width from colfmt
#   
                    if self.debug:
                        logging.debug ('')
                        logging.debug (f'width= {width:d}')
                    
                    if (len(col_str) > width):
                        width = len(col_str)
                    if (len(self.coltype[col_str]) > width):
                        width = len(self.coltype[col_str])
                    if (len(self.colunits[col_str]) > width):
                        width = len(self.colunits[col_str])
                    fmtstr = str(width) + 'd'
                        
                    if self.debug:
                        logging.debug ('')
                        logging.debug (f'width= {width:d}')
                        logging.debug (f'fmtstr= {fmtstr:s}')
                        
                        
#
#} end coltype == 'int/short'
#
                elif (self.coltype[col_str] == 'long'): 
#
# { coltype == 'long': default 20
#
                    width = 20
                    if (self.colfmt[col_str] != 'null'):

                        fmtstr = self.colfmt[col_str]
                    
                        if self.debug:
                            logging.debug ('')
                            logging.debug ('extract width from fmtstr')
                            logging.debug (f'fmtstr= {fmtstr:s}')
			    
                        ind = fmtstr.find ('d')
                        
                        if (ind != -1):
                            widthstr = fmtstr[0:ind]
                            width = int (widthstr)

                    if (len(col_str) > width):
                        width = len(col_str)
                    if (len(self.coltype[col_str]) > width):
                        width = len(self.coltype[col_str])
                    if (len(self.colunits[col_str]) > width):
                        width = len(self.colunits[col_str])
                    fmtstr = str(width) + 'd'
                    
                    if self.debug:
                        logging.debug ('')
                        logging.debug (f'width= {width:d}')
                        logging.debug (f'fmtstr= {fmtstr:s}')
                        
#
#  } end coltype == 'long'
#
                elif ((self.coltype[col_str] == 'double') or \
                    (self.coltype[col_str] == 'float')):
#
#
#  { coltype == 'double/float'
#
                    width = 0
                    if (self.colfmt[col_str] != 'null'):

                        fmtstr = self.colfmt[col_str]
                    
                        if self.debug:
                            logging.debug ('')
                            logging.debug ('extract width from fmtstr')
                            logging.debug (f'fmtstr= {fmtstr:s}')
			    
                        ind = fmtstr.find ('.')
                        
                        if (ind != -1):
                            widthstr = fmtstr[0:ind]
                            width = int (widthstr)

                            if self.debug:
                                logging.debug ('')
                                logging.debug (f'widthstr= {widthstr:s}')
                                logging.debug (f'width= {width:d}')
  
                            remstr = fmtstr[ind+1:]

                            if self.debug:
                                logging.debug ('')
                                logging.debug (f'remstr= {remstr:s}')
                        else:
                            width = 20
                            remstr = '11e' 

                    else:
                        width = 20
                        remstr = '11e' 
                        
                    if (len(col_str) > width):
                        width = len(col_str)
                    if (len(self.coltype[col_str]) > width):
                        width = len(self.coltype[col_str])
                    if (len(self.colunits[col_str]) > width):
                        width = len(self.colunits[col_str])
                   
                    fmtstr = str(width) + '.' + remstr

                    if self.debug:
                        logging.debug ('')
                        logging.debug (f'width= {width:d}')
                        logging.debug (f'fmtstr= {fmtstr:s}')
                        
#
#} end coltype == 'double/float'
#

                self.colwidth[col_str] = width
                self.colfmt[col_str] = fmtstr 
                    
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'col_str= {col_str:s}')
                    logging.debug (f'colwidth= {self.colwidth[col_str]:d}')
                    logging.debug (f'colfmt= {self.colfmt[col_str]:s}')
                
                i = i + 1
#
#}  end for loop
#
            
            if len(rows) < cursor.arraysize:
                break

#
#} end while loop
#
        if self.debug:
            logging.debug ('')
            logging.debug ('done with dd retrieval')

        return


