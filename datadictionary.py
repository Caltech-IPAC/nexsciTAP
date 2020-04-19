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
    ddtable = ''

#
#    dd columns
#
    ncols = 0
    colname = {}

    colfmt   = {}
    coltype  = {}
    coldbtype = {}
    coldesc  = {}
    colunits = {}
    colwidth = {}

    nfetch = 1000
   
    def __init__(self, conn, table, **kwargs):

        """
        A dataDictionary specifies the following properites of each column in
	a database table; it is used for re-formating the output IPAC ASCII 
	table.

        name:          colnam in DB table,
	original_name: original colname,
        description:   column description,
	units:         unit,
	intype:        input data type (char, integer, double, date),
        format:        output format (20s, 20.10f, 10d, etc...),
	dbtype:        data type in DB table (varchar(20), integer, float, date,
	               timestamp, etc...),
	nulls:         whether null value if allowed,
        
       
	Required Input:

	    conn:              database connection handle,
	
	    table (char):      database dictionary table name,
        
         	
	Usage:

          dd = dataDictionary (conn, table)

        """

        if ('debug' in kwargs):
            self.debug = kwargs['debug']

        if self.debug:
            logging.debug ('')
            logging.debug (f'Enter dataDictionary.init')

        self.conn = conn
        self.ddtable = table

        if self.debug:
            logging.debug ('')
            logging.debug (f'ddtable= {self.ddtable:s}')
       

#
# construct and submit data dictionary query
#        
        cursor = self.conn.cursor()
        if self.debug:
            logging.debug ('')
            logging.debug ('cursor:')
            logging.debug (cursor)
        

        sql = 'select * from ' + self.ddtable + ' order by cntr'
        
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
#    Retrieve dd table to determine datatype, unit, colwidth, and format
#
#    col1: colname,
#    col3: description,
#    col4: unit,
#    col5: datatype,
#    col6: format (in the form of 11d, 12.6f, 25.25s etc..,
#    col7: db format (in the form of varchar, integer, float, date, timestamp, 
#          etc..,
#    col8: nulls,
#        
        cursor.arraysize = self.nfetch
        
        self.colfmt   = {}
        self.coltype  = {}
        self.coldbtype = {}
        self.coldesc  = {}
        self.colunits = {}
        self.colwidth = {}
        self.ncols = 0
        
        while True:
        
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
                
                if (row[3] is None):
                    self.coldesc[col_str] = ''
                else:
                    self.coldesc[col_str] = str(row[3]).strip()
                 
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'coldesc= {self.coldesc[col_str]:s}')
                
                
                if (row[4] is None):
                    self.colunits[col_str] = ''
                else:
                    self.colunits[col_str] = str(row[4]).strip()
                 

                if self.debug:
                    logging.debug ('')
                    logging.debug (f'colunits= {self.colunits[col_str]:s}')
                
                
                if (row[5] is None):
                    self.coltype[col_str] = ''
                else:
                    self.coltype[col_str] = str(row[5]).strip()

                if self.debug:
                    logging.debug ('')
                    logging.debug (f'coltype= {self.coltype[col_str]:s}')
                
                if (self.coltype[col_str] == 'integer'):
                    self.coltype[col_str] = 'int'
                
                if (row[6] is None):
                    self.colfmt[col_str] = ''
                else:
                    self.colfmt[col_str] = str(row[6]).strip().replace('i', 'd')
                 

                if self.debug:
                    logging.debug ('')
                    logging.debug (f'colfmt= {self.colfmt[col_str]:s}')
                
                
                if (row[7] is None):
                    self.coldbtyp[col_str] = ''
                else:
                    self.coldbtype[col_str] = str(row[7]).strip()
                 
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'coldbtype= {self.coldbtype[col_str]:s}')
                
                
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'col_str= {col_str:s}')
                    logging.debug (f'coldesc= {self.coldesc[col_str]:s}')
                    logging.debug (f'coltype= {self.coltype[col_str]:s}')
                    logging.debug (f'colunits= {self.colunits[col_str]:s}')
                    logging.debug (f'colfmt= {self.colfmt[col_str]:s}')
                    logging.debug (f'coldbtype= {self.coldbtype[col_str]:s}')


#
#    default width for various datatype 
#
                fmtstr = ''
                width = 0

                if (len(self.colfmt[col_str]) == 0):
		
                    if self.debug:
                        logging.debug ('')
                        logging.debug ('colfmt blank')
			    
                    if (self.coltype[col_str] == 'char'):

                        if (self.coldbtype[col_str] == 'timestamp'):
                            
                            if self.debug:
                                logging.debug ('')
                                logging.debug (\
				    'col not found: dbtype timestamp')
			    
                            fmtstr = '30s'
                            width = 30
                        
                        elif (self.coldbtype[col_str] == 'date'):
                            
                            if self.debug:
                                logging.debug ('')
                                logging.debug (\
				    'col not found: dbtype date')
			    
                            fmtstr = '30s'
                            width = 30 
                        
                        else:
                            if self.debug:
                                logging.debug ('')
                                logging.debug (\
				    'col not found: dbtype varchar')
			    
                            ind1 = self.coldbtype[col_str].find ('(')
                            ind2 = self.coldbtype[col_str].find (')')
                
                            fmtstr = self.coldbtype[col_str][ind1+1:ind2] + 's'
                            widthstr = self.coldbtype[col_str][ind1+1:ind2]
                            width = int(widthstr)

                    elif (self.coltype[col_str] == 'date'):
                            
                        if self.debug:
                            logging.debug ('')
                            logging.debug ('xxx date coltype')
			    
                        fmtstr = '30s'
                        width = 30 
                        
                    elif (self.coltype[col_str] == 'integer'):
                        fmtstr = '12d'
                        width = 12 

                    elif (self.coltype[col_str] == 'int'):
                        fmtstr = '12d'
                        width = 12 

                    elif (self.coltype[col_str] == 'long'):
                        fmtstr = '20d'
                        width = 20

                    elif (self.coltype[col_str] == 'double'):
                        fmtstr = '20.11f'
                        width = 20
                      
                    elif (self.coltype[col_str] == 'float'):
                        fmtstr = '20.11f'
                        width = 20
                      
                    if self.debug:
                        logging.debug ('')
                        logging.debug ('xxx1:')
                        logging.debug (f'fmtstr= {fmtstr:s}')
#
#    colfmt not blank
#
                else:
                    if self.debug:
                        logging.debug ('')
                        logging.debug ('colfmt NOT blank')
			    
                    if (self.coldbtype[col_str] == 'date'):

                        fmtstr = '30s'
                        width = 30

                    elif (self.coltype[col_str] == 'char'):

#
#    if char type, use dbtype format is more accurate
#
                        ind1 = self.coldbtype[col_str].find ('(')
                        ind2 = self.coldbtype[col_str].find (')')
                
                        fmtstr = self.coldbtype[col_str][ind1+1:ind2] + 's'
                        widthstr = self.coldbtype[col_str][ind1+1:ind2]
                        width = int(widthstr)

                        if self.debug:
                            logging.debug ('')
                            logging.debug ('char col, use dbtype to find width')
                            logging.debug (f'fmtstr= {fmtstr:s}')
                            logging.debug (f'widthstr= {widthstr:s}')
               
#
#    others: int and double
#
                    else:
                        fmtstr = self.colfmt[col_str]
                    
                        if self.debug:
                            logging.debug ('')
                            logging.debug (f'fmtstr= {fmtstr:s}')

                        wstr = itertools.takewhile(str.isdigit, fmtstr)
                        if self.debug:
                            logging.debug ('')
                            logging.debug ('wstr=')
                            logging.debug (wstr)
                        
                        width = (int)(''.join(wstr))
               
                        if self.debug:
                            logging.debug ('')
                            logging.debug (f'width= {width:d}')
                
                if (len(col_str) > width):
                    width = len(col_str)
        
                if (len(self.coltype[col_str]) > width):
                    width = len(self.coltype[col_str])
        
                if (len(self.colunits[col_str]) > width):
                    width = len(self.colunits[col_str])
        
                self.colwidth[col_str] = width
                self.colfmt[col_str] = fmtstr 
                    
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'col_str= {col_str:s}')
                    logging.debug (f'colwidth= {self.colwidth[col_str]:d}')
                    logging.debug (f'colfmt= {self.colfmt[col_str]:s}')
                
                i = i + 1
            
            if len(rows) < cursor.arraysize:
                break

        if self.debug:
            logging.debug ('')
            logging.debug ('done with dd retrieval')

        return


