#
#    writeResult class
#
import sys
import os
import io
import logging

import datetime
import time
import cx_Oracle

import writerecs
    
class writeResult:

    time00 = None
    time0 = None
    time1 = None
    
    delt = 0.0 
    deltsum_pack = 0.0 
    deltsum_cwrite = 0.0 
    delt_dd = 0.0 

    debug = 0 
    debug1 =0 
    debugtime = 0 

    cursor = None
    workdir = None
    dd = None

    ncols_dd = 0
    ind_racol = -1
    ind_deccol = -1
    ind_exclcol = -1
   
    racol = ''
    deccol = ''
    exclcol = ''

    format = 'votable'
    maxrec = -1

    outpath = ''
    ntot = 0
    ncol = 0
    overflow = 0
    ishdr = 0
    istail = 0

    prepare_time = 0
    write_time = 0

    msg = ''
    status = ''

    def __init__ (self, cursor, workdir, dd, **kwargs):
#
#{
#
        """
        cursor:       oracle query returned cursor containing data stream,
        workdir:      work directory for output files,
        dd:           data dictionary for the output columns,
        coldes (0/1): indictes whether to include column descriptions in the
                  output file header

        kwargs inputs:

            format (char),
            maxrec (int),
            racol,
            deccol,
            exclcol (int): exclude column index

        Usage:

            wresult = writeResult (cursor, \
                workdir, \
                dd, \
                format=format, \
                maxrec=maxrec, \
                coldesc=coldesc, \
                racol=racol, \
                deccol=sdeccol)
        """

        if ('debug' in kwargs):
            self.debug = kwargs['debug']
        
        if ('debugtime' in kwargs):
            self.debugtime = kwargs['debugtime']

        if self.debugtime:
            self.time00 = datetime.datetime.now()
            logging.debug ('')
            logging.debug ('Enter writeResult')
        
        if self.debug:
            logging.debug ('')
            logging.debug ('Enter writeResult')

        self.cursor = cursor    
        self.workdir = workdir
        self.dd = dd

        self.ncols_dd = len(self.dd.colname)
        if self.debug:
            logging.debug ('')
            logging.debug (f'ncols_dd= {self.ncols_dd:d}')

        if ('racol' in kwargs):
            self.racol = kwargs['racol']
            self.ind_racol = self.__getDDIndex__ (self.dd, self.racol)
    
        if ('deccol' in kwargs):
            self.deccol = kwargs['deccol']
            self.ind_deccol = self.__getDDIndex__ (self.dd, self.deccol)
  
        if ('exclcol' in kwargs):
            self.ind_exclcol = kwargs['exclcol']

        if ('format' in kwargs):
            self.format = kwargs['format']

        if ('maxrec' in kwargs):
            self.maxrec = kwargs['maxrec']
   
        if self.debug:
            logging.debug ('')
            logging.debug (f'format= {self.format:s}')
            logging.debug (f'maxrec= {self.maxrec:d}')

        self.coldesc = 0 
        if ('coldesc' in kwargs):
            self.coldesc = kwargs['coldesc']
    
        if self.debug:
            logging.debug ('')
            logging.debug (f'racol= {self.racol:s}')
            logging.debug (f'deccol= {self.deccol:s}')
            logging.debug (f'ind_racol= {self.ind_racol:d}')
            logging.debug (f'ind_deccol= {self.ind_deccol:d}')
            logging.debug (f'coldesc= {self.coldesc:d}')
            logging.debug (f'ind_exclcol= {self.ind_exclcol:d}')

        self.status = ''
#        
# open querypath for output
#    
        resulttbl = ''
        if (self.format == 'votable'):
            resulttbl = 'result.xml'
        elif (self.format == 'ipac'):
            resulttbl = 'result.tbl'
        elif (self.format == 'csv'):
            resulttbl = 'result.csv'
        elif (self.format == 'tsv'):
            resulttbl = 'result.tsv'

        self.outpath = self.workdir + '/' + resulttbl
    
        if self.debug:
            logging.debug ('')
            logging.debug (f'outpath= {self.outpath:s}')

#  
#    cursor description contains a list of tuples, each tuple is a 7-item 
#    sequences describing each output column:
#
#    colname, type, displya_size, internal_size, precision, scale, null_ok
#
#    We use it to determine the columns' datatype and width for the columns 
#    that  are not in out data dictionary
#
        self.ncol = len (self.cursor.description)
        
        if self.debug:
            logging.debug ('')
            logging.debug (f'ncol= {self.ncol:d}')
            logging.debug ('cursor.description:')
            logging.debug (self.cursor.description)
   
#
#    C interface list:
#    
#    [namearr, typearr, dbtypearr, fmtarr, unitsarr, widtharr, descarr]
#
        ddlist = []

        namearr = []
        typearr = []
        dbtypearr = []
        fmtarr = [] 
        unitsarr = []
        widtharr = []
        descarr = []

#
#    for non-dd column and dbtype == NUMBER, check data to see if 
#    it is int or float: if any number in the fltcntarr, consider it float
#
        isddcolarr = [] 
        intcntarr = []
        fltcntarr = []


        dbdatatype = None
        precision = 0
        scale = None 
        size = 0

        nfetch = 10000
        self.cursor.arraysize = nfetch

        if self.debugtime:
            self.time0 = datetime.datetime.now()
        
        i = 0
        for col in self.cursor.description:
#
#{ for loop for cursor description: 
# cursor description is a list with ncols tuples: each tuple contains 
#  each output colname's name, datatype, dbdatatype, size, etc...
#
            if self.debug:
                logging.debug ('')
                logging.debug (f'ind_exclcol= {self.ind_exclcol:d}')
                logging.debug (f'i= {i:d} col=')
                logging.debug (col)
           
#
#    extract colname, dbdatatype, size, precision, scale from col array
#
            colname = str(col[0])

            if self.debug:
                logging.debug ('')
                logging.debug (f'colname= {colname:s}')

            if (i == self.ind_exclcol): 
                
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'i=ind_exclcol: skipped')

                continue

#
#    check if colname is in DD,
#
#    colname in dd: get dbtype, width from DD
#
#    colname NOT in dd: get dbtype, width from col[1], col[2], col[4], 
#        and col[5] 
#
            dbtype = '' 
            coltype = '' 
            units = '' 
            desc = '' 
            fmt = ''
            width = 0
        
            ind = self.__getDDIndex__ (self.dd, colname)
        
            if (ind != -1):
#
#{ col in dd
#
                if self.debug1:
                    logging.debug ('')
                    logging.debug (f'column in dd')
            
                width = self.dd.colwidth[colname]
                if self.debug1:
                    logging.debug ('')
                    logging.debug (f'width= {width:d}')
            
                dbtype = self.dd.coldbtype[colname] 
                if self.debug1:
                    logging.debug ('')
                    logging.debug (f'dbtype= {dbtype:s}')
            
                coltype = self.dd.coltype[colname] 
                if self.debug1:
                    logging.debug ('')
                    logging.debug (f'coltype= {coltype:s}')
            
                units = self.dd.colunits[colname] 
                if self.debug1:
                    logging.debug ('')
                    logging.debug (f'units= {units:s}')
            
                fmt  = self.dd.colfmt[colname]
                if self.debug1:
                    logging.debug ('')
                    logging.debug (f'fmt= {fmt:s}')

                desc = self.dd.coldesc[colname] 
                if self.debug1:
                    logging.debug ('')
                    logging.debug (f'desc= {desc:s}')
            
                isddcolarr.append(1)
                intcntarr.append(0)
                fltcntarr.append(0)


#
#} end col in dd
#

            else:
#
#{ col Not in dd
#

                if self.debug:
                    logging.debug ('')
                    logging.debug (f'column not in dd')

#
#    extract dbdatatype, display_size, precision, scale from col_array:
#    col[1], col[2], col[4], and col[5] 
#
                dbdatatypestr = str(col[1])

                ind = dbdatatypestr.find ("cx_Oracle.")
                if (ind != -1):
                    dbdatatypestr = dbdatatypestr[ind+10:]

                if self.debug1:
                    logging.debug ('')
                    logging.debug (f'dbdatatypestr= {dbdatatypestr:s}')

                ind = dbdatatypestr.find ("'")
                if (ind != -1):
                    dbdatatype = dbdatatypestr[:ind]
             
                if self.debug1:
                    logging.debug ('')
                    logging.debug (f'colname= {colname:s}')
                    logging.debug (f'dbdatatype= {dbdatatype:s}')

                size = None
                if (col[2] is not None):
                    
                    size = col[2]
                    if (len(colname) > size):
                        size = len(colname)
                
                    if self.debug1:
                        logging.debug ('')
                        logging.debug (f'size= {size:d}')


                precision = None
                if (col[4] is not None):
                    precision = col[4]
                
                    if self.debug1:
                        logging.debug ('')
                        logging.debug (f'precision= {precision:d}')

                scale = None
                if (col[5] is not None):
                    scale = col[5]
	
                    if self.debug1:
                        logging.debug ('')
                    logging.debug (f'scale= {scale:d}')

                
                if (dbdatatype == 'STRING'):
#
#{ dbdatatype == string
#
                    width = 0 
                    if (size is not None):
                        width = size
                    
                    if (len(colname) > width):
                        width = len(colname)

                    coltype = 'char'
                    dbtype = "varchar(" + str(width) + ")"
                    fmt = str(width) + 's'
                  		
                    if self.debug1:
                        logging.debug ('')
                        logging.debug (f'width= {width:d}')
                        logging.debug (f'dbtype= {dbtype:s}')
                        logging.debug (f'coltype= {coltype:s}')
                        logging.debug (f'fmt= {fmt:s}')
                
#
#} end dbdatatype == STRING
#
                elif (dbdatatype == 'LONG_BINARY'):
#
#{ dbdatatype == LONG_BINARY 
#
                    if self.debug1:
                        logging.debug ('')
                        logging.debug (f'here1-1-1-1: long int')
            
                    coltype = 'long'
                    dbtype = 'long_binary'
                    fmt = 'd22'
                    width = 22 
                
                    if self.debug1:
                        logging.debug ('')
                        logging.debug (f'here1-1: NATIVE_FLOAT column')
                        logging.debug (f'width= {width:d}')
                        logging.debug (f'dbtype= {dbtype:s}')
                        logging.debug (f'coltype= {coltype:s}')
                        logging.debug (f'fmt= {fmt[colname]:s}')
            
#
#} end dbdatatype == LONG_BINARY 
#

                elif (dbdatatype == 'NATIVE_FLOAT'):

#
#{ dbdatatype == NATIVE_FLOAT 
#
                    if self.debug1:
                        logging.debug ('')
                        logging.debug (f'here1-1-1-1: NUMBER--double')
            
                    coltype = 'double'
                    dbtype = 'float'
                    fmt = '22.14e'
                    width = 22 
                
                    if self.debug1:
                        logging.debug ('')
                        logging.debug (f'here1-1: NATIVE_FLOAT column')
                        logging.debug (f'width= {width:d}')
                        logging.debug (f'dbtype= {dbtype:s}')
                        logging.debug (f'coltype= {coltype:s}')
                        logging.debug (f'fmt= {fmt[colname]:s}')
            
#
#} end dbdatatype == NATIVE_FLOAT 
#
                elif (dbdatatype == 'NUMBER'):
               
#
#{ dbdatatype == NUMBER 
#
                    if self.debug1:
                        logging.debug ('')
                        logging.debug (f'here1-1: NUMBER column')
                        logging.debug (f'scale=')
                        logging.debug (scale)
            
            
                    if (scale == 0):
                
                        if self.debug1:
                            logging.debug ('')
                            logging.debug (f'here1-1-0: NUMBER--int')
            
                        coltype = 'int'
                        dbtype = 'NUMBER'
                        fmt = '22d'
                        width = 22 

#                    elif (scale == -127):
                    else:
                    
                        if self.debug1:
                            logging.debug ('')
                            logging.debug (f'here1-1-1-1: NUMBER--double')
            
                        coltype = 'double'
                        dbtype = 'NUMBER'
                        fmt = '22.14e'
                        width = 22 
                
                    if self.debug1:
                        logging.debug ('')
                        logging.debug (f'here1-1: NUMBER column')
                        logging.debug (f'width= {width:d}')
                        logging.debug (f'dbtype= {dbtype:s}')
                        logging.debug (f'coltype= {coltype:s}')
                        logging.debug (f'fmt= {fmt:s}')
            
#
#} end dbdatatype == NUMBER 
#

                else: 
#
#{ dbdatatype == DATE, TIMESTAMP 
#
#    timestamp etc. are not included in the above but most of them should
#    come out as STRING
#
                    coltype = 'char'
                    dbtype = 'varchar(30)'
                    fmt = '30s'
                    width = 30 
	        
                    if self.debug1:
                        logging.debug ('')
                        logging.debug (f'here1-2: other column')
                        logging.debug (f'width= {width:d}')
                        logging.debug (f'dbtype= {dbtype:s}')
                        logging.debug (f'coltype= {coltype:s}')
                        logging.debug (f'fmt= {fmt:s}')
           
#
#} end dbdatatype == DATE, TIMESTAMP 
#

#
#  special cases: if colname == 'RA' or 'DEC' use racol, decol in dd 
#
                if (colname.lower() == 'ra'):  
   
                    if self.debug1:
                        logging.debug ('')
                        logging.debug (
                            f'special case (ra): colname= {colname:s}')
                        logging.debug (f'racol= {self.racol:s}')
                        logging.debug (f'dbdatatype= {dbdatatype:s}')
                
                
                    if ((dbdatatype == 'NUMBER') and (self.ind_racol != -1)):
	    
                        width = dd.colwidth[self.racol]
                        dbtype = dd.coldbtype[self.racol] 
                        coltype = dd.coltype[self.racol] 
                        units = dd.colunits[self.racol] 
                        fmt = dd.colfmt[self.racol] 
	    
                elif (colname.lower() == 'dec'):  
   
                    if self.debug1:
                        logging.debug ('')
                        logging.debug (
                            f'special case (dec) : colname= {colname:s}')
                        logging.debug (f'deccol= {self.deccol:s}')
                
                    if ((dbdatatype == 'NUMBER') and (self.ind_deccol != -1)):
	    
                        width = dd.colwidth[self.deccol]
                        dbtype = dd.coldbtype[self.deccol] 
                        coltype = dd.coltype[self.deccol] 
                        units = dd.colunits[self.deccol] 
                        fmt = dd.colfmt[self.deccol] 
	    
                if self.debug1:
                    logging.debug ('')
                    logging.debug (f'width= {width:d}')
                    logging.debug (f'dbtype= {dbtype:s}')
                    logging.debug (f'coltype= {coltype:s}')
#                    logging.debug (f'units= {units:s}')
                    logging.debug (f'fmt= {fmt:s}')

                
                isddcolarr.append(0)
                intcntarr.append(0)
                fltcntarr.append(0)


#
#} end col Not in dd
#

            if self.debug1:
                logging.debug ('')
                logging.debug (f'final width= {width:d}')
                logging.debug (f'dbtype= {dbtype:s}')
                logging.debug (f'coltype= {coltype:s}')
                logging.debug (f'fmt= {fmt:s}')
                logging.debug (f'desc= {desc:s}')

            """
            namearr = namearr + (colname,)
            typearr = typearr + (coltype,)
            dbtypearr = dbtypearr + (dbtype,)
            fmtarr = fmtarr + (fmt,)
            widtharr = widtharr + (width,)
            unitsarr = unitsarr + (units,)
            descarr = descarr + (desc,)
            """
            
            namearr.append (colname)
            typearr.append (coltype)
            dbtypearr.append (dbtype)
            fmtarr.append (fmt)
            widtharr.append (width)
            unitsarr.append (units)
            descarr.append (desc)

            i = i + 1
#
#
#} end of for loop for analysing dd's cursor description
#  at this point the namearr, typearr, dbtypearr, widtharr of output columns
#  are assigned: add them to the ddlist for sending to C routine
#  (We don't send the whold dd to C routine to avoid excess data transfer) 
#
        
        len_arr = len (namearr)
    
        if self.debug:
            logging.debug ('')
            logging.debug (f'len_arr= {len_arr:d}')

  
        if self.debug:
            
            for i in range (0, len_arr):
                logging.debug ('')
                logging.debug (f'name[{i:d}]= [{namearr[i]:s}]')
                logging.debug (f'type[{i:d}]= [{typearr[i]:s}]')
                logging.debug (f'dbtype[{i:d}]= [{dbtypearr[i]:s}]')
                logging.debug (f'fmt[{i:d}]= [{fmtarr[i]:s}]')
                logging.debug (f'width[{i:d}]= [{widtharr[i]:d}]')
                logging.debug (f'units[{i:d}]= [{unitsarr[i]:s}]')
                logging.debug (f'desc[{i:d}]= [{descarr[i]:s}]')

        ddlist.append (namearr)
        ddlist.append (typearr)
        ddlist.append (dbtypearr)
        ddlist.append (fmtarr)
        ddlist.append (unitsarr)
        ddlist.append (descarr)
        ddlist.append (widtharr)

        if self.debug:
            logging.debug ('')
            logging.debug (f'ddlist= ')
            logging.debug (ddlist)

        if self.debugtime:
            self.time1 = datetime.datetime.now()
            self.delt = (self.time1 - self.time0).total_seconds()
            logging.debug ('')
            logging.debug (f'time (prepare outcolumn ddlist): {self.delt:f}')

#
#    if maxrec == 0: write header and exit
#
        if (self.maxrec == 0):
#
#{ maxrec == 0
#
            self.ishdr = 1
            self.overflow = 1 
            self.istail = 1 
            
            self.status = None 
            rowslist = []
            try:
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'call writerecs: maxrec=0')

                istatus = writerecs.writerecs (self.outpath, self.format, \
                    ddlist, rowslist, self.ishdr, self.coldesc, \
                    self.overflow, self.istail)
       
                if (istatus == 0):
                    self.status = 'ok'

                if self.debug:
                    logging.debug ('')
                    logging.debug (f'returned writerecs: istatus= {istatus:d}')
                    logging.debug (f'returned writerecs: {self.status:s}')

            except Exception as e:
        
                self.status = 'error'
                self.msg = str(e)

                if self.debug:
                    logging.debug ('')
                    logging.debug (f'writerecs exception: {str(e):s}')

                raise Exception (str(e)) 

            return

#
#} end maxrec == 0
#

        if self.debug:
            logging.debug ('')
            logging.debug (f'got here: start constructing rowslist')


#
#    start fetching data lines
#        
#        nfetch = 50 
        nfetch = 10000
        self.cursor.arraysize = nfetch
        
        ibatch = 0

        nullstr = ''
        self.overflow = 0
        irow = 0
        self.ntot = 0
        while True:
#
#{ start of while loop for fechting data lines; max 10000 lines at a time
#        
            if self.debugtime:
                self.time0 = datetime.datetime.now()
       
            rows = cursor.fetchmany()
        
            if self.debug:
                logging.debug ('')
                logging.debug (f'ibatch= {ibatch:d}')
                logging.debug (f'rows=')
                logging.debug (rows)
         
            nrec = len(rows)
        
            if self.debug:
                logging.debug ('')
                logging.debug (f'nrec= {nrec:d}')

            rowslist = []

            for l in range (0, nrec):
#
#{  beginning l loop: one row 
#
                row = rows[l]

                rowlist = []
                for i in range (0, len(row)):
#
#{  beginning i loop: one col
#
                    if self.debug:
                        logging.debug ('')
                        logging.debug (f'i= {i:d}')
                        logging.debug (f'isddcolarr= {isddcolarr[i]:d}')
                        logging.debug (f'dbtypearr= {dbtypearr[i]:s}')

                    if (i == self.ind_exclcol):
                    
                        if self.debug: 
                            logging.debug ('') 
                            logging.debug (f'icol= {i:d} skipped')
                        continue


                    if (row[i] is None):
                    
                        if self.debug:
                            logging.debug ('')
                            logging.debug (f'row[i] is None')

                    
                    if ((isddcolarr[i] == 0) and \
                        (dbtypearr[i] == 'NUMBER') and \
                        (ibatch == 0)):
                    
                        if self.debug:
                            logging.debug ('')
                            logging.debug (f'row[i]:')
                            logging.debug (row[i])
                            logging.debug (type(row[i]))
                            logging.debug (type(row[i]).__name__)

                        dtype = type (row[i]).__name__

                        if self.debug:
                            logging.debug ('')
                            logging.debug (f'dtype= {dtype:s}')
                      
                        if (dtype == 'int'):
                            intcntarr[i] = intcntarr[i] + 1
                        else:
                            fltcntarr[i] = fltcntarr[i] + 1


                    if ((dbtypearr[i] == 'date') or \
                        (dbtypearr[i] == 'timestamp')):

                        if self.debug:
                            logging.debug ('')
                            logging.debug (
                                f'i= {i:d} dbtypearr= {dbtypearr[i]:s}')
                            logging.debug (f'row[{i:d}]= {str(row[i]):s}')
                            logging.debug (f'typearr= {typearr[i]:s}')

                        rowlist.append (str(row[i])) 
                        
                    else:
                        rowlist.append (row[i]) 

#
#}  end of i loop
#
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'rowlist:')
                    logging.debug (rowlist)

                rowslist.append (rowlist)
                
                irow = irow + 1
            
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'maxrec= {self.maxrec:d}')
                    logging.debug (f'irow= {irow:d}')
                
                if ((self.maxrec > 0) and (irow >= self.maxrec)):
                    self.overflow = 1
                    break
#
#}    end of l loop
#
            if (ibatch == 0):
#
#{ if ibatch == 0
#
                        
                for i in range (0, len(isddcolarr)):
#
#{ check intcntarr and fltcntarr 
#
                    if self.debug:
                        logging.debug ('')
                        logging.debug (f'check intcntarr: i= {i:d}')
                        logging.debug (f'isddcolarr= {isddcolarr[i]:d}')
                        logging.debug (f'check namearr= {namearr[i]:s}')
                        logging.debug (f'typearr= {typearr[i]:s}')
                        logging.debug (f'dbtypearr= {dbtypearr[i]:s}')
                        logging.debug (f'fmtarr= {fmtarr[i]:s}')
                        logging.debug (f'widtharr= {widtharr[i]:d}')
                        
                    
                    if ((isddcolarr[i] == 0) and \
                        (dbtypearr[i] == 'NUMBER')):
            
                        if self.debug:
                            logging.debug ('')
                            logging.debug (f'intcntarr= {intcntarr[i]:d}')
                            logging.debug (f'fltcntarr= {fltcntarr[i]:d}')

                        if ((intcntarr[i] > 0) and (fltcntarr[i] == 0)):
                
                            typearr[i] = 'int'
                            dbtypearr[i] = 'integer'
                            fmtarr[i] = '22d'
                            widtharr[i] = 22 
                
                        else:
                            typearr[i] = 'double'
                            dbtypearr[i] = 'float'
                            fmtarr[i] = '22.14e'
                            widtharr[i] = 22 


                    if self.debug:
                        logging.debug ('')
                        logging.debug (f'typearr= {typearr[i]:s}')
                        logging.debug (f'dbtypearr= {dbtypearr[i]:s}')
                        logging.debug (f'fmtarr= {fmtarr[i]:s}')
                        logging.debug (f'widtharr= {widtharr[i]:d}')
#
#} end checking intcntarr and fltcntarr 
#

#
#} end if ibatch == 0
#
                        
            if self.debug:
                logging.debug ('')
                logging.debug (f'rowslist= ')
                logging.debug (rowslist)

            if (self.ntot == 0):
                self.ishdr = 1
            else:
                self.ishdr = 0
        
            self.ntot = self.ntot + nrec
       
            if ((self.overflow==1) and (irow >= self.maxrec)):
                self.istail = 1

            if (len(rows) < self.cursor.arraysize):
                self.istail = 1

            if self.debugtime:
                self.time1 = datetime.datetime.now()
                self.delt = (self.time1 - self.time0).total_seconds()
                self.deltsum_pack = self.deltsum_pack + self.delt
                
#                logging.debug ('')
#                logging.debug (f'time (pack): {self.delt:f}')

            if self.debugtime:
                self.time0 = datetime.datetime.now()
            
            self.status = None 
            try:
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'call writerecs= {nrec:d}')
                    logging.debug (f'ntot= {self.ntot:d} ishdr= {self.ishdr:d}')
                    logging.debug (f'istail= {self.istail:d}')
                    logging.debug (f'overflow= {self.overflow:d}')

                istatus = writerecs.writerecs (self.outpath, self.format, \
                    ddlist, rowslist, self.ishdr, self.coldesc, self.overflow, \
                    self.istail)
       
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'returned writerecs')
                
                if (istatus == 0):
                    self.status = 'ok'

            except Exception as e:
        
                self.status = 'error'
                self.msg = str(e)

                if self.debug:
                    logging.debug ('')
                    logging.debug (f'writerecs exception: {str(e):s}')

                raise Exception (str(e)) 
        
            if self.debugtime:
                self.time1 = datetime.datetime.now()
                self.delt = (self.time1 - self.time0).total_seconds()
                self.deltsum_cwrite = self.deltsum_cwrite + self.delt
                
#                logging.debug ('')
#                logging.debug (f'time (cwrite): {self.delt:f}')

        
            if ((self.overflow==1) and (irow >= self.maxrec)):
            
                if self.debug:
                    logging.debug ('')
                    logging.debug (f'irow= {irow:d} maxrec= {self.maxrec}')
                break

#            if self.debug:
#                logging.debug ('')
#                logging.debug (f'ntot= {self.ntot:d}')
                   
            if (len(rows) < self.cursor.arraysize):
                break

            ibatch = ibatch + 1

#
#} end while loop for fetching data lines
#
        if self.debugtime:
            self.time1 = datetime.datetime.now()
            self.delt = (self.time1 - self.time00).total_seconds()
            logging.debug ('')
            logging.debug (f'time (total): {self.delt:f}')
            logging.debug (f'time (prepare lists): {self.deltsum_pack:f}')
            logging.debug (f'time (c_routine): {self.deltsum_cwrite:f}')

        if self.debug:
            logging.debug ('')
            logging.debug ('Done writeResultfile')
            logging.debug (f'ntot= {self.ntot:d}')
        
        return

#
#} end of init def 
#

    def __getArrIndex__ (self, arr, name):
#
#{
#
        debug = 0 

        if debug:
            logging.debug ('')
            logging.debug (f'Enter getArrIndex: name= [{name:s}]')
            logging.debug (f'len(arr)= {len(arr):d}')

        ind = -1    
        for i in range (len(arr)):
       
            if debug:
                logging.debug ('')
                logging.debug (f'i= {i:d} arr= [{arr[i]:s}]')

            if (name.lower() == arr[i]):
                ind = i
                break

        if debug:
            logging.debug ('')
            logging.debug (f'done: ind= {ind:d}')

        return (ind)

#
#} end of getArrIndex def 
#

    def __getDDIndex__ (self, dd, name):
#
#{
#
        debug = 0 

        if debug:
            logging.debug ('')
            logging.debug (f'Enter getDDIndex: name= [{name:s}]')

        ind = -1    
        for i in range (len(dd.colname)):
       
            if debug:
                logging.debug ('')
                logging.debug (f'i= {i:d} colname= [{dd.colname[i]:s}]')

            if (name == dd.colname[i]):
                ind = i
                break

        if debug:
            logging.debug ('')
            logging.debug (f'done: ind= {ind:d}')

        return (ind)

