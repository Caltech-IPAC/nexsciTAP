# Copyright (c) 2020, Caltech IPAC.
# This code is released with a BSD 3-clause license. License information is at
#   https://github.com/Caltech-IPAC/nexsciTAP/blob/master/LICENSE

#
# writeResult class write query result to an output file:
#
# 1. This routine extracts the dbdatatypes from the cursor description, using
#    them to determine the datatypes of the output columns.
#
# 2. In order to increase the speed of I/O, the real output of data is done
#    in a C routine.
#
import logging

import datetime
import decimal

from TAP import writerecs

class writeResult:

    time00 = None
    time0 = None
    time1 = None

    delt = 0.0
    deltsum_pack = 0.0
    deltsum_cwrite = 0.0
    delt_dd = 0.0

    debug = 0

    cursor = None
    workdir = None
    dd = None
    dbms = None 

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


    def __init__(self, cursor, workdir, dd, **kwargs):

    #
    # { writeResult.init
    #

        """
        positional inputs (required):

        cursor:       DBMS query (oracle, pgsql, mysql, sqlite are implemented)
                      returned cursor containing data stream,
        workdir:      work directory for output file,
        dd:           data dictionary for the output columns (maybe None),

        kwargs inputs (required):

            dbms (char):    DBMS (oracle, pgsql, mysql, or sqlite)
            racol (double): ra column in deg; this column is specified in
                            the congif file TAP.ini,  
            deccol(double): dec column in deg; this column is specified in
                            the congif file TAP.ini
        
        kwargs inputs (optional):

            format (char),
            coldes (0/1):   flag indicting whether to include column 
                            descriptions in the output file header,
            maxrec (int):   max records to ouput,
            exclcol (int):  exclude column index (this only happens in a join
                            operation that contains redundant columns)

        Usage example:

            wresult = writeResult(cursor, \
                workdir, \
                dd, \
                dbms=dbms, \
                racol=racol, \
                deccol=sdeccol, \
                format=format, \
                maxrec=maxrec, \
                coldesc=coldesc)
        """

        if ('debug' in kwargs):
            self.debug = kwargs['debug']

        if self.debug:
            logging.debug('')
            logging.debug('Enter writeResult:')
            logging.debug(f'self.debug= {self.debug:d}')
        
        
        self.cursor = cursor
        self.workdir = workdir
        self.dd = dd

        curr_rowcnt = self.cursor.rowcount
        if self.debug:
            logging.debug('')
            logging.debug(f'curr_rowcnt= {curr_rowcnt:d}')

        self.ncols_dd = 0
        if (self.dd != None):
            self.ncols_dd = len(self.dd.colname)

        if self.debug:
            logging.debug('')
            logging.debug(f'ncols_dd= {self.ncols_dd:d}')
        
        if ('dbms' in kwargs):
            self.dbms = kwargs['dbms']
        
        if self.debug:
            logging.debug('')
            logging.debug(f'dbms= {self.dbms:s}')
        
        if ('racol' in kwargs):
            self.racol = kwargs['racol']

            if (self.dd != None):
                self.ind_racol = self.__getDDIndex__(self.dd, self.racol)

        if ('deccol' in kwargs):
            self.deccol = kwargs['deccol']
            
            if (self.dd != None):
                self.ind_deccol = self.__getDDIndex__(self.dd, self.deccol)

        if ('exclcol' in kwargs):
            self.ind_exclcol = kwargs['exclcol']

        
        if('format' in kwargs):
            self.format = kwargs['format']

        if('maxrec' in kwargs):
            self.maxrec = kwargs['maxrec']

        self.coldesc = 0
        if('coldesc' in kwargs):
            self.coldesc = kwargs['coldesc']

        self.arraysize = 10000
        if('arraysize' in kwargs):
            self.arraysize = kwargs['arraysize']

        if self.debug:
            logging.debug('')
            logging.debug('from kwargs:')
            logging.debug(f'      dbms       = {self.dbms:s}')
            logging.debug(f'      racol       = {self.racol:s}')
            logging.debug(f'      deccol      = {self.deccol:s}')
            logging.debug(f'      ind_racol   = {self.ind_racol:d}')
            logging.debug(f'      ind_deccol  = {self.ind_deccol:d}')
            logging.debug(f'      coldesc     = {self.coldesc:d}')
            logging.debug(f'      ind_exclcol = {self.ind_exclcol:d}')
            logging.debug(f'      maxrec      = {self.maxrec:d}')
            logging.debug(f'      arraysize   = {self.arraysize:d}')

        self.status = ''
       
        #
        # open querypath for output
        #

        resulttbl = ''
        if(self.format == 'votable'):
            resulttbl = 'result.xml'
        elif(self.format == 'ipac'):
            resulttbl = 'result.tbl'
        elif(self.format == 'csv'):
            resulttbl = 'result.csv'
        elif(self.format == 'tsv'):
            resulttbl = 'result.tsv'
        elif(self.format == 'json'):
            resulttbl = 'result.json'

        self.outpath = self.workdir + '/' + resulttbl

        if self.debug:
            logging.debug('')
            logging.debug(f'outpath= {self.outpath:s}')

        #
        # Cursor description contains a list of tuples, each tuple is a 7-item
        # sequences describing each output column:
        #
        # colname, type, display_size, internal_size, precision, scale, null_ok
        #
        # We use it to determine the columns' datatype and width for the
        # columns that are not in out data dictionary
        #

        self.ncol = len (self.cursor.description)

        if self.debug:
            logging.debug('')
            logging.debug(f'ncol = {self.ncol:d}\n')
            logging.debug('cursor.description:')
            logging.debug('------------------------------------------------')
            logging.debug(self.cursor.description)
            logging.debug('------------------------------------------------')

        for desc in self.cursor.description:
            
            name = desc[0]
            coltype = desc[1]
            dispsize = desc[2]
            internalsz = desc[3]
            precision = desc[4]
            scale = desc[5] 
            nullok = desc[6]
            
            """
            if self.debug:
                logging.debug('')
                logging.debug(f'name = {name:s}')
                logging.debug(f'coltype:')
                logging.debug(coltype)
                logging.debug(f'dispsize:')
                logging.debug(dispsize)
                logging.debug(f'internalsz:')
                logging.debug(internalsz)
                logging.debug(f'precision:')
                logging.debug(precision)
                logging.debug(f'scale:')
                logging.debug(scale)
                logging.debug(f'nullok:')
                logging.debug(nullok)
            """

        #
        # C interface list:
        #
        # [namearr, typearr, dbtypearr, fmtarr, unitsarr, widtharr, descarr]
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
        # -- column exist in dd, use dd's data type as typearr for output;
        #
        # -- column not in dd: examine the dbtype in cursor description
        #    to determine ouput data type;
        #
        #    varchr, sting -- char,
        #    int, integer, long, etc.. -- int (32bit),
        #
        #    oracle: dbtype == NUMBER, check first batch of data to see if 
        #        it is int or float: if any number in this batch is float, 
        #        consider the column dtype as double 
        #
        #    pgsql: dbtype == decimal, assume it is double
        #

        isddcolarr = []
        intcntarr = []
        fltcntarr = []

        dbdatatype = None
        precision = None 
        scale = None
        size = None 

        nfetch = self.arraysize
        self.cursor.arraysize = nfetch

        if self.debug:
            logging.debug('')
            logging.debug(f'cursor.description:')
            logging.debug(self.cursor.description)
        
        i = 0
        for col in self.cursor.description:
        #
        # { cursor description for loop:
        # 
        #   cursor description is a list with ncols tuples:
        #   each tuple contains each output colname's name, datatype,
        #   dbdatatype, size, precision, scale but most DBMSs' 
        #   implementation are incomplete, only name and dbdatatype 
        #   are reliably implemented.
        #

            if self.debug:
                logging.debug('')
                logging.debug('----------------------------------------------')
                logging.debug(f'ind_exclcol= {self.ind_exclcol:d}\n')
                logging.debug(f'i = {i:d} col = ' + str(col))

            #
            # Extract colname, dbdatatype, size, precision, scale from
            # a columns's cursor description.
            #

            colname = str(col[0]).lower()

            if self.debug:
                logging.debug('')
                logging.debug(f'colname(lower) = {colname:s}')

            if(i == self.ind_exclcol):

                if self.debug:
                    logging.debug('')
                    logging.debug('i=ind_exclcol: skipped')

                continue

            #
            # { Extract dbdatatype, display_size, precision, scale from
            # col_array: col[1], col[2], col[4], and col[5]
            #

            if self.debug:
                logging.debug('')
                logging.debug(f'analyze description array:')
                logging.debug(f'dbms= {self.dbms:s}')

            if (self.dbms.lower() == 'oracle'):
            #
            # { oracle datatype from descriptor
            #    
                if self.debug:
                    logging.debug('')
                    logging.debug('oracle:')
                    logging.debug('col[1]:')
                    logging.debug(col[1])
                    logging.debug(f'str(col[1]= {str(col[1]):s}')

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

                ind = dbdatatypestr.find("DATE")
                if(ind != -1):
                    dbdatatype = 'DATE'

                ind = dbdatatypestr.find("DATETIME")
                if(ind != -1):
                    dbdatatype = 'DATETIME'

                ind = dbdatatypestr.find("TIMESTAMP")
                if(ind != -1):
                    dbdatatype = 'TIMESTAMP'

                ind = dbdatatypestr.find("NUMBER")
                if(ind != -1):
                    dbdatatype = 'NUMBER'

                ind = dbdatatypestr.find("LONG")
                if(ind != -1):
                    dbdatatype = 'LONG'

                ind = dbdatatypestr.find("FLOAT")
                if(ind != -1):
                    dbdatatype = 'FLOAT'
                
                if self.debug:
                    logging.debug('')
                    logging.debug(f'dbdatatype = {dbdatatype:s}')
            #
            # } end oracle datatype
            #
            
            elif (self.dbms.lower() == 'pgsql'):
            #
            # { pgsql datatype from descriptor
            #    
                if self.debug:
                    logging.debug('')
                    logging.debug('pgsql:')
                    logging.debug('col[1]:')
                    logging.debug(col[1])

                oid = col[1]

                if self.debug:
                    logging.debug('')
                    logging.debug('oid=')
                    logging.debug(oid)
                              
                if (oid == 1043):
                    dbdatatype = 'VARCHAR'
                
                if (oid == 1082):
                    dbdatatype = 'date'

                if (oid == 1083):
                    dbdatatype = 'time'

                #if (oid == 1083):
                #    dbdatatype = 'DATETIME'

                if (oid == 1114):
                    dbdatatype = 'TIMESTAMP'

                if (oid == 23):
                    dbdatatype = 'integer'
                
                if (oid == 21):
                    dbdatatype = 'smallint'

                if (oid == 20):
                    dbdatatype = 'bigint'

                if (oid == 700):
                    dbdatatype = 'real'
                
                if (oid == 701):
                    dbdatatype = 'double'
                
                if (oid == 1700):
                    dbdatatype = 'numeric'

                if self.debug:
                    logging.debug('')
                    logging.debug(f'dbdatatype = {dbdatatype:s}')
            #
            # } end postgresql datatype
            #
            
            elif (self.dbms.lower() == 'mysql'):
            #
            # { mysql datatype from descriptor
            #    
                if self.debug:
                    logging.debug('')
                    logging.debug(f'mysql:')
                    logging.debug('col[1]:')
                    logging.debug(col[1])

                coltype = int(col[1])
                
                if (coltype == 253):
                    dbdatatype = 'VARCHAR'
             
                if (coltype == 3):
                    dbdatatype = 'LONG'

                if (coltype == 5):
                    dbdatatype = 'DOUBLE'

                if (coltype == 4):
                    dbdatatype = 'FLOAT'

                if (coltype == 6):
                    dbdatatype = 'NULL'

                if (coltype == 7):
                    dbdatatype = 'TIMESTAMP'

                if (coltype == 8):
                    dbdatatype = 'LONGLONG'

                if (coltype == 10):
                    dbdatatype = 'DATE'
            #
            # } end mysql datatype
            #

            if self.debug:
                logging.debug(f'dbdatatype    = {dbdatatype:s}')
                logging.debug('size=')
                logging.debug(size)

            if(col[2] is not None):
                size = int(col[2])
            
                if self.debug:
                    logging.debug(f'size= {size:d}')

            if(col[3] is not None):
                internal_size = int(col[3])
            
                if self.debug:
                    logging.debug(f'internal_size= {internal_size:d}')

            if(col[4] is not None):
                precision = int(col[4])

                if self.debug:
                    logging.debug(f'precision = {precision:d}')

            if(col[5] is not None):
                scale = int(col[5])

                if self.debug:
                    logging.debug(f'scale     = {scale:d}')

            #
            # } end  extract dbdatatype, display_size, precision,
            #   scale from
            #   col_array: col[1], col[2], col[4], and col[5]

            #
            # Check if colname is in DD,
            #
            # colname in dd: if datatype == char/date/timestamp: analyze
            #                description
            #
            # colname NOT in dd: get dbtype, width, fmt from col[1],
            #                    col[2], col[4] and col[5]
            #

            ind = -1
            if (self.dd != None):
                ind = self.__getDDIndex__(self.dd, colname)

            dbtype = ''
            coltype = ''
            units = ''
            desc = ''
            fmt = ''
            width = 0

            if(ind != -1):

            #
            # { col in dd
            #

                width = self.dd.colwidth[colname]

                coltype = self.dd.coltype[colname]

                dbtype = dbdatatype

                units = self.dd.colunits[colname]

                fmt = self.dd.colfmt[colname]

                desc = self.dd.coldesc[colname]

                isddcolarr.append(1)
                intcntarr.append(0)
                fltcntarr.append(0)

                #
                # {  char/date/datetime/timestamp: verify format with
                #    description dbtype
                #

                if ((dbtype.lower() == 'string') or \
                    (dbtype.lower() == 'varchar')):

                    if self.debug:
                        logging.debug('')
                        logging.debug(f'char type col in dd')
                        logging.debug('size=')
                        logging.debug(size)
                    
                    #if (size is not None):
                    #    width = size
  
                    #if self.debug:
                    #    logging.debug('')

                    if (len(colname) > width):
                        width = len(colname)

                    coltype = 'char'
                    dbtype = "varchar(" + str(width) + ")"
                    fmt = str(width) + 's'

                elif(dbtype.lower() == 'date'):

                    coltype = 'char'

                    width = 14
                    if(len(colname) > width):
                        width = len(colname)
                    fmt = str(width) + 's'


                elif((dbtype.lower() == 'datetime') or \
                    (dbtype.lower() == 'timestamp')):

                    coltype = 'char'

                    width = 30
                    if(len(colname) > width):
                        width = len(colname)
                    fmt = str(width) + 's'


                if self.debug:
                    logging.debug('')
                    logging.debug('Column is in dd:')
                    logging.debug(f'      colname = {colname:s}')
                    logging.debug(f'      coltype = {coltype:s}')
                    logging.debug(f'      dbtype  = {dbtype:s}')
                    logging.debug(f'      fmt     = {fmt:s}')
                    logging.debug(f'      width   = {width:d}')
                    logging.debug(f'      desc    = {desc:s}')

                #
                # } end char/date/datetime/timestamp: verify format with
                #   description dbtype
                #

            #
            # } end col in dd
            #

            else:

            #
            # { col Not in dd
            #

                if ((dbdatatype.lower() == 'string') or \
                    (dbdatatype.lower() == 'varchar')):

                #
                # { dbdatatype == string, varchar
                #

                    width = 80
                    if ((size is not None) and (size > 0)):
                        width = size
  
                    if (len(colname) > width):
                        width = len(colname)

                    coltype = 'char'
                    dbtype = dbdatatype
                    fmt = str(width) + 's'

                #
                # } end dbdatatype == varchar 
                #

                elif ((dbdatatype.lower() == 'date') or \
                    (dbdatatype.lower() == 'time') or \
                    (dbdatatype.lower() == 'datetime') or \
                    (dbdatatype.lower() == 'timestamp')):

                #
                #
                # { dbdatatype == DATE, datetime, TIMESTAMP
                #
                #  written as 'char'
                #

                    coltype = 'char'
                    dbtype  = dbdatatype
                    width   = 30

                    if (len(colname) > width):
                        width = len(colname)
                    fmt = str(width) + 's'

                #
                # } end dbdatatype == DATE, datetime, TIMESTAMP
                #

                elif ((dbdatatype.lower() == 'long') or \
                    (dbdatatype.lower() == 'int') or \
                    (dbdatatype.lower() == 'integer') or \
                    (dbdatatype.lower() == 'smallint')):
                #
                #
                # { dbdatatype <= 32 bits integer
                #

                    coltype = 'int'
                    dbtype = 'long'

                    width = 22
                    if(len(colname) > width):
                        width = len(colname)
                    fmt = str(width) + 'd'

                #
                # } end dbdatatype == integer <= 32 bits
                #

                elif ((dbdatatype.lower() == 'longlong') or \
                    (dbdatatype.lower() == 'bigint')):
                    #
                    # { dbdatatype == 64 bits integer
                    #

                    coltype = 'longlong'
                    dbtype = 'longlong'

                    width = 24
                    if(len(colname) > width):
                        width = len(colname)
                    fmt = str(width) + 'ld'

                    #
                    # } end dbdatatype == LONGLONG
                    #

                elif ((dbdatatype.lower() == 'float') or \
                    (dbdatatype.lower() == 'real') or \
                    (dbdatatype.lower() == 'double')):

                #
                # { dbdatatype == NATIVE_FLOAT
                #

                    coltype = 'double'
                    dbtype = 'float'
                    fmt = '22.14e'

                    width = 22
                    if(len(colname) > width):
                        width = len(colname)
                    fmt = str(width) + '.14e'

                #
                # } end dbdatatype == NATIVE_FLOAT
                #

                elif ((dbdatatype.lower() == 'number') and \
                    (self.dbms.lower() == 'oracle')):

                #
                # { dbdatatype == NUMBER: this is Oracle special dtype
                #

                    if (scale is None):

                        coltype = 'int'
                        dbtype = dbdatatype

                        width = 22
                        if(len(colname) > width):
                            width = len(colname)
                        fmt = str(width) + 'd'

                    else:

                        coltype = 'double'
                        dbtype = dbdatatype

                        width = 22
                        if(len(colname) > width):
                            width = len(colname)
                        fmt = str(width) + '.14e'

                #
                # } end dbdatatype == NUMBER
                #

                elif ((dbdatatype.lower() == 'numeric') and \
                    (self.dbms.lower() == 'pgsql')):

                #
                # { dbdatatype == numeric: this is pgsql oid=1700 special dtype,
                #  if no dd, then we assume it is a double number, although
                #  this dbdatatype is often used to represent a large integer.
                # 

                    coltype = 'double'
                    dbtype = dbdatatype

                    width = 22
                    if(len(colname) > width):
                        width = len(colname)
                    fmt = str(width) + '.14e'

                #
                # } end dbdatatype == numeric 
                #

                else:

                #
                # { unknown type: default to char (80)
                #

                    coltype = 'char'
                    dbtype = dbdatatype

                    width = 80
                    if(len(colname) > width):
                        width = len(colname)
                    fmt = str(width) + 's'

                #
                # } end unknown type
                #

                #
                # Special cases: if colname == 'RA' or 'DEC' use 
                #                racol, decol in dd
                #

                if (dd is not None):
                
                    if(colname.lower() == 'ra'):

                        if((dbdatatype == 'NUMBER') and \
                            (self.ind_racol != -1)):

                            width = dd.colwidth[self.racol.lower()]
                            coltype = dd.coltype[self.racol.lower()]
                            units = dd.colunits[self.racol.lower()]
                            fmt = dd.colfmt[self.racol.lower()]

                    elif(colname.lower() == 'dec'):

                        if((dbdatatype == 'NUMBER') and \
                            (self.ind_deccol != -1)):

                            width = dd.colwidth[self.deccol.lower()]
                            coltype = dd.coltype[self.deccol.lower()]
                            units = dd.colunits[self.deccol.lower()]
                            fmt = dd.colfmt[self.deccol.lower()]

                if self.debug:
                    logging.debug('')
                    logging.debug('column not in dd')
                    logging.debug(f'      width   = {width:d}')
                    logging.debug(f'      dbtype  = {dbtype:s}')
                    logging.debug(f'      coltype = {coltype:s}')
                    logging.debug(f'      fmt     = {fmt:s}')


                isddcolarr.append(0)
                intcntarr.append(0)
                fltcntarr.append(0)

            #
            # } end col Not in dd
            #

            namearr.append(colname)
            typearr.append(coltype)
            dbtypearr.append(dbtype)
            fmtarr.append(fmt)
            widtharr.append(width)
            unitsarr.append(units)
            descarr.append(desc)

            i = i + 1

        #
        # } end of analysing cursor description for loop
        #

        #
        # At this point the namearr, typearr, dbtypearr, widtharr of
        # output columns are assigned: add them to the ddlist for
        # sending to C routine (We don't send the whole dd to C routine
        # to avoid excess data transfer)
        #

        len_arr = len(namearr)

        if self.debug:

            logging.debug('')
            logging.debug('DD summary:')

            for i in range(0, len_arr):
                logging.debug('')
                logging.debug(f'      name[{i:d}]   = [{namearr[i]:s}]')
                logging.debug(f'      type[{i:d}]   = [{typearr[i]:s}]')
                logging.debug(f'      dbtype[{i:d}] = [{dbtypearr[i]:s}]')
                logging.debug(f'      fmt[{i:d}]    = [{fmtarr[i]:s}]')
                logging.debug(f'      width[{i:d}]  = [{widtharr[i]:d}]')
                logging.debug(f'      units[{i:d}]  = [{unitsarr[i]:s}]')
                logging.debug(f'      desc[{i:d}]   = [{descarr[i]:s}]')

        ddlist.append(namearr)
        ddlist.append(typearr)
        ddlist.append(dbtypearr)
        ddlist.append(fmtarr)
        ddlist.append(unitsarr)
        ddlist.append(descarr)
        ddlist.append(widtharr)

        if self.debug:
            logging.debug('')
            logging.debug('ddlist = ')
            logging.debug('-----------------------------------------------')
            logging.debug(ddlist)
            logging.debug('-----------------------------------------------')

        #
        # If maxrec == 0: write header and exit
        #
        if self.debug:
            logging.debug('')
            logging.debug(f'self.maxrec= {self.maxrec:d}')

        if(self.maxrec == 0):
        #
        # {
        #

            self.ishdr = 1
            self.overflow = 1
            self.istail = 1

            self.status = '' 
            rowslist = []
            try:

                istatus = writerecs.writerecs(self.outpath, self.format,
                                              ddlist, rowslist, self.ishdr,
                                              self.coldesc, self.overflow,
                                              self.istail)

                if(istatus == 0):
                    self.status = 'ok'

            except Exception as e:

                self.status = 'error'
                self.msg = str(e)

                if self.debug:
                    logging.debug('')
                    logging.debug(f'writerecs exception: {str(e):s}')

                raise Exception(str(e))

            return

        #
        # } end maxrec == 0
        #

        if self.debug:
            logging.debug('')
            logging.debug('Start constructing rowslist')
            logging.debug('----------------------------------------')

        #
        # Start fetching data lines
        #

        nfetch = self.arraysize
        if self.debug:
            logging.debug('')
            logging.debug(f'nfetch = {nfetch:d}')


        self.cursor.arraysize = nfetch
        curr_rowcnt = cursor.rowcount

        if self.debug:
            logging.debug('')
            logging.debug(f'total rowcnt in cursor: {curr_rowcnt:d}')


        ibatch = 0

        self.overflow = 0
        irow = 0
        self.ntot = 0

        #
        # set maxrec to 20 for debug
        #
        #self.maxrec = 20
        #
        while True:

        #
        # { start of while loop for fetching data lines;
        #   each batch is set to 10000 lines by nfetch = 10000
        #

            #if (self.dbms.lower() == 'mysql'):
            #    rows = cursor.fetchmany (self.cursor.arraysize)
            #else: 
            #    rows = cursor.fetchmany(self.cursor.arraysize)
            
            rows = cursor.fetchmany (self.cursor.arraysize)

            nrec = len (rows)

            if self.debug:
                logging.debug(f'nrec = {nrec:d}')
                logging.debug('')

            if (nrec == 0):
                break

            rowslist = []


            # This block was added for cases like SQLite, where
            # the column "description" block returned by the DBMS
            # does not give any datatypes.  For those cases where
            # we don't have a priori schema information, we have to
            # wait for the first data return to determine types. 

            # This is complicated by the possibility that for some
            # records some columns are sometimes NULL (value 'None').
            # So while we don't want to scan all the data, we have 
            # to scan until we find some value for every column.

            if ((ibatch == 0) and (self.dbms.lower() == 'sqlite')):
            #
            # { sqlite special treatment block
            #
                for ll in range(0, nrec):

                    row = rows[ll]

                    #rowlist = []

                    for i in range(0, len(row)):
                            
                            dtype = type(row[i]).__name__

                            if ((dtype != None) and \
                                (dbtypearr[i] == '' or typearr[i] == '')):

                                if dtype.lower() == 'int':
                                    dbtypearr[i] = 'NUMBER'
                                    typearr[i] = 'int'

                                if dtype.lower() == 'float':
                                    dbtypearr[i] = 'NUMBER'
                                    typearr[i] = 'double'

                                if dtype.lower() == 'str':
                                    dbtypearr[i] = 'VARCHAR'
                                    typearr[i] = 'char'

            #
            # } end special treatment for sqlite datatype assignment
            #


            for ll in range(0, nrec):

            #
            # { Beginning ll loop: one row
            #

                if self.debug:
                    logging.debug('')
                    logging.debug('start processing each row of data')
                
                row = rows[ll]

                rowlist = []

                for i in range(0, len(row)):

                #
                # { Beginning i loop: each data col in this row
                #
                   
                    if self.debug:
                        logging.debug('')
                        logging.debug(f'i = {i:d}')
                        logging.debug(f'row[i]= {row[i]}')

                    if(i == self.ind_exclcol):
                        continue

                    if((dbtypearr[i].lower() == 'date') or \
                        (dbtypearr[i].lower() == 'time') or \
                        (dbtypearr[i].lower() == 'datetime') or \
                        (dbtypearr[i].lower() == 'timestamp')):

                        rowlist.append (str(row[i]))
                    else:
                        if (self.dbms.lower() == 'pgsql'):
                        #
                        # {if pgsql dbtype = 'numeric', dtype will be decimal,
                        # make it integer or float depending on the python type
                        # of typearr value 
                        #
                            dtype = type (row[i]).__name__
                            
                            if self.debug:
                                logging.debug('')
                                logging.debug( \
                                    'check if the datum is pgsql decimal type')
                                logging.debug(f'i = {i:d} dtype= {dtype}')

                            if (dtype.lower() == 'decimal'):
                                
                                x = float(row[i])

                                if self.debug:
                                    logging.debug('')
                                    logging.debug('convert row[i] to float')
                                    logging.debug(f'x = {x}')
                                    logging.debug ( \
                                        f'i = {i:d} typearr[i]= {typearr[i]}')
                                    logging.debug ( \
                                        f'dbtypearr[i]= {dbtypearr[i]}')
                         
                                if (typearr[i] == 'int'):
                                    dbtypearr[i] = 'integer'
                                    rowlist.append (int(x))

                                else:
                                    dbtypearr[i] = 'double'
                                    rowlist.append (x)
                            else:
                                rowlist.append (row[i])

                            if self.debug:
                                logging.debug('')
                                logging.debug('final pgsql result')
                                logging.debug(f'typearr= {typearr[i]}')
                                logging.debug(f'dbtypearr= {dbtypearr[i]}')
                            #
                            # } end pgsql decimal number treatment
                            #
                        else:
                            rowlist.append(row[i])
                        

                        #
                        # For DBMSs like Oracle that store numbers as generic 
                        # NUMBER type, we need to try to distinguish between
                        # int and double by checking the output row[i]'s 
                        # python datatype -- which might not be completely
                        # accurate so we check the whole column to make 
                        # summary determination 
                        #
                        if ((self.dbms.lower() == 'oracle') and \
                            (ibatch == 0) and \
                            (isddcolarr[i] == 0) and \
                            (dbtypearr[i] == 'NUMBER')):

                            dtype = type(row[i]).__name__
                    
                            if self.debug:
                                logging.debug('')
                                logging.debug(f'i = {i:d} dtype=')
                                logging.debug(dtype)

                            if (dtype == 'int'):
                                intcntarr[i] = intcntarr[i] + 1
                            else:
                                fltcntarr[i] = fltcntarr[i] + 1

                #
                # } end of i loop
                #

                if self.debug:
                    logging.debug('')
                    logging.debug(f'irow= {irow:d} rowlist: ')
                    logging.debug(f'typearr= {typearr}')
                    logging.debug(f'dbtypearr= {dbtypearr}')
                    logging.debug(rowlist)

                rowslist.append(rowlist)

                irow = irow + 1

                if self.debug:
                    logging.debug('')
                    logging.debug( \
                        f'irow = {irow:d} self.maxrec= {self.maxrec:d}')
                
                if((self.maxrec > 0) and (irow >= self.maxrec)):
                    self.overflow = 1
                    break

            #
            # } end of l loop
            #

            if self.debug:
                logging.debug('----------------------------------------')

            #
            # The following block determines the NUMBER type ORACLE arrays's
            # datatype -- whether the column should be double or int based
            # on our summary count.
            #
            if ((self.dbms.lower() == 'oracle') and \
                (ibatch == 0)):
            #
            # {
            #

                for i in range(0, len(isddcolarr)):

                #
                # { check intcntarr and fltcntarr
                #

                    if((isddcolarr[i] == 0) and (dbtypearr[i] == 'NUMBER')):

                        if((intcntarr[i] > 0) and (fltcntarr[i] == 0)):

                            typearr[i] = 'int'
                            dbtypearr[i] = 'integer'

                            widtharr[i] = 22
                            if(len(namearr[i]) > widtharr[i]):
                                widtharr[i] = len(namearr[i])

                            fmtarr[i] = str(widtharr[i]) + 'd'

                        else:
                            typearr[i] = 'double'
                            dbtypearr[i] = 'float'

                            widtharr[i] = 22
                            if(len(namearr[i]) > widtharr[i]):
                                widtharr[i] = len(namearr[i])
                            fmtarr[i] = str(widtharr[i]) + '.14e'

                #
                # } end checking intcntarr and fltcntarr
                #
            #
            # } end if ibatch == 0
            #

            if self.debug:
                logging.debug('got here0')

            if(self.ntot == 0):
                self.ishdr = 1
            else:
                self.ishdr = 0

            self.ntot = self.ntot + nrec

            if((self.overflow == 1) and (irow >= self.maxrec)):
                self.istail = 1

            if(len(rows) < self.cursor.arraysize):
                self.istail = 1

            self.status = None

            try:

                if self.debug:
                    logging.debug('call writerecs.writerecs')

                istatus = writerecs.writerecs(self.outpath, self.format,
                                              ddlist, rowslist, self.ishdr,
                                              self.coldesc, self.overflow,
                                              self.istail)
                
                if self.debug:
                    logging.debug('returned writerecs.writerecs')

                if(istatus == 0):
                    self.status = 'ok'

            except Exception as e:

                self.status = 'error'
                self.msg = str(e)

                if self.debug:
                    logging.debug('')
                    logging.debug(f'writerecs exception: {str(e):s}')

                raise Exception(str(e))

            if((self.overflow == 1) and (irow >= self.maxrec)):
                break

            if self.debug:
                logging.debug('got here0-1')

            if (len(rows) < self.cursor.arraysize):
                break

            if self.debug:
                logging.debug('got here0-2')

            ibatch = ibatch + 1

        #
        # } end while loop for fetching data lines
        #

        return

    #
    # } end of writeResult.init
    #


    def __getArrIndex__(self, arr, name):

    #
    # {
    #

        ind = -1

        for i in range(len(arr)):

            if(name.lower() == arr[i]):
                ind = i
                break

        return(ind)

    #
    # } end of getArrIndex def
    #


    def __getDDIndex__(self, dd, name):

    #
    # {
    #

        ind = -1

        for i in range(len(dd.colname)):

            if(name.lower() == dd.colname[i].lower()):
                ind = i
                break

        return(ind)

    #
    # } end __getDDIndex
    #
