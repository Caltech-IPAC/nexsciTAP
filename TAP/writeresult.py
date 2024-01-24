# Copyright (c) 2020, Caltech IPAC.
# This code is released with a BSD 3-clause license. License information is at
#   https://github.com/Caltech-IPAC/nexsciTAP/blob/master/LICENSE


#    writeResult class
#
import logging

import datetime

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
    dbms = ''

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
        # {
        #

        """
        cursor:       oracle query returned cursor containing data stream,
        workdir:      work directory for output files,
        filename:     output file name. 
        dd:           data dictionary for the output columns,
        coldes(0/1): indictes whether to include column descriptions in the
                  output file header

        kwargs inputs:

            format(char),
            maxrec(int),
            racol,
            deccol,
            exclcol(int): exclude column index

        Usage:

            wresult = writeResult(cursor, \
                workdir, \
                filename, \
                dd, \
                format=format, \
                maxrec=maxrec, \
                coldesc=coldesc, \
                racol=racol, \
                deccol=sdeccol)
        """

        if('debug' in kwargs):
            self.debug = kwargs['debug']

        if self.debug:
            logging.debug('')
            logging.debug('Enter writeResult:')
            logging.debug(f'self.debug= {self.debug:d}')
        
        
        self.cursor = cursor
        self.workdir = workdir
        self.dd = dd

        self.ncols_dd = 0
        if (self.dd != None):
            self.ncols_dd = len(self.dd.colname)

        if self.debug:
            logging.debug('')
            logging.debug(f'ncols_dd= {self.ncols_dd:d}')
        
        if('racol' in kwargs):
            self.racol = kwargs['racol']

            if (self.dd != None):
                self.ind_racol = self.__getDDIndex__(self.dd, self.racol)

        if('deccol' in kwargs):
            self.deccol = kwargs['deccol']
            
            if (self.dd != None):
                self.ind_deccol = self.__getDDIndex__(self.dd, self.deccol)

        if('exclcol' in kwargs):
            self.ind_exclcol = kwargs['exclcol']

        if('dbms' in kwargs):
            self.dbms = kwargs['dbms']
        
        if self.debug:
            logging.debug('')
            logging.debug(f'dbms= {self.dbms:s}')
        
        
        if('format' in kwargs):
            self.format = kwargs['format']

        if('maxrec' in kwargs):
            self.maxrec = kwargs['maxrec']

        if self.debug:
            logging.debug('')
            logging.debug('here0-2')
        
        self.coldesc = 0
        if('coldesc' in kwargs):
            self.coldesc = kwargs['coldesc']

        self.arraysize = 10000
        if('arraysize' in kwargs):
            self.arraysize = kwargs['arraysize']

        if self.debug:
            logging.debug('')
            logging.debug('from kwargs:')
            logging.debug(f'      format     = {self.format:s}')
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

        self.filename = 'result'

        if('filename' in kwargs):
            self.filename = kwargs['filename']

            if(self.filename == None):
                self.filename = 'result'

                if(self.format == 'votable'):
                    self.filename = self.filename + '.xml'
                elif(self.format == 'ipac'):
                    self.filename = self.filename + '.tbl'
                elif(self.format == 'csv'):
                    self.filename = self.filename + '.csv'
                elif(self.format == 'tsv'):
                    self.filename = self.filename + '.tsv'
                elif(self.format == 'json'):
                    self.filename = self.filename + '.json'
            
            if(self.filename[0] != '/'):
                self.outpath = self.workdir + '/' + self.filename
 
        else:
            if(self.format == 'votable'):
                self.filename = self.filename + '.xml'
            elif(self.format == 'ipac'):
                self.filename = self.filename + '.tbl'
            elif(self.format == 'csv'):
                self.filename = self.filename + '.csv'
            elif(self.format == 'tsv'):
                self.filename = self.filename + '.tsv'
            elif(self.format == 'json'):
                self.filename = self.filename + '.json'
 
            self.outpath = self.workdir + '/' + resulttbl

        if self.debug:
            logging.debug('')
            logging.debug(f'filename= {self.filename:s}')
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

        self.ncol = len(self.cursor.description)

        if self.debug:
            logging.debug('')
            logging.debug(f'ncol = {self.ncol:d}\n')
            logging.debug('cursor.description:')
            logging.debug('------------------------------------------------')
            logging.debug(self.cursor.description)
            logging.debug('------------------------------------------------')

        #XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
        for desc in self.cursor.description:
            name = desc[0]
            coltype = desc[1]
            dispsize = desc[2]
            internalsz = desc[3]
            precision = desc[4]
            scale = desc[5] 
            nullok = desc[6]
        


        #
        # C interface list:
        #
        # [namearr, typearr, dbtypearr, fmtarr, unitsarr, widtharr, descarr]
        #

        ddlist = []

        namearr   = []
        typearr   = []
        dbtypearr = []
        fmtarr    = []
        unitsarr  = []
        widtharr  = []
        descarr   = []

        #
        # For non-dd column and dbtype == NUMBER, check data to see if
        # it is int or float: if any number in the fltcntarr, consider it float
        #

        isddcolarr = []
        intcntarr  = []
        fltcntarr  = []


        dbdatatype = None
        precision  = None 
        scale      = None
        size       = None 

        i = 0

        for col in self.cursor.description:

            #
            # { For loop for cursor description:
            #   cursor description is a list with ncols tuples:
            #   each tuple
            #   contains each output colname's name, datatype,
            #   dbdatatype, size, etc...
            #

            if self.debug:
                logging.debug('')
                logging.debug('----------------------------------------------')
                logging.debug(f'ind_exclcol= {self.ind_exclcol:d}\n')
                logging.debug(f'i = {i:d} col = ' + str(col))

            #
            # Extract colname, dbdatatype, size, precision, scale from
            # col array
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
            # col_array:
            #   col[1], col[2], col[4], and col[5]
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

            elif (self.dbms.lower() == 'sqlite3'):
            #
            # { mysql datatype from descriptor
            #    
                if self.debug:
                    logging.debug('')
                    logging.debug(f'sqlite3:')

                dbdatatype = ''
            #
            # } end sqlite3 datatype
            #

            if self.debug:
                try:
                    logging.debug(f'dbdatatype = {dbdatatype:s}')
                    logging.debug(f'size       = {size:d}')

                except Exception as e:
                    logging.debug(f'dbdatatype not set.')
                    pass


            if(col[2] is not None):
                size = int(col[2])
            
                if self.debug:
                    logging.debug(f'size= {size:d}')


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

            dbtype  = ''
            coltype = ''
            units   = ''
            desc    = ''
            fmt     = ''
            width   = 0

            if(ind != -1):

                #
                # { col in dd
            #
                dbtype  = dbdatatype
                width   = self.dd.colwidth[colname]
                coltype = self.dd.coltype [colname]
                units   = self.dd.colunits[colname]
                fmt     = self.dd.colfmt  [colname]
                desc    = self.dd.coldesc [colname]

                isddcolarr.append(1)
                intcntarr.append(0)
                fltcntarr.append(0)

                #
                # {  char/date/datetime/timestamp: verify format with
                #    description dbtype
                #

                if dbtype == None:
                    pass

                elif ((dbtype.lower() == 'string') or \
                      (dbtype.lower() == 'varchar')):

                    if self.debug:
                        logging.debug('')
                        logging.debug(f'here0: char type col in dd')
                        logging.debug('size=')
                        logging.debug(size)
                
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
                if dbdatatype == None:
                    pass


                elif(dbdatatype == 'STRING') or (dbdatatype == 'VARCHAR'):

                    #
                    # { dbdatatype == string
                    #

                    width = 80
                    if ((size is not None) and (size > 0)):
                        width = size
  
                    if(len(colname) > width):
                        width = len(colname)

                    coltype = 'char'
                    dbtype = dbdatatype
                    fmt = str(width) + 's'

                    #
                    # } end dbdatatype == STRING
                    #

                elif((dbdatatype == 'DATE')
                        or (dbdatatype == 'DATETIME')
                        or (dbdatatype == 'TIMESTAMP')):

                    #
                    #
                    # { dbdatatype == DATE, TIMESTAMP
                    #
                    #    timestamp etc. are not included in the above
                    #    but most of them should come out as STRING
                    #

                    coltype = 'char'
                    dbtype  = dbdatatype
                    width   = 30

                    if(len(colname) > width):
                        width = len(colname)
                    fmt = str(width) + 's'

                    #
                    # } end dbdatatype == DATE, TIMESTAMP
                    #

                elif ((dbdatatype == 'LONG') or \
                    (dbdatatype == 'INT')):
                    #
                    # { dbdatatype == LONG or INT
                    #

                    coltype = 'int'
                    dbtype = 'long'

                    width = 22
                    if(len(colname) > width):
                        width = len(colname)
                    fmt = str(width) + 'd'

                    #
                    # } end dbdatatype == LONG or INT
                    #

                elif (dbdatatype == 'LONGLONG'):
                    #
                    # { dbdatatype == LONGLONG
                    #

                    coltype = 'long'
                    dbtype = 'longlong'

                    width = 24
                    if(len(colname) > width):
                        width = len(colname)
                    fmt = str(width) + 'ld'

                    #
                    # } end dbdatatype == LONGLONG
                    #

                elif ((dbdatatype == 'FLOAT') or \
                    (dbdatatype == 'DOUBLE')):

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

                elif(dbdatatype == 'NUMBER'):

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

                else:

                    #
                    # { unknown type: default to char
                    #

                    coltype = 'unknown'
                    dbtype = dbdatatype
                    width = 0
                    fmt = ''

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
            # } end of for loop for analysing dd's cursor description
            #

        #
        # At this point the namearr, typearr, dbtypearr, widtharr of
        # output columns are assigned: add them to the ddlist for
        # sending to C routine (We don't send the whole dd to C routine
        # to avoid excess data transfer)
        #

        len_arr = len(namearr)

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

        if(self.maxrec == 0):

            #
            # {
            #

            self.ishdr = 1
            self.overflow = 1
            self.istail = 1

            self.status = None
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
            logging.debug(f'nfetch = {nfetch:d}')
            logging.debug('')

        self.cursor.arraysize = nfetch

        ibatch        = 0
        self.overflow = 0
        irow          = 0
        self.ntot     = 0

        #
        #    set maxrec to 20 for debug
        #
        #self.maxrec = 20
        #

        while True:

            if self.debug:
                logging.debug(f'batch = {ibatch:d}')
                logging.debug('')


            #
            # { start of while loop for fetching data lines;
            #   max 10000 lines at a time
            #

            if (self.dbms.lower() == 'mysql'):
                rows = cursor.fetchmany (self.cursor.arraysize)
            else: 
                rows = cursor.fetchmany(self.cursor.arraysize)

            nrec = len(rows)

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

            if ((ibatch == 0) and (self.dbms.lower() == 'sqlite3')):
                
                for ll in range(0, nrec):

                    row = rows[ll]

                    rowlist = []

                    for i in range(0, len(row)):
                            
                        logging.debug('\n\n')

                        dtype = type(row[i]).__name__

                        coltype = ddlist[1][i]

                        llen = len(ddlist[5][i])

                        if coltype == 'unknown':

                            nwidth = len(ddlist[0][i])
                            twidth = len(ddlist[1][i])
                            uwidth = len(ddlist[4][i])

                            newwidth = 22

                            if nwidth > newwidth:
                                newwidth = nwidth

                            if twidth > newwidth:
                                newwidth = twidth

                            if uwidth > newwidth:
                                newwidth = uwidth


                            if dtype.lower() == 'int':
                                ddlist[3][i] = '22d'
                                ddlist[6][i] = newwidth
                                dbtypearr[i] = 'NUMBER'
                                typearr[i] = 'int'


                            if dtype.lower() == 'float':

                                ddlist[3][i] = '22.14e'
                                ddlist[6][i] = newwidth
                                dbtypearr[i] = 'NUMBER'
                                typearr[i] = 'double'


                            if dtype.lower() == 'str':
                                dbtypearr[i] = 'VARCHAR'
                                typearr[i] = 'char'

            #
            # end special treatment for sqlite datatype assignment
            #


            for ll in range(0, nrec):

                #
                # { Beginning ll loop: one row
                #

                row = rows[ll]

                if self.debug:
                    
                    if (ll < 2):
                        logging.debug('')
                        logging.debug('----------')
                        logging.debug(f'row {ll:d}:')
                        logging.debug(row)


                rowlist = []

                for i in range(0, len(row)):

                    #
                    # { Beginning i loop: one col
                    #
                   
                    if self.debug:
                        logging.debug('')
                        logging.debug(f'col {i:d}')
                        logging.debug(row[i])
                        logging.debug(type(row[i]).__name__)

                    if(i == self.ind_exclcol):
                        continue

                    if((dbtypearr[i].lower() == 'date') or \
                        (dbtypearr[i].lower() == 'datetime') or \
                        (dbtypearr[i].lower() == 'timestamp')):

                        rowlist.append(str(row[i]))
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

                logging.debug('')
                logging.debug('ddlist[3]:')
                logging.debug(ddlist[3])

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
        # } end of init
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
