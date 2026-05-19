#!/bin/env python

# This example is as much documentation as processing.  In a real
# application we would only need parts of it.

from TAP.tapstore  import tapStore
from TAP.taputil   import tapUtil
from TAP.tapquery  import tapQuery


# We may run into problems while connecting configuratio information,
# translating the ADQL query into SQL, or submitting the user query
# for execution.  These all throw exceptions which in this example
# program are returned by a top-level exception handler.

def tapc(catalog, select_stmt, result_tbl):

    try:
        # The 'tapUtil' object parse our TAP configuration file and
        # sets up the DBMS connection.  The configuration information
        # and that connection are stored in a simple set/get object
        # and retrieve for use in this program.

        store  = tapStore()
        utils  = tapUtil(store=store)

        config = store.getConfig()
        conn   = store.getConn()


        # Here are the user-settable query parameters

        adql_string = select_stmt
        filename    = result_tbl
        format      = 'ipac'


        #  Since we have the database cursor, we can do any additional processing we need.
        #  This is particularly useful if we need to get row counts, drop tables or delete
        #  rows, or create temporary tables for special joins.

        cursor = conn.cursor()
        cursor.execute('select count(*) from ' + catalog)
        rowcount = cursor.fetchone()[0]
        

        # But the main purpose of TAP is to run ADQL queries, including spatial constraints
        # and output formatting (output file type, column width, and number formatting).

        sql_string  = utils.ADQL(adql_string)

        query = tapQuery(connectInfo=config.connectInfo, conn=conn, query=sql_string, filename=filename, format=format)

        status  = query.stat.upper()
        outfile = query.outpath
        nrec    = query.ntot
        msg     = query.returnMsg


        # The returned 'query' object has several internal parameters but the ones most
        # useful for further processing are the return status, the output file name 
        # (though this is just the name we put in above), and the return record count.

        if status == 'OK':
            return status, outfile, rowcount, nrec
        else:
            return status, msg, 0, 0

    except Exception as e:
        return 'ERROR', msg, 0, 0
