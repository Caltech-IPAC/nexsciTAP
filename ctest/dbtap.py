from TAP.tapstore  import tapStore
from TAP.taputil   import tapUtil
from TAP.tapquery  import tapQuery

dbutils  = None
dbconfig = None
dbconn   = None

dbdebug = False

def dbinit(debug):

    global dbutils
    global dbconfig
    global dbconn
    global dbadql
    global dbdebug

    dbdebug = debug

    if dbdebug == True:
        print('DBDEBUG> running dbinit()')

    try:
        dbstore  = tapStore()

        if dbdebug == True:
            print('DBDEBUG> dbstore created.')

        dbutils  = tapUtil(store=dbstore)

        if dbdebug == True:
            print('DBDEBUG> config initialized and dbutils created.')

        dbconfig = dbstore.getConfig()

        if dbdebug == True:
            print('DBDEBUG> Retrieved dbconfig.')

        dbconn   = dbstore.getConn()

        if dbdebug == True:
            print('DBDEBUG> Retrieved dbconn.')

        dbadql   = dbstore.getADQL()

        if dbdebug == True:
            print('DBDEBUG> Retrieved adql.')

    except Exception as e:
        if dbdebug == True:
            print('Exception: dbinit() failed.')


def dbquery(adql_string, filename):

    global dbutils
    global dbconfig
    global dbconn
    global dbadql
    global dbdebug

    if dbdebug == True:
        print('DBDEBUG> running DB.query()')

    msg = 'Query failed.'

    try:
        format = 'ipac'

        cursor = dbconn.cursor()

        if dbdebug == True:
            print('DBDEBUG> cursor created.')
        
        sql_string  = dbadql.sql(adql_string)

        if dbdebug == True:
            print('DBDEBUG> ADQL string translated.')
            print('   ', adql_string)
            print('-->', sql_string)

        dbQuery = tapQuery(connectInfo=dbconfig.connectInfo,
                           conn=dbconn, 
                           query=sql_string, 
                           filename=filename,
                           format=format)

        if dbdebug == True:
            print('DBDEBUG> tapQuery() run.')

        status  = dbQuery.stat.upper()

        outfile = dbQuery.outpath
        nrec    = dbQuery.ntot
        msg     = dbQuery.returnMsg

        if dbdebug == True:
            print('DBDEBUG> tapQuery() return parsed.')

        if status == 'OK':
            return status, outfile, nrec
        else:
            return status, msg, 0

    except Exception as e:
            return status, msg, 0

