from TAP.tapstore  import tapStore
from TAP.taputil   import tapUtil
from TAP.tapquery  import tapQuery

dbutils  = None
dbconfig = None
dbconn   = None

def dbinit():

    global dbutils
    global dbconfig
    global dbconn
    global dbadql

    try:
        dbstore  = tapStore()
        dbutils  = tapUtil(store=dbstore)
        dbconfig = dbstore.getConfig()
        dbconn   = dbstore.getConn()
        dbadql   = dbstore.getADQL()

    except Exception as e:
        return 0
    
    return 1


def dbquery(adql_string, filename):

    global dbutils
    global dbconfig
    global dbconn
    global dbadql

    msg = 'Query failed.'

    try:
        format = 'ipac'

        cursor = dbconn.cursor()
        
        sql_string  = dbadql.sql(adql_string)

        dbQuery = tapQuery(connectInfo=dbconfig.connectInfo,
                           conn=dbconn, 
                           query=sql_string, 
                           filename=filename,
                           format=format)

        status  = dbQuery.stat.upper()

        outfile = dbQuery.outpath
        nrec    = dbQuery.ntot
        msg     = dbQuery.returnMsg

        if status == 'OK':
            return status, outfile, nrec
        else:
            return status, msg, 0

    except Exception as e:
            return status, msg, 0

