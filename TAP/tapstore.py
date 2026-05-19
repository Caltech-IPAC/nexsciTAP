
class tapStore:

    def __init__(self):

        self.config = None
        self.adql   = None


    # Configuration information

    def setConfig(self, config):
        self.config = config

    def getConfig(self):
        return self.config


    # ADQL translator

    def setADQL(self, adql):
        self.adql = adql

    def getADQL(self):
        return self.adql



    # DBMS connection

    def setConn(self, conn):
        self.conn = conn

    def getConn(self):
        return self.conn

