import getopt
import logging 
import os 
import psycopg2
import psycopg2.extras
import sys

import dataplatformhelpers as DPH

class StoredProcedureLoaderAndUnloader(object):
    """docstring for StoredProcedureLoaderAndUnloader"""
    def __init__(self):
        super(StoredProcedureLoaderAndUnloader, self).__init__()

        #Variable Declarations
        self.host = None
        self.database = None
        self.userName = None
        self.password = None
        self.storedProceduresDirectory = os.getcwd()
        self.logger = None
        self.mode = None

        #Setup Logger
        logFilePath = os.path.join(os.getcwd(),'StoredProcedureLoaderUnloader.log')
        logging.basicConfig(filename = logFilePath,filemode='a',format = '%(levelname)-10s %(asctime)s || %(message)s',level = logging.DEBUG)
        self.logger = logging.getLogger('StoredProcedureLoaderUnloader')
    
    def usage(self):
        print '\n\n'
        print 'Usage options for {scriptname}:'.format(scriptname=sys.argv[0])
        print '-h, --help                                       Shows help message and exits.\n'

        print '------Input Directory Settings---------------------------------\n'
        print '-i, --inputdir  [dirpath]                        The directory that contains the stored procedures you want to load to the HERE/NT database.'

        print '------Input Directory Settings---------------------------------\n'
        print '-m, --mode  [dirpath]                            Specify the script mode [\'load\' or \'unload\'].'

        print '------NT/HERE Database Connection Settings-----------------------------------\n'
        print '-s, --server    [Database host server]           Specifies the server hosting the HERE/NT database.'
        print '-d, --database  [NT/HERE database name]          Specifies HERE/NT database name.'
        print '-u, --user      [Database host user name]        Indicate database host user name.'
        print '-p, --password  [Database host user password]    Indicate the database host user\'s password.'

    def parseCommandLineArguments(self):

        #Parse command line arguments
        try:
            options, remainder = getopt.getopt(sys.argv[1:],'hi:m:s:d:u:p:',['help','inputdir=','mode=','server=','database=','user=','password='])
        except getopt.GetoptError as err:
                print '\nERROR:\n\t',err,'\n'
                self.usage()
                sys.exit(2)

        #Process command line argument options
        for opt,arg in options:
            if opt in ('-h','--help'):
                self.usage()
                sys.exit(0)
            if opt in ('-i','--inputdir'):
                self.storedProceduresDirectory = os.path.join(arg)
            if opt in ('-s', '--server'):
                self.host = arg
            if opt in ('-d', '--database'):
                self.database = arg
            if opt in ('-u','--user'):
                self.userName = arg
            if opt in ('-p','--password'):
                self.password = arg
            if opt in ('-m','--mode'):
                self.mode = arg

    def validateCommandLineArguments(self):
        #Validate commandline arguments
        if self.mode == None:
            beginTestMsg = "ERROR: Script mode not specified. Halting script."
            print beginTestMsg
            self.logger.error(beginTestMsg)
            sys.exit(1)

        if not os.path.exists(self.storedProceduresDirectory) or self.storedProceduresDirectory == None:
            beginTestMsg = "ERROR: Stored Procedures directory not specified or invalid. Halting script."
            print beginTestMsg
            self.logger.error(beginTestMsg)
            sys.exit(1)

        if self.host == None:
            beginTestMsg = "ERROR: Database host not specified. Halting script."
            print beginTestMsg
            self.logger.error(beginTestMsg)
            sys.exit(1)

        if self.database == None:
            beginTestMsg = "ERROR: Source database name not specified. Halting script."
            print beginTestMsg
            self.logger.error(beginTestMsg)
            sys.exit(1)

        if self.userName == None:
            beginTestMsg = "ERROR: database user name not specified. Halting script."
            print beginTestMsg
            self.logger.error(beginTestMsg)
            sys.exit(1)

        if self.password == None:
            beginTestMsg = "ERROR: database user password not specified. Halting script."
            print beginTestMsg
            self.logger.error(beginTestMsg)
            sys.exit(1)


        try:
            dbSourceConnectionValidate = psycopg2.connect('dbname={dbname} host = {host} user={user} password={password}'.format(host=self.host,dbname=self.database,user=self.userName,password=self.password))
            dbSourceConnectionValidate.close()
        except Exception, e:
            beginTestMsg = "ERROR: Connection parameters to HERE/NT database are invalid. Halting script."
            print e
            print beginTestMsg
            self.logger.error(beginTestMsg)
            sys.exit(1)

    def loadStoredProcedures(self):

        #Variable Declarations
        postgresConnection = DPH.PostGresDataBaseConnectionCursor(self.host,self.database,self.userName,self.password)
        count = 0
        filePath = None

        #Begin Loading
        loggerMessage = 'Loading Stored Procedures From {inputdir} to {databasename} within {databasehost}'.format(inputdir=self.storedProceduresDirectory,databasename=self.database,databasehost=self.host)
        self.logger.info(loggerMessage)

        #Iterate Through Stored Procedure File List
        for root,dirs,files in os.walk(self.storedProceduresDirectory):
            for file in files:
                if file.split('.')[1]=='sql':
                    loggerMessage = 'Creating Stored Procedure with: {filename}.'.format(filename=file)
                    self.logger.info(loggerMessage)
                    filePath = os.path.join(self.storedProceduresDirectory,file)
                    inputfile = open(filePath)
                    sqlQuery = inputfile.read()
                    inputfile.close()
                    postgresConnection.executeQuery(sqlQuery)  
                    count = count + 1

                else:
                    loggerMessage = 'The file {filename} is invalid for querying.'.format(filename=file)

        #End Loading
        loggerMessage = 'Loaded {loadcount} Stored Procedures From {inputdir} to {databasename} within {databasehost}'.format(loadcount=count,inputdir=self.storedProceduresDirectory,databasename=self.database,databasehost=self.host)
        self.logger.info(loggerMessage)
        postgresConnection.close()

    def generateDropStoredProcedureQuery(self,storedProcedureName):
        
        #Variable Declarations
        storedProcedureArgumentTableReturnQuery =  'SELECT proname,argtype,argmode,typname '+\
            'FROM (SELECT proname,argtype,argmode '+\
            'FROM (SELECT  *, unnest(proallargtypes) as argtype, unnest(proargmodes) as argmode '+\
            'FROM    pg_catalog.pg_namespace n '+\
            'JOIN    pg_catalog.pg_proc p '+\
            'ON      p.pronamespace = n.oid '+\
            'WHERE   n.nspname = \'public\' and proname like \'tn_%\' and proname = \'{inputstoredprocedure}\') as foo '+\
            'WHERE argmode = \'i\') storedprocedurearguments '+\
            'LEFT JOIN pg_type '+\
            'ON storedprocedurearguments.argtype = pg_type.typelem '

        storedProcedureArgumentSingleDataTypeReturnQuery = 'SELECT proname, foo.proargtypes[0],typname '+\
            'FROM  (SELECT  proname, proargtypes '+\
            'FROM    pg_catalog.pg_namespace n '+\
            'JOIN    pg_catalog.pg_proc p '+\
            'ON      p.pronamespace = n.oid '+\
            'WHERE   n.nspname = \'public\' and proname = \'{inputstoredprocedure}\') as foo '+\
            'LEFT JOIN pg_type ' +\
            'ON foo.proargtypes[0] = pg_type.typelem '

        dropFunctionQueryTemplate = 'DROP FUNCTION {inputstoredprocedure}({arguments})'
        dropFunctionQuery = None
        inputArguments = []
        inputArgument = None
        stringArguments = None
        typeNameColumn = 3

        #Execute Query for Stored Procedure Arguments that Returns Tables
        postgresConnection = DPH.PostGresDataBaseConnectionCursor(self.host,self.database,self.userName,self.password)
        postgresConnection.executeQuery(storedProcedureArgumentTableReturnQuery.format(inputstoredprocedure=storedProcedureName))
        queryResults = postgresConnection.retrieveAllQueryResults()

        if len(queryResults) > 0:

            typeNameColumn = 3

        else:

            #Execute Query for Stored Procedure Arguments that Returns Just One Data Type (eg., char, int, varchar, bigint etc)
            postgresConnection = DPH.PostGresDataBaseConnectionCursor(self.host,self.database,self.userName,self.password)
            postgresConnection.executeQuery(storedProcedureArgumentSingleDataTypeReturnQuery.format(inputstoredprocedure=storedProcedureName))
            queryResults = postgresConnection.retrieveAllQueryResults()     

            typeNameColumn = 2

        #Parse Stored Procedure Arguments
        for result in queryResults:
            inputArgument = result[typeNameColumn].replace('_','')
            inputArguments.append(inputArgument)                   

        #Build Drop Function Query Statement
        stringArguments = str(inputArguments).strip('[]').replace('\'','')
        dropFunctionQuery = dropFunctionQueryTemplate.format(inputstoredprocedure=storedProcedureName,arguments=stringArguments)
        return dropFunctionQuery

    def unloadCustomTypes(self):

        #Variable Declarations
        tnCustomTypeListQuery = 'SELECT typname '+\
            'FROM pg_type '+\
            'WHERE typname LIKE \'tn_%\' '        
        dropCustomTypeQueryTemplate = 'DROP TYPE {inputtypename}'
        dropCustomTypeQuery = None
        customTypeName = None
        dropCustomTypeMessageTemplate = 'Executed {custometypequery}.'
        dropCustomTypeMessage = None

        postgresConnection = DPH.PostGresDataBaseConnectionCursor(self.host,self.database,self.userName,self.password)

        postgresConnection.executeQuery(tnCustomTypeListQuery)
        queryResults = postgresConnection.retrieveAllQueryResults()

        #Drop All Identified TN Custom Types

        for result in queryResults:

            customTypeName = result[0]
            dropCustomTypeQuery = dropCustomTypeQueryTemplate.format(inputtypename=customTypeName)
            postgresConnection.executeQuery(dropCustomTypeQuery)
            dropCustomTypeMessage = dropCustomTypeMessageTemplate.format(custometypequery=dropCustomTypeQuery)
            self.logger.info(dropCustomTypeMessage)

        postgresConnection.close()

    def unloadStoredProcedures(self):

        #Variable Declarations
        queryResults = None
        tnStoredProcedureListQuery = 'SELECT proname '+\
            'FROM    pg_catalog.pg_namespace namespace '+\
            'JOIN    pg_catalog.pg_proc procedures '+\
            'ON      procedures.pronamespace = namespace.oid '+\
            'WHERE   namespace.nspname = \'public\' and proname like \'tn_%\' '  
        dropStoredProcedureQuery = None
        storedProcedureName = None
        dropStoredProcedureMessageTemplate = 'Executed {storedprocedurequery}.'
        dropStoredProcedureMessage = None
        postgresConnection = DPH.PostGresDataBaseConnectionCursor(self.host,self.database,self.userName,self.password)

        postgresConnection.executeQuery(tnStoredProcedureListQuery)
        queryResults = postgresConnection.retrieveAllQueryResults()

        #Drop All Identified TN Stored Procedures
        for result in queryResults:
            storedProcedureName = result[0]
            dropStoredProcedureQuery = self.generateDropStoredProcedureQuery(storedProcedureName)
            postgresConnection.executeQuery(dropStoredProcedureQuery)
            dropStoredProcedureMessage = dropStoredProcedureMessageTemplate.format(storedprocedurequery=dropStoredProcedureQuery)
            self.logger.info(dropStoredProcedureMessage)

        postgresConnection.close()

    def main(self):

        #Parse and Validate Commandline Arguments
        self.parseCommandLineArguments()        
        self.validateCommandLineArguments()
        
        #Start Logging
        loggerMessage = '{inputmode} of Stored Procedures is starting for {inputdatabasename} within {inputhost}'.format(inputmode=self.mode,inputdatabasename=self.database,inputhost=self.host)
        self.logger.info(loggerMessage)

        #Load or Unload Telenav Stored procedures
        if self.mode == 'load':
            self.loadStoredProcedures()

        if self.mode == 'unload':
            self.unloadStoredProcedures()
            self.unloadCustomTypes()

        #End Logging
        loggerMessage = '{inputmode} of Stored Procedures is complete for {inputdatabasename} within {inputhost}\n'.format(inputmode=self.mode,inputdatabasename=self.database,inputhost=self.host)
        self.logger.info(loggerMessage)


if __name__ == "__main__":

    program = StoredProcedureLoaderAndUnloader()
    program.main()