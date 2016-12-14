import error_model as em 
import osm_object_model as osm
import datetime as dt
import getopt
import logging
from multiprocessing import Pool
import os
import os.path
import psycopg2
import psycopg2.extras
import sys

#Integrity Check Functions
def processWayBucketResult(bucketQueryResults):
    errorResults = em.WayErrorResults()

    for missingNodeResult in bucketQueryResults:

        wayIdentifier = missingNodeResult[0]
        nodeIdentifier = missingNodeResult[1]
            
        #Create a new relation member
        missingNode = osm.Node(nodeIdentifier)

        #Add Relation Member
        errorResults.addMissingNode(wayIdentifier,missingNode)

    return errorResults
    
def processRelationBucketResult(bucketQueryResults):
    errorResults = em.RelationErrorResults()

    for missingRelationMemberResult in bucketQueryResults:

        relationIdentifer = missingRelationMemberResult[0]
        memberIdentifier = missingRelationMemberResult[1]
        memberType = missingRelationMemberResult[2]
        memberRole = missingRelationMemberResult[3]
        expectedIdentifer = missingRelationMemberResult[4]
        strRelationIdentifier = str(relationIdentifer)

        #If the expected identifer was not found
        if expectedIdentifer == None:
            
            #Create a new relation member
            missingRelationMember = osm.RelationMember(memberIdentifier,memberType,memberRole)

            #If Relation Member Identifier Does not have a -1L value
            if memberIdentifier != -1L and strRelationIdentifier[-3:] != '013':

                errorResults.addMissingRelationMember(relationIdentifer,missingRelationMember)

    return errorResults

def integritycheck(args):
    """Returns an array of tuples"""
    ##Relations Test Queries
    qryRelationMemberTest_Relations = 'select relation_id, member_id, member_type, member_role,relations.id '+\
        'from relation_members '+\
        'left join relations '+\
        'on relation_members.member_id = relations.id '+\
        'where member_type = \'R\' and (relation_id >={bucketStart} and relation_id < {bucketEnd}) and relations.id is null and (member_role != \'POI\' and member_role != \'CF\') '

    qryRelationMemberTest_Ways = 'select relation_id, member_id, member_type, member_role,ways.id '+\
        'from relation_members '+\
        'left join ways '+\
        'on relation_members.member_id = ways.id '+\
        'where member_type = \'W\' and (relation_id >={bucketStart} and relation_id < {bucketEnd}) and ways.id is null and (member_role != \'POI\' and member_role != \'CF\' )'

    qryRelationMemberTest_Nodes = 'select relation_id, member_id, member_type, member_role,nodes.id '+\
        'from relation_members '+\
        'left join nodes '+\
        'on relation_members.member_id = nodes.id '+\
        'where member_type = \'N\' and (relation_id >={bucketStart} and relation_id < {bucketEnd}) and nodes.id is null and (member_role != \'POI\' and member_role != \'CF\') '

    ##Ways Test Query
    qryWayNodesTest = 'select a.*,nodes.id as nodes_id '+\
        'from (select id as wayid, unnest(nodes) as nodeid from ways where id >={bucketStart} and id < {bucketEnd}) as a '+\
        'left join nodes on a.nodeid=nodes.id '+\
        'where nodes.id is null'

    #Process Input Arguments
    host = args[0]
    uniDBDatabase = args[1]
    userName = args[2]
    password = args[3]
    bucketStart = args[4]
    bucketEnd = args[5]
    bucketType = args[6]
    results = None
    processedErrors = None

    #Empty ErrorList
    errorList = []
    integrityCheckRelationErrors = em.RelationErrorResults()
    integrityCheckWayErrors = None

    #Connection
    uniDBConnection = psycopg2.connect('dbname={dbname} host = {host} user={user} password={password}'.format(host=host,\
        dbname=uniDBDatabase,\
        user=userName,\
        password=password))
    uniDBCursor = uniDBConnection.cursor()

    #Process Data
    if bucketType == 'relations':

        uniDBCursor.execute(qryRelationMemberTest_Relations.format(bucketStart=bucketStart,bucketEnd=bucketEnd))
        results = uniDBCursor.fetchall()

        if len(results)>0:
            errorList.append(results)
            processedErrors = processRelationBucketResult(results)
            integrityCheckRelationErrors.add(processedErrors)

        uniDBCursor.execute(qryRelationMemberTest_Ways.format(bucketStart=bucketStart,bucketEnd=bucketEnd))
        results = uniDBCursor.fetchall()

        if len(results)>0:
            errorList.append(results)
            processedErrors = processRelationBucketResult(results)
            integrityCheckRelationErrors.add(processedErrors)

        uniDBCursor.execute(qryRelationMemberTest_Nodes.format(bucketStart=bucketStart,bucketEnd=bucketEnd))
        results = uniDBCursor.fetchall()

        if len(results)>0:
            errorList.append(results)
            processedErrors = processRelationBucketResult(results)
            integrityCheckRelationErrors.add(processedErrors)

    if bucketType == 'ways':
        uniDBCursor.execute(qryWayNodesTest.format(bucketStart=bucketStart,bucketEnd=bucketEnd))

        results = uniDBCursor.fetchall()
        integrityCheckWayErrors = processWayBucketResult(results)

    #Close Database Connection and Errors
    uniDBCursor.close()
    uniDBConnection.close()

    #Return Errors
    if bucketType == 'relations':
        return integrityCheckRelationErrors

    if bucketType == 'ways':
        return integrityCheckWayErrors
    
def getBucketList(dbHost,uniDBdbName,dbUser,dbPassword,bucketType,bucketSize):
    """Gets the List of Buckets to Test, based on the bucket size (number of ID's per bucket)."""

    #Determine What Bucket Retrieval Query to Run
    qryRetrieveBuckets = 'select id '+\
        'from (select id, row_number() over (order by id asc) as rownum from {buckettype}) as foo '+\
        'where rownum % {bucketsize}=1 order by id '

    #Open Database Connection
    uniDBConnection = psycopg2.connect('dbname={dbname} host = {host} user={user} password={password}'.format(host=dbHost,\
        dbname=uniDBdbName,\
        user=dbUser,\
        password=dbPassword))

    uniDBCursor = uniDBConnection.cursor()

    #Retrieve Bucket List
    uniDBCursor.execute(qryRetrieveBuckets.format(buckettype=bucketType,bucketsize=bucketSize))
    bucketList = uniDBCursor.fetchall()

    uniDBCursor.close()
    uniDBConnection.close()

    return bucketList

def createJobList(dbHost,uniDBdbName,dbUser,dbPassword,bucket,bucketType):
    """Returns an array of job arrays from an array of ID's"""
    jobList = []
    for i in range(0,len(bucket)):
        if i < (len(bucket)-1):

            low = bucket[i][0]
            high = bucket[i+1][0]
        else:

            low = bucket[i][0]
            high = bucket[i][0]

        jobList.append([dbHost,uniDBdbName,dbUser,dbPassword,low,high,bucketType])
    return jobList

def runParallelIntegrityChecks(jobList,numProcesses):
    """Returns Result of Parallel Processes"""
    p = Pool(numProcesses)
    poolResult = [p.apply_async(integritycheck,args=(job,)) for job in jobList]
    poolResultOutput = [p.get() for p in poolResult]

    return poolResultOutput

def getDatabaseAdapterDataVersion(databaseName):
    """Gets Adapter version and data version from UniDB Database Name"""
    adapterVersion = ''
    dataVersion = ''

    if len(databaseName.split('.')) == 4:
        adapterVersion = databaseName.split('.')[3].split('-')[0]
        dataVersion = databaseName.split('.')[3].split('-')[1]
    #to handle non-normal databasename
    if len(databaseName.split('.')) == 5:
        adapterVersion = databaseName.split('.')[3]
        dataVersion = databaseName.split('.')[4].split('-')[1]

    return  (adapterVersion,dataVersion)

def generateDataModelIntegrityCheckReport(outputDirectory,results,timeCost,databaseName,dbHost,numProcesses,bucketSize,bucketType):
    """Processes Bucket Results to create a report"""

    #Open Output File
    reportTime = str(dt.datetime.now()).split(' ')
    outputFileName = 'Data_Model_IntegrityCheck_' + reportTime[0].replace('-','') + '_' + reportTime[1].split('.')[0].split(':')[0]+ reportTime[1].split('.')[0].split(':')[1] + '.txt'
    outputFile = open(os.path.join(outputDirectory,outputFileName),'w')

    #Set column Width
    columnWidth = 20

    #Parse Report Parameters
    adapterVersion = getDatabaseAdapterDataVersion(databaseName)[0]
    dataVersion = getDatabaseAdapterDataVersion(databaseName)[1]

    #Create Header
    outputFile.write('This is the Report was generated on {reportdate}\n'.format(reportdate=dt.datetime.now()))
    outputFile.write('Number of Processes used: {numprocess}\n'.format(numprocess=numProcesses))
    outputFile.write('Database Host Tested:     {dbhost}\n'.format(dbhost=dbHost))
    outputFile.write('Database Tested:          {dbname}\n'.format(dbname=databaseName))
    outputFile.write('Adapter Version Tested:   {adapterversion}\n'.format(adapterversion=adapterVersion))
    outputFile.write('Data Version Tested:      {dataversion}\n'.format(dataversion=dataVersion))
    outputFile.write('Analysis Time Cost:       {timecost}\n'.format(timecost=str(timeCost)))
    outputFile.write('Analysis Bucket Size:     {bucketsize}\n'.format(bucketsize=str(bucketSize)))
    outputFile.write('Analysis Bucket Type:     {buckettype}\n\n'.format(buckettype=str(bucketType)))

    #Process Results
    if bucketType=='relations':

        #For each bucketErrorResult
        for bucketErrorResult in results:

            #If the number of errors for the bucket
            if bucketErrorResult.size() > 0:

                #Process Each Relation
                for errorRelation in bucketErrorResult.getRelationsWithMissingMembers():

                    #Output Relation Section Header
                    outputFile.write('\n\nRelation Members for Relation ID {relationid} are missing:\n\n'.format(relationid=errorRelation.getRelationIdentifer()))
                    outputFile.write(('-'*60)+'\n')

                    #Output Column Headers
                    outputFile.write('{memberid}| {membertype}| {memberrole}\n'.format(\
                        memberid='Member ID'.ljust(columnWidth,' '),\
                        membertype='Member Type'.center(columnWidth,' '),\
                        memberrole='Member Role'.center(columnWidth,' ')))
                    outputFile.write(('-'*60)+'\n')

                    #Process Each Relation Member
                    for errorRelationMember in errorRelation.getRelationMembers():  

                        #Output Relation Error Row
                        memberIdentifier = str(errorRelationMember.getMemberIdentifer())
                        memberType = str(errorRelationMember.getMemberType())
                        memberRole = str(errorRelationMember.getMemberRole())
                        outputFile.write('{memberid} {membertype} {memberrole}\n'.format(\
                            memberid=memberIdentifier.ljust(columnWidth,' '),\
                            membertype=memberType.center(columnWidth,' '),\
                            memberrole=memberRole.center(columnWidth,' ')))
    
    if bucketType=='ways':

        #Output subheader for ways
        outputFile.write('Missing Way Nodes List:\n\n')
        outputFile.write('{wayid} | {nodeid} \n'.format(wayid='Way ID'.ljust(columnWidth,' '),nodeid='Node ID'.ljust(columnWidth,' ')))

        #For each bucketErrorResult
        for bucketErrorResult in results:

            if bucketErrorResult.size() > 0:
                for errorWay in bucketErrorResult.getWaysWithMissingNodes():

                    wayIdentifier = str(errorWay.getWayIdentifier())

                    #Output Missing Way-Node Pairs
                    for missingNode in errorWay.getNodes():

                        nodeIdentifier = str(missingNode.getNodeIdentifier())
                        outputFile.write('{wayid} | {nodeid} \n'.format(wayid=wayIdentifier.ljust(columnWidth,' '),nodeid=nodeIdentifier.ljust(columnWidth,' ')))

    outputFile.close()

    return outputFileName

def usage():
    print '\n\n'
    print 'Usage options for validation_script.py:'
    print '-h, --help                                       Shows help message and exits.\n'
    print '------Script Input Parameter Settings---------------------------\n'
    print '-n, --numprocess [#]                             Indicates the number of parallel processes to analyze data model integrity.'
    print '-b, --bucketsize [#]                             Indicates the number of unique ID\'s per bucket that represents a job list. Default value is 50,000.'
    print '-t, --typeobject [ways/relations]                Indicates the type (\'ways\' or \'relations\') of object to run integrity check against.'

    print '------Output Directory Settings---------------------------------\n'
    print '-o, --outputdir [dirpath]                        Indicates the directory to save the results files. Default is current working directory.\n'

    print '------UniDB Connection Settings-----------------------------------\n'
    print '-c, --host      [Database host server]           Spefifies the database server hosting the UniDB database.'
    print '-d, --osmdb     [UniDB database name]            Specifies UniDB database name. Overrides Database Parameter XML file.'
    print '-u, --user      [Database host user name]        Indicate database host user name.'
    print '-p, --password  [Database host user password]    Indicate the database host user\'s password.'

def main():
    """"Main Entry Point of Script"""
    #Local Variables
    numProcesses = None
    bucketSize = 50000
    objectType = None
    outputDirectory = os.getcwd()
    dbHost = None
    uniDBName = None
    userName = None
    password = None

    #Setup Test Logger
    logFilePath=os.path.join(outputDirectory,'validation_data_model.log')
    logging.basicConfig(filename = logFilePath,filemode='a',format = '%(levelname)-10s %(asctime)s || %(message)s',level = logging.DEBUG)
    logger = logging.getLogger('unidb_validation')

    #Parse command line arguments
    try:
        options, remainder = getopt.getopt(sys.argv[1:],'hn:b:t:o:c:d:u:p:',['help','numprocess=','bucketsize=','typeobject=','outputdir=','host=','unidb=','user=','password='])
    except getopt.GetoptError as err:
            print '\nERROR:\n\t',err,'\n'
            usage()
            sys.exit(2)

    #Process command line argument options
    for opt,arg in options:
        if opt in ('-h','--help'):
            usage()
            sys.exit(0)
        if opt in ('-n','--numprocess'):
            try:
                numProcesses = int(arg)
            except Exception, e:
                beginTestMsg = 'ERROR -n (--numprocess) argument not an integer'
                print beginTestMsg
                logger.error(beginTestMsg)
                sys.exit(1)
        if opt in ('-b','--bucketsize'):
            try:
                bucketSize = int(arg)
            except Exception, e:
                beginTestMsg = 'ERROR -b (--bucketsize) argument not an integer'
                print beginTestMsg
                logger.error(beginTestMsg)
                sys.exit(1)
        if opt in ('-t','--typeobject'):
            objectType = arg
        if opt in ('-o','--outputdir'):
            outputDirectory = os.path.join(arg)
        if opt in ('-c', '--host'):
            dbHost = arg
        if opt in ('-d', '--osmdb'):
            uniDBName = arg
        if opt in ('-u','--user'):
            userName = arg
        if opt in ('-p','--password'):
            password = arg

    #Validate commandline arguments
    if numProcesses <= 2:
        beginTestMsg = "ERROR: Number of parallel processes must be greater than 2. Halting test."
        print beginTestMsg
        logger.error(beginTestMsg)
        sys.exit(1)

    if objectType == None:
        beginTestMsg = "ERROR: Object Type not specified. Halting test."
        print beginTestMsg
        logger.error(beginTestMsg)
        sys.exit(1)
    else:
        if not (objectType == 'ways' or objectType == 'relations'):
            beginTestMsg = "ERROR: Object Type invalid. Halting test."
            print beginTestMsg
            logger.error(beginTestMsg)
            sys.exit(1)

    if not os.path.exists(outputDirectory) or outputDirectory == None:
        beginTestMsg = "ERROR: Output directory not specified or invalid. Halting test."
        print beginTestMsg
        logger.error(beginTestMsg)
        sys.exit(1)

    if dbHost == None:
        beginTestMsg = "ERROR: Database host not specified. Halting test."
        print beginTestMsg
        logger.error(beginTestMsg)
        sys.exit(1)

    if uniDBName == None:
        beginTestMsg = "ERROR: UniDB database name not specified. Halting test."
        print beginTestMsg
        logger.error(beginTestMsg)
        sys.exit(1)

    if userName == None:
        beginTestMsg = "ERROR: database user name not specified. Halting test."
        print beginTestMsg
        logger.error(beginTestMsg)
        sys.exit(1)

    if password == None:
        beginTestMsg = "ERROR: database user password not specified. Halting test."
        print beginTestMsg
        logger.error(beginTestMsg)
        sys.exit(1)

    try:
        dbSourceConnectionValidate = psycopg2.connect('dbname={dbname} host = {host} user={user} password={password}'.format(host=dbHost,dbname=uniDBName,user=userName,password=password))
        dbSourceConnectionValidate.close()
    except Exception, e:
        beginTestMsg = "ERROR: Connection parameters to uniDB database invalid. Halting test."
        print beginTestMsg
        logger.error(beginTestMsg)
        sys.exit(1)

    #Retrieve Bucket List
    startTime = dt.datetime.now()
    bucketList = getBucketList(dbHost,uniDBName,userName,password,objectType,bucketSize)

    #Create Job List
    jobList = createJobList(dbHost,uniDBName,userName,password,bucketList,objectType)

    #Analyze Buckets
    outputText = 'Validation with {number} parallel processes. Buckets of {bucketsize} IDs. Starting at {time}.\n'.format(number=numProcesses,bucketsize=bucketSize,time=startTime)
    print outputText
    logger.info(outputText)
    results = runParallelIntegrityChecks(jobList,numProcesses)

    #End Timing
    endTime = dt.datetime.now()
    timeCost = endTime - startTime

    #Generate Results
    outputFileName = generateDataModelIntegrityCheckReport(outputDirectory,results,timeCost,uniDBName,dbHost,numProcesses,bucketSize,objectType)
    outputText =  'Integrity Check Results Generated to: {outputfilename}\n'.format(outputfilename=os.path.join(outputDirectory,outputFileName))
    logger.info(outputText)
    print outputText
    outputText = 'Validation with Parallel Processes: {number}. Time cost: {timecost}. Test End Time: {endtime}.\n'.format(number=5,timecost=timeCost,endtime=endTime)
    logger.info(outputText)
    print outputText

    return 0

if __name__ == "__main__":
    main()
