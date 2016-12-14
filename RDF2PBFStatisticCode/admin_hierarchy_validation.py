import datetime as dt
import logging
import getopt
from multiprocessing import Pool
import os
import os.path
import psycopg2
import psycopg2.extras
import sys
import xml.etree.ElementTree as ET


def integrityCheck(args):
    """Executes Validation Test Queries"""
    host = args[0]
    uniDBDatabase = args[1]
    userName = args[2]
    password = args[3]
    bucketStart = args[4]
    bucketEnd = args[5]
    bucketType = args[6]
    xmlMapFeature = args[7]

    #Empty ErrorList and Results
    errorList = []
    results = None
    relationMemberResults = None

    #Connection
    uniDBConnection = psycopg2.connect('dbname={dbname} host = {host} user={user} password={password}'.format(host=host,\
        dbname=uniDBDatabase,\
        user=userName,\
        password=password))
    uniDBCursor = uniDBConnection.cursor()

    #Integrity Check
    validationQuery = generateValidationQuery(xmlMapFeature,bucketType,bucketStart,bucketEnd)
    uniDBCursor.execute(validationQuery)

    #Return Results To Query
    if bucketType == 'nodes' or bucketType=='ways':
        results = uniDBCursor.fetchall()
        errorList.append(results)
    if bucketType == 'relations':
        results = uniDBCursor.fetchall()

        #Loop to Check Relation Way Admin Hierarchy
        for i in range(0,len(results)):
            relationID = results[i][0]
            validationQuery = generateValidationQuery(xmlMapFeature,'relation_members',relationID,relationID)
            uniDBCursor.execute(validationQuery)
            relationMemberResults = [uniDBCursor.fetchall()]
            errorList.append(relationMemberResults)

    #Close DB Cursor and Connection
    uniDBCursor.close()
    uniDBConnection.close()
    #Return Results of Query
    if len(errorList) > 0:
        return errorList
    else:
        return None

def runParallelIntegrityChecks(jobList,numProcesses):
    """Returns Result of Parallel Processes"""
    p = Pool(numProcesses)
    poolResult = [p.apply_async(integrityCheck,args=(job,)) for job in jobList]
    poolResultOutput = [p.get() for p in poolResult]

    return poolResultOutput

def createJobList(dbHost,uniDBdbName,dbUser,dbPassword,bucket,bucketType,xmlMapFeature):
    """Returns an array of job arrays from an array of ID's"""
    jobList = []
    for i in range(0,len(bucket)):
        if i < (len(bucket)-1):
            jobList.append([dbHost,uniDBdbName,dbUser,dbPassword,bucket[i][0],bucket[i+1][0],bucketType,xmlMapFeature])
        else:
            jobList.append([dbHost,uniDBdbName,dbUser,dbPassword,bucket[i][0],bucket[i][0],bucketType,xmlMapFeature])
    return jobList

def getBucketList(dbHost,uniDBdbName,dbUser,dbPassword,bucketType,bucketSize,xmlMapFeature):
    """Gets the List of Buckets to Test, based on the bucket size (number of ID's per bucket)."""

    #Determine What Bucket Retrieval Query to Run
    qryRetrieveBuckets = generateBucketQuery(xmlMapFeature,bucketSize)

    #Open Database Connection
    uniDBConnection = psycopg2.connect('dbname={dbname} host = {host} user={user} password={password}'.format(host=dbHost,\
        dbname=uniDBdbName,\
        user=dbUser,\
        password=dbPassword))
    uniDBCursor = uniDBConnection.cursor()

    #Retrieve Bucket List
    uniDBCursor.execute(qryRetrieveBuckets)
    bucketList = uniDBCursor.fetchall()
    uniDBCursor.close()
    uniDBConnection.close()
    return bucketList

def generateValidationQuery(xmlMapFeature,bucketType,bucketStart,bucketEnd):
    """Generates a Validation Test Query with given parameters """

    #Query Templates
    ##Batch/Bucket Queries
    if bucketStart==bucketEnd:
        batchRelationQuery = 'select id from relations where (id between {bucketstart} and {bucketend}) and {wherelogic}'

        batchNodeQuery = 'select id, (tags ? \'link_id\') from nodes where (id between {bucketstart} and {bucketend}) and {wherelogic}'

        batchZipCenterNodeQuery = 'select id, '+\
            'tags->\'pc_admin:l1\' as l1, '+\
            'tags->\'pc_admin:l2\' as l2,'+\
            'tn_admin_relation_exist(cast(tags->\'pc_admin:l1\' as bigint)) as l1_exist, '+\
            'tn_admin_relation_exist(cast(tags->\'pc_admin:l2\' as bigint)) as l2_exist '+\
            'from nodes '+\
            'where (id between {bucketstart} and {bucketend}) and {wherelogic}'        

        batchWayQuery = 'select id, '+\
            'tags->\'l1:left\' as l1_left, '+\
            'tags->\'l1:right\' as l1_right, '+\
            'tags->\'l2:left\' as l2_left, '+\
            'tags->\'l2:right\' as l2_right, '+\
            'tn_admin_relation_exist(cast(tags->\'l1:left\' as bigint)) as l1_left_exist, '+\
            'tn_admin_relation_exist(cast(tags->\'l1:right\' as bigint)) as l1_right_exist, '+\
            'tn_admin_relation_exist(cast(tags->\'l2:left\' as bigint)) as l2_left_exist, '+\
            'tn_admin_relation_exist(cast(tags->\'l2:right\' as bigint)) as l2_right_exist '+\
            'from ways '+\
            'where (id between {bucketstart} and {bucketend}) and {wherelogic} '            
    else:
        batchRelationQuery = 'select id from relations where (id >= {bucketstart} and id < {bucketend}) and {wherelogic}'

        batchNodeQuery = 'select id, (tags ? \'link_id\') from nodes where (id >= {bucketstart} and id < {bucketend})  and {wherelogic}'

        batchZipCenterNodeQuery = 'select id, '+\
            'tags->\'pc_admin:l1\' as l1, '+\
            'tags->\'pc_admin:l2\' as l2,'+\
            'tn_admin_relation_exist(cast(tags->\'pc_admin:l1\' as bigint)) as l1_exist, '+\
            'tn_admin_relation_exist(cast(tags->\'pc_admin:l2\' as bigint)) as l2_exist '+\
            'from nodes '+\
            'where (id between {bucketstart} and {bucketend}) and {wherelogic}'        

        batchWayQuery = 'select id, '+\
            'tags->\'l1:left\' as l1_left, '+\
            'tags->\'l1:right\' as l1_right, '+\
            'tags->\'l2:left\' as l2_left, '+\
            'tags->\'l2:right\' as l2_right, '+\
            'tn_admin_relation_exist(cast(tags->\'l1:left\' as bigint)) as l1_left_exist, '+\
            'tn_admin_relation_exist(cast(tags->\'l1:right\' as bigint)) as l1_right_exist, '+\
            'tn_admin_relation_exist(cast(tags->\'l2:left\' as bigint)) as l2_left_exist, '+\
            'tn_admin_relation_exist(cast(tags->\'l2:right\' as bigint)) as l2_right_exist '+\
            'from ways '+\
            'where (id >= {bucketstart} and id < {bucketend}) and {wherelogic} '

    ##Single Relation-Way Member Query
    singleRelationQuery = 'select foo.*, '+\
        'tags->\'l1:left\' as l1_left, '+\
        'tags->\'l1:right\' as l1_right, ' +\
        'tags->\'l2:left\' as l2_left, ' +\
        'tags->\'l2:right\' as l2_right, '+\
        'tn_admin_relation_exist(cast(tags->\'l1:left\' as bigint)) as l1_left_exist, '+\
        'tn_admin_relation_exist(cast(tags->\'l1:right\' as bigint)) as l1_right_exist, '+\
        'tn_admin_relation_exist(cast(tags->\'l2:left\' as bigint)) as l2_left_exist, '+\
        'tn_admin_relation_exist(cast(tags->\'l2:right\' as bigint)) as l2_right_exist '+\
        'from (select relations.id,relation_members.member_id '+\
        'from relations '+\
        'left join relation_members '+\
        'on relations.id = relation_members.relation_id '+\
        'where id={bucketstart} and relation_members.member_type=\'W\') foo '+\
        'left join ways '+\
        'on foo.member_id  = ways.id '

    #Get Table Name and Where Logic For Batch Way Node Relation Queries
    if xmlMapFeature.tag.lower() == 'node' or xmlMapFeature.tag.lower() == 'way' or xmlMapFeature.tag.lower() == 'relation':
        tableName = xmlMapFeature.tag.lower()+'s'
        description = xmlMapFeature.attrib['description']
        whereLogic = getMapFeatureWhereLogic(xmlMapFeature)

    #Return Query Based on Selection
    if bucketType == 'nodes':
        if description=='Zip Center':
            return batchZipCenterNodeQuery.format(bucketstart=bucketStart,\
                bucketend=bucketEnd,\
                wherelogic=whereLogic)
        else:
            return batchNodeQuery.format(bucketstart=bucketStart,\
                bucketend=bucketEnd,\
                wherelogic=whereLogic)
    elif bucketType == 'ways':
        return batchWayQuery.format(bucketstart=bucketStart,\
            bucketend=bucketEnd,\
            wherelogic=whereLogic)
    elif bucketType == 'relations':
        return batchRelationQuery.format(bucketstart=bucketStart,\
            bucketend=bucketEnd,\
            wherelogic=whereLogic)
    elif bucketType == 'relation_members':
        return singleRelationQuery.format(bucketstart=bucketStart)
    else:
        return ''

def generateBucketQuery(xmlMapFeature,bucketSize):
    """Generates a query to Get Bucket List"""

    query = 'select id '+\
        'from (select id, row_number() over (order by id asc) as rownum from {tablename} where {wherelogic}) as foo '+\
        'where rownum %{bucketsize}=1 order by id'
    tableName = xmlMapFeature.tag.lower()+'s'
    whereLogic = getMapFeatureWhereLogic(xmlMapFeature)

    return query.format(tablename=tableName,\
        wherelogic=whereLogic,\
        bucketsize=bucketSize)

def getTagWhereLogic(key,value):
    """Generates the specific logic for a tag key-value pair"""
    if value == '*':
        return ' (tags ? \'{inputkey}\') '.format(inputkey=key)
    else:
        return ' tags->\'{inputkey}\'=\'{inputvalue}\' '.format(inputkey=key,inputvalue=value)

def getMapFeatureWhereLogic(xmlMapFeature):
    """Generates a WHERE SQL clause logic for a particular map feature represented as an XML element"""

    tagList = xmlMapFeature.getchildren()[0].getchildren()
    tagLogic = ''
    whereClause = ''
    keyword = ''

    for i in range(0,len(tagList)):
        tag = tagList[i]
        tagLogic = getTagWhereLogic(tag.attrib['key'],tag.attrib['value'])
        keyword = tag.attrib['logic'].upper()
        if len(tagList) == 1:
            return tagLogic
        else:
            if i == 0:
                whereClause = whereClause + '(' + tagLogic + keyword
            elif i == len(tagList) - 1:
                whereClause = whereClause + tagLogic + ')'
            else:
                whereClause = whereClause + tagLogic + keyword
    return whereClause

def getDatabaseAdapterDataVersion(databaseName):
    """Gets Adapter version and data version from UniDB Database Name"""

    if len(databaseName.split('.')) == 4:
        adapterVersion = databaseName.split('.')[3].split('-')[0]
        dataVersion = databaseName.split('.')[3].split('-')[1]
    #to handle non-normal databasename
    if len(databaseName.split('.')) == 5:
        adapterVersion = databaseName.split('.')[3]
        dataVersion = databaseName.split('.')[4].split('-')[1]

    return  (adapterVersion,dataVersion)

def generateIntegrityCheckReport(outputDirectory,checkResults,startTime,databaseName,dbHost,numProcesses,bucketSize,bucketType,testedMapFeatureDescription):
    """Processes Bucket Results to create a report"""

    #Parse Report Parameters
    adapterVersion = getDatabaseAdapterDataVersion(databaseName)[0]
    dataVersion = getDatabaseAdapterDataVersion(databaseName)[1]

    #Error List Message Template, Headers, and Error Message List
    relationMessageHeader = 'Relation ID | Error Message'
    relationMessageDivider = '-'*60
    relationMessageTemplate = 'Relation {id} | Missing Way With Admin. Hierarchy'

    wayMessageTemplate = 'Way {id} | {l1_left_missing} | {l1_right_missing} | {l2_left_missing} | {l2_right_missing} '
    wayMessageDivider = '-'*75

    nodeMessageTemplate = 'Node {id} | Is tags->\'link_id\' Missing '
    zipCenterNodeMessageTemplate = 'Node {id} | {isl1missing} | {isl2missing} '
    nodeMessageDivider = '-'*60
    errorMessages = []

    #Set Error Counts
    totalErrorCount = 0
    objectError = 0
    caseCounts = len(checkResults)

    #Create Detail Header
    if bucketType == 'nodes':
        if testedMapFeatureDescription == 'Zip Center':
            nodeResultHeader = zipCenterNodeMessageTemplate.format(id='ID',isl1missing= 'pc_admin:l1 missing',isl2missing='pc_admin:l2 missing')
            errorMessages.append(nodeResultHeader)
            errorMessages.append(nodeMessageDivider)
        else:
            nodeResultHeader = nodeMessageTemplate.format(id='ID')
            errorMessages.append(nodeResultHeader)
            errorMessages.append(nodeMessageDivider)
    elif bucketType == 'ways':
        #Create Way Result Header and Divider
        wayResultHeader = wayMessageTemplate.format(id='ID',\
            l1_left_missing='l1:left Missing',\
            l1_right_missing='l1:right Missing',\
            l2_left_missing='l2:left Missing',\
            l2_right_missing='l2:right Missing')
        errorMessages.append(wayResultHeader)
        errorMessages.append(wayMessageDivider)
    elif bucketType == 'relations':
        #Create Relation Error List Header and Divider
        errorMessages.append(relationMessageHeader)
        errorMessages.append(relationMessageDivider)
    else:
        return None

    #Analyze Results and Aggregate Error Messages to an Array
    ##Process Each Bucket of Results
    for bucketResult in checkResults:

        if bucketType == 'nodes' or bucketType == 'ways':

            bucketQueryResults = bucketResult[0]

            if len(bucketQueryResults) == 0:
                continue

            #Iterate Through Query Result
            for queryResult in bucketQueryResults:

                #Analyze Query Result Based on UniDB Object Type
                if bucketType == 'nodes':

                    #Analyse Node Validation Result
                    if testedMapFeatureDescription == 'Zip Center':
                        if (queryResult[1]==False) and (queryResult[1]==False):
                            errorMessages.append(zipCenterNodeMessageTemplate.format(id=queryResult[0],\
                                isl1missing= not queryResult[3],\
                                isl2missing=not queryResult[4]))
                            totalErrorCount += 1
                    else:
                        if not queryResult[1]:
                            errorMessages.append(nodeMessageTemplate.format(id=queryResult[0]))
                            totalErrorCount += 1

                if bucketType == 'ways':

                    #Reset Error Count and Parse Way ID
                    errorCount = 0
                    wayID = [0]

                    #Continue to Next Result if Current Result is Empty
                    if len(queryResult) == 0:
                        continue

                    #Analyse Way Validation Result
                    #Throw Error if an Admin Hierarchy Doesn't Exist
                    if (queryResult[5]==True or queryResult[6]==True) and (queryResult[7]==True or queryResult[8]==True):
                        pass
                    else:
                        errorMessages.append(wayMessageTemplate.format(id=queryResult[0],\
                            l1_left_missing=not queryResult[5],\
                            l1_right_missing=not queryResult[6],\
                            l2_left_missing=not queryResult[7],\
                            l2_right_missing=not queryResult[8]))
                        totalErrorCount += 1

        elif bucketType == 'relations':
            bucketQueryResults = bucketResult     

            for queryResult in bucketQueryResults:

                #Iterate Through Relation Results
                for relationResult in queryResult:

                    #Continue to Next Result if Current Result is Empty
                    if len(relationResult) == 0:
                        continue

                    #Reset Error Count and Parse Relation ID
                    objectPass = 0
                    objectError = 0
                    relationID = relationResult[0][0]

                    #Analyse Relation Way Member Validation Result
                    for wayResult in relationResult:
                        if (wayResult[6] or wayResult[7]) and (wayResult[8] or wayResult[9]):
                            objectPass +=1
                        else:
                            objectError += 1

                    #Report Error Message if there are Way Hierarchy Errors
                    if objectPass == 0:
                        errorMessages.append(relationMessageTemplate.format(id=relationID))
                        totalErrorCount += 1
        else:
            return None

    #Output Results to Output File
    #Open Output File
    reportTime = str(dt.datetime.now()).split(' ')
    outputFileName = 'Admin_Hierarchy_IntegrityCheck_' + testedMapFeatureDescription.replace(' ','_')  + '_' + reportTime[0].replace('-','') + '_' + reportTime[1].split('.')[0].split(':')[0]+ reportTime[1].split('.')[0].split(':')[1] + '.txt'
    outputFile = open(os.path.join(outputDirectory,outputFileName),'w')

    #Create Header
    outputFile.write('This is the Report was generated on {reportdate}\n'.format(reportdate=dt.datetime.now()))
    outputFile.write('Number of Processes used: {numprocess}\n'.format(numprocess=numProcesses))
    outputFile.write('Database Host Tested:     {dbhost}\n'.format(dbhost=dbHost))
    outputFile.write('Database Tested:          {dbname}\n'.format(dbname=databaseName))
    outputFile.write('Adapter Version Tested:   {adapterversion}\n'.format(adapterversion=adapterVersion))
    outputFile.write('Data Version Tested:      {dataversion}\n'.format(dataversion=dataVersion))
    outputFile.write('Analysis Time Cost:       {timecost}\n'.format(timecost=str(dt.datetime.now() - startTime)))
    outputFile.write('Analyzed Map Feature:     {mapfeature}\n'.format(mapfeature=str(testedMapFeatureDescription)))
    outputFile.write('Analysis Bucket Size:     {bucketsize}\n'.format(bucketsize=str(bucketSize)))
    outputFile.write('Analysis Bucket Type:     {buckettype}\n\n'.format(buckettype=str(bucketType)))
    outputFile.write('Error Count         :     {errorcount}\n\n'.format(errorcount=str(totalErrorCount)))

    #Write Error Details to Report
    for errorMessage in errorMessages:
        outputFile.write(errorMessage+"\n")

    #Close Output File
    outputFile.close()
    return os.path.join(outputDirectory,outputFileName)

def usage():
    print '\n\n'
    print 'Usage options for {scriptname}:'.format(scriptname=sys.argv[0])
    print '-h, --help                                           Shows help message and exits.\n'
    print '------Script Input Parameter Settings---------------------------\n'
    print '-n, --numprocess     [#]                             Indicates the number of parallel processes to analyze data model integrity.'
    print '-b, --bucketsize     [#]                             Indicates the number of unique ID\'s per bucket that represents a job list. Default value is 50,000.'
    print '-m, --mapfeatures    [filepath]                      Indicates the type filepath of the map_feature.xml file that lists the Map Features to Test\n'

    print '------Output Directory Settings---------------------------------\n'
    print '-o, --outputdir      [dirpath]                        Indicates the directory to save the results files. Default is current working directory.\n'

    print '------UniDB Connection Settings-----------------------------------\n'
    print '-c, --host           [Database host server]           Spefifies the database server hosting the UniDB database.'
    print '-d, --unidb          [UniDB database name]            Specifies UniDB database name. Overrides Database Parameter XML file.'
    print '-u, --user           [Database host user name]        Indicate database host user name.'
    print '-p, --password       [Database host user password]    Indicate the database host user\'s password.'

def main():
    """"Main Entry Point of Script"""
    #Local Variables
    numProcesses = None
    bucketSize = 50000
    mapFeatureListFilePath = None
    outputDirectory = os.getcwd()
    dbHost = None
    uniDBName = None
    userName = None
    password = None

    #Setup Test Logger
    logFilePath=os.path.join(outputDirectory,'admin_hierarchy_validation.log')
    logging.basicConfig(filename = logFilePath,filemode='a',format = '%(levelname)-10s %(asctime)s || %(message)s',level = logging.DEBUG)
    logger = logging.getLogger('unidb_validation')

    #Parse command line arguments
    try:
        options, remainder = getopt.getopt(sys.argv[1:],'hn:b:m:o:c:d:u:p:',['help','numprocess=','bucketsize=','mapfeatures=','outputdir=','host=','unidb=','user=','password='])
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
        if opt in ('-m','--mapfeatures'):
            mapFeatureListFilePath = arg
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

    if not os.path.isfile(mapFeatureListFilePath):
        beginTestMsg = "ERROR: Map Feature XML File Does Not Exist"
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

    #Validate Data and Generate Reports
    #Set Start Time
    startTime = dt.datetime.now()
    outputText = 'Starting validation at: {starttime}\n'.format(starttime=startTime)
    print outputText
    logger.info(outputText)

    reportCount = 0
    xmlMapFeatureDoc = ET.parse(mapFeatureListFilePath)
    xmlMapFeatureDocRoot = xmlMapFeatureDoc.getroot()
    xmlMapFeatureElements = xmlMapFeatureDocRoot.getchildren()

    for xmlMapFeatureElement in xmlMapFeatureElements:
        featureAnalysisStartTime = dt.datetime.now()
        outputText = 'Analyzing Admin. Hierarchy for {admindescription} at {starttime}.'.format(admindescription=xmlMapFeatureElement.attrib['description'],starttime=featureAnalysisStartTime)
        print outputText
        logger.info(outputText)

        #Get Bucket Type
        bucketType = (xmlMapFeatureElement.tag + 's').lower()

        #Get Bucket List (ID Ranges)
        bucketList = getBucketList(dbHost,uniDBName,userName,password,bucketType,bucketSize,xmlMapFeatureElement)

        #Create Job List
        mapFeatureJobList = createJobList(dbHost,uniDBName,userName,password,bucketList,bucketType,xmlMapFeatureElement)

        #Run Parallel Jobs
        integrityCheckResults = runParallelIntegrityChecks(mapFeatureJobList,numProcesses)

        #Generate Report
        outputFilePath = generateIntegrityCheckReport(outputDirectory,integrityCheckResults,featureAnalysisStartTime,uniDBName,dbHost,numProcesses,bucketSize,bucketType,xmlMapFeatureElement.attrib['description'])
        reportCount += 1
        outputText = 'Generated report to: {filepath}'.format(filepath=outputFilePath)
        print outputText
        logger.info(outputText)

    #End Test
    endTime = dt.datetime.now()
    timeCost = endTime - startTime
    outputText =  '{num} Integrity Check Reports Generated to: {outputdirectory}\n'.format(num=reportCount,outputdirectory=outputDirectory)
    logger.info(outputText)
    print outputText
    outputText = 'Validation with Parallel Processes: {number}. Time cost: {timecost}. Test End Time: {endtime}.\n'.format(number=numProcesses,timecost=timeCost,endtime=endTime)
    logger.info(outputText)
    print outputText

if __name__ == "__main__":
    main()

