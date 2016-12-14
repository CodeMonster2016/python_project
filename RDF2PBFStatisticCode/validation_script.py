import csv
import datetime as dt
import getopt
import logging
import os
import os.path
import psycopg2
import psycopg2.extras
import sys
import vreport
import xml2TestCaseAdapter as XML2TC
import StoredProcCSV2TestCaseAdapter as SPCSV2TC
import xml.etree.ElementTree as ET

#FUNCTION DECLARATION
def getUniDBTestCasesFromXML(inputXMLTestCasesParametersFile,dbConnection,dbCursor):
    """Processes XML File of Test Cases and returns a list of UniDBTest Case Objects, with DB Connections and Cursors, to validate"""

    #Get Case list
    testCaseListXML = XML2TC.xml2TestCaseAdapter.getTestCasesFromXMLFile(inputXMLTestCasesParametersFile)
    testCaseList = XML2TC.xml2TestCaseAdapter.createTestCaseListFromXML(testCaseListXML)

    #Set UniDB Connection & Cursor (pointers) for each case
    for i in range(0,len(testCaseList)):
        case = testCaseList[i]
        case.setUniDBConnection(dbConnection)
        case.setUniDBCursor(dbCursor)

        #Set Relation Member UniDB Connection & Cursor (pointers) for each relation member
        if case.getObjectType() == 'relation_members':
            for j in range(0,len(case.getMemberList())):
                relationMember = case.getRelationMember(j)
                relationMember.setUniDBConnection(dbConnection)
                relationMember.setUniDBCursor(dbCursor)

    return testCaseList

def getUniDBTestCasesFromDB(dbSourceDataProcessor,rawListCasesFromDB,testNumber,validationTestMappingRules,dbConnection,dbCursor):
    """Processes Test Cases from DB and returns a list of UniDBTest Case Objects, with DB Connections and Cursors, to validate"""
    testCaseList = dbSourceDataProcessor.createTestCaseListFromSourceDB(rawListCasesFromDB,testNumber,validationTestMappingRules)

    #Set UniDB Connection & Cursor (pointers) for each case
    for i in range(0,len(testCaseList)):
        case = testCaseList[i]
        case.setUniDBConnection(dbConnection)
        case.setUniDBCursor(dbCursor)

    return testCaseList

def validateTestCaseList(testCaseList,validationTestName,outputDirectory,testLogger):
    """
    Validates a list of UniDBTest Cases and Generates an detailed report of the results.
    """
    #Local Variables
    logger = testLogger
    uniDBValidationTestName = validationTestName
    resultsarray = []
    validationTestStatistics = vreport.TestStatistics(uniDBValidationTestName) #Create Validation Test Class to Log Results

    #Run Test Outlined in inputXMLTestCasesParametersFile
    beginTestMsg = u'Beginning Test \'{testname}\' '.format(testname=uniDBValidationTestName)

    #Iterate and evaluate each test case
    for i in xrange(0,len(testCaseList)):

        #Create Instance of Validation Test Case
        testCase = testCaseList[i]
        sourceIDName = testCase.getSourceDataIDName()
        sourceID = testCase.getSourceDataID()
        uniDBID = testCase.getUniDBID()

        resultsarray.append(u'\nExecuting Check For {nt_id_name}={input_rdf_id}\n'.format(nt_id_name=sourceIDName,input_rdf_id=sourceID))
        resultsarray.append(u'Failure/Success--------------------|Actual Result----------------------|Expected Result---------------------\n')

        #Check if Target Map Feature Exists in uniDB
        if testCase.isUniDBObjectExist():
            failure_success = u'SUCCESS'
            actual_result = u'id = ' + str(uniDBID)
            expected_result = u'id = ' + str(uniDBID)
            resultsarray.append(u'{failure_success} {actual_result} {expected_result}'.format(failure_success=failure_success+(u' '*(36-len(failure_success))),actual_result=actual_result+(u' '*(36-len(actual_result))),expected_result=expected_result+(u' '*(36-len(expected_result))))+u'\n')
            testCase.setPassedTest()

            #Validate Tags
            for key in sorted(testCase.getExpectedTags()):
                expected_osm_tag = key
                expected_osm_tag_value = testCase.getExpectedTags()[key].rstrip() #Trailing spaces are trimed from string
                actual_osm_tag_value = testCase.getActualTagValue(expected_osm_tag)
                expected_result = u'tags->{osm_tag}={value}'.format(osm_tag=expected_osm_tag,value=expected_osm_tag_value)
                actual_result = u'tags->{osm_tag}={value}'.format(osm_tag=expected_osm_tag,value=actual_osm_tag_value)

                #Compare Expected and Actual Tag Values of Test Case
                if expected_osm_tag_value == actual_osm_tag_value:
                    failure_success = u'SUCCESS'
                    loggerMsg = None
                    testCase.setPassedTest()
                else:
                    if not testCase.isExpectedTagValuePairExist(expected_osm_tag):
                        failure_success = u'FAILURE'
                        loggerMsg = u'Expected Result of {expected_result} not found for {source_id_name}={source_id}'.format(\
                            expected_result=expected_result,\
                            source_id_name=sourceIDName,\
                            source_id=sourceID)
                        testCase.setFailedRule()
                    else:
                        failure_success = u'FAILURE'
                        loggerMsg = u'Other type of failure when validating: {expected_result} for {source_id_name} = {source_id}'.format(\
                            expected_result=expected_result,\
                            source_id_name=sourceIDName,\
                            source_id=sourceID)
                        testCase.setOtherFail()

                tagTestResultMsg = u'{failure_success} {actual_result} {expected_result}'.format(
                        failure_success=failure_success+(' '*(36-len(failure_success))),\
                        actual_result=actual_result+(' '*(36-len(actual_result))),\
                        expected_result=expected_result+(' '*(36-len(expected_result))))+'\n'

                #Write Failure Message to Test Results File (via resultsarray)
                if failure_success == u'FAILURE':
                    resultsarray.append(tagTestResultMsg)
                    logger.error(loggerMsg)

                #Write Success Message to Test Results File (via resultsarray)
                if failure_success == u'SUCCESS':
                    resultsarray.append(tagTestResultMsg)

            #Validate Relation Members (if applicable)
            if testCase.getObjectType() == 'relation_members':
                for j in xrange(0,len(testCase.getMemberList())):
                    relationMember = testCase.getRelationMember(j)
                    expected_relation_id_result = 'relation_id = {relation_id}'.format(relation_id=relationMember.getRelationID())
                    expected_member_id_result = 'member_id = {member_id}'.format(member_id=relationMember.getMemberID())
                    expected_member_type_result = 'member_type = {member_type}'.format(member_type=relationMember.getMemberType())
                    expected_member_role_result = 'member_role = {member_role}'.format(member_role=relationMember.getMemberRole())
                    expected_sequence_id_result = 'sequence_id = {sequence_id}'.format(sequence_id=relationMember.getSequenceID())

                    #Test to Determine if expected relation member exists
                    if relationMember.isMemberExist():
                        failure_success = 'SUCCESS'
                        actual_relation_id_result = expected_relation_id_result
                        actual_member_id_result = expected_member_id_result
                        actual_member_type_result = expected_member_type_result
                        actual_member_role_result = expected_member_role_result
                        actual_sequence_id_result = expected_sequence_id_result
                        testCase.setPassedTest()

                    else:
                        failure_success = 'FAILURE'
                        actual_relation_id_result = 'relation_id = {relation_id}'.format(relation_id=None)
                        actual_member_id_result = 'member_id = {member_id}'.format(member_id=None)
                        actual_member_type_result = 'member_type = {member_type}'.format(member_type=None)
                        actual_member_role_result = 'member_role = {member_role}'.format(member_role=None)
                        actual_sequence_id_result = 'sequence_id = {sequence_id}'.format(sequence_id=None)
                        loggerMsg = 'The relation member with the following properties did not exist: '+\
                            '{expected_relation_id_result};{expected_member_id_result};'.format(expected_relation_id_result=expected_relation_id_result,expected_member_id_result=expected_member_id_result)+\
                            '{expected_member_type_result};{expected_member_role_result};'.format(expected_member_type_result=expected_member_type_result,expected_member_role_result=expected_member_role_result)+\
                            '{expected_sequence_id_result}'.format(expected_sequence_id_result=expected_sequence_id_result)
                        logger.error(loggerMsg)
                        testCase.setOtherFail()

                    #Aggregate member test expected & actual results into array
                    expectedMemberRoleResults = [unicode(expected_relation_id_result),unicode(expected_member_id_result),\
                        unicode(expected_member_type_result),unicode(expected_member_role_result),\
                        unicode(expected_sequence_id_result)]

                    actualMemberRoleResults = [unicode(actual_relation_id_result),unicode(actual_member_id_result),\
                        unicode(actual_member_type_result),unicode(actual_member_role_result),
                        unicode(actual_sequence_id_result)]

                    #Write Member Test Results
                    for k in xrange(0,5):
                        actual_result = actualMemberRoleResults[k]
                        expected_result = expectedMemberRoleResults[k]
                        relationMemberTestResultMsg = u'{failure_success} {actual_result} {expected_result}'.format(
                            failure_success=failure_success+(u' '*(36-len(failure_success))),\
                            actual_result=actual_result+(u' '*(36-len(actual_result))),\
                            expected_result=expected_result+(u' '*(36-len(expected_result))))+'\n'
                        resultsarray.append(relationMemberTestResultMsg)

        else:
            #Procedure if Target Map Feature Does Not Exist in uniDB
            failure_success = u'FAILURE'
            actual_result = u'id = ' + str(None)
            expected_result = u'id = ' + str(uniDBID)
            resultsarray.append(u'{failure_success} {actual_result} {expected_result}'.format(failure_success=failure_success+(u' '*(36-len(failure_success))),actual_result=actual_result+(u' '*(36-len(actual_result))),expected_result=expected_result+(u' '*(36-len(expected_result))))+u'\n')
            loggerMsg = u'Expected id = {input_osm_id} not found in {osm_table} table'.format(input_osm_id=str(uniDBID),osm_table=testCase.getObjectType())
            logger.error(loggerMsg)
            testCase.setNotFound()

        #Process Test Case
        validationTestStatistics.processTestCase(testCase)

    #Write Results of the test to the file.
    resultsarray.append(u'\n\n\n')
    time = unicode(dt.datetime.now()).split(' ')
    outputfileName =  os.path.join(outputDirectory,uniDBValidationTestName + time[0].replace(u'-',u'') + u'_' + time[1].split(u'.')[0].split(u':')[0]+ time[1].split(u'.')[0].split(u':')[1] + u'.txt')
    output_results = open(outputfileName,'w')
    for l in xrange(0,len(resultsarray)):
        output_results.write(resultsarray[l].encode("UTF-8"))
    output_results.close()

    #Last Logger For Test
    loggerMsg = u'Results for the \'{validation_test_name}\' test written to {outputfileName}'.format(validation_test_name=uniDBValidationTestName,outputfileName=outputfileName)
    print loggerMsg
    logger.info(loggerMsg)

    return validationTestStatistics

def processValidationTests(scriptLogger,inputDirectory,outputDirectory,caseSourceType,dbHost,uniDBdbName,sourceDBName,dbuser,dbPassword,inputTestParameterSettingsXML):
    """
        Processes test case lists from XML and automated (stored procedure+csv) sources.
        Also adds DB Connnection & Cursor information
    """

    #Local Variables
    arrayXMLTestCaseFiles = []
    logger = scriptLogger

    # #Setup Test Logger
    # logFileName='validation_script_nt2osm.log'
    # logging.basicConfig(filename = logFileName,filemode='a',format = '%(levelname)-10s %(asctime)s || %(message)s',level = logging.DEBUG)
    # logger = logging.getLogger('nt2osm_validation')
    # beginTestMsg = 'Beginning Validation Test'
    # print beginTestMsg
    # logger.info(beginTestMsg)

    #Create Validation Test Report Instance
    validationReport = vreport.ValidationReportClass(outputDirectory)

    #Database Connection Parameters
    host = dbHost
    uniDBDatabase = uniDBdbName
    sourceDatabase = sourceDBName
    username = dbuser
    password = dbPassword

    #Setup uniDB Database Connection and Cursor
    uni_dbConnection = psycopg2.connect('dbname={dbname} host = {host} user={user} password={password}'.format(host=host,dbname=uniDBDatabase,user=username,password=password))
    uni_dbCursor = uni_dbConnection.cursor()

    #Setup source Database Connection and Cursor
    source_dbConnection = psycopg2.connect('dbname={dbname} host = {host} user={user} password={password}'.format(host=host,dbname=sourceDatabase,user=username,password=password))
    source_dbCursor = source_dbConnection.cursor()

    #Set Test Begin Time
    validationReport.setStartTime()

    #Validate All XML Case Lists
    if caseSourceType == 'xml' or caseSourceType == 'both':

    #Get List of XML Files from Input Directory.
        for root,dirs,files in os.walk(inputDirectory):
            for file in files:
                fileExtension = file.split('.')[1]
                xmlFileType = file.split('.')[0].split('_')[0]
                if fileExtension== 'xml' and xmlFileType == 'xmllist':
                    arrayXMLTestCaseFiles.append(os.path.join(inputDirectory,file))

        for i in range(0,len(arrayXMLTestCaseFiles)):
            xmlFileName = arrayXMLTestCaseFiles[i]

            #Identify Test Name and Test Case List
            uniDBValidationTestName = XML2TC.xml2TestCaseAdapter.getTestNameFromXMLFile(xmlFileName)
            testCaseList = getUniDBTestCasesFromXML(xmlFileName,uni_dbConnection,uni_dbCursor)

            #Validate Test Case List
            testResults = validateTestCaseList(testCaseList,uniDBValidationTestName,outputDirectory,logger)

            #Add Validation Test Result to Validation Report
            validationReport.addTest(testResults)

    #Validate All CSV Case Lists
    if caseSourceType == 'db' or caseSourceType == 'both':
        csvDBCaseProcessor = SPCSV2TC.StoredProcCSV2TestCaseAdapter(inputTestParameterSettingsXML,inputDirectory,source_dbConnection,source_dbCursor)
        numberCSVTests = csvDBCaseProcessor.getNumberValidationTests()

        for i in range(0,numberCSVTests):
            #Identify Test Name and Test Case List
            uniDBValidationTestName = csvDBCaseProcessor.getTestNameFromTestParametersXMLFile(i)
            rawListCasesFromDB = csvDBCaseProcessor.getCasesFromSourceDB(i)
            validationTestMappingRules = csvDBCaseProcessor.getMappingRulesFromCSV(i)
            testCaseList = getUniDBTestCasesFromDB(csvDBCaseProcessor,rawListCasesFromDB,i,validationTestMappingRules,uni_dbConnection,uni_dbCursor)

            #Validate Test Case List
            testResults = validateTestCaseList(testCaseList,uniDBValidationTestName,outputDirectory,logger)

            #Add Validation Test Result to Validation Report
            validationReport.addTest(testResults)

    #Set Test End time
    validationReport.setEndTime()

    #Generate Validation Report
    reportOutput = validationReport.outputTestReport()

    #Last Validation Report Logger Information
    loggerMsg = 'Validation Script has completed!'
    print loggerMsg
    logger.info(loggerMsg)
    loggerMsg = 'All individual test results and the validation test summary saved to: {outputFilePath}'.format(outputFilePath=outputDirectory)
    print loggerMsg
    logger.info(loggerMsg)

def usage():
    print '\n\n'
    print 'Usage options for validation_script.py:'
    print '-h, --help                                       Shows help message and exits.\n'
    print '------Script Input Parameter Settings---------------------------\n'
    print '-t, --testparamsettings     [filename]           File path for the test parameter settings XML file. Default is test_parameter_settings.xml in current working directory.'
    print '-s, --source                [xml/db/both]        Source of test parameters. XML (\'xml\'), source database via csv/stored procedure(\'db\'), or both sources (\'both\').'

    print '------Input/Output Directory Settings---------------------------------\n'
    print '-i, --inputdir  [dirpath]                        Indicates the directory to read the mapping rules and xml case list files from. Default is current working directory.'
    print '-o, --outputdir [dirpath]                        Indicates the directory to save the results files. Default is current working directory.\n'

    print '------NT/HERE and OSM/UniDB Connection Settings-----------------------------------\n'
    print '-c, --host      [Database host server]           Spefifies the database server hosting the HERE/NT & OSM/UniDB databases.'
    print '-a, --ntdb      [NT/HERE database name]          Specifies HERE/NT database name. Overrides Databaese Parameter XML file.'
    print '-b, --osmdb     [OSM/UniDB database name]         Specifies OSM/UniDB database name. Overrides Database Parameter XML file.'
    print '-u, --user      [Database host user name]        Indicate database host user name.'
    print '-p, --password  [Database host user password]    Indicate the database host user\'s password.'

def main():
    #Local Variables
    inputDirectory = os.getcwd()
    outputDirectory = os.getcwd()
    caseListSource = None
    inputTestParameterSettingsXML = None
    dbHost = None
    sourceDBName = None
    uniDBName = None
    userName = None
    password = None

    #Parse command line arguments
    try:
        options, remainder = getopt.getopt(sys.argv[1:],'ht:s:i:o:c:a:b:u:p:',['help','testparamsettings=','source=','inputdir=','outputdir=','host=','ntdb=','osmdb=','user=','password='])
    except getopt.GetoptError as err:
            print '\nERROR:\n\t',err,'\n'
            usage()
            sys.exit(2)

    #Process command line argument options
    for opt,arg in options:
        if opt in ('-h','--help'):
            usage()
            sys.exit(0)
        if opt in ('-t','--testparamsettings'):
            inputTestParameterSettingsXML=os.path.join(arg)
        if opt in ('-s','--source'):
            caseListSource = arg
        if opt in ('-i','--inputdir'):
            inputDirectory = os.path.join(arg)
        if opt in ('-o','--outputdir'):
            outputDirectory = os.path.join(arg)
        if opt in ('-c', '--host'):
            dbHost = arg
        if opt in ('-a', '--ntdb'):
            sourceDBName = arg
        if opt in ('-b', '--osmdb'):
            uniDBName = arg
        if opt in ('-u','--user'):
            userName = arg
        if opt in ('-p','--password'):
            password = arg

    #Set default Test Parameters XML file path
    if inputTestParameterSettingsXML == None and (caseListSource == 'db' or caseListSource == 'both'):
        inputTestParameterSettingsXML = os.path.join(inputDirectory,'test_parameter_settings.xml')

    #Setup Test Logger
    logFilePath=os.path.join(outputDirectory,'validation_script_nt2osm.log')
    logging.basicConfig(filename = logFilePath,filemode='a',format = '%(levelname)-10s %(asctime)s || %(message)s',level = logging.DEBUG)
    logger = logging.getLogger('nt2osm_validation')

    #Validate commandline arguments
    if not os.path.exists(inputDirectory) or inputDirectory == None:
        beginTestMsg = "ERROR: Input directory not specified or invalid. Halting test."
        print beginTestMsg
        logger.error(beginTestMsg)
        sys.exit(1)
    if not os.path.exists(outputDirectory) or outputDirectory == None:
        beginTestMsg = "ERROR: Output directory not specified or invalid. Halting test."
        print beginTestMsg
        logger.error(beginTestMsg)
        sys.exit(1)
    if caseListSource == None or (caseListSource != 'xml' and caseListSource != 'db' and caseListSource != 'both'):
        beginTestMsg = "ERROR: Test case list source not, or incorrectly, specified. Halting test."
        print beginTestMsg
        logger.error(beginTestMsg)
        sys.exit(1)
    else:
        if caseListSource == 'db':
            if not os.path.isfile(inputTestParameterSettingsXML) :
                beginTestMsg = "ERROR: Test Parameters Settings XML File is not valid. Halting test."
                print beginTestMsg
                logger.error(beginTestMsg)
                sys.exit(1)
    if dbHost == None:
        beginTestMsg = "ERROR: Database host not specified. Halting test."
        print beginTestMsg
        logger.error(beginTestMsg)
        sys.exit(1)
    if sourceDBName == None:
        beginTestMsg = "ERROR: Source database name not specified. Halting test."
        print beginTestMsg
        logger.error(beginTestMsg)
        sys.exit(1)
    if uniDBName == None:
        beginTestMsg = "ERROR: OSM database name not specified. Halting test."
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
        dbSourceConnectionValidate = psycopg2.connect('dbname={dbname} host = {host} user={user} password={password}'.format(host=dbHost,dbname=sourceDBName,user=userName,password=password))
        dbSourceConnectionValidate.close()
    except Exception, e:
        beginTestMsg = "ERROR: Connection parameters to source database invalid. Halting test."
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

    #Validation Test
    beginTestMsg = 'Beginning Validation Test'
    print beginTestMsg
    logger.info(beginTestMsg)
    result = processValidationTests(logger,inputDirectory,outputDirectory,caseListSource,dbHost,uniDBName,sourceDBName,userName,password,inputTestParameterSettingsXML)
    endTestMsg = 'End Validation Test\n'
    print endTestMsg
    logger.info(endTestMsg)

if __name__ == "__main__":
    main()
