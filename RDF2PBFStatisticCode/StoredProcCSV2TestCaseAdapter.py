import csv
import os
import os.path
import psycopg2
import psycopg2.extras
import vreport
import xml.etree.ElementTree as ET

class StoredProcCSV2TestCaseAdapter(object):
    """
        Utility that proccesses XML files of test case parameters,
        CSV files of mappping rules,
        and uses stored procedures to build a single, or lists, of UniDB Test Case objects.
    """
    def __init__(self, inputXMLFile, inputDirectory,dbConnection,dbCursor):
        super(StoredProcCSV2TestCaseAdapter, self).__init__()
        self.testParametersXMLFile = inputXMLFile
        self.settingsTESTParamXMLTree = ET.parse(self.testParametersXMLFile)
        self.settingsTESTParamXMLRoot = self.settingsTESTParamXMLTree.getroot()
        self.inputDirectory = inputDirectory
        self.sourceDBConnection = dbConnection
        self.sourceDBCursor = dbCursor

    def getTestNameFromTestParametersXMLFile(self,testNumber):
        """Gets the name of the test associated with the corresponding test number"""
        featureTestName = self.settingsTESTParamXMLRoot[testNumber][1].text

        return featureTestName

    def getTestDBSchema(self,testNumber):
        """Gets the database schema of the source database to retrieve test cases."""
        testDBSchema = self.settingsTESTParamXMLRoot[testNumber][0].text

        return testDBSchema

    def getMappingRulesFromCSV(self,testNumber):
        """Gets the Mapping Rules from the Input CSV File Listed in the XML Test Parameters file."""
        mappingRulesFilepath = os.path.join(self.inputDirectory,self.settingsTESTParamXMLRoot[testNumber][4].text)

        mappingRulesArray = []
        with open(mappingRulesFilepath) as csvfile:
            csvreader = csv.reader(csvfile)
            for row in csvreader:
                mappingRulesArray.append(row)

        return mappingRulesArray

    def getTestSourceIDName(self,testNumber):
        """Gets the Name of the Source ID To Test"""
        sourceIDName = self.settingsTESTParamXMLRoot[testNumber][6].text

        return sourceIDName

    def getTestSourceIDAppend(self,testNumber):
        """Gets the Append Value for creating the uniDB ID from the source ID"""
        sourceIDAppend = self.settingsTESTParamXMLRoot[testNumber][7].text

        return sourceIDAppend

    def getTestOSMObjectType(self,testNumber):
        """Gets the Test OSM Object To Test"""
        testOSMObject = self.settingsTESTParamXMLRoot[testNumber][5].text

        if testOSMObject == 'N':
            return 'nodes'

        if testOSMObject == 'W':
            return 'ways'

        if testOSMObject == 'R':
            return 'relations'

    def getTestSourceCaseInfoStoredProcedure(self,testNumber):
        """Gets the Test Stored Procedure Used to test the get source DB Info for the Test Case"""
        return self.settingsTESTParamXMLRoot[testNumber][3].text

    def getNumberValidationTests(self):
        """Gets the number of tests listed in inputXMLFile"""
        return len(self.settingsTESTParamXMLRoot)

    def getCasesFromSourceDB(self,testNumber):
        """Gets the list of cases (sourceID's) from the source DB."""
        rawCaseList = []
        retrieve_test_case_list_stored_procedure = self.settingsTESTParamXMLRoot[testNumber][2].text
        order1schema = self.getTestDBSchema(testNumber)
        testCaseListQuery = 'select * from {retrieve_test_case_list_stored_procedure}(\'{order1schema}\')'.format(\
            retrieve_test_case_list_stored_procedure=retrieve_test_case_list_stored_procedure,\
            order1schema=order1schema)

        self.sourceDBCursor.execute(testCaseListQuery)
        results = self.sourceDBCursor.fetchall()

        #Iterate through query results and build list of raw source ID values (cases)
        for i in range(0,len(results)):
            rawCaseList.append(results[i][0])

        return rawCaseList

    def buildTestCasefromSourceDB(self,sourceDBID,sourceDBIDName,uniDBID,uniDBObjectType,testNumber,mappingRules):
        """Builds a test case from source DB data. Sans UniDB Connection and Cursor"""
        source_data_id = sourceDBID
        source_id_name = sourceDBIDName
        unidb_id = uniDBID
        object_type = uniDBObjectType
        sourceDBInfoStoredProcedure = self.getTestSourceCaseInfoStoredProcedure(testNumber)
        qryRetriveSourceDBInfo = 'select * from {test_case_info_stored_procedure}({source_DB_ID})'.format(test_case_info_stored_procedure=sourceDBInfoStoredProcedure,source_DB_ID=sourceDBID)

        uniDBTestCase = vreport.UniDBBasicElementTestCase(source_data_id,source_id_name,unidb_id,object_type,None,None)

        #Populate Expected Tags (if applicable)
        for i in xrange(1,len(mappingRules)):
            self.sourceDBCursor.execute(qryRetriveSourceDBInfo)
            caseResults = self.sourceDBCursor.fetchall()[0]
            indexResult = int(mappingRules[i][7])
            expectedMapRuleTagValue = mappingRules[i][6]
            if caseResults[indexResult] == None or caseResults[indexResult] == False:
                pass
            else:
                #Set Key 
                key = mappingRules[i][5]

                #Set Key Value
                if expectedMapRuleTagValue  == '(RDF Value)':                    
                    value = str(caseResults[indexResult])
                    uniDBTestCase.addExpectedTag(key,str(value))
                else:
                    if str(caseResults[indexResult]) == str(mappingRules[i][2]):
                        value = expectedMapRuleTagValue

                        uniDBTestCase.addExpectedTag(key,str(value))

        return uniDBTestCase

    def createTestCaseListFromSourceDB(self,dbCaseList,testNumber,mappingRules):
        """
            Creates a list of test cases from the sourceDB
        """
        caseList = []

        for i in range(0,len(dbCaseList)):
            source_data_id = dbCaseList[i]
            source_id_name = self.getTestSourceIDName(testNumber)
            append_unidb_id =self.getTestSourceIDAppend(testNumber)
            if append_unidb_id == None:
                append_unidb_id = ''
            unidb_id = long(str(dbCaseList[i]) + append_unidb_id)
            object_type = self.getTestOSMObjectType(testNumber)
            mappingRules = self.getMappingRulesFromCSV(testNumber)
            case = self.buildTestCasefromSourceDB(source_data_id,source_id_name,unidb_id,object_type,testNumber,mappingRules)
            caseList.append(case)

        return caseList
