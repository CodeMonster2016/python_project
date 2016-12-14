import os
import os.path
import datetime as dt
import time

#create tests
class TestCase(object):
    """Logs Result of Simple Test Case"""
    def __init__(self, uniDBConnection, uniDBCursor):
        super(TestCase, self).__init__()
        self.notFound = False 
        self.failedRule = False
        self.otherFail = False 
        self.passedTest = False
        self.uniDBConnection = uniDBConnection
        self.uniDBCursor = uniDBCursor

    def getUniDBConnection(self):
        return self.uniDBConnection

    def getUniDBCursor(self):
        return self.uniDBCursor

    def setNotFound(self):
        self.notFound = True

    def setFailedRule(self):
        self.failedRule = True

    def setOtherFail(self):
        self.otherFail = True

    def setPassedTest(self):
        self.passedTest = True

    def setUniDBConnection(self,dbConnection):
        self.uniDBConnection = dbConnection

    def setUniDBCursor(self,dbCursor):
        self.uniDBCursor = dbCursor

    def isNotFound(self):
        return self.notFound

    def isFailedRule(self):
        return self.failedRule

    def isOtherFail(self):
        return self.otherFail

    def isPassedTest(self):
        return self.passedTest

class UniDBBasicElementTestCase(TestCase):
    """Expands on base TestCase class to store and test expected values for node, way, relation tags"""    
    def __init__(self,source_data_id,source_data_id_name,uniDB_ID,objectType,uniDBConnection, uniDBCursor):
        TestCase.__init__(self,uniDBConnection, uniDBCursor)
        self.sourceDataID = long(source_data_id)
        self.sourceDataIDName = source_data_id_name
        self.uniDB_ID = long(uniDB_ID)
        self.objectType = objectType        
        self.expectedTags = {}

        if self.objectType == 'relation_members':
            self.uniDBTable = 'relations'
        else:
            self.uniDBTable = self.objectType

    def addExpectedTag(self,key,value):
        """Adds an expected tag to the test case"""
        if isinstance(value,str):
            self.expectedTags[key] = unicode(value,'utf-8')
        else:
            self.expectedTags[key] = value     

    def getSourceDataID(self):
        """Retrieves the source id value."""
        return self.sourceDataID

    def getSourceDataIDName(self):
        """Retrieves the Test cases' source ID name."""
        return self.sourceDataIDName    

    def getUniDBID(self):
        """Retrieves the uniDB ID of the Test Case."""
        return self.uniDB_ID    

    def getObjectType(self):
        """Retrieves the Object Type (nodes, ways, relations) of the Test Case"""
        return self.objectType        

    def getExpectedTags(self):
        """Retrieves expected tag key:value pairs of test case"""
        return self.expectedTags    

    def getActualTagValue(self,key):
        """Retrieves the actual value of the tag in target uniDB database. """
        sql = 'select tags->\'{tag_name}\' as actual_value from {unidb_table} where id = {input_unidb_id}  '
        self.uniDBCursor.execute(sql.format(tag_name=key,unidb_table=self.uniDBTable,input_unidb_id=self.uniDB_ID))
        results = self.uniDBCursor.fetchall()

        if isinstance(results[0][0],str):
            return unicode(results[0][0],'utf-8')
        else:
            return results[0][0]        

    def changeExpectedTagValuePair(self,key,newvalue):
        """Changes an expected tag value pair to another value"""
        self.expectedTags[key] = newvalue    

    def isUniDBObjectExist(self):
        """Tests to see if the Test Case UniDB Object Exists in the target UniDB database"""
        sql = 'select * from {unidb_table} where id = {input_uni_id}'

        self.uniDBCursor.execute(sql.format(unidb_table=self.uniDBTable,input_uni_id=self.uniDB_ID))
        results  = self.uniDBCursor.fetchall()    

        if len(results) > 0:
            return True

        if len(results) == 0:
            return False 
         
    def isExpectedTagExist(self,key):
        """Tests to see if Expected Test Case Tag Exists in UniDB"""
        sql = 'select (tags ? \'{tag_name}\') from {unidb_table} where id = {input_unidb_id}'

        self.uniDBCursor.execute(sql.format(tag_name=key,unidb_table=self.uniDBTable,input_unidb_id=self.uniDB_ID))
        results = self.uniDBCursor.fetchall()
        return results[0][0]

    def isExpectedTagValuePairExist(self,key):
        """Tests to see if an Expected Tag Key-Value Pair Exists"""
        sql = 'select tags->\'{tag_name}\'=\'{tag_value}\' from {unidb_table} where id = {input_uni_id}'

        try:
            self.uniDBCursor.execute(sql.format(tag_name=key,tag_value=self.expectedTags[key],unidb_table=self.uniDBTable,input_uni_id=self.uniDB_ID))
        except Exception, e:
            self.uniDBConnection.reset()
            return False
        
        results = self.uniDBCursor.fetchall()
        return results[0][0]

class UniDBRelationMemberTestCase(TestCase):
    """
        Expands on base TestCase class to store and test expected values of a relation member
        **NOTE** that the UniDB ID associated with the super class Test Case is the 'relation_id' associated with the
        relation member.
    """
    def __init__(self,inputRelationID,inputMemberID,inputMemberType,inputMemberRole,inputSequenceID,uniDBConnection, uniDBCursor):
        TestCase.__init__(self,uniDBConnection, uniDBCursor)    
        self.relationID = long(inputRelationID)
        self.memberID = long(inputMemberID)
        self.memberType = inputMemberType
        self.memberRole = inputMemberRole
        self.sequenceID = inputSequenceID   

    def getRelationID(self):
        return self.relationID

    def getMemberID(self):
        return self.memberID

    def getMemberType(self):
        return self.memberType

    def getMemberRole(self):
        return self.memberRole

    def getSequenceID(self):
        return self.sequenceID                        

    def isMemberExist(self):
        sql = 'select * from {osm_table} '+\
            'where relation_id = {relation_id} and '+\
            'member_id={member_id} and '+\
            'member_type=\'{member_type}\' and '+\
            'member_role=\'{member_role}\' and '+\
            'sequence_id={sequence_id} '

        sql = sql.format(osm_table='relation_members',relation_id=self.relationID,member_id=self.memberID,\
            member_type=self.memberType,member_role=self.memberRole,sequence_id=self.sequenceID)

        self.uniDBCursor.execute(sql) 
        results = self.uniDBCursor.fetchall()

        if len(results) > 0:
            return True
        elif len(results) == 0:
            return False
        else:
            return False

class UniDBRelationMemberListTestCase(UniDBBasicElementTestCase):
    """Expands on the UniDBBasicElementTestCase class by storing expected relation member list information and test it."""
    def __init__(self,source_data_id,source_data_id_name,uniDB_ID,objectType,uniDBConnection, uniDBCursor):
        UniDBBasicElementTestCase.__init__(self,source_data_id,source_data_id_name,uniDB_ID,objectType,uniDBConnection, uniDBCursor)
        self.relationMembers = []
        self.missingMember = False

    def addRelationMember(self,inputRelationID,inputMemberID,inputMemberType,inputMemberRole,inputSequenceID):
        """Adds a Relation Member to the Relation Test Case Relation Members List."""
        relationMember = UniDBRelationMemberTestCase(inputRelationID,inputMemberID,inputMemberType,inputMemberRole,inputSequenceID,\
            self.uniDBConnection,self.uniDBCursor)

        self.relationMembers.append(relationMember)

    def getRelationID(self):
        return self.uniDB_ID

    def getRelationMember(self,memberIndex):
        return self.relationMembers[memberIndex]    

    def getMemberList(self):
        return self.relationMembers

    def setMissingMember(self):
        self.missingMember = True

    def isMissingMember(self):
        """Checks to see if a member is missing"""
        for i in xrange(0,len(self.relationMembers)):
            relationMember = self.relationMembers[i]
            if not relationMember.isMemberExist():
                self.setMissingMember()

                return self.missingMember

        return self.missingMember

class TestStatistics(object):
    """Logs Results of Evaluated Test Cases in Test"""
    def __init__(self, testName):
        super(TestStatistics, self).__init__()
        self.name = testName
        self.countTotalCases = 0
        self.countNotFoundCases = 0
        self.countFailedRuleCases = 0        
        self.countOtherFailCases = 0
        self.countPassedCases = 0
        self.resultsText = []

    def processTestCase(self,testCase):
        self.countTotalCases += 1

        if testCase.isNotFound():
            self.countNotFoundCases += 1
            return

        if testCase.isFailedRule():
            self.countFailedRuleCases += 1
            return

        if testCase.isOtherFail():
            self.countOtherFailCases += 1
            return

        if testCase.isPassedTest():
            self.countPassedCases += 1
            return

    def getName(self):
        return self.name

    def getCountTotalCases(self):
        return self.countTotalCases

    def getCountNotFoundCases(self):
        return self.countNotFoundCases

    def getCountFailedRuleCases(self):
        return self.countFailedRuleCases

    def getCountOtherFailCases(self):
        return self.countOtherFailCases

    def getCountPassedCases(self):
        return self.countPassedCases
        
class ValidationReportClass(object):
    """Logs Results of All Tests in Validation"""
    def __init__(self,inputOutputDirectory):
        super(ValidationReportClass, self).__init__()        
        self.countTests = 0
        self.listTests = list()
        self.startTime = None
        self.endTime = None
        self.outputDirectory = inputOutputDirectory

    def addTest(self,test):
        self.listTests.append(test)
        self.countTests += 1

    def getTestCount(self):
        return self.countTests

    def getTestResult(self,testNumber):
        test = self.listTests[testNumber]      

        #Calculate Percentage If there is 0 or more than 0 test cases
        if test.getCountTotalCases() == 0:
            percentage_passedTestResult = round(0.00,3)
            percentage_failedRuleTestResult = round(0.00,3)
            percentage_notFoundTestResult = round(0.00,3)
            percentage_otherFailCasesResult = round(0.00,3)
        else:
            percentage_passedTestResult = str(round(float(test.getCountPassedCases())/test.getCountTotalCases(),3)*100)
            percentage_failedRuleTestResult = str(round(float(test.getCountFailedRuleCases())/test.getCountTotalCases(),3)*100)
            percentage_notFoundTestResult = str(round(float(test.getCountNotFoundCases())/test.getCountTotalCases(),3)*100)
            percentage_otherFailCasesResult= str(round(float(test.getCountOtherFailCases())/test.getCountTotalCases(),3)*100)

        passedTestResult = "{value} ({percentage}%)".format(value=str(test.getCountPassedCases()), percentage=percentage_passedTestResult)
        failedRuleTestResult = "{value} ({percentage}%)".format(value=str(test.getCountFailedRuleCases()), percentage=percentage_failedRuleTestResult)
        notFoundTestResult = "{value} ({percentage}%)".format(value=str(test.getCountNotFoundCases()), percentage=percentage_notFoundTestResult)
        otherFailCasesResult = "{value} ({percentage}%)".format(value=str(test.getCountOtherFailCases()), percentage=percentage_otherFailCasesResult)

        return '{testnumber}.{col1}{testname}{col2}{totaltests}{col3}{passedtest}{col4}{failedruletest}{col5}{notfound}{col6}{otherfail}\n'.format(testnumber=testNumber,col1=' '*(10-len(str(testNumber))),\
            testname=test.getName(),col2=' '*(50-len(str(test.getName()))),totaltests=test.getCountTotalCases(),col3=' '*(20-len(str(test.getCountTotalCases()))),\
            passedtest=passedTestResult,col4=' '*(20-len(passedTestResult)),\
            failedruletest=failedRuleTestResult,col5=' '*(20-len(failedRuleTestResult)),\
            notfound=notFoundTestResult,col6=' '*(20-len(notFoundTestResult)),\
            otherfail=otherFailCasesResult)

    def getTestReport(self):
        """Generates Test Report String"""
        report = 'Validation Report\n'
        report += 'Generated on: {datetime} \n'.format(datetime=time.asctime())
        report += 'Validation processed {number} tests\n\n'.format(number=self.getTestCount())
        report += 'Test Results are as follows:\n\n'
        report += 'Test No.{col1}Test Name{col2}Total Cases{col3}Passed Cases{col4}Failed Rule Cases{col5}Not Found Cases{col6}Other Fail Cases\n'.format(col1=' '*(10-len('Test No.')),col2=' '*(50-len('Test Name')),\
             col3=' '*(20-len('Total Cases')),col4=' '*(20-len('Passed Cases')),col5=' '*(20-len('Failed Rule Cases')),col6=' '*(20-len('Not Found Cases')))
        report += '{hyphen}\n'.format(hyphen='-'*160)

        for i in xrange(0,self.getTestCount()):
            report += self.getTestResult(i)
                
        return report

    def outputTestReport(self):
        """Sends Test Report String to Console and to File"""
        #Get Text Report String
        reportOutput = self.getTestReport()         

        #Send Report Output to Console
        print "\n\n",reportOutput

        #Send Report Output to File
        reportTime = str(dt.datetime.now()).split(' ')
        reportFileName = os.path.join(self.outputDirectory,'Validation_Test' + reportTime[0].replace('-','') + '_' + reportTime[1].split('.')[0].split(':')[0]+ reportTime[1].split('.')[0].split(':')[1] + '.txt')
        reportFile = open(reportFileName,'w')
        reportFile.write(reportOutput)
        reportFile.write('\n\n\n' + 'Beginning Test: ' +  str(self.startTime) + '\n')
        reportFile.write('Ending Test: '+ str(self.endTime) +'\n')
        reportFile.write('Time Cost: ' + str(self.endTime - self.startTime))
        reportFile.close()

    def setStartTime(self):
        self.startTime = dt.datetime.now()

    def setEndTime(self):
        self.endTime = dt.datetime.now()
