import csv
import os
import os.path
import psycopg2
import psycopg2.extras
import vreport
import xml.etree.ElementTree as ET

class xml2TestCaseAdapter(object):
    """
        Utility that proccesses XML files of test cases (basic elements or full relation information).
        Creates Element Tree (xml) Python objects that represent test cases and
        converts them to a UniDB Test Case object or list of objects.
    """
    def __init__(self):
        super(xml2TestCaseAdapter, self).__init__()    

    @staticmethod
    def convertBasicXMLCaseElem2UnidbTestCase(case):
        """
            Transforms basic case stored in a xml etree element to unidbobject.
            Returns a test case sans the OSM DB Connection & Cursor
        """

        source_data_id = case[0].text
        source_id_name = case[0].get('source_data_id_name')
        unidb_id = case[1].text
        object_type = case[2].text
        
        #Add Basic UniDB Object Info (Tags, ID)
        uniDBTestCase = vreport.UniDBBasicElementTestCase(source_data_id,source_id_name,unidb_id,object_type,None,None)
        for i in xrange(0,len(case[3])):
            key = case[3][i].get('TAG_NAME')
            value = case[3][i].text
            uniDBTestCase.addExpectedTag(key,value)

        return uniDBTestCase

    @staticmethod
    def convertRelationMemberListXMLCaseElem2UnidbTestCase(case):
        """
            Transforms relation case (relation info+relation member list) to unidbobject
            Returns test case sans OSM DB Connection & Cursor
        """    

        source_data_id = case[0].text
        source_id_name = case[0].get('source_data_id_name')
        unidb_id = case[1].text
        object_type = case[2].text

        #Add Basic UniDB Object Info (Tags, ID)
        uniDBTestCase = vreport.UniDBRelationMemberListTestCase(source_data_id,source_id_name,unidb_id,object_type,None,None)
        for i in xrange(0,len(case[3])):
            key = case[3][i].get('TAG_NAME')
            value = case[3][i].text
            uniDBTestCase.addExpectedTag(key,value)

        #Add relation members
        for i in xrange(0,len(case[4])):
            relation_id = uniDBTestCase.getRelationID()
            member_id =  case[4][i].get('MEMBER_ID')
            member_type =  case[4][i].get('MEMBER_TYPE')
            member_role =  case[4][i].get('MEMBER_ROLE')
            sequence_id =  case[4][i].get('SEQUENCE_ID')
            uniDBTestCase.addRelationMember(relation_id,member_id,member_type,member_role,sequence_id)

        return uniDBTestCase

    @staticmethod
    def createTestCaseListFromXML(xmlCaseList):
        """
            Creates list of cases, no matter the type of case.
        """
        caseList = []

        for i in xrange(0,len(xmlCaseList)):

            if xmlCaseList[i].get('CASE_TYPE') == 'FULL_RELATION_VALIDATION':
                case = xml2TestCaseAdapter.convertRelationMemberListXMLCaseElem2UnidbTestCase(xmlCaseList[i])

            if xmlCaseList[i].get('CASE_TYPE') == 'TAG_VALIDATION':
                case = xml2TestCaseAdapter.convertBasicXMLCaseElem2UnidbTestCase(xmlCaseList[i])

            caseList.append(case)

        return caseList

    @staticmethod
    def getTestCasesFromXMLFile(inputXMLFile):
        """Parses Input XML File and Retrieves the Case List as an ElementTree object."""
        xmlObject = ET.parse(inputXMLFile)
        xmlCaseList = xmlObject.getroot()
        return xmlCaseList

    @staticmethod
    def getTestNameFromXMLFile(inputXMLFile):
        """
            Parses Input XML File and Retrieves the Case List as an ElementTree object.
        """
        xmlObject = ET.parse(inputXMLFile)
        xmlCaseList = xmlObject.getroot()
        return xmlCaseList.get('TEST_NAME')
