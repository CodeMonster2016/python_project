#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      kuangh
#
# Created:     04/09/2015
# Copyright:   (c) kuangh 2015
# Licence:     <your licence>
#-------------------------------------------------------------------------------
# in the test suites folder, the file name is the UniDB table name,
# dilimter by "#"
# like ways#link

import os
import json
from ReadData import ReadDBData

CASE_TYPE_STATS = "stats"
CASE_TYPE_TAGS  = "tags"
DELIMITER_POUND = "#"
JSON_SUFFIX     = ".json"
STATS_JSON      = "stats.json"

class CaseProcessor:
    def __init__(self, hostname, dbname, dbuser, testsuites, output):
        self.hostname   = hostname
        self.dbname     = dbname
        self.dbuser     = dbuser
        self.testsuites = testsuites
        self.output     = output

    def run(self):
        #parse test suites
        if not self.run_testsuites():
            return False
        return True

    def run_testsuites(self):
        if not os.path.exists(self.testsuites):
            return False
        test_suites_output = os.path.join(self.output,os.path.basename(self.testsuites))
        os.path.exists(test_suites_output) or os.makedirs(test_suites_output)
        #get the test table
        for table in os.listdir(self.testsuites):
            testcases_output_path = os.path.join(test_suites_output, table)
            testcases_path        = os.path.join(self.testsuites, table)
            os.path.exists(testcases_output_path) or os.makedirs(testcases_output_path)
            self.query_database_to_generate_cases(table, testcases_path, testcases_output_path)
        return True

    def query_database_to_generate_cases(self, table, testcases_path, testcases_output_path):
        case_table_type_category = table.split(DELIMITER_POUND)
        table_name = case_table_type_category[0]
        case_type  = len(case_table_type_category) > 1 and case_table_type_category[1] or None
        if  CASE_TYPE_STATS == case_type:
            self.__gene_stats_cases(table_name, testcases_output_path)
        elif CASE_TYPE_TAGS == case_type:
            self.__gene_tags_cases(table_name, testcases_path, testcases_output_path)

    def __gene_tags_cases(self,table_name, testcases_path, testcases_output_path):
        #get testcase id
        with open(testcases_path,'r') as testcases_f:
            ids = testcases_f.readlines()
            ids = list(set(map(lambda(px):px.strip().isdigit() and px.strip() or None,ids)) - set([None]))
        if len(ids) == 0:
            return
        #get all records
        cases_tags = ReadDBData(self.hostname, self.dbname, self.dbuser).get_tags(table_name, testcases_path)
        for obj_id in ids:
            if not cases_tags.has_key(obj_id):
                continue
            with open(os.path.join(testcases_output_path, obj_id+JSON_SUFFIX), 'w') as case_f:
                case_f.write(json.dumps(cases_tags.get(obj_id)))

    def __gene_stats_cases(self, table_name, testcases_output_path):
        stats = ReadDBData(self.hostname, self.dbname, self.dbuser).get_stats(table_name)
        with open(os.path.join(testcases_output_path, STATS_JSON), 'w') as case_f:
            case_f.write(json.dumps(stats))

# unit test in here
def main(hostname, dbname, dbuser, testsuites, output):
    if CaseProcessor(hostname, dbname, dbuser, testsuites, output).run():
        print "Case generate successfully!"
    else:
        print "Error"

if __name__ == '__main__':
    main("","","","","")