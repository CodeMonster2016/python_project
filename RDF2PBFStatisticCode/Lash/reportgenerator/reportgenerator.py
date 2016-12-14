#!/usr/bin/env python
# -*- coding: utf-8 -*-
#-------------------------------------------------------------------------------
# Name:        ReportGenerator
# Purpose:
#
# Author:      qfding
#
# Created:     30-01-2013
# Copyright:   (c) qfding 2013
# Licence:
#-------------------------------------------------------------------------------

import os
import time
import datetime
import argparse
import sys
import re
import shutil
import thread

from case_dump import CaseDump
from Utility import Utility
from html_report import *

TEST_OUTPUT = "testoutput"
TEST_SUITES = "testsuites"
REPORT      = "report"

LESS_THAN        = "less_than"
GREATER_THAN     = "greater_than"
EQUAL            = "equal"
COMPARE_OPERATOR = {LESS_THAN:"<",
                    GREATER_THAN:">",
                    EQUAL:"=="}

FAIL = "Fail"
PASS = "Pass"
CONDITION_NA = "condition N/A"
NA = "n/a"

NOT_IN_COMPARE_RULE = ""
IS_FAIL = True

XML_SUFFIX          = ".xml"
REF_SUFFIX          = ".ref"
OUT_SUFFIX          = ".out"
HTML_SUFFIX         = ".html"
CASE_FILE_TYPE_JSON = "json"

INDEX_HTML = "index.html"

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

class Report:
    def __init__(self,testoutput,report_path):
        self.testoutput_folder = testoutput
        self.report_path       = report_path

    def generate_report(self):
##        #generate testcase report structure
##        #test_suites_result = map(lambda(key):self.__generate_test_suites(key,self.report_path),os.listdir(self.testoutput_folder))
##        #create test_suites index in report folder
##        #remove dup element & ()
##        #test_suites_result = list(set(test_suites_result)-set([()]))
##        input_set_result = map(lambda(key):self.__generate_cases_report(os.path.join(self.testoutput_folder,key),self.report_path,key),os.listdir(self.testoutput_folder))
##        #create input sets index in test_suite folder
##        #remove dup element & ()
##        input_set_result = list(set(input_set_result)-set([()]))
##        self.__create_input_sets_index(input_set_result,os.path.join(self.report_path,INDEX_HTML))
##        return (test_suite,map(lambda px:px[1]!=0 and IS_FAIL or None,input_set_result).count(IS_FAIL))
##
##        self.__create_test_suites_index(test_suites_result,os.path.join(self.report_path,INDEX_HTML))
##        return map(lambda px:px[1]!=0 and IS_FAIL or None,test_suites_result).count(IS_FAIL)
        input_set_result = self.__generate_cases_report(self.testoutput_folder, self.report_path)

##    def __generate_test_suites(self,test_suite,report_path):
##        test_suite_path = os.path.join(self.testoutput_folder,test_suite)
##        if not os.path.exists(test_suite_path):
##            return ()
##        #if test_suite exist, create report path of test_suite
##        report_path_of_test_suite = os.path.join(report_path,test_suite)
##        os.path.exists(report_path_of_test_suite) or os.makedirs(report_path_of_test_suite)
##
##        input_set_result = map(lambda(key):self.__generate_cases_report(test_suite_path,report_path_of_test_suite,key),os.listdir(test_suite_path))
##        #create input sets index in test_suite folder
##        #remove dup element & ()
##        input_set_result = list(set(input_set_result)-set([()]))
##        self.__create_input_sets_index(input_set_result,os.path.join(report_path_of_test_suite,INDEX_HTML))
##        return (test_suite,map(lambda px:px[1]!=0 and IS_FAIL or None,input_set_result).count(IS_FAIL))

    def __generate_cases_report(self,test_case_path ,report_path_of_test_suite):
        case_dir = test_case_path
        if not os.path.exists(case_dir):
            return ()
        #if test_suite/inputset exist, create report path of report:test_suite/input_set
        report_path_of_input_set = report_path_of_test_suite
        os.path.exists(report_path_of_input_set) or os.makedirs(report_path_of_input_set)

        #get all case {testsuites=>{inputset=>[case_list]}}
        case_list = list(set(map(lambda px:(px.endswith(REF_SUFFIX) or px.endswith(OUT_SUFFIX)) and os.path.splitext(px)[0] or None,
                                 os.listdir(case_dir))) - set([None]))

        #return this case's differen field number
        bg = datetime.datetime.now()
        case_result = map(lambda px:self.__generate_case_detail(case_dir,px,report_path_of_input_set),case_list)
        ed = datetime.datetime.now()
        print "Generate ["+str(len(case_result))+"] cases detail pages cost time:"+str(ed - bg)

        #don't sort hear
        #sorted(case_result,key=lambda px:px[1],reverse = True)

        #create case index in input_set folder
        bg = datetime.datetime.now()
        self.__create_cases_index(case_result,os.path.join(report_path_of_input_set,INDEX_HTML))
        ed = datetime.datetime.now()
        print "Create case index page cost time:"+str(ed - bg)
        #dup case_list loop
        return (os.path.basename(case_dir),map(lambda px:px[1]!=0 and IS_FAIL or None,case_result).count(IS_FAIL))

    def __generate_case_detail(self,case_dir,case,report_path):
        ref = CaseDump(os.path.join(case_dir,case+REF_SUFFIX),CASE_FILE_TYPE_JSON).dump()
        out = CaseDump(os.path.join(case_dir,case+OUT_SUFFIX),CASE_FILE_TYPE_JSON).dump()
        #fields = list(set(ref.keys()+out.keys()))
        fields = ref.keys()
        fields.sort()
        #get compare condition
        result = map(lambda(field):self.__compare_case(ref,out,field),fields)
        #sort result fail is on the top [field_name,ref_value,out_value,condition,status]
        result.sort(key=lambda px:px[4])

        #thread.start_new_thread(self.__create_case_detail_page,(result+other_result,os.path.join(report_path,case+HTML_SUFFIX)))
        self.__create_case_detail_page(result,os.path.join(report_path,case+HTML_SUFFIX))

        return (case,map(lambda px:px[4]==FAIL and IS_FAIL or None,result).count(IS_FAIL), not (ref and out) and NA or "" )

    def __compare_case(self,ref,out,field):
        status = FAIL

        ref_v = ref.has_key(field) and ref.get(field) or ""
        out_v = out.has_key(field) and out.get(field) or ""
        status = (ref_v==out_v and PASS or FAIL)
        #[field_name,ref_value,out_value,condition,status]
        return [field,str(ref_v),str(out_v),COMPARE_OPERATOR.get(EQUAL),status]

    def __create_case_detail_page(self,result,case_report_path):
        #create case detail page [field_name,ref_value,out_value,condition,status]
        HtmlReport().create_case_page(case_report_path,
                                       result,
                                       self.report_path,
                                       CASE_DETAIL_HEAD)

    def __create_cases_index(self,case_list,case_index_report_path):
        #create index.html
        HtmlReport().create_index_page(case_index_report_path,
                                       case_list,
                                       self.report_path,
                                       CASE_INDEX_HEAD,
                                       True)

    def __create_input_sets_index(self,input_set_list,input_set_report_path):
        #create index.htnl
        HtmlReport().create_index_page(input_set_report_path,
                                       input_set_list,
                                       self.report_path,
                                       INPUT_SET_INDEX_HEAD)

    def __create_test_suites_index(self,test_suites_list,test_suites_report_path):
        HtmlReport().create_index_page( test_suites_report_path,
                                        test_suites_list,
                                        self.report_path,
                                        TEST_SUITES_INDEX_HEAD)

def merge_ref_out(ref,out,testoutput):
    #remove output path and recreate it
    if os.path.exists(testoutput):
        try:
            shutil.rmtree(testoutput)
        except Exception, e:
            print "*** [Exception] Can't delete report directory, maybe the directory is locked."
            sys.exit(255)
    #create testoutput dir
    try:
        os.makedirs(testoutput)
    except Exception, e:
        time.sleep(5)
        os.makedirs(testoutput)

    #rename ref
    rename(ref,"ref")
    #rename out
    rename(out,"out")

    #move ref & out to testoutput
    copysubtree(ref,testoutput)
    copysubtree(out,testoutput)

def rename(folder,suffix):
    for fname in os.listdir(folder):
        f_path = os.path.join(folder,fname)
        if os.path.isdir(f_path):
            rename(f_path,suffix)
        elif os.path.isfile(f_path) and not f_path.endswith(suffix):
            os.rename(f_path,os.path.splitext(f_path)[0]+"."+suffix)

def copysubtree(src,dst):
    for src_f in os.listdir(src):
        src_path = os.path.join(src,src_f)
        if os.path.isdir(src_path):
            dst_path = os.path.join(dst,src_f)
            if not os.path.exists(dst_path):
                shutil.copytree(src_path,dst_path)
            else:
                copysubtree(src_path,dst_path)
        elif os.path.isfile(src_path):
            dst_path = os.path.join(dst,src_f)
            if not os.path.exists(dst_path):
                shutil.copy(src_path,dst_path)

if __name__ == '__main__':
    #all variables in this block is global variable
    parser = argparse.ArgumentParser()
    parser.add_argument('-rdf', dest  ='rdf',    default=os.path.join(ROOT_DIR,'..','st_rdf','output','rdf'), help='rdf statistic')
    parser.add_argument('-osm', dest  ='osm',    default=os.path.join(ROOT_DIR,'..','st_osm','output'),       help='osm statistic')
    parser.add_argument('-r',   dest  ='report', default=os.path.join(ROOT_DIR,'..','report'),                help='report path')
    args = parser.parse_args()

    testoutput  = os.path.join(ROOT_DIR,TEST_OUTPUT)
    merge_ref_out(os.path.abspath(args.rdf),os.path.abspath(args.osm),testoutput)
    report_path = os.path.abspath(args.report)

    #check testoutput
    if not os.path.exists(testoutput):
        print "*** [Exception] Oops! Argument kind is wrong."
        print "*** [Exception] Please make sure testoutput folder is accessible!"
        printUsage()
        sys.exit(255)

    #remove report path and recreate it
    if os.path.exists(report_path):
        try:
            shutil.rmtree(report_path)
        except Exception, e:
            print "*** [Exception] Can't delete report directory, maybe the directory is locked."
            sys.exit(255)

    #create testreport dir
    try:
        os.makedirs(report_path)
    except Exception, e:
        time.sleep(5)
        os.makedirs(report_path)

    #prepare reoprt assets, include css and javascript file
    not os.path.exists(os.path.join(report_path,"assets")) and \
    Utility.copy(os.path.join(sys.path[0],"assets"),report_path)

    lines = []
    bg = datetime.datetime.now()
    lines.append("Start generate report at "+bg.strftime("%Y-%m-%d %H:%M:%S"))
    print lines[-1]

    #fail test suites number
    fail_num = Report(testoutput,report_path).generate_report()

    ed = datetime.datetime.now()
    lines.append("Finished at "+ed.strftime("%Y-%m-%d %H:%M:%S"))
    print lines[-1]
    lines.append("Total spend "+str(ed - bg)+" in report generate process")
    print lines[-1]

    result = open(os.path.join(report_path,(fail_num == 0) and PASS.lower() or FAIL.lower() ),'w')
    result.write("\n".join(lines))
    result.close()
