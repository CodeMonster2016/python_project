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
import os
import sys
import argparse
import shutil
from caseprocessor import CaseProcessor

ROOT_DIR             = os.path.dirname(os.path.abspath(__file__))
BASELINE_DIR         = os.path.join(ROOT_DIR,"baseline")
REPORT_GENERATOR_DIR = os.path.join(ROOT_DIR,"reportgenerator")
OUTPUT_DIR           = os.path.join(ROOT_DIR,"output")
REPORT_DIR           = os.path.join(ROOT_DIR,"report")
TESTSUITES_SET       = "testsuites"

def processCase(hostname, dbname, dbuser, testsuites, output):
    # porcess only one testsuite
    for testsuite in os.listdir(testsuites):
        testsuite = os.path.join(testsuites,testsuite)
        if CaseProcessor(hostname, dbname, dbuser, testsuite, output).run():
            print "Case generate successfully!"
        else:
            print "Error"

def generateReport(baseline, output, report):
    reportgenerator_path = os.path.join(REPORT_GENERATOR_DIR, "reportgenerator.py")
    cmd = "python2.7 "+reportgenerator_path+" -ref "+baseline+" -out "+output+" -r "+report
    print cmd
    os.system(cmd)

def runRegression(hostnam, dbname, dbuser, testsuites, baseline, output, report):
    #run testcase
    # clear output
    __clear_path(output)
    processCase(hostnam, dbname, dbuser, testsuites, output)
    generateReport(baseline, output, report)

def __clear_path(path):
    # clear path
    if os.path.exists(path):
        try:
            shutil.rmtree(path)
        except Exception, e:
            print "*** [Exception] Can't delete directory, maybe the directory is locked."
            sys.exit(255)
    #create dir
    os.makedirs(path)

def printUsage():
    print "regressionex.py -testex -h [hostname] -d [dbname] -U [dbuser] -testsuites testsuites_path [-v1 [baseline_dir]"
    print "testex: the parameter includes ""caseprocess"" and ""generatereport"""
    print "regressionex.py -caseprocess -h [hostname] -d [dbname] -U [postgres] -testsuites testsuites_path <-o [case_output]>"
    print "regressionex.py -generatereport -v1 [v1_case_results] -v2 [v2_case_results] -r [output_report]"
    sys.exit(-1)

if __name__ == '__main__':
    len(sys.argv) < 7 and printUsage()
    process_type = 'testex'
    hostname     = 'localhost'
    dbname       = ''
    dbuser       = 'postgres'
    testsuites   = ''
    baseline     = ''
    caseoutput   = ''
    report       = ''

    i = 0
    while i < len(sys.argv):
        if sys.argv[i] == '-caseprocess':
            process_type = 'caseprocess'
        elif sys.argv[i] == '-generatereport':
            process_type = 'generatereport'
        elif sys.argv[i] == '-testex':
            process_type = 'testex'
        elif sys.argv[i] == '-h':
            i += 1
            hostname = sys.argv[i]
        elif sys.argv[i] == '-d':
            i += 1
            dbname = sys.argv[i]
        elif sys.argv[i] == '-U':
            i += 1
            dbuser = sys.argv[i]
        elif sys.argv[i] == '-testsuites':
            i += 1
            testsuites = sys.argv[i]
        elif sys.argv[i] == '-o':
            i += 1
            caseoutput = sys.argv[i]
        elif sys.argv[i] == '-v1':
            i += 1
            baseline = sys.argv[i]
        elif sys.argv[i] == '-v2':
            i += 1
            caseoutput = sys.argv[i]
        elif sys.argv[i] == '-r':
            i += 1
            report = sys.argv[i]
        i += 1

    # set some default value, baseline path,  output path, report path
    if '' == baseline:
        baseline = os.path.join(BASELINE_DIR,os.path.basename(testsuites))
    if '' == caseoutput:
        caseoutput = OUTPUT_DIR
    if '' == report:
        report = REPORT_DIR

    if process_type == 'caseprocess':
        processCase(hostname, dbname, dbuser, testsuites, caseoutput)
    elif process_type == 'generatereport':
        generateReport(baseline, caseoutput, report)
    elif process_type == 'testex':
        runRegression(hostname, dbname, dbuser, testsuites, baseline, caseoutput, report)