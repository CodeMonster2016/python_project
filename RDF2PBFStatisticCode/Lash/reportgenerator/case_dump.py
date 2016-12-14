#!/usr/bin/env python
# -*- coding: utf-8 -*-
#-------------------------------------------------------------------------------
# Name:        case dump into hash
# Purpose:
#
# Author:      qfding
#
# Created:     27-02-2013
# Copyright:   (c) qfding 2013
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import os
import json

CASE_FILE_TYPE_CFG  = "cfg"
CASE_FILE_TYPE_JSON = "json"

class CaseDump:
    def __init__(self,case_file,filetype=CASE_FILE_TYPE_CFG):
        self.case_file = case_file
        self.type      = filetype

    #case key=>value
    def dump(self):
        if self.type == CASE_FILE_TYPE_CFG:
            return self.dump_cfg()
        if self.type == CASE_FILE_TYPE_JSON:
          return self.dump_json()

    def dump_cfg(self):
        if not os.path.exists(self.case_file):
            return {}
        case = open(self.case_file,'r')
        case_lines = case.readlines()
        case.close()
        return case_lines and reduce(lambda px,py: px.update(py) or px, map(lambda px:self.__parse_case_line(px),case_lines)) or {}

    def dump_json(self):
        if not os.path.exists(self.case_file):
            return {}
        with open(self.case_file) as json_f:
            jd = json.load(json_f)
        #filter some keys
        filters = []
        for filt in filters:
            try:
                del jd[filt]
            except:
                pass
                #print "No this key"
        return jd

    def __parse_case_line(self,line):
        line = line.strip()
        equal_posi = line and line.find("=") or -1
        return -1 != equal_posi and {line[:equal_posi]:line[equal_posi+1:]} or {}

if __name__ == '__main__':
    CaseDump(r'D:\workspace\UniDB_regression\reportgenerator\version.json','json').dump()
