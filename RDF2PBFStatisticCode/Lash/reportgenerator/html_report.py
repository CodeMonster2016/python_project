#!/usr/bin/env python
# -*- coding: utf-8 -*-
#-------------------------------------------------------------------------------
# Name:        create html of regression test
# Purpose:
#
# Author:      qfding
#
# Created:     27-02-2013
# Copyright:   (c) qfding 2013
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import os

import sys
import urllib
import time
reload(sys)
sys.setdefaultencoding('utf8')

TITLE = "Statistic Compare Report"

BODY_HEAD = "<div class='header'><h1 class='title'>"+TITLE+"</h1></div>"
BODY_FOOTER = "<div class='footer'>Copyright "+time.strftime('%Y-%m-%d %H:%M:%S')+" @ TeleNav.com</div>"

FAIL = "Fail"
PASS = "Pass"
CONDITION_NA = "condition N/A"
UNAVAILABLE = "unavailable "

HTML_PATH_SEQ    = "/"
URL_ESCAPE_CHARS = {'#':'%23'}

TEST_SUITES_INDEX_HEAD              = ("No.","Test Suites","Diff Num","Status")
TEST_SUITES_INDEX_HEAD_COLUMN_WIDTH = {TEST_SUITES_INDEX_HEAD[0]:"10%",
                                       TEST_SUITES_INDEX_HEAD[1]:"70%",
                                       TEST_SUITES_INDEX_HEAD[2]:"10%",
                                       TEST_SUITES_INDEX_HEAD[3]:"10%"}

INPUT_SET_INDEX_HEAD              = ("No.","Input Sets","Diff Num","Status")
INPUT_SET_INDEX_HEAD_COLUMN_WIDTH = {INPUT_SET_INDEX_HEAD[0]:"10%",
                                     INPUT_SET_INDEX_HEAD[1]:"70%",
                                     INPUT_SET_INDEX_HEAD[2]:"10%",
                                     INPUT_SET_INDEX_HEAD[3]:"10%"}

CASE_INDEX_HEAD               = ("No.","Case Name","Diff Num","Status")
CASE_INDEX_HEAD_COLUMN_WIDTH  = {CASE_INDEX_HEAD[0]:"10%",
                                 CASE_INDEX_HEAD[1]:"70%",
                                 CASE_INDEX_HEAD[2]:"10%",
                                 CASE_INDEX_HEAD[3]:"10%"}

CASE_DETAIL_HEAD              = ("Field Name","RDF Value","OSM Value","Condition","Status")
CASE_DETAIL_HEAD_COLUMN_WIDTH = {CASE_DETAIL_HEAD[0]:"42%",
                                 CASE_DETAIL_HEAD[1]:"20%",
                                 CASE_DETAIL_HEAD[2]:"20%",
                                 CASE_DETAIL_HEAD[3]:"8%",
                                 CASE_DETAIL_HEAD[4]:"10%"}

TABLE_COLUMN_WIDTH_INFO = {TEST_SUITES_INDEX_HEAD:TEST_SUITES_INDEX_HEAD_COLUMN_WIDTH,
                           INPUT_SET_INDEX_HEAD:INPUT_SET_INDEX_HEAD_COLUMN_WIDTH,
                           CASE_INDEX_HEAD:CASE_INDEX_HEAD_COLUMN_WIDTH,
                           CASE_DETAIL_HEAD:CASE_DETAIL_HEAD_COLUMN_WIDTH}

class HtmlReport:
    def __init__(self):
        self.__temporary = 0

    #[field_name,ref_value,out_value,condition,status]
    def create_case_page(self,case_page_path,data,report_root,head):
        f = open(case_page_path,'w')
        f.write(self.__case_detail_page(case_page_path,data,report_root,head))
        f.close()

    def __case_detail_page(self,case_page_path,data,report_root,head):
        return self.__html_head(report_root,case_page_path)+"""<body>"""+BODY_HEAD+"""<div class='layout'>"""+\
            self.__nav_bar(report_root,case_page_path)+\
            self.__generate_case_table(head,data)+\
            """</div>"""+BODY_FOOTER+"""</body></html>"""

    def __generate_case_table(self,head,data):
        #table head
        #[field_name,ref_value,out_value,condition,status]
        table_head = "<thead><tr>"+"\n".join(map(lambda px:"<th style='width:"+TABLE_COLUMN_WIDTH_INFO.get(head).get(px)+"'>"+px+"</th>",head))+"</tr></thead>"
        #table body
        table_body = "<tbody>"
        table_body+="\n".join(map(lambda px:"<tr class='"+self.__cycle("even","odd")+" "+
                                                       (px[4] in [FAIL,PASS] and px[4].lower() or (px[4]=="" and UNAVAILABLE or ""))+"'>"+
                                                "<td title='"+px[0]+"'>"+px[0]+"</td>"+
                                                "<td title='"+px[1]+"'>"+px[1]+"</td>"+
                                                "<td title='"+px[2]+"'>"+px[2]+"</td>"+
                                                "<td title='"+px[3]+"'>"+px[3]+"</td>"+
                                                "<td title='"+px[4]+"'>"+px[4]+"</td>"+"</tr>",data))
        table_body += "</tbody>"
        #table
        return "<table width='100%' cellspacing='0' border='0' class='index_list'>"+table_head+table_body+"</table>"

    def create_index_page(self,index_path,data_list,report_root,index_head,case_index=False):
        f = open(index_path,'w')
        f.write(self.__index_page(index_path,data_list,report_root,index_head,case_index))
        f.close()

    def __index_page(self,index_path,data_list,report_root,index_head,case_index=False):
        #prepare data_list [name,diff_num]
        count = 0
        data = []
        for datum in data_list:
            count+=1
            data.append([str(count),datum[0],str(datum[1]),(len(datum) > 2 and datum[2] or "") or (datum[1]!=0 and FAIL or PASS)])
        #data [No.,name,diff_num,status]
        sorted(data,key=lambda px:px[3],reverse = True)
        return self.__html_head(report_root,index_path)+"""<body>"""+BODY_HEAD+"""<div class='layout'>"""+\
            self.__nav_bar(report_root,index_path)+\
            self.__generate_index_table(index_head,data,case_index)+\
            """</div>"""+BODY_FOOTER+"""</body></html>"""

    def __generate_index_table(self,index_head,data,case_index=False):
        #table head
        #[No.,Name,Diff_Num,Status]
        table_head = "<thead><tr>"+"\n".join(map(lambda px:"<th style='width:"+TABLE_COLUMN_WIDTH_INFO.get(index_head).get(px)+"'>"+px+"</th>",index_head))+"</tr></thead>"
        #table body
        table_body = "<tbody>"
        table_body+="\n".join(map(lambda px:"<tr class='"+self.__cycle("even","odd")+" "+(px[3].upper() in [FAIL.upper(),PASS.upper()] and px[3].lower() or UNAVAILABLE)+"'>"+
                                            "<td>"+self.__link(case_index and px[1]+".html" or px[1]+"/index.html",px[0])+"</td>"+
                                            "<td>"+self.__link(case_index and px[1]+".html" or px[1]+"/index.html",px[1])+"</td>"+
                                            "<td>"+self.__link(case_index and px[1]+".html" or px[1]+"/index.html",px[2])+"</td>"+
                                            "<td>"+self.__link(case_index and px[1]+".html" or px[1]+"/index.html",px[3])+"</td>"
                                             +"</tr>",data))
        table_body += "</tbody>"
        #table
        return "<table width='100%' cellspacing='0' border='0' class='index_list'>"+table_head+table_body+"</table>"

    def __nav_bar(self,report_root,report_path):
        #report_path = os.path.dirname(report_path)
        paths = report_path.replace(report_root,"").split(os.path.sep)
        path_len = len(paths)
        page_paths = []
        for path_index in range(0,len(paths)):
            if paths[path_index] == "":
                page_paths.append(["Home", HTML_PATH_SEQ.join([".."]*(path_len-path_index-2)+["index.html"])])
            elif paths[path_index] == "index.html":
                pass
            elif paths[path_index].endswith(".html"):
                page_paths.append([os.path.splitext(paths[path_index])[0], HTML_PATH_SEQ.join([".."]*(path_len-path_index-2)+[paths[path_index]])])
            else:
                page_paths.append([paths[path_index], HTML_PATH_SEQ.join([".."]*(path_len-path_index-2)+["index.html"])])
        #page_paths[-1]=="index.html" and page_paths[:-1] or os.path.splitext(page_paths[-1])[0]
        page_link_paths =  map(lambda px:"<a href='"+self.__link_string(px[1])+"'>"+px[0]+"</a>",page_paths)
        return "<span class='nav_path'>Path> / "+" / ".join(page_link_paths)+"</span>"

    def __link(self,href,name):
        return "<a style='color:#000;' href='"+self.__link_string(href)+"'>"+name+"</a>"

    def __cycle(self,even,odd):
        self.__temporary+=1
        return (even,odd)[self.__temporary%2]

    def __html_head(self,report_root,report_path):
        return """<!DOCTYPE html>
<html lang="en" xmlns="http://www.w3.org/1999/xhtml" xml:lang="en">
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>"""+TITLE+"""</title>
"""+self.__assets(report_root,report_path)+"""
</head>
"""

    def __assets(self,report_root,report_path):
        paths = report_path.replace(report_root,"").split(os.path.sep)
        path_len = len(paths)
        relative_path_prefix = HTML_PATH_SEQ.join([".."]*(path_len-2))
        relative_path_prefix = relative_path_prefix and relative_path_prefix+HTML_PATH_SEQ
        return "<script type='text/javascript' src='"+relative_path_prefix+"assets/javascripts/jquery.min.js'></script>"+\
               "<script type='text/javascript' src='"+relative_path_prefix+"assets/javascripts/jquery.tablesorter.min.js'></script>"+\
               "<script type='text/javascript' src='"+relative_path_prefix+"assets/javascripts/external_event.js'></script>"+\
               "<link type='text/css' rel='stylesheet' media='screen' href='"+relative_path_prefix+"assets/stylesheets/main.css'>"+\
               "<link type='text/css' rel='stylesheet' media='screen' href='"+relative_path_prefix+"assets/stylesheets/tablesorter/style.css'>"

    def __link_string(self, str_link):
        return urllib.quote(str_link)



if __name__ == '__main__':
    test_html = r'D:\Workspace\txd_regression\current\regressionex\report\index.html'
    report_root = r'D:\Workspace\txd_regression\current\regressionex\report'
    data_list = [["abc1",12],["abc2",120],["abc3",1],["abc4",0],["abc5",11]]
    HtmlReport().create_index_page(test_html,data_list,report_root,TEST_SUITES_INDEX_HEAD)
