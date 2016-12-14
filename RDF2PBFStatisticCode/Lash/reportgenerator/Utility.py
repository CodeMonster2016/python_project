#-------------------------------------------------------------------------------
# Name:        Utility
# Purpose:
#
# Author:      qfding
#
# Created:     30-01-2013
# Copyright:   (c) qfding 2013
# Licence:     <your licence>
#-------------------------------------------------------------------------------
#!/usr/bin/env python
#coding=utf-8
import os
import sys
import platform
import commands

WINDOWS = "WINDOWS"
LINUX = "LINUX"

class Utility:
    def __init__(self):
        pass

    # run command line for windows or linux
    # @para cmd: string command line
    @staticmethod
    def run_cmd(cmd):
        print cmd
        os.system(cmd)

    # print log to screen or log file
    # @para log: string log
    # @para log_file: file object, default is None
    @staticmethod
    def print_log(log,log_file=None):
        str_log = "["+Utility.time_stamp()+"] "+log
        print str_log
        if log_file:
            log_file.write(str_log+LF)

    @staticmethod
    def time_stamp():
        return time.strftime("%Y%m%d%H%M%S")

    # cp file, folder and subfolder
    # @para src_path: string
    # @para dest_path: string
    @staticmethod
    def copy(src_path,dest_path):
        copy = ""
        if platform.system().upper() == WINDOWS:
            dest_path = os.path.join(dest_path,os.path.split(src_path)[1])
            if not os.path.exists(dest_path):
                os.makedirs(dest_path)
            copy = "xcopy /YS"
        else:
            copy = "cp -r"
        Utility.run_cmd(copy+" "+src_path+" "+dest_path)

if __name__ == '__main__':
    #some test case
    pass
