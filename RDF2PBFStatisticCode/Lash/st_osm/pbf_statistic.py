#-------------------------------------------------------------------------------
# Name:        pbf_statistic
# Purpose:
#
# Author:      fwu
#
# Created:     09/12/2015
# Copyright:   (c) TeleNav 2015
# Licence:     <your licence>
#-------------------------------------------------------------------------------

from pbfcounter import PBFCounter
from configreader import ConfigReader
from optparse import OptionParser
import sys
import time
import os
from imposm.parser import OSMParser


global options
parser = OptionParser()
config = ConfigReader()

def main(pbf_file, outpath):

    config.readerConfigure()

    start_time = time.localtime()
    print "PBF Parsing Start at: (%d/%d/%d) [%d:%d:%d]" % (start_time.tm_year, start_time.tm_mon, start_time.tm_mday,
                                                       start_time.tm_hour, start_time.tm_min, start_time.tm_sec)
    start_sec = time.mktime(start_time)

    parsepbf(pbf_file, outpath)

    end_time = time.localtime()
    print "PBF Parsing End at: (%d/%d/%d) [%d:%d:%d]" % (end_time.tm_year, end_time.tm_mon, end_time.tm_mday,
                                                       end_time.tm_hour, end_time.tm_min, end_time.tm_sec)
    end_sec = time.mktime(end_time)
    elapse_sec = end_sec - start_sec
    elps_h = elapse_sec / 3600
    elps_m = (elapse_sec % 3600) / 60
    elps_s = elapse_sec % 60
    print "procedure done, time cost:  %d h:%d m:%d s\n" % (elps_h, elps_m, elps_s)

def optionInit():
    parser.add_option("-p", "--pbf",action='store', dest='ipbf', help='PBF file')
    parser.add_option("-o", "--output",action='store', dest='iout', help='Output dir')

def checkParameter():

    if options.iout and not os.path.isdir(options.iout):
        sys.stderr.write( "[ERROR] parameter -o : "+ options.iout +" is not a directory \n")
        parser.print_help()
        sys.exit(-1)


    temp_ipbf = None

    for file in os.listdir(os.path.join(sys.path[0],'pbf')):
        if file.endswith('pbf'):
            temp_ipbf = os.path.join(sys.path[0],'pbf',file)

    options.ipbf = options.ipbf if options.ipbf else temp_ipbf
    options.iout = options.iout if options.iout else os.path.join(sys.path[0],'output')

    if not os.path.exists(options.ipbf):
        sys.stderr.write( "[ERROR] pbf file does not exists")
        sys.exit(-1)



def parsepbf(pbf_file, outdir):
    ways_counter = PBFCounter(config.getkeylist(), u'ways')
    relations_counter = PBFCounter(config.getkeylist(table = 'RELATIONS'),u'relations')
    nodes_counter = PBFCounter(config.getkeylist(table = 'NODES'),u'nodes')


    print "start parsing ......"
    p = OSMParser(ways_callback=ways_counter.parse_func,relations_callback=relations_counter.parse_func,nodes_callback=nodes_counter.parse_func)
    p.parse(pbf_file)


    print "output results......"
    outputstatistic(ways_counter, os.path.join(outdir,'ways'))
    outputstatistic(relations_counter,os.path.join(outdir,'relations'))
    outputstatistic(nodes_counter,os.path.join(outdir,'nodes'))


def outputstatistic(counter,outfile):
    counter.outputstatistic(outfile)

if __name__ == '__main__':

    global options


    optionInit()
    (options, args) = parser.parse_args(sys.argv)
    checkParameter()

    print 'pbf file: ' + options.ipbf

    main(options.ipbf, options.iout)
