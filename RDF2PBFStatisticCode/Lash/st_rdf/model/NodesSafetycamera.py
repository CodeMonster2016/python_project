#-------------------------------------------------------------------------------
# Name:        NodesSafetycamera model
# Purpose:     this model is used to mapping the
#              columns: [ ]
#
# Author:      rex
#
# Created:     10/12/2015
# Copyright:   (c) rex 2015
# Licence:     <your licence>
#-------------------------------------------------------------------------------

from record import Record
from constants import *
import os
import sys
import datetime
import json

ROOT_DIR          = os.path.join(os.path.dirname(os.path.abspath(__file__)),"..")
GLOBAL_KEY_PREFIX = "nodes_safety_camera_node_"
CSV_SEP           = '`'
LF                = '\n'

#(key, category, function)
STATISTIC_KEYS    = (("type",False,"type"),
("link_id",False,"link_id"),
("cam_type_id",True,"cam_type_id"),
("cam_type",False,"cam_type"))

class NodesSafetycamera(Record):
    def __init__(self, region):
        Record.__init__(self)
        self.dump_file = os.path.join(ROOT_DIR, "temporary", self.__class__.__name__)
        self.stat      = {}
        self.region    = region

    def dump2file(self):
        cmd = "SELECT \
DISTINCT(xscp.poi_entity_id_text), \
xscp.linkid_text, \
xscp.cameratype_text \
FROM \
public.xml_safety_camera_poi AS xscp \
WHERE xscp.countrycode_text IN (%s)"%(REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX))
        print cmd
        self.cursor.copy_expert("COPY (%s) TO STDOUT DELIMITER '`'"%(cmd),open(self.dump_file,"w"))

    def get_statistic(self):
        try:
            self.dump2file()
        except:
            print "Some table or schema don't exist! Please check the upper sql"
            return {}
        processcount = 0
        with open(self.dump_file, "r",1024*1024*1024) as csv_f:
            for line in csv_f:
                line = line.rstrip()
                line_p = line.split(CSV_SEP)
                if len(line_p) < 1:
                    continue
                self.__statistic(line_p)
                processcount += 1
                if processcount%5000 == 0:
                    print "\rProcess index [ "+str(processcount)+" ]",
            print "\rProcess index [ "+str(processcount)+" ]",
        # write to file
        with open(os.path.join(ROOT_DIR, "output", "stat", self.__class__.__name__), 'w') as stf:
            stf.write(json.dumps(self.stat))
        return self.stat

    def __statistic(self,line):
        for keys in STATISTIC_KEYS:
            try:
                getattr(self,'_NodesSafetycamera__get_'+keys[2])(keys,line)
            except:
                print "The statistic [ %s ] didn't exist"%(keys[2])
                print ("Unexpected error:[ NodesSafetycamera.py->__statistic] "+str(sys.exc_info()))

    def __count(self,key):
        if self.stat.has_key(key):
            self.stat[key] += 1
        else:
            self.stat[key] = 1

    # all statistic method
    def __get_type(self,keys,line):
        if '\N' != line[0]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_link_id(self,keys,line):
        if '\N' != line[1]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_cam_type_id(self,keys,line):
        cam_type_id = None
        if 'Speed' == line[2]:
            cam_type_id = '1'
        elif 'RedLight' == line[2]:
            cam_type_id = '2'
        elif 'BusLane' == line[2]:
            cam_type_id = '8'
        elif 'RedLightAndSpeed' == line[2]:
            cam_type_id = '16'
        elif 'SectionStart' == line[2]:
            cam_type_id = '17'
        elif 'SectionEnd' == line[2]:
            cam_type_id = '18'
        elif 'Distance' == line[2]:
            cam_type_id = '19'

        if None != cam_type_id:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(cam_type_id) or ""))

    def __get_cam_type(self,keys,line):
        if '\N' != line[2]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

if __name__ == "__main__":
    # use to test this model
    bg = datetime.datetime.now()
    stat =  NodesSafetycamera('na').get_statistic()
    keys = stat.keys()
    print "==>"
    print "{%s}"%(",".join(map(lambda px: "\"%s\":%s"%(px,stat[px]) ,keys)))
    print "<=="
    ed = datetime.datetime.now()
    print "Cost time:"+str(ed - bg)
