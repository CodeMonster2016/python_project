#-------------------------------------------------------------------------------
# Name:        RelationsAdmin model
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
GLOBAL_KEY_PREFIX = "relations_zone_"
CSV_SEP           = '`'
LF                = '\n'

#(key, category, function)
STATISTIC_KEYS    = (
("type",False,"type"),
("zone_type",False,"zone_type"),
("admin_place_id", False, "admin_place_id"),
("iso", True, "iso"),
)

class RelationsZone(Record):
    def __init__(self, region):
        Record.__init__(self)
        self.dump_file = os.path.join(ROOT_DIR, "temporary", self.__class__.__name__)
        self.stat      = {}
        self.region    = region

    def dump2file(self):
        cmd = "SELECT \
DISTINCT(rz.zone_id), \
rz.zone_type, \
rz.admin_place_id, \
rah.iso_country_code \
FROM \
public.rdf_zone AS rz LEFT JOIN public.rdf_admin_hierarchy AS rah ON rah.admin_place_id=rz.admin_place_id \
WHERE rah.iso_country_code IN (%s)"%(REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX))
        print cmd
        self.cursor.copy_expert("COPY (%s) TO STDOUT DELIMITER '`'"%(cmd),open(self.dump_file,"w"))

    def get_statistic(self):
        return {}
        try:
            self.dump2file()
        except:
            print "Some table or schema don't exist! Please check the upper sql"
            print "Unexpected error:[ %s.py->%s] %s"%(self.__class__.__name__, 'get_statistic', str(sys.exc_info()))
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
                getattr(self,'_%s__get_%s'%(self.__class__.__name__,keys[2]))(keys,line)
            except:
                print "The statistic [ %s ] didn't exist"%(keys[2])
                print "Unexpected error:[ %s.py->%s] %s"%(self.__class__.__name__, '__statistic', str(sys.exc_info()))

    def __count(self,key):
        key = "%s%s"%(GLOBAL_KEY_PREFIX,key)
        if self.stat.has_key(key):
            self.stat[key] += 1
        else:
            self.stat[key] = 1

    # all statistic method
    def __get_type(self,keys,line):
        if '\N' != line[0]:
            self.__count(keys[0])

    def __get_zone_type(self,keys,line):
        if '\N' != line[1]:
            self.__count(keys[0])

    def __get_admin_place_id(self,keys,line):
        if '\N' != line[2] and '0' != line[2]:
            self.__count(keys[0])

    def __get_iso(self,keys,line):
        if '\N' != line[3]:
            self.__count("%s%s"%(keys[0],keys[1] and "#%s"%(line[3]) or ""))


if __name__ == "__main__":
    # use to test this model
    bg = datetime.datetime.now()
    stat =  RelationsZone('na').get_statistic()
    keys = stat.keys()
    print "==>"
    print "{%s}"%(",".join(map(lambda px: "\"%s\":%s"%(px,stat[px]) ,keys)))
    print "<=="
    ed = datetime.datetime.now()
    print "Cost time:"+str(ed - bg)
