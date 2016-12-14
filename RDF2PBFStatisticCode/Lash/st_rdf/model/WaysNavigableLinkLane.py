#-------------------------------------------------------------------------------
# Name:        WaysNavigableLinkLane model
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
GLOBAL_KEY_PREFIX = "ways_navlink_"
CSV_SEP           = '`'
LF                = '\n'

#(key, category, function)
STATISTIC_KEYS    = (("oneway:lanes",False,"oneway_lanes"),
("type:lanes",False,"type_lanes"),
("divider:lanes",False,"divider_lanes"),
("turn:lanes",False,"turn_lanes"))

class WaysNavigableLinkLane(Record):
    def __init__(self, region):
        Record.__init__(self)
        self.dump_file = os.path.join(ROOT_DIR, "temporary", self.__class__.__name__)
        self.stat      = {}
        self.region    = region

    def dump2file(self):
        cmd = "SELECT \
DISTINCT(rl.link_id), \
COUNT(rl.lane_id) as lane_num \
FROM \
public.rdf_lane AS rl LEFT JOIN public.rdf_nav_link AS rnl ON rnl.link_id=rl.link_id \
WHERE rnl.iso_country_code IN (%s) \
GROUP BY rl.link_id HAVING COUNT(rl.lane_id) > 1"%(REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX))
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
                getattr(self,'_WaysNavigableLinkLane__get_'+keys[2])(keys,line)
            except:
                print "The statistic [ %s ] didn't exist"%(keys[2])
                print ("Unexpected error:[ WaysNavigableLinkLane.py->__statistic] "+str(sys.exc_info()))

    def __count(self,key):
        if self.stat.has_key(key):
            self.stat[key] += 1
        else:
            self.stat[key] = 1

    # all statistic method
    def __get_oneway_lanes(self,keys,line):
        if '\N' != line[0]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_type_lanes(self,keys,line):
        if '\N' != line[0]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_divider_lanes(self,keys,line):
        if '\N' != line[0]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_turn_lanes(self,keys,line):
        if '\N' != line[0]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

if __name__ == "__main__":
    # use to test this model
    bg = datetime.datetime.now()
    stat =  WaysNavigableLinkLane('na').get_statistic()
    keys = stat.keys()
    print "==>"
    print "{%s}"%(",".join(map(lambda px: "\"%s\":%s"%(px,stat[px]) ,keys)))
    print "<=="
    ed = datetime.datetime.now()
    print "Cost time:"+str(ed - bg)
