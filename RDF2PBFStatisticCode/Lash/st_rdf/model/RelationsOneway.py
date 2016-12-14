#-------------------------------------------------------------------------------
# Name:        RelationsOneway model
# Purpose:     this model is used to mapping the
#              columns: [ ]
#
# Author:      rex
#
# Created:     2016/01/20
# Copyright:   (c) rex 2016
# Licence:     <your licence>
#-------------------------------------------------------------------------------

from record import Record
from constants import *
import os
import sys
import datetime
import json

ROOT_DIR          = os.path.join(os.path.dirname(os.path.abspath(__file__)),"..")
GLOBAL_KEY_PREFIX = "relations_oneway_"
CSV_SEP           = '`'
LF                = '\n'

#(key, category, function)
STATISTIC_KEYS    = (
("type",False,"type"),
("oneway",True,"oneway"),
)

class RelationsOneway(Record):
    def __init__(self, region):
        Record.__init__(self)
        self.dump_file = os.path.join(ROOT_DIR, "temporary", self.__class__.__name__)
        self.stat      = {}
        self.region    = region

    def dump_navstrand(self):
        cmd = "SELECT \
DISTINCT(rc.condition_id), \
rc.condition_type, \
rcdt.bearing \
FROM \
public.rdf_condition AS rc LEFT JOIN public.rdf_nav_strand AS rns ON rns.nav_strand_id=rc.nav_strand_id \
LEFT JOIN public.rdf_nav_link AS rnl ON rns.link_id = rnl.link_id \
LEFT JOIN public.rdf_condition_direction_travel AS rcdt ON rcdt.condition_id=rc.condition_id \
WHERE rc.condition_type='5' AND rnl.iso_country_code IN (%s)"%(REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX))
        print cmd
        f = "%s_navstrand"%(self.dump_file)
        self.cursor.copy_expert("COPY (%s) TO STDOUT DELIMITER '`'"%(cmd),open(f,"w"))
        return f

    def dump_lanenavstrand(self):
        cmd = "SELECT \
DISTINCT(rc.condition_id), \
rc.condition_type, \
rcdt.bearing \
FROM \
public.rdf_condition AS rc LEFT JOIN public.rdf_lane_nav_strand AS rlns ON rlns.condition_id=rc.condition_id \
LEFT JOIN public.rdf_lane AS rl ON rl.lane_id = rlns.lane_id \
LEFT JOIN public.rdf_nav_link AS rnl ON rl.link_id = rnl.link_id \
LEFT JOIN public.rdf_condition_direction_travel AS rcdt ON rcdt.condition_id=rc.condition_id \
WHERE rc.condition_type='5' AND rnl.iso_country_code IN (%s)"%(REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX))
        print cmd
        f = "%s_lanenavstrand"%(self.dump_file)
        self.cursor.copy_expert("COPY (%s) TO STDOUT DELIMITER '`'"%(cmd),open(f,"w"))
        return f

    def get_statistic(self):
        try:
            self.__check_file(self.dump_navstrand())
            self.__check_file(self.dump_lanenavstrand())
        except:
            print "Oops! Some table or schema don't exist! Please check the upper sql"
            print "Unexpected error:[ %s.py->%s] %s"%(self.__class__.__name__, 'get_statistic', str(sys.exc_info()))
            return {}
        # write to file
        with open(os.path.join(ROOT_DIR, "output", "stat", self.__class__.__name__), 'w') as stf:
            stf.write(json.dumps(self.stat))
        return self.stat

    def __check_file(self, f):
        processcount = 0
        with open(f, "r",1024*1024*1024) as csv_f:
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

    def __statistic(self,line):
        for keys in STATISTIC_KEYS:
            try:
                getattr(self,'_%s__get_%s'%(self.__class__.__name__,keys[2]))(keys,line)
            except:
                print "The statistic [ %s ] didn't exist"%(keys[2])
                print "Unexpected error:[ %s.py->%s] %s"%(self.__class__.__name__, '__statistic', str(sys.exc_info()))

    def __count(self,key):
        if self.stat.has_key(key):
            self.stat[key] += 1
        else:
            self.stat[key] = 1

    # all statistic method
    def __get_type(self,keys,line):
        if '\N' != line[0]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_oneway(self,keys,line):
        mp = {'1':'yes','2':'-1','3':'no'}
        if mp.has_key(line[2]):
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(mp.get(line[2])) or ""))


if __name__ == "__main__":
    # use to test this model
    bg = datetime.datetime.now()
    stat =  RelationsOneway('na').get_statistic()
    keys = stat.keys()
    print "==>"
    print "{%s}"%(",".join(map(lambda px: "\"%s\":%s"%(px,stat[px]) ,keys)))
    print "<=="
    ed = datetime.datetime.now()
    print "Cost time:"+str(ed - bg)
