#-------------------------------------------------------------------------------
# Name:        RelationsAdasmaxspeed model
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
GLOBAL_KEY_PREFIX = "relations_adas:maxspeed_"
CSV_SEP           = '`'
LF                = '\n'

#(key, category, function)
STATISTIC_KEYS    = (
("type",False,"type"),
("adas:special_maxspeed:forward", False,"adas_special_maxspeed_forward"),
("adas:special_maxspeed:backward", False, "adas_special_maxspeed_backward"),
("adas:special_maxspeed", False, "adas_special_maxspeed"),
("special_speed_type", True, "special_speed_type"),
("traffic_calming", True, "traffic_calming"),
("depdendent_speed_type", True, "depdendent_speed_type"),
("time override", True, "time_override"),
)

class RelationsAdasmaxspeed(Record):
    def __init__(self, region):
        Record.__init__(self)
        self.dump_file = os.path.join(ROOT_DIR, "temporary", self.__class__.__name__)
        self.stat      = {}
        self.region    = region

    def dump2file(self):
        cmd = "SELECT \
DISTINCT(rc.condition_id), \
rc.condition_type, \
rcs.special_speed_limit, \
rcs.special_speed_type, \
rcs.dependent_speed_type, \
rcs.time_override, \
rcs.direction \
FROM \
public.rdf_condition AS rc LEFT JOIN public.rdf_nav_strand AS rns ON rns.nav_strand_id=rc.nav_strand_id \
LEFT JOIN public.rdf_nav_link AS rnl ON rns.link_id = rnl.link_id \
LEFT JOIN public.rdf_condition_speed AS rcs ON rcs.condition_id=rc.condition_id \
WHERE rc.condition_type='10' AND rnl.iso_country_code IN (%s)"%(REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX))
        print cmd
        self.cursor.copy_expert("COPY (%s) TO STDOUT DELIMITER '`'"%(cmd),open(self.dump_file,"w"))

    def get_statistic(self):
        try:
            self.dump2file()
        except:
            print "Oops! Some table or schema don't exist! Please check the upper sql"
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
                getattr(self,'_RelationsAdasmaxspeed__get_'+keys[2])(keys,line)
            except:
                print "The statistic [ %s ] didn't exist"%(keys[2])
                print ("Unexpected error:[ RelationsAdasmaxspeed.py->__statistic] "+str(sys.exc_info()))

    def __count(self,key):
        if self.stat.has_key(key):
            self.stat[key] += 1
        else:
            self.stat[key] = 1

    # all statistic method
    def __get_type(self,keys,line):
        if '\N' != line[0]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_adas_special_maxspeed_forward(self,keys,line):
        if '1' == line[6] and '\N' != line[2] and '0' != line[2]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_adas_special_maxspeed_backward(self,keys,line):
        if '2' == line[6] and '\N' != line[2] and '0' != line[2]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_adas_special_maxspeed(self,keys,line):
        if '1' != line[6] and '2' != line[6] and '\N' != line[2] and '0' != line[2]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_special_speed_type(self,keys,line):
        mp = {'1':'1','2':'2'}
        if line[3] in mp:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(mp.get(line[3])) or ""))

    def __get_traffic_calming(self,keys,line):
        if '3' == line[3]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('yes') or ""))

    def __get_depdendent_speed_type(self,keys,line):
        mp = {'1':'school','2':'rain','3':'snow','4':'time','5':'seasonal','6':'lane','7':'fog'}
        if line[4] in mp:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(mp.get(line[4])) or ""))

    def __get_time_override(self,keys,line):
        mp = {'1':'DAWN TO DUSK','2':'DUSK TO DAWN'}
        if line[5] in mp:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(mp.get(line[5])) or ""))

if __name__ == "__main__":
    # use to test this model
    bg = datetime.datetime.now()
    stat =  RelationsAdasmaxspeed('na').get_statistic()
    keys = stat.keys()
    print "==>"
    print "{%s}"%(",".join(map(lambda px: "\"%s\":%s"%(px,stat[px]) ,keys)))
    print "<=="
    ed = datetime.datetime.now()
    print "Cost time:"+str(ed - bg)
