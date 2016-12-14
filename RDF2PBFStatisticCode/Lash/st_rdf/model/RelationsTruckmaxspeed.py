#-------------------------------------------------------------------------------
# Name:        RelationsTruckmaxspeed model
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
GLOBAL_KEY_PREFIX = "relations_truck_maxspeed_"
CSV_SEP           = '`'
LF                = '\n'

#(key, category, function)
STATISTIC_KEYS    = (("type",False,"type"),
("maxspeed:truck:forward",False,"maxspeed_truck_forward"),
("maxspeed:truck:backward", False, "maxspeed_truck_backward"),
("maxspeed:truck", False, "maxspeed_truck"),
("truck_speed_type", True, "truck_speed_type"),
("weather_type", True, "weather_type"),
("maxspeed_type", True, "maxspeed_type"),
("time_override", True, "time_override"),
("weight_dependent", False, "weight_dependent"),
("hazmat_type", False, "hazmat_type"))

class RelationsTruckmaxspeed(Record):
    def __init__(self, region):
        Record.__init__(self)
        self.dump_file = os.path.join(ROOT_DIR, "temporary", self.__class__.__name__)
        self.stat      = {}
        self.region    = region

    def dump2file(self):
        cmd = "SELECT \
DISTINCT(rc.condition_id), \
rc.condition_type, \
rct.direction, \
rct.transport_speed_limit, \
rct.transport_speed_type, \
rct.weather_type, \
rct.speed_limit_type, \
rct.time_override, \
rct.weight_dependent, \
rct.hazardous_material_type \
FROM \
public.rdf_condition AS rc LEFT JOIN public.rdf_nav_strand AS rns ON rns.nav_strand_id=rc.nav_strand_id \
LEFT JOIN public.rdf_nav_link AS rnl ON rns.link_id = rnl.link_id \
LEFT JOIN public.rdf_condition_transport AS rct ON rct.condition_id=rc.condition_id \
WHERE rc.condition_type='25' AND rnl.iso_country_code IN (%s)"%(REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX))
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
                getattr(self,'_RelationsTruckmaxspeed__get_'+keys[2])(keys,line)
            except:
                print "The statistic [ %s ] didn't exist"%(keys[2])
                print ("Unexpected error:[ RelationsTruckmaxspeed.py->__statistic] "+str(sys.exc_info()))

    def __count(self,key):
        if self.stat.has_key(key):
            self.stat[key] += 1
        else:
            self.stat[key] = 1

    # all statistic method
    def __get_type(self,keys,line):
        if '\N' != line[0]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_maxspeed_truck_forward(self,keys,line):
        if '1' == line[2]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_maxspeed_truck_backward(self,keys,line):
        if '2' == line[2]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_maxspeed_truck(self,keys,line):
        if '3' == line[2]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_truck_speed_type(self,keys,line):
        if '\N' != line[4]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[4]) or ""))

    def __get_weather_type(self,keys,line):
        weather = None
        if '1' == line[5]:
            weather = 'rain'
        elif '2' == line[5]:
            weather = 'snow'
        elif '3' == line[5]:
            weather = 'fog'
        if None != weather:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(weather) or ""))

    def __get_maxspeed_type(self,keys,line):
        if '\N' != line[6]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[6]) or ""))

    def __get_time_override(self,keys,line):
        time_override = None
        if '1' == line[7]:
            time_override = 'DAWN_TO_DUSK'
        elif '2' == line[7]:
            time_override = 'DUSK_TO_DAWN'
        if None != time_override:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(time_override) or ""))

    def __get_weight_dependent(self,keys,line):
        if '\N' != line[8]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_hazmat_type(self,keys,line):
        if '\N' != line[9]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

if __name__ == "__main__":
    # use to test this model
    bg = datetime.datetime.now()
    stat =  RelationsTruckmaxspeed('na').get_statistic()
    keys = stat.keys()
    print "==>"
    print "{%s}"%(",".join(map(lambda px: "\"%s\":%s"%(px,stat[px]) ,keys)))
    print "<=="
    ed = datetime.datetime.now()
    print "Cost time:"+str(ed - bg)
