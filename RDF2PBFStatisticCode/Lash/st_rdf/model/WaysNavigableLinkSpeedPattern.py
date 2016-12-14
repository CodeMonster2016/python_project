#-------------------------------------------------------------------------------
# Name:        WaysNavigableLinkGeom model
# Purpose:     this model is used to mapping the rdf_nav_link, rdf_link and rdf_access
#              columns: [ link_id,iso_country_code,access_id,status_id,functional_class,controlled_access,travel_direction,boat_ferry,
#                         rail_ferry,multi_digitized,divider,divider_legal,frontage,paved,ramp,private,tollway,poi_access,intersection_category,
#                         speed_category,lane_category,coverage_indicator,from_ref_num_lanes,to_ref_num_lanes,physical_num_lanes,
#                         from_ref_speed_limit,to_ref_speed_limit,speed_limit_source,low_mobility,public_access,grade_category,confidence_level_rating,
#                         pedestrian_preferred,limited_access_road,road_class,overpass_underpassrdf,
#                         bridge, tunnel,
#                         automobiles, buses, taxis, carpools, pedestrians, trucks, deliveries, emergency_vehicles, through_traffic, motorcycles]
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
STATISTIC_KEYS    = (
("spd_id:t",False,"spd_id_t"),
("spd_kph:t", False, "spd_kph_t"),
("spd_id:f",False,"spd_id_f"),
("spd_kph:f", False, "spd_kph_f")
)

class WaysNavigableLinkSpeedPattern(Record):
    def __init__(self, region):
        Record.__init__(self)
        self.navlink_file = os.path.join(ROOT_DIR, "temporary", self.__class__.__name__)
        self.stat         = {}
        self.region       = region

    def dump2file(self):
        cmd = "SELECT \
tpm.link_id, \
tpm.direction, \
tpm.profile_id, \
tpm.kph \
FROM \
traffic_pattern.mapping AS tpm LEFT JOIN public.rdf_nav_link as rnl ON tpm.link_id=rnl.link_id \
WHERE rnl.iso_country_code in (%s)"%(REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX))
        print cmd
        self.cursor.copy_expert("COPY (%s) TO STDOUT DELIMITER '`'"%(cmd),open(self.navlink_file,"w"))

    def get_statistic(self):
        try:
            self.dump2file()
        except:
            print "Some table or schema don't exist! Please check the upper sql"
            return {}
        processcount = 0
        with open(self.navlink_file, "r",1024*1024*1024) as csv_f:
            for line in csv_f:
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
        if self.stat.has_key(key):
            self.stat[key] += 1
        else:
            self.stat[key] = 1

    # all statistic method
    def __get_spd_id_t(self,keys,line):
        if '\N' != line[0] and 'T' == line[1] and '\N' != line[2] :
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_spd_kph_t(self,keys,line):
        if '\N' != line[0] and 'T' == line[1] and '\N' != line[3] :
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_spd_id_f(self,keys,line):
        if '\N' != line[0] and 'F' == line[1] and '\N' != line[2] :
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_spd_kph_f(self,keys,line):
        if '\N' != line[0] and 'F' == line[1] and '\N' != line[3] :
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

if __name__ == "__main__":
    # use to test this model
    bg = datetime.datetime.now()
    navlink_stat =  WaysNavigableLinkSpeedPattern('na').get_statistic()
    keys = navlink_stat.keys()
    print "==>"
    print "{%s}"%(",".join(map(lambda px: "\"%s\":%s"%(px,navlink_stat[px]) ,keys)))
    print "<=="
    ed = datetime.datetime.now()
    print "Cost time:"+str(ed - bg)
