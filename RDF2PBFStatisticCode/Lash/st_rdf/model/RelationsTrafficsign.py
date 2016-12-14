#-------------------------------------------------------------------------------
# Name:        RelationsTrafficsign model
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
GLOBAL_KEY_PREFIX = "relations_traffic_sign_"
CSV_SEP           = '`'
LF                = '\n'

#(key, category, function)
STATISTIC_KEYS    = (
("type",False,"type"),
("location",True,"location"),
("traffic_sign", True, "traffic_sign"),
("hazard", True, "hazard"),
("traffic_sign:type", True, "traffic_sign_type"),
("traffic_sign:priority", True, "traffic_sign_priority"),
("incline", False, "incline"),
("weather", True, "weather"),
("calc_importance", False, "calc_importance")
)

class RelationsTrafficsign(Record):
    def __init__(self, region):
        Record.__init__(self)
        self.dump_file = os.path.join(ROOT_DIR, "temporary", self.__class__.__name__)
        self.stat      = {}
        self.region    = region

    def dump2file(self):
        cmd = "SELECT \
DISTINCT(rc.condition_id), \
rc.condition_type, \
rcda.signal_sign_location, \
rcda.traffic_sign_type, \
rcda.gen_warning_sign_type, \
rcda.traffic_sign_category, \
rcda.traffic_sign_subcategory, \
rcda.sign_vehicle_truck, \
rcda.sign_vehicle_heavy_truck, \
rcda.sign_vehicle_bus, \
rcda.sign_vehicle_auto_trailer, \
rcda.sign_vehicle_motorhome, \
rcda.sign_vehicle_motorcycle, \
rcda.traffic_sign_value, \
rcda.weather_type, \
rcda.importance_ind \
FROM \
public.rdf_condition AS rc LEFT JOIN public.rdf_nav_strand AS rns ON rns.nav_strand_id=rc.nav_strand_id \
LEFT JOIN public.rdf_nav_link AS rnl ON rns.link_id = rnl.link_id \
LEFT JOIN public.rdf_condition_driver_alert AS rcda ON rcda.condition_id=rc.condition_id \
WHERE rc.condition_type='17' AND rnl.iso_country_code IN (%s)"%(REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX))
        print cmd
        self.cursor.copy_expert("COPY (%s) TO STDOUT DELIMITER '`'"%(cmd),open(self.dump_file,"w"))

    def get_statistic(self):
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
        if self.stat.has_key(key):
            self.stat[key] += 1
        else:
            self.stat[key] = 1

    # all statistic method
    def __get_type(self,keys,line):
        if '\N' != line[0]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_location(self,keys,line):
        location_dict = {'1':'Right','2':'Left','3':'Overhead'}
        if location_dict.has_key(line[2]):
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(location_dict.get(line[2])) or ""))


    def __get_traffic_sign(self,keys,line):
        traffic_sign_dict = {'1':'begin_overtaking',
                             '2':'end_overtaking',
                             '3':'protected_overtaking_extra_lane',
                             '4':'protected_overtaking_extra_lane_right_side',
                             '5':'protected_overtaking_extra_lane_left_side',
                             '6':'lane_merge_right','7':'lane_merge_left',
                             '8':'lane_merge_center',
                             '9':'railway_crossing_protected',
                             '10':'railway_crossing_unprotected',
                             '11':'road_narrows',
                             '12':'sharp_curve_left',
                             '13':'sharp_curve_right',
                             '14':'winding_road_starting_left',
                             '15':'winding_road_starting_right',
                             '16':'begin_overtaking_trucks',
                             '17':'end_overtaking_trucks',
                             '18':'steep_hill_upwards',
                             '19':'steep_hill_downwards',
                             '20':'stop',
                             '21':'lateral_wind',
                             '22':'general_warning_sign',
                             '23':'risk_of_grounding',
                             '24':'general_curve',
                             '25':'end_of_all_restrictions',
                             '26':'general_hill',
                             '27':'animal_crossing',
                             '28':'icy_conditions',
                             '29':'slippery_road',
                             '30':'falling_rocks',
                             '31':'school_zone',
                             '32':'tramway_crossing',
                             '33':'congestion_hazard',
                             '34':'accident_hazard',
                             '35':'priority_over_oncoming_traffic',
                             '36':'yield_to_oncoming_traffic',
                             '37':'crossing_with_priority_from_right',
                             '41':'pedestrian_crossing',
                             '42':'yield',
                             '43':'double_hairpin',
                             '44':'triple_hairpin',
                             '45':'embankment',
                             '46':'two_way_traffic',
                             '47':'urban_area',
                             '48':'hump_bridge',
                             '49':'uneven_road',
                             '50':'flood_area',
                             '51':'obstacle',
                             '52':'horn_sign',
                             '53':'begin_no_engine_brake',
                             '54':'end_no_engine_brake',
                             '55':'no_idling',
                             '56':'truck_roll_over',
                             '57':'begin_low_gear',
                             '58':'end_low_gear',
                             '59':'bicycle_crossing',
                             '60':'yield_to_bicycles'}
        if traffic_sign_dict.has_key(line[3]):
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(traffic_sign_dict.get(line[3])) or ""))

    def __get_hazard(self,keys,line):
        hazard_dict = {'1':'object_overhang','2':'risk_of_grounding','3':'animal_crossing','4':'accident_hazard',}
        if hazard_dict.has_key(line[4]):
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(hazard_dict.get(line[4])) or ""))

    def __get_traffic_sign_type(self,keys,line):
        traffic_sign_type_dict = {'1':'regulatory','2':'informative','3':'warning'}
        if traffic_sign_type_dict.has_key(line[5]):
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(traffic_sign_type_dict.get(line[5])) or ""))

    def __get_traffic_sign_priority(self,keys,line):
        if '1' == line[6]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('yes') or ""))

    def __get_applicable_to(self,keys,line):
        if 'Y' == line[7]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%("truck") or ""))
        if 'Y' == line[8]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%("hgv") or ""))
        if 'Y' == line[9]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%("bus") or ""))
        if 'Y' == line[10]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%("trailer") or ""))
        if 'Y' == line[11]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%("motorhome") or ""))
        if 'Y' == line[12]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%("motorcycle") or ""))

    def __get_incline(self,keys,line):
        if '\N' != line[13]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_weather(self,keys,line):
        weather = None
        if '1' == line[14]:
            weather = 'rain'
        elif '2' == line[14]:
            weather = 'snow'
        elif '3' == line[14]:
            weather = 'fog'
        if None != weather:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(weather) or ""))

    def __get_calc_importance(self,keys,line):
        if '\N' != line[15] and '0' != line[15] and '20' == line[13]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

if __name__ == "__main__":
    # use to test this model
    bg = datetime.datetime.now()
    stat =  RelationsTrafficsign('na').get_statistic()
    keys = stat.keys()
    print "==>"
    print "{%s}"%(",".join(map(lambda px: "\"%s\":%s"%(px,stat[px]) ,keys)))
    print "<=="
    ed = datetime.datetime.now()
    print "Cost time:"+str(ed - bg)
