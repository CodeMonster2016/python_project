#-------------------------------------------------------------------------------
# Name:        WaysNavigableLink model
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
("adas:form_of_way",True,"adas_form_of_way"),
("adas:route_type", False, "adas_route_type"),
("maxspeed", False, "maxspeed"),
("adas:special_maxspeed", False, "adas_special_maxspeed"),
("adas:special_maxspeed:forward", False, "adas_special_maxspeed_forward"),
("adas:special_maxspeed:backward", False, "adas_special_maxspeed_backward"),
("adas:special_maxspeed:type", False, "adas_special_maxspeed_type"),
("speed_unit", True, "speed_unit"),
("adas:divided_road", True, "adas_divided_road"),
("adas:bua", True, "adas_bua"),
("adas:complex_intersection", True, "adas_complex_intersection"),
("adas:driving_side", True, "adas_driving_side"),
("adas:urban", True, "adas_urban"),
("in_process_data", True, "in_process_data")
)

class WaysNavigableLinkAdas(Record):
    def __init__(self, region):
        Record.__init__(self)
        self.navlink_file = os.path.join(ROOT_DIR, "temporary", self.__class__.__name__)
        self.stat         = {}
        self.region       = region

    def dump2file(self):
        cmd = "SELECT \
DISTINCT(rnl.link_id), \
rnl.iso_country_code, \
rnl.access_id, \
rnl.status_id, \
rnl.functional_class, \
rnl.controlled_access, \
rnl.travel_direction, \
rnl.boat_ferry, \
rnl.rail_ferry, \
rnl.multi_digitized, \
rnl.divider, \
rnl.divider_legal, \
rnl.frontage, \
rnl.paved, \
rnl.ramp, \
rnl.private, \
rnl.tollway, \
rnl.poi_access, \
rnl.intersection_category, \
rnl.speed_category, \
rnl.lane_category, \
rnl.coverage_indicator, \
rnl.from_ref_num_lanes, \
rnl.to_ref_num_lanes, \
rnl.physical_num_lanes, \
rnl.from_ref_speed_limit, \
rnl.to_ref_speed_limit, \
rnl.speed_limit_source, \
rnl.low_mobility, \
rnl.public_access, \
rnl.grade_category, \
rnl.confidence_level_rating, \
rnl.pedestrian_preferred, \
rnl.limited_access_road, \
rnl.road_class, \
rnl.overpass_underpass, \
rl.bridge, \
rl.tunnel, \
ra.automobiles, \
ra.buses, \
ra.taxis, \
ra.carpools, \
ra.pedestrians, \
ra.trucks, \
ra.deliveries, \
ra.emergency_vehicles, \
ra.through_traffic, \
ra.motorcycles, \
rnls.urban, \
rnla.carpool_road, \
rnla.supplemental_geo_bitset, \
rl.left_admin_place_id, \
rl.right_admin_place_id, \
rnls.in_process_data, \
adassms.adassm_link_id, \
drcy.speed_limit_unit, \
drcy.driving_side, \
rts.route_type_s \
FROM \
public.rdf_nav_link AS rnl LEFT JOIN public.rdf_link AS rl ON rnl.link_id=rl.link_id \
LEFT JOIN public.rdf_nav_link_attribute AS rnla ON rnl.link_id=rnla.link_id \
LEFT JOIN public.rdf_access AS ra ON ra.access_id=rnl.access_id \
LEFT JOIN public.rdf_nav_link_status AS rnls ON rnls.status_id=rnl.status_id \
LEFT JOIN ( \
  SELECT DISTINCT(rnl.link_id), SUM(rrn.route_type) AS route_type_s \
  FROM public.rdf_nav_link AS rnl, public.rdf_road_link AS rrl, public.rdf_road_name AS rrn \
  WHERE rnl.link_id=rrl.link_id AND \
        rrn.road_name_id=rrl.road_name_id AND \
        rnl.iso_country_code IN (%s) GROUP BY rnl.link_id HAVING SUM(rrn.route_type) > 0 \
) AS rts ON rts.link_id=rnl.link_id \
LEFT JOIN ( \
  SELECT DISTINCT(rcy.iso_country_code),\
         rcy.speed_limit_unit,\
         rcy.driving_side \
  FROM public.rdf_country AS rcy) AS drcy ON drcy.iso_country_code=rnl.iso_country_code \
LEFT JOIN ( \
  SELECT DISTINCT(rnl.link_id) AS adassm_link_id FROM \
  public.rdf_nav_link AS rnl LEFT JOIN public.rdf_nav_strand AS rns ON rns.link_id = rnl.link_id \
  LEFT JOIN public.rdf_condition AS rc ON rns.nav_strand_id=rc.nav_strand_id \
  LEFT JOIN public.rdf_condition_speed AS rcs ON rcs.condition_id=rc.condition_id \
  WHERE rc.condition_type='10') AS adassms ON adassms.adassm_link_id=rnl.link_id \
WHERE rnl.iso_country_code IN (%s)"%(REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX),REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX))
        print cmd
        self.cursor.copy_expert("COPY (%s) TO STDOUT DELIMITER '`'"%(cmd),open(self.navlink_file,"w"))

    def dump_adminplace(self):
        cmd = "SELECT \
DISTINCT(rap.admin_place_id) \
FROM \
public.rdf_admin_place AS rap, public.rdf_admin_hierarchy AS rah \
WHERE rap.admin_type='3110' and rap.admin_place_id = rah.admin_place_id and rah.iso_country_code IN (%s)"%(REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX))
        print cmd
        adminplace_dumpfile = self.navlink_file+"_adminplace"
        self.cursor.copy_expert("COPY (%s) TO STDOUT DELIMITER '`'"%(cmd),open(adminplace_dumpfile,"w"))
        print "Dump Admin place id"
        self.adminplaceids = {}
        processcount = 0
        with open(adminplace_dumpfile, "r",1024*1024*1024) as csv_f:
            for line in csv_f:
                line = line.strip()
                if not line:
                    continue
                self.adminplaceids[line] = None
                processcount += 1
                if processcount%5000 == 0:
                    print "\rProcess index [ "+str(processcount)+" ]",
            print "\rProcess index [ "+str(processcount)+" ]",
        print "Admin place information's initialization is done"

    def get_statistic(self):
        try:
            self.dump2file()
            self.dump_adminplace()
            self.__dump_parking_facility()
        except:
            print "Some table or schema don't exist! Please check the upper sql"
            print "Unexpected error:[ %s.py->%s] %s"%(self.__class__.__name__, 'get_statistic', str(sys.exc_info()))
            return {}
        processcount = 0
        with open(self.navlink_file, "r",1024*1024*1024) as csv_f:
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

    def __dump_parking_facility(self):
        cmd = "SELECT rnl.link_id \
FROM public.rdf_nav_link AS rnl LEFT JOIN public.rdf_location AS rl ON rl.link_id=rnl.link_id \
LEFT JOIN public.rdf_poi_address AS rpa ON rpa.location_id=rl.location_id \
LEFT JOIN public.rdf_poi AS rp ON rp.poi_id=rpa.poi_id \
WHERE rp.cat_id IN (7520,7521,7522) AND rnl.iso_country_code IN (%s) GROUP BY rnl.link_id HAVING COUNT(rnl.link_id) > 0"%(REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX))
        print cmd
        parking_dumpfile = self.navlink_file+"_parking"
        self.cursor.copy_expert("COPY (%s) TO STDOUT DELIMITER '`'"%(cmd),open(parking_dumpfile,"w"))
        print "Dump parking facility id..."
        self.parking_facility = {}
        processcount = 0
        with open(parking_dumpfile, "r",1024*1024*1024) as csv_f:
            for line in csv_f:
                line = line.strip()
                if not line:
                    continue
                self.parking_facility[line] = None
                processcount += 1
                if processcount%5000 == 0:
                    print "\rProcess index [ "+str(processcount)+" ]",
            print "\rProcess index [ "+str(processcount)+" ]",
        print "Parking facility's initialization is done"

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
    def __get_adas_form_of_way(self,keys,line):
        afow = None
        if 'Y' == line[5] and 'Y' == line[14]:
            afow = "9"
        elif  1 == self.__fow(line):
            afow = "1"
        elif 2 == self.__fow(line):
            afow = "2"
        elif 3 == self.__fow(line):
            afow = "3"
        elif 4 == self.__fow(line):
            afow = "4"
        elif 5 == self.__fow(line) or 9 == self.__fow(line) :
            afow = "5"
        elif 10 == self.__fow(line):
            afow = "10"
        elif 11 == self.__fow(line):
            afow = "11"
        elif 12 == self.__fow(line):
            afow = "12"
        elif 13 == self.__fow(line):
            afow = "13"
        elif 15 == self.__fow(line):
            afow = "14"
        else:
            afow = '3'
        if None != afow:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(afow) or ""))

    def __get_adas_route_type(self,keys,line):
        if '\N' != line[57]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_maxspeed(self,keys,line):
        if '\N' != line[25] and '\N' != line[26] and \
           line[25] == line[26] and '0' != line[25] and '998' != line[25]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))
        elif ('\N' != line[25] and '0' != line[25] and '998' != line[25]) and ('\N' == line[26] or '0' == line[26] or '998' == line[26]):
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))
        elif ('\N' == line[25] or '0' == line[25] or '998' == line[25]) and ('\N' != line[26] and '0' != line[26] and '998' != line[26]):
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_adas_special_maxspeed(self,keys,line):
        if '\N' == line[54]:
            return
        if 'T' != line[6] and 'F' != line[6] :
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_adas_special_maxspeed_forward(self,keys,line):
        if '\N' == line[54]:
            return
        if 'F' == line[6]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_adas_special_maxspeed_backward(self,keys,line):
        if '\N' == line[54]:
            return
        if 'T' == line[6]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_adas_special_maxspeed_type(self,keys,line):
        if '\N' != line[54]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_speed_unit(self,keys,line):
        if '\N' != line[55]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[55]) or ""))

    def __get_adas_divided_road(self,keys,line):
        if  ('Y' == line[9] and 'Y' == line[5]) or \
            ('1' == line[10] or '2' == line[10] or 'A' == line[10] or 'L' == line[10]):
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('yes') or ""))

    def __get_adas_bua(self,keys,line):
        if '\N' != line[51] and line[51] in self.adminplaceids:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('yes') or ""))
        elif '\N' != line[52] and line[52] in self.adminplaceids:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('yes') or ""))

    def __get_adas_complex_intersection(self,keys,line):
        if '1' == line[18] or '2' == line[18] or '3' == line[18]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('yes') or ""))

    def __get_adas_driving_side(self,keys,line):
        if '\N' != line[56]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[56]) or ""))

    def __get_adas_urban(self,keys,line):
        if 'Y' == line[48]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('yes') or ""))

    def __get_in_process_data(self,keys,line):
        if 'Y' == line[53]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('yes') or ""))

    def __digital_compare(self, i1, comp, i2):
        if not (i1.isdigit() and i2.isdigit()):
            return False
        try:
            return eval("%s%s%s"%(i1,comp,i2))
        except:
            return False

    def __fow(self,line):
        if 'Y' == line[7] or 'Y' == line[8]:
            return 0
        if 'Y' == line[5]:
            return 1
        if '4' == line[18]:
            return 4
        if '6' == line[18]:
            return 5
        if '5' == line[18]:
            return 9
        if 'Y' == line[17]:
            return (self.__is_parking_facility(line[0]) and 12 or 13)
        if 'Y' == line[14] and 'N' == line[5]:
            return 10
        if 'Y' == line[12]:
            return 11
        if self.__digital_compare(line[19],">","6") and 'N' == line[38] and 'Y' == line[42] and 'Y' == line[44]:
            return 14
        if ('N','N','N','N','Y','N','N','N','N') == (line[38],line[39],line[40],line[41],line[42],line[43],line[44],line[45],line[47]):
            return 15
        if 'Y' == line[9]:
            return 2
        return 3

    def __is_parking_facility(self,link_id):
        return link_id in self.parking_facility

if __name__ == "__main__":
    # use to test this model
    bg = datetime.datetime.now()
    navlink_stat =  WaysNavigableLinkAdas('na').get_statistic()
    keys = navlink_stat.keys()
    print "==>"
    print "{%s}"%(",".join(map(lambda px: "\"%s\":%s"%(px,navlink_stat[px]) ,keys)))
    print "<=="
    ed = datetime.datetime.now()
    print "Cost time:"+str(ed - bg)
