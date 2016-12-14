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
STATISTIC_KEYS    = (("links",False,"links"),
("iso", True, "iso"),
("access_id", False, "access_id"),
("status_id", False, "status_id"),
("functional_class", False, "functional_class"),
("controlled_access", False, "controlled_access"),
("oneway", True, "oneway"),
("route", True, "route"),
("rail_ferry", True, "rail_ferry"),
("multi_digitized", False, "multi_digitized"),
("divider", False, "divider"),
("divider_legal", False, "divider_legal"),
("frontage", False, "frontage"),
("surface", True, "surface"),
("ramp", False, "ramp"),
("access", True, "access"),
("toll", True, "toll"),
("poi_access", False, "poi_access"),
("intersection_cat", True, "intersection_cat"),
("speed_cat", False, "speed_cat"),
("lane_cat", True, "lane_cat"),
("coverage_indicator", False, "coverage_indicator"),
("lanes:forward", False, "lanes_forward"),
("lanes:backward", False, "lanes_backward"),
("lanes", False, "lanes"),
("maxspeed:forward", False, "maxspeed_forward"),
("maxspeed:backward", False, "maxspeed_backward"),
("source:maxspeed", False, "source_maxspeed"),
("low_mobility", False, "low_mobility"),
("grade_cat", False, "grade_cat"),
("confidence_level_rating", False, "confidence_level_rating"),
("pedestrian_preferred", False, "pedestrian_preferred"),
("limited_access_road", False, "limited_access_road"),
("road_class", False, "road_class"),
("overpass_underpass", False, "overpass_underpass"),
("bridge", True, "bridge"),
("tunnel", True, "tunnel"),
("motorcar", True, "motorcar"),
("bus", True, "bus"),
("taxi", True, "taxi"),
("hov", True, "hov"),
("foot", True, "foot"),
("truck", True, "truck"),
("delivery", True, "delivery"),
("emergency", True, "emergency"),
("access_through_traffic", True, "access_through_traffic"),
("motorcycle", True, "motorcycle"),
("rt", True, "rt"),
("rst", True, "rst"),
("fc", True, "fc"),
("sc", True, "sc"),
("highway", True, "highway"),
("service", True, "service")
)

class WaysNavigableLink(Record):
    def __init__(self, region):
        Record.__init__(self)
        self.navlink_file = os.path.join(ROOT_DIR, "temporary", self.__class__.__name__)
        self.stat         = {}
        self.region       = region

    def dump2file(self):
        cmd = "SELECT \
rnl.link_id, \
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
rnla.supplemental_geo_bitset \
FROM \
public.rdf_nav_link as rnl left join public.rdf_link as rl on rnl.link_id=rl.link_id \
left join public.rdf_nav_link_attribute as rnla on rnl.link_id=rnla.link_id \
left join public.rdf_access as ra on ra.access_id=rnl.access_id \
left join public.rdf_nav_link_status as rnls on rnls.status_id=rnl.status_id \
WHERE rnl.iso_country_code in (%s)"%(REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX))
        print cmd
        self.cursor.copy_expert("COPY (%s) TO STDOUT DELIMITER '`'"%(cmd),open(self.navlink_file,"w"))

    def get_statistic(self):
        try:
            self.dump2file()
            self.__dump_parking_facility()
        except:
            print "Some table or schema don't exist! Please check the upper sql"
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
        print "Dump parking facility id"
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
                getattr(self,'_WaysNavigableLink__get_'+keys[2])(keys,line)
            except:
                print "The statistic [ %s ] didn't exist"%(keys[2])
                print ("Unexpected error:[ WaysNavigableLink.py->__statistic] "+str(sys.exc_info()))

    def __count(self,key):
        if self.stat.has_key(key):
            self.stat[key] += 1
        else:
            self.stat[key] = 1

    # all statistic method
    def __get_links(self,keys,line):
        self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_iso(self,keys,line):
        self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[1]) or ""))

    def __get_access_id(self,keys,line):
        self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[2]) or ""))

    def __get_status_id(self,keys,line):
        self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[3]) or ""))

    def __get_functional_class(self,keys,line):
        self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[4]) or ""))

    def __get_controlled_access(self,keys,line):
        self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[5]) or ""))

    def __get_oneway(self,keys,line):
        if '\N' != line[6]:
            oneway = ('F' == line[6].upper() and "yes" or ('T' == line[6].upper() and "-1" or None))
            if None != oneway:
                self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(oneway) or ""))

    def __get_route(self,keys,line):
        #route = ('Y' == line[7].upper() and "ferry" or None)
        if 'Y' == line[7].upper():
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%("ferry") or ""))

    def __get_rail_ferry(self,keys,line):
        #route = ('Y' == line[7].upper() and "ferry" or None)
        if 'Y' == line[8].upper():
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%("yes") or ""))

    def __get_multi_digitized(self,keys,line):
        self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[9]) or ""))

    def __get_divider(self,keys,line):
        self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[10]) or ""))

    def __get_divider_legal(self,keys,line):
        self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[11]) or ""))

    def __get_frontage(self,keys,line):
        self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[12]) or ""))

    def __get_surface(self,keys,line):
        if 'N' == line[13].upper():
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%("unpaved") or ""))

    def __get_ramp(self,keys,line):
        self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[14]) or ""))

    def __get_access(self,keys,line):
        if 'Y' == line[15].upper():
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%("private") or ""))
        if 'Y' == line[29].upper() and 'Y' != line[15].upper():
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%("public") or ""))

    def __get_toll(self,keys,line):
        if 'Y' == line[16].upper():
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%("yes") or ""))

    def __get_poi_access(self,keys,line):
        self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[17]) or ""))

    def __get_intersection_cat(self,keys,line):
        intersection_type = line[18]
        if '\N' == intersection_type:
            return
        if '4' != intersection_type:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(intersection_type) or ""))
        else:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,"junction",keys[1] and "#%s"%("roundabout") or ""))

    def __get_speed_cat(self,keys,line):
        self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[19]) or ""))

    def __get_lane_cat(self,keys,line):
        self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[20]) or ""))

    def __get_coverage_indicator(self,keys,line):
        if '\N' != line[21]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[21]) or ""))

    def __get_lanes_forward(self,keys,line):
        if '\N' != line[22]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[22]) or ""))

    def __get_lanes_backward(self,keys,line):
        if '\N' != line[23]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[23]) or ""))

    def __get_lanes(self,keys,line):
        if '\N' != line[24]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[24]) or ""))

    def __get_maxspeed_forward(self,keys,line):
        if '\N' != line[25] and '0' != line[25] and '998' != line[25]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[25]) or ""))

    def __get_maxspeed_backward(self,keys,line):
        if '\N' != line[26] and '0' != line[26] and '998' != line[26]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[26]) or ""))

    def __get_source_maxspeed(self,keys,line):
        if '\N' != line[27]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[27]) or ""))

    def __get_low_mobility(self,keys,line):
        self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[28]) or ""))

    def __get_grade_cat(self,keys,line):
        if '\N' != line[30]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[30]) or ""))

    def __get_confidence_level_rating(self,keys,line):
        if '\N' != line[31]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[31]) or ""))

    def __get_pedestrian_preferred(self,keys,line):
        if '\N' != line[32]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[32]) or ""))

    def __get_limited_access_road(self,keys,line):
        self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[33]) or ""))

    def __get_road_class(self,keys,line):
        if '\N' != line[34]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[34]) or ""))

    def __get_overpass_underpass(self,keys,line):
        if '\N' != line[35]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[35]) or ""))

    def __get_bridge(self,keys,line):
        if 'Y' == line[36].upper():
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%("yes") or ""))

    def __get_tunnel(self,keys,line):
        if 'Y' == line[37].upper():
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%("yes") or ""))

    def __get_motorcar(self,keys,line):
        self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('Y' == line[38].upper() and "yes" or ('N' == line[38].upper() and "no" or None)) or ""))

    def __get_bus(self,keys,line):
        self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('Y' == line[39].upper() and "yes" or ('N' == line[39].upper() and "no" or None)) or ""))

    def __get_taxi(self,keys,line):
        self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('Y' == line[40].upper() and "yes" or ('N' == line[40].upper() and "no" or None)) or ""))

    def __get_hov(self,keys,line):
        hov = None
        if 'Y' == line[49]:
            hov = 'designated'
        elif 'Y' == line[41]:
            hov = 'yes'
        elif 'N' == line[41]:
            hov = 'no'
        if None != hov:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(hov) or ""))

    def __get_foot(self,keys,line):
        self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('Y' == line[42].upper() and "yes" or ('N' == line[42].upper() and "no" or None)) or ""))

    def __get_truck(self,keys,line):
        self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('Y' == line[43].upper() and "yes" or ('N' == line[43].upper() and "no" or None)) or ""))

    def __get_delivery(self,keys,line):
        self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('Y' == line[44].upper() and "yes" or ('N' == line[44].upper() and "no" or None)) or ""))

    def __get_emergency(self,keys,line):
        self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('Y' == line[45].upper() and "yes" or ('N' == line[45].upper() and "no" or None)) or ""))

    def __get_access_through_traffic(self,keys,line):
        self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('Y' == line[46].upper() and "yes" or ('N' == line[46].upper() and "no" or None)) or ""))

    def __get_motorcycle(self,keys,line):
        self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('Y' == line[47].upper() and "yes" or ('N' == line[47].upper() and "no" or None)) or ""))

    def __get_rt(self,keys,line):
        rt = None
        if 'Y' == line[7]:
            rt = "10"
        elif 'Y' == line[8]:
            rt = "11"
        elif ('N','Y','N','N','N','N','N','N','N') == (line[38],line[39],line[40],line[41],line[42],line[43],line[44],line[45],line[47]):
            rt = "12"
        elif "N" == line[29] or "Y" == line[15]:
            rt = "7"
        elif  1 == self.__fow(line) and "Y" == line[48]:
            rt = "1"
        elif 1 == self.__fow(line):
            rt = "0"
        elif 11 == self.__fow(line):#"Y" == line[12]:
            rt = "5"
        elif 15 == self.__fow(line):
            rt = "8"
        elif "1" == line[19] or "2" == line[19] or "3" == line[19]:
            rt = "2"
        elif "4" == line[19] or "5" == line[19]:
            rt = "3"
        elif "6" == line[19]:
            rt = "4"
        elif "7" == line[19] or "8" == line[19]:
            rt = "6"
            if REGION_CN == self.region and 14 == self.__fow(line) and "8" == line[19]:
                rt = "8"
        elif 14 == self.__fow(line):
            rt = "9"
        else:
            rt = '9'
        if None != rt:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(rt) or ""))

    def __get_rst(self,keys,line):
        rst = None
        if "Y" == line[14]:
            rst = "5"
        elif 12 == self.__fow(line) or 13 == self.__fow(line):
            rst = "6"
        elif 9 == self.__fow(line):
            rst = "7"
        elif "2" == line[18] or "3" == line[18]:
            rst = "3"
        elif "1" == line[18]:
            rst = "4"
        elif 4 == self.__fow(line) or 5 == self.__fow(line):
            rst = "0"
        elif 'Y' == line[36]:
            rst = "12"
        elif 'Y' == line[37]:
            rst = "11"
        elif "N" == line[9]:
            rst = "1"
        elif "Y" == line[9]:
            rst = "2"
        else:
            rst = "1"
        if None != rst:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(rst) or ""))

    def __get_fc(self,keys,line):
        fc = None
        if "5" == line[4]:
            fc = "1"
        elif "4" == line[4]:
            fc = "2"
        elif "3" == line[4]:
            fc = "3"
        elif "2" == line[4]:
            fc = "4"
        elif "1" == line[4]:
            fc = "5"
        if None != fc:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(fc) or ""))

    def __get_sc(self,keys,line):
        sc = None
        if 'Y' == line[8]:
            line[19] = '5'
        if "1" == line[19]:
            sc = "1"
        elif "2" == line[19]:
            sc = "2"
        elif "3" == line[19]:
            sc = "3"
        elif "4" == line[19]:
            sc = "5"
        elif "5" == line[19]:
            sc = "7"
        elif "6" == line[19]:
            sc = "11"
        elif "7" == line[19]:
            sc = "14"
        elif "8" == line[19]:
            sc = "15"
        if None != sc:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(sc) or ""))

    def __get_highway(self,keys,line):
        if 'Y' == line[7] or 'Y' == line[8]:
            return
        highway = None
        if 'Y' == line[5] and 'N' == line[14]:
            highway = "motorway"
        elif 'Y' == line[5] and 'Y' == line[14]:
            highway = "motorway_link"
        elif 'N' == line[5] and 'N' == line[14] and \
            (self.__digital_compare(line[4],"==","1") or self.__digital_compare(line[4],"==","2")) and \
            ('\N' == line[18] or (self.__digital_compare(line[18],"!=","2") and self.__digital_compare(line[18],"!=","3"))):
            highway = "trunk"
        elif ('N' == line[5] and 'Y' == line[14] and \
            (self.__digital_compare(line[4],"==","1") or self.__digital_compare(line[4],"==","2"))) or \
            ('N' == line[14] and \
            (self.__digital_compare(line[4],"==","1") or self.__digital_compare(line[4],"==","2")) and \
            (self.__digital_compare(line[18],"==","2") or self.__digital_compare(line[18],"==","3"))):
            highway = "trunk_link"
        elif 'N' == line[5] and 'N' == line[14] and \
            self.__digital_compare(line[4],"==","3") and \
            ('\N' == line[18] or (self.__digital_compare(line[18],"!=","2") and self.__digital_compare(line[18],"!=","3"))):
            highway = "primary"
        elif 'N' == line[5] and \
             (('Y' == line[14] and self.__digital_compare(line[4],"==","3")) or \
              ('N' == line[14] and \
               self.__digital_compare(line[4],"==","3") and \
               (self.__digital_compare(line[18],"==","2") or self.__digital_compare(line[18],"==","3")))):
            highway = "primary_link"
        elif 'N' == line[5] and 'N' == line[14] and \
             self.__digital_compare(line[4],"==","4") and \
            ('\N' == line[18] or (self.__digital_compare(line[18],"!=","2") and self.__digital_compare(line[18],"!=","3"))):
            highway = "secondary"
        elif 'N' == line[5] and (('Y' == line[14] and '4' == line[4]) or \
                                 ('N' == line[14] and \
                                  '4' == line[4] and \
                                  (self.__digital_compare(line[18],"==","2") or self.__digital_compare(line[18],"==","3")))):
            highway = "secondary_link"
        elif REGION_CN != self.region and \
            'N' == line[5] and 'N' == line[14] and \
             self.__digital_compare(line[19],"<=","5") and \
             '5' == line[4] and \
             ('\N' == line[18] or (self.__digital_compare(line[18],"!=","2") and self.__digital_compare(line[18],"!=","3"))):
            highway = "tertiary"
        elif REGION_CN != self.region and \
            'N' == line[5] and (('Y' == line[14] and self.__digital_compare(line[19],"<=","5") and '5' == line[4]) or \
                                 ('N' == line[14] and self.__digital_compare(line[19],"<=","5") and '5' == line[4] and \
                                  (self.__digital_compare(line[18],"==","2") or self.__digital_compare(line[18],"==","3")))):
            highway = "tertiary_link"
        elif REGION_CN != self.region and \
            'N' == line[5] and '6' == line[19] and '5' == line[4]:
            highway = "residential"
        elif REGION_CN == self.region and \
            'N' == line[5] and '5' == line[4] and 'Y' == line[46].upper():
            highway = "residential"
        elif REGION_CN != self.region and \
            'N' == line[5] and self.__digital_compare(line[19],">=","7") and '5' == line[4] and 'Y' == line[13]:
            highway = "service"
        elif REGION_CN == self.region and \
            'N' == line[5] and '5' == line[4] and 'N' == line[46].upper():
            highway = "service"
        elif 'N' == line[5] and self.__digital_compare(line[19],">=","7") and '5' == line[4] and 'N' == line[13]:
            highway = "track"
        else:
            highway = "unclassified"
        # if need to override, this is from the adaptorG2
        if '1' == line[50]:
            highway = "raceway"
        elif ('N','N','N','N','Y','N','N','N','N') == (line[38],line[39],line[40],line[41],line[42],line[43],line[44],line[45],line[47]):
            highway = "pedestrian"

        self.precondition_isservice = "service" == highway
        if None != highway:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(highway) or ""))

    def __get_service(self,keys,line):
        if not self.precondition_isservice:
            return
        mp = {'4':'driveway','8':'alley'}
        if line[50] in mp:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(mp.get(line[50])) or ""))

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
    navlink_stat =  WaysNavigableLink('na').get_statistic()
    keys = navlink_stat.keys()
    print "==>"
    print "{%s}"%(",".join(map(lambda px: "\"%s\":%s"%(px,navlink_stat[px]) ,keys)))
    print "<=="
    ed = datetime.datetime.now()
    print "Cost time:"+str(ed - bg)
