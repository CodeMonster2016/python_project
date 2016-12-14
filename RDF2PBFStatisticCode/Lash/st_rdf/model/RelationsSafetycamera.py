#-------------------------------------------------------------------------------
# Name:        RelationsSafetycamera model
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
GLOBAL_KEY_PREFIX = "relations_safety_camera_"
CSV_SEP           = '`'
LF                = '\n'

#(key, category, function)
STATISTIC_KEYS    = (("type",False,"type"),
("link_id",False,"link_id"),
("cam_type_id",True,"cam_type_id"),
("cam_type",False,"cam_type"),
("maxspeed",False,"maxspeed"),
("speed_unit",True,"speed_unit"),
("side_of_st",True,"side_of_st"),
("iso",True,"iso"),
("fixture_status",False,"fixture_status"),
("cat_id",False,"cat_id"),
("driving_dir",False,"driving_dir"),
("link_heading",False,"link_heading")
)

class RelationsSafetycamera(Record):
    def __init__(self, region):
        Record.__init__(self)
        self.dump_file = os.path.join(ROOT_DIR, "temporary", self.__class__.__name__)
        self.stat      = {}
        self.region    = region

    def dump2file(self):
        cmd = "SELECT \
DISTINCT(xscp.poi_entity_id_text), \
xscp.linkid_text, \
xscp.cameratype_text, \
xscp.speedlimit_text, \
xscp.speedlimit_unit, \
xscp.side_of_st_text, \
xscp.countrycode_text, \
xscp.fixturestat_text, \
xscp.category_id_text, \
xscp.drivingdirection_text, \
xscp.drivingdirection_linkheading \
FROM \
public.xml_safety_camera_poi AS xscp \
WHERE xscp.countrycode_text IN (%s)"%(REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX))
        print cmd
        self.cursor.copy_expert("COPY (%s) TO STDOUT DELIMITER '`'"%(cmd),open(self.dump_file,"w"))

    def get_statistic(self):
        try:
            self.__pre_check()
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

    # check column drivingdirection_text, drivingdirection_linkheading
    # if not exist, add it.
    def __pre_check(self):
        self.run_sql(self.cursor.execute,"SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='xml_safety_camera_poi' AND (column_name='drivingdirection_text' OR column_name='drivingdirection_linkheading')")
        cols = map(lambda px:px[0],self.cursor.fetchall())
        ddt = 0 != cols.count('drivingdirection_text')
        ct = 0 != cols.count('drivingdirection_linkheading')
        if ddt and ct:
            return
        ddt_cmd = not ddt and "ADD COLUMN drivingdirection_text varchar(4000)"  or ""
        ct_cmd  = not ct  and "ADD COLUMN drivingdirection_linkheading varchar(4000)"        or ""
        self.run_sql(self.cursor.execute,"ALTER TABLE public.xml_safety_camera_poi %s%s%s"%(ddt_cmd,(ddt_cmd !="" and ct_cmd != "") and "," or "",ct_cmd))

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
        elif 'NoLRTurns' == line[2]:
            cam_type_id = '20'
        elif 'Other' == line[2]:
            cam_type_id = '999'

        if None != cam_type_id:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(cam_type_id) or ""))

    def __get_cam_type(self,keys,line):
        if '\N' != line[2]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_maxspeed(self,keys,line):
        if '\N' != line[3]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_speed_unit(self,keys,line):
        speedunit = None
        if 'MPH' == line[4]:
            speedunit = 'M'
        elif 'KPH' == line[4]:
            speedunit = 'K'

        if None != speedunit:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(speedunit) or ""))

    def __get_side_of_st(self,keys,line):
        if '\N' != line[5]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[5]) or ""))

    def __get_iso(self,keys,line):
        if '\N' != line[6]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[6]) or ""))

    def __get_fixture_status(self,keys,line):
        if '\N' != line[7]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_cat_id(self,keys,line):
        if '\N' != line[8]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_driving_dir(self,keys,line):
        if '\N' != line[9]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_link_heading(self,keys,line):
        if '\N' != line[10]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

if __name__ == "__main__":
    # use to test this model
    bg = datetime.datetime.now()
    stat =  RelationsSafetycamera('na').get_statistic()
    keys = stat.keys()
    print "==>"
    print "{%s}"%(",".join(map(lambda px: "\"%s\":%s"%(px,stat[px]) ,keys)))
    print "<=="
    ed = datetime.datetime.now()
    print "Cost time:"+str(ed - bg)
