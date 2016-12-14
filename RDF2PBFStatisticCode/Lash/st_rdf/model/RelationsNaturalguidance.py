#-------------------------------------------------------------------------------
# Name:       RelationsNaturalguidance model
# Purpose:    this model is used to mapping the rdf_nav_link, rdf_link and rdf_access
#             columns: [ ]
#
# Author:      fwu
#
# Created:     29/12/2015
# Copyright:   (c) TeleNav 2015
# Licence:     <your licence>
#-------------------------------------------------------------------------------

from record import Record
from constants import *
import os
import sys
import datetime
import json

ROOT_DIR          = os.path.join(os.path.dirname(os.path.abspath(__file__)),"..")
GLOBAL_KEY_PREFIX = "relations_natural_guidance_"
CSV_SEP           = '`'
LF                = '\n'

#(key, category, function)
STATISTIC_KEYS    = (
("direction",True,"direction"),
("visibility", False, "visibility"),
("seasonal_dependency", False, "seasonal_dependency"),
("relative_distance", False, "relative_distance"),
("calc_importance", False, "calc_importance"),
("asso_type", False, "asso_type"),
("time", False, "time"),
)


class RelationsNaturalguidance(Record):
    def __init__(self, region):
        Record.__init__(self)
        self.dump_file = os.path.join(ROOT_DIR, "temporary", self.__class__.__name__)
        self.stat      = {}
        self.region    = region

    def dump2file(self):
        cmd = "SELECT \
rang.asso_id, \
rang.direction,\
rang.visibility,\
rang.seasonal_dependency,\
rang.relative_distance,\
rang.calc_importance, \
assotp.asso_type, \
rtd.time_domain, \
rtd.owner \
FROM \
public.rdf_asso_natural_guidance AS rang \
INNER JOIN (SELECT \
asso.asso_id, \
asso.asso_type, \
assolk.iso_country_code \
FROM \
public.rdf_asso AS asso \
INNER JOIN (SELECT distinct \
assolk.asso_id, \
lk.link_id, \
lk.iso_country_code \
FROM \
public.rdf_asso_link AS assolk \
INNER JOIN (SELECT  \
lk.link_id, \
adh.iso_country_code \
FROM \
public.rdf_link as lk \
INNER JOIN ( SELECT \
adpl.admin_place_id, \
adh.iso_country_code \
FROM \
public.rdf_admin_place as adpl \
INNER JOIN \
public.rdf_admin_hierarchy as adh \
ON adpl.admin_place_id = adh.admin_place_id \
) AS adh \
ON lk.left_admin_place_id = adh.admin_place_id OR lk.right_admin_place_id = adh.admin_place_id \
) AS lk \
ON assolk.link_id = lk.link_id \
) AS assolk \
ON assolk.asso_id = asso.asso_id \
) AS assotp \
ON rang.asso_id = assotp.asso_id AND assotp.iso_country_code  IN (%s) LEFT JOIN public.rdf_time_domain as rtd ON rtd.feature_id=rang.asso_id"%(REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX))
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
    def __get_direction(self,keys,line):
        if '\N' != line[1]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[1]) or ""))


    def __get_visibility(self,keys,line):
        if '\N' != line[2]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_seasonal_dependency(self,keys,line):
        if 'Y' == line[3]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_relative_distance(self,keys,line):
        if '\N' != line[4]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_calc_importance(self,keys,line):
        if '\N' != line[5]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_asso_type(self,keys,line):
        if '\N' != line[6]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_time(self,keys,line):
        if '\N' != line[7] and 'B' == line[8]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

if __name__ == '__main__':
        # use to test this model
    bg = datetime.datetime.now()
    stat =  RelationsNaturalguidance('na').get_statistic()
    keys = stat.keys()
    print "==>"
    print "{%s}"%(",".join(map(lambda px: "\"%s\":%s"%(px,stat[px]) ,keys)))
    print "<=="
    ed = datetime.datetime.now()
    print "Cost time:"+str(ed - bg)
