#-------------------------------------------------------------------------------
# Name:        RelationsAdmin model
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
GLOBAL_KEY_PREFIX = "relations_admin_"
CSV_SEP           = '`'
LF                = '\n'

#(key, category, function)
STATISTIC_KEYS    = (("type",False,"type"),
("admin_order",False,"admin_order"),
("iso", True, "iso"),
("admin_level", True, "admin_level"),
("admin_type", True, "admin_type"),
("timezone", False, "timezone"),
("dst_observed", True, "dst_observed"),
("dst_start_day", False, "dst_start_day"),
("dst_start_weekday", False, "dst_start_weekday"),
("dst_start_month", False, "dst_start_month"),
("dst_start_time", False, "dst_start_time"),
("dst_end_day", False, "dst_end_day"),
("dst_end_weekday", False, "dst_end_weekday"),
("dst_end_month", False, "dst_end_month"),
("dst_end_time", False, "dst_end_time"))

class RelationsAdmin(Record):
    def __init__(self, region):
        Record.__init__(self)
        self.dump_file = os.path.join(ROOT_DIR, "temporary", self.__class__.__name__)
        self.stat      = {}
        self.region    = region

    def dump2file(self):
        cmd = "SELECT \
DISTINCT(rah.admin_place_id), \
rah.admin_order, \
rah.iso_country_code, \
rap.admin_type, \
rap.time_zone, \
rad.dst_observed, \
rad.dst_start_day, \
rad.dst_start_weekday, \
rad.dst_start_month, \
rad.dst_start_time, \
rad.dst_end_day, \
rad.dst_end_weekday, \
rad.dst_end_month, \
rad.dst_end_time \
FROM \
public.rdf_admin_place AS rap LEFT JOIN public.rdf_admin_hierarchy AS rah ON rap.admin_place_id=rah.admin_place_id \
LEFT JOIN public.rdf_admin_dst AS rad ON rad.dst_id = rap.dst_id \
WHERE rah.iso_country_code IN (%s)"%(REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX))
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
                getattr(self,'_RelationsAdmin__get_'+keys[2])(keys,line)
            except:
                print "The statistic [ %s ] didn't exist"%(keys[2])
                print ("Unexpected error:[ RelationsAdmin.py->__statistic] "+str(sys.exc_info()))

    def __count(self,key):
        if self.stat.has_key(key):
            self.stat[key] += 1
        else:
            self.stat[key] = 1

    # all statistic method
    def __get_type(self,keys,line):
        if '\N' != line[0]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_admin_order(self,keys,line):
        if '\N' != line[1]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_iso(self,keys,line):
        if '\N' != line[2]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[2]) or ""))

    def __get_admin_level(self,keys,line):
        pass

    def __get_admin_type(self,keys,line):
        if '\N' != line[3]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[3]) or ""))

    def __get_timezone(self,keys,line):
        if '\N' != line[4]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_dst_observed(self,keys,line):
        if 'Y' == line[5]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('yes') or ""))

    def __get_dst_start_day(self,keys,line):
        if '\N' != line[6]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_dst_start_weekday(self,keys,line):
        if '\N' != line[7]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_dst_start_month(self,keys,line):
        if '\N' != line[8]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_dst_start_time(self,keys,line):
        if '\N' != line[9]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_dst_end_day(self,keys,line):
        if '\N' != line[10]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_dst_end_weekday(self,keys,line):
        if '\N' != line[11]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_dst_end_month(self,keys,line):
        if '\N' != line[12]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_dst_end_time(self,keys,line):
        if '\N' != line[13]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

if __name__ == "__main__":
    # use to test this model
    bg = datetime.datetime.now()
    stat =  RelationsAdmin('na').get_statistic()
    keys = stat.keys()
    print "==>"
    print "{%s}"%(",".join(map(lambda px: "\"%s\":%s"%(px,stat[px]) ,keys)))
    print "<=="
    ed = datetime.datetime.now()
    print "Cost time:"+str(ed - bg)
