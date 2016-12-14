#-------------------------------------------------------------------------------
# Name:        RelationsRestrictionHov model
# Purpose:     this model is used to mapping the rdf_nav_link, rdf_link and rdf_access
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
GLOBAL_KEY_PREFIX = "relations_restriction_"
CSV_SEP           = '`'
LF                = '\n'

#(key, category, function)
STATISTIC_KEYS    = (("type",False,"type"),
("restriction", True, "restriction"),
("time_override", True, "time_override"),
("seasonal", False, "seasonal"),
("hov", True, "hov"),
("hov:minimum", False, "hov_minimum"),
("hov:access:hybrid", True, "hov_access_hybrid"),
("hov:access:motorcycle", True, "hov_access_motorcycle"),
("hov:access:alternative", True, "hov_access_alternative"),
("hov:toll", True, "hov_toll"))

class RelationsRestrictionHovAccess(Record):
    def __init__(self, region):
        Record.__init__(self)
        self.dump_file = os.path.join(ROOT_DIR, "temporary", self.__class__.__name__)
        self.stat      = {}
        self.region    = region

    def dump2file(self):
        cmd = "SELECT \
DISTINCT(grc.condition_id), \
access.condition_id AS access_cid, \
access.condition_type AS access_ctype, \
access.time_override, \
access.seasonal_closure, \
hov.condition_id AS hov_cid, \
hov.condition_type AS hov_ctype, \
hov.min_passengers, \
hov.hybrid_car, \
hov.motorcycle, \
hov.alternate_fuel_carpool,\
hov.fee_pay_carpool \
FROM \
  (SELECT \
  DISTINCT(rc.condition_id) \
  FROM \
  public.rdf_condition AS rc LEFT JOIN public.rdf_nav_strand AS rns ON rns.nav_strand_id=rc.nav_strand_id \
  LEFT JOIN public.rdf_nav_link AS rnl ON rns.link_id = rnl.link_id \
  WHERE rc.condition_type='8' AND (rnl.iso_country_code IN (%s) OR rc.nav_strand_id IS NULL)) AS grc \
LEFT JOIN \
  (SELECT \
  DISTINCT(rc.condition_id), \
  rc.condition_type, \
  rca.time_override, \
  rca.seasonal_closure \
  FROM \
  public.rdf_condition AS rc, public.rdf_condition_access AS rca \
  WHERE rca.condition_id=rc.condition_id AND rc.condition_type='8') AS access ON grc.condition_id = access.condition_id \
LEFT OUTER JOIN \
  (SELECT \
  DISTINCT(rc.condition_id), \
  rc.condition_type, \
  rch.min_passengers, \
  rch.hybrid_car, \
  rch.motorcycle, \
  rch.alternate_fuel_carpool, \
  rch.fee_pay_carpool \
  FROM \
  public.rdf_condition AS rc, public.rdf_condition_hov AS rch \
  WHERE rch.condition_id=rc.condition_id and rc.condition_type='8') AS hov ON grc.condition_id=hov.condition_id"%(REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX))
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
                getattr(self,'_RelationsRestrictionHovAccess__get_'+keys[2])(keys,line)
            except:
                print "The statistic [ %s ] didn't exist"%(keys[2])
                print ("Unexpected error:[ RelationsRestrictionHovAccess.py->__statistic] "+str(sys.exc_info()))

    def __count(self,key):
        if self.stat.has_key(key):
            self.stat[key] += 1
        else:
            self.stat[key] = 1

    # all statistic method
    def __get_type(self,keys,line):
        if '\N' != line[0]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_restriction(self,keys,line):
        if '\N' != line[1]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('access') or ""))
        if '\N' != line[5]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('hov') or ""))
        elif '\N' == line[1]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('access') or ""))

    def __get_time_override(self,keys,line):
        if ('\N' == line[5] or '\N' != line[1]) and '\N' == line[5]:
            category_d = {'1':'DAWN TO DUSK','2':'DUSK TO DAWN'}
            if category_d.has_key(line[3]):
                self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(category_d.get(line[3])) or ""))

    def __get_seasonal(self,keys,line):
        if ('\N' == line[5] or '\N' != line[1]) and 'Y' == line[4]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_hov(self,keys,line):
        if '\N' != line[5]:# and '\N' != line[7]: if line[7] is null, this value will be set 0
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('designated') or ""))

    def __get_hov_minimum(self,keys,line):
        if '\N' != line[5]:# and '\N' != line[7]: if line[7] is null, this value will be set 0
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_hov_access_hybrid(self,keys,line):
        if '\N' != line[5] and 'Y' == line[8]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('yes') or ""))

    def __get_hov_access_motorcycle(self,keys,line):
        if '\N' != line[5] and 'Y' == line[9]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('yes') or ""))

    def __get_hov_access_alternative(self,keys,line):
        if '\N' != line[5] and 'Y' == line[10]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('yes') or ""))

    def __get_hov_toll(self,keys,line):
        if '\N' != line[5] and 'Y' == line[11]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('yes') or ""))

if __name__ == "__main__":
    # use to test this model
    bg = datetime.datetime.now()
    stat =  RelationsRestrictionHovAccess('na').get_statistic()
    keys = stat.keys()
    print "==>"
    print "{%s}"%(",".join(map(lambda px: "\"%s\":%s"%(px,stat[px]) ,keys)))
    print "<=="
    ed = datetime.datetime.now()
    print "Cost time:"+str(ed - bg)
