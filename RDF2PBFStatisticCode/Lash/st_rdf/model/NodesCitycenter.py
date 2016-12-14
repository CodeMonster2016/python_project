#-------------------------------------------------------------------------------
# Name:        NodesCitycenter model
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
GLOBAL_KEY_PREFIX = "nodes_city_center_"
CSV_SEP           = '`'
LF                = '\n'

#(key, category, function)
STATISTIC_KEYS    = (("cat_id",False,"cat_id"),
("type",False,"type"),
("place", True, "place"),
("population", False, "population"),
("street_name", False, "street_name"),
("iso", True, "iso"),
("postal_code", False, "postal_code"),
("long_haul", True, "long_haul"),
("capital", True, "capital"),
("capital_order1", True, "capital_order1"),
("capital_order2", True, "capital_order2"),
("capital_order8", True, "capital_order8"),
("claimed_by", False, "claimed_by"),
("controlled_by", False, "controlled_by"),
("location_score", False, "location_score"),
("place_score", False, "place_score"),
("calculated_level", False, "calculated_level"),
("link_count", False, "link_count")
)

class NodesCitycenter(Record):
    def __init__(self, region):
        Record.__init__(self)
        self.dump_file = os.path.join(ROOT_DIR, "temporary", self.__class__.__name__)
        self.stat      = {}
        self.region    = region

    def dump2file(self):
        cmd = "SELECT \
rcp.poi_id, \
rcp.cat_id, \
rcp.population, \
rcp.street_name, \
rcp.iso_country_code, \
rcp.postal_code, \
rcp.long_haul, \
rcp.capital_country, \
rcp.capital_order1, \
rcp.capital_order2, \
rcp.capital_order8, \
rcp.claimed_by, \
rcp.controlled_by, \
rcp.location_score, \
rcp.place_score, \
rcp.calculated_level, \
rcp.named_place_id, \
rcp.named_place_type, \
rah.num_links, \
rah.iso_country_code \
FROM \
public.rdf_city_poi AS rcp LEFT JOIN public.rdf_location AS rl on rcp.location_id=rl.location_id \
LEFT JOIN public.rdf_admin_hierarchy AS rah on rcp.named_place_id = rah.admin_place_id \
WHERE rcp.iso_country_code IN (%s)"%(REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX))
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
                getattr(self,'_NodesCitycenter__get_'+keys[2])(keys,line)
            except:
                print "The statistic [ %s ] didn't exist"%(keys[2])
                print ("Unexpected error:[ NodesCitycenter.py->__statistic] "+str(sys.exc_info()))

    def __count(self,key):
        if self.stat.has_key(key):
            self.stat[key] += 1
        else:
            self.stat[key] = 1

    # all statistic method
    def __get_cat_id(self,keys,line):
        if '\N' != line[1]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_type(self,keys,line):
        if '\N' != line[0]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_place(self,keys,line):
        place = None
        if '4444' == line[1]:
            place = 'city'
        elif '9709' == line[1]:
            place = 'neighbourhood'
        elif '9998' == line[1]:
            place = 'hamlet'
        if None != place:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(place) or ""))

    def __get_population(self,keys,line):
        if '\N' != line[2] and '0' != line[2]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_street_name(self,keys,line):
        if '\N' != line[3]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_iso(self,keys,line):
        if '\N' != line[4]:
            iso = line[4]
            if 'A' == line[17] and '\N' != line[19]:
                iso = line[19]
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(iso) or ""))

    def __get_postal_code(self,keys,line):
        if '\N' != line[5]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_long_haul(self,keys,line):
        if 'Y' == line[6]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('Y') or ""))

    def __get_capital(self,keys,line):
        if 'Y' == line[7]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('yes') or ""))

    def __get_capital_order1(self,keys,line):
        if 'Y' == line[8]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('yes') or ""))

    def __get_capital_order2(self,keys,line):
        if 'Y' == line[9]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('yes') or ""))

    def __get_capital_order8(self,keys,line):
        if 'Y' == line[10]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('yes') or ""))

    def __get_claimed_by(self,keys,line):
        if '\N' != line[11]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_controlled_by(self,keys,line):
        if '\N' != line[12]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_location_score(self,keys,line):
        if '\N' != line[13]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_place_score(self,keys,line):
        if '\N' != line[14]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_calculated_level(self,keys,line):
        if '\N' != line[15]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_link_count(self,keys,line):
        if '\N' != line[18] and self.__digital_compare(line[18],'>','0'):
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __digital_compare(self, i1, comp, i2):
        if not (i1.isdigit() and i2.isdigit()):
            return False
        try:
            return eval("%s%s%s"%(i1,comp,i2))
        except:
            return False

if __name__ == "__main__":
    # use to test this model
    bg = datetime.datetime.now()
    stat =  NodesCitycenter('na').get_statistic()
    keys = stat.keys()
    print "==>"
    print "{%s}"%(",".join(map(lambda px: "\"%s\":%s"%(px,stat[px]) ,keys)))
    print "<=="
    ed = datetime.datetime.now()
    print "Cost time:"+str(ed - bg)
