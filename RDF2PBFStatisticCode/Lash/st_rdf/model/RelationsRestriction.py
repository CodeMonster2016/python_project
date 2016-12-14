#-------------------------------------------------------------------------------
# Name:        RelationsRestriction model
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
("u_turn_allowed", True, "u_turn_allowed"),
("rdm_type", True, "rdm_type"),
("pdm_type", True, "pdm_type"))

class RelationsRestriction(Record):
    def __init__(self, region):
        Record.__init__(self)
        self.dump_file = os.path.join(ROOT_DIR, "temporary", self.__class__.__name__)
        self.stat      = {}
        self.region    = region

    def dump2file(self):
        cmd = "SELECT \
DISTINCT(rc.condition_id), \
rc.condition_type, \
rcr.rdm_type, \
rcr.pdm_type \
FROM \
public.rdf_condition AS rc LEFT JOIN public.rdf_condition_rdm AS rcr on rc.condition_id=rcr.condition_id \
LEFT JOIN public.rdf_nav_strand as rns on rc.nav_strand_id=rns.nav_strand_id \
left join public.rdf_nav_link as rnl on rns.link_id = rnl.link_id \
WHERE (rc.condition_type=7 or rc.condition_type=39) and rnl.iso_country_code in (%s)"%(REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX))
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
                getattr(self,'_RelationsRestriction__get_'+keys[2])(keys,line)
            except:
                print "The statistic [ %s ] didn't exist"%(keys[2])
                print ("Unexpected error:[ RelationsRestriction.py->__statistic] "+str(sys.exc_info()))

    def __count(self,key):
        if self.stat.has_key(key):
            self.stat[key] += 1
        else:
            self.stat[key] = 1

    # all statistic method
    def __get_type(self,keys,line):
        self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_u_turn_allowed(self,keys,line):
        if '39' == line[1]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('yes') or ""))

    def __get_rdm_type(self,keys,line):
        if '\N' != line[2]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(line[2]) or ""))

    def __get_pdm_type(self,keys,line):
        if '1' == line[3]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('1') or ""))

if __name__ == "__main__":
    # use to test this model
    bg = datetime.datetime.now()
    stat =  RelationsRestriction('na').get_statistic()
    keys = stat.keys()
    print "==>"
    print "{%s}"%(",".join(map(lambda px: "\"%s\":%s"%(px,stat[px]) ,keys)))
    print "<=="
    ed = datetime.datetime.now()
    print "Cost time:"+str(ed - bg)
