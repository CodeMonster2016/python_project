#-------------------------------------------------------------------------------
# Name:        RelationsJunctionview model
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
GLOBAL_KEY_PREFIX = "relations_junction_view_"
CSV_SEP           = '`'
LF                = '\n'

#(key, category, function)
STATISTIC_KEYS    = (
("type",False,"type"),
("jv_file_name",False,"jv_file_name"),
("jv_file_name:type",False,"jv_file_name_type")
)

class RelationsJunctionview(Record):
    def __init__(self, region):
        Record.__init__(self)
        self.dump_file = os.path.join(ROOT_DIR, "temporary", self.__class__.__name__)
        self.stat      = {}
        self.region    = region

    def dump2file(self):
        cmd = "SELECT \
DISTINCT(rc.condition_id), \
rc.condition_type, \
STRING_AGG(CONCAT_WS('_',rf.file_type::text,rf.file_name),',') AS file_type_names \
FROM \
public.rdf_condition AS rc LEFT JOIN public.rdf_nav_strand AS rns ON rns.nav_strand_id=rc.nav_strand_id \
LEFT JOIN public.rdf_nav_link AS rnl ON rns.link_id = rnl.link_id \
LEFT JOIN public.rdf_file_feature AS rff ON rc.condition_id=rff.feature_id \
LEFT JOIN public.rdf_file AS rf ON rf.file_id=rff.file_id \
WHERE rc.condition_type='20' AND rnl.iso_country_code IN (%s) \
GROUP BY rc.condition_id"%(REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX))
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
        key = "%s%s"%(GLOBAL_KEY_PREFIX,key)
        if self.stat.has_key(key):
            self.stat[key] += 1
        else:
            self.stat[key] = 1

    # all statistic method
    def __get_type(self,keys,line):
        if '\N' != line[0]:
            self.__count(keys[0])

    # About multiple jv_file_name_x & jv_file_name_x:type
    # all information in the line[2], seperate by comma ',' example see below
    # 1_US_NY_90764.JPG,2_US_NY_90764_L.GIF,34_JV_US_34128969.svg,25_SR_US_34128969.svg,1_US_NY_90764.JPG,2_US_NY_90764_L.GIF,34_JV_US_34128969.svg,25_SR_US_34128969.svg
    # can count all jv_file_name_x & jv_file_name_x:type
    # but here, we only statistic one default jv_file_name & jv_file_name:type, which will be used to compare with the pbf statistic
    #
    def __get_jv_file_name(self,keys,line):
        if '\N' != line[2]:
            self.__count(keys[0])

    def __get_jv_file_name_type(self,keys,line):
        if '\N' != line[2]:
            self.__count(keys[0])

if __name__ == "__main__":
    # use to test this model
    bg = datetime.datetime.now()
    stat =  RelationsJunctionview('na').get_statistic()
    keys = stat.keys()
    print "==>"
    print "{%s}"%(",".join(map(lambda px: "\"%s\":%s"%(px,stat[px]) ,keys)))
    print "<=="
    ed = datetime.datetime.now()
    print "Cost time:"+str(ed - bg)
