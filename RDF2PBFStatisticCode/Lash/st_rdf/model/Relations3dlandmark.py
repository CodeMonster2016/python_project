#-------------------------------------------------------------------------------
# Name:        Relations3dlandmark model
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
GLOBAL_KEY_PREFIX = "relations_3d_landmark_"
CSV_SEP           = '`'
LF                = '\n'
ST_BUILDING       = 'st_buildings'
ST_BUILDING_INDEX = 'st_buildings_index'

#(key, category, function)
STATISTIC_KEYS    = (
("type",False,"type"),
("file_path",False,"file_path"),
("3d_landmark_model_standard:file_name",False,"3d_landmark_model_standard_file_name"),
("3d_landmark_model_light:file_name",False,"3d_landmark_model_light_file_name"),
)

class Relations3dlandmark(Record):
    def __init__(self, region):
        Record.__init__(self)
        self.dump_file = os.path.join(ROOT_DIR, "temporary", self.__class__.__name__)
        self.stat      = {}
        self.region    = region

    def dump2file(self):
        cmd = "SELECT \
DISTINCT(rc.cf_id) AS cf_id, \
string_agg(rf.file_type::text,',') AS file_types, \
string_agg(rf.file_name::text,',') AS file_names, \
string_agg(rcb.building_id::text,',') AS rcb_buildings, \
string_agg(stb.building_id::text,',') AS buildings, \
count(rf.file_name) AS count_filenames, \
count(uli.file_dir) AS count_filedirs \
FROM public.rdf_file_feature AS rff, \
public.rdf_file AS rf \
LEFT JOIN usr.usr_landmark_info AS uli ON uli.file_name=rf.file_name, \
public.rdf_cf AS rc \
LEFT JOIN public.rdf_cf_building AS rcb ON rcb.cf_id=rc.cf_id \
LEFT JOIN public.%s AS stb ON stb.building_id=rcb.building_id \
WHERE rc.cf_type='G' AND rc.cf_id=rff.feature_id AND rff.file_id = rf.file_id GROUP BY rc.cf_id"%(ST_BUILDING)
        print cmd
        self.cursor.copy_expert("COPY (%s) TO STDOUT DELIMITER '`'"%(cmd),open(self.dump_file,"w"))

    def get_statistic(self):
        try:
            self.__init_buildings_table()
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

    def __init_buildings_table(self):
        if self.is_existtable('public',ST_BUILDING):
            return
        cmd = "SELECT * INTO public.%s FROM (\
SELECT DISTINCT(rbf.building_id) AS building_id \
FROM public.rdf_cf AS rc LEFT JOIN public.rdf_cf_building AS rcb ON rcb.cf_id=rc.cf_id \
LEFT JOIN public.rdf_building_face AS rbf ON rbf.building_id=rcb.building_id \
LEFT JOIN public.rdf_face_link AS rfl ON rfl.face_id=rbf.face_id \
LEFT JOIN public.rdf_link AS rl ON rfl.link_id=rl.link_id \
LEFT JOIN public.rdf_admin_hierarchy AS rah ON (rah.admin_place_id=rl.left_admin_place_id OR rah.admin_place_id=rl.right_admin_place_id) \
WHERE rah.iso_country_code IN (%s) \
) AS foo"%(ST_BUILDING,REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX))
        self.run_sql(self.cursor.execute,cmd)
        self.run_sql(self.cursor.execute,'CREATE INDEX %s ON public.%s (building_id)'%(ST_BUILDING_INDEX,ST_BUILDING))

    def __filter(self,line):
        return line[3] != line[4] or '0' == line[6]

    def __statistic(self,line):
        if self.__filter(line):
            return
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

    def __get_file_path(self,keys,line):
        if '\N' != line[2]:
            self.__count(keys[0])

    def __get_3d_landmark_model_standard_file_name(self,keys,line):
        if '\N' != line[1] and -1 != line[1].find('11'):
            self.__count(keys[0])

    def __get_3d_landmark_model_light_file_name(self,keys,line):
        if '\N' != line[1] and -1 != line[1].find('12'):
            self.__count(keys[0])

if __name__ == "__main__":
    # use to test this model
    bg = datetime.datetime.now()
    stat =  Relations3dlandmark('na').get_statistic()
    keys = stat.keys()
    print "==>"
    print "{%s}"%(",".join(map(lambda px: "\"%s\":%s"%(px,stat[px]) ,keys)))
    print "<=="
    ed = datetime.datetime.now()
    print "Cost time:"+str(ed - bg)
