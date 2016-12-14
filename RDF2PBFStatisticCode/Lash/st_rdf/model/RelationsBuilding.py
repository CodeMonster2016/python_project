#-------------------------------------------------------------------------------
# Name:        RelationsGeneralCarto model
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
GLOBAL_KEY_PREFIX = "relations_multipolygon_"
CSV_SEP           = '`'
LF                = '\n'

FEATURE_TYPE_MAPPING = \
{
'2005000': [('building', 'commercial')],
'2005001': [('building', 'commercial'), ('amenity', 'bank')],
'2005002': [('building', 'commercial')],
'2005003': [('building', 'hotel')],
'2005004': [('building', 'commercial'), ('amenity', 'car_rental')],
'2005005': [('building', 'commercial'), ('shop', 'car')],
'2005006': [('building', 'commercial'), ('shop', 'car_repair')],
'2005007': [('building', 'commercial'), ('amenity', 'fuel')],
'2005050': [('building', 'civic')],
'2005100': [('building', 'civic')],
'2005101': [('building', 'civic'), ('amenity', 'library')],
'2005102': [('building', 'civic'), ('amenity', 'museum')],
'2005103': [('building', 'civic'), ('amenity', 'performing_arts')],
'2005104': [('building', 'civic'), ('amenity', 'community_centre')],
'2005150': [('building', 'school')],
'2005151': [('building', 'university')],
'2005152': [('building', 'school')],
'2005200': [('building', 'public')],
'2005201': [('building', 'public'), ('amenity', 'police')],
'2005250': [('building', 'public')],
'2005251': [('building', 'civic'), ('amenity', 'embassy')],
'2005252': [('building', 'public'), ('amenity', 'post_office')],
'2005253': [('building', 'public'), ('amenity', 'townhall')],
'2005254': [('building', 'public'), ('amenity', 'courthouse')],
'2005255': [('building', 'commercial'), ('office', 'government')],
'2005256': [('building', 'public'), ('amenity', 'county_council')],
'2005300': [('building', 'historic')],
'2005301': [('building', 'historic')],
'2005350': [('building', 'hospital')],
'2005351': [('building', 'hospital')],
'2005352': [('building', 'hospital')],
'2005400': [('building', 'leisure_activity')],
'2005401': [('building', 'leisure_activity'), ('amenity', 'bar')],
'2005402': [('building', 'leisure_activity'), ('sport', '10pin')],
'2005403': [('building', 'leisure_activity'), ('amenity', 'casino')],
'2005404': [('building', 'leisure_activity'), ('amenity', 'cinema')],
'2005405': [('building', 'leisure_activity')],
'2005406': [('building', 'leisure_activity')],
'2005407': [('building', 'leisure_activity'), ('tourism', 'theme_park')],
'2005408': [('building', 'leisure_activity'), ('leisure', 'golf_course')],
'2005409': [('building', 'leisure_activity'), ('sport', 'skating')],
'2005410': [('building', 'leisure_activity')],
'2005411': [('building', 'leisure_activity'), ('leisure', 'park')],
'2005412': [('building', 'leisure_activity')],
'2005413': [('building', 'leisure_activity'), ('leisure', 'ski_resort')],
'2005450': [('building', 'residential')],
'2005451': [('building', 'residential')],
'2005452': [('building', 'apartments')],
'2005453': [('building', 'house')],
'2005500': [('building', 'retail')],
'2005501': [('building', 'retail')],
'2005502': [('building', 'retail')],
'2005503': [('building', 'retail')],
'2005504': [('building', 'retail')],
'2005505': [('building', 'retail')],
'2005506': [('building', 'retail')],
'2005507': [('building', 'retail')],
'2005508': [('building', 'retail')],
'2005509': [('building', 'retail')],
'2005510': [('building', 'retail')],
'2005511': [('building', 'retail')],
'2005512': [('building', 'retail')],
'2005513': [('building', 'retail')],
'2005514': [('building', 'retail')],
'2005550': [('building', 'civic'), ('amenity', 'sports_centre')],
'2005551': [('building', 'civic'), ('amenity', 'sports_centre')],
'2005552': [('building', 'civic'), ('amenity', 'sports_activity')],
'2005600': [('building', 'tourism')],
'2005601': [('building', 'tourism')],
'2005602': [('building', 'tourism')],
'2005603': [('building', 'bridge')],
'2005650': [('building', 'transportation')],
'2005651': [('building', 'airport')],
'2005652': [('building', 'transportation'), ('public_transport', 'station')],
'2005653': [('building', 'transportation'), ('public_transport', 'station')],
'2005654': [('building', 'transportation'), ('amenity', 'ferry_terminal')],
'2005655': [('building', 'train_station')],
'2005656': [('building', 'transportation')],
'2005657': [('building', 'transportation')],
'2005700': [('building', 'yes')],
'2005750': [('building', 'church')],
'2005751': [('building', 'church')],
'2005752': [('building', 'church'), ('religion', 'muslim')],
'2005753': [('building', 'church')],
'2005754': [('building', 'church'), ('religion', 'jewish')],
'2005755': [('building', 'church'), ('religion', 'hindu')],
'2005800': [('building', 'industrial')],
'2005801': [('building', 'industrial'), ('industrial', 'factory')],
'2005850': [('building', 'parking_garage')],
'2005900': [('building', 'yes')],
'2005901': [('building', 'bridge')]
}

STATISTIC_GENERAL_KEYS = \
(
('long_haul', True, 'long_haul'),
('cov_indicator', True, 'cov_indicator'),
('height', False, 'height'),
('ground_clearance', False, 'ground_clearance'),
)

class RelationsBuilding(Record):
    def __init__(self, region):
        Record.__init__(self)
        self.building_dump_file = os.path.join(ROOT_DIR, 'temporary', self.__class__.__name__)
        self.enbuilding_dump_file = os.path.join(ROOT_DIR, 'temporary', self.__class__.__name__+'_enhance')
        self.stat                                          = {}
        self.enbuilding_main_feature_type                  = {}
        self.region                                        = region

    def dump2file(self):
        cmd = "select \
            distinct rbf.face_id, rb.building_id, rb.feature_type, rb.height, rb.ground_clearance, rbf.long_haul, rbf.coverage_indicator \
            from \
            rdf_building rb, rdf_building_face rbf, rdf_face_link rfl, rdf_link rl, rdf_admin_hierarchy rah \
            where \
            rb.building_id = rbf.building_id and \
            rb.feature_type != 2005999 and \
            rbf.face_id = rfl.face_id and \
            rfl.link_id = rl.link_id and \
            (rl.left_admin_place_id = rah.admin_place_id or rl.right_admin_place_id = rah.admin_place_id) and \
            rah.iso_country_code in (%s)"% (REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX))
        print cmd
        self.cursor.copy_expert("COPY (%s) TO STDOUT DELIMITER '`'"%(cmd),open(self.building_dump_file,"w"))

    def dumpenbuilding2file(self):
        cmd = "select \
            distinct rb.building_id, rbf.face_id, rbef.feature_type, rb.height, rb.ground_clearance, rbf.long_haul, rbf.coverage_indicator, rbef.main_feat_type \
            from \
            rdf_building rb, rdf_building_face rbf, rdf_building_enh_feature rbef, rdf_face_link rfl, rdf_link rl, rdf_admin_hierarchy rah \
            where \
            rb.building_id = rbf.building_id and \
            rb.building_id = rbef.building_id and \
            rb.feature_type = 2005999 and \
            rbf.face_id = rfl.face_id and \
            rfl.link_id = rl.link_id and \
            (rl.left_admin_place_id = rah.admin_place_id or rl.right_admin_place_id = rah.admin_place_id) and \
            rah.iso_country_code in (%s) \
            order by rb.building_id, rbef.feature_type"  %(REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX))
        print cmd
        self.cursor.copy_expert("COPY (%s) TO STDOUT DELIMITER '`'"%(cmd),open(self.enbuilding_dump_file,"w"))

        processcount = 0
        with open(self.enbuilding_dump_file, "r",1024*1024*100) as csv_f:
            for line in csv_f:
                line = line.strip()
                if not line:
                    continue
                line_p = line.split(CSV_SEP)
                if 'Y' == line_p[7]:
                    self.enbuilding_main_feature_type[line_p[1]] = None
                processcount += 1
                if processcount%5000 == 0:
                    print "\rProcess index [ "+str(processcount)+" ]",
            print "\rProcess index [ "+str(processcount)+" ]",

    def get_statistic(self):
        try:
            self.dump2file()
            self.dumpenbuilding2file()
        except:
            print 'Some table or schema don\'t exist! Please check the upper sql'
            return {}
        processcount = 0
        with open(self.building_dump_file, "r",1024*1024*1024) as csv_f:
            for line in csv_f:
                line = line.rstrip()
                line_p = line.split(CSV_SEP)
                if line_p[1] == "753219332":
                   pass
                if len(line_p) < 1:
                    continue
                self.__statistic(line_p)
                processcount += 1
                if processcount%5000 == 0:
                    print "\rProcess index [ "+str(processcount)+" ]",
            print "\rProcess index [ "+str(processcount)+" ]",

        processed_enbuilding = {}
        processcount = 0
        with open(self.enbuilding_dump_file, "r", 1024*1024*1024) as csv_f:
            for line in csv_f:
                line = line.rstrip()
                line_p = line.split(CSV_SEP)
                if line_p[1] == "753219332":
                   pass
                if len(line_p) < 1:
                    continue
                if self.enbuilding_main_feature_type.has_key(line[1]):
                    if "Y" == line[7]:
                        self.__statistic(line_p)
                        processed_enbuilding[line_p[1]] = None
                else:
                    if not processed_enbuilding.has_key(line_p[1]):
                        self.__statistic(line_p)
                        processed_enbuilding[line_p[1]] = None

        # write to file
        with open(os.path.join(ROOT_DIR, "output", "stat", self.__class__.__name__), 'w') as stf:
            stf.write(json.dumps(self.stat))
        return self.stat

    def __statistic(self,line):
        try:
            self.__get_featuretype(line)
        except:
            print "The statistic [ %s ] didn't exist"%(line[2])
            print ("Unexpected error:[ RelationGeneralCarto.py->__statistic] "+str(sys.exc_info()))

        for keys in STATISTIC_GENERAL_KEYS:
            try:
                getattr(self,'_RelationsBuilding__get_'+keys[2])(keys,line)
            except:
                print "The statistic [ %s ] didn't exist"%(keys[2])
                print ("Unexpected error:[ RelationGeneralCarto.py->__statistic] "+str(sys.exc_info()))

    def __count(self,key):
        if self.stat.has_key(key):
            self.stat[key] += 1
        else:
            self.stat[key] = 1

    def __get_featuretype(self, line):
        vals = []
        if FEATURE_TYPE_MAPPING.has_key(line[2]):
            vals = FEATURE_TYPE_MAPPING[line[2]]
            for val in vals:
                self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,val[0], "#%s"%(val[1]) or ""))

    def __get_long_haul(self, keys, line):
        if 'Y' == line[5]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0], keys[1] and "#%s"%('Y') or ""))

    def __get_cov_indicator(self, keys, line):
        if '\N' != line[6]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0], keys[1] and "#%s"%(line[6]) or ""))

    def __get_height(self, keys, line):
        if '\N' != line[3]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0], keys[1] and "#%s"%(line[3]) or ""))

    def __get_ground_clearance(self, keys, line):
        if '\N' != line[4]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0], keys[1] and "#%s"%(line[4]) or ""))

if __name__ == "__main__":
    # use to test this model
    bg = datetime.datetime.now()
    stat =  RelationsBuilding('na').get_statistic()
    keys = stat.keys()
    print "==>"
    print "{%s}"%(",".join(map(lambda px: "\"%s\":%s"%(px,stat[px]) ,keys)))
    print "<=="
    ed = datetime.datetime.now()
    print "Cost time:"+str(ed - bg)
