#-------------------------------------------------------------------------------
# Name:        RelationsGeneralCarto model
# Purpose:     this model is used to mapping the
#              columns: [ ]
#
# Author:      Kuang
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
GLOBAL_KEY_PREFIX = "ways_unavlink_"
CSV_SEP           = '`'
LF                = '\n'

FEATURE_TYPE_MAPPING = \
{
'907197': [('boundary', 'administrative'), ('admin_level', '2'), ('boundary_type', 'disputed')],
'907196': [('boundary', 'administrative'), ('admin_level', '2')],
'909996': [('boundary', 'administrative'), ('admin_level', '4')],
'900170': [('boundary', 'administrative'), ('admin_level', '6')],
#'908000': [('boundary', 'cartographic_administrative'), ('admin_level', '2')],
#'908001': [('boundary', 'cartographic_administrative'), ('admin_level', '4')],
'908002': [('boundary', 'neighborhood')],
'908003': [('boundary', 'cartographic_administrative'), ('admin_level', '10')],
#'908004': [('boundary', 'cartographic_administrative'), ('admin_level', '2'), ('boundary_type', 'disputed')],
#'9997022': [('boundary', 'cartographic_administrative'), ('admin_level', '2'), ('boundary_type', 'disputed')],
#'908005': [('boundary', 'cartographic_administrative'), ('admin_level', '4'), ('boundary_type', 'disputed')],
#'9997023': [('boundary', 'cartographic_administrative'), ('admin_level', '4'), ('boundary_type', 'disputed')],
'909997': [('boundary', 'administrative'), ('admin_level', '4'), ('boundary_type', 'disputed')],
'9997019': [('boundary', 'cartographic_administrative'), ('boundary_type', 'special_admin_region')],
'1800201': [('railway', 'rail')],
'1800202': [('railway', 'subway')],
'1800203': [('railway', 'light_rail')],
'500412': [('waterway', 'river')],
'500413': [('waterway', 'wadi')],
'500414': [('waterway', 'canal')],
'9997020': [('boundary', 'cartographic_administrative'), ('boundary_type', 'south_sea')]
}

STATISTIC_GENERAL_KEYS = \
(
('name_place_type', True, 'name_place_type'),
('display_class', True, 'display_class'),
('polygon_restriction', True, 'polygon_restriction'),
('severity_rating', True, 'severity_rating'),
)

class WaysCartoLine(Record):
    def __init__(self, region):
        Record.__init__(self)
        self.carto_dump_file = os.path.join(ROOT_DIR, 'temporary', self.__class__.__name__)
        self.stat                    = {}
        self.region                  = region

    def dump2file(self):
        cmd = "SELECT \
            DISTINCT rl.link_id, \
            rc.carto_id, \
            rc.feature_type, \
            rc.named_place_type, \
            rc.display_class, \
            rc.polygon_restriction, \
            rc.severity_rating \
            from \
            public.rdf_carto rc, public.rdf_carto_link rcl, rdf_link rl, rdf_admin_hierarchy rah \
            where \
            rc.carto_id = rcl.carto_id and \
            rcl.link_id = rl.link_id and \
            (rl.left_admin_place_id = rah.admin_place_id or rl.right_admin_place_id = rah.admin_place_id) and \
            rah.iso_country_code in (%s)" % (REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX))
        print cmd
        self.cursor.copy_expert("COPY (%s) TO STDOUT DELIMITER '`'"%(cmd),open(self.carto_dump_file,"w"))

    def get_statistic(self):
        try:
            self.dump2file()
        except:
            print 'Some table or schema don\'t exist! Please check the upper sql'
            return {}
        processcount = 0
        with open(self.carto_dump_file, "r",1024*1024*1024) as csv_f:
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
        try:
            self.__get_featuretype(line)
        except:
            print line
            print "The statistic [ %s ] didn't exist"%(line[2])
            print ("Unexpected error:[ WaysCartoLine.py->__statistic] "+str(sys.exc_info()))

        for keys in STATISTIC_GENERAL_KEYS:
            try:
                getattr(self,'_WaysCartoLine__get_'+keys[2])(keys,line)
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

    def __get_name_place_type(self, keys, line):
        if ' ' != line[3]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0], keys[1] and "#%s"%(line[3]) or ""))

    def __get_display_class(self, keys, line):
        if '\N' != line[4]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0], keys[1] and "#%s"%(line[4]) or ""))

    def __get_polygon_restriction(self, keys, line):
        if '\N' != line[5]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0], keys[1] and "#%s"%(line[5]) or ""))

    def __get_severity_rating(self, keys, line):
        if '\N' != line[6]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0], keys[1] and "#%s"%(line[6]) or ""))

if __name__ == "__main__":
    # use to test this model
    bg = datetime.datetime.now()
    stat =  WaysCartoLine('anz').get_statistic()
    keys = stat.keys()
    print "==>"
    print "{%s}"%(",".join(map(lambda px: "\"%s\":%s"%(px,stat[px]) ,keys)))
    print "<=="
    ed = datetime.datetime.now()
    print "Cost time:"+str(ed - bg)
