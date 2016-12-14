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
import re

ROOT_DIR          = os.path.join(os.path.dirname(os.path.abspath(__file__)),"..")
GLOBAL_KEY_PREFIX = "relations_multipolygon_"
CSV_SEP           = '`'
LF                = '\n'

FEATURE_TYPE_MAPPING = \
{
'500116': [('natural', 'ocean')],
'500412': [('waterway', 'riverbank')],
'500413': [('waterway', 'wadi')],
'500414': [('waterway', 'canal')],
'500421': [('natural', 'water')],
'507116': [('natural', 'bay')],
'509997': [('natural', 'glacier')],
'509998': [('natural', 'beach')],
'509999': [('place', 'island')],
'600101': [('prone_area', 'hurricane')],
'600102': [('prone_area', 'flood')],
'600103': [('prone_area', 'tsunami')],
'900101': [('boundary', 'administrative'), ('admin_level', '8')],
'900103': [('boundary', 'national_park')],
'900107': [('boundary', 'native_american_resv')],
'908002': [('boundary', 'neighborhood')],
'900108': [('landuse', 'military')],
'900130': [('leisure', 'state_park')],
'900140': [('leisure', 'park_in_water')],
'900150': [('leisure', 'park')],
'900156': [('boundary', 'built_up_area')],
'900158': [('highway', 'pedestrian')],
'900159': [('highway', 'undefined_traffic_area')],
'900160': [('landuse', 'apartment_complex')],
'900170': [('boundary', 'administrative'), ('admin_level', '6')],
'900202': [('landuse', 'forest')],
'1700215': [('amenity', 'parking')],
'1700216': [('landuse', 'garages')],
'1900403': [('aeroway', 'aerodrome')],
'1907403': [('aeroway', 'runway')],
'2000123': [('leisure', 'golf_course')],
'2000124': [('shop', 'mall')],
'2000200': [('landuse', 'industrial')],
'2000403': [('amenity', 'university')],
'2000408': [('amenity', 'hospital')],
'2000420': [('landuse', 'cemetery')],
'2000457': [('leisure', 'sports_centre')],
'2000460': [('tourism', 'theme_park')],
'2000461': [('tourism', 'zoo')],
'9997004': [('boundary', 'congestion_zone')],
'9997007': [('boundary', 'railyard')],
'9997008': [('landuse', 'harbour')],
'9997010': [('boundary', 'environmental_zone')],
'9997019': [('boundary', 'special_admin_region')]
}

#(key, category, function)
STATISTIC_FEATURE_TYPE_KEYS = \
(
('waterway',True,'waterway'),
('natural',True,'natural'),
('place',False,'place'),
("prone_area",True,"prone_area"),
('boundary', True, 'boundary'),
('landuse', True, 'Landuse'),
('leisure', True, 'leisure'),
('highway', True, 'highway'),
('admin_level', True, 'admin_level'),
('amenity', True, 'amenity'),
('aeroway', True, 'aeroway'),
('shop', True, 'shop'),
('tourism', True, 'tourism'),
)

STATISTIC_GENERAL_KEYS = \
(
('name_place_type', True, 'name_place_type'),
('display_class', True, 'display_class'),
('polygon_restriction', True, 'polygon_restriction'),
('severity_rating', True, 'severity_rating'),
('long_haul', True, 'long_haul'),
('cov_indicator', True, 'cov_indicator'),
('claimed_by', False, 'claimed_by'),
('controlled_by', False, 'controlled_by'),
)

class RelationsGeneralCarto(Record):
    def __init__(self, region):
        Record.__init__(self)
        self.carto_dump_file            = os.path.join(ROOT_DIR, 'temporary', self.__class__.__name__)
        self.cartoid_mface_dump_file    = os.path.join(ROOT_DIR, 'temporary', self.__class__.__name__+'_cartoid_mfaces')
        self.carto_candidator_dump_file = os.path.join(ROOT_DIR, 'temporary', self.__class__.__name__+'_carto_candidator')
        self.version                    = re.search('[a-zA-Z]+_[a-zA-Z]+(\d+[a-zA-Z]{1}\d+)', self.database).group(1).lower()
        self.continent                  = re.search('([a-zA-Z]+)_*.*', region).group(1)
        self.cartoid_polygoncliiper_cfg = os.path.join(ROOT_DIR, "config", "carto", self.continent, self.version)
        self.region                     = region
        self.stat                       = {}
        self.cartoidmface               = {}
        self.cartoid_polygonclipper     = {}
        self.carto_candidator           = {}

    def dump2file(self):
        cmd = "SELECT \
            DISTINCT rcf.face_id, \
            rc.carto_id, \
            rc.feature_type, \
            rc.named_place_type, \
            rc.display_class, \
            rc.polygon_restriction, \
            rc.severity_rating, \
            rcf.long_haul, \
            rcf.coverage_indicator, \
            rcf.claimed_by, \
            rcf.controlled_by \
            from \
            public.rdf_carto rc, public.rdf_carto_face rcf \
            where \
            rc.carto_id = rcf.carto_id"
        print cmd
        self.cursor.copy_expert("COPY (%s) TO STDOUT DELIMITER '`'"%(cmd),open(self.carto_dump_file,"w"))

    def dumpcartocandidator2file(self):
        cmd = 'SELECT \
        distinct rc.carto_id \
        from \
        public.rdf_carto rc, \
        public.rdf_carto_face rcf, \
        public.rdf_face_link rfl, \
        rdf_link rl, \
        rdf_admin_hierarchy rah \
        where \
        rc.carto_id = rcf.carto_id \
        and \
        rcf.face_id = rfl.face_id \
        and \
        rfl.link_id = rl.link_id \
        and \
        (rl.left_admin_place_id = rah.admin_place_id \
        or \
        rl.right_admin_place_id = rah.admin_place_id) \
        and \
        rah.iso_country_code in (%s)' % (REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX, 'carto'))
        print cmd
        self.cursor.copy_expert("COPY (%s) TO STDOUT DELIMITER '`'"%(cmd),open(self.carto_candidator_dump_file,"w"))
        print 'dump candidator carto id'

        processcount = 0
        with open(self.carto_candidator_dump_file, 'r', 1024*1024*100) as carto_candidator_f:
            for line in carto_candidator_f:
                line = line.strip()
                if not line:
                    continue
                self.carto_candidator[line] = None
                processcount += 1
                if processcount%5000 == 0:
                    print "\rProcess index [ "+str(processcount)+" ]",
            print "\rProcess index [ "+str(processcount)+" ]\n",
            print "Finishing dumping and loading candidator carto id"

    def dumpcartowithmface2file(self):
        cmd = 'SELECT \
            rc.carto_id \
            FROM \
            public.rdf_carto rc, public.rdf_carto_face rcf \
            WHERE \
            rc.carto_id = rcf.carto_id \
            group by rc.carto_id having count(distinct rcf.face_id) > 1'
        print cmd
        self.cursor.copy_expert("COPY (%s) TO STDOUT DELIMITER '`'"%(cmd),open(self.cartoid_mface_dump_file,"w"))

        print "Dump carto id with multiple faces"
        processcount = 0
        with open(self.cartoid_mface_dump_file, "r",1024*1024*10) as csv_f:
            for line in csv_f:
                line = line.strip()
                if not line:
                    continue
                self.cartoidmface[line] = None
                processcount += 1
                if processcount%5000 == 0:
                    print "\rProcess index [ "+str(processcount)+" ]",
            print "\rProcess index [ "+str(processcount)+" ]",

    def get_statistic(self):
        try:
            self.dump2file()
            self.dumpcartowithmface2file()
            self.loadcartoidpolygonclipper()
            self.dumpcartocandidator2file()
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
                if (not self.carto_candidator.has_key(line_p[1]) and line_p[2] != '500116') or self.cartoid_polygonclipper.has_key(line_p[1]) or self.cartoid_polygonclipper.has_key(line_p[0]):
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

    def loadcartoidpolygonclipper(self):
        iscartopolygonclipper = False
        with open(self.cartoid_polygoncliiper_cfg, "r", 1024*1024) as cfg:
            for line in cfg:
                line = line.strip()
                if len(line) == 0:
                    continue
                if line.startswith("[") and line.endswith("]") and -1 != line.find("carto_id_polygon_clipper"):
                    iscartopolygonclipper = True
                    continue
                if iscartopolygonclipper:
                    self.cartoid_polygonclipper[line] = None
                if line.startswith("[") and line.endswith("]") and -1 == line.find("carto_id_polygon_clipper"):
                    iscartopolygonclipper = False

    def __statistic(self,line):
        try:
            self.__get_featuretype(line)
        except:
            print line
            print "The statistic [ %s ] didn't exist"%(line[2])
            print ("Unexpected error:[ RelationGeneralCarto.py->__statistic] "+str(sys.exc_info()))

        for keys in STATISTIC_GENERAL_KEYS:
            try:
                getattr(self,'_RelationsGeneralCarto__get_'+keys[2])(keys,line)
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

        if self.cartoidmface.has_key(line[1]):
            for val in vals:
                self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,val[0], "#%s"%(val[1]) or ""))
            self.__get_name_place_type(('name_place_type', True, 'name_place_type'), line)
            self.__get_display_class(('display_class', True, 'display_class'), line)
            self.__get_polygon_restriction(('polygon_restriction', True, 'polygon_restriction'), line)
            self.__get_severity_rating(('severity_rating', True, 'severity_rating'), line)
            self.cartoidmface.pop(line[1], None)

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

    def __get_long_haul(self, keys, line):
        if 'Y' == line[7]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0], keys[1] and "#%s"%('Y') or ""))

    def __get_cov_indicator(self, keys, line):
        if '\N' != line[8]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0], keys[1] and "#%s"%(line[8]) or ""))

    def __get_claimed_by(self, keys, line):
        if '\N' != line[9]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0], keys[1] and "#%s"%(line[9]) or ""))

    def __get_controlled_by(self, keys, line):
        if '\N' != line[10]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0], keys[1] and "#%s"%(line[10]) or ""))

if __name__ == "__main__":
    # use to test this model
    bg = datetime.datetime.now()
    stat =  RelationsGeneralCarto('eu').get_statistic()
    keys = stat.keys()
    print "==>"
    print "{%s}"%(",".join(map(lambda px: "\"%s\":%s"%(px,stat[px]) ,keys)))
    print "<=="
    ed = datetime.datetime.now()
    print "Cost time:"+str(ed - bg)
