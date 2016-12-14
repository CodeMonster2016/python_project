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
from operator import itemgetter
from xml.dom import minidom
from collections import defaultdict

import os
import sys
import datetime
import json

ROOT_DIR          = os.path.join(os.path.dirname(os.path.abspath(__file__)),"..")
PREERRED_ROUTE_CONFIG_FILE     = os.path.join(os.path.dirname(ROOT_DIR), "..", "config", "Route.xml")
GLOBAL_KEY_PREFIX = "ways_navlink_"
CSV_SEP           = '`'
LF                = '\n'

PREV_LINKID = -1

STATISTIC_GENERAL_NAME_FEATURE_KEYS = (
'route_type',
'attached_to_base',
'precedes_base',
'name_direction_prefix',
'street_type',
'name_direction_suffix',
'name_base',
'is_exonym',
'name_type',
'direction_on_sign',
'street_name'
)

STATISTIC_TRANS_NAME_FEATURE_KEYS = (
'attached_to_base',
'precedes_base',
'name_direction_prefix',
'street_type',
'name_direction_suffix',
'name_base',
'direction_on_sign',
'street_name'
)

class WaysName(Record):
    def __init__(self, region):
        Record.__init__(self)
        self.name_dump_file = os.path.join(ROOT_DIR, 'temporary', self.__class__.__name__)
        self.admin_hierarchy_dump_file = os.path.join(ROOT_DIR, 'temporary', 'admin_hierarchy')
        self.stat                    = {}
        self.region                  = region
        self.link_name_dict          = {}
        self.admin_hierarchy_dict    = {}
        self.preferred_route_type    = defaultdict()
        #self.debug = os.path.join(ROOT_DIR, 'temporary', "debug")
        #self.debug_id = []

    def dump2file(self):
        cmd = "select \
rnl.link_id, \
rrl.road_name_id, \
rrl.is_exit_name, \
rrl.is_name_on_roadsign, \
rrn.route_type, \
rrn.name_type, \
rrn.is_exonym, \
rrn.language_code, \
rrn.attached_to_base, \
rrn.precedes_base, \
rrn.prefix, \
rrn.street_type, \
rrn.suffix, \
rrn.base_name, \
rrn.direction_on_sign, \
rrn.street_name, \
rrnt.*, \
vrn.*, \
vpt.*, \
vgo.*, \
rl.left_admin_place_id, \
rl.right_admin_place_id, \
rrl.road_link_id, \
rnl.iso_country_code \
from \
rdf_nav_link rnl \
inner join rdf_road_link rrl on rnl.link_id = rrl.link_id \
inner join rdf_road_name rrn on rrl.road_name_id = rrn.road_name_id \
left join rdf_road_name_trans rrnt on rrn.road_name_id = rrnt.road_name_id \
left join vce_road_name vrn on rrn.road_name_id = vrn.road_name_id \
left join vce_phonetic_text vpt on vrn.phonetic_id = vpt.phonetic_id \
left join vce_geo_override vgo on vrn.phonetic_id = vgo.phonetic_id \
inner join rdf_link rl on rnl.link_id = rl.link_id \
where rnl.iso_country_code in (%s) \
order by rnl.link_id" % (REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX))
        print cmd
        self.cursor.copy_expert("COPY (%s) TO STDOUT DELIMITER '`'"%(cmd),open(self.name_dump_file,"w"))

    def dumpadminhierarchyfile(self):
        cmd = "select * from rdf_admin_hierarchy"
        print cmd
        self.cursor.copy_expert("COPY (%s) TO STDOUT DELIMITER '`'" % (cmd), open(self.admin_hierarchy_dump_file, "w"))
        #load admin hierarchy file into memory
        with open(self.admin_hierarchy_dump_file, "r") as admin_hierarchy_f:
            for admin_line in admin_hierarchy_f:
                admin_line = admin_line.strip()
                admin_line_p = admin_line.split(CSV_SEP)
                if len(admin_line_p) < 1:
                    continue
                self.admin_hierarchy_dict[admin_line_p[0]] = [admin_line_p[3], admin_line_p[4], admin_line_p[5], admin_line_p[6], admin_line_p[7]]

    def get_statistic(self):
        self._load_preferred_route_type()
        self.dumpadminhierarchyfile()
        try:
            self.dump2file()
        except:
            print 'Some table or schema don\'t exist! Please check the upper sql'
            return {}
        processcount = 0
        with open(self.name_dump_file, "r",1024*1024*1024) as csv_f:
            for line in csv_f:
                line = line.rstrip()
                line_p = line.split(CSV_SEP)
                if len(line_p) < 1:
                    continue
                self._build(line_p)
                processcount += 1
                if processcount%5000 == 0:
                    print "\rProcess index [ "+str(processcount)+" ]",
            print "\rProcess index [ "+str(processcount)+" ]",
            self.__statistic()

        # write to file
        with open(os.path.join(ROOT_DIR, "output", "stat", self.__class__.__name__), 'w') as stf:
            stf.write(json.dumps(self.stat))
        return self.stat

    def _load_preferred_route_type(self):
        doc = None
        try:
            doc = minidom.parse(PREERRED_ROUTE_CONFIG_FILE)
        except:
            print "failed to parse route type configure file"
            return
        if not doc:
            return
        root = doc.documentElement
        if root:
            routeelmts = root.getElementsByTagName('Route')
            if routeelmts:
                for recelmt in routeelmts:
                    iso_country_code = self._get_node_value(recelmt, "ISO_COUNTRY_CODE")
                    route_type  = self._get_node_value(recelmt, "ROUTE_TYPE")
                    is_preferred_display  = self._get_node_value(recelmt, "IS_PREFERRED_DISPLAY")
                    key = "%s_%s"%(iso_country_code, route_type)
                    if is_preferred_display and is_preferred_display == "Y":
                        self.preferred_route_type[key.lower()] = 0
                    elif is_preferred_display and is_preferred_display == "N":
                        self.preferred_route_type[key.lower()] = 2

    def _get_node_value(self, xmlnode, key):
        elmt = xmlnode.getElementsByTagName(key)
        if not elmt:
            return None
        return elmt[0].childNodes[0].data

    def _get_name_type(self, line_p):
        road_type = 0 if line_p[3] == "\\N" else int(line_p[3])
        if line_p[1] == "Y":
            return "exit_ref"
        elif road_type >= 1 and road_type <=6:
            return "ref"
        elif line_p[2] == "N":
            return "alt_name"
        elif not (road_type >= 1 and road_type <=6) and (line_p[4] == "B" and line_p[5] == "N"):
            return "name"
        elif not (road_type >= 1 and road_type <=6) and not (line_p[4] == "B" and line_p[5] == "N"):
            return "alt_name"

    def _routetypecmp(self, list_a, list_b):
        key_a = "%s_%s" % (list_a[18], list_a[3])
        #is_prefer_a = self.preferred_route_type.has_key(key_a.lower())
        prefer_a_priority = self.preferred_route_type.get(key_a.lower(), 1)
        key_b = "%s_%s" % (list_b[18], list_b[3])
        #is_prefer_b = self.preferred_route_type.has_key(key_b.lower())
        prefer_b_priority = self.preferred_route_type.get(key_b.lower(), 1)
        #v = 0 if (is_prefer_a and is_prefer_b) or (not is_prefer_a and not is_prefer_b) else -1 if is_prefer_a else 1;
        v = prefer_a_priority - prefer_b_priority
        if v != 0:
        	return v
        route_type_a = int(list_a[3])
        route_type_b = int(list_b[3])
        v = route_type_a - route_type_b
        if v != 0:
        	return v
        if list_a[14] < list_b[14]:
        	return -1
        else:
        	return 1

    def __statistic(self):
        name_dict = {}
        for name_id in self.link_name_dict.keys():
            names = self.link_name_dict.get(name_id)
            name_type = self._get_name_type(names)
            if not name_dict.has_key(names[6]):
                name_dict[names[6]] = {}
            name_type_dict = name_dict[names[6]]
            if name_type_dict.has_key(name_type):
                name_type_list = name_type_dict[name_type]
                name_type_list.append(names)
            else:
                name_type_dict[name_type] = [names]

        for lang in name_dict.keys():
            for nametype in name_dict[lang].keys():
                if nametype != "ref":
                    nameslist = sorted(name_dict[lang][nametype], key=itemgetter(14, 17))
                else:
                    nameslist = sorted(name_dict[lang][nametype], cmp=self._routetypecmp)
                for key in STATISTIC_GENERAL_NAME_FEATURE_KEYS:
                    getattr(self,'_WaysName__get_'+key)(nametype, lang.lower(), key, nameslist[0])
                for transkey in STATISTIC_TRANS_NAME_FEATURE_KEYS:
                    trans_literation_type = nameslist[0][15][0][1]
                    if trans_literation_type == "\N":
                        continue
                    if transkey == STATISTIC_TRANS_NAME_FEATURE_KEYS[7]:
                        trans_full_key = "{0}{1}:{2}:trans:{3}".format(GLOBAL_KEY_PREFIX, nametype, lang.lower(), trans_literation_type)
                    else:
                        trans_full_key = "{0}{1}:{2}:trans:{3}:{4}".format(GLOBAL_KEY_PREFIX, nametype, lang.lower(), trans_literation_type, transkey)
                    getattr(self,'_WaysName__get_trans_'+transkey)(trans_full_key, nameslist[0][15][0])

                for p_keys in self._get_phonetic_key(nameslist[0][16]):
                    self.__count("{0}{1}:{2}:{3}".format(GLOBAL_KEY_PREFIX, nametype, lang.lower(), p_keys))

    #26(0):road_name_id, 27(1):phonetic_id, 28(2):preferred, 29(3):type
    #30(4):phonetic_id, 31(5):phonetic_string, 32(6):phonetic_language_code, 33(7):transcription_method
    #34(8):geo_override_id, 35(9):phonetic_id, 36(10):admin_place_id, 37(11):preferred
    #38(12):left_admin_place_id, 39(13):right_admin_place_id
    def _get_phonetic_key(self, listphonetics):
        preferred_phonetics_dict = {}
        for phonetics in listphonetics:
            if phonetics[10] != "\N":
                if self._is_geo_override(phonetics[12], phonetics[10]) or self._is_geo_override(phonetics[13], phonetics[10]):
                    preferred_phonetics_dict[phonetics[1]] = phonetics

        if len(preferred_phonetics_dict) == 0:
            for phonetics in listphonetics:
                if phonetics[2] == "Y":
                    preferred_phonetics_dict[phonetics[1]] = phonetics

        preferred_phonetic = {}
        length = 0
        for key, vals in preferred_phonetics_dict.items():
            phonetic_key = "phonetics:{0}:{1}:{2}".format(vals[6], vals[7], vals[3])
            if not preferred_phonetic.has_key(phonetic_key):
                preferred_phonetic[phonetic_key] = None

        if len(preferred_phonetic) == 0:
            return []
        return preferred_phonetic.keys()

    def _is_geo_override(self, admin_place_id, geo_admin_place_id):
        if self.admin_hierarchy_dict.has_key(admin_place_id):
            admin_hierarchy = self.admin_hierarchy_dict.get(admin_place_id)
        if admin_hierarchy[4] == geo_admin_place_id:
            return True
        if admin_hierarchy[3] == geo_admin_place_id:
            return True
        if admin_hierarchy[2] == geo_admin_place_id:
            return True
        if admin_hierarchy[1] == geo_admin_place_id:
            return True
        if admin_hierarchy[0] == geo_admin_place_id:
            return True

    def __get_trans_attached_to_base(self, fullkey, p):
        if p[8] == "Y":
            self.__count(fullkey)

    def __get_trans_precedes_base(self, fullkey, p):
        if p[9] == "Y":
            self.__count(fullkey)

    def __get_trans_name_direction_prefix(self, fullkey, p):
        if p[5] != "\N":
            self.__count(fullkey)

    def __get_trans_street_type(self, fullkey, p):
        if p[4] != "\N":
            self.__count(fullkey)

    def __get_trans_name_direction_suffix(self, fullkey, p):
        if p[6] != "\N":
            self.__count(fullkey)

    def __get_trans_name_base(self, fullkey, p):
        if p[2] != "\N":
            self.__count(fullkey)

    def __get_trans_direction_on_sign(self, fullkey, p):
        if p[7] != "\N":
            self.__count(fullkey)

    def __get_trans_street_name(self, fullkey, p):
        if p[3] != "\N":
            self.__count(fullkey)


    def __get_route_type(self, nametype, language, key, p):
        if p[3] != "\N":
            self.__count("%s%s:%s:%s"%(GLOBAL_KEY_PREFIX, nametype, language, key))

    def __get_attached_to_base(self, nametype, language, key, p):
        if p[7] == "Y":
            self.__count("%s%s:%s:%s"%(GLOBAL_KEY_PREFIX, nametype, language, key))

    def __get_precedes_base(self, nametype, language, key, p):
        if p[8] == "Y":
            self.__count("%s%s:%s:%s"%(GLOBAL_KEY_PREFIX, nametype, language, key))

    def __get_name_direction_prefix(self, nametype, language, key, p):
        if p[9] != "\N":
            self.__count("%s%s:%s:%s"%(GLOBAL_KEY_PREFIX, nametype, language, key))

    def __get_street_type(self, nametype, language, key, p):
        if p[10] != "\N":
            self.__count("%s%s:%s:%s"%(GLOBAL_KEY_PREFIX, nametype, language, key))

    def __get_name_direction_suffix(self, nametype, language, key, p):
        if p[11] != "\N":
            self.__count("%s%s:%s:%s"%(GLOBAL_KEY_PREFIX, nametype, language, key))

    def __get_name_base(self, nametype, language, key, p):
        if p[12] != "\N":
            self.__count("%s%s:%s:%s"%(GLOBAL_KEY_PREFIX, nametype, language, key))

    def __get_is_exonym(self, nametype, language, key, p):
        if p[5] == "Y":
            self.__count("%s%s:%s:%s"%(GLOBAL_KEY_PREFIX, nametype, language, key))

    def __get_name_type(self, nametype, language, key, p):
        if p[4] != "\N":
            self.__count("%s%s:%s:%s"%(GLOBAL_KEY_PREFIX, nametype, language, key))

    def __get_direction_on_sign(self, nametype, language, key, p):
        if p[13] != "\N":
            self.__count("%s%s:%s:%s"%(GLOBAL_KEY_PREFIX, nametype, language, key))

    def __get_street_name(self, nametype, language, key, p):
        if p[14] != "\N":
            self.__count("%s%s:%s"%(GLOBAL_KEY_PREFIX, nametype, language))

    #0:link_id, 1:road_name_id, 2:is_exit_name, 3:is_name_on_roadsign, 4:route_type, 5:name_type, 6:is_exonym, 7:language_code
    #8:attached_to_base, 9:precedes_base, 10:prefix, 11:street_type, 12:suffix, 13:base_name, 14:direction_on_sign, 15:street_name
    #16:road_name_id, 17:transliteration_type, 18:base_name, 19:street_name, 20:street_type, 21:prefix, 22:suffix, 23:direction_on_sign, 24:attached_to_base, 25:precedes_base
    #26(0):road_name_id, 27(1):phonetic_id, 28(2):preferred, 29(3):type
    #30(4):phonetic_id, 31(5):phonetic_string, 32(6):phonetic_language_code, 33(7):transcription_method
    #34(8):geo_override_id, 35(9):phonetic_id, 36(10):admin_place_id, 37(11):preferred
    #38(12):left_admin_place_id, 39(13):right_admin_place_id, 40:road link id, 41: iso country code
    def _build(self, line_p):
        global PREV_LINKID
        if int(line_p[0]) != PREV_LINKID:
            if len(self.link_name_dict) != 0:
                self.__statistic()
            self.link_name_dict.clear()
            PREV_LINKID = int(line_p[0])
        road_name_id = line_p[1]
        if not self.link_name_dict.has_key(road_name_id):
            road_name_list = [line_p[1], line_p[2], line_p[3], line_p[4], line_p[5], line_p[6], line_p[7], line_p[8], line_p[9], line_p[10], line_p[11], line_p[12], line_p[13], line_p[14], line_p[15], [], [], long(line_p[40]), line_p[41]]
        else:
            road_name_list = self.link_name_dict[road_name_id]
        trans = [line_p[16], line_p[17], line_p[18], line_p[19], line_p[20], line_p[21], line_p[22], line_p[23], line_p[24], line_p[25]]
        road_name_list[15].append(trans)
        phonetics = [line_p[26], line_p[27], line_p[28], line_p[29], line_p[30], line_p[31], line_p[32], line_p[33], line_p[34], line_p[35], line_p[36], line_p[37], line_p[38], line_p[39]]
        road_name_list[16].append(phonetics)
        self.link_name_dict[road_name_id] = road_name_list

    def __add(self, line, dict):
        subdict = {}
        if not dict.has_key(line[0]):
           dict[line[0]] = subdict
        else:
           subdict = dict.get(line[0])
        subdict[line[2]] = (float(line[3])/100000, float(line[4])/100000)

    def __count(self,key):
        key = key.lower()
        if self.stat.has_key(key):
            self.stat[key] += 1
        else:
            self.stat[key] = 1

if __name__ == "__main__":
    # use to test this model
    bg = datetime.datetime.now()
    stat =  WaysName('na').get_statistic()
    keys = stat.keys()
    print "==>"
    print "{%s}"%(",".join(map(lambda px: "\"%s\":%s"%(px,stat[px]) ,keys)))
    print "<=="
    ed = datetime.datetime.now()
    print "Cost time:"+str(ed - bg)
