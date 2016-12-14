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

import os
import sys
import datetime
import json

ROOT_DIR          = os.path.join(os.path.dirname(os.path.abspath(__file__)),"..")
GLOBAL_KEY_PREFIX = "relations_signpost_"
CSV_SEP           = '`'
LF                = '\n'

PREV_SIGNPOST_ID = -1

class RelationsSignpostElement(Record):
    def __init__(self, region):
        Record.__init__(self)
        self.signpost_dump_file = os.path.join(ROOT_DIR, 'temporary', self.__class__.__name__)
        self.admin_hierarchy_dump_file = os.path.join(ROOT_DIR, 'temporary', 'signpost_element_admin_hierarchy')
        self.stat                    = {}
        self.region                  = region
        self.sign_element_dict       = {}
        self.admin_hierarchy_dict    = {}

    def dump2file(self):
        cmd = "select \
            distinct \
            rso.originating_link_id, \
            rsd.dest_link_id, \
            rsd.exit_number, \
            rsd.straight_on_sign, \
            rsd.language_code, \
            rsd.alt_exit_number, \
            rse.*, \
            rset.*, \
            vse.*, \
            vpt.*, \
            vgo.*, \
            rl.left_admin_place_id, \
            rl.right_admin_place_id \
            from \
            rdf_nav_link rnl \
            inner join rdf_sign_origin rso on rnl.link_id = rso.originating_link_id \
            inner join rdf_sign_destination rsd on rso.sign_id = rsd.sign_id \
            inner join rdf_sign_element rse on rsd.sign_id = rse.sign_id and rsd.destination_number = rse.destination_number \
            left join rdf_sign_element_trans rset on rse.sign_element_id = rset.sign_element_id \
            left join vce_sign_element vse on rse.sign_element_id = vse.sign_element_id \
            left join vce_phonetic_text vpt on vse.phonetic_id = vpt.phonetic_id \
            left join vce_geo_override vgo on vpt.phonetic_id = vgo.phonetic_id \
            inner join rdf_link rl on rsd.dest_link_id = rl.link_id \
            where iso_country_code in (%s) \
            order by rse.sign_id, rse.destination_number, rse.entry_number" % (REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX))
        print cmd
        self.cursor.copy_expert("COPY (%s) TO STDOUT DELIMITER '`'"%(cmd),open(self.signpost_dump_file,"w"))

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
        self.dumpadminhierarchyfile()
        try:
            self.dump2file()
        except:
            print 'Some table or schema don\'t exist! Please check the upper sql'
            return {}
        processcount = 0
        with open(self.signpost_dump_file, "r",1024*1024*1024) as csv_f:
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

    def _get_sign_element_type(self, line_p):
        if line_p[10] == "R" and line_p[8] == "T":
            return "sign_route_t"
        elif line_p[10] == "R" and line_p[8] == "B":
            return "sign_route_b"
        elif line_p[10] == "T" and line_p[8] == "T":
            return "sign_text_t"
        elif line_p[10] == "T" and line_p[8] == "B":
            return "sign_text_b"

    #0:exit_number, 1:straight_on_sign, 2:language_code, 3:alt_exit_number
    #4:sign_element_id, 5:sign_id, 6:destination_number, 7:entry_number, 8:entry_type, 9:text_number, 10:text_type, 11:text, 12:language_code, 13:direction_code
    #trans:
    #0:sign_element_id, 1:transliteration_type, 2:text
    def __statistic(self):
        self.__count("{0}type".format(GLOBAL_KEY_PREFIX))
        element_dict = {}
        sign_elements_list = sorted(list(self.sign_element_dict.values()), key=itemgetter(7))
        if sign_elements_list[0][1] == "Y":
            self.__count("{0}straight_on_sign".format(GLOBAL_KEY_PREFIX))

        for sign_elements in sign_elements_list:
            sign_element_type = self._get_sign_element_type(sign_elements)
            if not element_dict.has_key(sign_elements[12]):
                element_dict[sign_elements[12]] = {}
            sign_element_type_dict = element_dict[sign_elements[12]]
            if sign_element_type_dict.has_key(sign_element_type):
                sign_element_type_list = sign_element_type_dict[sign_element_type]
                sign_element_type_list.append(sign_elements)
            else:
                sign_element_type_dict[sign_element_type] = [sign_elements]

        for lang in element_dict.keys():
            for sign_element_type in element_dict[lang].keys():
                if element_dict[lang][sign_element_type][0][11] != "\N":
                    self.__count("{0}{1}:{2}".format(GLOBAL_KEY_PREFIX, sign_element_type, lang))

                trans_literation_type = element_dict[lang][sign_element_type][0][14][0][1]
                if trans_literation_type != "\N":
                    self.__count("{0}{1}:{2}:trans:{3}".format(GLOBAL_KEY_PREFIX, sign_element_type, lang, trans_literation_type))

                for p_keys in self._get_phonetic_key(element_dict[lang][sign_element_type][0][15]):
                    self.__count("{0}{1}:{2}:{3}".format(GLOBAL_KEY_PREFIX, sign_element_type, lang, p_keys))

    #phonetic:
    #0:sign_element_id, 1:phonetic_id, 2:preferred
    #3:phonetic_id, 4:phonetic_string, 5:phonetic_language_code, 6:transcription_method
    #7:geo_override_id, 8:phonetic_id, 9:admin_place_id, 10:preferred
    #11:left_admin_place_id, 12:right_admin_place_id
    def _get_phonetic_key(self, listphonetics):
        preferred_phonetics_dict = {}
        for phonetics in listphonetics:
            if phonetics[9] != "\N":
                if self._is_geo_override(phonetics[11], phonetics[9]) or self._is_geo_override(phonetics[12], phonetics[9]):
                    preferred_phonetics_dict[phonetics[1]] = phonetics

        if len(preferred_phonetics_dict) == 0:
            for phonetics in listphonetics:
                if phonetics[2] == "Y":
                    preferred_phonetics_dict[phonetics[1]] = phonetics

        preferred_phonetic = {}
        length = 0
        for key, vals in preferred_phonetics_dict.items():
            phonetic_key = "phonetics:{0}:{1}".format(vals[5], vals[6])
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

    #0:originating_link_id, 1:dest_link_id, 2:exit_number, 3:straight_on_sign, 4:language_code, 5:alt_exit_number
    #6:sign_element_id, 7:sign_id, 8:destination_number, 9:entry_number, 10:entry_type, 11:text_number, 12:text_type, 13:text, 14:language_code, 15:direction_code
    #16:sign_element_id, 17:transliteration_type, 18:text
    #19:sign_element_id, 20:phonetic_id, 21:preferred
    #22:phonetic_id, 23:phonetic_string, 24:phonetic_language_code, 25:transcription_method
    #26:geo_override_id, 27:phonetic_id, 28:admin_place_id, 29:preferred
    #30:left_admin_place_id, 31:right_admin_place_id
    def _build(self, line_p):
        global PREV_SIGNPOST_ID
        sign_id = "{0:d}{1:04d}".format(int(line_p[7]), int(line_p[8]))
        if sign_id != PREV_SIGNPOST_ID:
            if len(self.sign_element_dict) != 0:
                self.__statistic()
                self.sign_element_dict.clear()
            PREV_SIGNPOST_ID = sign_id
        sign_element_id = line_p[6]
        if not self.sign_element_dict.has_key(sign_element_id):
            sign_element_list = [line_p[2], line_p[3], line_p[4], line_p[5], line_p[6], line_p[7], line_p[8], int(line_p[9]), line_p[10], line_p[11], line_p[12], line_p[13], line_p[14], line_p[15], [], []]
        else:
            sign_element_list = self.sign_element_dict[sign_element_id]
        trans = [line_p[16], line_p[17], line_p[18]]
        sign_element_list[14].append(trans)
        phonetics = [line_p[19], line_p[20], line_p[21], line_p[22], line_p[23], line_p[24], line_p[25], line_p[26], line_p[27], line_p[28], line_p[29], line_p[30], line_p[31]]
        sign_element_list[15].append(phonetics)
        self.sign_element_dict[sign_element_id] = sign_element_list

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
    stat =  RelationsSignpostElement('na').get_statistic()
    keys = stat.keys()
    print "==>"
    print "{%s}"%(",".join(map(lambda px: "\"%s\":%s"%(px,stat[px]) ,keys)))
    print "<=="
    ed = datetime.datetime.now()
    print "Cost time:"+str(ed - bg)
