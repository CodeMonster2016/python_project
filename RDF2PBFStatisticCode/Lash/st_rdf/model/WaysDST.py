#-------------------------------------------------------------------------------
# Name:        WaysDST
# Purpose:
#
# Author:      kuangh
#
# Created:     30/01/2016
# Copyright:   (c) kuangh 2016
# Licence:     <your licence>
#-------------------------------------------------------------------------------
from record import Record
from constants import *
from xml.dom import minidom
import os
import sys
import datetime
import json

ROOT_DIR            = os.path.join(os.path.dirname(os.path.abspath(__file__)),"..")
DST_CONFIG_FILE     = os.path.join(os.path.dirname(ROOT_DIR), "..", "config", "dst.xml")
GLOBAL_KEY_PREFIX   = "ways_link_"
CSV_SEP             = '`'

STATISTIC_GENERAL_KEYS = \
(
('dst_pattern', True, 'dst_pattern'),
('dst_pattern:left', True, 'dst_pattern:left'),
('dst_pattern:right', True, 'dst_pattern:right')
)

class WaysDST(Record):
    def __init__(self, region):
        Record.__init__(self)
        self._ways_dump_file            = os.path.join(ROOT_DIR, 'temporary', self.__class__.__name__)
        self._admin_hierary_dump_file   = os.path.join(ROOT_DIR, 'temporary', 'admin_hierarchy')
        self._admin_dst_dump_file       = os.path.join(ROOT_DIR, 'temporary', 'admin_dst')
        self.temp_file                  = os.path.join(ROOT_DIR, 'temporary', 'temp')

        self._stat                      = {}
        self._dst_pattern_map           = {}
        self._admin_dst_map             = {}
        self._admin_hierarchy_map       = {}
        self._admin_sides_dst_map       = {}
        self._region                    = region

    def _dump_way(self):
        cmd = "select \
            distinct rl.link_id, \
            rl.left_admin_place_id, \
            rl.right_admin_place_id \
            from \
            rdf_link rl, rdf_admin_hierarchy rah \
            where \
            (rl.left_admin_place_id = rah.admin_place_id or \
            rl.right_admin_place_id = rah.admin_place_id) and \
            rah.iso_country_code in (%s)" % (REGION_COUNTRY_CODES(self._region, GLOBAL_KEY_PREFIX))
        print cmd
        self.cursor.copy_expert("COPY (%s) TO STDOUT DELIMITER '`'"%(cmd),open(self._ways_dump_file,"w"))

    def _dump_admin_hierarchy(self):
        cmd = "select \
            admin_place_id, \
            country_id, \
            order1_id, \
            order2_id, \
            order8_id, \
            builtup_id \
            from rdf_admin_hierarchy"
        print cmd
        self.cursor.copy_expert("COPY (%s) TO STDOUT DELIMITER '`'"%(cmd),open(self._admin_hierary_dump_file,"w"))
        with open(self._admin_hierary_dump_file, 'r', 1024*1024*100) as admin_hierarchy_f:
            for line in admin_hierarchy_f:
                line = line.rstrip()
                if len(line) == 0:
                    continue
                line_p = line.split(CSV_SEP)
                self._admin_hierarchy_map[line_p[0]] = [line_p[1], line_p[2], line_p[3], line_p[4], line_p[5]]

    def _dump_admin_dst(self):
        cmd = "select \
            rap.admin_place_id, \
            rad.dst_observed, \
            rad.dst_start_day, \
            rad.dst_start_weekday, \
            rad.dst_start_month, \
            rad.dst_start_time, \
            rad.dst_end_day, \
            rad.dst_end_weekday, \
            rad.dst_end_month, \
            rad.dst_end_time \
            from \
            rdf_admin_place rap, \
            rdf_admin_dst rad \
            where \
            rap.dst_id = rad.dst_id"
        print cmd
        self.cursor.copy_expert("COPY (%s) TO STDOUT DELIMITER '`'"%(cmd),open(self._admin_dst_dump_file,"w"))

        self._load_dst_config()
        with open(self._admin_dst_dump_file, 'r', 1024*1024*10) as dst_f:
            for line in dst_f:
                line = line.rstrip()
                if len(line) == 0:
                    continue
                line_p = line.split(CSV_SEP)
                pattern = None
                if line_p[1] == "Y":
                    pattern = self._build_dst_pattern(line_p[2], line_p[3], line_p[4], line_p[5], line_p[6], line_p[7], line_p[8], line_p[9])
                    if not self._dst_pattern_map.has_key(pattern):
                        raise "dst pattern not existed in config file"
                        continue
                    self._admin_dst_map[line_p[0]] = (line_p[1], self._dst_pattern_map[pattern])
                else:
                    self._admin_dst_map[line_p[0]] = (line_p[1], None)
        dst_f.close()

    def _load_dst_config(self):
        doc = None
        try:
            doc = minidom.parse(DST_CONFIG_FILE)
        except:
            print "failed to parse dst configure file"
            return
        if not doc:
            return
        root = doc.documentElement
        if root:
            recordelmts = root.getElementsByTagName('record')
            if recordelmts:
                for recelmt in recordelmts:
                    dst_pattern_id  = self._get_node_value(recelmt, "dst_pattern")
                    observed        = self._get_node_value(recelmt, "dst_observed")
                    start_day       = self._get_node_value(recelmt, "dst_start_day")
                    start_weekday   = self._get_node_value(recelmt, "dst_start_weekday")
                    start_month     = self._get_node_value(recelmt, "dst_start_month")
                    start_time      = self._get_node_value(recelmt, "dst_start_time")
                    end_day         = self._get_node_value(recelmt, "dst_end_day")
                    end_weekday     = self._get_node_value(recelmt, "dst_end_weekday")
                    end_month       = self._get_node_value(recelmt, "dst_end_month")
                    end_time        = self._get_node_value(recelmt, "dst_end_time")
                    if observed == "Y":
                        self._dst_pattern_map[self._build_dst_pattern(start_day, start_weekday, start_month, start_time, end_day, end_weekday, end_month, end_time)] = str(dst_pattern_id)

    def _build_dst_pattern(self, start_day, start_weekday, start_month, start_time, end_day, end_weekday, end_month, end_time):
        patterns = (format(int(start_day), '02d'), format(int(start_weekday), '01d'), format(int(start_month), '02d'), format(int(start_time), '04d'), \
                            format(int(end_day), '02d'), format(int(end_weekday), '01d'), format(int(end_month), '02d'), format(int(end_time), '04d'))
        return "".join(patterns)

    def _get_node_value(self, xmlnode, key):
        elmt = xmlnode.getElementsByTagName(key)
        return elmt[0].childNodes[0].data

    def get_statistic(self):
        try:
            self._dump_way()
            self._dump_admin_hierarchy()
            self._dump_admin_dst()
        except:
            print 'Some table or schema don\'t exist! Please check the upper sql'
            return {}
        processcount = 0
        with open(self._ways_dump_file, 'r', 1024*1024*1024) as csv_f:
            for line in csv_f:
                line = line.rstrip()
                if len(line) == 0:
                    continue
                line_p = line.split(CSV_SEP)
                self._statistic(line_p)
                processcount += 1
                if processcount%10000 == 0:
                    print "\rProcess index [ "+str(processcount)+" ]",
        print "\rProcess index [ "+str(processcount)+" ]",

        # write to file
        with open(os.path.join(ROOT_DIR, "output", "stat", self.__class__.__name__), 'w') as stf:
            stf.write(json.dumps(self._stat))
        return self._stat

    def _statistic(self,line_p):
        dsts =  self._get_link_dst(line_p)
        if (not dsts[0] or dsts[0][0] == "N") and (not dsts[1] or dsts[1][0] == "N"):
            return
        if dsts[0] == dsts[1]:
            self._count("%s%s%s" % (GLOBAL_KEY_PREFIX, STATISTIC_GENERAL_KEYS[0][2], "#%s" % dsts[0][1]))
        else:
            if dsts[0][0] != "N":
                self._count("%s%s%s" % (GLOBAL_KEY_PREFIX, STATISTIC_GENERAL_KEYS[1][2], "#%s" % dsts[0][1]))
            if dsts[1][0] != "N":
                self._count("%s%s%s" % (GLOBAL_KEY_PREFIX, STATISTIC_GENERAL_KEYS[2][2], "#%s" % dsts[1][1]))

    def _get_side(self, line_p):
        if line_p[1] == line_p[7] or \
            line_p[1] == line_p[6] or \
            line_p[1] == line_p[5] or \
            line_p[1] == line_p[4] or \
            line_p[1] == line_p[3]:
                return "l"
        elif line_p[2] == line_p[7] or \
            line_p[2] == line_p[6] or \
            line_p[2] == line_p[5] or \
            line_p[2] == line_p[4] or \
            line_p[2] == line_p[3]:
                return "r"

    def _get_link_dst(self, line_p):
        left_admin_place_id = line_p[1]
        left_dst = self._get_dst(left_admin_place_id)
        right_admin_place_id = line_p[2]
        right_dst = self._get_dst(right_admin_place_id)
        return [left_dst, right_dst]

    def _get_dst(self, admin_place_id):
        if not self._admin_hierarchy_map.has_key(admin_place_id):
            return null
        admin_hierarchys = self._admin_hierarchy_map[admin_place_id]
        #builtup
        if '\N' != admin_hierarchys[4]:
            if self._admin_dst_map.has_key(admin_hierarchys[4]):
                return self._admin_dst_map[admin_hierarchys[4]]
        #order8
        if '\N' != admin_hierarchys[3]:
            if self._admin_dst_map.has_key(admin_hierarchys[3]):
                return self._admin_dst_map[admin_hierarchys[3]]
        #order2
        if '\N' != admin_hierarchys[2]:
            if self._admin_dst_map.has_key(admin_hierarchys[2]):
                return self._admin_dst_map[admin_hierarchys[2]]
        #order1
        if '\N' != admin_hierarchys[1]:
            if self._admin_dst_map.has_key(admin_hierarchys[1]):
                return self._admin_dst_map[admin_hierarchys[1]]
        #countryid
        if '\N' != admin_hierarchys[0]:
            if self._admin_dst_map.has_key(admin_hierarchys[0]):
                return self._admin_dst_map[admin_hierarchys[0]]

    def _count(self,key):
        if self._stat.has_key(key):
            self._stat[key] += 1
        else:
            self._stat[key] = 1

if __name__ == '__main__':
    # use to test this model
    bg = datetime.datetime.now()
    stat =  WaysDST('na').get_statistic()
    keys = stat.keys()
    print "==>"
    print "{%s}"%(",".join(map(lambda px: "\"%s\":%s"%(px,stat[px]) ,keys)))
    print "<=="
    ed = datetime.datetime.now()
    print "Cost time:"+str(ed - bg)
