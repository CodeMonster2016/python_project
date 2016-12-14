# -------------------------------------------------------------------------------
# Name:        RelationsSafetycamera model
# Purpose:     this model is used to mapping the
#              columns: [ ]
#
# Author:      chshi
#
# Created:     12/12/2016
# Copyright:   (c) chshi 2016
# Licence:     <your licence>
# -------------------------------------------------------------------------------

from record import Record
from constants import *
import os
import sys
import datetime
import json

ROOT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
GLOBAL_KEY_PREFIX = "relations_virtual_connection_"
CSV_SEP = '`'
LF = '\n'

# (key, category, function)
STATISTIC_KEYS = [("type", False, "type"),
                  ("virtual_conn_type", False, "connection_type"),
                  ("stairs_traversal", False, "stairs_traversal"),
                  ("time_override", False, "time_override")]


class RelationsVirtualConnection(Record):
    def __init__(self, region):
        Record.__init__(self)
        self.dump_file = os.path.join(ROOT_DIR, "temporary", self.__class__.__name__)
        self.stat = {}
        self.region = region

    def dump2file(self):
        cmd = "select distinct ref_node_id, nonref_node_id, connection_type, stairs_traversal, time_override from ( \
              SELECT vc.ref_node_id, vc.nonref_node_id, vc.connection_type, vc.stairs_traversal, vc.time_override  \
              FROM rdf_virtual_connection AS vc \
              LEFT JOIN rdf_link AS l ON vc.ref_node_id = l.ref_node_id  \
              LEFT JOIN rdf_nav_link AS nl ON l.link_id = nl.link_id \
              WHERE nl.iso_country_code IN (%s) \
              UNION ALL \
              SELECT vc.ref_node_id, vc.nonref_node_id, vc.connection_type, vc.stairs_traversal, vc.time_override \
              FROM rdf_virtual_connection AS vc \
              LEFT JOIN rdf_link AS l ON vc.ref_node_id = l.nonref_node_id \
              LEFT JOIN rdf_nav_link AS nl ON l.link_id = nl.link_id \
              WHERE nl.iso_country_code IN (%s)) as T" % \
              (REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX),
               REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX))
        print cmd
        self.cursor.copy_expert("COPY (%s) TO STDOUT DELIMITER '`'" % cmd, open(self.dump_file, "w"))

    def get_statistic(self):
        try:
            self.dump2file()
        except:
            print "Some table or schema don't exist! Please check the upper sql"
            print "Unexpected error:[ %s.py->%s] %s" % (self.__class__.__name__, 'get_statistic', str(sys.exc_info()))
            return {}
        process_count = 0
        with open(self.dump_file, "r", 1024 * 1024 * 1024) as csv_f:
            for line in csv_f:
                line = line.rstrip()
                line_p = line.split(CSV_SEP)
                if len(line_p) < 1:
                    continue
                self.__statistic(line_p)
                process_count += 1
                if process_count % 5000 == 0:
                    print "\rProcess index [ " + str(process_count) + " ]",
            print "\rProcess index [ " + str(process_count) + " ]",
        # write to file
        with open(os.path.join(ROOT_DIR, "output", "stat", self.__class__.__name__), 'w') as stf:
            stf.write(json.dumps(self.stat))
        return self.stat

    # noinspection PyBroadException
    def __statistic(self, line):
        for key in STATISTIC_KEYS:
            try:
                getattr(self, '_%s__get_%s' % (self.__class__.__name__, key[2]))(key, line)
            except:
                print "The statistic [ %s ] didn't exist" % (keys[2])
                print "Unexpected error:[ %s.py->%s] %s" % (self.__class__.__name__, '__statistic', str(sys.exc_info()))

    def __count(self, key):
        if key in self.stat:
            self.stat[key] += 1
        else:
            self.stat[key] = 1

    # all statistic method
    def __get_type(self, attr_keys, line):
        if '\N' != line[2]:
            self.__count("%s%s" % (GLOBAL_KEY_PREFIX, attr_keys[0]))

    def __get_connection_type(self, attr_keys, line):
        if '\N' != line[2]:
            self.__count("%s%s" % (GLOBAL_KEY_PREFIX, attr_keys[0]))

    def __get_stairs_traversal(self, attr_keys, line):
        if '\N' != line[3]:
            self.__count("%s%s" % (GLOBAL_KEY_PREFIX, attr_keys[0]))

    def __get_time_override(self, attr_keys, line):
        if '\N' != line[4]:
            self.__count("%s%s" % (GLOBAL_KEY_PREFIX, attr_keys[0]))


if __name__ == "__main__":
    # use to test this model
    bg = datetime.datetime.now()
    stat = RelationsVirtualConnection('na').get_statistic()
    keys = stat.keys()
    print "==>"
    print "{%s}" % (",".join(map(lambda px: "\"%s\":%s" % (px, stat[px]), keys)))
    print "<=="
    ed = datetime.datetime.now()
    print "Cost time:" + str(ed - bg)
