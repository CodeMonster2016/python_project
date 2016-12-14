#-------------------------------------------------------------------------------
# Name:        WaysNavigableLinkTimezone model
# Purpose:     this model is used to mapping the rdf_nav_link, rdf_link and rdf_access
#              columns: [ ]
#
# Author:      rex
#
# Created:     2016-01-29
# Copyright:   (c) rex 2016
# Licence:     <your licence>
#-------------------------------------------------------------------------------

from record import Record
from constants import *
import os
import sys
import datetime
import json

ROOT_DIR          = os.path.join(os.path.dirname(os.path.abspath(__file__)),"..")
GLOBAL_KEY_PREFIX = "ways_navlink_"
CSV_SEP           = '`'
LF                = '\n'

#(key, category, function)
STATISTIC_KEYS    = (
("timezone:left",False,"timezone_left"),
("timezone:right", False, "timezone_right")
)

class WaysNavigableLinkTimezone(Record):
    def __init__(self, region):
        Record.__init__(self)
        self.dump_file = os.path.join(ROOT_DIR, "temporary", self.__class__.__name__)
        self.stat      = {}
        self.region    = region

    def dump2file(self):
        cmd = "SELECT \
rnl.link_id, \
rl.left_admin_place_id, \
rl.right_admin_place_id \
FROM \
public.rdf_nav_link as rnl left join public.rdf_link as rl on rnl.link_id=rl.link_id \
WHERE rnl.iso_country_code in (%s)"%(REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX))
        print cmd
        self.cursor.copy_expert("COPY (%s) TO STDOUT DELIMITER '`'"%(cmd),open(self.dump_file,"w"))

    def get_statistic(self):
        try:
            self.dump2file()
            self.__build_admins()
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

    def __build_admins(self):
        processcount = 0
        admins = {}
        with open(self.__dump_adminplaceid(), "r",1024*1024*1024) as csv_f:
            for line in csv_f:
                line = line.rstrip()
                line_p = line.split(CSV_SEP)
                if len(line_p) < 1:
                    continue
                if line_p[0] in admins:
                    continue
                admins[line_p[0]] = line_p[1:]
                processcount += 1
                if processcount%5000 == 0:
                    print "\rProcess index [ "+str(processcount)+" ]",
            print "\rProcess index [ "+str(processcount)+" ]",
        print "build admin time zone hierarchy"
        for api in admins:
            #check order8
            admins[api][1] = (admins.get(api)[1] in admins and '\N' != admins.get(admins.get(api)[1])[0]) and 1 or 0
            #check order2
            admins[api][2] = (admins.get(api)[2] in admins and '\N' != admins.get(admins.get(api)[2])[0]) and 1 or 0
            #check order1
            admins[api][3] = (admins.get(api)[3] in admins and '\N' != admins.get(admins.get(api)[3])[0]) and 1 or 0
            #check country
            admins[api][4] = (admins.get(api)[4] in admins and '\N' != admins.get(admins.get(api)[4])[0]) and 1 or 0
        self.admins = admins

    def __dump_adminplaceid(self):
        cmd = "SELECT \
rap.admin_place_id, \
rap.time_zone, \
rah.order8_id, \
rah.order2_id, \
rah.order1_id, \
rah.country_id \
FROM \
public.rdf_admin_place AS rap, public.rdf_admin_hierarchy AS rah \
WHERE rap.admin_place_id=rah.admin_place_id and rah.iso_country_code IN (%s)"%(REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX))
        print cmd
        f = "%s_admins"%(self.dump_file)
        self.cursor.copy_expert("COPY (%s) TO STDOUT DELIMITER '`'"%(cmd),open(f,"w"))
        return f

    def __statistic(self,line):
        for keys in STATISTIC_KEYS:
            try:
                getattr(self,'_%s__get_%s'%(self.__class__.__name__,keys[2]))(keys,line)
            except:
                print "The statistic [ %s ] didn't exist"%(keys[2])
                print "Unexpected error:[ %s.py->%s] %s"%(self.__class__.__name__, '__statistic', str(sys.exc_info()))

    def __count(self,key):
        if self.stat.has_key(key):
            self.stat[key] += 1
        else:
            self.stat[key] = 1

    # all statistic method
    def __get_timezone_left(self,keys,line):
        if '\N' != line[1] and reduce(lambda px,py:px+py,self.admins.get(line[1])[1:]) > 0:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_timezone_right(self,keys,line):
        if '\N' != line[2] and reduce(lambda px,py:px+py,self.admins.get(line[2])[1:]) > 0:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

if __name__ == "__main__":
    # use to test this model
    bg = datetime.datetime.now()
    navlink_stat =  WaysNavigableLinkTimezone('na').get_statistic()
    keys = navlink_stat.keys()
    print "==>"
    print "{%s}"%(",".join(map(lambda px: "\"%s\":%s"%(px,navlink_stat[px]) ,keys)))
    print "<=="
    ed = datetime.datetime.now()
    print "Cost time:"+str(ed - bg)
