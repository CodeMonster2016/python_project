#-------------------------------------------------------------------------------
# Name:        WaysNavigableLink Adas CHS model
# Purpose:     this model is used to mapping the rdf_nav_link, rdf_link
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
GLOBAL_KEY_PREFIX = "ways_navlink_"
CSV_SEP           = '`'
LF                = '\n'

#(key, category, function)
STATISTIC_KEYS    = (
("adas:chs",True,"adas_chs"),
)

class WaysNavigableLinkAdasChs(Record):
    def __init__(self, region):
        Record.__init__(self)
        self.navlink_file = os.path.join(ROOT_DIR, "temporary", self.__class__.__name__)
        self.stat         = {}
        self.region       = region

    def dump2file(self):
        cmd = "SELECT COUNT(1) FROM ( \
SELECT DISTINCT(alg.link_id) \
FROM public.adas_link_geometry AS alg, public.rdf_nav_link AS rnl \
WHERE alg.link_id=rnl.link_id AND rnl.iso_country_code IN (%s) \
GROUP BY alg.link_id HAVING COUNT(alg.link_id) > 2 \
) AS foo"%(REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX))
        print cmd
        self.cursor.copy_expert("COPY (%s) TO STDOUT DELIMITER '`'"%(cmd),open(self.navlink_file,"w"))

    def get_statistic(self):
        try:
            self.dump2file()
        except:
            print "Some table or schema don't exist! Please check the upper sql"
            print ("Unexpected error:[ WaysNavigableLinkAdasChs.py->get_statistic] "+str(sys.exc_info()))
            return {}
        processcount = 0
        with open(self.navlink_file, "r",1024*1024*1024) as csv_f:
            for line in csv_f:
                line = line.rstrip()
                if not line:
                    continue
                self.__statistic(line)
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
                getattr(self,'_WaysNavigableLinkAdasChs__get_'+keys[2])(keys,line)
            except:
                print "The statistic [ %s ] didn't exist"%(keys[2])
                print ("Unexpected error:[ WaysNavigableLinkAdasChs.py->__statistic] "+str(sys.exc_info()))

    # all statistic method
    def __get_adas_chs(self,keys,line):
        self.stat["%s%s"%(GLOBAL_KEY_PREFIX,keys[0])] = int(line)

if __name__ == "__main__":
    # use to test this model
    bg = datetime.datetime.now()
    navlink_stat =  WaysNavigableLinkAdasChs('na').get_statistic()
    keys = navlink_stat.keys()
    print "==>"
    print "{%s}"%(",".join(map(lambda px: "\"%s\":%s"%(px,navlink_stat[px]) ,keys)))
    print "<=="
    ed = datetime.datetime.now()
    print "Cost time:"+str(ed - bg)
