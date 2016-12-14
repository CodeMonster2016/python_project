#-------------------------------------------------------------------------------
# Name:        NodesNodeBarrier model
# Purpose:     this model is used to mapping the rdf_nav_link, rdf_link and rdf_access
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
GLOBAL_KEY_PREFIX = "nodes_node_"
CSV_SEP           = '`'
LF                = '\n'

#(key, category, function)
STATISTIC_KEYS    = (("barrier",True,"barrier"),)

class NodesNodeBarrier(Record):
    def __init__(self, region):
        Record.__init__(self)
        self.dump_file = os.path.join(ROOT_DIR, "temporary", self.__class__.__name__)
        self.stat      = {}
        self.region    = region

    def dump2file(self):
        cmd = "SELECT \
DISTINCT(rn.node_id), \
rc.condition_type \
FROM \
public.rdf_node as rn left join public.rdf_nav_strand as rns on rn.node_id=rns.node_id \
left join public.rdf_nav_link as rnl on rns.link_id = rnl.link_id left join \
public.rdf_condition AS rc on rns.nav_strand_id=rc.nav_strand_id \
WHERE rc.condition_type in (1) and rnl.iso_country_code in (%s)"%(REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX))
        print cmd
        self.cursor.copy_expert("COPY (%s) TO STDOUT DELIMITER '`'"%(cmd),open(self.dump_file,"w"))

    def get_statistic(self):
        try:
            self.dump2file()
        except:
            print "Some table or schema don't exist! Please check the upper sql"
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

    def __statistic(self,line):
        for keys in STATISTIC_KEYS:
            try:
                getattr(self,'_NodesNodeBarrier__get_'+keys[2])(keys,line)
            except:
                print "The statistic [ %s ] didn't exist"%(keys[2])
                print ("Unexpected error:[ NodesNodeBarrier.py->__statistic] "+str(sys.exc_info()))

    def __count(self,key):
        if self.stat.has_key(key):
            self.stat[key] += 1
        else:
            self.stat[key] = 1

    # all statistic method
    def __get_barrier(self,keys,line):
        barrier = ('1' == line[1] and 'toll_booth' or ('4' == line[1] and 'gate' or None))
        if '\N' != line[0] and None != barrier:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%(barrier) or ""))

if __name__ == "__main__":
    # use to test this model
    bg = datetime.datetime.now()
    stat =  NodesNodeBarrier('na').get_statistic()
    keys = stat.keys()
    print "==>"
    print "{%s}"%(",".join(map(lambda px: "\"%s\":%s"%(px,stat[px]) ,keys)))
    print "<=="
    ed = datetime.datetime.now()
    print "Cost time:"+str(ed - bg)
