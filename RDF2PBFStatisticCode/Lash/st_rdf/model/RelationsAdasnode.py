#-------------------------------------------------------------------------------
# Name:        RelationsAdasmaxspeed model
# Purpose:     this model is used to mapping the
#              columns: [ ]
#
# Author:      rex
#
# Created:     2016/01/20
# Copyright:   (c) rex 2016
# Licence:     <your licence>
#-------------------------------------------------------------------------------

from record import Record
from constants import *
import os
import sys
import datetime
import json

ROOT_DIR                     = os.path.join(os.path.dirname(os.path.abspath(__file__)),"..")
GLOBAL_KEY_PREFIX            = "relations_adas_node_"
CSV_SEP                      = '`'
LF                           = '\n'
ST_ADAS_NODE_IDS             = 'st_adas_node_ids'
ST_ADAS_NODE_IDS_INDEX       = 'st_adas_node_ids_index'
ST_ADAS_NODE_SLOPE           = 'st_adas_node_slope'
ST_ADAS_NODE_SLOPE_INDEX     = 'st_adas_node_slope_index'
ST_ADAS_NODE_CURVATURE       = 'st_adas_node_curvature'
ST_ADAS_NODE_CURVATURE_INDEX = 'st_adas_node_curvature_index'

#(key, category, function)
##STATISTIC_KEYS    = (
##("type",False,"type"),
##("curvature", False,"curvature"),
##("heading", False, "heading"),
##("slope_f", False, "slope_f"),
##("slope_t", False, "slope_t"),
##("slope", False, "slope"),
##)

class RelationsAdasnode(Record):
    def __init__(self, region):
        Record.__init__(self)
        self.dump_file = os.path.join(ROOT_DIR, "temporary", self.__class__.__name__)
        self.stat      = {}
        self.region    = region

    def get_statistic(self):
        try:
            self.__init_tables()
            self.__statistic()
        except:
            print "Oops! Some table or schema don't exist! Please check the upper sql"
            print "Unexpected error:[ %s.py->%s] %s"%(self.__class__.__name__, 'get_statistic', str(sys.exc_info()))
            return {}
        # write to file
        with open(os.path.join(ROOT_DIR, "output", "stat", self.__class__.__name__), 'w') as stf:
            stf.write(json.dumps(self.stat))
        return self.stat

    def __init_tables(self):
        self.__init_adasnodeslope_table()
        self.__init_adasnodeids_table()
        self.__init_adasnodecurvature_table()

    def __init_adasnodeids_table(self):
        if self.is_existtable('public',ST_ADAS_NODE_IDS):
            return
        cmd = "SELECT * INTO public.%s FROM (\
SELECT DISTINCT(anc.node_id) AS node_id \
FROM public.adas_node_curvature AS anc LEFT JOIN public.rdf_nav_link AS rnl ON rnl.link_id = anc.from_link_id \
WHERE rnl.iso_country_code IN (%s) \
UNION \
SELECT DISTINCT(anc.node_id) AS node_id  \
FROM public.adas_node_curvature AS anc LEFT JOIN public.rdf_nav_link AS rnl ON rnl.link_id = anc.to_link_id \
WHERE rnl.iso_country_code IN (%s) \
UNION \
SELECT DISTINCT(ans.node_id) AS node_id \
FROM public.%s AS ans \
) AS foo"%(ST_ADAS_NODE_IDS,REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX),REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX),ST_ADAS_NODE_SLOPE)
        self.run_sql(self.cursor.execute,cmd)
        self.run_sql(self.cursor.execute,'CREATE INDEX %s ON public.%s (node_id)'%(ST_ADAS_NODE_IDS_INDEX,ST_ADAS_NODE_IDS))

    def __init_adasnodecurvature_table(self):
        if self.is_existtable('public',ST_ADAS_NODE_CURVATURE):
            return
        cmd = "SELECT * INTO public.%s FROM (\
SELECT anc.* \
FROM public.adas_node_curvature AS anc, public.%s AS st_ani \
WHERE anc.node_id=st_ani.node_id \
) AS foo"%(ST_ADAS_NODE_CURVATURE,ST_ADAS_NODE_IDS)
        self.run_sql(self.cursor.execute,cmd)
        self.run_sql(self.cursor.execute,'CREATE INDEX %s ON public.%s (node_id)'%(ST_ADAS_NODE_CURVATURE_INDEX,ST_ADAS_NODE_CURVATURE))

    def __init_adasnodeslope_table(self):
        if self.is_existtable('public',ST_ADAS_NODE_SLOPE):
            return
        cmd = "SELECT * INTO public.%s FROM (\
SELECT rnl.link_id AS to_link_id, rl.ref_node_id AS node_id, 'Y' AS ref_type, alg.slope AS slope \
FROM public.adas_link_geometry AS alg, public.rdf_nav_link AS rnl, public.rdf_link AS rl \
WHERE alg.link_id = rnl.link_id AND alg.seq_num = 0 AND rnl.iso_country_code IN (%s) AND rl.link_id=rnl.link_id AND alg.slope IS NOT NULL \
UNION ALL \
SELECT rnl.link_id AS to_link_id, rl.nonref_node_id AS node_id, 'N' AS ref_type, alg.slope AS slope \
FROM public.adas_link_geometry AS alg, public.rdf_nav_link AS rnl, public.rdf_link AS rl \
WHERE alg.link_id = rnl.link_id AND alg.seq_num = 999999 AND rnl.iso_country_code IN (%s) AND rl.link_id=rnl.link_id AND alg.slope IS NOT NULL  \
) AS foo"%(ST_ADAS_NODE_SLOPE,REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX),REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX))
        self.run_sql(self.cursor.execute,cmd)
        self.run_sql(self.cursor.execute,'CREATE INDEX %s ON public.%s (node_id)'%(ST_ADAS_NODE_SLOPE_INDEX,ST_ADAS_NODE_SLOPE))

    def __statistic(self):
        # statistic type, curvature, heading
        self.__statistic_ch()
        # statistic type, slope
        self.__statistic_slope()
        # statistic slope, slope_t, slope_f
        self.__statistic_slope_ft()

    def __statistic_ch(self):
        cmd = "SELECT \
COUNT(1) \
FROM public.%s"%(ST_ADAS_NODE_CURVATURE)
        self.run_sql(self.cursor.execute,cmd)
        rows = self.cursor.fetchall()
        #print rows
        if len(rows) <= 0:
            return
        count = rows[0][0]
        self.__count('type',count)
        self.__count('curvature',count)
        self.__count('heading',count)

    def __statistic_slope(self):
        cmd = "SELECT \
COUNT(1) \
FROM public.%s AS ans WHERE NOT EXISTS (SELECT 1 FROM public.%s AS anc WHERE anc.node_id=ans.node_id)"%(ST_ADAS_NODE_SLOPE,ST_ADAS_NODE_CURVATURE)
        self.run_sql(self.cursor.execute,cmd)
        rows = self.cursor.fetchall()
        #print rows
        if len(rows) <= 0:
            return
        count = rows[0][0]
        self.__count('type',count)
        self.__count('slope',count)


    def __statistic_slope_ft(self):
        # slope t count
        cmd = "SELECT \
COUNT(1) \
FROM public.%s AS anc WHERE EXISTS (SELECT 1 FROM public.%s AS ans WHERE anc.node_id=ans.node_id AND ans.to_link_id = anc.to_link_id)"%(ST_ADAS_NODE_CURVATURE,ST_ADAS_NODE_SLOPE)
        self.run_sql(self.cursor.execute,cmd)
        rows = self.cursor.fetchall()
        #print rows
        if len(rows) <= 0:
            return
        count = rows[0][0]
        self.__count('slope_t',count)

        # slope f count
        cmd = "SELECT \
COUNT(1) \
FROM public.%s AS anc WHERE EXISTS (SELECT 1 FROM public.%s AS ans WHERE anc.node_id=ans.node_id AND ans.to_link_id = anc.from_link_id)"%(ST_ADAS_NODE_CURVATURE,ST_ADAS_NODE_SLOPE)
        self.run_sql(self.cursor.execute,cmd)
        rows = self.cursor.fetchall()
        #print rows
        if len(rows) <= 0:
            return
        count = rows[0][0]
        self.__count('slope_f',count)

        # slope count
        cmd = "SELECT \
COUNT(1) \
FROM public.%s AS ans_p WHERE \
  EXISTS (SELECT 1 FROM public.%s AS anc1 WHERE anc1.node_id=ans_p.node_id AND ans_p.to_link_id = anc1.from_link_id) \
  OR \
  EXISTS (SELECT 1 FROM public.%s AS anc2 WHERE anc2.node_id=ans_p.node_id AND ans_p.to_link_id = anc2.to_link_id)"%(ST_ADAS_NODE_SLOPE,ST_ADAS_NODE_CURVATURE,ST_ADAS_NODE_CURVATURE)
        self.run_sql(self.cursor.execute,cmd)
        rows = self.cursor.fetchall()
        #print rows
        if len(rows) <= 0:
            return
        slope_ft_count = rows[0][0]

        cmd = "SELECT COUNT(1) FROM public.%s AS ans WHERE EXISTS (SELECT 1 FROM public.%s AS anc WHERE anc.node_id=ans.node_id)"%(ST_ADAS_NODE_SLOPE,ST_ADAS_NODE_CURVATURE)
        self.run_sql(self.cursor.execute,cmd)
        rows = self.cursor.fetchall()
        #print rows
        if len(rows) <= 0:
            return
        slope_count = rows[0][0]

        self.__count('type',slope_count-slope_ft_count)
        self.__count('slope',slope_count-slope_ft_count)

    def __count(self,key,num):
        key = "%s%s"%(GLOBAL_KEY_PREFIX, key)
        if key in self.stat:
            self.stat[key] += num
        else:
            self.stat[key] = num

if __name__ == "__main__":
    # use to test this model
    bg = datetime.datetime.now()
    stat =  RelationsAdasnode('na').get_statistic()
    keys = stat.keys()
    print "==>"
    print "{%s}"%(",".join(map(lambda px: "\"%s\":%s"%(px,stat[px]) ,keys)))
    print "<=="
    ed = datetime.datetime.now()
    print "Cost time:"+str(ed - bg)
