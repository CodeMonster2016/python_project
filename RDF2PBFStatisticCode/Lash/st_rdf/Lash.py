#-------------------------------------------------------------------------------
# Name:        Lash
# Purpose:     Lash is statistic tools
#              It will run by python2.7+
#              like:
#
# Author:      rex
#
# Created:     08/10/2015
# Copyright:   (c) rex 2015
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import sys
import os
import time
import argparse
import ConfigParser
import re
import json
import datetime

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

#import all statistic model
for lib_dir in ["model"]:
    module_system_path = os.path.join(ROOT_DIR,lib_dir)
    sys.path.append(module_system_path)
    for lib_file in os.listdir(module_system_path):
        name_ext = os.path.splitext(lib_file)
        if name_ext[1].upper() == ".PY" and name_ext[0] != "__init__" and None != re.match('([A-Z]+[^A-Z]+)',name_ext[0]):
            if sys.modules.has_key(name_ext[0]):
                mod = sys.modules[name_ext[0]]
                reload(mod)
            else:
                exec("from %s import %s"%(name_ext[0],name_ext[0]))

class Lash:
    def __init__(self, host, database, user, region):
        self.host     = host
        self.database = database
        self.user     = user
        self.region   = region
        self.output   = os.path.join(ROOT_DIR, "output")

    def start(self):
        # init database config file
        self.__init_database_cfg()
        # run statistic
        self.__run_statistics()
        # merge the statistic result
        self.merge_statistic()

    def single_start(self,module):
        # init database config file
        self.__init_database_cfg()
        # run statistic
        self.__run_statistics(module)

    def merge_statistic(self):
        all_stat = {}
        for stat_file in os.listdir(os.path.join(self.output, "stat")):
            m = re.match('([A-Z]+[^A-Z0-9]+)',stat_file)
            if m:
                st_type = m.groups()[0].lower()
                if not all_stat.has_key(st_type):
                    all_stat[st_type] = self.__read_json(os.path.join(self.output, "stat", stat_file))
                else:
                    self.__merge_json(all_stat[st_type],self.__read_json(os.path.join(self.output, "stat", stat_file)))
        for key in all_stat.keys():
            with open(os.path.join(self.output, "rdf", key), 'w') as stf:
                stf.write(json.dumps(all_stat.get(key)))

    # private
    def __merge_json(self,m1,m2):
        for key in m2.keys():
            if m1.has_key(key):
                m1[key] = m1.get(key) + m2.get(key)
            else:
                m1[key] = m2.get(key)

    def __read_json(self, json_fp):
        with open(json_fp) as json_f:
            jd = json.load(json_f)
        return jd

    def __run_statistics(self,module=None):
        for stat_file in os.listdir(os.path.join(ROOT_DIR, "model")):
            name_ext = os.path.splitext(stat_file)
            if name_ext[1].upper() == ".PY" and name_ext[0] != "__init__" and None != re.match('([A-Z]+[^A-Z]+)',stat_file):
                if module and name_ext[0] != module:
                    continue
                globals()[name_ext[0]](self.region).get_statistic()

    def __init_database_cfg(self):
        cf = ConfigParser.ConfigParser()
        cf.add_section('development')
        cf.set('development', 'encoding', 'utf8')
        cf.set('development', 'database', self.database)
        cf.set('development', 'username', self.user)
        cf.set('development', 'password', 'postgres')
        cf.set('development', 'host',     self.host)
        with open(os.path.join(ROOT_DIR, "config", "database.cfg"), 'wb') as configfile:
            cf.write(configfile)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--host',     dest  ='host',       default='hqd-ssdpostgis-04.mypna.com', help='host address')
    parser.add_argument('--database', dest  ='database',   default='HERE_NA15Q1',                 help='database name')
    parser.add_argument('--user',     dest  ='user',       default='postgres',                    help='database username')
    parser.add_argument('--region',   dest  ='region',     default='na',                          help='statistic region code, different region include different country')
    parser.add_argument('--module',   dest  ='module',     default='',                            help='indicate the special module to run the statistic process')
    parser.add_argument('-merge',     action='store_true', default=None,                          help='merge statistic result')
    args = parser.parse_args()

    bg = datetime.datetime.now()
    if args.merge:
        Lash(args.host, args.database, args.user, args.region).merge_statistic()
    elif "" != args.module:
        Lash(args.host, args.database, args.user, args.region).single_start(args.module)
    else:
        Lash(args.host, args.database, args.user, args.region).start()
    ed = datetime.datetime.now()
    print "Process done! Cost time:"+str(ed - bg)

