#-------------------------------------------------------------------------------
# Name:        TollBooth model
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
GLOBAL_KEY_PREFIX = "relations_barrier_"
CSV_SEP           = '`'
LF                = '\n'

#(key, category, function)
STATISTIC_KEYS    = (("type",False,"type"),
("barrier",True,"barrier"),
("toll_sys_type", False, "toll_sys_type"),
("toll_feat_type", False, "toll_feat_type"),
("payment:cash", True, "payment_cash"),
("payment:debit_cards", True, "payment_debit_cards"),
("payment:credit_cards", True, "payment_credit_cards"),
("payment:pay_pass", True, "payment_pay_pass"),
("payment:transponder", True, "payment_transponder"),
("payment:video", True, "payment_video"),
("payment:exact_cash", True, "payment_exact_cash"),
("payment:fuel_card", True, "payment_fuel_card"),
("fixed_fee", True, "fixed_fee"),
("obtain_ticket", True, "obtain_ticket"),
("pay_ticket", True, "pay_ticket"),
("electronic", True, "electronic"))

class RelationsTollBooth(Record):
    def __init__(self, region):
        Record.__init__(self)
        self.dump_file = os.path.join(ROOT_DIR, "temporary", self.__class__.__name__)
        self.stat      = {}
        self.region    = region

    def dump_navstrand(self):
        cmd = "SELECT \
DISTINCT(rc.condition_id), \
rc.condition_type, \
rct.toll_system_type, \
rct.toll_feature_type, \
rct.payment_cash, \
rct.payment_bank_card, \
rct.payment_credit_card, \
rct.payment_pass, \
rct.payment_transponder, \
rct.payment_video, \
rct.payment_exact_cash, \
rct.payment_travel_card, \
rct.structure_type_fixed_fee, \
rct.structure_type_obtain_ticket, \
rct.structure_type_pay_ticket, \
rct.structure_type_electronic \
FROM \
public.rdf_condition AS rc LEFT JOIN public.rdf_nav_strand AS rns ON rns.nav_strand_id=rc.nav_strand_id \
LEFT JOIN public.rdf_nav_link AS rnl ON rns.link_id = rnl.link_id \
LEFT JOIN public.rdf_condition_toll AS rct ON rct.condition_id=rc.condition_id \
WHERE rc.condition_type='1' AND rnl.iso_country_code IN (%s)"%(REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX))
        print cmd
        f = "%s_navstrand"%(self.dump_file)
        self.cursor.copy_expert("COPY (%s) TO STDOUT DELIMITER '`'"%(cmd),open(f,"w"))
        return f

    def dump_lanenavstrand(self):
        cmd = "SELECT \
DISTINCT(rc.condition_id), \
rc.condition_type, \
rct.toll_system_type, \
rct.toll_feature_type, \
rct.payment_cash, \
rct.payment_bank_card, \
rct.payment_credit_card, \
rct.payment_pass, \
rct.payment_transponder, \
rct.payment_video, \
rct.payment_exact_cash, \
rct.payment_travel_card, \
rct.structure_type_fixed_fee, \
rct.structure_type_obtain_ticket, \
rct.structure_type_pay_ticket, \
rct.structure_type_electronic \
FROM \
public.rdf_condition AS rc LEFT JOIN public.rdf_lane_nav_strand AS rlns ON rlns.condition_id=rc.condition_id \
LEFT JOIN public.rdf_lane AS rl ON rl.lane_id = rlns.lane_id \
LEFT JOIN public.rdf_nav_link AS rnl ON rl.link_id = rnl.link_id \
LEFT JOIN public.rdf_condition_toll AS rct ON rct.condition_id=rc.condition_id \
WHERE rc.condition_type='1' AND rnl.iso_country_code IN (%s)"%(REGION_COUNTRY_CODES(self.region, GLOBAL_KEY_PREFIX))
        print cmd
        f = "%s_lanenavstrand"%(self.dump_file)
        self.cursor.copy_expert("COPY (%s) TO STDOUT DELIMITER '`'"%(cmd),open(f,"w"))
        return f

    def get_statistic(self):
        try:
            self.__check_file(self.dump_navstrand())
            self.__check_file(self.dump_lanenavstrand())
        except:
            print "Some table or schema don't exist! Please check the upper sql"
            print ("Unexpected error:[ RelationsTollBooth.py->get_statistic] "+str(sys.exc_info()))
            return {}
        # write to file
        with open(os.path.join(ROOT_DIR, "output", "stat", self.__class__.__name__), 'w') as stf:
            stf.write(json.dumps(self.stat))
        return self.stat

    def __check_file(self,f):
        processcount = 0
        with open(f, "r",1024*1024*1024) as csv_f:
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

    def __statistic(self,line):
        for keys in STATISTIC_KEYS:
            try:
                getattr(self,'_RelationsTollBooth__get_'+keys[2])(keys,line)
            except:
                print "The statistic [ %s ] didn't exist"%(keys[2])
                print ("Unexpected error:[ RelationsTollBooth.py->__statistic] "+str(sys.exc_info()))

    def __count(self,key):
        if self.stat.has_key(key):
            self.stat[key] += 1
        else:
            self.stat[key] = 1

    # all statistic method
    def __get_type(self,keys,line):
        if '\N' != line[0]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_barrier(self,keys,line):
        if '1' == line[1]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('toll_booth') or ""))

    def __get_toll_sys_type(self,keys,line):
        if '\N' != line[2]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_toll_feat_type(self,keys,line):
        if '\N' != line[3]:
            self.__count("%s%s"%(GLOBAL_KEY_PREFIX,keys[0]))

    def __get_payment_cash(self,keys,line):
        if 'Y' == line[4]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('yes') or ""))

    def __get_payment_debit_cards(self,keys,line):
        if 'Y' == line[5]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('yes') or ""))

    def __get_payment_credit_cards(self,keys,line):
        if 'Y' == line[6]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('yes') or ""))

    def __get_payment_pay_pass(self,keys,line):
        if 'Y' == line[7]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('yes') or ""))

    def __get_payment_transponder(self,keys,line):
        if 'Y' == line[8]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('yes') or ""))

    def __get_payment_video(self,keys,line):
        if 'Y' == line[9]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('yes') or ""))

    def __get_payment_exact_cash(self,keys,line):
        if 'Y' == line[10]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('yes') or ""))

    def __get_payment_fuel_card(self,keys,line):
        if 'Y' == line[11]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('yes') or ""))

    def __get_fixed_fee(self,keys,line):
        if 'Y' == line[12]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('yes') or ""))

    def __get_obtain_ticket(self,keys,line):
        if 'Y' == line[13]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('yes') or ""))

    def __get_pay_ticket(self,keys,line):
        if 'Y' == line[14]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('yes') or ""))

    def __get_electronic(self,keys,line):
        if 'Y' == line[15]:
            self.__count("%s%s%s"%(GLOBAL_KEY_PREFIX,keys[0],keys[1] and "#%s"%('yes') or ""))

if __name__ == "__main__":
    # use to test this model
    bg = datetime.datetime.now()
    stat =  RelationsTollBooth('na').get_statistic()
    keys = stat.keys()
    print "==>"
    print "{%s}"%(",".join(map(lambda px: "\"%s\":%s"%(px,stat[px]) ,keys)))
    print "<=="
    ed = datetime.datetime.now()
    print "Cost time:"+str(ed - bg)
