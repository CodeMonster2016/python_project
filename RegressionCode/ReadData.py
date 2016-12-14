#-------------------------------------------------------------------------------
# Name:        ReadDBData
# Purpose:     connect the database to get tags and stats
#              retrun value
#              id => dict
#              like:
#              2009032090 => {'id':'23','name':'fd'}
#
# Author:      kuangh
#
# Created:     03/09/2015
# Copyright:   (c) kuangh 2015
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import os
import sys
import StringIO
import psycopg2

TEMPORARY_ID_TABLE = "temp_ids".lower()

class ReadDBData:
    def __init__(self, hostname, dbname, dbuser):
        self.hostname = hostname
        self.dbname   = dbname
        self.dbuser   = dbuser
        self.conn     = None
        self.cursor   = None

    #return cursor
    def connect(self):
        try:
            self.conn   = psycopg2.connect("dbname='" + self.dbname + "' user='" + self.dbuser + "' host='" + self.hostname + "'")
        except:
            print "[ERROR] Unable to connect to the database!"
            return None

        try:
            self.cursor = self.conn.cursor()
        except:
            self.close_conn()
            print "[ERROR] Unable to get cursor!"
            return None
        return True

    def get_stats(self,table_name):
        if not self.connect():
            return {}
        self.__run_sql(self.cursor.execute,"SELECT * FROM public."+table_name+";")
        rows = self.cursor.fetchall()
        self.close_cursor()
        self.close_conn()
        stats = {}
        for row in rows:
            stats[row[0]] = row[1]
        return stats

    def close_cursor(self):
        self.cursor.close()

    def close_conn(self):
        self.conn.close()

    def get_tags(self, table_name, testcases_path):
        if not self.connect():
            return {}
        print testcases_path
        self.__create_temporary_table()
        self.__dump_ids(testcases_path)
        return self.__select_data(table_name)


    def __create_temporary_table(self):
        self.__run_sql(self.cursor.execute,"DROP TABLE IF EXISTS public."+TEMPORARY_ID_TABLE+";")
        self.__run_sql(self.cursor.execute,"CREATE TABLE public."+TEMPORARY_ID_TABLE+
                                           " (id bigint NOT NULL,CONSTRAINT pk_temp_ids PRIMARY KEY (id)) WITH (OIDS=FALSE);"+
                                           "ALTER TABLE public."+TEMPORARY_ID_TABLE+" OWNER TO postgres;")

    def __dump_ids(self,testcases_path):
        self.cursor.copy_from(open(testcases_path,'r'),
                              "public."+TEMPORARY_ID_TABLE)
        self.conn.commit()

    def __select_data(self, table_name):
        self.__run_sql(self.cursor.execute,
                       "SELECT mt.id,mt.tags FROM public."+TEMPORARY_ID_TABLE+" AS tids LEFT JOIN public."+table_name+" AS mt ON tids.id = mt.id;")
        rows = self.cursor.fetchall()
        self.close_cursor()
        self.close_conn()
        tags = {}
        for row in rows:
            if row[0] == None:
                continue
            tags[str(row[0])] = self.__parse_kv_string(row[1])
        return tags

    def __parse_kv_string(self, kv_string):
        kv = {}
        if kv_string == None:
            return kv
        tags = kv_string.split('", ')
        #check dup name
        for tag in tags:
            key_value = tag.split('"=>"')
            if len(key_value) != 2:
                continue
            key   = key_value[0].strip().strip('"')
            value = key_value[1].strip().strip('"')
            if not kv.has_key(key):
                kv[key] = value
        return kv

    def __run_sql(self,execute,cmd):
        print cmd
        result = execute(cmd)
        self.conn.commit()
        print result

# unit test in here
def main():
    ReadDBData("hqd-ssdpostgis-04.mypna.com",
               "UniDB_HERE_NA15Q1_1.0.0.394617-20150819173337-TEST",
               "postgres").get_tags('ways',
                                    r'D:\workspace\UniDB_regression\current\UnidbRegressionTester\testsuites\na\ways#tags')

if __name__ == '__main__':
    main()