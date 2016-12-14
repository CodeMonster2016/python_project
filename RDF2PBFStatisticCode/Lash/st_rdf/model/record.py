#-------------------------------------------------------------------------------
# Name:        base record model
# Purpose:     list database information and the basic method
#
# Author:      rex
#
# Created:     08/10/2015
# Copyright:   (c) rex 2015
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import os
import ConfigParser
import psycopg2
import sys

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

class Record:
    def __init__(self, mode="development"):
        #data base config file path is constant
        self.database_cfg = os.path.join(ROOT_DIR, "..", "config", "database.cfg")
        self.mode         = mode
        self.__parse_cfg()
        self.__connect()

    # public method
    def close_cursor(self):
        self.cursor.close()

    def close_conn(self):
        self.conn.close()

    def run_sql(self,execute,cmd):
        result = None
        try:
            print cmd
            result = execute(cmd)
            self.conn.commit()
            #print result
        except:
            print "Unexpected error:[ %s.py->%s] %s"%(self.__class__.__name__, 'run_sql', str(sys.exc_info()))
            self.conn.rollback()
        return result

    def rollback(self):
        self.conn.rollback()

    # private method
    def __parse_cfg(self):
        cf = ConfigParser.ConfigParser()
        cf.read(self.database_cfg)
        #database=None, user=None, password=None, host=None, port=None,
        self.host     = cf.get(self.mode,"host")
        self.database = cf.get(self.mode,"database")
        self.user     = cf.get(self.mode,"username")
        self.password = cf.get(self.mode,"password")
        #print [self.host, self.database, self.user, self.password]

    def __connect(self):
        try:
            self.conn = psycopg2.connect("dbname='" + self.database + "' user='" + self.user + "' host='" + self.host + "' password='"+self.password+"'")
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

    def is_existtable(self,schema,table):
        self.run_sql(self.cursor.execute,"select tablename from pg_tables where schemaname='"+schema+"';")
        return 0 != map(lambda px:px[0],self.cursor.fetchall()).count(table)


if __name__ == "__main__":
    # use to test this model
    pass

