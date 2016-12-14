###-------------------------------------------------------------------------------
# Name:
# Purpose:
#
# Author:      lgwu
#
# Created:     20-06-2013
# Copyright:   (c) lgwu 2013
# Licence:     <your licence>
#-------------------------------------------------------------------------------
#!/usr/bin/env python
import sys
import os
import psycopg2
import psycopg2.extras
from psycopg2 import Warning, Error
import optparse
import time
import copy
import multiprocessing

USR_LINK_COUNTRY = 'usr.usr_link_country'
USR_LINK_ORDER1 = 'usr.usr_link_order1'

class CfgParser(object):
    """
        class comment
    """

    def __init__(self, cfg, region):
        self.cfg = cfg
        self.region = region
        self.countries = []
        self.countries_div_order0 = []
        self.countries_div_order1 = []

        self._parse_cfg()

    def _parse_cfg(self):
        import ConfigParser as parser

        cf = parser.ConfigParser()
        cf.read(self.cfg)

        if self.region not in cf.sections():
            sys.stderr.write("Warning: can't find region %s \n" % self.region)

        if self.region in cf.sections():
            v = self._parse_cfg_region(self.region)
        else:
            v = self._parse_cfg_without_region()

        self.countries = v[0]
        self.countries_div_order0 = v[1]
        self.countries_div_order1 = v[2]

    def has_country(self, ic):
        return ic in self.countries

    def _parse_cfg_without_region(self):
        import ConfigParser as parser

        cf = parser.ConfigParser()
        cf.read(self.cfg)

        countries = []
        countries_div_order0 = []
        countries_div_order1 = []

        for region in cf.sections():
            v = self._parse_cfg_region(region)
            countries.extend(v[0])
            countries_div_order0.extend(v[1])
            countries_div_order1.extend(v[2])

        return countries, countries_div_order0, countries_div_order1

    def _parse_cfg_region(self, region):
        import ConfigParser as parser

        cf = parser.ConfigParser()
        cf.read(self.cfg)

        countries = []
        countries_div_order0 = []
        countries_div_order1 = []

        if cf.has_option(region, 'country_list'):
            countries = cf.get(region, 'country_list').strip().split(',')
            countries = [c.strip() for c in countries if c]
            countries = filter(None, countries)

        if cf.has_option(region, 'country_div_order0'):
            countries_div_order0 = cf.get(region, 'country_div_order0').strip().split(',')
            countries_div_order0 = [c.strip() for c in countries_div_order0 if c]
            countries_div_order0 = filter(None, countries_div_order0)

        countries_div_order1 = [c for c in countries if c not in countries_div_order0]

        return countries, countries_div_order0, countries_div_order1

class District(object):
    """
        class comment
    """
    def __init__(self, order, id, country):
        self.order = order
        self.id = id
        self.country = country.lower()

    def format_schema(self):
        if self.order == 0:
            return '%s_country_%d' % (self.country, self.id)
        else:
            return '%s_order%d_%d' % (self.country, self.order, self.id)

    def sql(self):
        if self.order == 0:
            return 'country_id = %d' %(self.id)
        else:
            return 'order%d_id = %d' %(self.order, self.id)

    def sql_key(self):
        if self.order == 0:
            return 'country_id'
        else:
            return 'order%d_id' % self.order

    def __str__(self):
        return '[%d, %d, %s]' %(self.order, self.id, self.country)

    def __repr__(self):
        return self.__str__()

    def __eq__(self, o):
        return self.country == o.country and self.order == o.order and self.id == o.id

    def __hash__(self):
        return hash((self.order, self.id, self.country))

    def __cmp__(self, o):
        return cmp((self.country, self.order, self.id), (o.country, o.order, o.id))


class SqlMeta(object):
    """
        class comment
    """

    def __init__(self, table):
        self.table = table
        self.sqls = []
        self.refs = []

    def add(self, sql):
        if sql not in self.sqls:
            self.sqls.append(sql)

    def ref(self, ref):
        if ref not in self.refs:
            self.refs.append(ref)

    def __str__(self):
        return '[%s]' %(self.table)

    def short_table(self):
        return self.table.split('.')[-1]

    def __cmp__(self, o):
        return cmp(self._get_table_key(), o._get_table_key())

    def _get_table_key(self):
        table_name = self.table.split('.')[-1]
        return table_name[4:]

class MetaCollection(object):
    """
        class comment
    """

    def __init__(self, district):
        self.district = district

        self.metas = []

        self.init()

    def init(self):
        import inspect

        for member in inspect.getmembers(self):
            name, method = member
            if not name.startswith('get_mt'):
                continue
            mt = method()
            if not mt:
                print method.name
            #print mt
            self.metas.append(mt)

        self.metas.sort()

    def get_metas(self):
        return self.metas

    """LINK AND FACE"""
    def get_mt_rdf_link(self):
        schema = self.district.format_schema()

        if self.district.order == 0:
            ref = USR_LINK_COUNTRY
        elif self.district.order == 1:
            ref = USR_LINK_ORDER1
        else:
            print 'Error: district order is unknown! %s' % self.district
            sys.exit(-1)

        table = '%s.rdf_link' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT b.* FROM %s a, rdf_link b WHERE %s and a.link_id = b.link_id)' %(mt.table, ref, self.district.sql()))
        mt.add('ALTER TABLE %s ADD CONSTRAINT pk_rdflink PRIMARY KEY(link_id)' % table)

        mt.add(self._index_sql(mt.table, 'ref_node_id'))
        mt.add(self._index_sql(mt.table, 'nonref_node_id'))

        mt.ref(ref)

        return mt

    def get_mt_rdf_node(self):
        """-->rdf_link"""
        schema = self.district.format_schema()

        table = '%s.rdf_node' % schema
        ref = '%s.rdf_link' % schema
        mt = SqlMeta(table)

        sqls = []
        sqls.append('SELECT DISTINCT b.* FROM %s a, rdf_node b WHERE a.ref_node_id = b.node_id' % ref)
        sqls.append('SELECT DISTINCT b.* FROM %s a, rdf_node b WHERE a.nonref_node_id = b.node_id' % ref)

        mt.add('CREATE TABLE %s AS (%s)' %(mt.table, ' UNION '.join(sqls)))
        mt.add(self._pk_sql(mt.table, 'node_id'))

        mt.ref(ref)

        return mt

    def get_mt_adas_link_geometry(self):
        """-->adas_link_geometry"""
        schema = self.district.format_schema()

        table = '%s.adas_link_geometry' % schema
        ref = '%s.rdf_link' % schema
        mt = SqlMeta(table)

        mt.add('CREATE TABLE %s AS (SELECT b.* FROM %s a, adas_link_geometry b WHERE a.link_id = b.link_id)' %(mt.table, ref))
        mt.add(self._index_sql(mt.table, 'link_id'))

        mt.ref(ref)

        return mt

    def get_mt_adas_node(self):
        # 2014.4.14 lgwu@telenav.cn, ADAS model change, start
##        """-->adas_node"""
##        schema = self.district.format_schema()
##
##        table = '%s.adas_node' % schema
##        ref = '%s.rdf_node' % schema
##        mt = SqlMeta(table)
##
##        mt.add('CREATE TABLE %s AS (SELECT b.* FROM %s a, adas_node b WHERE a.node_id = b.node_id)' %(mt.table, ref))
##        mt.add(self._pk_sql(mt.table, 'node_id'))
##
##        mt.ref(ref)
        schema = self.district.format_schema()
        table = '%s.adas_node' % schema

        sqls = []
        sqls.append('SELECT rn.* FROM %s.adas_node_slope ans, %s.rdf_node rn WHERE ans.node_id = rn.node_id' % (schema, schema))
        sqls.append('SELECT rn.* FROM %s.adas_node_curvature anc, %s.rdf_node rn WHERE anc.node_id = rn.node_id' % (schema, schema))

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (%s)' % (mt.table, ' UNION '.join(sqls)))

        mt.add(self._pk_sql(mt.table, 'node_id'))

        mt.ref('%s.adas_node_slope' % schema)
        mt.ref('%s.adas_node_curvature' % schema)
        mt.ref('%s.rdf_node' % schema)

        # 2014.4.14 lgwu@telenav.cn, ADAS model change, end

        return mt

    def get_mt_adas_node_curvature(self):
        """-->adas_node_curvature"""
        schema = self.district.format_schema()

        table = '%s.adas_node_curvature' % schema
        ref = '%s.rdf_node' % schema
        mt = SqlMeta(table)

        mt.add('CREATE TABLE %s AS (SELECT b.* FROM %s a, adas_node_curvature b WHERE a.node_id = b.node_id)' %(mt.table, ref))
        mt.add(self._index_sql(mt.table, 'node_id'))

        mt.ref(ref)

        return mt

    def get_mt_adas_node_slope(self):
        # 2014.4.14 lgwu@telenav.cn, ADAS model change, start
        schema = self.district.format_schema()

        table = '%s.adas_node_slope' % schema
##        ref = '%s.rdf_node' % schema
##        mt = SqlMeta(table)
##
##        mt.add('CREATE TABLE %s AS (SELECT b.* FROM %s a, adas_node_slope b WHERE a.node_id = b.node_id)' %(mt.table, ref))
##        mt.add(self._index_sql(mt.table, 'node_id'))
##
##        mt.ref(ref)
        mt = SqlMeta(table)

        mt.add('CREATE TABLE %s (to_link_id bigint, node_id bigint, ref_type char, slope bigint)' % mt.table)
        mt.add("INSERT INTO %s (SELECT rl.link_id AS to_link_id, rl.ref_node_id AS node_id, 'Y' AS ref_type, alg.slope FROM %s.adas_link_geometry alg,  %s.rdf_link rl WHERE alg.seq_num = 0 AND alg.slope IS NOT NULL AND alg.link_id = rl.link_id)" % (mt.table, schema, schema))
        mt.add("INSERT INTO %s (SELECT rl.link_id AS to_link_id, rl.nonref_node_id AS node_id, 'N' AS ref_type, alg.slope FROM %s.adas_link_geometry alg,  %s.rdf_link rl WHERE alg.seq_num = 999999 AND alg.slope IS NOT NULL AND alg.link_id = rl.link_id)" % (mt.table, schema, schema))

        mt.add(self._index_sql(mt.table, 'node_id'))

        mt.ref('%s.adas_link_geometry' % schema)
        mt.ref('%s.rdf_link' % schema)

        return mt
        # 2014.4.14 lgwu@telenav.cn, ADAS model change, end

    def get_mt_rdf_nav_link(self):
        schema = self.district.format_schema()

        table = '%s.rdf_nav_link' % schema
        ref = '%s.rdf_link' % schema

        mt = SqlMeta(table)
        sql = 'CREATE TABLE %s AS (SELECT b.* FROM %s a, rdf_nav_link b WHERE a.link_id = b.link_id)' %(mt.table, ref)
        mt.add(sql)
        sql = 'ALTER TABLE %s ADD CONSTRAINT pk_rdfnavlink PRIMARY KEY(link_id)' % table
        mt.add(sql)

        mt.ref(ref)

        return mt

    def get_mt_rdf_nav_link_status(self):
        """-->rdf_nav_link"""
        schema = self.district.format_schema()

        table = '%s.rdf_nav_link_status' % schema
        ref = '%s.rdf_nav_link' % schema

        mt = SqlMeta(table)
        sql = 'CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_nav_link_status b WHERE a.status_id = b.status_id)' %(mt.table, ref)
        mt.add(sql)
        sql = 'ALTER TABLE %s ADD CONSTRAINT pk_rdfnavlinkstatus PRIMARY KEY(status_id)' % table
        mt.add(sql)

        mt.ref(ref)

        return mt

    def get_mt_wkt_link(self):
        schema = self.district.format_schema()

        ref = '%s.rdf_link' % schema
        table = '%s.wkt_link' % schema
        mt = SqlMeta(table)
        sql = 'CREATE TABLE %s AS (SELECT b.* FROM %s a, wkt_link b WHERE a.link_id = b.link_id)' %(mt.table, ref)
        mt.add(sql)
        sql = 'ALTER TABLE %s ADD CONSTRAINT pk_wktlink PRIMARY KEY(link_id)' % table
        mt.add(sql)

        mt.ref(ref)

        return mt

    def get_mt_rdf_road_link(self):
        schema = self.district.format_schema()

        ref = '%s.rdf_nav_link' % schema
        table = '%s.rdf_road_link' % schema
        mt = SqlMeta(table)
        sql = 'CREATE TABLE %s AS (SELECT b.* FROM %s a, rdf_road_link b WHERE a.link_id = b.link_id)' %(mt.table, ref)
        mt.add(sql)
        sql = self._index_sql(table, 'link_id')
        mt.add(sql)
        sql = self._index_sql(table, 'left_address_range_id')
        mt.add(sql)
        sql = self._index_sql(table, 'right_address_range_id')
        mt.add(sql)

        mt.ref(ref)

        return mt

    def get_mt_rdf_road_name(self):
        schema = self.district.format_schema()

        ref = '%s.rdf_road_link' % schema
        table = '%s.rdf_road_name' % schema
        mt = SqlMeta(table)
        sql = 'CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_road_name b WHERE a.road_name_id = b.road_name_id)' %(mt.table, ref)
        mt.add(sql)
        sql = 'ALTER TABLE %s ADD CONSTRAINT pk_rdfroadname PRIMARY KEY(road_name_id)' % table
        mt.add(sql)

        mt.ref(ref)

        return mt

    def get_mt_rdf_road_name_trans(self):
        schema = self.district.format_schema()
        ref = '%s.rdf_road_link' % schema
        table = '%s.rdf_road_name_trans' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_road_name_trans b WHERE a.road_name_id = b.road_name_id)' %(mt.table, ref))
        mt.add(self._index_sql(mt.table, 'road_name_id'))

        mt.ref(ref)

        return mt

    def get_mt_rdf_address_range(self):
        schema = self.district.format_schema()

        ref = '%s.rdf_road_link' % schema
        table = '%s.rdf_address_range' % schema
        mt = SqlMeta(table)
        sql = 'CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_address_range b WHERE (a.left_address_range_id = b.address_range_id) UNION  SELECT DISTINCT b.* FROM %s a, rdf_address_range b WHERE (a.right_address_range_id = b.address_range_id))' %(mt.table, ref, ref)
        mt.add(sql)
        sql = self._index_sql(table, 'address_range_id')
        mt.add(sql)

        mt.ref(ref)

        return mt

    def get_mt_rdf_link_zone(self):
        schema = self.district.format_schema()

        ref = '%s.rdf_nav_link' % schema
        table = '%s.rdf_link_zone' % schema
        mt = SqlMeta(table)
        sql = 'CREATE TABLE %s AS (SELECT b.* FROM %s a, rdf_link_zone b WHERE a.link_id = b.link_id)' %(mt.table, ref)
        mt.add(sql)
        sql = self._index_sql(table, 'link_id')
        mt.add(sql)

        mt.ref(ref)
        return mt

    def get_mt_rdf_link_height(self):
        schema = self.district.format_schema()

        ref = '%s.rdf_link' % schema
        table = '%s.rdf_link_height' % schema
        mt = SqlMeta(table)
        sql = 'CREATE TABLE %s AS (SELECT b.* FROM %s a, rdf_link_height b WHERE a.link_id = b.link_id)' %(mt.table, ref)
        mt.add(sql)
        sql = self._index_sql(table, 'link_id')
        mt.add(sql)

        mt.ref(ref)
        return mt

    def get_mt_rdf_link_geometry(self):
        schema = self.district.format_schema()

        ref = '%s.rdf_link' % schema
        table = '%s.rdf_link_geometry' % schema
        mt = SqlMeta(table)
        sql = 'CREATE TABLE %s AS (SELECT b.* FROM %s a, rdf_link_geometry b WHERE a.link_id = b.link_id)' %(mt.table, ref)
        mt.add(sql)
        sql = self._index_sql(table, 'link_id')
        mt.add(sql)

        mt.ref(ref)
        return mt

    def get_mt_rdf_link_tpeg(self):
        schema = self.district.format_schema()

        ref = '%s.rdf_link' % schema
        table = '%s.rdf_link_tpeg' % schema
        mt = SqlMeta(table)
        sql = 'CREATE TABLE %s AS (SELECT b.* FROM %s a, rdf_link_tpeg b WHERE a.link_id = b.link_id)' %(mt.table, ref)
        mt.add(sql)
        sql = self._pk_sql(table, 'link_id')
        mt.add(sql)

        mt.ref(ref)
        return mt

    def get_mt_rdf_zone(self):
        schema = self.district.format_schema()

        ref = '%s.rdf_link_zone' % schema
        ref2 = '%s.rdf_city_poi' % schema
        table = '%s.rdf_zone' % schema
        mt = SqlMeta(table)
        sqls = ['SELECT b.* FROM %s a, rdf_zone b WHERE a.zone_id = b.zone_id' % ref,
                "SELECT b.* FROM %s a, rdf_zone b WHERE a.named_place_id = b.zone_id AND a.named_place_type='Z'" % ref2,]
        sql = 'CREATE TABLE %s AS (%s)' %(mt.table, ' UNION '.join(sqls))
        mt.add(sql)
        sql = 'ALTER TABLE %s ADD CONSTRAINT pk_rdfzone PRIMARY KEY(zone_id)' % table
        mt.add(sql)

        mt.ref(ref)
        mt.ref(ref2)

        return mt

    def get_mt_rdf_link_tmc(self):
        schema = self.district.format_schema()

        ref = '%s.rdf_nav_link' % schema
        table = '%s.rdf_link_tmc' % schema
        mt = SqlMeta(table)
        sql = 'CREATE TABLE %s AS (SELECT b.* FROM %s a, rdf_link_tmc b WHERE a.link_id = b.link_id)' %(mt.table, ref)
        mt.add(sql)
        sql = self._index_sql(table, 'link_id')
        mt.add(sql)

        mt.ref(ref)

        return mt

    def get_mt_rdf_lane(self):
        schema = self.district.format_schema()

        ref = '%s.rdf_nav_link' % schema
        table = '%s.rdf_lane' % schema
        mt = SqlMeta(table)
        sql = 'CREATE TABLE %s AS (SELECT b.* FROM %s a, rdf_lane b WHERE a.link_id = b.link_id)' %(mt.table, ref)
        mt.add(sql)
        sql = 'ALTER TABLE %s ADD CONSTRAINT pk_rdflane PRIMARY KEY(lane_id)' % table
        mt.add(sql)
        sql = self._index_sql(table, 'link_id')
        mt.add(sql)

        mt.ref(ref)

        return mt

    def get_mt_rdf_lane_nav_strand(self):
        schema = self.district.format_schema()

        ref = '%s.rdf_lane' % schema
        table = '%s.rdf_lane_nav_strand' % schema
        mt = SqlMeta(table)
        sql = 'CREATE TABLE %s AS (SELECT b.* FROM %s a, rdf_lane_nav_strand b WHERE a.lane_id = b.lane_id)' %(mt.table, ref)
        mt.add(sql)
        sql = self._index_sql(table, 'condition_id')
        mt.add(sql)
        sql = self._index_sql(table, 'lane_id')
        mt.add(sql)

        mt.ref(ref)

        return mt

    def get_mt_rdf_nav_link_attribute(self):
        schema = self.district.format_schema()

        ref = '%s.rdf_nav_link' % schema
        table = '%s.rdf_nav_link_attribute' % schema
        mt = SqlMeta(table)
        sql = 'CREATE TABLE %s AS (SELECT b.* FROM %s a, rdf_nav_link_attribute b WHERE a.link_id = b.link_id)' %(mt.table, ref)
        mt.add(sql)
        sql = 'ALTER TABLE %s ADD CONSTRAINT pk_rdfnavlinkattribute PRIMARY KEY(link_id)' % table
        mt.add(sql)

        mt.ref(ref)

        return mt

    def get_mt_rdf_condition(self):
        schema = self.district.format_schema()

        ref = '%s.rdf_nav_link' % schema
        table = '%s.rdf_condition' % schema
        mt = SqlMeta(table)
        sql = 'CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_nav_strand c, rdf_condition b WHERE a.link_id = c.link_id and c.seq_num = 0 and c.nav_strand_id = b.nav_strand_id)' %(mt.table, ref)
        mt.add(sql)

        ref2 = '%s.rdf_lane_nav_strand' % schema
        sql = 'INSERT INTO %s (SELECT DISTINCT b.* FROM %s a, rdf_condition b WHERE a.condition_id = b.condition_id)' % (mt.table, ref2)
        mt.add(sql)

        sql = 'ALTER TABLE %s ADD CONSTRAINT pk_rdfcondition PRIMARY KEY(condition_id)' % table
        mt.add(sql)

        sql = self._index_sql(table, 'nav_strand_id')
        mt.add(sql)

        mt.ref(ref)
        mt.ref(ref2)

        return mt

    def get_mt_rdf_nav_strand(self):
        schema = self.district.format_schema()

        ref = '%s.rdf_condition' % schema
        table = '%s.rdf_nav_strand' % schema
        mt = SqlMeta(table)
        sql = 'CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_nav_strand b WHERE a.nav_strand_id = b.nav_strand_id)' %(mt.table, ref)
        mt.add(sql)
        #sql = 'ALTER TABLE %s ADD CONSTRAINT pk_rdfnavstrand PRIMARY KEY(nav_strand_id)' % table
        sql = self._index_sql(table, 'nav_strand_id')
        mt.add(sql)
        sql = self._index_sql(table, 'link_id')
        mt.add(sql)

        mt.ref(ref)

        return mt

    def get_mt_rdf_condition_access(self):
        schema = self.district.format_schema()

        ref = '%s.rdf_condition' % schema
        table = '%s.rdf_condition_access' % schema
        mt = SqlMeta(table)
        sql = 'CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_condition_access b WHERE a.condition_id = b.condition_id)' %(mt.table, ref)
        mt.add(sql)

        mt.add(sql)
        sql = 'ALTER TABLE %s ADD CONSTRAINT pk_rdfconditionaccess PRIMARY KEY(condition_id)' % table
        mt.add(sql)

        mt.ref(ref)

        return mt

    def get_mt_rdf_condition_hov(self):
        schema = self.district.format_schema()

        ref = '%s.rdf_condition' % schema
        table = '%s.rdf_condition_hov' % schema
        mt = SqlMeta(table)
        sql = 'CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_condition_hov b WHERE a.condition_id = b.condition_id)' %(mt.table, ref)
        mt.add(sql)

##        ref2 = '%s.rdf_lane_nav_strand' % schema
##        sql = 'INSERT INTO %s (SELECT DISTINCT b.* FROM %s a, rdf_condition_hov b WHERE a.condition_id = b.condition_id)' % (mt.table, ref2)

        sql = 'ALTER TABLE %s ADD CONSTRAINT pk_rdfconditionhov PRIMARY KEY(condition_id)' % table
        mt.add(sql)

        mt.ref(ref)

        return mt

    def get_mt_rdf_condition_direction_travel(self):
        schema = self.district.format_schema()

        ref = '%s.rdf_condition' % schema
        table = '%s.rdf_condition_direction_travel' % schema
        mt = SqlMeta(table)
        sql = 'CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_condition_direction_travel b WHERE a.condition_id = b.condition_id)' %(mt.table, ref)
        mt.add(sql)

        sql = 'ALTER TABLE %s ADD CONSTRAINT pk_rdfconditiondirectiontravel PRIMARY KEY(condition_id)' % table
        mt.add(sql)

        mt.ref(ref)

        return mt

    def get_mt_rdf_condition_blackspot(self):
        """--->rdf_condition"""
        schema = self.district.format_schema()
        ref = '%s.rdf_condition' % schema
        table = '%s.rdf_condition_blackspot' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_condition_blackspot b WHERE a.condition_id = b.condition_id)' %(mt.table, ref))
        mt.add(self._pk_sql(mt.table, 'condition_id'))

        mt.ref(ref)

        return mt

    def get_mt_rdf_condition_driver_alert(self):
        """--->rdf_condition"""
        schema = self.district.format_schema()
        ref = '%s.rdf_condition' % schema
        table = '%s.rdf_condition_driver_alert' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_condition_driver_alert b WHERE a.condition_id = b.condition_id)' %(mt.table, ref))
        mt.add(self._pk_sql(mt.table, 'condition_id'))

        mt.ref(ref)

        return mt

##    def get_mt_rdf_condition_divider(self):
##        """--->rdf_condition"""
##        schema = self.district.format_schema()
##        ref = '%s.rdf_condition' % schema
##        table = '%s.rdf_condition_divider' % schema
##
##        mt = SqlMeta(table)
##        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_condition_divider b WHERE a.condition_id = b.condition_id)' %(mt.table, ref))
##        mt.add(self._pk_sql(mt.table, 'divider_id'))
##
##        mt.ref(ref)
##
##        return mt

    def get_mt_rdf_condition_dt(self):
        """--->rdf_condition"""
        schema = self.district.format_schema()
        ref = '%s.rdf_condition' % schema
        table = '%s.rdf_condition_dt' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_condition_dt b WHERE a.condition_id = b.condition_id)' %(mt.table, ref))
        mt.add(self._index_sql(mt.table, 'condition_id'))

        mt.ref(ref)

        return mt

    def get_mt_rdf_condition_env_zone(self):
        """--->rdf_condition"""
        schema = self.district.format_schema()
        ref = '%s.rdf_condition' % schema
        table = '%s.rdf_condition_env_zone' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_condition_env_zone b WHERE a.condition_id = b.condition_id)' %(mt.table, ref))
        mt.add(self._pk_sql(mt.table, 'condition_id'))

        mt.ref(ref)

        return mt

    def get_mt_rdf_condition_evacuation(self):
        """--->rdf_condition"""
        schema = self.district.format_schema()
        ref = '%s.rdf_condition' % schema
        table = '%s.rdf_condition_evacuation' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_condition_evacuation b WHERE a.condition_id = b.condition_id)' %(mt.table, ref))
        mt.add(self._pk_sql(mt.table, 'condition_id'))

        mt.ref(ref)

        return mt

    def get_mt_rdf_condition_gate(self):
        """--->rdf_condition"""
        schema = self.district.format_schema()
        ref = '%s.rdf_condition' % schema
        table = '%s.rdf_condition_gate' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_condition_gate b WHERE a.condition_id = b.condition_id)' %(mt.table, ref))
        mt.add(self._pk_sql(mt.table, 'condition_id'))

        mt.ref(ref)

        return mt

    def get_mt_rdf_condition_parking(self):
        """--->rdf_condition"""
        schema = self.district.format_schema()
        ref = '%s.rdf_condition' % schema
        table = '%s.rdf_condition_parking' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_condition_parking b WHERE a.condition_id = b.condition_id)' %(mt.table, ref))
        mt.add(self._pk_sql(mt.table, 'condition_id'))

        mt.ref(ref)

        return mt

    def get_mt_rdf_condition_rdm(self):
        """--->rdf_condition"""
        schema = self.district.format_schema()
        ref = '%s.rdf_condition' % schema
        table = '%s.rdf_condition_rdm' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_condition_rdm b WHERE a.condition_id = b.condition_id)' %(mt.table, ref))
        mt.add(self._pk_sql(mt.table, 'condition_id'))

        mt.ref(ref)

        return mt

    def get_mt_rdf_condition_speed(self):
        """--->rdf_condition"""
        schema = self.district.format_schema()
        ref = '%s.rdf_condition' % schema
        table = '%s.rdf_condition_speed' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_condition_speed b WHERE a.condition_id = b.condition_id)' %(mt.table, ref))
        mt.add(self._pk_sql(mt.table, 'condition_id'))

        mt.ref(ref)

        return mt

    def get_mt_rdf_condition_text(self):
        """--->rdf_condition"""
        schema = self.district.format_schema()
        ref = '%s.rdf_condition' % schema
        table = '%s.rdf_condition_text' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_condition_text b WHERE a.condition_id = b.condition_id)' %(mt.table, ref))
        mt.add(self._index_sql(mt.table, 'condition_id'))

        mt.ref(ref)

        return mt

    def get_mt_rdf_condition_toll(self):
        """--->rdf_condition"""
        schema = self.district.format_schema()
        ref = '%s.rdf_condition' % schema
        table = '%s.rdf_condition_toll' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_condition_toll b WHERE a.condition_id = b.condition_id)' %(mt.table, ref))
        mt.add(self._pk_sql(mt.table, 'condition_id'))

        mt.ref(ref)

        return mt

    def get_mt_rdf_condition_transport(self):
        """--->rdf_condition"""
        schema = self.district.format_schema()
        ref = '%s.rdf_condition' % schema
        table = '%s.rdf_condition_transport' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_condition_transport b WHERE a.condition_id = b.condition_id)' %(mt.table, ref))
        mt.add(self._pk_sql(mt.table, 'condition_id'))

        mt.ref(ref)

        return mt

    def get_mt_rdf_admin_hierarchy(self):
        schema = self.district.format_schema()

        rdf_zone = '%s.rdf_zone' % schema
        ref = '%s.rdf_link' % schema
        tmp = '%s.rdf_admin_hierarchy_tmp' % schema
        table = '%s.rdf_admin_hierarchy' % schema

        mt = SqlMeta(table)
        sqls = ['SELECT DISTINCT b.order8_id FROM %s a, rdf_admin_hierarchy b WHERE a.left_admin_place_id = b.admin_place_id' % ref,
                'SELECT DISTINCT b.order8_id FROM %s a, rdf_admin_hierarchy b WHERE a.right_admin_place_id = b.admin_place_id' % ref,
                'SELECT DISTINCT b.order8_id FROM %s a, rdf_admin_hierarchy b WHERE a.admin_place_id = b.admin_place_id' % rdf_zone
             ]

        sql = 'CREATE TABLE %s AS (SELECT b.* FROM (%s) AS a, rdf_admin_hierarchy b WHERE a.order8_id = b.order8_id AND b.admin_order >=8 )' % (mt.table, ' UNION '.join(sqls))
        mt.add(sql)
        sql = 'INSERT INTO %s (SELECT DISTINCT b.* FROM %s a, rdf_admin_hierarchy b WHERE a.order2_id = b.admin_place_id)' %(table, table)
        mt.add(sql)
        sql = 'INSERT INTO %s (SELECT DISTINCT b.* FROM %s a, rdf_admin_hierarchy b WHERE a.order1_id = b.admin_place_id)' %(table, table)
        mt.add(sql)
        sql = 'INSERT INTO %s (SELECT DISTINCT b.* FROM %s a, rdf_admin_hierarchy b WHERE a.country_id = b.admin_place_id)' %(table, table)
        mt.add(sql)

        mt.add('DELETE FROM %s WHERE ctid NOT IN (SELECT MIN(ctid) FROM %s GROUP BY admin_place_Id)' %(table, table))

        sql = 'ALTER TABLE %s ADD CONSTRAINT pk_rdfadminhierarchy PRIMARY KEY(admin_place_id)' % table
        mt.add(sql)

        mt.ref(ref)
        mt.ref(rdf_zone)

        return mt

    def get_mt_rdf_admin_place(self):
        schema = self.district.format_schema()

        ref = '%s.rdf_admin_hierarchy' % schema
        table = '%s.rdf_admin_place' % schema

        mt = SqlMeta(table)
        sql = 'CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_admin_place b WHERE a.admin_place_id = b.admin_place_id)' %(mt.table, ref)
        mt.add(sql)

        sql = 'ALTER TABLE %s ADD CONSTRAINT pk_rdfadminplace PRIMARY KEY(admin_place_id)' % table
        mt.add(sql)

        mt.ref(ref)

        return mt

    def get_mt_rdf_admin_attribute(self):
        """-->rdf_admin_hierarchy"""
        schema = self.district.format_schema()
        pubic = 'rdf_admin_attribute'
        ref = '%s.rdf_admin_hierarchy' % schema
        table = '%s.%s' % (schema, pubic)

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT b.* FROM %s a, %s b WHERE a.admin_place_id = b.admin_place_id)' %(mt.table, ref, pubic))
        mt.add(self._pk_sql(mt.table, 'admin_place_id'))

        mt.ref(ref)

        return mt

    def get_mt_rdf_admin_census(self):
        """-->rdf_admin_hierarchy"""
        schema = self.district.format_schema()
        pubic = 'rdf_admin_census'
        ref = '%s.rdf_admin_hierarchy' % schema
        table = '%s.%s' % (schema, pubic)

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, %s b WHERE a.admin_place_id = b.admin_place_id)' %(mt.table, ref, pubic))
        mt.add(self._index_sql(mt.table, 'admin_place_id'))
        mt.add(self._index_sql(mt.table, 'census_id'))

        mt.ref(ref)

        return mt

    def get_mt_rdf_admin_dst(self):
        """-->rdf_admin_place"""
        schema = self.district.format_schema()
        pubic = 'rdf_admin_dst'
        ref = '%s.rdf_admin_place' % schema
        table = '%s.%s' % (schema, pubic)

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, %s b WHERE a.dst_id = b.dst_id)' %(mt.table, ref, pubic))
        mt.add(self._pk_sql(mt.table, 'dst_id'))

        mt.ref(ref)

        return mt

    def get_mt_rdf_annotation(self):
        """ rdf_admin_place
        """
        schema = self.district.format_schema()
        pubic = 'rdf_annotation'
        ref = '%s.rdf_admin_place' % schema
        table = '%s.%s' % (schema, pubic)

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, %s b WHERE a.admin_place_id = b.admin_place_id)' %(mt.table, ref, pubic))
        mt.add(self._pk_sql(mt.table, 'annotation_id'))

        mt.ref(ref)

        return mt

    def get_mt_rdf_time_domain(self):
        schema = self.district.format_schema()

        ref = '%s.rdf_condition' % schema
        table = '%s.rdf_time_domain' % schema
        ref2 = '%s.rdf_admin_hierarchy' % schema
        mt = SqlMeta(table)
        sql = 'CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_time_domain b WHERE a.condition_id = b.feature_id)' %(mt.table, ref)
        mt.add(sql)
        sql = 'INSERT INTO %s (SELECT DISTINCT b.* FROM %s a, rdf_time_domain b WHERE a.admin_place_id = b.feature_id)' %(mt.table, ref2)
        mt.add(sql)
        sql = self._index_sql(table, 'feature_id')
        mt.add(sql)

        mt.ref(ref)
        mt.ref(ref2)

        return mt

    def get_mt_rdf_face_link(self):
        """-->rdf_face"""
        schema = self.district.format_schema()

        ref = '%s.rdf_face' % schema
        table = '%s.rdf_face_link' % schema

        mt = SqlMeta(table)
        sql = 'CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_face_link b WHERE a.face_id = b.face_id)' %(mt.table, ref)
        mt.add(sql)

        sql = self._index_sql(mt.table, 'face_id')
        mt.add(sql)
        sql = self._index_sql(mt.table, 'link_id')
        mt.add(sql)

        mt.ref(ref)

        return mt

    def get_mt_rdf_face(self):
        """-->rdf_link"""
        schema = self.district.format_schema()

        ref = '%s.rdf_link' % schema
        table = '%s.rdf_face' % schema

        mt = SqlMeta(table)
        sql = 'CREATE TABLE %s AS (SELECT DISTINCT b.face_id FROM %s a, rdf_face_link b WHERE a.link_id = b.link_id)' %(mt.table, ref)
        mt.add(sql)

        sql = 'ALTER TABLE %s ADD CONSTRAINT pk_rdfface PRIMARY KEY(face_id)' % table
        mt.add(sql)

        mt.ref(ref)

        return mt

    def get_mt_wkt_face(self):
        """-->rdf_face"""
        schema = self.district.format_schema()

        ref = '%s.rdf_face' % schema
        table = '%s.wkt_face' % schema

        mt = SqlMeta(table)
        sql = 'CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, wkt_face b WHERE a.face_id = b.face_id)' %(mt.table, ref)
        mt.add(sql)

        sql = 'ALTER TABLE %s ADD CONSTRAINT pk_wktface PRIMARY KEY(face_id)' % table
        mt.add(sql)

        mt.ref(ref)

        return mt

    def get_mt_rdf_carto_face(self):
        """-->rdf_face"""
        schema = self.district.format_schema()

        ref = '%s.rdf_face' % schema
        table = '%s.rdf_carto_face' % schema

        mt = SqlMeta(table)
        sql = 'CREATE TABLE %s AS (SELECT b.* FROM %s a, rdf_carto_face b WHERE a.face_id = b.face_id)' %(mt.table, ref)
        mt.add(sql)

        mt.add(self._index_sql(mt.table, 'carto_id'))
        mt.add(self._index_sql(mt.table, 'face_id'))

        mt.ref(ref)

        return mt

    def get_mt_rdf_carto_link(self):
        """-->rdf_link"""
        schema = self.district.format_schema()

        ref = '%s.rdf_link' % schema
        table = '%s.rdf_carto_link' % schema

        mt = SqlMeta(table)
        sql = 'CREATE TABLE %s AS (SELECT b.* FROM %s a, rdf_carto_link b WHERE a.link_id = b.link_id)' %(mt.table, ref)
        mt.add(sql)

        mt.add(self._index_sql(mt.table, 'carto_id'))
        mt.add(self._index_sql(mt.table, 'link_id'))

        mt.ref(ref)

        return mt

    def get_mt_rdf_carto(self):
        """-->rdf_carto_link, rdf_carto_face"""
        schema = self.district.format_schema()

        ref = '%s.rdf_carto_link' % schema
        ref2 = '%s.rdf_carto_face' % schema
        table = '%s.rdf_carto' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_carto b WHERE a.carto_id = b.carto_id)' %(mt.table, ref))
        mt.add('INSERT INTO %s (SELECT DISTINCT b.* FROM %s a, rdf_carto b WHERE a.carto_id = b.carto_id)' %(mt.table, ref2))

        #mt.add('delete from %s c USING rdf_admin_hierarchy p where c.named_place_id = p.admin_place_id and %s is not null and %s != %d' %(mt.table, self.district.sql_key(), self.district.sql_key(), self.district.id))
        #TODO: delete invalid carto

        mt.add('ALTER TABLE %s ADD CONSTRAINT pk_rdfcarto PRIMARY KEY(carto_id)' %(mt.table))

        mt.ref(ref)
        mt.ref(ref2)

        return mt

    """
    POI tables
    """
    def get_mt_rdf_poi_address(self):
        """-->None"""
        schema = self.district.format_schema()

        table = '%s.rdf_poi_address' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT * FROM rdf_poi_address  WHERE %s)' %(mt.table, self.district.sql()))
        mt.add(self._index_sql(mt.table, 'location_id'))
        mt.add(self._index_sql(mt.table, 'poi_id'))

        return mt

    def get_mt_rdf_poi_address_trans(self):
        """-->rdf_poi_address"""
        schema = self.district.format_schema()
        ref = '%s.rdf_poi_address' % schema
        table = '%s.rdf_poi_address_trans' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT b.* FROM %s a, rdf_poi_address_trans b  WHERE a.poi_id = b.poi_id)' %(mt.table, ref))
        mt.add(self._index_sql(mt.table, 'poi_id'))

        mt.ref(ref)

        return mt

    def get_mt_rdf_poi(self):
        """-->rdf_poi_address"""
        schema = self.district.format_schema()
        ref = '%s.rdf_poi_address' % schema
        table = '%s.rdf_poi' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT distinct b.* FROM %s a, rdf_poi b WHERE a.poi_id = b.poi_id)' %(mt.table, ref))
        mt.add('ALTER TABLE %s ADD CONSTRAINT pk_rdfpoi PRIMARY KEY(poi_id)' % mt.table)

        mt.ref(ref)

        return mt

    def get_mt_rdf_location(self):
        """-->rdf_nav_link"""
        schema = self.district.format_schema()
        ref = '%s.rdf_nav_link' % schema
        table = '%s.rdf_location' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_location b WHERE a.link_id = b.link_id)' %(mt.table, ref))
        mt.add('ALTER TABLE %s ADD CONSTRAINT pk_rdflocation PRIMARY KEY(location_id)' % mt.table)

        mt.ref(ref)

        return mt

    def get_mt_rdf_poi_names(self):
        """-->rdf_poi"""
        schema = self.district.format_schema()
        ref = '%s.rdf_poi' % schema
        table = '%s.rdf_poi_names' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT b.* FROM %s a, rdf_poi_names b WHERE a.poi_id = b.poi_id)' %(mt.table, ref))
        mt.add(self._index_sql(mt.table, 'poi_id'))
        mt.add(self._index_sql(mt.table, 'name_id'))

        mt.ref(ref)

        return mt

    def get_mt_rdf_poi_name(self):
        """-->rdf_poi_names"""
        schema = self.district.format_schema()
        ref = '%s.rdf_poi_names' % schema
        table = '%s.rdf_poi_name' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_poi_name b WHERE a.name_id = b.name_id)' %(mt.table, ref))
        mt.add('ALTER TABLE %s ADD CONSTRAINT pk_rdfpoiname PRIMARY KEY(name_id)' % mt.table)

        mt.ref(ref)
        return mt

    def get_mt_rdf_poi_name_trans(self):
        """-->rdf_poi"""
        schema = self.district.format_schema()
        ref = '%s.rdf_poi_name' % schema
        table = '%s.rdf_poi_name_trans' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_poi_name_trans b WHERE a.name_id = b.name_id)' %(mt.table, ref))
        mt.add(self._index_sql(mt.table, 'name_id'))

        mt.ref(ref)
        return mt

##    def get_mt_rdf_poi_name_trans(self):
##        """-->rdf_poi"""
##        schema = self.district.format_schema()
##        ref = '%s.rdf_poi' % schema
##        table = '%s.rdf_poi_name_trans' % schema
##
##        mt = SqlMeta(table)
##        mt.add('CREATE TABLE %s AS (SELECT b.* FROM %s a, rdf_poi_name_trans b WHERE a.poi_id = b.poi_id)' %(mt.table, ref))
##        mt.add(self._index_sql(mt.table, 'name_id'))
##
##        mt.ref(ref)
##        return mt

    def get_mt_rdf_poi_airport(self):
        """-->rdf_poi"""
        schema = self.district.format_schema()
        ref = '%s.rdf_poi' % schema
        table = '%s.rdf_poi_airport' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT b.* FROM %s a, rdf_poi_airport b WHERE a.poi_id = b.poi_id)' %(mt.table, ref))
        mt.add('ALTER TABLE %s ADD CONSTRAINT pk_rdfpoiairport PRIMARY KEY(poi_id)' %mt.table)

        mt.ref(ref)
        return mt

    def get_mt_rdf_poi_association(self):
        """-->rdf_poi"""
        schema = self.district.format_schema()
        ref = '%s.rdf_poi' % schema
        table = '%s.rdf_poi_association' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT b.* FROM %s a, rdf_poi_association b WHERE a.poi_id = b.poi_id)' %(mt.table, ref))
        mt.add(self._index_sql(mt.table, 'poi_id'))

        mt.ref(ref)
        return mt

    def get_mt_rdf_poi_chains(self):
        """-->rdf_poi"""
        schema = self.district.format_schema()
        ref = '%s.rdf_poi' % schema
        table = '%s.rdf_poi_chains' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT b.* FROM %s a, rdf_poi_chains b WHERE a.poi_id = b.poi_id)' %(mt.table, ref))
        mt.add(self._index_sql(mt.table, 'poi_id'))

        mt.ref(ref)
        return mt

    def get_mt_rdf_poi_children(self):
        """-->rdf_poi"""
        schema = self.district.format_schema()
        ref = '%s.rdf_poi' % schema
        table = '%s.rdf_poi_children' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_poi_children b WHERE a.poi_id = b.poi_id or a.poi_id = b.child_poi_id)' %(mt.table, ref))
        mt.add(self._index_sql(mt.table, 'poi_id'))

        mt.ref(ref)
        return mt

    def get_mt_rdf_poi_contact_information(self):
        """-->rdf_poi"""
        schema = self.district.format_schema()
        ref = '%s.rdf_poi' % schema
        table = '%s.rdf_poi_contact_information' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT b.* FROM %s a, rdf_poi_contact_information b WHERE a.poi_id = b.poi_id)' %(mt.table, ref))
        mt.add(self._index_sql(mt.table, 'poi_id'))

        mt.ref(ref)
        return mt

    def get_mt_rdf_poi_feature(self):
        """-->rdf_poi"""
        schema = self.district.format_schema()
        ref = '%s.rdf_poi' % schema
        table = '%s.rdf_poi_feature' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT b.* FROM %s a, rdf_poi_feature b WHERE a.poi_id = b.poi_id)' %(mt.table, ref))
        mt.add(self._index_sql(mt.table, 'poi_id'))
        mt.add(self._index_sql(mt.table, 'feature_id'))

        mt.ref(ref)
        return mt

    def get_mt_rdf_poi_petrol_station(self):
        """-->rdf_poi"""
        schema = self.district.format_schema()
        ref = '%s.rdf_poi' % schema
        table = '%s.rdf_poi_petrol_station' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT b.* FROM %s a, rdf_poi_petrol_station b WHERE a.poi_id = b.poi_id)' %(mt.table, ref))
        mt.add(self._index_sql(mt.table, 'poi_id'))

        mt.ref(ref)
        return mt

    def get_mt_rdf_poi_place_of_worship(self):
        """-->rdf_poi"""
        schema = self.district.format_schema()
        ref = '%s.rdf_poi' % schema
        table = '%s.rdf_poi_place_of_worship' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT b.* FROM %s a, rdf_poi_place_of_worship b WHERE a.poi_id = b.poi_id)' %(mt.table, ref))
        mt.add(self._index_sql(mt.table, 'poi_id'))

        mt.ref(ref)
        return mt

    def get_mt_rdf_poi_rest_area(self):
        """-->rdf_poi"""
        schema = self.district.format_schema()
        ref = '%s.rdf_poi' % schema
        table = '%s.rdf_poi_rest_area' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT b.* FROM %s a, rdf_poi_rest_area b WHERE a.poi_id = b.poi_id)' %(mt.table, ref))
        mt.add(self._index_sql(mt.table, 'poi_id'))

        mt.ref(ref)
        return mt

    def get_mt_rdf_poi_restaurant(self):
        """-->rdf_poi"""
        schema = self.district.format_schema()
        ref = '%s.rdf_poi' % schema
        table = '%s.rdf_poi_restaurant' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT b.* FROM %s a, rdf_poi_restaurant b WHERE a.poi_id = b.poi_id)' %(mt.table, ref))
        mt.add(self._index_sql(mt.table, 'poi_id'))

        mt.ref(ref)
        return mt

    def get_mt_rdf_poi_subcategory(self):
        """-->rdf_poi"""
        schema = self.district.format_schema()
        ref = '%s.rdf_poi' % schema
        ref_city_poi = '%s.rdf_city_poi' % schema
        table = '%s.rdf_poi_subcategory' % schema

        mt = SqlMeta(table)
        sqls = []
        sqls.append('SELECT b.* FROM %s a, rdf_poi_subcategory b WHERE a.poi_id = b.poi_id' % ref)
        sqls.append('SELECT b.* FROM %s a, rdf_poi_subcategory b WHERE a.poi_id = b.poi_id' % ref_city_poi)
        mt.add('CREATE TABLE %s AS (%s)' % (mt.table, ' UNION '.join(sqls)))
        mt.add(self._index_sql(mt.table, 'poi_id'))

        mt.ref(ref)
        mt.ref(ref_city_poi)
        return mt

##    def get_mt_rdf_poi_supplier(self):
##        """-->rdf_poi"""
##        schema = self.district.format_schema()
##        ref = '%s.rdf_poi' % schema
##        table = '%s.rdf_poi_supplier' % schema
##
##        mt = SqlMeta(table)
##        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_poi_supplier b WHERE a.supplier_id = b.supplier_id)' %(mt.table, ref))
##        mt.add('ALTER TABLE %s ADD CONSTRAINT pk_rdfpoisupplier PRIMARY KEY(supplier_id)' % mt.table)
##
##        mt.ref(ref)
##        return mt

    def get_mt_rdf_poi_vanity_city(self):
        """-->rdf_poi, rdf_city_poi"""
        schema = self.district.format_schema()
        ref = '%s.rdf_poi' % schema
        ref2 = '%s.rdf_city_poi' % schema
        table = '%s.rdf_poi_vanity_city' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT b.* FROM %s a, rdf_poi_vanity_city b WHERE a.poi_id = b.poi_id UNION SELECT b.* FROM %s a, rdf_poi_vanity_city b WHERE a.poi_id = b.poi_id)' %(mt.table, ref, ref2))
        mt.add(self._index_sql(mt.table, 'poi_id'))

        mt.ref(ref)
        mt.ref(ref2)
        return mt

    """CF """
    def get_mt_rdf_cf_link(self):
        """-->rdf_link"""
        schema = self.district.format_schema()
        ref = '%s.rdf_link' % schema
        table = '%s.rdf_cf_link' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT b.* FROM %s a, rdf_cf_link b WHERE a.link_id = b.link_id)' %(mt.table, ref))
        mt.add(self._index_sql(mt.table, 'cf_id'))
        mt.add(self._index_sql(mt.table, 'link_id'))

        mt.ref(ref)
        return mt

    def get_mt_rdf_cf_building(self):
        """-->rdf_building"""
        schema = self.district.format_schema()
        ref = '%s.rdf_building' % schema
        table = '%s.rdf_cf_building' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_cf_building b WHERE a.building_id = b.building_id)' %(mt.table, ref))
        mt.add(self._index_sql(mt.table, 'cf_id'))
        mt.add(self._index_sql(mt.table, 'building_id'))

        mt.ref(ref)
        return mt

    def get_mt_rdf_cf(self):
        """-->rdf_cf_link, rdf_cf_building"""
        schema = self.district.format_schema()
        table = '%s.rdf_cf' % schema
        mt = SqlMeta(table)

        sqls = []
        for t in ['rdf_cf_link', 'rdf_cf_building']:
            ref = '%s.%s' % (schema, t)
            sqls.append('SELECT DISTINCT b.* FROM %s a, rdf_cf b WHERE a.cf_id = b.cf_id' %(ref))
            mt.ref(ref)

        mt.add('CREATE TABLE %s AS (%s)' %(mt.table, ' UNION '.join(sqls)))
        mt.add(self._pk_sql(mt.table, 'cf_id'))

        return mt

    def get_mt_rdf_cf_attribute(self):
        """-->rdf_cf"""
        schema = self.district.format_schema()
        ref = '%s.rdf_cf' % schema
        table = '%s.rdf_cf_attribute' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_cf_attribute b WHERE a.cf_id = b.cf_id)' %(mt.table, ref))
        mt.add(self._index_sql(mt.table, 'cf_id'))

        mt.ref(ref)
        return mt

    def get_mt_rdf_cf_carto(self):
        """-->rdf_cf"""
        schema = self.district.format_schema()
        ref = '%s.rdf_cf' % schema
        table = '%s.rdf_cf_carto' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_cf_carto b WHERE a.cf_id = b.cf_id)' %(mt.table, ref))
        mt.add(self._index_sql(mt.table, 'cf_id'))
        mt.add(self._index_sql(mt.table, 'carto_id'))

        mt.ref(ref)
        return mt

    def get_mt_rdf_cf_cf(self):
        """-->rdf_cf"""
        schema = self.district.format_schema()
        ref = '%s.rdf_cf' % schema
        table = '%s.rdf_cf_cf' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_cf_cf b WHERE a.cf_id = b.cf_id)' %(mt.table, ref))
        mt.add(self._index_sql(mt.table, 'cf_id'))
        mt.add(self._index_sql(mt.table, 'child_cf_id'))

        mt.ref(ref)
        return mt

    def get_mt_rdf_cf_node(self):
        """-->rdf_cf"""
        schema = self.district.format_schema()
        ref = '%s.rdf_cf' % schema
        table = '%s.rdf_cf_node' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_cf_node b WHERE a.cf_id = b.cf_id)' %(mt.table, ref))
        mt.add(self._index_sql(mt.table, 'cf_id'))
        mt.add(self._index_sql(mt.table, 'node_id'))

        mt.ref(ref)
        return mt

    """CITY POI"""
    def get_mt_rdf_city_poi(self):
        """-->None"""
        schema = self.district.format_schema()
        table = '%s.rdf_city_poi' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT * FROM rdf_city_poi  WHERE %s)' %(mt.table, self.district.sql()))
        mt.add('ALTER TABLE %s ADD CONSTRAINT pk_rdfcitypoi PRIMARY KEY(poi_id)' % mt.table)

        return mt

    def get_mt_rdf_city_poi_names(self):
        """-->rdf_city_poi"""
        schema = self.district.format_schema()
        ref = '%s.rdf_city_poi' % schema
        table = '%s.rdf_city_poi_names' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT b.* FROM %s a, rdf_city_poi_names b WHERE a.poi_id = b.poi_id)' %(mt.table, ref))
        mt.add(self._index_sql(mt.table, 'poi_id'))
        mt.add(self._index_sql(mt.table, 'name_id'))

        mt.ref(ref)
        return mt

    def get_mt_rdf_city_poi_name(self):
        """-->rdf_city_poi"""
        schema = self.district.format_schema()
        ref = '%s.rdf_city_poi_names' % schema
        table = '%s.rdf_city_poi_name' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_city_poi_name b WHERE a.name_id = b.name_id)' %(mt.table, ref))
        mt.add('ALTER TABLE %s ADD CONSTRAINT pk_rdfcitypoiname PRIMARY KEY(name_id)' % mt.table)

        mt.ref(ref)

        return mt

    def get_mt_rdf_city_poi_name_trans(self):
        """-->rdf_city_poi"""
        schema = self.district.format_schema()
        ref = '%s.rdf_city_poi_name' % schema
        table = '%s.rdf_city_poi_name_trans' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_city_poi_name_trans b WHERE a.name_id = b.name_id)' %(mt.table, ref))
        mt.add(self._index_sql(mt.table, 'name_id'))

        mt.ref(ref)

        return mt

    def get_mt_rdf_feature_point(self):
        """-->rdf_link"""
        schema = self.district.format_schema()

        ref = '%s.rdf_link' % schema
        table = '%s.rdf_feature_point' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_feature_point b WHERE a.link_id = b.link_id)' % (mt.table, ref))
        mt.add(self._pk_sql(mt.table, 'fp_id'))

        mt.ref(ref)

        return mt

    def get_mt_rdf_feature_names(self):
        """--->rdf_admin_hierarchy, rdf_building, rdf_carto, rdf_cf, rdf_zone, rdf_annotation

            • AdminPlace (all administrative areas, RDF_ADMIN_PLACE)
            • Structure Footprints (RDF_BUILDING)
            • Cartographic feature (RDF_CARTO)
                Retrieve Names of Administrative Cartographic Features via
                RDF_CARTO.NAMED_PLACE_ID.
            • Complex Feature (RDF_CF)
            • Zone (RDF_ZONE)
            • Annotation Names (RDF_ANNOTATION)
            Names of links are stored in RDF_ROAD_NAME.
            Names of POIs are stored in RDF_POI_NAME.
            Names of Named Place POIs are stored in RDF_CITY_POI_NAME
        """
        schema = self.district.format_schema()
        ref = '%s.rdf_admin_hierarchy' % schema
        ref2 = '%s.rdf_building' % schema
        ref3 = '%s.rdf_carto' % schema
        ref4 = '%s.rdf_cf' % schema
        ref5 = '%s.rdf_zone' % schema
        ref6 = '%s.rdf_annotation' % schema

        sqls = []
        sqls.append('SELECT b.* from %s a, rdf_feature_names b where a.admin_place_id = b.feature_id' %(ref))
        sqls.append('SELECT b.* from %s a, rdf_feature_names b where a.building_id = b.feature_id' %(ref2))
        sqls.append('SELECT b.* from %s a, rdf_feature_names b where a.carto_id = b.feature_id' %(ref3))
        sqls.append('SELECT b.* from %s a, rdf_feature_names b where a.cf_id = b.feature_id' %(ref4))
        sqls.append('SELECT b.* from %s a, rdf_feature_names b where a.zone_id = b.feature_id' %(ref5))
        sqls.append('SELECT b.* from %s a, rdf_feature_names b where a.annotation_id = b.feature_id' %(ref6))

        table = '%s.rdf_feature_names' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (%s)' %(mt.table, ' UNION '.join(sqls)))

        mt.add(self._index_sql(mt.table, 'feature_id'))
        mt.add(self._index_sql(mt.table, 'name_id'))

        mt.ref(ref)
        mt.ref(ref2)
        mt.ref(ref3)
        mt.ref(ref4)
        mt.ref(ref5)
        mt.ref(ref6)

        return mt

    def get_mt_rdf_feature_name(self):
        """-->rdf_feature_names"""
        schema = self.district.format_schema()

        ref = '%s.rdf_feature_names' % schema
        table = '%s.rdf_feature_name' % schema
        mt = SqlMeta(table)
        sql = 'CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_feature_name b WHERE a.name_id = b.name_id)' %(mt.table, ref)
        mt.add(sql)
        mt.add('ALTER TABLE %s ADD CONSTRAINT pk_rdffeaturename PRIMARY KEY(name_id)' % mt.table)
        mt.add(sql)

        mt.ref(ref)

        return mt

    def get_mt_rdf_feature_name_trans(self):
        """-->rdf_feature_name"""
        schema = self.district.format_schema()

        ref = '%s.rdf_feature_name' % schema
        table = '%s.rdf_feature_name_trans' % schema
        mt = SqlMeta(table)
        sql = 'CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_feature_name_trans b WHERE a.name_id = b.name_id)' %(mt.table, ref)
        mt.add(sql)
        sql = self._index_sql(table, 'name_id')
        mt.add(sql)

        mt.ref(ref)

        return mt

    """VCE """
    def get_mt_vce_road_name(self):
        schema = self.district.format_schema()

        ref = '%s.rdf_road_name' % schema
        table = '%s.vce_road_name' % schema
        mt = SqlMeta(table)
        sql = 'CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, vce_road_name b WHERE a.road_name_id = b.road_name_id)' %(mt.table, ref)
        mt.add(sql)
        sql = self._index_sql(table, 'road_name_id')
        mt.add(sql)

        mt.ref(ref)

        return mt

    def get_mt_vce_poi_name(self):
        schema = self.district.format_schema()

        ref = '%s.rdf_poi_name' % schema
        table = '%s.vce_poi_name' % schema
        mt = SqlMeta(table)
        sql = 'CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, vce_poi_name b WHERE a.name_id = b.name_id)' %(mt.table, ref)
        mt.add(sql)
        sql = self._index_sql(table, 'name_id')
        mt.add(sql)

        mt.ref(ref)

        return mt

    def get_mt_vce_city_poi_name(self):
        schema = self.district.format_schema()

        ref = '%s.rdf_city_poi_name' % schema
        table = '%s.vce_city_poi_name' % schema
        mt = SqlMeta(table)
        sql = 'CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, vce_city_poi_name b WHERE a.name_id = b.name_id)' %(mt.table, ref)
        mt.add(sql)
        sql = self._index_sql(table, 'name_id')
        mt.add(sql)

        mt.ref(ref)

        return mt

    def get_mt_vce_feature_name(self):
        schema = self.district.format_schema()
        ref = '%s.rdf_feature_name' % schema
        table = '%s.vce_feature_name' % schema

        mt = SqlMeta(table)
        sql = 'CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, vce_feature_name b WHERE a.name_id = b.name_id)' %(mt.table, ref)
        mt.add(sql)
        sql = self._index_sql(table, 'name_id')
        mt.add(sql)

        mt.ref(ref)

        return mt

    def get_mt_vce_phonetic_text(self):
        """--->vce_road_name, vce_poi_name, vce_city_poi_name, vce_feature_name, ..."""
        schema = self.district.format_schema()
        table = '%s.vce_phonetic_text' % schema
        mt = SqlMeta(table)

        sqls = []
        for t in ['vce_road_name', 'vce_poi_name', 'vce_city_poi_name', 'vce_feature_name', 'vce_sign_element', 'vce_sign_destination']:
            ref = '%s.%s' % (schema, t)
            sqls.append('SELECT DISTINCT b.* FROM %s a, vce_phonetic_text b WHERE a.phonetic_id = b.phonetic_id' % ref)
            mt.ref(ref)

        sql = 'CREATE TABLE %s AS (%s)' %(mt.table, ' UNION '.join(sqls))
        mt.add(sql)
        sql = self._index_sql(table, 'phonetic_id')
        mt.add(sql)

        return mt

    """ADM"""
    def get_mt_adm_city_poi_subregion(self):
        """-->rdf_city_poi"""
        schema = self.district.format_schema()
        pubic = 'adm_city_poi_subregion'
        ref = '%s.rdf_city_poi' % schema
        table = '%s.%s' % (schema, pubic)

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, %s b WHERE a.poi_id = b.poi_id)' %(mt.table, ref, pubic))
        mt.add(self._index_sql(table, 'poi_id'))

        mt.ref(ref)

        return mt

    def get_mt_adm_link_subregion(self):
        """-->rdf_link"""
        schema = self.district.format_schema()
        pubic = 'adm_link_subregion'
        ref = '%s.rdf_link' % schema
        table = '%s.%s' % (schema, pubic)

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, %s b WHERE a.link_id = b.link_id)' %(mt.table, ref, pubic))
        mt.add(self._index_sql(table, 'link_id'))

        mt.ref(ref)

        return mt

    def get_mt_adm_poi_subregion(self):
        """-->rdf_poi"""
        schema = self.district.format_schema()
        pubic = 'adm_poi_subregion'
        ref = '%s.rdf_poi' % schema
        table = '%s.%s' % (schema, pubic)

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, %s b WHERE a.poi_id = b.poi_id)' %(mt.table, ref, pubic))
        mt.add(self._index_sql(table, 'poi_id'))

        mt.ref(ref)

        return mt

    def get_mt_adm_subregion(self):
        """-->rdf_admin_place"""
        schema = self.district.format_schema()
        pubic = 'adm_subregion'
        ref = '%s.rdf_admin_place' % schema
        table = '%s.%s' % (schema, pubic)

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, %s b WHERE a.admin_place_id = b.admin_place_id)' %(mt.table, ref, pubic))
        mt.add(self._index_sql(table, 'admin_subregion_id'))
        mt.add(self._index_sql(table, 'admin_place_id'))

        mt.ref(ref)

        return mt

    def get_mt_adm_subregion_def(self):
        """-->adm_subregion"""
        schema = self.district.format_schema()
        pubic = 'adm_subregion_def'
        ref = '%s.adm_subregion' % schema
        table = '%s.%s' % (schema, pubic)

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, %s b WHERE a.admin_subregion_id = b.admin_subregion_id)' %(mt.table, ref, pubic))
        mt.add(self._index_sql(table, 'admin_subregion_id'))
        mt.add(self._index_sql(table, 'region_id'))

        mt.ref(ref)

        return mt

    def get_mt_rdf_access(self):
        """--->rdf_nav_link, rdf_lane, rdf_condition"""
        schema = self.district.format_schema()
        pubic = 'rdf_access'
        table = '%s.%s' % (schema, pubic)

        mt = SqlMeta(table)

        sqls = []
        for t in ['rdf_nav_link', 'rdf_lane', 'rdf_condition']:
            ref = '%s.%s' % (schema, t)
            sqls.append('SELECT DISTINCT b.* FROM %s a, %s b WHERE a.access_id = b.access_id' %(ref, pubic))
            mt.ref(ref)

        mt.add('CREATE TABLE %s AS (%s)' %(mt.table, ' UNION '.join(sqls)))
        mt.add('ALTER TABLE %s ADD CONSTRAINT pk_rdfaccess PRIMARY KEY(access_id)' % mt.table)

        return mt

    """ADDRESS POINT"""
    def get_mt_rdf_address_point(self):
        """-->rdf_road_link"""
        schema = self.district.format_schema()
        pubic = 'rdf_address_point'
        ref = '%s.rdf_nav_link' % schema
        table = '%s.%s' % (schema, pubic)

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT b.* FROM %s a, %s b WHERE a.link_id = b.arrival_link_id)' %(mt.table, ref, pubic))
        mt.add('ALTER TABLE %s ADD CONSTRAINT pk_rdfaddresspoint PRIMARY KEY(address_point_id, language_code)' % mt.table)
        mt.ref(ref)

        return mt

    def get_mt_rdf_address_point_trans(self):
        """-->rdf_address_point"""
        schema = self.district.format_schema()
        pubic = 'rdf_address_point_trans'
        ref = '%s.rdf_address_point' % schema
        table = '%s.%s' % (schema, pubic)

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT b.* FROM %s a, %s b WHERE a.address_point_id = b.address_point_id)' %(mt.table, ref, pubic))
        mt.add(self._index_sql(table, 'address_point_id'))

        mt.ref(ref)

        return mt

    def get_mt_rdf_address_micropoint(self):
        """-->rdf_address_micropoint"""
        schema = self.district.format_schema()
        pubic = 'rdf_address_micropoint'
        ref = '%s.rdf_address_point' % schema
        table = '%s.%s' % (schema, pubic)

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT b.* FROM %s a, %s b WHERE a.address_point_id = b.address_point_id)' %(mt.table, ref, pubic))
        mt.add(self._index_sql(table, 'address_point_id'))
        mt.add(self._index_sql(table, 'address_mpoint_id'))

        mt.ref(ref)

        return mt

    def get_mt_rdf_address_mpoint_trans(self):
        """-->rdf_address_micropoint"""
        schema = self.district.format_schema()
        pubic = 'rdf_address_mpoint_trans'
        ref = '%s.rdf_address_micropoint' % schema
        table = '%s.%s' % (schema, pubic)

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT b.* FROM %s a, %s b WHERE a.address_mpoint_id = b.address_mpoint_id)' %(mt.table, ref, pubic))
        mt.add(self._index_sql(table, 'address_mpoint_id'))

        mt.ref(ref)

        return mt

    def get_mt_rdf_postal_area(self):
        """-->rdf_address_point"""
        schema = self.district.format_schema()
        pubic = 'rdf_postal_area'
        ref = '%s.rdf_link' % schema
        table = '%s.%s' % (schema, pubic)

        mt = SqlMeta(table)
        sqls = []
        sqls.append('SELECT DISTINCT b.* FROM %s a, %s b WHERE a.left_postal_area_id = b.postal_area_id' %(ref, pubic))
        sqls.append('SELECT DISTINCT b.* FROM %s a, %s b WHERE a.right_postal_area_id = b.postal_area_id' %(ref, pubic))
        mt.add('CREATE TABLE %s AS (%s)' %(mt.table, ' UNION '.join(sqls)))
        mt.add(self._pk_sql(table, 'postal_area_id'))

        mt.ref(ref)

        return mt

    def get_mt_rdf_postal_code_midpoint(self):
        """-->rdf_postal_code_midpoint"""
        schema = self.district.format_schema()
        pubic = 'rdf_postal_code_midpoint'
        ref = '%s.rdf_link' % schema
        table = '%s.%s' % (schema, pubic)

        mt = SqlMeta(table)
        sqls = []
        sqls.append('SELECT DISTINCT b.* FROM %s a, %s b WHERE a.link_id = b.link_id' %(ref, pubic))
        mt.add('CREATE TABLE %s AS (%s)' %(mt.table, ' UNION '.join(sqls)))
        mt.add(self._index_sql(table, 'link_id'))

        mt.ref(ref)

        return mt

    """BUILDING"""
    def get_mt_rdf_building_face(self):
        """-->rdf_face"""
        schema = self.district.format_schema()

        ref = '%s.rdf_face' % schema
        table = '%s.rdf_building_face' % schema

        mt = SqlMeta(table)
        sql = 'CREATE TABLE %s AS (SELECT b.* FROM %s a, rdf_building_face b WHERE a.face_id = b.face_id)' %(mt.table, ref)
        mt.add(sql)

        mt.add(self._index_sql(mt.table, 'building_id'))
        mt.add(self._index_sql(mt.table, 'face_id'))

        mt.ref(ref)

        return mt

    def get_mt_rdf_building(self):
        """-->rdf_building_face"""
        schema = self.district.format_schema()

        ref = '%s.rdf_building_face' % schema
        table = '%s.rdf_building' % schema

        mt = SqlMeta(table)
        sql = 'CREATE TABLE %s AS (SELECT b.* FROM %s a, rdf_building b WHERE a.building_id = b.building_id)' %(mt.table, ref)
        mt.add(sql)

        mt.add(self._index_sql(mt.table, 'building_id'))

        mt.ref(ref)

        return mt

    def get_mt_rdf_building_enh_feature(self):
        """-->rdf_building"""
        schema = self.district.format_schema()

        ref = '%s.rdf_building' % schema
        table = '%s.rdf_building_enh_feature' % schema

        mt = SqlMeta(table)
        sql = 'CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_building_enh_feature b WHERE a.building_id = b.building_id)' %(mt.table, ref)
        mt.add(sql)

        mt.add(self._index_sql(mt.table, 'building_id'))

        mt.ref(ref)

        return mt

    def get_mt_wkt_building(self):
        """-->rdf_building"""
        schema = self.district.format_schema()

        ref = '%s.rdf_building' % schema
        table = '%s.wkt_building' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT b.* FROM %s a, wkt_building b WHERE a.building_id = b.building_id)' %(mt.table, ref))
        mt.add(self._pk_sql(mt.table, 'building_id'))

        mt.ref(ref)

        return mt

    def get_mt_rdf_sign_origin(self):
        """-->rdf_nav_link"""
        schema = self.district.format_schema()

        ref = '%s.rdf_nav_link' % schema
        table = '%s.rdf_sign_origin' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_sign_origin b WHERE a.link_id = b.originating_link_id)' %(mt.table, ref))
        mt.add(self._pk_sql(mt.table, 'sign_id'))

        mt.ref(ref)

        return mt

    def get_mt_rdf_sign_destination(self):
        """-->rdf_sign_origin"""
        schema = self.district.format_schema()

        ref = '%s.rdf_sign_origin' % schema
        table = '%s.rdf_sign_destination' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_sign_destination b WHERE a.sign_id = b.sign_id)' %(mt.table, ref))
        mt.add(self._index_sql(mt.table, 'sign_id'))

        mt.ref(ref)

        return mt

    def get_mt_rdf_sign_destination_trans(self):
        """-->rdf_sign_origin"""
        schema = self.district.format_schema()

        ref = '%s.rdf_sign_origin' % schema
        table = '%s.rdf_sign_destination_trans' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_sign_destination_trans b WHERE a.sign_id = b.sign_id)' %(mt.table, ref))
        mt.add(self._index_sql(mt.table, 'sign_id'))

        mt.ref(ref)

        return mt

    def get_mt_rdf_sign_element(self):
        """-->rdf_sign_origin"""
        schema = self.district.format_schema()

        ref = '%s.rdf_sign_origin' % schema
        table = '%s.rdf_sign_element' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_sign_element b WHERE a.sign_id = b.sign_id)' %(mt.table, ref))
        mt.add(self._pk_sql(mt.table, 'sign_element_id'))
        mt.add(self._index_sql(mt.table, 'sign_id'))

        mt.ref(ref)

        return mt

    def get_mt_rdf_sign_element_trans(self):
        """-->rdf_sign_element"""
        schema = self.district.format_schema()

        ref = '%s.rdf_sign_element' % schema
        table = '%s.rdf_sign_element_trans' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, rdf_sign_element_trans b WHERE a.sign_element_id = b.sign_element_id)' %(mt.table, ref))
        mt.add(self._pk_sql(mt.table, 'sign_element_id'))

        mt.ref(ref)

        return mt

    def get_mt_rdf_virtual_connection(self):
        """-->rdf_node"""
        schema = self.district.format_schema()

        ref = '%s.rdf_node' % schema
        table = '%s.rdf_virtual_connection' % schema

        mt = SqlMeta(table)

        mt.add('CREATE TABLE %s AS (SELECT a.* FROM rdf_virtual_connection a, %s b WHERE a.ref_node_id = b.node_id)' % (table, ref))
        mt.add(self._index_sql(mt.table, 'ref_node_id'))
        mt.add(self._index_sql(mt.table, 'nonref_node_id'))
        mt.ref(ref)

        return mt

    def get_mt_vce_sign_destination(self):
        """-->rdf_sign_destination"""
        schema = self.district.format_schema()

        ref = '%s.rdf_sign_destination' % schema
        table = '%s.vce_sign_destination' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, vce_sign_destination b WHERE a.sign_id = b.sign_id)' %(mt.table, ref))
        mt.add(self._index_sql(mt.table, 'sign_id'))

        mt.ref(ref)

        return mt

    def get_mt_vce_sign_element(self):
        """-->rdf_sign_element"""
        schema = self.district.format_schema()

        ref = '%s.rdf_sign_element' % schema
        table = '%s.vce_sign_element' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, vce_sign_element b WHERE a.sign_element_id = b.sign_element_id)' %(mt.table, ref))
        mt.add(self._index_sql(mt.table, 'sign_element_id'))

        mt.ref(ref)

        return mt

    def get_mt_usr_gs_node(self):
        """--->rdf_node"""
        schema = self.district.format_schema()

        ref = '%s.rdf_node' % schema
        table = '%s.usr_gs_node' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (select A.* from %s A, (select * from %s where zlevel != 0 ) as B where A.lat = B.lat and A.lon = B.lon)' %(mt.table, ref, ref))
        mt.add(self._index_sql(mt.table, 'node_id'))

        mt.ref(ref)

        return mt

    def get_mt_usr_node_link(self):
        """--->usr_gs_node"""
        schema = self.district.format_schema()

        ref = '%s.usr_gs_node' % schema
        table = '%s.usr_node_link' % schema

        sqls = []
        sqls.append('SELECT DISTINCT A.*, B.link_id, B.ref_node_id, B.nonref_node_id, C.link_id AS nav_link_id FROM %s A JOIN rdf_link B ON A.node_id = B.ref_node_id LEFT JOIN rdf_nav_link C ON (B.link_id = C.link_id)' % (ref))
        sqls.append('SELECT DISTINCT A.*, B.link_id, B.ref_node_id, B.nonref_node_id, C.link_id AS nav_link_id FROM %s A JOIN rdf_link B ON A.node_id = B.nonref_node_id LEFT JOIN rdf_nav_link C ON (B.link_id = C.link_id)' % (ref))
        mt = SqlMeta(table)

        mt.add('CREATE TABLE %s AS (%s)' %(mt.table, ' UNION '.join(sqls)))

        mt.ref(ref)

        return mt

    def get_mt_bdr_face(self):
        """--->rdf_face_link, rdf_link"""
        schema = self.district.format_schema()

        ref = '%s.rdf_face_link' % schema
        ref2 = '%s.rdf_link' % schema
        table = '%s.bdr_face' % schema

        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT face_id FROM (SELECT A.face_id, B.link_id FROM %s A LEFT JOIN %s B ON (A.link_id = B.link_id)) AS T WHERE T.link_id IS NULL)' %(table, ref, ref2))

        mt.add(self._index_sql(mt.table, 'face_id'))

        mt.ref(ref)
        mt.ref(ref2)

        return mt

    def get_mt_usr_ref_link(self):
        """--->rdf_nav_strand, usr_node_link, adas_node_curvature"""
        schema = self.district.format_schema()
        if self.district.order == 0:
            usr_link = USR_LINK_COUNTRY
        elif self.district.order == 1:
            usr_link = USR_LINK_ORDER1

        rdf_link = '%s.rdf_link' % schema
        rdf_nav_strand = '%s.rdf_nav_strand' % schema
        usr_node_link = '%s.usr_node_link' % schema
        adas_node_curvature = '%s.adas_node_curvature' % schema
        adas_node_slope = '%s.adas_node_slope' % schema
        rdf_sign_origin = '%s.rdf_sign_origin' % schema
        rdf_sign_destination = '%s.rdf_sign_destination' % schema

        sqls = []
        sqls.append('SELECT link_id FROM %s EXCEPT SELECT link_id FROM %s' % (rdf_nav_strand, rdf_link))
        sqls.append('SELECT link_id FROM %s EXCEPT SELECT link_id FROM %s' % (usr_node_link, rdf_link))
        sqls.append('SELECT from_link_id AS link_id FROM %s EXCEPT SELECT link_id FROM %s' % (adas_node_curvature, rdf_link))
        sqls.append('SELECT to_link_id AS link_id FROM %s EXCEPT SELECT link_id FROM %s' % (adas_node_curvature, rdf_link))
        sqls.append('SELECT to_link_id AS link_id FROM %s EXCEPT SELECT link_id FROM %s' % (adas_node_slope, rdf_link))
        sqls.append('SELECT originating_link_id AS link_id FROM %s EXCEPT SELECT link_id FROM %s' % (rdf_sign_origin, rdf_link))
        sqls.append('SELECT dest_link_id AS link_id FROM %s EXCEPT SELECT link_id FROM %s' % (rdf_sign_destination, rdf_link))

        ref_link_sql = ' UNION '.join(sqls)

        table = '%s.usr_ref_link' % schema
        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT DISTINCT l.* FROM (%s) AS t, rdf_link l, %s ul WHERE t.link_id = l.link_id and t.link_id = ul.link_id)' %(table, ref_link_sql,usr_link))

        mt.add(self._index_sql(mt.table, 'link_id'))

        mt.ref(rdf_link)
        mt.ref(rdf_nav_strand)
        mt.ref(usr_node_link)
        mt.ref(adas_node_curvature)
        mt.ref(adas_node_slope)
        mt.ref(rdf_sign_origin)
        mt.ref(rdf_sign_destination)
        mt.ref(usr_link)

        return mt

    def get_mt_usr_invalid_link(self):
        """
            The nav link id that is remove because of premium data
        """
        schema = self.district.format_schema()

        rdf_nav_link = '%s.rdf_nav_link' % schema
        rdf_link = '%s.rdf_link' % schema
        rdf_nav_strand = '%s.rdf_nav_strand' % schema
        usr_node_link = '%s.usr_node_link' % schema
        adas_node_curvature = '%s.adas_node_curvature' % schema
        adas_node_slope = '%s.adas_node_slope' % schema
        rdf_sign_origin = '%s.rdf_sign_origin' % schema
        rdf_sign_destination = '%s.rdf_sign_destination' % schema

        sqls = []
        sqls.append('SELECT link_id FROM %s' % rdf_nav_strand)
        sqls.append('SELECT nav_link_id as link_id FROM %s WHERE nav_link_id IS NOT NULL' % usr_node_link)
        sqls.append('SELECT from_link_id AS link_id FROM %s' % adas_node_curvature)
        sqls.append('SELECT to_link_id AS link_id FROM %s' % adas_node_curvature)
        sqls.append('SELECT to_link_id AS link_id FROM %s' % adas_node_slope)
        sqls.append('SELECT originating_link_id AS link_id FROM %s' % rdf_sign_origin)
        sqls.append('SELECT dest_link_id AS link_id FROM %s' % rdf_sign_destination)


        table = '%s.usr_invalid_link' % schema
        mt = SqlMeta(table)
        mt.add('CREATE TABLE %s AS (SELECT link_id FROM %s INTERSECT (%s) EXCEPT SELECT link_id FROM %s) ' %(table, rdf_link, ' UNION '.join(sqls), rdf_nav_link))
        mt.add(self._pk_sql(table, 'link_id'))

        mt.ref(rdf_nav_link)
        mt.ref(rdf_link)
        mt.ref(rdf_nav_strand)
        mt.ref(usr_node_link)
        mt.ref(adas_node_curvature)
        mt.ref(adas_node_slope)
        mt.ref(rdf_sign_origin)
        mt.ref(rdf_sign_destination)

        return mt

##    def get_mt_rdf_sign_origin(self):
##        """-->rdf_nav_link"""
##        schema = self.district.format_schema()
##
##        ref = '%s.rdf_nav_link' % schema
##        table = '%s.rdf_sign_origin' % schema
##
##        mt = SqlMeta(table)
##        mt.add('CREATE TABLE %s AS (SELECT DISTINCT b.* FROM %s a, wkt_building b WHERE a.link_id = b.originating_link_id)' %(mt.table, ref))
##        mt.add(self._pk_sql(mt.table, 'sign_id'))
##
##        mt.ref(ref)
##
##        return mt

    def _index_sql(self, table, column):
        index = self._index_name(table, column)
        sql = 'CREATE INDEX %s ON %s(%s)' %(index, table, column)

        return sql

    def _pk_sql(self, table, column):
        pk_name = self._pk_name(table, column)
        sql = 'ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY(%s)' %(table, pk_name, column)
        return sql

    def _index_name(self, table, column):
        table = table.split('.')[-1] # strip schema name
        return 'idx_%s_%s' %(table.replace('_', ''), column.replace('_', ''))

    def _pk_name(self, table, column):
        table = table.split('.')[-1] # strip schema name
        return 'pk_%s' %(table.replace('_', ''))


def divide(arg):
    options, district = arg

    divider = TableDivider(options)
    divider._create(district)

    return district, True, divider

class TableDivider(object):
    """
        class comment
    """

    def __init__(self, options):
        self.options = options
        #self.db_args = db_args
        self.conn = None
        self.cursor = None

        self.usr_tables = set()

        self.schemas = set()
        self.tables = set()
        self.indexes = set()

        self.districts = []

        self.mt_map = {}

        self.post_metas = []

        region = self._get_region_name(options.dbname)
        if not region:
            print 'Warning: can not get region name from %s ' % options.dbname
            print '\tDatabase name format: XX_YYZZZZ_OO or XX_YY_ZZZZ_OO (eg. HERE_NA15Q1_1)'
            print '\tXX   : Data vendor'
            print '\tYY   : Region'
            print '\tZZZZ : Data version, for example 15Q1'
            print '\tOO   : Other information'

            return

        self.cfg_parser = CfgParser('rdf_divide.cfg', region)

        self.get_post_process_method()

        self.init()

    def get_post_process_method(self):
        import inspect
        for member in inspect.getmembers(self):
            name, method = member
            if not name.startswith('post_process'):
                continue
            # print mt
            self.post_metas.append(name)
        self.post_metas.sort()

    def get_post_metas(self):
        return self.post_metas

    def init(self):
        options = self.options
        db_args = "host=%s port=%s user=%s password=%s dbname=%s" % (options.host, options.port, options.user, options.passwd, options.dbname)
        try:
            self.conn = psycopg2.connect(db_args)
            self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        except Error,e:
            sys.stderr.write(e.__str__())
            sys.exit(-2)
        except Exception, e:
            print e
            sys.exit(-3)

        self._get_existing_tables()
        self._get_existing_indexes()
        self._get_existing_schemas()

    def _print_division_info(self):
        print 'COUNTRY LIST: ', self.cfg_parser.countries
        print 'ORDER0 BASED DB: ', self.cfg_parser.countries_div_order0
        print 'ORDER1 BASED DB: ', self.cfg_parser.countries_div_order1

    def _preprocess(self):
        extensions  = ['postgis', 'hstore']
        sqls = ['CREATE EXTENSION IF NOT EXISTS %s' % ext for ext in extensions]

        self._exe_sql(sqls)

    def create(self):
        self._print_division_info()

        # 2015.1.23 add by lgwu@telenav.cn, add postgis extensions
        self._preprocess()

        districts = self._get_districts()
        self._create_usr_tables(districts)

        #for district in districts:
        #    self._create(district)

        options = [self.options for d in districts]
        self._create_public_index()
        #procs = max(1, multiprocessing.cpu_count()/2)
        procs = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(processes = 8)

        result = pool.map(divide, zip(options, districts))

        for r in result:
            print r

    def remove_schemas(self):
        districts = self._get_districts()

        schemas = [d.format_schema() for d in districts]
        schemas.extend(['usr', 'ref'])

        for schema in schemas:
            sqls = []
            sql = 'DROP SCHEMA IF EXISTS %s CASCADE' % schema

            sqls.append(sql)

            self._exe_sql(sqls)

    def clean_table(self, table):
        districts = self._get_districts()
        self._create_usr_tables(districts)

        for district in districts:
            schema = district.format_schema()
            if not self._table_exists(schema, table):
                continue

            col = MetaCollection(district)
            mts = col.get_metas()
            fulltable = '%s.%s' %(schema, table)
            tables = self._build_dependency(fulltable, mts)
            tables.add(fulltable)

            sqls = []
            for t in tables:
                sql = 'DROP TABLE IF EXISTS %s' % t
                sqls.append(sql)

            self._exe_sql(sqls)

    def generate_schemas(self, schema_cfg):
        with open(schema_cfg, 'w') as ofs:
            districts = self._get_districts()
            for district in districts:
                schema = district.format_schema()
                if not schema: continue

                ic = schema[:3].upper()

                ofs.write('%s,%s\n' % (ic, schema))

    def modify_database(self):
        # XXX, Town of --> Town of XXX
        # XXX, Village of --> Village of XXX

        districts = self._get_districts()
        schemas = [d.format_schema() for d in districts]
        schemas.append('public')

        sql_templates = self._get_sql_templates(self.cfg_parser.region)

        sqls = [sql_tmp.replace('%s', schema) for schema in schemas for sql_tmp in sql_templates]
        if not sqls:
            return True

        r = self._exe_sql(sqls)
        return r

    def _get_sql_templates(self, region):
        imp = {'NA': self._get_sql_templates_NA,
                   }

        if region in imp:
            return imp[region]()
        else:
            return []

    def _get_sql_templates_NA(self):
        sqls = []

        sqls.append("UPDATE %s.rdf_feature_name n SET name = regexp_replace(name, '(^.+), Town of', 'Town of \\1') WHERE n.name similar to '%, Town of'")
        sqls.append("UPDATE %s.rdf_city_poi_name n SET name = regexp_replace(name, '(^.+), Town of', 'Town of \\1') WHERE n.name similar to '%, Town of'")

        sqls.append("UPDATE %s.rdf_feature_name n SET name = regexp_replace(name, '(^.+), Village of', 'Village of \\1') WHERE n.name similar to '%, Village of'")
        sqls.append("UPDATE %s.rdf_city_poi_name n SET name = regexp_replace(name, '(^.+), Village of', 'Village of \\1') WHERE n.name similar to '%, Village of'")

        return sqls

    def _build_dependency(self, ref_table, mts):
        all_tables = set()
        tables = set()
        for mt in mts:
            if ref_table in mt.refs:
                tables.add(mt.table)

        if not tables:
            return set()

        all_tables.update(tables)

        for table in tables:
            all_tables.update(self._build_dependency(table, mts))

        return all_tables

    def _create_usr_tables(self, districts):
        self._create_schema_imp('usr')

        self._create_usr_table_link_country()
        self._create_usr_table_link_order1()
        self._create_usr_table_node_country()
        self._create_usr_table_node_order1()
        self._create_usr_table_carto_country()
        self._create_usr_table_carto_order1()
        self._create_usr_table_carto()
##        self._create_usr_table_border_carto_country()
##        self._create_usr_table_border_carto_order1()
        self._create_usr_table_building_country()
        self._create_usr_table_building_order1()
        self._create_usr_table_building()
##        self._create_usr_table_border_building_country()
##        self._create_usr_table_border_building_order1()
##        self._create_usr_table_zone_country()
##        self._create_usr_table_zone_order1()
        self._create_usr_table_link_in_carto()
        self._create_usr_table_link_geom_in_carto()
        self._create_usr_table_zip_center()
        self._create_usr_table_zip_link_count()

    def _create_usr_table_zip_center(self):
        mt = SqlMeta('usr.usr_zip_center')
        sql = "CREATE TABLE %s AS (select pa.*,st_astext(center) centroid from (select pid ,ST_ClosestPoint(ST_Collect(st_geomfromtext(link)),st_centroid(ST_ConvexHull(ST_Collect(st_geomfromtext(link))))) center from rdf_nav_link b, wkt_link c,(select left_postal_area_id pid, link_id from rdf_link where left_postal_area_id is not null union select right_postal_area_id pid, link_id from rdf_link where right_postal_area_id is not null ) as u where u.link_id=b.link_id and b.link_id=c.link_id and b.boat_ferry='N' and b.rail_ferry='N' group by pid) as res, rdf_postal_area pa where res.pid=pa.postal_area_id)" % (mt.table,)
        mt.add(sql)

        self._create_table(mt)

    def _create_usr_table_zip_link_count(self):
        mt = SqlMeta('usr.usr_zip_link_count')
        sql = "CREATE TABLE %s AS (select pid,aid,count(tup.link_id) from (select left_postal_area_id pid, left_admin_place_id aid,link_id from rdf_link where left_postal_area_id is not null and left_postal_area_id=right_postal_area_id union select right_postal_area_id pid, right_admin_place_id aid,link_id from rdf_link where right_postal_area_id is not null and left_postal_area_id=right_postal_area_id ) as tup, rdf_nav_link b where tup.link_id=b.link_id and b.boat_ferry='N' and b.rail_ferry='N' group by pid,aid)" % (mt.table,)
        mt.add(sql)

        self._create_table(mt)

    def _create_usr_table_link_country(self):
        countries = self.cfg_parser.countries_div_order0
        if not countries:
            # create the table even it's empty
            mt = SqlMeta('usr.usr_link_country')
            sql = "CREATE TABLE %s (link_id bigint,country_id bigint)" % (mt.table)
            mt.add(sql)
            self._create_table(mt)
            return

        sql_countries = ','.join(["'%s'" %i for i in countries])
        mt = SqlMeta('usr.usr_link_country')
        sql = "CREATE TABLE %s AS (SELECT DISTINCT link_id, country_id FROM rdf_link, rdf_admin_hierarchy WHERE (left_admin_place_id = admin_place_id or right_admin_place_id = admin_place_id) AND iso_country_code IN (%s))" % (mt.table, sql_countries)
        mt.add(sql)

        mt.add(self._index_sql(mt.table, 'link_id'))
        mt.add(self._index_sql(mt.table, 'country_id'))

        mt.add('CLUSTER %s USING %s' %(mt.table, self._index_name(mt.table, 'country_id')))

        self._create_table(mt)

    def _create_usr_table_node_country(self):
        mt = SqlMeta('usr.usr_node_country')
        sqls = []
        sql = "SELECT DISTINCT ref_node_id as node_id, country_id FROM usr.usr_link_country a, rdf_link b WHERE a.link_id=b.link_id"
        sqls.append(sql)
        sql = "SELECT DISTINCT nonref_node_id as node_id, country_id FROM usr.usr_link_country a, rdf_link b WHERE a.link_id=b.link_id"
        sqls.append(sql)

        sql = 'CREATE TABLE %s AS (%s)' % (mt.table, ' UNION '.join(sqls))

        mt.add(sql)

        mt.add(self._index_sql(mt.table, 'node_id'))
        mt.add(self._index_sql(mt.table, 'country_id'))

        mt.add('CLUSTER %s USING %s' %(mt.table, self._index_name(mt.table, 'country_id')))

        self._create_table(mt)

    def _create_usr_table_carto_country(self):
        mt = SqlMeta('usr.usr_carto_country')
        sqls = []
        sql = "SELECT DISTINCT b.carto_id, country_id FROM rdf_face_link a,rdf_carto_face b,usr.usr_link_country c, rdf_carto d where a.face_id=b.face_id and a.link_id=c.link_id and b.carto_id = d.carto_id and d.feature_type != 500116"
        sqls.append(sql)
        sql = "SELECT DISTINCT b.carto_id, -1 as country_id FROM rdf_face_link a,rdf_carto_face b,rdf_carto c where a.face_id=b.face_id and b.carto_id = c.carto_id and c.feature_type = 500116"
        sqls.append(sql)
        sql = "SELECT DISTINCT carto_id, country_id FROM rdf_carto_link b,usr.usr_link_country c where b.link_id=c.link_id"
        sqls.append(sql)

        sql = 'CREATE TABLE %s AS (%s)' % (mt.table, ' UNION '.join(sqls))

        mt.add(sql)

        mt.add(self._index_sql(mt.table, 'carto_id'))
        mt.add(self._index_sql(mt.table, 'country_id'))

        mt.add('CLUSTER %s USING %s' %(mt.table, self._index_name(mt.table, 'country_id')))

        self._create_table(mt)

    def _create_usr_table_link_in_carto(self):
        mt = SqlMeta('usr.usr_link_in_carto')
        sqls = []
        sql = "select temp.link_id,z.link_id as nav_link_id from (SELECT DISTINCT a.link_id  FROM rdf_face_link a,rdf_carto_face b,usr.usr_carto_order1 c where a.face_id=b.face_id and b.carto_id=c.carto_id) as temp left join rdf_nav_link z on temp.link_id=z.link_id"
        sqls.append(sql)
        sql = "select temp.link_id,z.link_id as nav_link_id from (SELECT DISTINCT a.link_id  FROM rdf_face_link a,rdf_carto_face b,usr.usr_carto_country c where a.face_id=b.face_id and b.carto_id=c.carto_id) as temp left join rdf_nav_link z on temp.link_id=z.link_id"
        sqls.append(sql)
        sql = "select temp.link_id,z.link_id as nav_link_id from (SELECT DISTINCT b.link_id  FROM rdf_carto_link b,usr.usr_carto_order1 c where b.carto_id=c.carto_id) as temp left join rdf_nav_link z on temp.link_id=z.link_id"
        sqls.append(sql)
        sql = "select temp.link_id,z.link_id as nav_link_id from (SELECT DISTINCT b.link_id  FROM rdf_carto_link b,usr.usr_carto_country c where b.carto_id=c.carto_id) as temp left join rdf_nav_link z on temp.link_id=z.link_id"
        sqls.append(sql)
        sql = "select temp.link_id,z.link_id as nav_link_id from (SELECT DISTINCT a.link_id  FROM rdf_face_link a,rdf_building_face b,usr.usr_building_order1 c where a.face_id=b.face_id and b.building_id=c.building_id) as temp left join rdf_nav_link z on temp.link_id=z.link_id"
        sqls.append(sql)
        sql = "select temp.link_id,z.link_id as nav_link_id from (SELECT DISTINCT a.link_id  FROM rdf_face_link a,rdf_building_face b,usr.usr_building_country c where a.face_id=b.face_id and b.building_id=c.building_id) as temp left join rdf_nav_link z on temp.link_id=z.link_id"
        sqls.append(sql)

        sql = 'CREATE TABLE %s AS (%s)' % (mt.table, ' UNION '.join(sqls))

        mt.add(sql)

        mt.add(self._index_sql(mt.table, 'link_id'))

        self._create_table(mt)

    def _create_usr_table_link_geom_in_carto(self):
        mt = SqlMeta('usr.usr_link_geom_in_carto')
        sql = "select b.* from usr.usr_link_in_carto a ,rdf_link_geometry b where a.link_id=b.link_id"

        sql = 'CREATE TABLE %s AS (%s)' % (mt.table, sql)

        mt.add(sql)

        mt.add(self._index_sql(mt.table, 'link_id'))

        self._create_table(mt)

##    def _create_usr_table_border_carto_country(self):
##        mt = SqlMeta('usr.usr_border_carto_country')
##        sql = "select * from usr.usr_carto_country where carto_id in (SELECT distinct carto_id from usr.usr_carto_country group by carto_id having count(*)>1)"
##
##        sql = 'CREATE TABLE %s AS (%s)' % (mt.table, sql)
##
##        mt.add(sql)
##
##        mt.add(self._index_sql(mt.table, 'carto_id'))
##        mt.add(self._index_sql(mt.table, 'country_id'))
##
##        mt.add('CLUSTER %s USING %s' %(mt.table, self._index_name(mt.table, 'country_id')))
##
##        self._create_table(mt)

    def _create_usr_table_building_country(self):
        mt = SqlMeta('usr.usr_building_country')
        sql = "SELECT DISTINCT building_id, country_id FROM rdf_face_link a,rdf_building_face b,usr.usr_link_country c where a.face_id=b.face_id and a.link_id=c.link_id"

        sql = 'CREATE TABLE %s AS (%s)' % (mt.table, sql)

        mt.add(sql)

        mt.add(self._index_sql(mt.table, 'building_id'))
        mt.add(self._index_sql(mt.table, 'country_id'))

        mt.add('CLUSTER %s USING %s' %(mt.table, self._index_name(mt.table, 'country_id')))

        self._create_table(mt)

##    def _create_usr_table_zone_country(self):
##        mt = SqlMeta('usr.usr_zone_country')
##        sql = "SELECT DISTINCT zone_id, country_id FROM rdf_link_zone a,usr.usr_link_country b where a.link_id=b.link_id"
##
##        sql = 'CREATE TABLE %s AS (%s)' % (mt.table, sql)
##
##        mt.add(sql)
##
##        mt.add(self._index_sql(mt.table, 'zone_id'))
##        mt.add(self._index_sql(mt.table, 'country_id'))
##
##        mt.add('CLUSTER %s USING %s' %(mt.table, self._index_name(mt.table, 'country_id')))
##
##        self._create_table(mt)

##    def _create_usr_table_zone_order1(self):
##        mt = SqlMeta('usr.usr_zone_order1')
##        sql = "SELECT DISTINCT zone_id, order1_id FROM rdf_link_zone a,usr.usr_link_order1 b where a.link_id=b.link_id"
##
##        sql = 'CREATE TABLE %s AS (%s)' % (mt.table, sql)
##
##        mt.add(sql)
##
##        mt.add(self._index_sql(mt.table, 'zone_id'))
##        mt.add(self._index_sql(mt.table, 'order1_id'))
##
##        mt.add('CLUSTER %s USING %s' %(mt.table, self._index_name(mt.table, 'order1_id')))
##
##        self._create_table(mt)

##    def _create_usr_table_border_building_country(self):
##        mt = SqlMeta('usr.usr_border_building_country')
##        sql = "select * from usr.usr_building_country where building_id in (SELECT distinct building_id from usr.usr_building_country group by building_id having count(*)>1)"
##
##        sql = 'CREATE TABLE %s AS (%s)' % (mt.table, sql)
##
##        mt.add(sql)
##
##        mt.add(self._index_sql(mt.table, 'building_id'))
##        mt.add(self._index_sql(mt.table, 'country_id'))
##
##        mt.add('CLUSTER %s USING %s' %(mt.table, self._index_name(mt.table, 'country_id')))
##
##        self._create_table(mt)

    def _create_usr_table_link_order1(self):
        countries = self.cfg_parser.countries_div_order1
        if not countries:
            # create the table even it's empty
            mt = SqlMeta('usr.usr_link_order1')
            sql = 'CREATE TABLE %s (link_id bigint,order1_id bigint)' % (mt.table)
            mt.add(sql)
            self._create_table(mt)
            return

        sql_countries = ','.join(["'%s'" %i for i in countries])

        mt = SqlMeta('usr.usr_link_order1')
        sqls = []
        sql = "SELECT DISTINCT link_id, order1_id FROM rdf_link, rdf_admin_hierarchy WHERE (left_admin_place_id = admin_place_id) AND iso_country_code IN (%s)" % (sql_countries)
        sqls.append(sql)
        sql = "SELECT DISTINCT link_id, order1_id FROM rdf_link, rdf_admin_hierarchy WHERE (right_admin_place_id = admin_place_id) AND iso_country_code IN (%s)" % (sql_countries)
        sqls.append(sql)

        sql = 'CREATE TABLE %s AS (%s)' % (mt.table, ' UNION '.join(sqls))

        mt.add(sql)

        mt.add(self._index_sql(mt.table, 'link_id'))
        mt.add(self._index_sql(mt.table, 'order1_id'))

        mt.add('CLUSTER %s USING %s' %(mt.table, self._index_name(mt.table, 'order1_id')))

        self._create_table(mt)

    def _create_usr_table_node_order1(self):
        mt = SqlMeta('usr.usr_node_order1')
        sqls = []
        sql = "SELECT DISTINCT ref_node_id as node_id, order1_id FROM usr.usr_link_order1 a, rdf_link b WHERE a.link_id=b.link_id"
        sqls.append(sql)
        sql = "SELECT DISTINCT nonref_node_id as node_id, order1_id FROM usr.usr_link_order1 a, rdf_link b WHERE a.link_id=b.link_id"
        sqls.append(sql)

        sql = 'CREATE TABLE %s AS (%s)' % (mt.table, ' UNION '.join(sqls))

        mt.add(sql)

        mt.add(self._index_sql(mt.table, 'node_id'))
        mt.add(self._index_sql(mt.table, 'order1_id'))

        mt.add('CLUSTER %s USING %s' %(mt.table, self._index_name(mt.table, 'order1_id')))

        self._create_table(mt)

    def _create_usr_table_carto_order1(self):
        mt = SqlMeta('usr.usr_carto_order1')
        sqls = []
        sql = "SELECT DISTINCT b.carto_id, order1_id FROM rdf_face_link a,rdf_carto_face b,usr.usr_link_order1 c, rdf_carto d where a.face_id=b.face_id and a.link_id=c.link_id and b.carto_id = d.carto_id and d.feature_type != 500116"
        sqls.append(sql)
        sql = "SELECT DISTINCT b.carto_id, -1 as order1_id FROM rdf_face_link a,rdf_carto_face b, rdf_carto c where a.face_id=b.face_id and b.carto_id = c.carto_id and c.feature_type = 500116"
        sqls.append(sql)
        sql = "SELECT DISTINCT carto_id, order1_id FROM rdf_carto_link b,usr.usr_link_order1 c where b.link_id=c.link_id"
        sqls.append(sql)

        sql = 'CREATE TABLE %s AS (%s)' % (mt.table, ' UNION '.join(sqls))

        mt.add(sql)

        mt.add(self._index_sql(mt.table, 'carto_id'))
        mt.add(self._index_sql(mt.table, 'order1_id'))

        mt.add('CLUSTER %s USING %s' %(mt.table, self._index_name(mt.table, 'order1_id')))

        self._create_table(mt)

##    def _create_usr_table_border_carto_order1(self):
##        mt = SqlMeta('usr.usr_border_carto_order1')
##        sql = "select * from usr.usr_carto_order1 where carto_id in (SELECT distinct carto_id from usr.usr_carto_order1 group by carto_id having count(*)>1)"
##
##        sql = 'CREATE TABLE %s AS (%s)' % (mt.table, sql)
##
##        mt.add(sql)
##
##        mt.add(self._index_sql(mt.table, 'carto_id'))
##        mt.add(self._index_sql(mt.table, 'order1_id'))
##
##        mt.add('CLUSTER %s USING %s' %(mt.table, self._index_name(mt.table, 'order1_id')))
##
##        self._create_table(mt)

    def _create_usr_table_building_order1(self):
        mt = SqlMeta('usr.usr_building_order1')
        sql = "SELECT DISTINCT building_id, order1_id FROM rdf_face_link a,rdf_building_face b,usr.usr_link_order1 c where a.face_id=b.face_id and a.link_id=c.link_id"

        sql = 'CREATE TABLE %s AS (%s)' % (mt.table, sql)

        mt.add(sql)

        mt.add(self._index_sql(mt.table, 'building_id'))
        mt.add(self._index_sql(mt.table, 'order1_id'))

        mt.add('CLUSTER %s USING %s' %(mt.table, self._index_name(mt.table, 'order1_id')))

        self._create_table(mt)

    def _create_usr_table_carto(self):
        mt = SqlMeta('usr.usr_carto')

        sql = """CREATE TABLE %s AS
                 (SELECT carto_id FROM usr.usr_carto_country UNION SELECT carto_id FROM usr.usr_carto_order1)""" % mt.table

        mt.add(sql)
        mt.add(self._index_sql(mt.table, 'carto_id'))

        self._create_table(mt)


    def _create_usr_table_building(self):
        mt = SqlMeta('usr.usr_building')

        sql = """CREATE TABLE %s AS
                 (SELECT building_id FROM usr.usr_building_country UNION SELECT building_id FROM usr.usr_building_order1)""" % mt.table

        mt.add(sql)
        mt.add(self._index_sql(mt.table, 'building_id'))

        self._create_table(mt)

##    def _create_usr_table_border_building_order1(self):
##        mt = SqlMeta('usr.usr_border_building_order1')
##        sql = "select * from usr.usr_building_order1 where building_id in (SELECT distinct building_id from usr.usr_building_order1 group by building_id having count(*)>1)"
##
##        sql = 'CREATE TABLE %s AS (%s)' % (mt.table, sql)
##
##        mt.add(sql)
##
##        mt.add(self._index_sql(mt.table, 'building_id'))
##        mt.add(self._index_sql(mt.table, 'order1_id'))
##
##        mt.add('CLUSTER %s USING %s' %(mt.table, self._index_name(mt.table, 'order1_id')))
##
##        self._create_table(mt)

    def _create(self, district):
        self._create_schema(district)
        self._create_tables(district)
        self._post_process_tables_data(district)
        return True

    #post insert postal code midpoint when link id is missing
    def post_process_postal_code_midpoint(self, district):
        """--->rdf_link"""
        if not self._table_exists('usr', 'gb_postal_code'):
            return
        sqls = []
        schema = district.format_schema()
        tbPcm = '%s.rdf_postal_code_midpoint' % schema
        tbLink = '%s.rdf_link' % schema
        sql = 'INSERT INTO %s (link_id,full_postal_code,lat,lon,geo_level,iso_country_code) \
              SELECT gpc.nt_linkid AS link_id,pcm.full_postal_code,pcm.lat,pcm.lon,pcm.geo_level,pcm.iso_country_code \
              FROM public.rdf_postal_code_midpoint AS pcm \
              JOIN usr.gb_postal_code AS gpc ON pcm.full_postal_code = gpc.post_full \
              JOIN %s AS l ON gpc.nt_linkid = l.link_id  \
              WHERE pcm.link_id is null' % (tbPcm, tbLink)
        sqls.append(sql)
        self._exe_sql(sqls)

    def _post_process_tables_data(self, district):
        mts = self.get_post_metas()
        for mt in mts:
            print mt
            func = getattr(self, mt)
            func(district)

    def _create_schema(self, district):
        schema = district.format_schema()
        if self._schema_exists(schema):
            return

        self._create_schema_imp(schema)

    def _create_schema_imp(self, schema):
        schema = schema.lower()
        if schema in self.schemas:
            return
        sql = 'CREATE SCHEMA %s' % schema
        self._exe_sql([sql])

    def _create_public_index(self):
        if not self._index_exists("public","idx_rdflinkgeometry_linkid"):
            sql = 'CREATE INDEX idx_rdflinkgeometry_linkid ON rdf_link_geometry USING btree (link_id)'
            self._exe_sql([sql])

    def _get_table_creating_mts(self, d):
        col = MetaCollection(d)
        mts = col.get_metas()

        for mt in mts:
            self.mt_map[mt.table] = mt

        return mts

    def _create_tables(self, d):
        mts = self._get_table_creating_mts(d)
        for mt in mts:
            #print mt
            self._create_table(mt)

    def _create_table(self, mt):
        items = mt.table.split('.')
        schema, table = items[0], items[-1]
        if self._table_exists(schema, table):
            return True

        for ref in mt.refs:
            items = ref.split('.')
            ref_schema, ref_table = items[0], items[-1]
            if self._table_exists(ref_schema, ref_table):
                continue
            ref_mt = self.mt_map[ref]
            # dependecy table can't be created
            if not self._create_table(ref_mt):
                return False

        r =  self._exe_sql(mt.sqls)
        if r:
            self.tables.add((schema, table))
            return True
        else:
            return False

    def _get_existing_schemas(self):
        for rec in self.tables:
            self.schemas.add(rec[0])

    def _get_existing_tables(self):
        sql = 'select schemaname,tablename from pg_tables'
        try:
            self.cursor.execute(sql)
            for rec in self.cursor:
                #print rec
                self.tables.add(tuple(rec))
        except Error,e:
            self.conn.rollback()
            sys.stderr.write(e.__str__())
        except:
            self.conn.rollback()

    def _get_existing_indexes(self):
        sql = 'select schemaname,indexname from pg_indexes'
        try:
            self.cursor.execute(sql)
            for rec in self.cursor:
                self.indexes.add(tuple(rec))
        except Error,e:
            self.conn.rollback()
            sys.stderr.write(e.__str__())
        except:
            self.conn.rollback()

    def _exe_sql(self, sqls):
        import time
        r = False
        try:
            for sql in sqls:
                start = time.time()
                #sql = 'EXPLAIN %s' % sql
                print sql
                self.cursor.execute(sql)
                print '%f\t TIME USED!\n' %(time.time() - start)
            self.conn.commit()
            r = True
        except Error,e:
            self.conn.rollback()
            sys.stderr.write(e.__str__())
            print 'EXIT SQL EXCEPTION!'
            sys.exit(-1)
        except:
            self.conn.rollback()
            print 'EXIT EXCEPTION!'
            sys.exit(-1)

        return r

    def _schema_exists(self, schema):
        return schema in self.schemas

    def _table_exists(self, schema, table):
        return (schema,table) in self.tables

    def _index_exists(self, schema, index):
        return (schema,index) in self.indexes

    def _index_sql(self, table, column):
        if not self.conn or not self.cursor:
            return None

        index = self._index_name(table, column)
        sql = 'CREATE INDEX %s on %s(%s)' %(index, table, column)

        return sql

    def _index_name(self, table, column):
        table = table.split('.')[-1] # strip schema name
        return 'idx_%s_%s' %(table.replace('_', ''), column.replace('_', ''))

    def _get_districts(self):
        districts = []

        self._get_districts_imp()
        #print self.districts

        districts =  self.districts
##        mn = District(1, 21000002, 'USA')
##        de = District(1, 21010619, 'USA')
##        ca = District(1, 21009408, 'USA')
##
##        districts.append(mn)
##        districts.append(de)
##        districts.append(ca)

        return districts

    def _get_districts_imp(self):
        try:
            sql = 'SELECT DISTINCT ISO_COUNTRY_CODE, COUNTRY_ID, ORDER1_ID FROM RDF_ADMIN_HIERARCHY'
            self.cursor.execute(sql)
            for rec in self.cursor:
                ic, country_id, order1_id = rec
                if not self.cfg_parser.has_country(ic):
                    continue

                if ic in self.cfg_parser.countries_div_order0:
                    if not country_id: continue
                    district = District(0,country_id, ic)
                    if district in self.districts: continue
                    self.districts.append(district)
                if ic in self.cfg_parser.countries_div_order1:
                    if not order1_id: continue
                    district = District(1,order1_id,ic)
                    if district in self.districts: continue
                    self.districts.append(district)
            r = True
        except Error,e:
            self.conn.rollback()
            sys.stderr.write(e.__str__())
        except:
            self.conn.rollback()


        self.districts.sort()

##        for d in self.districts:
##            print d

##        sys.exit(-1)

    def _drop_table(self, table):
        try:
            self.cursor.execute("drop table if exists %s" % table)
            self.conn.commit()
            r = True
        except Error,e:
            self.conn.rollback()
            sys.stderr.write(e.__str__())
        except:
            self.conn.rollback()

    def _get_region_name(self, db_name):
        import re
        m = re.match('[^_]+_([a-zA-Z]+)', db_name)
        if m:
            return m.group(1).upper()
        else:
            return ''

def check(options):
    if not options.host:
        print 'Error: please specify host!'
        return False
    if not options.port:
        print 'Error: please specify port!'
        return False
    if not options.user:
        print 'Error: please specify user!'
        return False
    if not options.passwd:
        print 'Error: please specify passwd!'
        return False
    if not options.dbname:
        print 'Error: please specify dbname!'
        return False

    return True

def main():
    #print 'START AT %s' % time.asctime()
    s = time.time()
    parser = optparse.OptionParser()

    parser.add_option('-H', '--host', help='hostname', dest='host')
    parser.add_option('-P', '--port', help='port', dest='port', default='5432')
    parser.add_option('-U', '--user', help='user', dest='user', default='postgres')
    parser.add_option('-p', '--passwd', help='password', dest='passwd', default='postgres')
    parser.add_option('-D', '--dbname', help='dbname', dest='dbname')

    parser.add_option('-C', '--cleantable', help='clean table', dest='cleantable')
    parser.add_option('-S', '--schema_file', help='generate divided schemas file', dest='schema_file')

    parser.add_option('-R', '--remove_schemas', action='store_true', help='remove schemea', dest='remove_schemas')
    parser.add_option('-m', '--modify', action='store_true', help='modify database', dest='modify')

    options, args = parser.parse_args()

    if not check(options):
        parser.print_help()
        sys.exit(-1)
##    options.host = '172.16.101.122'
##    #options.host = 'localhost'
##    options.port = '5432'
##    options.user = 'postgres'
##    options.passwd = 'postgres'
##    #options.dbname = 'Navteq_12Q3_MEA_RDF_V2'
##    options.dbname = 'Navteq_12Q1_NA_RDF'

    clean_table = options.cleantable

    divider = TableDivider(options)

    if options.schema_file:
        divider.generate_schemas(options.schema_file)
    elif clean_table:
        divider.clean_table(clean_table)
    elif options.remove_schemas:
        divider.remove_schemas()
    elif options.modify:
        divider.modify_database()
    else:
        divider.create()

    #print 'END AT %s' % time.asctime()
    print '-'*80
    print '%f\tTIME COST IN TOTAL' %(time.time() - s)

if __name__ == '__main__':
    main()
