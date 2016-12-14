#-------------------------------------------------------------------------------
# Name:        constants list
# Purpose:     constants list
#
# Author:      rex
#
# Created:     2016-01-11
# Copyright:   (c) rex 2016
# Licence:     Telenav
#-------------------------------------------------------------------------------
#!/usr/bin/env python
#coding=utf-8

DEV_NULL = "/dev/null"

#common region information
REGION_NA    = 'na'
REGION_EU    = 'eu'
REGION_EUALL = 'eu_all'
REGION_SA    = 'sa'
REGION_ANZ   = 'anz'
REGION_MEA   = 'mea'
REGION_CN    = 'cn'
REGION_SKOR  = 'skor'

#special region
REGION_EU14Q4 = 'eu_14q4'


def REGION_COUNTRY_CODES(region, ftype, subftype = None):
    if REGION_NA == region:
        if ftype in ('nodes_city_center_','nodes_node_','nodes_safety_camera_node_','nodes_zip_center_',
                     'relations_blackspot_','relations_divided_junction_','relations_barrier_','relations_gjv_','relations_junction_view_',
                     'relations_lane_connectivity_','relations_natural_guidance_','relations_restriction_','relations_restriction_','relations_safety_camera_',
                     'relations_barrier_','relations_traffic_sign_','relations_traffic_sign_','relations_truck_maxspeed_','relations_3d_landmark_',
                     'relations_traffic_signals_','relations_bifurcation_','relations_go_straight_','relations_construction_','relations_oneway_','relations_adas:maxspeed_',
                     'relations_adas_node_','relations_zone_',
                     'ways_navlink_', 'relations_multipolygon_','ways_unavlink_','ways_link_','relations_signpost_','relations_virtual_connection_'):
            return "'USA', 'CAN', 'MEX', 'VIR', 'PRI'"
        elif ftype in ('relations_admin_'):
            return "'USA', 'CAN', 'MEX', 'VIR', 'PRI', 'COL'"
    elif REGION_EU == region:
        if ftype in ('nodes_city_center_','nodes_node_','nodes_safety_camera_node_','nodes_zip_center_',
                     'relations_blackspot_','relations_divided_junction_','relations_barrier_','relations_gjv_','relations_junction_view_',
                     'relations_lane_connectivity_','relations_natural_guidance_','relations_restriction_','relations_restriction_','relations_safety_camera_',
                     'relations_barrier_','relations_traffic_sign_','relations_traffic_sign_','relations_truck_maxspeed_','relations_3d_landmark_',
                     'relations_traffic_signals_','relations_bifurcation_','relations_go_straight_','relations_construction_','relations_oneway_','relations_adas:maxspeed_',
                     'relations_adas_node_','relations_zone_',
                     'ways_navlink_', 'relations_multipolygon_','ways_unavlink_','ways_link_','relations_signpost_','relations_admin_','relations_virtual_connection_'):
            return "'ALB', 'AND', 'AUT', 'BEL', 'BGR', 'BIH', 'BLR', 'CHE', 'CZE', 'DEU', 'DNK', 'ESP', 'EST', 'FIN', 'FRA', 'GBR', 'GIB', 'GRC', 'HRV', 'HUN', 'IMN', 'IRL', 'ISL', 'ITA', 'LIE', 'LTU', 'LUX', 'LVA', 'MCO', 'MDA', 'MKD', 'MLT', 'MNE', 'NLD', 'NOR', 'POL', 'PRT', 'ROU', 'RUS', 'SMR', 'SRB', 'SVK', 'SVN', 'SWE', 'TUR', 'UKR', 'VAT', 'FRO', 'GRL', 'BSB', 'CUN', 'NCY', 'CYP', 'KAZ'"
    elif REGION_EU14Q4 == region:
        if ftype in ('nodes_city_center_','nodes_node_','nodes_safety_camera_node_','nodes_zip_center_',
                     'relations_blackspot_','relations_divided_junction_','relations_barrier_','relations_gjv_','relations_junction_view_',
                     'relations_lane_connectivity_','relations_natural_guidance_','relations_restriction_','relations_restriction_','relations_safety_camera_',
                     'relations_barrier_','relations_traffic_sign_','relations_traffic_sign_','relations_truck_maxspeed_','relations_3d_landmark_',
                     'relations_traffic_signals_','relations_bifurcation_','relations_go_straight_','relations_construction_','relations_oneway_','relations_adas:maxspeed_',
                     'relations_adas_node_','relations_zone_',
                     'ways_navlink_', 'ways_unavlink_','ways_link_','relations_signpost_','relations_virtual_connection_'):
            return "'ALB', 'AND', 'AUT', 'BEL', 'BGR', 'BIH', 'BLR', 'CHE', 'CZE', 'DEU', 'DNK', 'ESP', 'EST', 'FIN', 'FRA', 'GBR', 'GIB', 'GRC', 'HRV', 'HUN', 'IMN', 'IRL', 'ISL', 'ITA', 'LIE', 'LTU', 'LUX', 'LVA', 'MCO', 'MDA', 'MKD', 'MLT', 'MNE', 'NLD', 'NOR', 'POL', 'PRT', 'ROU', 'RUS', 'SMR', 'SRB', 'SVK', 'SVN', 'SWE', 'TUR', 'UKR', 'VAT', 'FRO', 'GRL'"
        elif ftype in ('relations_admin_'):
            return "'ALB', 'AND', 'AUT', 'BEL', 'BGR', 'BIH', 'BLR', 'CHE', 'CZE', 'DEU', 'DNK', 'ESP', 'EST', 'FIN', 'FRA', 'GBR', 'GIB', 'GRC', 'HRV', 'HUN', 'IMN', 'IRL', 'ISL', 'ITA', 'LIE', 'LTU', 'LUX', 'LVA', 'MCO', 'MDA', 'MKD', 'MLT', 'MNE', 'NLD', 'NOR', 'POL', 'PRT', 'ROU', 'RUS', 'SMR', 'SRB', 'SVK', 'SVN', 'SWE', 'TUR', 'UKR', 'VAT', 'FRO', 'GRL', 'CYP', 'GEO'"
        elif ftype in ('relations_multipolygon_') and subftype == 'carto':
            return "'ALB', 'AND', 'AUT', 'BEL', 'BGR', 'BIH', 'BLR', 'CHE', 'CZE', 'DEU', 'DNK', 'ESP', 'EST', 'FIN', 'FRA', 'GBR', 'GIB', 'GRC', 'HRV', 'HUN', 'IMN', 'IRL', 'ISL', 'ITA', 'LIE', 'LTU', 'LUX', 'LVA', 'MCO', 'MDA', 'MKD', 'MLT', 'MNE', 'NLD', 'NOR', 'POL', 'PRT', 'ROU', 'RUS', 'SMR', 'SRB', 'SVK', 'SVN', 'SWE', 'TUR', 'UKR', 'VAT', 'FRO', 'GRL', 'CYP'"
    elif REGION_EUALL == region:
        if ftype in ('nodes_city_center_','nodes_node_','nodes_safety_camera_node_','nodes_zip_center_',
                     'relations_blackspot_','relations_divided_junction_','relations_barrier_','relations_gjv_','relations_junction_view_',
                     'relations_lane_connectivity_','relations_natural_guidance_','relations_restriction_','relations_restriction_','relations_safety_camera_',
                     'relations_barrier_','relations_traffic_sign_','relations_traffic_sign_','relations_truck_maxspeed_','relations_3d_landmark_','relations_admin_',
                     'relations_traffic_signals_','relations_bifurcation_','relations_go_straight_','relations_construction_','relations_oneway_','relations_adas:maxspeed_',
                     'relations_adas_node_','relations_zone_',
                     'ways_navlink_', 'relations_multipolygon_','ways_unavlink_','ways_link_'):
            return "'ALB', 'AND', 'AUT', 'BEL', 'BGR', 'BIH', 'BLR', 'CHE', 'CZE', 'DEU', 'DNK', 'ESP', 'EST', 'FIN', 'FRA', 'GBR', 'GIB', 'GRC', 'HRV', 'HUN', 'IMN', 'IRL', 'ISL', 'ITA', 'LIE', 'LTU', 'LUX', 'LVA', 'MCO', 'MDA', 'MKD', 'MLT', 'MNE', 'NLD', 'NOR', 'POL', 'PRT', 'ROU', 'RUS', 'SMR', 'SRB', 'SVK', 'SVN', 'SWE', 'TUR', 'UKR', 'VAT', 'FRO', 'GRL', 'BSB', 'CUN', 'NCY', 'CYP', 'KAZ'"
    elif REGION_SA == region:
        if ftype in ('nodes_city_center_','nodes_node_','nodes_safety_camera_node_','nodes_zip_center_',
                     'relations_blackspot_','relations_divided_junction_','relations_barrier_','relations_gjv_','relations_junction_view_',
                     'relations_lane_connectivity_','relations_natural_guidance_','relations_restriction_','relations_restriction_','relations_safety_camera_',
                     'relations_barrier_','relations_traffic_sign_','relations_traffic_sign_','relations_truck_maxspeed_','relations_3d_landmark_','relations_admin_',
                     'relations_traffic_signals_','relations_bifurcation_','relations_go_straight_','relations_construction_','relations_oneway_','relations_adas:maxspeed_',
                     'relations_adas_node_','relations_zone_',
                     'ways_navlink_', 'relations_multipolygon_','ways_unavlink_','ways_link_','relations_signpost_','relations_virtual_connection_'):
            return "'COL', 'VEN', 'BRA', 'CHL', 'ARG', 'URY', 'PER'"
    elif REGION_ANZ == region:
        if ftype in ('nodes_city_center_','nodes_node_','nodes_safety_camera_node_','nodes_zip_center_',
                     'relations_blackspot_','relations_divided_junction_','relations_barrier_','relations_gjv_','relations_junction_view_',
                     'relations_lane_connectivity_','relations_natural_guidance_','relations_restriction_','relations_restriction_','relations_safety_camera_',
                     'relations_barrier_','relations_traffic_sign_','relations_traffic_sign_','relations_truck_maxspeed_','relations_3d_landmark_','relations_admin_',
                     'relations_traffic_signals_','relations_bifurcation_','relations_go_straight_','relations_construction_','relations_oneway_','relations_adas:maxspeed_',
                     'relations_adas_node_','relations_zone_',
                     'ways_navlink_', 'relations_multipolygon_','ways_unavlink_','ways_link_','relations_signpost_','relations_virtual_connection_'):
            return "'AUS', 'NZL'"
    elif REGION_MEA == region:
        if ftype in ('nodes_city_center_','nodes_node_','nodes_safety_camera_node_','nodes_zip_center_',
                     'relations_blackspot_','relations_divided_junction_','relations_barrier_','relations_gjv_','relations_junction_view_',
                     'relations_lane_connectivity_','relations_natural_guidance_','relations_restriction_','relations_restriction_','relations_safety_camera_',
                     'relations_barrier_','relations_traffic_sign_','relations_traffic_sign_','relations_truck_maxspeed_','relations_3d_landmark_','relations_admin_',
                     'relations_traffic_signals_','relations_bifurcation_','relations_go_straight_','relations_construction_','relations_oneway_','relations_adas:maxspeed_',
                     'relations_adas_node_','relations_zone_',
                     'ways_navlink_', 'relations_multipolygon_','ways_unavlink_','ways_link_','relations_signpost_','relations_virtual_connection_'):
            return "'OMN', 'SAU', 'ZAF', 'BHR', 'EGY', 'LBN', 'JOR', 'ISR', 'KWT', 'QAT', 'ARE'"
    elif REGION_CN == region:
        if ftype in ('nodes_city_center_','nodes_node_','nodes_safety_camera_node_','nodes_zip_center_',
                     'relations_blackspot_','relations_divided_junction_','relations_barrier_','relations_gjv_','relations_junction_view_',
                     'relations_lane_connectivity_','relations_natural_guidance_','relations_restriction_','relations_restriction_','relations_safety_camera_',
                     'relations_barrier_','relations_traffic_sign_','relations_traffic_sign_','relations_truck_maxspeed_','relations_3d_landmark_','relations_admin_',
                     'relations_traffic_signals_','relations_bifurcation_','relations_go_straight_','relations_construction_','relations_oneway_','relations_adas:maxspeed_',
                     'relations_adas_node_','relations_zone_',
                     'ways_navlink_', 'relations_multipolygon_','ways_unavlink_','ways_link_','relations_signpost_','relations_virtual_connection_'):
            return "'CHN'"
    elif REGION_SKOR == region:
        if ftype in ('nodes_city_center_','nodes_node_','nodes_safety_camera_node_','nodes_zip_center_',
                     'relations_blackspot_','relations_divided_junction_','relations_barrier_','relations_gjv_','relations_junction_view_',
                     'relations_lane_connectivity_','relations_natural_guidance_','relations_restriction_','relations_restriction_','relations_safety_camera_',
                     'relations_barrier_','relations_traffic_sign_','relations_traffic_sign_','relations_truck_maxspeed_','relations_3d_landmark_','relations_admin_',
                     'relations_traffic_signals_','relations_bifurcation_','relations_go_straight_','relations_construction_','relations_oneway_','relations_adas:maxspeed_',
                     'relations_adas_node_','relations_zone_',
                     'ways_navlink_', 'relations_multipolygon_','ways_unavlink_','ways_link_','relations_signpost_','relations_virtual_connection_'):
            return "'KOR'"
    else:
        return ""
