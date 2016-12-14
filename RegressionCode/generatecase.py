#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      kuangh
#
# Created:     08/09/2015
# Copyright:   (c) kuangh 2015
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import sys
import os
import psycopg2

RECALCULATE_COUNT = False
NEED_LIMIT_THRESHOLD = 6000

def getrandomid(conn, table, condition, count, needLimit):
    global RECALCULATE_COUNT
    global NEED_LIMIT_THRESHOLD

    cur = conn.cursor()
    querycondition = ''
    if condition != None:
        querycondition = ' where ' + condition

    # 2015.9.19 lgwu@telenav.cn, recalculate count
    if RECALCULATE_COUNT:
        sql = 'select count(*) from %s %s' % (table, querycondition)
        try:
            cur.execute(sql)
        except:
            print 'execute sql failed, sql=[%s]' % sql
            return []
        count = str(cur.fetchone()[0])
    if count >= NEED_LIMIT_THRESHOLD:
        needLimit = True

    limitCondition = ''
    if needLimit:
        limitCondition = ' offset random()*' + count + ' limit 3000'
    sql = 'select id from ' + table + querycondition + limitCondition
    try:
        cur.execute(sql)
    except:
        print 'cannot execute sql'
    rows = cur.fetchall()
    return rows

def createcase(conn, table, type, casefile, count, needLimit):
    f = open(casefile, 'w')
    rows = getrandomid(conn, table, type, count, needLimit)
    for row in rows:
        f.write(str(row[0]) + '\n')
    f.close

def main(hostname, dbuser, dbname, casefile):
    try:
        conn = psycopg2.connect("dbname='" + dbname + "' user='" + dbuser + "' host='" + hostname + "'")
    except:
        print "unable to connect to the database"

    destcasefile = os.path.join(casefile, "ways#tags")
    createcase(conn, 'ways', None, destcasefile, '54997220', True)
    destcasefile = os.path.join(casefile, "ways#tags#hovlink")
    createcase(conn, 'ways', 'tags->\'hov\' = \'designated\'', destcasefile, '4850', False)
    destcasefile = os.path.join(casefile, "ways#tags#navlink")
    createcase(conn, 'ways', 'tags ? \'fc\'', destcasefile, '42395095', True)

    destcasefile = os.path.join(casefile, "nodes#tags")
    createcase(conn, 'nodes', None, destcasefile, '554037141', True)
    destcasefile = os.path.join(casefile, "nodes#tags#addresspoint")
    createcase(conn, 'nodes', 'tags->\'type\' = \'address_point\'', destcasefile, '124207470', True)
    destcasefile = os.path.join(casefile, "nodes#tags#admin_center")
    createcase(conn, 'nodes', 'tags->\'type\' = \'admin_center\'', destcasefile, None, False)
    destcasefile = os.path.join(casefile, "nodes#tags#city_center")
    createcase(conn, 'nodes', 'tags->\'type\' = \'city_center\'', destcasefile, '257106', True)
    destcasefile = os.path.join(casefile, "nodes#tags#natural_guidance_node")
    createcase(conn, 'nodes', 'tags->\'type\' = \'natural_guidance_node\'', destcasefile, '404048', True)
    destcasefile = os.path.join(casefile, "nodes#tags#safety_camera_node")
    createcase(conn, 'nodes', 'tags->\'type\' = \'safety_camera_node\'', destcasefile, None, False)
    destcasefile = os.path.join(casefile, "nodes#tags#zip_center")
    createcase(conn, 'nodes', 'tags->\'type\' = \'zip_center\'', destcasefile, '874122', True)

    destcasefile = os.path.join(casefile, "relations#tags")
    createcase(conn, 'relations', None, destcasefile, '22993232', True)
    destcasefile = os.path.join(casefile, "relations#tags#3dlandmark")
    createcase(conn, 'relations', 'tags->\'type\' = \'3d_landmark\'', destcasefile, '3156', False)
    estcasefile = os.path.join(casefile, "relations#tags#adasmaxspeed")
    createcase(conn, 'relations', 'tags->\'type\' = \'adas:maxspeed\'', destcasefile, '764939', True)
    destcasefile = os.path.join(casefile, "relations#tags#adas_node")
    createcase(conn, 'relations', 'tags->\'type\' = \'adas_node\'', destcasefile, '7612883', True)
    destcasefile = os.path.join(casefile, "relations#tags#admin")
    createcase(conn, 'relations', 'tags->\'type\' = \'admin\'', destcasefile, '219269', True)
    destcasefile = os.path.join(casefile, "relations#tags#barrier")
    createcase(conn, 'relations', 'tags->\'type\' = \'barrier\'', destcasefile, '216114', True)
    destcasefile = os.path.join(casefile, "relations#tags#bifurcation")
    createcase(conn, 'relations', 'tags->\'type\' = \'bifurcation\'', destcasefile, '8558', True)
    destcasefile = os.path.join(casefile, "relations#tags#blackspot")
    createcase(conn, 'relations', 'tags->\'type\' = \'blackspot\'', destcasefile, '25214', True)
    destcasefile = os.path.join(casefile, "relations#tags#construction")
    createcase(conn, 'relations', 'tags->\'type\' = \'construction\'', destcasefile, '2139', False)
    destcasefile = os.path.join(casefile, "relations#tags#divided_junction")
    createcase(conn, 'relations', 'tags->\'type\' = \'divided_junction\'', destcasefile, '146730', True)
    destcasefile = os.path.join(casefile, "relations#tags#gjv")
    createcase(conn, 'relations', 'tags->\'type\' = \'gjv\'', destcasefile, '120654', True)
    destcasefile = os.path.join(casefile, "relations#tags#go_straight")
    createcase(conn, 'relations', 'tags->\'type\' = \'go_straight\'', destcasefile, '2140', False)
    destcasefile = os.path.join(casefile, "relations#tags#grade_separation")
    createcase(conn, 'relations', 'tags->\'type\' = \'grade_separation\'', destcasefile, '213552', True)
    destcasefile = os.path.join(casefile, "relations#tags#junction_view")
    createcase(conn, 'relations', 'tags->\'type\' = \'junction_view\'', destcasefile, '22415', True)
    destcasefile = os.path.join(casefile, "relations#tags#lane_connectivity")
    createcase(conn, 'relations', 'tags->\'type\' = \'lane_connectivity\'', destcasefile, '1472110', True)
    destcasefile = os.path.join(casefile, "relations#tags#multipolygon")
    createcase(conn, 'relations', 'tags->\'type\' = \'multipolygon\'', destcasefile, '4713863', True)
    destcasefile = os.path.join(casefile, "relations#tags#natural_guidance")
    createcase(conn, 'relations', 'tags->\'type\' = \'natural_guidance\'', destcasefile, '120917', True)
    destcasefile = os.path.join(casefile, "relations#tags#oneway")
    createcase(conn, 'relations', 'tags->\'type\' = \'oneway\'', destcasefile, '7815', True)
    destcasefile = os.path.join(casefile, "relations#tags#restriction")
    createcase(conn, 'relations', 'tags->\'type\' = \'restriction\'', destcasefile, '922357', True)
    destcasefile = os.path.join(casefile, "relations#tags#safety_camera")
    createcase(conn, 'relations', 'tags->\'type\' = \'safety_camera\'', destcasefile, '5401', False)
    destcasefile = os.path.join(casefile, "relations#tags#signpost")
    createcase(conn, 'relations', 'tags->\'type\' = \'signpost\'', destcasefile, '258435', True)
    destcasefile = os.path.join(casefile, "relations#tags#traffic_sign")
    createcase(conn, 'relations', 'tags->\'type\' = \'traffic_sign\'', destcasefile, '2033092', True)
    destcasefile = os.path.join(casefile, "relations#tags#traffic_signals")
    createcase(conn, 'relations', 'tags->\'type\' = \'traffic_signals\'', destcasefile, '742702', True)
    destcasefile = os.path.join(casefile, "relations#tags#truck_maxspeed")
    createcase(conn, 'relations', 'tags->\'type\' = \'truck_maxspeed\'', destcasefile, '109144', True)
    destcasefile = os.path.join(casefile, "relations#tags#zone")
    createcase(conn, 'relations', 'tags->\'type\' = \'zone\'', destcasefile, '77207', True)

def printUsage():
    print "generatecase.py -U [db_user] -h [host_name] -d [db_name] -o [output_case_file] <-r>"
    sys.exit(-1)

if __name__ == '__main__':
    len(sys.argv) >= 7 or printUsage()
    dbuser = 'postgres';
    hostname = 'localhost';
    dbname = '';
    casefile = ''

    i = 1
    while i < len(sys.argv):
        if sys.argv[i] == '-U':
            i += 1
            dbuser = sys.argv[i]
        elif sys.argv[i] == '-h':
            i += 1
            hostname = sys.argv[i]
        elif sys.argv[i] == '-d':
            i += 1
            dbname = sys.argv[i]
        elif sys.argv[i] == '-o':
            i += 1
            casefile = sys.argv[i]
        elif sys.argv[i] == '-r':
            RECALCULATE_COUNT = True
        i += 1

    main(hostname, dbuser, dbname, casefile)
