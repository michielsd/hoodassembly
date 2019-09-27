from operator import itemgetter

import psycopg2
import shapefile 
import shapely
from shapely.geometry import Point

#setup database connection
try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

#download list of gm codes
gemeenten = shapefile.Reader("gem_2018")
gemeenteshapes = gemeenten.shapeRecords()

gmcodes = []
for gemeente in gemeenteshapes:
    gmcodes.append(gemeente.record[0])

#download shapefile
wijken = shapefile.Reader("wijk_2018")
wijkshapes = wijken.shapeRecords()

#postgresl comms
stembureaucaller = """SELECT id, bureau, plaats, xc, yc FROM stembureau WHERE gem_code = %s"""
addphrase = """UPDATE stembureau SET wk_code = %s WHERE id = %s"""

#create list of stembureaus

for gemeentecode in gmcodes:
    cur.execute(stembureaucaller, (gemeentecode,))
    stembureautuple = cur.fetchall()
    stembureautabel = []
    for row in stembureautuple:
        stembureautabel.append(list(row))

    for stembureau in stembureautabel:
        postid = stembureau[0]
        stemlocation = Point(float(stembureau[-2]), float(stembureau[-1]))
        print(stembureau[1], stembureau[2])

        for wijk in wijkshapes:
            if wijk.record[2] == gemeentecode:
                poly = shapely.geometry.asShape(wijk.shape)
                if poly.contains(stemlocation):
                    wijkcode = wijk.record[0]
                    print(wijkcode, postid)
                    cur.execute(addphrase, (wijkcode, postid))
                    conn.commit()

cur.close()
conn.close()