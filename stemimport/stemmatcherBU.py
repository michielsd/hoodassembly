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
wijken = shapefile.Reader("wijk_2018")
wijkshapes = wijken.shapeRecords()

wkcodes = []
for wijk in wijkshapes:
    wkcodes.append(wijk.record[0])

#download shapefile
buurten = shapefile.Reader("buurt2018")
buurtshapes = buurten.shapeRecords()

#postgresl comms
stembureaucaller = """SELECT id, bureau, plaats, xc, yc FROM stembureau WHERE wk_code = %s AND bu_code IS NULL"""
addphrase = """UPDATE stembureau SET bu_code = %s WHERE id = %s"""

#create list of stembureaus

for wijkcode in wkcodes:
    cur.execute(stembureaucaller, (wijkcode,))
    stembureautuple = cur.fetchall()
    stembureautabel = []
    for row in stembureautuple:
        stembureautabel.append(list(row))
        print(row)

    for stembureau in stembureautabel:
        postid = stembureau[0]
        stemlocation = Point(float(stembureau[-2]), float(stembureau[-1]))
        print(stembureau[1], stembureau[2])

        for buurt in buurtshapes:
            print(buurt.record)
            if buurt.record[2] == buurtcode:
                poly = shapely.geometry.asShape(buurt.shape)
                if poly.contains(stemlocation):
                    buurtcode = buurt.record[0]
                    #cur.execute(addphrase, (buurtcode, postid))
                    #conn.commit()

cur.close()
conn.close()