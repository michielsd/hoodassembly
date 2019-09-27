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

#postgresl comms
stembureaucaller = """SELECT id, bureau, plaats, xc, yc FROM stembureau WHERE gem_code IS null"""
addphrase = """UPDATE stembureau SET bu_code = %s, wk_code = %s, gem_code = %s WHERE id = %s"""

#create list of stembureaus
cur.execute(stembureaucaller)
stembureautuple = cur.fetchall()
stembureautabel = []
for row in stembureautuple:
    stembureautabel.append(list(row))

#load shapefile
buurten = shapefile.Reader("buurt2018")
buurtshapes = buurten.shapeRecords()

#setup random first muni to check
hintnext = shapely.geometry.asShape(buurtshapes[5].shape)

for stembureau in stembureautabel:
    postid = stembureau[0]
    stemplaats = stembureau[2]
    stemlocation = Point(float(stembureau[-2]), float(stembureau[-1]))
    print(stembureau[1], stembureau[2])

    commitswitch = 1

    #if match with city name
    for buurt in buurtshapes:
        buurtplaats = buurt.record[4]
        if stemplaats == buurtplaats:
            poly = shapely.geometry.asShape(buurt.shape)
            if poly.contains(stemlocation):
                buurtcode = buurt.record[0]
                wijkcode = buurt.record[2]
                gemeentecode = buurt.record[3]
                cur.execute(addphrase, (buurtcode, wijkcode, gemeentecode, postid))
                conn.commit()
                commitswitch = 2
                print("******** Matched and updated: %s, %s ********" % (buurt.record[1], buurt.record[4]))
                break
    #if no match with city name: check last muni code
    if commitswitch == 1:    
        for buurt in buurtshapes:
            buurtgemcode = buurt.record[3]
            if hintnext == buurtgemcode:
                poly = shapely.geometry.asShape(buurt.shape)
                if poly.contains(stemlocation):
                    buurtcode = buurt.record[0]
                    wijkcode = buurt.record[2]
                    gemeentecode = buurt.record[3]
                    cur.execute(addphrase, (buurtcode, wijkcode, gemeentecode, postid))
                    conn.commit()
                    print("******** Matched and updated: %s, %s ********" % (buurt.record[1], buurt.record[4]))
                    break
            else:
                poly = shapely.geometry.asShape(buurt.shape)
                if poly.contains(stemlocation):
                    buurtcode = buurt.record[0]
                    wijkcode = buurt.record[2]
                    gemeentecode = buurt.record[3]
                    cur.execute(addphrase, (buurtcode, wijkcode, gemeentecode, postid))
                    conn.commit()
                    print("******** Matched and updated: %s, %s ********" % (buurt.record[1], buurt.record[4]))
                    break

    hintnext = gemeentecode

cur.close()
conn.close()