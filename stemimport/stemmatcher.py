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
addphrase = """UPDATE stembureau SET gem_code = %s WHERE id = %s"""

#create list of stembureaus
cur.execute(stembureaucaller)
stembureautuple = cur.fetchall()
stembureautabel = []
for row in stembureautuple:
    stembureautabel.append(list(row))

#sort by alphabetical order of town for quick contain check
stembureautabel = sorted(stembureautabel, key=itemgetter(2))

#load shapefile
gemeenten = shapefile.Reader("gem_2018")
gemeenteshapes = gemeenten.shapeRecords()

#setup random first muni to check
hintnext = shapely.geometry.asShape(gemeenteshapes[5].shape)

for stembureau in stembureautabel:
    postid = stembureau[0]
    stemlocation = Point(float(stembureau[-2]), float(stembureau[-1]))
    print(stembureau[1], stembureau[2])
    #check if stembureau is in poly of last
    if hintnext.contains(stemlocation):
        gemeentecode = gemeente.record[0]
        cur.execute(addphrase, (gemeentecode, postid))
        conn.commit()

    #if not, check all others
    else:
        for gemeente in gemeenteshapes:
            poly = shapely.geometry.asShape(gemeente.shape)
            if poly.contains(stemlocation):
                gemeentecode = gemeente.record[0]
                cur.execute(addphrase, (gemeentecode, postid))
                conn.commit()

                hintnext = poly

cur.close()
conn.close()