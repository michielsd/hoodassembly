import pandas as pd
import shapefile
import shapely.geometry
from shapely.geometry.polygon import Polygon
from simpledbf import Dbf5
import psycopg2

#functions
def FindShape(j, dataobject, code, shapes):

    ranginbestand = dataobject[dataobject[code] == j].index.values.astype(int)[0]
    shapeid = shapely.geometry.asShape(shapes.shape(ranginbestand))
    
    return shapeid

#data invoer
gem2018 = Dbf5('gem_2018.dbf').to_dataframe()
wijk2018 = Dbf5('wijk_2018.dbf').to_dataframe()
buurt2018 = Dbf5('buurt2018.dbf').to_dataframe()

gem2018 = gem2018.loc[
    (gem2018['WATER'] == 'NEE')
]
wijk2018 = wijk2018.loc[
    (wijk2018['WATER'] == 'NEE')
]
buurt2018 = buurt2018.loc[
    (buurt2018['WATER'] == 'NEE')
]

gem2018s = shapefile.Reader('gem_2018')
wijk2018s = shapefile.Reader('wijk_2018')
buurt2018s = shapefile.Reader('buurt2018')

gemeenten = []
for gem in gem2018.iloc[:,0]:
    gemeenten.append(gem)

wijken = []
for wijk in wijk2018.iloc[:,0]:
    wijken.append(wijk)

buurten = []
for buurt in buurt2018.iloc[:,0]:
    buurten.append(buurt)

try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

pullgros = """SELECT codering FROM neighbours WHERE nb1 IS NULL"""
cur.execute(pullgros)
groslijst = cur.fetchall()

pullphrase = """SELECT nb1, nb2, nb3, nb4, nb5, nb6, nb7, nb8, nb9, nb10, nb11, nb12, nb13, nb14, nb15, nb16, nb17, nb18, nb19, nb20, nb21, nb22, nb23, nb24, nb25, nb26, nb27, nb28, nb29, nb30, nb31, nb32, nb33, nb34, nb35, nb36, nb37, nb38, nb39, nb40, nb41, nb42, nb43, nb44, nb45 FROM neighbours WHERE codering = %s"""
insertphrase = """UPDATE neighbours SET nb1 = %s, nb2 = %s, nb3 = %s, nb4 = %s, nb5 = %s, nb6 = %s, nb7 = %s, nb8 = %s, nb9 = %s, nb10 = %s, nb11 = %s, nb12 = %s, nb13 = %s, nb14 = %s, nb15= %s, nb16 = %s, nb17 = %s, nb18 = %s, nb19 = %s, nb20 = %s, nb21 = %s, nb22 = %s, nb23 = %s, nb24 = %s, nb25 = %s, nb26 = %s, nb27 = %s, nb28 = %s, nb29 = %s, nb30 = %s, nb31 = %s, nb32 = %s, nb33 = %s, nb34 = %s, nb35 = %s, nb36 = %s, nb37 = %s, nb38 = %s, nb39 = %s, nb40 = %s, nb41 = %s, nb42 = %s, nb43 = %s, nb44 = %s, nb45 = %s WHERE codering = %s"""

#check neighbours
for code in groslijst:
    i = code[0]
    if i.startswith("GM"):
        insertcode = []
        ishape = FindShape(i, gem2018, 'GM_CODE', gem2018s)

        for j in gemeenten:
            jshape = FindShape(j, gem2018, 'GM_CODE', gem2018s)
            if jshape.intersects(ishape) and j != i:
                insertcode.append(j)

    if i.startswith("WK"):
        gmcode = "GM" + i[2:-2]
        sublist = [i[:-2]]
        cur.execute(pullphrase, (gmcode,))
        subtuple = cur.fetchall()
        for sub in subtuple[0]:
            if sub and len(sub) > 1:
                subbed = "WK" + sub[2:]
                sublist.append(subbed)

        insertcode = []
        ishape = FindShape(i, wijk2018, 'WK_CODE', wijk2018s)

        for j in wijken:
            if j[:-2] in sublist:
                jshape = FindShape(j, wijk2018, 'WK_CODE', wijk2018s)
                if jshape.intersects(ishape) and j != i:
                    insertcode.append(j)

    if i.startswith("BU"):
        wkcode = "WK" + i[2:-2]
        sublist = [i[:-2]]
        cur.execute(pullphrase, (wkcode,))
        subtuple = cur.fetchall()
        for sub in subtuple[0]:
            if sub and len(sub) > 1:
                subbed = "BU" + sub[2:]
                sublist.append(subbed)

        insertcode = []
        ishape = FindShape(i, buurt2018, 'BU_CODE', buurt2018s)

        for j in buurten:
                if j[:-2] in sublist:
                    jshape = FindShape(j, buurt2018, 'BU_CODE', buurt2018s)
                    if jshape.intersects(ishape) and j != i:
                        insertcode.append(j)

    
    addition = [""]*(45-len(insertcode))
    insertcode = insertcode + addition + [i]
    print(len(insertcode))
    cur.execute(insertphrase, insertcode)
    conn.commit()
    print("Committed %s" % i)
        

conn.close()