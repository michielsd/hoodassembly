import pandas as pd
import shapefile
import shapely.geometry
from shapely.geometry.polygon import Polygon
from simpledbf import Dbf5
from sqlalchemy import create_engine
import psycopg2

#functions
def FindShape(j, dataobject, code, shapes):


    ranginbestand = dataobject[dataobject[code] == j].index.values.astype(int)[0]
    shapeid = shapely.geometry.asShape(shapes.shape(ranginbestand))
    
    return shapeid

def FindCentroid(j, dataobject, code, shapes):


    ranginbestand = dataobject[dataobject[code] == j].index.values.astype(int)[0]

    loncounter = 0
    latcounter = 0
    posn = 0
    
    for position in shapes.shape(ranginbestand).points:
        posn += 1
        loncounter += position[0]
        latcounter += position[1]
    centerlon = loncounter / posn
    centerlat = latcounter / posn

    centroid = shapely.geometry.Point([centerlon, centerlat])

    return centroid


#data invoer
gem2018 = Dbf5('gem_2018.dbf').to_dataframe()
wijk2018 = Dbf5('wijk_2018.dbf').to_dataframe()
buurt2018 = Dbf5('buurt2018.dbf').to_dataframe()

gem2018s = shapefile.Reader('gem_2018')
wijk2018s = shapefile.Reader('wijk_2018')
buurt2018s = shapefile.Reader('buurt2018')

try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

pullgros = """SELECT * FROM neighbours"""
cur.execute(pullgros)
groslijst = cur.fetchall()

pullphrase = """SELECT nb1, nb2, nb3, nb4, nb5, nb6, nb7, nb8, nb9, nb10, nb11, nb12, nb13, nb14, nb15, nb16, nb17, nb18, nb19, nb20, nb21, nb22, nb23, nb24, nb25, nb26, nb27, nb28, nb29, nb30, nb31, nb32, nb33, nb34, nb35, nb36, nb37, nb38, nb39, nb40, nb41, nb42, nb43, nb44, nb45 FROM neighbours WHERE codering = %s"""
insertphrase = """UPDATE neighbours SET nb1 = %s, nb2 = %s, nb3 = %s, nb4 = %s, nb5 = %s, nb6 = %s, nb7 = %s, nb8 = %s, nb9 = %s, nb10 = %s, nb11 = %s, nb12 = %s, nb13 = %s, nb14 = %s, nb15= %s, nb16 = %s, nb17 = %s, nb18 = %s, nb19 = %s, nb20 = %s, nb21 = %s, nb22 = %s, nb23 = %s, nb24 = %s, nb25 = %s, nb26 = %s, nb27 = %s, nb28 = %s, nb29 = %s, nb30 = %s, nb31 = %s, nb32 = %s, nb33 = %s, nb34 = %s, nb35 = %s, nb36 = %s, nb37 = %s, nb38 = %s, nb39 = %s, nb40 = %s, nb41 = %s, nb42 = %s, nb43 = %s, nb44 = %s, nb45 = %s WHERE codering = %s"""


conn.close()

#check neighbours
newlist = []
for code in groslijst:
    center = code[1]
    neighbourgros = code[2:]
    
    neighbours = []
    sortedneighbours = []
    for neighbour in neighbourgros:
        if neighbour:
            if neighbour.startswith("GM") or neighbour.startswith("WK") or neighbour.startswith("BU"):
                neighbours.append(neighbour)

    if len(neighbours) <= 10:
        sortedneighbours = neighbours

    else:

        if center.startswith("GM"):

            centercentroid = FindCentroid(center, gem2018, 'GM_CODE', gem2018s)

            distancetable = []
            for n in neighbours:
                neighbourcentroid = FindCentroid(n, gem2018, 'GM_CODE', gem2018s)
                distancetable.append(centercentroid.distance(neighbourcentroid))

            sortedneighbours = [x for _,x in sorted(zip(distancetable, neighbours))][:10]

        elif center.startswith("WK"):

            centercentroid = FindCentroid(center, wijk2018, 'WK_CODE', wijk2018s)

            distancetable = []
            for n in neighbours:
                neighbourcentroid = FindCentroid(n, wijk2018, 'WK_CODE', wijk2018s)
                distancetable.append(centercentroid.distance(neighbourcentroid))

            sortedneighbours = [x for _,x in sorted(zip(distancetable, neighbours))][:10]

        elif center.startswith("BU"):

            centercentroid = FindCentroid(center, buurt2018, 'BU_CODE', buurt2018s)

            distancetable = []
            for n in neighbours:
                neighbourcentroid = FindCentroid(n, buurt2018, 'BU_CODE', buurt2018s)
                distancetable.append(centercentroid.distance(neighbourcentroid))

            sortedneighbours = [x for _,x in sorted(zip(distancetable, neighbours))][:10]
    
    newlist.append([center] + sortedneighbours)
    print(groslijst.index(code) / len(groslijst))

columns = ['center', 'nb1', 'nb2', 'nb3', 'nb4', 'nb5', 'nb6', 'nb7', 'nb8', 'nb9', 'nb10']

df = pd.DataFrame(newlist)

df.columns = columns

engine = create_engine('postgresql+psycopg2://buurtuser:123456@localhost/dbbuurt')

df.to_sql('neighboursmaxed', engine)
