import pandas as pd
import shapefile
import shapely.geometry
from shapely.geometry.polygon import Polygon
from simpledbf import Dbf5

#functions

def FindFalsePos(lijst1, lijst2, code):
    

    preselector = []
    for j in lijst1:
        if j not in lijst2 and j.startswith(code):
            preselector.append(j)

    return preselector

def FindCentroid(i, dataobject, code, shapes):


    ranginbestand = dataobject[dataobject[code] == i].index.values.astype(int)[0]
    ishape = shapes.shape(ranginbestand).points
    posn = 0
    loncounter = 0
    latcounter = 0
    for position in ishape:
        posn += 1
        loncounter += position[0]
        latcounter += position[1]
    centroid = shapely.geometry.Point([loncounter / posn, latcounter / posn])

    return centroid

def FindShape(j, dataobject, code, shapes):

    ranginbestand = dataobject[dataobject[code] == j].index.values.astype(int)[0]
    shapeid = shapely.geometry.asShape(shapes.shape(ranginbestand))
    
    return shapeid


#data invoer
gem2018 = pd. read_csv('2019/gm_2019.csv' encoding="Windows-1252")
wijk2018 = pd. read_csv('2019/wk_2019.csv' encoding="Windows-1252")
buurt2018 = pd. read_csv('2019/bu_2019.csv' encoding="Windows-1252")

gem2018s = shapefile.Reader('gem_2018')
wijk2018s = shapefile.Reader('wijk_2018')
buurt2018s = shapefile.Reader('buurt2018')

gem2017 = Dbf5('gem_2018.dbf').to_dataframe()
wijk2017 = Dbf5('wijk_2018.dbf').to_dataframe()
buurt2017 = Dbf5('buurt_2018.dbf').to_dataframe()

gem2017s = shapefile.Reader('gem_2018')
wijk2017s = shapefile.Reader('wijk_2018')
buurt2017s = shapefile.Reader('buurt_2018')

#in één lijst
lijst2018 = []
for gem in gem2018.iloc[:,0]:
    if gem not in lijst2018:
        lijst2018.append(gem)

for wijk in wijk2018.iloc[:,0]:
    if wijk not in lijst2018:
        lijst2018.append(wijk)

for buurt in buurt2018.iloc[:,0]:
    if buurt not in lijst2018:
        lijst2018.append(buurt)

lijst2017 = []
for gem in gem2017.iloc[:,0]:
    if gem not in lijst2017:
        lijst2017.append(gem)

for wijk in wijk2017.iloc[:,0]:
    if wijk not in lijst2017:
        lijst2017.append(wijk)

for buurt in buurt2017.iloc[:,0]:
    if buurt not in lijst2017:
        lijst2017.append(buurt)

selectgm = FindFalsePos(lijst2017, lijst2018, 'GM')
selectwk = FindFalsePos(lijst2017, lijst2018, 'WK')
selectbu = FindFalsePos(lijst2017, lijst2018, 'BU')

print("data verzameld")

#bewerking
jointlijst = []
for i in lijst2018:
    print(i)
    if i in lijst2017:
        jointlijst.append([i, i])
    else:
        if i.startswith("GM"):
            gmmatches = []
            ishape = FindShape(i, gem2018, 'GM_CODE', gem2018s)

            for gm in selectgm:
                gmshape = FindShape(gm, gem2017, 'GM_CODE', gem2017s)
                if gmshape.intersects(ishape):
                    gmmatches.append(gm)

            jointlijst.append([i] + gmmatches)        
            

        elif i.startswith("WK"):
            wkmatches = []
            ishape = FindShape(i, wijk2018, 'WK_CODE', wijk2018s)

            for wk in selectwk:
                wkshape = FindShape(wk, wijk2017, 'WK_CODE', wijk2017s)
                if wkshape.intersects(ishape):
                    wkmatches.append(wk)

            if not wkmatches:
                for gm in gem2017.iloc[:,0]:
                    print("no match %s" % gm)
                    gmshape = FindShape(gm, gem2017, 'GM_CODE', gem2017s)
                    if gmshape.intersects(ishape):
                        wkmatches.append(gm)

            jointlijst.append([i] + wkmatches)
            

        elif i.startswith("BU"):
            bumatches = []
            ishape = FindShape(i, buurt2018, 'BU_CODE', buurt2018s)

            for bu in selectbu:
                bushape = FindShape(bu, buurt2017, 'BU_CODE', buurt2017s)
                if bushape.intersects(ishape):
                    bumatches.append(bu)

            if not bumatches:
                for wk in wijk2017.iloc[:,0]:
                    wkshape = FindShape(wk, wijk2017, 'WK_CODE', wijk2017s)
                    if wkshape.intersects(ishape):
                        bumatches.append(gm)

            jointlijst.append([i] + bumatches)


df = pd.DataFrame.from_records(jointlijst)
df.to_csv("matchlist.csv")