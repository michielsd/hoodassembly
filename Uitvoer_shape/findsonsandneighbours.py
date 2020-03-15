#CREATE TABLE sncodes2019 (code TEXT, s1 TEXT, s2 TEXT, s3 TEXT, s4 TEXT, s5 TEXT, s6 TEXT, s7 TEXT, s8 TEXT, s9 TEXT, s10 TEXT, s11 TEXT, s12 TEXT, s13 TEXT, s14 TEXT, s15 TEXT, s16 TEXT, s17 TEXT, s18 TEXT, s19 TEXT, s20 TEXT, s21 TEXT, s22 TEXT, s23 TEXT, s24 TEXT, s25 TEXT, s26 TEXT, s27 TEXT, s28 TEXT, s29 TEXT, s30 TEXT, s31 TEXT, s32 TEXT, s33 TEXT, s34 TEXT, s35 TEXT, s36 TEXT, s37 TEXT, s38 TEXT, s39 TEXT, s40 TEXT, s41 TEXT, s42 TEXT, s43 TEXT, s44 TEXT, s45 TEXT, s46 TEXT, s47 TEXT, s48 TEXT, s49 TEXT, s50 TEXT, s51 TEXT, s52 TEXT, s53 TEXT, s54 TEXT, s55 TEXT, s56 TEXT, s57 TEXT, s58 TEXT, s59 TEXT, s60 TEXT, s61 TEXT, s62 TEXT, s63 TEXT, s64 TEXT, s65 TEXT, s66 TEXT, s67 TEXT, s68 TEXT, s69 TEXT, s70 TEXT, s71 TEXT, s72 TEXT, s73 TEXT, s74 TEXT, s75 TEXT, s76 TEXT, s77 TEXT, s78 TEXT, s79 TEXT, s80 TEXT, s81 TEXT, s82 TEXT, s83 TEXT, s84 TEXT, s85 TEXT, s86 TEXT, s87 TEXT, s88 TEXT, s89 TEXT, s90 TEXT, s91 TEXT, s92 TEXT, s93 TEXT, s94 TEXT, s95 TEXT, s96 TEXT, s97 TEXT, s98 TEXT, s99 TEXT, s100 TEXT, n1 TEXT, n2 TEXT, n3 TEXT, n4 TEXT, n5 TEXT, n6 TEXT, n7 TEXT, n8 TEXT, n9 TEXT, n10 TEXT, n11 TEXT, n12 TEXT, n13 TEXT, n14 TEXT, n15 TEXT);

import pandas as pd
from sqlalchemy import create_engine
import shapefile
import shapely.geometry
from shapely.geometry.polygon import Polygon
from simpledbf import Dbf5
import psycopg2

#helper functions
def FindShape(j, dataobject, code, shapes):

    ranginbestand = dataobject[dataobject[code] == j].index.values.astype(int)[0]
    shapeid = shapely.geometry.asShape(shapes.shape(ranginbestand))
    
    return shapeid

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

def FindSons(broncode, codelist):
    sonsdict = {}

    matchlist = []
    if broncode.startswith("GM"):
        zoekterm = broncode.replace("GM","WK")

        for zoekcode in codelist:
            if zoekcode.startswith(zoekterm):
                matchlist.append(zoekcode)

    elif broncode.startswith("WK"):
        zoekterm = broncode.replace("WK","BU")

        for zoekcode in codelist:
            if zoekcode.startswith(zoekterm):
                matchlist.append(zoekcode)

    if len(matchlist) > 100:
        outputlist = matchlist[:100]
    else:
        lenlist = len(matchlist)
        outputlist = matchlist + (100 - lenlist) * [None]

    return outputlist

def NeighbourLoop(codedict, broncode, gem, gemkey, gems):

    matchlist = []
    bronshape = FindShape(broncode, gem, gemkey, gems)
    broncentroid = FindCentroid(broncode, gem, gemkey, gems)

    for index, row in gem.iterrows():
        doelcode = row[gemkey]
        
        if doelcode in codedict:
            doelshape = FindShape(doelcode, gem, gemkey, gems)

            if broncode != doelcode and not any(doelcode in sl for sl in matchlist) and bronshape.intersects(doelshape):
                doelcentroid = FindCentroid(doelcode, gem, gemkey, gems)
                distance = broncentroid.distance(doelcentroid)
                matchlist.append([doelcode, distance])

    matchlist.sort(key = lambda x : x[1])
    
    outputlist = [item[0] for item in matchlist]

    if len(outputlist) > 15:
        outputlist = outputlist[:15]
    else:
        listlen = len(outputlist)
        outputlist = outputlist + (15-listlen) * [None]
    
    return outputlist

def FindNeighbours(broncode, codedict, gem, wijk, buurt, gems, wijks, buurts):
        
    grabphrase = """SELECT code, n1, n2, n3, n4, n5, n6, n7, n8, n9, n10, n11, n12, n13, n14, n15 FROM sncodes2019 WHERE code = %s"""

    if broncode.startswith("GM"):
        
        return NeighbourLoop(
            codedict, broncode, gem, "GM_CODE", gems
        )

    elif broncode.startswith("WK"):
        fq = "GM%s" % broncode[2:-2]
        cur.execute(grabphrase, (fq,))
        flist = cur.fetchall()
        customdict = []
        if flist:
            for c in codedict:
                for f in flist[0]:
                    if f:
                        fq = "WK%s" % f[2:]
                        if c.startswith(fq):
                            customdict.append(c)
        else:
            customdict = codedict

        return NeighbourLoop(
            customdict, broncode, wijk, "WK_CODE", wijks
        )

    elif broncode.startswith("BU"):
        fq = "WK%s" % broncode[2:-2]
        cur.execute(grabphrase, (fq,))
        flist = cur.fetchall()
        customdict = []
        if flist:
            for c in codedict:
                for f in flist[0]:
                    if f:
                        fq = "BU%s" % f[2:]
                        if c.startswith(fq):
                            customdict.append(c)
        else:
            customdict = codedict

        return NeighbourLoop(
            customdict, broncode, buurt, "BU_CODE", buurts
        )

#data invoer
try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()



gem2019 = pd.read_csv('2019/gm_2019.csv', encoding="Windows-1252")
wijk2019 = pd.read_csv('2019/wk_2019.csv', encoding="Windows-1252")
buurt2019 = pd.read_csv('2019/bu_2019.csv', encoding="Windows-1252")

gem2019s = shapefile.Reader('2019/gm_2019')
wijk2019s = shapefile.Reader('2019/wk_2019')
buurt2019s = shapefile.Reader('2019/bu_2019')

engine = create_engine('postgresql+psycopg2://buurtuser:123456@localhost/dbbuurt')
pullgros = """SELECT codering_3, urlbu, urlwk, urlgm, wijkenenbuurten FROM wijkbuurt2019"""
dfgros = pd.read_sql_query(pullgros, engine)

#ADD URLS, TURN INTO DICTIONARY VALUES

codes = []
for index, row in dfgros.iterrows():
    code = row['codering_3']
    #urlbu = row['urlbu']
    #urlwk = row['urlwk']
    #urlgm = row['urlgm']
    #wenb = row['wijkenenbuurten']
    codes.append(code)

gemeenten = []
for gem in gem2019.iloc[:,0]:
    gemeenten.append(gem)

wijken = []
for wijk in wijk2019.iloc[:,0]:
    wijken.append(wijk)

buurten = []
for buurt in buurt2019.iloc[:,0]:
    buurten.append(buurt)

colson = ['s%d' % (x) for x in range(1,101)]
colneigh = ['n%d' % (x) for x in range(1,16)]

allcol = ['code'] + colson + colneigh
allrefs = 116*['%s']

insertphrase = """INSERT INTO sncodes2019 (%s) VALUES (%s)""" % (", ".join(allcol), ", ".join(allrefs))

insertphrase = """INSERT INTO sncodes2019 (code, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17, s18, s19, s20, s21, s22, s23, s24, s25, s26, s27, s28, s29, s30, s31, s32, s33, s34, s35, s36, s37, s38, s39, s40, s41, s42, s43, s44, s45, s46, s47, s48, s49, s50, s51, s52, s53, s54, s55, s56, s57, s58, s59, s60, s61, s62, s63, s64, s65, s66, s67, s68, s69, s70, s71, s72, s73, s74, s75, s76, s77, s78, s79, s80, s81, s82, s83, s84, s85, s86, s87, s88, s89, s90, s91, s92, s93, s94, s95, s96, s97, s98, s99, s100, n1, n2, n3, n4, n5, n6, n7, n8, n9, n10, n11, n12, n13, n14, n15) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
#print((", ").join(colson), (", ".join(colneigh)))

checkphrase = """SELECT * FROM sncodes2019 WHERE code = %s"""

for code in codes:
    cur.execute(checkphrase, (code,))
    check = cur.fetchall()
    if len(check) == 0 and not code.startswith("NL"):
        print("checking against %s" % code)
        ss = FindSons(code, codes)
        ns = FindNeighbours(code, codes, gem2019, wijk2019, buurt2019, gem2019s, wijk2019s, buurt2019s)
        valuelist = tuple([code] + ss + ns)
        cur.execute(insertphrase, valuelist)
        conn.commit()
        print("committed %s" % code)