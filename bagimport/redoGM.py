#CREATE TABLE newbag (id SERIAL PRIMARY KEY NOT NULL, openbareruimte TEXT, huisnummer TEXT, woonplaats TEXT, postcode TEXT, lon TEXT, lat TEXT, gem_code TEXT, wk_code TEXT, bu_code TEXT);

import psycopg2
import pandas as pd
import shapefile 
import shapely
from shapely.geometry import Point
from sqlalchemy import create_engine

#MAAK CORRECTIE VOOR ADM DE RUIJTER LAAN IN COEVORDEN
#DOOR ALLE ADRESSEN IN EEN STRAAT, ALS AFWIJKER NIET IN
#BUURGEMEENTE (MATCH TABEL) DAN CORRIGEREN

def FindShape(j, dataobject, code, shapes):

    ranginbestand = dataobject[dataobject[code] == j].index.values.astype(int)[0]
    shapeid = shapely.geometry.asShape(shapes.shape(ranginbestand))
    
    return shapeid

def SplitBag(baglist, keyclue, buurtdata, key, buurten):

    results = {}
    keyprev = []

    streetnames = []
    bagwithinteger = []
    for line in baglist:
        streetnames.append(line[0])
        bagwithinteger.append([line[0]] + [int(line[1])] + list(line[2:]))

    streetnames = set(streetnames)
    for street in streetnames:
        print("                    ", street)
        streetlist = [i for i in bagwithinteger if i[0] == street]
        
        streetlist.sort(key = lambda x : x[1])
        firstreturn = MatchBag(streetlist[0], keyclue, keyprev, buurtdata, key, buurten)
        lastreturn = MatchBag(streetlist[-1], keyclue, keyprev, buurtdata, key, buurten)
        
        if firstreturn == lastreturn:
            addressname = streetlist[0][0] + ";"  + streetlist[0][2] + ";" + streetlist[0][3]
            #for update
            addressname = streetlist[0][0] + ";" + streetlist[0][2]
            results[addressname] = firstreturn
            keyprev.append(firstreturn[2])
        else:
            for address in streetlist:
                addressresult = MatchBag(address, keyclue, keyprev, buurtdata, key, buurten)
                addressname = address[0] + ";"  + str(address[1]) + ";" + address[2] + ";" + address[3]
                #for update
                addressname = address[0] + ";"  + str(address[1]) + ";" + address[2]
                keyprev.append(addressresult[2])
                results[addressname] = addressresult
        
    return results


def MatchBag(address, keyclue, keyprev, buurtdata, key, buurten):

    location = Point(float(address[4]), float(address[5]))

    altgm = "GM%s" % keyclue[2:]
    juistebucode = False

    for index, buurt in buurtdata.iterrows():
        if buurt['WK_CODE'].startswith(keyclue):
            buurtcode = buurt['WK_CODE']
            buurtshape = FindShape(buurtcode, buurtdata, key, buurten)
            if buurtshape.contains(location):
                juistewkcode = buurtcode
                juistegmcode = "GM%s" % buurtcode[2:-2]
                break
    
    if juistebucode:
        return [juistegmcode, juistewkcode]
    else:
        return [altgm, None]



try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

engine = create_engine('postgresql+psycopg2://buurtuser:123456@localhost/dbbuurt')

buurten = shapefile.Reader("C:/Dashboard/Buurt2/Dataimport/Uitvoer_shape/2019/wk_2019", encoding="Windows-1252")
buurtshapes = buurten.shapeRecords()
buurtdata = pd.read_csv('C:/Dashboard/Buurt2/Dataimport/Uitvoer_shape/2019/wk_2019.csv', encoding="Windows-1252")
gem19 = pd.read_csv('gem19.csv')

codedict = {}
for index, row in gem19.iterrows():
    code = row['Gemeentecode']
    naam = row['Gemeentenaam']
    codedict[naam] = code


gmpull = """SELECT openbareruimte, huisnummer, postcode, gemeente, lon, lat, gem_code FROM bagadres WHERE gem_code LIKE 'GMGM%%'"""
gmpush = """UPDATE bagadres SET gem_code = %s WHERE openbareruimte = %s AND postcode = %s""" 

custompull = """SELECT openbareruimte, huisnummer, postcode, gemeente, lon, lat, gem_code FROM bagadres WHERE openbareruimte = %s AND postcode = %s"""

bagpull = """SELECT gem_code, wk_code, bu_code FROM bagadres WHERE openbareruimte = %(or)s AND postcode = %(pc)s"""

cur.execute(gmpull)
gmgm = cur.fetchall()

correctbag = []
correctset = []
for gm in gmgm:
    #print(gm)
    propercode = codedict[gm[3]]
    correctbag.append((gm[0], gm[1], gm[2], propercode))
    correctset.append((propercode, gm[0], gm[2]))

correctset = set(correctset)

correctdict = {}
for street in correctset:
    
    streetnumbers = []
    for gm in gmgm:
        if gm[0] == street[1] and gm[2] == street[2]:
            streetnumbers.append(gm[1])

    nrextension = """ AND (huisnummer = %s"""
    for nuber in streetnumbers[1:]:
        nrextension += """ OR huisnummer = %s"""
    nrextension += """)"""

    bagpush = gmpush + nrextension
    insertline = street + tuple(streetnumbers)
    cur.execute(bagpush, insertline)
    conn.commit()
    print("UPDATED %d / %d" % (list(correctset).index(street), len(correctset)))
    

"""
for row in correctbag:
    cur.execute(gmpush, row)
    conn.commit()
    print(row)
"""

"""
bagwithinteger = []
streetpostcodes = []
for line in gmgm:
    streetpostcodes.append((line[0], line[2]))
    bagwithinteger.append([line[0]] + [int(line[1])] + list(line[2:]))

streetpostcodes = set(streetpostcodes)
keyprev = "WK000300"

for street in streetpostcodes:

    streetbag = [i for i in bagwithinteger if i[0] == street[0] and i[2] == street[1]]
    streetbag.sort(key = lambda x : x[1])
    middlestreet = streetbag[int(len(streetbag) / 2)]
    gemeentecode = codedict[middlestreet[3]]
    keyclue = "WK%s" % gemeentecode[2:]
    middleresult = MatchBag(middlestreet, keyclue, keyprev, buurtdata, "WK_CODE", buurten)
    

    insertline = tuple(middleresult + [middlestreet[0]] + [middlestreet[2]])
    try: 
        keyprev = middleresult[-1]
    except:
        pass
    print(gmpush, (insertline))
    conn.commit()
    if middleresult[-1]:
        print("UPDATED AND COMMITTED %s, %s" % (middlestreet[0], middlestreet[3]))
"""