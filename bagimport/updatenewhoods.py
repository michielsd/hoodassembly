#CREATE TABLE newbag (id SERIAL PRIMARY KEY NOT NULL, openbareruimte TEXT, huisnummer TEXT, woonplaats TEXT, postcode TEXT, lon TEXT, lat TEXT, gem_code TEXT, wk_code TEXT, bu_code TEXT);

import psycopg2
import pandas as pd
import shapefile 
import shapely
from shapely.geometry import Point

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
    clueshape = FindShape(keyclue, buurtdata, key, buurten)

    juistebucode = False

    if clueshape.contains(location):
        juistebucode = keyclue
        juistewkcode = "WK%s" % keyclue[2:-2]
        juistegmcode = "GM%s" % keyclue[2:-4]
    elif len(keyprev) > 0:
        for prev in keyprev:
            prevshape = FindShape(prev, buurtdata, key, buurten)
            if prevshape.contains(location):
                juistebucode = prev
                juistewkcode = "WK%s" % prev[2:-2]
                juistegmcode = "GM%s" % prev[2:-4]
                break
    
    if not juistebucode:
        for index, buurt in buurtdata.iterrows():
            buurtcode = buurt['BU_CODE']
            buurtshape = FindShape(buurtcode, buurtdata, key, buurten)
            if buurtshape.contains(location):
                juistebucode = buurtcode
                juistewkcode = "WK%s" % buurtcode[2:-2]
                juistegmcode = "GM%s" % buurtcode[2:-4]
                break

    return [juistegmcode, juistewkcode, juistebucode]


try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

buurten = shapefile.Reader("C:/Dashboard/Buurt2/Dataimport/Uitvoer_shape/2019/bu_2019", encoding="Windows-1252")
buurtshapes = buurten.shapeRecords()
buurtdata = pd.read_csv('C:/Dashboard/Buurt2/Dataimport/Uitvoer_shape/2019/bu_2019.csv', encoding="Windows-1252")

pullnewold = """SELECT current, old FROM match1819"""
cur.execute(pullnewold)
newold = cur.fetchall()

codelist = []
nrdict = {}
nrcounter = 0
for line in newold:
    if line[0].startswith("BU"):
        newline = [line[0]] + line[1].split(";")
        codelist.append(newline)
        
        nrcounter += 1
        nrdict[line[0]] = [str(nrcounter), str(round(100*nrcounter / 13594, 2))]

bagpull = """SELECT openbareruimte, huisnummer, postcode, woonplaats, lon, lat, gem_code, wk_code, bu_code FROM bagadres WHERE bu_code = %s"""
newbagcheck = """SELECT bu_code FROM newbag WHERE bu_code = %s LIMIT 1"""

bagpush = """INSERT INTO newbag (openbareruimte, huisnummer, postcode, woonplaats, lon, lat, gem_code, wk_code, bu_code) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
bagpushaddress = """INSERT INTO newbag (openbareruimte, huisnummer, postcode, woonplaats, gem_code, wk_code, bu_code) VALUES (%s, %s, %s, %s, %s, %s, %s)"""
bagpushstreet = """INSERT INTO newbag (openbareruimte, postcode, woonplaats, gem_code, wk_code, bu_code) VALUES (%s, %s, %s, %s, %s, %s)"""

bagupdateaddress = """UPDATE bagadres SET gem_code = %s, wk_code = %s, bu_code = %s WHERE openbareruimte = %s AND huisnummer = %s AND postcode = %s""" 
bagupdatestreet = """UPDATE bagadres SET gem_code = %s, wk_code = %s, bu_code = %s WHERE openbareruimte = %s AND postcode = %s"""

progressdf = pd.read_csv("progressdf.csv", index_col=0)

#551 blijft hangen: BU00850111.
for line in codelist:
    key = line[0]
    values = line[1:]
    if len(values) == 1 and key.startswith("BU"):
        pass
        """
        print("Checking %s. Ordernr %s,  %s percent - codes identical" % (key, nrdict[key][0], nrdict[key][1]))
        cur.execute(newbagcheck, (key,))
        checkdata = cur.fetchall()
        if not len(checkdata) > 0:
            cur.execute(bagpull, (key,))
            valuedata = cur.fetchall()
            if valuedata: #sommige buurten hebben kennelijk geen addressen..?
                cur.executemany(bagpush, tuple(valuedata))
                conn.commit()
                print("Updated and committed %s - %s" % (key, valuedata[-1][3]))
        """
    if len(values) > 1 and key.startswith("BU"):
        print("Checking %s. Ordernr %s,  %s percent - recoded" % (key, nrdict[key][0], nrdict[key][1]))
        for value in values:
            """
            for insert
            cur.execute(newbagcheck, (key,))
            checkdata = cur.fetchall()
            if not len(checkdata) > 0:
            """
            dbcheck = progressdf.loc[progressdf['codes'] == key]['codes'].values

            if len(dbcheck) == 0:

                cur.execute(bagpull, (value,))
                redolines = cur.fetchall()
                print("redolines")
                if len(redolines) > 0:
                    redoresults = SplitBag(redolines, key, buurtdata, "BU_CODE", buurten)
                    print("redoresults")
                    addresslist = False
                    if redoresults:
                        streetpush = []
                        addresspush = []
                        for address, codes in redoresults.items():
                            addresslist = address.split(";")
                            #for insert into
                            pushline = addresslist + codes

                            if len(addresslist) == 2:
                                #for update
                                pushline = codes + addresslist
                                streetpush.append(tuple(pushline))
                            elif len(addresslist) == 3:
                                #for update
                                pushline = codes + addresslist
                                addresspush.append(tuple(pushline))
                        
                        if streetpush:
                            cur.executemany(bagupdatestreet, tuple(streetpush))
                            conn.commit()
                            print("Updated and committed %s - %s" % (key, addresslist[-1]))
                        elif addresspush:
                            cur.executemany(bagupdateaddress, tuple(addresspush))
                            conn.commit()
                            print("Updated and committed %s - %s" % (key, addresslist[-1]))

                        progressdf.loc[len(progressdf)] = key
                        progressdf.to_csv("progressdf.csv")
