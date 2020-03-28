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

    altgm = "GM%s" % keyclue[2:]
    juistebucode = False
    """
    if clueshape.contains(location):
        juistebucode = keyclue
        juistewkcode = "WK%s" % keyclue[2:-2]
        juistegmcode = "GM%s" % keyclue[2:-4]
    else:
    """
    for index, buurt in buurtdata.iterrows():
        if buurt['BU_CODE'].startswith(keyclue):
            buurtcode = buurt['BU_CODE']
            buurtshape = FindShape(buurtcode, buurtdata, key, buurten)
            if buurtshape.contains(location):
                juistebucode = buurtcode
                juistewkcode = "WK%s" % buurtcode[2:-2]
                juistegmcode = "GM%s" % buurtcode[2:-4]
                break
    
    if juistebucode:
        return [juistegmcode, juistewkcode, juistebucode]
    else:
        return [altgm, None, None]



try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

buurten = shapefile.Reader("C:/Dashboard/Buurt2/Dataimport/Uitvoer_shape/2019/bu_2019", encoding="Windows-1252")
buurtshapes = buurten.shapeRecords()
buurtdata = pd.read_csv('C:/Dashboard/Buurt2/Dataimport/Uitvoer_shape/2019/bu_2019.csv', encoding="Windows-1252")
buurtprev = pd.read_csv('C:/Dashboard/Buurt2/Dataimport/Uitvoer_shape/buurt2018.csv', encoding="Windows-1252")
gem19 = pd.read_csv('gem19.csv')

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

bagpull = """SELECT openbareruimte, huisnummer, postcode, woonplaats, lon, lat, gemeente FROM bagadres WHERE bu_code = %s"""

#bagpull = """SELECT openbareruimte, huisnummer, postcode, woonplaats, lon, lat, gem_code, wk_code, bu_code FROM bagadres WHERE bu_code LIKE %s"""

newbagcheck = """SELECT bu_code FROM newbag WHERE bu_code = %s LIMIT 1"""

bagpush = """INSERT INTO newbag (openbareruimte, huisnummer, postcode, woonplaats, lon, lat, gem_code, wk_code, bu_code) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
bagpushaddress = """INSERT INTO newbag (openbareruimte, huisnummer, postcode, woonplaats, gem_code, wk_code, bu_code) VALUES (%s, %s, %s, %s, %s, %s, %s)"""
bagpushstreet = """INSERT INTO newbag (openbareruimte, postcode, woonplaats, gem_code, wk_code, bu_code) VALUES (%s, %s, %s, %s, %s, %s)"""

bagupdateaddress = """UPDATE bagadres SET gem_code = %s, wk_code = %s, bu_code = %s WHERE openbareruimte = %s AND huisnummer = %s AND postcode = %s""" 
bagupdatestreet = """UPDATE bagadres SET gem_code = %s, wk_code = %s, bu_code = %s WHERE openbareruimte = %s AND postcode = %s"""
bagupdatebuurt = """UPDATE bagadres SET gem_code = %s, wk_code = %s, bu_code = %s WHERE bu_code = %s""" 

codedict = {}
for index, row in gem19.iterrows():
    code = row['Gemeentecode']
    naam = row['Gemeentenaam']
    codedict[naam] = code

progressdf = pd.read_csv("progressdf.csv", index_col=0)

#HERSTEL EZING ETC.

faulty = {
    #'1966' : ['0005', '1651', '1663', '0053'],
    '0014' : ['0009', '0017'],
    '1969' : ['0015', '0022', '0025', '0056'],
    '1970' : ['0058', '0079', '1722'],
    '1960' : ['0236', '0304', '0733'],
    '0394' : ['0393'],
    '1961' : ['0545', '0620', '0707'],
    '1963' : ['0584', '0585', '0588', '0611', '0617'],
    '1978' : ['0689', '1927'],
    '1959' : ['0738', '0870', '0874'],
    '1954' : ['0881', '0951', '0962']
}

faultycodes = ['0005', '1651', '1663', '0053', '0009', '0017', '0015', '0022', '0025', '0056', '0058', '0079', '1722', '0236', '0304', '0733', '0393', '0545', '0620', '0707', '0584', '0585', '0588', '0611', '0617', '0689', '1927', '0738', '0870', '0874', '0881', '0951', '0962']

buurten2019 = buurtdata.loc[:, 'BU_CODE'].values.tolist()
buurten2018 = buurtprev.loc[:, 'BU_CODE,C,10'].values.tolist()

verdwenenbuurten = []
for buurt18 in buurten2018:
    if buurt18 not in buurten2019 and buurt18[2:-4] not in faultycodes:
        verdwenenbuurten.append(buurt18)


for buurt in verdwenenbuurten[26:]:

    cur.execute(bagpull, (buurt,))
    nogsteeds = cur.fetchall()
    print("CHECKING #%s: %s percent. %s" % (verdwenenbuurten.index(buurt), 100*int(verdwenenbuurten.index(buurt)/len(verdwenenbuurten)), buurt))

    if nogsteeds:
        gemeentecode = codedict[nogsteeds[0][6]]
        keyclue = "BU%s" % gemeentecode

        bagwithinteger = []
        streetpostcodes = []
        for line in nogsteeds:
            streetpostcodes.append((line[0], line[2]))
            bagwithinteger.append([line[0]] + [int(line[1])] + list(line[2:]))

        streetpostcodes = set(streetpostcodes)
        keyprev = "BU00030000"

        for street in streetpostcodes:

            streetbag = [i for i in bagwithinteger if i[0] == street[0] and i[2] == street[1]]
            streetbag.sort(key = lambda x : x[1])
            middlestreet = streetbag[int(len(streetbag) / 2)]
            middleresult = MatchBag(middlestreet, keyclue, keyprev, buurtdata, "BU_CODE", buurten)
            
            insertline = tuple(middleresult + [middlestreet[0]] + [middlestreet[2]])
            try: 
                keyprev = middleresult[-1]
            except:
                pass
            cur.execute(bagupdatestreet, (insertline))
            conn.commit()
            print("               UPDATED AND COMMITTED %s, %s" % (middlestreet[0], middlestreet[3]))



"""
for key, values in faulty.items():
    keyphrase = "BU%s" % key

    for value in values:
        valuephrase = "BU%s%%" % value
        cur.execute(bagpull, (valuephrase,))
        faultyhits = cur.fetchall()
        
        if len(faultyhits) > 0:
            bagwithinteger = []
            streetpostcodes = []
            for line in faultyhits:
                streetpostcodes.append((line[0], line[2]))
                bagwithinteger.append([line[0]] + [int(line[1])] + list(line[2:]))

            streetpostcodes = set(streetpostcodes)
            keyprev = "BU00030000"

            for street in streetpostcodes:

                streetbag = [i for i in bagwithinteger if i[0] == street[0] and i[2] == street[1]]
                streetbag.sort(key = lambda x : x[1])
                middlestreet = streetbag[int(len(streetbag) / 2)]
                middleresult = MatchBag(middlestreet, keyphrase, keyprev, buurtdata, "BU_CODE", buurten)
                print("CHECKING %s: %s, %s" % (key, middlestreet[0], middlestreet[3]))

                insertline = tuple(middleresult + [middlestreet[0]] + [middlestreet[2]])
                cur.execute(bagupdatestreet, (insertline))
                conn.commit()
                print("UPDATED AND COMMITTED %s: %s, %s" % (key, middlestreet[0], middlestreet[3]))




#551 blijft hangen: BU00850111.
for line in codelist:
    key = line[0]
    values = line[1:]
    if len(values) == 1 and key.startswith("BU1969") and key != values[0]:
        print("Checking %s. Ordernr %s,  %s percent - codes identical" % (key, nrdict[key][0], nrdict[key][1]))
        
        gem_code = "GM%s" % key[2:-4] 
        wk_code = "WK%s" % key[2:-2]
        bu_code = key

        cur.execute(bagupdatebuurt, (gem_code, wk_code, bu_code, values[0]))
        conn.commit()
        #print("Updated and committed %s - %s" % (key, valuedata[-1][3]))

        
        cur.execute(bagpull, (values[0],))
        valuedata = cur.fetchall()
        
        if valuedata: #sommige buurten hebben kennelijk geen addressen..?
            gem_code = "GM%s" % key[2:-4] 
            wk_code = "WK%s" % key[2:-2]
            bu_code = key

            cur.execute(bagupdatebuurt, (gem_code, wk_code, bu_code, values[0]))
            conn.commit()
            print("Updated and committed %s - %s" % (key, valuedata[-1][3]))
"""        
