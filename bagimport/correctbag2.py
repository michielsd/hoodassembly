import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import shapefile 
import shapely
from shapely.geometry import Point

def FindShape(j, dataobject, code, shapes):

    ranginbestand = dataobject[dataobject[code] == j].index.values.astype(int)[0]
    shapeid = shapely.geometry.asShape(shapes.shape(ranginbestand))
    
    return shapeid

try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

engine = create_engine('postgresql+psycopg2://buurtuser:123456@localhost/dbbuurt')

#m list of 
herindeling = {}
herindeling['Groningen'] = [
    2018, {
        'Groningen': 1
        , 'Haren': 1
        , 'Ten Boer': 1
    }
]

herindeling['Het Hogeland'] = [
    2018, {
        'Bedum': 1
        , 'De Marne': 1
        , 'Eemsmond': 1
        , 'Winsum': 0.884
    }
]

herindeling['Westerkwartier'] = [
    2018, {
        'Grootegast': 1
        , 'Leek': 1
        , 'Marum': 1
        , 'Zuidhorn': 1
        , 'Winsum': 0.1157
    }
]

herindeling['Altena'] = [
    2018, {
        'Aalburg': 1
        , 'Werkendam': 1
        , 'Woudrichem': 1
    }
]

herindeling['Beekdaelen'] = [
    2018, {
        'Nuth': 1
        , 'Onderbanken': 1
        , 'Schinnen': 1
    }
]

herindeling['Haarlemmermeer'] = [
    2018, {
        'Haarlemmerliede en Spaarnwoude': 1
        , 'Haarlemmermeer': 1
    }
]

herindeling['Hoeksche Waard'] = [
    2018, {
        'Binnenmaas': 1
        , 'Cromstrijen': 1
        , 'Korendijk': 1
        , 'Oud-Beijerland': 1
        , 'Strijen': 1
    }
]

herindeling['Noardeast-Fryslân'] = [
    2018, {
        'Dongeradeel': 1
        , 'Ferwerderadiel': 1
        , 'Kollumerland en Nieuwkruisland': 1
    }
]

herindeling['Molenlanden'] = [
    2018, {
        'Giessenlanden': 1
        , 'Molenwaard': 1
    }
]

herindeling['Noordwijk'] = [
    2018, {
        'Noordwijk': 1
        , 'Noordwijkerhout': 1
    }
]

herindeling['Vijfheerenlanden'] = [
    2018, {
        'Leerdam': 1
        , 'Zederik': 1
        , 'Vianen': 1
    }
]

herindeling['West Betuwe'] = [
    2018, {
        'Geldermalsen': 1
        , 'Lingewaal': 1
        , 'Neerijnen': 1
    }
]

importgems = """SELECT codering_3, gemeentenaam_1 FROM wijkbuurt2019"""
cur.execute(importgems)
gemsimport = cur.fetchall()

#organiseer gemeenten
gems = []
for gem in gemsimport:
    if gem[0].startswith("GM"):
        if gem[1] in herindeling:
            for her in herindeling[gem[1]][1]:
                gems.append([gem[0], her])
        else:
            gems.append(list(gem))

#ga per gemeente na welke adressen niet in de gemeente zijn toebedeeld
verifygem = """SELECT openbareruimte, postcode, lon, lat FROM bagadres WHERE gemeente = %s AND gem_code <> %s"""
seeneighbours = """SELECT openbareruimte, huisnummer, gem_code, wk_code, bu_code FROM bagadres WHERE openbareruimte = %(dor)s AND postcode = %(dpc)s"""
seeneighbours2 = """SELECT openbareruimte, huisnummer, gem_code, wk_code, bu_code FROM bagadres WHERE openbareruimte = %s AND postcode = %s"""
#bagdf = pd.read_sql_query(seeneighbours, engine, params={"dor": loc[0], "dpc": loc[2]})

resetneighbours = """UPDATE bagadres SET gem_code = %s, wk_code = %s, bu_code = %s WHERE openbareruimte = %s AND postcode = %s"""

buurtshapes = shapefile.Reader("C:/Dashboard/Buurt2/Dataimport/Uitvoer_shape/2019/bu_2019", encoding="Windows-1252")
buurtdata = pd.read_csv('C:/Dashboard/Buurt2/Dataimport/Uitvoer_shape/2019/bu_2019.csv', encoding="Windows-1252")

counter = -1
for gem in gems[7:]:
    counter += 1
    print(counter, gem)
    cur.execute(verifygem, (gem[1], gem[0]))
    falselocs = cur.fetchall()
    locsset = set(falselocs)

    # preset van alle buurten in de gemeente
    if falselocs:
        clue = "BU%s" % gem[0][2:]
        cluematches = []
        for index, row in buurtdata.iterrows():
            #print(row)
            bucode = row['BU_CODE']
            if bucode.startswith(clue):
                cluematches.append(bucode)

    #preset van alle gematchte buurten in de gemeente
    matched = []

    for loc in locsset:
        cur.execute(seeneighbours2, (loc[0], loc[1]))
        addresses = cur.fetchall()

        addresslist = []
        for address in addresses:
            addresslist.append([address[0]] + [int(address[1])] + list(address[2:]))

        addresslist.sort(key = lambda x : x[1])

        if addresslist[0][2] == gem[0] or addresslist[-1][2] == gem[0]:
            if addresslist[0][4] == addresslist[-1][4]:
                insertlist = (addresslist[0][2], addresslist[0][3], addresslist[0][4], loc[0], loc[1])
                cur.execute(resetneighbours, insertlist)
        else:
            insertlist = []

            location = Point(float(loc[2]), float(loc[3]))
            
            if matched:
                for code in matched:
                    shape = FindShape(code, buurtdata, "BU_CODE", buurtshapes)
                    if shape.contains(location):
                        bu_code = code
                        wk_code = "WK" + code[2:-2]
                        gm_code = "GM" + code[2:-4]
                        insertlist = [gm_code, wk_code, bu_code, loc[0], loc[1]]
                        cur.execute(resetneighbours, insertlist)

            if not insertlist:
                for code in cluematches:
                    shape = FindShape(code, buurtdata, "BU_CODE", buurtshapes)
                    if shape.contains(location):
                        bu_code = code
                        wk_code = "WK" + code[2:-2]
                        gm_code = "GM" + code[2:-4]
                        insertlist = [gm_code, wk_code, bu_code, loc[0], loc[1]]
                        matched.append(bu_code)
                        cur.execute(resetneighbours, insertlist)

            if not insertlist:
                insertlist = [gm_code, "", "", loc[0], loc[1]]

            cur.execute(resetneighbours, insertlist)
            conn.commit()
            print(insertlist)








#tally disappeared hoods
select18 = """SELECT codering_3 FROM wijkbuurt2018""" 
select19 = """SELECT codering_3 FROM wijkbuurt2019"""

cur.execute(select18)
codes18 = cur.fetchall()
cur.execute(select19)
codes19 = cur.fetchall()

badhits = []
for code18 in codes18:
    if code18 not in codes19 and code18[0].startswith("BU"):
        badhits.append(code18[0])

#check if in bag

selectbadhit = """SELECT openbareruimte, woonplaats, huisnummer FROM bagadres WHERE bu_code = %s"""

for badhit in badhits:
    cur.execute(selectbadhit, (badhit,))
    badaddressses = cur.fetchall()
    print("BAD HIT", badhit)
    