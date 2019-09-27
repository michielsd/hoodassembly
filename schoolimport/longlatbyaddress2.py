import psycopg2
import pandas as pd

try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

#helper#
def TurnFloat(coordinate):

    firstbit = coordinate.split(".", 1)[0]
    secondbit = coordinate.split(".", 1)[1]

    secondbit = secondbit.replace(".", "")
    textcoord = firstbit + "." + secondbit

    floatcoord = float(textcoord)

    return floatcoord

def PostplusExact(straat, huisnummer, postcode):
    

    sqlphrase = """SELECT openbareruimte, huisnummer, lon, lat, gem_code, wk_code, bu_code FROM bagadres WHERE openbareruimte = %s AND huisnummer = %s AND postcode = %s"""
    cur.execute(
        sqlphrase
        , (straat, huisnummer, postcode)
    )
    addresslist = cur.fetchall()

    if len(addresslist) >= 1:
        
        lon = addresslist[0][2]
        lat = addresslist[0][3]
        gem_code = addresslist[0][4]
        wk_code = addresslist[0][5]
        bu_code = addresslist[0][6]
        datalist = [lon, lat, gem_code, wk_code, bu_code]
    
    else:
        datalist = False

    return datalist

def NoPostExtract(straat, huisnummer, plaats):


    sqlphrase = """SELECT openbareruimte, huisnummer, lon, lat, gem_code, wk_code, bu_code FROM bagadres WHERE openbareruimte = %s AND huisnummer = %s AND woonplaats = %s"""
    cur.execute(
        sqlphrase
        , (straat, huisnummer, plaats)
    )
    addresslist = cur.fetchall()

    if len(addresslist) >= 1:
        
        lon = addresslist[0][2]
        lat = addresslist[0][3]
        gem_code = addresslist[0][4]
        wk_code = addresslist[0][5]
        bu_code = addresslist[0][6]
        datalist = [lon, lat, gem_code, wk_code, bu_code]
    
    else:
        
        datalist = False

    return datalist

def OnlyStreetExtract(straat, huisnummer):
    

    sqlphrase = """SELECT openbareruimte, huisnummer, woonplaats, lon, lat, gem_code, wk_code, bu_code FROM bagadres WHERE openbareruimte = %s AND huisnummer = %s"""
    cur.execute(
        sqlphrase
        , (straat, huisnummer)
    )
    
    addresslist = cur.fetchall()
    addresses = []
    for address in addresslist:
        addresses.append(list(address))

    for address in addresses:
        address[3] = TurnFloat(address[3])
        address[4] = TurnFloat(address[4])

    df = pd.DataFrame(addresses)
    if not df.empty:
        if len(df[6].unique()) == 1:
            lon = df[3].mean()
            lat = df[4].mean()
            gem_code = addresslist[0][5]
            wk_code = addresslist[0][6]
            bu_code = addresslist[0][7]
            datalist = [lon, lat, gem_code, wk_code, bu_code]
        else:
            datalist = False
    else:
        datalist = False

    return datalist

def OnlyPostExtract(postcode):


    sqlphrase = """SELECT openbareruimte, huisnummer, lon, lat, gem_code, wk_code, bu_code FROM bagadres WHERE postcode = %s"""
    cur.execute(
        sqlphrase
        , (postcode,)
    )
    addresslist = cur.fetchall()
    
    addresses = []
    for address in addresslist:
        addresses.append(list(address))

    for address in addresses:
        address[2] = TurnFloat(address[2])
        address[3] = TurnFloat(address[3])

    df = pd.DataFrame(addresses)

    if not df.empty:
        if len(df[6].unique()) == 1:
            lon = df[2].mean()
            lat = df[3].mean()
            gem_code = addresslist[0][4]
            wk_code = addresslist[0][5]
            bu_code = addresslist[0][6]
            datalist = [lon, lat, gem_code, wk_code, bu_code]
        else:
            datalist = False
    else:
        datalist = False

    return datalist

def BagExtract(bagid):

    sqlphrase = """SELECT lon, lat, gem_code, wk_code, bu_code FROM bagadres WHERE object_id = %s"""
    cur.execute(
        sqlphrase
        , (bagid,)
    )
    address = cur.fetchall()
    if len(address) > 0:
        datalist = list(address[0])
    else:
        datalist = False

    return datalist

# sql phrases
totalnumber = """SELECT COUNT(lrk_id) FROM kinderopvang"""
importschool = """SELECT lrk_id, actuele_naam_oko, bag_id, opvanglocatie_adres, opvanglocatie_postcode, opvanglocatie_woonplaats, id FROM kinderopvang WHERE bu_code is null"""
exportcode = """UPDATE kinderopvang SET lon = %s, lat = %s, gem_code = %s, wk_code = %s, bu_code = %s WHERE lrk_id = %s"""

cur.execute(importschool)
schoollist = cur.fetchall()

cur.execute(totalnumber)
schoolnumber = cur.fetchall()[0][0]

for school in schoollist:

    vestigingsnummer = school[0]
    bagid = school[2]
    schoolstraat = school[3].rsplit(' ', 1)[0]
    schoolnummer = ''.join(c for c in school[3].rsplit(' ', 1)[1] if c.isdigit())
    postcode = school[4]
    schoolplaats = school[5]
    progress = round(100*(int(school[6])/int(schoolnumber)), 1)

    address = BagExtract(
        bagid
    )

    if not address:
        address = PostplusExact(
        schoolstraat, schoolnummer, postcode
    )

    if not address:
        address = NoPostExtract(
        schoolstraat, schoolnummer, schoolplaats
    )

    if not address:
        address = OnlyStreetExtract(
            schoolstraat, schoolnummer
        )

    if not address:
        address = OnlyPostExtract(
            postcode
        )

    if address:
        insertdata = address + [vestigingsnummer]

        cur.execute(exportcode
            , insertdata
        )
        conn.commit()
        print("Added codes to: %s, %s. %d procent voltooid" % (school[1], school[-2], progress))
    else:
        print("Did not match codes to: %s, %s. %d procent voltooid" % (school[1], school[-2], progress))

conn.close()
print("Finished and connection closed")