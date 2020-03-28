import psycopg2
import pandas as pd
from sqlalchemy import create_engine

try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

engine = create_engine('postgresql+psycopg2://buurtuser:123456@localhost/dbbuurt')

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

# sql phrases
importschool = """SELECT id, keten, adres, postcode, plaats FROM supermarkt WHERE bu_code is null"""
exportcode = """UPDATE supermarkt SET lon = %s, lat = %s, gem_code = %s, wk_code = %s, bu_code = %s WHERE id = %s"""

superbag = """SELECT id, keten, adres, postcode, plaats FROM supermarkt WHERE bu_code is null"""

cur.execute(importschool)
schoollist = cur.fetchall()

straten = []
for school in schoollist:
    try:
        schoolstraat = school[2].rsplit(' ', 1)[0]
        schoolnummer = ''.join(c for c in school[2].rsplit(' ', 1)[1] if c.isdigit())
    except:
        schoolstraat = school[2]

    straten.append(schoolstraat)

straten = list(set(straten))
straten.sort()

print(straten)
for limit in range(0,150):
    basisbag = """SELECT openbareruimte, huisnummer, postcode, gem_code, wk_code, bu_code, lon, lat FROM bagadres WHERE openbareruimte = %s"""
    lowerlimit = 50* limit
    counterlowerlimit = lowerlimit + 1
    higherlimit = lowerlimit + 50
    for straat in straten[counterlowerlimit:higherlimit]:
        basisbag += """ OR openbareruimte = %s"""

    straatselectie = straten[lowerlimit:higherlimit]
    bagdf = pd.read_sql_query(basisbag, engine, params=straatselectie)

    for school in schoollist:
        print(school)
        try:
            schoolstraat = school[2].rsplit(' ', 1)[0]
            schoolnummer = ''.join(c for c in school[2].rsplit(' ', 1)[1] if c.isdigit())
        except:
            schoolstraat = school[2]

        if schoolstraat in straten[lowerlimit:higherlimit]:
            vestigingsnummer = school[0]
            postcode = school[3].replace(" ","")
            schoolplaats = school[4]

            bagresult = bagdf.loc[
                (bagdf['openbareruimte'] == schoolstraat)
                & (bagdf['huisnummer'] == schoolnummer)
                & (bagdf['postcode'] == postcode)
            ]

            if not bagresult.empty:
                resultrow = bagresult.values.tolist()[0]
                print(resultrow)
                gem_code = resultrow[3]
                wk_code = resultrow[4]
                bu_code = resultrow[5]
                lon = resultrow[6]
                lat = resultrow[7]

                #print(gem_code, wk_code, bu_code, vestigingsnummer)
                cur.execute(exportcode, (lon, lat, gem_code, wk_code, bu_code, vestigingsnummer))
                conn.commit()
                print("Committed %s to %s" % (school[1], bu_code))





"""
for school in schoollist:

    vestigingsnummer = school[0]

    try:
        schoolstraat = school[2].rsplit(' ', 1)[0]
        schoolnummer = ''.join(c for c in school[2].rsplit(' ', 1)[1] if c.isdigit())
    except:
        schoolstraat = school[2]



    postcode = school[3].replace(" ","")
    schoolplaats = school[4]

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
        print("Added codes to: %s, %s" % (school[1], school[-1]))
    else:
        print("Did not match codes to: %s, %s" % (school[1], school[-1]))

conn.close()
print("Finished and connection closed")
"""