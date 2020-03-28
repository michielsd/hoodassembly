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

# sql phrases
importschool = """SELECT vestigingsnummer, vestigingsnaam, straatnaam, huisnummer, postcode, plaatsnaam FROM basisschool WHERE bu_code is null"""
importexactaddress = """SELECT openbareruimte, huisnummer, gem_code, wk_code, bu_code FROM bagadres WHERE postcode = %s AND openbareruimte = %s AND huisnummer = %s"""
importaddress = """SELECT openbareruimte, huisnummer, gem_code, wk_code, bu_code FROM bagadres WHERE postcode = %s"""
alterimport = """SELECT openbareruimte, huisnummer, gem_code, wk_code, bu_code FROM bagadres WHERE openbareruimte = %s AND woonplaats = %s"""
exportcode = """UPDATE basisschool SET gem_code = %s, wk_code = %s, bu_code = %s WHERE vestigingsnummer = %s"""

cur.execute(importschool)
schoollist = cur.fetchall()

straten = []
for school in schoollist:
    straten.append(school[2])

straten = list(set(straten))
straten.sort()

for limit in range(0,150):
    basisbag = """SELECT openbareruimte, huisnummer, postcode, gem_code, wk_code, bu_code FROM bagadres WHERE openbareruimte = %s"""
    lowerlimit = 50* limit
    counterlowerlimit = lowerlimit + 1
    higherlimit = lowerlimit + 50
    for straat in straten[counterlowerlimit:higherlimit]:
        basisbag += """ OR openbareruimte = %s"""

    bagdf = pd.read_sql_query(basisbag, engine, params=straten[lowerlimit:higherlimit])

    for school in schoollist:
        if school[2] in straten[lowerlimit:higherlimit]:

            print(school)
            vestigingsnummer = school[0]
            schoolstraat = school[2]
            schoolnummer = school[3]
            postcode = school[4].replace(" ","")

            bagresult = bagdf.loc[
                (bagdf['openbareruimte'] == schoolstraat)
                & (bagdf['huisnummer'] == schoolnummer)
                & (bagdf['postcode'] == postcode)
            ]

            if not bagresult.empty:
                resultrow = bagresult.values.tolist()[0]
                gem_code = resultrow[3]
                wk_code = resultrow[4]
                bu_code = resultrow[5]

            cur.execute(exportcode, (gem_code, wk_code, bu_code, vestigingsnummer))
            conn.commit()
            print("Committed %s to %s" % (school[1], bu_code))


"""
for school in schoollist:

    print(school)
    vestigingsnummer = school[0]
    schoolstraat = school[2]
    schoolnummer = school[3]
    postcode = school[4].replace(" ","")

    cur.execute(importexactaddress, (postcode, schoolstraat, schoolnummer))
    address = cur.fetchall()

    if len(address) >= 1:
        gem_code = address[0][2]
        wk_code = address[0][3]
        bu_code = address[0][4]

    else:
        cur.execute(importaddress, (postcode,))
        addresslist = cur.fetchall()
        df = pd.DataFrame(addresslist)

        if len(df[4].unique()) == 1:
            gem_code = addresslist[0][2]
            wk_code = addresslist[0][3]
            bu_code = addresslist[0][4]

        else:
            switchdummy = 0
            for row in addresslist:
                if row[0] == schoolstraat and row[1] == schoolnummer:
                    gem_code = row[2]
                    wk_code = row[3]
                    bu_code = row[4]
                    switchdummy = 1
                    break
            
            if switchdummy == 0:
                if len(df.loc[df[0] == schoolstraat ][4].unique()) == 1:
                    print(df)
                    for row in addresslist:
                        if row[0] == schoolstraat:
                            gem_code = row[2]
                            wk_code = row[3]
                            bu_code = row[4]
                            switchdummy = 1
                            break

            if switchdummy == 1:

                cur.execute(alterimport, (schoolstraat, school[-1]))
                addresslist = cur.fetchall()

                try:
                    gem_code = row[2]
                    wk_code = row[3]
                    bu_code = row[4]
                    switchdummy = 1
                except:
                    pass


    if bu_code:
        cur.execute(exportcode
            , (gem_code, wk_code, bu_code, vestigingsnummer)
        )
        conn.commit()
        print("Added codes to: %s, %s" % (school[1], school[-1]))

conn.close()
print("Finished and connection closed")

"""