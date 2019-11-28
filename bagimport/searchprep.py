from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import psycopg2

#set up database connections
#for individual update
try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

#for pandas
engine = create_engine('postgresql+psycopg2://buurtuser:123456@localhost/dbbuurt')

#pull buurtdata
wijkbuurtpull = """SELECT codering_3, urlname, urlwk, urlgm, wijkenenbuurten FROM wijkbuurt2018"""
cur.execute(wijkbuurtpull)
wijkbuurt = cur.fetchall()

#pull woonplaats en straat data
woonplaatspull = """SELECT DISTINCT(woonplaats) FROM bagadres"""
cur.execute(woonplaatspull)
woonplaatstuple = cur.fetchall()


woonplaatsen = []
for x in woonplaatstuple:
    woonplaatsen.append(x[0])

woonplaatsen.sort()

#create urldict
urldict = {}
for wijk in wijkbuurt:
    codering = wijk[0]
    names = wijk[1:]
    urldict[codering] = names

#pull bagdata per batch
# no in bagadres: 9090846
insertbag = """UPDATE bagadres SET zoekadres = %s, gmurl = %s, wkurl = %s, buurl = %s WHERE id = %s"""
insertstreet = """UPDATE bagadres SET zoekadres = %s, gmurl = %s, wkurl = %s, buurl = %s WHERE woonplaats = %s AND openbareruimte = %s"""

bagpull = """SELECT id, openbareruimte, huisnummer, huisletter, huisnummertoevoeging, woonplaats, gem_code, wk_code, bu_code FROM bagadres WHERE gmurl IS NULL and woonplaats = %(dwp)s AND openbareruimte = %(dor)s"""

streetpull = """SELECT DISTINCT(openbareruimte) FROM bagadres WHERE woonplaats = %s"""

for woonplaats in woonplaatsen[1:]:
    print("woonplaats: %d" % woonplaatsen.index(woonplaats))
    cur.execute(streetpull, (woonplaats,))
    straten = cur.fetchall()

    for straat in straten:
        bagdf = pd.read_sql_query(bagpull, engine, params={"dwp": woonplaats, "dor": straat[0]})
        
        codes_q = len(set(bagdf.loc[:, 'bu_code'].values))
        
        if codes_q == 1:
            
            gmcode = bagdf.loc[:, 'gem_code'].values[0]
            wkcode = bagdf.loc[:, 'wk_code'].values[0]
            bucode = bagdf.loc[:, 'bu_code'].values[0]
            
            zoekadres = "%s, %s" % (straat[0], woonplaats)
            gmurl = urldict[gmcode][0]
            if wkcode in urldict:
                wkurl = urldict[wkcode][2] + "/" + urldict[wkcode][0]
                buurl = urldict[bucode][2] + "/" + urldict[bucode][1] + "/" + urldict[bucode][0]
            else:
                wkurl = None
                buurl = None

            datainsert = (zoekadres, gmurl, wkurl, buurl, woonplaats, straat[0])
            cur.execute(insertstreet, datainsert)
            conn.commit()
            print(datainsert)
        else:
            for index, row in bagdf.iterrows():
                idnr = row['id']
                if row['huisletter']:
                    fulladdress = row['openbareruimte'] + " " + row['huisnummer'] + "-" +  row['huisletter'] + ", " + row['woonplaats']
                elif row['huisnummertoevoeging']:
                    fulladdress = row['openbareruimte'] + " " + row['huisnummer'] + "-" + row['huisnummertoevoeging'] + ", " + row['woonplaats']
                else:
                    fulladdress = row['openbareruimte'] + " " + row['huisnummer'] + ", " + row['woonplaats']

                gemeenteurl = urldict[row['gem_code']][0]
                
                if row['wk_code'] in urldict:
                    wijkurl = urldict[row['wk_code']][2] + "/" + urldict[row['wk_code']][0]
                    buurturl = urldict[row['bu_code']][2] + "/" + urldict[row['bu_code']][1] + "/" + urldict[row['bu_code']][0]
                else:
                    wijkurl = None
                    buurturl = None
                
                datainsert = (fulladdress, gemeenteurl, wijkurl, buurturl, idnr)
                cur.execute(insertbag, datainsert)
                conn.commit()
                print(datainsert)


    


"""
tally = 0
while tally < 9090846:
    custompull = bagpull % (tally, (tally+100000))
    bagdf = pd.read_sql_query(custompull, engine)

    #new column, full address
    for index, row in bagdf.iterrows():
        idnr = row['id']
        if row['huisletter']:
            fulladdress = row['openbareruimte'] + " " + row['huisnummer'] + "-" +  row['huisletter'] + ", " + row['woonplaats']
        elif row['huisnummertoevoeging']:
            fulladdress = row['openbareruimte'] + " " + row['huisnummer'] + "-" + row['huisnummertoevoeging'] + ", " + row['woonplaats']
        else:
            fulladdress = row['openbareruimte'] + " " + row['huisnummer'] + ", " + row['woonplaats']

        gemeenteurl = urldict[row['gem_code']][0]
        
        if row['wk_code'] in urldict:
            wijkurl = urldict[row['wk_code']][2] + "/" + urldict[row['wk_code']][0]
            buurturl = urldict[row['bu_code']][2] + "/" + urldict[row['bu_code']][1] + "/" + urldict[row['bu_code']][0]
        else:
            wijkurl = None
            buurturl = None
        
        datainsert = (fulladdress, gemeenteurl, wijkurl, buurturl, idnr)

        cur.execute(insertbag, datainsert)
        conn.commit()
        print(datainsert)

    tally += 100000 
"""
conn.close()
