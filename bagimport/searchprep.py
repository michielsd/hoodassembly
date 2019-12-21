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

wijkbuurt2 = []
for buurt in wijkbuurt:
    altline = []
    for cell in buurt:
        if cell and cell.startswith("Wijk"):
            altline.append(cell[8:])
        else:
            altline.append(cell)
    wijkbuurt2.append(altline)

print(wijkbuurt2[:5])

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

print("Prep complete")

#pull bagdata per batch
# no in bagadres: 9090846
insertbag = """INSERT INTO searchbase (searchindex, gmurl, wkurl, buurl, woonplaats) VALUES (%s, %s, %s, %s, %s)"""

bagpull = """SELECT id, openbareruimte, huisnummer, huisletter, huisnummertoevoeging, woonplaats, gem_code, wk_code, bu_code FROM bagadres WHERE woonplaats = %(dwp)s"""
searchbasepull = """SELECT searchindex FROM searchbase WHERE woonplaats = %s"""

streetpull = """SELECT DISTINCT(openbareruimte) FROM bagadres WHERE woonplaats = %s"""


#CHECK woonplaats 2008
for woonplaats in woonplaatsen:
    print("woonplaats: %d / %d" % (woonplaatsen.index(woonplaats), len(woonplaatsen)))
    woonplaatslist = []
    cur.execute(streetpull, (woonplaats,))
    straten = cur.fetchall()

    bagdf = pd.read_sql_query(bagpull, engine, params={"dwp": woonplaats,})

    for straat in straten:
        
        selectdf = bagdf.loc[bagdf['openbareruimte'] == straat[0]]
        codes_q = len(set(selectdf.loc[:, 'bu_code'].values))

        if codes_q == 1:
            
            gmcode = selectdf.loc[:, 'gem_code'].values[0]
            wkcode = selectdf.loc[:, 'wk_code'].values[0]
            bucode = selectdf.loc[:, 'bu_code'].values[0]

            searchindex = "%s, %s" % (straat[0], woonplaats)
            gmurl = urldict[gmcode][0]
            if wkcode in urldict:
                wkurl = urldict[wkcode][2] + "/" + urldict[wkcode][0]
                buurl = urldict[bucode][2] + "/" + urldict[bucode][1] + "/" + urldict[bucode][0]
            else:
                wkurl = None
                buurl = None

            datainsert = (searchindex, gmurl, wkurl, buurl, woonplaats)
            woonplaatslist.append(datainsert)
            print(datainsert)
        else:
            for index, row in selectdf.iterrows():
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
                
                datainsert = (fulladdress, gemeenteurl, wijkurl, buurturl, woonplaats)
                woonplaatslist.append(datainsert)
                print(datainsert)

    cur.execute(searchbasepull, (woonplaats,))
    
    try: 
        woonplaatscheck = cur.fetchall()
        checklist = [x[0] for x in woonplaatscheck]

        for row in datainsert:
            if row[0] in checklist:
                datainsert.remove(row)
    except:
        pass

    print("EXECUTING INTO DBASE FOR %s" % woonplaats)
    cur.executemany(insertbag, woonplaatslist)
    conn.commit()
    print("INSERT SUCCESFUL")
    

    


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
