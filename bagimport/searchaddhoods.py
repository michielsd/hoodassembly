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

wijkbuurtpull = """SELECT codering_3, urlbu, urlwk, urlgm, wijkenenbuurten, gemeentenaam_1 FROM wijkbuurt2018"""
cur.execute(wijkbuurtpull)
wijkbuurt = cur.fetchall()

gemeentepush = """INSERT INTO searchbase (searchindex, gmurl, woonplaats) VALUES (%s, %s, %s)"""
wijkpush = """INSERT INTO searchbase (searchindex, gmurl, wkurl, woonplaats) VALUES (%s, %s, %s, %s)"""
buurtpush = """INSERT INTO searchbase (searchindex, gmurl, wkurl, buurl, woonplaats) VALUES (%s, %s, %s, %s, %s)"""

for wijk in wijkbuurt:
    buurttxt = wijk[1]
    wijktxt = wijk[2]
    gemeentetxt = wijk[3]
    naam = wijk[4]
    woonplaats = wijk[5]
    print(naam)
    if wijk[0].startswith("GM"):
        gemeenteurl = gemeentetxt
        cur.execute(gemeentepush, (naam, gemeenteurl, woonplaats))
        conn.commit()
    elif wijk[0].startswith("WK"):
        if naam.endswith(woonplaats) and len(name) > len(woonplaats):
            naam = naam.replace(woonplaats, "")
        wijkindex = naam + ", " + woonplaats
        gemeenteurl = gemeentetxt
        wijkurl = gemeentetxt + "/" + wijktxt
        cur.execute(wijkpush, (wijkindex, gemeenteurl, wijkurl, woonplaats))
    elif wijk[0].startswith("BU"):
        if naam.endswith(woonplaats):
            naam = naam.replace(woonplaats, "")
        buurtindex = naam + ", " + woonplaats
        gemeenteurl = gemeentetxt
        wijkurl = gemeentetxt + "/" + wijktxt
        buurturl = gemeentetxt + "/" + wijktxt + "/" + buurttxt
        cur.execute(buurtpush, (buurtindex, gemeenteurl, wijkurl, buurturl, woonplaats))
        conn.commit()

conn.close()