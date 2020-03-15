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


insertphrase = """UPDATE searchbase SET zoekadres = %s, gmurl = %s, wkurl = %s, buurl = %s, woonplaats = %s WHERE zoekadres = %s AND gmurl = %s AND wkurl = %s AND buurl = %s AND woonplaats = %s"""

selectphrase = """SELECT zoekadres, gmurl, wkurl, buurl, woonplaats FROM searchbase WHERE woonplaats = '''s-Gravenhage' OR woonplaats = '''s-Hertogenbosch'"""
cur.execute(selectphrase)
dhdb = cur.fetchall()

for row in dhdb[:1000]:
    zoekadres = row[0]
    gmurl = row[1]
    wkurl = row[2]
    buurl = row[3]
    woonplaats = row[4]

    zoekadres2 = zoekadres.replace("'s-Hertogenbosch", "Den Bosch") 
    zoekadres2 = zoekadres2.replace("'s-Gravenhage", "Den Haag")
    if gmurl:
        gmurl2 = gmurl.replace("sHertogenbosch", "Den_Bosch")
        gmurl2 = gmurl2.replace("sGravenhage", "Den_Haag")
    if wkurl:
        wkurl2 = wkurl.replace("sHertogenbosch", "Den_Bosch")
        wkurl2 = wkurl2.replace("sGravenhage", "Den_Haag")
    if buurl:
        buurl2 = buurl.replace("sHertogenbosch", "Den_Bosch")
        buurl2 = buurl2.replace("sGravenhage", "Den_Haag")
    woonplaats2 = woonplaats.replace("'s-Hertogenbosch", "Den Bosch")
    woonplaats2 = woonplaats2.replace("'s-Gravenhage", "Den Haag")

    insertrow = (zoekadres2, gmurl2, wkurl2, buurl2, woonplaats2, zoekadres, gmurl, wkurl, buurl, woonplaats)

    cur.execute(insertphrase, insertrow)
    conn.commit()
    print(zoekadres2)

conn.close()