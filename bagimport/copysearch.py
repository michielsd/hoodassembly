from sqlalchemy import create_engine
import pandas as pd
import psycopg2

#set up database connections
#for individual update
try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

engine = create_engine('postgresql+psycopg2://buurtuser:123456@localhost/dbbuurt')

pullsearchcolumns = """SELECT zoekadres, gmurl, wkurl, buurl FROM bagadres WHERE id > %d AND id < %d"""
pullwijkcolumns = """SELECT codering_3, wijkenenbuurten, urlname, urlwk, urlgm FROM wijkbuurt2018"""

insert = """INSERT INTO searchbase (zoekadres, gmurl, wkurl, buurl) VALUES (%s, %s, %s, %s)"""

cur.execute(pullwijkcolumns)
wijkcolumns = cur.fetchall()

for wijk in wijkcolumns:
    zoekadres = wijk[1]
    gmurl = None
    wkurl = None
    buurl = None
    if wijk[0].startswith('GM'):
        gmurl = wijk[2]
    elif wijk[0].startswith('WK'):
        wkurl = wijk[4] + "/" + wijk[2]
    elif wijk[0].startswith("BU"):
        buurl = wijk[4] + "/" + wijk[3] + "/" + wijk[2]

    datainsert = [zoekadres, gmurl, wkurl, buurl]
    cur.execute(insert, datainsert)
    print(datainsert)
    conn.commit()


tally = 0
while tally < 9090846:
    custompull = pullsearchcolumns % (tally, (tally+100000))
    bagdf = pd.read_sql_query(custompull, engine)

    for index, row in bagdf.iterrows():
        datainsert = [row['zoekadres'], row['gmurl'], row['wkurl'], row['buurl']]
        cur.execute(insert, datainsert)
        conn.commit()
        print(row['zoekadres'])

    tally += 100000
