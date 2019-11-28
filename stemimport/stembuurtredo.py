import psycopg2
import pandas as pd
from sqlalchemy import create_engine

#setup database connection
try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

stempull = """SELECT code, vvd, pvda, pvv, sp, cda, d66, christenunie, sgp, pvdd, vijftigplus, denk, fvd FROM stembuurt"""

cur.execute(stempull)
stemtuple = cur.fetchall()
stemtabel = []
for row in stemtuple:
    stemtabel.append(list(row))

stemlist = []
for stem in stemtabel:
    code = stem[0]
    stemdict = {}
    stemdict['vvd'] = stem[1]
    stemdict['pvda'] = stem[2]
    stemdict['pvv'] = stem[3]
    stemdict['sp'] = stem[4]
    stemdict['cda'] = stem[5]
    stemdict['d66'] = stem[6]
    stemdict['christenunie'] = stem[7]
    stemdict['sgp'] = stem[8]
    stemdict['pvdd'] = stem[9]
    stemdict['vijftigplus'] = stem[10]
    stemdict['denk'] = stem[11]
    stemdict['fvd'] = stem[12]

    orderedstem = sorted(stemdict.items(), reverse=True, key=lambda x: x[1])

    party1name = orderedstem[0][0]
    party1value = orderedstem[0][1]
    party2name = orderedstem[1][0]
    party2value = orderedstem[1][1]
    party3name = orderedstem[2][0]
    party3value = orderedstem[2][1]

    populistvalue = stemdict['pvv'] + stemdict['denk'] + stemdict['fvd']

    stemlist.append([code, party1name, party1value, party2name, party2value, party3name, party3value, populistvalue])

headers = ['code', 'part1naam', 'part1pct', 'part2naam', 'part2pct', 'part3naam', 'part3pct', 'popupct']

df = pd.DataFrame(stemlist, columns=headers, index=None)

engine = create_engine('postgresql+psycopg2://buurtuser:123456@localhost/dbbuurt')

df.to_sql('stemmen', engine)

print('success!')