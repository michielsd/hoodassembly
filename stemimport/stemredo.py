from operator import itemgetter
import pandas as pd
from sqlalchemy import create_engine

import psycopg2

#setup database connection
try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

#postgresl comms
stembureaucaller = """SELECT gem_code, wk_code, bu_code, vvd, pvda, pvv, sp, cda, d66, christenunie, groenlinks, sgp, pvdd, vijftigplus, denk, fvd FROM stembureau"""
codecaller = """SELECT codering_3 FROM wijkbuurt2018"""

#create list of stembureaus
cur.execute(stembureaucaller)
stembureautuple = cur.fetchall()
stembureautabel = []
for row in stembureautuple:
    codes = list(row[:3])
    numbers = [float(x) for x in row[3:]]
    stembureautabel.append(codes + numbers)

#create list of codes
cur.execute(codecaller)
codetuple = cur.fetchall()
codetabel = []
for row in codetuple:
    codetabel.append(list(row))

conn.close()
print("Connection closed")

engine = create_engine('postgresql+psycopg2://buurtuser:123456@localhost/dbbuurt')

resultlist = []
for code in codetabel:

    stemlist = []
    matches = 0
    for stem in stembureautabel:

        gem_code = stem[0]
        wk_code = stem[1]
        bu_code = stem[2]

        if code[0] == gem_code or code[0] == wk_code or code[0] == bu_code:
            matches += 1
            stemlist.append(stem[3:])

    endscore = []
    if matches > 1:
        stemsum = [sum(x) for x in zip(*stemlist)]
        endscore = [x/matches for x in stemsum]
        resultlist.append([code[0]] + endscore)
    elif matches == 1:
        endscore = stemlist[0]
        resultlist.append([code[0]] + endscore)

    
    print(codetabel.index(code) / len(codetabel))


headers1 = ['code', 'vvd', 'pvda', 'pvv', 'sp', 'cda', 'd66', 'christenunie', 'groenlinks', 'sgp', 'pvdd', 'vijftigplus', 'denk', 'fvd']
df1 = pd.DataFrame(resultlist, columns=headers1)

df1.to_sql('stembuurt', engine)

print(df1)

print('stembuurt uploaded')

for stem in resultlist:
    print(stem)
    code = stem[0]
    stemdict = {}
    stemdict['VVD'] = stem[1]
    stemdict['PvdA'] = stem[2]
    stemdict['PVV'] = stem[3]
    stemdict['SP'] = stem[4]
    stemdict['CDA'] = stem[5]
    stemdict['D66'] = stem[6]
    stemdict['ChristenUnie'] = stem[7]
    stemdict['GroenLinks'] = stem[8]
    stemdict['SGP'] = stem[9]
    stemdict['Partij voor de Dieren'] = stem[10]
    stemdict['50Plus'] = stem[11]
    stemdict['DENK'] = stem[12]
    stemdict['Forum voor Democratie'] = stem[13]

    orderedstem = sorted(stemdict.items(), reverse=True, key=lambda x: x[1])

    party1name = orderedstem[0][0]
    party1value = orderedstem[0][1]
    party2name = orderedstem[1][0]
    party2value = orderedstem[1][1]
    party3name = orderedstem[2][0]
    party3value = orderedstem[2][1]

    populistvalue = stemdict['PVV'] + stemdict['DENK'] + stemdict['Forum voor Democratie']

    stemlist.append([code, party1name, party1value, party2name, party2value, party3name, party3value, populistvalue])

headers2 = ['code', 'part1naam', 'part1pct', 'part2naam', 'part2pct', 'part3naam', 'part3pct', 'popupct']
df2 = pd.DataFrame(stemlist, columns=headers2)

df2.to_sql('stemmen', engine)

print('stemmen uploaded')
print('Success!')


