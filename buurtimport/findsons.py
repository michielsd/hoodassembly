import re, string

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

pull2018 = """SELECT * FROM wijkbuurt2018"""

df = pd.read_sql_query(pull2018, engine)

#define neighbours
codedict = {}
for index, row in df.iterrows():
    key = row['codering_3']
    value = row['wijkenenbuurten']
    valuelist = value.split(" ")
    switchdummy = 0
    if 'wijk' in valuelist:
        for w in re.finditer('wijk', value):
            sp = w.start()
            substring = value[sp:(sp+7)]
            if len(substring)>=7 and substring[6].isdigit():
                replacement = value.replace(substring, "").strip()
                switchdummy = 1
                if not replacement:
                    codedict[key] = value
                elif replacement.startswith(":") or replacement.startswith("-"):
                    codedict[key] = value[2:]
                else:
                    codedict[key] = replacement
    if 'Wijk' in valuelist:
        for w in re.finditer('Wijk', value):
            sp = w.start()
            substring = value[sp:(sp+7)]
            if len(substring)>=7 and substring[5].isdigit():
                replacement = value.replace(substring, "").strip()
                switchdummy = 1
                if not replacement:
                    codedict[key] = value
                elif replacement.startswith(":") or replacement.startswith("-"):
                    codedict[key] = value[2:]
                else:
                    codedict[key] = replacement
    if switchdummy == 0:
        codedict[key] = value

urldict = {}
for key, value in codedict.items():
    if key.startswith('GM'):
        url = df.loc[df['codering_3'] == key]['urlname'].item()
        urldict[key] = url
        print([key, url])
    elif key.startswith('WK'):
        urlgm = df.loc[df['codering_3'] == key]['urlgm'].item()
        url = df.loc[df['codering_3'] == key]['urlname'].item()
        urldict[key] = urlgm + "/" + url
    elif key.startswith('BU'):
        urlgm = df.loc[df['codering_3'] == key]['urlgm'].item()
        urlwk = df.loc[df['codering_3'] == key]['urlwk'].item()
        url = df.loc[df['codering_3'] == key]['urlname'].item()
        urldict[key] = urlgm + "/" + urlwk + "/" + url

#sort out neighbours
neighbourpull = """SELECT center, nb1, nb2, nb3, nb4, nb5, nb6, nb7, nb8, nb9, nb10 FROM neighboursmaxed"""
cur.execute(neighbourpull)
neightuple = cur.fetchall()

print("")

neighbourlist = [['NL00', None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None]]
for row in neightuple:
    center = row[0]
    neighcodes = row[1:]
    neighcomp = [[codedict[y]] +  [urldict[y]] for y in neighcodes if y]
    neighbours = [item for sublist in neighcomp for item in sublist]
    addition = [None] * (20-len(neighbours))
    neighbours = neighbours + addition
    neighbourlist.append([center] + neighbours)
    print(neighbours)

for row in neightuple[0:5]:
    print(row)

for row in neighbourlist[0:5]:
    print(row)

codes = []
for index, row in df.iterrows():
    code = row['codering_3']
    codes.append(code)

sonsdict = {}
for index, row in df.iterrows():
    code = row['codering_3']
    if code.startswith('BU'):
        sonsdict[code] = (100*[None])
    elif code.startswith('GM'):
        searchclue = "WK" + code[2:]
        sons = [codedict[x] for x in codes if x.startswith(searchclue)]
        sonsaddition = [None] * (100-len(sons))
        totalsons = sons + sonsaddition
        sonsdict[code] = totalsons
    elif code.startswith('WK'):
        searchclue = "BU" + code[2:]
        sons = [codedict[x] for x in codes if x.startswith(searchclue)]
        sonsaddition = [None] * (100-len(sons))
        totalsons = sons + sonsaddition
        sonsdict[code] = totalsons
    else:
        pass

print("completed sons")

for neighbour in neighbourlist:
    if neighbour[0] != "NL00" and neighbour[0] in sonsdict:
        neighbour.extend(sonsdict[neighbour[0]])
        print(neighbour[0])

outputdf = pd.DataFrame(neighbourlist)

colnb = []
for x in range(1,11):
    title = 'nb%d' % (x,)
    urltitle = 'nburl%d' % (x,)
    extras = [title, urltitle]
    colnb.extend(extras)

colson = ['s%d' % (x) for x in range(1,101)]

columns = ['code'] + colnb + colson

outputdf.columns = columns

outputdf.to_sql('navbase', engine)
print("Sons and neighbours executed into Database")