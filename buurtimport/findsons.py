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

df = pd.read_csv('buurtdata2018.csv', encoding='Windows-1252', index_col=0)
df.columns = [x.lower() for x in df.columns]
df.columns = df.columns.str.strip()

df['codering_3'] = df['codering_3'].str.strip()

#define neighbours
codedict = {}
urldict = {}
for index, row in df.iterrows():
    code = row['codering_3']
    name = row['wijkenenbuurten'].replace('[^\w\s]','').replace(" ", "_")
    codedict[code] = name
    urldict[name] = code
    print(code, name)

#sort out neighbours
neighbourpull = """SELECT center, nb1, nb2, nb3, nb4, nb5, nb6, nb7, nb8, nb9, nb10 FROM neighboursmaxed"""
cur.execute(neighbourpull)
neightuple = cur.fetchall()

neighbourlist = []
neighbourlist = [['NL00', None, None, None, None, None, None, None, None, None, None]]
for row in neightuple:
    center = row[0]
    neighbours = row[1:]
    neighbours = [codedict[y] for y in neighbours if y]
    addition = [None] * (10-len(neighbours))
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
        sons = [x for x in codes if x.startswith(searchclue)]
        sonsaddition = [None] * (100-len(sons))
        totalsons = sons + sonsaddition
        sonsdict[code] = totalsons
    elif code.startswith('WK'):
        searchclue = "BU" + code[2:]
        sons = [x for x in codes if x.startswith(searchclue)]
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

colnb = ['nb%d' % (x) for x in range(1,11)]
colson = ['s%d' % (x) for x in range(1,101)]

columns = ['code'] + colnb + colson

outputdf.columns = columns

outputdf.to_sql('navbase', engine)
print("Sons and neighbours executed into Database")