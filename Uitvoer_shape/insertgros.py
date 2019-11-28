import psycopg2
from simpledbf import Dbf5
import pandas as pd

try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

#data invoer
gem2018 = Dbf5('gem_2018.dbf').to_dataframe()
wijk2018 = Dbf5('wijk_2018.dbf').to_dataframe()
buurt2018 = Dbf5('buurt2018.dbf').to_dataframe()

gem2018 = gem2018.loc[
    (gem2018['WATER'] == 'NEE')
]
wijk2018 = wijk2018.loc[
    (wijk2018['WATER'] == 'NEE')
]
buurt2018 = buurt2018.loc[
    (buurt2018['WATER'] == 'NEE')
]

groslijst = []

gemeenten = []
for gem in gem2018.iloc[:,0]:
    groslijst.append(gem)

wijken = []
for wijk in wijk2018.iloc[:,0]:
    groslijst.append(wijk)

buurten = []
for buurt in buurt2018.iloc[:,0]:
    groslijst.append(buurt)

insertphrase = """INSERT INTO neighbours (codering) VALUES (%s)"""

for code in groslijst:
    print(code)
    cur.execute(insertphrase, (code,))
    conn.commit()
"""

pullphrase = SELECT nb1, nb2, nb3, nb4, nb5, nb6, nb7, nb8, nb9, nb10 FROM neighbours WHERE codering = %s

i = 'WK000300'

gmcode = "GM" + i[2:-2]
print(gmcode)
sublist = [i[:-2]]
cur.execute(pullphrase, (gmcode,))
subtuple = cur.fetchall()
for sub in subtuple[0]:
    if len(sub) > 1:
        subbed = "WK" + sub[2:]
        sublist.append(subbed)

for j in groslijst:
    if j.startswith("WK"):
        if j[:-2] in sublist:
            print(j)
"""