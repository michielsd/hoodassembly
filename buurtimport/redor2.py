import re

from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import psycopg2


engine = create_engine('postgresql+psycopg2://buurtuser:123456@localhost/dbbuurt')

# pull data sql
# SYNC STEMMEN EN NEIGHBOURS MET HOOFD TABLE
selectwb = """SELECT * FROM wijkbuurt2019"""
selectstemmen = """SELECT code, part1naam, part1pct, part2naam, part2pct, part3naam, part3pct, popupct FROM stemmen""" 
selectnav = """SELECT * FROM navbase"""

dfwb = pd.read_sql_query(selectwb, engine)
dfstemmen = pd.read_sql_query(selectstemmen, engine)
dfnav = pd.read_sql_query(selectnav, engine)

#add columns stemmen
colstemmen = ['part1naam', 'part1pct', 'part2naam', 'part2pct', 'part3naam', 'part3pct', 'popupct']

for item in colstemmen:
    dfwb[item] = np.nan 

#add columns neighbours
colnb = []
for x in range(1,11):
    title = 'nb%d' % (x,)
    urltitle = 'nburl%d' % (x,)
    extras = [title, urltitle]
    colnb.extend(extras)

colson = ['s%d' % (x) for x in range(1,101)]

colneighbours = colnb + colson

for item in colneighbours:
    dfwb[item] = np.nan

print(dfwb)

# loop through via codes
codes = dfwb['codering_3'].values.tolist()

for code in codes:
    
    condition = (dfwb['codering_3'] == code)

    stemrow = dfstemmen.loc[dfstemmen['code'] == code]
    
    if not stemrow['part1naam'].empty:
        dfwb.loc[condition, 'part1naam'] = stemrow['part1naam'].values[0]
        dfwb.loc[condition, 'part1pct'] = stemrow['part1pct'].values[0]
        dfwb.loc[condition, 'part2naam'] = stemrow['part2naam'].values[0]
        dfwb.loc[condition, 'part2pct'] = stemrow['part2pct'].values[0]
        dfwb.loc[condition, 'part3naam'] = stemrow['part3naam'].values[0]
        dfwb.loc[condition, 'part3pct'] = stemrow['part3pct'].values[0]
        dfwb.loc[condition, 'popupct'] = stemrow['popupct'].values[0]

    navrow = dfnav.loc[dfnav['code'] == code]

    for item in colneighbours:
        if not navrow[item].empty:
            dfwb.loc[condition, item] = navrow[item].values[0]

    print(dfwb.loc[condition, 'wijkenenbuurten'].values.tolist())

dfwb.to_sql('wijkbuurtall', engine)

#read 2018 from csv


print("")

