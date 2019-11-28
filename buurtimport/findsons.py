import psycopg2
import pandas as pd

try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()


df = pd.read_csv('buurtdata2018.csv', encoding='Windows-1252', index_col=0)
df.columns = [x.lower() for x in df.columns]
df.columns = df.columns.str.strip()

df['codering_3'] = df['codering_3'].str.strip()

codes = []
for index, row in df.iterrows():
    code = row['codering_3']
    codes.append(code)

sonslist = []
for index, row in df.iterrows():
    code = row['codering_3']
    if code.startswith('BU'):
        pass
    elif code.startswith('GM'):
        searchclue = "WK" + code[2:]
        sons = [x for x in codes if x.startswith(searchclue)]
        sonslist.append([code] + sons)
    elif code.startswith('WK'):
        searchclue = "BU" + code[2:]
        sons = [x for x in codes if x.startswith(searchclue)]
        sonslist.append([code] + sons)

maxlength = 0
for row in sonslist:
    if len(row) > 80:
        print(row)

