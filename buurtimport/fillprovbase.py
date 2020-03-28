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

df2019 = pd.read_csv("buurtdata2019.csv", encoding='Windows-1252', index_col=0)
df2019['Codering_3'] = df2019['Codering_3'].str.strip()
codes2019 = []
for index, row in df2019.iterrows():
    codes2019.append(row['Codering_3'])

dfprov = pd.read_csv('GemeentenAlfabetisch.csv', encoding="Windows-1252")

provbase = []
for code in codes2019:
    if code.startswith("NL"):
        pass 
    else:
        codemask = "GM%s" % (code[2:6])
        gemeente = dfprov.loc[dfprov['GemeentecodeGM'] == codemask]['Gemeentenaam'].values[0]
        provincie = dfprov.loc[dfprov['GemeentecodeGM'] == codemask]['Provincienaam'].values[0]
        print(code, gemeente, provincie)
        provbase.append([code, gemeente, provincie])

provexp = pd.DataFrame(provbase)

columnlist = ['code', 'gemeente' ,'provincie']

provexp.columns = columnlist

print(provexp)

provexp.to_sql('provbase', engine)
