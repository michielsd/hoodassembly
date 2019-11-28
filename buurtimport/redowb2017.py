from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import psycopg2

try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

provpull = """SELECT code, gemeente, provincie FROM provbase"""
cur.execute(provpull)
provtuple = cur.fetchall()

pullmatchold = """SELECT current, old FROM matchold"""
cur.execute(pullmatchold)
matchold = cur.fetchall()

conn.close()

provdict = {}
for row in provtuple:
    print(row)
    provdict[row[0]] = row[2]

engine = create_engine('postgresql+psycopg2://buurtuser:123456@localhost/dbbuurt')

df = pd.read_csv('buurtdata2017.csv', encoding='Windows-1252', index_col=0)

bestanden = [
    'buurtdata2013.csv'
    , 'buurtdata2014.csv'
    , 'buurtdata2015.csv'
    , 'buurtdata2016.csv'
    , 'buurtdata2017.csv'
    , 'buurtdata2018.csv'
]

df.columns = [x.lower() for x in df.columns]
df.columns = df.columns.str.strip()

df['wijkenenbuurten'] = df['wijkenenbuurten'].str.strip()
df['gemeentenaam_1'] = df['gemeentenaam_1'].str.strip()
df['codering_3'] = df['codering_3'].str.strip()
df['meestvoorkomendepostcode_102'] = df['meestvoorkomendepostcode_102'].str.strip()
df['pctvrouwen_200'] = round(100*df['vrouwen_7'] / df['aantalinwoners_5'], 1)
df['pctkinderen_201'] = round(100*df['k_0tot15jaar_8'] / df['aantalinwoners_5'], 1)
df['pctstudenten_202'] = round(100*df['k_15tot25jaar_9'] / df['aantalinwoners_5'], 1)
df['pctbijstand_203'] = round(100*df['personenpersoortuitkeringbijstand_74'] / df['aantalinwoners_5'], 1)  

#add neighbourhoods not existing in 2017
for code in matchold:
    
    if code[0] != code[1] and code[0] != '0':
        newcode = code[0]
        oldcode = code[1]

        income = None
        hhhigh = None
        hhlow = None
        welfare = None

        rightrow = df.loc[df['codering_3'] == oldcode]

        if not rightrow['gemiddeldinkomenperinkomensontvanger_65'].empty:
            income = rightrow['gemiddeldinkomenperinkomensontvanger_65'].item()

        if not rightrow['k_20huishoudensmethoogsteinkomen_71'].empty:
            hhhigh = rightrow['k_20huishoudensmethoogsteinkomen_71'].item()

        if not rightrow['huishoudensmeteenlaaginkomen_72'].empty:
            hhlow = rightrow['huishoudensmeteenlaaginkomen_72'].item()

        if not rightrow['pctbijstand_203'].empty:
            welfare = rightrow['pctbijstand_203'].item()

        length = len(df)
        df.loc[length] = [np.nan] * len(df.columns)
        df.at[length, 'codering_3'] = newcode
        df.at[length, 'gemiddeldinkomenperinkomensontvanger_65'] = income
        df.at[length, 'k_20huishoudensmethoogsteinkomen_71'] = hhhigh
        df.at[length, 'huishoudensmeteenlaaginkomen_72'] = hhlow
        df.at[length, 'pctbijstand_203'] = welfare

    # if not in 18: take new code, create list of old codes to loop over
    elif ";" in code[1] and code[0] != '0':
        print(code[0])
        newcode = code[0]
        oldcodes = code[1].split(";")

        # ready variable set
        income = 0
        ino = 0
        
        hhhigh = 0
        hno = 0
        
        hhlow = 0
        lno = 0
        
        welfare = 0
        wno = 0

        #find old hoods
        for index, row in df.iterrows():
            if len(oldcodes) == 0:
                break
            elif row['codering_3'] in oldcodes:
                if row['gemiddeldinkomenperinkomensontvanger_65']:
                    income += row['gemiddeldinkomenperinkomensontvanger_65']
                    ino += 1
                if row['k_20huishoudensmethoogsteinkomen_71']:
                    hhhigh += row['k_20huishoudensmethoogsteinkomen_71']
                    hno += 1
                if row['huishoudensmeteenlaaginkomen_72']:
                    hhlow += row['huishoudensmeteenlaaginkomen_72']
                    lno += 1
                if row['pctbijstand_203']:
                    welfare += row['pctbijstand_203']
                    wno += 1

                oldcodes.remove(row['codering_3'])

        avincome = None
        avhigh = None
        avlow = None
        avfare = None

        if ino > 0:
            avincome = str(round(income / ino, 1))
        if hno > 0:
            avhigh = str(round(hhhigh / hno, 1))
        if lno > 0: 
            avlow = str(round(hhlow / lno, 1))
        if wno > 0:
            avfare = str(round(welfare / wno, 1))

        length = len(df)
        df.loc[length] = [np.nan] * len(df.columns)
        df.at[length, 'codering_3'] = newcode
        df.at[length, 'gemiddeldinkomenperinkomensontvanger_65'] = round(avincome, 1)
        df.at[length, 'k_20huishoudensmethoogsteinkomen_71'] = round(avhigh, 1)
        df.at[length, 'huishoudensmeteenlaaginkomen_72'] = round(avlow, 1)
        df.at[length, 'pctbijstand_203'] = round(avfare, 1)


df.to_sql('wijkbuurt2017', engine)
print("Wijken, buurten toegevoegd")
print('Succes!')