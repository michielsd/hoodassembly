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

pullmatchold = """SELECT current, old FROM match1819"""
cur.execute(pullmatchold)
matchtuple = cur.fetchall()
matchold = [['NL00', 'NL00']]
for row in matchtuple:
    matchold.append(row)

conn.close()

engine = create_engine('postgresql+psycopg2://buurtuser:123456@localhost/dbbuurt')

df = pd.read_csv('buurtdata2018.csv', encoding='Windows-1252', index_col=0)

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
#df['meestvoorkomendepostcode_102'] = df['meestvoorkomendepostcode_102'].str.strip()
#df['pctvrouwen_200'] = round(100*df['vrouwen_7'] / df['aantalinwoners_5'], 1)
#df['pctkinderen_201'] = round(100*df['k_0tot15jaar_8'] / df['aantalinwoners_5'], 1)
#df['pctstudenten_202'] = round(100*df['k_15tot25jaar_9'] / df['aantalinwoners_5'], 1)

emptyref = []
for code in matchold:
    newcode = code[0]
    oldcode = code[1]

    if len(oldcode) == 1:
        income = None
        hhhigh = None
        hhlow = None
        diefstal = None
        vernieling = None
        geweld = None

        rightrow = df.loc[df['codering_3'] == oldcode]

        if not rightrow['gemiddeldinkomenperinkomensontvanger_65'].empty:
            income = rightrow['gemiddeldinkomenperinkomensontvanger_65'].values[0]

        if not rightrow['k_20huishoudensmethoogsteinkomen_71'].empty:
            hhhigh = rightrow['k_20huishoudensmethoogsteinkomen_71'].values[0]

        if not rightrow['huishoudensmeteenlaaginkomen_72'].empty:
            hhlow = rightrow['huishoudensmeteenlaaginkomen_72'].values[0]

        if not rightrow['totaaldiefstaluitwoningschuured_78'].empty:
            diefstal = rightrow['totaaldiefstaluitwoningschuured_78'].values[0]

        if not rightrow['vernielingmisdrijftegenopenbareorde_79'].empty:
            vernieling = rightrow['vernielingmisdrijftegenopenbareorde_79'].values[0]

        if not rightrow['geweldsenseksuelemisdrijven_80'].empty:
            geweld = rightrow['geweldsenseksuelemisdrijven_80'].values[0]

        newline = [newcode, income, hhhigh, hhlow, diefstal, vernieling, geweld]
        print(newline)
        emptyref.append(newline)

    elif len(oldcode) > 1:
        oldcodes = oldcode.split(";")

        # ready variable set
        income = 0
        ino = 0
        
        hhhigh = 0
        hno = 0
        
        hhlow = 0
        lno = 0
        
        diefstal = 0
        dno = 0

        vernieling = 0
        vno = 0

        geweld = 0
        gno = 0

        for c in oldcodes:
            rightrow = df.loc[df['codering_3'] == c]

            if not rightrow['gemiddeldinkomenperinkomensontvanger_65'].empty:
                income += rightrow['gemiddeldinkomenperinkomensontvanger_65'].values[0]
                ino += 1
            if not rightrow['k_20huishoudensmethoogsteinkomen_71'].empty:
                hhhigh += rightrow['k_20huishoudensmethoogsteinkomen_71'].values[0]
                hno += 1
            if not rightrow['huishoudensmeteenlaaginkomen_72'].empty:
                hhlow += rightrow['huishoudensmeteenlaaginkomen_72'].values[0]
                lno += 1
            if not rightrow['totaaldiefstaluitwoningschuured_78'].empty:
                diefstal += rightrow['totaaldiefstaluitwoningschuured_78'].values[0]
                dno += 1
            if not rightrow['vernielingmisdrijftegenopenbareorde_79'].empty:
                vernieling += rightrow['vernielingmisdrijftegenopenbareorde_79'].values[0]
                vno += 1
            if not rightrow['geweldsenseksuelemisdrijven_80'].empty:
                geweld += rightrow['geweldsenseksuelemisdrijven_80'].values[0]
                gno += 1

        avincome = None
        avhigh = None
        avlow = None
        avdief = None
        avvern = None
        avgew = None

        if ino > 0:
            avincome = str(round(income / ino, 1))
        if hno > 0:
            avhigh = str(round(hhhigh / hno, 1))
        if lno > 0: 
            avlow = str(round(hhlow / lno, 1))
        if dno > 0:
            avdief = str(round(diefstal / dno, 1))
        if vno > 0:
            avvern = str(round(vernieling / vno, 1))
        if gno > 0:
            avgew = str(round(geweld / gno, 1))


        newline = [newcode, avincome, avhigh, avlow, avdief, avvern, avgew]
        print(newline)
        emptyref.append(newline)


dfexp = pd.DataFrame(emptyref)

columnlist = ['codering_3', 'gemiddeldinkomenperinkomensontvanger_65', 'k_20huishoudensmethoogsteinkomen_71', 'huishoudensmeteenlaaginkomen_72', 'totaaldiefstaluitwoningschuured_78', 'vernielingmisdrijftegenopenbareorde_79', 'geweldsenseksuelemisdrijven_80']

dfexp.columns = columnlist

print(dfexp)

dfexp.to_sql('wijkbuurt201819', engine)
print("Wijken, buurten toegevoegd")
print('Succes!')

"""    
    # if not in 18 and one old code
    if code[0] != code[1] and code[0] != '0':
        
        newcode = code[0]
        oldcode = code[1]

        income = None
        hhhigh = None
        hhlow = None
        welfare = None

        rightrow = df.loc[df['codering_3'] == oldcode]

        if not rightrow['gemiddeldinkomenperinkomensontvanger_65'].empty:
            income = rightrow['gemiddeldinkomenperinkomensontvanger_65'].values[0]

        if not rightrow['k_20huishoudensmethoogsteinkomen_71'].empty:
            hhhigh = rightrow['k_20huishoudensmethoogsteinkomen_71'].values[0]

        if not rightrow['huishoudensmeteenlaaginkomen_72'].empty:
            hhlow = rightrow['huishoudensmeteenlaaginkomen_72'].values[0]

        if not rightrow['pctbijstand_203'].empty:
            welfare = rightrow['pctbijstand_203'].values[0]

        print(code[0], income, hhhigh, hhlow, welfare)
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
 b
        print(code[0], income, hhhigh, hhlow, welfare)
        length = len(df)
        df.loc[length] = [np.nan] * len(df.columns)
        df.at[length, 'codering_3'] = newcode
        df.at[length, 'gemiddeldinkomenperinkomensontvanger_65'] = round(avincome, 1)
        df.at[length, 'k_20huishoudensmethoogsteinkomen_71'] = round(avhigh, 1)
        df.at[length, 'huishoudensmeteenlaaginkomen_72'] = round(avlow, 1)
        df.at[length, 'pctbijstand_203'] = round(avfare, 1)
""" 

