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

# pull data sql
provpull = """SELECT code, gemeente, provincie FROM provbase"""
cur.execute(provpull)
provtuple = cur.fetchall()

neighbourpull = """SELECT center, nb1, nb2, nb3, nb4, nb5, nb6, nb7, nb8, nb9, nb10 FROM neighboursmaxed"""
cur.execute(neighbourpull)
neightuple = cur.fetchall()

conn.close()

#sort provincies
provdict = {}
for row in provtuple:
    print(row)
    provdict[row[0]] = row[2]

engine = create_engine('postgresql+psycopg2://buurtuser:123456@localhost/dbbuurt')

#read 2018 from csv
df = pd.read_csv('buurtdata2018.csv', encoding='Windows-1252', index_col=0)

#read 2017 from sql
pull2017 = """SELECT codering_3, gemiddeldinkomenperinkomensontvanger_65, k_20huishoudensmethoogsteinkomen_71, huishoudensmeteenlaaginkomen_72, pctbijstand_203 FROM wijkbuurt2017"""
df2017 = pd.read_sql_query(pull2017, engine)

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
df['gemiddeldewoningwaarde_35'] = 1000* df['gemiddeldewoningwaarde_35']
df['meestvoorkomendepostcode_103'] = df['meestvoorkomendepostcode_103'].str.strip()
df['pctvrouwen_200'] = round(100*df['vrouwen_7'] / df['aantalinwoners_5'], 1)
df['pctkinderen_201'] = round(100*df['k_0tot15jaar_8'] / df['aantalinwoners_5'], 1)
df['pctstudenten_202'] = round(100*df['k_15tot25jaar_9'] / df['aantalinwoners_5'], 1)
df['pctmarok_203'] = round(100*df['marokko_19'] / df['aantalinwoners_5'], 1)
df['pctturk_204'] = round(100*df['turkije_22'] / df['aantalinwoners_5'], 1)
df['pctantilsur_205'] = round(100*(df['nederlandseantillenenaruba_20'] + df['suriname_21']) / df['aantalinwoners_5'], 1)
df['pctoverigallo_206'] = round(100*df['overignietwesters_23'] / df['aantalinwoners_5'], 1)
df['pctdiefstal_207'] = round(10000*df['totaaldiefstaluitwoningschuured_78'] / df['aantalinwoners_5'], 1) 
df['pctvernieling_208'] = round(10000*df['vernielingmisdrijftegenopenbareorde_79'] / df['aantalinwoners_5'], 1)
df['pctgeweld_209'] = round(10000*df['geweldsenseksuelemisdrijven_80'] / df['aantalinwoners_5'], 1)

codedict = {}
for index, row in df.iterrows():
    code = row['codering_3']
    name = row['wijkenenbuurten'].replace('[^\w\s]','').replace(" ", "_")
    codedict[code] = name
    print(code, name)

neighbourdict = {}
neighbourdict['NL00'] = [None, None, None, None, None, None, None, None, None, None]
for row in neightuple:
    center = row[0]
    neighbours = row[1:]
    neighbours = [codedict[y] for y in neighbours if y]
    addition = [None] * (10-len(neighbours))
    neighbours = neighbours + addition
    neighbourdict[center] = neighbours
    print(neighbours)

#namen wijken aanpassen voor URL: komma zonder spatie
wrongcommas = df[
    (df['wijkenenbuurten'].str.contains(','))
    & (~df['wijkenenbuurten'].str.contains(', '))
]['wijkenenbuurten'].values

commadict = {}
for comma in wrongcommas:
    commadict[comma] = comma.replace(",", ", ")

df = df.replace({'wijkenenbuurten': commadict})

df1 = pd.DataFrame().reindex_like(df)
df1['gemiddeldinkomen_210'] = np.nan
df1['hhhooginkomen_211'] = np.nan
df1['hhlaaginomen_212'] = np.nan
df1['pctbijstand_213'] = np.nan

df1['niveau_300'] = np.nan
df1['plusniveau_301'] = np.nan
df1['provincie_302'] = np.nan
df1['wijknaam_303'] = np.nan

df1['neighbour1'] = np.nan
df1['neighbour2'] = np.nan
df1['neighbour3'] = np.nan
df1['neighbour4'] = np.nan
df1['neighbour5'] = np.nan
df1['neighbour6'] = np.nan
df1['neighbour7'] = np.nan
df1['neighbour8'] = np.nan
df1['neighbour9'] = np.nan
df1['neighbour10'] = np.nan

df1 = df1.iloc[0:0]

mistakecounter = 0
for index, row in df.iterrows():
    df2017 = df2017.loc[df2017['codering_3'] == row['codering_3']]
    df2017['gemiddeldinkomen_210'] = 100*df2017['gemiddeldinkomen_210']
    values2017 = df2017.values.tolist()[0][1:]
    neighbours = neighbourdict[row['codering_3']]

    if row['codering_3'].startswith('NL'):
        df1.loc[len(df1)] = row.values.tolist() + values2017 + [None] + [None] + [None] + [None] + neighbours
        print("check")
    elif row['codering_3'].startswith('BU'):
        print(index / len(df))
        wijknummer = "WK" + row['codering_3'][2:8]
        wijk = df.loc[
            (df['codering_3'] == wijknummer)
        ]['wijkenenbuurten'].item()
        df1.loc[len(df1)] = row.values.tolist() + values2017 + ["buurt"] + ["wijk"] + [provdict[row['codering_3']]] + [wijk] + neighbours
    elif row['codering_3'].startswith('WK'):
        print(index/len(df))
        df1.loc[len(df1)] = row.values.tolist() + values2017 + ["wijk"] + ["gemeente"] + [provdict[row['codering_3']]] + [None] + neighbours
    else:
        df1.loc[len(df1)] = row.values.tolist() + values2017 + ["gemeente"] + [""] + [provdict[row['codering_3']]] + [None] + neighbours
        print(index/len(df))
    
df1['urlname'] = df1['wijkenenbuurten'].str.replace('[^\w\s]','').replace(" ", "_")
df1['urlwk'] = df1['wijknaam_303'].str.replace('[^\w\s]','').replace(" ", "_")
df1['urlgm'] = df1['gemeentenaam_1'].str.replace('[^\w\s]','').replace(" ", "_")

df1['urlname'] = df1['urlname'].str.replace(" ", "_")
df1['urlwk'] = df1['urlwk'].str.replace(" ", "_")
df1['urlgm'] = df1['urlgm'].str.replace(" ", "_")

df1.to_sql('wijkbuurt2018', engine)
print("Wijken, buurten toegevoegd")
print("Misfires: %d" % mistakecounter)
print('Succes!')