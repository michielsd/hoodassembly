import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql+psycopg2://buurtuser:123456@localhost/dbbuurt')

pctold = """SELECT * FROM pctscoresold"""
df2 = pd.read_sql_query(pctold, engine)

print(df2.columns)

if df2[df2['categorie'] == 'gemiddeldinkomenperinkomensontvanger_65'].empty:
    df2.loc[len(df2)] = [12, 'land', 'gemiddeldinkomenperinkomensontvanger_65', 'GM', '', '', '', 32.0, '', '', '']

print(df2)