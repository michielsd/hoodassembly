from sqlalchemy import create_engine
import pandas as pd
import csv

engine = create_engine('postgresql+psycopg2://buurtuser:123456@localhost/dbbuurt')

with open('matchlist.csv', 'r', newline='') as csvfile:
        inserttable = list(list(rec) for rec in csv.reader(csvfile, delimiter=',', quotechar='"'))

outputtable = []
for row in inserttable[1:]:
    specelements = []
    for element in row[2:]:
        if element:
            specelements.append(element)
    outputtable.append([row[1], ";".join(specelements)])

df = pd.DataFrame(outputtable)

columnlist = ['current', 'old']

df.columns = columnlist

print(df)

df.to_sql('matchold', engine)

print('Success')