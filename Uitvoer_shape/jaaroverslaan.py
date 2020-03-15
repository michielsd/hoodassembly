from sqlalchemy import create_engine
import pandas as pd
import csv
import psycopg2

# setup psycopg2 and engine
try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

engine = create_engine('postgresql+psycopg2://buurtuser:123456@localhost/dbbuurt')


# select data and make dicts
pullmatchold = """SELECT current, old FROM match1718"""
pullmatchnew = """SELECT current, old FROM match1819"""

dict1718 = {}
cur.execute(pullmatchold)
comp1718 = cur.fetchall()
for comp in comp1718:
    dict1718[comp[0]] = comp[1].split(";")

dict1819 = {}
cur.execute(pullmatchnew)
comp1819 = cur.fetchall()
for comp in comp1819:
    dict1819[comp[0]] = comp[1].split(";")

#match 17 and 19
comp1719 = []

for code19, codes18 in dict1819.items():
    # als buurt/wijk/gemeente onveranderd:
    if len(codes18) == 1:
        code18 = codes18[0]
        try:
            codes17 = dict1718[code18]

            if len(codes17) == 1:
                code17 = codes17[0]

                newline = [code19, code17]
            
            # als buurt/wijk/gemeente is veranderd in 17
            elif len(codes17) > 1:
                code17 = []
                for code in codes17:
                    code17.append(code)
            
                newline = [code19, ";".join(code17)]
        except:
            print(code18)


    # als buurt/wijk/gemeente is veranderd in 18
    elif len(codes18) > 1:
        print(code19, codes18)
        code17 = []
        for code in codes18:
            try:
                codes17 = dict1718[code]
                if len(codes17) == 1:
                    code17.append(codes17[0])
            
                #als wijk/buurt/gemeente veranderd in 17 en 18
                elif len(codes17) > 1:
                    for code in codes17:
                        code17.append(code)
            except:
                print(code)

        newline = [code19, ";".join(code17)]

    comp1719.append(newline)

df = pd.DataFrame(comp1719)

columnlist = ['current', 'old']

df.columns = columnlist

print(df)

df.to_sql('match1719', engine)

print('Success')
    
    #if key == dict1718[value]:
    #    newline = [key, value]
    #    print(newline)

"""
outputtable = []
for row in inserttable[1:]:
    specelements = []
    for element in row[2:]:
        if element:
            specelements.append(element)
    outputtable.append([row[1], ])

df = pd.DataFrame(outputtable)

columnlist = ['current', 'old']

df.columns = columnlist

print(df)

df.to_sql('matcheightnine', engine)

print('Success')

for key, value in dict1718.items():
    if key.startswith("GM"):
        print(key, value)


"""