import csv
import psycopg2

try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

#importeer lijst met provincies
with open('testmunis3.csv', newline='\n') as csvfile:
    inserttable = list(list(rec) for rec in csv.reader(csvfile, delimiter=';', quotechar='"'))

provdict = {}
for row in inserttable:
    provdict[row[0]] = row[1]

#importeer lijst met buurten
sqlselector = """SELECT codering_3, wijkenenbuurten, gemeentenaam_1, id FROM wijkbuurt2018"""
cur.execute(sqlselector)
buurtlijst = cur.fetchall()
worktable = []
for row in buurtlijst:
    worktable.append(list(row))

for buurt in worktable[1:]:
    try:
        buurt.append(provdict[buurt[2]])
    except:
        if buurt[0].startswith('GM'):
            pass
        else:
            clue = buurt[0][2:6]
            for row in worktable:
                if row[0].endswith(clue):
                    gemeente = row[2]
                    buurt.append(provdict[gemeente])

sqlupdater = """INSERT INTO provbase (code, wijkenenbuurten, gemeente, id, provincie) VALUES (%s, %s, %s, %s, %s) """
for row in worktable[1:]:
    cur.execute(sqlupdater, row)
    conn.commit()

conn.close()

print('succes!!')