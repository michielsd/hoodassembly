import csv
import psycopg2

file = 'vovest.csv'

try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

add_line = """INSERT INTO middelschool (Vestigingsnummer,  Vestigingsnaam,  Straatnaam,  Huisnummer,  Postcode,  Plaatsnaam,  Gemeentenaam,  Denominatie,  Telefoonnummer,  Internetadres) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

with open(file, 'r', newline='') as csvfile:
    inserttable = list(list(rec) for rec in csv.reader(csvfile, delimiter=';', quotechar='"'))

    for row in inserttable[1:]:
        selectrow = row[3:8] + [row[8].capitalize()] + [row[10].capitalize()] + row[11:14]  
        correctrow = [item.strip() for item in selectrow]
        for item in correctrow:
            if len(item) == 0:
                correctrow[correctrow.index(item)] = None              
        cur.execute(add_line, correctrow)
        print("Added %s" % correctrow[1])

conn.commit()
cur.close()
conn.close()

print('success!')




