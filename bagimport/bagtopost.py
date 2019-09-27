import csv
import psycopg2

try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

insertphrase = """INSERT INTO bagadres (openbareruimte, huisnummer, huisletter, huisnummertoevoeging, postcode, woonplaats, gemeente, provincie, object_id, object_type, nevenadres, x, y, lon, lat) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

with open('bagadres.csv', newline='', encoding='UTF-8') as csvfile:
    bagtable = list(list(rec) for rec in csv.reader(csvfile, delimiter=';', quotechar='"'))
    for row in bagtable:
        if row[0] != 'openbareruimte':
            for column in range(0, len(row)):
                cell = row[column]
                if cell == '':
                    row[column] = None
                elif isinstance(cell, str):
                    row[column] = cell.strip(' ')
            cur.execute(insertphrase, row)
            conn.commit()
            print('%s %s, %s' % (row[0], row[1], row[5]))

    

"""
gemeentetable = []
for row in buurttable:
    if row == buurttable[0]:
        gemeentetable.append(row)
    elif row[3] == 'Gemeente':
        gemeentetable.append(row)

filename = 'gemeentelijst.csv'

with open(filename, 'w', newline='') as newfile:
        writer = csv.writer(newfile)
        writer.writerows(gemeentetable)
"""

conn.close()

print("")
print('success!')
