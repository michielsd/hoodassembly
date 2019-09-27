import csv
import psycopg2

files = ['supers.csv', 'jumbos.csv', 'dirksuper.csv', 'deensuper.csv']

try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

add_line = """INSERT INTO supermarkt (keten, adres, postcode, plaats) VALUES (%s, %s, %s, %s)"""

for file in files:
    with open(file, 'r', newline='') as csvfile:
        inserttable = list(list(rec) for rec in csv.reader(csvfile, delimiter=';', quotechar='"'))

        if file == 'supers.csv':
            for row in inserttable:
                cur.execute(add_line, row)

        elif file == 'jumbos.csv':
            for row in inserttable:
                jumborow = ['Jumbo'] + row
                cur.execute(add_line, jumborow)

        elif file == 'dirksuper.csv':
            for row in inserttable:
                jumborow = ['Dirk van den Broek'] + row
                cur.execute(add_line, jumborow)

        elif file == 'deensuper.csv':
            for row in inserttable:
                jumborow = row
                cur.execute(add_line, jumborow)

conn.commit()
cur.close()
conn.close()

print('success!')




