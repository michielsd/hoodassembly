import psycopg2

try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

importer = """SELECT id, gem_code, wk_code, bu_code FROM bagadres WHERE wk_code LIKE 'GM____'"""
exporter = """UPDATE bagadres SET gem_code = %s, wk_code = %s, bu_code = %s WHERE id = %s"""

cur.execute(importer)
foutelijst = cur.fetchall()

tracker = 0

for adres in foutelijst:
    
    idnr = adres[0]
    gem_code = adres[2]
    wk_code = adres[1]
    bu_code = adres[3]

    cur.execute(exporter, (gem_code, wk_code, bu_code, idnr))
    conn.commit()
    tracker += 1
    print(tracker) 

conn.close()
print('Success!')