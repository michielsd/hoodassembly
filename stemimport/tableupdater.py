# CREATE TABLE stembureau (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, bureau TEXT, plaats TEXT, latitude FLOAT(20,10), longitude FLOAT(20,10), gpscall TEXT, gemeente TEXT, provincie TEXT, grootste TEXT, VVD FLOAT(3,1), PvdA FLOAT(3,1), PVV FLOAT(3,1), SP FLOAT(3,1), CDA FLOAT(3,1), D66 FLOAT(3,1), ChristenUnie FLOAT(3,1), GROENLINKS FLOAT(3,1), SGP FLOAT(3,1), PvdD FLOAT(3,1), 50Plus FLOAT(3,1), OndernemersPartij FLOAT(3,1), VNL FLOAT(3,1), Denk FLOAT(3,1), NieuweWegen FLOAT(3,1), FvD FLOAT(3,1));

import psycopg2

try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

#data insertion operator
add_data = "INSERT INTO stembureau (bureau, plaats, latitude, longitude, gpscall, gemeente, provincie, grootste, VVD, PvdA, PVV, SP, CDA, D66, ChristenUnie, GROENLINKS, SGP, PvdD, VijftigPlus, OndernemersPartij, VNL, Denk, NieuweWegen, FvD) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"

#data

# Replace commas by dots
for row in introvar:
	row[1:] = [c.replace(',','.') for c in row[1:]]

# Replace dashes with zeroes
for row in introvar:
	row[9:] = [c.replace('-','0') for c in row[9:]]	

#Select rows to be put in 
selectvar = []
for row in introvar:
	selectvar.append(row[0:24])

#replace lists by tuples
datavar = [tuple(c) for c in selectvar]

for row in datavar:
    cur.execute(add_data, row)
    print(row[0:2])

conn.commit()
cur.close()
conn.close()

print('success!')