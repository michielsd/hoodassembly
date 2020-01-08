import csv
import psycopg2

file = 'kovest.csv'

try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

add_line = """INSERT INTO kinderopvang (lrk_id,  type_oko,  actuele_naam_oko,  aantal_kindplaatsen,  opvanglocatie_adres,  opvanglocatie_postcode,  opvanglocatie_woonplaats,  bag_id,  correspondentie_postcode,  correspondentie_woonplaats,  contact_persoon,  contact_telefoon,  contact_emailadres,  contact_website) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

with open(file, 'r', newline='') as csvfile:
    inserttable = list(list(rec) for rec in csv.reader(csvfile, delimiter=';', quotechar='"'))

    for row in inserttable:
        if row[5] == 'Ingeschreven':
            selectrow = row[0:4] + row[7:11] + row[20:22] + row[23:27]
            correctrow = [item.strip().replace("'", "") for item in selectrow]
            for item in correctrow:
                if len(item) == 0:
                    correctrow[correctrow.index(item)] = None
            if correctrow[1] == 'VGO':
                correctrow[1] = 'Gastouder'
            elif correctrow[1] == 'KDV':
                correctrow[1] = 'Kinderdagverblijf'
            elif correctrow[1] == 'GOB':
                correctrow[1] = 'Gastouderbureau'
            elif correctrow[1] == 'BSO':
                correctrow[1] = 'Buitenschoolse opvang'

            #print(add_line, correctrow)
            cur.execute(add_line, correctrow)
            print("Added %s" % correctrow[2])

conn.commit()
cur.close()
conn.close()

print('success!')




