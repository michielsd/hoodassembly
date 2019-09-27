import psycopg2
import re

try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

#postgres phrases - UPDATE FOR TABLE
caller = """SELECT vestigingsnummer, vestigingsnaam, straatnaam, huisnummer, plaatsnaam, postcode FROM basisschool"""
bagfinder = """SELECT lat, lon, gem_code, wk_code, bu_code FROM bagadres WHERE openbareruimte = %s AND huisnummer = %s AND (postcode LIKE %s OR woonplaats = %s)"""
updatephrase = """UPDATE basisschool SET lat = %s, lon = %s, gem_code = %s, wk_code = %s, bu_code = %s WHERE vestigingsnummer = %s"""

#import list of schools
cur.execute(caller)
schooltuple = cur.fetchall()
schooltable = []
for row in schooltuple:
    schooltable.append(list(row))

for school in schooltable:
    #fetch location from bagadres
    idnr = school[0]
    if '-' in school[3]:
        school[3] = school[3].rsplit('-', 1)[0]
    addrestable = (school[2], school[3], school[5][:4], school[4])
    cur.execute(bagfinder, addrestable)
    bagtuple = cur.fetchall()
    bagtable = []
    for row in bagtuple:
            bagtable.append(list(row))

    bagadres = bagtable[0] + [school[0]]
    print(bagadres)
    #cur.execute(updatephrase, updatedata)

