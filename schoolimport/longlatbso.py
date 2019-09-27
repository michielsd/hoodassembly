import psycopg2
import pandas as pd

try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

#helper#
def TurnFloat(coordinate):

    firstbit = coordinate.split(".", 1)[0]
    secondbit = coordinate.split(".", 1)[1]

    secondbit = secondbit.replace(".", "")
    textcoord = firstbit + "." + secondbit

    floatcoord = float(textcoord)

    return floatcoord


def BagExtract(bagid):

    sqlphrase = """SELECT object_id, lon, lat, gem_code, wk_code, bu_code FROM bagadres WHERE object_id IN %s"""
    cur.execute(
        sqlphrase
        , (bagid,)
    )
    
    address = cur.fetchall()
    if len(address) > 0:
        datalist = [list(c) for c in address]
    else:
        datalist = False

    return datalist

# sql phrases
totalnumber = """SELECT COUNT(lrk_id) FROM kinderopvang"""
importschool = """SELECT lrk_id, actuele_naam_oko, bag_id, opvanglocatie_adres, opvanglocatie_postcode, opvanglocatie_woonplaats, id FROM kinderopvang WHERE bu_code is null"""
exportcode = """UPDATE kinderopvang SET lon = %s, lat = %s, gem_code = %s, wk_code = %s, bu_code = %s WHERE lrk_id = %s"""

cur.execute(importschool)
schoollist = cur.fetchall()

counter = 0
plus = 250
limit = 30000

while counter <= limit:
    
    schoolselection = schoollist[counter:(counter + plus)]

    baglist = []
    idlist = []
    for school in schoolselection:
        baglist.append(school[2])
        idlist.append([school[0], school[2], school[1], school[5]])

    bagtext = tuple(baglist)
    addresslist = BagExtract(bagtext)
    exportlist = []
    for i in idlist:
        for a in addresslist:
            if i[1] == a[0]:
                exportlist.append([i[2], i[3], a[1], a[2], a[3], a[4], a[5], i[0]])

    for export in exportlist:
        cur.execute(exportcode
            , export[2:]
        )
        conn.commit()
        print("Added codes to: %s, %s" % (export[0], export[1]))

    counter += plus

conn.close()
print("Finished and connection closed")