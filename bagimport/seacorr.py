import psycopg2
import statistics

try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

selectwater = """SELECT DISTINCT(openbareruimte) FROM bagadres WHERE wk_code = %s"""
selectaddress = """SELECT huisnummer FROM bagadres WHERE openbareruimte = %s AND wk_code = %s"""
selectneighbour = """SELECT wk_code, bu_code FROM bagadres WHERE openbareruimte = %s AND huisnummer = %s"""

comparestreet = """SELECT wk_code, bu_code FROM bagadres WHERE openbareruimte = %s"""

updatestreet = """UPDATE bagadres SET wk_code = %s, bu_code = %s WHERE openbareruimte = %s"""

waterwk = 'WK036399'
waterbu = 'BU03639997'


cur.execute(selectwater, (waterwk,))
selectedstreets = cur.fetchall()

#SIMPLE ALGO
"""
for street in selectedstreets:
    cur.execute(comparestreet, (street,))
    codes = cur.fetchall()
    wk = []
    bu = []
    for code in codes:
        wk.append(code[0])
        bu.append(code[1])

    wkset = list(set(wk))
    buset = list(set(bu))
    wkset.remove(waterwk)
    buset.remove(waterbu)
    if len(wkset) == 1 and len(buset) == 1:
        cur.execute(updatestreet, (wkset[0], buset[0], street[0]))
        conn.commit()
"""

for street in selectedstreets:
    cur.execute(selectaddress, (street[0], waterwk))
    addresses = cur.fetchall()
    adlist = []
    for ad in addresses:
        adlist.append(int(ad[0]))
    admin = min(adlist)
    admax = max(adlist)
    admed = statistics.median(adlist)

    if (admed - admin) < (admax - admed):
        huisno = admin - 2
    else:
        huisno = admax + 2
    
    cur.execute(selectneighbour, (street[0], str(huisno)))
    nextnumbers = cur.fetchall()
    if len(nextnumbers[0] == 2):
        

    




