import psycopg2

try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

emptycaller = """SELECT * FROM bagadres WHERE bu_code is null"""
neighborcaller = """SELECT gem_code, wk_code, bu_code FROM bagadres WHERE id = %s OR id = %s """
hoodcaller = """SELECT id, gem_code, wk_code, bu_code FROM bagadres WHERE id BETWEEN %s AND %s"""
addphrase = """UPDATE bagadres SET gem_code = %s, wk_code = %s, bu_code = %s WHERE ID = %s"""

cur.execute(emptycaller)
bagtuple = cur.fetchall()
bagtabel = []
for row in bagtuple:
    bagtabel.append(list(row))

"""
counter = 0
for bagadres in bagtabel:
    
    bagid = bagadres[0]
    bagminus = int(bagid) - 1
    bagplus = int(bagid) + 1

    cur.execute(neighborcaller, (bagminus, bagplus))
    neighbortuple = cur.fetchall()
    
    minusgm = neighbortuple[0][-3]
    minuswk = neighbortuple[0][-2]
    minusbu = neighbortuple[0][-1]
    try:
        plusbu = neighbortuple[1][-1]
    except:
        pass

    if plusbu and minusbu != None and minusbu == plusbu:
        cur.execute(addphrase, (minusgm, minuswk, minusbu, str(bagid)))
        conn.commit()
        print(bagadres)
"""

for bagadres in bagtabel:

    bagid = int(bagadres[0])
    bagplus1 = bagid + 1
    bagmin1 = bagid - 1
    bagplus25 = bagid + 150
    bagmin25 = bagid - 150

    cur.execute(hoodcaller, (bagplus1, bagplus25))
    plustuple = cur.fetchall()

    cur.execute(hoodcaller, (bagmin25, bagmin1))
    mintuple = cur.fetchall()

    pluscode = ""
    for row in plustuple:
        if row[1] != None:
            pluscode = row[-1]
            break

    mincode = ""
    for row in mintuple:
        if row[1] != None:
            print(row)
            mincode = row[-1]

    print(bagadres[1], bagadres[2], pluscode, mincode)



