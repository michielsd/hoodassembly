from operator import itemgetter

import psycopg2

#setup database connection
try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

#postgresl comms
stembureaucaller = """SELECT gem_code, wk_code, bu_code, vvd, pvda, pvv, sp, cda, d66, christenunie, sgp, pvdd, vijftigplus, denk, fvd FROM stembureau"""
codecaller = """SELECT codering_3 FROM wijkbuurt2018"""

addphrase = """INSERT INTO stembuurt (code, vvd, pvda, pvv, sp, cda, d66, christenunie, sgp, pvdd, vijftigplus, denk, fvd) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

#create list of stembureaus
cur.execute(stembureaucaller)
stembureautuple = cur.fetchall()
stembureautabel = []
for row in stembureautuple:
    stembureautabel.append(list(row))

#create list of codes
cur.execute(codecaller)
codetuple = cur.fetchall()
codetabel = []
for row in codetuple:
    codetabel.append(list(row))

for refcode in codetabel:
    tally = 0
    switchdummy = 0
    runningscore = []
    code = refcode[0]
    for stembureau in stembureautabel:
        gemeentecode = stembureau[0]
        wijkcode = stembureau[1]
        buurtcode = stembureau[2]
        if code == gemeentecode or code == wijkcode or code == buurtcode:
            switchdummy = 1
            if tally == 0:
                runningscore = stembureau[3:]
                tally += 1
            else:
                runnerup = stembureau[3:]
                runningscore = [x + y for x, y in zip(runningscore, runnerup)]
                tally += 1
    
    if switchdummy == 1:
        if tally == 1:
            adddata = [code] + endscore
        else:
            endscore = [round((x / tally), 1) for x in runningscore]
            adddata = [code] + endscore

        cur.execute(addphrase, adddata)


conn.commit()
cur.close()
conn.close()