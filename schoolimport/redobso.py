import psycopg2

try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

enter = """UPDATE kinderopvang SET contact_telefoon = %s WHERE lrk_id = %s"""
pullphone = """SELECT lrk_id, contact_telefoon FROM kinderopvang"""
cur.execute(pullphone)
phonelist = cur.fetchall()

correctphonelist = []
for entry in phonelist:
    correctphone = entry[1].replace("'","")
    print(entry)
    if correctphone == '':
        correctphonelist.append([None, entry[0]])
    else:
        correctphonelist.append([correctphone, entry[0]])

cur.executemany(enter, correctphonelist)
conn.commit()

print("Finished")