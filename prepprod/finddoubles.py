import psycopg2

try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

wbpull = """SELECT codering_3, wijkenenbuurten FROM wijkbuurt2018"""
cur.execute(wbpull)
wbtuple = cur.fetchall()

wblist = []
for i in wbtuple:
    wblist.append(list(i))

wbcorrectie = []
for i in wblist:
    if i[0].startswith("GM"):
        wbcopy = wblist
        wbcopy.remove(i)
        print(i, wbcopy)
        naam = i[1]
        for j in wbcopy:
            if naam == j[1]:
                print(i[1])