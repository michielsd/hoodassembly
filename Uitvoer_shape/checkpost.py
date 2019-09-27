import postgis
import psycopg2

try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

testphrase = """SELECT ST_AsEWKT(geom) FROM buurt2018 LIMIT 5"""

cur.execute(testphrase)
testoutput = cur.fetchall()
print(testoutput)