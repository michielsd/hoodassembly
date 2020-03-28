import psycopg2
import pandas as pd
from sqlalchemy import create_engine

try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

def TurnFloat(coordinate):

    firstbit = coordinate.split(".", 1)[0]
    secondbit = coordinate.split(".", 1)[1]

    secondbit = secondbit.replace(".", "")
    textcoord = firstbit + "." + secondbit

    floatcoord = float(textcoord)

    return floatcoord

#for pandas
engine = create_engine('postgresql+psycopg2://buurtuser:123456@localhost/dbbuurt')


importschool = """SELECT lrk_id, bag_id, bu_code FROM kinderopvang WHERE bu_code IS NULL"""
exportschool = """UPDATE kinderopvang SET lon = %s, lat = %s, gem_code = %s, wk_code = %s, bu_code = %s WHERE lrk_id = %s"""
cur.execute(importschool)
schoollist = cur.fetchall()

bagimport = """SELECT id, object_id, lon, lat, gem_code, wk_code, bu_code FROM bagadres WHERE id > %(min)s AND id < %(max)s"""

counter = 0

misfires = []
rightfires = []

while counter < 10000000:
    bagdf = pd.read_sql_query(bagimport, engine, params={"min": counter, "max":(counter + 100000)})
    hitlist = []

    for school in schoollist:
        if not school[2]:
            idcode = str(school[0])
            bagcode = school[1]
            schooldf = bagdf.loc[bagdf['object_id'] == bagcode]
            
            if not schooldf.empty:
                lon = schooldf.lon.values[0]
                lat = schooldf.lat.values[0]
                gem_code = schooldf.gem_code.values[0]
                wk_code = schooldf.wk_code.values[0]
                bu_code = schooldf.bu_code.values[0]

                cur.execute(exportschool, (lon, lat, gem_code, wk_code, bu_code, idcode))
                conn.commit()
                print("Executed %s into DB, Counter: %s / 100" % (idcode, int(counter / 100000)))

    #cur.executemany(exportschool, hitlist)
    print("Executed into database")

    counter += 100000

"""
for school in schoollist:
    
        idcode = str(school[0])
        
        cur.execute(bagimport, (school[1],))
        addresslist = cur.fetchall()

        if len(addresslist) == 1:
            throughput = list(addresslist[0])

            throughput = throughput + [idcode]

            rightfires.append(throughput)
            print(throughput)
        else:
            misfires.append(school)



for row in misfires:
    print(row)

"""

conn.close()
print("Finished") 