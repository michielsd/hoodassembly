import psycopg2
import shapefile 
import shapely
from shapely.geometry import Point

try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

#load shapefile
buurten = shapefile.Reader("buurt2018")
buurtshapes = buurten.shapeRecords()

#postgres commands
bagcaller = """SELECT id, openbareruimte, huisnummer, postcode, woonplaats, x, y FROM bagadres WHERE gem_code IS null AND id BETWEEN %s AND %s"""
bagcaller2 = """SELECT id, openbareruimte, huisnummer, postcode, woonplaats, x, y FROM bagadres WHERE gem_code IS null AND postcode LIKE %s"""
bagcaller3 = """SELECT DISTINCT(openbareruimte), postcode, woonplaats, x, y FROM bagadres WHERE gem_code IS null AND postcode LIKE %s"""
bagupdater = """UPDATE bagadres SET gem_code = %s, wk_code = %s, bu_code = %s WHERE ID = %s"""
bagupdater2 = """UPDATE bagadres SET gem_code = %s, wk_code = %s, bu_code = %s WHERE postcode LIKE %s"""
bagupdater3 = """UPDATE bagadres SET gem_code = %s, wk_code = %s, bu_code = %s WHERE openbareruimte = %s AND postcode LIKE %s"""

#mark postcodes that occur more than once
postcodemarker = {}
for postcode in range(1000,9999):
    counter = 0
    stringpostcode = str(postcode)
    for buurt in buurtshapes:
        buurtpostcode = buurt.record[7]
        if stringpostcode == buurtpostcode:
            counter += 1
    postcodemarker[postcode] = counter

#update bagadres
for postcode in range(1000,9999):
    print(postcode, postcodemarker[postcode])
    stringpostcode = str(postcode)
    if postcodemarker[postcode] == 0:
        pass
    elif postcodemarker[postcode] == 1:
        for buurt in buurtshapes:
            buurtpostcode = buurt.record[7]
            if buurtpostcode == stringpostcode:
                gem = buurt.record[2] 
                wk = buurt.record[3]
                bu = buurt.record[0]
                cur.execute(bagupdater3, (gem, wk, bu, cluepostcode))
                conn.commit()
                print("Matched and updated: %s, %s, %s, %s" % (bu, buurt.record[1], buurt.record[4], cluepostcode))
                break
    else:
        stringpostcode = str(postcode)
        cluepostcode = stringpostcode + '%%'
        cur.execute(bagcaller3, (cluepostcode,))
        bagtuple = cur.fetchall()
        bagtable = []
        for row in bagtuple:
            bagtable.append(list(row))
        
        prevshape = shapely.geometry.asShape(buurtshapes[5].shape)
        prevgem = buurtshapes[5].record[3]
        prevwk = buurtshapes[5].record[2]
        prevbu = buurtshapes[5].record[0]

        for address in bagtable:
            addressstreet = address[0] 
            addresspostcode = address[2][:4]
            addresslocation = Point(float(address[4]), float(address[5]))

            if prevshape.contains(addresslocation):
                cur.execute(bagupdater3, (prevgem, prevwk, prevbu, addressstreet, addresspostcode))
                conn.commit()
                print("Matched and updated: %s %s, %s" % (address[0], address[2], address[1]))
            else:
                for buurt in buurtshapes:
                    buurtpostcode = buurt.record[7]
                    if stringpostcode == buurtpostcode:
                        poly = shapely.geometry.asShape(buurt.shape)
                        gem = buurt.record[3]
                        wk = buurt.record[2]
                        bu = buurt.record[0]
                        if poly.contains(addresslocation):
                            cur.execute(bagupdater3, (prevgem, prevwk, prevbu, addressstreet, addresspostcode))
                            conn.commit()
                            print("Matched and updated: %s %s, %s" % (address[0], address[2], address[1]))
                            prevshape = poly


cur.close()
conn.close()
                

        
