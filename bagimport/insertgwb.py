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

#update bagadres. START AT LAST UPDATED
for postcode in range(6210,9999):
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
                cur.execute(bagupdater2, (gem, wk, bu, cluepostcode))
                conn.commit()
                print("Matched and updated: %s, %s, %s, %s" % (bu, buurt.record[1], buurt.record[4], cluepostcode))
                break
    else:
        stringpostcode = str(postcode)
        cluepostcode = stringpostcode + '%%'
        cur.execute(bagcaller2, (cluepostcode,))
        bagtuple = cur.fetchall()
        bagtable = []
        for row in bagtuple:
            bagtable.append(list(row))
        
        prevshape = shapely.geometry.asShape(buurtshapes[5].shape)
        prevgem = buurtshapes[5].record[3]
        prevwk = buurtshapes[5].record[2]
        prevbu = buurtshapes[5].record[0]

        for address in bagtable:
            addressid = address[0] 
            addresspostcode = address[3][:4]
            addresslocation = Point(float(address[5]), float(address[6]))

            if prevshape.contains(addresslocation):
                cur.execute(bagupdater, (prevgem, prevwk, prevbu, addressid))
                conn.commit()
                print("Matched and updated: %s %s, %s" % (address[1], address[2], address[4]))
            else:
                for buurt in buurtshapes:
                    buurtpostcode = buurt.record[7]
                    if stringpostcode == buurtpostcode:
                        poly = shapely.geometry.asShape(buurt.shape)
                        gem = buurt.record[3]
                        wk = buurt.record[2]
                        bu = buurt.record[0]
                        if poly.contains(addresslocation):
                            cur.execute(bagupdater, (gem, wk, bu, addressid))
                            conn.commit()
                            prevshape = poly
                            prevgem = gem
                            prevwk = wk
                            prevbu = bu
                            print("Matched and updated: %s %s, %s" % (address[1], address[2], address[4]))


"""





        cluepostcode = stringpostcode + '%%'
        cur.execute(bagcaller2, (cluepostcode,))
        bagtuple = cur.fetchall()
        bagtable = []
        for row in bagtuple:
            bagtable.append(list(row))

        prevshape = shapely.geometry.asShape(buurtshapes[5].shape)
        prevgem = buurtshapes[5].record[3]
        prevwk = buurtshapes[5].record[2]
        prevbu = buurtshapes[5].record[0]

        for address in bagtable:
            addressid = address[0] 
            addresspostcode = address[3][:4]
            addresslocation = Point(float(address[5]), float(address[6]))

            if prevshape.contains(addresslocation):
                cur.execute(bagupdater, (prevgem, prevwk, prevbu, addressid))
                conn.commit()
                print("Matched and updated: %s %s, %s" % (address[1], address[2], address[4]))
            else:
                for buurt in buurtshapes:
                    buurtpostcode = buurt.record[7]
                    if stringpostcode == buurtpostcode:
                        poly = shapely.geometry.asShape(buurt.shape)
                        gem = buurt.record[3]
                        wk = buurt.record[2]
                        bu = buurt.record[0]
                        if poly.contains(addresslocation):
                            cur.execute(bagupdater, (gem, wk, bu, addressid))
                            conn.commit()
                            print("Matched and updated: %s %s, %s" % (address[1], address[2], address[4]))



    
for counter in range(0, 100):
    start = counter * 10000
    finish = (counter + 1) * 10000

    cur.execute(bagcaller, (start, finish))
    bagtuple = cur.fetchall()
    bagtable = []
    for row in bagtuple:
        bagtable.append(list(row))

    
    """
cur.close()
conn.close()
                

        
