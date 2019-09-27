import psycopg2
import osgeo.ogr

#setup database connection
try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

shapepath = 'GM_POST.shp'
shapefile = osgeo.ogr.Open(shapepath)

layer = shapefile.GetLayer(0)

for i in range(layer.GetFeatureCount())[5:10]:
    feature = layer.GetFeature(i)
    name = feature.GetField('GM_CODE')
    geom = feature.GetGeometryRef()
    wkt = geom.ExportToWkt()
    print(wkt)
    cur.execute("INSERT INTO shapes (name, geom) " +"VALUES (%s, ST_GeometryFromText(%s, " +"32616))", (name.encode("utf8"), wkt))
    #print("added %s" % name)

conn.commit()
conn.close()