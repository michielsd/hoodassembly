from osgeo import ogr, osr
import os

#  deze transformatie eerst kopie maken met andere EPSG met onderstaande code
# ogr2ogr -a_srs EPSG:4326 INPUTFILE.shp OUTPUTFILE.shp

# ogr2ogr -a_srs EPSG:4326 wijk_2019_v1.shp wk_2019.shp
###################################################



driver = ogr.GetDriverByName('ESRI Shapefile')

# input SpatialReference
inSpatialRef = osr.SpatialReference()
inSpatialRef.ImportFromEPSG(28992)

# output SpatialReference
outSpatialRef = osr.SpatialReference()
outSpatialRef.ImportFromEPSG(4326)

# create the CoordinateTransformation
coordTrans = osr.CoordinateTransformation(inSpatialRef, outSpatialRef)

# get the input layer
inDataSet = driver.Open(r'2019/wijk_2019_v1.shp')
inLayer = inDataSet.GetLayer()

# create the output layer
outputShapefile = r'2019/wk_2019.shp'
if os.path.exists(outputShapefile):
    driver.DeleteDataSource(outputShapefile)
outDataSet = driver.CreateDataSource(outputShapefile)
outLayer = outDataSet.CreateLayer("basemap_4326", geom_type=ogr.wkbMultiPolygon)

# add fields
inLayerDefn = inLayer.GetLayerDefn()
for i in range(0, inLayerDefn.GetFieldCount()):
    fieldDefn = inLayerDefn.GetFieldDefn(i)
    outLayer.CreateField(fieldDefn)

# get the output layer's feature definition
outLayerDefn = outLayer.GetLayerDefn()

# loop through the input features
inFeature = inLayer.GetNextFeature()
while inFeature:
    # get the input geometry
    geom = inFeature.GetGeometryRef()
    # reproject the geometry
    geom.Transform(coordTrans)
    # create a new feature
    outFeature = ogr.Feature(outLayerDefn)
    # set the geometry and attribute
    outFeature.SetGeometry(geom)
    for i in range(0, outLayerDefn.GetFieldCount()):
        outFeature.SetField(outLayerDefn.GetFieldDefn(i).GetNameRef(), inFeature.GetField(i))
    # add the feature to the shapefile
    outLayer.CreateFeature(outFeature)
    # dereference the features and get the next input feature
    outFeature = None
    inFeature = inLayer.GetNextFeature()

# Save and close the shapefiles
inDataSet = None
outDataSet = None