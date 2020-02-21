import shapefile 
import shapely.geometry
import pandas as pd

shapefile = shapefile.Reader('2019/bu_2019')

##############################################
# toevoegen aan dbf files:                  #
# CEN_LON,C,20  CEN_LAT,C,20  MAX_DIST,C,20 #
#############################################


centroidlist = []
for item in range(0,len(shapefile)):
    loncounter = 0
    latcounter = 0
    posn = 0
    for position in shapefile.shape(item).points:
        posn += 1
        loncounter += position[0]
        latcounter += position[1]
    centerlon = loncounter / posn
    centerlat = latcounter / posn

    distancelist = []
    for position in shapefile.shape(item).points:
        lon = position[0]
        lat = position[1]
        distance = ((lon - centerlon)**2 + (lat - centerlat)**2 )**0.5
        distancelist.append(distance)

    largestdistance = max(distancelist)

    centroidlist.append([round(centerlon, 8), round(centerlat, 8), round(largestdistance, 8)])
    print(item / len(shapefile))


df = pd.DataFrame(centroidlist)

df.to_csv('centroids.csv')

"""
R code:

Sr2 = Polygon(cbind(c(5,4,2,5),c(2,3,2,2)))
Srs2=Polygons(list(Sr2), "s2")
spPol=SpatialPolygons(list(Srs2))

find_furthest_point=function(polygon){
  coords=fortify(polygon)[,c(1:2)]  
  center=as.data.frame(gCentroid(polygon))
  longs=coords[,1]
  lats=coords[,2]

  dist_fx=function(long, lat, center=center){
    dist=sqrt((long-center[1])^2+(lat-center[2])^2)
    return(dist)
  }
  dists=mapply(dist_fx, longs, lats, MoreArgs=list(center))
  furthest_index=as.integer(which(dists==max(dists)))
  furthest=coords[furthest_index,]  
  return(furthest)
}

find_furthest_point(Sr2)

"""
