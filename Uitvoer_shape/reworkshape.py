import shapefile
from shapely.geometry import Point # Point class
from shapely.geometry import shape
import matplotlib.pyplot as plt

class RDWGSConverter:
    X0 = 155000
    Y0 = 463000
    phi0 = 52.15517440
    lam0 = 5.38720621

    def fromRdToWgs( self, coords ):

        Kp = [0,2,0,2,0,2,1,4,2,4,1]
        Kq = [1,0,2,1,3,2,0,0,3,1,1]
        Kpq = [3235.65389,-32.58297,-0.24750,-0.84978,-0.06550,-0.01709,-0.00738,0.00530,-0.00039,0.00033,-0.00012]

        Lp = [1,1,1,3,1,3,0,3,1,0,2,5]
        Lq = [0,1,2,0,3,1,1,2,4,2,0,0]
        Lpq = [5260.52916,105.94684,2.45656,-0.81885,0.05594,-0.05607,0.01199,-0.00256,0.00128,0.00022,-0.00022,0.00026]

        dX = 1E-5 * ( coords[0] - self.X0 )
        dY = 1E-5 * ( coords[1] - self.Y0 )
        
        phi = 0
        lam = 0

        for k in range(len(Kpq)):
            phi = phi + ( Kpq[k] * dX**Kp[k] * dY**Kq[k] )
        phi = self.phi0 + phi / 3600

        for l in range(len(Lpq)):
            lam = lam + ( Lpq[l] * dX**Lp[l] * dY**Lq[l] )
        lam = self.lam0 + lam / 3600

        return [phi,lam]

    def fromWgsToRd( self, coords ):
        
        Rp = [0,1,2,0,1,3,1,0,2]
        Rq = [1,1,1,3,0,1,3,2,3]
        Rpq = [190094.945,-11832.228,-114.221,-32.391,-0.705,-2.340,-0.608,-0.008,0.148]

        Sp = [1,0,2,1,3,0,2,1,0,1]
        Sq = [0,2,0,2,0,1,2,1,4,4]
        Spq = [309056.544,3638.893,73.077,-157.984,59.788,0.433,-6.439,-0.032,0.092,-0.054]

        dPhi = 0.36 * ( coords[0] - self.phi0 )
        dLam = 0.36 * ( coords[1] - self.lam0 )

        X = 0
        Y = 0

        for r in range( len( Rpq ) ):
            X = X + ( Rpq[r] * dPhi**Rp[r] * dLam**Rq[r] ) 
        X = self.X0 + X

        for s in range( len( Spq ) ):
            Y = Y + ( Spq[s] * dPhi**Sp[s] * dLam**Sq[s] )
        Y = self.Y0 + Y

        return [X,Y]

file = "buurt2018"
#ALSO CHANGE FIELD!!!!

rsf = shapefile.Reader(file)
shapeRec = rsf.shapeRecords()

w = shapefile.Writer('BU_post')
w.field('BU_CODE', 'C', '10')
w.field('WK_CODE', 'C', '8')
w.field('GM_CODE', 'C', '6')

conv = RDWGSConverter()

for row in shapeRec:
    #dbf content
    gm_code = row.record[0]
    gm_code1 = row.record[2]
    gm_code2 = row.record[3]
    print(row.record[1], row.record[4])

    #geometry
    points = row.shape.points
    parts = row.shape.parts
    geom = []

    #handle parts
    for i in range(len(parts)):
        xy = []
        pt = None
        if i < len(parts) - 1:
            pt = points[parts[i]:parts[i + 1]]
        else:
            pt = points[parts[i]:]
        for x, y in pt:
            WGS = conv.fromRdToWgs([x,y])
            xy.append(WGS)
        geom.append(xy)

    w.poly(geom)
    w.record(gm_code, gm_code1, gm_code2)

"""

shapes = rsf.shapes()
records = rsf.records()

testshape = shapes[121]

print(records[121])
print("")
print(testshape.parts)
print("")
print(len(testshape.points))

newshape = []
for row in testshape.points:
    coordinates = conv.fromRdToWgs([row[0],row[1]])
    newshape.append(coordinates)
    
lons = [coordinate[0] for coordinate in testshape.points]
lats = [coordinate[1] for coordinate in testshape.points]
    
plt.plot(lons, lats, 'k')
plt.show()

"""