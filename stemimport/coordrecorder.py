import csv
import psycopg2

try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

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

# formule om toe te voegen aan MYSQL
def insertxy(idnr, xlist, ylist):
	add_data = "UPDATE stembureau SET xc = '%d', yc = '%d' WHERE id = '%s'" % (xlist, ylist, idnr)
	return add_data

#import stembureaus from dbase
stembureaucaller = ("SELECT * FROM stembureau")
cur.execute(stembureaucaller)
stembureautuple = cur.fetchall()

stembureautabel = []
for row in stembureautuple:
	stembureautabel.append(list(row))

#bereken RD coordinaten en voeg toe
conv = RDWGSConverter()
for row in stembureautabel:
	coordinates = conv.fromWgsToRd([float(row[3]),float(row[4])])
	row.extend(coordinates)
	cur.execute(insertxy(row[0],row[-2],row[-1]))

conn.commit()
cur.close()
conn.close()

print('success!')