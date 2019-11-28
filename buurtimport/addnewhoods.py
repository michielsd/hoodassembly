import psycopg2

try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

pull2017 = """SELECT codering_3, gemiddeldinkomenperinkomensontvanger_65, k_20huishoudensmethoogsteinkomen_71, huishoudensmeteenlaaginkomen_72, pctbijstand_203 FROM wijkbuurt2017"""
pullmatchold = """SELECT current, old FROM matchold"""

cur.execute(pullmatchold)
matchold = cur.fetchall()

cur.execute(pull2017)
wijkbuurt2017 = cur.fetchall()

newdata = []
idcounter = 16666
for code in matchold:
    if len(code[1]) < 3:
        print(code)

"""
    # if not in 18: take new code, create list of old codes to loop over
    if ";" in code[1] and code[0] != '0':
        newcode = code[0]
        oldcodes = code[1].split(";")

        # ready variable set
        income = 0
        ino = 0
        
        hhhigh = 0
        hno = 0
        
        hhlow = 0
        lno = 0
        
        welfare = 0
        wno = 0

        #find old hoods
        for wijk in wijkbuurt2017:
            #print(wijkbuurt2017.index(wijk) / len(wijkbuurt2017))
            if len(oldcodes) == 0:
                break
            elif wijk[0] in oldcodes:
                if wijk[1]:
                    income += wijk[1]
                    ino += 1
                if wijk[2]:
                    hhhigh += wijk[2]
                    hno += 1
                if wijk[3]:
                    hhlow += wijk[3]
                    lno += 1
                if wijk[4]:
                    welfare += wijk[4]
                    wno += 1

                oldcodes.remove(wijk[0])

        avincome = None
        avhigh = None
        avlow = None
        avfare = None

        if ino > 0:
            avincome = str(round(income / ino, 1))
        if hno > 0:
            avhigh = str(round(hhhigh / hno, 1))
        if lno > 0: 
            avlow = str(round(hhlow / lno, 1))
        if wno > 0:
            avfare = str(round(welfare / wno, 1))

        newdata.append((str(idcounter), newcode, avincome, avhigh, avlow, avfare))
        idcounter += 1

pushphrase = INSERT INTO wijkbuurt2017 (ID, codering_3, gemiddeldinkomenperinkomensontvanger_65, k_20huishoudensmethoogsteinkomen_71, huishoudensmeteenlaaginkomen_72, pctbijstand_203) VALUES (%s, %s, %s, %s, %s, %s)

for row in newdata:
   cur.execute(pushphrase, row)


conn.close()
print("Success!")
"""