import csv
from bs4 import BeautifulSoup
import psycopg2
import re

try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

extractphrase = """SELECT postcode FROM bagadres WHERE openbareruimte = %s AND huisnummer = %s AND woonplaats = %s"""
guessphrase = """SELECT postcode FROM bagadres WHERE openbareruimte ~ %s AND huisnummer = %s AND woonplaats ~ %s"""
broadphrase = """SELECT postcode FROM bagadres WHERE openbareruimte ~ %s AND huisnummer = %s"""

jumbohtml = open('jumbolist2.html')

soup = BeautifulSoup(jumbohtml, "lxml")

jumbodata = soup.find_all('h3')

with open('jumbos.csv', 'w', newline='') as csvfile:
    superwriter =  csv.writer(csvfile, delimiter=';', quotechar='|', quoting=csv.QUOTE_MINIMAL)

    for jumbo in jumbodata:
        adres = jumbo.next_sibling
        successor = adres.next_sibling
        woonplaats = successor.next_sibling
        
        straat = adres.rsplit(' ', 1)[0] #re.findall(r'(\w[^0-9]+)', adres)[0][:-1]
        huisnummer =  adres.rsplit(' ')[-1] #re.findall(r'\d+', adres)

        if '-' in huisnummer:
            huisnummer = huisnummer.split('-')[0]
        else:
            huisnummer = re.findall(r'\d+', huisnummer)[0]

        #huisnummer[0] if isinstance(huisnummer, list) else huisnummer

        curresult = None
        curresult2 = None
        postcode = None
        cur.execute(extractphrase, (straat, huisnummer, woonplaats))

        curresult = cur.fetchall()
        if curresult:
            postcode = curresult[0][0]
        else:
            straat2 = straat.split(" ")[-1]
            woonplaats = woonplaats.split("-")[0]
            cur.execute(guessphrase, (straat2, huisnummer, woonplaats))
            curresult2 = cur.fetchall()
            if curresult2:
                postcode = curresult2[0][0]
            else:
                cur.execute(broadphrase, (straat, huisnummer))
                curresult3 = cur.fetchall()
                if len(set(curresult3)) == 1:
                    postcode = curresult3[0][0]

        print(adres, postcode, woonplaats)
        superwriter.writerow([adres, postcode, woonplaats])

