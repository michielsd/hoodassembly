import csv
from bs4 import BeautifulSoup

deenhtml = open('deensupers.html')

soup = BeautifulSoup(deenhtml, "lxml")

deendata = soup.find_all('div', {'class': 'single-shop'})

with open('deensuper.csv', 'w', newline='') as csvfile:
    superwriter =  csv.writer(csvfile, delimiter=';', quotechar='|', quoting=csv.QUOTE_MINIMAL)

    storelist = []
    for shop in deendata:
        storedata = shop.find('tbody')

        storename = 'Deen'
        storeaddress = storedata.find_all('td')[0].text
        storepost = storedata.find_all('td')[1].text
        storepostalcode = storepost.split(" ", 1)[0]
        storetown = storepost.split(" ", 1  )[1]

        storelist.append([storename, storeaddress, storepostalcode, storetown])

    for store in storelist:
        superwriter.writerow(store)