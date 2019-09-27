import csv
from bs4 import BeautifulSoup

dirkhtml = open('dirklijst.html')

soup = BeautifulSoup(dirkhtml, "lxml")

streetdata = soup.find_all('span', {'class': 'stores__search__results__result__name'})
zipdata = soup.find_all('span', {'class': 'stores__search__results__result__zipcode'})

with open('dirksuper.csv', 'w', newline='') as csvfile:
    superwriter =  csv.writer(csvfile, delimiter=';', quotechar='|', quoting=csv.QUOTE_MINIMAL)

    for number in range(0, len(streetdata)):
        streetdatapoint = streetdata[number].text
        zipdatapoint = zipdata[number].text
        street = streetdatapoint.split(' ', 1)[1]
        zipcode = ''
        city = ''

        if zipdatapoint[4] == ' ':
            zipcode = ' '.join([zipdatapoint.split(' ', 2)[0], zipdatapoint.split(' ', 2)[1]])
            city =  zipdatapoint.split(' ', 2)[2]
        else:
            zipcode = ' '.join([zipcode[0:5], zipcode[5:7]])
            city = zipdatapoint.split(' ', 1)[1]
        superwriter.writerow([street, zipcode, city])