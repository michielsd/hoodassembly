import csv
from bs4 import BeautifulSoup
from selenium import webdriver

"""
missing:

Amazing Oriental
Deen +
Dirk +
Jumbo +



"""



urllist = [
    'https://www.reclamefolder.nl/albert-heijn/winkels',
    'https://www.reclamefolder.nl/aldi/winkels',
    'https://www.reclamefolder.nl/coop/winkels',
    'https://www.reclamefolder.nl/dekamarkt/winkels',
    'https://www.reclamefolder.nl/ekoplaza/winkels',
    'https://www.reclamefolder.nl/lidl/winkels',
    'https://www.reclamefolder.nl/plus/winkels',
    'https://www.reclamefolder.nl/spar/winkels',
]

with open('supers.csv', 'w', newline='') as csvfile:
    superwriter =  csv.writer(csvfile, delimiter=';', quotechar='|', quoting=csv.QUOTE_MINIMAL)

    for url in urllist:
        driver = webdriver.Chrome()
        driver.get(url)

        driver.find_element_by_xpath('//html/body/div[4]/div[2]/form/button').click()
        driver.set_page_load_timeout(20)

        soup = BeautifulSoup(driver.page_source, "lxml")

        storecards = soup.find_all('div', {'class': 'StyledStoreCard-sc-uafvh9 ghCwhJ'})

        driver.close()

        storelist = []
        for card in storecards:
            storename = card.find('div', {'class': 'Name'}).text
            storeaddress = card.find('div', {'class': 'Address'}).text
            storepostcal = card.find('div', {'class': 'PostcalCodeCity'}).text
            postcalsplit = storepostcal.split(',')
            storelist.append([storename, storeaddress, postcalsplit[0], postcalsplit[1]])

        for store in storelist:
            superwriter.writerow(store)