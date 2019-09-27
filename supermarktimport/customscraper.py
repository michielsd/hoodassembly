import csv
from bs4 import BeautifulSoup
from selenium import webdriver



with open('dirksuper.csv', 'w', newline='') as csvfile:
    superwriter =  csv.writer(csvfile, delimiter=';', quotechar='|', quoting=csv.QUOTE_MINIMAL)

    url = "https://www.dirk.nl/winkels/"

    driver = webdriver.Chrome()
    driver.get(url)

    driver.set_page_load_timeout(20)

    soup = BeautifulSoup(driver.page_source, "lxml")
    print(soup)

    driver.find_element_by_xpath('//*[@id="app"]/div[6]/div/div[2]/button[2]').click()
    driver.set_page_load_timeout(20)

    soup = BeautifulSoup(driver.page_source, "lxml")

    storecards = soup.find_all('table', {'class': 'showOnlyActiveShop'})

    driver.close()

    print(soup)

    for store in storecards:
        print(store)

    """
    storelist = []
    for card in storecards:
        storename = card.find('div', {'class': 'Name'}).text
        storeaddress = card.find('div', {'class': 'Address'}).text
        storepostcal = card.find('div', {'class': 'PostcalCodeCity'}).text
        postcalsplit = storepostcal.split(',')
        storelist.append([storename, storeaddress, postcalsplit[0], postcalsplit[1]])

    for store in storelist:
        superwriter.writerow(store)
    """