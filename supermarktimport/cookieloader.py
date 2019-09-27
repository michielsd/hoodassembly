import pickle
import selenium.webdriver 

driver = selenium.webdriver.Chrome()
driver.get("https://www.reclamefolder.nl")
pickle.dump( driver.get_cookies() , open("cookies.pkl","wb"))