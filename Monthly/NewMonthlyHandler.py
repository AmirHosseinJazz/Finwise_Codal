import time
from selenium import webdriver
import time
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException  
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
import Bank
import Construction
import Insurance
# import Investment
import Leasing
# import Service
import Productions3
import Productions2
# chrome_options = Options()  
# chrome_options.add_argument("--headless")  
# driver = webdriver.Chrome(chrome_options=chrome_options)
# driver.maximize_window()  
#####
print("Bank Monthly Initialize ...")
Bank.RUN()
time.sleep(5)
print("Construction Initialize ...")
Construction.RUN()
time.sleep(3)
print("Insurance Initialize ...")
Insurance.RUN()
time.sleep(3)
# print("Investment Initialize ...")
# Investment.RUN(driver)
# time.sleep(3)
print("Leasing Initialize ...")
Leasing.RUN()
time.sleep(3)
# print("Service Initialize ...")
# Service.RUN(driver)
# time.sleep(3)
print('Production Initialize 1...')
Productions2.RUN()
time.sleep(3)
print('Production Initialize 2...')
Productions3.RUN()
time.sleep(3)

###
# driver.quit()
