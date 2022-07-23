import time
from selenium import webdriver
import time
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException  
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
import N_20
import N_21
import N_22
import N_24
import N_25
import N_26
###
chrome_options = Options()  
chrome_options.add_argument("--headless")  
driver = webdriver.Chrome(chrome_options=chrome_options)
# driver=webdriver.Chrome()
driver.maximize_window()   
#####
print('N20')
N_20.RUN(driver)
time.sleep(3)
print('N21')
N_21.RUN(driver)
time.sleep(3)
print('N22')
N_22.RUN(driver)
time.sleep(3)
print('N24')
N_24.RUN(driver)
time.sleep(3)
print('N25')
N_25.RUN(driver)
time.sleep(3)
print('N26')
N_26.RUN(driver)
time.sleep(3)
###
driver.quit()
