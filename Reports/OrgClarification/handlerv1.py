import time
from selenium import webdriver
import time
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException  
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
import N_41
import N_45
import S_5
import S_6
import S_7
import S_8
import S_9
import S_12
import S_13
import S_14
import S_15
###
chrome_options = Options()  
chrome_options.add_argument("--headless")  
driver = webdriver.Chrome(chrome_options=chrome_options)
#driver = webdriver.Chrome()
driver.maximize_window()  
#####
print('N41')
#N_41.RUN(driver)
time.sleep(3)
print('N45')
# N_45.RUN(driver)
time.sleep(3)
print('S5')
S_5.RUN(driver)
time.sleep(3)
print('S6')
S_6.RUN(driver)
time.sleep(3)
print('S7')
S_7.RUN(driver)
time.sleep(3)
print('S8')
S_8.RUN(driver)
time.sleep(3)
print('S9')
S_9.RUN(driver)
time.sleep(3)
print('S12')
S_12.RUN(driver)
time.sleep(3)
print('S13')
S_13.RUN(driver)
time.sleep(3)
print('S14')
S_14.RUN(driver)
time.sleep(3)
print('S15')
S_15.RUN(driver)
time.sleep(3)
###
driver.quit()
