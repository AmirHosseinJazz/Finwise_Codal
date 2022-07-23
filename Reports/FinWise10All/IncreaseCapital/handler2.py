import time
from selenium import webdriver
import time
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException  
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
import N_60
import N_61
import N_62
import N_63
import N_64
import N_65
import N_66
import N_67
import N_70
import N_71
import N_72
import N_73

###
chrome_options = Options()  
chrome_options.add_argument("--headless")  
driver = webdriver.Chrome(chrome_options=chrome_options)
# driver=webdriver.Chrome()
driver.maximize_window()  
#####
print('N60')
N_60.RUN(driver)
time.sleep(3)
print('N61')
N_61.RUN(driver)
time.sleep(3)
print('N62')
N_62.RUN(driver)
time.sleep(3)
print('N63')
N_63.RUN(driver)
time.sleep(3)
print('N64')
N_64.RUN(driver)
time.sleep(3)
print('N65')
N_65.RUN(driver)
time.sleep(3)
print('N66')
N_66.RUN(driver)
time.sleep(3)
print('N67')
N_67.RUN(driver)
time.sleep(3)
print('N70')
N_70.RUN(driver)
time.sleep(3)
print('N71')
N_71.RUN(driver)
time.sleep(3)
print('N72')
N_72.RUN(driver)
time.sleep(3)
print('N73')
N_73.RUN(driver)
time.sleep(3)
###
driver.quit()
