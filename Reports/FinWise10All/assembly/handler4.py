import time
from selenium import webdriver
import time
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException  
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
import N_50
import N_51
import N_52
import N_55
import N_57
import N_58

###
chrome_options = Options()  
chrome_options.add_argument("--headless")  
driver = webdriver.Chrome(chrome_options=chrome_options)
# driver=webdriver.Chrome()
driver.maximize_window()  
#####
print('N50')
N_50.RUN(driver)
time.sleep(3)
print('N51')
N_51.RUN(driver)
time.sleep(3)
print('N52')
N_52.RUN(driver)
time.sleep(3)
print('N55')
N_55.RUN(driver)
time.sleep(3)
print('N57')
N_57.RUN(driver)
time.sleep(3)
print('N58')
N_58.RUN(driver)
time.sleep(3)
###
driver.quit()
