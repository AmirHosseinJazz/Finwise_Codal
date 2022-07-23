import time
from selenium import webdriver
import time
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException  
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
import FB1
import FB2
import FB3
import FB4
import FB5
import FB7
import FB8
# import FB9
#
chrome_options = Options()  
chrome_options.add_argument("--headless")  
driver = webdriver.Chrome(chrome_options=chrome_options)
#driver = webdriver.Chrome()
driver.maximize_window()  
#####
print('FB1')
FB1.RUN(driver)
time.sleep(3)
print('FB2')
FB2.RUN(driver)
time.sleep(3)
print('FB3')
FB3.RUN(driver)
time.sleep(3)
print('FB4')
FB4.RUN(driver)
time.sleep(3)
print('FB5')
FB5.RUN(driver)
time.sleep(3)
# print('FB9')
# FB9.RUN(driver)
# time.sleep(3)
print('FB7')
FB7.RUN(driver)
time.sleep(3)
print('FB8')
FB8.RUN(driver)
time.sleep(3)
###
driver.quit()
