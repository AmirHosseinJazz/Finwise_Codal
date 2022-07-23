import pandas as pd
import json
import numpy as np
from khayyam import *
from selenium import webdriver
import time
from selenium.webdriver.common.keys import Keys
from furl import furl
import psycopg2
import datetime
import pandas.io.sql as psql
from selenium.common.exceptions import NoSuchElementException  
from selenium.webdriver.support.ui import Select
import requests
db_username="Jay"
db_pass="Mehrad1"
db_host="127.0.0.1"
db_port="5432"
db_database="FinWisev10"
def check_type(driver):
    Type='Other'
    typelist=['Other','1']
    if check_exists_by_xpath(driver,'//table[@id="ctl00_cphBody_ucProduct2_Table1"]'):
        Type='Product'
        prod_type=1
        typelist=['Product',1]
    if check_exists_by_xpath(driver,'//table[@id="ctl00_cphBody_ucProduct1_Table1"]'):
        Type='Product'
        prod_type=2
        typelist=['Product',2]
    if check_exists_by_xpath(driver,'//table[@id="ctl00_cphBody_ucService1_Table1"]'):
        Type='Service'
        typelist=['Service']
    else:
        if check_exists_by_xpath(driver,'//*[@id="ctl00_h1MasterTitle"]'):
            mastertitle=driver.find_element_by_xpath('//*[@id="ctl00_h1MasterTitle"]').text
            if mastertitle=='صورت وضعیت پورتفوی':
                Type='Investment'
                typelist=['Investment']
    if (Type=='Other'):
        wholefile=str(driver.page_source)
        if('"ProductMonthlyActivityDesc1","cells":' in wholefile):
            Type='Product'
            typelist=['Product']
    if (Type=='Other'):
        wholefile=str(driver.page_source)
        if('ProductionAndSales' in wholefile):
            Type='Product'
            typelist=['Product']
    if(Type=='Other'):
        wholefile=str(driver.page_source)
        if('مقدار/تعداد تولید') in wholefile:
            Type='Product'
            prod_type=3
            typelist=['Product',3]
    if check_exists_by_xpath(driver,'//*[contains(@id, "Building")]'):
        Type='Construction'
        typelist=['Construction']
    if check_exists_by_xpath(driver,'//*[contains(@id, "ucLeasing")]'):
        Type='Leasing'
        typelist=['Leasing']
    if check_exists_by_xpath(driver,'//*[contains(@id, "ucBank")]'):
        Type='Bank'
        typelist=['Bank']
    if check_exists_by_xpath(driver,'//*[contains(@id, "Insurance")]'):
        Type='Insurance'
        typelist=['Insurance']
    return typelist
def check_exists_by_xpath(driver,xpath):
    try:
        driver.find_element_by_xpath(xpath)
    except NoSuchElementException:
        return False
    return True
def get_unconverted():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""

       select * from codalraw."allrawReports" where ("LetterCode"='ن-۳۰' or "LetterCode"='ن-۳۱')  
 and "TracingNo" not in (Select "report_ID" from monthly."PreMonthly") and "HasHtml"=True  and "HtmlUrl"!='' and "Available"=True
"""

                           , connection)
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read links", error)
    finally:
            if(connection):
                cursor.close()
                connection.close()
def get_titlebox(driver):
    results=[]
    titlebox=driver.find_elements_by_xpath('//div[@class="symbol_and_name"]/div/div')
    i=0
    for t in titlebox:
        if(i==0):
            try:
                header_name=(t.text.split(':\n')[1])
                header_name2 = ''.join([i for i in header_name if not i.isdigit()])
                header_name2 = header_name2.replace('ك','ک')
                header_name2 = header_name2.replace('ي','ی')
                header_name=header_name2
            except:
                header_name='NoReportedFirm'

        if(i==2):
            header_Ticker=(t.text.split(':\n')[1])
            header_Ticker_2 = ''.join([i for i in header_Ticker if not i.isdigit()])
            header_Ticker_2 = header_Ticker_2.replace('ك','ک')
            header_Ticker_2 = header_Ticker_2.replace('ي','ی')
            header_Ticker=header_Ticker_2
            if('(' in header_Ticker_2):
                header_Ticker_2=header_Ticker_2.split('(')[0]
            results.append(header_Ticker_2)
        if(i==4):
            try:
                header_ISIC=(t.text.split(':\n')[1])
                results.append(header_ISIC)
            except:
                results.append('')
        if(i==5):
            try:
                header_PeriodLength=[int(s) for s in str.split(t.text) if s.isdigit()][0]
            except:
                header_PeriodLength=''
            
            try:
                text=t.text
                if('(حسابرسی نشده)') in text:
                    text=text.replace(' (حسابرسی نشده)','')
                if('(حسابرسی شده)') in text:
                    text=text.replace(' (حسابرسی شده)','')
                header_until=text[-10:]
            except:
                header_until=''
            results.append(header_PeriodLength)
            results.append(header_until)
            results.append(header_name)
        if(i==7):
            try:
                results.append(t.text.split('/')[1])
            except:
                results.append('')
        i=i+1
    return results
def InsertPreMonth(Dict):
    try:

        connection = psycopg2.connect(user=db_username,
                                      password=db_pass,
                                      host=db_host,
                                      port=db_port,
                                      database=db_database)
        cursor = connection.cursor()
        postgres_insert_query = """
            DO 
            $$
                BEGIN
                    IF NOT EXISTS (select from   monthly."PreMonthly" where "report_ID"=%(report_ID)s) THEN
                        INSERT INTO monthly."PreMonthly"(
                        "report_ID", "Type", reported_firm, firm, "ISICcode", duration, "toDate", "Fiscal")
                        VALUES ( %(report_ID)s,%(Type)s,%(reportedFirm)s,(select "ID" from "Entity" where ticker=%(ticker)s),
                        %(ISIC)s,%(Duration)s,
                        %(toDate)s,%(Fiscal)s
                        );
                    END IF;
                END
            $$ 

        """

        cursor.execute(postgres_insert_query,Dict)
        connection.commit()
        print(str(Dict['report_ID'])+'  '+'--Done')
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print("Failed to Insert PreMonthly ", error)
                #log_it('Failed to Insert OrgClar_S6 -'+str(row['report_id']))
    finally:
        if(connection):
            cursor.close()
            connection.close() 
def UpdateError(driver,CodalRaw_ID):
    Error=False
    if check_exists_by_xpath(driver,'//*[text()="متاسفانه سیستم با خطا مواجه شده است."]'):
        Error=True
    if check_exists_by_xpath(driver,'//*[@id="Table2"]//span[text()="ضمائم"]'):
        Error=True
    if driver.page_source=='<html><head></head><body style="zoom: 50%;"></body></html>':
        Error=True
    if driver.page_source=='<html xmlns="http://www.w3.org/1999/xhtml"><head></head><body style="zoom: 50%;"></body></html>':
        Error=True
    if Error:
        InsertError(CodalRaw_ID)
        return False
    else:
        return True
def InsertError(CodalRaw_ID):
    try:
        connection = psycopg2.connect(user=db_username,
                                    password=db_pass,
                                    host=db_host,
                                    port=db_port,
                                    database=db_database)
        cursor = connection.cursor()
        postgres_insert_query = """
        
        UPDATE codalraw."allrawReports" SET "Available"=False WHERE "TracingNo"=%s

        """
        cursor.execute(postgres_insert_query,(CodalRaw_ID,))
        connection.commit() 
        print("Loading Error on Report")
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print("Failed to Update Error sheets", error)
                log_it('Failed to Update Error sheets -')
    finally:
        if(connection):
            cursor.close()
            connection.close()                               

def RUN(driver):
    df=get_unconverted()
    AllData=len(df.index)
    counter=0
    for index,row in df.iterrows():
        try:
            CodalRaw_ID=int(row['TracingNo'])
            CodalRaw_links=row['HtmlUrl']
            #print(CodalRaw_ID)
            driver.get('http://codal.ir'+CodalRaw_links)
            time.sleep(3)
            driver.execute_script("document.body.style.zoom='100%';document.body.style.zoom='50%'")
            if UpdateError(driver,CodalRaw_ID):
                Type=check_type(driver)
                #print(Type)
                titles=get_titlebox(driver)
                #print(titles)
                Identifier={}
                Identifier['Type']=Type[0]
                Identifier['ticker']=titles[0]
                Identifier['ISIC']=titles[1]
                Identifier['Duration']=titles[2]
                Identifier['toDate']=titles[3]
                Identifier['reportedFirm']=titles[4]
                Identifier['Fiscal']=titles[5]
                Identifier['report_ID']=CodalRaw_ID
                InsertPreMonth(Identifier)
                counter=counter+1
                percentage=(counter*100)/AllData
                print("{0:.2f}".format(percentage))
        except (Exception, psycopg2.Error) as error :
            print(error)
            print(CodalRaw_links)
            continue    