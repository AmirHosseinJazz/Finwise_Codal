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

       select P."report_ID",R."HtmlUrl"
            from monthly."PreMonthly" as P inner join codalraw."allrawReports" as R on P."report_ID"=R."TracingNo"
            WHERE P."Type"='Insurance' and P.converted=False""", connection)
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read links", error)
    finally:
            if(connection):
                cursor.close()
                connection.close()
def roundTheFloats(x):
    if(type(get_true_value(x))==float):
        return int(round(get_true_value(x)))
    else:
        return x
def get_announcments(driver):
    results={}
    if check_exists_by_xpath(driver,'//a[@id="ctl00_cphBody_ucNavigateToNextPrevLetter_hlPrevVersion"]'):
        linktoprevious=driver.find_element_by_xpath('//a[@id="ctl00_cphBody_ucNavigateToNextPrevLetter_hlPrevVersion"]').text
        previousAnnouncment=[int(s) for s in str.split(linktoprevious) if s.isdigit()][0]
    else:
        previousAnnouncment=0
    if check_exists_by_xpath(driver,'//a[@id="ctl00_cphBody_ucNavigateToNextPrevLetter_hlNewVersion"]'):
        linktonext=driver.find_element_by_xpath('//a[@id="ctl00_cphBody_ucNavigateToNextPrevLetter_hlNewVersion"]').text
        nextAnnouncment=[int(s) for s in str.split(linktonext) if s.isdigit()][0]
    else:
        nextAnnouncment=0
    results['Last']=previousAnnouncment
    results['Next']=nextAnnouncment
    return results
def get_titlebox(driver):
    results=[]
    titlebox=driver.find_elements_by_xpath('//div[@class="symbol_and_name"]/div/div')
    i=0
    for t in titlebox:
        if(i==0):
            header_name=(t.text.split(':\n')[1])
            header_name2 = ''.join([i for i in header_name if not i.isdigit()])
            header_name2 = header_name2.replace('ك','ک')
            header_name2 = header_name2.replace('ي','ی')
            header_name=header_name2

        if(i==2):
            header_Ticker=(t.text.split(':\n')[1])
            header_Ticker_2 = ''.join([i for i in header_Ticker if not i.isdigit()])
            header_Ticker_2 = header_Ticker_2.replace('ك','ک')
            header_Ticker_2 = header_Ticker_2.replace('ي','ی')
            header_Ticker=header_Ticker_2
            if('(' in header_Ticker_2):
                header_Ticker_2=header_Ticker_2.split('(')[0]
            results.append(header_Ticker_2)
        if(i==5):
            header_PeriodLength=[int(s) for s in str.split(t.text) if s.isdigit()][0]
            text=t.text
            if('(حسابرسی نشده)') in text:
                text=text.replace(' (حسابرسی نشده)','')
            if('(حسابرسی شده)') in text:
                text=text.replace(' (حسابرسی شده)','')
            header_until=text[-10:]
            results.append(header_PeriodLength)
            results.append(header_until)
            results.append(header_name)
        if(i==7):
            results.append(t.text.split('/')[1])
        i=i+1
    return results
def log_it(text):
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        postgres_insert_query = """
          
          INSERT INTO monthly._log(
            date, text)
                VALUES (%s, %s);
        """
    
        record_to_insert = (str(datetime.datetime.now()),text)
        
        cursor.execute(postgres_insert_query, record_to_insert)
        connection.commit()
    except(Exception, psycopg2.Error) as error:
        if(connection):
            print("Failed to insert log", error)
    finally:
        if(connection):
            cursor.close()
            connection.close()                
def isfloat(x):
    try:
        a = float(x)
    except ValueError:
        return False
    else:
        return True
def isint(x):
    try:
        a = float(x)
        b = int(a)
    except ValueError:
        return False
    else:
        return a == b
def get_true_value(x):
    x=str(x)
    negative=False
    if(',' in x):
        x=x.replace(',','')
    if('(' in x and ')' in x ):
        x=x.replace(')','')
        x=x.replace('(','')
        negative=True
    if isint(x):
        x=x.split('.')[0]
        if negative:
            x=int(x)*-1
        else:
            x=int(x)
    else:
        if isfloat(x):
            if negative:
                x=float(x)*-1
            else:
                x=float(x)
    return x    
def check_exists_by_xpath(driver,xpath):
    try:
        driver.find_element_by_xpath(xpath)
    except NoSuchElementException:
        return False
    return True
def find_nth(haystack, needle, n):
    start = haystack.find(needle)
    while start >= 0 and n > 1:
        start = haystack.find(needle, start+len(needle))
        n -= 1
    return start
def get_description_product_service(driver):
    results={}
    if check_exists_by_xpath(driver,'//span[@id="ctl00_cphBody_ucNavigateToNextPrevLetter_lblCorrectionDesc"]'):
        desc_title=driver.find_element_by_xpath('//span[@id="ctl00_cphBody_ucNavigateToNextPrevLetter_lblCorrectionDesc"]').text
    else:
        desc_title=""
    results['desc_title']=desc_title
    if check_exists_by_xpath(driver,'//span[@id="txbDescMonth"]'):
        desc_month=driver.find_element_by_xpath('//span[@id="txbDescMonth"]').text
    else:
        desc_month=""
    results['desc_month']=desc_month
    if check_exists_by_xpath(driver,'//span[@id="txbDescYear"]'):
        desc_year=driver.find_element_by_xpath('//span[@id="txbDescYear"]').text
    else:
        desc_year=""
    results['desc_year']=desc_year
    if check_exists_by_xpath(driver,'//span[@id="txtCorrectionComment"]'):
        desc_modif=driver.find_element_by_xpath('//span[@id="txtCorrectionComment"]').text
    else:
        desc_modif=""
    results['desc_modif']=desc_modif
    return results

def Inusrance_results(driver):
    listofDicts=[]
    InsuranceLabels_Modified=['InsuranceField','PreviousPeriods_IssuedInsurance_Amount','PreviousPeriods_IssuedInusrance_Share',
                     'PreviousPeriods_DamageRepaid_Amount','PreviousPeriods_DamageRepaid_Share',
                     'Modification_IssuedInsurance_Amount','Modification_DamageRepaid_Amount',
                     'PreviousModified_IssuedInsurance_Amount','PreviousModified_IssuedInusrance_Share','PreviousModified_DamageRepaid_Amount','PreviousModified_DamageRepaid_Share',
                     'ThisPeriod_IssuedInsurance_Amount','ThisPeriod_IssuedInusrance_Share','ThisPeriod_DamageRepaid_Amount','ThisPeriod_DamageRepaid_Share',
                     'Total_IssuedInsurance_Amount','Total_IssuedInusrance_Share','Total_DamageRepaid_Amount','Total_DamageRepaid_Share',             
                    ]
    InsuranceLabels=['InsuranceField','ThisPeriod_IssuedInsurance_count'
                     'ThisPeriod_IssuedInsurance_Amount','ThisPeriod_IssuedInusrance_Share',
                     'ThisPeriod_DamageRepaid_count','ThisPeriod_DamageRepaid_Amount','ThisPeriod_DamageRepaid_Share',
                     'Total_IssuedInsurance_count','Total_IssuedInsurance_Amount',
                     'Total_IssuedInusrance_Share','Total_DamageRepaid_count','Total_DamageRepaid_Amount','Total_DamageRepaid_Share','Empty','Empty2']
    for i in range(len(driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucInsurance2_dgInsurance"]//tr'))-1):
        rowitems=driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucInsurance2_dgInsurance"]//tr['+str(i+1)+']/td')
        itemCounter=0 
        dictionary={}
        for t in rowitems:
            dictionary[InsuranceLabels_Modified[itemCounter]]=get_true_value(t.text)
            #print(get_true_value(t.text))
            itemCounter=itemCounter+1
        listofDicts.append(dictionary)
    for i in range(len(driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucInsurance1_dgInsurance"]//tr'))-1):
        rowitems=driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucInsurance1_dgInsurance"]//tr['+str(i+1)+']/td')
        itemCounter=0 
        dictionary={}
        for t in rowitems:
            #print(itemCounter)
            #print(get_true_value(t.text))
            dictionary[InsuranceLabels[itemCounter]]=get_true_value(t.text)
            itemCounter=itemCounter+1
        listofDicts.append(dictionary)
    
    InsuranceDF=pd.DataFrame(listofDicts)
    for j in InsuranceLabels_Modified:
        if j not in InsuranceDF.columns.tolist():
            InsuranceDF[j]=0
    Differences=['ThisPeriod_IssuedInsurance_count','ThisPeriod_DamageRepaid_count','Total_IssuedInsurance_count','Total_DamageRepaid_count','Empty','Empty2']
    for delcol in Differences:
        if delcol in InsuranceDF.columns.tolist():
            InsuranceDF.drop(columns=[delcol],inplace=True)
    results={}
    InsuranceDF.replace('ك','ک',regex=True,inplace=True)
    InsuranceDF.replace('ي','ی',regex=True,inplace=True)
    
    InsuranceDF.dropna(subset=['InsuranceField','PreviousPeriods_IssuedInsurance_Amount'],inplace=True)
    InsuranceDF.fillna(0,inplace=True)
    results['Insurance']=InsuranceDF
    return results

    
def Insert_Insurance(desc,titles,announcments,CID,Clink,DFResults):
    df1=DFResults['Insurance']
    descdf=pd.DataFrame([desc])
    titlesdf=pd.DataFrame([titles])
    titlesdf.columns=['nemad','period','toDate','firm_reporting']
    announcedf=pd.DataFrame([announcments])
    df1['tmp'] = 1
    descdf['tmp'] = 1
    dftemp1 = pd.merge(df1, descdf, on=['tmp'])
    dftemp1 = dftemp1.drop('tmp', axis=1)
    dftemp1['tmp'] = 1
    titlesdf['tmp'] = 1
    dftemp2= pd.merge(dftemp1, titlesdf, on=['tmp'])
    dftemp3 = dftemp2.drop('tmp', axis=1)
    dftemp3['tmp'] = 1
    announcedf['tmp'] = 1
    dftemp4= pd.merge(dftemp3, announcedf, on=['tmp'])
    dftemp4 = dftemp4.drop('tmp', axis=1)
    dftemp4['report_id']=CID
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
                IF NOT EXISTS (select from monthly."Insurance_Monthly" where "report_id"=%(report_id)s and "InsuranceField"=%(InsuranceField)s) THEN
                    INSERT INTO monthly."Insurance_Monthly"(
                    firm, report_id, reported_firm, period, "toDate",
                    desc_modification, "desc_onePeriod", "desc_toDate",
                    desc_title, "lastAnnouncement", "NextAnnouncement", 
                    "InsuranceField", "PreviousPeriods_IssuedInsurance_Amount",
                    "PreviousPeriods_IssuedInusrance_Share", "PreviousPeriods_DamageRepaid_Amount", 
                    "PreviousPeriods_DamageRepaid_Share", "Modification_IssuedInsurance_Amount", 
                    "Modification_DamageRepaid_Amount", "PreviousModified_IssuedInsurance_Amount", 
                    "PreviousModified_IssuedInusrance_Share", "PreviousModified_DamageRepaid_Amount",
                    "PreviousModified_DamageRepaid_Share", "ThisPeriod_IssuedInsurance_Amount",
                    "ThisPeriod_IssuedInusrance_Share", "ThisPeriod_DamageRepaid_Amount",
                    "ThisPeriod_DamageRepaid_Share", "Total_IssuedInsurance_Amount", 
                    "Total_IssuedInusrance_Share", "Total_DamageRepaid_Amount", "Total_DamageRepaid_Share")
                    VALUES ((select "ID" from "Entity" where ticker=%(nemad)s), %(report_id)s, %(firm_reporting)s,
                    %(period)s, %(toDate)s,
                    %(desc_modif)s, %(desc_month)s, %(desc_year)s,
                    %(desc_title)s, %(Last)s, %(Next)s, 
                    %(InsuranceField)s,%(PreviousPeriods_IssuedInsurance_Amount)s,
                    %(PreviousPeriods_IssuedInusrance_Share)s,%(PreviousPeriods_DamageRepaid_Amount)s,
                    %(PreviousPeriods_DamageRepaid_Share)s,%(Modification_IssuedInsurance_Amount)s,
                    %(Modification_DamageRepaid_Amount)s,%(PreviousModified_IssuedInsurance_Amount)s,
                    %(PreviousModified_IssuedInusrance_Share)s,%(PreviousModified_DamageRepaid_Amount)s,
                    %(PreviousModified_DamageRepaid_Share)s,%(ThisPeriod_IssuedInsurance_Amount)s,
                    %(ThisPeriod_IssuedInusrance_Share)s,%(ThisPeriod_DamageRepaid_Amount)s,%(ThisPeriod_DamageRepaid_Share)s,
                    %(Total_IssuedInsurance_Amount)s,%(Total_IssuedInusrance_Share)s,
                    %(Total_DamageRepaid_Amount)s,%(Total_DamageRepaid_Share)s);
                END IF;
            END
            $$ 

        """

        cursor.executemany(postgres_insert_query,dftemp4.to_dict(orient='records'))
        connection.commit()
        postgres_insert_query3 = """
        UPDATE monthly."PreMonthly"
        SET converted=True
        WHERE "report_ID"=%s
        """
        record_to_insert3=([CID])
        cursor.execute(postgres_insert_query3, record_to_insert3)
        connection.commit()
        print(str(Clink)+'  '+'--Done')
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print("Failed to Insert Insurance", error)
                log_it('Failed to Insert Insurance -'+str(CID))
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
            header_name=(t.text.split(':\n')[1])
            header_name2 = ''.join([i for i in header_name if not i.isdigit()])
            header_name2 = header_name2.replace('ك','ک')
            header_name2 = header_name2.replace('ي','ی')
            header_name=header_name2

        if(i==2):
            header_Ticker=(t.text.split(':\n')[1])
            header_Ticker_2 = ''.join([i for i in header_Ticker if not i.isdigit()])
            header_Ticker_2 = header_Ticker_2.replace('ك','ک')
            header_Ticker_2 = header_Ticker_2.replace('ي','ی')
            header_Ticker=header_Ticker_2
            if('(' in header_Ticker_2):
                header_Ticker_2=header_Ticker_2.split('(')[0]
            results.append(header_Ticker_2)
        if(i==5):
            header_PeriodLength=[int(s) for s in str.split(t.text) if s.isdigit()][0]
            text=t.text
            if('(حسابرسی نشده)') in text:
                text=text.replace(' (حسابرسی نشده)','')
            if('(حسابرسی شده)') in text:
                text=text.replace(' (حسابرسی شده)','')
            header_until=text[-10:]
            results.append(header_PeriodLength)
            results.append(header_until)
            results.append(header_name)
        i=i+1
    return results         
def RUN(driver):
    # driver = webdriver.Chrome()
    # driver.maximize_window()                 
    df=get_unconverted()
    AllData=len(df.index)
    counter=0
    for index,row in df.iterrows():
        try:
            CodalRaw_ID=int(row['report_ID'])
            CodalRaw_links=row['HtmlUrl']
            #print(CodalRaw_ID)
            driver.get('http://codal.ir'+CodalRaw_links)
            time.sleep(3)
            driver.execute_script("document.body.style.zoom='100%';document.body.style.zoom='50%'")
            titles=get_titlebox(driver)
            Insert_Insurance(get_description_product_service(driver),titles,get_announcments(driver),CodalRaw_ID,CodalRaw_links,Inusrance_results(driver))   
            
            counter=counter+1
            percentage=(counter*100)/AllData
            print("{0:.2f}".format(percentage))
            
        except (Exception, psycopg2.Error) as error :
            print(error)
            print(CodalRaw_links)
            continue