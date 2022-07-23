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
db_database="FinWisev12"
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
            WHERE P."Type"='Bank' and P.converted=False
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


            
def Bank_results(driver):
    listofDicts=[]
    BankLabels=['title','StartPeriod_RemainingFacility','thisPeriod_IssuedFacilitiy','thisPeriod_SetteledFacility',
               'EnePeriod_RemainingFacility','thisPeriod_RevenueFromFacility','thisYear_RevenueFromFacility']
    BankLabesls_withModif=['title','StartPeriod_RemainingFacility','StartPeriod_RemainingFacility_modifications'
                           ,'StartPeriod_RemainingFacility_modified','thisPeriod_IssuedFacilitiy','thisPeriod_SetteledFacility',
                          'EnePeriod_RemainingFacility','PreviousPeriods_RevenueFromFacility',
                           'PreviousPeriods_RevenueFromFacility_modification','PreviousPeriods_RevenueFromFacility_modified',
                           'thisPeriod_RevenueFromFacility',
                           'thisYear_RevenueFromFacility']
    for i in range(len(driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucBank1_dgFacility"]//tr'))-1):
        rowitems=driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucBank1_dgFacility"]//tr['+str(i+1)+']/td')
        itemCounter=0 
        dictionary={}
        for t in rowitems:
            dictionary[BankLabels[itemCounter]]=get_true_value(t.text)
            #print(get_true_value(t.text))
            itemCounter=itemCounter+1
        listofDicts.append(dictionary)
    for i in range(len(driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucBank2_dgFacility"]//tr'))-1):
        rowitems=driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucBank2_dgFacility"]//tr['+str(i+1)+']/td')
        itemCounter=0 
        dictionary={}
        for t in rowitems:
            dictionary[BankLabesls_withModif[itemCounter]]=get_true_value(t.text)
            #print(get_true_value(t.text))
            itemCounter=itemCounter+1
        listofDicts.append(dictionary)
    BankDF=pd.DataFrame(listofDicts)
    for j in BankLabesls_withModif:
        if j not in BankDF.columns.tolist():
            BankDF[j]=0
    results={}
    BankDF.replace('ك','ک',regex=True,inplace=True)
    BankDF.replace('ي','ی',regex=True,inplace=True)
    results['Facilities']=BankDF
    #
    listofDicts=[]
    BankDepositLabels=['title','StartPeriod_Deposits','thisPeriod_IncomingDeposit','thisPeriod_setteledDeposits',
                                'EndPeriod_Deposits','thisPeriod_InterestOnDeposits','Total_InterestOnDeposits']
    
    BankDepositLabels_withModif=['title','StartPeriod_Deposits','StartPeriod_Deposits_modifications',
                                 'StartPeriod_Deposits_modified','thisPeriod_IncomingDeposit','thisPeriod_setteledDeposits',
                                'EndPeriod_Deposits','PreviousPeriods_InterestOnDeposits','PreviousPeriods_InterestOnDeposits_modifications'
                                 ,'PreviousPeriods_InterestOnDeposits_modified','thisPeriod_InterestOnDeposits','Total_InterestOnDeposits']
    for i in range(len(driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucBank1_dgDeposit"]//tr'))-1):
        rowitems=driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucBank1_dgDeposit"]//tr['+str(i+1)+']/td')
        itemCounter=0 
        dictionary={}
        for t in rowitems:
            dictionary[BankDepositLabels[itemCounter]]=get_true_value(t.text)
            #print(get_true_value(t.text))
            itemCounter=itemCounter+1
        listofDicts.append(dictionary)
    for i in range(len(driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucBank2_dgDeposit"]//tr'))-1):
        rowitems=driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucBank2_dgDeposit"]//tr['+str(i+1)+']/td')
        itemCounter=0 
        dictionary={}
        for t in rowitems:
            dictionary[BankDepositLabels_withModif[itemCounter]]=get_true_value(t.text)
            #print(get_true_value(t.text))
            itemCounter=itemCounter+1
        listofDicts.append(dictionary)
    DepositsDF=pd.DataFrame(listofDicts)
    for j in BankDepositLabels_withModif:
        if j not in DepositsDF.columns.tolist():
            DepositsDF[j]=0
    DepositsDF.replace('ك','ک',regex=True,inplace=True)
    DepositsDF.replace('ي','ی',regex=True,inplace=True)
    results['Deposits']=DepositsDF
    return results




def Insert_Bank(desc,titles,announcments,CID,Clink,DFResults):
    df1=DFResults['Facilities']
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
                IF NOT EXISTS (select from monthly."Bank_Monthly_Facilities" where "report_id"=%(report_id)s and "title"=%(title)s) THEN
                    INSERT INTO monthly."Bank_Monthly_Facilities"(
                    firm, report_id, period, "toDate", desc_modification, "desc_onePeriod", "desc_toDate", 
                    desc_title, "lastAnnouncement", "nextAnnouncement", reported_firm, 
                    title, "StartPeriod_RemainingFacility","StartPeriod_RemainingFacility_modifications",
                    "StartPeriod_RemainingFacility_modified","thisPeriod_IssuedFacilitiy","thisPeriod_SetteledFacility",
                    "EnePeriod_RemainingFacility","PreviousPeriods_RevenueFromFacility","PreviousPeriods_RevenueFromFacility_modification",
                    "PreviousPeriods_RevenueFromFacility_modified", "thisPeriod_RevenueFromFacility","thisYear_RevenueFromFacility")
                    VALUES ((select "ID" from "Publishers" where "persianName"=%(nemad)s), %(report_id)s, %(period)s, %(toDate)s,
                    %(desc_modif)s, %(desc_month)s, %(desc_year)s,
                    %(desc_title)s, %(Last)s, %(Next)s, %(firm_reporting)s,
                    %(title)s,%(StartPeriod_RemainingFacility)s,%(StartPeriod_RemainingFacility_modifications)s,
                    %(StartPeriod_RemainingFacility_modified)s,%(thisPeriod_IssuedFacilitiy)s,%(thisPeriod_SetteledFacility)s,
                    %(EnePeriod_RemainingFacility)s,%(PreviousPeriods_RevenueFromFacility)s,%(PreviousPeriods_RevenueFromFacility_modification)s,
                    %(PreviousPeriods_RevenueFromFacility_modified)s,%(thisPeriod_RevenueFromFacility)s,%(thisYear_RevenueFromFacility)s
                    );
                END IF;
            END
            $$ 

        """

        cursor.executemany(postgres_insert_query,dftemp4.to_dict(orient='records'))
        connection.commit()

    except(Exception, psycopg2.Error) as error:
            if(connection):
                print("Failed to Insert BankFacilities", error)
                log_it('Failed to Insert BankFacilities -'+str(CID))
    finally:
        if(connection):
            cursor.close()
            connection.close()
    ####
    dfdepos=DFResults['Deposits']
    descdf=pd.DataFrame([desc])
    titlesdf=pd.DataFrame([titles])
    titlesdf.columns=['nemad','period','toDate','firm_reporting']
    announcedf=pd.DataFrame([announcments])
    dfdepos['tmp'] = 1
    descdf['tmp'] = 1
    dfdepostemp1 = pd.merge(dfdepos, descdf, on=['tmp'])
    dfdepostemp1 = dfdepostemp1.drop('tmp', axis=1)
    dfdepostemp1['tmp'] = 1
    titlesdf['tmp'] = 1
    dfdepostemp2= pd.merge(dfdepostemp1, titlesdf, on=['tmp'])
    dfdepostemp2 = dfdepostemp2.drop('tmp', axis=1)
    dfdepostemp2['tmp'] = 1
    announcedf['tmp'] = 1
    dfdepostemp3= pd.merge(dfdepostemp2, announcedf, on=['tmp'])
    dfdepostemp3 = dfdepostemp3.drop('tmp', axis=1)
    dfdepostemp3['report_id']=CID
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
                IF NOT EXISTS (select from monthly."Bank_Monthly_Deposits" where "report_id"=%(report_id)s and "title"=%(title)s) THEN
                    INSERT INTO monthly."Bank_Monthly_Deposits"(
                    firm, report_id, period, "toDate",
                    desc_modification, "desc_onePeriod", 
                    "desc_toDate", desc_title, "lastAnnouncement", "nextAnnouncement",
                    reported_firm, title, "StartPeriod_Deposits", "thisPeriod_IncomingDeposit",
                    "thisPeriod_setteledDeposits", "EndPeriod_Deposits", "thisPeriod_InterestOnDeposits",
                    "Total_InterestOnDeposits", "StartPeriod_Deposits_modifications", "StartPeriod_Deposits_modified",
                    "PreviousPeriods_InterestOnDeposits", "PreviousPeriods_InterestOnDeposits_modifications", 
                    "PreviousPeriods_InterestOnDeposits_modified")
                    VALUES ((select "ID" from "Publishers" where "persianName"=%(nemad)s), %(report_id)s, %(period)s, %(toDate)s,
                    %(desc_modif)s, %(desc_month)s, %(desc_year)s,
                    %(desc_title)s, %(Last)s, %(Next)s, %(firm_reporting)s,%(title)s
                    ,%(StartPeriod_Deposits)s,%(thisPeriod_IncomingDeposit)s,
                    %(thisPeriod_setteledDeposits)s,%(EndPeriod_Deposits)s,
                    %(thisPeriod_InterestOnDeposits)s,%(Total_InterestOnDeposits)s
                    ,%(StartPeriod_Deposits_modifications)s,%(StartPeriod_Deposits_modified)s,
                    %(PreviousPeriods_InterestOnDeposits)s,%(PreviousPeriods_InterestOnDeposits_modifications)s,
                    %(PreviousPeriods_InterestOnDeposits_modified)s);

                END IF;
            END
            $$ 

        """

        cursor.executemany(postgres_insert_query,dfdepostemp3.to_dict(orient='records'))
        connection.commit()

    except(Exception, psycopg2.Error) as error:
            if(connection):
                print("Failed to Insert BankDeposits", error)
                log_it('Failed to Insert BankDeposits -'+str(CID))
    finally:
        if(connection):
            cursor.close()
            connection.close()
    try:
        connection = psycopg2.connect(user=db_username,
                                      password=db_pass,
                                      host=db_host,
                                      port=db_port,
                                      database=db_database)
        cursor = connection.cursor()
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
                print("Failed to Insert BankDeposits", error)
                log_it('Failed to Insert BankDeposits -'+str(CID))
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
def RUN():
    driver = webdriver.Chrome()
    driver.maximize_window()                 
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
            Insert_Bank(get_description_product_service(driver),titles,get_announcments(driver),CodalRaw_ID,CodalRaw_links,Bank_results(driver))   
            counter=counter+1
            percentage=(counter*100)/AllData
            print("{0:.2f}".format(percentage))
            
        except (Exception, psycopg2.Error) as error :
            print(error)
            print(CodalRaw_links)
            continue
    driver.quit()
RUN()