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
            WHERE P."Type"='Leasing' and P.converted=False"""

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



def Leasing_results(driver):
    listofDicts=[]
    LeasingLabels=['Title','thisPeriod_Amount','Total_Amount']
    for i in range(len(driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucLeasing1_dgAchievedRevenue"]//tr'))-1):
        rowitems=driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucLeasing1_dgAchievedRevenue"]//tr['+str(i+1)+']/td')
        itemCounter=0 
        dictionary={}
        for t in rowitems:
            dictionary[LeasingLabels[itemCounter]]=get_true_value(t.text)
            #print(get_true_value(t.text))
            itemCounter=itemCounter+1
        listofDicts.append(dictionary)
    LeasingAchievedRev=pd.DataFrame(listofDicts)
    #
    listofDicts=[]
    LeasingLabels2=['title','thisPeriod_Achievedcost','Total_Achievedcost','thisPeriod_TakenFacilityRemaind','Total_TakenFacilityRemained']
    for i in range(len(driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucLeasing1_dgAchievedCost"]//tr'))-1):
        rowitems=driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucLeasing1_dgAchievedCost"]//tr['+str(i+1)+']/td')
        itemCounter=0 
        dictionary={}
        for t in rowitems:
            dictionary[LeasingLabels2[itemCounter]]=get_true_value(t.text)
            #print(get_true_value(t.text))
            itemCounter=itemCounter+1
        listofDicts.append(dictionary)
    LeasingAchievedCost=pd.DataFrame(listofDicts)
    #
    listofDicts=[]
    LeasingLabels3=['title','StartMonth_count','StartMonth_Amount','thismonth_NewFacilityCount','thismonth_NewFacilityAmount','thismonth_SetteledCount','thismonth_SetteledAmount','EndMonth_FacilityCount','EndMonth_FacilityAmount']
    for i in range(len(driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucLeasing1_dgDelegatedGoodsSummary"]//tr'))-1):
        rowitems=driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucLeasing1_dgDelegatedGoodsSummary"]//tr['+str(i+1)+']/td')
        itemCounter=0 
        dictionary={}
        for t in rowitems:
            dictionary[LeasingLabels3[itemCounter]]=get_true_value(t.text)
            #print(get_true_value(t.text))
            itemCounter=itemCounter+1
        listofDicts.append(dictionary)
    DelegatedGoods=pd.DataFrame(listofDicts)
    results={}
    LeasingAchievedRev.replace('ك','ک',regex=True,inplace=True)
    LeasingAchievedCost.replace('ك','ک',regex=True,inplace=True)
    DelegatedGoods.replace('ك','ک',regex=True,inplace=True)
    LeasingAchievedRev.replace('ي','ی',regex=True,inplace=True)
    LeasingAchievedCost.replace('ي','ی',regex=True,inplace=True)
    DelegatedGoods.replace('ي','ی',regex=True,inplace=True)
    results['Revenue']=LeasingAchievedRev
    results['Costs']=LeasingAchievedCost
    results['Delegated']=DelegatedGoods
    return results            
def Insert_Leasing(desc,titles,announcments,CID,Clink,DFResults):
    df1=DFResults['Revenue']
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
                IF NOT EXISTS (select from monthly."Leasing_Monthly_Revenue" where "report_id"=%(report_id)s and "itemTitle"=%(Title)s) THEN
                    INSERT INTO monthly."Leasing_Monthly_Revenue"(
                    firm, report_id, period, "ToDate", desc_modification,
                    "desc_onePeriod", "desc_toDate", desc_title, "lastAnnouncement", 
                    "nextAnnouncement", reported_firm, "itemTitle", "thisPeriodAmount",
                    "TotalAmount")
                    VALUES (  (select "ID" from "Entity" where ticker=%(nemad)s), %(report_id)s, %(period)s,
                    %(toDate)s, %(desc_modif)s, %(desc_month)s, %(desc_year)s,
                    %(desc_title)s, %(Last)s, %(Next)s, %(firm_reporting)s, 
                    %(Title)s, %(thisPeriod_Amount)s, %(Total_Amount)s);
                END IF;
            END
            $$ 
        
        """

        cursor.executemany(postgres_insert_query,dftemp4.to_dict(orient='records'))
        connection.commit()
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print("Failed to InsertLeasing_Revenue", error)
                log_it('Failed to Insert InsertLeasing_Revenue -'+str(CID))
    finally:
        if(connection):
            cursor.close()
            connection.close()
    df_2=DFResults['Costs']
    descdf=pd.DataFrame([desc])
    titlesdf=pd.DataFrame([titles])
    titlesdf.columns=['nemad','period','toDate','firm_reporting']
    announcedf=pd.DataFrame([announcments])
    df_2['tmp'] = 1
    descdf['tmp'] = 1
    df_2temp = pd.merge(df_2, descdf, on=['tmp'])
    df_2temp = df_2temp.drop('tmp', axis=1)
    df_2temp['tmp'] = 1
    titlesdf['tmp'] = 1
    df_3temp= pd.merge(df_2temp, titlesdf, on=['tmp'])
    df_3temp = df_3temp.drop('tmp', axis=1)
    df_3temp['tmp'] = 1
    announcedf['tmp'] = 1
    df_4temp= pd.merge(df_3temp, announcedf, on=['tmp'])
    df_4temp = df_4temp.drop('tmp', axis=1)
    df_4temp['report_id']=CID
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
                IF NOT EXISTS (select from monthly."Leasing_Monthly_Cost" where "report_id"=%(report_id)s and "itemTitle"=%(title)s) THEN
                  
                  INSERT INTO monthly."Leasing_Monthly_Cost"(
                firm, period, "toDate", report_id, 
                desc_modification, desc_title,
                "desc_toDate", "desc_onePeriod",
                "lastAnnouncement", "NextAnnouncement",
                reported_firm, "itemTitle", "thisPeriod_Achievedcost", 
                "Total_Achievedcost", "thisPeriod_TakenFacilityRemaind"
                , "Total_TakenFacilityRemained")
                VALUES ( (select "ID" from "Entity" where ticker=%(nemad)s), %(period)s, %(toDate)s, %(report_id)s,
                %(desc_modif)s, %(desc_title)s, %(desc_year)s, %(desc_month)s, %(Last)s, %(Next)s, 
                %(firm_reporting)s, %(title)s, %(thisPeriod_Achievedcost)s, %(Total_Achievedcost)s, %(thisPeriod_TakenFacilityRemaind)s, %(Total_TakenFacilityRemained)s);
                END IF;



            END
            $$ 
        
        """

        cursor.executemany(postgres_insert_query,df_4temp.to_dict(orient='records'))
        connection.commit()
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print("Failed to Insert Leasing_cost", error)
                log_it('Failed to Insert Leasing_cost -'+str(CID))
    finally:
        if(connection):
            cursor.close()
            connection.close()
    df__3=DFResults['Delegated']
    descdf=pd.DataFrame([desc])
    titlesdf=pd.DataFrame([titles])
    titlesdf.columns=['nemad','period','toDate','firm_reporting']
    announcedf=pd.DataFrame([announcments])
    df__3['tmp'] = 1
    descdf['tmp'] = 1
    df__3temp2 = pd.merge(df__3, descdf, on=['tmp'])
    df__3temp2 = df__3temp2.drop('tmp', axis=1)
    df__3temp2['tmp'] = 1
    titlesdf['tmp'] = 1
    df__3temp3= pd.merge(df__3temp2, titlesdf, on=['tmp'])
    df__3temp4 = df__3temp3.drop('tmp', axis=1)
    df__3temp4['tmp'] = 1
    announcedf['tmp'] = 1
    df__3temp5= pd.merge(df__3temp4, announcedf, on=['tmp'])
    df__3temp5 = df__3temp5.drop('tmp', axis=1)
    df__3temp5['report_id']=CID
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
                IF NOT EXISTS (select from monthly."Leasing_Monthly_DelegatedSummary" where "report_id"=%(report_id)s and "itemTitle"=%(title)s) THEN
                  
                  INSERT INTO monthly."Leasing_Monthly_DelegatedSummary"(
                    firm, report_id, period, "toDate", desc_modification,
                    "desc_onePeriod", "desc_toDate", desc_title, "lastAnnouncement", 
                    "nextAnnouncement", reported_firm, "itemTitle", "StartMonth_count",
                    "StartMonth_Amount", "thismonth_NewFacilityCount", "thismonth_NewFacilityAmount",
                    "thismonth_SetteledCount", "thismonth_SetteledAmount", "EndMonth_FacilityCount", 
                    "EndMonth_FacilityAmount")
                    
                    VALUES ((select "ID" from "Entity" where ticker=%(nemad)s), %(report_id)s, %(period)s, %(toDate)s,
                    %(desc_modif)s, %(desc_month)s, %(desc_year)s, 
                    %(desc_title)s, %(Last)s, %(Next)s,
                    %(firm_reporting)s, %(title)s, %(StartMonth_count)s,
                    %(StartMonth_Amount)s, %(thismonth_NewFacilityCount)s, %(thismonth_NewFacilityAmount)s,
                    %(thismonth_SetteledCount)s, %(thismonth_SetteledAmount)s, %(EndMonth_FacilityCount)s,%(EndMonth_FacilityAmount)s);
                END IF;



            END
            $$ 
        
        """

        cursor.executemany(postgres_insert_query,df__3temp5.to_dict(orient='records'))
        connection.commit()
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print("Failed to Insert Leasing_Delegated", error)
                log_it('Failed to Insert Leasing_Delegated -'+str(CID))
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
                print("Failed to Update MR", error)
                log_it('Failed to Update Leasing -'+str(CID))
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
            titles=get_titlebox(driver)
            driver.execute_script("document.body.style.zoom='100%';document.body.style.zoom='50%'")
            Insert_Leasing(get_description_product_service(driver),titles,get_announcments(driver),CodalRaw_ID,CodalRaw_links,Leasing_results(driver)) 
            counter=counter+1
            percentage=(counter*100)/AllData
            print("{0:.2f}".format(percentage))
            
        except (Exception, psycopg2.Error) as error :
            print(error)
            print(CodalRaw_links)
            continue
        #driver.quit()    