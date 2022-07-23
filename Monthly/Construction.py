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
            WHERE P."Type"='Construction' and P.converted=False"""

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
    
    
def Constructions_results(driver):
    listofDicts=[]
    ConstructionLabels=[
        'ProjectName','Location','TypeofProject','Unit','thisMonth_Cost','MeterSold','SaleRate','SaleAmount',
        'FromBefore_Cost','FromBefore_Revenue','TotalYear_Cost','TotalYear_MeterSold','TotalYear_SaleRate','TotalYear_SaleAmount']
    for i in range(len(driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucBuilding1_dgBuilding"]//tr'))-1):
        rowitems=driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucBuilding1_dgBuilding"]//tr['+str(i+1)+']/td')
        itemCounter=0 
        dictionary={}
        for t in rowitems:
            try:
                dictionary[ConstructionLabels[itemCounter]]=get_true_value(t.text)
                itemCounter=itemCounter+1
            except:
                continue
        listofDicts.append(dictionary)
    SoldProjects=pd.DataFrame(listofDicts)
    listofDicts=[]
    ConstructionLabels_2=[
        'ProjectName','Location','TypeofProject','Ownership','Unit','NetMeter','lastMonth_physicalProgress','lastMonth_Cost',
        'lastMonth_EstimationOfRemainingCost','lastMonth_EstmiatedTotalCost','SoldProjectsDuringMonth_meter','SoldProjectsDuringMonth_cost',
        'thisMonth_physicalProgress','thisMonth_cost','thisMonth_EstimationRemainingCost','thisMonth_EsitmatedTotalCost']
    for i in range(len(driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucBuilding1_ucProjStat_dgBuildingProjStat"]//tr'))-1):
        rowitems=driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucBuilding1_ucProjStat_dgBuildingProjStat"]//tr['+str(i+1)+']/td')
        itemCounter=0 
        dictionary={}
        for t in rowitems:
            try:
                dictionary[ConstructionLabels_2[itemCounter]]=get_true_value(t.text)
                itemCounter=itemCounter+1
            except:
                continue
        listofDicts.append(dictionary)
    Progress=pd.DataFrame(listofDicts)
    results={}
    SoldProjects.replace('ك','ک',regex=True,inplace=True)
    SoldProjects.replace('ي','ی',regex=True,inplace=True)
    Progress.replace('ك','ک',regex=True,inplace=True)
    Progress.replace('ي','ی',regex=True,inplace=True)
    results['SoldProjects']=SoldProjects
    results['Progress']=Progress
    return results
          




  

def Insert_construction(desc,titles,announcments,CID,Clink,DFResults):
    df1=DFResults['SoldProjects']
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
                IF NOT EXISTS (select from monthly."Construction_Monthly_Sold" where "report_id"=%(report_id)s and "projectName"=%(ProjectName)s) THEN
                    INSERT INTO monthly."Construction_Monthly_Sold"(report_id, reported_firm, period, "toDate", 
                    desc_modification, "desc_onePeriod", "desc_toDate", 
                    desc_title, "lastAnnouncement", "nextAnnoucement",
                    "projectName", "Location", "typeOfProject", "projectUnit", 
                    "thisMonth_Cost", "thisMonth_MeterSold", "thisMonth_SaleRate", 
                    "thisMonth_SaleAmount", "FromBefore_Cost", "FromBefore_Revenue",
                    "TotalYear_Cost", "TotalYear_MeterSold", "TotalYear_SaleRate",
                    "TotalYear_SaleAmount", firm)
                    VALUES ( %(report_id)s, %(firm_reporting)s, %(period)s, %(toDate)s,
                    %(desc_modif)s, %(desc_month)s, %(desc_year)s, %(desc_title)s,
                    %(Last)s, %(Next)s, %(ProjectName)s, %(Location)s,
                    %(TypeofProject)s, %(Unit)s, %(thisMonth_Cost)s,
                    %(MeterSold)s, %(SaleRate)s, %(SaleAmount)s, %(FromBefore_Cost)s,
                    %(FromBefore_Revenue)s, %(TotalYear_Cost)s, %(TotalYear_MeterSold)s, %(TotalYear_SaleRate)s,
                    %(TotalYear_SaleAmount)s, (select "ID" from "Publishers" where "persianName"=%(nemad)s)
                    );
                END IF;
            END
            $$ 
        
        """

        cursor.executemany(postgres_insert_query,dftemp4.to_dict(orient='records'))
        connection.commit()
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print("Failed to InsertConstruction", error)
                log_it('Failed to Insert Construction -'+str(CID))
    finally:
        if(connection):
            cursor.close()
            connection.close()
    df1=DFResults['Progress']
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
    dftemp4_2 = dftemp4.drop('tmp', axis=1)
    dftemp4_2['report_id']=CID
    try:

        connection = psycopg2.connect(user=db_username,
                                      password=db_pass,
                                      host=db_host,
                                      port=db_port,
                                      database=db_database)
        cursor = connection.cursor()
        postgres_insert_query2 = """
        DO 
                $$
                BEGIN
                    IF NOT EXISTS (select from monthly."Construction_Monthly_Ongoing" where "report_id"=%(report_id)s and "projectName"=%(ProjectName)s ) THEN
                        INSERT INTO monthly."Construction_Monthly_Ongoing"(
                        firm, report_id, period, "toDate", desc_modification,
                        "desc_onePeriod", "desc_toDate", desc_title, "lastAnnouncement",
                        "nextAnnouncement", reported_firm, "projectName", "Location", 
                        "typeOfProject", "Ownership", "projectUnit", "NetMeter", 
                        "lastMonth_physicalProgress", "lastMonth_Cost", "lastMonth_EstimationOfRemainingCost", 
                        "lastMonth_EstmiatedTotalCost", "SoldProjectsDuringMonth_meter", "SoldProjectsDuringMonth_cost",
                        "thisMonth_physicalProgress", "thisMonth_cost", "thisMonth_EstimationRemainingCost", 
                        "thisMonth_EsitmatedTotalCost")
                        VALUES ((select "ID" from "Publishers" where "persianName"=%(nemad)s), %(report_id)s, %(period)s, %(toDate)s,
                        %(desc_modif)s, %(desc_month)s, %(desc_year)s
                        , %(desc_title)s, %(Last)s, %(Next)s, %(firm_reporting)s,
                        %(ProjectName)s, %(Location)s, %(TypeofProject)s
                        , %(Ownership)s, %(Unit)s, %(NetMeter)s, %(lastMonth_physicalProgress)s, 
                        %(lastMonth_Cost)s, %(lastMonth_EstimationOfRemainingCost)s, %(lastMonth_EstmiatedTotalCost)s
                        , %(SoldProjectsDuringMonth_meter)s, %(SoldProjectsDuringMonth_cost)s, 
                        %(thisMonth_physicalProgress)s, %(thisMonth_cost)s, 
                        %(thisMonth_EstimationRemainingCost)s, %(thisMonth_EsitmatedTotalCost)s);
                    END IF;
                END
                $$ 

        """
        #print(dftemp4_2.columns)
        cursor.executemany(postgres_insert_query2,dftemp4_2.to_dict(orient='records'))
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
                print("Failed to InsertConstruction", error)
                log_it('Failed to Insert Construction -'+str(CID))
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
            Insert_construction(get_description_product_service(driver),titles,get_announcments(driver),CodalRaw_ID,CodalRaw_links,Constructions_results(driver))
            counter=counter+1
            percentage=(counter*100)/AllData
            print("{0:.2f}".format(percentage))
            
        except (Exception, psycopg2.Error) as error :
            print(error)
            print(CodalRaw_links)
            continue
    #driver.quit()    