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
from selenium.webdriver.chrome.options import Options
import re
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
def find_nth(haystack, needle, n):
    start = haystack.find(needle)
    while start >= 0 and n > 1:
        start = haystack.find(needle, start+len(needle))
        n -= 1
    return start
def UpdateError(driver,CodalRaw_ID):
    Error=False
    if check_exists_by_xpath(driver,'//*[text()="متاسفانه سیستم با خطا مواجه شده است."]'):
        Error=True
    if check_exists_by_xpath(driver,'//*[@id="Table2"]//span[text()="ضمائم"]'):
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
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print("Failed to Update Error sheets", error)
                log_it('Failed to Update Error sheets -')
    finally:
        if(connection):
            cursor.close()
            connection.close()     
def roundTheFloats(x):
    if(type(get_true_value(x))==float):
        return int(round(get_true_value(x)))
    else:
        return x
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
def log_it(text):
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        postgres_insert_query = """
          
          INSERT INTO public._log(
            date, action,source)
                VALUES (%s, %s,%s);
        """
    
        record_to_insert = (str(datetime.datetime.now()),text,'Disclosure_N20')
        
        cursor.execute(postgres_insert_query, record_to_insert)
        connection.commit()
    except(Exception, psycopg2.Error) as error:
        if(connection):
            print("Failed to insert log", error)
    finally:
        if(connection):
            cursor.close()
            connection.close()           
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
def get_unconverted():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
    #         df = psql.read_sql("""
    #         select * FROM codalraw."allrawReports" where "LetterCode"='ن-۲۰' and "SentTime" > '۱۳۹۶/۱۰/۰۵ '
    # and "HtmlUrl"!='' and "TracingNo" not in (select "report_ID" from codalraw."DisclosureConverted") 
    #         """, connection)
        df = psql.read_sql("""
        select * FROM codalraw."allrawReports" where "LetterCode"='ن-۲۰' and "HtmlUrl"!='' and "Available"=True  and "TracingNo" not in (select "report_ID" from codalraw."DisclosureConverted") 
        """, connection)
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read links", error)
    finally:
            if(connection):
                cursor.close()
                connection.close()
def chunks(l, n):
    n = max(1, n)
    return (l[i:i+n] for i in range(0, len(l), n))  
def InsertDiscImportantInfo(row):
    try:

        connection = psycopg2.connect(user=db_username,
                                      password=db_pass,
                                      host=db_host,
                                      port=db_port,
                                      database=db_database)
        cursor = connection.cursor()
        Dict=row.to_dict()
        pouery = """
            DO 
            $$
                BEGIN
                    IF NOT EXISTS (select from  public."Entity" where "ticker"=%(Nemad)s) THEN
                        INSERT INTO  public."Entity"(
                        ticker,"Type")
                        VALUES (%(Nemad)s,81
                        );
                    END IF;
                END
            $$ 

        """
        cursor.execute(pouery,Dict)
        postgres_insert_query = """
            DO 
            $$
                BEGIN
                    IF NOT EXISTS (select from codalreports."Disclosure_ImportantInfo" where "report_id"=%(report_id)s) THEN
                        INSERT INTO codalreports."Disclosure_ImportantInfo"(
                        firm, report_id, "Correction", "CorrectionDetails", "PublishDate", "DisclosureSubject")
                        VALUES ( (select "ID" from "Entity" where ticker=%(Nemad)s), %(report_id)s,
                        %(correction)s,%(Details)s
                        ,%(DateOf)s,%(DisclousreSubject)s);
                    END IF;
                END
            $$ 

        """

        cursor.execute(postgres_insert_query,Dict)
        connection.commit()

        InnerDF=row['Data']
        InnerDF['report_id']=row['report_id']
        for index,row in InnerDF.iterrows():
            Dict2=row.to_dict()
            postgres_insert_query2 = """
            DO 
            $$
                BEGIN
                    IF NOT EXISTS (select from codalreports."Disclosure_ImportantInfo_Details" where "DisclosureID"=(select "ID" from  codalreports."Disclosure_ImportantInfo" where report_id=%(report_id)s) and "ItemCaption"=%(ItemCaption)s ) THEN
                        INSERT INTO codalreports."Disclosure_ImportantInfo_Details"(
                        "DisclosureID", "ItemCaption", "ItemValue", "ItemUnit")
                        VALUES ( (select "ID" from codalreports."Disclosure_ImportantInfo" where report_id=%(report_id)s),%(ItemCaption)s,%(ItemValue)s,%(ItemUnit)s);
                    END IF;
                END
            $$ 

        """
            cursor.execute(postgres_insert_query2,Dict2)
            connection.commit()




        postgres_insert_query3 = """
        INSERT INTO codalraw."DisclosureConverted"(
        "report_ID", converted)
        VALUES (%s, True);
        """
        record_to_insert3=([row['report_id']])
        cursor.execute(postgres_insert_query3, record_to_insert3)
        connection.commit()
        print(str(row['report_id'])+'  '+'--Done')
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print("Failed to Insert Disclosure_ImportantInfo N20-", error)
                log_it('Failed to Insert Disclosure_ImportantInfo N20-'+str(row['report_id']))
    finally:
        if(connection):
            cursor.close()
            connection.close()   
def handle_all(driver,df):
    listofDicts=[]
    counter=0
    for index,row in df.iterrows():
        try:
            ###Browser And Dictionary Setting###
            CodalRaw_ID=str(row['TracingNo'])
            CodalRaw_links=row['HtmlUrl']
            driver.get('http://codal.ir'+CodalRaw_links)
            if UpdateError(driver,CodalRaw_ID):
                time.sleep(2)
                Dict_IC_Time={}
                Dict_IC_Time['report_id']=CodalRaw_ID
                Dict_IC_Time['Nemad']=row['Ticker']
                ##Initializing Variables#################
                Nemad=''
                Details=''
                correction=False
                ToDate=''
                DisclousreSubject=''
                ####row['Title'] Data###
                Title=row['title']
                Title=Title.replace('(','')
                Title=Title.replace(')','')
                if (Title.split('-')[1].strip()=='سایر اطلاعات بااهمیت'):
                    DisclousreSubject=Title.split('-')[2]
                else:
                    DisclousreSubject=Title.split('-')[1]
                if 'منتهی' in row['title']:
                    x=re.search("\d\d\d\d.\d\d.\d\d",row['title'])
                    ToDate=x.group()
                Dict_IC_Time['DisclousreSubject']=DisclousreSubject
                Dict_IC_Time['ToDate']=ToDate
                if ('اصلاح') in row['title']:
                        correction=True
                Dict_IC_Time['correction']=correction  
                publishDate=row['PublishTime']
                Dict_IC_Time['DateOf']=publishDate
                ##### DETAILS FROM TEXT###
                txt=driver.find_element_by_tag_name('body').text
                if('دلایل اصلاح:') in txt:
                    kk=1
                    while find_nth(txt,'\n',kk)<txt.find('دلایل اصلاح:'):
                        kk=kk+1
                        if kk>10:
                            kk=2
                            break
                    Details=txt[txt.find('دلایل اصلاح:'):find_nth(txt,'\n',kk)] 
                Dict_IC_Time['Details']=Details
                ####NGDATA#####################
                if check_exists_by_xpath(driver,'//*[@id="ctl00_cphBody_upAssemblyShareHolder11"]'):
                    subject=driver.find_element_by_xpath('//*[@id="ctl00_cphBody_upAssemblyShareHolder11"]//tr[2]//td[2]').text
                    desc=driver.find_element_by_xpath('//*[@id="ctl00_cphBody_upAssemblyShareHolder11"]//tr[3]//td[2]').text
                    outcome=driver.find_element_by_xpath('//*[@id="ctl00_cphBody_upAssemblyShareHolder11"]//tr[4]//td[2]').text
                    listOfSpecs=[]
                    innerDict={}
                    innerDict['ItemCaption']='subject'
                    innerDict['ItemValue']=subject
                    innerDict['ItemUnit']=''
                    listOfSpecs.append(innerDict)
                    innerDict={}
                    innerDict['ItemCaption']='desc'
                    innerDict['ItemValue']=desc
                    innerDict['ItemUnit']=''
                    listOfSpecs.append(innerDict)
                    innerDict={}
                    innerDict['ItemCaption']='outcome'
                    innerDict['ItemValue']=outcome
                    innerDict['ItemUnit']=''
                    listOfSpecs.append(innerDict)
                    InnerDf=pd.DataFrame(listOfSpecs)
                    if InnerDf.empty:
                        Dict_IC_Time['Data']=pd.DataFrame()
                    else:
                        Dict_IC_Time['Data']=InnerDf
                else:
                    wholeHtml=driver.page_source
                    jsonData=json.loads(wholeHtml[wholeHtml.find('var clientDataSource =')+23:wholeHtml.find('}};')+2])
                    listOfSpecs=[]
                    for item in jsonData['FormData']['Result']['Fields']:
                        innerDict={}
                        innerDict['ItemCaption']=item['Caption']
                        innerDict['ItemValue']=str(item['Value'])
                        innerDict['ItemUnit']=str(item['UnitList'][0]['Text'])
                        listOfSpecs.append(innerDict)
                    InnerDf=pd.DataFrame(listOfSpecs)
                    if InnerDf.empty:
                        Dict_IC_Time['Data']=pd.DataFrame()
                    else:
                        Dict_IC_Time['Data']=InnerDf
                ######
                listofDicts.append(Dict_IC_Time)
                counter=counter+1
                print(counter/len(df))
        except:
            continue
    DFIC=pd.DataFrame(listofDicts)
    DFIC.replace('ك','ک',regex=True,inplace=True)
    DFIC.replace('ي','ی',regex=True,inplace=True)  
    for index,row in DFIC.iterrows():
        InsertDiscImportantInfo(row) 
def RUN(driver):
    df=get_unconverted()
    if not df.empty:
        handle_all(driver,df)
    else:
        print('All Disclosure Important Info DONE')                                           