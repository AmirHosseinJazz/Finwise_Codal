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
db_database="FinWisev12"
IC_license=['lblSixth_FirstchkBox','lblSixth_SecondchkBox',
            'lblSixthPlaning','lblSixthLetterNo','lblSixthLetterDate',
            'lblSixthOpportunity','lblSixthTransparenciesImpotant','lblSixthToDate',
           'lblSixthExtended','lblSixthTransparenciesObserveInstructions']
false_Labels=['lblSixth_FirstchkBox','lblSixth_SecondchkBox','lblSixthToDate',
            'lblSixthPlaning','lblSixthLetterNo','lblSixthLetterDate',
            'lblSixthOpportunity','lblSixthTransparenciesImpotant',
           'lblSixthExtended','lblSixthTransparenciesObserveInstructions']


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
    
        record_to_insert = (str(datetime.datetime.now()),text,'FB4')
        
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
        df = psql.read_sql("""
        select * FROM codalraw."allrawReports" where ("LetterCode"='ف ب-۴' OR "LetterCode"='ب-۴' )and "HtmlUrl"!='' and "TracingNo" not in (select "report_ID" from codalraw."BourseClarificationConverted")                
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
def InsertExtendOpport_Further(row):
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
                    IF NOT EXISTS (select from  public."Publishers" where "persianName"=%(Nemad)s) THEN
                        INSERT INTO public."Publishers"(
                        "persianName")
                        VALUES (%(Nemad)s);
                    END IF;
                END
            $$ 

        """
        cursor.execute(pouery,Dict)
        connection.commit()
        postgres_insert_query = """
            DO 
            $$
                BEGIN
                    IF NOT EXISTS (select from  codalreports."BourseClarification_ExtendingOpportunityFurthermore" where "report_id"=%(report_id)s ) THEN
                        INSERT INTO  codalreports."BourseClarification_ExtendingOpportunityFurthermore"(
                        firm, report_id, "PublishTime", "Source", 
                        "Issue", "LetterCode", "LetterDate",
                        "FirstOpp", "FurtherMore", "ExtendedToDate")
                        VALUES ((select "ID" from "Publishers" where "persianName"=%(Nemad)s), %(report_id)s,
                        %(DateOf)s,%(lblSixthPlaning)s,%(Issues)s,%(lblSixthLetterNo)s,%(lblSixthLetterDate)s,
                        %(Opportuinty)s,%(OpportuintySecond)s,%(lblSixthToDate)s
                        );
                    END IF;
                END
            $$ 

        """
        cursor.execute(postgres_insert_query,Dict)
        connection.commit()
        postgres_insert_query3 = """
        
        DO 
            $$
                BEGIN
                    IF NOT EXISTS (select from  codalraw."BourseClarificationConverted" where "report_ID"=%s) THEN
                        INSERT INTO  codalraw."BourseClarificationConverted"(
                        "report_ID", converted)
                        VALUES (%s, True
                        );
                    END IF;
                END
        $$ 
        """
        record_to_insert3=([row['report_id'],row['report_id']])
        cursor.execute(postgres_insert_query3, record_to_insert3)
        connection.commit()
        print(str(row['report_id'])+'  '+'--Done')
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print("Failed to Insert NonCompliance ", error)
                #log_it('Failed to Insert NonCompliance -'+str(row['report_id']))
    finally:
        if(connection):
            cursor.close()
            connection.close() 

def handle_all(driver,df):
    listofDicts=[]
    counter=0
    for index,row in df.iterrows():
        try:
            CodalRaw_ID=str(row['TracingNo'])
            CodalRaw_links=row['HtmlUrl']
            driver.get('http://codal.ir'+CodalRaw_links)
            time.sleep(2)
            if UpdateError(driver,CodalRaw_ID):
                Dict_IC_Time={}
                Dict_IC_Time['report_id']=CodalRaw_ID
                Nemad=''
                correction=False
                Subject='NecessityOfHoldingOnConferenceCall'
                Details=''
                Length=''
                
                publishDate=row['PublishTime']
                txt=driver.find_element_by_tag_name('body').text
                if ('نماد: ') in txt:
                    kk=1
                    while find_nth(txt,'\n',kk)<txt.find('نماد: '):
                        kk=kk+1
                        if kk>10:
                            kk=2
                            break
                    Nemad=txt[txt.find('نماد: '):find_nth(txt,'\n',kk)].split(':')[1].strip()
                    if('(') in Nemad:
                        Nemad=Nemad.split('(')[0]
                else:
                        Nemad=row['Ticker'].strip()
                try:
                    Length=re.search("[\d]+(?=\s+روز کاری)",txt)[0]
                except:
                    Length=''
                #Dict_IC_Time['length']=Length
                if('دلایل اصلاح:') in txt:
                    kk=1
                    while find_nth(txt,'\n',kk)<txt.find('دلایل اصلاح:'):
                        kk=kk+1
                        if kk>10:
                            kk=2
                            break
                    Details=txt[txt.find('دلایل اصلاح:'):find_nth(txt,'\n',kk)] 
                Dict_IC_Time['Details']=Details
                if ('اصلاح') in row['title']:
                        correction=True
                Dict_IC_Time['correction']=correction    
                Dict_IC_Time['Details']=Details
                Dict_IC_Time['Nemad']=Nemad
                #Dict_IC_Time['Subject']=Subject
                Dict_IC_Time['DateOf']=publishDate
                for i in IC_license:
                    if check_exists_by_xpath(driver,'//*[contains(@id,"'+i+'")]'):
                        if ('lbl' in i) and (i not in false_Labels):
                            Dict_IC_Time[i]=True
                        else:
                            if driver.find_element_by_xpath(('//*[contains(@id,"'+i+'")]')).get_attribute('value') is not None:
                                Dict_IC_Time[i]=driver.find_element_by_xpath(('//*[contains(@id,"'+i+'")]')).get_attribute('value')
                            else:
                                Dict_IC_Time[i]=driver.find_element_by_xpath(('//*[contains(@id,"'+i+'")]')).text
            #     Dict_IC_Time['Boards']=Translate_tableBoard()
                listofDicts.append(Dict_IC_Time)
                counter=counter+1
                print(counter/len(df))
        except Exception as e:
            print(e)
            continue
    DFIC=pd.DataFrame(listofDicts)
    for i in DFIC.columns:
        if 'lbl' in i:
            DFIC[i].fillna('False',inplace=True)
    DFIC.replace('ك','ک',regex=True,inplace=True)
    DFIC.replace('ي','ی',regex=True,inplace=True)
    for i in IC_license:
        if (i not in DFIC.columns)&('lbl' not in i):
            DFIC[i]=None
        if (i not in DFIC.columns)&('lbl' in i):
            DFIC[i]=''
    DFIC['Issues']=DFIC['lblSixth_FirstchkBox']+' - '+DFIC['lblSixth_SecondchkBox']
    DFIC['Opportuinty']=DFIC['lblSixthOpportunity']+' - '+DFIC['lblSixthTransparenciesImpotant']
    DFIC['OpportuintySecond']=DFIC['lblSixthExtended']+' - '+DFIC['lblSixthTransparenciesObserveInstructions']
    for index,row in DFIC.iterrows():
        InsertExtendOpport_Further(row)                
def RUN(driver):
    df=get_unconverted()
    if not df.empty:
        # chrome_options = Options()  
        # chrome_options.add_argument("--headless")  
        # driver = webdriver.Chrome(chrome_options=chrome_options)
        # driver.maximize_window()  
        handle_all(driver,df)
        #driver.quit()                       
    else:
        print('All FB4 DONE')            