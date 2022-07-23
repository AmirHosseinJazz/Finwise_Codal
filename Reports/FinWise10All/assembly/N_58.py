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
import re
from selenium.webdriver.chrome.options import Options

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
        log_it("N_58_Error_"+str(CodalRaw_ID))
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
    
        record_to_insert = (str(datetime.datetime.now()),text,'Cancelation_N58')
        
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

        select * FROM codalraw."allrawReports" where "LetterCode"='ن-۵۸' and "HtmlUrl"!='' and "Available"=True and "TracingNo" not in (select "report_ID" from codalraw."AssemblyConverted")


        
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
Cancel_labels=['turn',
                     'txbHour','txbLocation','txbday','txbDate','lblIsListenedBoardMemberReport',
                     'lblIsApproveStatements','lblYearEndToDate1','lblIsSelectInspector','lblIsSelectNewspaper',
                     'lblIsSelectBoardMember','lblIsSelectBoardMember','lblIsBoardMemberGift',
                     'lblIsPublishSecurity','lblIsLocationChange','lblIsNameChange','lblIsActivitySubjectChange',
                     'lblIsConvertSecurityToShare','lblIsFinancialYearChange','lblIsOther1','lblIsPresentIncome','lblIsBoardMemberIncome',
                     'lblIsOther2','lblIsOther3','txbOther1','txbOther2','txbOther3',
                     'lblIsDecidedClause141','lblIsAccordWithSEOStatute','txbAttendanceSheet','txbCaller',
                     'txbProvinceName','txbCityName','capitalChangeState','lblOther1','lblOther2',
                     'provinceRef','cityRef','lblIsCorrectionStatute','primaryMarketTracingNo','lblIsCapitalIncrease','lblIsBoardMemberWage'
    ]
def insertCancleation(row):
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
        connection.commit()
        for i in Cancel_labels:
            if (i=='turn' or i=='provinceRef' or i =='capitalChangeState' or i=='cityRef') &(i not in Dict.keys()):
                Dict[i]=None
            if (i not in Dict.keys())&('lbl' not in i):
                Dict[i]=None
            if (i not in Dict.keys())&('lbl' in i):
                Dict[i]=False
        postgres_insert_query = """
            DO 
            $$
                BEGIN
                    IF NOT EXISTS (select from codalreports."Assembly_Invitation_Cancelation" where "report_id"=%(report_id)s) THEN
                        INSERT INTO codalreports."Assembly_Invitation_Cancelation"(
                        firm, report_id, turn, hour,
                        location, day, date,
                        "IsListenedBoardReport",
                        "IsApproveStatement", "IsSelectInspector",
                        "IsSelectNewsPaper", "IsBoardMemberWage", 
                        "IsBoardMemberGift", "IsPublishSecurity", "capitalChangeState",
                        "IsLocationChange", 
                        "IsNameChange", "IsActivitySubjectChange",
                        "IsConvertSecurityToShare", 
                        "IsFinancialYearChange", other, "IsDecidedClause141", 
                        "IsAccordWithSEOStatute", "cityRef", "cityName", "provinceRef", 
                        "provinceName", "attendanceSheet", caller, "Correction", 
                        "primaryMarketTracingNo", "isCorrectionStatute", "isCapitalIncrease")
                        VALUES ( (select "ID" from "Entity" where ticker=%(Nemad)s), %(report_id)s,
                        %(turn)s,%(txbHour)s, %(txbLocation)s, %(txbday)s, 
                        %(txbDate)s, %(lblIsListenedBoardMemberReport)s, %(lblIsApproveStatements)s, %(lblIsSelectInspector)s, 
                        %(lblIsSelectNewspaper)s, %(lblIsBoardMemberWage)s, %(lblIsBoardMemberGift)s, %(lblIsPublishSecurity)s, 
                        %(capitalChangeState)s, %(lblIsLocationChange)s, %(lblIsNameChange)s, %(lblIsActivitySubjectChange)s, 
                        %(lblIsConvertSecurityToShare)s, %(lblIsFinancialYearChange)s, %(Other)s, %(lblIsDecidedClause141)s, 
                        %(lblIsAccordWithSEOStatute)s, %(cityRef)s, %(txbCityName)s, %(provinceRef)s, 
                        %(txbProvinceName)s, %(txbAttendanceSheet)s, %(txbCaller)s, %(correction)s,
                        %(primaryMarketTracingNo)s, %(lblIsCorrectionStatute)s, %(lblIsCapitalIncrease)s);
                    END IF;
                END
            $$ 

        """

        cursor.execute(postgres_insert_query,Dict)
        connection.commit()
        postgres_insert_query3 = """
        INSERT INTO codalraw."AssemblyConverted"(
        "report_ID", converted)
        VALUES (%s, True);
        """
        record_to_insert3=([row['report_id']])
        cursor.execute(postgres_insert_query3, record_to_insert3)
        connection.commit()
        print(str(row['report_id'])+' _N58_ '+'--Done')
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print("Failed to Insert Cancelation_N58 ", error)
                log_it('Failed to Insert Cancelation_N58 -'+str(row['report_id']))
    finally:
        if(connection):
            cursor.close()
            connection.close()   

def handleCancelation(driver,df):
    Cancelation=df
    listofDicts=[]
    counter=0
    for index,row in Cancelation.iterrows():
        try:
            CodalRaw_ID=str(row['TracingNo'])
            CodalRaw_links=row['HtmlUrl']
            driver.get('http://codal.ir'+CodalRaw_links)
            if UpdateError(driver,CodalRaw_ID):
                time.sleep(5)
                Dict_cancel={}
                Dict_cancel['report_id']=CodalRaw_ID
                Nemad=''
                Details=''
                correction=False
                period=0
                toDate=''
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
                Dict_cancel['Nemad']=Nemad
                Dict_cancel['toDate']=toDate
                if('دلایل اصلاح:') in txt:
                    kk=1
                    while find_nth(txt,'\n',kk)<txt.find('دلایل اصلاح:'):
                        kk=kk+1
                        if kk>10:
                            kk=2
                            break
                    Details=txt[txt.find('دلایل اصلاح:'):find_nth(txt,'\n',kk)] 
                Dict_cancel['Details']=Details
                if ('اصلاح') in row['title']:
                    correction=True
                Dict_cancel['correction']=correction    
                for i in Cancel_labels:
                    if check_exists_by_xpath(driver,'//*[contains(@id,"'+i+'")]'):
                        if 'lbl' in i:
                            Dict_cancel[i]=True
                            if 'Other' in i:
                                Dict_cancel[i]=driver.find_element_by_xpath(('//*[contains(@id,"'+i+'")]')).text
                        else:
                            Dict_cancel[i]=driver.find_element_by_xpath(('//*[contains(@id,"'+i+'")]')).text
                listofDicts.append(Dict_cancel)
                counter=counter+1
                print(counter/len(df))
        except:
            continue
    DFCancelation=pd.DataFrame(listofDicts)
    for i in DFCancelation.columns:
        if 'lbl' in i:
            DFCancelation[i].fillna('False',inplace=True)
    DFCancelation.replace('ك','ک',regex=True,inplace=True)
    DFCancelation.replace('ي','ی',regex=True,inplace=True)   
    DFCancelation['Other']=''
    for i in DFCancelation.columns:
        if 'Other' in i:
            DFCancelation['Other']=DFCancelation['Other']+DFCancelation[i]
    DFCancelation['Other']=DFCancelation['Other'].str.replace('False','')
    if 'lblIsBoardMemberIncome' in  DFCancelation.columns:
        if DFCancelation.loc[0,'lblIsBoardMemberIncome']==True:
            DFCancelation['lblIsBoardMemberGift']=True
    if 'lblIsPresentIncome' in  DFCancelation.columns:
        if DFCancelation.loc[0,'lblIsPresentIncome']==True:
            DFCancelation['lblIsBoardMemberWage']=True        
#     return DFCancelation
    for index,row in DFCancelation.iterrows():
        insertCancleation(row)        
def RUN(driver):
    df=get_unconverted()
    if not df.empty:
        handleCancelation(driver,df)
    else:
        print('No New Cancelation_N58')        