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
IC_license=['txbAuditCommitteeDate','txbAuditCommitteeMemberCount','txbInternalAuditDate','txbInternalAuditMemberCount',
           'txbInternalAuditContract','txbDate']
false_Labels=['txbAuditCommitteeDate','txbAuditCommitteeMemberCount','txbInternalAuditDate','txbInternalAuditMemberCount',
           'txbInternalAuditContract','txbDate']
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
    
        record_to_insert = (str(datetime.datetime.now()),text,'N41')
        
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
        select * FROM codalraw."allrawReports" where "LetterCode"='ن-۴۱' and "HtmlUrl"!='' and "Available"=True and "TracingNo" not in (select "report_ID" from codalraw."BoardChangeConverted")
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
def InsertBoard(row):
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
                        INSERT INTO  public."Publishers"(
                        ticker,"Type")
                        VALUES (%(Nemad)s,81
                        );
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
                    IF NOT EXISTS (select from  codalreports."BoardChange_AuditComittee" where "report_id"=%(report_id)s and "FullName"=%(FullName)s) THEN
                        INSERT INTO  codalreports."BoardChange_AuditComittee"(
                         firm, report_id, "Correction", 
                         "CorrectionDetails", "PublishDate",
                         "ReportDate", "AuditCommitteeDate", "AuditCommitteeMemberCount", 
                         "FullName", "SSID", "Position",
                         "DegreeField", "LastDegree", "JoinDate", "CV")
                        VALUES ( (select "ID" from "Publishers" where "persianName"=%(Nemad)s), %(report_id)s,
                        %(correction)s,%(Details)s,
                        %(DateOf)s,%(txbDate)s,%(txbAuditCommitteeDate)s,%(txbAuditCommitteeMemberCount)s,%(FullName)s,
                        %(SSID)s,%(Position)s,%(degree)s,%(LastDegree)s,%(JoinDate)s,%(CVFull)s
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
                    IF NOT EXISTS (select from  codalraw."BoardChangeConverted" where "report_ID"=%s) THEN
                        INSERT INTO  codalraw."BoardChangeConverted"(
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
                print("Failed to Insert AuditComittee ", error)
                log_it('Failed to Insert AuditComittee -'+str(row['report_id']))
    finally:
        if(connection):
            cursor.close()
            connection.close()   
def InsertInternalAudit(row):
    try:
        connection = psycopg2.connect(user=db_username,
                                      password=db_pass,
                                      host=db_host,
                                      port=db_port,
                                      database=db_database)
        cursor = connection.cursor()
        Dict=row.to_dict()
        postgres_insert_query = """
            DO 
            $$
                BEGIN
                    IF NOT EXISTS (select from  codalreports."BoardChange_InternalAudit" where "report_id"=%(report_id)s and "FullName"=%(FullName)s) THEN
                        INSERT INTO  codalreports."BoardChange_InternalAudit"(
                         firm, report_id, "Correction", "CorrectionDetails", 
                         "PublishDate", "ReportDate", "InternalAuditDate", 
                         "InternalAuditMemberCount", "InternalAuditContract", 
                         "FullName", "SSID", "Position", "DegreeField",
                         "LastDegree", "JoinDate", "CV")
                        VALUES ( (select "ID" from "Publishers" where "persianName"=%(Nemad)s), %(report_id)s,
                        %(correction)s,%(Details)s,
                        %(DateOf)s,%(txbDate)s,%(txbInternalAuditDate)s,
                        %(txbInternalAuditMemberCount)s,%(txbInternalAuditContract)s,%(FullName)s,
                        %(SSID)s,%(Position)s,%(degree)s,%(LastDegree)s
                        ,%(JoinDate)s,%(CVFull)s
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
                    IF NOT EXISTS (select from  codalraw."BoardChangeConverted" where "report_ID"=%s) THEN
                        INSERT INTO  codalraw."BoardChangeConverted"(
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
                print("Failed to Insert InternalAudit ", error)
                log_it('Failed to Insert InternalAudit -'+str(row['report_id']))
    finally:
        if(connection):
            cursor.close()
            connection.close()   
def InsertHighestFinancial(row):
    try:

        connection = psycopg2.connect(user=db_username,
                                      password=db_pass,
                                      host=db_host,
                                      port=db_port,
                                      database=db_database)
        cursor = connection.cursor()
        Dict=row.to_dict()
        postgres_insert_query = """
            DO 
            $$
                BEGIN
                    IF NOT EXISTS (select from  codalreports."BoardChange_HighestFinancialPosition" where "report_id"=%(report_id)s and "FullName"=%(FullName)s) THEN
                        INSERT INTO  codalreports."BoardChange_HighestFinancialPosition"(
                        firm, report_id, "Correction", "CorrectionDetails", "PublishDate",
                        "ReportDate", "FullName", "SSID", 
                        "Position", "DegreeField", "LastDegree", "JoinDate", "CV")
                        VALUES ( (select "ID" from "Publishers" where "persianName"=%(Nemad)s), %(report_id)s,
                        %(correction)s,%(Details)s,
                        %(DateOf)s,%(txbDate)s,%(FullName)s,
                        %(SSID)s,%(Position)s,%(degree)s,%(LastDegree)s
                        ,%(JoinDate)s,%(CVFull)s
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
                    IF NOT EXISTS (select from  codalraw."BoardChangeConverted" where "report_ID"=%s) THEN
                        INSERT INTO  codalraw."BoardChangeConverted"(
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
                print("Failed to Insert HighestFinancial ", error)
                log_it('Failed to Insert HighestFinancial -'+str(row['report_id']))
    finally:
        if(connection):
            cursor.close()
            connection.close()   
def Translate_Audit_Comittee(driver):
    dictHead={}
    counter=1
    for tablerow in driver.find_elements_by_xpath('//*[@id="ctl00_cphBody_ucAuditMember_gvAudit"]//tbody/tr[1]'):
        for j in tablerow.find_elements_by_xpath('.//th'):
            dictHead[counter]=j.text.replace('\n','')
            counter=counter+1
    listofDictrows=[]
    counter=1
    for tablerow in driver.find_elements_by_xpath('//*[@id="ctl00_cphBody_ucAuditMember_gvAudit"]//tr'):
        DictRow={}
        counter=1
        for j in tablerow.find_elements_by_xpath('.//td'):
            DictRow[counter]=j.text.replace('\n','')
            counter=counter+1
        listofDictrows.append(DictRow)
    DFROWS=pd.DataFrame(listofDictrows)
    DFROWS.dropna(inplace=True)
    DFROWS.columns=dictHead.values()        
    swap_dict={
        'نام و نام خانوادگی':'FullName',
        'کدملی':'SSID',
        'سمت':'Position',
        'عضو غیرموظف هیات مدیره':'NonBoardMember',
        'شخص حقوقی':'Legal',
        'عضو مستقل':'IndependentMemberTitle',
        'عضو مالی':'FinancialMemberTitle',
        'رشته تحصیلی':'degree',
        'آخرین مقطع تحصیلی':'LastDegree',
        'تاریخ عضویت در کمیته حسابرسی':'JoinDate',
        'هم سوابق - مدت زمان':'CV',
        'سریال سمت':'SerialTitle',
        'اهم سوابق - مدت زمان/محل و موضوع سابقه فعالیت':"CVFull"


    }
    newcols=[]
    for i in DFROWS.columns:
        newcols.append(swap_dict[i])
    DFROWS.columns=newcols
    return DFROWS 
def Translate_Internal_Audit_Comittee(driver):
    dictHead={}
    counter=1
    for tablerow in driver.find_elements_by_xpath('//*[@id="ctl00_cphBody_ucInternalAudit_gvInternal"]//tbody/tr[1]'):
        for j in tablerow.find_elements_by_xpath('.//th'):
            dictHead[counter]=j.text.replace('\n','')
            counter=counter+1
    listofDictrows=[]
    counter=1
    for tablerow in driver.find_elements_by_xpath('//*[@id="ctl00_cphBody_ucInternalAudit_gvInternal"]//tr'):
        DictRow={}
        counter=1
        for j in tablerow.find_elements_by_xpath('.//td'):
            DictRow[counter]=j.text.replace('\n','')
            counter=counter+1
        listofDictrows.append(DictRow)
    DFROWS=pd.DataFrame(listofDictrows)
    DFROWS.dropna(inplace=True)
    DFROWS.columns=dictHead.values()        
    swap_dict={
        'نام و نام خانوادگی':'FullName',
        'کدملی':'SSID',
        'سمت':'Position',
        'عضو غیرموظف هیات مدیره':'NonBoardMember',
        'شخص حقوقی':'Legal',
        'عضو مستقل':'IndependentMemberTitle',
        'عضو مالی':'FinancialMemberTitle',
        'رشته تحصیلی':'degree',
        'آخرین مقطع تحصیلی':'LastDegree',
        'تاریخ انتصاب':'JoinDate',
        'هم سوابق - مدت زمان':'CV',
        'سریال سمت':'SerialTitle',
        'اهم سوابق - مدت زمان/محل و موضوع سابقه فعالیت':"CVFull"


    }
    newcols=[]
    for i in DFROWS.columns:
        newcols.append(swap_dict[i])
    DFROWS.columns=newcols
    return DFROWS
def Translate_HighestFinancial(driver):
    dictHead={}
    counter=1
    for tablerow in driver.find_elements_by_xpath('//*[@id="ctl00_cphBody_UcHighestFinancial_gvInternal"]//tbody/tr[1]'):
        for j in tablerow.find_elements_by_xpath('.//th'):
            dictHead[counter]=j.text.replace('\n','')
            counter=counter+1
    listofDictrows=[]
    counter=1
    for tablerow in driver.find_elements_by_xpath('//*[@id="ctl00_cphBody_UcHighestFinancial_gvInternal"]//tr'):
        DictRow={}
        counter=1
        for j in tablerow.find_elements_by_xpath('.//td'):
            DictRow[counter]=j.text.replace('\n','')
            counter=counter+1
        listofDictrows.append(DictRow)
    DFROWS=pd.DataFrame(listofDictrows)
    DFROWS.dropna(inplace=True)
    DFROWS.columns=dictHead.values()        
    swap_dict={
        'نام و نام خانوادگی':'FullName',
        'کدملی':'SSID',
        'سمت':'Position',
        'عضو غیرموظف هیات مدیره':'NonBoardMember',
        'شخص حقوقی':'Legal',
        'عضو مستقل':'IndependentMemberTitle',
        'عضو مالی':'FinancialMemberTitle',
        'رشته تحصیلی':'degree',
        'آخرین مقطع تحصیلی':'LastDegree',
        'تاریخ انتصاب':'JoinDate',
        'هم سوابق - مدت زمان':'CV',
        'سریال سمت':'SerialTitle',
        'اهم سوابق - مدت زمان/محل و موضوع سابقه فعالیت':"CVFull"


    }
    newcols=[]
    for i in DFROWS.columns:
        newcols.append(swap_dict[i])
    DFROWS.columns=newcols
    return DFROWS
def handle_all(driver,df):
    listofDicts=[]
    counter=0
    for index,row in df.iterrows():
        try:
            CodalRaw_ID=str(row['TracingNo'])
            CodalRaw_links=row['HtmlUrl']
            driver.get('http://codal.ir'+CodalRaw_links)
            if UpdateError(driver,CodalRaw_ID):
                time.sleep(2)
                Dict_IC_Time={}
                Dict_IC_Time['report_id']=CodalRaw_ID
                Nemad=''
                correction=False
                Subject='BoardChanges'
                Details=''
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
                if('دلایل اصلاح') in txt:
                    kk=1
                    while find_nth(txt,'<',kk)<txt.find('دلایل اصلاح:'):
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
                Dict_IC_Time['Subject']=Subject
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
                Dict_IC_Time['AuditCom']=Translate_Audit_Comittee(driver)
                Dict_IC_Time['InternalAudi']=Translate_Internal_Audit_Comittee(driver)
                Dict_IC_Time['HighestFinancial']=Translate_HighestFinancial(driver)
                listofDicts.append(Dict_IC_Time)
                counter=counter+1
                print(counter/len(df))
        except:
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
            DFIC[i]=False
    AllDFSBoards=[]
    for index,row in DFIC.iterrows():
        temp=(row['AuditCom'])
        temp['report_id']=row['report_id']
        temp['Nemad']=row['Nemad']
        temp['DateOf']=row['DateOf']
        temp['txbDate']=row['txbDate']
        temp['txbInternalAuditDate']=row['txbInternalAuditDate']
        temp['txbInternalAuditMemberCount']=row['txbInternalAuditMemberCount']
        temp['txbInternalAuditContract']=row['txbInternalAuditContract']
        temp['txbAuditCommitteeDate']=row['txbAuditCommitteeDate']
        temp['txbAuditCommitteeMemberCount']=row['txbAuditCommitteeMemberCount']
        temp['correction']=row['correction']
        temp['Details']=row['Details']
        AllDFSBoards.append(temp)
    AllDFInternalAudits=[]   
    for index,row in DFIC.iterrows():
        temp=(row['InternalAudi'])
        temp['report_id']=row['report_id']
        temp['Nemad']=row['Nemad']
        temp['DateOf']=row['DateOf']
        temp['txbDate']=row['txbDate']
        temp['txbInternalAuditDate']=row['txbInternalAuditDate']
        temp['txbInternalAuditMemberCount']=row['txbInternalAuditMemberCount']
        temp['txbInternalAuditContract']=row['txbInternalAuditContract']
        temp['txbAuditCommitteeDate']=row['txbAuditCommitteeDate']
        temp['txbAuditCommitteeMemberCount']=row['txbAuditCommitteeMemberCount']
        temp['correction']=row['correction']
        temp['Details']=row['Details']
        AllDFInternalAudits.append(temp)
    AllDFHighestFinancial=[]   
    for index,row in DFIC.iterrows():
        temp=(row['HighestFinancial'])
        temp['report_id']=row['report_id']
        temp['Nemad']=row['Nemad']
        temp['DateOf']=row['DateOf']
        temp['txbDate']=row['txbDate']
        temp['txbInternalAuditDate']=row['txbInternalAuditDate']
        temp['txbInternalAuditMemberCount']=row['txbInternalAuditMemberCount']
        temp['txbInternalAuditContract']=row['txbInternalAuditContract']
        temp['txbAuditCommitteeDate']=row['txbAuditCommitteeDate']
        temp['txbAuditCommitteeMemberCount']=row['txbAuditCommitteeMemberCount']
        temp['correction']=row['correction']
        temp['Details']=row['Details']
        AllDFHighestFinancial.append(temp)
    for t in AllDFSBoards:
        for index,row in t.iterrows():
            InsertBoard(row)
    for t in AllDFInternalAudits:
        for index,row in t.iterrows():
            InsertInternalAudit(row)        
    for t in AllDFHighestFinancial:
        for index,row in t.iterrows():
            InsertHighestFinancial(row)        
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
        print('All Audit CHANGES DONE')            