import asyncio
from concurrent.futures.thread import ThreadPoolExecutor
from selenium import webdriver
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
from pandas import ExcelWriter

db_username="Jay"
db_pass="Mehrad1"
db_host="127.0.0.1"
db_port="5432"
db_database="FinWisev12"

def update_Empty(Report_ID,TypeOf):
    try:
        connection = psycopg2.connect(user=db_username,
                                    password=db_pass,
                                    host=db_host,
                                    port=db_port,
                                    database=db_database)
        cursor = connection.cursor()
        if TypeOf=='BS':
            postgres_insert_query = """
            UPDATE codalraw."SheetsConverted" SET "Exist_Bs"=NULL WHERE "report_ID"=%s"""
        if TypeOf=='BSCONS':
            postgres_insert_query = """
            UPDATE codalraw."SheetsConverted" SET "Exist_bsCons"=NULL WHERE "report_ID"=%s"""
        if TypeOf=='NetAsset':
            postgres_insert_query = """
            UPDATE codalraw."SheetsConverted" SET "Exist_NetAsset"=NULL WHERE "report_ID"=%s"""
        cursor.execute(postgres_insert_query,(Report_ID,))
        connection.commit()
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print("Failed to Update Error sheets", error)
                log_it('Failed to Update Error sheets -')
    finally:
        if(connection):
            cursor.close()
            connection.close()      
def UpdateError(driver,CodalRaw_ID,TypeOf):
    Error=False
    if check_exists_by_xpath(driver,'//*[text()="متاسفانه سیستم با خطا مواجه شده است."]'):
        Error=True
    if check_exists_by_xpath(driver,'//h2[text()="403 - Forbidden: Access is denied."]'):
        Error=True
    if '<head></head><body></body>' in str(driver.page_source):
        Error=True
    if Error:
        update_Empty(CodalRaw_ID,TypeOf) 

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
    
        record_to_insert = (str(datetime.datetime.now()),text,'BalanceSheets')
        
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
        select "report_ID","HtmlUrl" from codalraw."SheetsConverted" inner join codalraw."allrawReports" on "report_ID"="TracingNo" 
        where ("Exist_Bs"=False or "Exist_bsCons"=False or "Exist_NetAsset"=False) order by "SentTime" desc
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
def get_options(driver):
    listOFOptions=[]
    select = Select(driver.find_element_by_id('ddlTable'))
    for i in select.options:
        listOFOptions.append(str(i.text).strip().replace('\u200c',''))
    return listOFOptions       
def get_balancesheet_type1(driver,TypeOfBS,Report_ID):
    select = Select(driver.find_element_by_id('ddlTable'))
    if TypeOfBS=='BSCONS':
        try:
            select.select_by_visible_text('ترازنامه تلفیقی')
        except:
            select.select_by_visible_text(reverse_damnPersian('ترازنامه تلفیقی'))
    if TypeOfBS=='BS':
        try:
            select.select_by_visible_text('ترازنامه')
        except:
            select.select_by_visible_text(reverse_damnPersian('ترازنامه'))
    time.sleep(4)
    if 'دارایی' not in driver.page_source:  
        print('Emptyyyyy')
        # update_Empty(Report_ID,TypeOfBS)
    # UpdateError(driver,Report_ID,TypeOfBS)
    if 'rayanDynamicStatement' in driver.page_source:
        Type='NG'
    else:
        Type='NonNG'
    # print(Type)
    if(Type=='NG'):
        wholefile=str(driver.page_source)
        wholefile=wholefile[(wholefile.find('"cells":['))+8:]
        wholefile=wholefile[:wholefile.find('</script>')-10]
        wholefile=wholefile[:wholefile.rfind(']')]
        wholefile=wholefile[:wholefile.rfind(']')+1]
        wholefile=wholefile.replace('[','')
        wholefile=wholefile.replace(']','')
        wholefile=wholefile.replace('\u200c', '')
        listofDicts=[]
        for i in range(1,wholefile.count('{')+1):
            try:
                temp=wholefile[find_nth(wholefile,'{',i):find_nth(wholefile,'}',i)+1]
                listofDicts.append(json.loads(temp))
            except:
                continue
        df1=pd.DataFrame(listofDicts)
        # print(df1)
        # writer = ExcelWriter('PythonExport.xlsx')
        # df1.to_excel(writer,'Sheet5')
        # writer.save()
        AssetColumns=df1[(df1['rowTypeName']=='FixedRow' )&(df1['value']!='')&(df1['value']!='')&(df1['address'].str.contains('A'))&(df1['cellGroupName']!='Header')].value.tolist()
        AssetRowSeq=df1[(df1['rowTypeName']=='FixedRow' )&(df1['value']!='')&(df1['value']!='')&(df1['address'].str.contains('A'))&(df1['cellGroupName']!='Header')].rowSequence.tolist()
        AssetPeriod=df1[(df1['rowSequence'].isin(AssetRowSeq) )&(df1['address'].str.contains('B'))&(df1['cellGroupName']!='Header')].value.tolist()
        AssetLastFiscalyear=df1[(df1['rowSequence'].isin(AssetRowSeq) )&(df1['address'].str.contains('C'))&(df1['cellGroupName']!='Header')].value.tolist()
        temp=pd.DataFrame()
        temp['title']=AssetColumns
        temp['thisperiod']=AssetPeriod
        temp['lastyear']=AssetLastFiscalyear
        temp['type']='Asset'
        for x in temp[temp['title'].notnull()].title.unique().tolist():
            if ('بدهی') in x and ('جمع') in x:
                wholefile=str(driver.page_source)
                wholefile=wholefile[(wholefile.find('"cells":['))+8:]
                wholefile=wholefile[:wholefile.find('</script>')-10]
                wholefile=wholefile[:wholefile.rfind(']')]
                wholefile=wholefile[:wholefile.rfind(']')+1]
                wholefile=wholefile.replace('[','')
                wholefile=wholefile.replace(']','')
                wholefile=wholefile.replace('\u200c', '')
                listofDicts=[]
                for i in range(1,wholefile.count('{')+1):
                    try:
                        temp=wholefile[find_nth(wholefile,'{',i):find_nth(wholefile,'}',i)+1]
                        listofDicts.append(json.loads(temp))
                    except:
                        continue
                df1=pd.DataFrame(listofDicts)
                AssetRowSeq=df1[(df1['rowTypeName']=='FixedRow' )&(df1['value']!='')&(df1['address'].str.contains('A'))&(df1['cellGroupName']!='Header')].rowSequence.tolist()
                T1=pd.DataFrame(columns=['title','thisperiod','lastyear','beforelast'])
                for x in AssetRowSeq:
                    titles=df1[(df1['rowSequence']==x)&(df1['address'].str.contains('A'))&(df1['cellGroupName']!='Header')].value.tolist()
                    AssetPeriod=df1[(df1['rowSequence']==x)&(df1['address'].str.contains('B'))&(df1['cellGroupName']!='Header')].value.tolist()
                    AssetLastFiscalyear=df1[(df1['rowSequence']==x)&(df1['address'].str.contains('C'))&(df1['cellGroupName']!='Header')].value.tolist()
                    AssetLast2Fiscalyear=df1[(df1['rowSequence']==x)&(df1['address'].str.contains('D'))&(df1['cellGroupName']!='Header')].value.tolist()
                    temp=pd.DataFrame()
                    temp['title']=titles
                    temp['thisperiod']=AssetPeriod
                    temp['lastyear']=AssetLastFiscalyear
                    temp['beforelast']=AssetLast2Fiscalyear
                    T1=T1.append(temp)
                temp=T1.copy()
                temp.replace('ك','ک',regex=True,inplace=True)
                temp.replace('ي','ی',regex=True,inplace=True)
                temp.reset_index(inplace=True)
                temp.drop(columns='index',inplace=True)
                # temp=temp[(temp['title']!='')&(temp['thisperiod']!='')&(temp['lastyear']!='')&(temp['beforelast']!='')]
                if TypeOfBS=='BSCONS':
                    temp['aggregated']=True
                if TypeOfBS=='BS':
                    temp['aggregated']=False
                
                temp['type']='Asset'
                temp['title']=temp['title'].apply(lambda x : str(x).strip())
                # print(temp)
                try:
                    # print(temp[temp['title']=='جمع داراییها'])
                    temp.loc[temp[temp['title']=='جمع داراییها'].index.tolist()[0]+1:]['type']='Equity'
                except:
                    # print(temp[temp['title']=='جمع دارایی‌ها'])
                    temp.loc[temp[temp['title']=='جمع دارایی‌ها'].index.tolist()[0]+1:]['type']='Equity'
                temp.loc[temp[temp['title']=='جمع حقوق مالکانه'].index.tolist()[0]+1:]['type']='Liability'
                temp=temp[~temp['thisperiod'].str.contains('دوره')]
                return temp

        #////
        LiaEquiColumns=df1[(df1['rowTypeName']=='FixedRow' )&(df1['value']!='')&(df1['value']!='')&(df1['address'].str.contains('E'))&(df1['cellGroupName']!='Header')].value.tolist()
        LiaEquiRowSeq=df1[(df1['rowTypeName']=='FixedRow' )&(df1['value']!='')&(df1['value']!='')&(df1['address'].str.contains('E'))&(df1['cellGroupName']!='Header')].rowSequence.tolist()
        LiaEquiPeriod=df1[(df1['rowSequence'].isin(LiaEquiRowSeq) )&(df1['address'].str.contains('F'))&(df1['cellGroupName']!='Header')].value.tolist()
        LiaEquiLastFiscalyear=df1[(df1['rowSequence'].isin(LiaEquiRowSeq) )&(df1['address'].str.contains('G'))&(df1['cellGroupName']!='Header')].value.tolist()
        
        temp2=pd.DataFrame()
        temp2['title']=LiaEquiColumns
        temp2['thisperiod']=LiaEquiPeriod
        temp2['lastyear']=LiaEquiLastFiscalyear
        temp2['type']='LiaEqui'
        
        if('حقوق صاحبان سهام') in temp2.title.tolist():
            tempEqui=temp2[(temp2.index>=temp2[temp2['title']=='حقوق صاحبان سهام'].index.tolist()[0])&(temp2['title']!='جمع بدهیها و حقوق صاحبان سهام')]
            tempLia=temp2[(temp2.index<temp2[temp2['title']=='حقوق صاحبان سهام'].index.tolist()[0])&(temp2['title']!='جمع بدهیها و حقوق صاحبان سهام')]
        else:
            if('حقوق سرمایه‌گذاران') in temp2.title.tolist():
                tempEqui=temp2[(temp2.index>=temp2[temp2['title']=='حقوق سرمایه‌گذاران'].index.tolist()[0])&(temp2['title']!='جمع بدهی‌ها و حقوق سرمایه‌گذاران')]
                tempLia=temp2[(temp2.index<temp2[temp2['title']=='حقوق سرمایه‌گذاران'].index.tolist()[0])&(temp2['title']!='جمع بدهی‌ها و حقوق سرمایه‌گذاران')]
            else:
                if 'حقوق دارندگان واحدهای سرمایهگذاری' in temp2.title.tolist():
                    tempEqui=temp2[(temp2.index>=temp2[temp2['title']=='حقوق دارندگان واحدهای سرمایهگذاری'].index.tolist()[0])&(temp2['title']!='جمع بدهیها و حقوق دارندگان واحدهای سرمایهگذاری')]
                    tempLia=temp2[(temp2.index<temp2[temp2['title']=='حقوق دارندگان واحدهای سرمایهگذاری'].index.tolist()[0])&(temp2['title']!='جمع بدهیها و حقوق دارندگان واحدهای سرمایهگذاری')]
        tempEqui['type']='Equity'
        tempLia['type']='Liability'
        temp=temp.append(tempLia)
        temp=temp.append(tempEqui)
        temp.replace('ك','ک',regex=True,inplace=True)
        temp.replace('ي','ی',regex=True,inplace=True)
        temp.reset_index(inplace=True)
        temp.drop(columns='index',inplace=True)
        if TypeOfBS=='BSCONS':
            temp['aggregated']=True
        if TypeOfBS=='BS':
            temp['aggregated']=False
        temp['beforelast']=''
        temp.replace('',np.nan,inplace=True)
        temp = temp.where(pd.notnull(temp), None)
        # print(temp)
        return (temp)
    if(Type=='NonNG'):
        AssetNames=[]
        LiabilityDescription=[]
        Asset_Value_thisperiod=[]
        Asset_Value_lastyear=[]
        Liability_thisperiod=[]
        Liability_lastyear=[]

        for i in driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucSFinancialPosition_grdSFinancialPosition"]/tbody/tr[not(contains(@class, "GridHeader"))]'):
            for t in i.find_elements_by_xpath('./td/span'):
                if 'AssetDescription' in t.get_attribute('id'):
                    AssetNames.append(t.text)
                if 'LiabilityDescription' in t.get_attribute('id'):
                    LiabilityDescription.append(t.text)
        for i in driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucSFinancialPosition_grdSFinancialPosition"]/tbody/tr[not(contains(@class, "GridHeader"))]'):
            for t in i.find_elements_by_xpath('./td[contains(@class,"CurrentPeriod Asset")]'):
                Asset_Value_thisperiod.append(get_true_value(t.text))
            for t in i.find_elements_by_xpath('./td[contains(@class,"PastYearEndToDate Asset")]'):
                Asset_Value_lastyear.append(get_true_value(t.text))
            for t in i.find_elements_by_xpath('./td[contains(@class,"CurrentPeriod Liability")]'):
                Liability_thisperiod.append(get_true_value(t.text))
            for t in i.find_elements_by_xpath('./td[contains(@class,"PastYearEndToDate Liability")]'):
                Liability_lastyear.append(get_true_value(t.text))          
        temp=pd.DataFrame()
        temp['title']=AssetNames
        temp['thisperiod']=Asset_Value_thisperiod
        temp['lastyear']=Asset_Value_lastyear
        temp['type']='Asset'
        temp2=pd.DataFrame()
        temp2['title']=LiabilityDescription
        temp2['thisperiod']=Liability_thisperiod
        temp2['lastyear']=Liability_lastyear
        if('حقوق صاحبان سهام') in temp2.title.tolist():
            tempEqui=temp2[(temp2.index>=temp2[temp2['title']=='حقوق صاحبان سهام'].index.tolist()[0])&(temp2['title']!='جمع بدهیها و حقوق صاحبان سهام')]
            tempLia=temp2[(temp2.index<temp2[temp2['title']=='حقوق صاحبان سهام'].index.tolist()[0])&(temp2['title']!='جمع بدهیها و حقوق صاحبان سهام')]
        else:
            if('حقوق سرمایه‌گذاران') in temp2.title.tolist():
                tempEqui=temp2[(temp2.index>=temp2[temp2['title']=='حقوق سرمایه‌گذاران'].index.tolist()[0])&(temp2['title']!='جمع بدهی‌ها و حقوق سرمایه‌گذاران')]
                tempLia=temp2[(temp2.index<temp2[temp2['title']=='حقوق سرمایه‌گذاران'].index.tolist()[0])&(temp2['title']!='جمع بدهی‌ها و حقوق سرمایه‌گذاران')]
            else:
                if 'حقوق دارندگان واحدهای سرمایهگذاری' in temp2.title.tolist():
                    tempEqui=temp2[(temp2.index>=temp2[temp2['title']=='حقوق دارندگان واحدهای سرمایهگذاری'].index.tolist()[0])&(temp2['title']!='جمع بدهیها و حقوق دارندگان واحدهای سرمایهگذاری')]
                    tempLia=temp2[(temp2.index<temp2[temp2['title']=='حقوق دارندگان واحدهای سرمایهگذاری'].index.tolist()[0])&(temp2['title']!='جمع بدهیها و حقوق دارندگان واحدهای سرمایهگذاری')]
        tempEqui['type']='Equity'
        tempLia['type']='Liability'
        temp=temp.append(tempLia)
        temp=temp.append(tempEqui)
        temp.replace('ك','ک',regex=True,inplace=True)
        temp.replace('ي','ی',regex=True,inplace=True)    
        temp.replace('ك','ک',regex=True,inplace=True)
        temp.replace('ي','ی',regex=True,inplace=True)
        temp.reset_index(inplace=True)
        temp.drop(columns='index',inplace=True)
        temp['beforelast']=''
        temp=temp[(temp['title']!='')&(temp['thisperiod']!='')&(temp['lastyear']!='')]
        if TypeOfBS=='BSCONS':
            temp['aggregated']=True
        if TypeOfBS=='BS':
            temp['aggregated']=False
        return temp  
def get_balancesheet_type3(driver,TypeOfBS,Report_ID):
    select = Select(driver.find_element_by_id('ddlTable'))
    # print(TypeOfBS)
    if TypeOfBS=='BSCONS':
        try:
            select.select_by_visible_text('صورت وضعیت مالی تلفیقی')
        except:
            select.select_by_visible_text(reverse_damnPersian('صورت وضعیت مالی تلفیقی'))
    if TypeOfBS=='BS':
        try:
            select.select_by_visible_text('صورت وضعیت مالی')
        except:
            select.select_by_visible_text(reverse_damnPersian('صورت وضعیت مالی'))
    time.sleep(4)
    if 'rayanDynamicStatement' in driver.page_source:
        Type='NG'
    else:
        Type='NonNG'
    # print(Type)
    if 'دارایی' not in driver.page_source:  
        print('Emptyyyyy')
        # update_Empty(Report_ID,TypeOfBS)
    # UpdateError(driver,Report_ID,TypeOfBS)
    if(Type=='NG'):
        try:
            wholefile=str(driver.page_source)
            wholefile=wholefile[(wholefile.find('"cells":['))+8:]
            wholefile=wholefile[:wholefile.find('</script>')-10]
            wholefile=wholefile[:wholefile.rfind(']')]
            wholefile=wholefile[:wholefile.rfind(']')+1]
            wholefile=wholefile.replace('[','')
            wholefile=wholefile.replace(']','')
            wholefile=wholefile.replace('\u200c', '')
            listofDicts=[]
            for i in range(1,wholefile.count('{')+1):
                try:
                    temp=wholefile[find_nth(wholefile,'{',i):find_nth(wholefile,'}',i)+1]
                    listofDicts.append(json.loads(temp))
                except:
                    continue
            df1=pd.DataFrame(listofDicts)
            AssetRowSeq=df1[(df1['rowTypeName']=='FixedRow' )&(df1['value']!='')&(df1['address'].str.contains('A'))&(df1['cellGroupName']!='Header')].rowSequence.tolist()
            T1=pd.DataFrame(columns=['title','thisperiod','lastyear','beforelast'])
            for x in AssetRowSeq:
                titles=df1[(df1['rowSequence']==x)&(df1['address'].str.contains('A'))&(df1['cellGroupName']!='Header')].value.tolist()
                AssetPeriod=df1[(df1['rowSequence']==x)&(df1['address'].str.contains('B'))&(df1['cellGroupName']!='Header')].value.tolist()
                AssetLastFiscalyear=df1[(df1['rowSequence']==x)&(df1['address'].str.contains('C'))&(df1['cellGroupName']!='Header')].value.tolist()
                AssetLast2Fiscalyear=df1[(df1['rowSequence']==x)&(df1['address'].str.contains('D'))&(df1['cellGroupName']!='Header')].value.tolist()
                temp=pd.DataFrame()
                temp['title']=titles
                temp['thisperiod']=AssetPeriod
                temp['lastyear']=AssetLastFiscalyear
                temp['beforelast']=AssetLast2Fiscalyear
                T1=T1.append(temp)
            temp=T1.copy()
            temp.replace('ك','ک',regex=True,inplace=True)
            temp.replace('ي','ی',regex=True,inplace=True)
            temp.reset_index(inplace=True)
            temp.drop(columns='index',inplace=True)
            # temp=temp[(temp['title']!='')&(temp['thisperiod']!='')&(temp['lastyear']!='')&(temp['beforelast']!='')]
            if TypeOfBS=='BSCONS':
                temp['aggregated']=True
            if TypeOfBS=='BS':
                temp['aggregated']=False
            
            temp['type']='Asset'
            temp['title']=temp['title'].apply(lambda x : str(x).strip())
            # print(temp)
            try:
                # print(temp[temp['title']=='جمع داراییها'])
                temp.loc[temp[temp['title']=='جمع داراییها'].index.tolist()[0]+1:]['type']='Equity'
            except:
                # print(temp[temp['title']=='جمع دارایی‌ها'])
                temp.loc[temp[temp['title']=='جمع دارایی‌ها'].index.tolist()[0]+1:]['type']='Equity'
            temp.loc[temp[temp['title']=='جمع حقوق مالکانه'].index.tolist()[0]+1:]['type']='Liability'
            temp=temp[~temp['thisperiod'].str.contains('دوره')]
            return temp
        except Exception as E:
            print(E)
def get_balancesheet_type2_MutualFunds(driver,Report_ID):
    select = Select(driver.find_element_by_id('ddlTable'))
    select.select_by_visible_text('صورت خالص دارایی ها')
    time.sleep(4)
    if 'دارایی' not in driver.page_source:  
        print('Emptyyyyy')
        # update_Empty(Report_ID,'NetAsset')
    # UpdateError(driver,Report_ID,'NetAsset')
    if 'rayanDynamicStatement' in driver.page_source:
        Type='NG'
    else:
        Type='NonNG'
    if(Type=='NG'):
        wholefile=str(driver.page_source)
        wholefile=wholefile[(wholefile.find('"cells":['))+8:]
        wholefile=wholefile[:wholefile.find('</script>')-10]
        wholefile=wholefile[:wholefile.rfind(']')]
        wholefile=wholefile[:wholefile.rfind(']')+1]
        wholefile=wholefile.replace('[','')
        wholefile=wholefile.replace(']','')
        wholefile=wholefile.replace('\u200c', '')
        listofDicts=[]
        for i in range(1,wholefile.count('{')+1):
            try:
                temp=wholefile[find_nth(wholefile,'{',i):find_nth(wholefile,'}',i)+1]
                listofDicts.append(json.loads(temp))
            except:
                continue
        df1=pd.DataFrame(listofDicts)
        AssetRowSeq=df1[(df1['rowTypeName']=='FixedRow' )&(df1['value']!='')&(df1['value']!='')&(df1['address'].str.contains('A'))&(df1['cellGroupName']!='Header')].rowSequence.tolist()
        T1=pd.DataFrame(columns=['title','thisperiod','lastyear','beforelast'])
        for x in AssetRowSeq:
            temp=pd.DataFrame()
            titles=df1[(df1['rowSequence']==x )&(df1['address'].str.contains('A'))&(df1['cellGroupName']!='Header')].value.tolist()
            AssetPeriod=df1[(df1['rowSequence']==x )&(df1['address'].str.contains('B'))&(df1['cellGroupName']!='Header')].value.tolist()
            AssetLastFiscalyear=df1[(df1['rowSequence']==x  )&(df1['address'].str.contains('C'))&(df1['cellGroupName']!='Header')].value.tolist()
            if len([t for t in df1['address'].unique().tolist() if 'D' in t])!=0:
                AssetLast2Fiscalyear=df1[(df1['rowSequence']==x  )&(df1['address'].str.contains('D'))&(df1['cellGroupName']!='Header')].value.tolist()
                temp['beforelast']=AssetLast2Fiscalyear
            else:
                temp['beforelast']=['']
            temp['title']=titles
            temp['thisperiod']=AssetPeriod
            temp['lastyear']=AssetLastFiscalyear
            T1=T1.append(temp)
        temp=T1.copy()
        temp.replace('ك','ک',regex=True,inplace=True)
        temp.replace('ي','ی',regex=True,inplace=True)
        temp.reset_index(inplace=True)
        temp.drop(columns='index',inplace=True)
        temp.replace('',np.nan,inplace=True)
        temp = temp.where(pd.notnull(temp), None)
        temp=temp[(temp['title']!='')&(temp['thisperiod']!='')&(temp['lastyear']!='')]
        return temp 

def Insert_BalanceSheet(DF,CID,Clink,TypeOfReport):
    try:
        DF['report_id']=CID
        DF=DF.replace('',0)
        # print(TypeOfReport)
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
                IF NOT EXISTS (select from statement."BalanceSheet" where "report_id"=%(report_id)s and "Item"=%(title)s and "aggregated"=%(aggregated)s)  THEN
                INSERT INTO statement."BalanceSheet"(
                report_id, "Item", "thisPeriod", "lastYear", "yearBeforeLastyear", type, aggregated)
                VALUES (%(report_id)s,%(title)s, %(thisperiod)s, %(lastyear)s, %(beforelast)s, %(type)s, %(aggregated)s);

                END IF;
            END
            $$ 

        """

        cursor.executemany(postgres_insert_query,DF.to_dict(orient='records'))
        connection.commit()
        if TypeOfReport=='bsCons':
            updateMRquery = """
            UPDATE codalraw."SheetsConverted"
            SET "Exist_bsCons"=True
            WHERE "report_ID"=%s;
            """
            RecordMR=([CID])
            cursor.execute(updateMRquery, RecordMR)
            connection.commit()
            print(str(CID)+' bsCons')
        if TypeOfReport=='Bs':
            updateMRquery = """
            UPDATE codalraw."SheetsConverted"
            SET "Exist_Bs"=True
            WHERE "report_ID"=%s;
            """
            RecordMR=([CID])
            cursor.execute(updateMRquery, RecordMR)
            connection.commit()
            print(str(CID)+' Bs')
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print("Failed to Insert BalanceSheet", error)
                
    finally:
        if(connection):
            cursor.close()
            connection.close()
def Insert_BalanceSheet_MututalFund(DF,CID,Clink,TypeOfReport):
    try:
        DF['report_id']=CID
        DF=DF.replace('',0)
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
                IF NOT EXISTS (select from statement."BalanceSheet_MutualFund" where "report_id"=%(report_id)s and "Item"=%(title)s) THEN
                    INSERT INTO statement."BalanceSheet_MutualFund"(
                    report_id, "Item", "thisPeriod", "lastYear", "yearBeforeLastyear")
                    VALUES ( %(report_id)s,%(title)s, %(thisperiod)s, %(lastyear)s, %(beforelast)s);

                END IF;
            END
            $$ 

        """

        cursor.executemany(postgres_insert_query,DF.to_dict(orient='records'))
        connection.commit()
        if TypeOfReport=='bsCons':
            updateMRquery = """
            UPDATE codalraw."SheetsConverted"
            SET "Exist_bsCons"=True
            WHERE "report_ID"=%s;
            """
            RecordMR=([CID])
            cursor.execute(updateMRquery, RecordMR)
            connection.commit()
            print(str(CID)+' bsCons')
        if TypeOfReport=='Bs':
            updateMRquery = """
            UPDATE codalraw."SheetsConverted"
            SET "Exist_Bs"=True
            WHERE "report_ID"=%s;
            """
            RecordMR=([CID])
            cursor.execute(updateMRquery, RecordMR)
            connection.commit()
            print(str(CID)+' Bs')
        if TypeOfReport=='BSM':
            updateMRquery = """
            UPDATE codalraw."SheetsConverted"
            SET "Exist_NetAsset"=True
            WHERE "report_ID"=%s;
            """
            RecordMR=([CID])
            cursor.execute(updateMRquery, RecordMR)
            connection.commit()
            print(str(CID)+' Bs')
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print("Failed to Insert BalanceSheet", error)
                #log_it('Failed to Insert Insurance -'+str(CID))
    finally:
        if(connection):
            cursor.close()
            connection.close()     
def reverse_damnPersian(x):
    x=x.replace('ک','ك')       
    x=x.replace('ی','ي')
    return x
def damnpersian(x):
    x=x.replace('ك','ک')       
    x=x.replace('ي','ی')
    return x
def takeCareOFBS(driver,CodalRaw_ID,CodalRaw_links):
    allOptions=get_options(driver)
    allOptions2=[damnpersian(x) for x in allOptions]
    # print(allOptions)
    if('ترازنامه') in allOptions or ('ترازنامه') in allOptions2:
        # print('BS')
        Insert_BalanceSheet(get_balancesheet_type1(driver,"BS",CodalRaw_ID),CodalRaw_ID,CodalRaw_links,'Bs')
    if('ترازنامه تلفیقی') in allOptions or ('ترازنامه تلفیقی') in allOptions2:
        # print('BSCons')
        Insert_BalanceSheet(get_balancesheet_type1(driver,"BSCONS",CodalRaw_ID),CodalRaw_ID,CodalRaw_links,'bsCons')
    if('صورت خالص دارایی ها') in allOptions or ('صورت خالص دارایی ها') in allOptions2:
        # print('BSM')
        Insert_BalanceSheet_MututalFund(get_balancesheet_type2_MutualFunds(driver,CodalRaw_ID),CodalRaw_ID,CodalRaw_links,'BSM')
    if('صورت وضعیت مالی') in allOptions or ('صورت وضعیت مالی') in allOptions2:
        # print('BS2')
        Insert_BalanceSheet(get_balancesheet_type3(driver,"BS",CodalRaw_ID),CodalRaw_ID,CodalRaw_links,'Bs')
    if('صورت وضعیت مالی تلفیقی') in allOptions or ('صورت وضعیت مالی تلفیقی') in allOptions2:
        # print('BSC2')
        Insert_BalanceSheet(get_balancesheet_type3(driver,"BSCONS",CodalRaw_ID),CodalRaw_ID,CodalRaw_links,'bsCons')
def RUN(driver,df):
    #df=get_unconverted()
    # driver=webdriver.Chrome()
    # driver.maximize_window()  
    for index,row in df.iterrows():
        try:
            print(row['report_ID'])
            print('https://codal.ir'+str(row['HtmlUrl']))
            driver.get('https://codal.ir'+str(row['HtmlUrl']))
            time.sleep(5)
            takeCareOFBS(driver,row['report_ID'],row['HtmlUrl'])   
        except:
            continue
        
def scrape(df, *, loop):
    loop.run_in_executor(executor, scraper, df)
def scraper(df):
    chrome_options = Options()  
    chrome_options.add_argument("--headless")  
    chrome_options.add_argument('ignore-certificate-errors')

    driver = webdriver.Chrome(chrome_options=chrome_options)
    # driver=webdriver.Chrome()
    driver.maximize_window()  
    RUN(driver,df)
    # driver.quit()
def split_dataframe(df, chunk_size = 100): 
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i*chunk_size:(i+1)*chunk_size])
    return chunks

executor = ThreadPoolExecutor(5)
loop = asyncio.get_event_loop()
chunks=split_dataframe(get_unconverted())
for df in chunks:
    scrape(df, loop=loop)
loop.run_until_complete(asyncio.gather(*asyncio.all_tasks(loop)))