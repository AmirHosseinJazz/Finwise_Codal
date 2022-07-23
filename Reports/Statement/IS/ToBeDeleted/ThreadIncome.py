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
    
        record_to_insert = (str(datetime.datetime.now()),text,'Income')
        
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
        df = psql.read_sql("""select "report_ID","HtmlUrl" from codalraw."SheetsConverted" 
        inner join codalraw."allrawReports" on "report_ID"="TracingNo" where 
        ("Exist_Income"=False or "Exist_IncomeCons"=False or "Exist_IncomeComp"=False  or "Exist_IncomeCompCons"=False)
        
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
def get_IncomeStatement_typeI(driver,TypeOfIS):
    results={}
    descs=[]
    select = Select(driver.find_element_by_id('ddlTable'))
    if TypeOfIS=="ISCONS":
        select.select_by_visible_text('صورت سود و زیان تلفیقی')
        time.sleep(2)
    if TypeOfIS=="IS":
        select.select_by_visible_text('صورت سود و زیان')
        time.sleep(2)
    if 'rayanDynamicStatement' in driver.page_source:
        Type='NG'
        wholefile=str(driver.page_source)
        wholefile=wholefile[(wholefile.find('"cells":['))+8:]
        wholefile=wholefile[:wholefile.find('</script>')-10]
        wholefile=wholefile[:wholefile.rfind(']')]
        wholefile=wholefile[:wholefile.rfind(']')+1]
        wholefile=wholefile.replace('[','')
        wholefile=wholefile.replace(']','')
        wholefile=wholefile.replace('\u200c', '')
        #wholefile=wholefile.replace('"','\'')
        listofDicts=[]
        for i in range(1,wholefile.count('{')+1):
            try:
                temp=wholefile[find_nth(wholefile,'{',i):find_nth(wholefile,'}',i)+1]
                listofDicts.append(json.loads(temp))
            except:
                continue
        df1=pd.DataFrame(listofDicts)
        descs=df1[(df1['rowSequence'].isin((df1[(df1['value'].str.contains('دلا'))].rowSequence+1).tolist()) )&(df1['address'].str.contains('A'))&(df1['cellGroupName']!='Header')].value.tolist()
        results['desc']=descs
        titles=df1[(df1['rowTypeName']=='FixedRow' )&(df1['value']!='')&(df1['value']!='')&(df1['address'].str.contains('A'))&(df1['cellGroupName']!='Header')].value.tolist()
        AssetRowSeq=df1[(df1['rowTypeName']=='FixedRow' )&(df1['value']!='')&(df1['value']!='')&(df1['address'].str.contains('A'))&(df1['cellGroupName']!='Header')].rowSequence.tolist()
        titlesPeriod=df1[(df1['rowSequence'].isin(AssetRowSeq) )&(df1['address'].str.contains('B'))&(df1['cellGroupName']!='Header')].value.tolist()
        titlesLastyearthisperiod=df1[(df1['rowSequence'].isin(AssetRowSeq) )&(df1['address'].str.contains('C'))&(df1['cellGroupName']!='Header')].value.tolist()
        titlesLastyearFiscal=df1[(df1['rowSequence'].isin(AssetRowSeq) )&(df1['address'].str.contains('D'))&(df1['cellGroupName']!='Header')&(df1['formula']=='')].value.tolist()
        
        if len(set(titlesLastyearFiscal))<2:
            titlesLastyearFiscal=df1[(df1['rowSequence'].isin(AssetRowSeq) )&(df1['address'].str.contains('E'))&(df1['cellGroupName']!='Header')].value.tolist()
        else:
            titlesLastyearFiscal=df1[(df1['rowSequence'].isin(AssetRowSeq) )&(df1['address'].str.contains('D'))&(df1['cellGroupName']!='Header')].value.tolist()
        for i in range(len(titles)-len(titlesPeriod)):
            titlesPeriod.append('')    
        for i in range(len(titles)-len(titlesLastyearthisperiod)):
            titlesLastyearthisperiod.append('')    
        for i in range(len(titles)-len(titlesLastyearFiscal)):
            titlesLastyearFiscal.append('') 
        temp=pd.DataFrame()
        temp['title']=titles
        temp['thisperiod']=titlesPeriod
        temp['LastYearThisperiod']=titlesLastyearthisperiod
        temp['LastYear']=titlesLastyearFiscal

        for i in ['title','thisperiod','LastYear']:
                    temp=temp[temp[i]!='']
        temp.replace('ك','ک',regex=True,inplace=True)
        temp.replace('ي','ی',regex=True,inplace=True)
        temp=temp[temp['title']!='دوره منتهی به']
        temp.reset_index(inplace=True)
        temp.drop(columns='index',inplace=True)
        if TypeOfIS=="ISCONS":
            temp['aggregated']=True
        if TypeOfIS=="IS":
            temp['aggregated']=False
        
        results['result']=temp
        return results
    else:
        Type='NonNG'
        if check_exists_by_xpath(driver,'//*[@id="ctl00_cphBody_ucInterimStatement_txbInterimStatementDsc1"]'):
            desc_thisperiod=driver.find_element_by_xpath('//*[@id="ctl00_cphBody_ucInterimStatement_txbInterimStatementDsc1"]').text
            descs.append(desc_thisperiod)
        else:
            desc_thisperiod=''
            descs.append(desc_thisperiod)
        if check_exists_by_xpath(driver,'//*[@id="ctl00_cphBody_ucInterimStatement_txbInterimStatementDsc2"]'):
            desc_lastyear_thisperiod=driver.find_element_by_xpath('//*[@id="ctl00_cphBody_ucInterimStatement_txbInterimStatementDsc2"]').text
            descs.append(desc_lastyear_thisperiod)
        else:
            desc_lastyear_thisperiod=''
            descs.append(desc_lastyear_thisperiod)                
        if check_exists_by_xpath(driver,'//*[@id="ctl00_cphBody_ucInterimStatement_txbInterimStatementDsc3"]'):
            desc_lastyear=driver.find_element_by_xpath('//*[@id="ctl00_cphBody_ucInterimStatement_txbInterimStatementDsc3"]').text
            descs.append(desc_lastyear)    
        else:
            desc_lastyear=''
            descs.append(desc_lastyear)
        results['desc']=descs
        statementTitles=[]
        Thisperiod=[]
        LastYearThisperiod=[]
        LastYear=[]
        for i in driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucInterimStatement_grdInterimStatement"]/tbody/tr[not(contains(@class, "GridHeader"))]'):
            for t in i.find_elements_by_xpath('./td[contains(@class,"CurrentPeriod")]'):
                Thisperiod.append(get_true_value(t.text))
            for t in i.find_elements_by_xpath('./td[contains(@class,"PastSimillarPeriod")]'):
                LastYearThisperiod.append(get_true_value(t.text))
            for t in i.find_elements_by_xpath('./td[contains(@class,"PastYearEndToDate")]'):
                LastYear.append(get_true_value(t.text))
            for t in i.find_elements_by_xpath('./td[contains(@class,"DescriptionColumn")]'):
                statementTitles.append(get_true_value(t.text))
        temp=pd.DataFrame()
        temp['title']=statementTitles
        temp['thisperiod']=Thisperiod
        if(len(LastYearThisperiod)>0):
            temp['LastYearThisperiod']=LastYearThisperiod
        if(len(LastYear)>0):
            temp['LastYear']=LastYear
        
        for i in temp.columns:
            temp=temp[temp[i]!='']
        temp=temp[temp['title']!='دوره منتهی به']
        if temp.empty:
            results['result']=pd.DataFrame()
            return results
        if ('LastYearThisperiod' in temp.columns) &('LastYear' in temp.columns):
            if (temp.LastYearThisperiod.tolist()[0])>(temp.LastYear.tolist()[0]):
                temp['tmp']=temp['LastYear']
                temp['LastYear']=temp['LastYearThisperiod']
                temp['LastYearThisperiod']=temp['tmp']
                temp.drop(columns=['tmp'],inplace=True)
        temp.replace('ك','ک',regex=True,inplace=True)
        temp.replace('ي','ی',regex=True,inplace=True)    
        temp.reset_index(inplace=True)
        temp.drop(columns='index',inplace=True)
        if TypeOfIS=="ISCONS":
            temp['aggregated']=True
        if TypeOfIS=="IS":
            temp['aggregated']=False
        results['result']=temp
        return results
def get_IncomeStatement_typeII_jame(driver,TypeOfIS):
    results={}
    descs=[]
    select = Select(driver.find_element_by_id('ddlTable'))
    if TypeOfIS=="ISCONSCOMP":
        select.select_by_visible_text('صورت سود و زیان جامع تلفیقی')
        time.sleep(2)
    if TypeOfIS=="ISCOMP":
        select.select_by_visible_text('صورت سود و زیان جامع')
        time.sleep(2)
    
    
    if 'rayanDynamicStatement' in driver.page_source:
        Type='NG'
        wholefile=str(driver.page_source)
        wholefile=wholefile[(wholefile.find('"cells":['))+8:]
        wholefile=wholefile[:wholefile.find('</script>')-10]
        wholefile=wholefile[:wholefile.rfind(']')]
        wholefile=wholefile[:wholefile.rfind(']')+1]
        wholefile=wholefile.replace('[','')
        wholefile=wholefile.replace(']','')
        wholefile=wholefile.replace('\u200c', '')
        #wholefile=wholefile.replace('"','\'')
        listofDicts=[]
        for i in range(1,wholefile.count('{')+1):
            try:
                temp=wholefile[find_nth(wholefile,'{',i):find_nth(wholefile,'}',i)+1]
                listofDicts.append(json.loads(temp))
            except:
                continue
        df1=pd.DataFrame(listofDicts)
        descs=df1[(df1['rowSequence'].isin((df1[(df1['value'].str.contains('دلا'))].rowSequence+1).tolist()) )&(df1['address'].str.contains('A'))&(df1['cellGroupName']!='Header')].value.tolist()
        results['desc']=descs
        titles=df1[(df1['rowTypeName']=='FixedRow' )&(df1['value']!='')&(df1['value']!='')&(df1['address'].str.contains('A'))&(df1['cellGroupName']!='Header')].value.tolist()
        AssetRowSeq=df1[(df1['rowTypeName']=='FixedRow' )&(df1['value']!='')&(df1['value']!='')&(df1['address'].str.contains('A'))&(df1['cellGroupName']!='Header')].rowSequence.tolist()
        titlesPeriod=df1[(df1['rowSequence'].isin(AssetRowSeq) )&(df1['address'].str.contains('B'))&(df1['cellGroupName']!='Header')].value.tolist()
        titlesLastyearthisperiod=df1[(df1['rowSequence'].isin(AssetRowSeq) )&(df1['address'].str.contains('C'))&(df1['cellGroupName']!='Header')].value.tolist()
        titlesLastyearFiscal=df1[(df1['rowSequence'].isin(AssetRowSeq) )&(df1['address'].str.contains('D'))&(df1['cellGroupName']!='Header')&(df1['formula']=='')].value.tolist()
        
        if len(set(titlesLastyearFiscal))<2:
            titlesLastyearFiscal=df1[(df1['rowSequence'].isin(AssetRowSeq) )&(df1['address'].str.contains('E'))&(df1['cellGroupName']!='Header')].value.tolist()
        else:
            titlesLastyearFiscal=df1[(df1['rowSequence'].isin(AssetRowSeq) )&(df1['address'].str.contains('D'))&(df1['cellGroupName']!='Header')].value.tolist()
        for i in range(len(titles)-len(titlesPeriod)):
            titlesPeriod.append('')    
        for i in range(len(titles)-len(titlesLastyearthisperiod)):
            titlesLastyearthisperiod.append('')    
        for i in range(len(titles)-len(titlesLastyearFiscal)):
            titlesLastyearFiscal.append('') 
        temp=pd.DataFrame()
        temp['title']=titles
        temp['thisperiod']=titlesPeriod
        temp['LastYearThisperiod']=titlesLastyearthisperiod
        temp['LastYear']=titlesLastyearFiscal

        for i in ['title','thisperiod','LastYear']:
                    temp=temp[temp[i]!='']
        temp.replace('ك','ک',regex=True,inplace=True)
        temp.replace('ي','ی',regex=True,inplace=True)
        temp=temp[temp['title']!='دوره منتهی به']
        temp.reset_index(inplace=True)
        temp.drop(columns='index',inplace=True)
        temp['aggregated']=False
        results['result']=temp
        return results
    else:
        Type='NonNG'
        if check_exists_by_xpath(driver,'//*[@id="ctl00_cphBody_ucInterimStatement_txbInterimStatementDsc1"]'):
            desc_thisperiod=driver.find_element_by_xpath('//*[@id="ctl00_cphBody_ucInterimStatement_txbInterimStatementDsc1"]').text
            descs.append(desc_thisperiod)
        else:
            desc_thisperiod=''
            descs.append(desc_thisperiod)
        if check_exists_by_xpath(driver,'//*[@id="ctl00_cphBody_ucInterimStatement_txbInterimStatementDsc2"]'):
            desc_lastyear_thisperiod=driver.find_element_by_xpath('//*[@id="ctl00_cphBody_ucInterimStatement_txbInterimStatementDsc2"]').text
            descs.append(desc_lastyear_thisperiod)
        else:
            desc_lastyear_thisperiod=''
            descs.append(desc_lastyear_thisperiod)                
        if check_exists_by_xpath(driver,'//*[@id="ctl00_cphBody_ucInterimStatement_txbInterimStatementDsc3"]'):
            desc_lastyear=driver.find_element_by_xpath('//*[@id="ctl00_cphBody_ucInterimStatement_txbInterimStatementDsc3"]').text
            descs.append(desc_lastyear)    
        else:
            desc_lastyear=''
            descs.append(desc_lastyear)
        results['desc']=descs
        statementTitles=[]
        Thisperiod=[]
        LastYearThisperiod=[]
        LastYear=[]
        for i in driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucInterimStatement_grdInterimStatement"]/tbody/tr[not(contains(@class, "GridHeader"))]'):
            for t in i.find_elements_by_xpath('./td[contains(@class,"CurrentPeriod")]'):
                Thisperiod.append(get_true_value(t.text))
            for t in i.find_elements_by_xpath('./td[contains(@class,"PastSimillarPeriod")]'):
                LastYearThisperiod.append(get_true_value(t.text))
            for t in i.find_elements_by_xpath('./td[contains(@class,"PastYearEndToDate")]'):
                LastYear.append(get_true_value(t.text))
            for t in i.find_elements_by_xpath('./td[contains(@class,"DescriptionColumn")]'):
                statementTitles.append(get_true_value(t.text))
        temp=pd.DataFrame()
        temp['title']=statementTitles
        temp['thisperiod']=Thisperiod
        if(len(LastYearThisperiod)>0):
            temp['LastYearThisperiod']=LastYearThisperiod
        if(len(LastYear)>0):
            temp['LastYear']=LastYear
        
        for i in temp.columns:
            temp=temp[temp[i]!='']
        temp=temp[temp['title']!='دوره منتهی به']
        if temp.empty:
            results['result']=pd.DataFrame()
            return results
        if ('LastYearThisperiod' in temp.columns) &('LastYear' in temp.columns):
            if (temp.LastYearThisperiod.tolist()[0])>(temp.LastYear.tolist()[0]):
                temp['tmp']=temp['LastYear']
                temp['LastYear']=temp['LastYearThisperiod']
                temp['LastYearThisperiod']=temp['tmp']
                temp.drop(columns=['tmp'],inplace=True)
        temp.replace('ك','ک',regex=True,inplace=True)
        temp.replace('ي','ی',regex=True,inplace=True)    
        temp.reset_index(inplace=True)
        temp.drop(columns='index',inplace=True)
        if TypeOfIS=="ISCONSCOMP":
            temp['aggregated']=True
        if TypeOfIS=="ISCOMP":
            temp['aggregated']=False
        results['result']=temp
        return results 
def get_options(driver):
    listOFOptions=[]
    select = Select(driver.find_element_by_id('ddlTable'))
    for i in select.options:
        listOFOptions.append(str(i.text).strip().replace('\u200c',''))
    return listOFOptions               
def takeCareOFIS(driver,CodalRaw_ID,CodalRaw_links):
    try:
        allOptions=get_options(driver)
        if('صورت سود و زیان') in allOptions:
            Insert_IncomeStatement(get_IncomeStatement_typeI(driver,"IS"),CodalRaw_ID,CodalRaw_links,'Income')
        if('صورت سود و زیان جامع') in allOptions:
            Insert_Comprehensive_IncomeStatement(get_IncomeStatement_typeII_jame(driver,""),CodalRaw_ID,CodalRaw_links,'IncomeComp')
        if('صورت سود و زیان تلفیقی') in allOptions:
            Insert_IncomeStatement(get_IncomeStatement_typeI(driver,"ISCONS"),CodalRaw_ID,CodalRaw_links,'IncomeCons')
        if('صورت سود و زیان جامع تلفیقی') in allOptions:
            Insert_Comprehensive_IncomeStatement(get_IncomeStatement_typeII_jame(driver,"ISCONSCOMP"),CodalRaw_ID,CodalRaw_links,'IncomeCompCons')       
    except Exception as E:
        print(E)

def Insert_IncomeStatement(results,CID,Clink,TypeOfReport):
    try:
        connection = psycopg2.connect(user=db_username,
                                      password=db_pass,
                                      host=db_host,
                                      port=db_port,
                                      database=db_database)
        cursor = connection.cursor()
        DF=results['result']
        DESCS=results['desc']
        for i in range(3-len(DESCS)):
            DESCS.append('')
            
        if not DF.empty:
            DF['desc_thisperiod']=DESCS[0]
            DF['desc_thisperiodlastYear']=DESCS[1]
            DF['desc_lastYear']=DESCS[2]
            DF['report_id']=CID
            if('LastYear') not in DF.columns.tolist():
                DF['LastYear']=0
            DF=DF.replace('',0)
           
            postgres_insert_query = """
             DO 
                $$
                BEGIN
                    IF NOT EXISTS (select from statement."IncomeStatement" where "report_id"=%(report_id)s and "Item"=%(title)s and "aggregated"=%(aggregated)s) THEN
                       INSERT INTO statement."IncomeStatement"(
                        report_id, "Item", "thisPeriod", "lastYearThisperiod", 
                        "lastYear", aggregated, "desc_thisPeriod", "desc_thisperiodLastyear", "desc_lastYear")
                        VALUES (%(report_id)s, %(title)s, %(thisperiod)s, %(LastYearThisperiod)s, %(LastYear)s,
                        %(aggregated)s, %(desc_thisperiod)s, %(desc_thisperiodlastYear)s, %(desc_lastYear)s);

                    END IF;
                END
                $$ 

            """

            cursor.executemany(postgres_insert_query,DF.to_dict(orient='records'))
            connection.commit()
            if TypeOfReport=='Income':
                updateMRquery = """
                UPDATE codalraw."SheetsConverted"
                SET "Exist_Income"=True
                WHERE "report_ID"=%s;
                """
                RecordMR=([CID])
                cursor.execute(updateMRquery, RecordMR)
                connection.commit()
                print(str(CID)+' Income')
            if TypeOfReport=='IncomeCons':
                updateMRquery = """
                UPDATE codalraw."SheetsConverted"
                SET "Exist_IncomeCons"=True
                WHERE "report_ID"=%s;
                """
                RecordMR=([CID])
                cursor.execute(updateMRquery, RecordMR)
                connection.commit()
                print(str(CID)+' IncomeCons')
            print(str(CID)+'  '+'--Done')
            
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print("Failed to Insert IncomeStatement", error)
                #log_it('Failed to Insert IncomeStatement -'+str(CID))
    finally:
        if(connection):
            cursor.close()
            connection.close()
def Insert_Comprehensive_IncomeStatement(results,CID,Clink,TypeOfReport):
    try:
        
        connection = psycopg2.connect(user=db_username,
                                      password=db_pass,
                                      host=db_host,
                                      port=db_port,
                                      database=db_database)
        cursor = connection.cursor()
        DF=results['result']
        DESCS=results['desc']
        for i in range(3-len(DESCS)):
            DESCS.append('')
            
        if not DF.empty:
            DF['desc_thisperiod']=DESCS[0]
            DF['desc_thisperiodlastYear']=DESCS[1]
            DF['desc_lastYear']=DESCS[2]
            DF['report_id']=CID
            if('LastYear') not in DF.columns.tolist():
                DF['LastYear']=0
            DF=DF.replace('',0)
           
            postgres_insert_query = """
             DO 
                $$
                BEGIN
                    IF NOT EXISTS (select from statement."IncomeStatement_Comprehensive" where "report_id"=%(report_id)s and "Item"=%(title)s and "aggregated"=%(aggregated)s) THEN
                       INSERT INTO statement."IncomeStatement_Comprehensive"(
                        report_id, "Item", "thisPeriod", "lastYearThisperiod", "lastYear", aggregated,
                        desc_thisperiod, "desc_thisperiodLastYear", "desc_lastYear")
                        VALUES (%(report_id)s, %(title)s, %(thisperiod)s, %(LastYearThisperiod)s, %(LastYear)s,
                        %(aggregated)s, %(desc_thisperiod)s, %(desc_thisperiodlastYear)s, %(desc_lastYear)s);

                    END IF;
                END
                $$ 

            """

            cursor.executemany(postgres_insert_query,DF.to_dict(orient='records'))
            connection.commit()
            if TypeOfReport=='IncomeComp':
                updateMRquery = """
                UPDATE codalraw."SheetsConverted"
                SET  "Exist_IncomeComp"=True
                WHERE "report_ID"=%s;
                """
                RecordMR=([CID])
                cursor.execute(updateMRquery, RecordMR)
                connection.commit()
                print(str(CID)+'IncomeComp')
            if TypeOfReport=='IncomeCompCons':
                updateMRquery = """
                UPDATE codalraw."SheetsConverted"
                SET  "Exist_IncomeCompCons"=True
                WHERE "report_ID"=%s;
                """
                RecordMR=([CID])
                cursor.execute(updateMRquery, RecordMR)
                connection.commit()
                print(str(CID)+'IncomeCompCons')
            print(str(CID)+'  '+'--Done')
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print("Failed to Insert IncomeStatement", error)
                #log_it('Failed to Insert IncomeStatement -'+str(CID))
    finally:
        if(connection):
            cursor.close()
            connection.close()                                     
def RUN(driver,df):
    #df=get_unconverted()
    for index,row in df.iterrows():
        try:
            driver.get('https://codal.ir'+str(row['HtmlUrl']))
            time.sleep(2)
            takeCareOFIS(driver,row['report_ID'],row['HtmlUrl'])          
        except:
            continue
def scrape(df, *, loop):
    loop.run_in_executor(executor, scraper, df)
def scraper(df):
    chrome_options = Options()  
    chrome_options.add_argument("--headless")  
    driver = webdriver.Chrome(chrome_options=chrome_options)
    # driver=webdriver.Chrome()
    # driver.maximize_window()  
    RUN(driver,df)
    driver.quit()
def split_dataframe(df, chunk_size = 150): 
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i*chunk_size:(i+1)*chunk_size])
    return chunks

executor = ThreadPoolExecutor(10)
loop = asyncio.get_event_loop()
chunks=split_dataframe(get_unconverted())
for df in chunks:
    scrape(df, loop=loop)
loop.run_until_complete(asyncio.gather(*asyncio.all_tasks(loop)))