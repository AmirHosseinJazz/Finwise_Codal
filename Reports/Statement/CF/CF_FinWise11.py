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
    
        record_to_insert = (str(datetime.datetime.now()),text,'CashFlow')
        
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
    select "report_ID","HtmlUrl" from codalraw."SheetsConverted" inner join codalraw."allrawReports"
     on "report_ID"="TracingNo" where ("Exist_Cf"=False or "Exist_cfCons"=False) order by "SentTime"
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
def check_types(driver):
    Type='Other'
    typelist=['Other']
    if check_exists_by_xpath(driver,'//table[@id="ctl00_cphBody_ucProduct2_Table1"]'):
        Type='Product'
        prod_type=1
        typelist=['Product',1]
    if check_exists_by_xpath(driver,'//table[@id="ctl00_cphBody_ucProduct1_Table1"]'):
        Type='Product'
        prod_type=2
        typelist=['Product',2]
    if check_exists_by_xpath(driver,'//table[@id="ctl00_cphBody_ucService1_Table1"]'):
        Type='Service'
        typelist=['Service']
    else:
        if check_exists_by_xpath(driver,'//*[@id="ctl00_h1MasterTitle"]'):
            mastertitle=driver.find_element_by_xpath('//*[@id="ctl00_h1MasterTitle"]').text
            if mastertitle=='صورت وضعیت پورتفوی':
                Type='Investment'
                typelist=['Investment']
    if (Type=='Other'):
        wholefile=str(driver.page_source)
        if('"ProductMonthlyActivityDesc1","cells":' in wholefile):
            Type='NewProduct'
            typelist=['NewProduct']
    if check_exists_by_xpath(driver,'//*[contains(@id, "Building")]'):
        Type='Construction'
        typelist=['Construction']
    if check_exists_by_xpath(driver,'//*[contains(@id, "ucLeasing")]'):
        Type='Leasing'
        typelist=['Leasing']
    if check_exists_by_xpath(driver,'//*[contains(@id, "ucBank")]'):
        Type='Bank'
        typelist=['Bank']
    if check_exists_by_xpath(driver,'//*[contains(@id, "Insurance")]'):
        Type='Insurance'
        typelist=['Insurance']
    return typelist  
def get_options(driver):
    listOFOptions=[]
    try:
        select = Select(driver.find_element_by_id('ddlTable'))
    except: 
        select = Select(driver.find_element_by_id('ctl00_ddlTable'))
    for i in select.options:
        listOFOptions.append(str(i.text).strip().replace('\u200c',''))
    return listOFOptions       
def get_CF_type1_nonAggregated(driver):
    try:
        select = Select(driver.find_element_by_id('ddlTable'))
    except: 
        select = Select(driver.find_element_by_id('ctl00_ddlTable'))
    allOptions=get_options(driver)
    if('صورت جریان های نقدی' in allOptions):
        select.select_by_visible_text('صورت جریان های نقدی')
        time.sleep(4)
    else:
        if  'صورت جریان وجوه نقد' in allOptions:
            select.select_by_visible_text('صورت جریان وجوه نقد')
            time.sleep(4)
        else:
            if  'جریان وجوه نقد' in allOptions:
                select.select_by_visible_text('جریان وجوه نقد')        
                time.sleep(4)   
    if 'rayanDynamicStatement' in driver.page_source:
        Type='NG'
    else:
        Type='NonNG'
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
            #wholefile=wholefile.replace('"','\'')
            listofDicts=[]
            for i in range(1,wholefile.count('{')+1):
                try:
                    temp=wholefile[find_nth(wholefile,'{',i):find_nth(wholefile,'}',i)+1]
                    listofDicts.append(json.loads(temp))
                except:
                    continue
            df1=pd.DataFrame(listofDicts)
            headers=df1[(df1['rowTypeName']=='FixedRow' )&(df1['value']!='')&(df1['cellGroupName']=='Header')].value.tolist()
            while 'درصد تغییر' in headers:
                headers.remove('درصد تغییر')
            while 'درصد تغییرات' in headers:
                headers.remove('درصد تغییرات')
            while 'درصد\n تغییرات' in headers:
                headers.remove('درصد\n تغییرات')
            while 'درصد\nتغييرات' in headers:
                headers.remove('درصد\nتغييرات')
            for k in headers:
                if ('شرح' in k) and headers.index(k)!=0:
                    headers.remove(k)
                    headers.insert(0,k)
            dict1=[]
            for head in headers:
                Column=df1[(df1['value']==head)&(df1['cellGroupName']=='Header')].address.tolist()
                ColumnCheck=''
                for item in Column:
                    for k in item:
                        if not k.isdigit():
                            ColumnCheck=ColumnCheck+k
                if len(ColumnCheck)>1:
                    for i in ColumnCheck:
                        T=df1[(df1['address'].str.contains(i))&(df1['cellGroupName']!='Header')].value.tolist()
                        dict1.append(T)
                else:
                    T=df1[(df1['address'].str.contains(ColumnCheck))&(df1['cellGroupName']!='Header')].value.tolist()
                    dict1.append(T)
            DF=pd.DataFrame(dict1).transpose()  

            DF.replace('ك','ک',regex=True,inplace=True)
            DF.replace('ي','ی',regex=True,inplace=True)
            DF.replace('',np.nan,inplace=True)
            DF = DF.where(pd.notnull(DF), None)
            DF=DF.T.drop_duplicates().T
            for k in DF.columns:
                    if len(DF[k].unique().tolist())<=2:
                            DF.drop(columns=[k],inplace=True)
            if len(DF.columns)==4:
                DF.columns=['Item','ThisPeriod','ThisPeriodLastYear','LastYear']
            if len(DF.columns)==3:
                DF.columns=['Item','ThisPeriod','LastYear']
                DF['ThisPeriodLastYear']=np.nan     
            DF = DF.where(pd.notnull(DF), None)               
            DF.reset_index(inplace=True)
            DF.drop(columns='index',inplace=True)
            DF.dropna(how='all',inplace=True)
            DF['Order']=DF.index
            DF['aggregated']=False
            DF=DF[(DF['Item'].notnull()) & (DF['Item']!='--')]
            return DF
        except:
            return pd.DataFrame()
    if(Type=='NonNG'):
        try:
            if len(driver.find_elements_by_xpath('//th'))==5:
                dict1={}
                for k in range(5):
                    listOf=[]
                    for t in driver.find_elements_by_xpath('//table[contains(@id,"gvCashFlow")]//tr/td['+str(k+1)+']'):
                        g=''
                        if t.text=='' or t.text==None:
                            if check_exists_by_xpath(t,'./input'):
                                g=(get_true_value(t.find_element_by_xpath('./input').get_attribute('value')))
                        else:
                                g=t.text
                        listOf.append(g)
                    dict1['listof'+str(k+1)]=listOf
                DF=pd.DataFrame(dict1)
                DF.columns=['Item','ThisPeriod','ThisPeriodLastYear','Change','LastYear']    
            else:
                dict1={}
                for k in range(4):
                    listOf=[]
                    for t in driver.find_elements_by_xpath('//table[contains(@id,"gvCashFlow")]//tr/td['+str(k+1)+']'):
                        g=''
                        if t.text=='' or t.text==None:
                            if check_exists_by_xpath(t,'./input'):
                                g=(get_true_value(t.find_element_by_xpath('./input').get_attribute('value')))
                        else:
                                g=t.text
                        listOf.append(g)
                    dict1['listof'+str(k+1)]=listOf
                DF=pd.DataFrame(dict1)
                DF.columns=['Item','ThisPeriod','LastYear','Change']  
                DF['ThisPeriodLastYear']=np.nan
            DF['Order']=DF.index
            DF.replace('ك','ک',regex=True,inplace=True)
            DF.replace('ي','ی',regex=True,inplace=True)
            DF.replace('',np.nan,inplace=True)
            DF = DF.where(pd.notnull(DF), None)
            DF['aggregated']=False
            DF=DF[(DF['Item'].notnull()) & (DF['Item']!='--')]
            return DF
        except:
            return pd.DataFrame()
def get_CF_type1_Aggregated(driver):
    try:
        select = Select(driver.find_element_by_id('ddlTable'))
    except: 
        select = Select(driver.find_element_by_id('ctl00_ddlTable'))
    allOptions=get_options(driver)
    if('صورت جریان های نقدی تلفیقی' in allOptions):
        select.select_by_visible_text('صورت جریان های نقدی تلفیقی')
        time.sleep(2)
    else:
        if  'جریان وجوه نقد تلفیقی' in allOptions:
            select.select_by_visible_text('جریان وجوه نقد تلفیقی')
            time.sleep(2)
        else:
            if  'صورت جریان وجوه نقد تلفیقی' in allOptions:
                select.select_by_visible_text('صورت جریان وجوه نقد تلفیقی')  
                time.sleep(2)      
    if 'rayanDynamicStatement' in driver.page_source:
        Type='NG'
    else:
        Type='NonNG'
    
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
            #wholefile=wholefile.replace('"','\'')
            listofDicts=[]
            for i in range(1,wholefile.count('{')+1):
                try:
                    temp=wholefile[find_nth(wholefile,'{',i):find_nth(wholefile,'}',i)+1]
                    listofDicts.append(json.loads(temp))
                except:
                    continue
            df1=pd.DataFrame(listofDicts)
            headers=df1[(df1['rowTypeName']=='FixedRow' )&(df1['value']!='')&(df1['cellGroupName']=='Header')].value.tolist()
            while 'درصد تغییر' in headers:
                headers.remove('درصد تغییر')
            while 'درصد تغییرات' in headers:
                headers.remove('درصد تغییرات')
            while 'درصد\n تغییرات' in headers:
                headers.remove('درصد\n تغییرات')
            while 'درصد\nتغييرات' in headers:
                headers.remove('درصد\nتغييرات')
            for k in headers:
                if ('شرح' in k) and headers.index(k)!=0:
                    headers.remove(k)
                    headers.insert(0,k)
            dict1=[]
            for head in headers:
                Column=df1[(df1['value']==head)&(df1['cellGroupName']=='Header')].address.tolist()
                ColumnCheck=''
                for item in Column:
                    for k in item:
                        if not k.isdigit():
                            ColumnCheck=ColumnCheck+k
                if len(ColumnCheck)>1:
                    for i in ColumnCheck:
                        T=df1[(df1['address'].str.contains(i))&(df1['cellGroupName']!='Header')].value.tolist()
                        dict1.append(T)
                else:
                    T=df1[(df1['address'].str.contains(ColumnCheck))&(df1['cellGroupName']!='Header')].value.tolist()
                    dict1.append(T)
            DF=pd.DataFrame(dict1).transpose()  

            DF.replace('ك','ک',regex=True,inplace=True)
            DF.replace('ي','ی',regex=True,inplace=True)
            DF.replace('',np.nan,inplace=True)
            DF = DF.where(pd.notnull(DF), None)
            DF=DF.T.drop_duplicates().T
            for k in DF.columns:
                    if len(DF[k].unique().tolist())<=2:
                            DF.drop(columns=[k],inplace=True)
            if len(DF.columns)==4:
                DF.columns=['Item','ThisPeriod','ThisPeriodLastYear','LastYear']
            if len(DF.columns)==3:
                DF.columns=['Item','ThisPeriod','LastYear']
                DF['ThisPeriodLastYear']=np.nan     
            DF = DF.where(pd.notnull(DF), None)               
            DF.reset_index(inplace=True)
            DF.drop(columns='index',inplace=True)
            DF.dropna(how='all',inplace=True)
            DF['Order']=DF.index
            DF['aggregated']=True
            DF=DF[(DF['Item'].notnull()) & (DF['Item']!='--')]
            return DF
        except:
            return pd.DataFrame()
    if(Type=='NonNG'):
        try:
            if len(driver.find_elements_by_xpath('//th'))==5:
                dict1={}
                for k in range(5):
                    listOf=[]
                    for t in driver.find_elements_by_xpath('//table[contains(@id,"gvCashFlow")]//tr/td['+str(k+1)+']'):
                        g=''
                        if t.text=='' or t.text==None:
                            if check_exists_by_xpath(t,'./input'):
                                g=(get_true_value(t.find_element_by_xpath('./input').get_attribute('value')))
                        else:
                                g=t.text
                        listOf.append(g)
                    dict1['listof'+str(k+1)]=listOf
                DF=pd.DataFrame(dict1)
                DF.columns=['Item','ThisPeriod','ThisPeriodLastYear','Change','LastYear']    
            else:
                dict1={}
                for k in range(4):
                    listOf=[]
                    for t in driver.find_elements_by_xpath('//table[contains(@id,"gvCashFlow")]//tr/td['+str(k+1)+']'):
                        g=''
                        if t.text=='' or t.text==None:
                            if check_exists_by_xpath(t,'./input'):
                                g=(get_true_value(t.find_element_by_xpath('./input').get_attribute('value')))
                        else:
                                g=t.text
                        listOf.append(g)
                    dict1['listof'+str(k+1)]=listOf
                DF=pd.DataFrame(dict1)
                DF.columns=['Item','ThisPeriod','LastYear','Change']  
                DF['ThisPeriodLastYear']=np.nan
                
            DF['Order']=DF.index
            DF.replace('ك','ک',regex=True,inplace=True)
            DF.replace('ي','ی',regex=True,inplace=True)
            DF.replace('',np.nan,inplace=True)
            DF = DF.where(pd.notnull(DF), None)
            DF['aggregated']=True
            DF=DF[(DF['Item'].notnull()) & (DF['Item']!='--')]
            return DF   
        except:
            return pd.DataFrame()     
def InsertCF(DF,CID,Clink,TypeOfReport):
    if not DF.empty:
        try:
            DF['report_id']=CID
            DF.replace('--',np.nan,inplace=True)
            DF = DF.where(pd.notnull(DF), None)
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
                    IF NOT EXISTS (select from statement."CashFlow" where "report_id"=%(report_id)s and "Item"=%(Item)s) THEN
                    INSERT INTO statement."CashFlow"(
                    report_id, "Item", "ThisPeriod", "LastYearThisPeriod", "LastYear", "Order", "Aggregated")
                    VALUES (%(report_id)s, %(Item)s, %(ThisPeriod)s, %(ThisPeriodLastYear)s, %(LastYear)s, %(Order)s, %(aggregated)s);
                    END IF;
                END
                $$ 

            """

            cursor.executemany(postgres_insert_query,DF.to_dict(orient='records'))
            connection.commit()
            if TypeOfReport=='CF':
                updateMRquery = """
                UPDATE codalraw."SheetsConverted"
                SET "Exist_Cf"=True
                WHERE "report_ID"=%s;
                """
                RecordMR=([CID])
                cursor.execute(updateMRquery, RecordMR)
                connection.commit()
                print(str(CID)+' CF')
            if TypeOfReport=='CFCons':
                updateMRquery = """
                UPDATE codalraw."SheetsConverted"
                SET "Exist_cfCons"=True
                WHERE "report_ID"=%s;
                """
                RecordMR=([CID])
                cursor.execute(updateMRquery, RecordMR)
                connection.commit()
                print(str(CID)+' CFCons')
            
        except(Exception, psycopg2.Error) as error:
                if(connection):
                    print("Failed to Insert CF", error)
        finally:
            if(connection):
                cursor.close()
                connection.close()
    else:
        if TypeOfReport=='CF':
            InsertError(CID,'NotCons') 
        if TypeOfReport=='CFCons':
            InsertError(CID,'Cons') 
def InsertError(CodalRaw_ID,type):
    try:
        connection = psycopg2.connect(user=db_username,
                                    password=db_pass,
                                    host=db_host,
                                    port=db_port,
                                    database=db_database)
        cursor = connection.cursor()
        if type=='NotCons':
            postgres_insert_query = """
            
            UPDATE codalraw."SheetsConverted" SET "Exist_Cf"=NULL WHERE "report_ID"=%s

            """
        if type=='Cons':
            postgres_insert_query = """
            
            UPDATE codalraw."SheetsConverted" SET "Exist_cfCons"=NULL WHERE "report_ID"=%s

            """
        cursor.execute(postgres_insert_query,(CodalRaw_ID,))
        connection.commit()
        log_it('Set To NULL Rights' + str(CodalRaw_ID) )
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print("Failed to Update Error sheets", error)
                log_it('Failed to Update Error sheets -')
    finally:
        if(connection):
            cursor.close()
            connection.close()  
def takeCareOFCF(driver,CodalRaw_ID,CodalRaw_links):
    allOptions=get_options(driver)
    if('صورت جریان های نقدی' in allOptions) or ('صورت جریان وجوه نقد' in allOptions) or ('جریان وجوه نقد' in allOptions):
        InsertCF(get_CF_type1_nonAggregated(driver),CodalRaw_ID,CodalRaw_links,'CF')
    if('صورت جریان های نقدی تلفیقی' in allOptions) or ('جریان وجوه نقد تلفیقی' in allOptions) or ('صورت جریان وجوه نقد تلفیقی' in allOptions):
        InsertCF(get_CF_type1_Aggregated(driver),CodalRaw_ID,CodalRaw_links,'CFCons')

def RUN():
    df=get_unconverted()
    driver=webdriver.Chrome()
    driver.maximize_window()  
    for index,row in df.iterrows():
        driver.get('https://codal.ir'+str(row['HtmlUrl']))
        takeCareOFCF(driver,row['report_ID'],row['HtmlUrl'])
RUN()