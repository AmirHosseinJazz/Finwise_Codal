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


def UpdateError(driver,CodalRaw_ID,type):
    Error=False
    if check_exists_by_xpath(driver,'//*[text()="متاسفانه سیستم با خطا مواجه شده است."]'):
        Error=True
    if check_exists_by_xpath(driver,'//*[@id="Table2"]//span[text()="ضمائم"]'):
        Error=True
    if check_exists_by_xpath(driver,'//h2[text()="403 - Forbidden: Access is denied."]'):
        Error=True
    if '<head></head><body></body>' in str(driver.page_source):
        Error=True
    if Error:
        InsertError(CodalRaw_ID,type)
        return False
    else:
        return True
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
            
            UPDATE codalraw."SheetsConverted" SET "Exist_PropertyRight"=NULL WHERE "report_ID"=%s

            """
        if type=='Cons':
            postgres_insert_query = """
            
            UPDATE codalraw."SheetsConverted" SET "Exist_PropertyRightCons"=NULL WHERE "report_ID"=%s

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
    
        record_to_insert = (str(datetime.datetime.now()),text,'rights')
        
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
         on "report_ID"="TracingNo" where ("Exist_PropertyRightCons"=False or "Exist_PropertyRight"=False) order by "SentTime" desc
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
def takeCareOFRights(CodalRaw_ID,CodalRaw_links,driver):
    allOptions=get_options(driver)
    if('صورت تغییرات در حقوق مالکانه') in allOptions:
        Insert_RightStatement(get_Rights_notCons(CodalRaw_ID,driver),CodalRaw_ID,CodalRaw_links,'Right')
    if('صورت تغییرات در حقوق مالکانه تلفیقی') in allOptions:
        Insert_RightStatement_CONS(get_Rights_Cons(CodalRaw_ID,driver),CodalRaw_ID,CodalRaw_links,'RightCons')      
def Insert_RightStatement(results,CID,Clink,TypeOfReport):
    try:
        connection = psycopg2.connect(user=db_username,
                                      password=db_pass,
                                      host=db_host,
                                      port=db_port,
                                      database=db_database)
        cursor = connection.cursor()
        DF=results['result']
        if not DF.empty:
            DF['report_id']=CID
          
            postgres_insert_query = """
             DO 
                $$
                BEGIN
                    IF NOT EXISTS (select from statement."PropertyRights" where "report_id"=%(report_id)s and "Desc"=%(Desc)s and "Aggregated"=%(aggregated)s and "Order"=%(Order)s) THEN                   
                    INSERT INTO statement."PropertyRights"(
                    report_id, "Desc", "Capital", "CurrentIncreaseCapital", "SarfSaham", "SarfSahamKhazane", "LegalReserve", "OtherReserves", "ReevalutaionSurplus", "TasiirDifference", "Retained", "SahamKhazane", "TotalSum", "Aggregated","Order")
                    VALUES ( %(report_id)s, %(Desc)s, %(Capital)s, %(CurrentIC)s, %(SarfSaham)s, %(SarfSahamKhazane)s,
                     %(AndookhteGhanuni)s, %(SayerAndookhteha)s, %(MazadeTajdid)s, %(TafavoteTasiir)s, %(Anbashte)s, %(SahamKhazane)s, %(TotalSum)s, %(aggregated)s,%(Order)s );

                    END IF;
                END
                $$ 

            """

            cursor.executemany(postgres_insert_query,DF.to_dict(orient='records'))
            connection.commit()
            if TypeOfReport=='Right':
                updateMRquery = """
                UPDATE codalraw."SheetsConverted"
                SET "Exist_PropertyRight"=True
                WHERE "report_ID"=%s;
                """
                RecordMR=([CID])
                cursor.execute(updateMRquery, RecordMR)
                connection.commit()
                print(str(CID)+' Right')
            if TypeOfReport=='RightCons':
                updateMRquery = """
                UPDATE codalraw."SheetsConverted"
                SET "Exist_PropertyRightCons"=True
                WHERE "report_ID"=%s;
                """
                RecordMR=([CID])
                cursor.execute(updateMRquery, RecordMR)
                connection.commit()
                print(str(CID)+' RightCons')
            print(str(CID)+'  '+'--Done')
            
    except(Exception, psycopg2.Error) as error:
        if TypeOfReport=='Right':
            InsertError(CID,'NotCons')
        if TypeOfReport=='RightCons':
            InsertError(CID,'Cons')
        if(connection):
            print("Failed to Insert Rights", error)
                #log_it('Failed to Insert IncomeStatement -'+str(CID))
    finally:
        if(connection):
            cursor.close()
            connection.close()
def Insert_RightStatement_CONS(results,CID,Clink,TypeOfReport):
    try:
        connection = psycopg2.connect(user=db_username,
                                      password=db_pass,
                                      host=db_host,
                                      port=db_port,
                                      database=db_database)
        cursor = connection.cursor()
        DF=results['result']
        if not DF.empty:
            DF['report_id']=CID
          
            postgres_insert_query = """
             DO 
                $$
                BEGIN
                    IF NOT EXISTS (select from statement."PropertyRights" where "report_id"=%(report_id)s and "Desc"=%(Desc)s and "Aggregated"=%(aggregated)s and "Order"=%(Order)s) THEN                   
                    INSERT INTO statement."PropertyRights"(
                    report_id, "Desc", "Capital", "CurrentIncreaseCapital", "SarfSaham", "SarfSahamKhazane", "LegalReserve", "OtherReserves", "ReevalutaionSurplus", "TasiirDifference", "Retained", "SahamKhazane", "TotalSum", "Aggregated","Order","GhabeleEntesabBeFirm","NotControledResources")
                    VALUES ( %(report_id)s, %(Desc)s, %(Capital)s, %(CurrentIC)s, %(SarfSaham)s, %(SarfSahamKhazane)s,
                     %(AndookhteGhanuni)s, %(SayerAndookhteha)s, %(MazadeTajdid)s, %(TafavoteTasiir)s, %(Anbashte)s, %(SahamKhazane)s, %(TotalSum)s, %(aggregated)s,%(Order)s ,%(GhabeleEntesab)s,%(NotContorledResources)s);

                    END IF;
                END
                $$ 

            """

            cursor.executemany(postgres_insert_query,DF.to_dict(orient='records'))
            connection.commit()
            if TypeOfReport=='Right':
                updateMRquery = """
                UPDATE codalraw."SheetsConverted"
                SET "Exist_PropertyRight"=True
                WHERE "report_ID"=%s;
                """
                RecordMR=([CID])
                cursor.execute(updateMRquery, RecordMR)
                connection.commit()
                print(str(CID)+' Right')
            if TypeOfReport=='RightCons':
                updateMRquery = """
                UPDATE codalraw."SheetsConverted"
                SET "Exist_PropertyRightCons"=True
                WHERE "report_ID"=%s;
                """
                RecordMR=([CID])
                cursor.execute(updateMRquery, RecordMR)
                connection.commit()
                print(str(CID)+' RightCons')
                
            print(str(CID)+'  '+'--Done')
            
    except(Exception, psycopg2.Error) as error:
        
        if TypeOfReport=='Right':
            InsertError(CID,'NotCons')
        if TypeOfReport=='RightCons':
            InsertError(CID,'Cons')
        if(connection):
            print("Failed to Insert Rights", error)
                #log_it('Failed to Insert IncomeStatement -'+str(CID))
    finally:
        if(connection):
            cursor.close()
            connection.close()            
def get_Rights_notCons(report_id,driver):
    results={}
    descs=[]
    select = Select(driver.find_element_by_id('ddlTable'))
    select.select_by_visible_text('صورت تغییرات در حقوق مالکانه')
    if not (UpdateError(driver,report_id,'NotCons')):
        return pd.DataFrame()
    try:
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
            headers=df1[(df1['rowTypeName']=='FixedRow')&(df1['value']!='')&(df1['cellGroupName']=='Header')].value.tolist()
            headers_dict={
            'شرح':'Desc',
            'سرمایه':'Capital',
            'افزایش سرمایه در جریان':'CurrentIC',
            'صرف سهام':'SarfSaham',
            'صرف سهام خزانه':'SarfSahamKhazane',
            'اندوخته قانونی':'AndookhteGhanuni',
            'سایر اندوختهها':'SayerAndookhteha',
            'مازاد تجدید ارزیابی داراییها':'MazadeTajdid',
            'تفاوت تسعیر ارز عملیات خارجی':'TafavoteTasiir',
            'سود (زيان) انباشته':'Anbashte',
            'سود انباشته':'Anbashte',
            'سهام خزانه':'SahamKhazane',
            'جمع کل':'TotalSum'

            }
            Test=pd.DataFrame(columns=[headers_dict[t] for t in headers])
            for head in headers:
                Column=df1[(df1['value']==head)&(df1['cellGroupName']=='Header')].address.tolist()[0]
                ColumnCheck=''
                for k in Column:
                    if not k.isdigit():
                        ColumnCheck=ColumnCheck+k
                T=df1[(df1['address'].str.contains(ColumnCheck))&(df1['cellGroupName']!='Header')].value.tolist()
                Test[headers_dict[head]]=T
            Test.replace('ك','ک',regex=True,inplace=True)
            Test.replace('ي','ی',regex=True,inplace=True)
            Test.replace('',np.nan,inplace=True)
            Test = Test.where(pd.notnull(Test), None)
            Test.reset_index(inplace=True)
            Test.drop(columns='index',inplace=True)
            Test['aggregated']=False
            results['result']=Test
            Test['Order']=Test.index
            return results
        else:
            InsertError(report_id,'Cons')  
    except:
        InsertError(report_id,'NotCons')      
def get_Rights_Cons(report_id,driver):
    results={}
    descs=[]
    select = Select(driver.find_element_by_id('ddlTable'))
    select.select_by_visible_text('صورت تغییرات در حقوق مالکانه تلفیقی')
    if not (UpdateError(driver,report_id,'NotCons')):
        return pd.DataFrame()
    try:
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
            headers=df1[(df1['rowTypeName']=='FixedRow')&(df1['value']!='')&(df1['cellGroupName']=='Header')].value.tolist()
            headers_dict={
            'شرح':'Desc',
            'سرمایه':'Capital',
            'افزایش سرمایه در جریان':'CurrentIC',
            'صرف سهام':'SarfSaham',
            'صرف سهام خزانه':'SarfSahamKhazane',
            'اندوخته قانونی':'AndookhteGhanuni',
            'سایر اندوختهها':'SayerAndookhteha',
            'مازاد تجدید ارزیابی داراییها':'MazadeTajdid',
            'تفاوت تسعیر ارز عملیات خارجی':'TafavoteTasiir',
            'سود (زيان) انباشته':'Anbashte',
            'قابل انتساب به مالکان شرکت اصلی':'GhabeleEntesab',
            'منافع فاقد حق کنترل':'NotContorledResources',
            'سود انباشته':'Anbashte',
            'سهام خزانه':'SahamKhazane',
            'جمع کل':'TotalSum'

            }
            Test=pd.DataFrame(columns=[headers_dict[t] for t in headers])
            for head in headers:
                Column=df1[(df1['value']==head)&(df1['cellGroupName']=='Header')].address.tolist()[0]
                ColumnCheck=''
                for k in Column:
                    if not k.isdigit():
                        ColumnCheck=ColumnCheck+k
                T=df1[(df1['address'].str.contains(ColumnCheck))&(df1['cellGroupName']!='Header')].value.tolist()
                Test[headers_dict[head]]=T
            Test.replace('ك','ک',regex=True,inplace=True)
            Test.replace('ي','ی',regex=True,inplace=True)
            Test.replace('',np.nan,inplace=True)
            Test = Test.where(pd.notnull(Test), None)
            Test.reset_index(inplace=True)
            Test.drop(columns='index',inplace=True)
            Test['aggregated']=True
            results['result']=Test
            Test['Order']=Test.index
            return results
        else:
            InsertError(report_id,'Cons')                                                 
    except:
        InsertError(report_id,'Cons')                                                 
def RUN(driver,df):
    # driver=webdriver.Chrome()
    # driver.maximize_window()  
    #df=get_unconverted()
    print('Initializing Rights Handler...')
    for index,row in df.iterrows():
        driver.get('https://codal.ir'+str(row['HtmlUrl']))
        time.sleep(4)
        takeCareOFRights(row['report_ID'],row['HtmlUrl'],driver)            
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