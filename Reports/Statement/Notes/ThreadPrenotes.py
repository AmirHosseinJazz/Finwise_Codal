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

def Translate_NotesPages(driver):
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
    listofDicts2=[]
    for i in range(1,wholefile.count(',"title_En":')+1):
        try:
            temp=wholefile[find_nth(wholefile,',"title_En":',i)-20:find_nth(wholefile,',"title_En":',i)+200]
            listofDicts2.append(temp)
        except:
            continue
    highlvlheaderstemp={}
    highlvlheaders={}
    for i in listofDicts2:
        highlvlheaderstemp[re.findall('"metaTableId.*title_Fa',i)[0][:-10].split(',')[0].split(':')[1]]=re.findall('"metaTableId.*title_Fa',i)[0][:-10].split(',')[1].split(':')[1]
    for key,val in highlvlheaderstemp.items():
        highlvlheaders[key]=val.replace('"','')
    hlvldf=pd.DataFrame.from_dict(highlvlheaders,orient='index')
    hlvldf.reset_index(inplace=True)
    hlvldf.columns=['metaID','highlvlDFName'] 
    hlvldf['metaID']=hlvldf['metaID'].astype(int)   
    ALLDFS=[]
    TableIDs=df1.metaTableId.unique()
    for metatableID in TableIDs:
        try:
            dftemp=df1[df1['metaTableId']==metatableID]
            dfheaders=dftemp[dftemp['cellGroupName']=='Header'] 
            if len(dfheaders)==0:
                rawdict={}
                rawdict[metatableID]='-'.join(dftemp.value.tolist())
                DFt=pd.DataFrame.from_dict(rawdict,orient='index')
                DFt.reset_index(inplace=True)
                DFt.columns=['metaID','desc']
                ALLDFS.append(DFt)
            else:
                
                dfheaders=dfheaders[['address','rowSpan','rowCode','rowSequence','colSpan','columnCode','columnSequence','value']]
                for index,row in dfheaders.iterrows():
                    dfheaders.at[index,'level']=row['rowCode']-dfheaders.rowCode.min()   
                dfheaders['columnSequence']=dfheaders['columnSequence'].astype(int)  
                for k in dfheaders.level.unique().tolist():
                    if k!=0:
                        dfheaderstemp=dfheaders[dfheaders['level']==k]
                        for index,row in dfheaderstemp.iterrows():
                            df3=dfheaders[dfheaders['level']<=row['level']-1]
                            df3.sort_values(by=['columnSequence'],inplace=True)
                            parent=df3[df3['columnSequence']<=row['columnSequence']].iloc[-1].address
                            dfheaderstemp.at[index,'parent']=parent
                            dfheaders.loc[dfheaders['address']==row['address'],'parent']=parent
                dfheaders.sort_values(by=['level'],ascending=False,inplace=True)  
                listofDFS=[]
                for i in range(int(dfheaders.level.max())+1):
                    listofDFS.append(dfheaders.copy()) 
                dfdf=listofDFS[0]
                for i in range(int(dfheaders.level.max())):
                    try:
                        dfdf=pd.merge(dfdf,listofDFS[i+1],how='left',left_on='parent',right_on='address')
                    except:
                        dfdf=pd.merge(dfdf,listofDFS[i+1],how='left',left_on='parent_y',right_on='address') 
                col=[]
                titles=[]
                for index,row in dfdf.iterrows():
                    title=''
                    if (row[6]) not in col:
                        col.append(row[6])
                        for key,value in row.iteritems():
                            if 'value' in key:
                                if str(value)!='nan':
                                    title=str(value)+' '+title
                        titles.append(title)
                dictf={}
                for i in range(len(col)):
                    dictf[col[i]]=titles[i] 
                dictf2={}
                for k in sorted(dictf):
                    dictf2[k]=(dictf[k])
                DFFinal=pd.DataFrame(columns=dictf2.values()) 
                df1values=dftemp[dftemp['cellGroupName']=='Body'] 
                for key,val in dictf2.items():
                    DFFinal[val]=df1values[df1values['columnSequence']==key].value.tolist()            
                DFFinal=DFFinal[DFFinal[DFFinal.columns[0]]!='']
                DFFinal['metaID']=int(metatableID)
                ALLDFS.append(DFFinal)
        except:
            continue 
    Mdicts={}
    for index,row in hlvldf.iterrows():
        for k in ALLDFS:
            try:
                if k.metaID.unique().tolist()[0]==row['metaID']:
                    Mdicts[row['highlvlDFName']]=k
            except:
                continue   
                
    return Mdicts                
def InsertAllData(CID,allData):
    readytoInsert={}
    for i in allData:
        for key,value in i.items():
            if key!='null':
                ss=str(value.to_csv())
                readytoInsert[key]=ss
    Final=pd.DataFrame().from_dict(readytoInsert,orient='index')
    Final.reset_index(inplace=True)
    Final.columns=['title','Data']
    Final['report_ID']=CID
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
                IF NOT EXISTS (select from statement."PreNotes" where "report_ID"=%(report_ID)s and "tableID"=%(title)s) THEN
                INSERT INTO statement."PreNotes"(
                "report_ID", "tableID", "Datatable")
                VALUES (%(report_ID)s, %(title)s, %(Data)s);
                END IF;
            END
            $$ 

        """
        cursor.executemany(postgres_insert_query,Final.to_dict(orient='records'))
        connection.commit()
        updateMRquery = """
        UPDATE codalraw."SheetsConverted"
        SET "Exist_Interpret"=True
        WHERE "report_ID"=%s;
        """
        RecordMR=([CID])
        cursor.execute(updateMRquery, RecordMR)
        connection.commit()
        print(str(CID)+' Notes Done')
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print("Failed to Insert CF", error)
    finally:
        if(connection):
            cursor.close()
            connection.close()    

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
    
        record_to_insert = (str(datetime.datetime.now()),text,'N52')
        
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

            select "report_ID","HtmlUrl" from codalraw."SheetsConverted" inner join
            codalraw."allrawReports" on "report_ID"="TracingNo" where ("Exist_Interpret"=False )
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
                     
def RUN(driver,df):
#     driver=webdriver.Chrome()
#     driver.maximize_window()  
#     df=get_unconverted()
    for index,row in df.iterrows():
        try:
            driver.get('https://codal.ir'+str(row['HtmlUrl']))
            allData=[]
            listof=[]
            for i in get_options(driver):
                if ('تفسیر') in str(i):
                    listof.append(i)

            for k in listof:
                select = Select(driver.find_element_by_id('ddlTable'))
                select.select_by_visible_text(k)
                allData.append(Translate_NotesPages(driver))
                time.sleep(5) 
            InsertAllData(row['report_ID'],allData)                 
        except Exception as e:
            print(e)
            continue
def scrape(df, *, loop):
    loop.run_in_executor(executor, scraper, df)
def scraper(df):
    chrome_options = Options()  
    chrome_options.add_argument("--headless")  
    driver = webdriver.Chrome(chrome_options=chrome_options)
    driver.set_page_load_timeout(7)
#     driver=webdriver.Chrome()
#     driver.maximize_window()  
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