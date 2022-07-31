import requests
import time
import pandas as pd
import psycopg2
import pandas.io.sql as psql
from khayyam import *
import time
from datetime import date, datetime
import numpy as np
from scipy.stats import norm
import pandas.io.sql as psql
import json
import statistics
from pandas.io.json import json_normalize
import psycopg2.extras as extras
import asyncio
import aiohttp
import asyncpg
db_username="Jay"
db_pass="Mehrad1"
db_host="127.0.0.1"
db_port="5432"
db_database="FinWisev12"
start=time.time()
ct=0


async def download_All(sites):
    header = {'User-Agent': 'Thunder Client (https://www.thunderclient.com)'}
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False),headers=header) as session:
        tasks=[]
        for url in sites:
            task=asyncio.ensure_future(download_OneStock(url,session))
            tasks.append(task)
        await asyncio.gather(*tasks, return_exceptions=True)  
async def download_OneStock(url,session):
    flag=True
    while flag:
        try:

            async with session.get('https://search.codal.ir/api/search/v2/q?&Audited=true&AuditorRef=-1&Category=-1&Childs=true&CompanyState=-1&CompanyType=-1&Consolidatable=true&IsNotAudited=false&Length=-1&LetterType=-1&Mains=true&NotAudited=true&NotConsolidatable=true&PageNumber='+str(url)+'&Publisher=false&TracingNo=-1&search=true') as resp:
                wholeHtml=await resp.text()
        
        
            js=json.loads(wholeHtml)
            DF=pd.json_normalize(js['Letters'])
            # print(DF.columns)
            # print(DF.iloc[0])
            DF.replace('ك','ک',regex=True,inplace=True)
            DF.replace('ي','ی',regex=True,inplace=True)   
            cols=[i for i in DF.columns if i in ["TracingNo"]]
            for col in cols:
                DF[col]=pd.to_numeric(DF[col])
            cols=[i for i in DF.columns if i not in ["TracingNo"]]
            for col in cols:
                DF[col]=(DF[col]).apply(str)
            cols=[i for i in DF.columns if i in ["HasHtml","HasExcel","HasPdf","HasPdf","HasXbrl","HasAttachment"]]
            for col in cols:
                DF[col]=(DF[col]).apply(bool)
            if 'IsEstimate' in DF.columns:     
                DF=DF.drop(columns=['IsEstimate'])      
            tuples = [tuple(x) for x in DF.to_numpy()]   
            con = await asyncpg.connect(host=db_host,port=db_port,user=db_username,password=db_pass,database=db_database)    
            await con.execute('''CREATE TEMPORARY TABLE _data(
                "TracingNo" bigint NOT NULL,
                "Ticker" character varying(100) COLLATE pg_catalog."default" NOT NULL,
                reported_firm character varying(300) COLLATE pg_catalog."default" NOT NULL,
                "underSupervision" character varying(5) COLLATE pg_catalog."default",
                title text COLLATE pg_catalog."default" NOT NULL,
                "LetterCode" character varying(25) COLLATE pg_catalog."default" NOT NULL,
                "SentTime" character varying(75) COLLATE pg_catalog."default" NOT NULL,
                "PublishTime" character(75) COLLATE pg_catalog."default" NOT NULL,
                "HasHtml" boolean,
                "HtmlUrl" text COLLATE pg_catalog."default",
                "HasExcel" boolean,
                "HasPdf" boolean,
                "HasXbrl" boolean,
                "HasAttachment" boolean,
                "AttachmentUrl" text COLLATE pg_catalog."default",
                "PdfUrl" text COLLATE pg_catalog."default",
                "ExcelUrl" text COLLATE pg_catalog."default",
                "XbrlUrl" text COLLATE pg_catalog."default",
                "TedanUrl" text COLLATE pg_catalog."default",
                "SuperVision_UnderSupervision" text COLLATE pg_catalog."default",
                "SuperVision_AdditionalInfo" text COLLATE pg_catalog."default",
                "SuperVision_Reasons" text COLLATE pg_catalog."default"
            )''')
            result = await con.copy_records_to_table('_data', records=tuples)
            await con.execute('''
            INSERT INTO {table}(
            "TracingNo", "Ticker", reported_firm, "underSupervision", title, "LetterCode", "SentTime", "PublishTime", "HasHtml", "HtmlUrl", "HasExcel", "HasPdf", "HasXbrl", "HasAttachment", "AttachmentUrl", "PdfUrl", "ExcelUrl", "XbrlUrl", "TedanUrl", "SuperVision_UnderSupervision", "SuperVision_AdditionalInfo", "SuperVision_Reasons")
            SELECT * FROM _data
            ON CONFLICT ("TracingNo", "Ticker", "SentTime", "PublishTime", reported_firm)
            DO NOTHING
        '''.format(table='codalraw."allrawReports"'))
            await con.close()
            flag=False
        except (Exception, psycopg2.Error) as error :
            print(error)
            continue


if __name__=="__main__":
    start1=time.time()
    ct=0
    http_proxy  = "http://80.48.119.28:8080"
    headers = {'User-Agent': 'Thunder Client (https://www.thunderclient.com)'}

    resp = requests.get('https://search.codal.ir/api/search/v2/q?&Audited=true&AuditorRef=-1&Category=-1&Childs=true&CompanyState=-1&CompanyType=-1&Consolidatable=true&IsNotAudited=false&Length=-1&LetterType=-1&Mains=true&NotAudited=true&NotConsolidatable=true&PageNumber=1&Publisher=false&TracingNo=-1&search=true',verify=False,timeout=10,headers=headers)

    if resp.status_code != 200:
        print('error')
        # raise ApiError('GET /tasks/ {}'.format(resp.status_code))
    else:
        js=json.loads(resp.text)
        DF=pd.json_normalize(js['Letters'])
        Range=js['Page']
    Rangelist=range(1,Range)
    # Rangelist=range(1,3)
    chunksize=20
    chuncks=round(Range/chunksize)
    for i in range(1+chuncks):
        print(str(i)+'/'+str(Range))
        if i !=chuncks:
            asyncio.get_event_loop().run_until_complete(download_All(Rangelist[chunksize*i:chunksize*(i+1)]))            
        else:
            asyncio.get_event_loop().run_until_complete(download_All(Rangelist[chunksize*i:]))            
    # asyncio.get_event_loop().run_until_complete(download_All(Rangelist[1:3]))            
    print('All took', time.time()-start1, 'seconds.To Add ')