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
import requests
import pandas.io.sql as psql
from selenium.common.exceptions import NoSuchElementException  
from selenium.webdriver.support.ui import Select
from lxml import html
db_username="Jay"
db_pass="Mehrad1"
db_host="127.0.0.1"
db_port="5432"
db_database="FinWisev12"

def GET_CODALS():
    driver = webdriver.Chrome()
    driver.maximize_window() 
    DFWHOLE=pd.DataFrame()
    for i in range(104):
        driver.get('https://codal360.ir/fa/publishers?&page='+str(i+1))
        listOFDICTS1=[]
        listOFDICTS2=[]
        listOFDICTS3=[]
        listOFDICTS4=[]
        listOFDICTS5=[]
        listOFDICTS6=[]
        listOFDICTS7=[]
        tree = html.fromstring(driver.page_source)
        listOFDICTS1=tree.xpath('//tbody[@id="template-container"]//td[1]//a/text()')
        listOFDICTS2=tree.xpath('//tbody[@id="template-container"]//td[2]//a/text()')
        #listOFDICTS3=tree.xpath('//tbody[@id="template-container"]//td[3]//text()')
        listOFDICTS4=tree.xpath('//tbody[@id="template-container"]//td[4]//span/text()')
        listOFDICTS5=tree.xpath('//tbody[@id="template-container"]//td[5]//span/text()')
        listOFDICTS6=tree.xpath('//tbody[@id="template-container"]//td[6]//span/text()')
        listOFDICTS7=tree.xpath('//tbody[@id="template-container"]//td[7]//a/text()')
        DFInside=pd.DataFrame()
        DFInside['Ticker']=listOFDICTS1
        DFInside['Name']=listOFDICTS2
        #DFInside['War']=listOFDICTS3
        DFInside['TypeOFcompany']=listOFDICTS4
        DFInside['Type']=listOFDICTS5
        DFInside['Ind']=listOFDICTS6
        DFInside['URL']=listOFDICTS7
        DFInside.replace('  ','',regex=True,inplace=True)
        DFInside.replace('\n','',regex=True,inplace=True)
        DFWHOLE=DFWHOLE.append(DFInside)
        time.sleep(2)
    DFWHOLE.replace('ك','ک',regex=True,inplace=True)
    DFWHOLE.replace('ي','ی',regex=True,inplace=True)
    DFWHOLE['Ticker'] = DFWHOLE.apply(lambda x: x.str.strip())
    InsertFromCodal(DFWHOLE)
    driver.quit()
def InsertFromCodal(df):
    try:
        connection = psycopg2.connect(user=db_username,
                                      password=db_pass,
                                      host=db_host,
                                      port=db_port,
                                      database=db_database)
        cursor = connection.cursor()
        postgres_insert_query_cheif = """
         DO 
            $$
            BEGIN
                IF NOT EXISTS (select from public."Publishers" where "persianName"=%(Ticker)s ) THEN
                    INSERT INTO public."Publishers"(
                    "persianName", "FullName","TypeOfCompany", "Status", "Industry", "Url")
                    VALUES (%(Ticker)s, %(Name)s, %(TypeOFcompany)s, %(Type)s, %(Ind)s, %(URL)s);
                END IF;
            END
            $$ 

        """
        cursor.executemany(postgres_insert_query_cheif,df.to_dict(orient='records'))
        #######
        
        connection.commit()
        print('PublishersCodalDone')
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print('Failed to Insert Publishers Codal ', error)
GET_CODALS()