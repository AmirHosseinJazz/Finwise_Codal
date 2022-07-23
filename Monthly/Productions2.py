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
import requests
from lxml import html
import re
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
def get_unconverted():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
          
             select P."report_ID",R."HtmlUrl"
            from monthly."PreMonthly" as P inner join codalraw."allrawReports" as R on P."report_ID"=R."TracingNo"
            WHERE P."Type"='Product' and P.converted=False order by random()
            """
        

                           , connection)
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read links", error)
    finally:
            if(connection):
                cursor.close()
                connection.close()
def roundTheFloats(x):
    if(type(get_true_value(x))==float):
        return int(round(get_true_value(x)))
    else:
        return x   
DataMappingtoDBfield={
    'ctl00_cphBody_ucProduct2_lblProductNameCaption':'product_Name',
    'ctl00_cphBody_ucProduct2_lblProductUnitCaption':'product_unit',
    'ctl00_cphBody_ucProduct2_lblPrevTotalProductionYearCaption':'prevTotalProductionYear',
    'ctl00_cphBody_ucProduct2_lblPrevTotalSalesYearCaption':'prevTotalSalesYear',
    'ctl00_cphBody_ucProduct2_lblPrevSalesRateYearCaption':'prevTotalSalesRateYear',
    'ctl00_cphBody_ucProduct2_lblPrevSalesAmountYearCaption':'prevTotalSalesAmountYear',
    'ctl00_cphBody_ucProduct2_lblTotalProductionModifiedCaption':'modification_Production',
    'ctl00_cphBody_ucProduct2_lblTotalSalesModifiedCaption':'modification_Sales',
    'ctl00_cphBody_ucProduct2_lblSalesAmountModifiedCaption':'modification_SalesAmount',
    'ctl00_cphBody_ucProduct2_lblPrevModifiedTotalProductionYearCaption':'prev_modified_TotalProduction',
    'ctl00_cphBody_ucProduct2_lblPrevModifiedTotalSalesYearCaption':'prev_modified_TotalSales',
    'ctl00_cphBody_ucProduct2_lblPrevModifiedSalesRateYearCaption':'prev_modified_TotalSalesRate',
    'ctl00_cphBody_ucProduct2_lblPrevModifiedSalesAmountYearCaption':'prev_modified_TotalSalesAmount',
    'ctl00_cphBody_ucProduct2_lblTotalProdctionMonthCaption':'totalProductionPeriod',
    'ctl00_cphBody_ucProduct2_lblTotalSalesMonthCaption':'totalSalePeriod',
    'ctl00_cphBody_ucProduct2_lblSalesRateMonthCaption':'saleRatePeriod',
    'ctl00_cphBody_ucProduct2_lblSalesAmountMonthCaption':'saleAmountPeriod',
    'ctl00_cphBody_ucProduct2_lblTotalProductionYearCaption':'totalProductionYear',
    'ctl00_cphBody_ucProduct2_lblTotalSalesYearCaption':'totalSaleYear',
    'ctl00_cphBody_ucProduct2_lblSalesRateYearCaption':'saleRateYear',
    'ctl00_cphBody_ucProduct2_lblSalesAmountYearCaption':'saleAmountYear'                                       
                     }
DataMappingtoDBfield_alternative={
    
    'ctl00_cphBody_ucProduct1_lblProductNameCaption':'product_Name',
    'ctl00_cphBody_ucProduct1_lblProductUnitCaption':'product_unit',
    'ctl00_cphBody_ucProduct1_lblPrevTotalProductionYearCaption':'prevTotalProductionYear',
    'ctl00_cphBody_ucProduct1_lblPrevTotalSalesYearCaption':'prevTotalSalesYear',
    'ctl00_cphBody_ucProduct1_lblPrevSalesRateYearCaption':'prevTotalSalesRateYear',
    'ctl00_cphBody_ucProduct1_lblPrevSalesAmountYearCaption':'prevTotalSalesAmountYear',
    'ctl00_cphBody_ucProduct1_lblTotalProductionModifiedCaption':'modification_Production',
    'ctl00_cphBody_ucProduct1_lblTotalSalesModifiedCaption':'modification_Sales',
    'ctl00_cphBody_ucProduct1_lblSalesAmountModifiedCaption':'modification_SalesAmount',
    'ctl00_cphBody_ucProduct1_lblPrevModifiedTotalProductionYearCaption':'prev_modified_TotalProduction',
    'ctl00_cphBody_ucProduct1_lblPrevModifiedTotalSalesYearCaption':'prev_modified_TotalSales',
    'ctl00_cphBody_ucProduct1_lblPrevModifiedSalesRateYearCaption':'prev_modified_TotalSalesRate',
    'ctl00_cphBody_ucProduct1_lblPrevModifiedSalesAmountYearCaption':'prev_modified_TotalSalesAmount',
    'ctl00_cphBody_ucProduct1_lblTotalProdctionMonthCaption':'totalProductionPeriod',
    'ctl00_cphBody_ucProduct1_lblTotalSalesMonthCaption':'totalSalePeriod',
    'ctl00_cphBody_ucProduct1_lblSalesRateMonthCaption':'saleRatePeriod',
    'ctl00_cphBody_ucProduct1_lblSalesAmountMonthCaption':'saleAmountPeriod',
    'ctl00_cphBody_ucProduct1_lblTotalProductionYearCaption':'totalProductionYear',
    'ctl00_cphBody_ucProduct1_lblTotalSalesYearCaption':'totalSaleYear',
    'ctl00_cphBody_ucProduct1_lblSalesRateYearCaption':'saleRateYear',
    'ctl00_cphBody_ucProduct1_lblSalesAmountYearCaption':'saleAmountYear'           
}         
DataMappingtoDBfield_alternative3={
    
    'ctl00_cphBody_ucProductionAndSales1_lblProductNameCaption':'product_Name',
    'ctl00_cphBody_ucProductionAndSales1_lblProductUnitCaption':'product_unit',
    'ctl00_cphBody_ucProductionAndSales1_lblPrevTotalProductionYearCaption':'prevTotalProductionYear',
    'ctl00_cphBody_ucProductionAndSales1_lblPrevTotalSalesYearCaption':'prevTotalSalesYear',
    'ctl00_cphBody_ucProductionAndSales1_lblPrevSalesRateYearCaption':'prevTotalSalesRateYear',
    'ctl00_cphBody_ucProductionAndSales1_lblPrevSalesAmountYearCaption':'prevTotalSalesAmountYear',
    'ctl00_cphBody_ucProductionAndSales1_lblTotalProductionModifiedCaption':'modification_Production',
    'ctl00_cphBody_ucProductionAndSales1_lblTotalSalesModifiedCaption':'modification_Sales',
    'ctl00_cphBody_ucProductionAndSales1_lblSalesAmountModifiedCaption':'modification_SalesAmount',
    'ctl00_cphBody_ucProductionAndSales1_lblPrevModifiedTotalProductionYearCaption':'prev_modified_TotalProduction',
    'ctl00_cphBody_ucProductionAndSales1_lblPrevModifiedTotalSalesYearCaption':'prev_modified_TotalSales',
    'ctl00_cphBody_ucProductionAndSales1_lblPrevModifiedSalesRateYearCaption':'prev_modified_TotalSalesRate',
    'ctl00_cphBody_ucProductionAndSales1_lblPrevModifiedSalesAmountYearCaption':'prev_modified_TotalSalesAmount',
    'ctl00_cphBody_ucProductionAndSales1_lblTotalProdctionMonthCaption':'totalProductionPeriod',
    'ctl00_cphBody_ucProductionAndSales1_lblTotalSalesMonthCaption':'totalSalePeriod',
    'ctl00_cphBody_ucProductionAndSales1_lblSalesRateMonthCaption':'saleRatePeriod',
    'ctl00_cphBody_ucProductionAndSales1_lblSalesAmountMonthCaption':'saleAmountPeriod',
    'ctl00_cphBody_ucProductionAndSales1_lblTotalProductionYearCaption':'totalProductionYear',
    'ctl00_cphBody_ucProductionAndSales1_lblTotalSalesYearCaption':'totalSaleYear',
    'ctl00_cphBody_ucProductionAndSales1_lblSalesRateYearCaption':'saleRateYear',
    'ctl00_cphBody_ucProductionAndSales1_lblSalesAmountYearCaption':'saleAmountYear'           
}           
def get_announcments(driver):
    results={}
    if check_exists_by_xpath(driver,'//a[@id="ctl00_cphBody_ucNavigateToNextPrevLetter_hlPrevVersion"]'):
        linktoprevious=driver.find_element_by_xpath('//a[@id="ctl00_cphBody_ucNavigateToNextPrevLetter_hlPrevVersion"]').text
        previousAnnouncment=[int(s) for s in str.split(linktoprevious) if s.isdigit()][0]
    else:
        previousAnnouncment=0
    if check_exists_by_xpath(driver,'//a[@id="ctl00_cphBody_ucNavigateToNextPrevLetter_hlNewVersion"]'):
        linktonext=driver.find_element_by_xpath('//a[@id="ctl00_cphBody_ucNavigateToNextPrevLetter_hlNewVersion"]').text
        nextAnnouncment=[int(s) for s in str.split(linktonext) if s.isdigit()][0]
    else:
        nextAnnouncment=0
    results['Last']=previousAnnouncment
    results['Next']=nextAnnouncment
    return results
def log_it(text):
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        postgres_insert_query = """
          
          INSERT INTO monthly._log(
            date, text)
                VALUES (%s, %s);
        """
    
        record_to_insert = (str(datetime.datetime.now()),text)
        
        cursor.execute(postgres_insert_query, record_to_insert)
        connection.commit()
    except(Exception, psycopg2.Error) as error:
        if(connection):
            print("Failed to insert log", error)
    finally:
        if(connection):
            cursor.close()
            connection.close()                
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

def find_nth(haystack, needle, n):
    start = haystack.find(needle)
    while start >= 0 and n > 1:
        start = haystack.find(needle, start+len(needle))
        n -= 1
    return start
def check_type(driver):
    Type='Other'
    typelist=['Other','1']
    if check_exists_by_xpath(driver,'//table[@id="ctl00_cphBody_ucProduct2_Table1"]'):
        Type='Product'
        prod_type=1
        typelist=['Product',1]
    if check_exists_by_xpath(driver,'//table[@id="ctl00_cphBody_ucProduct1_Table1"]'):
        Type='Product'
        prod_type=2
        typelist=['Product',2]
    else:
        if check_exists_by_xpath(driver,'//*[@id="ctl00_cphBody_ucProductionAndSales1_dgProduction"]'):
            Type='Product'
            prod_type=3
            typelist=['Product',3]
    if (Type=='Other'):
        wholefile=str(driver.page_source)
        if('"metaTableId' in wholefile):
            Type='NewProduct'
            typelist=['NewProduct']
    return typelist 

def FinWise10_ProductOneAndTwo(driver,TypeOf,CID):
    XpathTables={
    1:'//table[@id="ctl00_cphBody_ucProduct2_dgProduction"]/tbody/tr/td[',
    2:'//table[@id="ctl00_cphBody_ucProduct1_dgProduction"]/tbody/tr/td[',
    3:'//table[@id="ctl00_cphBody_ucProductionAndSales1_dgProduction"]/tbody/tr/td['
    }
    XpathHeaders={
        1:'//table[@id="ctl00_cphBody_ucProduct2_Table1"]/tbody/tr[2]/td/table[@class="gv"]/tbody/tr[2]/th[not(contains(@style,"display: none;"))]/span/@id',
        2:'//table[@id="ctl00_cphBody_ucProduct1_Table1"]/tbody/tr[2]/td/table[@class="gv"]/tbody/tr[2]/th/span/@id',
        3:'//table[@id="ctl00_cphBody_ucProductionAndSales1_Table1"]/tbody/tr[2]/td/table[@class="gv"]/tbody/tr[2]/th[not(contains(@style,"display: none;"))]/span/@id'
    }
    tree = html.fromstring(driver.page_source)
    Headers=tree.xpath(XpathHeaders[TypeOf])
    if TypeOf==1:
        DFNew=pd.DataFrame(columns=[DataMappingtoDBfield[x] for x in Headers])
        for k in range(len(Headers)):
            DFNew[DataMappingtoDBfield[Headers[k]]]=tree.xpath(XpathTables[TypeOf]+str(k+1)+']/text()')
    if TypeOf==2:
        DFNew=pd.DataFrame(columns=[DataMappingtoDBfield_alternative[x] for x in Headers])
        for k in range(len(Headers)):
            DFNew[DataMappingtoDBfield_alternative[Headers[k]]]=tree.xpath(XpathTables[TypeOf]+str(k+1)+']/text()')
    if TypeOf==3:
        DFNew=pd.DataFrame(columns=[DataMappingtoDBfield_alternative3[x] for x in Headers])
        for k in range(len(Headers)):
            DFNew[DataMappingtoDBfield_alternative3[Headers[k]]]=tree.xpath(XpathTables[TypeOf]+str(k+1)+']/text()')
    DFNew.replace('ك','ک',regex=True,inplace=True)
    DFNew.replace('ي','ی',regex=True,inplace=True) 
    for j in DFNew.columns:
        DFNew[j]=DFNew[j].apply(get_true_value)
    for x in list(DataMappingtoDBfield.values()):
        if x not in DFNew.columns:
            DFNew[x]=None
    DFNew['product_Name']=DFNew['product_Name'].str.strip()
    DFNew=DFNew[DFNew['product_Name']!='جمع']
    descs=pd.DataFrame([get_description_product_service(driver)])
    announc=pd.DataFrame([get_announcments(driver)])
    DFNew['tmp'] = 1
    descs['tmp'] = 1
    NEW2_1 = pd.merge(DFNew, descs, on=['tmp'])
    NEW2_1 = NEW2_1.drop('tmp', axis=1)
    NEW2_1['tmp'] = 1
    announc['tmp'] = 1
    NEW2_3= pd.merge(NEW2_1, announc, on=['tmp'])
    NEW2_3 = NEW2_3.drop('tmp', axis=1)
    NEW2_3['report_id']=CID
    
    NEW2_3=NEW2_3[(NEW2_3['product_Name']!='')&(NEW2_3['product_Name']!=None)&(NEW2_3['product_Name']!='None')&(NEW2_3['product_Name'].notnull())]
    return NEW2_3
def FinWise10_ProductOneAndTwo_Insert(DF_Prod):    
    connection = psycopg2.connect(user=db_username,
                                  password=db_pass,
                                  host=db_host,
                                  port=db_port,
                                  database=db_database)
    cursor = connection.cursor()
    for index,row in DF_Prod.iterrows():
        pq = """ 
        DO 
        $$
        BEGIN
            IF NOT EXISTS (select from monthly.goods where "name"=%s) THEN
                INSERT INTO monthly.goods ("name","unit") VALUES (%s,%s);
            END IF;
        END
        $$
        """
        rq = (row['product_Name'],row['product_Name'],row['product_unit'])
        cursor.execute(pq, rq)
    postgres_insert_query = """
     DO 
        $$
        BEGIN
            IF NOT EXISTS (select from monthly."productionMonthly" where ("report_id"=%(report_id)s and "good"=(select "ID" from monthly.goods where name=%(product_Name)s limit 1)))
            THEN
            INSERT INTO monthly."productionMonthly"(
	 good, desc_modification, "desc_onePeriod", "desc_toDate", desc_title, "lastAnnouncment", "totalProductionPeriod", "totalSalePeriod", "saleRatePeriod", "saleAmountPeriod", "totalProductionYear", "totalSaleYear", "saleRateYear", "saleAmountYear", "prevTotalProductionYear", "prevTotalSalesYear", "prevTotalSalesRateYear", "prevTotalSalesAmountYear", "modification_Production", "modification_Sales", "modification_SalesAmount", "prev_modified_TotalProduction", "prev_modified_TotalSalesRate", "prev_modified_TotalSalesAmount", "prev_modified_TotalSales", report_id, "nextAnnouncement")
                VALUES ((select "ID" from monthly.goods where name=%(product_Name)s limit 1),
                %(desc_modif)s, %(desc_month)s, %(desc_year)s, %(desc_title)s, %(Last)s, %(totalProductionPeriod)s, 
                %(totalSalePeriod)s, %(saleRatePeriod)s, %(saleAmountPeriod)s, %(totalProductionYear)s, %(totalSaleYear)s, %(saleRateYear)s, 
                %(saleAmountYear)s, %(prevTotalProductionYear)s, %(prevTotalSalesYear)s, %(prevTotalSalesRateYear)s, %(prevTotalSalesAmountYear)s, %(modification_Production)s, 
                %(modification_Sales)s, %(modification_SalesAmount)s, %(prev_modified_TotalProduction)s, %(prev_modified_TotalSalesRate)s, %(prev_modified_TotalSalesAmount)s, 
                %(prev_modified_TotalSales)s,%(report_id)s, %(Next)s);
            END IF;
        END
        $$ 

    """
    cursor.executemany(postgres_insert_query,DF_Prod.to_dict(orient='records'))
    connection.commit()
    postgres_insert_query3 = """
    UPDATE monthly."PreMonthly"
    SET converted=True
    WHERE "report_ID"=%(report_id)s
    """
    cursor.executemany(postgres_insert_query3,DF_Prod.to_dict(orient='records'))
    connection.commit()  
    print(str(DF_Prod.report_id.unique().tolist()[0])+'  '+'--Done')
def get_description_product_service(driver):
    results={}
    if check_exists_by_xpath(driver,'//span[@id="ctl00_cphBody_ucNavigateToNextPrevLetter_lblCorrectionDesc"]'):
        desc_title=driver.find_element_by_xpath('//span[@id="ctl00_cphBody_ucNavigateToNextPrevLetter_lblCorrectionDesc"]').text
    else:
        desc_title=""
    results['desc_title']=desc_title
    if check_exists_by_xpath(driver,'//span[@id="txbDescMonth"]'):
        desc_month=driver.find_element_by_xpath('//span[@id="txbDescMonth"]').text
    else:
        desc_month=""
    results['desc_month']=desc_month
    if check_exists_by_xpath(driver,'//span[@id="txbDescYear"]'):
        desc_year=driver.find_element_by_xpath('//span[@id="txbDescYear"]').text
    else:
        desc_year=""
    results['desc_year']=desc_year
    if check_exists_by_xpath(driver,'//span[@id="txtCorrectionComment"]'):
        desc_modif=driver.find_element_by_xpath('//span[@id="txtCorrectionComment"]').text
    else:
        desc_modif=""
    results['desc_modif']=desc_modif
    return results 
def RUN(driver):
#     driver = webdriver.Chrome()
#     driver.maximize_window()                 
    df=get_unconverted()
    AllData=len(df.index)
    counter=0
    for index,row in df.iterrows():
        try:
            CodalRaw_ID=int(row['report_ID'])
            CodalRaw_links=row['HtmlUrl']
            #print(CodalRaw_ID)
            driver.get('http://codal.ir'+CodalRaw_links)
            time.sleep(3)
            # driver.execute_script("document.body.style.zoom='100%';document.body.style.zoom='50%'")
            # titles=get_titlebox()
            # log_it('checked'+str(CodalRaw_ID))
            Type=check_type(driver)
            print(Type) 
            if(Type[0]=='Product'):
                DF_Prod=FinWise10_ProductOneAndTwo(driver,Type[1],CodalRaw_ID)
                FinWise10_ProductOneAndTwo_Insert(DF_Prod)
            # if(Type[0]=='NewProduct'):
            #     DF_Prod=FinWise10_NewProduct(driver,CodalRaw_ID)
            #     FinWise10_NewProductInsert(DF_Prod,CodalRaw_ID,CodalRaw_links) 
            counter=counter+1
            percentage=(counter*100)/AllData
            print("{0:.2f}".format(percentage))
            
        except (Exception, psycopg2.Error) as error :
            print(error)
            print(CodalRaw_links)
            continue