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
db_database="FinWisev10"
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
            WHERE P."Type"='Product' and P.converted=False order by "report_ID" desc
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
def FinWise10_NG_Product_desc(driver):
    wholefile=str(driver.page_source)
    wholefile=wholefile[wholefile.find('var datasource')+16:]
    wholefile=wholefile[:wholefile.find('</script>')]
    wholefile=wholefile.replace('\u200c', '')
    wholefile=wholefile.replace('\n', '')
    wholefile=wholefile.replace(';', '')
    json1=json.loads(wholefile)
    dict1=[]
    counter=0
    for k in json1['sheets'][0]['tables']:
        if k['aliasName']=='ProductMonthlyActivityDesc1':
            for t in k['cells']:
                if counter%2==1:
                    dict1.append(t['value'])
                counter=counter+1
    desc_titles=['desc_modif','desc_month','desc_year']
    alldescs={}
    desccounter=0
    for i in range(len(dict1)):
        alldescs[desc_titles[i]]=dict1[i]
    if check_exists_by_xpath(driver,'//span[@id="ctl00_cphBody_ucNavigateToNextPrevLetter_lblCorrectionDesc"]'):
        alldescs['desc_title']=driver.find_element_by_xpath('//span[@id="ctl00_cphBody_ucNavigateToNextPrevLetter_lblCorrectionDesc"]').text
    else:
        alldescs['desc_title']=""
    return alldescs
def FinWise10_NewProduct(driver,CID):
    newProduct_labels={
        1:'good',
        2:'unit',
        3:'prev_production',
        4:'prev_sale_count',
        5:'prev_sale_rate',
        6:'prev_sale_amount',
        7:'modif_production',
        8:'modif_salecount',
        9:'modif_saleamount',
        10:'prev_modified_production',
        11:'prev_modified_sale_count',
        12:'prev_modified_sale_rate',
        13:'prev_modified_sale_amount',
        14:'period_production',
        15:'period_sale_count',
        16:'period_sale_rate',
        17:'period_sale_amount',
        18:'total_production',
        19:'total_sale_count',
        20:'total_sale_rate',
        21:'total_sale_amount',
        22:'lastYear_production',
        23:'lastYear_sale_count',
        24:'lastYear_sale_rate',
        25:'lastYear_sale_amount',
        26:'status'
    }
    numerics={
        3:'prev_production',
        4:'prev_sale_count',
        5:'prev_sale_rate',
        6:'prev_sale_amount',
        7:'modif_production',
        8:'modif_salecount',
        9:'modif_saleamount',
        10:'prev_modified_production',
        11:'prev_modified_sale_count',
        12:'prev_modified_sale_rate',
        13:'prev_modified_sale_amount',
        14:'period_production',
        15:'period_sale_count',
        16:'period_sale_rate',
        17:'period_sale_amount',
        18:'total_production',
        19:'total_sale_count',
        20:'total_sale_rate',
        21:'total_sale_amount',
        22:'lastYear_production',
        23:'lastYear_sale_count',
        24:'lastYear_sale_rate',
        25:'lastYear_sale_amount'
    }
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
    for i in range(1,wholefile.count('value"')+1):
        start=find_nth(wholefile,'value"',i)
        end=find_nth(wholefile,'value"',i)+50
        wholefile=wholefile[:start]+(wholefile[start:end].replace('{',' '))+wholefile[end:]
        wholefile=wholefile[:start]+(wholefile[start:end].replace('}',' '))+wholefile[end:]
    for i in range(1,wholefile.count('{')+1):
        try:
            temp=wholefile[find_nth(wholefile,'{',i):find_nth(wholefile,'}',i)+1]
            listofDicts.append(json.loads(temp))
        except:
            continue
    df1=pd.DataFrame(listofDicts)
    NEW=pd.DataFrame()
    firstIter=True
    for i in range(len(newProduct_labels.keys())):
        category=df1[(df1['columnCode']==i+1)&(df1['valueTypeName']!='Blank')&(df1['rowTypeName']!='FixedRow')].sort_values(by='rowSequence').category.tolist()
        temp_list=df1[(df1['columnCode']==i+1)&(df1['valueTypeName']!='Blank')&(df1['rowTypeName']!='FixedRow')].sort_values(by='rowSequence').value.tolist()
        if (firstIter):
            NEW[newProduct_labels[i+1]]=temp_list
            NEW['Categories']=category
        else:
            if(len(temp_list)<len(NEW.index)):
                for k in range(len(NEW.index)-len(temp_list)):
                    temp_list.append('')
            NEW[newProduct_labels[i+1]]=temp_list
        firstIter=False
    NEW=NEW[NEW['good']!='']
    df_discount=df1[df1['category']==5][['value']].T
    if not df_discount.empty:
        df_discount.columns=newProduct_labels.values()
        df_discount['Categories']=5
        df_discount.reset_index(inplace=True)
        df_discount.drop(columns='index',inplace=True)
        NEW=NEW.append(df_discount)
    Categoriesdf=pd.DataFrame.from_dict({0:'Whole',1:'Domestic_Sale',2:'Export_Sale',3:'Service_revenue',4:'Refund',5:'Discount'  },orient='index')
    Categoriesdf.reset_index(inplace=True)
    Categoriesdf.columns=['Categories','categoryName']
    NEW.replace('',0,regex=True,inplace=True)
    NEW=NEW.replace('-Infinity',0)
    NEW=NEW.replace('Infinity',0)
    NEW=NEW.applymap(roundTheFloats)
    NEW2=pd.merge(NEW,Categoriesdf,on='Categories')
    NEW2.drop(columns=['Categories'],inplace=True)
    NEW2.replace('ك','ک',regex=True,inplace=True)
    NEW2.replace('ي','ی',regex=True,inplace=True)
    for i in numerics.values():
        NEW2[i] = NEW2[i].astype(str)
        if i in NEW2.columns:
            NEW2[i] = NEW2[i].str.replace(r'\D+', '0')
            NEW2[i]=NEW2[i].astype(int)
    descdf=pd.DataFrame([FinWise10_NG_Product_desc(driver)])
    # titlesdf=pd.DataFrame([titles])
    # titlesdf.columns=['nemad','period','toDate','firm_reporting']
    announcedf=pd.DataFrame([get_announcments(driver)])
    NEW2['tmp'] = 1
    descdf['tmp'] = 1
    NEW2_1 = pd.merge(NEW2, descdf, on=['tmp'])
    NEW2_1 = NEW2_1.drop('tmp', axis=1)
    NEW2_1['tmp'] = 1
    announcedf['tmp'] = 1
    NEW2_3= pd.merge(NEW2_1, announcedf, on=['tmp'])
    NEW2_3 = NEW2_3.drop('tmp', axis=1)
    NEW2_3['report_id']=CID
    NEW2_3=NEW2_3[(NEW2_3['good']!='')&(NEW2_3['good']!=None)&(NEW2_3['good']!='None')&(NEW2_3['good'].notnull())]
    return NEW2_3
def FinWise10_NewProductInsert(DF_Prod,CID,Clink):    
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
        rq = (row['good'],row['good'],row['unit'])
        cursor.execute(pq, rq)
    postgres_insert_query = """
     DO 
        $$
        BEGIN
            IF NOT EXISTS (select from monthly."productionMonthly" where ("report_id"=%(report_id)s and "good"=(select "ID" from monthly.goods where name=%(good)s) and category=%(categoryName)s))THEN
            INSERT INTO monthly."productionMonthly"(
	 good, desc_modification, "desc_onePeriod", "desc_toDate", desc_title, "lastAnnouncment", "totalProductionPeriod", "totalSalePeriod", "saleRatePeriod", "saleAmountPeriod", "totalProductionYear", "totalSaleYear", "saleRateYear", "saleAmountYear", "prevTotalProductionYear", "prevTotalSalesYear", "prevTotalSalesRateYear", "prevTotalSalesAmountYear", "modification_Production", "modification_Sales", "modification_SalesAmount", "prev_modified_TotalProduction", "prev_modified_TotalSalesRate", "prev_modified_TotalSalesAmount", "prev_modified_TotalSales", report_id, "nextAnnouncement", "lastyear_Production", "lastyear_saleCount", "lastyear_saleAmount", "lastyear_saleRate", category, status)
                VALUES ((select "ID" from monthly.goods where name=%(good)s),
                %(desc_modif)s, %(desc_month)s, %(desc_year)s, %(desc_title)s, %(Last)s, %(period_production)s, 
                %(period_sale_count)s, %(period_sale_rate)s, %(period_sale_amount)s, %(total_production)s, %(total_sale_count)s, %(total_sale_rate)s, 
                %(total_sale_amount)s, %(prev_production)s, %(prev_sale_count)s, %(prev_sale_rate)s, %(prev_sale_amount)s, %(modif_production)s, 
                %(modif_salecount)s, %(modif_saleamount)s, %(prev_modified_production)s, %(prev_modified_sale_rate)s, %(prev_modified_sale_amount)s, %(prev_modified_sale_count)s,
                %(report_id)s, %(Next)s, %(lastYear_production)s, %(lastYear_sale_count)s, %(lastYear_sale_amount)s, 
                %(lastYear_sale_rate)s, %(categoryName)s, %(status)s);
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
    print(str(Clink)+'  '+'--Done')    
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
        if check_exists_by_xpath(driver,'//*[@id="ctl00_h1MasterTitle"]'):
            mastertitle=driver.find_element_by_xpath('//*[@id="ctl00_h1MasterTitle"]').text
            if mastertitle=='صورت وضعیت پورتفوی':
                Type='Investment'
                typelist=['Investment']
    if (Type=='Other'):
        wholefile=str(driver.page_source)
        if('"metaTableId' in wholefile):
            Type='NewProduct'
            typelist=['NewProduct']
    return typelist           
def RUN(driver):
#     driver = webdriver.Chrome()
#     driver.maximize_window()                 
    df=get_unconverted()
    AllData=len(df.index)
    counter=0
    for index,row in df.tail(2).iterrows():
        try:
            CodalRaw_ID=int(row['report_ID'])
            CodalRaw_links=row['HtmlUrl']
            #print(CodalRaw_ID)
            driver.get('http://codal.ir'+CodalRaw_links)
            time.sleep(3)
            driver.execute_script("document.body.style.zoom='100%';document.body.style.zoom='50%'")
            # titles=get_titlebox()
            # log_it('checked'+str(CodalRaw_ID))
            Type=check_type(driver)
            print(Type) 
            # if(Type[0]=='Product'):
            #     #titles
            #     alldesc=get_description_product_service()
            #     Announcment=get_announcments()
            #     DF_Prod=get_data_product(Type[1])
            #     insertProduct(titles,Announcment,alldesc,DF_Prod,CodalRaw_ID,CodalRaw_links)
            if(Type[0]=='NewProduct'):
                DF_Prod=FinWise10_NewProduct(driver,CodalRaw_ID)
                FinWise10_NewProductInsert(DF_Prod,CodalRaw_ID,CodalRaw_links)
            #     InsertProductNG_ultimate(DF_Prod,CodalRaw_ID,CodalRaw_links)
                #insertProduct(titles,Announcment,alldesc,DF_Prod,CodalRaw_ID,CodalRaw_links) 
            counter=counter+1
            percentage=(counter*100)/AllData
            print("{0:.2f}".format(percentage))
            
        except (Exception, psycopg2.Error) as error :
            print(error)
            print(CodalRaw_links)
            continue
    # driver.quit()    