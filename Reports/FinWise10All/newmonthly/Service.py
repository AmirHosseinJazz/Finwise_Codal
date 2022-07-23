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
            WHERE P."Type"='Service' and P.converted=False"""

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
def get_invest_desc(driver):
    results={}
    investDeficit=''
    if check_exists_by_xpath(driver,'*//input[@id="ctl00_cphBody_txbReserveForInvestment"]'):
        investDeficit=driver.find_element_by_id('ctl00_cphBody_txbReserveForInvestment').get_attribute('value')
    if('(' in str(investDeficit) and ')' in str(investDeficit) ):
        investDeficit=str(investDeficit).replace(')','')
        investDeficit=investDeficit.replace('(','')
        investDeficit=investDeficit.replace(',','')
    if (investDeficit.isdigit()):
        investDeficit=int(investDeficit)
    else:
        if (',' in str(investDeficit)) | (investDeficit=='۰') | (str(investDeficit).isdigit()) | ('.' in str(investDeficit)) :
            investDeficit=str(investDeficit).replace(',','')
            investDeficit.replace('.','0')
            if ('.' in investDeficit):
                investDeficit = investDeficit.split('.')[0]
            if (investDeficit=='۰'):
                investDeficit=0
            investDeficit=int(investDeficit)
    if check_exists_by_xpath(driver,'//textarea[@name="ctl00$cphBody$txbIndustryGrpInvestmentDsc"]'):
        desc=driver.find_element_by_xpath('//textarea[@name="ctl00$cphBody$txbIndustryGrpInvestmentDsc"]').text
    results['deficit']=investDeficit
    results['desc']=desc
    return results
def get_data_product(x,driver):
    if (x==1):
        AllDataheaders=driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucProduct2_Table1"]/tbody/tr[2]/td/table[@class="gv"]/tbody/tr[2]/th[not(contains(@style,"display: none;"))]/span')
        AvailableData={}
        k=1
        for i in AllDataheaders:
            AvailableData[k]=i.get_attribute('id')
            k=k+1
        AllnumericData=driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucProduct2_dgProduction"]/tbody/tr') 
        rows=[]
        for i in AllnumericData:
            datatemp={}
            itemnum=1
            TDS=i.find_elements_by_xpath('.//td')
            for j in TDS:
                data=j.text
                datatemp[str(DataMappingtoDBfield[AvailableData[itemnum]])]=get_true_value(data)
                itemnum=itemnum+1
            rows.append(datatemp)
        A=pd.DataFrame.from_dict(rows, orient='columns')
    if(x==2):
        AllDataheaders=driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucProduct1_Table1"]/tbody/tr[2]/td/table[@class="gv"]/tbody/tr[2]/th/span')
        AvailableData={}
        k=1
        for i in AllDataheaders:
            AvailableData[k]=i.get_attribute('id')
            k=k+1
        AllnumericData=driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucProduct1_dgProduction"]/tbody/tr') 
        rows=[]
        for i in AllnumericData:
            datatemp={}
            itemnum=1
            TDS=i.find_elements_by_xpath('.//td')
            for j in TDS:
                flag=False
                data=j.text
                datatemp[str(DataMappingtoDBfield_alternative[AvailableData[itemnum]])]=get_true_value(data)
                itemnum=itemnum+1
            rows.append(datatemp)
        A=pd.DataFrame.from_dict(rows, orient='columns')     
        A=A.iloc[:-1]
    A.replace('ك','ک',regex=True,inplace=True)
    A.replace('ي','ی',regex=True,inplace=True)
    return A
def ng_product_desc(driver):
    wholefile=str(driver.page_source)
    wholefile.find('"ProductMonthlyActivityDesc1","cells":')
    wholefile=wholefile[(wholefile.find('"ProductMonthlyActivityDesc1","cells":['))+8:]
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
    All=df1.value.tolist()
    counter=0
    desc_titles=['desc_title','desc_modif','desc_month','desc_year']
    alldescs={}
    alldescs['desc_title']=''
    desccounter=1
    for i in range(len(All)):
        counter=counter+1
        if counter%2==0:
            alldescs[desc_titles[desccounter]]=All[i]
            desccounter=desccounter+1
            #print(All[i])
    return alldescs         
def InsertService(DF):
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
                IF NOT EXISTS (select from monthly.services where name=%(Item)s ) THEN
                    INSERT INTO monthly.services(
                    name)
                    VALUES (%(Item)s);
                END IF;
            END
            $$ 

        """
        cursor.executemany(postgres_insert_query_cheif,DF.to_dict(orient='records'))
        connection.commit()





        postgres_insert_query_cheif = """
         DO 
            $$
            BEGIN
                IF NOT EXISTS (select from monthly."ServiceMonthly" where report_id=%(CodalID)s and service=(SELECT "ID" from monthly.services where name=%(Item)s limit (1) ) ) THEN
                    INSERT INTO monthly."ServiceMonthly"(
	service, desc_modification, "desc_toDate", desc_title, "lastAnnouncement", "nextAnnouncement", report_id, "contractDate", "contract_Duration", "RevenueUntilStartOfPeriod", "ModificationToStart", "TotalYearToStartOfPeriodModified", "RevenuePeriod", "TotalYearIncludingThisPeriod", "TotalRevLastYear", "PredictionRevenueYear", "PredictionCostYear", "ItemDesc", desc_month)
	VALUES ((SELECT "ID" from monthly.services where name=%(Item)s ), %(DESCMod)s, %(DESCY)s, %(DESCT)s, %(AnPrev)s, %(AnNext)s, %(CodalID)s, %(ContractStartDate)s, %(ContractDuration)s, %(RevUntilStartDate)s, %(Modifications)s, %(RevUntilStartDateModified)s, %(RevPeriod)s, %(RevUntilEndOfPeriod)s, %(RevLastYear)s, %(EstimationRevYear)s, %(EstimationCostYear)s, %(Desc)s, %(DESCM)s);
                END IF;
            END
            $$ 

        """
        cursor.executemany(postgres_insert_query_cheif,DF.to_dict(orient='records'))
        #######
        
        connection.commit()
        postgres_insert_query3 = """
        UPDATE monthly."PreMonthly"
        SET converted=True
        WHERE "report_ID"=%(CodalID)s
        """
        cursor.executemany(postgres_insert_query3,DF.to_dict(orient='records'))
        connection.commit()  
        print('Service Done ')
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print('Failed to Update FARA Bourse Companies ', error)
                # log_it('Failed to Update FARA Bourse Companies ')
    finally:
        if(connection):
            cursor.close()
            connection.close()
                  
def RUN(driver):                
    # driver = webdriver.Chrome()
    # driver.maximize_window()               
    df=get_unconverted()
    AllData=len(df.index)
    DFS=[]
    counter=0
    for index,row in df.iterrows():
        try:
            CodalRaw_ID=int(row['report_ID'])
            CodalRaw_links=row['HtmlUrl']
            #print(CodalRaw_ID)
            driver.get('http://codal.ir'+CodalRaw_links)
            time.sleep(3)
            driver.execute_script("document.body.style.zoom='100%';document.body.style.zoom='50%'")
            alldesc=get_description_product_service(driver)
            Announcment=get_announcments(driver)
            tree = html.fromstring(driver.page_source)
            toDates=[]
            
            for t in tree.xpath('//table[@id="ctl00_cphBody_ucService1_Table1"]//table//th/span'):
                if re.search('\d\d\d\d/\d\d/\d\d',t.text):
                    ss=(re.search('\d\d\d\d/\d\d/\d\d',t.text)[0])
                    if ss!="" and ss not in toDates:
                        toDates.append(ss)
            rows=[]
            for k in tree.xpath('//table[@id="ctl00_cphBody_ucService1_Table1"]//tr[3]//table//tr'):
                row=(k.xpath('.//td//text()'))
                rows.append(row)
            DF=pd.DataFrame(rows)
            if len(tree.xpath('//table[@id="ctl00_cphBody_ucService1_Table1"]//table//th/span'))==14:
                DF.columns=['ID','Item','ContractStartDate','ContractDuration','A','B','RevUntilStartDate','Modifications','RevUntilStartDateModified', 'RevPeriod','RevUntilEndOfPeriod','RevLastYear','Desc']
                DF['EstimationRevYear']=np.nan
                DF['EstimationCostYear']=np.nan
            if len(tree.xpath('//table[@id="ctl00_cphBody_ucService1_Table1"]//table//th/span'))==11:
                DF.columns=['Item','ContractStartDate','ContractDuration','EstimationRevYear','EstimationCostYear','RevPeriod',
            'RevUntilEndOfPeriod','RevLastYear','Desc']
                DF['RevUntilStartDate']=np.nan
                DF['Modifications']=np.nan
                DF['RevUntilStartDateModified']=np.nan
            
            DF.replace('ك','ک',regex=True,inplace=True)
            DF.replace('ي','ی',regex=True,inplace=True)
            for i in DF.columns:
                if i not in ['Item','ID','ContractStartDate','Desc']:
                    DF[i]=DF[i].apply(get_true_value)
            DF['AnNext']=Announcment['Next']
            DF['AnPrev']=Announcment['Last']
            DF['DESCT']=alldesc['desc_title']
            DF['DESCM']=alldesc['desc_month']
            DF['DESCY']=alldesc['desc_year']
            DF['DESCMod']=alldesc['desc_modif']
            DF['CodalRawLink']=CodalRaw_links
            DF['CodalID']=CodalRaw_ID
            DF = DF.replace({np.nan: None})
            # DFS.append(DF)
            InsertService(DF)
            counter=counter+1
            percentage=(counter*100)/AllData
            print("{0:.2f}".format(percentage))
            
        except (Exception, psycopg2.Error) as error :
            print(error)
            print(CodalRaw_links)
            continue
    # driver.quit()
        