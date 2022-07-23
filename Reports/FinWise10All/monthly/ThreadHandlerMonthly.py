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
import requests
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

       select * from codalraw."allrawReports" where ("LetterCode"='ن-۳۰' or "LetterCode"='ن-۳۱') and "TracingNo" not in (Select "report_ID" from codalraw."MonthlyConverted") and "HasHtml"=True """

                           , connection)
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read links", error)
    finally:
            if(connection):
                cursor.close()
                connection.close()
Investment_6=['company','portfo_endoftheyear_date','Assembly_date','stockCount_inAssembly','Capital','NominalValue',
              'Ownership','EPS','DPS','Incomethisyear','IncomelastYear','empty']
Investment_5=['out_firm','out_shareCount','out_shareCost','out_TotalCost','out_shareSellValue','out_TotalSellValue','Out_net']
Investment_4=['in_firm','in_shareCount','in_shareCost','in_TotalpublicCost','in_TotalOtcCost']
Investment_3=['companyCapital','NominalshareValue','start_shareCount','start_cost','change_shareCount','Change_cost','end_own','end_cost','end_costShare','policy']
Investment_2=['companyCapital','NominalshareValue','start_shareCount','start_cost','start_marketValue','change_shareCount','Change_cost','Change_marketValue','end_own','end_cost','end_marketValue','end_costShare','end_shareValue','end_change']
Investment_1=['public_start_companyCount', 'public_start_cost', 
              'public_start_marketValue', 'public_changes_cost', 'public_changes_marketValue',
              'public_end_companyCount', 'public_end_cost', 'public_end_marketValue', 'private_start_companyCount',
              'private_start_cost', 'private_changes_cost', 
              'private_end_companyCount', 'private_end_cost', 'investment_deficit']
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
    'ctl00_cphBody_ucProduct1_lblSalesAmountYearCaption':'saleAmountYear'           }
DataMappingtoDBfield_service={
    'ctl00_cphBody_ucService1_lblServiceContractNameCaption':'service_name',
    'ctl00_cphBody_ucService1_lblContractDateCaption':'contract_date',
    'ctl00_cphBody_ucService1_lblContractDurationCaption':'contract_duration',
    'ctl00_cphBody_ucService1_lblBeginningLastPeriod':'totalyear_tostartofperiod',
    'ctl00_cphBody_ucService1_lblRevenueDiff':'modif_tostartofperiod',
    'ctl00_cphBody_ucService1_lblEndOfTheFinancialLastPeriod':'totalyear_tostartofperiod_modified',
    'ctl00_cphBody_ucService1_lblRevenueDuringThePeriodCaption':'revenueperiod',
    'ctl00_cphBody_ucService1_lblRevenueFromTheBeginningCaption':'totalyear_toendofperiod',
    'ctl00_cphBody_ucService1_lblRevenueEndOfTheFinancialPeriodCaption':'totallastyear',
    'ctl00_cphBody_ucService1_lblDescCaption':'itemdesc',
    'ctl00_cphBody_ucService1_lblContractAmountCaption':'prediction_revenue_totalyear',
    'ctl00_cphBody_ucService1_lblAnticipatedContractCaption':'prediction_costofcontract_totalyear'}
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
def get_titlebox(driver):
    results=[]
    titlebox=driver.find_elements_by_xpath('//div[@class="symbol_and_name"]/div/div')
    i=0
    for t in titlebox:
        if(i==0):
            header_name=(t.text.split(':\n')[1])
            header_name2 = ''.join([i for i in header_name if not i.isdigit()])
            header_name2 = header_name2.replace('ك','ک')
            header_name2 = header_name2.replace('ي','ی')
            header_name=header_name2

        if(i==2):
            header_Ticker=(t.text.split(':\n')[1])
            header_Ticker_2 = ''.join([i for i in header_Ticker if not i.isdigit()])
            header_Ticker_2 = header_Ticker_2.replace('ك','ک')
            header_Ticker_2 = header_Ticker_2.replace('ي','ی')
            header_Ticker=header_Ticker_2
            if('(' in header_Ticker_2):
                header_Ticker_2=header_Ticker_2.split('(')[0]
            results.append(header_Ticker_2)
        if(i==5):
            header_PeriodLength=[int(s) for s in str.split(t.text) if s.isdigit()][0]
            text=t.text
            if('(حسابرسی نشده)') in text:
                text=text.replace(' (حسابرسی نشده)','')
            if('(حسابرسی شده)') in text:
                text=text.replace(' (حسابرسی شده)','')
            header_until=text[-10:]
            results.append(header_PeriodLength)
            results.append(header_until)
            results.append(header_name)
        if(i==7):
            results.append(t.text.split('/')[1])
        i=i+1
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
def get_data_service(driver):
    AllDataheaders=driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucService1_Table1"]/tbody/tr[2]/td/table[@class="gv"]/tbody/tr[2]/th/span')
    AvailableData={}
    AvailableData[1]='ctl00_cphBody_ucService1_lblServiceContractNameCaption'
    k=2
    for i in AllDataheaders:
        AvailableData[k]=i.get_attribute('id')
        k=k+1
    AvailableData[k]='ctl00_cphBody_ucService1_lblDescCaption'
    AllnumericData=driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucService1_dgContract"]/tbody/tr') 
    rows=[]
    for i in AllnumericData:
        datatemp={}
        itemnum=1
        TDS=i.find_elements_by_xpath('.//td')
        #       if str(TDS[0].get_attribute('innerHTML')).replace('-','').isdigit():
        #           TDS=TDS[1:]
        for j in TDS:
            data=j.get_attribute('innerHTML')
            temp_j=data.replace('&nbsp;','')
            temp_j=data.replace(',','')
            temp_j=temp_j.replace('(','')
            temp_j=temp_j.replace(')','')
            #print(itemnum)
            if(temp_j.isdigit()):
                datatemp[str(DataMappingtoDBfield_service[AvailableData[itemnum]])]=get_true_value(data)
            else:
                datatemp[str(DataMappingtoDBfield_service[AvailableData[itemnum]])]=data
            itemnum=itemnum+1
        rows.append(datatemp)
    A=pd.DataFrame.from_dict(rows, orient='columns')
    A.replace('ك','ک',regex=True,inplace=True)
    A.replace('ي','ی',regex=True,inplace=True)
    A.replace('&nbsp;','',regex=True,inplace=True)
    if A.empty:
        return A
    A=A[A['service_name']!='جمع']
    return A
def ng_product(driver):
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
    dt1=df1[(df1["address"].str.contains("A", na=False)) &(df1['cssClass']=='')&(df1['cellGroupName']=='Body')&(df1['value']!='')&(df1['rowTypeName']=='CustomRow')]
    dt1.drop_duplicates(inplace=True)
    products=dt1[['rowSequence','value']]
    products_newCol=products.copy(deep=True)
    products_newCol.columns=['seq','item']
    dt2=df1[(df1["address"].str.contains("B", na=False)) &(df1['cssClass']=='')&(df1['cellGroupName']=='Body')&(df1['value']!='')&(df1['rowTypeName']=='CustomRow')]
    dt2.drop_duplicates(inplace=True)
    units=dt2[['rowSequence','value']]
    dates=df1.periodEndToDate.unique().tolist()
    dates.remove('')
    dates.sort()
    test2=df1[(df1['periodEndToDate']==dates[0])&(df1['rowTypeName']=='CustomRow')&(df1['valueTypeName']=='FormControl')&(df1['dataTypeName']=='Integer')].drop_duplicates()
    test2=test2.sort_values(by='rowSequence')[['rowSequence','columnSequence','value']]
    test3=test2.merge(products,on='rowSequence')
    test3=test3[['value_x','value_y','columnSequence']]
    test3.columns=['value','item','sequence']
    test3pivot=test3.pivot(index='item' ,columns='sequence', values='value').reset_index()
    dates_0_final=test3pivot.merge(products_newCol,on='item').sort_values(by='seq')
    dates_1=df1[(df1['periodEndToDate']==dates[1])&(df1['rowTypeName']=='CustomRow')&(df1['valueTypeName']=='FormControl')&(df1['dataTypeName']=='Integer')].drop_duplicates()
    dates_1=dates_1.sort_values(by='rowSequence')[['rowSequence','columnSequence','value']]
    dates_1_2=dates_1.merge(products,on='rowSequence')
    dates_1_2=dates_1_2[['value_x','value_y','columnSequence']]
    dates_1_2.columns=['value','item','sequence']
    dates_1_2_pivot=dates_1_2.pivot(index='item' ,columns='sequence', values='value').reset_index()
    dates_1_final=dates_1_2_pivot.merge(products_newCol,on='item').sort_values(by='seq')
    modif=df1[(df1['periodEndToDate']=='')&(df1['rowTypeName']=='CustomRow')&(df1['valueTypeName']=='FormControl')&(df1['dataTypeName']=='Integer')].drop_duplicates()
    modif=modif.sort_values(by='rowSequence')[['rowSequence','columnSequence','value']]
    modif_1=modif.merge(products,on='rowSequence')
    modif_1=modif_1[['value_x','value_y','columnSequence']]
    modif_1.columns=['value','item','sequence']
    modif_1_pivot=modif_1.pivot(index='item' ,columns='sequence', values='value').reset_index()
    modif_final=modif_1_pivot.merge(products_newCol,on='item').sort_values(by='seq')
    dates_0_final.columns=['product_Name',"prevTotalProductionYear","prevTotalSalesYear","prevTotalSalesRateYear",
                           "prevTotalSalesAmountYear","prev_modified_TotalProduction","prev_modified_TotalSales",
                           "prev_modified_TotalSalesRate","prev_modified_TotalSalesAmount",'seq']
    dates_1_final.columns=['product_Name',"totalProductionPeriod","totalSalePeriod","saleRatePeriod",
            "saleAmountPeriod","totalProductionYear","totalSaleYear",
            "saleRateYear","saleAmountYear",'seq']
    modif_final.columns=['product_Name',"modification_Production","modification_Sales","modification_SalesAmount",'seq']
    final=dates_0_final.merge(modif_final,on='product_Name')
    final_1=final.merge(dates_1_final,on='product_Name')
    units.columns=['seq','product_unit']
    final_1=final_1.merge(units,on=['seq'])

    final_1.drop(columns=['seq','seq_x','seq_y'],inplace=True)
    final_1.fillna(0,inplace=True)
    final_1=final_1.round(decimals=0)
    for i in final_1.columns:
        if not (i=='product_Name' or i=='product_unit'):
            final_1[i]=final_1[i].apply(lambda L: str(L).split('.')[0])
            final_1[i]=final_1[i].astype(np.int64)
    return final_1
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
def get_data_investment(driver):
    results=[]
    select = Select(driver.find_element_by_id('ddlTable'))
    select.select_by_visible_text('صورت ريزمعاملات سهام - واگذار شده')
    time.sleep(4)
    if check_exists_by_xpath(driver,'*//table[@id="ctl00_cphBody_dgInvIncome1"]/tbody/tr'):
        tablerows=driver.find_elements_by_xpath('*//table[@id="ctl00_cphBody_dgInvIncome1"]/tbody/tr')
        invest_df=pd.DataFrame()
        ind=[]
        for i in tablerows:
            k=0
            dictionary_invest5={}
            for t in i.find_elements_by_xpath('.//td'):
                data=t.text
                dictionary_invest5[str(Investment_5[k])]=get_true_value(data)
                k=k+1
            ind.append(dictionary_invest5)
        Invest5_readyforDB=pd.DataFrame.from_dict(ind,orient='columns')
        Invest5_readyforDB.replace('ك','ک',regex=True,inplace=True)
        Invest5_readyforDB.replace('ي','ی',regex=True,inplace=True)
        results.append(Invest5_readyforDB)
    if(len(results)<1):
        results.append(pd.DataFrame())
    select = Select(driver.find_element_by_id('ddlTable'))
    select.select_by_visible_text('صورت ريزمعاملات سهام - تحصیل شده')
    time.sleep(4)
    if check_exists_by_xpath(driver,'*//table[@id="ctl00_cphBody_dgInvIncome0"]/tbody/tr'):
        tablerows=driver.find_elements_by_xpath('*//table[@id="ctl00_cphBody_dgInvIncome0"]/tbody/tr')
        invest_df=pd.DataFrame()
        ind=[]
        for i in tablerows:
            k=0
            dictionary_invest4={}
            for t in i.find_elements_by_xpath('.//td'):
                data=t.text
                dictionary_invest4[str(Investment_4[k])]=get_true_value(data)
                k=k+1
            ind.append(dictionary_invest4)
        Invest4_readyforDB=pd.DataFrame.from_dict(ind,orient='columns')
        Invest4_readyforDB.replace('ك','ک',regex=True,inplace=True)
        Invest4_readyforDB.replace('ي','ی',regex=True,inplace=True)
        results.append(Invest4_readyforDB)
    if(len(results)<2):
        results.append(pd.DataFrame())
    select = Select(driver.find_element_by_id('ddlTable'))
    select.select_by_visible_text('صورت وضعیت پورتفوی شرکتهای خارج از بورس')
    time.sleep(4)
    if check_exists_by_xpath(driver,'*//div[@role="ctl00_cphBody_dgPeriodicPortfo1"]/div/div/div/table/tbody/tr/td[@class="ui-tinytbl-firstcol ui-tinytbl-lastcol"]'):
        companies=driver.find_elements_by_xpath('*//div[@role="ctl00_cphBody_dgPeriodicPortfo1"]/div/div/div/table/tbody/tr/td[@class="ui-tinytbl-firstcol ui-tinytbl-lastcol"]')
        listofCompanies=[]
        for i in companies:
            #for t in i.find_elements_by_xpath('.//td'):
            listofCompanies.append(i.get_attribute("innerHTML"))
        datatoget=driver.find_elements_by_xpath('*//div[@class="ui-tinytbl-tb-right ui-widget-content"]/div/table/tbody/tr')
        invest_df=pd.DataFrame()
        ind=[]
        for i in datatoget:
            k=0
            dictionary_invest3={}
            for t in i.find_elements_by_xpath('.//td'): 
                data=t.text
                dictionary_invest3[str(Investment_3[k])]=get_true_value(data)
                k=k+1
            ind.append(dictionary_invest3)
        Invest3_readyforDB=pd.DataFrame.from_dict(ind,orient='columns')
        Invest3_readyforDB['company']=listofCompanies
        Invest3_readyforDB['Type']='private'
        Invest3_readyforDB.replace('ك','ک',regex=True,inplace=True)
        Invest3_readyforDB.replace('ي','ی',regex=True,inplace=True)
        results.append(Invest3_readyforDB)
    else:
        if check_exists_by_xpath(driver,'//table[@id="ctl00_cphBody_dgPeriodicPortfo1"]/tbody/tr'):
            listofDicts=[]
            Invest3Titles2=['company']+Investment_3
            for i in range(len(driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_dgPeriodicPortfo1"]//tbody/tr'))-1):
                rowitems=driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_dgPeriodicPortfo1"]//tbody/tr['+str(i+1)+']/td')
                itemCounter=0 
                dictionary={}
                for t in rowitems:
                    dictionary[Invest3Titles2[itemCounter]]=get_true_value(t.text)
                    #print(get_true_value(t.text))
                    itemCounter=itemCounter+1
                listofDicts.append(dictionary)
            Invest3_readyforDB=pd.DataFrame(listofDicts)
            Invest3_readyforDB['Type']='private'
            Invest3_readyforDB.replace('ك','ک',regex=True,inplace=True)
            Invest3_readyforDB.replace('ي','ی',regex=True,inplace=True)
            results.append(Invest3_readyforDB)
    if(len(results)<3):
        results.append(pd.DataFrame())
    select = Select(driver.find_element_by_id('ddlTable'))
    select.select_by_visible_text('صورت وضعیت پورتفوی شرکتهای پذیرفته شده در بورس')
    time.sleep(4)
    if check_exists_by_xpath(driver,'*//div[@role="ctl00_cphBody_dgPeriodicPortfo0"]/div/div/div/table/tbody/tr/td[@class="ui-tinytbl-firstcol ui-tinytbl-lastcol"]'):
        companies=driver.find_elements_by_xpath('*//div[@role="ctl00_cphBody_dgPeriodicPortfo0"]/div/div/div/table/tbody/tr/td[@class="ui-tinytbl-firstcol ui-tinytbl-lastcol"]')
        listofCompanies=[]
        for i in companies:
            #for t in i.find_elements_by_xpath('.//td'):
            listofCompanies.append(i.get_attribute("innerHTML"))
        datatoget=driver.find_elements_by_xpath('*//div[@class="ui-tinytbl-tb-right ui-widget-content"]/div/table/tbody/tr')
        invest_df=pd.DataFrame()
        ind=[]
        for i in datatoget:
            k=0
            dictionary_invest2={}
            for t in i.find_elements_by_xpath('.//td'): 
                data=t.text
                dictionary_invest2[str(Investment_2[k])]=get_true_value(data)
                k=k+1
            ind.append(dictionary_invest2)
        Invest2_readyforDB=pd.DataFrame.from_dict(ind,orient='columns')
        Invest2_readyforDB['company']=listofCompanies
        Invest2_readyforDB['Type']='public'
        Invest2_readyforDB.replace('ك','ک',regex=True,inplace=True)
        Invest2_readyforDB.replace('ي','ی',regex=True,inplace=True)        
        results.append(Invest2_readyforDB)
    if(len(results)<4):
        results.append(pd.DataFrame())
    select = Select(driver.find_element_by_id('ddlTable'))
    select.select_by_visible_text('صورت خلاصه سرمایه گذاریها به تفکیک گروه صنعت')
    time.sleep(4)
    if check_exists_by_xpath(driver,'*//div[@role="ctl00_cphBody_dgIndustryGroupInvestment"]/div/div/div/table/tbody/tr/td[@class="ui-tinytbl-firstcol ui-tinytbl-lastcol"]'):
        companies=driver.find_elements_by_xpath('*//div[@role="ctl00_cphBody_dgIndustryGroupInvestment"]/div/div/div/table/tbody/tr/td[@class="ui-tinytbl-firstcol ui-tinytbl-lastcol"]')
        listofCompanies=[]
        for i in companies:
            #for t in i.find_elements_by_xpath('.//td'):
            listofCompanies.append(i.get_attribute("innerHTML"))
        datatoget=driver.find_elements_by_xpath('*//div[@class="ui-tinytbl-tb-right ui-widget-content"]/div/table/tbody/tr')
        invest_df=pd.DataFrame()
        ind=[]
        for i in datatoget:
            k=0
            dictionary_invest1={}
            for t in i.find_elements_by_xpath('.//td'): 
                data=t.text
                dictionary_invest1[str(Investment_1[k])]=get_true_value(data)
                k=k+1
                if(k==13):
                    break
            ind.append(dictionary_invest1)
        Invest1_readyforDB=pd.DataFrame.from_dict(ind,orient='columns')
        Invest1_readyforDB['company']=listofCompanies
        Invest1_readyforDB.replace('ك','ک',regex=True,inplace=True)
        Invest1_readyforDB.replace('ي','ی',regex=True,inplace=True)        
        results.append(Invest1_readyforDB)
    if(len(results)<5):
        results.append(pd.DataFrame())
    select = Select(driver.find_element_by_id('ddlTable'))
    if ('درآمد حاصل از سود سهام محقق شده') in select.options:
        select.select_by_visible_text('درآمد حاصل از سود سهام محقق شده')
        time.sleep(4)
        if check_exists_by_xpath(driver,'*//table[@id="ctl00_cphBody_ucDividendRealized_dgPeriodicPortfoDividendRealized"]/tbody/tr'):
            tablerows=driver.find_elements_by_xpath('*//table[@id="ctl00_cphBody_ucDividendRealized_dgPeriodicPortfoDividendRealized"]/tbody/tr')
            invest_df=pd.DataFrame()
            ind=[]
            for i in tablerows:
                k=0
                dictionary_invest4={}
                for t in i.find_elements_by_xpath('.//td'):
                    data=t.text
                    #print(data)
                    dictionary_invest4[str(Investment_6[k])]=get_true_value(data)
                    k=k+1
                    #print(k)
                ind.append(dictionary_invest4)
            Invest6_readyforDB=pd.DataFrame.from_dict(ind,orient='columns')
            Invest6_readyforDB.replace('ك','ک',regex=True,inplace=True)
            Invest6_readyforDB.replace('ي','ی',regex=True,inplace=True)
            if check_exists_by_xpath(driver,'//*[@id="ctl00_cphBody_ucDividendRealized_txbRealizedMonthLastMonth"]'):
                prev_total_income=get_true_value(str(driver.find_element_by_id('ctl00_cphBody_ucDividendRealized_txbRealizedMonthLastMonth').get_attribute('value')))
            if check_exists_by_xpath(driver,'//*[@id="ctl00_cphBody_ucDividendRealized_txbRealizedMonthCurrentMonth"]'):
                total_income_thisyear=get_true_value(str(driver.find_element_by_id('ctl00_cphBody_ucDividendRealized_txbRealizedMonthCurrentMonth').get_attribute('value')))
            Invest6_readyforDB['total_income_thisyear']=total_income_thisyear
            Invest6_readyforDB['prev_total_income']=prev_total_income
            results.append(Invest6_readyforDB)
    if(len(results)<6):
        DF=pd.DataFrame(columns=Investment_6)
        if check_exists_by_xpath(driver,'//*[@id="ctl00_cphBody_ucDividendRealized_txbRealizedMonthLastMonth"]'):
            DF=DF.append(pd.Series(), ignore_index=True)
            prev_total_income=get_true_value(str(driver.find_element_by_id('ctl00_cphBody_ucDividendRealized_txbRealizedMonthLastMonth').get_attribute('value')))
            DF['prev_total_income']=prev_total_income

        if check_exists_by_xpath(driver,'//*[@id="ctl00_cphBody_ucDividendRealized_txbRealizedMonthCurrentMonth"]'):
            total_income_thisyear=get_true_value(str(driver.find_element_by_id('ctl00_cphBody_ucDividendRealized_txbRealizedMonthCurrentMonth').get_attribute('value')))
            DF['total_income_thisyear']=total_income_thisyear
        results.append(DF)            
    return results      
def Constructions_results(driver):
    listofDicts=[]
    ConstructionLabels=[
        'ProjectName','Location','TypeofProject','Unit','thisMonth_Cost','MeterSold','SaleRate','SaleAmount',
        'FromBefore_Cost','FromBefore_Revenue','TotalYear_Cost','TotalYear_MeterSold','TotalYear_SaleRate','TotalYear_SaleAmount']
    for i in range(len(driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucBuilding1_dgBuilding"]//tr'))-1):
        rowitems=driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucBuilding1_dgBuilding"]//tr['+str(i+1)+']/td')
        itemCounter=0 
        dictionary={}
        for t in rowitems:
            dictionary[ConstructionLabels[itemCounter]]=get_true_value(t.text)
            itemCounter=itemCounter+1
        listofDicts.append(dictionary)
    SoldProjects=pd.DataFrame(listofDicts)
    listofDicts=[]
    ConstructionLabels_2=[
        'ProjectName','Location','TypeofProject','Ownership','Unit','NetMeter','lastMonth_physicalProgress','lastMonth_Cost',
        'lastMonth_EstimationOfRemainingCost','lastMonth_EstmiatedTotalCost','SoldProjectsDuringMonth_meter','SoldProjectsDuringMonth_cost',
        'thisMonth_physicalProgress','thisMonth_cost','thisMonth_EstimationRemainingCost','thisMonth_EsitmatedTotalCost']
    for i in range(len(driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucBuilding1_ucProjStat_dgBuildingProjStat"]//tr'))-1):
        rowitems=driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucBuilding1_ucProjStat_dgBuildingProjStat"]//tr['+str(i+1)+']/td')
        itemCounter=0 
        dictionary={}
        for t in rowitems:
            #print(t.text)
            dictionary[ConstructionLabels_2[itemCounter]]=get_true_value(t.text)
            itemCounter=itemCounter+1
        listofDicts.append(dictionary)
    Progress=pd.DataFrame(listofDicts)
    results={}
    SoldProjects.replace('ك','ک',regex=True,inplace=True)
    SoldProjects.replace('ي','ی',regex=True,inplace=True)
    Progress.replace('ك','ک',regex=True,inplace=True)
    Progress.replace('ي','ی',regex=True,inplace=True)
    results['SoldProjects']=SoldProjects
    results['Progress']=Progress
    return results
def Leasing_results(driver):
    listofDicts=[]
    LeasingLabels=['Title','thisPeriod_Amount','Total_Amount']
    for i in range(len(driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucLeasing1_dgAchievedRevenue"]//tr'))-1):
        rowitems=driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucLeasing1_dgAchievedRevenue"]//tr['+str(i+1)+']/td')
        itemCounter=0 
        dictionary={}
        for t in rowitems:
            dictionary[LeasingLabels[itemCounter]]=get_true_value(t.text)
            #print(get_true_value(t.text))
            itemCounter=itemCounter+1
        listofDicts.append(dictionary)
    LeasingAchievedRev=pd.DataFrame(listofDicts)
    #
    listofDicts=[]
    LeasingLabels2=['title','thisPeriod_Achievedcost','Total_Achievedcost','thisPeriod_TakenFacilityRemaind','Total_TakenFacilityRemained']
    for i in range(len(driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucLeasing1_dgAchievedCost"]//tr'))-1):
        rowitems=driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucLeasing1_dgAchievedCost"]//tr['+str(i+1)+']/td')
        itemCounter=0 
        dictionary={}
        for t in rowitems:
            dictionary[LeasingLabels2[itemCounter]]=get_true_value(t.text)
            #print(get_true_value(t.text))
            itemCounter=itemCounter+1
        listofDicts.append(dictionary)
    LeasingAchievedCost=pd.DataFrame(listofDicts)
    #
    listofDicts=[]
    LeasingLabels3=['title','StartMonth_count','StartMonth_Amount','thismonth_NewFacilityCount','thismonth_NewFacilityAmount','thismonth_SetteledCount','thismonth_SetteledAmount','EndMonth_FacilityCount','EndMonth_FacilityAmount']
    for i in range(len(driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucLeasing1_dgDelegatedGoodsSummary"]//tr'))-1):
        rowitems=driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucLeasing1_dgDelegatedGoodsSummary"]//tr['+str(i+1)+']/td')
        itemCounter=0 
        dictionary={}
        for t in rowitems:
            dictionary[LeasingLabels3[itemCounter]]=get_true_value(t.text)
            #print(get_true_value(t.text))
            itemCounter=itemCounter+1
        listofDicts.append(dictionary)
    DelegatedGoods=pd.DataFrame(listofDicts)
    results={}
    LeasingAchievedRev.replace('ك','ک',regex=True,inplace=True)
    LeasingAchievedCost.replace('ك','ک',regex=True,inplace=True)
    DelegatedGoods.replace('ك','ک',regex=True,inplace=True)
    LeasingAchievedRev.replace('ي','ی',regex=True,inplace=True)
    LeasingAchievedCost.replace('ي','ی',regex=True,inplace=True)
    DelegatedGoods.replace('ي','ی',regex=True,inplace=True)
    results['Revenue']=LeasingAchievedRev
    results['Costs']=LeasingAchievedCost
    results['Delegated']=DelegatedGoods
    return results            
def Bank_results(driver):
    listofDicts=[]
    BankLabels=['title','StartPeriod_RemainingFacility','thisPeriod_IssuedFacilitiy','thisPeriod_SetteledFacility',
               'EnePeriod_RemainingFacility','thisPeriod_RevenueFromFacility','thisYear_RevenueFromFacility']
    BankLabesls_withModif=['title','StartPeriod_RemainingFacility','StartPeriod_RemainingFacility_modifications'
                           ,'StartPeriod_RemainingFacility_modified','thisPeriod_IssuedFacilitiy','thisPeriod_SetteledFacility',
                          'EnePeriod_RemainingFacility','PreviousPeriods_RevenueFromFacility',
                           'PreviousPeriods_RevenueFromFacility_modification','PreviousPeriods_RevenueFromFacility_modified',
                           'thisPeriod_RevenueFromFacility',
                           'thisYear_RevenueFromFacility']
    for i in range(len(driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucBank1_dgFacility"]//tr'))-1):
        rowitems=driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucBank1_dgFacility"]//tr['+str(i+1)+']/td')
        itemCounter=0 
        dictionary={}
        for t in rowitems:
            dictionary[BankLabels[itemCounter]]=get_true_value(t.text)
            #print(get_true_value(t.text))
            itemCounter=itemCounter+1
        listofDicts.append(dictionary)
    for i in range(len(driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucBank2_dgFacility"]//tr'))-1):
        rowitems=driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucBank2_dgFacility"]//tr['+str(i+1)+']/td')
        itemCounter=0 
        dictionary={}
        for t in rowitems:
            dictionary[BankLabesls_withModif[itemCounter]]=get_true_value(t.text)
            #print(get_true_value(t.text))
            itemCounter=itemCounter+1
        listofDicts.append(dictionary)
    BankDF=pd.DataFrame(listofDicts)
    for j in BankLabesls_withModif:
        if j not in BankDF.columns.tolist():
            BankDF[j]=0
    results={}
    BankDF.replace('ك','ک',regex=True,inplace=True)
    BankDF.replace('ي','ی',regex=True,inplace=True)
    results['Facilities']=BankDF
    #
    listofDicts=[]
    BankDepositLabels=['title','StartPeriod_Deposits','thisPeriod_IncomingDeposit','thisPeriod_setteledDeposits',
                                'EndPeriod_Deposits','thisPeriod_InterestOnDeposits','Total_InterestOnDeposits']
    
    BankDepositLabels_withModif=['title','StartPeriod_Deposits','StartPeriod_Deposits_modifications',
                                 'StartPeriod_Deposits_modified','thisPeriod_IncomingDeposit','thisPeriod_setteledDeposits',
                                'EndPeriod_Deposits','PreviousPeriods_InterestOnDeposits','PreviousPeriods_InterestOnDeposits_modifications'
                                 ,'PreviousPeriods_InterestOnDeposits_modified','thisPeriod_InterestOnDeposits','Total_InterestOnDeposits']
    for i in range(len(driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucBank1_dgDeposit"]//tr'))-1):
        rowitems=driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucBank1_dgDeposit"]//tr['+str(i+1)+']/td')
        itemCounter=0 
        dictionary={}
        for t in rowitems:
            dictionary[BankDepositLabels[itemCounter]]=get_true_value(t.text)
            #print(get_true_value(t.text))
            itemCounter=itemCounter+1
        listofDicts.append(dictionary)
    for i in range(len(driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucBank2_dgDeposit"]//tr'))-1):
        rowitems=driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucBank2_dgDeposit"]//tr['+str(i+1)+']/td')
        itemCounter=0 
        dictionary={}
        for t in rowitems:
            dictionary[BankDepositLabels_withModif[itemCounter]]=get_true_value(t.text)
            #print(get_true_value(t.text))
            itemCounter=itemCounter+1
        listofDicts.append(dictionary)
    DepositsDF=pd.DataFrame(listofDicts)
    for j in BankDepositLabels_withModif:
        if j not in DepositsDF.columns.tolist():
            DepositsDF[j]=0
    DepositsDF.replace('ك','ک',regex=True,inplace=True)
    DepositsDF.replace('ي','ی',regex=True,inplace=True)
    results['Deposits']=DepositsDF
    return results
def Inusrance_results(driver):
    listofDicts=[]
    InsuranceLabels_Modified=['InsuranceField','PreviousPeriods_IssuedInsurance_Amount','PreviousPeriods_IssuedInusrance_Share',
                     'PreviousPeriods_DamageRepaid_Amount','PreviousPeriods_DamageRepaid_Share',
                     'Modification_IssuedInsurance_Amount','Modification_DamageRepaid_Amount',
                     'PreviousModified_IssuedInsurance_Amount','PreviousModified_IssuedInusrance_Share','PreviousModified_DamageRepaid_Amount','PreviousModified_DamageRepaid_Share',
                     'ThisPeriod_IssuedInsurance_Amount','ThisPeriod_IssuedInusrance_Share','ThisPeriod_DamageRepaid_Amount','ThisPeriod_DamageRepaid_Share',
                     'Total_IssuedInsurance_Amount','Total_IssuedInusrance_Share','Total_DamageRepaid_Amount','Total_DamageRepaid_Share',             
                    ]
    InsuranceLabels=['InsuranceField','ThisPeriod_IssuedInsurance_count'
                     'ThisPeriod_IssuedInsurance_Amount','ThisPeriod_IssuedInusrance_Share',
                     'ThisPeriod_DamageRepaid_count','ThisPeriod_DamageRepaid_Amount','ThisPeriod_DamageRepaid_Share',
                     'Total_IssuedInsurance_count','Total_IssuedInsurance_Amount',
                     'Total_IssuedInusrance_Share','Total_DamageRepaid_count','Total_DamageRepaid_Amount','Total_DamageRepaid_Share','Empty','Empty2']
    for i in range(len(driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucInsurance2_dgInsurance"]//tr'))-1):
        rowitems=driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucInsurance2_dgInsurance"]//tr['+str(i+1)+']/td')
        itemCounter=0 
        dictionary={}
        for t in rowitems:
            dictionary[InsuranceLabels_Modified[itemCounter]]=get_true_value(t.text)
            #print(get_true_value(t.text))
            itemCounter=itemCounter+1
        listofDicts.append(dictionary)
    for i in range(len(driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucInsurance1_dgInsurance"]//tr'))-1):
        rowitems=driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucInsurance1_dgInsurance"]//tr['+str(i+1)+']/td')
        itemCounter=0 
        dictionary={}
        for t in rowitems:
            #print(itemCounter)
            #print(get_true_value(t.text))
            dictionary[InsuranceLabels[itemCounter]]=get_true_value(t.text)
            itemCounter=itemCounter+1
        listofDicts.append(dictionary)
    
    InsuranceDF=pd.DataFrame(listofDicts)
    for j in InsuranceLabels_Modified:
        if j not in InsuranceDF.columns.tolist():
            InsuranceDF[j]=0
    Differences=['ThisPeriod_IssuedInsurance_count','ThisPeriod_DamageRepaid_count','Total_IssuedInsurance_count','Total_DamageRepaid_count','Empty','Empty2']
    for delcol in Differences:
        if delcol in InsuranceDF.columns.tolist():
            InsuranceDF.drop(columns=[delcol],inplace=True)
    results={}
    InsuranceDF.replace('ك','ک',regex=True,inplace=True)
    InsuranceDF.replace('ي','ی',regex=True,inplace=True)
    
    InsuranceDF.dropna(subset=['InsuranceField','PreviousPeriods_IssuedInsurance_Amount'],inplace=True)
    InsuranceDF.fillna(0,inplace=True)
    results['Insurance']=InsuranceDF
    return results
def NG_product_ultimate(titles,announcments,desc,CID,Clink,driver):
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
    Categoriesdf=pd.DataFrame.from_dict({1:'Domestic_Sale',2:'Export_Sale',3:'Service_revenue',4:'Refund',5:'Discount'  },orient='index')
    Categoriesdf.reset_index(inplace=True)
    Categoriesdf.columns=['Categories','categoryName']
    NEW.replace('',0,regex=True,inplace=True)
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
    descdf=pd.DataFrame([desc])
    titlesdf=pd.DataFrame([titles])
    titlesdf.columns=['nemad','period','toDate','firm_reporting']
    announcedf=pd.DataFrame([announcments])
    NEW2['tmp'] = 1
    descdf['tmp'] = 1
    NEW2_1 = pd.merge(NEW2, descdf, on=['tmp'])
    NEW2_1 = NEW2_1.drop('tmp', axis=1)
    NEW2_1['tmp'] = 1
    titlesdf['tmp'] = 1
    NEW2_2= pd.merge(NEW2_1, titlesdf, on=['tmp'])
    NEW2_2 = NEW2_2.drop('tmp', axis=1)
    NEW2_2['tmp'] = 1
    announcedf['tmp'] = 1
    NEW2_3= pd.merge(NEW2_2, announcedf, on=['tmp'])
    NEW2_3 = NEW2_3.drop('tmp', axis=1)
    NEW2_3['report_id']=CID
    NEW2_3=NEW2_3[(NEW2_3['good']!='')&(NEW2_3['good']!=None)&(NEW2_3['good']!='None')&(NEW2_3['good'].notnull())]
    return NEW2_3
def insertProduct(titles,Announcment,alldesc,DF,CID,Clink):
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        counter=0
        listofAll=["totalProductionPeriod","totalSalePeriod","saleRatePeriod",
        "saleAmountPeriod","totalProductionYear","totalSaleYear",
        "saleRateYear","saleAmountYear","prevTotalProductionYear",
        "prevTotalSalesYear","prevTotalSalesRateYear","prevTotalSalesAmountYear",
        "modification_Production","modification_Sales","modification_SalesAmount",
        "prev_modified_TotalProduction","prev_modified_TotalSalesRate",
        "prev_modified_TotalSalesAmount","prev_modified_TotalSales"]
        for t in listofAll:
            if t not in DF.columns:
                DF[t]=None
        for index,row in DF.iterrows():
            if (row['product_Name']=='جمع'):
                continue
            postgres_insert_query = """ 
            DO 
            $$
            BEGIN
                IF NOT EXISTS (select from monthly.goods where "name"=%s) THEN
                    INSERT INTO monthly.goods ("name","unit") VALUES (%s,%s);
                END IF;
            END
            $$ """
            record_to_insert = (str(row['product_Name']).strip(),str(row['product_Name']).strip(),str(row['product_unit']).strip())
            postgres_insert_query2= """
             DO 
                $$
                BEGIN
                    IF NOT EXISTS (select from monthly."monthlyProduction" where "report_id"=%s and "good"=(Select "ID" from monthly.goods where "name"=%s)) THEN
                       INSERT INTO monthly."monthlyProduction"( firm, reported_firm, "period", "toDate", good, desc_modification, 
             "desc_onePeriod", "desc_toDate", desc_title, "lastAnnouncment","nextAnnouncement",
             "totalProductionPeriod", "totalSalePeriod", "saleRatePeriod", "saleAmountPeriod", 
           "totalProductionYear", "totalSaleYear", "saleRateYear", "saleAmountYear", "prevTotalProductionYear", 
            "prevTotalSalesYear", "prevTotalSalesRateYear", "prevTotalSalesAmountYear", "modification_Production", 
            "modification_Sales", "modification_SalesAmount", "prev_modified_TotalProduction", "prev_modified_TotalSalesRate", 
            "prev_modified_TotalSalesAmount", "prev_modified_TotalSales","report_id")
         VALUES ( (SELECT "ID" from "Entity" where "ticker"= %s ), %s, %s, %s, (Select "ID" from monthly.goods where "name"=%s), %s,%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    END IF;
                END
                $$ 

            """
            record_to_insert2 = (int(CID),str(row['product_Name']).strip(),
                                 titles[0],titles[3],titles[1],titles[2],str(row['product_Name']).strip(),
                                 alldesc['desc_modif'],alldesc['desc_month'],alldesc['desc_year'],alldesc['desc_title'],Announcment['Last'],Announcment['Next'],
                                 row["totalProductionPeriod"], row["totalSalePeriod"], row["saleRatePeriod"], 
                                 row["saleAmountPeriod"], row["totalProductionYear"], row["totalSaleYear"], 
                                 row["saleRateYear"], row["saleAmountYear"], row["prevTotalProductionYear"], 
                                 row["prevTotalSalesYear"], row["prevTotalSalesRateYear"], row["prevTotalSalesAmountYear"], 
                                 row["modification_Production"], row["modification_Sales"], row["modification_SalesAmount"], 
                                 row["prev_modified_TotalProduction"], row["prev_modified_TotalSalesRate"], 
                                 row["prev_modified_TotalSalesAmount"], row["prev_modified_TotalSales"],int(CID)
                                )
            cursor.execute(postgres_insert_query, record_to_insert)
            #print('GoodsDone')
            cursor.execute(postgres_insert_query2, record_to_insert2)
            #print(record_to_insert2)
            counter=counter+1
        postgres_insert_query3 = """
        INSERT INTO codalraw."MonthlyConverted"(
        "report_ID", converted)
        VALUES (%s, True);
        """
        print(titles[0] + '-- '+ str(counter)+' row')
        record_to_insert3=([CID])
        cursor.execute(postgres_insert_query3, record_to_insert3)
        connection.commit()
    except(Exception, psycopg2.Error) as error:
        if(connection):
            print("Failed to insert record", error)
            log_it("insertProduct Failed "+str(CID))
            print(str(CodalRaw_links))
    finally:
        if(connection):
            cursor.close()
            connection.close()
def insertService(titles,Announcment,alldesc,DF,CID,Clink):
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        counter=0
        listofAll=[
        "contract_date", "contract_duration", "totalyear_tostartofperiod", 
                                 "modif_tostartofperiod", 
                                 "totalyear_tostartofperiod_modified", "revenueperiod", 
                                 "totalyear_toendofperiod", "totallastyear", "itemdesc",
                                 "prediction_revenue_totalyear", "prediction_costofcontract_totalyear"
        ]
        for t in listofAll:
            if t not in DF.columns:
                DF[t]=None
        for index,row in DF.iterrows():
            if (row['service_name']=='جمع'):
                continue
            postgres_insert_query = """ 
            DO 
            $$
            BEGIN
                IF NOT EXISTS (select from monthly.services where "name"=%s) THEN
                    INSERT INTO monthly.services ("name") VALUES (%s);
                END IF;
            END
            $$ """
            record_to_insert = (str(row['service_name']).strip(),str(row['service_name']).strip())
            cursor.execute(postgres_insert_query, record_to_insert)
            postgres_insert_query2= """
             DO 
                $$
                BEGIN
                    IF NOT EXISTS (select from monthly."monthlyService" where "report_id"=%s and "service"=(Select "ID" from monthly.services where "name"=%s)) THEN
                         INSERT INTO monthly."monthlyService"(
                         firm, period, "toDate", service,
                         desc_modification, "desc_toDate", desc_title,desc_month,
                         "lastAnnouncment", "nextAnnouncment", report_id,
                         reported_firm, contract_date, contract_duration,
                         totalyear_tostartofperiod, modif_tostartofperiod,
                         totalyear_tostartofperiod_modified, revenueperiod,
                         totalyear_toendofperiod, totallastyear, itemdesc,
                         prediction_revenue_totalyear, prediction_costofcontract_totalyear)
                        VALUES ( (SELECT "ID" from "Entity" where "ticker"= %s ),  %s,  %s,  (Select "ID" from monthly.services where "name"=%s), 
                        %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s,  %s);
                    END IF;
                END
                $$ 

            """

            record_to_insert2 = (int(CID),str(row['service_name']).strip(),
                                titles[0],titles[1],titles[2],
                                 str(row['service_name']).strip()
                                 ,alldesc['desc_modif']
                                 ,alldesc['desc_year']
                                 ,alldesc['desc_title']
                                 ,alldesc['desc_month']
                                 ,Announcment['Last'],Announcment['Next'],int(CID),titles[3],
                                 row["contract_date"], row["contract_duration"], row["totalyear_tostartofperiod"], 
                                 row["modif_tostartofperiod"], 
                                 row["totalyear_tostartofperiod_modified"], row["revenueperiod"], 
                                 row["totalyear_toendofperiod"], row["totallastyear"], row["itemdesc"],
                                 row["prediction_revenue_totalyear"], row["prediction_costofcontract_totalyear"]
                                )

            #print('GoodsDone')
            cursor.execute(postgres_insert_query2, record_to_insert2)
            #print(record_to_insert2)
            counter=counter+1
        postgres_insert_query3 = """
        INSERT INTO codalraw."MonthlyConverted"(
        "report_ID", converted)
        VALUES (%s, True);
        """
        record_to_insert3=([CID])
        cursor.execute(postgres_insert_query3, record_to_insert3)
        connection.commit()
        print(str(Clink)+'  '+'--Done')
    except(Exception, psycopg2.Error) as error:
        if(connection):
            print("Failed to insert record", error)
            log_it("insertService Failed "+str(CID))
            print(str(CodalRaw_links))
    finally:
        if(connection):
            cursor.close()
            connection.close()  
def insertInvest(titles,alldesc,DF_Invests,CID,Clink):
    try:
        connection = psycopg2.connect(user=db_username,
                                  password=db_pass,
                                  host=db_host,
                                  port=db_port,
                                  database=db_database)

        cursor = connection.cursor()
        counter=0

        if not DF_Invests[5].empty:
            if (not np.isnan(DF_Invests[5].company.tolist()[0])):
                for index,row in DF_Invests[5].iterrows():
                    postgres_insert_Q= """
                     DO 
                        $$
                        BEGIN
                            IF NOT EXISTS (select from monthly."monthlyInvestment_DPSIncome" where "report_id"=%s and "company"=%s) THEN
                                  INSERT INTO monthly."monthlyInvestment_DPSIncome"(
                                    firm, period, "toDate", desc_title, report_id, reported_firm,
                                    company, "company_endOfyear", "assemblyDate", "stockCount_inAssembly",
                                    capital, "nominalValue", "Ownership", "EPS", "DPS", "IncomeThisYear",
                                    "IncomeLastYear", perv_total_income, total_income_aggregated)
                                    VALUES ((SELECT "ID" from "Entity" where "ticker"= %s ), %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                            END IF;
                        END
                        $$ 

                    """

                    record_to_I= (int(CID),row["company"],titles[0],titles[1],titles[2],''
                                         ,int(CID),titles[3],row["company"],
                                         row["portfo_endoftheyear_date"], row["Assembly_date"], row["stockCount_inAssembly"], 
                                         row["Capital"], row["NominalValue"], row["Ownership"], 
                                         row["EPS"], row["DPS"], row["Incomethisyear"], 
                                         row["IncomelastYear"], row["prev_total_income"], row["total_income_thisyear"]
                                        )
                    cursor.execute(postgres_insert_Q, record_to_I)
                    counter=counter+1



        for index,row in DF_Invests[4].iterrows():
            postgres_insert_query2= """
             DO 
                $$
                BEGIN
                    IF NOT EXISTS (select from monthly."monthlyInvestment_Summary" where "report_id"=%s and "industry"=%s) THEN
                          INSERT INTO monthly."monthlyInvestment_Summary"(firm, period, "toDate",reported_firm, report_id, industry, desc_title,
                            "public_start_companyCount", public_start_cost, "public_start_marketValue", 
                            public_changes_cost, "public_changes_marketValue", "public_end_companyCount", public_end_cost, 
                            "public_end_marketValue", "private_start_companyCount", private_start_cost, private_changes_cost,
                            "private_end_companyCount", private_end_cost, investment_deficit)
                            VALUES ((SELECT "ID" from "Entity" where "ticker"= %s ), %s, %s, %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    END IF;
                END
                $$ 

            """
    #             postgres_insert_query2="""
    #             INSERT INTO public."monthlyInvestment_Summary"(firm, period, "toDate",reported_firm, report_id, industry, desc_title,
    #             "public_start_companyCount", public_start_cost, "public_start_marketValue", 
    #             public_changes_cost, "public_changes_marketValue", "public_end_companyCount", public_end_cost, 
    #             "public_end_marketValue", "private_start_companyCount", private_start_cost, private_changes_cost,
    #             "private_end_companyCount", private_end_cost, investment_deficit)
    #             VALUES ((SELECT "ID" from "Entity" where "ticker"= %s ), %s, %s, %s, %s, %s, %s, %s, 
    #             %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    #             """
            record_to_insert2 = (int(CID),row["company"],titles[0],titles[1],titles[2],titles[3]
             ,int(CID),row["company"],alldesc['desc'],
             row["public_start_companyCount"], row["public_start_cost"], row["public_start_marketValue"], 
             row["public_changes_cost"], row["public_changes_marketValue"], row["public_end_companyCount"], 
             row["public_end_cost"], row["public_end_marketValue"], row["private_start_companyCount"], 
             row["private_start_cost"], row["private_changes_cost"], row["private_end_companyCount"], 
             row["private_end_cost"],alldesc['deficit'])
            cursor.execute(postgres_insert_query2, record_to_insert2)
            counter=counter+1



        for index,row in DF_Invests[3].iterrows():
            postgres_insert_query3= """
            DO 
            $$
                BEGIN
                        IF NOT EXISTS (select from monthly."monthlyInvestment_Portfolio" where "report_id"=%s and "company"=%s) THEN
                            INSERT INTO monthly."monthlyInvestment_Portfolio"(
                            firm, period, "toDate",reported_firm, report_id, "typeOfCompany", company, "companyCapital",
                            "companyshare_NominalValue", "start_companyShare", start_cost, "start_sharesMarketValue", 
                            "changes_companyShare", changes_cost, "changes_sharesMarketValue", "end_ownPercentage", "end_costperShare", 
                            "end_costTotal", "end_MarketValue", "end_valueperShare", "end_TotalChange")
                            VALUES ((SELECT "ID" from "Entity" where "ticker"= %s ), %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    END IF;
                END
            $$ 

            """
            #             postgres_insert_query3="""
            #             INSERT INTO public."monthlyInvestment_Portfolio"(
            #             firm, period, "toDate",reported_firm, report_id, "typeOfCompany", company, "companyCapital",
            #             "companyshare_NominalValue", "start_companyShare", start_cost, "start_sharesMarketValue", 
            #             "changes_companyShare", changes_cost, "changes_sharesMarketValue", "end_ownPercentage", "end_costperShare", 
            #             "end_costTotal", "end_MarketValue", "end_valueperShare", "end_TotalChange")
            #             VALUES ((SELECT "ID" from "Entity" where "ticker"= %s ), %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            #             """
            record_to_insert3 = (int(CID),row['company'],titles[0],titles[1],titles[2],titles[3],int(CID),row["Type"],row['company'],row["companyCapital"], row["NominalshareValue"], row["start_shareCount"], 
             row["start_cost"], row["start_marketValue"], row["change_shareCount"], 
             row["Change_cost"], row["Change_marketValue"], row["end_own"], 
             row["end_costShare"], row["end_cost"], row["end_marketValue"], 
             row["end_shareValue"], row["end_change"]
            )
            cursor.execute(postgres_insert_query3, record_to_insert3)
            counter=counter+1


        for index,row in DF_Invests[2].iterrows():
    #             postgres_insert_query4="""
    #             INSERT INTO public."monthlyInvestment_Portfolio"(
    #             firm, period, "toDate",reported_firm, report_id, "typeOfCompany", company, "companyCapital",
    #             "companyshare_NominalValue", "start_companyShare", start_cost,
    #             "changes_companyShare", changes_cost, "end_ownPercentage", "end_costperShare", 
    #             "end_costTotal")
    #             VALUES ((SELECT "ID" from "Entity" where "ticker"= %s ), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);

    #             """
            postgres_insert_query4= """
             DO 
                $$
                    BEGIN
                        IF NOT EXISTS (select from monthly."monthlyInvestment_Portfolio" where "report_id"=%s and "company"=%s) THEN
                               INSERT INTO monthly."monthlyInvestment_Portfolio"(
                                firm, period, "toDate",reported_firm, report_id, "typeOfCompany", company, "companyCapital",
                                "companyshare_NominalValue", "start_companyShare", start_cost,
                                "changes_companyShare", changes_cost, "end_ownPercentage", "end_costperShare", 
                                "end_costTotal")
                                VALUES ((SELECT "ID" from "Entity" where "ticker"= %s ), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                        END IF;
                    END
                $$ 

            """
            record_to_insert4 = (int(CID),row['company'],titles[0],titles[1],titles[2],titles[3]
                                 ,int(CID),row["Type"],row['company'],
                                 row["companyCapital"], row["NominalshareValue"], row["start_shareCount"], 
                                 row['start_cost'],
                                 row["change_shareCount"], 
                                 row["Change_cost"],float(row["end_own"]), 
                                 row["end_costShare"], row["end_cost"]
                                )
            cursor.execute(postgres_insert_query4, record_to_insert4)
            counter=counter+1 


        for index,row in DF_Invests[1].iterrows():
        #             postgres_insert_query5="""
        #             INSERT INTO public."monthlyInvestment_In_Transactions"(
        #             firm, period, "toDate",reported_firm, report_id, in_firm,
        #             "in_shareCount", "in_shareCost", "in_TotalpublicCost", "in_TotalOtcCost")
        #             VALUES ( (SELECT "ID" from "Entity" where "ticker"= %s ), %s, %s, %s, %s, %s, %s, %s, %s, %s);

        #             """
            postgres_insert_query5= """
            DO 
            $$
                BEGIN
                    IF NOT EXISTS (select from monthly."monthlyInvestment_In_Transactions" where "report_id"=%s and "in_firm"=%s) THEN
                        INSERT INTO monthly."monthlyInvestment_In_Transactions"(
                        firm, period, "toDate",reported_firm, report_id, in_firm,
                        "in_shareCount", "in_shareCost", "in_TotalpublicCost", "in_TotalOtcCost")
                        VALUES ( (SELECT "ID" from "Entity" where "ticker"= %s ), %s, %s, %s, %s, %s, %s, %s, %s, %s);

                    END IF;
                END
            $$ 

            """
            record_to_insert5 = (int(CID)
            ,row["in_firm"],titles[0],titles[1],titles[2],titles[3]
            ,int(CID)
            ,row["in_firm"],row['in_shareCount'],
            row["in_shareCost"], row["in_TotalpublicCost"], row["in_TotalOtcCost"]
            )
            cursor.execute(postgres_insert_query5, record_to_insert5)
            counter=counter+1   

        for index,row in DF_Invests[0].iterrows():
            #             postgres_insert_query="""
            #             INSERT INTO public."monthlyInvestment_Out_Transactions"(firm, period, "toDate",reported_firm, report_id, out_firm, 
            #             "out_shareCount", "out_shareCost", "out_TotalCost", "out_shareSellValue", "out_TotalSellValue", "Out_net")
            #             VALUES ( (SELECT "ID" from "Entity" where "ticker"= %s ), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);

            #             """
            postgres_insert_query= """
            DO 
            $$
                BEGIN
                    IF NOT EXISTS (select from monthly."monthlyInvestment_Out_Transactions" where "report_id"=%s and "out_firm"=%s) THEN
                        INSERT INTO monthly."monthlyInvestment_Out_Transactions"(firm, period, "toDate",reported_firm, report_id, out_firm, 
                        "out_shareCount", "out_shareCost", "out_TotalCost", "out_shareSellValue", "out_TotalSellValue", "Out_net")
                        VALUES ( (SELECT "ID" from "Entity" where "ticker"= %s ), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    END IF;
                END
            $$ 

            """
            record_to_insert = [int(CID)
            ,row["out_firm"],
            titles[0],titles[1],titles[2],titles[3]
            ,int(CID)
            ,row["out_firm"],row['out_shareCount']
            ,row["out_shareCost"], row["out_TotalCost"]
            ,row["out_shareSellValue"]
            ,row["out_TotalSellValue"],row["Out_net"]
            ]
            #print(record_to_insert)
            cursor.execute(postgres_insert_query, record_to_insert)
            counter=counter+1


        postgres_insert_query3 = """
        INSERT INTO codalraw."MonthlyConverted"(
        "report_ID", converted)
        VALUES (%s, True);
        """
        record_to_insert3=([CID])
        cursor.execute(postgres_insert_query3, record_to_insert3)
        connection.commit()
        print(str(Clink)+'  '+'--Done')
    except(Exception, psycopg2.Error) as error:
                if(connection):
                    print("Failed to insert record", error)
                    log_it("InserInvest Failed "+str(CID))
                    print(str(Clink))
    finally:
        if(connection):
            cursor.close()
            connection.close()
def Insert_construction(desc,titles,announcments,CID,Clink,DFResults):
    df1=DFResults['SoldProjects']
    descdf=pd.DataFrame([desc])
    titlesdf=pd.DataFrame([titles])
    titlesdf.columns=['nemad','period','toDate','firm_reporting']
    announcedf=pd.DataFrame([announcments])
    df1['tmp'] = 1
    descdf['tmp'] = 1
    dftemp1 = pd.merge(df1, descdf, on=['tmp'])
    dftemp1 = dftemp1.drop('tmp', axis=1)
    dftemp1['tmp'] = 1
    titlesdf['tmp'] = 1
    dftemp2= pd.merge(dftemp1, titlesdf, on=['tmp'])
    dftemp3 = dftemp2.drop('tmp', axis=1)
    dftemp3['tmp'] = 1
    announcedf['tmp'] = 1
    dftemp4= pd.merge(dftemp3, announcedf, on=['tmp'])
    dftemp4 = dftemp4.drop('tmp', axis=1)
    dftemp4['report_id']=CID
   
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
                IF NOT EXISTS (select from monthly."Construction_Monthly_Sold" where "report_id"=%(report_id)s and "projectName"=%(ProjectName)s) THEN
                    INSERT INTO monthly."Construction_Monthly_Sold"(report_id, reported_firm, period, "toDate", 
                    desc_modification, "desc_onePeriod", "desc_toDate", 
                    desc_title, "lastAnnouncement", "nextAnnoucement",
                    "projectName", "Location", "typeOfProject", "projectUnit", 
                    "thisMonth_Cost", "thisMonth_MeterSold", "thisMonth_SaleRate", 
                    "thisMonth_SaleAmount", "FromBefore_Cost", "FromBefore_Revenue",
                    "TotalYear_Cost", "TotalYear_MeterSold", "TotalYear_SaleRate",
                    "TotalYear_SaleAmount", firm)
                    VALUES ( %(report_id)s, %(firm_reporting)s, %(period)s, %(toDate)s,
                    %(desc_modif)s, %(desc_month)s, %(desc_year)s, %(desc_title)s,
                    %(Last)s, %(Next)s, %(ProjectName)s, %(Location)s,
                    %(TypeofProject)s, %(Unit)s, %(thisMonth_Cost)s,
                    %(MeterSold)s, %(SaleRate)s, %(SaleAmount)s, %(FromBefore_Cost)s,
                    %(FromBefore_Revenue)s, %(TotalYear_Cost)s, %(TotalYear_MeterSold)s, %(TotalYear_SaleRate)s,
                    %(TotalYear_SaleAmount)s, (select "ID" from "Entity" where ticker=%(nemad)s)
                    );
                END IF;
            END
            $$ 
        
        """

        cursor.executemany(postgres_insert_query,dftemp4.to_dict(orient='records'))
        connection.commit()
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print("Failed to InsertConstruction", error)
                log_it('Failed to Insert Construction -'+str(CID))
    finally:
        if(connection):
            cursor.close()
            connection.close()
    df1=DFResults['Progress']
    descdf=pd.DataFrame([desc])
    titlesdf=pd.DataFrame([titles])
    titlesdf.columns=['nemad','period','toDate','firm_reporting']
    announcedf=pd.DataFrame([announcments])
    df1['tmp'] = 1
    descdf['tmp'] = 1
    dftemp1 = pd.merge(df1, descdf, on=['tmp'])
    dftemp1 = dftemp1.drop('tmp', axis=1)
    dftemp1['tmp'] = 1
    titlesdf['tmp'] = 1
    dftemp2= pd.merge(dftemp1, titlesdf, on=['tmp'])
    dftemp3 = dftemp2.drop('tmp', axis=1)
    dftemp3['tmp'] = 1
    announcedf['tmp'] = 1
    dftemp4= pd.merge(dftemp3, announcedf, on=['tmp'])
    dftemp4_2 = dftemp4.drop('tmp', axis=1)
    dftemp4_2['report_id']=CID
    try:

        connection = psycopg2.connect(user=db_username,
                                      password=db_pass,
                                      host=db_host,
                                      port=db_port,
                                      database=db_database)
        cursor = connection.cursor()
        postgres_insert_query2 = """
        DO 
                $$
                BEGIN
                    IF NOT EXISTS (select from monthly."Construction_Monthly_Ongoing" where "report_id"=%(report_id)s and "projectName"=%(ProjectName)s ) THEN
                        INSERT INTO monthly."Construction_Monthly_Ongoing"(
                        firm, report_id, period, "toDate", desc_modification,
                        "desc_onePeriod", "desc_toDate", desc_title, "lastAnnouncement",
                        "nextAnnouncement", reported_firm, "projectName", "Location", 
                        "typeOfProject", "Ownership", "projectUnit", "NetMeter", 
                        "lastMonth_physicalProgress", "lastMonth_Cost", "lastMonth_EstimationOfRemainingCost", 
                        "lastMonth_EstmiatedTotalCost", "SoldProjectsDuringMonth_meter", "SoldProjectsDuringMonth_cost",
                        "thisMonth_physicalProgress", "thisMonth_cost", "thisMonth_EstimationRemainingCost", 
                        "thisMonth_EsitmatedTotalCost")
                        VALUES ((select "ID" from "Entity" where ticker=%(nemad)s), %(report_id)s, %(period)s, %(toDate)s,
                        %(desc_modif)s, %(desc_month)s, %(desc_year)s
                        , %(desc_title)s, %(Last)s, %(Next)s, %(firm_reporting)s,
                        %(ProjectName)s, %(Location)s, %(TypeofProject)s
                        , %(Ownership)s, %(Unit)s, %(NetMeter)s, %(lastMonth_physicalProgress)s, 
                        %(lastMonth_Cost)s, %(lastMonth_EstimationOfRemainingCost)s, %(lastMonth_EstmiatedTotalCost)s
                        , %(SoldProjectsDuringMonth_meter)s, %(SoldProjectsDuringMonth_cost)s, 
                        %(thisMonth_physicalProgress)s, %(thisMonth_cost)s, 
                        %(thisMonth_EstimationRemainingCost)s, %(thisMonth_EsitmatedTotalCost)s);
                    END IF;
                END
                $$ 

        """
        #print(dftemp4_2.columns)
        cursor.executemany(postgres_insert_query2,dftemp4_2.to_dict(orient='records'))
        connection.commit()
        postgres_insert_query3 = """
        INSERT INTO codalraw."MonthlyConverted"(
        "report_ID", converted)
        VALUES (%s, True);
        """
        record_to_insert3=([CID])
        cursor.execute(postgres_insert_query3, record_to_insert3)
        connection.commit()
        print(str(Clink)+'  '+'--Done')
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print("Failed to InsertConstruction", error)
                log_it('Failed to Insert Construction -'+str(CID))
    finally:
        if(connection):
            cursor.close()
            connection.close()
def Insert_Leasing(desc,titles,announcments,CID,Clink,DFResults):
    df1=DFResults['Revenue']
    descdf=pd.DataFrame([desc])
    titlesdf=pd.DataFrame([titles])
    titlesdf.columns=['nemad','period','toDate','firm_reporting']
    announcedf=pd.DataFrame([announcments])
    df1['tmp'] = 1
    descdf['tmp'] = 1
    dftemp1 = pd.merge(df1, descdf, on=['tmp'])
    dftemp1 = dftemp1.drop('tmp', axis=1)
    dftemp1['tmp'] = 1
    titlesdf['tmp'] = 1
    dftemp2= pd.merge(dftemp1, titlesdf, on=['tmp'])
    dftemp3 = dftemp2.drop('tmp', axis=1)
    dftemp3['tmp'] = 1
    announcedf['tmp'] = 1
    dftemp4= pd.merge(dftemp3, announcedf, on=['tmp'])
    dftemp4 = dftemp4.drop('tmp', axis=1)
    dftemp4['report_id']=CID
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
                IF NOT EXISTS (select from monthly."Leasing_Monthly_Revenue" where "report_id"=%(report_id)s and "itemTitle"=%(Title)s) THEN
                    INSERT INTO monthly."Leasing_Monthly_Revenue"(
                    firm, report_id, period, "ToDate", desc_modification,
                    "desc_onePeriod", "desc_toDate", desc_title, "lastAnnouncement", 
                    "nextAnnouncement", reported_firm, "itemTitle", "thisPeriodAmount",
                    "TotalAmount")
                    VALUES (  (select "ID" from "Entity" where ticker=%(nemad)s), %(report_id)s, %(period)s,
                    %(toDate)s, %(desc_modif)s, %(desc_month)s, %(desc_year)s,
                    %(desc_title)s, %(Last)s, %(Next)s, %(firm_reporting)s, 
                    %(Title)s, %(thisPeriod_Amount)s, %(Total_Amount)s);
                END IF;
            END
            $$ 
        
        """

        cursor.executemany(postgres_insert_query,dftemp4.to_dict(orient='records'))
        connection.commit()
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print("Failed to InsertLeasing_Revenue", error)
                log_it('Failed to Insert InsertLeasing_Revenue -'+str(CID))
    finally:
        if(connection):
            cursor.close()
            connection.close()
    df_2=DFResults['Costs']
    descdf=pd.DataFrame([desc])
    titlesdf=pd.DataFrame([titles])
    titlesdf.columns=['nemad','period','toDate','firm_reporting']
    announcedf=pd.DataFrame([announcments])
    df_2['tmp'] = 1
    descdf['tmp'] = 1
    df_2temp = pd.merge(df_2, descdf, on=['tmp'])
    df_2temp = df_2temp.drop('tmp', axis=1)
    df_2temp['tmp'] = 1
    titlesdf['tmp'] = 1
    df_3temp= pd.merge(df_2temp, titlesdf, on=['tmp'])
    df_3temp = df_3temp.drop('tmp', axis=1)
    df_3temp['tmp'] = 1
    announcedf['tmp'] = 1
    df_4temp= pd.merge(df_3temp, announcedf, on=['tmp'])
    df_4temp = df_4temp.drop('tmp', axis=1)
    df_4temp['report_id']=CID
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
                IF NOT EXISTS (select from monthly."Leasing_Monthly_Cost" where "report_id"=%(report_id)s and "itemTitle"=%(title)s) THEN
                  
                  INSERT INTO monthly."Leasing_Monthly_Cost"(
                firm, period, "toDate", report_id, 
                desc_modification, desc_title,
                "desc_toDate", "desc_onePeriod",
                "lastAnnouncement", "NextAnnouncement",
                reported_firm, "itemTitle", "thisPeriod_Achievedcost", 
                "Total_Achievedcost", "thisPeriod_TakenFacilityRemaind"
                , "Total_TakenFacilityRemained")
                VALUES ( (select "ID" from "Entity" where ticker=%(nemad)s), %(period)s, %(toDate)s, %(report_id)s,
                %(desc_modif)s, %(desc_title)s, %(desc_year)s, %(desc_month)s, %(Last)s, %(Next)s, 
                %(firm_reporting)s, %(title)s, %(thisPeriod_Achievedcost)s, %(Total_Achievedcost)s, %(thisPeriod_TakenFacilityRemaind)s, %(Total_TakenFacilityRemained)s);
                END IF;



            END
            $$ 
        
        """

        cursor.executemany(postgres_insert_query,df_4temp.to_dict(orient='records'))
        connection.commit()
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print("Failed to Insert Leasing_cost", error)
                log_it('Failed to Insert Leasing_cost -'+str(CID))
    finally:
        if(connection):
            cursor.close()
            connection.close()
    df__3=DFResults['Delegated']
    descdf=pd.DataFrame([desc])
    titlesdf=pd.DataFrame([titles])
    titlesdf.columns=['nemad','period','toDate','firm_reporting']
    announcedf=pd.DataFrame([announcments])
    df__3['tmp'] = 1
    descdf['tmp'] = 1
    df__3temp2 = pd.merge(df__3, descdf, on=['tmp'])
    df__3temp2 = df__3temp2.drop('tmp', axis=1)
    df__3temp2['tmp'] = 1
    titlesdf['tmp'] = 1
    df__3temp3= pd.merge(df__3temp2, titlesdf, on=['tmp'])
    df__3temp4 = df__3temp3.drop('tmp', axis=1)
    df__3temp4['tmp'] = 1
    announcedf['tmp'] = 1
    df__3temp5= pd.merge(df__3temp4, announcedf, on=['tmp'])
    df__3temp5 = df__3temp5.drop('tmp', axis=1)
    df__3temp5['report_id']=CID
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
                IF NOT EXISTS (select from monthly."Leasing_Monthly_DelegatedSummary" where "report_id"=%(report_id)s and "itemTitle"=%(title)s) THEN
                  
                  INSERT INTO monthly."Leasing_Monthly_DelegatedSummary"(
                    firm, report_id, period, "toDate", desc_modification,
                    "desc_onePeriod", "desc_toDate", desc_title, "lastAnnouncement", 
                    "nextAnnouncement", reported_firm, "itemTitle", "StartMonth_count",
                    "StartMonth_Amount", "thismonth_NewFacilityCount", "thismonth_NewFacilityAmount",
                    "thismonth_SetteledCount", "thismonth_SetteledAmount", "EndMonth_FacilityCount", 
                    "EndMonth_FacilityAmount")
                    
                    VALUES ((select "ID" from "Entity" where ticker=%(nemad)s), %(report_id)s, %(period)s, %(toDate)s,
                    %(desc_modif)s, %(desc_month)s, %(desc_year)s, 
                    %(desc_title)s, %(Last)s, %(Next)s,
                    %(firm_reporting)s, %(title)s, %(StartMonth_count)s,
                    %(StartMonth_Amount)s, %(thismonth_NewFacilityCount)s, %(thismonth_NewFacilityAmount)s,
                    %(thismonth_SetteledCount)s, %(thismonth_SetteledAmount)s, %(EndMonth_FacilityCount)s,%(EndMonth_FacilityAmount)s);
                END IF;



            END
            $$ 
        
        """

        cursor.executemany(postgres_insert_query,df__3temp5.to_dict(orient='records'))
        connection.commit()
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print("Failed to Insert Leasing_Delegated", error)
                log_it('Failed to Insert Leasing_Delegated -'+str(CID))
    finally:
        if(connection):
            cursor.close()
            connection.close()
    try:
        connection = psycopg2.connect(user=db_username,
                                      password=db_pass,
                                      host=db_host,
                                      port=db_port,
                                      database=db_database)
        cursor = connection.cursor()
        postgres_insert_query3 = """
        INSERT INTO codalraw."MonthlyConverted"(
        "report_ID", converted)
        VALUES (%s, True);
        """
        record_to_insert3=([CID])
        cursor.execute(postgres_insert_query3, record_to_insert3)
        connection.commit()
        print(str(Clink)+'  '+'--Done')
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print("Failed to Update MR", error)
                log_it('Failed to Update Leasing -'+str(CID))
    finally:
        if(connection):
            cursor.close()
            connection.close()
def Insert_Bank(desc,titles,announcments,CID,Clink,DFResults):
    df1=DFResults['Facilities']
    descdf=pd.DataFrame([desc])
    titlesdf=pd.DataFrame([titles])
    titlesdf.columns=['nemad','period','toDate','firm_reporting']
    announcedf=pd.DataFrame([announcments])
    df1['tmp'] = 1
    descdf['tmp'] = 1
    dftemp1 = pd.merge(df1, descdf, on=['tmp'])
    dftemp1 = dftemp1.drop('tmp', axis=1)
    dftemp1['tmp'] = 1
    titlesdf['tmp'] = 1
    dftemp2= pd.merge(dftemp1, titlesdf, on=['tmp'])
    dftemp3 = dftemp2.drop('tmp', axis=1)
    dftemp3['tmp'] = 1
    announcedf['tmp'] = 1
    dftemp4= pd.merge(dftemp3, announcedf, on=['tmp'])
    dftemp4 = dftemp4.drop('tmp', axis=1)
    dftemp4['report_id']=CID
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
                IF NOT EXISTS (select from monthly."Bank_Monthly_Facilities" where "report_id"=%(report_id)s and "title"=%(title)s) THEN
                    INSERT INTO monthly."Bank_Monthly_Facilities"(
                    firm, report_id, period, "toDate", desc_modification, "desc_onePeriod", "desc_toDate", 
                    desc_title, "lastAnnouncement", "nextAnnouncement", reported_firm, 
                    title, "StartPeriod_RemainingFacility","StartPeriod_RemainingFacility_modifications",
                    "StartPeriod_RemainingFacility_modified","thisPeriod_IssuedFacilitiy","thisPeriod_SetteledFacility",
                    "EnePeriod_RemainingFacility","PreviousPeriods_RevenueFromFacility","PreviousPeriods_RevenueFromFacility_modification",
                    "PreviousPeriods_RevenueFromFacility_modified", "thisPeriod_RevenueFromFacility","thisYear_RevenueFromFacility")
                    VALUES ((select "ID" from "Entity" where ticker=%(nemad)s), %(report_id)s, %(period)s, %(toDate)s,
                    %(desc_modif)s, %(desc_month)s, %(desc_year)s,
                    %(desc_title)s, %(Last)s, %(Next)s, %(firm_reporting)s,
                    %(title)s,%(StartPeriod_RemainingFacility)s,%(StartPeriod_RemainingFacility_modifications)s,
                    %(StartPeriod_RemainingFacility_modified)s,%(thisPeriod_IssuedFacilitiy)s,%(thisPeriod_SetteledFacility)s,
                    %(EnePeriod_RemainingFacility)s,%(PreviousPeriods_RevenueFromFacility)s,%(PreviousPeriods_RevenueFromFacility_modification)s,
                    %(PreviousPeriods_RevenueFromFacility_modified)s,%(thisPeriod_RevenueFromFacility)s,%(thisYear_RevenueFromFacility)s
                    );
                END IF;
            END
            $$ 

        """

        cursor.executemany(postgres_insert_query,dftemp4.to_dict(orient='records'))
        connection.commit()

    except(Exception, psycopg2.Error) as error:
            if(connection):
                print("Failed to Insert BankFacilities", error)
                log_it('Failed to Insert BankFacilities -'+str(CID))
    finally:
        if(connection):
            cursor.close()
            connection.close()
    ####
    dfdepos=DFResults['Deposits']
    descdf=pd.DataFrame([desc])
    titlesdf=pd.DataFrame([titles])
    titlesdf.columns=['nemad','period','toDate','firm_reporting']
    announcedf=pd.DataFrame([announcments])
    dfdepos['tmp'] = 1
    descdf['tmp'] = 1
    dfdepostemp1 = pd.merge(dfdepos, descdf, on=['tmp'])
    dfdepostemp1 = dfdepostemp1.drop('tmp', axis=1)
    dfdepostemp1['tmp'] = 1
    titlesdf['tmp'] = 1
    dfdepostemp2= pd.merge(dfdepostemp1, titlesdf, on=['tmp'])
    dfdepostemp2 = dfdepostemp2.drop('tmp', axis=1)
    dfdepostemp2['tmp'] = 1
    announcedf['tmp'] = 1
    dfdepostemp3= pd.merge(dfdepostemp2, announcedf, on=['tmp'])
    dfdepostemp3 = dfdepostemp3.drop('tmp', axis=1)
    dfdepostemp3['report_id']=CID
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
                IF NOT EXISTS (select from monthly."Bank_Monthly_Deposits" where "report_id"=%(report_id)s and "title"=%(title)s) THEN
                    INSERT INTO monthly."Bank_Monthly_Deposits"(
                    firm, report_id, period, "toDate",
                    desc_modification, "desc_onePeriod", 
                    "desc_toDate", desc_title, "lastAnnouncement", "nextAnnouncement",
                    reported_firm, title, "StartPeriod_Deposits", "thisPeriod_IncomingDeposit",
                    "thisPeriod_setteledDeposits", "EndPeriod_Deposits", "thisPeriod_InterestOnDeposits",
                    "Total_InterestOnDeposits", "StartPeriod_Deposits_modifications", "StartPeriod_Deposits_modified",
                    "PreviousPeriods_InterestOnDeposits", "PreviousPeriods_InterestOnDeposits_modifications", 
                    "PreviousPeriods_InterestOnDeposits_modified")
                    VALUES ((select "ID" from "Entity" where ticker=%(nemad)s), %(report_id)s, %(period)s, %(toDate)s,
                    %(desc_modif)s, %(desc_month)s, %(desc_year)s,
                    %(desc_title)s, %(Last)s, %(Next)s, %(firm_reporting)s,%(title)s
                    ,%(StartPeriod_Deposits)s,%(thisPeriod_IncomingDeposit)s,
                    %(thisPeriod_setteledDeposits)s,%(EndPeriod_Deposits)s,
                    %(thisPeriod_InterestOnDeposits)s,%(Total_InterestOnDeposits)s
                    ,%(StartPeriod_Deposits_modifications)s,%(StartPeriod_Deposits_modified)s,
                    %(PreviousPeriods_InterestOnDeposits)s,%(PreviousPeriods_InterestOnDeposits_modifications)s,
                    %(PreviousPeriods_InterestOnDeposits_modified)s);

                END IF;
            END
            $$ 

        """

        cursor.executemany(postgres_insert_query,dfdepostemp3.to_dict(orient='records'))
        connection.commit()

    except(Exception, psycopg2.Error) as error:
            if(connection):
                print("Failed to Insert BankDeposits", error)
                log_it('Failed to Insert BankDeposits -'+str(CID))
    finally:
        if(connection):
            cursor.close()
            connection.close()
    try:
        connection = psycopg2.connect(user=db_username,
                                      password=db_pass,
                                      host=db_host,
                                      port=db_port,
                                      database=db_database)
        cursor = connection.cursor()
        postgres_insert_query3 = """
        INSERT INTO codalraw."MonthlyConverted"(
        "report_ID", converted)
        VALUES (%s, True);
        """
        record_to_insert3=([CID])
        cursor.execute(postgres_insert_query3, record_to_insert3)
        connection.commit()
        print(str(Clink)+'  '+'--Done')
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print("Failed to Insert BankDeposits", error)
                log_it('Failed to Insert BankDeposits -'+str(CID))
    finally:
        if(connection):
            cursor.close()
            connection.close()     
def Insert_Insurance(desc,titles,announcments,CID,Clink,DFResults):
    df1=DFResults['Insurance']
    descdf=pd.DataFrame([desc])
    titlesdf=pd.DataFrame([titles])
    titlesdf.columns=['nemad','period','toDate','firm_reporting']
    announcedf=pd.DataFrame([announcments])
    df1['tmp'] = 1
    descdf['tmp'] = 1
    dftemp1 = pd.merge(df1, descdf, on=['tmp'])
    dftemp1 = dftemp1.drop('tmp', axis=1)
    dftemp1['tmp'] = 1
    titlesdf['tmp'] = 1
    dftemp2= pd.merge(dftemp1, titlesdf, on=['tmp'])
    dftemp3 = dftemp2.drop('tmp', axis=1)
    dftemp3['tmp'] = 1
    announcedf['tmp'] = 1
    dftemp4= pd.merge(dftemp3, announcedf, on=['tmp'])
    dftemp4 = dftemp4.drop('tmp', axis=1)
    dftemp4['report_id']=CID
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
                IF NOT EXISTS (select from monthly."Insurance_Monthly" where "report_id"=%(report_id)s and "InsuranceField"=%(InsuranceField)s) THEN
                    INSERT INTO monthly."Insurance_Monthly"(
                    firm, report_id, reported_firm, period, "toDate",
                    desc_modification, "desc_onePeriod", "desc_toDate",
                    desc_title, "lastAnnouncement", "NextAnnouncement", 
                    "InsuranceField", "PreviousPeriods_IssuedInsurance_Amount",
                    "PreviousPeriods_IssuedInusrance_Share", "PreviousPeriods_DamageRepaid_Amount", 
                    "PreviousPeriods_DamageRepaid_Share", "Modification_IssuedInsurance_Amount", 
                    "Modification_DamageRepaid_Amount", "PreviousModified_IssuedInsurance_Amount", 
                    "PreviousModified_IssuedInusrance_Share", "PreviousModified_DamageRepaid_Amount",
                    "PreviousModified_DamageRepaid_Share", "ThisPeriod_IssuedInsurance_Amount",
                    "ThisPeriod_IssuedInusrance_Share", "ThisPeriod_DamageRepaid_Amount",
                    "ThisPeriod_DamageRepaid_Share", "Total_IssuedInsurance_Amount", 
                    "Total_IssuedInusrance_Share", "Total_DamageRepaid_Amount", "Total_DamageRepaid_Share")
                    VALUES ((select "ID" from "Entity" where ticker=%(nemad)s), %(report_id)s, %(firm_reporting)s,
                    %(period)s, %(toDate)s,
                    %(desc_modif)s, %(desc_month)s, %(desc_year)s,
                    %(desc_title)s, %(Last)s, %(Next)s, 
                    %(InsuranceField)s,%(PreviousPeriods_IssuedInsurance_Amount)s,
                    %(PreviousPeriods_IssuedInusrance_Share)s,%(PreviousPeriods_DamageRepaid_Amount)s,
                    %(PreviousPeriods_DamageRepaid_Share)s,%(Modification_IssuedInsurance_Amount)s,
                    %(Modification_DamageRepaid_Amount)s,%(PreviousModified_IssuedInsurance_Amount)s,
                    %(PreviousModified_IssuedInusrance_Share)s,%(PreviousModified_DamageRepaid_Amount)s,
                    %(PreviousModified_DamageRepaid_Share)s,%(ThisPeriod_IssuedInsurance_Amount)s,
                    %(ThisPeriod_IssuedInusrance_Share)s,%(ThisPeriod_DamageRepaid_Amount)s,%(ThisPeriod_DamageRepaid_Share)s,
                    %(Total_IssuedInsurance_Amount)s,%(Total_IssuedInusrance_Share)s,
                    %(Total_DamageRepaid_Amount)s,%(Total_DamageRepaid_Share)s);
                END IF;
            END
            $$ 

        """

        cursor.executemany(postgres_insert_query,dftemp4.to_dict(orient='records'))
        connection.commit()
        postgres_insert_query3 = """
        INSERT INTO codalraw."MonthlyConverted"(
        "report_ID", converted)
        VALUES (%s, True);
        """
        record_to_insert3=([CID])
        cursor.execute(postgres_insert_query3, record_to_insert3)
        connection.commit()
        print(str(Clink)+'  '+'--Done')
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print("Failed to Insert Insurance", error)
                log_it('Failed to Insert Insurance -'+str(CID))
    finally:
        if(connection):
            cursor.close()
            connection.close()    
def InsertProductNG_ultimate(DF_Prod,CID,Clink):    
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
            IF NOT EXISTS (select from monthly."monthlyProduction" where ("report_id"=%(report_id)s and "good"=(select "ID" from monthly.goods where name=%(good)s) and category=%(categoryName)s))THEN
                INSERT INTO monthly."monthlyProduction"(
                firm, period, "toDate", good, desc_modification,
                "desc_onePeriod", "desc_toDate", desc_title, 
                "lastAnnouncment", "totalProductionPeriod", 
                "totalSalePeriod", "saleRatePeriod", "saleAmountPeriod",
                "totalProductionYear", "totalSaleYear", "saleRateYear", 
                "saleAmountYear", "prevTotalProductionYear", "prevTotalSalesYear",
                "prevTotalSalesRateYear", "prevTotalSalesAmountYear", "modification_Production", 
                "modification_Sales", "modification_SalesAmount", "prev_modified_TotalProduction",
                "prev_modified_TotalSalesRate", "prev_modified_TotalSalesAmount", "prev_modified_TotalSales",
                report_id, "nextAnnouncement", reported_firm, "lastyear_Production", "lastyear_saleCount", 
                "lastyear_saleAmount", "lastyear_saleRate", category, status)
                VALUES ((select "ID" from "Entity" where ticker=%(nemad)s), %(period)s, %(toDate)s, (select "ID" from monthly.goods where name=%(good)s),
                %(desc_modif)s, %(desc_month)s, %(desc_year)s, %(desc_title)s, %(Last)s, %(period_production)s, 
                %(period_sale_count)s, %(period_sale_rate)s, %(period_sale_amount)s, %(total_production)s, %(total_sale_count)s, %(total_sale_rate)s, 
                %(total_sale_amount)s, %(prev_production)s, %(prev_sale_count)s, %(prev_sale_rate)s, %(prev_sale_amount)s, %(modif_production)s, 
                %(modif_salecount)s, %(modif_saleamount)s, %(prev_modified_production)s, %(prev_modified_sale_rate)s, %(prev_modified_sale_amount)s, %(prev_modified_sale_count)s,
                %(report_id)s, %(Next)s, %(firm_reporting)s, %(lastYear_production)s, %(lastYear_sale_count)s, %(lastYear_sale_amount)s, 
                %(lastYear_sale_rate)s, %(categoryName)s, %(status)s);
            END IF;
        END
        $$ 

    """
    cursor.executemany(postgres_insert_query,DF_Prod.to_dict(orient='records'))
    connection.commit()
    postgres_insert_query3 = """
    INSERT INTO codalraw."MonthlyConverted"(
    "report_ID", converted)
    VALUES (%s, True);
    """
    record_to_insert3=([CID])
    cursor.execute(postgres_insert_query3, record_to_insert3)
    connection.commit()
    print(str(Clink)+'  '+'--Done')  
def InsertOther(CID,Clink):    
    connection = psycopg2.connect(user=db_username,
                                  password=db_pass,
                                  host=db_host,
                                  port=db_port,
                                  database=db_database)
    cursor = connection.cursor()
    postgres_insert_query3 = """
    INSERT INTO codalraw."MonthlyConverted"(
    "report_ID",converted, error)
    VALUES (%s,False, True);
    """
    record_to_insert3=([CID])
    cursor.execute(postgres_insert_query3, record_to_insert3)
    connection.commit()
    print(str(Clink)+'  '+'--Done')      
def RUN(driver,df):
#     driver = webdriver.Chrome()
#     driver.maximize_window()                 
#     df=get_unconverted().head(5)
    AllData=len(df.index)
    counter=0
    for index,row in df.iterrows():
        try:
            CodalRaw_ID=int(row['TracingNo'])
            CodalRaw_links=row['HtmlUrl']
            #print(CodalRaw_ID)
            driver.get('http://codal.ir'+CodalRaw_links)
            time.sleep(3)
            driver.execute_script("document.body.style.zoom='100%';document.body.style.zoom='50%'")
            titles=get_titlebox(driver)
            log_it('checked'+str(CodalRaw_ID))
            Type=check_type(driver)
            print(Type)
            if(Type[0]=='Construction'):
                Insert_construction(get_description_product_service(driver),titles,get_announcments(driver),CodalRaw_ID,CodalRaw_links,Constructions_results(driver))
            if(Type[0]=='Leasing'):
                Insert_Leasing(get_description_product_service(driver),titles,get_announcments(driver),CodalRaw_ID,CodalRaw_links,Leasing_results(driver))   
            if(Type[0]=='Insurance'):
                Insert_Insurance(get_description_product_service(driver),titles,get_announcments(driver),CodalRaw_ID,CodalRaw_links,Inusrance_results(driver))   
            if(Type[0]=='Bank'):
                Insert_Bank(get_description_product_service(driver),titles,get_announcments(driver),CodalRaw_ID,CodalRaw_links,Bank_results(driver))   
            if(Type[0]=='Product'):
                #titles
                alldesc=get_description_product_service(driver)
                Announcment=get_announcments(driver)
                DF_Prod=get_data_product(Type[1],driver)
                insertProduct(titles,Announcment,alldesc,DF_Prod,CodalRaw_ID,CodalRaw_links)
            if(Type[0]=='Service'):
                alldesc=get_description_product_service(driver)
                Announcment=get_announcments(driver)
                DF_serv=get_data_service(driver) 
                insertService(titles,Announcment,alldesc,DF_serv,CodalRaw_ID,CodalRaw_links)
            if(Type[0]=='Investment'):
                alldesc=get_invest_desc(driver)
                Announcment=get_announcments(driver)
                DF_Invests=get_data_investment(driver)
                insertInvest(titles,alldesc,DF_Invests,CodalRaw_ID,CodalRaw_links)
            if(Type[0]=='NewProduct'):
                alldesc=ng_product_desc(driver)
                Announcment=get_announcments(driver)
                DF_Prod=NG_product_ultimate(titles,Announcment,alldesc,CodalRaw_ID,CodalRaw_links,driver)
                InsertProductNG_ultimate(DF_Prod,CodalRaw_ID,CodalRaw_links)
                #insertProduct(titles,Announcment,alldesc,DF_Prod,CodalRaw_ID,CodalRaw_links) 
            if(Type[0]=="Other"):
                InsertOther(CodalRaw_ID,CodalRaw_links)
            counter=counter+1
            percentage=(counter*100)/AllData
            print("{0:.2f}".format(percentage))
        except (Exception, psycopg2.Error) as error :
            print(error)
            print(CodalRaw_links)
            continue
    #driver.quit()    
def scrape(df, *, loop):
    loop.run_in_executor(executor, scraper, df)
def scraper(df):
    chrome_options = Options()  
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(chrome_options=chrome_options)
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
chunks=split_dataframe(get_unconverted().head(50))
for df in chunks:
    scrape(df, loop=loop)
loop.run_until_complete(asyncio.gather(*asyncio.all_tasks(loop)))