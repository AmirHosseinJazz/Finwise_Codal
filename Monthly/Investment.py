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
db_database="FinWisev12"
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
            WHERE P."Type"='Investment' and P.converted=False """

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
def get_data_investment(driver):
    results=[]
    try:
        select = Select(driver.find_element_by_id('ddlTable'))
    except:
        select = Select(driver.find_element_by_id('ctl00_ddlTable'))
    select.select_by_visible_text('صورت ريزمعاملات سهام - واگذار شده')
    time.sleep(2)
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
    try:
        select = Select(driver.find_element_by_id('ddlTable'))
    except:
        select = Select(driver.find_element_by_id('ctl00_ddlTable'))
    select.select_by_visible_text('صورت ريزمعاملات سهام - تحصیل شده')
    time.sleep(2)
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
    try:
        select = Select(driver.find_element_by_id('ddlTable'))
    except:
        select = Select(driver.find_element_by_id('ctl00_ddlTable'))
    select.select_by_visible_text('صورت وضعیت پورتفوی شرکتهای خارج از بورس')
    time.sleep(2)
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
    try:
        select = Select(driver.find_element_by_id('ddlTable'))
    except:
        select = Select(driver.find_element_by_id('ctl00_ddlTable'))
    select.select_by_visible_text('صورت وضعیت پورتفوی شرکتهای پذیرفته شده در بورس')
    time.sleep(2)
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
    try:
        select = Select(driver.find_element_by_id('ddlTable'))
    except:
        select = Select(driver.find_element_by_id('ctl00_ddlTable'))
    select.select_by_visible_text('صورت خلاصه سرمایه گذاریها به تفکیک گروه صنعت')
    time.sleep(2)
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
    try:
        select = Select(driver.find_element_by_id('ddlTable'))
    except:
        select = Select(driver.find_element_by_id('ctl00_ddlTable'))
    if ('درآمد حاصل از سود سهام محقق شده') in select.options:
        select.select_by_visible_text('درآمد حاصل از سود سهام محقق شده')
        time.sleep(2)
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
                                    VALUES ((SELECT "ID" from "Publishers" where "persianName"= %s ), %s, %s, %s,
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
                            VALUES ((SELECT "ID" from "Publishers" where "persianName"= %s ), %s, %s, %s, %s, %s, %s, %s, 
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
    #             VALUES ((SELECT "ID" from firm where "ticker"= %s ), %s, %s, %s, %s, %s, %s, %s, 
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
                            VALUES ((SELECT "ID" from "Publishers" where "persianName"= %s ), %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
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
            #             VALUES ((SELECT "ID" from firm where "ticker"= %s ), %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
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
    #             VALUES ((SELECT "ID" from firm where "ticker"= %s ), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);

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
                                VALUES ((SELECT "ID" from "Publishers" where "persianName"= %s ), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
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
        #             VALUES ( (SELECT "ID" from firm where "ticker"= %s ), %s, %s, %s, %s, %s, %s, %s, %s, %s);

        #             """
            postgres_insert_query5= """
            DO 
            $$
                BEGIN
                    IF NOT EXISTS (select from monthly."monthlyInvestment_In_Transactions" where "report_id"=%s and "in_firm"=%s) THEN
                        INSERT INTO monthly."monthlyInvestment_In_Transactions"(
                        firm, period, "toDate",reported_firm, report_id, in_firm,
                        "in_shareCount", "in_shareCost", "in_TotalpublicCost", "in_TotalOtcCost")
                        VALUES ( (SELECT "ID" from "Publishers" where "persianName"= %s ), %s, %s, %s, %s, %s, %s, %s, %s, %s);

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
            #             VALUES ( (SELECT "ID" from firm where "ticker"= %s ), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);

            #             """
            postgres_insert_query= """
            DO 
            $$
                BEGIN
                    IF NOT EXISTS (select from monthly."monthlyInvestment_Out_Transactions" where "report_id"=%s and "out_firm"=%s) THEN
                        INSERT INTO monthly."monthlyInvestment_Out_Transactions"(firm, period, "toDate",reported_firm, report_id, out_firm, 
                        "out_shareCount", "out_shareCost", "out_TotalCost", "out_shareSellValue", "out_TotalSellValue", "Out_net")
                        VALUES ( (SELECT "ID" from "Publishers" where "persianName"= %s ), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
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
        UPDATE monthly."PreMonthly"
        SET converted=True
        WHERE "report_ID"=%s
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

def RUN(driver):
    driver = webdriver.Chrome()
    driver.maximize_window()                 
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
            driver.execute_script("document.body.style.zoom='100%';document.body.style.zoom='50%'")
            titles=get_titlebox(driver)
            alldesc=get_invest_desc(driver)
            Announcment=get_announcments(driver)
            DF_Invests=get_data_investment(driver)
            insertInvest(titles,alldesc,DF_Invests,CodalRaw_ID,CodalRaw_links)
            counter=counter+1
            percentage=(counter*100)/AllData
            print("{0:.2f}".format(percentage))
            
        except (Exception, psycopg2.Error) as error :
            print(error)
            print(CodalRaw_links)
            continue