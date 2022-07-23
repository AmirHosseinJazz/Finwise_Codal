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
    
        record_to_insert = (str(datetime.datetime.now()),text,'preparing Sheets')
        
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
            select * FROM codalraw."allrawReports" where "LetterCode"='ن-۱۰'
            and "HtmlUrl"!='' and "Available"=True and "TracingNo" not in (select "report_ID" from codalraw."SheetsConverted")
        """, connection)
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read links", error)
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
    if check_exists_by_xpath(driver,'//h2[text()="403 - Forbidden: Access is denied."]'):
        Error=True
    if '<head></head><body></body>' in str(driver.page_source):
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
def chunks(l, n):
    n = max(1, n)
    return (l[i:i+n] for i in range(0, len(l), n))  
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
def get_titleBox(driver):
    i=0
    results={}
    titlebox=driver.find_elements_by_xpath('//div[@class="symbol_and_name"]/div/div')
    for k in titlebox:
        if(i==0):
            header_name=(k.text.split(':\n')[1])
            header_name2 = ''.join([i for i in header_name if not i.isdigit()])
            header_name2 = header_name2.replace('ك','ک')
            header_name2 = header_name2.replace('ي','ی')
            header_name=header_name2
            results['firm']=header_name.strip()
        if(i==1):
            try:
                registered=(k.text.split(':\n')[1])
                registered2 = ''.join([i for i in registered if i.isdigit()])
                results['registered']=registered2
            except:
                results['registered']=k.text
        if(i==2):
            ticker=(k.text.split(':\n')[1])
            ticker2 = ''.join([i for i in ticker if not i.isdigit()])
            ticker2 = ticker2.replace('ك','ک')
            ticker2 = ticker2.replace('ي','ی')
            ticker=ticker2
            ticker3=ticker.split('(')[0]
            results['ticker']=ticker3
        if(i==3):
            try:
                unregistered=(k.text.split(':\n')[1])
                unregistered2 = ''.join([i for i in unregistered if i.isdigit()])
                results['unregistered']=unregistered2
            except:
                results['unregistered']=k.text
        if(i==4):
            try:
                industCode=(k.text.split(':\n')[1])
                industCode2 = ''.join([i for i in industCode if i.isdigit()])
                results['industryCode']=industCode2
            except:
                results['industryCode']=k.text
        if(i==5):
            try:
                status=(k.text.split('(')[-1]).split(')')[0]
                status2 = status.replace('ك','ک')
                status2 = status.replace('ي','ی')
                reprortTitle=k.text.split('(حسابرسی')[0]
                reportDate=(reprortTitle.split('منتهی به')[1]).strip()
                reportDatetemp=(reprortTitle.split('منتهی به')[0]).strip()
                header_PeriodLength=[int(s) for s in str.split(reportDatetemp) if s.isdigit()][0]
                results['status']=status2
                results['reportDate']=reportDate
                results['header_PeriodLength']=header_PeriodLength
            except:
                results['status']=k.text
                results['reportDate']=k.text
                results['header_PeriodLength']=k.text
        if(i==7):
            try:
                Ficsal=(k.text)
                results['FicsalYear']=Ficsal
            except:
                results['FicsalYear']=k.text
        if(i==9):
            try:
                publisherStatus=(k.text.split(':')[0])
                publisherStatus2 = ''.join([i for i in publisherStatus if not i.isdigit()])
                publisherStatus2 = publisherStatus2.replace('ك','ک')
                publisherStatus2 = publisherStatus2.replace('ي','ی')
                publisherStatus=publisherStatus2
                results['publisherStatus']=publisherStatus
            except:
                results['publisherStatus']=k.text
        i=i+1
    return results
def find_nth(haystack, needle, n):
    start = haystack.find(needle)
    while start >= 0 and n > 1:
        start = haystack.find(needle, start+len(needle))
        n -= 1
    return start
def insert_titlebox(titles):
    try:
        connection = psycopg2.connect(user=db_username,
                                      password=db_pass,
                                      host=db_host,
                                      port=db_port,
                                      database=db_database)
        cursor = connection.cursor()
        pouery = """
            DO 
            $$
               BEGIN
                    IF NOT EXISTS (select from  public."Publishers" where "persianName"=%(ticker)s) THEN
                        INSERT INTO public."Publishers"(
                        "persianName")
                        VALUES (%(ticker)s);
                    END IF;
                END
            $$ 

        """
        cursor.executemany(pouery,pd.DataFrame([titles]).to_dict(orient='records'))
        connection.commit()

        postgres_insert_query = """
         DO 
            $$
            BEGIN
                IF NOT EXISTS (select from statement."Titles" where "report_id"=%(report_id)s) THEN
                    INSERT INTO statement."Titles"(
                    report_id, reported_firm, "registeredCapital",
                    "unregisteredCapital", firm, "ISIC", period, "toDate", 
                    "reportStatus", "fiscalYear", "publisherStatus")
                    VALUES (%(report_id)s, %(firm)s, %(registered)s,
                    %(unregistered)s,(select "ID" from "Publishers" where "persianName"=%(ticker)s), %(industryCode)s, %(header_PeriodLength)s,
                    %(reportDate)s, %(status)s,
                    %(FicsalYear)s, %(publisherStatus)s);
                END IF;
            END
            $$ 

        """

        cursor.executemany(postgres_insert_query,pd.DataFrame([titles]).to_dict(orient='records'))
        connection.commit()
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print("Failed to Insert Titles", error)
                #log_it('Failed to Insert Titles -'+str(CID))
    finally:
        if(connection):
            cursor.close()
            connection.close()
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
    # print(driver.page_source)
    try:
        select = Select(driver.find_element_by_id('ddlTable'))
    except:
        select = Select(driver.find_element_by_id('ctl00_ddlTable'))
    for i in select.options:
        listOFOptions.append(str(i.text).strip().replace('\u200c',''))
    return listOFOptions     
def get_audit(report_id,driver):
    results={}
    listOFOptions=get_options(driver)
    if('نظر حسابرس') in listOFOptions:
        try:
            select = Select(driver.find_element_by_id('ddlTable'))
        except:
            select = Select(driver.find_element_by_id('ctl00_ddlTable'))
        select.select_by_visible_text('نظر حسابرس')
        signers=[]
        Audit_Text=[]
        Reciever=''
        Subject=''
        State=''
        if check_exists_by_xpath(driver,'//table[@id="ctl00_cphBody_ucLetterAuditingV2_ucLetterSigner1_dgSigner"]//td'):
            for i in driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucLetterAuditingV2_ucLetterSigner1_dgSigner"]//td'):
                signers.append(i.text)
        if check_exists_by_xpath(driver,'//td[@id="ctl00_cphBody_ucLetterAuditingV2_tdReciverDesc"]/preceding-sibling::td'):
            Reciever=driver.find_element_by_xpath('//td[@id="ctl00_cphBody_ucLetterAuditingV2_tdReciverDesc"]/preceding-sibling::td').text
        if check_exists_by_xpath(driver,'//td[@id="ctl00_cphBody_ucLetterAuditingV2_tdSubjectDesc"]/preceding-sibling::td'):
            Subject=driver.find_element_by_xpath('//td[@id="ctl00_cphBody_ucLetterAuditingV2_tdSubjectDesc"]/preceding-sibling::td').text
        if check_exists_by_xpath(driver,"//table[@id='ctl00_cphBody_ucLetterAuditingV2_DataList1']//td"):
            for i in driver.find_elements_by_xpath("//table[@id='ctl00_cphBody_ucLetterAuditingV2_DataList1']//td"):
                Audit_Text.append(i.text)
        

        if check_exists_by_xpath(driver,'//td[@id="ctl00_cphBody_ucLetterAuditing1_tdReciverDesc"]/preceding-sibling::td[1]'):
            Reciever=driver.find_element_by_xpath('//td[@id="ctl00_cphBody_ucLetterAuditing1_tdReciverDesc"]/preceding-sibling::td[1]').text    
        if check_exists_by_xpath(driver,'//td[@id="ctl00_cphBody_ucLetterAuditing1_tdSubjectDesc"]/preceding-sibling::td[1]'):
            Subject=driver.find_element_by_xpath('//td[@id="ctl00_cphBody_ucLetterAuditing1_tdSubjectDesc"]/preceding-sibling::td[1]').text    
        
        if check_exists_by_xpath(driver,'//table[@id="ctl00_cphBody_ucLetterAuditing1_gvAuditingReport"]'):
            for k in driver.find_elements_by_xpath('//table[@id="ctl00_cphBody_ucLetterAuditing1_gvAuditingReport"]//td'):
                Audit_Text.append(k.text)
        if check_exists_by_xpath(driver,'//td[@id="ctl00_cphBody_ucLetterAuditingDifferentType1_tdSigner"]'):
            signers.append(driver.find_element_by_id('ctl00_cphBody_ucLetterAuditingDifferentType1_lblAuditorName').text)
            for i in driver.find_elements_by_xpath('//*[@id="ctl00_cphBody_ucLetterAuditingDifferentType1_ucLetterSigner1_dgSigner"]//td'):
                signers.append(i.text)        
        if check_exists_by_xpath(driver,'//td[@id="ctl00_cphBody_tdAuditState"]'):
            Subject=driver.find_element_by_xpath('//td[@id="ctl00_cphBody_tdAuditState"]').text
        if check_exists_by_xpath(driver,'//td[@id="ctl00_cphBody_tdAuditorComment"]'):
            Audit_Text.append(driver.find_element_by_xpath('//td[@id="ctl00_cphBody_tdAuditorComment"]').text)  
        results['AuditText']=Audit_Text
        results['signers']=signers
        results['Reciever']=Reciever
        results['Subject']=Subject
        results['reportID']=report_id
        return results
    else:
        return pd.DataFrame()
def Insert_Audit(res):
    if len(res)!=0:
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
                    IF NOT EXISTS (select from statement."AuditNotes" where "report_id"=%s) THEN
                        INSERT INTO statement."AuditNotes"(
                        report_id, "auditText", "auditSigners", "auditReciever", "auditSubject")
                        VALUES (%s, %s, %s, %s, %s);

                    END IF;
                END
                $$ 

            """
            rq = (res['reportID'],res['reportID'],res['AuditText'],res['signers'],res['Reciever'],res['Subject'])
            cursor.execute(postgres_insert_query,rq)
            postgres_insert_query = """
            UPDATE codalraw."SheetsConverted"
            SET "Exist_Audit"=True
            WHERE "report_ID"=%s
            """
            rq = (res['reportID'],)
            cursor.execute(postgres_insert_query,rq)
            connection.commit()
        except(Exception, psycopg2.Error) as error:
                if(connection):
                    print("Failed to Insert AuditNotes", error)
                    log_it('Failed to Insert AuditNotes -')
        finally:
            if(connection):
                cursor.close()
                connection.close()
def set_Options(driver,CodalRaw_ID):
    dictListOf={
        'نظر حسابرس':'Audit',
        
        'صورت گردش خالص دارایی‌ها':'NetworkAssest',
        'صورت گردش خالص داراییها':'NetworkAsset',
        'صورت خالص دارایی ها':'Net',
        'سود (زیان) ناخالص فعالیت بیمهای (قبل از درآمد سرمایهگذاری از محل ذخایر فنی)':'Insurance',
        'درآمد حاصل از سرمایهگذاریها':'Invest',
        
    'ترازنامه تلفيقي':'BSCons',
        'صورت سود و زیان':'IN',
        'صورت سود و زیان تلفیقی':'Incons',
        'صورت سود و زیان جامع تلفیقی':'IncCompCons',
        'صورت سود و زیان جامع':'INComp',
        
        'ترازنامه':'BS',
        'ترازنامه تلفیقی':'BSCons',
        'صورت وضعیت مالی تلفیقی':'BSCons',
        'صورت وضعیت مالی':'BS',
        'آمار تولید و فروش':'Product',
        'صورت جریان وجوه نقد':'CF',
        'جریان وجوه نقد':'CF',
        'صورت تغییرات در حقوق مالکانه':'Right',
        'صورت تغییرات در حقوق مالکانه تلفیقی':'RightCons',
        'صورت جریان های نقدی':'CF',
        'صورت جریان های نقدی تلفیقی':'CFCons',
        'جریان وجوه نقد تلفیقی':'CFCons',
        'خلاصه اطلاعات گزارش تفسیری - صفحه 1':'Interpret',
        'خلاصه اطلاعات گزارش تفسیری - صفحه 2':'Interpret',
        'خلاصه اطلاعات گزارش تفسیری - صفحه 3':'Interpret',
        'خلاصه اطلاعات گزارش تفسیری - صفحه 4':'Interpret',
        'خلاصه اطلاعات گزارش تفسیری - صفحه 5':'Interpret',
        'صورت جریان وجوه نقد تلفیقی':'CFCons',
        'صورت خلاصه سرمایه گذاریها به تفکیک گروه صنعت':'IndustryDivided',
        'صورت وضعیت پورتفوی شرکتهای پذیرفته شده در بورس':'InBourse',
        'صورت وضعیت پورتفوی شرکتهای خارج از بورس':'NotInBourse',
        'صورت ريزمعاملات سهام - تحصیل شده':'bought',
        'صورت ريزمعاملات سهام - واگذار شده':'Sold',
        'درآمد سود سهام محقق شده':'Div',
    }
    
    listOf=get_options(driver)
    Available=[]
    Available.append('RepID')
    for i in listOf:
        Available.append(dictListOf[i])
    DF=pd.DataFrame(columns=Available)
    DF=DF.append({'RepID':CodalRaw_ID},ignore_index=True)
    DF.fillna(False,inplace=True)
    for t in dictListOf.values():
        if t not in DF.columns:
            DF[t]=None
    SetConvertable(DF)
def SetConvertable(DF):
    try:
        AllLabels=[]
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
                IF NOT EXISTS (select from codalraw."SheetsConverted" where "report_ID"=%(RepID)s) THEN
                   INSERT INTO codalraw."SheetsConverted"(
            "report_ID", "Exist_Bs", "Exist_Income", "Exist_Cf", "Exist_productAmount", "Exist_Interpret", "Exist_bsCons", "Exist_IncomeCons", "Exist_cfCons", "Exist_IncomeComp", "Exist_PropertyRight", "Exist_IncomeCompCons", "Exist_PropertyRightCons", "Exist_InvestIndustGroup", "Exist_InvestSale", "Exist_InvestBuy", "Exist_periodicPortfo_Accepted", "Exist_periodicPortfo_NotAccepted", "Exist_InsuranceActivity", "Exist_insurance", "Exist_networkingAsset", "Exist_NetAsset", "Exist_InvestDiv", "Exist_Audit")
            VALUES (%(RepID)s, %(BS)s, %(IN)s, %(CF)s, %(Product)s, %(Interpret)s, %(BSCons)s, %(Incons)s, %(CFCons)s, %(INComp)s, %(Right)s, %(IncCompCons)s, %(RightCons)s, %(IndustryDivided)s, %(Sold)s, %(bought)s, %(InBourse)s, %(NotInBourse)s, %(Invest)s, %(Insurance)s, %(NetworkAssest)s, %(Net)s, %(Div)s, %(Audit)s);
                END IF;
            END
            $$ 

        """

        cursor.executemany(postgres_insert_query,DF.to_dict(orient='records'))
        connection.commit()
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print("Failed to Update Convertable", error)
    finally:
        if(connection):
            cursor.close()
            connection.close()    
def RUN():
    df=get_unconverted().head(1)
    driver=webdriver.Chrome()
    driver.maximize_window()  
    AllData=len(df.index)
    counter=0
    #for index,row in TestDF.iterrows():
    for index,row in df.head(1).iterrows():
        try:
            CodalRaw_ID=int(row['TracingNo'])
            CodalRaw_links='https://www.codal.ir'+str(row['HtmlUrl'])
            print(CodalRaw_links)
            driver.get(CodalRaw_links)
            if UpdateError(driver,CodalRaw_ID):
                titles=get_titleBox(driver)
                titles['report_id']=CodalRaw_ID
                insert_titlebox(titles)
                set_Options(driver,CodalRaw_ID)
                Insert_Audit(get_audit(CodalRaw_ID,driver))
                # takeCareOFBS(CodalRaw_ID,CodalRaw_links)
                # takeCareOFIS(CodalRaw_ID,CodalRaw_links)
                time.sleep(0.5)
                counter=counter+1
                percentage=(counter*100)/AllData
                print("{0:.2f}".format(percentage))
                if counter%100==0:
                    print('***')
                    print(counter)
                    print(AllData)
                    print('***')
        except(Exception) as error:
            print(error)
            continue                 
RUN()            