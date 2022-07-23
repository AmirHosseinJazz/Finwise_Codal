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
        log_it("N_51_Error_"+str(CodalRaw_ID))
        connection.commit() 
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print("Failed to Update Error sheets", error)
                log_it('Failed to Update Error sheets -')
    finally:
        if(connection):
            cursor.close()
            connection.close()      
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

        select * FROM codalraw."allrawReports" where "LetterCode"='ن-۵۲' and "HtmlUrl"!='' and "Available"=True and "TracingNo" not in (select "report_ID" from codalraw."AssemblyConverted")


        
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
def Translate_PresenetShareholders(driver):
    try:
        dictHead={}
        counter=1
        for tablerow in driver.find_elements_by_xpath('//*[@id="ucAssemblyShareHolder1_upAssemblyShareHolder"]/table/tbody/tr[2]//table/tbody/tr'):
            for j in tablerow.find_elements_by_xpath('.//th'):
                dictHead[counter]=j.text.replace('\n','')
                counter=counter+1
        listofDictrows=[]
        counter=1
        for tablerow in driver.find_elements_by_xpath('//*[@id="ucAssemblyShareHolder1_gvAssemblyShareHolder"]//tr'):
            DictRow={}
            counter=1
            for j in tablerow.find_elements_by_xpath('.//td'):
                DictRow[counter]=j.text.replace('\n','')
                counter=counter+1
            listofDictrows.append(DictRow)
        DFROWS=pd.DataFrame(listofDictrows)
        if not DFROWS.empty:
            DFROWS.dropna(inplace=True)
            DFROWS.columns=dictHead.values()        
            swap_dict={
                'اسامی سهامداران':'Shareholders',
                'تعداد سهام':'ShareCount',
                'درصد مالکیت':'OwnerPercentage',
                '':''
            }
            newcols=[]
            for i in DFROWS.columns:
                newcols.append(swap_dict[i])
            DFROWS.columns=newcols
            if '' in DFROWS.columns:
                DFROWS.drop(columns=[''],inplace=True)
            DFROWS.replace('ك','ک',regex=True,inplace=True)
            DFROWS.replace('ي','ی',regex=True,inplace=True)      
            return DFROWS
        else:
            return pd.DataFrame()
    except:
        return pd.DataFrame()
def Translate_AssemblyCheif(driver):
    try:
        listofDicts=[]
        labels=['txtAssemblyChief','txtAssemblySuperVisor1','txtAssemblySuperVisor2','txtAssemblySecretary']
        for i in labels:
            dictOne={}
            if check_exists_by_xpath(driver,'//*[@id="'+i+'"]'):
                dictOne['Position']=i
                dictOne['Name']=driver.find_element_by_id(i).text
                listofDicts.append(dictOne)
        DFROWS=pd.DataFrame(listofDicts)
        if not DFROWS.empty:
            swap_dict={
                        'txtAssemblyChief':'Cheif',
                        'txtAssemblySuperVisor1':'Supervisor1',
                        'txtAssemblySuperVisor2':'Supervisor2',
                        'txtAssemblySecretary':'Secretary'
                        
            }
            for t in swap_dict.keys():
                DFROWS.Position=DFROWS.Position.replace(t,swap_dict[t])
            DFROWS.replace('ك','ک',regex=True,inplace=True)
            DFROWS.replace('ي','ی',regex=True,inplace=True)    
            return DFROWS
        else:
            return pd.DataFrame()
    except:
        return pd.DataFrame()
def Translate_board(driver):
    try:
        dictHead={}
        counter=1
        for tablerow in driver.find_elements_by_xpath('//*[@id="upLastBoardMember"]/table/tbody/tr[2]//table/tbody/tr'):
            for j in tablerow.find_elements_by_xpath('.//td'):
                dictHead[counter]=j.text.replace('\n','')
                counter=counter+1
        listofDictrows=[]
        counter=1
        for tablerow in driver.find_elements_by_xpath('//*[@id="gvLastBoardMember"]//tr'):
            DictRow={}
            counter=1
            for j in tablerow.find_elements_by_xpath('.//td'):
                DictRow[counter]=j.text.replace('\n','')
                counter=counter+1
            listofDictrows.append(DictRow)
        DFROWS=pd.DataFrame(listofDictrows)
        if not DFROWS.empty:
            DFROWS.dropna(inplace=True)
            DFROWS.columns=dictHead.values()        
            swap_dict={
                'نام عضو حقیقی یا حقوقی هیئت مدیره':'FullName',
                'شمارۀ ثبت عضو حقوقی/کد ملی':'SSID',
                'نوع شرکت':'typeOfCompany',
                'نوع عضویت':'Membership',
                'نام نماینده عضو حقوقی':'AgentName',
                'کد ملی نماینده عضو حقوقی':'AgentSSID',
                'سمت':'Position',
                'موظف/غیر موظف':'Duty',
                'مدرک تحصیلی':'Degree',
                '':''
                    }
            newcols=[]
            for i in DFROWS.columns:
                newcols.append(swap_dict[i])
            DFROWS.columns=newcols
            if '' in DFROWS.columns:
                DFROWS.drop(columns=[''],inplace=True)
            DFROWS.replace('ك','ک',regex=True,inplace=True)
            DFROWS.replace('ي','ی',regex=True,inplace=True)      
            return DFROWS
        else:
            return pd.DataFrame() 
    except:
        return pd.DataFrame()
def Translate_CEO(driver):
    try:
        listofDicts=[]
        dictOne={}
        if check_exists_by_xpath(driver,'//*[@id="txbDirectorManager"]'):
            if driver.find_element_by_id('txbDirectorManager').text=='':
                dictOne['FullName']=driver.find_element_by_id('txbDirectorManager').get_attribute('value')
        if check_exists_by_xpath(driver,'//*[@id="txbDMNationalCode"]'):
            if driver.find_element_by_id('txbDMNationalCode').text=='':
                dictOne['SSID']=driver.find_element_by_id('txbDMNationalCode').get_attribute('value')
        if check_exists_by_xpath(driver,'//*[@id="txbDMDegree"]'):
            if driver.find_element_by_id('txbDMDegree').text=='':
                dictOne['Degree']=driver.find_element_by_id('txbDMDegree').get_attribute('value')                
        listofDicts.append(dictOne)
        DFROWS=pd.DataFrame(listofDicts)
        if not DFROWS.empty:
            DFROWS.replace('ك','ک',regex=True,inplace=True)
            DFROWS.replace('ي','ی',regex=True,inplace=True)      
            return DFROWS
        else:
            return pd.DataFrame()       
    except:
        return pd.DataFrame()
def Translate_WagesGift(driver):
    try:
        dictHead={}
        counter=1
        for tablerow in driver.find_elements_by_xpath('//*[@id="tblWageGift"]/tbody/tr'):
            for j in tablerow.find_elements_by_xpath('.//th'):
                dictHead[counter]=j.text.replace('\n','')
                counter=counter+1
        listofDictrows=[]
        counter=1
        for tablerow in driver.find_elements_by_xpath('//*[@id="tblWageGift"]/tbody/tr'):
            DictRow={}
            counter=1
            for j in tablerow.find_elements_by_xpath('.//td'):
                DictRow[counter]=j.text.replace('\n','')
                counter=counter+1
            listofDictrows.append(DictRow)
        DFROWS=pd.DataFrame(listofDictrows)
        if not DFROWS.empty:
            DFROWS.dropna(inplace=True)
            DFROWS.columns=dictHead.values()        
            swap_dict={
                'شرح':'Desc',
                'سال قبل - مبلغ':'LastYear',
                'سال جاری - مبلغ':'CurrentYear',
                'توضیحات':'MoreInfo',
                '':''
            }
            newcols=[]
            for i in DFROWS.columns:
                newcols.append(swap_dict[i])
            DFROWS.columns=newcols
            if '' in DFROWS.columns:
                DFROWS.drop(columns=[''],inplace=True)
            DFROWS.replace('ك','ک',regex=True,inplace=True)
            DFROWS.replace('ي','ی',regex=True,inplace=True) 
            DFROWS['LastYear']=DFROWS['LastYear'].apply(get_true_value) 
            DFROWS['CurrentYear']=DFROWS['CurrentYear'].apply(get_true_value) 
            return DFROWS
        else:
            return pd.DataFrame() 
    except:
        return pd.DataFrame()
def Translate_NEWBoard(driver):
    try:
        dictHead={}
        counter=1
        for tablerow in driver.find_elements_by_xpath('//*[@id="upNewBoardMember"]/table/tbody/tr[2]//table/tbody/tr'):
            for j in tablerow.find_elements_by_xpath('.//td'):
                dictHead[counter]=j.text.replace('\n','')
                counter=counter+1
        listofDictrows=[]
        counter=1
        for tablerow in driver.find_elements_by_xpath('//*[@id="gvNewBoardMember"]/tbody/tr'):
            DictRow={}
            counter=1
            for j in tablerow.find_elements_by_xpath('.//td'):
                DictRow[counter]=j.text.replace('\n','')
                counter=counter+1
            listofDictrows.append(DictRow)
        DFROWS=pd.DataFrame(listofDictrows)
        if not DFROWS.empty:
            DFROWS.dropna(inplace=True)
            DFROWS.columns=dictHead.values()        
            swap_dict={
                'نام عضو حقیقی یا حقوقی هیئت مدیره':'Name',
                'ماهیت':'','نوع شرکت':'Type',
                'شمارۀ ثبت عضو حقوقی/کد ملی':'SSID',
                'نوع عضویت':'Duty',
                '':''
            }
            newcols=[]
            for i in DFROWS.columns:
                newcols.append(swap_dict[i])
            DFROWS.columns=newcols
            if '' in DFROWS.columns:
                DFROWS.drop(columns=[''],inplace=True)
            DFROWS.replace('ك','ک',regex=True,inplace=True)
            DFROWS.replace('ي','ی',regex=True,inplace=True) 
            return DFROWS
        else:
            return pd.DataFrame() 
    except:
        return pd.DataFrame()
def Translate_StatementConfirmed(driver,ADATE):
    try:
        dictHead={}
        counter=1
        for tablerow in driver.find_elements_by_xpath('//*[@id="ucAssemblyPRetainedEarning_grdAssemblyProportionedRetainedEarning"]/tbody/tr'):
            for j in tablerow.find_elements_by_xpath('.//th'):
                dictHead[counter]=j.text.replace('\n','')
                counter=counter+1
        listofDictrows=[]
        counter=1
        for tablerow in driver.find_elements_by_xpath('//*[@id="ucAssemblyPRetainedEarning_grdAssemblyProportionedRetainedEarning"]/tbody/tr'):
            DictRow={}
            counter=1
            for j in tablerow.find_elements_by_xpath('.//td'):
                DictRow[counter]=j.text.replace('\n','')
                counter=counter+1
            listofDictrows.append(DictRow)
        DFROWS=pd.DataFrame(listofDictrows)
        if not DFROWS.empty:
            DFROWS.dropna(inplace=True)
            DFROWS.columns=dictHead.values()        
            newcols=['','Desc','Value']
            DFROWS.columns=newcols
            if '' in DFROWS.columns:
                DFROWS.drop(columns=[''],inplace=True)
            DFROWS=DFROWS[DFROWS['Desc']!='']
            DFROWS.replace('ك','ک',regex=True,inplace=True)
            DFROWS.replace('ي','ی',regex=True,inplace=True)
            DFROWS['Value']=DFROWS['Value'].apply(get_true_value) 
            DFROWS['AssemblyDate']=ADATE
            return DFROWS
        else:
            return pd.DataFrame()
    except:
        return pd.DataFrame() 
def insert_general(row):
    try:

        connection = psycopg2.connect(user=db_username,
                                      password=db_pass,
                                      host=db_host,
                                      port=db_port,
                                      database=db_database)
        cursor = connection.cursor()
        Dict=row.to_dict()
        pouery = """
            DO 
            $$
                BEGIN
                    IF NOT EXISTS (select from  public."Publishers" where "persianName"=%(Nemad)s) THEN
                         INSERT INTO public."Publishers"(
                        "persianName")
                        VALUES (%(Nemad)s);
                    END IF;
                END
            $$ 

        """
        cursor.execute(pouery,Dict)
        connection.commit()
        if ('toDate' not in Dict.keys()):
            Dict['toDate']=None  
            
        # pfirm="""
        #     DO 
        #     $$
        #     BEGIN
        #         IF NOT EXISTS (select from public.firm where "ticker"=%(Nemad)s) THEN
        #             INSERT INTO  public.firm(
        #             firm")
        #             VALUES ( (select "ID" from "firm" where ticker=%(Nemad)s
                    
        #             );
        #         END IF;
        #     END
        #     $$ 

        
        
        # """
        # cursor.execute(pfirm,Dict)
        #connection.commit()
        postgres_insert_query = """
         DO 
            $$
            BEGIN
                IF NOT EXISTS (select from codalreports."Assembly_General" where "report_id"=%(report_id)s) THEN
                    INSERT INTO codalreports."Assembly_General"(
                    firm, report_id, "ToDate", "Correction", "CorrectionDetails", "NewsPaper", "Inspector", "OtherDesc", "IsListenedBoardMemberReport",     "IsSelectInspector", "IsSelectNewspaper", "IsBoardMemberGift", "IsOther", "IsBoardMemberWage", "IsSelectBoardMember", "IsPublishSecurity", "IsLocationChange",  "IsNameChange", "IsActivitySubjectChange", "IsConvertSecurityToShare", "IsFinancialYearChange", "IsCapitalIncrease")
                    VALUES ( (select "ID" from "Publishers" where "persianName"=%(Nemad)s), %(report_id)s,
                    %(ToDate)s, %(correction)s, %(Details)s,
                    %(NewsPaper)s, %(Inspector)s, 
                    
                    %(txbOtherDes)s, %(lblIsListenedBoardMemberReport)s, %(lblIsSelectInspector)s, %(lblIsSelectNewspaper)s, %(lblIsBoardMemberGift)s, %(lblIsOther)s, %(lblIsBoardMemberWage)s, %(lblIsSelectBoardMember)s, %(lblIsPublishSecurity)s, %(lblIsLocationChange)s,  %(lblIsNameChange)s, %(lblIsActivitySubjectChange)s, %(lblIsConvertSecurityToShare)s, %(lblIsFinancialYearChange)s, %(lblIsCapitalIncrease)s
                    
                    );
                END IF;
            END
            $$ 

        """

        cursor.execute(postgres_insert_query,Dict)
        #connection.commit()

        #####
        ASCheif=row['AssmblyCheif']
        ASCheif['report_id']=row['report_id']
        postgres_insert_query_cheif = """
         DO 
            $$
            BEGIN
                IF NOT EXISTS (select from codalreports."Assembly_General_AssemblyChief" where "SummaryID"=(Select "ID" from codalreports."Assembly_General" where "report_id"=%(report_id)s) and "Position"=%(Position)s ) THEN
                    INSERT INTO codalreports."Assembly_General_AssemblyChief"(
                    "SummaryID", "Name", "Position")
                    VALUES ( (select "ID" from codalreports."Assembly_General" where report_id=%(report_id)s),%(Name)s,%(Position)s
                    
                    );
                END IF;
            END
            $$ 

        """
        if len(ASCheif.columns)==3:
            cursor.executemany(postgres_insert_query_cheif,ASCheif.to_dict(orient='records'))
        #######
        ASBoard=row['Board']
        ASBoard['report_id']=row['report_id']
        postgres_insert_query_board = """
         DO 
            $$
            BEGIN
                IF NOT EXISTS (select from codalreports."Assembly_General_Board" where "SummaryID"=(Select "ID" from codalreports."Assembly_General" where "report_id"=%(report_id)s) and "FullName"=%(FullName)s and "SSID"=%(SSID)s ) THEN
                    INSERT INTO codalreports."Assembly_General_Board"(
                    "SummaryID", "FullName", "SSID", "typeOfCompany", "Membership", "AgentName", "AgentSSID", "Position", "Duty", "Degree")
                    VALUES ( (select "ID" from codalreports."Assembly_General" where report_id=%(report_id)s),
                    
                    %(FullName)s,%(SSID)s,%(typeOfCompany)s,%(Membership)s,%(AgentName)s,%(AgentSSID)s,%(Position)s, %(Duty)s, %(Degree)s
                    
                    );
                END IF;
            END
            $$ 

        """
        if len(ASBoard.columns)>3:
            cursor.executemany(postgres_insert_query_board,ASBoard.to_dict(orient='records'))
        ##########
        ASCEO=row['CEO']
        ASCEO['report_id']=row['report_id']
        postgres_insert_query_ceo = """
         DO 
            $$
            BEGIN
                IF NOT EXISTS (select from codalreports."Assembly_General_CEO" where "SummaryID"=(Select "ID" from codalreports."Assembly_General" where "report_id"=%(report_id)s) and "FullName"=%(FullName)s and "SSID"=%(SSID)s ) THEN
                    INSERT INTO codalreports."Assembly_General_CEO"(
                    "SummaryID", "FullName", "SSID", "Degree")
                    VALUES ( (select "ID" from codalreports."Assembly_General" where report_id=%(report_id)s),
                    
                    %(FullName)s,%(SSID)s, %(Degree)s
                    
                    );
                END IF;
            END
            $$ 

        """
        if len(ASCEO.columns)>3:
            cursor.executemany(postgres_insert_query_ceo,ASCEO.to_dict(orient='records'))
        ###########
        
        ASNewBoard=row['NewBoard']
        ASNewBoard['report_id']=row['report_id']
        postgres_insert_query_newBoard = """
         DO 
            $$
            BEGIN
                IF NOT EXISTS (select from codalreports."Assembly_General_NewBoard" where "SummaryID"=(Select "ID" from codalreports."Assembly_General" where "report_id"=%(report_id)s) and "Name"=%(Name)s) THEN
                    INSERT INTO codalreports."Assembly_General_NewBoard"(
                    "SummaryID", "Name", "Type", "SSID", "Duty")
                    VALUES ( (select "ID" from codalreports."Assembly_General" where report_id=%(report_id)s),
                    
                    %(Name)s,%(Type)s, %(SSID)s,%(Duty)s
                    
                    );
                END IF;
            END
            $$ 

        """
        if len(ASNewBoard.columns)>3:
            cursor.executemany(postgres_insert_query_newBoard,ASNewBoard.to_dict(orient='records'))
        #####
        ASShareholders=row['PresentShareHolders']
        ASShareholders['report_id']=row['report_id']
        postgres_insert_query_present= """
         DO 
            $$
            BEGIN
                IF NOT EXISTS (select from codalreports."Assembly_General_PresentShareHolders" where "SummaryID"=(Select "ID" from codalreports."Assembly_General" where "report_id"=%(report_id)s) and "Shareholders"=%(Shareholders)s  ) THEN
                    INSERT INTO codalreports."Assembly_General_PresentShareHolders"(
                    "SummaryID", "Shareholders", "ShareCount", "OwnerPercentage")
                    VALUES ( (select "ID" from codalreports."Assembly_General" where report_id=%(report_id)s),
                    %(Shareholders)s,%(ShareCount)s, %(OwnerPercentage)s
                    );
                END IF;
            END
            $$ 

        """
        if len(ASShareholders.columns)>3:
            cursor.executemany(postgres_insert_query_present,ASShareholders.to_dict(orient='records'))
        ###
        Asstatment=row['Statement']
        Asstatment['report_id']=row['report_id']
        postgres_insert_query_statement= """
         DO 
            $$
            BEGIN
                IF NOT EXISTS (select from codalreports."Assembly_General_Statement" where "SummaryID"=(Select "ID" from codalreports."Assembly_General" where "report_id"=%(report_id)s) and "Title"=%(Desc)s  ) THEN
                    INSERT INTO codalreports."Assembly_General_Statement"(
                    "SummaryID", "Title", "Value","AssemblyDate")
                    VALUES ( (select "ID" from codalreports."Assembly_General" where report_id=%(report_id)s),
                    %(Desc)s,%(Value)s,%(AssemblyDate)s
                    );
                END IF;
            END
            $$ 

        """
        if len(Asstatment.columns)==4:
            cursor.executemany(postgres_insert_query_statement,Asstatment.to_dict(orient='records'))
        ########
        
        AsWage=row['Wages']
        AsWage['report_id']=row['report_id']
        postgres_insert_query_wage= """
         DO 
            $$
            BEGIN
                IF NOT EXISTS (select from codalreports."Assembly_General_WageGift" where "SummaryID"=(Select "ID" from codalreports."Assembly_General" where "report_id"=%(report_id)s) and "Title"=%(Desc)s  ) THEN
                    INSERT INTO codalreports."Assembly_General_WageGift"(
                    "SummaryID", "Title", "LastYear", "CurrentYear", "MoreDetails")
                    VALUES ( (select "ID" from codalreports."Assembly_General" where report_id=%(report_id)s),
                    %(Desc)s,%(LastYear)s,%(CurrentYear)s,%(MoreInfo)s
                    );
                END IF;
            END
            $$ 

        """
        if len(AsWage.columns)>3:
            cursor.executemany(postgres_insert_query_wage,AsWage.to_dict(orient='records'))
        ####
        connection.commit()
        postgres_insert_query3 = """
        INSERT INTO codalraw."AssemblyConverted"(
        "report_ID", converted)
        VALUES (%s, True);
        """
        record_to_insert3=([row['report_id']])
        cursor.execute(postgres_insert_query3, record_to_insert3)
        connection.commit()
        print(str(row['report_id'])+'  '+'--Done')
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print("Failed to Insert General ", error)
                log_it('Failed to Insert General -'+str(row['report_id']))
    finally:
        if(connection):
            cursor.close()
            connection.close()         

def handleAllGeneral(driver,df):
    General=df[(df['title'].str.contains('مجمع عمومی عادی سالیانه'))]
    invi_general_labels=['lblIsListenedBoardMemberReport',
                        'lblIsApproveStatements','lblIsSelectInspector','lblIsSelectNewspaper',
                        'lblIsSelectBoardMember','lblIsBoardMemberGift',
                        'lblIsPublishSecurity','lblIsLocationChange','lblIsNameChange','lblIsActivitySubjectChange',
                        'lblIsConvertSecurityToShare','lblIsFinancialYearChange','lblIsOther',
                        'txbOtherDes','lblIsCapitalIncrease','lblIsBoardMemberWage']

    listofDicts=[]
    counter=0
    for index,row in General.iterrows():
        try:
            CodalRaw_ID=str(row['TracingNo'])
            CodalRaw_links=row['HtmlUrl']
            driver.get('http://codal.ir/'+CodalRaw_links)
            if UpdateError(driver,CodalRaw_ID):
                AssemblyDate=''
                if check_exists_by_xpath(driver,'//bdo'):
                    try:
                        AssemblyDate=(driver.find_elements_by_xpath('//bdo')[1].text.strip())
                    except:
                        AssemblyDate=''
                Dict_IC_Time={}
                Dict_IC_Time['report_id']=CodalRaw_ID
                Dict_IC_Time['Nemad']=row['Ticker']
                Dict_IC_Time['typeOfAssembly']='General'
                ##Initializing Variables#################
                Nemad=''
                Details=''
                correction=False
                ToDate=''
                DisclousreSubject=''
                ####row['Title'] Data###
                Title=row['title']
                Title=Title.replace('(','')
                Title=Title.replace(')','')
                if 'منتهی' in row['title']:
                    x=re.search("\d\d\d\d.\d\d.\d\d",row['title'])
                    ToDate=x.group()
                Dict_IC_Time['ToDate']=ToDate
                if ('اصلاح') in row['title']:
                        correction=True
                Dict_IC_Time['correction']=correction  
                publishDate=row['PublishTime']
                Dict_IC_Time['DateOf']=publishDate
                ##### DETAILS FROM TEXT###
                txt=driver.find_element_by_tag_name('body').text
                if('دلایل اصلاح:') in txt:
                    kk=1
                    while find_nth(txt,'\n',kk)<txt.find('دلایل اصلاح:'):
                        kk=kk+1
                        if kk>10:
                            kk=2
                            break
                    Details=txt[txt.find('دلایل اصلاح:'):find_nth(txt,'\n',kk)] 
                Dict_IC_Time['Details']=Details
                Dict_IC_Time['PresentShareHolders']=Translate_PresenetShareholders(driver)
                Dict_IC_Time['AssmblyCheif']=Translate_AssemblyCheif(driver)
                Dict_IC_Time['Board']=Translate_board(driver)
                Dict_IC_Time['CEO']=Translate_CEO(driver)
                Dict_IC_Time['Wages']=Translate_WagesGift(driver)
                Dict_IC_Time['NewBoard']=Translate_NEWBoard(driver)
                Dict_IC_Time['Statement']=Translate_StatementConfirmed(driver,AssemblyDate)
                #############
                if check_exists_by_xpath(driver,'//*[@id="upNewsPaper"]'):
                    NewsPaper=driver.find_element_by_id('upNewsPaper').text.strip()
                else:
                    NewsPaper=''
                Dict_IC_Time['NewsPaper']=NewsPaper

                if check_exists_by_xpath(driver,'//*[@id="divSelectInspector"]'):
                    Inspector=driver.find_element_by_id('divSelectInspector').text.strip().replace('\u200c','')
                else:
                    Inspector=''
                Dict_IC_Time['Inspector']=Inspector
                ####
                for i in invi_general_labels:
                    if check_exists_by_xpath(driver,'//*[contains(@id,"'+i+'")]'):
                        if 'lbl' in i:
                            Dict_IC_Time[i]=True
                        else:
                            Dict_IC_Time[i]=driver.find_element_by_xpath(('//*[contains(@id,"'+i+'")]')).text

                listofDicts.append(Dict_IC_Time)
                counter=counter+1
                print(counter/len(df))
            
        except:
            continue
    DFIC=pd.DataFrame(listofDicts)
    for j in invi_general_labels:
        if j not in DFIC.columns:
            DFIC[j]=None
    for i in DFIC.columns:
        if 'lbl' in i:
            DFIC[i].fillna('False',inplace=True)
    DFIC.replace('ك','ک',regex=True,inplace=True)
    DFIC.replace('ي','ی',regex=True,inplace=True)      
    for index,row in DFIC.iterrows():
        insert_general(row)

def RUN(driver):
    df=get_unconverted()
    if not df.empty:
        handleAllGeneral(driver,df)
    else:
        print('No New General Assembly _ N52')            