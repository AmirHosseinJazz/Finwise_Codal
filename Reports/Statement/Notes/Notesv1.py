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
from lxml import html
from io import StringIO
import re
db_username="Jay"
db_pass="Mehrad1"
db_host="127.0.0.1"
db_port="5432"
db_database="FinWisev12"
def DeleteFromPreNotes_NullGE(DF4):
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
                DELETE FROM statement."PreNotes"
                WHERE "report_ID"=%(report_ID)s and "tableID"=%(engtitle)s;
            END
            $$ 

        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print('Failed to Update Data ', error)
                #log_it('Failed to Update OtherLayers ')
                #return False
            
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
def InsertGeneralStatements(DF4):
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
                IF NOT EXISTS (select from statementnotes."GeneralStatements" where ("report_ID"=%(report_ID)s and title=%(title)s and "Statement"=%(desc)s)) THEN
               INSERT INTO statementnotes."GeneralStatements"(
                   "report_ID", title, "Statement", "engTitle")
        	VALUES (%(report_ID)s,%(title)s,%(desc)s,%(engtitle)s);
                END IF;
            END
            $$ 

        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print('Failed to Update Data ', error)
                #log_it('Failed to Update OtherLayers ')
                #return False
            
    finally:
        if(connection):
            cursor.close()
            connection.close()        
def CirculationAmountRialCommodityInventory_all():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" where p."tableID"='Circulation amount - Rial commodity inventory' and p."report_ID" not in (Select "report_ID" from statementnotes."CirculationAmountRialCommodityInventory")
        """, connection)
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close()     
def InsertCirculationAmountRialCommodityInventory(DF4):
    try:
        connection = psycopg2.connect(user=db_username,
                                        password=db_pass,
                                        host=db_host,
                                        port=db_port,
                                        database=db_database)
        cursor = connection.cursor()
        prodquery = """
         DO 
            $$
            BEGIN
                IF NOT EXISTS (select from monthly.goods where "name"=%(product)s ) THEN
                  INSERT INTO monthly.goods(
                    name)
                    VALUES (%(product)s);
                END IF;
            END
            $$ 

        """
        cursor.executemany(prodquery,DF4.to_dict(orient='records'))
        postgres_insert_query_cheif = """
         DO 
            $$
            BEGIN
                IF NOT EXISTS (select from statementnotes."CirculationAmountRialCommodityInventory" where "report_ID"=%(report_ID)s and product=(select "ID" from monthly.goods where name=%(product)s)) THEN
                  INSERT INTO statementnotes."CirculationAmountRialCommodityInventory"(
        "report_ID", product, unit, period, "toDate", startofperiod_amount, startofperiod_fee, startifoperiod_cost, production_amount, production_fee, production_cost, modifications_amount, modifications_fee, modifications_cost, sell_amount, sell_fee, sell_cost, endofperiod_amount, endofperiod_fee, endofperiod_cost)
	VALUES (%(report_ID)s,(select "ID" from monthly.goods where name=%(product)s),%(unit)s,%(period)s,%(toDate)s,%(spamount)s,%(spfee)s,%(spcost)s,%(proAmount)s,%(proFee)s,%(proCost)s,%(adjamount)s,%(adjfee)s,%(adjcost)s,%(sellAm)s,%(sellFee)s,%(sellCost)s,%(endAmou)s,%(endfee)s,%(endcost)s);
                END IF;
            END
            $$ 

        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print('Failed to Update Data ', error)
                #log_it('Failed to Update OtherLayers ')
                #return False
            
    finally:
        if(connection):
            cursor.close()
            connection.close()  
def Handle_CirculationAmountRialCommodityInventory():
    df=CirculationAmountRialCommodityInventory_all()
    for index,row in df.iterrows():
        # if(index%200==0):
        #     print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        toDate=""
        period=""
        for col in df1.columns:
            if re.search('\d\d\d\d/\d\d/\d\d',col):
                toDate=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
            if re.search('دوره.\d{1,2}.ماهه',col):
                period=re.search('\d{1,2}',(re.search('دوره.\d{1,2}.ماهه',col)[0]))[0]
                
            if period !="" and toDate!="":
                # print(toDate)
                break
        df1["period"]=period
        df1["toDate"]=toDate
        df1 = df1.rename(columns={'واحد ': 'unit'})
        df1 = df1.rename(columns={'شرح محصول ': 'product'})
        for col in df1.columns:
            if 'مقدار' in col and 'اول دوره' in col:
                df1 = df1.rename(columns={col: 'spamount'})
            if 'نرخ' in col and 'اول دوره' in col:
                df1 = df1.rename(columns={col: 'spfee'})
            if 'بها' in col and 'اول دوره' in col:
                df1 = df1.rename(columns={col: 'spcost'})
            if 'مقدار' in col and 'توليد' in col:
                df1 = df1.rename(columns={col: 'proAmount'})
            if 'نرخ' in col and 'توليد' in col:
                df1 = df1.rename(columns={col: 'proFee'})
            if 'بها' in col and 'توليد' in col:
                df1 = df1.rename(columns={col: 'proCost'})
            if 'مقدار' in col and 'تعديلات' in col:
                df1 = df1.rename(columns={col: 'adjamount'})
            if 'نرخ' in col and 'تعديلات' in col:
                df1 = df1.rename(columns={col: 'adjfee'})
            if 'بها' in col and 'تعديلات' in col:
                df1 = df1.rename(columns={col: 'adjcost'})      
            if 'مقدار' in col and 'فروش' in col:
                df1 = df1.rename(columns={col: 'sellAm'})
            if 'نرخ' in col and 'فروش' in col:
                df1 = df1.rename(columns={col: 'sellFee'})
            if 'بها' in col and 'فروش' in col:
                df1 = df1.rename(columns={col: 'sellCost'})  
            if 'مقدار' in col and 'پايان' in col:
                df1 = df1.rename(columns={col: 'endAmou'})
            if 'نرخ' in col and 'پايان' in col:
                df1 = df1.rename(columns={col: 'endfee'})
            if 'بها' in col and 'پايان' in col:
                df1 = df1.rename(columns={col: 'endcost'})
        df1['report_ID']=row['report_ID']
        df1['product'] = df1['product'].str.strip()
        for col in df1.columns:
            if 'مقدار' in col:
                    df1 = df1.rename(columns={col: 'spamount'})
            if 'نرخ' in col :
                    df1 = df1.rename(columns={col: 'spfee'})
            if 'بها' in col :
                    df1 = df1.rename(columns={col: 'spcost'})
        InsertCirculationAmountRialCommodityInventory(df1)
def InsertDepositsreceivedfromcustomers(DF4):
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
                IF NOT EXISTS (select from statementnotes."DepositRecievedFromCustomers" where "report_ID"=%(report_ID)s and title=%(title)s) THEN
                  INSERT INTO statementnotes."DepositRecievedFromCustomers"(
	"report_ID", title, "depositStartdate", "depositEndDate", "interestRatestartDate", "interestRateendDated", "ManagerPosition")
	VALUES (%(report_ID)s,%(title)s, %(depositstart)s,%(depositend)s,%(intereststart)s,%(interestend)s,%(managervision)s);
                END IF;
            END
            $$ 

        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print('Failed to Update Data ', error)
                #log_it('Failed to Update OtherLayers ')
                #return False
            
    finally:
        if(connection):
            cursor.close()
            connection.close()
def Depositsreceivedfromcustomers_all():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()

        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" 
        where p."tableID"='Deposits received from customers' and p."report_ID" not in (Select "report_ID" from statementnotes."DepositRecievedFromCustomers")
        """, connection)
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close()               
def Handle_Depositsreceivedfromcustomers_all():
    df=Depositsreceivedfromcustomers_all()
    for index,row in df.iterrows():
        if(index%200==0):
            print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        toDate1=""
        toDate2=""
        for col in df1.columns:
            if re.search('\d\d\d\d/\d\d/\d\d',col):
                ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                if ss!="" and toDate1=="":
                    toDate1=ss
                if ss!=toDate1 and ss!="":
                    toDate2=ss
                
            if toDate2 !="" and toDate1!="":
                # print(toDate)
                break
        endDate=max(toDate1,toDate2)
        startDate=min(toDate1,toDate2)
        # df1["toDate"]=toDate
        
        df1 = df1.rename(columns={'شرح ': 'title'})
        for col in df1.columns:
            if 'مانده' in col and endDate in col:
                df1 = df1.rename(columns={col: 'depositend'})
            if 'مانده' in col and startDate in col:
                df1 = df1.rename(columns={col: 'depositstart'})
            if 'متوسط' in col and endDate in col:
                df1 = df1.rename(columns={col: 'interestend'})
            if 'متوسط' in col and startDate in col:
                df1 = df1.rename(columns={col: 'intereststart'})
            if 'برآورد' in col:
                df1 = df1.rename(columns={col: 'managervision'})
        df1=df1[(df1['depositstart'].notna())|(df1['depositend'].notna())|(df1['intereststart'].notna())|(df1['interestend'].notna())]
        df1['report_ID']=row['report_ID']
        InsertDepositsreceivedfromcustomers(df1)       
def AdministrativeAndPublicExpenses_all():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" where p."tableID"='Administrative and public expenses' and p."report_ID" not in (Select "report_ID" from statementnotes."AdministrativeAndPublicExpenses")
        """, connection)
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_AdministrativeAndPublicExpenses():
    df=AdministrativeAndPublicExpenses_all()
    for index,row in df.iterrows():
        if(index%200==0):
            print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        df1=df1.drop(columns=[df1.columns[-2]])
        if len(df1.columns)!=6:
            print('Err')
        toDates=[]
        for col in df1.columns:
            if re.search('\d\d\d\d/\d\d/\d\d',col):
                ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                if ss!="" and ss not in toDates:
                    toDates.append(ss)
        df1 = df1.rename(columns={'شرح ': 'title'})
        for col in df1.columns:
            if min(toDates) in col:
                df1 = df1.rename(columns={col: 'lastyear'})
            if max(toDates) in col:
                df1 = df1.rename(columns={col: 'nextPeriodPrediction'})
            if toDates[1] in col:
                df1 = df1.rename(columns={col: 'CurrentPeriod'})
        df1['LastYear']=min(toDates)
        df1['NextPeriod']=max(toDates)
        df1['CurrentperiodDate']=toDates[1]
        df1['report_ID']=row['report_ID']
        df1=df1[(df1['title'].notna())]
        InsertAdministrativeExpenses(df1)  
def InsertAdministrativeExpenses(DF4):
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
                IF NOT EXISTS (select from statementnotes."AdministrativeAndPublicExpenses" where "report_ID"=%(report_ID)s and title=%(title)s) THEN
                INSERT INTO statementnotes."AdministrativeAndPublicExpenses"(
                    "report_ID", title, "LastYear", "ThisPeriod", "NextPeriodPrediction", "LastYearDate", "CurrentPeriodDate", "NextPeriodDate")
	VALUES (%(report_ID)s,%(title)s, %(lastyear)s,%(CurrentPeriod)s,%(nextPeriodPrediction)s,%(LastYear)s,%(CurrentperiodDate)s,%(NextPeriod)s);
                END IF;
            END
            $$ 

        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print('Failed to Update Data ', error)
                #log_it('Failed to Update OtherLayers ')
                #return False
            
    finally:
        if(connection):
            cursor.close()
            connection.close()     
def AllocatedFacility():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" where p."tableID"='Allocated Facility Detail in the Last Period'
        and p."report_ID" not in (Select "report_ID" from statementnotes."AllocatedFacilityDetail")
        """, connection)
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_AllocatedFacility():
    df=AllocatedFacility()
    for index,row in df.iterrows():
        if(index%200==0):
            print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        # for t in df1.columns:
        #     if t not in allcols:
        #         allcols.append(t)
        df1 = df1.rename(columns={'نوع ': 'title'})
        df1 = df1.rename(columns={'مبلغ - ميليون ريال ': 'fee'})
        df1 = df1.rename(columns={'متوسط نرخ سود ': 'AverageInterest'})
        df1 = df1.rename(columns={'مبلغ درآمد طي دوره ': 'Rev'})
        df1 = df1.rename(columns={'درآمد تخفيفات تجاري ': 'RevDisc'})
        df1['report_ID']=row['report_ID']
        InsertAllocatedFacility(df1)  
def InsertAllocatedFacility(DF4):
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
                IF NOT EXISTS (select from statementnotes."AllocatedFacilityDetail" where "report_ID"=%(report_ID)s and title=%(title)s) THEN
                INSERT INTO statementnotes."AllocatedFacilityDetail"(
	            "report_ID", title, fee, interest, "PeriodRevenue", "RevenueFromDiscount")
	VALUES (%(report_ID)s,%(title)s,%(fee)s,%(AverageInterest)s,%(Rev)s,%(RevDisc)s);
                END IF;
            END
            $$ 

        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print('Failed to Update Data ', error)
                #log_it('Failed to Update OtherLayers ')
                #return False
            
    finally:
        if(connection):
            cursor.close()
            connection.close()   
def CombineCostOfPortfolio():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" where p."tableID"='Combine the cost of the portfolio of the company'
        and p."report_ID" not in (Select "report_ID" from statementnotes."CombinedCostOfPortfolio")   
        """, connection)
        
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_CombineCostOfPortfolio():
    df=CombineCostOfPortfolio()
    for index,row in df.iterrows():
        if(index%200==0):
            print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        toDates=[]
        for col in df1.columns:
            if re.search('\d\d\d\d/\d\d/\d\d',col):
                ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                if ss!="" and ss not in toDates:
                    toDates.append(ss)
                
        
        df1 = df1.rename(columns={'شرح ': 'title'})
        for col in df1.columns:
            if min(toDates) in col:
                df1 = df1.rename(columns={col: 'StartCost'})
            if max(toDates) in col:
                df1 = df1.rename(columns={col: 'EndCost'})
            if 'توضیحات تکمیلی شرکت' in col:
                df1 = df1.rename(columns={col: 'Detail'})
        df1['EndPeriod']=min(toDates)
        df1['StartPeriod']=max(toDates)
        df1['report_ID']=row['report_ID']
        InsertCombineCostOfPortfolio(df1)  
def InsertCombineCostOfPortfolio(DF4):
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
                IF NOT EXISTS (select from statementnotes."CombinedCostOfPortfolio" where "report_ID"=%(report_ID)s and title=%(title)s) THEN
                INSERT INTO statementnotes."CombinedCostOfPortfolio"(
                "report_ID", title, "StartCost", "EndCost", "StartPeriod", "EndPeriod", "Details")
	VALUES (%(report_ID)s,%(title)s,%(StartCost)s,%(EndCost)s,%(StartPeriod)s,%(EndPeriod)s,%(Detail)s);
                END IF;
            END
            $$ 

        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print('Failed to Update Data ', error)
                #log_it('Failed to Update OtherLayers ')
                #return False
            
    finally:
        if(connection):
            cursor.close()
            connection.close()                
def EstimationOfAllocatedFacilitiesCostChange():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" 
        where p."tableID"='Companies Estimation of Allocated Facilities Cost Change' and p."report_ID" not in 
        (select "report_ID" from statementnotes."GeneralStatements" where "engTitle"='Companies Estimation of Allocated Facilities Cost Change')
        """, connection)
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_EstimationOfAllocatedFacilitiesCostChange():
    df=EstimationOfAllocatedFacilitiesCostChange()
    allcols=[]
    for index,row in df.iterrows():
        if(index%200==0):
            print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        df1['title']=['برآورد شرکت از تغییرات نرخ تسهیلات اعطایی و نرخ تامین مالی و نحوه تامین مالی']
        df1['engtitle']=['Companies Estimation of Allocated Facilities Cost Change']
        df1['report_ID']=row['report_ID']
        df1 = df1.where(pd.notnull(df1), None)
        if (df1.desc[0] is not None)and( df1.desc[0]!=0)and( df1.desc[0]!=1) :
            InsertGeneralStatements(df1) 
        else:
            print('Should Be Deleted')
            print(row['Datatable'])
            DeleteFromPreNotes_NullGE(df1)
def CompanyEstimatesofChangesincostfactors():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" where p."tableID"='Company estimates of changes in cost factors'
        and p."report_ID" not in (select "report_ID" from statementnotes."GeneralStatements" where "engTitle"='Company Estimates of Changes in cost factors')
        """, connection)
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_CompanyEstimatesofChangesincostfactors():
    df=CompanyEstimatesofChangesincostfactors()
    for index,row in df.iterrows():
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        df1['title']=['برآورد شرکت از تغییرات عوامل بهای تمام شده']
        df1['engtitle']=['Company Estimates of Changes in cost factors']
        df1['report_ID']=row['report_ID']
        df1 = df1.where(pd.notnull(df1), None)
        if (df1.desc[0] is not None)and( df1.desc[0]!=0)and( df1.desc[0]!=1) :
            InsertGeneralStatements(df1) 
        else:
            print('Should Be Deleted')
            print(row['Datatable'])
            DeleteFromPreNotes_NullGE(df1)
def CompanyEstimatesofChangesinFacilityBalances():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" where p."tableID"='Company estimates of changes in facility balances' 
        and p."report_ID" not in (select "report_ID" from statementnotes."GeneralStatements" where "engTitle"='Company estimates of changes in facility balances')
        """, connection)
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_CompanyEstimatesofChangesinFacilityBalances():
    df=CompanyEstimatesofChangesinFacilityBalances()
    for index,row in df.iterrows():
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        df1['title']=['برآورد شرکت از تغییرات در مانده تسهیلات']
        df1['engtitle']=['Company estimates of changes in facility balances']
        df1['report_ID']=row['report_ID']
        df1 = df1.where(pd.notnull(df1), None)
        if (df1.desc[0] is not None)and( df1.desc[0]!=0)and( df1.desc[0]!=1) :
            InsertGeneralStatements(df1) 
        else:
            print('Should Be Deleted')
            print(row['Datatable'])
            DeleteFromPreNotes_NullGE(df1)
def CompanyEstimatesofChangesinpublicExpenses():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" where p."tableID"='Company estimates of changes in public'  
        and p."report_ID" not in (select "report_ID" from statementnotes."GeneralStatements" where "engTitle"='Company estimates of changes in public Expenses')
        """, connection)
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_CompanyEstimatesofChangesinpublicExpenses():
    df=CompanyEstimatesofChangesinpublicExpenses()
    for index,row in df.iterrows():
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        df1['title']=['برآورد شرکت از تغییرات هزینه های عمومی اداری و خالص سایر درآمدها (هزینه های) عملیاتی']
        df1['engtitle']=['Company estimates of changes in public Expenses']
        df1['report_ID']=row['report_ID']
        if (df1.desc[0] is not None)and( df1.desc[0]!=0)and( df1.desc[0]!=1) :
            InsertGeneralStatements(df1) 
        else:
            print('Should Be Deleted')
            print(row['Datatable'])
            DeleteFromPreNotes_NullGE(df1)
def CompanyEstimatesofChangesinreservebalancesofdoubtfulreceipts():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" where p."tableID"='Company estimates of changes in reserve balances of doubtful receipts' 
        and p."report_ID" not in (select "report_ID" from statementnotes."GeneralStatements" where "engTitle"='Company estimates of changes in reserve balances of doubtful receipts')
        """, connection)
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_CompanyEstimatesofChangesinreservebalancesofdoubtfulreceipts():
    df=CompanyEstimatesofChangesinreservebalancesofdoubtfulreceipts()
    for index,row in df.iterrows():
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        df1['title']=['برآورد شرکت از تغییرات در مانده ذخایر مطالبات مشکوک الوصول']
        df1['engtitle']=['Company estimates of changes in reserve balances of doubtful receipts']
        df1['report_ID']=row['report_ID']
        df1 = df1.where(pd.notnull(df1), None)
        if (df1.desc[0] is not None)and( df1.desc[0]!=0)and( df1.desc[0]!=1) :
            InsertGeneralStatements(df1) 
        else:
            print('Should Be Deleted')
            print(row['Datatable'])
            DeleteFromPreNotes_NullGE(df1)
def Companyestimatesofchangesintherateofinterestonconcessionalfacilities():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" where p."tableID"='Company estimates of changes in the rate of interest on concessional facilities'  
        and p."report_ID" not in (select "report_ID" from statementnotes."GeneralStatements" where "engTitle"='Company estimates of changes in the rate of interest on concessional facilities')
        """, connection)
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_Companyestimatesofchangesintherateofinterestonconcessionalfacilities():
    df=Companyestimatesofchangesintherateofinterestonconcessionalfacilities()
    for index,row in df.iterrows():
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        df1['title']=['برآورد شرکت از تغییرات در نرخ سود تسهیلات اعطایی']
        df1['engtitle']=['Company estimates of changes in the rate of interest on concessional facilities']
        df1['report_ID']=row['report_ID']
        if (df1.desc[0] is not None)and( df1.desc[0]!=0)and( df1.desc[0]!=1) :
            InsertGeneralStatements(df1) 
        else:
            print('Should Be Deleted')
            print(row['Datatable'])
            DeleteFromPreNotes_NullGE(df1)
def Companyestimatesofchangesintherateofsalesofproductsandthepurchasepriceofrawmaterials():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" where p."tableID"='Company estimates of changes in the rate of sales of products and the purchase price of raw materials'  
        and p."report_ID" not in (select "report_ID" from statementnotes."GeneralStatements" where "engTitle"='Company estimates of changes in the rate of sales of products and the purchase price of raw materials')
        """, connection)
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_Companyestimatesofchangesintherateofsalesofproductsandthepurchasepriceofrawmaterials():
    df=Companyestimatesofchangesintherateofsalesofproductsandthepurchasepriceofrawmaterials()
    for index,row in df.iterrows():
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        df1['title']=['برآورد شرکت از تغییرات نرخ فروش محصولات و نرخ خرید مواد اولیه']
        df1['engtitle']=['Company estimates of changes in the rate of sales of products and the purchase price of raw materials']
        df1['report_ID']=row['report_ID']
        df1 = df1.where(pd.notnull(df1), None)
        if (df1.desc[0] is not None)and( df1.desc[0]!=0)and( df1.desc[0]!=1) :
            InsertGeneralStatements(df1) 
        else:
            print('Should Be Deleted')
            print(row['Datatable'])
            DeleteFromPreNotes_NullGE(df1)
def Companyestimatesoffinancingprogramsandcompanyfinancechanges():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" where (p."tableID"='Company estimates of financing programs and company finance changes' or p."tableID"='Company Estimation of Financial Supply Plans and Company Financial Expense Changes')
        and p."report_ID" not in (select "report_ID" from statementnotes."GeneralStatements" where "engTitle"='Company estimates of financing programs and company finance changes')
        """, connection)
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_Companyestimatesoffinancingprogramsandcompanyfinancechanges():
    df=Companyestimatesoffinancingprogramsandcompanyfinancechanges()
    for index,row in df.iterrows():
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        df1['title']=['برآورد شرکت از برنامه های تامین مالی و تغییرات هزینه های مالی شرکت']
        df1['engtitle']=['Company estimates of financing programs and company finance changes']
        df1['report_ID']=row['report_ID']
        if (df1.desc[0] is not None)and( df1.desc[0]!=0)and( df1.desc[0]!=1) :
            InsertGeneralStatements(df1) 
        else:
            print('Should Be Deleted')
            print(row['Datatable'])
            DeleteFromPreNotes_NullGE(df1)
def CompanyEstimationofRecivedDepositsProfitRate():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" where
        (p."tableID"='Company Estimation of Recived Deposits Profit Rate'  or p."tableID"='Company Estimation of Recived Deposits Profit Rate ')
        and p."report_ID" not in (select "report_ID" from statementnotes."GeneralStatements" where "engTitle"='Company Estimation of Recived Deposits Profit Rate')
        """, connection)
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_CompanyEstimationofRecivedDepositsProfitRate():
    df=CompanyEstimationofRecivedDepositsProfitRate()
    for index,row in df.iterrows():
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        df1['title']=['برآورد شرکت از تغیرات در نرخ سود سپرده های دریافتی']
        df1['engtitle']=['Company Estimation of Recived Deposits Profit Rate']
        df1['report_ID']=row['report_ID']
        df1 = df1.where(pd.notnull(df1), None)
        if (df1.desc[0] is not None)and( df1.desc[0]!=0)and( df1.desc[0]!=1) :
            InsertGeneralStatements(df1) 
        else:
            print('Should Be Deleted')
            print(row['Datatable'])
            DeleteFromPreNotes_NullGE(df1)
def CompanyEstimationOfTheChangeInTheRestofDeposits():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" where p."tableID"='Company Estimation Of The Change In The Rest of Deposits'  
        and p."report_ID" not in (select "report_ID" from statementnotes."GeneralStatements" where "engTitle"='Company Estimation Of The Change In The Rest of Deposits')
        """, connection)
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_CompanyEstimationOfTheChangeInTheRestofDeposits():
    df=CompanyEstimationOfTheChangeInTheRestofDeposits()
    for index,row in df.iterrows():
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        df1['title']=['برآورد شرکت از تغیرات در مانده سپرده ها']
        df1['engtitle']=['Company Estimation Of The Change In The Rest of Deposits']
        df1['report_ID']=row['report_ID']
        df1 = df1.where(pd.notnull(df1), None)
        if (df1.desc[0] is not None)and( df1.desc[0]!=0)and( df1.desc[0]!=1) :
            InsertGeneralStatements(df1) 
        else:
            print('Should Be Deleted')
            print(row['Datatable'])
            DeleteFromPreNotes_NullGE(df1)
def CompanyEstimationofthegeneralExpenses():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" where p."tableID"='Company Estimation of the general'
        and p."report_ID" not in (select "report_ID" from statementnotes."GeneralStatements" where "engTitle"='Company Estimation of the general Expenses')  
        """, connection)
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_CompanyEstimationofthegeneralExpenses():
    df=CompanyEstimationofthegeneralExpenses()
    for index,row in df.iterrows():
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        df1['title']=['برآورد شرکت از تغییرات هزینه های عمومی اداری تشکیلاتی']
        df1['engtitle']=['Company Estimation of the general Expenses']
        df1['report_ID']=row['report_ID']
        df1 = df1.where(pd.notnull(df1), None)
        if (df1.desc[0] is not None)and( df1.desc[0]!=0)and( df1.desc[0]!=1) :
            InsertGeneralStatements(df1) 
        else:
            print('Should Be Deleted')
            print(row['Datatable'])
            DeleteFromPreNotes_NullGE(df1)
def Companyplansdescriptioninordertocompletedevelopingschemes():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on  
        p."report_ID"=s."TracingNo" where p."tableID"='Company plans description in order to complete developing schemes' 
        and p."report_ID" not in (Select "report_ID" from statementnotes."CompanyDevelopingSchemas") 
        """, connection)
        #  
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_Companyplansdescriptioninordertocompletedevelopingschemes():
    df=Companyplansdescriptioninordertocompletedevelopingschemes()
    for index,row in df.iterrows():
        if(index%200==0):
            print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        toDates=[]
        if len(df1.columns)!=15:
            print('Err')
        for col in df1.columns:
            if re.search('\d\d\d\d/\d\d/\d\d',col):
                ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                if ss!="" and ss not in toDates:
                    toDates.append(ss)
                
        toDates.sort()
        df1 = df1.rename(columns={'طرح هاي عمده در دست اجرا ': 'projectTitle'})
        df1 = df1.rename(columns={'هزينه هاي برآوردي ريالي طرح - ميليون ريال ': 'EstimatedRialCost'})
        df1 = df1.rename(columns={'هزينه هاي برآوردي ارزي طرح ': 'EstimatedDollarCost'})
        df1 = df1.rename(columns={'نوع ارز ': 'ForeignCurrency'})
        df1 = df1.rename(columns={'هزينه هاي برآوردي تکميل طرح- ميليون ريال ': 'RemainingCostsEstimation'})
        df1 = df1.rename(columns={'تاريخ برآوردي بهره برداري از طرح ': 'EstimatedCompletion'})
        df1 = df1.rename(columns={'تشريح تاثير طرح در فعاليتهاي آتي شرکت ': 'ProjectGoalandImpacts'})
        df1 = df1.rename(columns={'توضيحات ': 'moreDesc'})
        for col in df1.columns:
            if toDates[1] in col and 'هزينه هاي انجام شده' in col:
                df1 = df1.rename(columns={col: 'AllCostsUntilNow'})
            if toDates[0] in col and 'درصد پيشرفت طرح' in col:
                df1 = df1.rename(columns={col: 'StartOfPeriodProgress'})
            if toDates[1] in col and 'درصد پيشرفت طرح' in col:
                df1 = df1.rename(columns={col: 'NowProgress'})
            if toDates[2] in col and 'درصد پيشرفت برآوردي طرح' in col:
                df1 = df1.rename(columns={col: 'ShortTermProgressEstimation'})
            try:
                if toDates[3] in col and 'درصد پيشرفت برآوردي طرح' in col:
                    df1 = df1.rename(columns={col: 'LongTermProgressEstimation'})
            except:
                if toDates[2] not in col and 'درصد پيشرفت برآوردي طرح' in col:
                    df1 = df1.rename(columns={col: 'LongTermProgressEstimation'})
        df1['StartPeriod']=toDates[0]
        df1['CurrentPeriod']=toDates[1]
        df1['NextPeriodShortTerm']=toDates[2]
        
        try:
            df1['NextPeriodLongTerm']=toDates[3]
        except:
            df1['NextPeriodLongTerm']=""
        df1['report_ID']=row['report_ID']
        INSERTCompanyplansdescriptioninordertocompletedevelopingschemes(df1)  
def INSERTCompanyplansdescriptioninordertocompletedevelopingschemes(DF4):
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
                IF NOT EXISTS (select from statementnotes."CompanyDevelopingSchemas" where "report_ID"=%(report_ID)s and "projectTitle"=%(projectTitle)s) THEN
                INSERT INTO statementnotes."CompanyDevelopingSchemas"(
        "report_ID", "projectTitle", "EstimatedRialCost", "EstimatedForeignCurrencyCost", "ForeignCurrency", "AllCostsuntilNow", "RemainingCostsEstimation", "StartOfPeriodProgress", "NowProgress", "LongTermProgressEstimation", "ShortTermProgressEstimation", "EstimatedCompletion", "ProjectGoalandImpacts", "StartPeriod", "CurrentPeriod", "NextPeriodShortTerm", "NextPeriodLongTerm","moreDetails")
        VALUES (%(report_ID)s, %(projectTitle)s, %(EstimatedRialCost)s, %(EstimatedDollarCost)s, %(ForeignCurrency)s, %(AllCostsUntilNow)s, %(RemainingCostsEstimation)s, %(StartOfPeriodProgress)s, %(NowProgress)s, %(LongTermProgressEstimation)s, %(ShortTermProgressEstimation)s, %(EstimatedCompletion)s, %(ProjectGoalandImpacts)s, %(StartPeriod)s, %(CurrentPeriod)s, %(NextPeriodShortTerm)s, %(NextPeriodLongTerm)s, %(moreDesc)s);
                END IF;
            END
            $$ 

        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print(DF4.report_ID)
                print('Failed to Update Data ', error)
                #log_it('Failed to Update OtherLayers ')
                #return False
            
    finally:
        if(connection):
            cursor.close()
            connection.close()           
def CompanyPlansExpressionforCompletingDevelopmentPlansexceptforBuildingProjects():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" 
        where p."tableID"='Company Plans Expression for Completing Development Plans(except for Building Projects) ' 
        and p."report_ID" not in (Select "report_ID" from statementnotes."CompanyDevelopingSchemasExceptBuilding") 
        """, connection)
        #  
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_CompanyPlansExpressionforCompletingDevelopmentPlansexceptforBuildingProjects():
    df=CompanyPlansExpressionforCompletingDevelopmentPlansexceptforBuildingProjects()
    for index,row in df.iterrows():
        if(index%200==0):
            print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        toDates=[]
        if len(df1.columns)!=15:
            print('Err')
        for col in df1.columns:
            if re.search('\d\d\d\d/\d\d/\d\d',col):
                ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                if ss!="" and ss not in toDates:
                    toDates.append(ss)
                
        toDates.sort()
        df1 = df1.rename(columns={'طرح هاي عمده در دست اجرا ': 'projectTitle'})
        df1 = df1.rename(columns={'هزينه هاي برآوردي ريالي طرح - ميليون ريال ': 'EstimatedRialCost'})
        df1 = df1.rename(columns={'هزينه هاي برآوردي ارزي طرح ': 'EstimatedDollarCost'})
        df1 = df1.rename(columns={'نوع ارز ': 'ForeignCurrency'})
        df1 = df1.rename(columns={'هزينه هاي برآوردي تکميل طرح- ميليون ريال ': 'RemainingCostsEstimation'})
        df1 = df1.rename(columns={'تاريخ برآوردي بهره برداري از طرح ': 'EstimatedCompletion'})
        df1 = df1.rename(columns={'تشريح تاثير طرح در فعاليتهاي آتي شرکت ': 'ProjectGoalandImpacts'})
        df1 = df1.rename(columns={'توضيحات ': 'moreDesc'})
        for col in df1.columns:
            if toDates[1] in col and 'هزينه هاي انجام شده' in col:
                df1 = df1.rename(columns={col: 'AllCostsUntilNow'})
            if toDates[0] in col and 'درصد پيشرفت فيزيکي طرح' in col:
                df1 = df1.rename(columns={col: 'StartOfPeriodProgress'})
            if toDates[1] in col and 'درصد پيشرفت فيزيکي طرح' in col:
                df1 = df1.rename(columns={col: 'NowProgress'})
            if toDates[2] in col and 'درصد پيشرفت فيزيکي برآوردي' in col:
                df1 = df1.rename(columns={col: 'ShortTermProgressEstimation'})
            try:
                if toDates[3] in col and 'درصد پيشرفت فيزيکي برآوردي' in col:
                    df1 = df1.rename(columns={col: 'LongTermProgressEstimation'})
            except:
                if toDates[2] not in col and 'درصد پيشرفت فيزيکي برآوردي' in col:
                    df1 = df1.rename(columns={col: 'LongTermProgressEstimation'})
        df1['StartPeriod']=toDates[0]
        df1['CurrentPeriod']=toDates[1]
        df1['NextPeriodShortTerm']=toDates[2]
        
        try:
            df1['NextPeriodLongTerm']=toDates[3]
        except:
            df1['NextPeriodLongTerm']=""
        df1['report_ID']=row['report_ID']
        INSERTCompanyPlansExpressionforCompletingDevelopmentPlansexceptforBuildingProjects(df1)
def INSERTCompanyPlansExpressionforCompletingDevelopmentPlansexceptforBuildingProjects(DF4):
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
                IF NOT EXISTS (select from statementnotes."CompanyDevelopingSchemasExceptBuilding" where "report_ID"=%(report_ID)s and "projectTitle"=%(projectTitle)s) THEN
                INSERT INTO statementnotes."CompanyDevelopingSchemasExceptBuilding"(
	"report_ID", "projectTitle", "EstimatedRialCost", "EstimatedForeignCurrencyCost", "ForeignCurrency", "AllCostsuntilNow", "RemainingCostsEstimation", "StartOfPeriodProgress", "NowProgress", "LongTermProgressEstimation", "ShortTermProgressEstimation", "EstimatedCompletion", "ProjectGoalandImpacts", "StartPeriod", "CurrentPeriod", "NextPeriodShortTerm", "NextPeriodLongTerm","moreDetails")
	VALUES (%(report_ID)s, %(projectTitle)s, %(EstimatedRialCost)s, %(EstimatedDollarCost)s, %(ForeignCurrency)s, %(AllCostsUntilNow)s, %(RemainingCostsEstimation)s, %(StartOfPeriodProgress)s, %(NowProgress)s, %(LongTermProgressEstimation)s, %(ShortTermProgressEstimation)s, %(EstimatedCompletion)s, %(ProjectGoalandImpacts)s, %(StartPeriod)s, %(CurrentPeriod)s, %(NextPeriodShortTerm)s, %(NextPeriodLongTerm)s, %(moreDesc)s);
                END IF;
            END
            $$ 

        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print(DF4.report_ID)
                print('Failed to Update Data ', error)
                #log_it('Failed to Update OtherLayers ')
                #return False
            
    finally:
        if(connection):
            cursor.close()
            connection.close() 
def Companypredictaionofoperationalsupportplansandcompanysfinancialchanges():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on
        p."report_ID"=s."TracingNo" where p."tableID"='Company predictaion of operational support plans and company''s financial changes'
        and p."report_ID" not in (select "report_ID" from statementnotes."GeneralStatements" 
        where "engTitle"='Company predictaion of operational support plans and company''s financial changes')
        """, connection)
        #  select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo"
        # where p."tableID" like '%Company predictaion of operational support plans and company%'  and p."report_ID" not in (select "report_ID" from statementnotes."GeneralStatements" where "engTitle" like '%Company predictaion of operational support plans and company%' )
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close()                
def handle_Companypredictaionofoperationalsupportplansandcompanysfinancialchanges():
    df=Companypredictaionofoperationalsupportplansandcompanysfinancialchanges()
    for index,row in df.iterrows():
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        df1['title']=['برآورد شرکت از برنامه های تامین مالی و تغییرات هزینه های مالی شرکت']
        df1['engtitle']=["Company predictaion of operational support plans and company's financial changes"]
        df1['report_ID']=row['report_ID']
        if (df1.desc[0] is not None)and( df1.desc[0]!=0)and( df1.desc[0]!=1) :
            InsertGeneralStatements(df1) 
        else:
            print('Should Be Deleted')
            print(row['Datatable'])
            DeleteFromPreNotes_NullGE(df1)
def CompanyPredictationofthegeneralExpenses():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo"
        where p."tableID"='Company Predictation of the general' 
         and p."report_ID" not in (select "report_ID" from statementnotes."GeneralStatements" where "engTitle"='Company Predictation of the general Expenses' )
        """, connection)
        #  
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_CompanyPredictationofthegeneralExpenses():
    df=CompanyPredictationofthegeneralExpenses()
    for index,row in df.iterrows():
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        df1['title']=['برآورد شرکت از تغییرات در هزینه های عمومی اداری تشکیلاتی و خالص  سایر درآمدها (هزینه ها) عملیاتی']
        df1['engtitle']=["Company Predictation of the general Expenses"]
        df1['report_ID']=row['report_ID']
        df1 = df1.where(pd.notnull(df1), None)
        if (df1.desc[0] is not None)and( df1.desc[0]!=0)and( df1.desc[0]!=1) :
            InsertGeneralStatements(df1) 
        else:
            print('Should Be Deleted')
            print(row['Datatable'])
            DeleteFromPreNotes_NullGE(df1)
def Companypredictionofprimecostfactors():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo"
        where p."tableID"='Company prediction of prime cost factors' 
        and p."report_ID" not in (select "report_ID" from statementnotes."GeneralStatements" where "engTitle"='Company prediction of prime cost factors' )
        """, connection)
        #  
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_Companypredictionofprimecostfactors():
    df=Companypredictionofprimecostfactors()
    for index,row in df.iterrows():
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        df1['title']=['برآورد شرکت از تعییرات عوامل بهای تمام شده']
        df1['engtitle']=["Company prediction of prime cost factors"]
        df1['report_ID']=row['report_ID']
        df1 = df1.where(pd.notnull(df1), None)
        if (df1.desc[0] is not None)and( df1.desc[0]!=0)and( df1.desc[0]!=1) :
            InsertGeneralStatements(df1) 
        else:
            print('Should Be Deleted')
            print(row['Datatable'])
            DeleteFromPreNotes_NullGE(df1)
def Companysplanfordivisionofprofit():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
            select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo"
            where  (p."tableID"='Company''s plan for division of profit' or p."tableID"='Participation plan for profit sharing')
            and p."report_ID" not in(select "report_ID" from statementnotes."CompanyPlansForDivisionOfProfit")
        """, connection)
        #  
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def INSERTCompanysplanfordivisionofprofit(DF4):
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
                IF NOT EXISTS (select from statementnotes."CompanyPlansForDivisionOfProfit" where "report_ID"=%(report_ID)s) THEN
                INSERT INTO statementnotes."CompanyPlansForDivisionOfProfit"(
                    "report_ID", "RetainedEarningLastYear", "BoardProposedDPSLastYear", "EarningLastYear", "DividendLastYear", "BoardProposedDPS")
	VALUES (%(report_ID)s, %(RetainedEarningLastYear)s, %(BoardProposedDPSLastYear)s, %(EarningLastYear)s, %(DividendLastYear)s, %(BoardProposedDPS)s);
                END IF;
            END
            $$ 

        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print(DF4.report_ID)
                print('Failed to Update Data ', error)
                #log_it('Failed to Update OtherLayers ')
                #return False
            
    finally:
        if(connection):
            cursor.close()
            connection.close()        
def handle_Companysplanfordivisionofprofit():
    df=Companysplanfordivisionofprofit()
    allcols=[]
    for index,row in df.iterrows():
        if(index%200==0):
            print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        for i in df1.columns:
            if i not in allcols:
                allcols.append(i)
        if len(df1.columns) !=7:
            print('err')
        df1.replace('ک','ك',regex=True,inplace=True)
        df1.replace('ی','ي',regex=True,inplace=True)
        df1.columns= df1.columns.str.replace('ي','ی',regex=True)
        df1.columns= df1.columns.str.replace('ي','ی',regex=True)
        df1 = df1.rename(columns={'مبلغ سود انباشته پایان سال مالی گذشته ': 'RetainedEarningLastYear'})
        df1 = df1.rename(columns={'سود سهام پیشنهادی هیئت مدیره در سال مالی گذشته ': 'BoardProposedDPSLastYear'})
        df1 = df1.rename(columns={'مبلغ سود خالص سال مالی گذشته ': 'EarningLastYear'})
        df1 = df1.rename(columns={'سود سهام مصوب مجمع سال مالی گذشته ': 'DividendLastYear'})
        df1 = df1.rename(columns={'پیشنهاد هیئت مدیره درخصوص درصد تقسیم سود سال مالی جاری ': 'BoardProposedDPS'})
        df1['report_ID']=row['report_ID']
        INSERTCompanysplanfordivisionofprofit(df1)
def Companyspredictionoffullpricechangefactors():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
         select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo"
        where p."tableID"='Company''s prediction of full price change factors' and p."report_ID" not in 
        (select "report_ID" from statementnotes."GeneralStatements" where "engTitle"='Company prediction of full price change factors' )

        """, connection)
        #   
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_Companyspredictionoffullpricechangefactors():
    df=Companyspredictionoffullpricechangefactors()
    for index,row in df.iterrows():
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        df1['title']=['برآورد شرکت از تعییرات عوامل بهای تمام شده']
        df1['engtitle']=["Company prediction of full price change factors"]
        df1['report_ID']=row['report_ID']
        df1 = df1.where(pd.notnull(df1), None)
        if (df1.desc[0] is not None)and( df1.desc[0]!=0)and( df1.desc[0]!=1) :
            InsertGeneralStatements(df1) 
        else:
            print('Should Be Deleted')
            print(row['Datatable'])
            DeleteFromPreNotes_NullGE(df1)
def ConsumptionItemlist():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo"
        where  p."tableID"='Consumption Item list' and p."report_ID" not in(select "report_ID" from statementnotes."ConsumptionList")
        """, connection)
        #   
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def INSERTConsumptionItemlist(DF4):
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
                IF NOT EXISTS (select from statementnotes."ConsumptionList" where "report_ID"=%(report_ID)s and "title"=%(itemTitle)s) THEN
                INSERT INTO statementnotes."ConsumptionList"(
                "report_ID", title, "LastYearValue", "CurrentValue", "ShortTermEstimation", "LongTermEstimation", "StartPeriod", "CurrentPeriod", "NextPeriodShortTerm", "NextPeriodLongTerm")
	VALUES (%(report_ID)s,%(itemTitle)s, %(LastYearValue)s, %(CurrentPeriodValue)s, %(ShortTermProgressEstimation)s, %(LongTermProgressEstimation)s, %(StartPeriod)s, %(CurrentPeriod)s, %(NextPeriodShortTerm)s, %(NextPeriodLongTerm)s);
                END IF;
            END
            $$ 

        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print(DF4.report_ID)
                print('Failed to Update Data ', error)
                #log_it('Failed to Update OtherLayers ')
                #return False
            
    finally:
        if(connection):
            cursor.close()
            connection.close()        
def handle_ConsumptionItemlist():
    df=ConsumptionItemlist()
    allcols=[]
    for index,row in df.iterrows():
        if(index%200==0):
            print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        for i in df1.columns:
            if i not in allcols:
                allcols.append(i)
        toDates=[]
        for col in df1.columns:
                if re.search('\d\d\d\d/\d\d/\d\d',col):
                    ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                    if ss!="" and ss not in toDates:
                        toDates.append(ss)
        toDates.sort()
        df1 = df1.rename(columns={'شرح مصارف ': 'itemTitle'})
        for col in df1.columns:
            if toDates[1] in col :
                df1 = df1.rename(columns={col: 'CurrentPeriodValue'})
            if toDates[0] in col:
                df1 = df1.rename(columns={col: 'LastYearValue'})
            if toDates[2] in col:
                df1 = df1.rename(columns={col: 'ShortTermProgressEstimation'})
            
            try:
                if toDates[3] in col:
                    df1 = df1.rename(columns={col: 'LongTermProgressEstimation'})
            except:
                if toDates[2] not in col and 'برآورد' in col:
                    df1 = df1.rename(columns={col: 'LongTermProgressEstimation'})
        df1['StartPeriod']=toDates[0]
        df1['CurrentPeriod']=toDates[1]
        df1['NextPeriodShortTerm']=toDates[2]
        
        try:
            df1['NextPeriodLongTerm']=toDates[3]
        except:
            df1['NextPeriodLongTerm']=""
        df1['report_ID']=row['report_ID']
        INSERTConsumptionItemlist (df1)   
def Corporateincomeprogram():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo"
        where (p."tableID"='Corporate income program' or p."tableID"='Corporate Income Program'  )
        and p."report_ID" not in(select "report_ID" from statementnotes."CompanyPlansForDivisionOfProfit")
        """, connection)
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_Corporateincomeprogram():
    df=Corporateincomeprogram()
    allcols=[]
    for index,row in df.iterrows():
        if(index%200==0):
            print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        for i in df1.columns:
            if i not in allcols:
                allcols.append(i)
        if len(df1.columns)!=7:
            print('error')
        df1.replace('ک','ك',regex=True,inplace=True)
        df1.replace('ی','ي',regex=True,inplace=True)
        df1.columns= df1.columns.str.replace('ي','ی',regex=True)
        df1.columns= df1.columns.str.replace('ي','ی',regex=True)
        df1 = df1.rename(columns={'مبلغ سود انباشته پایان سال مالی گذشته ': 'RetainedEarningLastYear'})
        df1 = df1.rename(columns={'مبلغ سود (زیان) انباشته پایان سال مالی گذشته ': 'RetainedEarningLastYear'})
        df1 = df1.rename(columns={'سود سهام پیشنهادی هیئت مدیره در سال مالی گذشته ': 'BoardProposedDPSLastYear'})
        df1 = df1.rename(columns={'مبلغ سود خالص سال مالی گذشته ': 'EarningLastYear'})
        df1 = df1.rename(columns={'مبلغ سود (زیان) خالص سال مالی گذشته ': 'EarningLastYear'})

        df1 = df1.rename(columns={'سود سهام مصوب مجمع سال مالی گذشته ': 'DividendLastYear'})
        df1 = df1.rename(columns={'پیشنهاد هیئت مدیره درخصوص درصد تقسیم سود سال مالی جاری ': 'BoardProposedDPS'})
        df1 = df1.rename(columns={ 'پیشنهاد هیئت مدیره درخصوص تقسیم سود سال مالی جاری (درصد) ': 'BoardProposedDPS'})
        df1['report_ID']=row['report_ID']
        INSERTCompanysplanfordivisionofprofit(df1)
def Corporateoperatingrevenues():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo"
        where p."tableID"='Corporate operating revenues' and p."report_ID" not in (select "report_ID" from statementnotes."CorporateOperatingRevenues")
        """, connection)
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_Corporateoperatingrevenues():
    df=Corporateoperatingrevenues()
    allcols=[]
    for index,row in df.iterrows():
        if(index%200==0):
            print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        for i in df1.columns:
            if i not in allcols:
                allcols.append(i)
        if len(df1.columns)!=6:
            print(len(df1.columns))
        toDates=[]
        for col in df1.columns:
                if re.search('\d\d\d\d/\d\d/\d\d',col):
                    ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                    if ss!="" and ss not in toDates:
                        toDates.append(ss)
        toDates.sort()
        df1 = df1.rename(columns={'شرح ': 'title'})
        df1 = df1.rename(columns={'توضیحات تکمیلی شرکت درخصوص روند درآمد و برآوردهای آتی ': 'ManagerPosition'})
        for col in df1.columns:
            if toDates[0] in col:
                df1 = df1.rename(columns={col: 'LastYearValue'})
            if toDates[1] in col :
                df1 = df1.rename(columns={col: 'CurrentPeriodValue'})
        df1['StartPeriod']=toDates[0]
        df1['CurrentPeriod']=toDates[1]
        df1['report_ID']=row['report_ID']
        INSERTCorporateoperatingrevenues(df1)    
def INSERTCorporateoperatingrevenues(DF4):
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
                IF NOT EXISTS (select from statementnotes."CorporateOperatingRevenues" where "report_ID"=%(report_ID)s and "title"=%(title)s) THEN
                INSERT INTO statementnotes."CorporateOperatingRevenues"(
	"report_ID", title, "LastYearValue", "CurrentPeriodValue", "ManagerDesc", "LastPeriod", "CurrentPeriod")
	VALUES (%(report_ID)s,%(title)s, %(LastYearValue)s, %(CurrentPeriodValue)s, %(ManagerPosition)s, %(StartPeriod)s, %(CurrentPeriod)s);
                END IF;
            END
            $$ 

        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print(DF4.report_ID)
                print('Failed to Update Data ', error)
                #log_it('Failed to Update OtherLayers ')
                #return False
            
    finally:
        if(connection):
            cursor.close()
            connection.close()  
def Currencysituation():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo"
        where (p."tableID"='Currency situation' or p."tableID"='Currency Status' or p."tableID"='Exchange state') and p."report_ID" not in (select "report_ID" from statementnotes."CurrencySituation")
        """, connection)
        
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def InsertCurrencysituation(DF4):
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
                IF NOT EXISTS (select from statementnotes."CurrencySituation" where "report_ID"=%(report_ID)s and "title"=%(title)s) THEN
                INSERT INTO statementnotes."CurrencySituation"(
	"report_ID", title, "Currency", "LastYearCurrencyValue", "LastYearRialValue", "CuurentYearCurrencyValue", "CuurentYearRialValue", "StartPeriod", "CurrentPeriod")
	VALUES (%(report_ID)s,%(title)s, %(Currency)s, %(LastYearCurrencyValue)s, %(LastYearRialValue)s, %(CuurentYearCurrencyValue)s, %(CuurentYearRialValue)s,  %(StartPeriod)s,  %(CurrentPeriod)s);
                END IF;
            END
            $$ 

        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print(DF4.report_ID)
                print('Failed to Update Data ', error)
                #log_it('Failed to Update OtherLayers ')
                #return False
            
    finally:
        if(connection):
            cursor.close()
            connection.close()    
def handle_Currencysituation():
    df=Currencysituation()
    # allcols=[]
    for index,row in df.iterrows():
        if(index%200==0):
            print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        if len(df1.columns)!=8:
            print(len(df1.columns))
        toDates=[]
        for col in df1.columns:
                if re.search('\d\d\d\d/\d\d/\d\d',col):
                    ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                    if ss!="" and ss not in toDates:
                        toDates.append(ss)
        toDates.sort()
        df1 = df1.rename(columns={'شرح ': 'title'})
        df1 = df1.rename(columns={'نوع ارز ': 'Currency'})
        if len(toDates)==2:
            for col in df1.columns:
                if toDates[0] in col and 'مبلغ ارزي' in col:
                    df1 = df1.rename(columns={col: 'LastYearCurrencyValue'})
                if toDates[0] in col and 'ريالي' in col:
                    df1 = df1.rename(columns={col: 'LastYearRialValue'})
                if toDates[1] in col and 'مبلغ ارزي' in col:
                    df1 = df1.rename(columns={col: 'CuurentYearCurrencyValue'})
                if toDates[1] in col and 'ريالي' in col:
                    df1 = df1.rename(columns={col: 'CuurentYearRialValue'})
            df1['StartPeriod']=toDates[0]
            df1['CurrentPeriod']=toDates[1]
            df1['report_ID']=row['report_ID']
        if len(toDates)<2:
            df1.columns=['ii','title','Currency','LastYearCurrencyValue','LastYearRialValue','CuurentYearCurrencyValue','CuurentYearRialValue','metaID']
            df1['StartPeriod']=""
            df1['CurrentPeriod']=""
            df1['report_ID']=row['report_ID']
            # allcols.append(df1)
        InsertCurrencysituation(df1)               
def Damages():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo"
        where p."tableID"='Damages' and p."report_ID" not in (select "report_ID" from statementnotes."Damages")
        """, connection)
        
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_Damages():
    df=Damages()
    allcols=[]
    for index,row in df.iterrows():
        # if(index%200==0):
        #     print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        for i in df1.columns:
            if i not in allcols:
                allcols.append(i)
        if len(df1.columns)!=11:
            print(len(df1.columns))
        toDates=[]
        for col in df1.columns:
                if re.search('\d\d\d\d/\d\d/\d\d',col):
                    ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                    if ss!="" and ss not in toDates:
                        toDates.append(ss)
        toDates.sort()
        df1 = df1.rename(columns={'رشته بيمه ': 'title'})
        # df1 = df1.rename(columns={'نوع ارز ': 'Currency'})
        if len(toDates)==4: 
            for col in df1.columns:
                if toDates[0] in col and 'پرداختي' in col:
                    df1 = df1.rename(columns={col: 'paidforLastYear'})
                if toDates[0] in col and 'دريافتي اتکايي' in col:
                    df1 = df1.rename(columns={col: 'getInLastYear'})
                if toDates[1] in col and 'پرداختي' in col:
                    df1 = df1.rename(columns={col: 'paidforCurrentPeriod'})
                if toDates[1] in col and 'دريافتي اتکايي' in col:
                    df1 = df1.rename(columns={col: 'getInCurrentPeriod'})
                if toDates[2] in col and 'پرداختي' in col:
                    df1 = df1.rename(columns={col: 'paidforShortEstimation'})
                if toDates[2] in col and 'دريافتي اتکايي' in col:
                    df1 = df1.rename(columns={col: 'getInShortEstimation'})
                if toDates[3] in col and 'پرداختي' in col:
                    df1 = df1.rename(columns={col: 'paidforLongEstimation'})
                if toDates[3] in col and 'دريافتي اتکايي' in col:
                    df1 = df1.rename(columns={col: 'getInLongEstimation'})
        else:
            for col in df1.columns:
                if toDates[0] in col and 'پرداختي' in col:
                    df1 = df1.rename(columns={col: 'paidforLastYear'})
                if toDates[0] in col and 'دريافتي اتکايي' in col:
                    df1 = df1.rename(columns={col: 'getInLastYear'})
                if toDates[1] in col and 'پرداختي' in col:
                    df1 = df1.rename(columns={col: 'paidforCurrentPeriod'})
                if toDates[1] in col and 'دريافتي اتکايي' in col:
                    df1 = df1.rename(columns={col: 'getInCurrentPeriod'})
                if toDates[2] in col and 'پرداختي' in col:
                    df1 = df1.rename(columns={col: 'paidforShortEstimation'})
                if toDates[2] in col and 'دريافتي اتکايي' in col:
                    df1 = df1.rename(columns={col: 'getInShortEstimation'})
                if 'پرداختي' in col and toDates[0] not in col and toDates[1] not in col and toDates[2] not in col:
                    df1 = df1.rename(columns={col: 'paidforLongEstimation'})
                if 'دريافتي اتکايي' in col and toDates[0] not in col and toDates[1] not in col and toDates[2] not in col:
                    df1 = df1.rename(columns={col: 'getInLongEstimation'})
        df1['StartPeriod']=toDates[0]
        df1['CurrentPeriod']=toDates[1]
        df1['ShortTermEstimationPeriod']=toDates[2]
        if len(toDates)==4:
            df1['LongTermEstimationPeriod']=toDates[3]
        else:
            df1['LongTermEstimationPeriod']=''
        df1['report_ID']=row['report_ID']  
        InsertDamages(df1)
def InsertDamages(DF4):
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
                IF NOT EXISTS (select from statementnotes."Damages" where "report_ID"=%(report_ID)s and "title"=%(title)s) THEN
                INSERT INTO statementnotes."Damages"("report_ID", title, "paidforLastYear", "getInLastYear", "paidforCurrentPeriod", "getInCurrentPeriod", "paidforShortEstimation", "getInShortEstimation", "paidforLongEstimation", "getInLongEstimation", "StartPeriod", "CurrentPeriod", "ShortTermEstimationPeriod", "LongTermEstimationPeriod")
	VALUES (%(report_ID)s,%(title)s,%(paidforLastYear)s,%(getInLastYear)s,%(paidforCurrentPeriod)s,%(getInCurrentPeriod)s,%(paidforShortEstimation)s,%(getInShortEstimation)s,%(paidforLongEstimation)s,%(getInLongEstimation)s,%(StartPeriod)s,%(CurrentPeriod)s,%(ShortTermEstimationPeriod)s,%(LongTermEstimationPeriod)s);
    END IF;
            END
            $$ 
        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print(DF4.report_ID)
                print('Failed to Update Data ', error)
                #log_it('Failed to Update OtherLayers ')
                #return False
            
    finally:
        if(connection):
            cursor.close()
            connection.close()                          
def Describecompanyplanstocompletedevelopmentplans():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo"
        where p."tableID"='Describe company plans to complete development plans' and p."report_ID" not in (select "report_ID" from statementnotes."CompanyDevelopingSchemasExceptBuilding")
        """, connection)
        #
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_Describecompanyplanstocompletedevelopmentplans():
    df=Describecompanyplanstocompletedevelopmentplans()
    for index,row in df.iterrows():
        if(index%200==0):
            print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        toDates=[]
        if len(df1.columns)!=14:
            print(len(df1.columns))
        for col in df1.columns:
            if re.search('\d\d\d\d/\d\d/\d\d',col):
                ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                if ss!="" and ss not in toDates:
                    toDates.append(ss)
                
        toDates.sort()
        df1 = df1.rename(columns={'نام طرح ': 'projectTitle'})
        df1 = df1.rename(columns={'هزينه هاي برآوردي ريالي طرح - ميليون ريال ': 'EstimatedRialCost'})
        df1 = df1.rename(columns={'هزينه هاي برآوردي ارزي طرح ': 'EstimatedDollarCost'})
        df1 = df1.rename(columns={'نوع ارز ': 'ForeignCurrency'})
        df1 = df1.rename(columns={'هزينه هاي برآوردي تکميل طرح- ميليون ريال ': 'RemainingCostsEstimation'})
        df1 = df1.rename(columns={'تاريخ برآوردي بهره برداري از طرح ': 'EstimatedCompletion'})
        df1 = df1.rename(columns={'تشريح تاثير طرح در فعاليتهاي آتي شرکت ': 'ProjectGoalandImpacts'})
        # df1 = df1.rename(columns={'توضيحات ': 'moreDesc'})
        if len(toDates)==4:
            for col in df1.columns:
                if toDates[1] in col and 'هزينه هاي انجام شده' in col:
                    df1 = df1.rename(columns={col: 'AllCostsUntilNow'})
                if toDates[0] in col and 'درصد پيشرفت فيزيکي طرح' in col:
                    df1 = df1.rename(columns={col: 'StartOfPeriodProgress'})
                if toDates[1] in col and 'درصد پيشرفت فيزيکي طرح' in col:
                    df1 = df1.rename(columns={col: 'NowProgress'})
                if toDates[2] in col and 'درصد پيشرفت فيزيکي برآوردي' in col:
                    df1 = df1.rename(columns={col: 'ShortTermProgressEstimation'})
                if toDates[3] in col and 'درصد پيشرفت فيزيکي برآوردي' in col:
                    df1 = df1.rename(columns={col: 'LongTermProgressEstimation'})
            df1['StartPeriod']=toDates[0]
            df1['CurrentPeriod']=toDates[1]
            df1['NextPeriodShortTerm']=toDates[2]
            df1['NextPeriodLongTerm']=toDates[3]
        if len(toDates)==3:
            for col in df1.columns:
                if toDates[1] in col and 'هزينه هاي انجام شده' in col:
                    df1 = df1.rename(columns={col: 'AllCostsUntilNow'})
                if toDates[0] in col and 'درصد پيشرفت فيزيکي طرح' in col:
                    df1 = df1.rename(columns={col: 'StartOfPeriodProgress'})
                if toDates[1] in col and 'درصد پيشرفت فيزيکي طرح' in col:
                    df1 = df1.rename(columns={col: 'NowProgress'})
                if toDates[2] in col and 'درصد پيشرفت فيزيکي برآوردي' in col:
                    df1 = df1.rename(columns={col: 'ShortTermProgressEstimation'})
                if toDates[2] not in col and 'درصد پيشرفت فيزيکي برآوردي' in col:
                    df1 = df1.rename(columns={col: 'LongTermProgressEstimation'})
            df1['StartPeriod']=toDates[0]
            df1['CurrentPeriod']=toDates[1]
            df1['NextPeriodShortTerm']=toDates[2]
            df1['NextPeriodLongTerm']=""
        if len(toDates)==2:
            for col in df1.columns:
                if toDates[1] in col and 'هزينه هاي انجام شده' in col:
                    df1 = df1.rename(columns={col: 'AllCostsUntilNow'})
                if toDates[0] in col and 'درصد پيشرفت فيزيکي طرح' in col:
                    df1 = df1.rename(columns={col: 'StartOfPeriodProgress'})
                if toDates[1] in col and 'درصد پيشرفت فيزيکي طرح' in col:
                    df1 = df1.rename(columns={col: 'NowProgress'})
                if col=='درصد پيشرفت فيزيکي برآوردي طرح در تاريخ ':
                    df1 = df1.rename(columns={col: 'ShortTermProgressEstimation'})
                if col=='درصد پيشرفت فيزيکي برآوردي طرح در تاريخ .1':
                    df1 = df1.rename(columns={col: 'LongTermProgressEstimation'})
            df1['StartPeriod']=toDates[0]
            df1['CurrentPeriod']=toDates[1]
            df1['NextPeriodShortTerm']=""
            df1['NextPeriodLongTerm']=""
        df1['report_ID']=row['report_ID']
        df1['moreDesc']=""
        INSERTCompanyPlansExpressionforCompletingDevelopmentPlansexceptforBuildingProjects(df1)
def Describethecompanysplanstocompletethedevelopmentplansofthemaincompanyandsubsidiariesandaffiliates():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on 
        p."report_ID"=s."TracingNo" where p."tableID"='Describe the company''s plans to complete the development plans of the main company and subsidiaries and affiliates'
         and p."report_ID" not in (select "report_ID" from statementnotes."CompanyAndAffiliatesDevelopingSchemas")
        """, connection)
        #
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_Describethecompanysplanstocompletethedevelopmentplansofthemaincompanyandsubsidiariesandaffiliates():
    df=Describethecompanysplanstocompletethedevelopmentplansofthemaincompanyandsubsidiariesandaffiliates()
    for index,row in df.iterrows():
        if(index%200==0):
            print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        toDates=[]
        if len(df1.columns)!=13:
            print(len(df1.columns))
        for col in df1.columns:
            if re.search('\d\d\d\d/\d\d/\d\d',col):
                ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                if ss!="" and ss not in toDates:
                    toDates.append(ss)
                
        toDates.sort()
        df1 = df1.rename(columns={'طرح های عمده در دست اجرا ': 'projectTitle'})
        df1 = df1.rename(columns={'نام شرکت (شرکت اصلی یا فرعی و وابسته) ': 'AffiliatedOrCompany'})
        df1 = df1.rename(columns={'هزینه های برآوردی طرح ': 'EstimatedCost'})
        # df1 = df1.rename(columns={'هزينه هاي برآوردي ارزي طرح ': 'EstimatedDollarCost'})
        # df1 = df1.rename(columns={'نوع ارز ': 'ForeignCurrency'})
        df1 = df1.rename(columns={'هزینه های برآوردی تکمیل طرح ': 'RemainingCostsEstimation'})
        df1 = df1.rename(columns={'تاریخ برآوردی بهره برداری از طرح ': 'EstimatedCompletion'})
        df1 = df1.rename(columns={'تشریح تاثیر طرح در فعالیتهای آتی شرکت اصلی و شرکت سرمایه پذیر ': 'ProjectGoalandImpacts'})
        df1 = df1.rename(columns={'توضیحات ': 'moreDesc'})
        if len(toDates)==4:
                for col in df1.columns:
                    if toDates[0] in col and 'درصد پیشرفت فیزیکی طرح' in col:
                        df1 = df1.rename(columns={col: 'StartOfPeriodProgress'})
                    if toDates[1] in col and 'درصد پیشرفت فیزیکی طرح' in col:
                        df1 = df1.rename(columns={col: 'NowProgress'})
                    if toDates[2] in col and 'درصد پیشرفت فیزیکی برآوردی' in col:
                        df1 = df1.rename(columns={col: 'ShortTermProgressEstimation'})
                    if toDates[3] in col and 'درصد پیشرفت فیزیکی برآوردی' in col:
                        df1 = df1.rename(columns={col: 'LongTermProgressEstimation'})
                df1['StartPeriod']=toDates[0]
                df1['CurrentPeriod']=toDates[1]
                df1['NextPeriodShortTerm']=toDates[2]
                df1['NextPeriodLongTerm']=toDates[3]
        if len(toDates)==3:
            for col in df1.columns:
                if toDates[0] in col and 'درصد پیشرفت فیزیکی طرح' in col:
                    df1 = df1.rename(columns={col: 'StartOfPeriodProgress'})
                if toDates[1] in col and 'درصد پیشرفت فیزیکی طرح' in col:
                    df1 = df1.rename(columns={col: 'NowProgress'})
                if toDates[2] in col and 'درصد پیشرفت فیزیکی برآوردی' in col:
                    df1 = df1.rename(columns={col: 'ShortTermProgressEstimation'})
                if toDates[2] not in col and 'درصد پیشرفت فیزیکی برآوردی' in col:
                    df1 = df1.rename(columns={col: 'LongTermProgressEstimation'})
            df1['StartPeriod']=toDates[0]
            df1['CurrentPeriod']=toDates[1]
            df1['NextPeriodShortTerm']=toDates[2]
            df1['NextPeriodLongTerm']=""
        df1['report_ID']=row['report_ID']
        InsertDescribethecompanysplanstocompletethedevelopmentplansofthemaincompanyandsubsidiariesandaffiliates(df1)
def InsertDescribethecompanysplanstocompletethedevelopmentplansofthemaincompanyandsubsidiariesandaffiliates(DF4):
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
                IF NOT EXISTS (select from statementnotes."CompanyAndAffiliatesDevelopingSchemas" where "report_ID"=%(report_ID)s and "title"=%(projectTitle)s) THEN
               INSERT INTO statementnotes."CompanyAndAffiliatesDevelopingSchemas"(
	"report_ID", title, "Owner", "EstimatedCost", "StartOfPeriodProgress", "NowProgress", "ShortTermProgressEstimation", "LongTermProgressEstimation", "RemainingCostsEstimation", "EstimatedCompletion", "ProjectGoalandImpacts", "moreDesc", "StartPeriod", "CurrentPeriod", "NextPeriodShortTerm", "NextPeriodLongTerm")
	VALUES (%(report_ID)s,%(projectTitle)s,%(AffiliatedOrCompany)s,%(EstimatedCost)s,%(StartOfPeriodProgress)s,%(NowProgress)s,%(ShortTermProgressEstimation)s,%(LongTermProgressEstimation)s,%(RemainingCostsEstimation)s,%(EstimatedCompletion)s,%(ProjectGoalandImpacts)s,%(moreDesc)s,%(StartPeriod)s,%(CurrentPeriod)s,%(NextPeriodShortTerm)s,%(NextPeriodLongTerm)s);
    END IF;
            END
            $$ 
        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print(DF4.report_ID)
                print('Failed to Update Data ', error)
                #log_it('Failed to Update OtherLayers ')
                #return False
            
    finally:
        if(connection):
            cursor.close()
            connection.close()                                                      
def Descriptionfordescribecompanyplanstocompletedevelopmentplans():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" 
        where p."tableID"='Description for describe company plans to complete development plans' 
        and p."report_ID" not in (select "report_ID" from statementnotes."GeneralStatements" where
         "engTitle"='Description for describe company plans to complete development plans')

        """, connection)
        #   
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_Descriptionfordescribecompanyplanstocompletedevelopmentplans():
    df=Descriptionfordescribecompanyplanstocompletedevelopmentplans()
    for index,row in df.iterrows():
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        df1['title']=['توضیحات در خصوص تشریح برنامه های شرکت جهت تکمیل طرح های توسعه']
        df1['engtitle']=["Description for describe company plans to complete development plans"]
        df1['report_ID']=row['report_ID']
        df1 = df1.where(pd.notnull(df1), None)
        if (df1.desc[0] is not None)and( df1.desc[0]!=0)and( df1.desc[0]!=1) and(df1.desc[0]!=1.1111111111111107) and(df.desc[0]!=1.11111111111111111111111111111111111111111111111111111111111111111111111111111111) :
            InsertGeneralStatements(df1) 
        else:
            print('Should Be Deleted')
            print(row['Datatable'])
            DeleteFromPreNotes_NullGE(df1)
def Descriptionfordetailsofthefinancingofthecompanyattheendoftheperiod():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" 
        where p."tableID"='Description for details of the financing of the company at the end of the period' 
        and p."report_ID" not in (select "report_ID" from statementnotes."GeneralStatements" where "engTitle"='Description for details of the financing of the company at the end of the period')

        """, connection)
        #   
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_Descriptionfordetailsofthefinancingofthecompanyattheendoftheperiod():
    df=Descriptionfordetailsofthefinancingofthecompanyattheendoftheperiod()
    for index,row in df.iterrows():
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        df1['title']=['توضیحات در خصوص جزئیات منابع تامین مالی شرکت در پایان دوره']
        df1['engtitle']=['Description for details of the financing of the company at the end of the period']
        df1['report_ID']=row['report_ID']
        df1 = df1.where(pd.notnull(df1), None)
        if (df1.desc[0] is not None)and( df1.desc[0]!=0)and( df1.desc[0]!=1) :
            InsertGeneralStatements(df1) 
        else:
            print('Should Be Deleted')
            print(row['Datatable'])
            DeleteFromPreNotes_NullGE(df1)
def Descriptionforthestatusofviablecompanies():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
     select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" 
        where p."tableID"='Description for the status of viable companies'   and p."report_ID" not in (select "report_ID" from statementnotes."GeneralStatements"
         where "engTitle"='Description for the status of viable companies' )

        """, connection)
        #   
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_Descriptionforthestatusofviablecompanies():
    df=Descriptionforthestatusofviablecompanies()
    for index,row in df.iterrows():
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        df1['title']=['توضیحات در در خصوص وضعیت شرکت های سرمایه پذیر']
        df1['engtitle']=['Description for the status of viable companies' ]
        df1['report_ID']=row['report_ID']
        df1 = df1.where(pd.notnull(df1), None)
        if (df1.desc[0] is not None)and( df1.desc[0]!=0)and( df1.desc[0]!=1) :
            InsertGeneralStatements(df1) 
        else:
            print('Should Be Deleted')
            print(row['Datatable'])
            DeleteFromPreNotes_NullGE(df1)    
def DescriptionsofStaffstatistics():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" 
        where p."tableID"='Descriptions of Staff statistics'
        and p."report_ID" not in (select "report_ID" from statementnotes."GeneralStatements" where "engTitle"='Descriptions of Staff statistics' )

        """, connection)
        #   
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_DescriptionsofStaffstatistics():
    df=DescriptionsofStaffstatistics()
    for index,row in df.iterrows():
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")   
        df1['title']=['توضیحات وضعیت کارکنان']
        df1['engtitle']=['Descriptions of Staff statistics' ]
        df1['report_ID']=row['report_ID']
        df1 = df1.where(pd.notnull(df1), None)
        if (df1.desc[0] is not None)and( df1.desc[0]!=0)and( df1.desc[0]!=1) :
            InsertGeneralStatements(df1) 
        else:
            print('Should Be Deleted')
            print(row['Datatable'])
            DeleteFromPreNotes_NullGE(df1)    
def Descriptionsoftheprocessofdamagesandtechnicalreserves():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
     select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" 
        where p."tableID"='Descriptions of the process of damages and technical reserves' 
         and p."report_ID" not in (select "report_ID" from statementnotes."GeneralStatements" where "engTitle"='Descriptions of the process of damages and technical reserves' )

        """, connection)
        #   
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_Descriptionsoftheprocessofdamagesandtechnicalreserves():
    df=Descriptionsoftheprocessofdamagesandtechnicalreserves()
    for index,row in df.iterrows():
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")   
        df1['title']=['توضیحات در خصوص روند خسارت ها و ذخایر فنی']
        df1['engtitle']=['Descriptions of the process of damages and technical reserves']
        df1['report_ID']=row['report_ID']
        df1 = df1.where(pd.notnull(df1), None)
        if (df1.desc[0] is not None)and( df1.desc[0]!=0)and( df1.desc[0]!=1) :
            InsertGeneralStatements(df1) 
        else:
            print('Should Be Deleted')
            print(row['Datatable'])
            DeleteFromPreNotes_NullGE(df1) 
def Detailsofthefinancingofthecompanyattheendoftheperiod():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" 
        where p."tableID"='Details of the financing of the company at the end of the period' 
        and p."report_ID" not in (select "report_ID" from statementnotes."FinancingDetailsOfCompany" )
        """, connection)
        
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_Detailsofthefinancingofthecompanyattheendoftheperiod():
    df=Detailsofthefinancingofthecompanyattheendoftheperiod()
    for index,row in df.iterrows():
        if(index%200==0):
            print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        toDates=[]
        if len(df1.columns)!=13:
            print(len(df1.columns))
        df1.replace('ك','ک',regex=True,inplace=True)
        df1.replace('ي','ی',regex=True,inplace=True)
        df1.columns= df1.columns.str.replace('ي','ی',regex=True)
        df1.columns= df1.columns.str.replace('ي','ی',regex=True)
        df1 = df1.rename(columns={'نوع تامین مالی ': 'FinancingType'})
        df1 = df1.rename(columns={'نرخ سود(درصد) ': 'InterestRate'})
        df1 = df1.rename(columns={'مانده اول دوره تسهیلات ارزی و ریالی (میلیون ریال) ': 'StartPeriodRemains_RialAndDollar'})
        df1 = df1.rename(columns={'مانده پایان دوره (اصل و فرع) ریالی ': 'EndOfPeriodRemainsRial'})
        df1 = df1.rename(columns={'مانده پایان دوره (اصل و فرع) ارزی نوع ارز ': 'EndOfPeriodRemainsForeignCurrency'})
        df1 = df1.rename(columns={'مانده پایان دوره (اصل و فرع) ارزی مبلغ ارزی ':'EndofPeriodRemainsForeginCurrencyValue'})
        df1 = df1.rename(columns={'مانده پایان دوره (اصل و فرع) ارزی معادل ریالی تسهیلات ارزی ': 'EndOfPeriodRemainsRialEquivalent'})
        df1 = df1.rename(columns={'مانده پایان دوره به تفکیک سررسید کوتاه مدت ': 'EndOfPeriodRemainsShortTerm'})
        df1 = df1.rename(columns={'مانده پایان دوره به تفکیک سررسید بلند مدت ': 'EndOfPeriodRemainsLongTerm'})
        df1 = df1.rename(columns={'مبلغ هزینه مالی طی دوره ': 'FinanceCost'})
        df1 = df1.rename(columns={'سایر توضیحات ': 'MoreDesc'})
        df1['report_ID']=row['report_ID']
        InsertDetailsofthefinancingofthecompanyattheendoftheperiod(df1)
def InsertDetailsofthefinancingofthecompanyattheendoftheperiod(DF4):
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
                IF NOT EXISTS (select from statementnotes."FinancingDetailsOfCompany" where "report_ID"=%(report_ID)s and "financingType"=%(FinancingType)s) THEN
               INSERT INTO statementnotes."FinancingDetailsOfCompany"( "report_ID", "financingType", "InterestRate", "StartPeriodRemains(RialAndDollar)", "EndOfPeriodRemainsRial", "EndOfPeriodRemainsForeignCurrency", "EndofPeriodRemainsForeginCurrencyValue", "EndOfPeriodRemainsRialEquivalent", "EndOfPeriodRemainsShortTerm", "EndOfPeriodRemainsLongTerm", "FinanceCost", "MoreDesc")
	VALUES (%(report_ID)s,%(FinancingType)s,%(InterestRate)s,%(StartPeriodRemains_RialAndDollar)s,%(EndOfPeriodRemainsRial)s,%(EndOfPeriodRemainsForeignCurrency)s,%(EndofPeriodRemainsForeginCurrencyValue)s,%(EndOfPeriodRemainsRialEquivalent)s,%(EndOfPeriodRemainsShortTerm)s,%(EndOfPeriodRemainsLongTerm)s,%(FinanceCost)s,%(MoreDesc)s);
    END IF;
            END
            $$ 
        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print(DF4.report_ID)
                print('Failed to Update Data ', error)
                #log_it('Failed to Update OtherLayers ')
                #return False
            
    finally:
        if(connection):
            cursor.close()
            connection.close()   
def Estimatedcompanychangesinadministrativeandgeneralexpensesandotheroperatingexpenses():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo"
        where p."tableID"='Estimated company changes in administrative and general expenses and other operating expenses (operating expenses)' 
         and p."report_ID" not in (select "report_ID" from statementnotes."GeneralStatements" where "engTitle"='Estimated company changes in administrative and general expenses and other operating expenses (operating expenses)' )
        """, connection)
        #  
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_Estimatedcompanychangesinadministrativeandgeneralexpensesandotheroperatingexpenses():
    df=Estimatedcompanychangesinadministrativeandgeneralexpensesandotheroperatingexpenses()
    for index,row in df.iterrows():
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        df1['title']=['برآورد شرکت از تغییرات در هزینه های عمومی اداری تشکیلاتی و خالص  سایر درآمدها (هزینه ها) عملیاتی']
        df1['engtitle']=['Estimated company changes in administrative and general expenses and other operating expenses (operating expenses)']
        df1['report_ID']=row['report_ID']
        df1 = df1.where(pd.notnull(df1), None)
        if (df1.desc[0] is not None)and( df1.desc[0]!=0)and( df1.desc[0]!=1) :
            InsertGeneralStatements(df1) 
        else:
            print('Should Be Deleted')
            print(row['Datatable'])
            DeleteFromPreNotes_NullGE(df1)                         
def Estimatedcompanychangesinpublic():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo"
        where p."tableID"='Estimated company changes in public'  
         and p."report_ID" not in (select "report_ID" from statementnotes."GeneralStatements" where "engTitle"='Estimated company changes in public' )
        """, connection)
        #  
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_Estimatedcompanychangesinpublic():
    df=Estimatedcompanychangesinpublic()
    for index,row in df.iterrows():
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        df1['title']=['برآورد شرکت از تغییرات در هزینه های عمومی اداری تشکیلاتی و خالص  سایر درآمدها (هزینه ها) عملیاتی']
        df1['engtitle']=['Estimated company changes in public']
        df1['report_ID']=row['report_ID']
        df1 = df1.where(pd.notnull(df1), None)
        if (df1.desc[0] is not None)and( df1.desc[0]!=0)and( df1.desc[0]!=1) :
            InsertGeneralStatements(df1) 
        else:
            print('Should Be Deleted')
            print(row['Datatable'])
            DeleteFromPreNotes_NullGE(df1) 
def Futuregoalsandstrategiesforportfoliomanagement():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo"
        where p."tableID"='Future goals and strategies for portfolio management' 
         and p."report_ID" not in (select "report_ID" from statementnotes."GeneralStatements" where "engTitle"='Future goals and strategies for portfolio management' )
        """, connection)
        #  
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_Futuregoalsandstrategiesforportfoliomanagement():
    df=Futuregoalsandstrategiesforportfoliomanagement()
    for index,row in df.iterrows():
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        df1['title']=[' اهداف و راهبردهای آتی مدیریت در خصوص ترکیب پرتفوی']
        df1['engtitle']=['Future goals and strategies for portfolio management']
        df1['report_ID']=row['report_ID']
        df1 = df1.where(pd.notnull(df1), None)
        if (df1.desc[0] is not None)and( df1.desc[0]!=0)and( df1.desc[0]!=1) :
            InsertGeneralStatements(df1) 
        else:
            print('Should Be Deleted')
            print(row['Datatable'])
            DeleteFromPreNotes_NullGE(df1)     
def Futuregoalsandstrategiesofmanagementregardingcompanyactivity():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo"
        where p."tableID"='Future goals and strategies of management regarding company activity'  
        and p."report_ID" not in (select "report_ID" from statementnotes."GeneralStatements" where
        "engTitle"='Future goals and strategies of management regarding company activity')
        """, connection)
        #  
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_Futuregoalsandstrategiesofmanagementregardingcompanyactivity():
    df=Futuregoalsandstrategiesofmanagementregardingcompanyactivity()
    for index,row in df.iterrows():
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        df1['title']=['اهداف و راهبردهای آتی مدیریت در خصوص فعالیت شرکت']
        df1['engtitle']=['Future goals and strategies of management regarding company activity' ]
        df1['report_ID']=row['report_ID']
        df1 = df1.where(pd.notnull(df1), None)
        if (df1.desc[0] is not None)and( df1.desc[0]!=0)and( df1.desc[0]!=1) :
            InsertGeneralStatements(df1) 
        else:
            print('Should Be Deleted')
            print(row['Datatable'])
            DeleteFromPreNotes_NullGE(df1)
def FuturemanagementgoalsandstrategiesforResourceandConsumption():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo"
        where p."tableID"='Future management goals and strategies for Resource and Consumption'
        and p."report_ID" not in (select "report_ID" from statementnotes."GeneralStatements" where "engTitle"='Future management goals and strategies for Resource and Consumption' )
        """, connection)
        #  
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_FuturemanagementgoalsandstrategiesforResourceandConsumption():
    df=FuturemanagementgoalsandstrategiesforResourceandConsumption()
    for index,row in df.iterrows():
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        df1['title']=['اهداف و راهبردهای آتی مدیریت در خصوص منابع و مصارف']
        df1['engtitle']=['Future management goals and strategies for Resource and Consumption' ]
        df1['report_ID']=row['report_ID']
        df1 = df1.where(pd.notnull(df1), None)
        if (df1.desc[0] is not None)and( df1.desc[0]!=0)and( df1.desc[0]!=1) :
            InsertGeneralStatements(df1) 
        else:
            print('Should Be Deleted')
            print(row['Datatable'])
            DeleteFromPreNotes_NullGE(df1)
def Futuremanagementgoalsandstrategiesfortheproductionandsaleofproducts():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo"
        where p."tableID"='Future management goals and strategies for the production and sale of products'  
         and p."report_ID" not in (select "report_ID" from statementnotes."GeneralStatements" where "engTitle"='Future management goals and strategies for the production and sale of products' )
        """, connection)
        #  
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_Futuremanagementgoalsandstrategiesfortheproductionandsaleofproducts():
    df=Futuremanagementgoalsandstrategiesfortheproductionandsaleofproducts()
    for index,row in df.iterrows():
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        df1['title']=['اهداف و راهبردهای آتی مدیریت در خصوص تولید و فروش محصولات']
        df1['engtitle']=['Future management goals and strategies for the production and sale of products' ]
        df1['report_ID']=row['report_ID']
        df1 = df1.where(pd.notnull(df1), None)
        if (df1.desc[0] is not None)and( df1.desc[0]!=0)and( df1.desc[0]!=1) :
            InsertGeneralStatements(df1) 
        else:
            print('Should Be Deleted')
            print(row['Datatable'])
            DeleteFromPreNotes_NullGE(df1)
def Futuremanagementgoalsandstrategiesregardingcompanyinvestments():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo"
        where p."tableID"='Future management goals and strategies regarding company investments'   
        and p."report_ID" not in (select "report_ID" from statementnotes."GeneralStatements" where "engTitle"='Future management goals and strategies regarding company investments')
        """, connection)
        #  
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_Futuremanagementgoalsandstrategiesregardingcompanyinvestments():
    df=Futuremanagementgoalsandstrategiesregardingcompanyinvestments()
    for index,row in df.iterrows():
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        df1['title']=['اهداف و راهبردهای آتی مدیریت در خصوص سرمایه گذاری های شرکت']
        df1['engtitle']=['Future management goals and strategies regarding company investments']
        df1['report_ID']=row['report_ID']
        df1 = df1.where(pd.notnull(df1), None)
        if (df1.desc[0] is not None)and( df1.desc[0]!=0)and( df1.desc[0]!=1) :
            InsertGeneralStatements(df1) 
        else:
            print('Should Be Deleted')
            print(row['Datatable'])
            DeleteFromPreNotes_NullGE(df1)        
def Futuremanagementgoalsandstrategiesregardingtheamountandcompositionofcompanyinsuranceportfolios():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo"
        where p."tableID"='Future management goals and strategies regarding the amount and composition of company insurance portfolios' 
        and p."report_ID" not in (select "report_ID" from statementnotes."GeneralStatements" where "engTitle"='Future management goals and strategies regarding the amount and composition of company insurance portfolios')
        """, connection)
        #  
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_Futuremanagementgoalsandstrategiesregardingtheamountandcompositionofcompanyinsuranceportfolios():
    df=Futuremanagementgoalsandstrategiesregardingtheamountandcompositionofcompanyinsuranceportfolios()
    for index,row in df.iterrows():
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        df1['title']=['اهداف و راهبردهای آتی مدیریت در خصوص مبلغ و ترکیب پرتفوی بیمه ای شرکت']
        df1['engtitle']=['Future management goals and strategies regarding the amount and composition of company insurance portfolios']
        df1['report_ID']=row['report_ID']
        df1 = df1.where(pd.notnull(df1), None)
        if (df1.desc[0] is not None)and( df1.desc[0]!=0)and( df1.desc[0]!=1) :
            InsertGeneralStatements(df1) 
        else:
            print('Should Be Deleted')
            print(row['Datatable'])
            DeleteFromPreNotes_NullGE(df1)  
def Investment():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" 
        where p."tableID"='Investments' and p."report_ID" not in (select "report_ID" from statementnotes."Investment" )  

        """, connection)
        #and p."report_ID" not in (select "report_ID" from statementnotes."Investments" )        
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_Investment():
    df=Investment()
    for index,row in df.iterrows():
        if(index%200==0):
            print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        if len(df1.columns)!=7:
            print(len(df1.columns))
        toDates=[]
        for col in df1.columns:
                if re.search('\d\d\d\d/\d\d/\d\d',col):
                    ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                    if ss!="" and ss not in toDates:
                        toDates.append(ss)
        toDates.sort()
        if len(toDates)!=2:
            print(len(toDates))
        df1.replace('ك','ک',regex=True,inplace=True)
        df1.replace('ي','ی',regex=True,inplace=True)
        df1 = df1.rename(columns={'شرح ': 'title'})
        for col in df1.columns:
            if 'مبلغ سرمايه' in col and toDates[0] in col:
                df1 = df1.rename(columns={col: 'StartPeriodAmount'})  
            if 'مبلغ سرمايه' in col and toDates[1] in col:
                df1 = df1.rename(columns={col: 'EndPeriodAmount'})
            if 'سود' in col and toDates[0] in col:
                df1 = df1.rename(columns={col: 'StartPeriodProfit'})
            if 'سود' in col and toDates[1] in col:
                df1 = df1.rename(columns={col: 'EndPeriodProfit'})
        df1['report_ID']=row['report_ID']
        InsertInvestment(df1)
def InsertInvestment(DF4):
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
                IF NOT EXISTS (select from statementnotes."Investment" where "report_ID"=%(report_ID)s and "title"=%(title)s) THEN
                INSERT INTO statementnotes."Investment"(
                "report_ID", title, "StartPeriodAmount", "StartPeriodProfit", "EndPeriodAmount", "EndPeriodProfit")
                VALUES ( %(report_ID)s, %(title)s,%(StartPeriodAmount)s, %(StartPeriodProfit)s, %(EndPeriodAmount)s, %(EndPeriodProfit)s);
                END IF;
            END
            $$ 
        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print(DF4.report_ID)
                print('Failed to Update Data ', error)
                #log_it('Failed to Update OtherLayers ')
                #return False
            
    finally:
        if(connection):
            cursor.close()
            connection.close() 
def Non_insuredincomecurrencyandcurrencystatusdescription():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo"
        where p."tableID"='Non-insured income (currency) and currency status description'  
        and p."report_ID" not in (select "report_ID" from statementnotes."GeneralStatements" where "engTitle"='Non-insured income (currency) and currency status description' )
        """, connection)
        #  
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_Non_insuredincomecurrencyandcurrencystatusdescription():
    df=Non_insuredincomecurrencyandcurrencystatusdescription()
    for index,row in df.iterrows():
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        df1['title']=['توضیحات درآمد (هزینه) های عیز بیمه ای و وضعیت ارزی']
        df1['engtitle']=['Non-insured income (currency) and currency status description' ]
        df1['report_ID']=row['report_ID']
        df1 = df1.where(pd.notnull(df1), None)
        if (df1.desc[0] is not None)and( df1.desc[0]!=0)and( df1.desc[0]!=1) :
            InsertGeneralStatements(df1) 
        else:
            print('Should Be Deleted')
            print(row['Datatable'])
            DeleteFromPreNotes_NullGE(df1)      
def Nonoperationalincomeandexpensesinvestmentincome():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" 
        where (p."tableID"='Non-operational income and expenses investment income' or p."tableID"='Non-operation income and expenses investment income')
        and p."report_ID" not in (select "report_ID" from statementnotes."NonOperationalIncomeandExpenses_InvestmentIncome" )     

        """, connection)
        #   
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_Nonoperationalincomeandexpensesinvestmentincome():
    df=Nonoperationalincomeandexpensesinvestmentincome()
    for index,row in df.iterrows():
        if(index%200==0):
            print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        if len(df1.columns)!=5:
            print(len(df1.columns))
        toDates=[]
        for col in df1.columns:
                if re.search('\d\d\d\d/\d\d/\d\d',col):
                    ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                    if ss!="" and ss not in toDates:
                        toDates.append(ss)
        toDates.sort()
        if len(toDates)!=2:
            print(len(toDates))
        df1.replace('ك','ک',regex=True,inplace=True)
        df1.replace('ي','ی',regex=True,inplace=True)
        df1 = df1.rename(columns={'شرح ': 'title'})
        for col in df1.columns:
            if toDates[0] in col:
                df1 = df1.rename(columns={col: 'StartPeriodAmount'})  
            if toDates[1] in col:
                df1 = df1.rename(columns={col: 'EndPeriodAmount'})
        df1['report_ID']=row['report_ID']
        df1['StartPeriod']=toDates[0]
        df1['EndPeriod']=toDates[1]
        InsertNonoperationalincomeandexpensesinvestmentincome(df1)
def InsertNonoperationalincomeandexpensesinvestmentincome(DF4):
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
                IF NOT EXISTS (select from statementnotes."NonOperationalIncomeandExpenses_InvestmentIncome" where "report_ID"=%(report_ID)s and "title"=%(title)s) THEN
               INSERT INTO statementnotes."NonOperationalIncomeandExpenses_InvestmentIncome"(
                 "report_ID", title, "StartPeriodValue", "EndPeriodValue", "StartPeriod", "EndPeriod")
                VALUES ( %(report_ID)s, %(title)s, %(StartPeriodAmount)s, %(EndPeriodAmount)s, %(StartPeriod)s, %(EndPeriod)s);
                END IF;
            END
            $$ 
        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print(DF4.report_ID)
                print('Failed to Update Data ', error)
                #log_it('Failed to Update OtherLayers ')
                #return False
            
    finally:
        if(connection):
            cursor.close()
            connection.close()  
def Nonoperationalincomeandexpensesmiscellaneousitems():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
     select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" 
        where (p."tableID"='Non-operational income and expenses miscellaneous items' or p."tableID"='Non-operation income and expenses miscellaneous items') 
          and p."report_ID" not in (select "report_ID" from statementnotes."NonOperationalIncomeandExpenses_Misc" )     

        """, connection)
        #   
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_Nonoperationalincomeandexpensesmiscellaneousitems():
    df=Nonoperationalincomeandexpensesmiscellaneousitems()
    for index,row in df.iterrows():
        if(index%200==0):
            print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        if len(df1.columns)!=5:
            print(len(df1.columns))
        toDates=[]
        for col in df1.columns:
                if re.search('\d\d\d\d/\d\d/\d\d',col):
                    ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                    if ss!="" and ss not in toDates:
                        toDates.append(ss)
        toDates.sort()
        if len(toDates)!=2:
            print(len(toDates))
        df1.replace('ك','ک',regex=True,inplace=True)
        df1.replace('ي','ی',regex=True,inplace=True)
        df1 = df1.rename(columns={'شرح ': 'title'})
        for col in df1.columns:
            if toDates[0] in col:
                df1 = df1.rename(columns={col: 'StartPeriodAmount'})  
            if toDates[1] in col:
                df1 = df1.rename(columns={col: 'EndPeriodAmount'})
        df1['report_ID']=row['report_ID']
        df1['StartPeriod']=toDates[0]
        df1['EndPeriod']=toDates[1]
        InsertNonoperationalincomeandexpensesmiscellaneousitems(df1)
def InsertNonoperationalincomeandexpensesmiscellaneousitems(DF4):
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
                IF NOT EXISTS (select from statementnotes."NonOperationalIncomeandExpenses_Misc" where "report_ID"=%(report_ID)s and "title"=%(title)s) THEN
               INSERT INTO statementnotes."NonOperationalIncomeandExpenses_Misc"(
                 "report_ID", title, "StartPeriodValue", "EndPeriodValue", "StartPeriod", "EndPeriod")
                VALUES ( %(report_ID)s, %(title)s, %(StartPeriodAmount)s, %(EndPeriodAmount)s, %(StartPeriod)s, %(EndPeriod)s);
                END IF;
            END
            $$ 
        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print(DF4.report_ID)
                print('Failed to Update Data ', error)
                #log_it('Failed to Update OtherLayers ')
                #return False
            
    finally:
        if(connection):
            cursor.close()
            connection.close()   
def Otherimportantcompanysplans():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
            select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo"
            where p."tableID"='Other important company''s plans'  
            and p."report_ID" not in (select "report_ID" from statementnotes."GeneralStatements" where "engTitle"='Other important company''s plans')
        """, connection)
        #  
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_Otherimportantcompanysplans():
    df=Otherimportantcompanysplans()
    for index,row in df.iterrows():
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        df1['title']=['سایر برنامه های با اهمیت شرکت']
        df1['engtitle']=["Other important company's plans"]
        df1['report_ID']=row['report_ID']
        df1 = df1.where(pd.notnull(df1), None)
        if (df1.desc[0] is not None)and( df1.desc[0]!=0)and( df1.desc[0]!=1) :
            InsertGeneralStatements(df1) 
        else:
            print('Should Be Deleted')
            print(row['Datatable'])
            DeleteFromPreNotes_NullGE(df1)       
def Otherimportantdescriptions():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo"
        where p."tableID"='Other important descriptions'
        and p."report_ID" not in (select "report_ID" from statementnotes."GeneralStatements" where "engTitle"='Other important descriptions')
        """, connection)
        #  
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_Otherimportantdescriptions():
    df=Otherimportantdescriptions()
    for index,row in df.iterrows():
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        df1['title']=['سایر اطلاعات با اهمیت']
        df1['engtitle']=['Other important descriptions']
        df1['report_ID']=row['report_ID']
        df1 = df1.where(pd.notnull(df1), None)
        if (df1.desc[0] is not None)and( df1.desc[0]!=0)and( df1.desc[0]!=1) :
            InsertGeneralStatements(df1) 
        else:
            print('Should Be Deleted')
            print(row['Datatable'])
            DeleteFromPreNotes_NullGE(df1)     
def Otherimportantnotes():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo"
        where p."tableID"='Other important notes'
        and p."report_ID" not in (select "report_ID" from statementnotes."GeneralStatements" where "engTitle"='Other important notes' )
        """, connection)
        #  
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_Otherimportantnotes():
    df=Otherimportantnotes()
    for index,row in df.iterrows():
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        df1['title']=['سایر توضیحات با اهمیت']
        df1['engtitle']=['Other important notes']
        df1['report_ID']=row['report_ID']
        df1 = df1.where(pd.notnull(df1), None)
        if (df1.desc[0] is not None)and( df1.desc[0]!=0)and( df1.desc[0]!=1) :
            InsertGeneralStatements(df1) 
        else:
            print('Should Be Deleted')
            print(row['Datatable'])
            DeleteFromPreNotes_NullGE(df1)   
def Otherimportantprograms():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo"
        where (p."tableID"='Other Important Programs' or p."tableID"='Other important programs')
        and p."report_ID" not in (select "report_ID" from statementnotes."GeneralStatements" where "engTitle"='Other important programs'  )
        """, connection)
        #  
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_Otherimportantprograms():
    df=Otherimportantprograms()
    for index,row in df.iterrows():
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        df1['title']=['سایر برنامه های با اهمیت شرکت']
        df1['engtitle']=['Other important programs']
        df1['report_ID']=row['report_ID']
        df1 = df1.where(pd.notnull(df1), None)
        if (df1.desc[0] is not None)and( df1.desc[0]!=0)and( df1.desc[0]!=1) :
            InsertGeneralStatements(df1) 
        else:
            print('Should Be Deleted')
            print(row['Datatable'])
            DeleteFromPreNotes_NullGE(df1)
def Otherincomesandexpensesandfinancialexpenses():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" 
        where p."tableID"='Other incomes and expenses and financial expenses' 
        and p."report_ID" not in (select "report_ID" from statementnotes."OtherIncomesAndExpensesAndFinancialExpenses" )     
        """, connection)
        #   

        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_Otherincomesandexpensesandfinancialexpenses():
    df=Otherincomesandexpensesandfinancialexpenses()
    for index,row in df.iterrows():
        if(index%200==0):
            print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        if len(df1.columns)!=5:
            print(len(df1.columns))
        toDates=[]
        for col in df1.columns:
                if re.search('\d\d\d\d/\d\d/\d\d',col):
                    ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                    if ss!="" and ss not in toDates:
                        toDates.append(ss)
        toDates.sort()
        if len(toDates)!=2:
            print(len(toDates))
        df1.replace('ك','ک',regex=True,inplace=True)
        df1.replace('ي','ی',regex=True,inplace=True)
        df1 = df1.rename(columns={'شرح ': 'title'})
        for col in df1.columns:
            if toDates[0] in col:
                df1 = df1.rename(columns={col: 'StartPeriodAmount'})  
            if toDates[1] in col:
                df1 = df1.rename(columns={col: 'EndPeriodAmount'})
        df1['report_ID']=row['report_ID']
        df1['StartPeriod']=toDates[0]
        df1['EndPeriod']=toDates[1]
        InsertOtherincomesandexpensesandfinancialexpenses(df1)
def InsertOtherincomesandexpensesandfinancialexpenses(DF4):
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
                IF NOT EXISTS (select from statementnotes."OtherIncomesAndExpensesAndFinancialExpenses" where "report_ID"=%(report_ID)s and "title"=%(title)s) THEN
               INSERT INTO statementnotes."OtherIncomesAndExpensesAndFinancialExpenses"(
                "report_ID", title, "StartPeriodAmount", "EndPeriodAmount", "StartPeriod", "EndPeriod")
                VALUES ( %(report_ID)s, %(title)s, %(StartPeriodAmount)s, %(EndPeriodAmount)s, %(StartPeriod)s, %(EndPeriod)s);
                END IF;
            END
            $$ 
        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print(DF4.report_ID)
                print('Failed to Update Data ', error)
                #log_it('Failed to Update OtherLayers ')
                #return False
            
    finally:
        if(connection):
            cursor.close()
            connection.close()   
def Otherinsuranceexpenses_Income():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" 
        where p."tableID"='Other insurance expenses (Income)'and p."report_ID" not in (select "report_ID" from statementnotes."OtherInsuranceExpensesIncome" )  
        """, connection)
        #       

        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_Otherinsuranceexpenses_Income():
    df=Otherinsuranceexpenses_Income()
    for index,row in df.iterrows():
        if(index%200==0):
            print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        if len(df1.columns)!=7:
            print(len(df1.columns))
        toDates=[]
        for col in df1.columns:
                if re.search('\d\d\d\d/\d\d/\d\d',col):
                    ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                    if ss!="" and ss not in toDates:
                        toDates.append(ss)
        toDates.sort()
        # if len(toDates)!=3:
        #     print(len(toDates))
        df1.replace('ك','ک',regex=True,inplace=True)
        df1.replace('ي','ی',regex=True,inplace=True)
        df1 = df1.rename(columns={'شرح ': 'title'})
        if len(toDates)==3:
            for col in df1.columns:
                if toDates[0] in col:
                    df1 = df1.rename(columns={col: 'StartPeriodAmount'})  
                if toDates[1] in col:
                    df1 = df1.rename(columns={col: 'EndPeriodAmount'})
                if toDates[2] in col:
                    df1 = df1.rename(columns={col: 'ShortTermEstimation'})
                if toDates[2] not in col and 'برآورد' in col:
                    df1 = df1.rename(columns={col: 'LongTermEstimation'})
                df1['report_ID']=row['report_ID']
                df1['StartPeriod']=toDates[0]
                df1['EndPeriod']=toDates[1]
                df1['ShortTerm']=toDates[2]
                df1['LongTerm']=""
        if len(toDates)==4:
            for col in df1.columns:
                if toDates[0] in col:
                    df1 = df1.rename(columns={col: 'StartPeriodAmount'})  
                if toDates[1] in col:
                    df1 = df1.rename(columns={col: 'EndPeriodAmount'})
                if toDates[2] in col:
                    df1 = df1.rename(columns={col: 'ShortTermEstimation'})
                if toDates[3] in col:
                    df1 = df1.rename(columns={col: 'LongTermEstimation'})
                df1['report_ID']=row['report_ID']
                df1['StartPeriod']=toDates[0]
                df1['EndPeriod']=toDates[1]
                df1['ShortTerm']=toDates[2]
                df1['LongTerm']=toDates[3]
        df1=df1[df1['title']==df1['title']]
        InsertOtherinsuranceexpenses_Income(df1)
def InsertOtherinsuranceexpenses_Income(DF4):
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
                IF NOT EXISTS (select from statementnotes."OtherInsuranceExpensesIncome" where "report_ID"=%(report_ID)s and "title"=%(title)s) THEN
               INSERT INTO statementnotes."OtherInsuranceExpensesIncome"(
                "report_ID", title, "StartPeriodAmount", "EndPeriodAmount", "ShortTermEstimation", "LongTermEstimation", "StartPeriod", "EndPeriod", "ShortTerm", "LongTerm")
                VALUES ( %(report_ID)s, %(title)s, %(StartPeriodAmount)s, %(EndPeriodAmount)s, %(ShortTermEstimation)s, %(LongTermEstimation)s,%(StartPeriod)s,%(EndPeriod)s,%(ShortTerm)s,%(LongTerm)s);
                END IF;
            END
            $$ 
        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print(DF4.report_ID)
                print('Failed to Update Data ', error)
                #log_it('Failed to Update OtherLayers ')
                #return False
            
    finally:
        if(connection):
            cursor.close()
            connection.close()  
def OtherNonfuncionalExpenses_OtherItem():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
         select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as
    	s on p."report_ID"=s."TracingNo" where p."tableID"='Other Nonfuncional Expenses-Other Items' 
        and p."report_ID" not in (select "report_ID" from statementnotes."OtherNonfuncionalExpenses_OtherItem" ) 
        """, connection)
        #    

        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_OtherNonfuncionalExpenses_OtherItem():
    df=OtherNonfuncionalExpenses_OtherItem()
    for index,row in df.iterrows():
        if(index%200==0):
            print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        if len(df1.columns)!=5:
            print(len(df1.columns))
        toDates=[]
        for col in df1.columns:
                if re.search('\d\d\d\d/\d\d/\d\d',col):
                    ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                    if ss!="" and ss not in toDates:
                        toDates.append(ss)
        toDates.sort()
        if len(toDates)!=2:
            print(len(toDates))
        df1.replace('ك','ک',regex=True,inplace=True)
        df1.replace('ي','ی',regex=True,inplace=True)
        df1 = df1.rename(columns={'شرح ': 'title'})
        for col in df1.columns:
            if toDates[0] in col:
                df1 = df1.rename(columns={col: 'StartPeriodAmount'})  
            if toDates[1] in col:
                df1 = df1.rename(columns={col: 'EndPeriodAmount'})
        df1['report_ID']=row['report_ID']
        df1['StartPeriod']=toDates[0]
        df1['EndPeriod']=toDates[1]
        InsertOtherNonfuncionalExpenses_OtherItem(df1)
def InsertOtherNonfuncionalExpenses_OtherItem(DF4):
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
                IF NOT EXISTS (select from statementnotes."OtherNonfuncionalExpenses_OtherItem" where "report_ID"=%(report_ID)s and "title"=%(title)s) THEN
               INSERT INTO statementnotes."OtherNonfuncionalExpenses_OtherItem"(
                "report_ID", title, "StartPeriodAmount", "EndPeriodAmount", "StartPeriod", "EndPeriod")
                VALUES ( %(report_ID)s, %(title)s, %(StartPeriodAmount)s, %(EndPeriodAmount)s, %(StartPeriod)s, %(EndPeriod)s);
                END IF;
            END
            $$ 
        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print(DF4.report_ID)
                print('Failed to Update Data ', error)
                #log_it('Failed to Update OtherLayers ')
                #return False
            
    finally:
        if(connection):
            cursor.close()
            connection.close()     
def OtherNonfuncionalIncomes_Investments():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as
	    s on p."report_ID"=s."TracingNo" where p."tableID"='Other nonfuncional incomes- Investment Incomes'
        and p."report_ID" not in (select "report_ID" from statementnotes."OtherNonfuncionalIncomes_Investments" ) 
        """, connection)
        #    

        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_OtherNonfuncionalIncomes_Investments():
    df=OtherNonfuncionalIncomes_Investments()
    for index,row in df.iterrows():
        if(index%200==0):
            print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        if len(df1.columns)!=5:
            print(len(df1.columns))
        toDates=[]
        for col in df1.columns:
                if re.search('\d\d\d\d/\d\d/\d\d',col):
                    ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                    if ss!="" and ss not in toDates:
                        toDates.append(ss)
        toDates.sort()
        if len(toDates)!=2:
            print(len(toDates))
        df1.replace('ك','ک',regex=True,inplace=True)
        df1.replace('ي','ی',regex=True,inplace=True)
        df1 = df1.rename(columns={'شرح ': 'title'})
        for col in df1.columns:
            if toDates[0] in col:
                df1 = df1.rename(columns={col: 'StartPeriodAmount'})  
            if toDates[1] in col:
                df1 = df1.rename(columns={col: 'EndPeriodAmount'})
        df1['report_ID']=row['report_ID']
        df1['StartPeriod']=toDates[0]
        df1['EndPeriod']=toDates[1]
        InsertOtherNonfuncionalIncomes_Investments(df1)
def InsertOtherNonfuncionalIncomes_Investments(DF4):
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
                IF NOT EXISTS (select from statementnotes."OtherNonfuncionalIncomes_Investments" where "report_ID"=%(report_ID)s and "title"=%(title)s) THEN
               INSERT INTO statementnotes."OtherNonfuncionalIncomes_Investments"(
                "report_ID", title, "StartPeriodAmount", "EndPeriodAmount", "StartPeriod", "EndPeriod")
                VALUES ( %(report_ID)s, %(title)s, %(StartPeriodAmount)s, %(EndPeriodAmount)s, %(StartPeriod)s, %(EndPeriod)s);
                END IF;
            END
            $$ 
        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print(DF4.report_ID)
                print('Failed to Update Data ', error)
                #log_it('Failed to Update OtherLayers ')
                #return False
            
    finally:
        if(connection):
            cursor.close()
            connection.close()   
def Othernon_insuranceincome():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as
        s on p."report_ID"=s."TracingNo" where p."tableID"='Other non-insurance income'  
        and  p."report_ID" not in (select "report_ID" from statementnotes."OtherNon_InsuranceIncome" )
        """, connection)
        #   

        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_Othernon_insuranceincome():
    df=Othernon_insuranceincome()
    for index,row in df.iterrows():
        if(index%200==0):
            print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        if len(df1.columns)!=5:
            print(len(df1.columns))
        toDates=[]
        for col in df1.columns:
                if re.search('\d\d\d\d/\d\d/\d\d',col):
                    ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                    if ss!="" and ss not in toDates:
                        toDates.append(ss)
        toDates.sort()
        if len(toDates)!=2:
            print(len(toDates))
        df1.replace('ك','ک',regex=True,inplace=True)
        df1.replace('ي','ی',regex=True,inplace=True)
        df1 = df1.rename(columns={'شرح ': 'title'})
        for col in df1.columns:
            if toDates[0] in col:
                df1 = df1.rename(columns={col: 'StartPeriodAmount'})  
            if toDates[1] in col:
                df1 = df1.rename(columns={col: 'EndPeriodAmount'})
        df1['report_ID']=row['report_ID']
        df1['StartPeriod']=toDates[0]
        df1['EndPeriod']=toDates[1]
        InsertOthernon_insuranceincome(df1)
def InsertOthernon_insuranceincome(DF4):
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
                IF NOT EXISTS (select from statementnotes."OtherNon_InsuranceIncome" where "report_ID"=%(report_ID)s and "title"=%(title)s) THEN
               INSERT INTO statementnotes."OtherNon_InsuranceIncome"(
                "report_ID", title, "StartPeriodAmount", "EndPeriodAmount", "StartPeriod", "EndPeriod")
                VALUES ( %(report_ID)s, %(title)s, %(StartPeriodAmount)s, %(EndPeriodAmount)s, %(StartPeriod)s, %(EndPeriod)s);
                END IF;
            END
            $$ 
        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print(DF4.report_ID)
                print('Failed to Update Data ', error)
                #log_it('Failed to Update OtherLayers ')
                #return False
            
    finally:
        if(connection):
            cursor.close()
            connection.close() 
def Othernon_operatingincome():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
            select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as
            s on p."report_ID"=s."TracingNo" where p."tableID"='Other non-operating income (expenses)' 
            and  p."report_ID" not in (select "report_ID" from statementnotes."OtherNon_OperatingIncome" ) 
        """, connection)
        #   

        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_Othernon_operatingincome():
    df=Othernon_operatingincome()
    for index,row in df.iterrows():
        if(index%200==0):
            print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        if len(df1.columns)!=5:
            print(len(df1.columns))
        toDates=[]
        for col in df1.columns:
                if re.search('\d\d\d\d/\d\d/\d\d',col):
                    ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                    if ss!="" and ss not in toDates:
                        toDates.append(ss)
        toDates.sort()
        if len(toDates)!=2:
            print(len(toDates))
        df1.replace('ك','ک',regex=True,inplace=True)
        df1.replace('ي','ی',regex=True,inplace=True)
        df1 = df1.rename(columns={'شرح ': 'title'})
        for col in df1.columns:
            if toDates[0] in col:
                df1 = df1.rename(columns={col: 'StartPeriodAmount'})  
            if toDates[1] in col:
                df1 = df1.rename(columns={col: 'EndPeriodAmount'})
        df1['report_ID']=row['report_ID']
        df1['StartPeriod']=toDates[0]
        df1['EndPeriod']=toDates[1]
        InsertOthernon_operatingincome(df1)
def InsertOthernon_operatingincome(DF4):
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
                IF NOT EXISTS (select from statementnotes."OtherNon_OperatingIncome" where "report_ID"=%(report_ID)s and "title"=%(title)s) THEN
               INSERT INTO statementnotes."OtherNon_OperatingIncome"(
                "report_ID", title, "StartPeriodAmount", "EndPeriodAmount", "StartPeriod", "EndPeriod")
                VALUES ( %(report_ID)s, %(title)s, %(StartPeriodAmount)s, %(EndPeriodAmount)s, %(StartPeriod)s, %(EndPeriod)s);
                END IF;
            END
            $$ 
        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print(DF4.report_ID)
                print('Failed to Update Data ', error)
                #log_it('Failed to Update OtherLayers ')
                #return False
            
    finally:
        if(connection):
            cursor.close()
            connection.close()     
def OtherOperatingExpenses():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as
        s on p."report_ID"=s."TracingNo" where (p."tableID"='Other operating expenses' 
        or p."tableID"='Other operational Expenses' or p."tableID"='Other Operational Expenses') and  p."report_ID" not in (select "report_ID" from statementnotes."OtherOperatingExpenses" ) 
        """, connection)
        #   

        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_OtherOperatingExpenses():
    df=OtherOperatingExpenses()
    for index,row in df.iterrows():
        if(index%200==0):
            print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        if len(df1.columns)!=5:
            print(len(df1.columns))
        toDates=[]
        for col in df1.columns:
                if re.search('\d\d\d\d/\d\d/\d\d',col):
                    ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                    if ss!="" and ss not in toDates:
                        toDates.append(ss)
        toDates.sort()
        if len(toDates)!=2:
            print(len(toDates))
        df1.replace('ك','ک',regex=True,inplace=True)
        df1.replace('ي','ی',regex=True,inplace=True)
        df1 = df1.rename(columns={'شرح ': 'title'})
        for col in df1.columns:
            if toDates[0] in col:
                df1 = df1.rename(columns={col: 'StartPeriodAmount'})  
            if toDates[1] in col:
                df1 = df1.rename(columns={col: 'EndPeriodAmount'})
        df1['report_ID']=row['report_ID']
        df1['StartPeriod']=toDates[0]
        df1['EndPeriod']=toDates[1]
        InsertOtherOperatingExpenses(df1)
def InsertOtherOperatingExpenses(DF4):
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
                IF NOT EXISTS (select from statementnotes."OtherOperatingExpenses" where "report_ID"=%(report_ID)s and "title"=%(title)s) THEN
               INSERT INTO statementnotes."OtherOperatingExpenses"(
                "report_ID", title, "StartPeriodAmount", "EndPeriodAmount", "StartPeriod", "EndPeriod")
                VALUES ( %(report_ID)s, %(title)s, %(StartPeriodAmount)s, %(EndPeriodAmount)s, %(StartPeriod)s, %(EndPeriod)s);
                END IF;
            END
            $$ 
        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print(DF4.report_ID)
                print('Failed to Update Data ', error)
                #log_it('Failed to Update OtherLayers ')
                #return False
            
    finally:
        if(connection):
            cursor.close()
            connection.close()   
def OtherOperatingIncome():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as
	    s on p."report_ID"=s."TracingNo" where (p."tableID"='Other operating income' or 
        p."tableID"='Other operational incomes' or p."tableID"='Other Operational Revenues') 
        and  p."report_ID" not in (select "report_ID" from statementnotes."OtherOperatingIncome" ) 
        """, connection)
        #   

        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_OtherOperatingIncome():
    df=OtherOperatingIncome()
    for index,row in df.iterrows():
        try:
            if(index%200==0):
                print(index)
            df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
            # if len(df1.columns)!=5:
            #     print(len(df1.columns))
            toDates=[]
            for col in df1.columns:
                    if re.search('\d\d\d\d/\d\d/\d\d',col):
                        ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                        if ss!="" and ss not in toDates:
                            toDates.append(ss)
            toDates.sort()
            # if len(toDates)!=2:
            #     print(len(toDates))
            df1.replace('ك','ک',regex=True,inplace=True)
            df1.replace('ي','ی',regex=True,inplace=True)
            df1 = df1.rename(columns={'شرح ': 'title'})
            for col in df1.columns:
                if toDates[0] in col:
                    df1 = df1.rename(columns={col: 'StartPeriodAmount'})  
                if toDates[1] in col:
                    df1 = df1.rename(columns={col: 'EndPeriodAmount'})
            df1['report_ID']=row['report_ID']
            df1['StartPeriod']=toDates[0]
            df1['EndPeriod']=toDates[1]
            InsertOtherOperatingIncome(df1)
        except:
            continue
def InsertOtherOperatingIncome(DF4):
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
                IF NOT EXISTS (select from statementnotes."OtherOperatingIncome" where "report_ID"=%(report_ID)s and "title"=%(title)s) THEN
               INSERT INTO statementnotes."OtherOperatingIncome"(
                "report_ID", title, "StartPeriodAmount", "EndPeriodAmount", "StartPeriod", "EndPeriod")
                VALUES ( %(report_ID)s, %(title)s, %(StartPeriodAmount)s, %(EndPeriodAmount)s, %(StartPeriod)s, %(EndPeriod)s);
                END IF;
            END
            $$ 
        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print(DF4.report_ID)
                print('Failed to Update Data ', error)
                #log_it('Failed to Update OtherLayers ')
                #return False
            
    finally:
        if(connection):
            cursor.close()
            connection.close()    
def OverHead():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" 
        where p."tableID"='Overhead' and p."report_ID" not in (select "report_ID" from statementnotes."Overhead" )    

        """, connection)
        #     
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_OverHead():
    df=OverHead()
    for index,row in df.iterrows():
        if(index%200==0):
            print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        if len(df1.columns)!=11:
            print(len(df1.columns))
        toDates=[]
        for col in df1.columns:
                if re.search('\d\d\d\d/\d\d/\d\d',col):
                    ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                    if ss!="" and ss not in toDates:
                        toDates.append(ss)
        toDates.sort()
        # if len(toDates)!=3:
        #     charttai.append(df1)
        #     print(len(toDates))
        df1.replace('ک','ك',regex=True,inplace=True)
        df1.replace('ی','ي',regex=True,inplace=True)
        df1.columns= df1.columns.str.replace('ي','ی',regex=True)
        df1.columns= df1.columns.str.replace('ي','ی',regex=True)
        df1 = df1.rename(columns={'شرح ': 'title'})
        if len(toDates)==2:
            for col in df1.columns:
                if toDates[0] in col and 'سربار' in col:
                    df1 = df1.rename(columns={col: 'OverheadStartPeriodAmount'})  
                if toDates[1] in col and 'سربار' in col:
                    df1 = df1.rename(columns={col: 'OverheadEndPeriodAmount'})
                if toDates[0] in col and 'اداری' in col:
                    df1 = df1.rename(columns={col: 'AdministrativeStartPeriodAmount'})  
                if toDates[1] in col and 'اداری' in col:
                    df1 = df1.rename(columns={col: 'AdministrativeEndPeriodAmount'})
            df1['report_ID']=row['report_ID']
            df1['StartPeriod']=toDates[0]
            df1['EndPeriod']=toDates[1]
            df1['ShortTerm']=""
            df1['LongTerm']=""
            df1['OverheadShortTermEstimation']=0
            df1['OverheadLongTermEstimation']=0
            df1['AdministrativeLongTermEstimation']=0
            df1['AdministrativeShortTermEstimation']=0
        if len(toDates)==3:
            for col in df1.columns:
                if toDates[0] in col and 'سربار' in col:
                    df1 = df1.rename(columns={col: 'OverheadStartPeriodAmount'})  
                if toDates[1] in col and 'سربار' in col:
                    df1 = df1.rename(columns={col: 'OverheadEndPeriodAmount'})
                if toDates[2] in col and 'سربار' in col:
                    df1 = df1.rename(columns={col: 'OverheadShortTermEstimation'})
                if toDates[2] not in col and 'برآورد' in col and 'سربار' in col:
                    df1 = df1.rename(columns={col: 'OverheadLongTermEstimation'})
                if toDates[0] in col and 'اداری' in col:
                    df1 = df1.rename(columns={col: 'AdministrativeStartPeriodAmount'})  
                if toDates[1] in col and 'اداری' in col:
                    df1 = df1.rename(columns={col: 'AdministrativeEndPeriodAmount'})
                if toDates[2] in col and 'اداری' in col:
                    df1 = df1.rename(columns={col: 'AdministrativeShortTermEstimation'})
                if toDates[2] not in col and 'برآورد' in col and 'اداری' in col:
                    df1 = df1.rename(columns={col: 'AdministrativeLongTermEstimation'})
            df1['report_ID']=row['report_ID']
            df1['StartPeriod']=toDates[0]
            df1['EndPeriod']=toDates[1]
            df1['ShortTerm']=toDates[2]
            df1['LongTerm']=""
            df1['AdministrativeLongTermEstimation']=0
            df1['OverheadLongTermEstimation']=0
        if len(toDates)==4:
            for col in df1.columns:
                if toDates[0] in col and 'سربار' in col:
                    df1 = df1.rename(columns={col: 'OverheadStartPeriodAmount'})  
                if toDates[1] in col and 'سربار' in col:
                    df1 = df1.rename(columns={col: 'OverheadEndPeriodAmount'})
                if toDates[2] in col and 'سربار' in col:
                    df1 = df1.rename(columns={col: 'OverheadShortTermEstimation'})
                if toDates[3] in col and  'سربار' in col:
                    df1 = df1.rename(columns={col: 'OverheadLongTermEstimation'})
                if toDates[0] in col and 'اداری' in col:
                    df1 = df1.rename(columns={col: 'AdministrativeStartPeriodAmount'})  
                if toDates[1] in col and 'اداری' in col:
                    df1 = df1.rename(columns={col: 'AdministrativeEndPeriodAmount'})
                if toDates[2] in col and 'اداری' in col:
                    df1 = df1.rename(columns={col: 'AdministrativeShortTermEstimation'})

                if toDates[3] in col and 'اداری' in col:
                    df1 = df1.rename(columns={col: 'AdministrativeLongTermEstimation'})
            df1['report_ID']=row['report_ID']
            df1['StartPeriod']=toDates[0]
            df1['EndPeriod']=toDates[1]
            df1['ShortTerm']=toDates[2]
            df1['LongTerm']=toDates[3]
        df1=df1[df1['title']==df1['title']]
        InsertOverHead(df1)
def InsertOverHead(DF4):
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
                IF NOT EXISTS (select from statementnotes."Overhead" where "report_ID"=%(report_ID)s and "title"=%(title)s) THEN
               INSERT INTO statementnotes."Overhead"(
	"report_ID", title, "OverheadStartPeriod", "OverheadEndPeriod", "OverheadShortEstimation", "OverheadLongEstimation", "AdminStartPeriod", "AdminEndPeriod", "AdminShortEstimation", "AdminLongEstimation", "StartPeriod", "EndPeriod", "ShortTermPeriod", "LongTermPeriod")
	VALUES (%(report_ID)s,%(title)s,%(OverheadStartPeriodAmount)s,%(OverheadEndPeriodAmount)s,%(OverheadShortTermEstimation)s,%(OverheadLongTermEstimation)s,%(AdministrativeStartPeriodAmount)s,%(AdministrativeEndPeriodAmount)s,%(AdministrativeShortTermEstimation)s,%(AdministrativeLongTermEstimation)s,%(StartPeriod)s,%(EndPeriod)s,%(ShortTerm)s,%(LongTerm)s);
                END IF;
            END
            $$ 
        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print(DF4.report_ID)
                print('Failed to Update Data ', error)
                #log_it('Failed to Update OtherLayers ')
                #return False
            
    finally:
        if(connection):
            cursor.close()
            connection.close()  
def OverHeadAndCorporate():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" 
        where p."tableID"='Overheads and corporate and administrative expenses' and p."report_ID" not in 
        (select "report_ID" from statementnotes."Overhead" )    

        """, connection)
        #     
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def handle_OverheadAndCorporate():
    df=OverHeadAndCorporate()
    for index,row in df.iterrows():
        if(index%200==0):
            print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        if len(df1.columns)!=11:
            print(len(df1.columns))
        toDates=[]
        for col in df1.columns:
                if re.search('\d\d\d\d/\d\d/\d\d',col):
                    ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                    if ss!="" and ss not in toDates:
                        toDates.append(ss)
        toDates.sort()
        # if len(toDates)!=3:
        #     charttai.append(df1)
        #     print(len(toDates))
        df1.replace('ک','ك',regex=True,inplace=True)
        df1.replace('ی','ي',regex=True,inplace=True)
        df1.columns= df1.columns.str.replace('ي','ی',regex=True)
        df1.columns= df1.columns.str.replace('ي','ی',regex=True)
        df1 = df1.rename(columns={'شرح ': 'title'})
        if len(toDates)==0:
            df1.columns=['ii','title','OverheadStartPeriodAmount','OverheadEndPeriodAmount','OverheadShortTermEstimation','OverheadLongTermEstimation','AdministrativeStartPeriodAmount','AdministrativeEndPeriodAmount','AdministrativeShortTermEstimation','AdministrativeLongTermEstimation','metaID']
            df1['report_ID']=row['report_ID']
            df1['StartPeriod']=""
            df1['EndPeriod']=""
            df1['ShortTerm']=""
            df1['LongTerm']=""
        if len(toDates)==2:
            for col in df1.columns:
                if toDates[0] in col and 'سربار' in col:
                    df1 = df1.rename(columns={col: 'OverheadStartPeriodAmount'})  
                if toDates[1] in col and 'سربار' in col:
                    df1 = df1.rename(columns={col: 'OverheadEndPeriodAmount'})
                if toDates[0] in col and 'اداری' in col:
                    df1 = df1.rename(columns={col: 'AdministrativeStartPeriodAmount'})  
                if toDates[1] in col and 'اداری' in col:
                    df1 = df1.rename(columns={col: 'AdministrativeEndPeriodAmount'})
            df1['report_ID']=row['report_ID']
            df1['StartPeriod']=toDates[0]
            df1['EndPeriod']=toDates[1]
            df1['ShortTerm']=""
            df1['LongTerm']=""
            df1['OverheadShortTermEstimation']=0
            df1['OverheadLongTermEstimation']=0
            df1['AdministrativeLongTermEstimation']=0
            df1['AdministrativeShortTermEstimation']=0
        if len(toDates)==3:
            for col in df1.columns:
                if toDates[0] in col and 'سربار' in col:
                    df1 = df1.rename(columns={col: 'OverheadStartPeriodAmount'})  
                if toDates[1] in col and 'سربار' in col:
                    df1 = df1.rename(columns={col: 'OverheadEndPeriodAmount'})
                if toDates[2] in col and 'سربار' in col:
                    df1 = df1.rename(columns={col: 'OverheadShortTermEstimation'})
                if toDates[2] not in col and 'برآورد' in col and 'سربار' in col:
                    df1 = df1.rename(columns={col: 'OverheadLongTermEstimation'})
                if toDates[0] in col and 'اداری' in col:
                    df1 = df1.rename(columns={col: 'AdministrativeStartPeriodAmount'})  
                if toDates[1] in col and 'اداری' in col:
                    df1 = df1.rename(columns={col: 'AdministrativeEndPeriodAmount'})
                if toDates[2] in col and 'اداری' in col:
                    df1 = df1.rename(columns={col: 'AdministrativeShortTermEstimation'})
                if toDates[2] not in col and 'برآورد' in col and 'اداری' in col:
                    df1 = df1.rename(columns={col: 'AdministrativeLongTermEstimation'})
            df1['report_ID']=row['report_ID']
            df1['StartPeriod']=toDates[0]
            df1['EndPeriod']=toDates[1]
            df1['ShortTerm']=toDates[2]
            df1['LongTerm']=""
            df1['AdministrativeLongTermEstimation']=0
            df1['OverheadLongTermEstimation']=0
        if len(toDates)==4:
            for col in df1.columns:
                if toDates[0] in col and 'سربار' in col:
                    df1 = df1.rename(columns={col: 'OverheadStartPeriodAmount'})  
                if toDates[1] in col and 'سربار' in col:
                    df1 = df1.rename(columns={col: 'OverheadEndPeriodAmount'})
                if toDates[2] in col and 'سربار' in col:
                    df1 = df1.rename(columns={col: 'OverheadShortTermEstimation'})
                if toDates[3] in col and  'سربار' in col:
                    df1 = df1.rename(columns={col: 'OverheadLongTermEstimation'})
                if toDates[0] in col and 'اداری' in col:
                    df1 = df1.rename(columns={col: 'AdministrativeStartPeriodAmount'})  
                if toDates[1] in col and 'اداری' in col:
                    df1 = df1.rename(columns={col: 'AdministrativeEndPeriodAmount'})
                if toDates[2] in col and 'اداری' in col:
                    df1 = df1.rename(columns={col: 'AdministrativeShortTermEstimation'})

                if toDates[3] in col and 'اداری' in col:
                    df1 = df1.rename(columns={col: 'AdministrativeLongTermEstimation'})
            df1['report_ID']=row['report_ID']
            df1['StartPeriod']=toDates[0]
            df1['EndPeriod']=toDates[1]
            df1['ShortTerm']=toDates[2]
            df1['LongTerm']=toDates[3]
        df1=df1[df1['title']==df1['title']]
        InsertOverHead(df1)
def PremiumIssued():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" 
            where p."tableID"='Premium issued (reciprocal acceptance)'  and p."report_ID" not in (select "report_ID" from statementnotes."PremiumIssued" )

        """, connection)
        #         
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def InsertPremiumIssued(DF4):
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
                IF NOT EXISTS (select from statementnotes."PremiumIssued" where "report_ID"=%(report_ID)s and "title"=%(title)s) THEN
               INSERT INTO statementnotes."PremiumIssued"(
 "report_ID","title", "IssuedStartPeriod", "IssuedEndPeriod", "IssuedShortTermEstimation", "IssuedLongTermEstimation", "EtkaiStartPeriod", "EtkaiEndPeriod", "EtkaiShortTermEstimation", "EtkaiLongTermEstimation", "StartPeriod", "EndPeriod", "ShortTerm", "LongTerm")
	VALUES (%(report_ID)s,%(title)s,%(IssuedStartPeriod)s,%(IssuedEndPeriod)s,%(IssuedShortTermEstimation)s,%(IssuedLongTermEstimation)s,%(EtkaiStartPeriod)s,%(IssuedEndPeriod)s,%(EtkaiShortTermEstimation)s,%(EtkaiLongTermEstimation)s,%(StartPeriod)s,%(EndPeriod)s,%(ShortTerm)s,%(LongTerm)s);
                END IF;
            END
            $$ 
        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
                if(connection):
                    print(DF4.report_ID)
                    print('Failed to Update Data ', error)
                    #log_it('Failed to Update OtherLayers ')
                    #return False
                
    finally:
        if(connection):
            cursor.close()
            connection.close()  
def handle_PremiumIssued():
    df=PremiumIssued()
    for index,row in df.iterrows():
        if(index%200==0):
            print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        if len(df1.columns)!=11:
            print(len(df1.columns))
        toDates=[]
        for col in df1.columns:
                if re.search('\d\d\d\d/\d\d/\d\d',col):
                    ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                    if ss!="" and ss not in toDates:
                        toDates.append(ss)
        toDates.sort()
        if len(toDates)!=4 and len(toDates)!=3:
        # #     charttai.append(df1)
            print(len(toDates))
        df1.replace('ک','ك',regex=True,inplace=True)
        df1.replace('ی','ي',regex=True,inplace=True)
        df1.columns= df1.columns.str.replace('ي','ی',regex=True)
        df1.columns= df1.columns.str.replace('ي','ی',regex=True)
        df1 = df1.rename(columns={'رشته بیمه ': 'title'})
        if len(toDates)==3:
            for col in df1.columns:
                if toDates[0] in col and 'صادره' in col:
                    df1 = df1.rename(columns={col: 'IssuedStartPeriod'})  
                if toDates[1] in col and 'صادره' in col:
                    df1 = df1.rename(columns={col: 'IssuedEndPeriod'})
                if toDates[2] in col and 'صادره' in col:
                    df1 = df1.rename(columns={col: 'IssuedShortTermEstimation'})
                if toDates[2] not in col and 'برآورد' in col and 'صادره' in col:
                    df1 = df1.rename(columns={col: 'IssuedLongTermEstimation'})
                if toDates[0] in col and 'اتکایی' in col:
                    df1 = df1.rename(columns={col: 'EtkaiStartPeriod'})  
                if toDates[1] in col and 'اتکایی' in col:
                    df1 = df1.rename(columns={col: 'EtkaiEndPeriod'})
                if toDates[2] in col and 'اتکایی' in col:
                    df1 = df1.rename(columns={col: 'EtkaiShortTermEstimation'})
                if toDates[2] not in col and 'برآورد' in col and 'اتکایی' in col:
                    df1 = df1.rename(columns={col: 'EtkaiLongTermEstimation'})
            df1['report_ID']=row['report_ID']
            df1['StartPeriod']=toDates[0]
            df1['EndPeriod']=toDates[1]
            df1['ShortTerm']=toDates[2]
            df1['LongTerm']=""
        if len(toDates)==4:
            for col in df1.columns:
                if toDates[0] in col and 'صادره' in col:
                    df1 = df1.rename(columns={col: 'IssuedStartPeriod'})  
                if toDates[1] in col and 'صادره' in col:
                    df1 = df1.rename(columns={col: 'IssuedEndPeriod'})
                if toDates[2] in col and 'صادره' in col:
                    df1 = df1.rename(columns={col: 'IssuedShortTermEstimation'})
                if toDates[3] in col and  'صادره' in col:
                    df1 = df1.rename(columns={col: 'IssuedLongTermEstimation'})
                if toDates[0] in col and 'اتکایی' in col:
                    df1 = df1.rename(columns={col: 'EtkaiStartPeriod'})  
                if toDates[1] in col and 'اتکایی' in col:
                    df1 = df1.rename(columns={col: 'EtkaiEndPeriod'})
                if toDates[2] in col and 'اتکایی' in col:
                    df1 = df1.rename(columns={col: 'EtkaiShortTermEstimation'})
                if toDates[3] in col and 'اتکایی' in col:
                    df1 = df1.rename(columns={col: 'EtkaiLongTermEstimation'})
            df1['report_ID']=row['report_ID']
            df1['StartPeriod']=toDates[0]
            df1['EndPeriod']=toDates[1]
            df1['ShortTerm']=toDates[2]
            df1['LongTerm']=toDates[3]
        df1=df1[df1['title']==df1['title']]
        InsertPremiumIssued(df1)
def Profitlossfrominvestmentsanddeposits():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" 
        where p."tableID"='Profit (loss) from investments and deposits'  and p."report_ID" not in (select "report_ID" from statementnotes."Profit_lossFromInvestmentAndDeposits" )       

        """, connection)
        #  
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close() 
def InsertProfitlossfrominvestmentsanddeposits(DF4):
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
                IF NOT EXISTS (select from statementnotes."Profit_lossFromInvestmentAndDeposits" where "report_ID"=%(report_ID)s and "title"=%(title)s) THEN
               INSERT INTO statementnotes."Profit_lossFromInvestmentAndDeposits"(
	"report_ID", title, "StartPeriodAmount", "EndPeriodAmount", "ShortTermEstimation", "ShortTermEstimationDesc", "LongTermEstimation", "LongTermEstimationDesc", "StartPeriod", "EndPeriod", "ShortTerm", "LongTerm")
	VALUES (%(report_ID)s,%(title)s,%(StartPeriodAmount)s,%(EndPeriodAmount)s,%(ShortTermEstimation)s,%(ShortTermEstimationDesc)s,%(LongTermEstimation)s,%(LongTermEstimationDesc)s,%(StartPeriod)s,%(EndPeriod)s,%(ShortTerm)s,%(LongTerm)s);
                END IF;
            END
            $$ 
        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
                if(connection):
                    print(DF4.report_ID)
                    print('Failed to Update Data ', error)
                    #log_it('Failed to Update OtherLayers ')
                    #return False
                
    finally:
        if(connection):
            cursor.close()
            connection.close()  
def handle_Profitlossfrominvestmentsanddeposits():
    df=Profitlossfrominvestmentsanddeposits()
    for index,row in df.iterrows():
        if(index%200==0):
            print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        if len(df1.columns)!=9:
            print(len(df1.columns))
        toDates=[]
        for col in df1.columns:
                if re.search('\d\d\d\d/\d\d/\d\d',col):
                    ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                    if ss!="" and ss not in toDates:
                        toDates.append(ss)
        toDates.sort()
        if len(toDates)!=4 and len(toDates)!=3:
            print(len(toDates))
        df1.replace('ک','ك',regex=True,inplace=True)
        df1.replace('ی','ي',regex=True,inplace=True)
        df1.columns= df1.columns.str.replace('ي','ی',regex=True)
        df1.columns= df1.columns.str.replace('ي','ی',regex=True)
        df1 = df1.rename(columns={'شرح ': 'title'})
        if len(toDates)==3:
            for col in df1.columns:
                if toDates[0] in col:
                    df1 = df1.rename(columns={col: 'StartPeriodAmount'})  
                if toDates[1] in col:
                    df1 = df1.rename(columns={col: 'EndPeriodAmount'})
                if toDates[2] in col and 'پیش بینی' in col:
                    df1 = df1.rename(columns={col: 'ShortTermEstimation'})
                if toDates[2] in col and 'روند' in col:
                    df1 = df1.rename(columns={col: 'ShortTermEstimationDesc'})
                if toDates[2] not in col and 'پیش بینی' in col:
                    df1 = df1.rename(columns={col: 'LongTermEstimation'})
                if toDates[2] not in col and 'روند' in col:
                    df1 = df1.rename(columns={col: 'LongTermEstimationDesc'})
            df1['report_ID']=row['report_ID']
            df1['StartPeriod']=toDates[0]
            df1['EndPeriod']=toDates[1]
            df1['ShortTerm']=toDates[2]
            df1['LongTerm']=""
        if len(toDates)==4:
            for col in df1.columns:
                if toDates[0] in col:
                    df1 = df1.rename(columns={col: 'StartPeriodAmount'})  
                if toDates[1] in col:
                    df1 = df1.rename(columns={col: 'EndPeriodAmount'})
                if toDates[2] in col and 'پیش بینی' in col:
                    df1 = df1.rename(columns={col: 'ShortTermEstimation'})
                if toDates[2] in col and 'روند' in col:
                    df1 = df1.rename(columns={col: 'ShortTermEstimationDesc'})
                if toDates[3] in col and 'پیش بینی' in col:
                    df1 = df1.rename(columns={col: 'LongTermEstimation'})
                if toDates[3] in col and 'روند' in col:
                    df1 = df1.rename(columns={col: 'LongTermEstimationDesc'})
            df1['report_ID']=row['report_ID']
            df1['StartPeriod']=toDates[0]
            df1['EndPeriod']=toDates[1]
            df1['ShortTerm']=toDates[2]
            df1['LongTerm']=toDates[3]
        df1=df1[df1['title']==df1['title']]
        InsertProfitlossfrominvestmentsanddeposits(df1)  
def Purchaseandconsumptionofrawmaterials():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" 
        where p."tableID"='Purchase and consumption of raw materials'  and p."report_ID" not in (select "report_ID" from statementnotes."RawMaterialPurchaseAndConsumption" )       

        """, connection)
        #  and p."report_ID" not in (select "report_ID" from statementnotes."RawMaterialPurchaseAndConsumption" )       
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close()
def InsertPurchaseandconsumptionofrawmaterials(DF4):
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
                IF NOT EXISTS (select from statementnotes."RawMaterialPurchaseAndConsumption" where "report_ID"=%(report_ID)s and "title"=%(title)s) THEN
               INSERT INTO statementnotes."RawMaterialPurchaseAndConsumption"(
	"report_ID", title, unit, "StartPeriodQuantity", "StartPeriodFee", "StartPeriodAmount", "BuyInQuantity", "BuyInFee", "BuyInAmount", "ConsumptionQuantity", "ConsumptionFee", "ConsumptionAmount", "EndOfPeriodQuantity", "EndOfPeriodFee", "EndOfPeriodAmount", "StartPeriod")
	VALUES (%(report_ID)s, %(title)s, %(unit)s, %(StartPeriodQuantity)s, %(StartPeriodFee)s, %(StartPeriodAmount)s, %(BuyInPeriodQuantity)s, %(BuyInPeriodFee)s, %(BuyInPeriodAmount)s, %(ConsumptionQuantity)s,%(ConsumptionFee)s, %(ConsumptionAmount)s, %(EndOfPeriodQuantity)s, %(EndOfPeriodFee)s, %(EndOfPeriodAmount)s, %(StartPeriod)s);
                END IF;
            END
            $$ 
        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
                if(connection):
                    print(DF4.report_ID)
                    print('Failed to Update Data ', error)
                    #log_it('Failed to Update OtherLayers ')
                    #return False
                
    finally:
        if(connection):
            cursor.close()
            connection.close()   
def handle_Purchaseandconsumptionofrawmaterials():
    df=Purchaseandconsumptionofrawmaterials()
    for index,row in df.iterrows():
        if(index%200==0):
            print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        if len(df1.columns)!=16:
            print(len(df1.columns))
        toDates=[]
        for col in df1.columns:
                if re.search('\d\d\d\d/\d\d/\d\d',col):
                    ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                    if ss!="" and ss not in toDates:
                        toDates.append(ss)
        toDates.sort()
        if len(toDates)!=1:
            print(len(toDates))
        df1.replace('ک','ك',regex=True,inplace=True)
        df1.replace('ی','ي',regex=True,inplace=True)
        df1.columns= df1.columns.str.replace('ي','ی',regex=True)
        df1.columns= df1.columns.str.replace('ي','ی',regex=True)
        df1 = df1.rename(columns={'شرح ': 'title'})
        df1 = df1.rename(columns={'واحد ': 'unit'})
        if len(toDates)==1:
            for col in df1.columns:
                if 'اول دوره' in col and 'مقدار' in col:
                    df1 = df1.rename(columns={col: 'StartPeriodQuantity'})
                if 'اول دوره' in col and 'نرخ' in col:
                    df1 = df1.rename(columns={col: 'StartPeriodFee'}) 
                if 'اول دوره' in col and 'مبلغ' in col:
                    df1 = df1.rename(columns={col: 'StartPeriodAmount'}) 
                if 'خرید' in col and 'مقدار' in col:
                    df1 = df1.rename(columns={col: 'BuyInPeriodQuantity'}) 
                if 'خرید' in col and 'نرخ' in col:
                    df1 = df1.rename(columns={col: 'BuyInPeriodFee'}) 
                if 'خرید' in col and 'مبلغ' in col:
                    df1 = df1.rename(columns={col: 'BuyInPeriodAmount'}) 
                if 'مصرف' in col and 'مقدار' in col:
                    df1 = df1.rename(columns={col: 'ConsumptionQuantity'}) 
                if 'مصرف' in col and 'نرخ' in col:
                    df1 = df1.rename(columns={col: 'ConsumptionFee'})   
                if 'مصرف' in col and 'مبلغ' in col:
                    df1 = df1.rename(columns={col: 'ConsumptionAmount'}) 
                if 'پایان دوره' in col and ('نرخ' not in col and 'مبلغ' not in col) or ('مقدار' in col):
                    df1 = df1.rename(columns={col: 'EndOfPeriodQuantity'}) 
                if 'پایان دوره' in col and 'نرخ' in col:
                    df1 = df1.rename(columns={col: 'EndOfPeriodFee'})   
                if 'پایان دوره' in col and 'مبلغ' in col:
                    df1 = df1.rename(columns={col: 'EndOfPeriodAmount'})  
            df1['report_ID']=row['report_ID']
            df1['StartPeriod']=toDates[0]
        df1=df1[df1['title']==df1['title']]
        if set(df1.columns)!=set(['Unnamed: 0','title','unit', 'StartPeriodQuantity', 'StartPeriodFee', 'StartPeriodAmount', 'BuyInPeriodQuantity', 'BuyInPeriodFee',
    'BuyInPeriodAmount', 'ConsumptionQuantity', 'ConsumptionFee', 'ConsumptionAmount', 'EndOfPeriodQuantity', 'EndOfPeriodFee', 'EndOfPeriodAmount', 'metaID', 'report_ID', 'StartPeriod']):
            df1.columns=['Unnamed: 0','title','unit', 'StartPeriodQuantity', 'StartPeriodFee', 'StartPeriodAmount', 'BuyInPeriodQuantity', 'BuyInPeriodFee',
    'BuyInPeriodAmount', 'ConsumptionQuantity', 'ConsumptionFee', 'ConsumptionAmount', 'EndOfPeriodQuantity', 'EndOfPeriodFee', 'EndOfPeriodAmount', 'metaID', 'report_ID', 'StartPeriod']
        InsertPurchaseandconsumptionofrawmaterials(df1)
def Reservefacilityforrecoveryandstorageofdoubtfulclaims():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" 
        where p."tableID"='Reserve facility for recovery and storage of doubtful claims'  and p."report_ID" not in (select "report_ID" from statementnotes."Reservefacility" )       
        """, connection)
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close()
def InsertReservefacilityforrecoveryandstorageofdoubtfulclaims(DF4):
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
                IF NOT EXISTS (select from statementnotes."Reservefacility" where "report_ID"=%(report_ID)s and "title"=%(title)s) THEN
               INSERT INTO statementnotes."Reservefacility"(
	"report_ID", title, "StartPeriodJari", "EndPeriodJari", "StartPeriodSarresid", "EndPeriodSarresid", "StartPeriodMoavagh", "EndPeriodMoavagh", "StartPeriodMashkuk", "EndPeriodMashkuk", "StartPeriodSUM", "EndPeriodSUM", "StartPeriod", "EndPeriod")
	VALUES (%(report_ID)s, %(title)s, %(StartPeriodJari)s, %(EndPeriodJari)s, %(StartPeriodSarresid)s, %(EndPeriodSarresid)s, %(StartPeriodMoavagh)s, %(EndPeriodMoavagh)s, %(StartPeriodMashkuk)s, %(EndPeriodMashkuk)s,%(StartPeriodSUM)s, %(EndPeriodSUM)s, %(StartPeriod)s, %(EndPeriod)s);
                END IF;
            END
            $$ 
        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
                if(connection):
                    print(DF4.report_ID)
                    print('Failed to Update Data ', error)
                    #log_it('Failed to Update OtherLayers ')
                    #return False
                
    finally:
        if(connection):
            cursor.close()
            connection.close() 
def handle_Reservefacilityforrecoveryandstorageofdoubtfulclaims():
    df=Reservefacilityforrecoveryandstorageofdoubtfulclaims()
    for index,row in df.iterrows():
        if(index%200==0):
            print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        if len(df1.columns)!=13:
            print(len(df1.columns))
        toDates=[]
        for col in df1.columns:
                if re.search('\d\d\d\d/\d\d/\d\d',col):
                    ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                    if ss!="" and ss not in toDates:
                        toDates.append(ss)
        toDates.sort()
        if len(toDates)!=2:
            print(len(toDates))
        df1.replace('ک','ك',regex=True,inplace=True)
        df1.replace('ی','ي',regex=True,inplace=True)
        df1.columns= df1.columns.str.replace('ي','ی',regex=True)
        df1.columns= df1.columns.str.replace('ي','ی',regex=True)
        df1 = df1.rename(columns={'شرح ': 'title'})
        df1 = df1.rename(columns={'واحد ': 'unit'})
        if len(toDates)==2:
            for col in df1.columns:
                if 'جاری' in col and toDates[0] in col:
                    df1 = df1.rename(columns={col: 'StartPeriodJari'}) 
                if 'جاری' in col and toDates[1] in col:
                    df1 = df1.rename(columns={col: 'EndPeriodJari'}) 
                if 'سررسید' in col and toDates[0] in col:
                    df1 = df1.rename(columns={col: 'StartPeriodSarresid'}) 
                if 'سررسید' in col and toDates[1] in col:
                    df1 = df1.rename(columns={col: 'EndPeriodSarresid'}) 
                if 'معوق' in col and toDates[0] in col:
                    df1 = df1.rename(columns={col: 'StartPeriodMoavagh'}) 
                if 'معوق' in col and toDates[1] in col:
                    df1 = df1.rename(columns={col: 'EndPeriodMoavagh'}) 
                if 'مشکوک' in col and toDates[0] in col:
                    df1 = df1.rename(columns={col: 'StartPeriodMashkuk'}) 
                if 'مشکوک' in col and toDates[1] in col:
                    df1 = df1.rename(columns={col: 'EndPeriodMashkuk'}) 
                if 'جمع' in col and toDates[0] in col:
                    df1 = df1.rename(columns={col: 'StartPeriodSUM'}) 
                if 'جمع' in col and toDates[1] in col:
                    df1 = df1.rename(columns={col: 'EndPeriodSUM'}) 
            df1['report_ID']=row['report_ID']
            df1['StartPeriod']=toDates[0]
            df1['EndPeriod']=toDates[1]
        df1=df1[df1['title']==df1['title']]
        InsertReservefacilityforrecoveryandstorageofdoubtfulclaims(df1)
def ResourceItemList():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" 
        where p."tableID"='Resource Item List ' and p."report_ID" not in (select "report_ID" from statementnotes."ResourceItemlist")       
        """, connection)
        #  
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close()
def InsertResourceItemList(DF4):
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
                IF NOT EXISTS (select from statementnotes."ResourceItemlist" where "report_ID"=%(report_ID)s and "title"=%(title)s) THEN
               INSERT INTO statementnotes."ResourceItemlist"(
	"report_ID", title, "StartPeriodAmount", "EndPeriodAmount", "ShortTermEstimation", "LongTermEstimation", "StartPeriod", "EndPeriod", "ShortTerm", "LongTerm")
	VALUES (%(report_ID)s, %(title)s, %(StartPeriodAmount)s, %(EndPeriodAmount)s, %(ShortTermEstimation)s, %(LongTermEstimation)s, %(StartPeriod)s, %(EndPeriod)s, %(ShortTerm)s, %(LongTerm)s);
                END IF;
            END
            $$ 
        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
                if(connection):
                    print(DF4.report_ID)
                    print('Failed to Update Data ', error)
                    #log_it('Failed to Update OtherLayers ')
                    #return False
                
    finally:
        if(connection):
            cursor.close()
            connection.close() 
def handle_ResourceItemList():
    df=ResourceItemList()
    for index,row in df.iterrows():
        if(index%200==0):
            print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        if len(df1.columns)!=7:
            print(len(df1.columns))
        toDates=[]
        for col in df1.columns:
                if re.search('\d\d\d\d/\d\d/\d\d',col):
                    ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                    if ss!="" and ss not in toDates:
                        toDates.append(ss)
        toDates.sort()
        # if len(toDates)!=4:
        #     print(len(toDates))
        df1.replace('ک','ك',regex=True,inplace=True)
        df1.replace('ی','ي',regex=True,inplace=True)
        df1.columns= df1.columns.str.replace('ي','ی',regex=True)
        df1.columns= df1.columns.str.replace('ي','ی',regex=True)
        df1 = df1.rename(columns={'شرح منابع ': 'title'})
        if len(toDates)==4:
            for col in df1.columns:
                if toDates[0] in col:
                    df1 = df1.rename(columns={col: 'StartPeriodAmount'}) 
                if toDates[1] in col:
                    df1 = df1.rename(columns={col: 'EndPeriodAmount'}) 
                if toDates[2] in col:
                    df1 = df1.rename(columns={col: 'ShortTermEstimation'}) 
                if toDates[3] in col:
                    df1 = df1.rename(columns={col: 'LongTermEstimation'}) 
            df1['report_ID']=row['report_ID']
            df1['StartPeriod']=toDates[0]
            df1['EndPeriod']=toDates[1]
            df1['ShortTerm']=toDates[2]
            df1['LongTerm']=toDates[3]
        if len(toDates)==3:
            for col in df1.columns:
                if toDates[0] in col:
                    df1 = df1.rename(columns={col: 'StartPeriodAmount'}) 
                if toDates[1] in col:
                    df1 = df1.rename(columns={col: 'EndPeriodAmount'}) 
                if toDates[2] in col:
                    df1 = df1.rename(columns={col: 'ShortTermEstimation'}) 
                if 'برآورد' in col and toDates[2] not in col:
                    df1 = df1.rename(columns={col: 'LongTermEstimation'}) 
            df1['report_ID']=row['report_ID']
            df1['StartPeriod']=toDates[0]
            df1['EndPeriod']=toDates[1]
            df1['ShortTerm']=toDates[2]
            df1['LongTerm']=""
        df1=df1[df1['title']==df1['title']]
        InsertResourceItemList(df1) 
def Rightinformationandcapitaladequacy():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" 
        where p."tableID"='Right information and capital adequacy' and p."report_ID" not in (select "report_ID" from statementnotes."RightInformationAndCapitalAdequacy")
        """, connection)
        #  and p."report_ID" not in (select "report_ID" from statementnotes."RightInformationAndCapitalAdequacy")
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close()
def InsertRightinformationandcapitaladequacy(DF4):
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
                IF NOT EXISTS (select from statementnotes."RightInformationAndCapitalAdequacy" where "report_ID"=%(report_ID)s and "title"=%(title)s) THEN
               INSERT INTO statementnotes."RightInformationAndCapitalAdequacy"(
	"report_ID", title, "StartPeriodAmount", "EndPeriodAmount", "ShortTermEstimation", "LongTermEstimation", "StartPeriod", "EndPeriod", "ShortTerm", "LongTerm")
	VALUES (%(report_ID)s, %(title)s, %(StartPeriodAmount)s, %(EndPeriodAmount)s, %(ShortTermEstimation)s, %(LongTermEstimation)s, %(StartPeriod)s, %(EndPeriod)s, %(ShortTerm)s, %(LongTerm)s);
                END IF;
            END
            $$ 
        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
                if(connection):
                    print(DF4.report_ID)
                    print('Failed to Update Data ', error)
                    #log_it('Failed to Update OtherLayers ')
                    #return False
                
    finally:
        if(connection):
            cursor.close()
            connection.close() 
def handle_RightInformationAndCapitalAdequacy():
    df=Rightinformationandcapitaladequacy()
    for index,row in df.iterrows():
        if(index%200==0):
            print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        if len(df1.columns)!=7:
            print(len(df1.columns))
        toDates=[]
        for col in df1.columns:
                if re.search('\d\d\d\d/\d\d/\d\d',col):
                    ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                    if ss!="" and ss not in toDates:
                        toDates.append(ss)
        toDates.sort()
        # if len(toDates)!=3:
        #     print(len(toDates))
        df1.replace('ک','ك',regex=True,inplace=True)
        df1.replace('ی','ي',regex=True,inplace=True)
        df1.columns= df1.columns.str.replace('ي','ی',regex=True)
        df1.columns= df1.columns.str.replace('ي','ی',regex=True)
        df1 = df1.rename(columns={'شرح ': 'title'})
        if len(toDates)==4:
            for col in df1.columns:
                if toDates[0] in col:
                    df1 = df1.rename(columns={col: 'StartPeriodAmount'}) 
                if toDates[1] in col:
                    df1 = df1.rename(columns={col: 'EndPeriodAmount'}) 
                if toDates[2] in col:
                    df1 = df1.rename(columns={col: 'ShortTermEstimation'}) 
                if toDates[3] in col:
                    df1 = df1.rename(columns={col: 'LongTermEstimation'}) 
            df1['report_ID']=row['report_ID']
            df1['StartPeriod']=toDates[0]
            df1['EndPeriod']=toDates[1]
            df1['ShortTerm']=toDates[2]
            df1['LongTerm']=toDates[3]
        if len(toDates)==3:
            for col in df1.columns:
                if toDates[0] in col:
                    df1 = df1.rename(columns={col: 'StartPeriodAmount'}) 
                if toDates[1] in col:
                    df1 = df1.rename(columns={col: 'EndPeriodAmount'}) 
                if toDates[2] in col:
                    df1 = df1.rename(columns={col: 'ShortTermEstimation'}) 
                if 'برآورد' in col and toDates[2] not in col:
                    df1 = df1.rename(columns={col: 'LongTermEstimation'}) 
            df1['report_ID']=row['report_ID']
            df1['StartPeriod']=toDates[0]
            df1['EndPeriod']=toDates[1]
            df1['ShortTerm']=toDates[2]
            df1['LongTerm']=""
        df1=df1[df1['title']==df1['title']]
        InsertRightinformationandcapitaladequacy(df1)                                                                                                                       
def Salestrendandcostoverthelast5years():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" 
        where p."tableID"='Sales trend and cost over the last 5 years'
         and p."report_ID" not in (select "report_ID" from statementnotes."Salestrendandcostoverthelast5years")
        """, connection)
        #  
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close()
def InsertSalestrendandcostoverthelast5years(DF4):
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
                IF NOT EXISTS (select from statementnotes."Salestrendandcostoverthelast5years" where "report_ID"=%(report_ID)s and "title"=%(title)s) THEN
               INSERT INTO statementnotes."Salestrendandcostoverthelast5years"(
	 "report_ID", title, "5yearsAgoAmount", "4yearsAgoAmount", "3yearsAgoAmount", "2yearsAgoAmount", "lastPeriodAmount", "CurrentPeriodAmount", "ShortTermEstimation", "LongTermEstimation", "5YearsAgo", "4YearsAgo", "3YearsAgo", "2YearsAgo", "LastPeriod", "CurrentPeriod", "ShortTerm", "LongTerm")
	VALUES (%(report_ID)s, %(title)s, %(5YearsAgo)s, %(4YearsAgo)s,%(3YearsAgo)s, %(2YearsAgo)s,%(LastPeriod)s, %(CurrentPeriod)s, %(ShortTermEstimation)s, %(LongTermEstimation)s, %(5YearsAgoDate)s, %(4YearsAgoDate)s, %(3YearsAgoDate)s, %(2YearsAgoDate)s, %(LastPeriodDate)s, %(CurrentPeriodDate)s, %(ShortTerm)s, %(LongTerm)s);
                END IF;
            END
            $$ 
        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
                if(connection):
                    print(DF4.report_ID)
                    print('Failed to Update Data ', error)
                    #log_it('Failed to Update OtherLayers ')
                    #return False
                
    finally:
        if(connection):
            cursor.close()
            connection.close()       
def handle_Salestrendandcostoverthelast5years():
    df=Salestrendandcostoverthelast5years()
    for index,row in df.iterrows():
        try:
            if(index%200==0):
                print(index)
            df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
            # if len(df1.columns)!=11:
            #     print(len(df1.columns))
            toDates=[]
            for col in df1.columns:
                    if re.search('\d\d\d\d/\d\d/\d\d',col):
                        ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                        if ss!="" and ss not in toDates:
                            toDates.append(ss)
            toDates.sort()
            # if len(toDates)!=7:
            print(len(toDates))
            df1.replace('ک','ك',regex=True,inplace=True)
            df1.replace('ی','ي',regex=True,inplace=True)
            df1.columns= df1.columns.str.replace('ي','ی',regex=True)
            df1.columns= df1.columns.str.replace('ي','ی',regex=True)
            df1 = df1.rename(columns={'شرح ': 'title'})
            if len(toDates)==7:
                for col in df1.columns:
                    if toDates[0] in col:
                        df1 = df1.rename(columns={col: '5YearsAgo'}) 
                    if toDates[1] in col:
                        df1 = df1.rename(columns={col: '4YearsAgo'}) 
                    if toDates[2] in col:
                        df1 = df1.rename(columns={col: '3YearsAgo'}) 
                    if toDates[3] in col:
                        df1 = df1.rename(columns={col: '2YearsAgo'}) 
                    if toDates[4] in col:
                        df1 = df1.rename(columns={col: 'LastPeriod'}) 
                    if toDates[5] in col:
                        df1 = df1.rename(columns={col: 'CurrentPeriod'}) 
                    if toDates[6] in col:
                        df1 = df1.rename(columns={col: 'ShortTermEstimation'}) 
                    if toDates[6] not in col and 'برآورد' in col:
                        df1 = df1.rename(columns={col: 'LongTermEstimation'}) 
                    # if toDates[7] in col:
                    #     df1 = df1.rename(columns={col: 'LongTermEstimation'})
                    
                df1['report_ID']=row['report_ID']
                df1['5YearsAgoDate']=toDates[0]
                df1['4YearsAgoDate']=toDates[1]
                df1['3YearsAgoDate']=toDates[2]
                df1['2YearsAgoDate']=toDates[3]
                df1['LastPeriodDate']=toDates[4]
                df1['CurrentPeriodDate']=toDates[5]
                df1['ShortTerm']=toDates[6]
                df1['LongTerm']=""
            if len(toDates)==8:
                for col in df1.columns:
                    if toDates[0] in col:
                        df1 = df1.rename(columns={col: '5YearsAgo'}) 
                    if toDates[1] in col:
                        df1 = df1.rename(columns={col: '4YearsAgo'}) 
                    if toDates[2] in col:
                        df1 = df1.rename(columns={col: '3YearsAgo'}) 
                    if toDates[3] in col:
                        df1 = df1.rename(columns={col: '2YearsAgo'}) 
                    if toDates[4] in col:
                        df1 = df1.rename(columns={col: 'LastPeriod'}) 
                    if toDates[5] in col:
                        df1 = df1.rename(columns={col: 'CurrentPeriod'}) 
                    if toDates[6] in col:
                        df1 = df1.rename(columns={col: 'ShortTermEstimation'}) 
                    if toDates[7] in col:
                        df1 = df1.rename(columns={col: 'LongTermEstimation'})
                    
                df1['report_ID']=row['report_ID']
                df1['5YearsAgoDate']=toDates[0]
                df1['4YearsAgoDate']=toDates[1]
                df1['3YearsAgoDate']=toDates[2]
                df1['2YearsAgoDate']=toDates[3]
                df1['LastPeriodDate']=toDates[4]
                df1['CurrentPeriodDate']=toDates[5]
                df1['ShortTerm']=toDates[6]
                df1['LongTerm']=toDates[7]
            if len(toDates)==6:
                for col in df1.columns:
                    if toDates[0] in col:
                        df1 = df1.rename(columns={col: '5YearsAgo'}) 
                    if toDates[1] in col:
                        df1 = df1.rename(columns={col: '4YearsAgo'}) 
                    if toDates[2] in col:
                        df1 = df1.rename(columns={col: '3YearsAgo'}) 
                    if toDates[3] in col:
                        df1 = df1.rename(columns={col: '2YearsAgo'}) 
                    if toDates[4] in col:
                        df1 = df1.rename(columns={col: 'LastPeriod'}) 
                    if toDates[5] in col:
                        df1 = df1.rename(columns={col: 'CurrentPeriod'}) 
                    # if toDates[6] in col:
                    #     df1 = df1.rename(columns={col: 'ShortTermEstimation'}) 
                    # if toDates[7] in col:
                    #     df1 = df1.rename(columns={col: 'LongTermEstimation'})
                    
                df1['report_ID']=row['report_ID']
                df1['5YearsAgoDate']=toDates[0]
                df1['4YearsAgoDate']=toDates[1]
                df1['3YearsAgoDate']=toDates[2]
                df1['2YearsAgoDate']=toDates[3]
                df1['LastPeriodDate']=toDates[4]
                df1['CurrentPeriodDate']=toDates[5]
                df1['ShortTerm']=""
                df1['LongTerm']=""
            if len(toDates)<6:
                df1.columns=['ii','title','5YearsAgo','4YearsAgo','3YearsAgo','2YearsAgo','LastPeriod','CurrentPeriod','ShortTermEstimation','LongTermEstimation','meta']
                df1['report_ID']=row['report_ID']
                df1['5YearsAgoDate']=""
                df1['4YearsAgoDate']=""
                df1['3YearsAgoDate']=""
                df1['2YearsAgoDate']=""
                df1['LastPeriodDate']=toDates[0]
                df1['CurrentPeriodDate']=toDates[1]
                df1['ShortTerm']=""
                df1['LongTerm']=""
            df1=df1[df1['title']==df1['title']]
            if set(df1.columns)!=set(['Unnamed: 0', 'title', '5YearsAgo', '4YearsAgo', '3YearsAgo', '2YearsAgo', 'LastPeriod', 'ShortTermEstimation', 'LongTermEstimation', 'CurrentPeriod', 'metaID', 'report_ID', '5YearsAgoDate', '4YearsAgoDate', '3YearsAgoDate', '2YearsAgoDate', 'LastPeriodDate', 'CurrentPeriodDate', 'ShortTerm', 'LongTerm']):
                df1.columns=['Unnamed: 0', 'title', '5YearsAgo', '4YearsAgo', '3YearsAgo', '2YearsAgo', 'LastPeriod', 'ShortTermEstimation', 'LongTermEstimation', 'CurrentPeriod', 'metaID', 'report_ID', '5YearsAgoDate', '4YearsAgoDate', '3YearsAgoDate', '2YearsAgoDate', 'LastPeriodDate', 'CurrentPeriodDate', 'ShortTerm', 'LongTerm']
            InsertSalestrendandcostoverthelast5years(df1)   
        except:
            continue 
def SellRevenueandExistetdPropertiesandCurrentProjects():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" 
        where p."tableID"='Sell Revenue and Existetd Properties and Current Projects' and p."report_ID" not in
         (select "report_ID" from statementnotes."SellRevenue_ExistetdPropertiesandCurrentProjects")
        """, connection)
        #   and p."report_ID" not in (select "report_ID" from statementnotes."SellRevenue_ExistetdPropertiesandCurrentProjects")
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close()
def InsertSellRevenueandExistetdPropertiesandCurrentProjects(DF4):
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
                IF NOT EXISTS (select from statementnotes."SellRevenue_ExistetdPropertiesandCurrentProjects" where "report_ID"=%(report_ID)s and "title"=%(title)s) THEN
               INSERT INTO statementnotes."SellRevenue_ExistetdPropertiesandCurrentProjects"(
	"report_ID", title, "Location", "typeOfProject", "CompanyOwnership", "ProjectStartDate", "ProjectEndDate", "CumulativeCostUntilNow", "CostEstimation", "Progress", "SoldMeterUntilStartPeriod", "SoldMeterThisPeriod", "CostOfEachMeterThisPeriod", "RevuneFromProgressThisPeriod", "RevenueFromNewSells", "EstimatedLongTermProgress", "SellLongTermEstimation", "RevuneFromProgressLongTermEstimation", "RevenueFromNewSellsLongTermEstimation", "EstimatedShortTermProgress", "SellShortTermEstimation", "RevuneFromProgressShortTermEstimation", "RevenueFromNewSellsShortTermEstimation", "EndPeriod", "ShortTerm", "LongTerm")
	VALUES (%(report_ID)s, %(title)s, %(Location)s, %(type)s,%(CompanyPercentage)s, %(ProjectStartDate)s,%(ProjectEndDate)s, %(CumulativeCostUntilNow)s, %(CostEstimation)s, %(Progress)s, %(SoldMeterUntilStartPeriod)s, %(SoldMeterThisPeriod)s, %(CostOfEachMeterThisPeriod)s, %(RevuneFromProgressThisPeriod)s,%(RevenueFromNewSells)s ,%(EstimatedLongTermProgress)s, %(SellLongTermEstimation)s, %(RevuneFromProgressLongTermEstimation)s, %(RevenueFromNewSellsLongTermEstimation)s,%(EstimatedShortTermProgress)s,%(SellShortTermEstimation)s,%(RevuneFromProgressShortTermEstimation)s,%(RevenueFromNewSellsShortTermEstimation)s,%(EndPeriod)s,%(ShortTerm)s,%(LongTerm)s);
                END IF;
            END
            $$ 
        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
                if(connection):
                    print(DF4.report_ID)
                    print('Failed to Update Data ', error)
                    #log_it('Failed to Update OtherLayers ')
                    #return False
                
    finally:
        if(connection):
            cursor.close()
            connection.close() 
def handle_SellRevenueandExistetdPropertiesandCurrentProjects():
    df=SellRevenueandExistetdPropertiesandCurrentProjects()
    for index,row in df.iterrows():
            if(index%200==0):
                print(index)
            df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
            if len(df1.columns)!=24:
                print(len(df1.columns))
            toDates=[]
            for col in df1.columns:
                    if re.search('\d\d\d\d/\d\d/\d\d',col):
                        ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                        if ss!="" and ss not in toDates:
                            toDates.append(ss)
            toDates.sort()
            if len(toDates)!=3 and len(toDates)!=2:
                print(len(toDates))
            df1.replace('ک','ك',regex=True,inplace=True)
            df1.replace('ی','ي',regex=True,inplace=True)
            df1.columns= df1.columns.str.replace('ي','ی',regex=True)
            df1.columns= df1.columns.str.replace('ي','ی',regex=True)
            df1 = df1.rename(columns={'نام پروژه ': 'title'})
            df1 = df1.rename(columns={'محل پروژه ': 'Location'})
            df1 = df1.rename(columns={'کاربری ': 'type'})
            df1 = df1.rename(columns={'متراژ قابل فروش (سهم شرکت) ': 'CompanyPercentage'})
            df1 = df1.rename(columns={'زمان شروع پروژه ': 'ProjectStartDate'})
            df1 = df1.rename(columns={'زمان تکمیل پروژه ': 'ProjectEndDate'})
            df1 = df1.rename(columns={'برآورد مخارج تکمیل پروژه ': 'CostEstimation'})
            df1 = df1.rename(columns={'درصد پیشرفت فیزیکی پروژه ': 'Progress'})
            df1 = df1.rename(columns={'متراژ واگذار شده تا ابتدای سال مالی ': 'SoldMeterUntilStartPeriod'})
            if len(toDates)==3:
                for col in df1.columns:
                    if toDates[0] in col and 'مخارج انباشته' in col:
                        df1 = df1.rename(columns={col: 'CumulativeCostUntilNow'}) 
                    if toDates[0] in col and 'متراژ واگذار شده' in col:
                        df1 = df1.rename(columns={col: 'SoldMeterThisPeriod'})
                    if toDates[0] in col and 'بهای تمام شده واحدهای فروش رفته' in col:
                        df1 = df1.rename(columns={col: 'CostOfEachMeterThisPeriod'})
                    if toDates[0] in col and 'درآمد حاصل از افزایش درصد تکمیل' in col:
                        df1 = df1.rename(columns={col: 'RevuneFromProgressThisPeriod'})
                    if toDates[0] in col and 'درآمد حاصل از فروش واحدهای جدید' in col:
                        df1 = df1.rename(columns={col: 'RevenueFromNewSells'})

                    if toDates[1] in col and 'درصد پیشرفت برآوردی' in col:
                        df1 = df1.rename(columns={col: 'EstimatedShortTermProgress'})
                    if toDates[1] in col and 'برآورد از متراژ واگذاری' in col:
                        df1 = df1.rename(columns={col: 'SellShortTermEstimation'})
                    if toDates[1] in col and 'از درآمد حاصل از افزایش درصد تکمیل' in col:
                        df1 = df1.rename(columns={col: 'RevuneFromProgressShortTermEstimation'})
                    if toDates[1] in col and 'درآمد حاصل از فروش واحدهای جدید' in col:
                        df1 = df1.rename(columns={col: 'RevenueFromNewSellsShortTermEstimation'})

                    
                    if toDates[2] in col and 'درصد پیشرفت برآوردی' in col:
                        df1 = df1.rename(columns={col: 'EstimatedLongTermProgress'})
                    if toDates[2] in col and 'برآورد از متراژ واگذاری' in col:
                        df1 = df1.rename(columns={col: 'SellLongTermEstimation'})
                    if toDates[2] in col and 'از درآمد حاصل از افزایش درصد تکمیل' in col:
                        df1 = df1.rename(columns={col: 'RevuneFromProgressLongTermEstimation'})
                    if toDates[2] in col and 'درآمد حاصل از فروش واحدهای جدید' in col:
                        df1 = df1.rename(columns={col: 'RevenueFromNewSellsLongTermEstimation'})
                df1['report_ID']=row['report_ID']
                df1['EndPeriod']=toDates[0]
                df1['ShortTerm']=toDates[1]
                df1['LongTerm']=toDates[2]
            if len(toDates)==2:
                for col in df1.columns:
                    if toDates[0] in col and 'مخارج انباشته' in col:
                        df1 = df1.rename(columns={col: 'CumulativeCostUntilNow'}) 
                    if toDates[0] in col and 'متراژ واگذار شده' in col:
                        df1 = df1.rename(columns={col: 'SoldMeterThisPeriod'})
                    if toDates[0] in col and 'بهای تمام شده واحدهای فروش رفته' in col:
                        df1 = df1.rename(columns={col: 'CostOfEachMeterThisPeriod'})
                    if toDates[0] in col and 'درآمد حاصل از افزایش درصد تکمیل' in col:
                        df1 = df1.rename(columns={col: 'RevuneFromProgressThisPeriod'})
                    if toDates[0] in col and 'درآمد حاصل از فروش واحدهای جدید' in col:
                        df1 = df1.rename(columns={col: 'RevenueFromNewSells'})

                    if toDates[1] in col and 'درصد پیشرفت برآوردی' in col:
                        df1 = df1.rename(columns={col: 'EstimatedShortTermProgress'})
                    if toDates[1] in col and 'برآورد از متراژ واگذاری' in col:
                        df1 = df1.rename(columns={col: 'SellShortTermEstimation'})
                    if toDates[1] in col and 'از درآمد حاصل از افزایش درصد تکمیل' in col:
                        df1 = df1.rename(columns={col: 'RevuneFromProgressShortTermEstimation'})
                    if toDates[1] in col and 'درآمد حاصل از فروش واحدهای جدید' in col:
                        df1 = df1.rename(columns={col: 'RevenueFromNewSellsShortTermEstimation'})

                    
                    if toDates[1] not in col and 'درصد پیشرفت برآوردی' in col:
                        df1 = df1.rename(columns={col: 'EstimatedLongTermProgress'})
                    if toDates[1] not in col and 'برآورد از متراژ واگذاری' in col and 'برآورد' in col:
                        df1 = df1.rename(columns={col: 'SellLongTermEstimation'})
                    if toDates[1] not in col and 'از درآمد حاصل از افزایش درصد تکمیل' in col and 'برآورد' in col:
                        df1 = df1.rename(columns={col: 'RevuneFromProgressLongTermEstimation'})
                    if toDates[1] not in col and 'درآمد حاصل از فروش واحدهای جدید' in col and 'برآورد' in col:
                        df1 = df1.rename(columns={col: 'RevenueFromNewSellsLongTermEstimation'})
                df1['report_ID']=row['report_ID']
                df1['EndPeriod']=toDates[0]
                df1['ShortTerm']=toDates[1]
                df1['LongTerm']=""
            df1=df1[df1['title']==df1['title']]
            InsertSellRevenueandExistetdPropertiesandCurrentProjects(df1) 
def Situationofcompaniesthatareinvested():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" 
        where p."tableID"='Situation of companies that are invested' and p."report_ID" not in (select "report_ID" from statementnotes."InvestedCompanies")
        """, connection)
        #   and p."report_ID" not in (select "report_ID" from statementnotes."InvestedCompanies")
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close()
def InsertInvestedCompanies(DF4):
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
                IF NOT EXISTS (select from statementnotes."InvestedCompanies" where "report_ID"=%(report_ID)s and "title"=%(title)s) THEN
               INSERT INTO statementnotes."InvestedCompanies"(
	 "report_ID", title, "OwnershipStartPeriod", "CostStartPeriod", "ChildFiscalYear", "RevenueLastPeriod", "OwnerShipEndPeriod", "CostEndPeriod", "RevenueEndPeriod", "FiscalYear", "VisionForChildFirm", "StartPeriod", "EndPeriod")
	VALUES (%(report_ID)s, %(title)s, %(OwnerShipStartPeriod)s, %(CostStartPeriod)s,%(ChildFiscalYear)s, %(RevenueLastPeriod)s,%(OwnerShipEndPeriod)s, %(CostEndPeriod)s, %(RevenueEndPeriod)s, %(FiscalYear)s, %(VisionForChildFirm)s, %(StartPeriod)s, %(EndPeriod)s);
                END IF;
            END
            $$ 
        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
                if(connection):
                    print(DF4.report_ID)
                    print('Failed to Update Data ', error)
                    #log_it('Failed to Update OtherLayers ')
                    #return False
                
    finally:
        if(connection):
            cursor.close()
            connection.close()         
def handle_InvestedFirms():
    diff=[]
    diff2=[]
    df=Situationofcompaniesthatareinvested()
    for index,row in df.iterrows():
            if(index%200==0):
                print(index)
            df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
            toDates=[]
            for col in df1.columns:
                    if re.search('\d\d\d\d/\d\d/\d\d',col):
                        ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                        if ss!="" and ss not in toDates:
                            toDates.append(ss)
            toDates.sort()
            if len(toDates)!=2:
                print(len(toDates))
            df1.replace('ک','ك',regex=True,inplace=True)
            df1.replace('ی','ي',regex=True,inplace=True)
            df1.columns= df1.columns.str.replace('ي','ی',regex=True)
            df1.columns= df1.columns.str.replace('ي','ی',regex=True)
            df1 = df1.rename(columns={'نام شرکت ': 'title'})
            df1 = df1.rename(columns={ 'تشریح آخرین وضعیت و برنامه های آتی شرکت در شرکت سرمایه پذیر ': 'VisionForChildFirm'})
            
            if len(toDates)==2:
                for col in df1.columns:
                    if toDates[0] in col and 'درصد مالکیت' in col:
                        df1 = df1.rename(columns={col: 'OwnerShipStartPeriod'}) 
                    if toDates[0] in col and 'بهای تمام شده' in col:
                        df1 = df1.rename(columns={col: 'CostStartPeriod'})
                    if toDates[0] in col and 'سال مالی شرکت سرمایه پذیر' in col:
                        df1 = df1.rename(columns={col: 'ChildFiscalYear'})
                    if toDates[0] in col and 'درآمد سرمایه گذاری' in col:
                        df1 = df1.rename(columns={col: 'RevenueLastPeriod'})

                    if toDates[1] in col and 'درصد مالکیت' in col:
                        df1 = df1.rename(columns={col: 'OwnerShipEndPeriod'})
                    if toDates[1] in col and 'بهای تمام شده' in col:
                        df1 = df1.rename(columns={col: 'CostEndPeriod'})
                    if toDates[1] in col and 'درآمد سرمایه گذاری' in col:
                        df1 = df1.rename(columns={col: 'RevenueEndPeriod'})
                df1['report_ID']=row['report_ID']
                df1['StartPeriod']=toDates[0]
                df1['EndPeriod']=toDates[1]
            if len(df1.columns)!=14:
                df1 = df1.rename(columns={ 'سال مالی ': 'FiscalYear'})
                diff.append(df1)
            
            else:
                df1['FiscalYear']=""
                diff2.append(df1)
            df1=df1[df1['title']==df1['title']]
            InsertInvestedCompanies(df1)
def Staffstatistics():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
     select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" 
        where (p."tableID"='Staff statistics' or  p."tableID"='Staff status' or p."tableID"='Staff Status')and p."report_ID" not in (select "report_ID" from statementnotes."StaffStatistics")
        """, connection)
        #   and p."report_ID" not in (select "report_ID" from statementnotes."StaffStatistics")
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close()
def InsertStaffStatistics(DF4):
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
                IF NOT EXISTS (select from statementnotes."StaffStatistics" where "report_ID"=%(report_ID)s and "title"=%(title)s) THEN
               INSERT INTO statementnotes."StaffStatistics"(
	"report_ID", title, "StartPeriodAmount", "EndPeriodAmount", "ShortTermEstimation", "LongTermEstimation", "StartPeriod", "EndPeriod", "ShortTerm", "LongTerm")
	VALUES (%(report_ID)s, %(title)s, %(StartPeriodAmount)s, %(EndPeriodAmount)s,%(ShortTermEstimation)s, %(LongTermEstimation)s,%(StartPeriod)s,%(EndPeriod)s, %(ShortTerm)s, %(LongTerm)s);
                END IF;
            END
            $$ 
        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
                if(connection):
                    print(DF4.report_ID)
                    print('Failed to Update Data ', error)
                    #log_it('Failed to Update OtherLayers ')
                    #return False
                
    finally:
        if(connection):
            cursor.close()
            connection.close() 
def handle_Staffstatistics():
    df=Staffstatistics()
    for index,row in df.iterrows():
        try:
            if(index%200==0):
                print(index)
            df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
            toDates=[]
            for col in df1.columns:
                    if re.search('\d\d\d\d/\d\d/\d\d',col):
                        ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                        if ss!="" and ss not in toDates:
                            toDates.append(ss)
            toDates.sort()
            # if len(df1.columns)!=7:
            #     print(len(df1.columns))
            # if len(toDates)!=3 and len(toDates)!=4 :
            #     print(len(toDates))
            df1.replace('ک','ك',regex=True,inplace=True)
            df1.replace('ی','ي',regex=True,inplace=True)
            df1.columns= df1.columns.str.replace('ي','ی',regex=True)
            df1.columns= df1.columns.str.replace('ي','ی',regex=True)
            df1 = df1.rename(columns={'شرح ': 'title'})
            df1 = df1.rename(columns={'تاریخ ': 'title'})
            if len(toDates)==4:
                for col in df1.columns:
                    if toDates[0] in col :
                        df1 = df1.rename(columns={col: 'StartPeriodAmount'}) 
                    if toDates[1] in col :
                        df1 = df1.rename(columns={col: 'EndPeriodAmount'})
                    if toDates[2] in col:
                        df1 = df1.rename(columns={col: 'ShortTermEstimation'})
                    if toDates[3] in col:
                        df1 = df1.rename(columns={col: 'LongTermEstimation'})
                df1['report_ID']=row['report_ID']
                df1['StartPeriod']=toDates[0]
                df1['EndPeriod']=toDates[1]
                df1['ShortTerm']=toDates[2]
                df1['LongTerm']=toDates[3]
            if len(toDates)==3:
                for col in df1.columns:
                    if toDates[0] in col :
                        df1 = df1.rename(columns={col: 'StartPeriodAmount'}) 
                    if toDates[1] in col :
                        df1 = df1.rename(columns={col: 'EndPeriodAmount'})
                    if toDates[2] in col:
                        df1 = df1.rename(columns={col: 'ShortTermEstimation'})
                    if toDates[2] not in col and 'برآورد' in col:
                        df1 = df1.rename(columns={col: 'LongTermEstimation'})
                df1['report_ID']=row['report_ID']
                df1['StartPeriod']=toDates[0]
                df1['EndPeriod']=toDates[1]
                df1['ShortTerm']=toDates[2]
                if 'LongTermEstimation' not in df1.columns:
                    df1['LongTermEstimation']="0"
                df1['LongTerm']=""
            if len(toDates)==2:
                for col in df1.columns:
                    if toDates[0] in col :
                        df1 = df1.rename(columns={col: 'StartPeriodAmount'}) 
                    if toDates[1] in col :
                        df1 = df1.rename(columns={col: 'EndPeriodAmount'})
                    if toDates[1] in col and 'برآورد' in col:
                        df1 = df1.rename(columns={col: 'ShortTermEstimation'})
                df1['report_ID']=row['report_ID']
                df1['StartPeriod']=toDates[0]
                df1['EndPeriod']=toDates[1]
                df1['ShortTerm']=""
                df1['LongTerm']=""
                df1['LongTermEstimation']="0"
                df1['ShortTermEstimation']="0"
            df1=df1[df1['title']==df1['title']]
            InsertStaffStatistics(df1)    
        except:
            continue         
def Technicalreserves():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" 
        where (p."tableID"='Technical reserves') and p."report_ID" not in (select "report_ID" from statementnotes."TechnicalReserves")
        """, connection)
        #  
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close()
def handle_Technicalreserves():
    df=Technicalreserves()
    for index,row in df.iterrows():
            if(index%200==0):
                print(index)
            df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
            toDates=[]
            for col in df1.columns:
                    if re.search('\d\d\d\d/\d\d/\d\d',col):
                        ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                        if ss!="" and ss not in toDates:
                            toDates.append(ss)
            toDates.sort()
            if len(df1.columns)!=13:
                print(len(df1.columns))
            if len(toDates)!=2  :
                print(len(toDates))
            df1.replace('ک','ك',regex=True,inplace=True)
            df1.replace('ی','ي',regex=True,inplace=True)
            df1.columns= df1.columns.str.replace('ي','ی',regex=True)
            df1.columns= df1.columns.str.replace('ي','ی',regex=True)
            df1 = df1.rename(columns={'رشته بیمه ': 'title'})
            if len(toDates)==2:
                for col in df1.columns:
                    if toDates[0] in col and ' ذخایر حق بیمه کل' in col :
                        df1 = df1.rename(columns={col: 'StartPeriodReserveTotal'})
                    if toDates[0] in col and 'ذخایر حق بیمه اتکایی' in col :
                        df1 = df1.rename(columns={col: 'StartPeriodReserveEtkai'})
                    if toDates[0] in col and 'ذخیره خسارت معوق و ریسکهای منقضی نشده کل' in col :
                        df1 = df1.rename(columns={col: 'MoavaghTotalReserveStartPeriod'})
                    if toDates[0] in col and ' ذخیره خسارت معوق و ریسکهای منقضی نشده اتکایی' in col :
                        df1 = df1.rename(columns={col: 'MoavaghEtkaiReserveStartPeriod'})
                    if toDates[0] in col and ' سایر ذخایر فنی' in col :
                        df1 = df1.rename(columns={col: 'OtherTechnicalReservesStartPeriod'})
                    if toDates[1] in col and ' ذخایر حق بیمه کل' in col :
                        df1 = df1.rename(columns={col: 'EndPeriodReserveTotal'})
                    if toDates[1] in col and 'ذخایر حق بیمه اتکایی' in col :
                        df1 = df1.rename(columns={col: 'EndPeriodReserveEtkai'})
                    if toDates[1] in col and 'ذخیره خسارت معوق و ریسکهای منقضی نشده کل' in col :
                        df1 = df1.rename(columns={col: 'MoavaghTotalReserveEndPeriod'})
                    if toDates[1] in col and ' ذخیره خسارت معوق و ریسکهای منقضی نشده اتکایی' in col :
                        df1 = df1.rename(columns={col: 'MoavaghEtkaiReserveEndPeriod'})
                    if toDates[1] in col and ' سایر ذخایر فنی' in col :
                        df1 = df1.rename(columns={col: 'OtherTechnicalReservesEndPeriod'})
                df1['report_ID']=row['report_ID']
                df1['StartPeriod']=toDates[0]
                df1['EndPeriod']=toDates[1]
            df1=df1[df1['title']==df1['title']]
            InsertTechnicalReserve(df1)
def InsertTechnicalReserve(DF4):
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
                IF NOT EXISTS (select from statementnotes."TechnicalReserves" where "report_ID"=%(report_ID)s and "title"=%(title)s) THEN
               INSERT INTO statementnotes."TechnicalReserves"(
	"report_ID", title, "StartPeriodReserveTotal", "StartPeriodReserveEtkai", "MoavaghTotalReserveStartPeriod", "MoavaghEtkaiReserveStartPeriod", "OtherTechnicalReservesStartPeriod", "EndPeriodReserveTotal", "EndPeriodReserveEtkai", "MoavaghTotalReserveEndPeriod", "MoavaghEtkaiReserveEndPeriod", "OtherTechnicalReservesEndPeriod", "StartPeriod", "EndPeriod")
	VALUES (%(report_ID)s, %(title)s, %(StartPeriodReserveTotal)s, %(StartPeriodReserveEtkai)s,%(MoavaghTotalReserveStartPeriod)s, %(MoavaghEtkaiReserveStartPeriod)s,%(OtherTechnicalReservesStartPeriod)s,%(EndPeriodReserveTotal)s, %(EndPeriodReserveEtkai)s, %(MoavaghTotalReserveEndPeriod)s,%(MoavaghEtkaiReserveEndPeriod)s,%(OtherTechnicalReservesEndPeriod)s,%(StartPeriod)s,%(EndPeriod)s);
                END IF;
            END
            $$ 
        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
                if(connection):
                    print(DF4.report_ID)
                    print('Failed to Update Data ', error)
                    #log_it('Failed to Update OtherLayers ')
                    #return False
                
    finally:
        if(connection):
            cursor.close()
            connection.close()           
def Thecostofthesoldgoods():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" 
        where (p."tableID"='The cost of the sold goods') and p."report_ID" not in (select "report_ID" from statementnotes."COGS")
        """, connection)
        #  and p."report_ID" not in (select "report_ID" from statementnotes."COGS")
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close()
def InsertCOGS(DF4):
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
                IF NOT EXISTS (select from statementnotes."COGS" where "report_ID"=%(report_ID)s and "title"=%(title)s) THEN
               INSERT INTO statementnotes."COGS"(
	 "report_ID", title, "StartPeriodAmount", "EndPeriodAmount", "StartPeriod", "EndPeriod")
	VALUES (%(report_ID)s, %(title)s, %(StartPeriodAmount)s,%(EndPeriodAmount)s, %(StartPeriod)s,%(EndPeriod)s);
                END IF;
            END
            $$ 
        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
                if(connection):
                    print(DF4.report_ID)
                    print('Failed to Update Data ', error)
                    #log_it('Failed to Update OtherLayers ')
                    #return False
                
    finally:
        if(connection):
            cursor.close()
            connection.close() 
def handle_COGS():
    df=Thecostofthesoldgoods()
    for index,row in df.iterrows():
            if(index%200==0):
                print(index)
            df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
            toDates=[]
            for col in df1.columns:
                    if re.search('\d\d\d\d/\d\d/\d\d',col):
                        ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                        if ss!="" and ss not in toDates:
                            toDates.append(ss)
            toDates.sort()
            if len(df1.columns)!=5:
                print(len(df1.columns))
            if len(toDates)!=2  :
                print(len(toDates))
            df1.replace('ک','ك',regex=True,inplace=True)
            df1.replace('ی','ي',regex=True,inplace=True)
            df1.columns= df1.columns.str.replace('ي','ی',regex=True)
            df1.columns= df1.columns.str.replace('ي','ی',regex=True)
            df1 = df1.rename(columns={'شرح ': 'title'})
            if len(toDates)==2:
                for col in df1.columns:
                    if toDates[0] in col :
                        df1 = df1.rename(columns={col: 'StartPeriodAmount'})
                    if toDates[1] in col  :
                        df1 = df1.rename(columns={col: 'EndPeriodAmount'})
                df1['report_ID']=row['report_ID']
                df1['StartPeriod']=toDates[0]
                df1['EndPeriod']=toDates[1]
            df1=df1[df1['title']==df1['title']]
            InsertCOGS(df1)
def Thestatusofviablecompanies():        
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" 
        where (p."tableID"='The status of viable companies') and p."report_ID" not in (select "report_ID" from statementnotes."InvestedCompanies")
        """, connection)
        #  and p."report_ID" not in (select "report_ID" from statementnotes."InvestedCompanies")
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close()
def handle_Thestatusofviablecompanies():
    diff=[]
    diff2=[]
    df=Thestatusofviablecompanies()
    for index,row in df.iterrows():
            if(index%200==0):
                print(index)
            df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
            toDates=[]
            for col in df1.columns:
                    if re.search('\d\d\d\d/\d\d/\d\d',col):
                        ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                        if ss!="" and ss not in toDates:
                            toDates.append(ss)
            toDates.sort()
            if len(toDates)!=2:
                print(len(toDates))
            df1.replace('ک','ك',regex=True,inplace=True)
            df1.replace('ی','ي',regex=True,inplace=True)
            df1.columns= df1.columns.str.replace('ي','ی',regex=True)
            df1.columns= df1.columns.str.replace('ي','ی',regex=True)
            df1 = df1.rename(columns={'نام شرکت ': 'title'})
            df1 = df1.rename(columns={ 'تشریح آخرین وضعیت و برنامه های آتی شرکت در شرکت سرمایه پذیر ': 'VisionForChildFirm'})
            if len(df1.columns)!=11:
                print(len(df1.columns))
            if len(toDates)==2:
                for col in df1.columns:
                    if toDates[0] in col and 'درصد مالکیت' in col:
                        df1 = df1.rename(columns={col: 'OwnerShipStartPeriod'}) 
                    if toDates[0] in col and 'بهای تمام شده' in col:
                        df1 = df1.rename(columns={col: 'CostStartPeriod'})
                    if toDates[0] in col and 'سال مالی شرکت سرمایه پذیر' in col:
                        df1 = df1.rename(columns={col: 'ChildFiscalYear'})
                    if toDates[0] in col and 'درآمد سرمایه گذاری' in col:
                        df1 = df1.rename(columns={col: 'RevenueLastPeriod'})

                    if toDates[1] in col and 'درصد مالکیت' in col:
                        df1 = df1.rename(columns={col: 'OwnerShipEndPeriod'})
                    if toDates[1] in col and 'بهای تمام شده' in col:
                        df1 = df1.rename(columns={col: 'CostEndPeriod'})
                    if toDates[1] in col and 'درآمد سرمایه گذاری' in col:
                        df1 = df1.rename(columns={col: 'RevenueEndPeriod'})
                df1['report_ID']=row['report_ID']
                df1['StartPeriod']=toDates[0]
                df1['EndPeriod']=toDates[1]
                df1['FiscalYear']=""
            df1=df1[df1['title']==df1['title']]
            InsertInvestedCompanies(df1)        
def Thestatusofinvestmentcompanies():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
     select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" 
        where (p."tableID"='The status of investment companies') and p."report_ID" not in (select "report_ID" from statementnotes."InvestedCompanies")
        """, connection)
        #  and p."report_ID" not in (select "report_ID" from statementnotes."InvestedCompanies")
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close()
def handle_Thestatusofinvestmentcompanies():
    diff=[]
    diff2=[]
    df=Thestatusofinvestmentcompanies()
    for index,row in df.iterrows():
            if(index%200==0):
                print(index)
            df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
            toDates=[]
            for col in df1.columns:
                    if re.search('\d\d\d\d/\d\d/\d\d',col):
                        ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                        if ss!="" and ss not in toDates:
                            toDates.append(ss)
            toDates.sort()
            if len(toDates)!=2:
                print(len(toDates))
            df1.replace('ک','ك',regex=True,inplace=True)
            df1.replace('ی','ي',regex=True,inplace=True)
            df1.columns= df1.columns.str.replace('ي','ی',regex=True)
            df1.columns= df1.columns.str.replace('ي','ی',regex=True)
            df1 = df1.rename(columns={'نام شرکت ': 'title'})
            df1 = df1.rename(columns={ 'تشریح آخرین وضعیت و برنامه های آتی شرکت در شرکت سرمایه پذیر ': 'VisionForChildFirm'})
            if len(df1.columns)!=11:
                print(len(df1.columns))
            if len(toDates)==2:
                for col in df1.columns:
                    if toDates[0] in col and 'درصد مالکیت' in col:
                        df1 = df1.rename(columns={col: 'OwnerShipStartPeriod'}) 
                    if toDates[0] in col and 'بهای تمام شده' in col:
                        df1 = df1.rename(columns={col: 'CostStartPeriod'})
                    if toDates[0] in col and 'سال مالی شرکت سرمایه پذیر' in col:
                        df1 = df1.rename(columns={col: 'ChildFiscalYear'})
                    if toDates[0] in col and 'درآمد سرمایه گذاری' in col:
                        df1 = df1.rename(columns={col: 'RevenueLastPeriod'})

                    if toDates[1] in col and 'درصد مالکیت' in col:
                        df1 = df1.rename(columns={col: 'OwnerShipEndPeriod'})
                    if toDates[1] in col and 'بهای تمام شده' in col:
                        df1 = df1.rename(columns={col: 'CostEndPeriod'})
                    if toDates[1] in col and 'درآمد سرمایه گذاری' in col:
                        df1 = df1.rename(columns={col: 'RevenueEndPeriod'})
                df1['report_ID']=row['report_ID']
                df1['StartPeriod']=toDates[0]
                df1['EndPeriod']=toDates[1]
                df1['FiscalYear']=""
            df1=df1[df1['title']==df1['title']]
            InsertInvestedCompanies(df1)   
def Theprocessofissuinganinsurancepolicy():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
     select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as s on p."report_ID"=s."TracingNo" 
        where (p."tableID"='The process of issuing an insurance policy') and p."report_ID" not in (select "report_ID" from statementnotes."ProcessOfIssuingInsurancePolicy")
        """, connection)
        #  and p."report_ID" not in (select "report_ID" from statementnotes."ProcessOfIssuingInsurancePolicy")
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close()
def InsertTheprocessofissuinganinsurancepolicy(DF4):
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
                IF NOT EXISTS (select from statementnotes."ProcessOfIssuingInsurancePolicy" where "report_ID"=%(report_ID)s and "title"=%(title)s) THEN
               INSERT INTO statementnotes."ProcessOfIssuingInsurancePolicy"(
	 "report_ID", title, "4YearsAgoAmount", "3YearsAgoAmount", "2YearsAgoAmount", "LastPeriodAmount", "CurrentPeriodAmount ", "ShortTermEstimation", "LongTermEstimation", "4yearsAgo", "3yearsAgo", "2yearsAgo", "LastPeriod", "CurrentPeriod", "ShortTerm", "LongTerm")
	VALUES ( %(report_ID)s, %(title)s, %(4yearsAgoAmount)s, %(3yearsAgoAmount)s, %(2yearsAgoAmount)s, %(LastPeriodAmount)s, %(CurrentPeriodAmount)s, %(ShortTermEstimation)s, %(LongTermEstimation)s, %(4yearsAgo)s, %(3yearsAgo)s, %(2yearsAgo)s, %(LastPeriod)s, %(CurrentPeriod)s, %(ShortTerm)s, %(LongTerm)s);
                END IF;
            END
            $$ 
        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        #return True
    except(Exception, psycopg2.Error) as error:
                if(connection):
                    print(DF4.report_ID)
                    print('Failed to Update Data ', error)
                    #log_it('Failed to Update OtherLayers ')
                    #return False
                
    finally:
        if(connection):
            cursor.close()
            connection.close() 
def handle_Theprocessofissuinganinsurancepolicy():
    df=Theprocessofissuinganinsurancepolicy()
    for index,row in df.iterrows():
        if(index%200==0):
            print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        if len(df1.columns)!=10:
            print(len(df1.columns))
        toDates=[]
        for col in df1.columns:
                if re.search('\d\d\d\d/\d\d/\d\d',col):
                    ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                    if ss!="" and ss not in toDates:
                        toDates.append(ss)
        toDates.sort()
        if len(toDates)!=6 and len(toDates)!=7:
            print(len(toDates))
        df1.replace('ک','ك',regex=True,inplace=True)
        df1.replace('ی','ي',regex=True,inplace=True)
        df1.columns= df1.columns.str.replace('ي','ی',regex=True)
        df1.columns= df1.columns.str.replace('ي','ی',regex=True)
        df1 = df1.rename(columns={'شرح ': 'title'})
        if len(toDates)==7:
            for col in df1.columns:
                if toDates[0] in col :
                    df1 = df1.rename(columns={col: '4yearsAgoAmount'})  
                if toDates[1] in col :
                    df1 = df1.rename(columns={col: '3yearsAgoAmount'})
                if toDates[2] in col:
                    df1 = df1.rename(columns={col: '2yearsAgoAmount'})
                if toDates[3] in col :
                    df1 = df1.rename(columns={col: 'LastPeriodAmount'})  
                if toDates[4] in col:
                    df1 = df1.rename(columns={col: 'CurrentPeriodAmount'})
                if toDates[5] in col:
                    df1 = df1.rename(columns={col: 'ShortTermEstimation'})
                if toDates[6] in col:
                    df1 = df1.rename(columns={col: 'LongTermEstimation'})
            df1['report_ID']=row['report_ID']
            df1['4yearsAgo']=toDates[0]
            df1['3yearsAgo']=toDates[1]
            df1['2yearsAgo']=toDates[2]
            df1['LastPeriod']=toDates[3]
            df1['CurrentPeriod']=toDates[4]
            df1['ShortTerm']=toDates[5]
            df1['LongTerm']=toDates[6]
        if len(toDates)==6:
            for col in df1.columns:
                if toDates[0] in col :
                    df1 = df1.rename(columns={col: '4yearsAgoAmount'})  
                if toDates[1] in col :
                    df1 = df1.rename(columns={col: '3yearsAgoAmount'})
                if toDates[2] in col:
                    df1 = df1.rename(columns={col: '2yearsAgoAmount'})
                if toDates[3] in col :
                    df1 = df1.rename(columns={col: 'LastPeriodAmount'})  
                if toDates[4] in col:
                    df1 = df1.rename(columns={col: 'CurrentPeriodAmount'})
                if toDates[5] in col:
                    df1 = df1.rename(columns={col: 'ShortTermEstimation'})
                if toDates[5] not in col and 'برآورد' in col:
                    df1 = df1.rename(columns={col: 'LongTermEstimation'})
            df1['report_ID']=row['report_ID']
            df1['4yearsAgo']=toDates[0]
            df1['3yearsAgo']=toDates[1]
            df1['2yearsAgo']=toDates[2]
            df1['LastPeriod']=toDates[3]
            df1['CurrentPeriod']=toDates[4]
            df1['ShortTerm']=toDates[5]
            df1['LongTerm']=""
        df1=df1[df1['title']==df1['title']]
        InsertTheprocessofissuinganinsurancepolicy(df1)
def Explainingthestatusofventurecapitalcompanies():        
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
       select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as
        s on p."report_ID"=s."TracingNo" where p."tableID"='Explaining the status of venture capital companies (bourse companies'
        and p."report_ID" not in (select "report_ID" from statementnotes."InvestedCompanies")
                """, connection)
        #  and p."report_ID" not in (select "report_ID" from statementnotes."InvestedCompanies")
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close()
def handle_Explainingthestatusofventurecapitalcompanies():
    df=Explainingthestatusofventurecapitalcompanies()
    for index,row in df.iterrows():
        if(index%200==0):
            print(index)
        df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
        toDates=[]
        for col in df1.columns:
                if re.search('\d\d\d\d/\d\d/\d\d',col):
                    ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                    if ss!="" and ss not in toDates:
                        toDates.append(ss)
        toDates.sort()
        if len(toDates)!=2:
            print(len(toDates))
        df1.replace('ک','ك',regex=True,inplace=True)
        df1.replace('ی','ي',regex=True,inplace=True)
        df1.columns= df1.columns.str.replace('ي','ی',regex=True)
        df1.columns= df1.columns.str.replace('ي','ی',regex=True)
        df1 = df1.rename(columns={'نام شرکت ': 'title'})
        df1 = df1.rename(columns={ 'تشریح آخرین وضعیت و برنامه های آتی شرکت در شرکت سرمایه پذیر ': 'VisionForChildFirm'})
        if len(df1.columns)!=11:
            print(len(df1.columns))
        if len(toDates)==2:
            for col in df1.columns:
                if toDates[0] in col and 'درصد مالکیت' in col:
                    df1 = df1.rename(columns={col: 'OwnerShipStartPeriod'}) 
                if toDates[0] in col and 'بهای تمام شده' in col:
                    df1 = df1.rename(columns={col: 'CostStartPeriod'})
                if toDates[0] in col and 'سال مالی شرکت سرمایه پذیر' in col:
                    df1 = df1.rename(columns={col: 'ChildFiscalYear'})
                if toDates[0] in col and 'درآمد سرمایه گذاری' in col:
                    df1 = df1.rename(columns={col: 'RevenueLastPeriod'})

                if toDates[1] in col and 'درصد مالکیت' in col:
                    df1 = df1.rename(columns={col: 'OwnerShipEndPeriod'})
                if toDates[1] in col and 'بهای تمام شده' in col:
                    df1 = df1.rename(columns={col: 'CostEndPeriod'})
                if toDates[1] in col and 'سال مالی شرکت سرمایه پذیر' in col:
                    df1 = df1.rename(columns={col: 'ChildFiscalYear2'})
                if toDates[1] in col and 'درآمد سرمایه گذاری' in col:
                    df1 = df1.rename(columns={col: 'RevenueEndPeriod'})
            df1['report_ID']=row['report_ID']
            df1['StartPeriod']=toDates[0]
            df1['EndPeriod']=toDates[1]
            df1['FiscalYear']=""
            df1['VisionForChildFirm']=""
            df1=df1[df1['title']==df1['title']]
            InsertInvestedCompanies(df1)  
def StatusDiscriptionofcompaniesthatareinvested():        
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
       select * FROM statement."PreNotes" as p inner join codalraw."allrawReports" as
        s on p."report_ID"=s."TracingNo" where p."tableID"='Status Discription of companies that are invested'
        and p."report_ID" not in (select "report_ID" from statementnotes."InvestedCompanies")
        """, connection)
        #  and p."report_ID" not in (select "report_ID" from statementnotes."InvestedCompanies")
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close()
def handle_StatusDiscriptionofcompaniesthatareinvested():
    diff=[]
    diff2=[]
    df=StatusDiscriptionofcompaniesthatareinvested()
    for index,row in df.iterrows():
            if(index%200==0):
                print(index)
            df1=pd.read_csv(StringIO(row['Datatable']), sep=",")
            toDates=[]
            for col in df1.columns:
                    if re.search('\d\d\d\d/\d\d/\d\d',col):
                        ss=(re.search('\d\d\d\d/\d\d/\d\d',col)[0])
                        if ss!="" and ss not in toDates:
                            toDates.append(ss)
            toDates.sort()
            if len(toDates)!=2:
                print(len(toDates))
            df1.replace('ک','ك',regex=True,inplace=True)
            df1.replace('ی','ي',regex=True,inplace=True)
            df1.columns= df1.columns.str.replace('ي','ی',regex=True)
            df1.columns= df1.columns.str.replace('ي','ی',regex=True)
            df1 = df1.rename(columns={'نام شرکت ': 'title'})
            df1 = df1.rename(columns={ 'تشریح آخرین وضعیت و برنامه های آتی شرکت در شرکت سرمایه پذیر ': 'VisionForChildFirm'})
            if len(df1.columns)!=11:
                print(len(df1.columns))
            if len(toDates)==2:
                for col in df1.columns:
                    if toDates[0] in col and 'درصد مالکیت' in col:
                        df1 = df1.rename(columns={col: 'OwnerShipStartPeriod'}) 
                    if toDates[0] in col and 'بهای تمام شده' in col:
                        df1 = df1.rename(columns={col: 'CostStartPeriod'})
                    if toDates[0] in col and 'سال مالی شرکت سرمایه پذیر' in col:
                        df1 = df1.rename(columns={col: 'ChildFiscalYear'})
                    if toDates[0] in col and 'درآمد سرمایه گذاری' in col:
                        df1 = df1.rename(columns={col: 'RevenueLastPeriod'})

                    if toDates[1] in col and 'درصد مالکیت' in col:
                        df1 = df1.rename(columns={col: 'OwnerShipEndPeriod'})
                    if toDates[1] in col and 'بهای تمام شده' in col:
                        df1 = df1.rename(columns={col: 'CostEndPeriod'})
                    if toDates[1] in col and 'درآمد سرمایه گذاری' in col:
                        df1 = df1.rename(columns={col: 'RevenueEndPeriod'})
                df1['report_ID']=row['report_ID']
                df1['StartPeriod']=toDates[0]
                df1['EndPeriod']=toDates[1]
                df1['FiscalYear']=""
            df1=df1[df1['title']==df1['title']]
            InsertInvestedCompanies(df1)                  
if __name__=="__main__":
    print('Circulation...')
    Handle_CirculationAmountRialCommodityInventory()
    print('Depositsrecievied...')
    Handle_Depositsreceivedfromcustomers_all()
    print('AdminstrativeExpenses...')
    handle_AdministrativeAndPublicExpenses()
    print('AllocatedFacility...')
    handle_AllocatedFacility()
    print('Combined Cost of Portfo ...')
    handle_CombineCostOfPortfolio()
    print('Estimation Of Allocated Facilities Cost Change ..._GE')
    handle_EstimationOfAllocatedFacilitiesCostChange()
    print('Estimation Of Changes in Cost Factors ..._GE')
    handle_CompanyEstimatesofChangesincostfactors()
    print('Estimation Of Changes in Facility Balance ..._GE')
    handle_CompanyEstimatesofChangesinFacilityBalances()
    print('Estimation Of Changes in Public Expenses..._GE')
    handle_CompanyEstimatesofChangesinpublicExpenses()
    print('Estimation Of Changes in Reserve Balance Of Doubtful Receipts..._GE')
    handle_CompanyEstimatesofChangesinreservebalancesofdoubtfulreceipts()
    print('Estimation Of Changes in Rate of the Interest on Concessional Facilities ..._GE')
    handle_Companyestimatesofchangesintherateofinterestonconcessionalfacilities()
    print('Estimation Of Changes in Rate of Sales of Product and Pruchase Price of Raw Material ..._GE')
    handle_Companyestimatesofchangesintherateofsalesofproductsandthepurchasepriceofrawmaterials()
    print('Estimation Of Financing Programs And Company Finance Changes ..._GE')
    handle_Companyestimatesoffinancingprogramsandcompanyfinancechanges()
    print('Estimation Of Recived Deposits Profit Rate ..._GE')
    handle_CompanyEstimationofRecivedDepositsProfitRate()
    print('Estimation Of Changes In The Rest of Deposits ..._GE')
    handle_CompanyEstimationOfTheChangeInTheRestofDeposits()
    print('Estimation Of Changes Of General Expenses ..._GE')
    handle_CompanyEstimationofthegeneralExpenses()
    print('Plans Of Developing Schemas...')
    handle_Companyplansdescriptioninordertocompletedevelopingschemes()
    print('Plans Of Developing Schemas _ Except Bulidings...')
    handle_CompanyPlansExpressionforCompletingDevelopmentPlansexceptforBuildingProjects()
    print('Estimation Of Operational Support Plans And Companys Financial Changes ..._GE')
    handle_Companypredictaionofoperationalsupportplansandcompanysfinancialchanges()
    print('Estimation Of General Expenses ..._GE')
    handle_CompanyPredictationofthegeneralExpenses()
    print('Estimation Of Prime Cost Factors ..._GE')
    handle_Companypredictionofprimecostfactors()
    print('Plans For Division Of Profit ...')
    handle_Companysplanfordivisionofprofit()
    print('Estimation Of Full price Change Factors ..._GE')
    handle_Companyspredictionoffullpricechangefactors()
    print('ConsumptionItemList ...')
    #handle_ConsumptionItemlist()
    print('Plans For Division Of Profit_Corporate Income Program ...')
    handle_Corporateincomeprogram()
    print('Corporate Operating Revenue ...')
    handle_Corporateoperatingrevenues()
    print('Currency Situation ...')
    handle_Currencysituation()
    print('Insurance''s Damages...')
    handle_Damages()
    print('Plans To Complete Development Plans...')
    handle_Describecompanyplanstocompletedevelopmentplans()
    print('Company and Affiliates Dev Plans ...')
    handle_Describethecompanysplanstocompletethedevelopmentplansofthemaincompanyandsubsidiariesandaffiliates()
    print('Company Plans to Complete Dev ...GE')
    # handle_Descriptionfordescribecompanyplanstocompletedevelopmentplans()
    print('Details of Financing At the end of The Period ...GE')
    handle_Descriptionfordetailsofthefinancingofthecompanyattheendoftheperiod()
    print('Desc of Status Of Viable Companies ... GE')
    handle_Descriptionforthestatusofviablecompanies()
    print('Desc of Staff Statistics ... GE')
    handle_DescriptionsofStaffstatistics()
    print('Desc of Process Of Damages And Technical Reserves ... GE')
    handle_Descriptionsoftheprocessofdamagesandtechnicalreserves()
    print('Details of Financing At the end of the Period ...')
    handle_Detailsofthefinancingofthecompanyattheendoftheperiod()
    print('Estimate of Changes in Administrative and General Expenses and Other Operating Expenses ... GE')
    handle_Estimatedcompanychangesinadministrativeandgeneralexpensesandotheroperatingexpenses()
    print('Estimate of Changes in Public Expenses ... GE')
    handle_Estimatedcompanychangesinpublic()
    print('Future Goals And Strategies for Portfo ... GE')
    handle_Futuregoalsandstrategiesforportfoliomanagement()
    print('Future Goals And Strategies for Company Activity ... GE')
    handle_Futuregoalsandstrategiesofmanagementregardingcompanyactivity()
    print('Future Goals And Strategies for Resource and Consumption... GE')
    handle_FuturemanagementgoalsandstrategiesforResourceandConsumption()
    print('Future Goals And Strategies for Products And Sale... GE')
    handle_Futuremanagementgoalsandstrategiesfortheproductionandsaleofproducts()
    print('Future Goals And Strategies for Company Investments... GE')
    handle_Futuremanagementgoalsandstrategiesregardingcompanyinvestments()
    print('Future Goals And Strategies for Composition of Company Insurance Portfolios... GE')
    handle_Futuremanagementgoalsandstrategiesregardingtheamountandcompositionofcompanyinsuranceportfolios()
    print('Investments...')
    handle_Investment()
    print('Desc NonInsured Income Currency ... GE')
    handle_Non_insuredincomecurrencyandcurrencystatusdescription()
    print('Non Operational Income And Expense Investment...')
    handle_Nonoperationalincomeandexpensesinvestmentincome()
    print('Non Operational Income And Expense Misc Items...')
    handle_Nonoperationalincomeandexpensesmiscellaneousitems()
    print('Other Important Plans ... GE')
    handle_Otherimportantcompanysplans()
    print('Other Important Descriptions ... GE')
    handle_Otherimportantdescriptions()
    print('Other Important Notes ... GE')
    handle_Otherimportantnotes()
    print('Other Important Programs ... GE')
    handle_Otherimportantprograms()
    print('Other Income and expenses and Financial Expenses ... ')
    handle_Otherincomesandexpensesandfinancialexpenses()
    print('Other Insurance expenses and Incomes ... ')
    handle_Otherinsuranceexpenses_Income()
    print('Other Non Functional expenses Other Items ... ')
    handle_OtherNonfuncionalExpenses_OtherItem()
    print('Other Non Functional Incomes Investment ... ')
    handle_OtherNonfuncionalIncomes_Investments()
    print('Other Non Insurance Incomes ... ')
    handle_Othernon_insuranceincome()
    print('Other Non operating Incomes ... ')
    handle_Othernon_operatingincome()
    print('Other  operating Expenses ... ')
    handle_OtherOperatingExpenses()
    print('Other  operating Icnome ... ')
    handle_OtherOperatingIncome()
    print('Overhead ... ')
    handle_OverHead()
    print('Overhead and corporate... ')
    handle_OverheadAndCorporate()
    print('PremiumIssued... ')
    handle_PremiumIssued()
    print('Profit_loss_from Investment And Deposits...')
    handle_Profitlossfrominvestmentsanddeposits()
    print('Purchase and Consumption Of Raw Material....')
    handle_Purchaseandconsumptionofrawmaterials()
    print('Reseve Facility For Recovery and Storage....')
    handle_Reservefacilityforrecoveryandstorageofdoubtfulclaims()
    print('ResourceItems....')
    handle_ResourceItemList()
    print('Right Information And Capital Adequacy....')
    handle_RightInformationAndCapitalAdequacy()
    print('5 Years Sale Trend And Costs')
    handle_Salestrendandcostoverthelast5years()
    print('SellRevenueExistedProperties...')
    handle_SellRevenueandExistetdPropertiesandCurrentProjects()
    print('InvestedFirms...')
    handle_InvestedFirms()
    print('Staff Statistics...')
    handle_Staffstatistics()
    print('Technical Reserves...')
    handle_Technicalreserves()
    print('COGS...')
    handle_COGS()
    print('Viable Companies...')
    handle_Thestatusofviablecompanies()
    print('Invested Companies...')
    handle_Thestatusofinvestmentcompanies()
    print('Process of Issuing Insurance Policy..')
    handle_Theprocessofissuinganinsurancepolicy()
    print('The Status of Venture Capital Companies..')
    handle_Explainingthestatusofventurecapitalcompanies()  
    print('The Status of Companies that are invested..')
    handle_StatusDiscriptionofcompaniesthatareinvested()      