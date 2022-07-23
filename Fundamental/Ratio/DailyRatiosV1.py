import requests
import time
import pandas as pd
from khayyam import *
import psycopg2
import numpy as np
import json
from requests.api import get
from scipy.stats import norm
import math
import pandas.io.sql as psql
import statistics
from furl import furl
import psycopg2.extras as extras
from lxml import html
from datetime import date, datetime
import psycopg2.extras as extras
import asyncio
import aiohttp
import asyncpg
from datetime import datetime, timezone, timedelta
import pytz
from khayyam import *
import traceback
LOCAL_TIMEZONE  = pytz.timezone('Asia/Tehran')
db_username="Jay"
db_pass="Mehrad1"
db_host="127.0.0.1"
db_port="5432"
db_database="FinWisev12"  
#/////////
def replaceName(x):
    if x in Names.keys():
        return Names[x]
    else:
        return x
Names={'AllEarningsWithNRI(TTM)':'PtoETTM_WithNRI',
'EarningWithoutNRI':'PtoETTM_WithoutNRI',
'AllOperationalCashFlow(TTM)':'PtoOCF(TTM)',
'AllRevenue(TTM)':'PtoSTTM',
'FCF(TTM)':'P/FCF'
}
def InsertUpdateTwo(DF4):
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
                IF NOT EXISTS (select from statement."OtherRatios" where "firm"=%(tickerID)s and "Ratio"=%(Ratio)s and  "CalculatedOn"=%(CalculatedOn)s )  THEN
                INSERT INTO statement."OtherRatios"(
                firm, quarter, year, "toDate", "Ratio", "RatioValue", "CalculatedOn", "displayTitle")
                VALUES (%(tickerID)s, %(quarter)s, %(year)s, %(toDate)s, %(Ratio)s,
                 %(RatioValue)s, %(CalculatedOn)s, %(displayTitle)s);
                END IF;
            END
            $$ 
        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        print('Data Inserted')
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print('Failed to Update Data ', error)
                #log_it('Failed to Update OtherLayers ')
                #return False
            
    finally:
        if(connection):
            cursor.close()
            connection.close()

def InsertUpdate(DF4):
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
                IF NOT EXISTS (select from statement."RatiosToDisplay" where "firm"=%(tickerID)s and "Ratio"=%(Ratio)s )  THEN
                INSERT INTO statement."RatiosToDisplay"(
                firm, quarter, year, "toDate", "Ratio", "RatioValue", 
                "CalculatedOn", "displayTitle", "toHistoricAverage", "toIndustryAverage")
                VALUES (%(tickerID)s, %(quarter)s, %(year)s, %(toDate)s, %(Ratio)s,
                 %(RatioValue)s, %(CalculatedOn)s, %(displayTitle)s,%(ToHistoricAverage)s, %(ToIndustryAverage)s);
            ELSE UPDATE statement."RatiosToDisplay"
                SET  quarter=%(quarter)s, year=%(year)s, "toDate"=%(toDate)s,
                 "RatioValue"=%(RatioValue)s,"CalculatedOn"=%(CalculatedOn)s, 
                 "displayTitle"=%(displayTitle)s, "toHistoricAverage"=%(ToHistoricAverage)s,
                 "toIndustryAverage"=%(ToIndustryAverage)s
                WHERE "firm"=%(tickerID)s and "Ratio"=%(Ratio)s;
                END IF;
            END
            $$ 
        """
        cursor.executemany(postgres_insert_query_cheif,DF4.to_dict(orient='records'))
        #######
        
        connection.commit()
        print('Data Inserted')
    except(Exception, psycopg2.Error) as error:
            if(connection):
                print('Failed to Update Data ', error)
                #log_it('Failed to Update OtherLayers ')
                #return False
            
    finally:
        if(connection):
            cursor.close()
            connection.close()


if __name__=="__main__":
    CalculatedON=str(datetime.now().strftime('%Y-%m-%d'))
    try:
        print('Fetching MarketWatch ')
        wholeFile=''
        try:
            head = {'Accept-Profile':'marketwatch'}
            resp=requests.get('http://185.231.115.223:3000/ViewWatch',timeout=15,headers=head)
            wholeFile= resp.text
            if wholeFile is None or wholeFile=='':
                raise Exception('NoData')
            MarketWatch=pd.DataFrame(json.loads(wholeFile))
            MarketWatchPrices=pd.DataFrame(json.loads(wholeFile))
        except Exception as E:
            print('Error: '+E)
        print('Fetching Industry ')
        wholeFile=''
        try:
            head = {'Accept-Profile':'public'}
            resp=requests.get('http://130.185.74.40:3000/Industry',timeout=15,headers=head)
            wholeFile= resp.text
            if wholeFile is None or wholeFile=='':
                raise Exception('NoData')
            Industries=pd.DataFrame(json.loads(wholeFile))
        except Exception as E:
            print('Error: '+E)
        print('Fetching FinancialStrengthRatios ')
        wholeFile=''
        try:
            head = {'Accept-Profile':'statement'}
            resp=requests.get('http://130.185.74.40:3000/View_FinancialStrengthRatiosAllTime',timeout=15,headers=head)
            wholeFile= resp.text
            if wholeFile is None or wholeFile=='':
                raise Exception('NoData')
            FinancialStrengthRatios=pd.DataFrame(json.loads(wholeFile))
        except Exception as E:
            print('Error: '+E)
        print('Fetching OtherRatiosAll ')
        wholeFile=''
        try:
            head = {'Accept-Profile':'statement'}
            resp=requests.get('http://130.185.74.40:3000/View_OtherRatiosAll',headers=head)
            wholeFile= resp.text
            if wholeFile is None or wholeFile=='':
                raise Exception('NoData')
            OtherRatiosAll=pd.DataFrame(json.loads(wholeFile))
        except Exception as E:
            print('Error: '+E)
        print('Fetching View_LatestValuationComponent ')
        wholeFile=''
        try:
            head = {'Accept-Profile':'statement'}
            resp=requests.get('http://130.185.74.40:3000/View_LatestValuationComponent',timeout=40,headers=head)
            wholeFile= resp.text
            if wholeFile is None or wholeFile=='':
                raise Exception('NoData')
            LatestComponents=pd.DataFrame(json.loads(wholeFile))
        except Exception as E:
            print('Error: '+E)
        print('Fetching View_StockSpecific ')
        wholeFile=''
        try:
            head = {'Accept-Profile':'public'}
            resp=requests.get('http://185.231.115.223:3000/View_StockSpecific',timeout=15,headers=head)
            wholeFile= resp.text
            if wholeFile is None or wholeFile=='':
                raise Exception('NoData')
            Specific=pd.DataFrame(json.loads(wholeFile))
        except Exception as E:
            print('Error: '+E)
        ####
        Industries=Industries[['ID','persianName']]
        MarketWatch=MarketWatch[['ID','ticker','industry']]
        MarketWatch.columns=['tickerID','ticker','persianName']
        Whole=MarketWatch.merge(Industries,on=['persianName'])
        FinancialStrengthRatios.columns=['tickerID','quarter','year','toDate','Ratio','RatioValue','CalculatedOn','displayTitle']
        DFFS=FinancialStrengthRatios.merge(Whole,on=['tickerID'])
        OtherRatiosAll.columns=['tickerID','quarter','year','toDate','Ratio','RatioValue','CalculatedOn','displayTitle']
        DFOR=OtherRatiosAll.merge(Whole,on=['tickerID'])
        #########Part 1 and 2 
        DFFS=DFFS.sort_values(by=['CalculatedOn','toDate','quarter','year'],ascending=[False,False,False,False])
        DFFS=DFFS[DFFS['RatioValue']!='NaN']
        DFAllLatest=DFFS.drop_duplicates(subset=['tickerID','Ratio'],keep='first')
        IndustryRatiosAverage=[]
        for Industry in DFAllLatest.ID.unique().tolist():
            for Ratio in DFAllLatest[DFAllLatest['ID']==Industry].Ratio.unique().tolist():
                IndValues=DFAllLatest[(DFAllLatest['ID']==Industry)&(DFAllLatest['Ratio']==Ratio)].RatioValue.tolist()
                while ('Infinity') in IndValues:
                    IndValues.remove('Infinity')
                while ('-Infinity') in IndValues:
                    IndValues.remove('-Infinity')
                
                IndustryRatiosAverage.append({'Industry':Industry,'Ratio':Ratio,'Values':IndValues})
        AllIndustryAverages=pd.DataFrame(IndustryRatiosAverage) 
        AllNewRows=[]
        for firm in DFAllLatest.tickerID.unique().tolist():
            # print(firm)
            for index,row in DFAllLatest[DFAllLatest['tickerID']==firm].iterrows():
                HistoricValues=DFFS[(DFFS['tickerID']==firm)&(DFFS['Ratio']==row['Ratio'])].RatioValue.tolist()
                while ('Infinity') in HistoricValues:
                    HistoricValues.remove('Infinity')
                while ('-Infinity') in HistoricValues:
                    HistoricValues.remove('-Infinity')
                try:
                    if max(HistoricValues)!=min(HistoricValues):
                        Mul=100/(max(HistoricValues)-min(HistoricValues))
                        HVal=(row['RatioValue']-min(HistoricValues))*Mul
                        HVal=(round(HVal))
                    else:
                        HVal=100
                except:
                    HVal=0
                row['ToHistoricAverage']=HVal
                IndustriValues=AllIndustryAverages[(AllIndustryAverages['Industry']==row['ID'])&(AllIndustryAverages['Ratio']==row['Ratio'])].Values.tolist()[0]
                try:
                    if max(IndustriValues)!=min(IndustriValues):
                        Mul=100/(max(IndustriValues)-min(IndustriValues))
                        IVal=(row['RatioValue']-min(IndustriValues))*Mul
                        IVal=round(IVal)
                    else:
                        IVal=100
                except:
                    IVal=0
                row['ToIndustryAverage']=IVal
                AllNewRows.append(row)
        FSResults=pd.DataFrame(AllNewRows)  
        InsertUpdate(FSResults)
        #
        DFOR=DFOR.sort_values(by=['CalculatedOn','toDate','quarter','year'],ascending=[False,False,False,False])
        DFOR=DFOR[DFOR['RatioValue']!='NaN']
        DFAllLatest=DFOR.drop_duplicates(subset=['tickerID','Ratio'],keep='first')
        IndustryRatiosAverage=[]
        for Industry in DFAllLatest.ID.unique().tolist():
            for Ratio in DFAllLatest[DFAllLatest['ID']==Industry].Ratio.unique().tolist():
                IndValues=DFAllLatest[(DFAllLatest['ID']==Industry)&(DFAllLatest['Ratio']==Ratio)].RatioValue.tolist()
                while ('Infinity') in IndValues:
                    IndValues.remove('Infinity')
                while ('-Infinity') in IndValues:
                    IndValues.remove('-Infinity')
                
                IndustryRatiosAverage.append({'Industry':Industry,'Ratio':Ratio,'Values':IndValues})
        AllIndustryAverages=pd.DataFrame(IndustryRatiosAverage) 
        AllNewRows=[]
        for firm in DFAllLatest.tickerID.unique().tolist():
            # print(firm)
            for index,row in DFAllLatest[DFAllLatest['tickerID']==firm].iterrows():
                HistoricValues=DFOR[(DFOR['tickerID']==firm)&(DFOR['Ratio']==row['Ratio'])].RatioValue.tolist()
                while ('Infinity') in HistoricValues:
                    HistoricValues.remove('Infinity')
                while ('-Infinity') in HistoricValues:
                    HistoricValues.remove('-Infinity')
                try:
                    if max(HistoricValues)!=min(HistoricValues):
                        Mul=100/(max(HistoricValues)-min(HistoricValues))
                        HVal=(row['RatioValue']-min(HistoricValues))*Mul
                        HVal=(round(HVal))
                    else:
                        HVal=100
                except:
                    HVal=0
                row['ToHistoricAverage']=HVal
                IndustriValues=AllIndustryAverages[(AllIndustryAverages['Industry']==row['ID'])&(AllIndustryAverages['Ratio']==row['Ratio'])].Values.tolist()[0]
                try:
                    if max(IndustriValues)!=min(IndustriValues):
                        Mul=100/(max(IndustriValues)-min(IndustriValues))
                        IVal=(row['RatioValue']-min(IndustriValues))*Mul
                        IVal=round(IVal)
                    else:
                        IVal=100
                except:
                    IVal=0
                row['ToIndustryAverage']=IVal
                AllNewRows.append(row)
        ORResults=pd.DataFrame(AllNewRows)  
        # print(ORResults)
        InsertUpdate(ORResults)
        #########DailyLatestComponents
        MarketWatchPrices=MarketWatchPrices[['ID','close']]
        LatestComponents.columns=['ID', 'quarter', 'year', 'toDate', 'Component', 'ComponentValue',
            'CalculatedOn', 'displayTitle']
        DX=MarketWatchPrices.merge(LatestComponents,on=['ID'])
        DX=DX.merge(Specific,on=['ID'])
        DX=DX[DX['ComponentValue']!='NaN']
        DX['ComponentValue']=pd.to_numeric(DX['ComponentValue'])
        DX['ShareCount']=pd.to_numeric(DX['ShareCount'])
        DX['RatioValue']=DX['close']*DX['ShareCount']/(DX['ComponentValue']*10000000)
        DX=DX[['ID','quarter','year','toDate','Component','RatioValue','CalculatedOn','displayTitle']]
        DX.columns=['tickerID','quarter','year','toDate','Ratio','RatioValue','CalculatedOn','displayTitle']
        DX['CalculatedOn']=str(datetime.now()).split(' ')[0]
        DX['Ratio']=DX['Ratio'].apply(replaceName)
        DX['displayTitle']=DX['Ratio']
        InsertUpdateTwo(DX)
    except Exception as E2:
        # print(E2)
        print('Error: '+str(E2))
        # return []           