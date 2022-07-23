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
DiscountRate=0.45
ReturnDebtRatio=0.17
#######?/////////
async def SortOneOut(ID,session):
    print('Calculating For :' +str(ID))
    CalculatedON=str(datetime.now().strftime('%Y-%m-%d'))
    LastDone=0
    try:
        print('Fetching Last Report ID')
        wholeFile=''
        try:
            connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
            cursor = connection.cursor()
            df = psql.read_sql("""
            SELECT * FROM statement."firmLastRatio_OtherRatios" where firm=
            """+str(ID), connection)
            LastDone=(df.iloc[0].LastReportID)
        except Exception as E:
            print('Error: '+E)
        # print(wholeFile)
    except Exception as E2:
        pass
    try:
        print('Fetching BS ')
        wholeFile=''
        try:
            head = {'Accept-Profile':'statement'}
            async with session.get('http://130.185.74.40:3000/rpc/bsall?a='+str(ID),timeout=15,headers=head) as resp:
                wholeFile=await resp.text()
                if wholeFile is None or wholeFile=='':
                    raise Exception('NoData')
        except Exception as E:
            print('Error: '+E)
        BSAll=pd.DataFrame(json.loads(wholeFile))
        # try:
        #     connection = psycopg2.connect(user=db_username,
        #                                   password=db_pass,
        #                                   host=db_host,
        #                                   port=db_port,
        #                                   database=db_database)
        #     cursor = connection.cursor()
        #     BSAll = psql.read_sql("""
        #     SELECT * from statement.bsall('"""+str(ID)+"""')""", connection)
        # except Exception as E:
        #     print('Error: '+E)
        # print(wholeFile)
        lastToDate=BSAll['toDate'].unique().tolist()[0]
        LastReportDue=BSAll.sort_values(by=['report_id'],ascending=False)[['StockID','report_id']].head(1)
        LastReportDue.columns=['firm','LastReportID']
        if LastReportDue.iloc[0].LastReportID==LastDone:
            print('Stock '+str(ID)+'... is Up to Date')
            return []
        if BSAll.empty:
            raise Exception('No BS Data')
    except Exception as E2:
        # print(E2)
        print('Error: '+str(ID)+' '+str(E2))
        return []
    try:
        print('Fetching GetAll Quarterly and yearly')
        wholeFile=''
        # try:
        #     head = {'Accept-Profile':'statement'}
        #     async with session.get('http://130.185.74.40:3000/RatioItems?firm=eq.'+str(ID),timeout=15,headers=head) as resp:
        #         wholeFile=await resp.text()
        #         if wholeFile is None or wholeFile=='':
        #             raise Exception('NoData')
        # except Exception as E:
        #     print('Error: '+E)
        # AllData=pd.DataFrame(json.loads(wholeFile))
        try:
            connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
            cursor = connection.cursor()
            AllData = psql.read_sql("""
            SELECT * from statement."RatioItems" WHERE firm="""+str(ID), connection)
        except Exception as E:
            print('Error: '+E)
        if AllData.empty:
            raise Exception('No BS Data')
    except Exception as E2:
        # print(E2)
        print('Error: '+str(ID)+' '+str(E2))
        return []
    try:
        print('Fetching ShareCount')
        wholeFile=''
        try:
            head = {'Accept-Profile':'public'}
            async with session.get('http://185.231.115.223:3000/View_StockSpecific?ID=eq.'+str(ID),timeout=15,headers=head) as resp:
                wholeFile=await resp.text()
                if wholeFile is None or wholeFile=='':
                    raise Exception('No ShareCount Data')
        except Exception as E:
            print('Error: '+E)
        # print(wholeFile)
        Last_ShareCount=pd.DataFrame(json.loads(wholeFile))
        if Last_ShareCount.empty:
            raise Exception('No ShareCount Data')
    except Exception as E2:
        # print(E2)
        print('Error: '+str(ID)+' '+str(E2))
        return []
    try:
        print('Fetching HistoricMarketCap')
        wholeFile=''
        try:
            head = {'Accept-Profile':'tse'}
            async with session.get('http://185.231.115.223:3000/HistoricMarketCap?stockID=eq.'+str(ID),timeout=15,headers=head) as resp:
                wholeFile=await resp.text()
                if wholeFile is None or wholeFile=='':
                    raise Exception('No Historic Market Cap Data')
        except Exception as E:
            print('Error: '+E)
        # print(wholeFile)
        HistoricMarketCap=pd.DataFrame(json.loads(wholeFile))
        if HistoricMarketCap.empty:
            raise Exception('No Historic Market Cap Data')
    except Exception as E2:
        # print(E2)
        print('Error: '+str(ID)+' '+str(E2))
        return []
        ######
    try:
        print('Fetching DPS')
        wholeFile=''
        # try:
        #     head = {'Accept-Profile':'public'}
        #     async with session.get('http://130.185.74.40:3000/rpc/dpsallforadjust?a='+str(ID),timeout=15,headers=head) as resp:
        #         wholeFile=await resp.text()
        #         if wholeFile is None or wholeFile=='':
        #             raise Exception('No CF Data')
        # except Exception as E:
        #     print('Error: '+E)
        # DPSALL=pd.DataFrame(json.loads(wholeFile))
        try:
            connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
            cursor = connection.cursor()
            DPSALL = psql.read_sql("""
            SELECT * from public.dpsallforadjust('"""+str(ID)+"""')""", connection)
        except Exception as E:
            print('Error: '+E)
        print(wholeFile)
        if DPSALL.empty:
            raise Exception('No DPS Data')
    except Exception as E2:
        # print(E2)
        print('Error: '+str(ID)+' '+str(E2))
        return []
    # ##///////
    NeededBS=[]
    NeededIS=[]
    NeededCF=[]
    for index,row in AllData[(AllData['engName']=='BSItems')&(AllData['Yearly']==True)&(AllData['aggregated']==False)].iterrows():
        Date=(row['year'])
        Value=row['value']
        Item=row['translatedName']
        NeededBS.append({'Item':Item,'date':Date,'value':Value})
    NeededBS_DF=pd.DataFrame(NeededBS)
    NeededBS_DF.sort_values(by=['date'],ascending=False,inplace=True)
    NeededBS_DF.drop_duplicates(inplace=True) 
    for index,row in AllData[(AllData['engName']=='ISItems')&(AllData['Yearly']==True)&(AllData['aggregated']==False)].iterrows():
        Date=(row['year'])
        Value=row['value']
        Item=row['translatedName']
        NeededIS.append({'Item':Item,'date':Date,'value':Value})
    NeededIS_DF=pd.DataFrame(NeededIS)
    NeededIS_DF.sort_values(by=['date'],ascending=False,inplace=True)
    NeededIS_DF.drop_duplicates(inplace=True)
    for index,row in AllData[(AllData['engName']=='CFItems')&(AllData['Yearly']==True)&(AllData['aggregated']==False)].iterrows():
        Date=(row['year'])
        Value=row['value']
        Item=row['translatedName']
        NeededCF.append({'Item':Item,'date':Date,'value':Value})
    NeededCF_DF=pd.DataFrame(NeededCF)
    NeededCF_DF.sort_values(by=['date'],ascending=False,inplace=True)
    NeededCF_DF.drop_duplicates(inplace=True)    
    NeededCF_DF.dropna(inplace=True)        
    DPS=DPSALL.sort_values(by=['AssemblyDate'],ascending=False)
    DPS['year']=DPS['AssemblyDate'].apply(lambda x: int(str(x).split('/')[0])-1)
    Temp=NeededBS_DF.append(NeededIS_DF)
    Total=Temp.append(NeededCF_DF)
    Total.sort_values(by=['date'],ascending=False,inplace=True)
    LastShareCount=float(Last_ShareCount.ShareCount)
    Quarterly=AllData[AllData['Yearly']==False]
    Quarterly=Quarterly.sort_values(by=['year','quarter'],ascending=[False,False])
    for name in Quarterly[(Quarterly['engName']=='CFItems')| (Quarterly['engName']=='ISItems')& (Quarterly['translatedName']!='سرمایه')].translatedName.unique().tolist():
        c=False
        QDFTemp=Quarterly[Quarterly['translatedName']==name]
        QDFTemp=QDFTemp.sort_values(by=['toDate'],ascending=False)
        QDFTemp.reset_index(drop=True,inplace=True)
        for t in QDFTemp[QDFTemp['year']>'1389'].year.unique().tolist()[1:-1]:
            if len(QDFTemp[QDFTemp['year']==t])!=4:
                c=False
            else:
                c=True 
        if c:
            for index,row in QDFTemp.iterrows():
                try:
                    QDFTemp.at[index,'ValueTTM']=row['value']+QDFTemp.iloc[index+1].value+QDFTemp.iloc[index+2].value+QDFTemp.iloc[index+3].value
                    
                except:
                    continue
            for index,row in QDFTemp.iterrows():
                Quarterly.loc[(Quarterly['translatedName']==row['translatedName'])& (Quarterly['toDate']==row['toDate']),'TTM']=row['ValueTTM']
    ######///////////
    try:
        FinalRatiosProfitability=[]
        FinalRatiosDividend=[]
        FinalRatiosValRank=[]
        AllYears=Quarterly.year.unique().tolist()
        for UniqueToDate in Quarterly.toDate.unique().tolist():
            try:
                QDFTemp2=Quarterly[Quarterly['toDate']==UniqueToDate]
                # print('toDate:' + UniqueToDate)
            
                year=QDFTemp2.year.unique().tolist()[0]
                # print('year:' + year)
                LastYear=AllYears[AllYears.index(year)+1]
                # print('LastYear:' + LastYear)
                YearBeforeLastYear=AllYears[AllYears.index(year)+2]
                # print('YearBeforeLastYear:' + YearBeforeLastYear)
                YekiGhabltareGhabli=AllYears[AllYears.index(year)+3]
                # print('YekiGhabltareGhabli:' + YekiGhabltareGhabli)
                quarter=QDFTemp2.quarter.unique().tolist()[0]
                # print('quarter:' + quarter)
                toDatet=QDFTemp2.toDate.unique().tolist()[0]
                # print('toDatet:' + toDatet)
                EBIT_TTM=QDFTemp2[QDFTemp2['translatedName']=='سود (زیان) عملیاتی'].iloc[0].TTM
                Revenue_TTM=QDFTemp2[QDFTemp2['translatedName']=='درآمدهای عملیاتی'].iloc[0].TTM
                NetIncomeTTM=QDFTemp2[QDFTemp2['translatedName']=='سود (زیان) خالص'].iloc[0].TTM
                Equity=QDFTemp2[QDFTemp2['translatedName']=='جمع حقوق صاحبان سهام'].iloc[0].value
                TotalAsset=QDFTemp2[QDFTemp2['translatedName']=='جمع دارایی\u200cها'].iloc[0].value
                CurrentAsset=Quarterly[(Quarterly['translatedName']=='جمع دارایی\u200cهای جاری')&(Quarterly['quarter']==quarter)&(Quarterly['year']==str(year))].iloc[0].value
                CURRENTDEBT=Quarterly[(Quarterly['translatedName']=='جمع بدهی\u200cهای جاری')&(Quarterly['quarter']==quarter)&(Quarterly['year']==str(year))].iloc[0].value
                CurrentInventory=Quarterly[(Quarterly['translatedName']=='موجودی مواد و کالا')&(Quarterly['quarter']==quarter)&(Quarterly['year']==str(year))].iloc[0].value
                Cash=Quarterly[(Quarterly['translatedName']=='موجودی نقد')&(Quarterly['quarter']==quarter)&(Quarterly['year']==str(year))].iloc[0].value
                FixedAsset=Quarterly[(Quarterly['translatedName']=='دارایی\u200cهای ثابت مشهود')&(Quarterly['quarter']==quarter)&(Quarterly['year']==str(year))].iloc[0].value
                InventoryT1=Total[(Total['Item']=='موجودی مواد و کالا')&(Total['date']==str(LastYear))].iloc[0].value
                InventoryT0=Total[(Total['Item']=='موجودی مواد و کالا')&(Total['date']==str(YearBeforeLastYear))].iloc[0].value
                COGST1=Total[(Total['Item']=='بهای تمام \u200cشده درآمدهای عملیاتی')&(Total['date']==str(LastYear))].iloc[0].value
                RecievablesShortTerm_NOW=Quarterly[(Quarterly['translatedName']=='دریافتنیهای تجاری و سایر دریافتنی ها')&(Quarterly['quarter']==quarter)&(Quarterly['year']==str(year))].iloc[0].value
                RecievablesShortTerm_T0=Quarterly[(Quarterly['translatedName']=='دریافتنیهای تجاری و سایر دریافتنی ها')&(Quarterly['quarter']==quarter)&(Quarterly['year']==str(LastYear))].iloc[0].value
                Revenue_T2=Total[(Total['Item']=='درآمدهای عملیاتی')&(Total['date']==str(LastYear))].iloc[0].value
                Revenue_T1=Total[(Total['Item']=='درآمدهای عملیاتی')&(Total['date']==str(YearBeforeLastYear))].iloc[0].value
                Revenue_T0=Total[(Total['Item']=='درآمدهای عملیاتی')&(Total['date']==str(YekiGhabltareGhabli))].iloc[0].value
                EBIT_T2=Total[(Total['Item']=='سود (زیان) عملیاتی')&(Total['date']==str(LastYear))].iloc[0].value
                EBIT_T1=Total[(Total['Item']=='درآمدهای عملیاتی')&(Total['date']==str(YearBeforeLastYear))].iloc[0].value
                EBIT_T0=Total[(Total['Item']=='سود (زیان) عملیاتی')&(Total['date']==str(YekiGhabltareGhabli))].iloc[0].value
                EPSWithNRI_T2=Total[(Total['Item']=='سود (زیان) خالص هر سهم–ریال')&(Total['date']==str(LastYear))].iloc[0].value
                EPS_T2=Total[(Total['Item']=='EPSWithoutNRI')&(Total['date']==str(LastYear))].iloc[0].value
                EPS_T1=Total[(Total['Item']=='EPSWithoutNRI')&(Total['date']==str(YearBeforeLastYear))].iloc[0].value
                EPS_T0=Total[(Total['Item']=='EPSWithoutNRI')&(Total['date']==str(YekiGhabltareGhabli))].iloc[0].value
                CommercialPayables_T1=Total[(Total['Item']=='پرداختنیهای تجاری و سایر پرداختنیها')&(Total['date']==str(LastYear))].iloc[0].value
                CommercialPayables_T0=Total[(Total['Item']=='پرداختنیهای تجاری و سایر پرداختنیها')&(Total['date']==str(YearBeforeLastYear))].iloc[0].value
                ######//////
                OperationMargin=EBIT_TTM/Revenue_TTM
                FinalRatiosProfitability.append({'firm':ID,'quarter':quarter,'year':year,'toDate':toDatet,'Ratio':'OperatingMargin','displayTitle':'حاشیه سود عملیاتی','RatioValue':OperationMargin,'CalculatedOn':CalculatedON})
                NetMargin=NetIncomeTTM/Revenue_TTM
                FinalRatiosProfitability.append({'firm':ID,'quarter':quarter,'year':year,'toDate':toDatet,'Ratio':'NetMargin','displayTitle':'حاشیه سود خالص','RatioValue':NetMargin,'CalculatedOn':CalculatedON})
                ROE=NetIncomeTTM/Equity
                FinalRatiosProfitability.append({'firm':ID,'quarter':quarter,'year':year,'toDate':toDatet,'Ratio':'ROE','displayTitle':'بازده حقوق صاحبان سهام','RatioValue':ROE,'CalculatedOn':CalculatedON})
                ROA=NetIncomeTTM/TotalAsset
                FinalRatiosProfitability.append({'firm':ID,'quarter':quarter,'year':year,'toDate':toDatet,'Ratio':'ROA','displayTitle':'بازده دارایی','RatioValue':ROA,'CalculatedOn':CalculatedON})
                ROC=EBIT_TTM/((FixedAsset+CurrentAsset-CURRENTDEBT)/2)
                FinalRatiosProfitability.append({'firm':ID,'quarter':quarter,'year':year,'toDate':toDatet,'Ratio':'ROC','displayTitle':'بازده سرمایه','RatioValue':ROC,'CalculatedOn':CalculatedON})
                RevGrowth3Year=(((Revenue_T2-Revenue_T1)/Revenue_T1)+((Revenue_T1-Revenue_T0)/Revenue_T0))/2
                FinalRatiosProfitability.append({'firm':ID,'quarter':quarter,'year':year,'toDate':toDatet,'Ratio':'RevGrowth_3Year','displayTitle':'رشد درآمدها ۳ ساله','RatioValue':RevGrowth3Year,'CalculatedOn':CalculatedON})
                EbitGrowht3Year=(((EBIT_T2-EBIT_T1)/EBIT_T1)+((EBIT_T1-EBIT_T0)/EBIT_T0))/2
                FinalRatiosProfitability.append({'firm':ID,'quarter':quarter,'year':year,'toDate':toDatet,'Ratio':'EbitGrowth_3Year','displayTitle':' رشد سود عملیاتی ۳ ساله','RatioValue':EbitGrowht3Year,'CalculatedOn':CalculatedON})
                EPSGrowht3YearWithouthNRI=(((EPS_T2-EPS_T1)/EPS_T1)+((EPS_T1-EPS_T0)/EPS_T0))/2
                FinalRatiosProfitability.append({'firm':ID,'quarter':quarter,'year':year,'toDate':toDatet,'Ratio':'EPSGrowht3YearWithouthNRI','displayTitle':'رشد درآمد عملیاتی ۳ ساله','RatioValue':EPSGrowht3YearWithouthNRI,'CalculatedOn':CalculatedON})
                ######//////
                DPS['Value']=pd.to_numeric(DPS['Value'])
                DPST2=DPS[(DPS['year']==int(LastYear))].iloc[0].Value
                DPST1=DPS[(DPS['year']==int(YearBeforeLastYear))].iloc[0].Value
                DPST0=DPS[(DPS['year']==int(YekiGhabltareGhabli))].iloc[0].Value
                # print(DPST2)
                # print(EPSWithNRI_T2)
                Dividend_yield=DPST2/EPSWithNRI_T2
                Dividend_PayoutRatio=DPST2/EPS_T2
                DivGrowht3Year=(((DPST2-DPST1)/DPST1)+((DPST1-DPST0)/DPST0))/2
                yieldOnCost=Dividend_yield*math.pow((1+DivGrowht3Year),5)
                FinalRatiosDividend.append({'firm':ID,'quarter':quarter,'year':year,'toDate':toDatet,'Ratio':'Dividend_yield','displayTitle':'بازده سود نقدی','RatioValue':Dividend_yield,'CalculatedOn':CalculatedON})
                FinalRatiosDividend.append({'firm':ID,'quarter':quarter,'year':year,'toDate':toDatet,'Ratio':'Dividend_PayoutRatio','displayTitle':'سود نقدی','RatioValue':Dividend_PayoutRatio,'CalculatedOn':CalculatedON})
                FinalRatiosDividend.append({'firm':ID,'quarter':quarter,'year':year,'toDate':toDatet,'Ratio':'DivGrowht3Year','displayTitle':'رشد ۳ ساله سود نقدی','RatioValue':DivGrowht3Year,'CalculatedOn':CalculatedON})
                FinalRatiosDividend.append({'firm':ID,'quarter':quarter,'year':year,'toDate':toDatet,'Ratio':'yieldOnCost','displayTitle':'yield On Cost','RatioValue':yieldOnCost,'CalculatedOn':CalculatedON})
                #####///////////
                CurrentRatio=CurrentAsset/CURRENTDEBT
                FinalRatiosValRank.append({'firm':ID,'quarter':quarter,'year':year,'toDate':toDatet,'Ratio':'CurrentRatio','displayTitle':'نسبت جاری','RatioValue':CurrentRatio,'CalculatedOn':CalculatedON})
                QuickRatio=(CurrentAsset-CurrentInventory)/CURRENTDEBT
                FinalRatiosValRank.append({'firm':ID,'quarter':quarter,'year':year,'toDate':toDatet,'Ratio':'QuickRatio','displayTitle':'نسبت سریع','RatioValue':QuickRatio,'CalculatedOn':CalculatedON})
                DaysInventory=(InventoryT0+InventoryT1)*365/(2*COGST1)
                FinalRatiosValRank.append({'firm':ID,'quarter':quarter,'year':year,'toDate':toDatet,'Ratio':'DaysInventory','displayTitle':'گردش موجودی انبار','RatioValue':DaysInventory,'CalculatedOn':CalculatedON})
                DaysSaleOutsanding=(RecievablesShortTerm_NOW+RecievablesShortTerm_T0)*365/(2*Revenue_T2)
                FinalRatiosValRank.append({'firm':ID,'quarter':quarter,'year':year,'toDate':toDatet,'Ratio':'DaysSaleOutsanding','displayTitle':'گردش فروش','RatioValue':DaysSaleOutsanding,'CalculatedOn':CalculatedON})                
                DaysPayable=(CommercialPayables_T1+CommercialPayables_T0)*365/(2*COGST1)
                FinalRatiosValRank.append({'firm':ID,'quarter':quarter,'year':year,'toDate':toDatet,'Ratio':'DaysPayable','displayTitle':'گردش دریافتنی','RatioValue':DaysPayable,'CalculatedOn':CalculatedON})                
            except:
                # print(traceback.format_exc())
                break
    except Exception as E3:
        print(E)
        # print(traceback.format_exc())
        return []
    try:
        Final=pd.DataFrame(FinalRatiosProfitability)
        Final=Final[Final['year']>'1390']
        Final=Final[['firm','quarter','year','toDate','Ratio','displayTitle','RatioValue','CalculatedOn']]

        Final2=pd.DataFrame(FinalRatiosDividend)
        Final2=Final2[Final2['year']>'1390']
        Final2=Final2[['firm','quarter','year','toDate','Ratio','displayTitle','RatioValue','CalculatedOn']]

        Final3=pd.DataFrame(FinalRatiosValRank)
        Final3=Final3[Final3['year']>'1390']
        Final3=Final3[['firm','quarter','year','toDate','Ratio','displayTitle','RatioValue','CalculatedOn']]
        # print(Final3)
        cols=[i for i in Final.columns if i not in ['quarter','year',"CalculatedOn",'Ratio','displayTitle','toDate']]
        for col in cols:
            Final[col]=pd.to_numeric(Final[col])
        cols=[i for i in Final2.columns if i not in ['quarter','year',"CalculatedOn",'Ratio','displayTitle','toDate']]
        for col in cols:
            Final2[col]=pd.to_numeric(Final2[col])
        cols=[i for i in Final2.columns if i not in ['quarter','year',"CalculatedOn",'Ratio','displayTitle','toDate']]
        for col in cols:
            Final3[col]=pd.to_numeric(Final3[col])
        tuples = [tuple(x) for x in Final.to_numpy()]   
        tuples2 = [tuple(x) for x in Final2.to_numpy()] 
        tuples3 = [tuple(x) for x in Final3.to_numpy()] 
        con = await asyncpg.connect(host=db_host,port=db_port,user=db_username,password=db_pass,database=db_database)    
        await con.execute('''
        CREATE TEMPORARY TABLE _data(
            firm bigint NOT NULL,
            quarter character varying COLLATE pg_catalog."default",
            year character varying COLLATE pg_catalog."default",
            "toDate" character varying COLLATE pg_catalog."default" NOT NULL,
            "Ratio" character varying COLLATE pg_catalog."default" NOT NULL,
            "displayTitle" character varying COLLATE pg_catalog."default",
            "RatioValue" double precision NOT NULL,
            "CalculatedOn" character varying COLLATE pg_catalog."default" NOT NULL,
            CONSTRAINT "OtherRatios_pkey" PRIMARY KEY (firm, "Ratio", "toDate", "RatioValue", "CalculatedOn")
        )''')
        result = await con.copy_records_to_table('_data', records=tuples)
        result2 = await con.copy_records_to_table('_data', records=tuples2)
        result3 = await con.copy_records_to_table('_data', records=tuples3)
        await con.execute('''
        INSERT INTO {table}(
        firm, quarter, year, "toDate", "Ratio", "displayTitle", "RatioValue", "CalculatedOn")
        SELECT * FROM _data
        ON CONFLICT  (firm, "Ratio", "toDate", "RatioValue", "CalculatedOn")
        DO NOTHING
        '''.format(table='statement."OtherRatios"'))
        await con.close()
    except Exception as E5:
        print(E5)
        # return []
    try:
        AllMarketCaps=HistoricMarketCap
        AllMarketCaps['persianDate']=AllMarketCaps['engDate'].apply(lambda x: str(JalaliDate(datetime.strptime(x,'%Y-%m-%d'))).replace('-','/'))
        #########///////
        Quarterly['toDate']=Quarterly['year'].apply(lambda x: str(x)+'/12/29')
        AllData['toDate']=AllData['year'].apply(lambda x: str(x)+'/12/29')
        
        AllEarnings=Quarterly[Quarterly['translatedName']=='EarningWithouNRI']
        AllEarnings.rename(columns={'toDate':'persianDate'},inplace=True)
        AllEarnings=AllEarnings.sort_values(by=['persianDate'],ascending=False)
        AllEarnings.reset_index(inplace=True,drop=True)
        
        DFT=AllMarketCaps.append(AllEarnings)
        DFT.sort_values(by=['persianDate'],ascending=False,inplace=True)
        DFT['TTM'].fillna(method='bfill',inplace=True)
        DFT=DFT[~DFT['stockID'].isnull()]
        DFT['RatioValue']=DFT['MarketCap']/(DFT['TTM']*1000000)
        DFT1=DFT[['stockID','engDate','RatioValue','persianDate']]
        DFT1.loc[:,'CalculatedOn']=CalculatedON
        DFT1.loc[:,'Ratio']='PtoETTM_WithoutNRI'
        DFT1.loc[:,'displayTitle']='P/E(TTM)_WithoutNRI'
        # print(DFT1)
        AllEarningsWithNRI=Quarterly[Quarterly['translatedName']=='سود (زیان) خالص']
        AllEarningsWithNRI.rename(columns={'toDate':'persianDate'},inplace=True)
        AllEarningsWithNRI=AllEarningsWithNRI.sort_values(by=['persianDate'],ascending=False)
        AllEarningsWithNRI.reset_index(inplace=True,drop=True)
        AllEarningsWithNRI['Component']='AllEarningsWithNRI(TTM)'
        DFT=AllMarketCaps.append(AllEarningsWithNRI)
        DFT.sort_values(by=['persianDate'],ascending=False,inplace=True)
        DFT['TTM'].fillna(method='bfill',inplace=True)
        DFT=DFT[~DFT['stockID'].isnull()]
        DFT['RatioValue']=DFT['MarketCap']/(DFT['TTM']*1000000)
        DFT2=DFT[['stockID','engDate','RatioValue']]
        DFT2.loc[:,'CalculatedOn']=CalculatedON
        DFT2.loc[:,'Ratio']='PtoETTM_WithNRI'
        DFT2.loc[:,'displayTitle']='P/E(TTM)_WithNRI'
        # print(DFT2)
        AllEquity=Quarterly[Quarterly['translatedName']=='جمع حقوق صاحبان سهام']
        AllEquity.rename(columns={'toDate':'persianDate'},inplace=True)
        AllEquity=AllEquity.sort_values(by=['persianDate'],ascending=False)
        AllEquity.reset_index(inplace=True,drop=True)
        DFT=AllMarketCaps.append(AllEquity)
        DFT.sort_values(by=['persianDate'],ascending=False,inplace=True)
        DFT['value'].fillna(method='bfill',inplace=True)
        DFT=DFT[~DFT['stockID'].isnull()]
        DFT['RatioValue']=DFT['MarketCap']/(DFT['value']*1000000)
        DFT3=DFT[['stockID','engDate','RatioValue']]
        DFT3.loc[:,'CalculatedOn']=CalculatedON
        DFT3.loc[:,'Ratio']='P/B'
        DFT3.loc[:,'displayTitle']='P/B'
        AllEquity['Component']='AllEquity'
        # print(DFT3)
        AllRevenue=Quarterly[Quarterly['translatedName']=='درآمدهای عملیاتی']
        AllRevenue.rename(columns={'toDate':'persianDate'},inplace=True)
        AllRevenue=AllRevenue.sort_values(by=['persianDate'],ascending=False)
        AllRevenue.reset_index(inplace=True,drop=True)
        
        DFT=AllMarketCaps.append(AllRevenue)
        DFT.sort_values(by=['persianDate'],ascending=False,inplace=True)
        DFT['TTM'].fillna(method='bfill',inplace=True)
        DFT=DFT[~DFT['stockID'].isnull()]
        DFT['RatioValue']=DFT['MarketCap']/(DFT['TTM']*1000000)
        DFT4=DFT[['stockID','engDate','RatioValue']]
        DFT4.loc[:,'CalculatedOn']=CalculatedON
        DFT4.loc[:,'Ratio']='PtoSTTM'
        DFT4.loc[:,'displayTitle']='P/S(TTM)'
        # print(DFT4)
        AllOperationalCashFlow=Quarterly[Quarterly['translatedName']=='جریانهای نقدی حاصل از فعالیتهای عملیاتی']
        AllOperationalCashFlow.rename(columns={'toDate':'persianDate'},inplace=True)
        AllOperationalCashFlow=AllOperationalCashFlow.sort_values(by=['persianDate'],ascending=False)
        AllOperationalCashFlow.reset_index(inplace=True,drop=True)
        AllOperationalCashFlow['Component']='AllOperationalCashFlow(TTM)'
        DFT=AllMarketCaps.append(AllOperationalCashFlow)
        DFT.sort_values(by=['persianDate'],ascending=False,inplace=True)
        DFT['TTM'].fillna(method='bfill',inplace=True)
        DFT=DFT[~DFT['stockID'].isnull()]
        DFT['RatioValue']=DFT['MarketCap']/(DFT['TTM']*1000000)
        DFT5=DFT[['stockID','engDate','RatioValue']]
        DFT5.loc[:,'CalculatedOn']=CalculatedON
        DFT5.loc[:,'Ratio']='PtoOCF(TTM)'
        DFT5.loc[:,'displayTitle']='P/OperationCashFlow(TTM)'
        # print(DFT5)
        DebtAndCash=Quarterly[(Quarterly['translatedName']=='جمع بدهی\u200cها')|(Quarterly['translatedName']=='موجودی نقد')]
        DebtAndCash.rename(columns={'toDate':'persianDate'},inplace=True)
        DebtAndCash=DebtAndCash.sort_values(by=['persianDate','translatedName'],ascending=[False,False])
        DebtAndCash.reset_index(inplace=True,drop=True)
        for tyear in DebtAndCash.persianDate.unique():
            try:
                r=(DebtAndCash[(DebtAndCash['translatedName']=='جمع بدهی\u200cها')&(DebtAndCash['persianDate']==tyear)]).iloc[0]
                r2=(DebtAndCash[(DebtAndCash['translatedName']=='موجودی نقد')&(DebtAndCash['persianDate']==tyear)]).iloc[0]
                DebtAndCash=DebtAndCash.append({'translatedName':'DebtMinusCash','engName':'BSItems','value':r.value-r2.value,'quarter':r['quarter']
                ,'year':r['year'],'aggregated':r['aggregated'],'firm':r['firm'],'persianDate':r['persianDate'],'Yearly':r['Yearly'],'TTM':r['TTM']},ignore_index=True)
            except:
                # print(r)
                break
        DebtAndCash=DebtAndCash.sort_values(by=['persianDate','translatedName'],ascending=[False,False])
        DebtAndCash.reset_index(inplace=True,drop=True)
        DebtAndCash=DebtAndCash[DebtAndCash['translatedName']=='DebtMinusCash']
        DebtAndCash['Component']='DebtAndCash'
        DFT=AllMarketCaps.append(DebtAndCash)
        DFT.sort_values(by=['persianDate'],ascending=False,inplace=True)
        DFT['value'].fillna(method='bfill',inplace=True)
        DFT=DFT[~DFT['stockID'].isnull()]
        DFT['RatioValue']=DFT['MarketCap']+(DFT['value']*1000000)
        DFT6=DFT[['stockID','engDate','RatioValue','persianDate']]
        DFT6.loc[:,'CalculatedOn']=CalculatedON
        DFT6.loc[:,'Ratio']='EV'
        DFT6.loc[:,'displayTitle']='EV'
        ############
        FCFCalc=Quarterly[(Quarterly['translatedName']=='جریانهای نقدی حاصل از فعالیتهای عملیاتی')|(Quarterly['translatedName']=='وجوه پرداختی بابت خرید دارایی‌های ثابت مشهود')]
        FCFCalc.rename(columns={'toDate':'persianDate'},inplace=True)
        FCFCalc=FCFCalc.sort_values(by=['persianDate','translatedName'],ascending=[False,False])
        FCFCalc.reset_index(inplace=True,drop=True)
        for tyear in FCFCalc.persianDate.unique():
            try:
                r=(FCFCalc[(FCFCalc['translatedName']=='جریانهای نقدی حاصل از فعالیتهای عملیاتی')&(FCFCalc['persianDate']==tyear)]).iloc[0]
                r2=(FCFCalc[(FCFCalc['translatedName']=='وجوه پرداختی بابت خرید دارایی‌های ثابت مشهود')&(FCFCalc['persianDate']==tyear)]).iloc[0]
                FCFCalc=FCFCalc.append({'translatedName':'FCF','engName':'BSItems','value':r.TTM+r2.TTM,'quarter':r['quarter']
                ,'year':r['year'],'aggregated':r['aggregated'],'firm':r['firm'],'persianDate':r['persianDate'],'Yearly':r['Yearly'],'TTM':r['TTM']},ignore_index=True)
            except:
                # print(r)
                break
        FCFCalc=FCFCalc.sort_values(by=['persianDate','translatedName'],ascending=[False,False])
        FCFCalc.reset_index(inplace=True,drop=True)
        FCFCalc=FCFCalc[FCFCalc['translatedName']=='FCF']
        FCFCalc['Component']='FCF(TTM)'
        DFT=AllMarketCaps.append(FCFCalc)
        DFT.sort_values(by=['persianDate'],ascending=False,inplace=True)
        DFT['value'].fillna(method='bfill',inplace=True)
        DFT=DFT[~DFT['stockID'].isnull()]
        DFT['RatioValue']=DFT['MarketCap']/(DFT['value']*1000000)
        DFT7=DFT[['stockID','engDate','RatioValue']]
        DFT7.loc[:,'CalculatedOn']=CalculatedON
        DFT7.loc[:,'Ratio']='P/FCF'
        DFT7.loc[:,'displayTitle']='P/FCF'
        #########
        AllEarnings=Quarterly[Quarterly['translatedName']=='EarningWithouNRI']
        AllEarnings.rename(columns={'toDate':'persianDate'},inplace=True)
        AllEarnings=AllEarnings.sort_values(by=['persianDate'],ascending=False)
        AllEarnings['Component']='EarningWithoutNRI(TTM)'
        DFT=DFT6.append(AllEarnings)
        DFT.sort_values(by=['persianDate'],ascending=False,inplace=True)
        DFT['TTM'].fillna(method='bfill',inplace=True)
        DFT=DFT[~DFT['stockID'].isnull()]
        DFT['RatioValue2']=DFT['RatioValue']/(DFT['TTM']*1000000)
        DFT8=DFT[['stockID','engDate','RatioValue2']]
        DFT8.columns=['stockID','engDate','RatioValue']
        DFT8.loc[:,'CalculatedOn']=CalculatedON
        DFT8.loc[:,'Ratio']='EVtoEbit(TTM)'
        DFT8.loc[:,'displayTitle']='EVtoEbit(TTM)'
        ####
        AllRevenue=Quarterly[Quarterly['translatedName']=='درآمدهای عملیاتی']
        AllRevenue.rename(columns={'toDate':'persianDate'},inplace=True)
        AllRevenue=AllRevenue.sort_values(by=['persianDate'],ascending=False)
        AllRevenue.reset_index(inplace=True,drop=True)
        AllRevenue['Component']='AllRevenue(TTM)'
        DFT=DFT6.append(AllRevenue)
        DFT.sort_values(by=['persianDate'],ascending=False,inplace=True)
        DFT['TTM'].fillna(method='bfill',inplace=True)
        DFT=DFT[~DFT['stockID'].isnull()]
        DFT['RatioValue2']=DFT['RatioValue']/(DFT['TTM']*1000000)
        DFT9=DFT[['stockID','engDate','RatioValue2']]
        DFT9.columns=['stockID','engDate','RatioValue']
        DFT9.loc[:,'CalculatedOn']=CalculatedON
        DFT9.loc[:,'Ratio']='EVtoRevenue(TTM)'
        DFT9.loc[:,'displayTitle']='EVtoRevenue(TTM)'
        ##########
        EBITYEARLY=AllData[(AllData['Yearly']==True)&(AllData['translatedName']=='سود (زیان) خالص')]
        EBITYEARLY.rename(columns={'toDate':'persianDate'},inplace=True)
        EBITYEARLY=EBITYEARLY.sort_values(by=['year'],ascending=False)
        for tyear in EBITYEARLY.year.unique():
            try:
                r=(EBITYEARLY[(EBITYEARLY['translatedName']=='سود (زیان) خالص')&(EBITYEARLY['year']==tyear)]).iloc[0]
                r2=(EBITYEARLY[(EBITYEARLY['translatedName']=='سود (زیان) خالص')&(EBITYEARLY['year']==str(int(tyear)-1))]).iloc[0].value
                # print(r2)
                r3=(EBITYEARLY[(EBITYEARLY['translatedName']=='سود (زیان) خالص')&(EBITYEARLY['year']==str(int(tyear)-2))]).iloc[0].value
                r4=(EBITYEARLY[(EBITYEARLY['translatedName']=='سود (زیان) خالص')&(EBITYEARLY['year']==str(int(tyear)-3))]).iloc[0].value
                r5=(EBITYEARLY[(EBITYEARLY['translatedName']=='سود (زیان) خالص')&(EBITYEARLY['year']==str(int(tyear)-4))]).iloc[0].value
                val=(((r.value/r2)-1)+((r2/r3)-1)+((r3/r4)-1)+((r4/r5)-1))/4
                EBITYEARLY=EBITYEARLY.append({'translatedName':'Ebit5YearGrowth','engName':'BSItems','value':val,'quarter':r['quarter']
                ,'year':r['year'],'aggregated':r['aggregated'],'firm':r['firm'],'persianDate':r['persianDate'],'Yearly':r['Yearly']},ignore_index=True)
            except:
                # print(traceback.format_exc())
                break
        EBITYEARLY['Component']='Ebit_5YearGrowth(NonTTM)'
        EBITYEARLY=EBITYEARLY[EBITYEARLY['translatedName']=='Ebit5YearGrowth']
        DFT=DFT1.append(EBITYEARLY)
        DFT.sort_values(by=['persianDate'],ascending=False,inplace=True)
        DFT['value'].fillna(method='bfill',inplace=True)
        DFT=DFT[~DFT['stockID'].isnull()]
        DFT['RatioValue2']=DFT['RatioValue']/(DFT['value'])
        DFT10=DFT[['stockID','engDate','RatioValue2']]
        DFT10.columns=['stockID','engDate','RatioValue']
        DFT10.loc[:,'CalculatedOn']=CalculatedON
        DFT10.loc[:,'Ratio']='PEGRatio'
        DFT10.loc[:,'displayTitle']='PEGRatio'
        # print(DFT10)
        # 
    except Exception as E6:
        print(E6)
        return []
    ##########////////
    try:
        Alltuples=[]
        for A in [DFT1,DFT2,DFT3,DFT4,DFT5,DFT6,DFT7,DFT8,DFT9,DFT10]:
            cols=['firm','RatioValue']
            A['firm']=ID
            for col in cols:
              A[col]=pd.to_numeric(A[col])
            A=A[['firm','engDate','Ratio','RatioValue','CalculatedOn','displayTitle']]
            Alltuples.append([tuple(x) for x in A.to_numpy()])
        con = await asyncpg.connect(host=db_host,port=db_port,user=db_username,password=db_pass,database=db_database)    
        await con.execute('''
        CREATE TEMPORARY TABLE _data3(
           firm bigint NOT NULL,
            "toDate" character varying COLLATE pg_catalog."default" NOT NULL,
            "Ratio" character varying COLLATE pg_catalog."default" NOT NULL,
            "RatioValue" double precision NOT NULL,
            "CalculatedOn" character varying COLLATE pg_catalog."default" NOT NULL,
            "displayTitle" character varying COLLATE pg_catalog."default",
            CONSTRAINT "OtherRatios_pkey" PRIMARY KEY (firm, "Ratio", "toDate", "RatioValue", "CalculatedOn")
            )''')
        for k in Alltuples:
            result = await con.copy_records_to_table('_data3', records=k)
        await con.execute('''
        INSERT INTO {table}(
	    firm, "toDate", "Ratio", "RatioValue", "CalculatedOn", "displayTitle")
        SELECT * FROM _data3
        ON CONFLICT (firm, "Ratio", "toDate", "RatioValue", "CalculatedOn")
        DO NOTHING
        '''.format(table='statement."OtherRatios"'))
        await con.close()
    except Exception as E5:
        print(E5)
        # return []

    try:
        Alltuples=[]
        for A in [AllEarnings,AllEarningsWithNRI,AllRevenue,AllOperationalCashFlow,FCFCalc]:
            cols=['firm','TTM']
            for col in cols:
              A[col]=pd.to_numeric(A[col])
            A['CalculatedOn']=CalculatedON
            A=A[['firm','quarter','year','persianDate','Component','TTM','CalculatedOn']]
            Alltuples.append([tuple(x) for x in A.to_numpy()])
        for A in [AllEquity,DebtAndCash,EBITYEARLY]:
            cols=['firm','value']
            for col in cols:
              A[col]=pd.to_numeric(A[col])
            A['CalculatedOn']=CalculatedON
            A=A[['firm','quarter','year','persianDate','Component','value','CalculatedOn']]
            Alltuples.append([tuple(x) for x in A.to_numpy()])
        con = await asyncpg.connect(host=db_host,port=db_port,user=db_username,password=db_pass,database=db_database)    
        await con.execute('''
        CREATE TEMPORARY TABLE _data3(
            firm bigint NOT NULL,
            quarter character varying COLLATE pg_catalog."default",
            year character varying COLLATE pg_catalog."default",
            "toDate" character varying COLLATE pg_catalog."default" NOT NULL,
            "Component" character varying COLLATE pg_catalog."default" NOT NULL,
            "ComponentValue" double precision NOT NULL,
            "CalculatedOn" character varying COLLATE pg_catalog."default" NOT NULL,
            CONSTRAINT "ValuationRatiosComponents_pkey" PRIMARY KEY (firm, "toDate", "Component", "ComponentValue", "CalculatedOn")
            )''')
        for k in Alltuples:
            result = await con.copy_records_to_table('_data3', records=k)
        await con.execute('''
        INSERT INTO {table}(
	    firm, quarter, year, "toDate", "Component", "ComponentValue", "CalculatedOn")
        SELECT * FROM _data3
        ON CONFLICT (firm, "toDate", "Component", "ComponentValue", "CalculatedOn")
        DO NOTHING
        '''.format(table='statement."ValuationRatiosComponents"'))
        await con.close()
    except Exception as E5:
        print(E5)
        # return []
    try:
        for col in LastReportDue:
            LastReportDue[col]=pd.to_numeric(LastReportDue[col])
        tuples2=[tuple(x) for x in LastReportDue.to_numpy()]   
        con = await asyncpg.connect(host=db_host,port=db_port,user=db_username,password=db_pass,database=db_database)    
        await con.execute('''
        CREATE TEMPORARY TABLE _data3(
            firm bigint NOT NULL,
            "LastReportID" bigint NOT NULL
            )''')
        result = await con.copy_records_to_table('_data3', records=tuples2)
        await con.execute('''
        INSERT INTO {table}(
        firm, "LastReportID")
        SELECT * FROM _data3
        ON CONFLICT (firm, "LastReportID")
        DO UPDATE
        SET "LastReportID"=EXCLUDED."LastReportID" 
        '''.format(table='statement."firmLastRatio_OtherRatios"'))
        await con.close()
    except Exception as E5:
        print(E5)
        return []
    print('Stock '+str(ID)+'...Done')
# #///////////
def get_All():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
            select "ID" from "Stocks" where "Parent" is null 
        """, connection)
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close()  
async def download_All(DF):
    async with aiohttp.ClientSession() as session:
        tasks=[]
        for stockID in DF.ID.tolist():
            task=asyncio.ensure_future(SortOneOut(stockID,session))
            tasks.append(task)
        await asyncio.gather(*tasks, return_exceptions=True) 
#///////////
if __name__=="__main__":
    start1=time.time()
    DF=get_All()
    Range=len(DF)
    # print(DF)
    chunksize=3
    chuncks=round(Range/chunksize)
    for i in range(1+chuncks):
        print(str(i)+'/'+str(chuncks))
        if i !=chuncks:
            asyncio.get_event_loop().run_until_complete(download_All(DF.iloc[chunksize*i:chunksize*(i+1)]))            
        else:
            asyncio.get_event_loop().run_until_complete(download_All(DF.iloc[chunksize*i:]))            
    # asyncio.get_event_loop().run_until_complete(download_All(pd.DataFrame([{'ID':'637'}])))            
    print('All took', time.time()-start1, 'seconds.To Calculate All Valuation Ratios ')