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
LOCAL_TIMEZONE  = pytz.timezone('Asia/Tehran')
db_username="Jay"
db_pass="Mehrad1"
db_host="127.0.0.1"
db_port="5432"
db_database="FinWisev12"  
#///////////
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
            SELECT * FROM statement."firmLastRatio" where firm=
            """+str(ID), connection)
            LastDone=(df.iloc[0].LastReportID)
        except Exception as E:
            print('Error: '+E)
    except Exception as E2:
        pass
    try:
        print('Fetching BS ')
        # wholeFile=''
        # try:
        #     head = {'Accept-Profile':'statement'}
        #     async with session.get('http://130.185.74.40:3000/rpc/bsall?a='+str(ID),timeout=15,headers=head) as resp:
        #         wholeFile=await resp.text()
        #         if wholeFile is None or wholeFile=='':
        #             raise Exception('NoData')
        # except Exception as E:
        #     print('Error: '+E)
        # BSAll=pd.DataFrame(json.loads(wholeFile))
        try:
            connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
            cursor = connection.cursor()
            BSAll = psql.read_sql("""
            SELECT * from statement.bsall('"""+str(ID)+"""')""", connection)
        except Exception as E:
            print('Error: '+E)
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
        # wholeFile=''
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
    ##///////
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
        RecentYear=Total.date.unique().tolist()[0]
        # print(RecentYear)
        TenYears=Total.date.unique().tolist()[:10]
        AdjustedEBITMARGINAll=[]
        TaxRatesAll=[]
        NetProfits=[]
        REVENUEYears=[]
        EBITDA=[]
        DiscountRate=0.45
        ReturnDebtRatio=0.17
        for t in TenYears:
            NonOperationalRevenue_Misc=0
            TaxThisYear=0
            OperationalIncome=0
            EarningProfitBeforTax=0
            Revenue=0
            if len(Total[(Total['Item']=='سود (زیان) خالص')&(Total['date']==t)])!=0:
                NetProfits.append({'Date':t,'Profit':Total[(Total['Item']=='سود (زیان) خالص')&(Total['date']==t)].iloc[0].value})
                if t==RecentYear:
                    RecentNetProfit=Total[(Total['Item']=='سود (زیان) خالص')&(Total['date']==t)].iloc[0].value
            if len(Total[(Total['Item']=='سود (زیان) عملیاتی')&(Total['date']==t)])!=0:
                OperationalIncome=Total[(Total['Item']=='سود (زیان) عملیاتی')&(Total['date']==t)].iloc[0].value
                if t==RecentYear:
                    RecentOperationalIncome=Total[(Total['Item']=='سود (زیان) عملیاتی')&(Total['date']==t)].iloc[0].value
                EBITDA.append({'Date':t,'EBITDA':OperationalIncome})
            if len(Total[(Total['Item']=='سایر درآمدها و هزینه‌های غیرعملیاتی- اقلام متفرقه')&(Total['date']==t)])!=0:
                NonOperationalRevenue_Misc=Total[(Total['Item']=='سایر درآمدها و هزینه‌های غیرعملیاتی- اقلام متفرقه')&(Total['date']==t)].iloc[0].value
                if t==RecentYear:
                    RecentNonOperationalRevenue_Misc=Total[(Total['Item']=='سایر درآمدها و هزینه‌های غیرعملیاتی- اقلام متفرقه')&(Total['date']==t)].iloc[0].value
            if len(Total[(Total['Item']=='مالیات')&(Total['date']==t)])!=0:
                TaxThisYear=Total[(Total['Item']=='مالیات')&(Total['date']==t)].iloc[0].value
                if t==RecentYear:
                    RecentTax=Total[(Total['Item']=='مالیات')&(Total['date']==t)].iloc[0].value
            if len(Total[(Total['Item']=='سود (زیان) عملیات در حال تداوم قبل از مالیات')&(Total['date']==t)])!=0:
                EarningProfitBeforTax=Total[(Total['Item']=='سود (زیان) عملیات در حال تداوم قبل از مالیات')&(Total['date']==t)].iloc[0].value 
                if t==RecentYear:
                    RecentEarningProfitBeforTax=Total[(Total['Item']=='سود (زیان) عملیات در حال تداوم قبل از مالیات')&(Total['date']==t)].iloc[0].value
            if len(Total[(Total['Item']=='درآمدهای عملیاتی')&(Total['date']==t)])!=0:
                Revenue=Total[(Total['Item']=='درآمدهای عملیاتی')&(Total['date']==t)].iloc[0].value   
                if t==RecentYear:
                    RecentRevenue=Total[(Total['Item']=='درآمدهای عملیاتی')&(Total['date']==t)].iloc[0].value
            NormalizedEbit=OperationalIncome-NonOperationalRevenue_Misc
            TaxRate=abs(TaxThisYear)/EarningProfitBeforTax
            TaxRatesAll.append({'Date':t,'TaxRates':TaxRate})
            AdjustedEarning=NormalizedEbit+TaxThisYear
            AdjustedEBITMARGIN=AdjustedEarning/Revenue
            AdjustedEBITMARGINAll.append({'Date':t,'AdjstedEbitMargin':AdjustedEBITMARGIN})
            if t==RecentYear:
                RecentDebt=Total[(Total['Item']=='جمع بدهی\u200cها')&(Total['date']==t)].iloc[0].value
                RecentEquity=Total[(Total['Item']=='جمع حقوق صاحبان سهام')&(Total['date']==t)].iloc[0].value
                RecentLongTermFac=Total[(Total['Item']=='تسهیلات مالی بلندمدت')&(Total['date']==t)].iloc[0].value
                RecentCash=Total[(Total['Item']=='موجودی نقد')&(Total['date']==t)].iloc[0].value
                RecentCF_FromOperation=0
                if len(Total[(Total['Item']=='جریانهای نقدی حاصل از فعالیتهای عملیاتی')&(Total['date']==t)])!=0:
                    RecentCF_FromOperation=+Total[(Total['Item']=='جریانهای نقدی حاصل از فعالیتهای عملیاتی')&(Total['date']==t)].value.tolist()[0]
                if len(Total[(Total['Item']=='جریان خالص ورود(خروج) وجه نقد ناشی از فعالیت‌های عملیاتی-عادی')&(Total['date']==t)])!=0:
                    RecentCF_FromOperation+=Total[(Total['Item']=='جریان خالص ورود(خروج) وجه نقد ناشی از فعالیت‌های عملیاتی-عادی')&(Total['date']==t)].value.tolist()[0]
                if len(Total[(Total['Item']=='جریان خالص ورود(خروج) وجه نقد ناشی از فعالیت‌های عملیاتی - استثنایی')&(Total['date']==t)])!=0:
                    RecentCF_FromOperation+=Total[(Total['Item']=='جریان خالص ورود(خروج) وجه نقد ناشی از فعالیت‌های عملیاتی - استثنایی')&(Total['date']==t)].value.tolist()[0]
                RecentCF_TobuyFixedAsset=0
                if len(Total[(Total['Item']=='وجوه پرداختی بابت خرید دارایی‌های ثابت مشهود')&(Total['date']==t)])!=0:
                    RecentCF_TobuyFixedAsset+=Total[(Total['Item']=='وجوه پرداختی بابت خرید دارایی‌های ثابت مشهود')&(Total['date']==t)].value.tolist()[0]
                if len(Total[(Total['Item']=='وجوه دریافتی بابت فروش دارایی‌های ثابت مشهود')&(Total['date']==t)])!=0:
                    RecentCF_TobuyFixedAsset+=Total[(Total['Item']=='وجوه دریافتی بابت فروش دارایی‌های ثابت مشهود')&(Total['date']==t)].value.tolist()[0]
                RecentCapital=Total[(Total['Item']=='سرمایه')&(Total['date']==t)].value.tolist()[0]
        Average10YearsNetProfits=pd.DataFrame(NetProfits).Profit.mean()
        Average_AllTaxRates5Years=pd.DataFrame(TaxRatesAll).head(5).TaxRates.mean()
        Average_AllAdjustedEbitMargin5Years=pd.DataFrame(AdjustedEBITMARGINAll).head(5).AdjstedEbitMargin.mean()
    except Exception as E3:
        print(E3)
        return []
    
    try:
        FinalItems=[]
        #EPV CALC
        Adjusted_AdjustedEarning=(RecentRevenue*Average_AllAdjustedEbitMargin5Years)+RecentTax
        WACC= (1-Average_AllTaxRates5Years)*RecentDebt/(RecentDebt+RecentEquity)*ReturnDebtRatio + (RecentEquity/(RecentEquity+RecentDebt)*DiscountRate)
        ShareCountThisYear=RecentCapital*1000
        EPV_NOTPERSHARE=Adjusted_AdjustedEarning/WACC
        EPVFINAL=(EPV_NOTPERSHARE+RecentCash-RecentLongTermFac)*1000000/ShareCountThisYear
        # print('EPVFINAL: '+str(EPVFINAL))
        FinalItems.append({'firm':ID,'Ratio':'EPV','RatioValue':EPVFINAL,'CalculatedOn':CalculatedON,'displayTitle':'قدرت سودآوری','toDate':lastToDate})
        ####DCF (FCF)
        NP=pd.DataFrame(NetProfits)
        NP['ProfitShifted']=NP['Profit'].shift(-1)
        NP['return']=((NP['Profit']-NP['ProfitShifted'])/(abs(NP['ProfitShifted'])))
        NP.dropna(subset=['return'],inplace=True)
        #G1 - Growht rate to y10
        #G2 - Growth rate from 10 to inf
        G1=NP['return'].mean()
        if (G1<0.3):
            G1=0.3
        if (G1>0.6):
            G1=0.6
        G2=0.30
        G1=0.3
        FCF=RecentCF_FromOperation+RecentCF_TobuyFixedAsset
        Coeef=[]
        for i in range(10):
            S=math.pow((1+G1),i+1)
            M=math.pow((1+DiscountRate),i+1)
            if i==9:
                Coeef.append((S/M)/(DiscountRate-G2))
            else:
                Coeef.append(S/M)
        DCF_FCF=(sum(Coeef)*FCF*1000000)/ShareCountThisYear
        FinalItems.append({'firm':ID,'Ratio':'DCF_FCF','RatioValue':DCF_FCF,'CalculatedOn':CalculatedON,'displayTitle':'تنزیل جریان نقد آزاد','toDate':lastToDate})
        DCF_EarningBased_Term=RecentNetProfit-RecentNonOperationalRevenue_Misc
       
        DCF_EarningBased=(sum(Coeef)*DCF_EarningBased_Term*1000000)/ShareCountThisYear
        # print(DCF_EarningBased)
        FinalItems.append({'firm':ID,'Ratio':'DCF_EarningBased','RatioValue':DCF_EarningBased,'CalculatedOn':CalculatedON,'displayTitle':'تنزیل سود','toDate':lastToDate})
        #########
        EB=pd.DataFrame(EBITDA)
        EB['ProfitShifted']=EB['EBITDA'].shift(-1)
        EB['return']=((EB['EBITDA']-EB['ProfitShifted'])/(abs(EB['ProfitShifted'])))
        EB.dropna(subset=['return'],inplace=True)
        #G1 - Growht rate to y10
        #G2 - Growth rate from 10 to inf
        G1=EB['return'].mean()
        if (G1<0.3):
            G1=0.3
        if (G1>0.6):
            G1=0.6
        PeterLynch=(Quarterly[Quarterly['translatedName']=='EarningWithouNRI'].iloc[0].TTM*100*G1*1000000)/float(LastShareCount)   
        # print(Quarterly[Quarterly['translatedName']=='EarningWithouNRI'].iloc[0].TTM)
        FinalItems.append({'firm':ID,'Ratio':'PeterLynch','RatioValue':PeterLynch,'CalculatedOn':CalculatedON,'displayTitle':'پیتر لینچ','toDate':lastToDate})
        ###############
        LastEquityQ=Quarterly[Quarterly['translatedName']=='جمع حقوق صاحبان سهام'].iloc[0].value
        LastIntangibleQ=Quarterly[Quarterly['translatedName']=='دارایی‌های نامشهود'].iloc[0].value
        TangibleBookPerShare=(LastEquityQ-LastIntangibleQ)*1000000/float(LastShareCount)
        FinalItems.append({'firm':ID,'Ratio':'TangibleBookPerShare','RatioValue':TangibleBookPerShare,'CalculatedOn':CalculatedON,'displayTitle':'داریی مشهود','toDate':lastToDate})
        ###############
        GRAHAMNUMBER=math.pow((Quarterly[Quarterly['translatedName']=='EarningWithouNRI'].iloc[0].TTM/float(LastShareCount)*1000000*TangibleBookPerShare*22.5),0.5)
        FinalItems.append({'firm':ID,'Ratio':'GRAHAMNUMBER','RatioValue':GRAHAMNUMBER,'CalculatedOn':CalculatedON,'displayTitle':'عدد گراهام','toDate':lastToDate})
        #####
        LastDebtQ=Quarterly[Quarterly['translatedName']=='جمع بدهی\u200cها'].iloc[0].value
        LastCurrentAssetQ=Quarterly[Quarterly['translatedName']=='جمع دارایی\u200cهای جاری'].iloc[0].value
        NCAV=(LastCurrentAssetQ-LastDebtQ)*1000000/float(LastShareCount)
        FinalItems.append({'firm':ID,'Ratio':'NCAV','RatioValue':NCAV,'CalculatedOn':CalculatedON,'displayTitle':'ارزش دارایی جاری','toDate':lastToDate})
        
    except Exception as E4:
        print(E4)
        return []
    try:
        DF=pd.DataFrame(FinalItems)
        cols=[i for i in DF.columns if i not in ["CalculatedOn",'Ratio','displayTitle','toDate']]
        for col in cols:
            DF[col]=pd.to_numeric(DF[col])
        tuples = [tuple(x) for x in DF.to_numpy()]   
        con = await asyncpg.connect(host=db_host,port=db_port,user=db_username,password=db_pass,database=db_database)    
        await con.execute('''
        CREATE TEMPORARY TABLE _data(
            firm bigint NOT NULL,
            "Ratio" character varying COLLATE pg_catalog."default" NOT NULL,
            "RatioValue" double precision NOT NULL,
            "CalculatedOn" character varying COLLATE pg_catalog."default" NOT NULL,
            "displayTitle" character varying COLLATE pg_catalog."default" NOT NULL,
            "toDate" character varying COLLATE pg_catalog."default" NOT NULL,
            CONSTRAINT "ValuationRatios_pkey" PRIMARY KEY (firm, "Ratio", "RatioValue", "CalculatedOn","toDate")
        )''')
        result = await con.copy_records_to_table('_data', records=tuples)
        await con.execute('''
        INSERT INTO {table}(
        firm, "Ratio", "RatioValue", "CalculatedOn","displayTitle","toDate")
        SELECT * FROM _data
        ON CONFLICT (firm, "Ratio", "RatioValue", "CalculatedOn","toDate")
        DO NOTHING
    '''.format(table='statement."ValuationRatios"'))
        await con.close()
        print('Stock '+str(ID)+'...Done')
    except Exception as E5:
        print(E5)
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
        AllEarnings=Quarterly[Quarterly['translatedName']=='EarningWithouNRI']
        AllEarnings.rename(columns={'toDate':'persianDate'},inplace=True)
        AllEarnings=AllEarnings.sort_values(by=['persianDate'],ascending=False)
        AllEarnings.reset_index(inplace=True,drop=True)
        AllMarketCaps=HistoricMarketCap
        AllMarketCaps['persianDate']=AllMarketCaps['engDate'].apply(lambda x: str(JalaliDate(datetime.strptime(x,'%Y-%m-%d'))).replace('-','/'))
        DFT=AllMarketCaps.append(AllEarnings)
        DFT.sort_values(by=['persianDate'],ascending=False,inplace=True)
        DFT['TTM'].fillna(method='bfill',inplace=True)
        DFT=DFT[~DFT['stockID'].isnull()]
        DFT['PtoETTM']=DFT['MarketCap']/(DFT['TTM']*1000000)
        DFT1=DFT[['stockID','engDate','PtoETTM']]
        DFT1.loc[:,'CalculatedOn']=CalculatedON
        DFT1.loc[:,'Ratio']='PtoETTM'
        DFT1.loc[:,'displayTitle']='P/E (TTM)'
        ######
        DFT1Component=DFT.head(1)
        DFT1Component=DFT1Component[['stockID','engDate','TTM']]
        DFT1Component.loc[:,'CalculatedOn']=CalculatedON
        DFT1Component.loc[:,'Component']='E(TTM)'
        DFT1Component.loc[:,'displayTitle']='E(TTM)_PtoE'
        ##########
        AllRevenues=Quarterly[Quarterly['translatedName']=='درآمدهای عملیاتی']
        AllRevenues.rename(columns={'toDate':'persianDate'},inplace=True)
        AllRevenues=AllRevenues.sort_values(by=['persianDate'],ascending=False)
        AllRevenues.reset_index(inplace=True,drop=True)
        AllMarketCaps=HistoricMarketCap
        AllMarketCaps['persianDate']=AllMarketCaps['engDate'].apply(lambda x: str(JalaliDate(datetime.strptime(x,'%Y-%m-%d'))).replace('-','/'))
        DFT2=AllMarketCaps.append(AllRevenues)
        DFT2.sort_values(by=['persianDate'],ascending=False,inplace=True)
        DFT2['TTM'].fillna(method='bfill',inplace=True)
        DFT2=DFT2[~DFT2['stockID'].isnull()]    
        DFT2['PToSTTM']=DFT2['MarketCap']/(DFT2['TTM']*1000000)
        DFT2=DFT2[DFT2['persianDate']>'1390/01/01']
        DFT2=DFT2[~DFT2['PToSTTM'].isnull()]    
        DFT2_1=DFT2[['stockID','engDate','PToSTTM']]
        DFT2_1.loc[:,'CalculatedOn']=CalculatedON
        DFT2_1.loc[:,'Ratio']='PtoSTTM'
        DFT2_1.loc[:,'displayTitle']='P/S (TTM)'
        #####
        DFT2Component=DFT.head(1)
        DFT2Component=DFT2Component[['stockID','engDate','TTM']]
        DFT2Component.loc[:,'CalculatedOn']=CalculatedON
        DFT2Component.loc[:,'Component']='R(TTM)'
        DFT2Component.loc[:,'displayTitle']='R(TTM)_PtoS' 
        ###
    except Exception as E4:
        print(E4)
        return []
    try:
        # cols=[i for i in DFT1.columns if i not in ["CalculatedOn",'Ratio','displayTitle','engDate']]
        # for col in cols:
        #     DF[col]=pd.to_numeric(DFT1[col])
        tuples = [tuple(x) for x in DFT1.to_numpy()]   
        tuples2=[tuple(x) for x in DFT2_1.to_numpy()]   
        con = await asyncpg.connect(host=db_host,port=db_port,user=db_username,password=db_pass,database=db_database)    
        await con.execute('''
        CREATE TEMPORARY TABLE _data2(
            firm bigint NOT NULL,
            "toDate" character varying COLLATE pg_catalog."default",
            "RatioValue" double precision NOT NULL,
            "CalculatedOn" character varying COLLATE pg_catalog."default" NOT NULL,
            "Ratio" character varying COLLATE pg_catalog."default" NOT NULL,
            "displayTitle" character varying COLLATE pg_catalog."default" NOT NULL,
            CONSTRAINT "ValuationRatios_pkey" PRIMARY KEY (firm, "Ratio", "RatioValue", "CalculatedOn","toDate")
        )''')
        result = await con.copy_records_to_table('_data2', records=tuples)
        result = await con.copy_records_to_table('_data2', records=tuples2)
        await con.execute('''
        INSERT INTO {table}(
        firm,"toDate", "RatioValue",  "CalculatedOn","Ratio","displayTitle")
        SELECT * FROM _data2
        ON CONFLICT (firm,"toDate", "Ratio", "RatioValue", "CalculatedOn","toDate")
        DO NOTHING
    '''.format(table='statement."ValuationRatios"'))
        await con.close()
        
    except Exception as E5:
        print(E5)
        return []
    try:
        # cols=[i for i in DFT1.columns if i not in ["CalculatedOn",'Ratio','displayTitle','engDate']]
        # for col in cols:
        #     DF[col]=pd.to_numeric(DFT1[col])
        tuples = [tuple(x) for x in DFT1Component.to_numpy()]   
        tuples2=[tuple(x) for x in DFT2Component.to_numpy()]   
        con = await asyncpg.connect(host=db_host,port=db_port,user=db_username,password=db_pass,database=db_database)    
        await con.execute('''
        CREATE TEMPORARY TABLE _data2(
            firm bigint NOT NULL,
            "toDate" character varying COLLATE pg_catalog."default" NOT NULL,
            "ComponentValue" double precision NOT NULL,
            "CalculatedOn" character varying COLLATE pg_catalog."default" NOT NULL,
            "Component" character varying COLLATE pg_catalog."default" NOT NULL,
            "displayTitle" character varying COLLATE pg_catalog."default" NOT NULL,
            CONSTRAINT "ValuationRatiosComponents_pkey" PRIMARY KEY (firm, "toDate", "Component", "ComponentValue", "CalculatedOn")
        )''')
        result = await con.copy_records_to_table('_data2', records=tuples)
        result = await con.copy_records_to_table('_data2', records=tuples2)
    #     await con.execute('''
    #     INSERT INTO {table}(
    #     firm, "toDate", "ComponentValue", "CalculatedOn", "Component", "displayTitle")
    #     SELECT * FROM _data2
    #     ON CONFLICT  (firm, "toDate", "Component", "ComponentValue", "CalculatedOn")
    #     DO NOTHING
    # '''.format(table='statement."ValuationRatiosComponents"'))
        await con.close()
        print('Stock Component'+str(ID)+'...Done')
    except Exception as E5:
        print(E5)
        return []
    try:
        tuples2=[tuple(x) for x in LastReportDue.to_numpy()]   
        con = await asyncpg.connect(host=db_host,port=db_port,user=db_username,password=db_pass,database=db_database)    
        await con.execute('''
        CREATE TEMPORARY TABLE _data2(
            firm bigint NOT NULL,
            "LastReportID" bigint NOT NULL,
            CONSTRAINT "firmLastRatio_pkey" PRIMARY KEY (firm, "LastReportID")
        )''')
        result = await con.copy_records_to_table('_data2', records=tuples2)
        await con.execute('''
        INSERT INTO {table}(
        firm, "LastReportID")
        SELECT * FROM _data2
        ON CONFLICT  (firm, "LastReportID")
        DO UPDATE
        SET  "LastReportID"=EXCLUDED."LastReportID" 
    '''.format(table='statement."firmLastRatio"'))
        await con.close()
        pass
    except Exception as E5:
        print(E5)
        return []
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