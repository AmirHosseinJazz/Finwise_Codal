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
            SELECT * FROM statement."firmLastRatio_financialStrength" where firm=
            """+str(ID), connection)
            LastDone=(df.iloc[0].LastReportID)
        except Exception as E:
            print('Error: '+E)
    except Exception as E2:
        pass
    try:
        print('Fetching BS ')
        wholeFile=''
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
        FinalRatios=[]
        AllYears=Quarterly.year.unique().tolist()
        # for t in Total.date.unique().tolist():
        #     Cash=(Total[(Total['date']==t)&(Total['Item']=='موجودی نقد')].iloc[0].value)
        #     Debt=(Total[(Total['date']==t)&(Total['Item']=='جمع بدهی\u200cها')].iloc[0].value)
        #     Equity=(Total[(Total['date']==t)&(Total['Item']=='جمع حقوق صاحبان سهام')].iloc[0].value)
        #     FinalRatios.append({'firm':ID,'quarter':'','year':t,'Ratio':'CashToDebt','displayTitle':'نقد به بدهی','RatioValue':Cash/Debt,'CalculatedOn':CalculatedON,'toDate':t})
        #     FinalRatios.append({'firm':ID,'quarter':'','year':t,'Ratio':'DebtToEquity','displayTitle':'بدهی به حقوق صاحبان سهام','RatioValue':Debt/Equity,'CalculatedOn':CalculatedON,'toDate':t})
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
                ###
                EBIT_TTM=QDFTemp2[QDFTemp2['translatedName']=='سود (زیان) عملیاتی'].iloc[0].TTM
                Debt=QDFTemp2[QDFTemp2['translatedName']=='جمع بدهی\u200cها'].iloc[0].value
                FinalRatios.append({'firm':ID,'quarter':quarter,'year':year,'toDate':toDatet,'Ratio':'DebtToEBIT','displayTitle':'بدهی به سود عملیاتی','RatioValue':Debt/EBIT_TTM,'CalculatedOn':CalculatedON})
                ##
                EBIT_TTM=QDFTemp2[QDFTemp2['translatedName']=='سود (زیان) عملیاتی'].iloc[0].TTM
                FinancialCostTTM=abs(QDFTemp2[QDFTemp2['translatedName']=='هزینه\u200cهای مالی'].iloc[0].TTM)
                FinalRatios.append({'firm':ID,'quarter':quarter,'year':year,'toDate':toDatet,'Ratio':'InterestCoverage','displayTitle':'پوشش بهره','RatioValue':EBIT_TTM/FinancialCostTTM,'CalculatedOn':CalculatedON})
                ######
                Piot=0
                NetIncomeTTM=QDFTemp2[QDFTemp2['translatedName']=='سود (زیان) خالص'].iloc[0].TTM
                TotalAsset=QDFTemp2[QDFTemp2['translatedName']=='جمع بدهی\u200cها'].iloc[0].value
                PiotStep1=NetIncomeTTM/TotalAsset
                if PiotStep1>0:
                    Piot=Piot+1
                CFFlowOperational=QDFTemp2[QDFTemp2['translatedName']=='جریانهای نقدی حاصل از فعالیتهای عملیاتی'].iloc[0].TTM
                PiotStep2=CFFlowOperational/TotalAsset
                if PiotStep2>0:
                    Piot=Piot+1
                #
                if PiotStep1<PiotStep2:
                    Piot=Piot+1
                NetIncomeNonTTM_T1=Total[(Total['Item']=='سود (زیان) خالص')&(Total['date']==str(LastYear))].iloc[0].value
                NetIncomeNonTTM_T0=Total[(Total['Item']=='سود (زیان) خالص')&(Total['date']==str(YearBeforeLastYear))].iloc[0].value
                TotalAssetNonTTM_T1=Total[(Total['Item']=='سود (زیان) خالص')&(Total['date']==str(LastYear))].iloc[0].value
                TotalAssetNonTTM_T0=Total[(Total['Item']=='سود (زیان) خالص')&(Total['date']==str(YearBeforeLastYear))].iloc[0].value
                
                ROAT1=NetIncomeNonTTM_T1/TotalAssetNonTTM_T1
                ROAT0=NetIncomeNonTTM_T0/TotalAssetNonTTM_T0
                #
                if ROAT1>ROAT0:
                    Piot=Piot+1
                LTDNONTTM_T1=Total[(Total['Item']=='تسهیلات مالی بلندمدت')&(Total['date']==str(LastYear))].iloc[0].value
                LTDNONTTM_T0=Total[(Total['Item']=='تسهیلات مالی بلندمدت')&(Total['date']==str(YearBeforeLastYear))].iloc[0].value
                LTDTOASSET_T1=LTDNONTTM_T1/TotalAssetNonTTM_T1
                LTDTOASSET_T0=LTDNONTTM_T0/TotalAssetNonTTM_T0
                #
                if LTDTOASSET_T1<LTDTOASSET_T0:
                    Piot=Piot+1


                CURRENTDEBT_T1=Total[(Total['Item']=='جمع بدهی\u200cهای جاری')&(Total['date']==str(LastYear))].iloc[0].value
                CURRENTDEBT_T0=Total[(Total['Item']=='جمع بدهی\u200cهای جاری')&(Total['date']==str(YearBeforeLastYear))].iloc[0].value
                CURRENTASSET_T1=Total[(Total['Item']=='جمع دارایی\u200cهای جاری')&(Total['date']==str(LastYear))].iloc[0].value
                CURRENTASSET_T0=Total[(Total['Item']=='جمع دارایی\u200cهای جاری')&(Total['date']==str(YearBeforeLastYear))].iloc[0].value
                CurrentAssetToDebt_T1=CURRENTASSET_T1/CURRENTDEBT_T1
                CurrentAssetToDebt_T0=CURRENTASSET_T0/CURRENTDEBT_T0
                #
                if CurrentAssetToDebt_T1>CurrentAssetToDebt_T0:
                    Piot=Piot+1
                    
                Capital_T2=Total[(Total['Item']=='سرمایه')&(Total['date']==str(LastYear))].iloc[0].value
                Capital_T1=Total[(Total['Item']=='سرمایه')&(Total['date']==str(YearBeforeLastYear))].iloc[0].value
                Capital_T0=Total[(Total['Item']=='سرمایه')&(Total['date']==str(YekiGhabltareGhabli))].iloc[0].value
                if (Capital_T2-Capital_T1)<=(Capital_T1-Capital_T0):
                    Piot=Piot+1
                
                RevenueNonTTM_T1=Total[(Total['Item']=='درآمدهای عملیاتی')&(Total['date']==str(LastYear))].iloc[0].value
                RevenueNonTTM_T2=Total[(Total['Item']=='درآمدهای عملیاتی')&(Total['date']==str(YearBeforeLastYear))].iloc[0].value
                RevToTotalAssetT1=NetIncomeNonTTM_T1/TotalAssetNonTTM_T1
                RevToTotalAssetT0=RevenueNonTTM_T1/RevenueNonTTM_T2
                if RevToTotalAssetT1>RevToTotalAssetT0:
                    Piot=Piot+1
                NetIncomeTTM_NOW=Quarterly[(Quarterly['translatedName']=='سود (زیان) خالص')&(Quarterly['quarter']==quarter)&(Quarterly['year']==str(year))].iloc[0].TTM
                NetIncomeTTM_LastYear=Quarterly[(Quarterly['translatedName']=='سود (زیان) خالص')&(Quarterly['quarter']==quarter)&(Quarterly['year']==str(LastYear))].iloc[0].TTM
                REVENUETTM_NOW=Quarterly[(Quarterly['translatedName']=='درآمدهای عملیاتی')&(Quarterly['quarter']==quarter)&(Quarterly['year']==str(year))].iloc[0].TTM
                REVENUETTM_LastYear=Quarterly[(Quarterly['translatedName']=='درآمدهای عملیاتی')&(Quarterly['quarter']==quarter)&(Quarterly['year']==str(year))].iloc[0].TTM
                if (NetIncomeTTM_NOW/REVENUETTM_NOW)>(NetIncomeTTM_LastYear/REVENUETTM_LastYear):
                    Piot=Piot+1
                FinalRatios.append({'firm':ID,'quarter':quarter,'year':year,'toDate':toDatet,'Ratio':'Piotrowski','displayTitle':'پیوتروسکی','RatioValue':Piot,'CalculatedOn':CalculatedON})
                # ##########///////////
                RecievablesShortTerm_NOW=Quarterly[(Quarterly['translatedName']=='دریافتنیهای تجاری و سایر دریافتنی ها')&(Quarterly['quarter']==quarter)&(Quarterly['year']==str(year))].iloc[0].value
                RecievablesShortTerm_T0=Quarterly[(Quarterly['translatedName']=='دریافتنیهای تجاری و سایر دریافتنی ها')&(Quarterly['quarter']==quarter)&(Quarterly['year']==str(LastYear))].iloc[0].value
                StaffCost_NOW=Quarterly[(Quarterly['translatedName']=='هزینه\u200cهای فروش، اداری و عمومی')&(Quarterly['quarter']==quarter)&(Quarterly['year']==str(year))].iloc[0].TTM
                StaffCost_T0=Quarterly[(Quarterly['translatedName']=='هزینه\u200cهای فروش، اداری و عمومی')&(Quarterly['quarter']==quarter)&(Quarterly['year']==str(LastYear))].iloc[0].TTM
                FixedAsset_NOW=Quarterly[(Quarterly['translatedName']=='دارایی\u200cهای ثابت مشهود')&(Quarterly['quarter']==quarter)&(Quarterly['year']==str(year))].iloc[0].value
                FixedAsset_T0=Quarterly[(Quarterly['translatedName']=='دارایی\u200cهای ثابت مشهود')&(Quarterly['quarter']==quarter)&(Quarterly['year']==str(LastYear))].iloc[0].value
                CurrentAsset_NOW=Quarterly[(Quarterly['translatedName']=='جمع دارایی\u200cهای جاری')&(Quarterly['quarter']==quarter)&(Quarterly['year']==str(year))].iloc[0].value
                CurrentAsset_T0=Quarterly[(Quarterly['translatedName']=='جمع دارایی\u200cهای جاری')&(Quarterly['quarter']==quarter)&(Quarterly['year']==str(LastYear))].iloc[0].value
                TotalAsset_NOW=Quarterly[(Quarterly['translatedName']=='جمع دارایی\u200cها')&(Quarterly['quarter']==quarter)&(Quarterly['year']==str(year))].iloc[0].value
                TotalAsset_T0=Quarterly[(Quarterly['translatedName']=='جمع دارایی\u200cها')&(Quarterly['quarter']==quarter)&(Quarterly['year']==str(LastYear))].iloc[0].value
                NonOperational_Investment_NOW=Quarterly[(Quarterly['translatedName']=='سایر درآمدها و هزینه\u200cهای غیرعملیاتی- درآمد سرمایه\u200cگذاری\u200cها')&(Quarterly['quarter']==quarter)&(Quarterly['year']==str(year))].iloc[0].TTM
                NonOperational_Investment_T0=Quarterly[(Quarterly['translatedName']=='سایر درآمدها و هزینه\u200cهای غیرعملیاتی- درآمد سرمایه\u200cگذاری\u200cها')&(Quarterly['quarter']==quarter)&(Quarterly['year']==str(LastYear))].iloc[0].TTM
                CFOperational_NOW=Quarterly[(Quarterly['translatedName']=='جریانهای نقدی حاصل از فعالیتهای عملیاتی')&(Quarterly['quarter']==quarter)&(Quarterly['year']==str(year))].iloc[0].TTM
                CFOperational_T0=Quarterly[(Quarterly['translatedName']=='جریانهای نقدی حاصل از فعالیتهای عملیاتی')&(Quarterly['quarter']==quarter)&(Quarterly['year']==str(LastYear))].iloc[0].TTM
                
                
                DSRI=(RecievablesShortTerm_NOW/REVENUETTM_NOW)/(RecievablesShortTerm_T0/REVENUETTM_LastYear)
                #
                GMI=(NetIncomeTTM_LastYear/REVENUETTM_LastYear)/(NetIncomeTTM_NOW/REVENUETTM_NOW)
                #
                AQI=(1-(CurrentAsset_NOW+FixedAsset_NOW)/TotalAsset_NOW)/(1-(FixedAsset_T0+CurrentAsset_T0)/TotalAsset_T0)
                #
                SGI=REVENUETTM_NOW/REVENUETTM_LastYear
                #
                DEPI=1
                #
                SGAI=(StaffCost_NOW/REVENUETTM_NOW)/(StaffCost_T0/REVENUETTM_LastYear)
                #
                TATA=(NetIncomeTTM_NOW-NonOperational_Investment_NOW-CFOperational_NOW)/TotalAsset_NOW
                #
                LVGI=((LTDNONTTM_T1+CURRENTDEBT_T1)/TotalAsset_NOW)/((LTDNONTTM_T0+CURRENTDEBT_T0)/TotalAsset_T0)
                BENISH= -4.84+0.92*DSRI+0.528*GMI +0.404*AQI+0.892*SGI +0.115*DEPI-0.172*SGAI+4.679*TATA-0.327*LVGI
                FinalRatios.append({'firm':ID,'quarter':quarter,'year':year,'toDate':QDFTemp2.toDate.unique().tolist()[0],'Ratio':'Benish','displayTitle':'بنیش','RatioValue':BENISH,'CalculatedOn':CalculatedON})
                ## Altman Z-Score

                X1 = (CurrentAsset_NOW - CURRENTDEBT_T1) /(TotalAsset_NOW)
                SCount=Capital_T2*1000
                Mcap=HistoricMarketCap[HistoricMarketCap['engDate']<str(JalaliDate(toDatet.split('/')[0],toDatet.split('/')[1],toDatet.split('/')[2]).todate())].iloc[0].MarketCap
                try:
                    DPSTemp=DPS[DPS['year']==year].iloc[0].Value
                except:
                    DPSTemp=0
                X2 =(NetIncomeNonTTM_T1 - DPSTemp*SCount)/TotalAsset_NOW
                X3 = (EBIT_TTM /TotalAsset_NOW)
                X4 = Mcap/(Debt*1000000)
                # print(Mcap)
                X5 = REVENUETTM_NOW/ TotalAsset_NOW
                Altman_Z=X1*1.2 + 1.4*X2 + 3.3*X3 +0.6*X4 +X5
                FinalRatios.append({'firm':ID,'quarter':QDFTemp2.quarter.unique().tolist()[0],'year':year,'toDate':QDFTemp2.toDate.unique().tolist()[0],'Ratio':'Altman_Z','displayTitle':'آلتمن','RatioValue':Altman_Z,'CalculatedOn':CalculatedON})
                #########
                TaxAmount=Total[(Total['Item']=='مالیات')&(Total['date']==str(LastYear))].iloc[0].value
                # print(TaxAmount)
                EBIT=Total[(Total['Item']=='سود (زیان) عملیات در حال تداوم قبل از مالیات')&(Total['date']==str(LastYear))].iloc[0].value
                # print(EBIT)
                RateTax=TaxAmount*-1/EBIT
                # print(RateTax)
                NOPAT=(1-RateTax)*EBIT
                # print(NOPAT)

                try:
                    CommercialPayables_T1=Total[(Total['Item']=='پرداختنیهای تجاری و سایر پرداختنیها')&(Total['date']==str(LastYear))].iloc[0].value
                except:
                    CommercialPayables_T1=0
                try:
                    CommercialPayables_T0=Total[(Total['Item']=='پرداختنیهای تجاری و سایر پرداختنیها')&(Total['date']==str(YearBeforeLastYear))].iloc[0].value
                except:
                    CommercialPayables_T0=0
                ShortTermInvestment_T1=Total[(Total['Item']=='سرمایه\u200cگذاری\u200c\u200cهای کوتاه\u200cمدت')&(Total['date']==str(LastYear))].iloc[0].value
                ShortTermInvestment_T0=Total[(Total['Item']=='سرمایه\u200cگذاری\u200c\u200cهای کوتاه\u200cمدت')&(Total['date']==str(YearBeforeLastYear))].iloc[0].value
                Cash_T1=Total[(Total['Item']=='موجودی نقد')&(Total['date']==str(LastYear))].iloc[0].value
                Cash_T0=Total[(Total['Item']=='موجودی نقد')&(Total['date']==str(YearBeforeLastYear))].iloc[0].value
                InvestedCapital_T0=TotalAsset_NOW-CommercialPayables_T1-Cash_T1-ShortTermInvestment_T1 -max(0,CURRENTDEBT_T1-CURRENTASSET_T1+Cash_T1+ShortTermInvestment_T1)
                InvestedCapital_T1=TotalAsset_T0-CommercialPayables_T0-Cash_T0-ShortTermInvestment_T0 -max(0,CURRENTDEBT_T0-CURRENTASSET_T0+Cash_T0+ShortTermInvestment_T0)
                InvestedCapitalFinal=(InvestedCapital_T0+InvestedCapital_T1)/2
                FinalRatios.append({'firm':ID,'quarter':quarter,'year':year,'toDate':toDatet,'Ratio':'ROIC','displayTitle':'ROIC','RatioValue':NOPAT/InvestedCapitalFinal,'CalculatedOn':CalculatedON})
                DBT1=Total[(Total['Item']=='جمع بدهی\u200cها')&(Total['date']==str(LastYear))].iloc[0].value
                EQT1=Total[(Total['Item']=='جمع حقوق صاحبان سهام')&(Total['date']==str(LastYear))].iloc[0].value
                WACC= (1-RateTax)*DBT1/(DBT1+EQT1)*ReturnDebtRatio + (EQT1/(EQT1+DBT1)*DiscountRate)
                FinalRatios.append({'firm':ID,'quarter':quarter,'year':year,'toDate':toDatet,'Ratio':'WACC','displayTitle':'WACC','RatioValue':WACC,'CalculatedOn':CalculatedON})
            except:
                # print(traceback.format_exc())
                break
    except Exception as E3:
        print(E)
        print(traceback.format_exc())
        return []
    
    try:
        Final=pd.DataFrame(FinalRatios)
        Final=Final[Final['year']>'1390']
        Final=Final[['firm','quarter','year','toDate','Ratio','displayTitle','RatioValue','CalculatedOn']]
        # print(Final)
        cols=[i for i in Final.columns if i not in ['quarter','year',"CalculatedOn",'Ratio','displayTitle','toDate']]
        for col in cols:
            Final[col]=pd.to_numeric(Final[col])
        tuples = [tuple(x) for x in Final.to_numpy()]   
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
            CONSTRAINT "FinancialStrengthRatios_pkey" PRIMARY KEY (firm, "Ratio", "toDate", "RatioValue", "CalculatedOn")
        )''')
        result = await con.copy_records_to_table('_data', records=tuples)
        await con.execute('''
        INSERT INTO {table}(
        firm, quarter, year, "toDate", "Ratio", "displayTitle", "RatioValue", "CalculatedOn")
        SELECT * FROM _data
        ON CONFLICT  (firm, "Ratio", "toDate", "RatioValue", "CalculatedOn")
        DO NOTHING
    '''.format(table='statement."FinancialStrengthRatios"'))
        await con.close()
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
            CONSTRAINT "firmLastRatio_financialStrength_pkey" PRIMARY KEY (firm, "LastReportID")
        )''')
        result = await con.copy_records_to_table('_data2', records=tuples2)
        await con.execute('''
        INSERT INTO {table}(
        firm, "LastReportID")
        SELECT * FROM _data2
        ON CONFLICT  (firm, "LastReportID")
        DO UPDATE
        SET  "LastReportID"=EXCLUDED."LastReportID" 
    '''.format(table='statement."firmLastRatio_financialStrength"'))
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
        print(str(i)+'/'+str(Range))
        if i !=chuncks:
            asyncio.get_event_loop().run_until_complete(download_All(DF.iloc[chunksize*i:chunksize*(i+1)]))            
        else:
            asyncio.get_event_loop().run_until_complete(download_All(DF.iloc[chunksize*i:]))            
    # asyncio.get_event_loop().run_until_complete(download_All(pd.DataFrame([{'ID':'637'}])))            
    print('All took', time.time()-start1, 'seconds.To Calculate All Valuation Ratios ')