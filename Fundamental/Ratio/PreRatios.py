from typing import Type
import requests
import pandas as pd
import json
import time
from khayyam import *
import psycopg2
import datetime
import requests
import pandas.io.sql as psql
db_username="Jay"
db_pass="Mehrad1"
db_host="127.0.0.1"
db_port="5432"
db_database="FinWisev12"
#pd.set_option('display.max_rows', 500)
QPeriod={'3':'Q1','6':'Q2','9':'Q3','12':'Q4'}

def getLast4BC(identifier):
    ct=0
    while ct<3:
        head = {'Accept-Profile':'statement'}
        resp = requests.get('http://130.185.74.40:3000/rpc/bsall?a='+str(identifier),headers=head)
        if resp.status_code == 200:
            return pd.DataFrame(json.loads(resp.text))
        else:
            time.sleep(2)
            ct=ct+1
    return ("noData")
def getLastIS(identifier):
    ct=0
    while ct<3:
        head = {'Accept-Profile':'statement'}
        resp = requests.get('http://130.185.74.40:3000/rpc/isall?a='+str(identifier),headers=head)
        if resp.status_code == 200:
            return pd.DataFrame(json.loads(resp.text))
        else:
            time.sleep(2)
            ct=ct+1
    return ("noData")
def getAll():
    ct=0
    while ct<3:
        head = {'Accept-Profile':'marketwatch'}
        resp = requests.get('http://185.231.115.223:3000/ViewTickers',headers=head)
        if resp.status_code == 200:
            return pd.DataFrame(json.loads(resp.text))
        else:
            time.sleep(2)
            ct=ct+1
    return ("noData")
def getAlreadyConverted(ID,Type):
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""
        select * from statement."RatioItemsConvertedSheets" where
         firm="""+str(ID)+""" and "Item"='"""+str(Type)+"'", connection)
        return df
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read firm", error)
    finally:
            if(connection):
                cursor.close()
                connection.close()        
def insertValues(df,Type):
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
                    IF NOT EXISTS (select from statement."RatioItems" where "firm"=%(firm)s and "aggregated"=%(aggregated)s and "year"=%(year)s and "quarter"=%(Qperiod)s and "translatedName"=%(Translated)s AND "Yearly"=%(Yearly)s) THEN
                        INSERT INTO statement."RatioItems"(
                        "translatedName", "engName", value, quarter, year, aggregated, firm,"toDate","Yearly")
                        VALUES (%(Translated)s,%(Engitem)s,%(thisPeriod)s,%(Qperiod)s,%(year)s,%(aggregated)s,%(firm)s,%(toDate)s,%(Yearly)s);
                        ELSE UPDATE statement."RatioItems"
                        SET "value"=%(thisPeriod)s
                        WHERE "translatedName"=%(Translated)s AND "quarter"=%(Qperiod)s AND "year"=%(year)s AND "firm"=%(firm)s  AND 
                        "toDate"=%(toDate)s  AND "Yearly"=%(Yearly)s;
                    END IF;
                END
                $$ 
            """
        cursor.executemany(postgres_insert_query_cheif,df.to_dict(orient='records'))
        #######
        if Type=="IS":
            postgres_insert_query_cheif = """
         DO 
            $$
            BEGIN
                IF NOT EXISTS (select from statement."RatioItemsConvertedSheets" where 
                "TracingNo"=%(report_id)s and "Item"='IS' ) THEN
                   INSERT INTO statement."RatioItemsConvertedSheets"(
                    "TracingNo", "Item", firm)
                    VALUES (%(report_id)s, 'IS', %(firm)s);
                END IF;
            END
            $$ 
            """
            cursor.executemany(postgres_insert_query_cheif,df.to_dict(orient='records'))
            connection.commit()
        if Type=="BS":
            postgres_insert_query_cheif = """
            DO 
                $$
                BEGIN
                    IF NOT EXISTS (select from statement."RatioItemsConvertedSheets" where 
                    "TracingNo"=%(report_id)s and "Item"='BS' ) THEN
                    INSERT INTO statement."RatioItemsConvertedSheets"(
                        "TracingNo", "Item", firm)
                        VALUES (%(report_id)s, 'BS', %(firm)s);
                    END IF;
                END
                $$ 

            """
            cursor.executemany(postgres_insert_query_cheif,df.to_dict(orient='records'))
            connection.commit()
        if Type=="CF":
            postgres_insert_query_cheif = """
                DO 
                $$
                BEGIN
                    IF NOT EXISTS (select from statement."RatioItemsConvertedSheets" where 
                    "TracingNo"=%(report_id)s and "Item"='CF' ) THEN
                        INSERT INTO statement."RatioItemsConvertedSheets"(
                        "TracingNo", "Item", firm)
                        VALUES (%(report_id)s, 'CF', %(firm)s);
                    END IF;
                END
                $$ 

            """
            cursor.executemany(postgres_insert_query_cheif,df.to_dict(orient='records'))
            connection.commit()
        print('Values Inserted')
        print(df.report_id.unique().tolist())
    except(Exception, psycopg2.Error) as error:
        print(error)
        if(connection):
            cursor.close()
            connection.close()    
    finally:
        if(connection):
            cursor.close()
            connection.close()    
def getLastCF(identifier):
    ct=0
    while ct<3:
        head = {'Accept-Profile':'statement'}
        resp = requests.get('http://130.185.74.40:3000/rpc/cfall?a='+str(identifier),headers=head)
        if resp.status_code == 200:
            return pd.DataFrame(json.loads(resp.text))
        else:
            time.sleep(2)
            ct=ct+1
    return ("noData")   
if __name__=="__main__":
    DX=getAll()
    start1=time.time() 

    for index,row in DX.iterrows():
        try:
            print('****IS*****')
            # row['ID']=637
            print(row['ID'])
            Conv=getAlreadyConverted(row['ID'],'IS')
            # print()
            DY=getLastIS(row['ID'])
            # print(DY)
            DN=DY[~DY['report_id'].isin(Conv.TracingNo.tolist())]
            DY=DY[['Translated','thisPeriod','lastYear','lastYearThisperiod','period','aggregated','toDate','report_id']]
            if not DN.empty:
                # print('AA')
                DY['firm']=row['ID']
                DY=DY[DY['aggregated']==False]
                DY['Qperiod']=DY['period'].apply(lambda x: QPeriod[str(x)])
                DY.loc[DY['Qperiod']=='Q4','year']=DY['toDate'].apply(lambda x: str(x).split('/')[0])
                DY.sort_values(by=['toDate'],ascending=False,inplace=True)
                DY['year']=DY['year'].fillna(method='ffill')
                lastYear=[x for x in DY[DY['year'].notnull()].year.unique().tolist() if x][0]
                lastYearIn=int(lastYear)+1
                DY.loc[DY['year'].isnull(),'year']=str(lastYearIn)
                DT=DY.copy()
                for index,row in DY.iterrows():
                    X=row['Translated']
                    Q=row['Qperiod']
                    Y=row['year']
                    res=0
                    temp=0
                    res=row['thisPeriod']
                    if X=='سرمایه':
                        DY.loc[index,'CalculatedQuarterly']=res
                    else:
                        if Q=='Q1':
                            DY.loc[index,'CalculatedQuarterly']=res
                        if Q=='Q2':
                            if len(DY.loc[(DY['Qperiod']=='Q1')&(DY['year']==row['year'])&(DY['Translated']==row['Translated'])]['thisPeriod'])!=0:
                                temp=DY.loc[(DY['Qperiod']=='Q1')&(DY['year']==row['year'])&(DY['Translated']==row['Translated'])]['thisPeriod'].tolist()[0]
                                res=row['thisPeriod']
                                DY.loc[index,'CalculatedQuarterly']=res-temp
                            else:
                                DY.loc[index,'CalculatedQuarterly']=res
                        if Q=='Q3':
                            if len(DY.loc[(DY['Qperiod']=='Q2')&(DY['year']==row['year'])&(DY['Translated']==row['Translated'])]['thisPeriod'])!=0:
                                temp=DY.loc[(DY['Qperiod']=='Q2')&(DY['year']==row['year'])&(DY['Translated']==row['Translated'])]['thisPeriod'].tolist()[0]
                                res=row['thisPeriod']
                                DY.loc[index,'CalculatedQuarterly']=res-temp
                            else:
                                DY.loc[index,'CalculatedQuarterly']=res
                        if Q=='Q4':
                            
                            if len(DY.loc[(DY['Qperiod']=='Q3')&(DY['year']==row['year'])&(DY['Translated']==row['Translated'])]['thisPeriod'])!=0:
                                temp=DY.loc[(DY['Qperiod']=='Q3')&(DY['year']==row['year'])&(DY['Translated']==row['Translated'])]['thisPeriod'].tolist()[0]
                                res=row['thisPeriod']
                                DY.loc[index,'CalculatedQuarterly']=res-temp
                            else:
                                DY.loc[index,'CalculatedQuarterly']=res
                DY['Engitem']='ISItems'
                DY.drop(columns=['thisPeriod'],inplace=True)
                DY = DY.rename(columns={'CalculatedQuarterly': 'thisPeriod'})
                Quarterly=DY
                Quarterly.sort_values(by=['year','Qperiod'],ascending=[False,False],inplace=True)
                for tDate in Quarterly.toDate.unique():
                    try:
                        r=(Quarterly[(Quarterly['Translated']=='سود (زیان) خالص')&(Quarterly['toDate']==tDate)]).iloc[0]
                        # r3=(Quarterly[(Quarterly['Translated']=='سود (زیان) خالص هر سهم–ریال')&(Quarterly['toDate']==tDate)]).iloc[0]
                        r4=(Quarterly[(Quarterly['Translated']=='سرمایه')&(Quarterly['toDate']==tDate)]).iloc[0]
                        try:
                            r2=(Quarterly[(Quarterly['Translated']=='سایر درآمدها و هزینه\u200cهای غیرعملیاتی- اقلام متفرقه')&(Quarterly['toDate']==tDate)]).iloc[0]
                            Quarterly=Quarterly.append({'Translated':'EarningWithouNRI','Engitem':'ISItems','thisPeriod':r.thisPeriod-r2.thisPeriod,'Qperiod':r['Qperiod']
                            ,'firm':r['firm'],'toDate':r['toDate'],'report_id':r['report_id'],'aggregated':r['aggregated'],'year':r['year']},ignore_index=True)
                            Quarterly=Quarterly.append({'Translated':'EPSWithoutNRI','Engitem':'ISItems','thisPeriod':(r.thisPeriod-r2.thisPeriod)/r4.thisPeriod*1000,'Qperiod':r['Qperiod']
                            ,'firm':r['firm'],'toDate':r['toDate'],'report_id':r['report_id'],'aggregated':r['aggregated'],'year':r['year']},ignore_index=True)
                        except:
                            Quarterly=Quarterly.append({'Translated':'EarningWithouNRI','Engitem':'ISItems','thisPeriod':r.thisPeriod,'Qperiod':r['Qperiod']
                            ,'firm':r['firm'],'toDate':r['toDate'],'report_id':r['report_id'],'aggregated':r['aggregated'],'year':r['year']},ignore_index=True)
                            Quarterly=Quarterly.append({'Translated':'EPSWithoutNRI','Engitem':'ISItems','thisPeriod':(r.thisPeriod)/r4.thisPeriod*1000,'Qperiod':r['Qperiod']
                            ,'firm':r['firm'],'toDate':r['toDate'],'report_id':r['report_id'],'aggregated':r['aggregated'],'year':r['year']},ignore_index=True)
                    except:
                        print(r)
                        break
                Quarterly.sort_values(by=['year','Qperiod'],ascending=[False,False],inplace=True)
                Quarterly=Quarterly[['Translated','thisPeriod','Qperiod','aggregated','toDate','report_id','firm','Qperiod','year']]
                Quarterly.columns=['Translated','thisPeriod','period','aggregated','toDate','report_id','firm','Qperiod','year']
                Quarterly['Yearly']=False
                Quarterly.reset_index(inplace=True,drop=True)
                DT=DT[DT['period']==12]

                DT2=DT.copy()
                LastYearItem=DT.year.tolist()[0]
                DT2=DT2[DT2['year']==LastYearItem]
                DT2=DT2[['Translated','thisPeriod','period','aggregated','toDate','report_id','firm','Qperiod','year']]
                DT2.columns=['Translated','thisPeriod','period','aggregated','toDate','report_id','firm','Qperiod','year']
                DT['year']=DT['year'].apply(lambda x: str(int(x)-1))
                for index,row in DT.iterrows():
                    if (row['lastYear']==0) & (row['lastYearThisperiod']!=0):
                        DT.loc[index,'lastYear']=row['lastYearThisperiod']
                DT=DT[['Translated','lastYear','period','aggregated','toDate','report_id','firm','Qperiod','year']]
                DT.columns=['Translated','thisPeriod','period','aggregated','toDate','report_id','firm','Qperiod','year']
                for tyear in DT.year.unique():
                    try:
                        r=(DT[(DT['Translated']=='سود (زیان) خالص')&(DT['year']==tyear)]).iloc[0]
                        # r3=(DT[(DT['Translated']=='سود (زیان) خالص هر سهم–ریال')&(DT['toDate']==tDate)]).iloc[0]
                        r4=DT[(DT['Translated']=='سرمایه')&(DT['year']==tyear)].iloc[0]
                        try:
                            r2=(DT[(DT['Translated']=='سایر درآمدها و هزینه\u200cهای غیرعملیاتی- اقلام متفرقه')&(DT['year']==tyear)]).iloc[0]
                            DT=DT.append({'Translated':'EarningWithouNRI','Engitem':'ISItems','thisPeriod':r.thisPeriod-r2.thisPeriod,'Qperiod':r['Qperiod']
                            ,'firm':r['firm'],'toDate':r['toDate'],'report_id':r['report_id'],'aggregated':r['aggregated'],'year':r['year']},ignore_index=True)
                        except:
                            DT=DT.append({'Translated':'EarningWithouNRI','Engitem':'ISItems','thisPeriod':r.thisPeriod,'Qperiod':r['Qperiod']
                            ,'firm':r['firm'],'toDate':r['toDate'],'report_id':r['report_id'],'aggregated':r['aggregated'],'year':r['year']},ignore_index=True)
                        try:
                            r2=(DT[(DT['Translated']=='سایر درآمدها و هزینه\u200cهای غیرعملیاتی- اقلام متفرقه')&(DT['year']==tyear)]).iloc[0]
                            DT=DT.append({'Translated':'EPSWithoutNRI','Engitem':'ISItems','thisPeriod':(r.thisPeriod-r2.thisPeriod)/r4.thisPeriod*1000,'Qperiod':r['Qperiod']
                            ,'firm':r['firm'],'toDate':r['toDate'],'report_id':r['report_id'],'aggregated':r['aggregated'],'year':r['year']},ignore_index=True)
                        except:
                            DT=DT.append({'Translated':'EPSWithoutNRI','Engitem':'ISItems','thisPeriod':(r.thisPeriod)/r4.thisPeriod*1000,'Qperiod':r['Qperiod']
                            ,'firm':r['firm'],'toDate':r['toDate'],'report_id':r['report_id'],'aggregated':r['aggregated'],'year':r['year']},ignore_index=True)
                    except:
                        print(r)
                        break
                for tyear in DT2.year.unique():
                    try:
                        r=(DT2[(DT2['Translated']=='سود (زیان) خالص')&(DT2['year']==tyear)]).iloc[0]
                        # r3=(DT2[(DT2['Translated']=='سود (زیان) خالص هر سهم–ریال')&(DT2['toDate']==tDate)]).iloc[0]
                        r4=(DT2[(DT2['Translated']=='سرمایه')&(DT2['year']==tyear)]).iloc[0]
                        try:
                            r2=(DT2[(DT2['Translated']=='سایر درآمدها و هزینه\u200cهای غیرعملیاتی- اقلام متفرقه')&(DT2['year']==tyear)]).iloc[0]
                            DT2=DT2.append({'Translated':'EarningWithouNRI','Engitem':'ISItems','thisPeriod':r.thisPeriod-r2.thisPeriod,'Qperiod':r['Qperiod']
                            ,'firm':r['firm'],'toDate':r['toDate'],'report_id':r['report_id'],'aggregated':r['aggregated'],'year':r['year']},ignore_index=True)
                            DT2=DT2.append({'Translated':'EPSWithoutNRI','Engitem':'ISItems','thisPeriod':(r.thisPeriod-r2.thisPeriod)/(r4.thisPeriod*1000),'Qperiod':r['Qperiod']
                            ,'firm':r['firm'],'toDate':r['toDate'],'report_id':r['report_id'],'aggregated':r['aggregated'],'year':r['year']},ignore_index=True)
                        except:
                            DT2=DT2.append({'Translated':'EarningWithouNRI','Engitem':'ISItems','thisPeriod':r.thisPeriod,'Qperiod':r['Qperiod']
                            ,'firm':r['firm'],'toDate':r['toDate'],'report_id':r['report_id'],'aggregated':r['aggregated'],'year':r['year']},ignore_index=True)
                            DT2=DT2.append({'Translated':'EPSWithoutNRI','Engitem':'ISItems','thisPeriod':(r.thisPeriod)/(r4.thisPeriod*1000),'Qperiod':r['Qperiod']
                            ,'firm':r['firm'],'toDate':r['toDate'],'report_id':r['report_id'],'aggregated':r['aggregated'],'year':r['year']},ignore_index=True)
                    except:
                        print(r)
                        break
                DT['Yearly']=True
                DT2['Yearly']=True
                DT.reset_index(inplace=True,drop=True)
                DT2.reset_index(inplace=True,drop=True)
                DTAll=DT.append(DT2)
                FinalDF=Quarterly.append(DTAll)
                FinalDF.sort_values(by=['toDate'],ascending=False,inplace=True)
                FinalDF['Engitem']='ISItems'
                insertValues(FinalDF,"IS")
                #######
                # insertValues(DY,"ISCons")
                # insertValues(DY,"ISComp")
                # insertValues(DY,"ISConsComp")
                # print(DY)
            else:
                print('Stock IS '+str(row['ID'])+'...Up to Date')
        except Exception as E:
            print(E)
            continue     
    for index,row in DX.iterrows():
        try:
            print('*****CF****')
            print(row['ID'])
            Conv=getAlreadyConverted(row['ID'],'CF')
            DY=getLastCF(row['ID'])

            DN=DY[~DY['report_id'].isin(Conv.TracingNo.tolist())]
            if not DN.empty:
                DY=DY[['Translated','thisPeriod','LastYear','LastYearThisPeriod','period','Aggregated','toDate','report_id']]
                DY = DY.rename(columns={'Aggregated': 'aggregated'})
                DY = DY.rename(columns={'LastYear': 'lastYear'})
                DY = DY.rename(columns={'LastYearThisPeriod': 'lastYearThisperiod'})
                DY['firm']=row['ID']
                DY=DY[DY['aggregated']==False]
                DY['Qperiod']=DY['period'].apply(lambda x: QPeriod[str(x)])
                DY.loc[DY['Qperiod']=='Q4','year']=DY['toDate'].apply(lambda x: str(x).split('/')[0])
                DY.sort_values(by=['toDate'],ascending=False,inplace=True)
                DY['year']=DY['year'].fillna(method='ffill')
                lastYear=[x for x in DY[DY['year'].notnull()].year.unique().tolist() if x][0]
                lastYearIn=int(lastYear)+1
                DY.loc[DY['year'].isnull(),'year']=str(lastYearIn)
                DT=DY.copy()
                for index,row in DY.iterrows():
                    X=row['Translated']
                    Q=row['Qperiod']
                    Y=row['year']
                    res=0
                    temp=0
                    res=row['thisPeriod']
                    if X=='سرمایه':
                        DY.loc[index,'CalculatedQuarterly']=res
                    else:
                        if Q=='Q1':
                            DY.loc[index,'CalculatedQuarterly']=res
                        if Q=='Q2':
                            if len(DY.loc[(DY['Qperiod']=='Q1')&(DY['year']==row['year'])&(DY['Translated']==row['Translated'])]['thisPeriod'])!=0:
                                temp=DY.loc[(DY['Qperiod']=='Q1')&(DY['year']==row['year'])&(DY['Translated']==row['Translated'])]['thisPeriod'].tolist()[0]
                                res=row['thisPeriod']
                                DY.loc[index,'CalculatedQuarterly']=res-temp
                            else:
                                DY.loc[index,'CalculatedQuarterly']=res
                        if Q=='Q3':
                            if len(DY.loc[(DY['Qperiod']=='Q2')&(DY['year']==row['year'])&(DY['Translated']==row['Translated'])]['thisPeriod'])!=0:
                                temp=DY.loc[(DY['Qperiod']=='Q2')&(DY['year']==row['year'])&(DY['Translated']==row['Translated'])]['thisPeriod'].tolist()[0]
                                res=row['thisPeriod']
                                DY.loc[index,'CalculatedQuarterly']=res-temp
                            else:
                                DY.loc[index,'CalculatedQuarterly']=res
                        if Q=='Q4':
                            
                            if len(DY.loc[(DY['Qperiod']=='Q3')&(DY['year']==row['year'])&(DY['Translated']==row['Translated'])]['thisPeriod'])!=0:
                                temp=DY.loc[(DY['Qperiod']=='Q3')&(DY['year']==row['year'])&(DY['Translated']==row['Translated'])]['thisPeriod'].tolist()[0]
                                res=row['thisPeriod']
                                DY.loc[index,'CalculatedQuarterly']=res-temp
                            else:
                                DY.loc[index,'CalculatedQuarterly']=res
                DY['Engitem']='ISItems'
                DY.drop(columns=['thisPeriod'],inplace=True)
                DY = DY.rename(columns={'CalculatedQuarterly': 'thisPeriod'})
                Quarterly=DY
                Quarterly.sort_values(by=['year','Qperiod'],ascending=[False,False],inplace=True)
                Quarterly=Quarterly[['Translated','thisPeriod','Qperiod','aggregated','toDate','report_id','firm','Qperiod','year']]
                Quarterly.columns=['Translated','thisPeriod','period','aggregated','toDate','report_id','firm','Qperiod','year']
                Quarterly['Yearly']=False
                Quarterly.reset_index(inplace=True,drop=True)
                DT=DT[DT['period']==12]

                DT2=DT.copy()
                LastYearItem=DT.year.tolist()[0]
                DT2=DT2[DT2['year']==LastYearItem]
                DT2=DT2[['Translated','thisPeriod','period','aggregated','toDate','report_id','firm','Qperiod','year']]
                DT2.columns=['Translated','thisPeriod','period','aggregated','toDate','report_id','firm','Qperiod','year']
                DT['year']=DT['year'].apply(lambda x: str(int(x)-1))
                for index,row in DT.iterrows():
                    if (row['lastYear']==0) & (row['lastYearThisperiod']!=0):
                        DT.loc[index,'lastYear']=row['lastYearThisperiod']
                DT=DT[['Translated','lastYear','period','aggregated','toDate','report_id','firm','Qperiod','year']]
                DT.columns=['Translated','thisPeriod','period','aggregated','toDate','report_id','firm','Qperiod','year']
                DT['Yearly']=True
                DT2['Yearly']=True
                DT2.dropna(subset=['thisPeriod'],inplace=True)
                DT.dropna(subset=['thisPeriod'],inplace=True)
                Quarterly.dropna(subset=['thisPeriod'],inplace=True)
                DT.reset_index(inplace=True,drop=True)
                DT2.reset_index(inplace=True,drop=True)
                DTAll=DT.append(DT2)
                FinalDF=Quarterly.append(DTAll)
                FinalDF.sort_values(by=['toDate'],ascending=False,inplace=True)
                FinalDF['Engitem']='CFItems'
                insertValues(FinalDF,'CF')
            print('Stock CF '+str(row['ID'])+'...Up to Date')
        except Exception as E:
            print(E)
            continue    
    for index,row in DX.iterrows():
        try:
            print('*****BS****')
            print(row['ID'])
            Conv=getAlreadyConverted(row['ID'],'BS')
            DY=getLast4BC(row['ID'])
            DN=DY[~DY['report_id'].isin(Conv.TracingNo.tolist())]
            if not DN.empty:
                DY=DY[['Translated','thisPeriod','lastYear','yearBeforeLastyear','period','aggregated','toDate','report_id']]
                DY['firm']=row['ID']
                DY=DY[DY['aggregated']==False]
                DY['Qperiod']=DY['period'].apply(lambda x: QPeriod[str(x)])
                DY.loc[DY['Qperiod']=='Q4','year']=DY['toDate'].apply(lambda x: str(x).split('/')[0])
                DY.sort_values(by=['toDate'],ascending=False,inplace=True)
                DY['year']=DY['year'].fillna(method='ffill')
                lastYear=[x for x in DY[DY['year'].notnull()].year.unique().tolist() if x][0]
                lastYearIn=int(lastYear)+1
                DY.loc[DY['year'].isnull(),'year']=str(lastYearIn)
                DT=DY.copy()
                for index,row in DY.iterrows():
                    X=row['Translated']
                    Q=row['Qperiod']
                    Y=row['year']
                    res=0
                    temp=0
                    res=row['thisPeriod']
                    DY.loc[index,'CalculatedQuarterly']=res
                DY.drop(columns=['thisPeriod'],inplace=True)
                DY = DY.rename(columns={'CalculatedQuarterly': 'thisPeriod'})
                Quarterly=DY
                Quarterly.sort_values(by=['year','Qperiod'],ascending=[False,False],inplace=True)
                Quarterly=Quarterly[['Translated','thisPeriod','Qperiod','aggregated','toDate','report_id','firm','Qperiod','year']]
                Quarterly.columns=['Translated','thisPeriod','period','aggregated','toDate','report_id','firm','Qperiod','year']
                Quarterly['Yearly']=False
                Quarterly.reset_index(inplace=True,drop=True)
                DT=DT[DT['period']==12]

                DT2=DT.copy()
                LastYearItem=DT.year.tolist()[0]
                DT2=DT2[DT2['year']==LastYearItem]
                DT2=DT2[['Translated','thisPeriod','period','aggregated','toDate','report_id','firm','Qperiod','year']]
                DT2.columns=['Translated','thisPeriod','period','aggregated','toDate','report_id','firm','Qperiod','year']
                DT['year']=DT['year'].apply(lambda x: str(int(x)-1))
                # for index,row in DT.iterrows():
                #     if (row['lastYear']==0) & (row['lastYearThisperiod']!=0):
                #         DT.loc[index,'lastYear']=row['lastYearThisperiod']
                DT=DT[['Translated','lastYear','period','aggregated','toDate','report_id','firm','Qperiod','year']]
                DT.columns=['Translated','thisPeriod','period','aggregated','toDate','report_id','firm','Qperiod','year']
                DT['Yearly']=True
                DT2['Yearly']=True
                DT2.dropna(subset=['thisPeriod'],inplace=True)
                DT.dropna(subset=['thisPeriod'],inplace=True)
                Quarterly.dropna(subset=['thisPeriod'],inplace=True)
                DT.reset_index(inplace=True,drop=True)
                DT2.reset_index(inplace=True,drop=True)
                DTAll=DT.append(DT2)
                FinalDF=Quarterly.append(DTAll)
                FinalDF.sort_values(by=['toDate'],ascending=False,inplace=True)
                FinalDF['Engitem']='BSItems'
                FinalDF.dropna(subset=['Translated'],inplace=True)
                insertValues(FinalDF,"BS")
            print('Stock BS '+str(row['ID'])+'...Up to Date')
        except Exception as E:
            print(E)
            continue
    print('All took', time.time()-start1, 'seconds.To Add Pre Ratios')