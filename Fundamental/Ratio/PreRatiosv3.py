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
import traceback
db_username="Jay"
db_pass="Mehrad1"
db_host="127.0.0.1"
db_port="5432"
db_database="FinWisev12"
#pd.set_option('display.max_rows', 500)
QPeriod={'3':'Q1','6':'Q2','9':'Q3','12':'Q4'}
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
        connection = psycopg2.connect(user=db_username,password=db_pass,host=db_host,port=db_port,database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql('select * from statement."RatioItemsConvertedSheets" where firm='+str(ID)+'and "Item"=\''+str(Type)+'\'', connection)
    except (Exception, psycopg2.Error) as error :
        if(connection):
            print("Failed to read firm", error)
    finally:
        if(connection):
            cursor.close()
            connection.close()
    return df
                
def insertValues(df,Type,reports):
    try:
        #######
        connection = psycopg2.connect(user=db_username,password=db_pass,host=db_host,port=db_port,database=db_database)
        cursor = connection.cursor()
        postgres_insert_query_cheif = """
            DO 
                $$
                BEGIN
                    IF NOT EXISTS (select from statement."RatioItems" where "firm"=%(firm)s and "aggregated"=%(aggregated)s and "year"=%(year)s and "quarter"=%(Qperiod)s and "translatedName"=%(Translated)s AND "Yearly"=%(Yearly)s) THEN
                        INSERT INTO statement."RatioItems"(
                        "translatedName", "engName", value, quarter, year, aggregated, firm,"toDate","Yearly")
                        VALUES (%(Translated)s,%(Engitem)s,%(val)s,%(Qperiod)s,%(year)s,%(aggregated)s,%(firm)s,%(toDate)s,%(Yearly)s);
                        ELSE UPDATE statement."RatioItems"
                        SET "value"=%(val)s
                        WHERE "translatedName"=%(Translated)s AND "quarter"=%(Qperiod)s AND "year"=%(year)s AND "firm"=%(firm)s  AND 
                        "toDate"=%(toDate)s  AND "Yearly"=%(Yearly)s;
                    END IF;
                END
                $$ 
            """
        cursor.executemany(postgres_insert_query_cheif,df.to_dict(orient='records'))
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
            cursor.executemany(postgres_insert_query_cheif,reports.to_dict(orient='records'))
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
            cursor.executemany(postgres_insert_query_cheif,reports.to_dict(orient='records'))
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
            cursor.executemany(postgres_insert_query_cheif,reports.to_dict(orient='records'))
            connection.commit()
        connection.commit()
        print('Values Inserted')
        print(df.report_id.unique())
    except(Exception, psycopg2.Error) as error:
        print(error)
        if(connection):
            cursor.close()
            connection.close()    
    finally:
        if(connection):
            cursor.close()
            connection.close()    

if __name__=="__main__":
    DX=getAll()
    # DX=DX[DX['ID']==298]
    start1=time.time() 

    for index,row in DX.iterrows():
        try:
            print('****IS*****')
            print(row['ID'])
            Conv=getAlreadyConverted(row['ID'],'IS')
            # print(Conv)
            DY=getLastIS(row['ID'])
            DN=DY[~DY['report_id'].isin(Conv.TracingNo.tolist())]
            # print(DN)
            if not DN.empty:
                DY=DY[['Translated','thisPeriod','lastYear','lastYearThisperiod','period','aggregated','toDate','report_id']]
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
                reports=DY[['report_id','firm']]
                ###
                Final=pd.DataFrame()
                yearsSituation={}
                for year in DY.year.unique().tolist():
                    yearsSituation[year]=DY[DY['year']==year].Qperiod.unique()
                for key, value in yearsSituation.items():
                    # print(key)
                    # print(value)
                    if int(key)==int(lastYearIn):
                        if len(value)==0:
                            continue
                        #
                        if ('Q1' in value) and ('Q2' not in value) and ('Q3' not in value) and ('Q4' not in value):
                            # X=='سرمایه'
                            # print('Only Q1 for Recent Year')
                            Scenario1DF1=DY[(DY['year']==key)&(DY['Qperiod']=='Q1')]
                            TempDF1=Scenario1DF1[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1.loc[:,'Yearly']=False
                            Final=Final.append(TempDF1)
                            TempDF1=Scenario1DF1[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            TempDF1=Scenario1DF1[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=False
                                Final=Final.append(TempDF1)
                        #
                        if ('Q1' not in value) and ('Q2' in value) and ('Q3' not in value) and ('Q4' not in value):
                            # print('Only Q2')
                            # X=='سرمایه'
                            TempDF_created=[]
                            Scenario2=DY[(DY['year']==key)&(DY['Qperiod']=='Q2')]
                            TempDF1=Scenario2[['Translated', 'thisPeriod','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1.loc[:,'Yearly']=False
                            for index,row in TempDF1.iterrows():
                                if row['Translated']!='سرمایه':
                                    newVal=row['val']/2
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q1', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q2', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q1', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q2', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario2[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            TempDF1=Scenario2[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=False
                                TempDF_created=[]
                                for index,row in TempDF1.iterrows():
                                    if row['Translated']!='سرمایه':
                                        newVal=row['val']/2
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q1', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q2', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q1', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q2', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                        #
                        if ('Q1' not in value) and ('Q2' not in value) and ('Q3' in value) and ('Q4' not in value):
                            # print('Only Q3')
                            Scenario3=DY[(DY['year']==key)&(DY['Qperiod']=='Q3')]
                            TempDF_created=[]
                            TempDF1=Scenario3[['Translated', 'thisPeriod','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1.loc[:,'Yearly']=False
                            for index,row in TempDF1.iterrows():
                                if row['Translated']!='سرمایه':
                                    newVal=row['val']/3
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q1', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q2', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q3', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q1', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q2', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q3', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario3[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            TempDF1=Scenario3[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=False
                                TempDF_created=[]
                                for index,row in TempDF1.iterrows():
                                    if row['Translated']!='سرمایه':
                                        newVal=row['val']/3
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q1', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q3', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q1', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q3', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                        #
                        if ('Q1' in value) and ('Q2' in value) and ('Q3' not in value) and ('Q4' not in value):
                            # print('Q1 va Q2')
                            Scenario4=DY[(DY['year']==key)&(DY['Qperiod']=='Q1')]
                            TempDF1Q=Scenario4[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1Q.loc[:,'Yearly']=False
                            Final=Final.append(TempDF1Q)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            TempDF1Q2=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1Q2.val.unique().tolist())>5:
                                TempDF1Q2.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1Q2.loc[:,'Yearly']=False
                                Final=Final.append(TempDF1Q2)
                            
                            ####Q1 End
                            ####Q2 Start
                            Scenario5=DY[(DY['year']==key)&(DY['Qperiod']=='Q2')]
                            TempDF2=Scenario5[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(TempDF1Q[TempDF1Q['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه':
                                    Q1Val=TempDF1Q[TempDF1Q['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    # print(Q2Val)
                                    TempDF_created.append({'Translated':row['Translated'], 'val':Q2Val-Q1Val , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            # print(Temp_CreatedDF2)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            ###
                            TempDF1=Scenario5[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            TempDF1=Scenario5[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=False
                                TempDF_created=[]
                                for index,row in TempDF1.iterrows():
                                    if len(TempDF1Q2[TempDF1Q2['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                                        LastYearQ1Val=TempDF1Q2[TempDF1Q2['Translated']==row['Translated']].val.values.tolist()[0]
                                        LastYearQ2Val=row['val']
                                        TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val-LastYearQ1Val , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q2', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                        #
                        if ('Q1' in value) and ('Q2' not in value) and ('Q3' in value) and ('Q4' not in value):
                            # print('Q1 va Q3')
                            Scenario6=DY[(DY['year']==key)&(DY['Qperiod']=='Q1')]
                            TempDF1Q=Scenario6[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1Q.loc[:,'Yearly']=False
                            Final=Final.append(TempDF1Q)
                            TempDF1=Scenario6[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            TempDF1Q2=Scenario6[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1Q2.val.unique().tolist())>5:
                                TempDF1Q2.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1Q2.loc[:,'Yearly']=False
                                Final=Final.append(TempDF1Q2)
                            ####Q1 End
                            #### Q3 Start
                            Scenario8=DY[(DY['year']==key)&(DY['Qperiod']=='Q3')]
                            TempDF2=Scenario8[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(TempDF1Q[TempDF1Q['Translated']==row['Translated']])>0:
                                    if row['Translated']!='سرمایه':
                                        Q1Val=TempDF1Q[TempDF1Q['Translated']==row['Translated']].val.values.tolist()[0]
                                        Q3Val=row['val']
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(Q3Val-Q1Val)/2 , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(Q3Val-Q1Val)/2 , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                else:
                                    if row['Translated']=='سرمایه':
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                    
                            # TempDF2.loc[:,'Yearly']=False
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            # Final=Final.append(TempDF2)
                            TempDF1=Scenario8[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            TempDF1=Scenario8[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=False
                                TempDF_created=[]
                                for index,row in TempDF1.iterrows():
                                    if len(TempDF1Q2[TempDF1Q2['Translated']==row['Translated']])>0:
                                        if row['Translated']!='سرمایه':
                                            LastYearQ1Val=TempDF1Q2[TempDF1Q2['Translated']==row['Translated']].val.values.tolist()[0]
                                            LastYearQ3Val=row['val']
                                            TempDF_created.append({'Translated':row['Translated'], 'val':(LastYearQ3Val-LastYearQ1Val)/2 , 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q2', 'year':row['year']})
                                            TempDF_created.append({'Translated':row['Translated'], 'val':(LastYearQ3Val-LastYearQ1Val)/2 , 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q3', 'year':row['year']})
                                        else:
                                            TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q2', 'year':row['year']})
                                            TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q3', 'year':row['year']})
                                    else:
                                        if row['Translated']=='سرمایه':
                                            TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                            TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                        else:
                                            TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                        #
                        if ('Q1' not in value) and ('Q2' in value) and ('Q3' in value) and ('Q4' not in value):
                            # print('Q2 va Q3')
                            TempDF_created=[]
                            Scenario9=DY[(DY['year']==key)&(DY['Qperiod']=='Q2')]
                            TempDF1=Scenario9[['Translated', 'thisPeriod','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1.loc[:,'Yearly']=False
                            TempDF2QThisyear=TempDF1.copy()
                            for index,row in TempDF1.iterrows():
                                if row['Translated']!='سرمایه':
                                    newVal=row['val']/2
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q1', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q2', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q2', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario9[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            TempDF1=Scenario9[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF2QLastyear=TempDF1.copy()
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=False
                                TempDF_created=[]
                                for index,row in TempDF1.iterrows():
                                    if row['Translated']!='سرمایه':
                                        newVal=row['val']/2
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q1', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q2', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q1', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q2', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                            ####Q2 End
                            #### Q3 Start
                            Scenario10=DY[(DY['year']==key)&(DY['Qperiod']=='Q3')]
                            TempDF2=Scenario10[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(TempDF2QThisyear[TempDF2QThisyear['Translated']==row['Translated']])>0:
                                    if row['Translated']!='سرمایه':
                                        Q2Val=TempDF2QThisyear[TempDF2QThisyear['Translated']==row['Translated']].val.values.tolist()[0]
                                        Q3Val=row['val']
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(Q3Val-Q1Val), 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'], 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                            # TempDF2.loc[:,'Yearly']=False
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            # Final=Final.append(TempDF2)
                            TempDF1=Scenario10[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            TempDF1=Scenario10[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=False
                                TempDF_created=[]
                                for index,row in TempDF1.iterrows():
                                    if len(TempDF2QLastyear[TempDF2QLastyear['Translated']==row['Translated']])>0:
                                        if row['Translated']!='سرمایه':
                                            LastYearQ2Val=TempDF2QLastyear[TempDF2QLastyear['Translated']==row['Translated']].val.values.tolist()[0]
                                            LastYearQ3Val=row['val']
                                            TempDF_created.append({'Translated':row['Translated'], 'val':(LastYearQ3Val-LastYearQ2Val) , 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q3', 'year':row['year']})
                                        else:
                                            TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q3', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                        #
                        if ('Q1' in value) and ('Q2' in value) and ('Q3' in value) and ('Q4' not in value):
                            # print('Q1 va Q2 va Q3')
                            Scenario11=DY[(DY['year']==key)&(DY['Qperiod']=='Q1')]
                            TempDF1Q=Scenario11[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1Q.loc[:,'Yearly']=False
                            Final=Final.append(TempDF1Q)
                            TempDF1=Scenario11[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            TempDF1Q2=Scenario11[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1Q2.val.unique().tolist())>5:
                                TempDF1Q2.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1Q2.loc[:,'Yearly']=False
                                Final=Final.append(TempDF1Q2)
                            ####
                            Scenaro12=DY[(DY['year']==key)&(DY['Qperiod']=='Q2')]
                            TempDF2Q=Scenaro12[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2Q.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF2Q.loc[:,'Yearly']=False
                            TempDF_created=[]
                            for index,row in TempDF2Q.iterrows():
                                if len(TempDF1Q[TempDF1Q['Translated']==row['Translated']])>0:
                                    if row['Translated']!='سرمایه':
                                        Q1Val=TempDF1Q[TempDF1Q['Translated']==row['Translated']].val.values.tolist()[0]
                                        Q2Val=row['val']
                                        TempDF_created.append({'Translated':row['Translated'], 'val':Q2Val-Q1Val , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenaro12[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            TempDF2Q2LastYearthisPeriod=Scenaro12[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2Q2LastYearthisPeriod.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF2Q2LastYearthisPeriod.val.unique().tolist())>5:
                                TempDF2Q2LastYearthisPeriod.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF2Q2LastYearthisPeriod.loc[:,'Yearly']=False
                                TempDF_created=[]
                                for index,row in TempDF2Q2LastYearthisPeriod.iterrows():
                                    if len(TempDF1Q2[TempDF1Q2['Translated']==row['Translated']])>0:
                                        if row['Translated']!='سرمایه':
                                            LastYearQ1Val=TempDF1Q2[TempDF1Q2['Translated']==row['Translated']].val.values.tolist()[0]
                                            LastYearQ2Val=row['val']
                                            TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val-LastYearQ1Val , 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q2', 'year':row['year']})
                                        else:
                                            TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q2', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                            ###
                            Scenario14=DY[(DY['year']==key)&(DY['Qperiod']=='Q3')]
                            TempDF1=Scenario14[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1.loc[:,'Yearly']=False
                            TempDF_created=[]
                            for index,row in TempDF1.iterrows():
                                if len(TempDF2Q[TempDF2Q['Translated']==row['Translated']])>0:
                                    if row['Translated']!='سرمایه':
                                        Q2Val=TempDF2Q[TempDF2Q['Translated']==row['Translated']].val.values.tolist()[0]
                                        Q3Val=row['val']
                                        TempDF_created.append({'Translated':row['Translated'], 'val':Q3Val-Q2Val , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                            # TempDF2.loc[:,'Yearly']=False
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario14[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            TempDF2Q3LastYearthisPeriod=Scenario14[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2Q3LastYearthisPeriod.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF2Q3LastYearthisPeriod.val.unique().tolist())>5:
                                TempDF2Q3LastYearthisPeriod.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF2Q3LastYearthisPeriod.loc[:,'Yearly']=False
                                TempDF_created=[]
                                for index,row in TempDF2Q3LastYearthisPeriod.iterrows():
                                    if len(TempDF2Q2LastYearthisPeriod[TempDF2Q2LastYearthisPeriod['Translated']==row['Translated']])>0:
                                        if row['Translated']!='سرمایه':
                                            LastYearQ2Val=TempDF2Q2LastYearthisPeriod[TempDF2Q2LastYearthisPeriod['Translated']==row['Translated']].val.values.tolist()[0]
                                            LastYearQ3Val=row['val']
                                            TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ3Val-LastYearQ2Val , 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q3', 'year':row['year']})
                                        else:
                                            TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q3', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                    else:
                        if ('Q1' in value) and ('Q2' in value) and ('Q3' in value) and ('Q4' in value):
                            # print('PerfectYear')
                            Scenario4=DY[(DY['year']==key)&(DY['Qperiod']=='Q1')]
                            TempDF1Q=Scenario4[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1Q.loc[:,'Yearly']=False
                            Final=Final.append(TempDF1Q)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            TempDF1Q2=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1Q2.val.unique().tolist())>5:
                                TempDF1Q2.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1Q2.loc[:,'Yearly']=False
                                Final=Final.append(TempDF1Q2)
                            
                            #
                            Scenario5=DY[(DY['year']==key)&(DY['Qperiod']=='Q2')]
                            TempDF2=Scenario5[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            Q2Temp=TempDF2.copy()
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(TempDF1Q[TempDF1Q['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه':
                                    Q1Val=TempDF1Q[TempDF1Q['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    # print(Q2Val)
                                    TempDF_created.append({'Translated':row['Translated'], 'val':Q2Val-Q1Val , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            LastYear6Month=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            LastYear6Month.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(LastYear6Month.val.unique().tolist())>5:
                                LastYear6Month.loc[:,'year']=str(int(lastYearIn)-1)
                                LastYear6Month.loc[:,'Yearly']=False
                                Final=Final.append(TempDF1Q2)
                                TempDF_created=[]
                                for index,row in LastYear6Month.iterrows():
                                    if len(TempDF1Q2[TempDF1Q2['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                                        LastYearQ1Val=TempDF1Q2[TempDF1Q2['Translated']==row['Translated']].val.values.tolist()[0]
                                        LastYearQ2Val=row['val']
                                        TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val-LastYearQ1Val , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q2', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                            #
                            Scenario6=DY[(DY['year']==key)&(DY['Qperiod']=='Q3')]
                            TempDF2=Scenario6[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            Q3Temp=TempDF2.copy()
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(Q2Temp[Q2Temp['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه':
                                    Q1Val=Q2Temp[Q2Temp['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    # print(Q2Val)
                                    TempDF_created.append({'Translated':row['Translated'], 'val':Q2Val-Q1Val , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            LastYearQ3=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            LastYearQ3.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(LastYearQ3.val.unique().tolist())>5:
                                LastYearQ3.loc[:,'year']=str(int(lastYearIn)-1)
                                LastYearQ3.loc[:,'Yearly']=False
                                Final=Final.append(TempDF1Q2)
                                TempDF_created=[]
                                for index,row in LastYearQ3.iterrows():
                                    if len(LastYear6Month[LastYear6Month['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                                        LastYearQ1Val=LastYear6Month[LastYear6Month['Translated']==row['Translated']].val.values.tolist()[0]
                                        LastYearQ2Val=row['val']
                                        TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val-LastYearQ1Val , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q3', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                            #
                            Scenario2=DY[(DY['year']==key)&(DY['Qperiod']=='Q4')]
                            TempDF2=Scenario6[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            Q4Temp=TempDF2.copy()
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(Q3Temp[Q3Temp['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه':
                                    Q1Val=Q3Temp[Q3Temp['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    # print(Q2Val)
                                    TempDF_created.append({'Translated':row['Translated'], 'val':Q2Val-Q1Val , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            LastYearQ4=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            LastYearQ4.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(LastYearQ4.val.unique().tolist())>5:
                                LastYearQ4.loc[:,'year']=str(int(lastYearIn)-1)
                                LastYearQ4.loc[:,'Yearly']=False
                                Final=Final.append(TempDF1Q2)
                                TempDF_created=[]
                                for index,row in LastYearQ4.iterrows():
                                    if len(LastYearQ3[LastYearQ3['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                                        LastYearQ1Val=LastYearQ3[LastYearQ3['Translated']==row['Translated']].val.values.tolist()[0]
                                        LastYearQ2Val=row['val']
                                        TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val-LastYearQ1Val , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q4', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                        #
                        if ('Q1' in value) and ('Q2' not in value) and ('Q3' not in value) and ('Q4' in value):
                            # print('no Q2 and Q3')
                            Scenario4=DY[(DY['year']==key)&(DY['Qperiod']=='Q1')]
                            TempDF1Q=Scenario4[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1Q.loc[:,'Yearly']=False
                            Final=Final.append(TempDF1Q)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            TempDF1Q2=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1Q2.val.unique().tolist())>5:
                                TempDF1Q2.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1Q2.loc[:,'Yearly']=False
                                Final=Final.append(TempDF1Q2)
                            
                            #
                            Scenario5=DY[(DY['year']==key)&(DY['Qperiod']=='Q4')]
                            TempDF2=Scenario5[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(TempDF1Q[TempDF1Q['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه':
                                    Q1Val=TempDF1Q[TempDF1Q['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    newVal=(Q2Val-Q1Val)/3
                                    # print(Q2Val)
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            LastYear12Month=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            LastYear12Month.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(LastYear12Month.val.unique().tolist())>5:
                                LastYear12Month.loc[:,'year']=str(int(lastYearIn)-1)
                                LastYear12Month.loc[:,'Yearly']=False
                                Final=Final.append(TempDF1Q2)
                                TempDF_created=[]
                                for index,row in LastYear12Month.iterrows():
                                    if len(TempDF1Q2[TempDF1Q2['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                                        LastYearQ1Val=TempDF1Q2[TempDF1Q2['Translated']==row['Translated']].val.values.tolist()[0]
                                        LastYearQ2Val=row['val']
                                        newVal=(LastYearQ2Val-LastYearQ1Val )/3
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal, 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal, 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q3', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal, 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q4', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                            #
                        #
                        if ('Q1' not in value) and ('Q2' in value) and ('Q3' not in value) and ('Q4' in value):
                            # print('no Q1 and Q3')
                            Scenario4=DY[(DY['year']==key)&(DY['Qperiod']=='Q2')]
                            TempDF1Q=Scenario4[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1Q.loc[:,'Yearly']=False
                            TempDF_created=[]
                            for index,row in TempDF1Q.iterrows():
                                if row['Translated']!='سرمایه':
                                    newVal=row['val']/2
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q1', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q2', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q1', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q2', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            # Final=Final.append(TempDF1Q)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            LastYear6Month=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            LastYear6Month.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(LastYear6Month.val.unique().tolist())>5:
                                LastYear6Month.loc[:,'year']=str(int(lastYearIn)-1)
                                LastYear6Month.loc[:,'Yearly']=False
                                TempDF_created=[]
                                for index,row in LastYear6Month.iterrows():
                                    if len(TempDF1Q2[TempDF1Q2['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                                        # LastYearQ1Val=TempDF1Q2[TempDF1Q2['Translated']==row['Translated']].val.values.tolist()[0]
                                        LastYearQ2Val=row['val']
                                        newVal=(LastYearQ2Val)/2
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal, 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q1', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal, 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q2', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q1', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)


                            #
                            Scenario5=DY[(DY['year']==key)&(DY['Qperiod']=='Q4')]
                            TempDF2=Scenario5[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(TempDF1Q[TempDF1Q['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه':
                                    Q1Val=TempDF1Q[TempDF1Q['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    newVal=(Q2Val-Q1Val)/2
                                    # print(Q2Val)
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            LastYear12Month=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            LastYear12Month.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(LastYear12Month.val.unique().tolist())>5:
                                LastYear12Month.loc[:,'year']=str(int(lastYearIn)-1)
                                LastYear12Month.loc[:,'Yearly']=False
                                TempDF_created=[]
                                for index,row in LastYear12Month.iterrows():
                                    if len(LastYear6Month[LastYear6Month['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                                        LastYearQ1Val=LastYear6Month[LastYear6Month['Translated']==row['Translated']].val.values.tolist()[0]
                                        LastYearQ2Val=row['val']
                                        newVal=(LastYearQ2Val-LastYearQ1Val )/2
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal, 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q3', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal, 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q4', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                        #
                        if ('Q1' not in value) and ('Q2' not in value) and ('Q3' in value) and ('Q4' in value):
                            # print('no Q2 and Q1')
                            Scenario4=DY[(DY['year']==key)&(DY['Qperiod']=='Q3')]
                            TempDF1Q=Scenario4[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1Q.loc[:,'Yearly']=False
                            TempDF_created=[]
                            for index,row in TempDF1Q.iterrows():
                                if row['Translated']!='سرمایه':
                                    newVal=row['val']/3
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q1', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q2', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q3', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q1', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q2', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q3', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            # Final=Final.append(TempDF1Q)
                            LastYear9Month=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            LastYear9Month.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(LastYear6Month.val.unique().tolist())>5:
                                LastYear6Month.loc[:,'year']=str(int(lastYearIn)-1)
                                LastYear6Month.loc[:,'Yearly']=False
                                TempDF_created=[]
                                for index,row in LastYear6Month.iterrows():
                                    if len(TempDF1Q2[TempDF1Q2['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                                        # LastYearQ1Val=TempDF1Q2[TempDF1Q2['Translated']==row['Translated']].val.values.tolist()[0]
                                        LastYearQ2Val=row['val']
                                        newVal=(LastYearQ2Val)/3
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal, 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q1', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal, 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal, 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q3', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q1', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)


                            #
                            Scenario5=DY[(DY['year']==key)&(DY['Qperiod']=='Q4')]
                            TempDF2=Scenario5[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(TempDF1Q[TempDF1Q['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه':
                                    Q1Val=TempDF1Q[TempDF1Q['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    newVal=(Q2Val-Q1Val)
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario5[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            LastYear12Month=Scenario5[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            LastYear12Month.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(LastYear12Month.val.unique().tolist())>5:
                                LastYear12Month.loc[:,'year']=str(int(lastYearIn)-1)
                                LastYear12Month.loc[:,'Yearly']=False
                                TempDF_created=[]
                                for index,row in LastYear12Month.iterrows():
                                    if len(LastYear6Month[LastYear6Month['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                                        LastYearQ1Val=LastYear6Month[LastYear6Month['Translated']==row['Translated']].val.values.tolist()[0]
                                        LastYearQ2Val=row['val']
                                        newVal=(LastYearQ2Val-LastYearQ1Val )/3
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal, 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q4', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                        #
                        if ('Q1' in value) and ('Q2' not in value) and ('Q3' in value) and ('Q4' in value):
                            # print('no Q2')
                            Scenario4=DY[(DY['year']==key)&(DY['Qperiod']=='Q1')]
                            TempDF1Q=Scenario4[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1Q.loc[:,'Yearly']=False
                            Final=Final.append(TempDF1Q)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            TempDF1Q2=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1Q2.val.unique().tolist())>5:
                                TempDF1Q2.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1Q2.loc[:,'Yearly']=False
                                Final=Final.append(TempDF1Q2)
                            
                            #
                            Scenario6=DY[(DY['year']==key)&(DY['Qperiod']=='Q3')]
                            TempDF2=Scenario6[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            Q3Temp=TempDF2.copy()
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(TempDF1Q[TempDF1Q['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه':
                                    Q1Val=TempDF1Q[TempDF1Q['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    if row['Translated']!='سرمایه' :
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val-Q1Val)/2, 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val-Q1Val)/2 , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':Q2Val, 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':Q2Val , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                else:
                                    if row['Translated']!='سرمایه' :
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'], 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            LastYearQ3=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            LastYearQ3.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(LastYearQ3.val.unique().tolist())>5:
                                LastYearQ3.loc[:,'year']=str(int(lastYearIn)-1)
                                LastYearQ3.loc[:,'Yearly']=False
                                # Final=Final.append(TempDF1Q2)
                                TempDF_created=[]
                                for index,row in LastYearQ3.iterrows():
                                    if len(TempDF1Q2[TempDF1Q2['Translated']==row['Translated']])>0 :
                                        LastYearQ1Val=TempDF1Q2[TempDF1Q2['Translated']==row['Translated']].val.values.tolist()[0]
                                        LastYearQ2Val=row['val']
                                        if row['Translated']!='سرمایه' :
                                            TempDF_created.append({'Translated':row['Translated'], 'val':(LastYearQ2Val-LastYearQ1Val)/2 , 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q3', 'year':row['year']})
                                            TempDF_created.append({'Translated':row['Translated'], 'val':(LastYearQ2Val-LastYearQ1Val)/2 , 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q2', 'year':row['year']})
                                        else:
                                            TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val , 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q3', 'year':row['year']})
                                            TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val, 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q2', 'year':row['year']})
                                    else:
                                        if row['Translated']!='سرمایه' :
                                            TempDF_created.append({'Translated':row['Translated'], 'val':row['val']/2, 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                            TempDF_created.append({'Translated':row['Translated'], 'val':row['val']/2 , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                                        else:
                                            TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q3', 'year':row['year']})
                                            TempDF_created.append({'Translated':row['Translated'], 'val':row['val'], 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q2', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                            #
                            Scenario2=DY[(DY['year']==key)&(DY['Qperiod']=='Q4')]
                            TempDF2=Scenario6[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            Q4Temp=TempDF2.copy()
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(Q3Temp[Q3Temp['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه':
                                    Q1Val=Q3Temp[Q3Temp['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    # print(Q2Val)
                                    TempDF_created.append({'Translated':row['Translated'], 'val':Q2Val-Q1Val , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            LastYearQ4=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            LastYearQ4.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(LastYearQ4.val.unique().tolist())>5:
                                LastYearQ4.loc[:,'year']=str(int(lastYearIn)-1)
                                LastYearQ4.loc[:,'Yearly']=False
                                Final=Final.append(TempDF1Q2)
                                TempDF_created=[]
                                for index,row in LastYearQ4.iterrows():
                                    if len(LastYearQ3[LastYearQ3['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                                        LastYearQ1Val=LastYearQ3[LastYearQ3['Translated']==row['Translated']].val.values.tolist()[0]
                                        LastYearQ2Val=row['val']
                                        TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val-LastYearQ1Val , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q4', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                        #
                        if ('Q1' in value) and ('Q2' in value) and ('Q3' not in value) and ('Q4' in value):
                            # print('no Q3')
                            Scenario4=DY[(DY['year']==key)&(DY['Qperiod']=='Q1')]
                            TempDF1Q=Scenario4[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1Q.loc[:,'Yearly']=False
                            Final=Final.append(TempDF1Q)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            TempDF1Q2=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1Q2.val.unique().tolist())>5:
                                TempDF1Q2.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1Q2.loc[:,'Yearly']=False
                                Final=Final.append(TempDF1Q2)
                            
                            #
                            Scenario6=DY[(DY['year']==key)&(DY['Qperiod']=='Q2')]
                            TempDF2=Scenario6[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            Q3Temp=TempDF2.copy()
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(TempDF1Q[TempDF1Q['Translated']==row['Translated']])>0:
                                    Q1Val=TempDF1Q[TempDF1Q['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    if row['Translated']!='سرمایه' :
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val-Q1Val), 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':Q2Val, 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                else:
                                    if row['Translated']!='سرمایه' :
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'], 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            LastYearQ3=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            LastYearQ3.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(LastYearQ3.val.unique().tolist())>5:
                                LastYearQ3.loc[:,'year']=str(int(lastYearIn)-1)
                                LastYearQ3.loc[:,'Yearly']=False
                                # Final=Final.append(TempDF1Q2)
                                TempDF_created=[]
                                for index,row in LastYearQ3.iterrows():
                                    if len(TempDF1Q2[TempDF1Q2['Translated']==row['Translated']])>0 :

                                        LastYearQ1Val=TempDF1Q2[TempDF1Q2['Translated']==row['Translated']].val.values.tolist()[0]
                                        LastYearQ2Val=row['val']
                                        if row['Translated']!='سرمایه' :
                                            TempDF_created.append({'Translated':row['Translated'], 'val':(LastYearQ2Val-LastYearQ1Val) , 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q2', 'year':row['year']})
                                        else:
                                            TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val, 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q2', 'year':row['year']})
                                    else:
                                        
                                        if row['Translated']!='سرمایه' :
                                            TempDF_created.append({'Translated':row['Translated'], 'val':row['val'], 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                                        else:
                                            TempDF_created.append({'Translated':row['Translated'], 'val':row['val'], 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q2', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                            #
                            Scenario2=DY[(DY['year']==key)&(DY['Qperiod']=='Q4')]
                            TempDF2=Scenario6[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            Q4Temp=TempDF2.copy()
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(Q3Temp[Q3Temp['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه':
                                    Q1Val=Q3Temp[Q3Temp['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    # print(Q2Val)
                                    if row['Translated']!='سرمایه' :
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val-Q1Val)/2 , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val-Q1Val)/2 , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q4', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val) , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val), 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q4', 'year':row['year']})
                                else:
                                    if row['Translated']!='سرمایه' :
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val']/2 , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val']/2 , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q4', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q4', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            LastYearQ4=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            LastYearQ4.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(LastYearQ4.val.unique().tolist())>5:
                                LastYearQ4.loc[:,'year']=str(int(lastYearIn)-1)
                                LastYearQ4.loc[:,'Yearly']=False
                                Final=Final.append(TempDF1Q2)
                                TempDF_created=[]
                                for index,row in LastYearQ4.iterrows():
                                    if len(LastYearQ3[LastYearQ3['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                                        LastYearQ1Val=LastYearQ3[LastYearQ3['Translated']==row['Translated']].val.values.tolist()[0]
                                        LastYearQ2Val=row['val']
                                        TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val-LastYearQ1Val , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q4', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                        #
                        if ('Q1' not in value) and ('Q2' in value) and ('Q3' in value) and ('Q4' in value):
                            # print('no Q1')
                            Scenario6=DY[(DY['year']==key)&(DY['Qperiod']=='Q2')]
                            TempDF2=Scenario6[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            Q3Temp=TempDF2.copy()
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if row['Translated']!='سرمایه':
                                    Q2Val=row['val']
                                    if row['Translated']!='سرمایه' :
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val)/2, 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q1', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val)/2, 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':Q2Val, 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':Q2Val, 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q1', 'year':row['year']})

                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            LastYearQ3=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            LastYearQ3.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(LastYearQ3.val.unique().tolist())>5:
                                LastYearQ3.loc[:,'year']=str(int(lastYearIn)-1)
                                LastYearQ3.loc[:,'Yearly']=False
                                # Final=Final.append(TempDF1Q2)
                                TempDF_created=[]
                                for index,row in LastYearQ3.iterrows():
                                    LastYearQ2Val=row['val']
                                    if row['Translated']!='سرمایه' :
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(LastYearQ2Val)/2 , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(LastYearQ2Val)/2 , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q2', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val, 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val, 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q2', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                            #
                            Scenario2=DY[(DY['year']==key)&(DY['Qperiod']=='Q3')]
                            TempDF2=Scenario6[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            Q4Temp=TempDF2.copy()
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(Q3Temp[Q3Temp['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه':
                                    Q1Val=Q3Temp[Q3Temp['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    # print(Q2Val)
                                    TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val-Q1Val) , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val) , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            LastYearQ4=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            LastYearQ4.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(LastYearQ4.val.unique().tolist())>5:
                                LastYearQ4.loc[:,'year']=str(int(lastYearIn)-1)
                                LastYearQ4.loc[:,'Yearly']=False
                                Final=Final.append(TempDF1Q2)
                                TempDF_created=[]
                                for index,row in LastYearQ4.iterrows():
                                    if len(LastYearQ3[LastYearQ3['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                                        LastYearQ1Val=LastYearQ3[LastYearQ3['Translated']==row['Translated']].val.values.tolist()[0]
                                        LastYearQ2Val=row['val']
                                        TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val-LastYearQ1Val , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q3', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                            #
                            Scenario2=DY[(DY['year']==key)&(DY['Qperiod']=='Q4')]
                            TempDF2=Scenario6[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            # Q4Temp=TempDF2.copy()
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(Q4Temp[Q4Temp['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه':
                                    Q1Val=Q4Temp[Q4Temp['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    # print(Q2Val)
                                    TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val-Q1Val) , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val), 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            LastYearQ5=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            LastYearQ5.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(LastYearQ5.val.unique().tolist())>5:
                                LastYearQ5.loc[:,'year']=str(int(lastYearIn)-1)
                                LastYearQ5.loc[:,'Yearly']=False
                                Final=Final.append(TempDF1Q2)
                                TempDF_created=[]
                                for index,row in LastYearQ5.iterrows():
                                    if len(LastYearQ4[LastYearQ4['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                                        LastYearQ1Val=LastYearQ4[LastYearQ4['Translated']==row['Translated']].val.values.tolist()[0]
                                        LastYearQ2Val=row['val']
                                        TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val-LastYearQ1Val , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q4', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                        #
                        if ('Q1' not in value) and ('Q2' not in value) and ('Q3' not in value) and ('Q4' in value):
                            # print('OnlyQ4')
                            TempDF_created=[]
                            Scenario2=DY[(DY['year']==key)&(DY['Qperiod']=='Q4')]
                            TempDF1=Scenario2[['Translated', 'thisPeriod','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            ##Add Salane
                            TempDF1.loc[:,'Yearly']=True
                            Final=Final.append(TempDF1)
                            ####
                            TempDF1.loc[:,'Yearly']=False
                            for index,row in TempDF1.iterrows():
                                if row['Translated']!='سرمایه':
                                    newVal=row['val']/4
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q1', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q2', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q3', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q4', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q1', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q2', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q3', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q4', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario2[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            TempDF1=Scenario2[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=False
                                TempDF_created=[]
                                for index,row in TempDF1.iterrows():
                                    if row['Translated']!='سرمایه':
                                        newVal=row['val']/4
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q1', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q3', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q4', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q1', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q3', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q4', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                
                Final = Final.sort_values(by='toDate', ascending=False)
                TempDF_created=[]
                for k in Final.toDate.unique().tolist():
                      All=(Final[(Final['Translated']=='سرمایه')&(Final['toDate']==k)])
                      for index,row in All.iterrows():
                        try:
                            r3=(Final[(Final['Translated']=='سود (زیان) خالص')&(Final['year']==row['year'])&(Final['toDate']==row['toDate'])&(Final['Qperiod']==row['Qperiod'])&(Final['Yearly']==row['Yearly'])]).iloc[0].val
                            r4=(Final[(Final['Translated']=='سرمایه')&(Final['year']==row['year'])&(Final['toDate']==row['toDate'])&(Final['Qperiod']==row['Qperiod'])&(Final['Yearly']==row['Yearly'])]).iloc[0].val
                            if r4==0:
                                continue
                            try:
                                r5=(Final[(Final['Translated']=='سایر درآمدها و هزینه\u200cهای غیرعملیاتی- اقلام متفرقه')&(Final['year']==row['year'])&(Final['toDate']==row['toDate'])&(Final['Qperiod']==row['Qperiod'])&(Final['Yearly']==row['Yearly'])]).iloc[0].val
                            except:
                                r5=0
                            TempDF_created.append({'Translated':'EarningWithouNRI', 'val':r3-r5 , 'aggregated':row['aggregated']
                                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                        , 'Qperiod':row['Qperiod'], 'year':row['year'], 'Yearly':row['Yearly']})
                            TempDF_created.append({'Translated':'EPSWithoutNRI', 'val':(r3-r5)*1000/(r4), 'aggregated':row['aggregated']
                                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                        , 'Qperiod':row['Qperiod'], 'year':row['year'], 'Yearly':row['Yearly']})
                        except:
                            # print(row)
                            print(traceback.format_exc())
                lastDF=pd.DataFrame(TempDF_created)
                Final=Final.append(lastDF)
                Final.loc[:,'Engitem']='ISItems'
                Final = Final.drop_duplicates(subset=['Translated','aggregated', 'firm', 'Qperiod', 'year','Yearly'], keep="first")
                reports=reports.drop_duplicates()
                # print(reports)
                insertValues(Final,"IS",reports)
                #######
                # insertValues(DY,"ISCons")
                # insertValues(DY,"ISComp")
                # insertValues(DY,"ISConsComp")
        except Exception as E:
            print(E)
            # print(traceback.format_exc())
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
                DY=DY.dropna(subset=['Translated'])
                DY=DY.dropna(subset=['thisPeriod'])
                DT=DY.copy()
                reports=DY[['report_id','firm']]
                Final=pd.DataFrame()
                yearsSituation={}
                for year in DY.year.unique().tolist():
                    yearsSituation[year]=DY[DY['year']==year].Qperiod.unique()
                for key, value in yearsSituation.items():
                    # print(key)
                    # print(value)
                    if int(key)==int(lastYearIn):
                        if len(value)==0:
                            continue
                        #
                        if ('Q1' in value) and ('Q2' not in value) and ('Q3' not in value) and ('Q4' not in value):
                            # X=='سرمایه'
                            # print('Only Q1 for Recent Year')
                            Scenario1DF1=DY[(DY['year']==key)&(DY['Qperiod']=='Q1')]
                            TempDF1=Scenario1DF1[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1.loc[:,'Yearly']=False
                            Final=Final.append(TempDF1)
                            TempDF1=Scenario1DF1[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            TempDF1=Scenario1DF1[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=False
                                Final=Final.append(TempDF1)
                        #
                        if ('Q1' not in value) and ('Q2' in value) and ('Q3' not in value) and ('Q4' not in value):
                            # print('Only Q2')
                            # X=='سرمایه'
                            TempDF_created=[]
                            Scenario2=DY[(DY['year']==key)&(DY['Qperiod']=='Q2')]
                            TempDF1=Scenario2[['Translated', 'thisPeriod','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1.loc[:,'Yearly']=False
                            for index,row in TempDF1.iterrows():
                                if row['Translated']!='سرمایه':
                                    newVal=row['val']/2
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q1', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q2', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q1', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q2', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario2[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            TempDF1=Scenario2[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=False
                                TempDF_created=[]
                                for index,row in TempDF1.iterrows():
                                    if row['Translated']!='سرمایه':
                                        newVal=row['val']/2
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q1', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q2', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q1', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q2', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                        #
                        if ('Q1' not in value) and ('Q2' not in value) and ('Q3' in value) and ('Q4' not in value):
                            # print('Only Q3')
                            Scenario3=DY[(DY['year']==key)&(DY['Qperiod']=='Q3')]
                            TempDF_created=[]
                            TempDF1=Scenario3[['Translated', 'thisPeriod','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1.loc[:,'Yearly']=False
                            for index,row in TempDF1.iterrows():
                                if row['Translated']!='سرمایه':
                                    newVal=row['val']/3
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q1', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q2', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q3', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q1', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q2', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q3', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario3[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            TempDF1=Scenario3[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=False
                                TempDF_created=[]
                                for index,row in TempDF1.iterrows():
                                    if row['Translated']!='سرمایه':
                                        newVal=row['val']/3
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q1', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q3', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q1', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q3', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                        #
                        if ('Q1' in value) and ('Q2' in value) and ('Q3' not in value) and ('Q4' not in value):
                            # print('Q1 va Q2')
                            Scenario4=DY[(DY['year']==key)&(DY['Qperiod']=='Q1')]
                            TempDF1Q=Scenario4[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1Q.loc[:,'Yearly']=False
                            Final=Final.append(TempDF1Q)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            TempDF1Q2=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1Q2.val.unique().tolist())>5:
                                TempDF1Q2.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1Q2.loc[:,'Yearly']=False
                                Final=Final.append(TempDF1Q2)
                            
                            ####Q1 End
                            ####Q2 Start
                            Scenario5=DY[(DY['year']==key)&(DY['Qperiod']=='Q2')]
                            TempDF2=Scenario5[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(TempDF1Q[TempDF1Q['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه':
                                    Q1Val=TempDF1Q[TempDF1Q['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    # print(Q2Val)
                                    TempDF_created.append({'Translated':row['Translated'], 'val':Q2Val-Q1Val , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            # print(Temp_CreatedDF2)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            ###
                            TempDF1=Scenario5[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            TempDF1=Scenario5[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=False
                                TempDF_created=[]
                                for index,row in TempDF1.iterrows():
                                    if len(TempDF1Q2[TempDF1Q2['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                                        LastYearQ1Val=TempDF1Q2[TempDF1Q2['Translated']==row['Translated']].val.values.tolist()[0]
                                        LastYearQ2Val=row['val']
                                        TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val-LastYearQ1Val , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q2', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                        #
                        if ('Q1' in value) and ('Q2' not in value) and ('Q3' in value) and ('Q4' not in value):
                            # print('Q1 va Q3')
                            Scenario6=DY[(DY['year']==key)&(DY['Qperiod']=='Q1')]
                            TempDF1Q=Scenario6[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1Q.loc[:,'Yearly']=False
                            Final=Final.append(TempDF1Q)
                            TempDF1=Scenario6[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            TempDF1Q2=Scenario6[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1Q2.val.unique().tolist())>5:
                                TempDF1Q2.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1Q2.loc[:,'Yearly']=False
                                Final=Final.append(TempDF1Q2)
                            ####Q1 End
                            #### Q3 Start
                            Scenario8=DY[(DY['year']==key)&(DY['Qperiod']=='Q3')]
                            TempDF2=Scenario8[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(TempDF1Q[TempDF1Q['Translated']==row['Translated']])>0:
                                    if row['Translated']!='سرمایه':
                                        Q1Val=TempDF1Q[TempDF1Q['Translated']==row['Translated']].val.values.tolist()[0]
                                        Q3Val=row['val']
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(Q3Val-Q1Val)/2 , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(Q3Val-Q1Val)/2 , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                else:
                                    if row['Translated']=='سرمایه':
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                    
                            # TempDF2.loc[:,'Yearly']=False
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            # Final=Final.append(TempDF2)
                            TempDF1=Scenario8[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            TempDF1=Scenario8[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=False
                                TempDF_created=[]
                                for index,row in TempDF1.iterrows():
                                    if len(TempDF1Q2[TempDF1Q2['Translated']==row['Translated']])>0:
                                        if row['Translated']!='سرمایه':
                                            LastYearQ1Val=TempDF1Q2[TempDF1Q2['Translated']==row['Translated']].val.values.tolist()[0]
                                            LastYearQ3Val=row['val']
                                            TempDF_created.append({'Translated':row['Translated'], 'val':(LastYearQ3Val-LastYearQ1Val)/2 , 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q2', 'year':row['year']})
                                            TempDF_created.append({'Translated':row['Translated'], 'val':(LastYearQ3Val-LastYearQ1Val)/2 , 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q3', 'year':row['year']})
                                        else:
                                            TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q2', 'year':row['year']})
                                            TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q3', 'year':row['year']})
                                    else:
                                        if row['Translated']=='سرمایه':
                                            TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                            TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                        else:
                                            TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                        #
                        if ('Q1' not in value) and ('Q2' in value) and ('Q3' in value) and ('Q4' not in value):
                            # print('Q2 va Q3')
                            TempDF_created=[]
                            Scenario9=DY[(DY['year']==key)&(DY['Qperiod']=='Q2')]
                            TempDF1=Scenario9[['Translated', 'thisPeriod','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1.loc[:,'Yearly']=False
                            TempDF2QThisyear=TempDF1.copy()
                            for index,row in TempDF1.iterrows():
                                if row['Translated']!='سرمایه':
                                    newVal=row['val']/2
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q1', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q2', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q2', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario9[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            TempDF1=Scenario9[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF2QLastyear=TempDF1.copy()
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=False
                                TempDF_created=[]
                                for index,row in TempDF1.iterrows():
                                    if row['Translated']!='سرمایه':
                                        newVal=row['val']/2
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q1', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q2', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q1', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q2', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                            ####Q2 End
                            #### Q3 Start
                            Scenario10=DY[(DY['year']==key)&(DY['Qperiod']=='Q3')]
                            TempDF2=Scenario10[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(TempDF2QThisyear[TempDF2QThisyear['Translated']==row['Translated']])>0:
                                    if row['Translated']!='سرمایه':
                                        Q2Val=TempDF2QThisyear[TempDF2QThisyear['Translated']==row['Translated']].val.values.tolist()[0]
                                        Q3Val=row['val']
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(Q3Val-Q1Val), 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'], 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                            # TempDF2.loc[:,'Yearly']=False
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            # Final=Final.append(TempDF2)
                            TempDF1=Scenario10[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            TempDF1=Scenario10[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=False
                                TempDF_created=[]
                                for index,row in TempDF1.iterrows():
                                    if len(TempDF2QLastyear[TempDF2QLastyear['Translated']==row['Translated']])>0:
                                        if row['Translated']!='سرمایه':
                                            LastYearQ2Val=TempDF2QLastyear[TempDF2QLastyear['Translated']==row['Translated']].val.values.tolist()[0]
                                            LastYearQ3Val=row['val']
                                            TempDF_created.append({'Translated':row['Translated'], 'val':(LastYearQ3Val-LastYearQ2Val) , 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q3', 'year':row['year']})
                                        else:
                                            TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q3', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                        #
                        if ('Q1' in value) and ('Q2' in value) and ('Q3' in value) and ('Q4' not in value):
                            # print('Q1 va Q2 va Q3')
                            Scenario11=DY[(DY['year']==key)&(DY['Qperiod']=='Q1')]
                            TempDF1Q=Scenario11[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1Q.loc[:,'Yearly']=False
                            Final=Final.append(TempDF1Q)
                            TempDF1=Scenario11[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            TempDF1Q2=Scenario11[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1Q2.val.unique().tolist())>5:
                                TempDF1Q2.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1Q2.loc[:,'Yearly']=False
                                Final=Final.append(TempDF1Q2)
                            ####
                            Scenaro12=DY[(DY['year']==key)&(DY['Qperiod']=='Q2')]
                            TempDF2Q=Scenaro12[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2Q.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF2Q.loc[:,'Yearly']=False
                            TempDF_created=[]
                            for index,row in TempDF2Q.iterrows():
                                if len(TempDF1Q[TempDF1Q['Translated']==row['Translated']])>0:
                                    if row['Translated']!='سرمایه':
                                        Q1Val=TempDF1Q[TempDF1Q['Translated']==row['Translated']].val.values.tolist()[0]
                                        Q2Val=row['val']
                                        TempDF_created.append({'Translated':row['Translated'], 'val':Q2Val-Q1Val , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenaro12[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            TempDF2Q2LastYearthisPeriod=Scenaro12[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2Q2LastYearthisPeriod.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF2Q2LastYearthisPeriod.val.unique().tolist())>5:
                                TempDF2Q2LastYearthisPeriod.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF2Q2LastYearthisPeriod.loc[:,'Yearly']=False
                                TempDF_created=[]
                                for index,row in TempDF2Q2LastYearthisPeriod.iterrows():
                                    if len(TempDF1Q2[TempDF1Q2['Translated']==row['Translated']])>0:
                                        if row['Translated']!='سرمایه':
                                            LastYearQ1Val=TempDF1Q2[TempDF1Q2['Translated']==row['Translated']].val.values.tolist()[0]
                                            LastYearQ2Val=row['val']
                                            TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val-LastYearQ1Val , 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q2', 'year':row['year']})
                                        else:
                                            TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q2', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                            ###
                            Scenario14=DY[(DY['year']==key)&(DY['Qperiod']=='Q3')]
                            TempDF1=Scenario14[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1.loc[:,'Yearly']=False
                            TempDF_created=[]
                            for index,row in TempDF1.iterrows():
                                if len(TempDF2Q[TempDF2Q['Translated']==row['Translated']])>0:
                                    if row['Translated']!='سرمایه':
                                        Q2Val=TempDF2Q[TempDF2Q['Translated']==row['Translated']].val.values.tolist()[0]
                                        Q3Val=row['val']
                                        TempDF_created.append({'Translated':row['Translated'], 'val':Q3Val-Q2Val , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                            # TempDF2.loc[:,'Yearly']=False
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario14[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            TempDF2Q3LastYearthisPeriod=Scenario14[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2Q3LastYearthisPeriod.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF2Q3LastYearthisPeriod.val.unique().tolist())>5:
                                TempDF2Q3LastYearthisPeriod.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF2Q3LastYearthisPeriod.loc[:,'Yearly']=False
                                TempDF_created=[]
                                for index,row in TempDF2Q3LastYearthisPeriod.iterrows():
                                    if len(TempDF2Q2LastYearthisPeriod[TempDF2Q2LastYearthisPeriod['Translated']==row['Translated']])>0:
                                        if row['Translated']!='سرمایه':
                                            LastYearQ2Val=TempDF2Q2LastYearthisPeriod[TempDF2Q2LastYearthisPeriod['Translated']==row['Translated']].val.values.tolist()[0]
                                            LastYearQ3Val=row['val']
                                            TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ3Val-LastYearQ2Val , 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q3', 'year':row['year']})
                                        else:
                                            TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q3', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                    else:
                        if ('Q1' in value) and ('Q2' in value) and ('Q3' in value) and ('Q4' in value):
                            # print('PerfectYear')
                            Scenario4=DY[(DY['year']==key)&(DY['Qperiod']=='Q1')]
                            TempDF1Q=Scenario4[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1Q.loc[:,'Yearly']=False
                            Final=Final.append(TempDF1Q)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            TempDF1Q2=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1Q2.val.unique().tolist())>5:
                                TempDF1Q2.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1Q2.loc[:,'Yearly']=False
                                Final=Final.append(TempDF1Q2)
                            
                            #
                            Scenario5=DY[(DY['year']==key)&(DY['Qperiod']=='Q2')]
                            TempDF2=Scenario5[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            Q2Temp=TempDF2.copy()
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(TempDF1Q[TempDF1Q['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه':
                                    Q1Val=TempDF1Q[TempDF1Q['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    # print(Q2Val)
                                    TempDF_created.append({'Translated':row['Translated'], 'val':Q2Val-Q1Val , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            LastYear6Month=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            LastYear6Month.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(LastYear6Month.val.unique().tolist())>5:
                                LastYear6Month.loc[:,'year']=str(int(lastYearIn)-1)
                                LastYear6Month.loc[:,'Yearly']=False
                                Final=Final.append(TempDF1Q2)
                                TempDF_created=[]
                                for index,row in LastYear6Month.iterrows():
                                    if len(TempDF1Q2[TempDF1Q2['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                                        LastYearQ1Val=TempDF1Q2[TempDF1Q2['Translated']==row['Translated']].val.values.tolist()[0]
                                        LastYearQ2Val=row['val']
                                        TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val-LastYearQ1Val , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q2', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                            #
                            Scenario6=DY[(DY['year']==key)&(DY['Qperiod']=='Q3')]
                            TempDF2=Scenario6[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            Q3Temp=TempDF2.copy()
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(Q2Temp[Q2Temp['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه':
                                    Q1Val=Q2Temp[Q2Temp['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    # print(Q2Val)
                                    TempDF_created.append({'Translated':row['Translated'], 'val':Q2Val-Q1Val , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            LastYearQ3=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            LastYearQ3.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(LastYearQ3.val.unique().tolist())>5:
                                LastYearQ3.loc[:,'year']=str(int(lastYearIn)-1)
                                LastYearQ3.loc[:,'Yearly']=False
                                Final=Final.append(TempDF1Q2)
                                TempDF_created=[]
                                for index,row in LastYearQ3.iterrows():
                                    if len(LastYear6Month[LastYear6Month['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                                        LastYearQ1Val=LastYear6Month[LastYear6Month['Translated']==row['Translated']].val.values.tolist()[0]
                                        LastYearQ2Val=row['val']
                                        TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val-LastYearQ1Val , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q3', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                            #
                            Scenario2=DY[(DY['year']==key)&(DY['Qperiod']=='Q4')]
                            TempDF2=Scenario6[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            Q4Temp=TempDF2.copy()
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(Q3Temp[Q3Temp['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه':
                                    Q1Val=Q3Temp[Q3Temp['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    # print(Q2Val)
                                    TempDF_created.append({'Translated':row['Translated'], 'val':Q2Val-Q1Val , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            LastYearQ4=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            LastYearQ4.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(LastYearQ4.val.unique().tolist())>5:
                                LastYearQ4.loc[:,'year']=str(int(lastYearIn)-1)
                                LastYearQ4.loc[:,'Yearly']=False
                                Final=Final.append(TempDF1Q2)
                                TempDF_created=[]
                                for index,row in LastYearQ4.iterrows():
                                    if len(LastYearQ3[LastYearQ3['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                                        LastYearQ1Val=LastYearQ3[LastYearQ3['Translated']==row['Translated']].val.values.tolist()[0]
                                        LastYearQ2Val=row['val']
                                        TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val-LastYearQ1Val , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q4', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                        #
                        if ('Q1' in value) and ('Q2' not in value) and ('Q3' not in value) and ('Q4' in value):
                            # print('no Q2 and Q3')
                            Scenario4=DY[(DY['year']==key)&(DY['Qperiod']=='Q1')]
                            TempDF1Q=Scenario4[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1Q.loc[:,'Yearly']=False
                            Final=Final.append(TempDF1Q)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            TempDF1Q2=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1Q2.val.unique().tolist())>5:
                                TempDF1Q2.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1Q2.loc[:,'Yearly']=False
                                Final=Final.append(TempDF1Q2)
                            
                            #
                            Scenario5=DY[(DY['year']==key)&(DY['Qperiod']=='Q4')]
                            TempDF2=Scenario5[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(TempDF1Q[TempDF1Q['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه':
                                    Q1Val=TempDF1Q[TempDF1Q['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    newVal=(Q2Val-Q1Val)/3
                                    # print(Q2Val)
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            LastYear12Month=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            LastYear12Month.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(LastYear12Month.val.unique().tolist())>5:
                                LastYear12Month.loc[:,'year']=str(int(lastYearIn)-1)
                                LastYear12Month.loc[:,'Yearly']=False
                                Final=Final.append(TempDF1Q2)
                                TempDF_created=[]
                                for index,row in LastYear12Month.iterrows():
                                    if len(TempDF1Q2[TempDF1Q2['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                                        LastYearQ1Val=TempDF1Q2[TempDF1Q2['Translated']==row['Translated']].val.values.tolist()[0]
                                        LastYearQ2Val=row['val']
                                        newVal=(LastYearQ2Val-LastYearQ1Val )/3
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal, 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal, 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q3', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal, 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q4', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                            #
                        #
                        if ('Q1' not in value) and ('Q2' in value) and ('Q3' not in value) and ('Q4' in value):
                            # print('no Q1 and Q3')
                            Scenario4=DY[(DY['year']==key)&(DY['Qperiod']=='Q2')]
                            TempDF1Q=Scenario4[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1Q.loc[:,'Yearly']=False
                            TempDF_created=[]
                            for index,row in TempDF1Q.iterrows():
                                if row['Translated']!='سرمایه':
                                    newVal=row['val']/2
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q1', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q2', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q1', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q2', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            # Final=Final.append(TempDF1Q)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            LastYear6Month=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            LastYear6Month.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(LastYear6Month.val.unique().tolist())>5:
                                LastYear6Month.loc[:,'year']=str(int(lastYearIn)-1)
                                LastYear6Month.loc[:,'Yearly']=False
                                TempDF_created=[]
                                for index,row in LastYear6Month.iterrows():
                                    if len(TempDF1Q2[TempDF1Q2['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                                        # LastYearQ1Val=TempDF1Q2[TempDF1Q2['Translated']==row['Translated']].val.values.tolist()[0]
                                        LastYearQ2Val=row['val']
                                        newVal=(LastYearQ2Val)/2
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal, 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q1', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal, 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q2', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q1', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)


                            #
                            Scenario5=DY[(DY['year']==key)&(DY['Qperiod']=='Q4')]
                            TempDF2=Scenario5[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(TempDF1Q[TempDF1Q['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه':
                                    Q1Val=TempDF1Q[TempDF1Q['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    newVal=(Q2Val-Q1Val)/2
                                    # print(Q2Val)
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            LastYear12Month=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            LastYear12Month.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(LastYear12Month.val.unique().tolist())>5:
                                LastYear12Month.loc[:,'year']=str(int(lastYearIn)-1)
                                LastYear12Month.loc[:,'Yearly']=False
                                TempDF_created=[]
                                for index,row in LastYear12Month.iterrows():
                                    if len(LastYear6Month[LastYear6Month['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                                        LastYearQ1Val=LastYear6Month[LastYear6Month['Translated']==row['Translated']].val.values.tolist()[0]
                                        LastYearQ2Val=row['val']
                                        newVal=(LastYearQ2Val-LastYearQ1Val )/2
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal, 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q3', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal, 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q4', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                        #
                        if ('Q1' not in value) and ('Q2' not in value) and ('Q3' in value) and ('Q4' in value):
                            # print('no Q2 and Q1')
                            Scenario4=DY[(DY['year']==key)&(DY['Qperiod']=='Q3')]
                            TempDF1Q=Scenario4[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1Q.loc[:,'Yearly']=False
                            TempDF_created=[]
                            for index,row in TempDF1Q.iterrows():
                                if row['Translated']!='سرمایه':
                                    newVal=row['val']/3
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q1', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q2', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q3', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q1', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q2', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q3', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            # Final=Final.append(TempDF1Q)
                            LastYear9Month=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            LastYear9Month.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(LastYear6Month.val.unique().tolist())>5:
                                LastYear6Month.loc[:,'year']=str(int(lastYearIn)-1)
                                LastYear6Month.loc[:,'Yearly']=False
                                TempDF_created=[]
                                for index,row in LastYear6Month.iterrows():
                                    if len(TempDF1Q2[TempDF1Q2['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                                        # LastYearQ1Val=TempDF1Q2[TempDF1Q2['Translated']==row['Translated']].val.values.tolist()[0]
                                        LastYearQ2Val=row['val']
                                        newVal=(LastYearQ2Val)/3
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal, 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q1', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal, 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal, 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q3', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q1', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)


                            #
                            Scenario5=DY[(DY['year']==key)&(DY['Qperiod']=='Q4')]
                            TempDF2=Scenario5[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(TempDF1Q[TempDF1Q['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه':
                                    Q1Val=TempDF1Q[TempDF1Q['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    newVal=(Q2Val-Q1Val)
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario5[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            LastYear12Month=Scenario5[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            LastYear12Month.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(LastYear12Month.val.unique().tolist())>5:
                                LastYear12Month.loc[:,'year']=str(int(lastYearIn)-1)
                                LastYear12Month.loc[:,'Yearly']=False
                                TempDF_created=[]
                                for index,row in LastYear12Month.iterrows():
                                    if len(LastYear6Month[LastYear6Month['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                                        LastYearQ1Val=LastYear6Month[LastYear6Month['Translated']==row['Translated']].val.values.tolist()[0]
                                        LastYearQ2Val=row['val']
                                        newVal=(LastYearQ2Val-LastYearQ1Val )/3
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal, 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q4', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                        #
                        if ('Q1' in value) and ('Q2' not in value) and ('Q3' in value) and ('Q4' in value):
                            # print('no Q2')
                            Scenario4=DY[(DY['year']==key)&(DY['Qperiod']=='Q1')]
                            TempDF1Q=Scenario4[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1Q.loc[:,'Yearly']=False
                            Final=Final.append(TempDF1Q)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            TempDF1Q2=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1Q2.val.unique().tolist())>5:
                                TempDF1Q2.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1Q2.loc[:,'Yearly']=False
                                Final=Final.append(TempDF1Q2)
                            
                            #
                            Scenario6=DY[(DY['year']==key)&(DY['Qperiod']=='Q3')]
                            TempDF2=Scenario6[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            Q3Temp=TempDF2.copy()
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(TempDF1Q[TempDF1Q['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه':
                                    Q1Val=TempDF1Q[TempDF1Q['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    if row['Translated']!='سرمایه' :
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val-Q1Val)/2, 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val-Q1Val)/2 , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':Q2Val, 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':Q2Val , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                else:
                                    if row['Translated']!='سرمایه' :
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'], 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            LastYearQ3=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            LastYearQ3.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(LastYearQ3.val.unique().tolist())>5:
                                LastYearQ3.loc[:,'year']=str(int(lastYearIn)-1)
                                LastYearQ3.loc[:,'Yearly']=False
                                # Final=Final.append(TempDF1Q2)
                                TempDF_created=[]
                                for index,row in LastYearQ3.iterrows():
                                    if len(TempDF1Q2[TempDF1Q2['Translated']==row['Translated']])>0 :
                                        LastYearQ1Val=TempDF1Q2[TempDF1Q2['Translated']==row['Translated']].val.values.tolist()[0]
                                        LastYearQ2Val=row['val']
                                        if row['Translated']!='سرمایه' :
                                            TempDF_created.append({'Translated':row['Translated'], 'val':(LastYearQ2Val-LastYearQ1Val)/2 , 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q3', 'year':row['year']})
                                            TempDF_created.append({'Translated':row['Translated'], 'val':(LastYearQ2Val-LastYearQ1Val)/2 , 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q2', 'year':row['year']})
                                        else:
                                            TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val , 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q3', 'year':row['year']})
                                            TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val, 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q2', 'year':row['year']})
                                    else:
                                        if row['Translated']!='سرمایه' :
                                            TempDF_created.append({'Translated':row['Translated'], 'val':row['val']/2, 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                            TempDF_created.append({'Translated':row['Translated'], 'val':row['val']/2 , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                                        else:
                                            TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q3', 'year':row['year']})
                                            TempDF_created.append({'Translated':row['Translated'], 'val':row['val'], 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q2', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                            #
                            Scenario2=DY[(DY['year']==key)&(DY['Qperiod']=='Q4')]
                            TempDF2=Scenario6[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            Q4Temp=TempDF2.copy()
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(Q3Temp[Q3Temp['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه':
                                    Q1Val=Q3Temp[Q3Temp['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    # print(Q2Val)
                                    TempDF_created.append({'Translated':row['Translated'], 'val':Q2Val-Q1Val , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            LastYearQ4=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            LastYearQ4.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(LastYearQ4.val.unique().tolist())>5:
                                LastYearQ4.loc[:,'year']=str(int(lastYearIn)-1)
                                LastYearQ4.loc[:,'Yearly']=False
                                Final=Final.append(TempDF1Q2)
                                TempDF_created=[]
                                for index,row in LastYearQ4.iterrows():
                                    if len(LastYearQ3[LastYearQ3['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                                        LastYearQ1Val=LastYearQ3[LastYearQ3['Translated']==row['Translated']].val.values.tolist()[0]
                                        LastYearQ2Val=row['val']
                                        TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val-LastYearQ1Val , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q4', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                        #
                        if ('Q1' in value) and ('Q2' in value) and ('Q3' not in value) and ('Q4' in value):
                            # print('no Q3')
                            Scenario4=DY[(DY['year']==key)&(DY['Qperiod']=='Q1')]
                            TempDF1Q=Scenario4[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1Q.loc[:,'Yearly']=False
                            Final=Final.append(TempDF1Q)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            TempDF1Q2=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1Q2.val.unique().tolist())>5:
                                TempDF1Q2.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1Q2.loc[:,'Yearly']=False
                                Final=Final.append(TempDF1Q2)
                            
                            #
                            Scenario6=DY[(DY['year']==key)&(DY['Qperiod']=='Q2')]
                            TempDF2=Scenario6[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            Q3Temp=TempDF2.copy()
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(TempDF1Q[TempDF1Q['Translated']==row['Translated']])>0:
                                    Q1Val=TempDF1Q[TempDF1Q['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    if row['Translated']!='سرمایه' :
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val-Q1Val), 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':Q2Val, 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                else:
                                    if row['Translated']!='سرمایه' :
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'], 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            LastYearQ3=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            LastYearQ3.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(LastYearQ3.val.unique().tolist())>5:
                                LastYearQ3.loc[:,'year']=str(int(lastYearIn)-1)
                                LastYearQ3.loc[:,'Yearly']=False
                                # Final=Final.append(TempDF1Q2)
                                TempDF_created=[]
                                for index,row in LastYearQ3.iterrows():
                                    if len(TempDF1Q2[TempDF1Q2['Translated']==row['Translated']])>0 :

                                        LastYearQ1Val=TempDF1Q2[TempDF1Q2['Translated']==row['Translated']].val.values.tolist()[0]
                                        LastYearQ2Val=row['val']
                                        if row['Translated']!='سرمایه' :
                                            TempDF_created.append({'Translated':row['Translated'], 'val':(LastYearQ2Val-LastYearQ1Val) , 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q2', 'year':row['year']})
                                        else:
                                            TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val, 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q2', 'year':row['year']})
                                    else:
                                        
                                        if row['Translated']!='سرمایه' :
                                            TempDF_created.append({'Translated':row['Translated'], 'val':row['val'], 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                                        else:
                                            TempDF_created.append({'Translated':row['Translated'], 'val':row['val'], 'aggregated':row['aggregated']
                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                            , 'Qperiod':'Q2', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                            #
                            Scenario2=DY[(DY['year']==key)&(DY['Qperiod']=='Q4')]
                            TempDF2=Scenario6[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            Q4Temp=TempDF2.copy()
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(Q3Temp[Q3Temp['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه':
                                    Q1Val=Q3Temp[Q3Temp['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    # print(Q2Val)
                                    if row['Translated']!='سرمایه' :
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val-Q1Val)/2 , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val-Q1Val)/2 , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q4', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val) , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val), 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q4', 'year':row['year']})
                                else:
                                    if row['Translated']!='سرمایه' :
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val']/2 , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val']/2 , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q4', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q4', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            LastYearQ4=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            LastYearQ4.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(LastYearQ4.val.unique().tolist())>5:
                                LastYearQ4.loc[:,'year']=str(int(lastYearIn)-1)
                                LastYearQ4.loc[:,'Yearly']=False
                                Final=Final.append(TempDF1Q2)
                                TempDF_created=[]
                                for index,row in LastYearQ4.iterrows():
                                    if len(LastYearQ3[LastYearQ3['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                                        LastYearQ1Val=LastYearQ3[LastYearQ3['Translated']==row['Translated']].val.values.tolist()[0]
                                        LastYearQ2Val=row['val']
                                        TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val-LastYearQ1Val , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q4', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                        #
                        if ('Q1' not in value) and ('Q2' in value) and ('Q3' in value) and ('Q4' in value):
                            # print('no Q1')
                            Scenario6=DY[(DY['year']==key)&(DY['Qperiod']=='Q2')]
                            TempDF2=Scenario6[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            Q3Temp=TempDF2.copy()
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if row['Translated']!='سرمایه':
                                    Q2Val=row['val']
                                    if row['Translated']!='سرمایه' :
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val)/2, 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q1', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val)/2, 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':Q2Val, 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':Q2Val, 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q1', 'year':row['year']})

                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            LastYearQ3=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            LastYearQ3.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(LastYearQ3.val.unique().tolist())>5:
                                LastYearQ3.loc[:,'year']=str(int(lastYearIn)-1)
                                LastYearQ3.loc[:,'Yearly']=False
                                # Final=Final.append(TempDF1Q2)
                                TempDF_created=[]
                                for index,row in LastYearQ3.iterrows():
                                    LastYearQ2Val=row['val']
                                    if row['Translated']!='سرمایه' :
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(LastYearQ2Val)/2 , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(LastYearQ2Val)/2 , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q2', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val, 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val, 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q2', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                            #
                            Scenario2=DY[(DY['year']==key)&(DY['Qperiod']=='Q3')]
                            TempDF2=Scenario6[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            Q4Temp=TempDF2.copy()
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(Q3Temp[Q3Temp['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه':
                                    Q1Val=Q3Temp[Q3Temp['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    # print(Q2Val)
                                    TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val-Q1Val) , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val) , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            LastYearQ4=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            LastYearQ4.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(LastYearQ4.val.unique().tolist())>5:
                                LastYearQ4.loc[:,'year']=str(int(lastYearIn)-1)
                                LastYearQ4.loc[:,'Yearly']=False
                                Final=Final.append(TempDF1Q2)
                                TempDF_created=[]
                                for index,row in LastYearQ4.iterrows():
                                    if len(LastYearQ3[LastYearQ3['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                                        LastYearQ1Val=LastYearQ3[LastYearQ3['Translated']==row['Translated']].val.values.tolist()[0]
                                        LastYearQ2Val=row['val']
                                        TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val-LastYearQ1Val , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q3', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                            #
                            Scenario2=DY[(DY['year']==key)&(DY['Qperiod']=='Q4')]
                            TempDF2=Scenario6[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            # Q4Temp=TempDF2.copy()
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(Q4Temp[Q4Temp['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه':
                                    Q1Val=Q4Temp[Q4Temp['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    # print(Q2Val)
                                    TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val-Q1Val) , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val), 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            LastYearQ5=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            LastYearQ5.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(LastYearQ5.val.unique().tolist())>5:
                                LastYearQ5.loc[:,'year']=str(int(lastYearIn)-1)
                                LastYearQ5.loc[:,'Yearly']=False
                                Final=Final.append(TempDF1Q2)
                                TempDF_created=[]
                                for index,row in LastYearQ5.iterrows():
                                    if len(LastYearQ4[LastYearQ4['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                                        LastYearQ1Val=LastYearQ4[LastYearQ4['Translated']==row['Translated']].val.values.tolist()[0]
                                        LastYearQ2Val=row['val']
                                        TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val-LastYearQ1Val , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q4', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                        #
                        if ('Q1' not in value) and ('Q2' not in value) and ('Q3' not in value) and ('Q4' in value):
                            # print('OnlyQ4')
                            TempDF_created=[]
                            Scenario2=DY[(DY['year']==key)&(DY['Qperiod']=='Q4')]
                            TempDF1=Scenario2[['Translated', 'thisPeriod','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            ##Add Salane
                            TempDF1.loc[:,'Yearly']=True
                            Final=Final.append(TempDF1)
                            ####
                            TempDF1.loc[:,'Yearly']=False
                            for index,row in TempDF1.iterrows():
                                if row['Translated']!='سرمایه':
                                    newVal=row['val']/4
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q1', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q2', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q3', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q4', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q1', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q2', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q3', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q4', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario2[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            TempDF1=Scenario2[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=False
                                TempDF_created=[]
                                for index,row in TempDF1.iterrows():
                                    if row['Translated']!='سرمایه':
                                        newVal=row['val']/4
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q1', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q3', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q4', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q1', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q3', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                        , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                        , 'Qperiod':'Q4', 'year':row['year']})
                                Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                                Temp_CreatedDF2.loc[:,'Yearly']=False
                                Final=Final.append(Temp_CreatedDF2)
                Final = Final.sort_values(by='toDate', ascending=False)
                # reports=Final[['report_id','firm']]
                Final = Final.drop_duplicates(subset=['Translated','aggregated', 'firm', 'Qperiod', 'year','Yearly'], keep="first")
                # Final=Final.append(lastDF)
                Final['Engitem']='CFItems'
                reports=reports.drop_duplicates()
                # print(Final.head())
                insertValues(Final,'CF',reports)
        except Exception as E:
            print(E)
            # print(traceback.format_exec())
            continue    
    for index,row in DX.iterrows():
        try:
            print('*****BS****')
            print(row['ID'])
            Conv=getAlreadyConverted(row['ID'],'BS')
            DY=getLast4BC(row['ID'])
            DN=DY[~DY['report_id'].isin(Conv.TracingNo.tolist())]
            if not DN.empty:
                # print(DY.columns)
                DY=DY[['Translated','thisPeriod','lastYear','period','aggregated','toDate','report_id']]
                DY=DY.dropna(subset=['Translated'])
                DY=DY.dropna(subset=['thisPeriod'])
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
                reports=DY[['report_id','firm']]
                ######
                Final=pd.DataFrame()
                yearsSituation={}
                for year in DY.year.unique().tolist():
                    yearsSituation[year]=DY[DY['year']==year].Qperiod.unique()
                for key, value in yearsSituation.items():
                    # print(key)
                    # print(value)
                    if int(key)==int(lastYearIn):
                        if len(value)==0:
                            continue
                        #
                        if ('Q1' in value) and ('Q2' not in value) and ('Q3' not in value) and ('Q4' not in value):
                            # X=='سرمایه'
                            # print('Only Q1 for Recent Year')
                            Scenario1DF1=DY[(DY['year']==key)&(DY['Qperiod']=='Q1')]
                            TempDF1=Scenario1DF1[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1.loc[:,'Yearly']=False
                            Final=Final.append(TempDF1)
                            TempDF1=Scenario1DF1[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            # TempDF1=Scenario1DF1[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            # TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            # if len(TempDF1.val.unique().tolist())>5:
                            #     TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                            #     TempDF1.loc[:,'Yearly']=False
                            #     Final=Final.append(TempDF1)
                        #
                        if ('Q1' not in value) and ('Q2' in value) and ('Q3' not in value) and ('Q4' not in value):
                            # print('Only Q2')
                            # X=='سرمایه'
                            TempDF_created=[]
                            Scenario2=DY[(DY['year']==key)&(DY['Qperiod']=='Q2')]
                            TempDF1=Scenario2[['Translated', 'thisPeriod','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1.loc[:,'Yearly']=False
                            for index,row in TempDF1.iterrows():
                                if row['Translated']!='سرمایه':
                                    newVal=row['val']
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q1', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q2', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q1', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q2', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario2[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            # TempDF1=Scenario2[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            # TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            # if len(TempDF1.val.unique().tolist())>5:
                            #     TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                            #     TempDF1.loc[:,'Yearly']=False
                            #     TempDF_created=[]
                            #     for index,row in TempDF1.iterrows():
                            #         if row['Translated']!='سرمایه':
                            #             newVal=row['val']
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q1', 'year':row['year']})
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q2', 'year':row['year']})
                            #         else:
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q1', 'year':row['year']})
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q2', 'year':row['year']})
                            #     Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            #     Temp_CreatedDF2.loc[:,'Yearly']=False
                            #     Final=Final.append(Temp_CreatedDF2)
                        #
                        if ('Q1' not in value) and ('Q2' not in value) and ('Q3' in value) and ('Q4' not in value):
                            # print('Only Q3')
                            Scenario3=DY[(DY['year']==key)&(DY['Qperiod']=='Q3')]
                            TempDF_created=[]
                            TempDF1=Scenario3[['Translated', 'thisPeriod','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1.loc[:,'Yearly']=False
                            for index,row in TempDF1.iterrows():
                                if row['Translated']!='سرمایه':
                                    newVal=row['val']
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q1', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q2', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q3', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q1', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q2', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q3', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario3[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            # TempDF1=Scenario3[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            # TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            # if len(TempDF1.val.unique().tolist())>5:
                            #     TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                            #     TempDF1.loc[:,'Yearly']=False
                            #     TempDF_created=[]
                            #     for index,row in TempDF1.iterrows():
                            #         if row['Translated']!='سرمایه':
                            #             newVal=row['val']
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q1', 'year':row['year']})
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q2', 'year':row['year']})
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q3', 'year':row['year']})
                            #         else:
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q1', 'year':row['year']})
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q2', 'year':row['year']})
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q3', 'year':row['year']})
                            # Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            # Temp_CreatedDF2.loc[:,'Yearly']=False
                            # Final=Final.append(Temp_CreatedDF2)
                        #
                        if ('Q1' in value) and ('Q2' in value) and ('Q3' not in value) and ('Q4' not in value):
                            # print('Q1 va Q2')
                            Scenario4=DY[(DY['year']==key)&(DY['Qperiod']=='Q1')]
                            TempDF1Q=Scenario4[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1Q.loc[:,'Yearly']=False
                            Final=Final.append(TempDF1Q)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            # TempDF1Q2=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            # TempDF1Q2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            # if len(TempDF1Q2.val.unique().tolist())>5:
                            #     TempDF1Q2.loc[:,'year']=str(int(lastYearIn)-1)
                            #     TempDF1Q2.loc[:,'Yearly']=False
                            #     Final=Final.append(TempDF1Q2)
                            
                            ####Q1 End
                            ####Q2 Start
                            Scenario5=DY[(DY['year']==key)&(DY['Qperiod']=='Q2')]
                            TempDF2=Scenario5[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(TempDF1Q[TempDF1Q['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه':
                                    # Q1Val=TempDF1Q[TempDF1Q['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    # print(Q2Val)
                                    TempDF_created.append({'Translated':row['Translated'], 'val':Q2Val , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            # print(Temp_CreatedDF2)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            ###
                            TempDF1=Scenario5[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            # TempDF1=Scenario5[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            # TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            # if len(TempDF1.val.unique().tolist())>5:
                            #     TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                            #     TempDF1.loc[:,'Yearly']=False
                            #     TempDF_created=[]
                            #     for index,row in TempDF1.iterrows():
                            #         if len(TempDF1Q2[TempDF1Q2['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                            #             # LastYearQ1Val=TempDF1Q2[TempDF1Q2['Translated']==row['Translated']].val.values.tolist()[0]
                            #             LastYearQ2Val=row['val']
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val , 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q2', 'year':row['year']})
                            #         else:
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #                                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                                 , 'Qperiod':'Q2', 'year':row['year']})
                            #     Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            #     Temp_CreatedDF2.loc[:,'Yearly']=False
                            #     Final=Final.append(Temp_CreatedDF2)
                        #
                        if ('Q1' in value) and ('Q2' not in value) and ('Q3' in value) and ('Q4' not in value):
                            # print('Q1 va Q3')
                            Scenario6=DY[(DY['year']==key)&(DY['Qperiod']=='Q1')]
                            TempDF1Q=Scenario6[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1Q.loc[:,'Yearly']=False
                            Final=Final.append(TempDF1Q)
                            TempDF1=Scenario6[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            # TempDF1Q2=Scenario6[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            # TempDF1Q2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            # if len(TempDF1Q2.val.unique().tolist())>5:
                            #     TempDF1Q2.loc[:,'year']=str(int(lastYearIn)-1)
                            #     TempDF1Q2.loc[:,'Yearly']=False
                            #     Final=Final.append(TempDF1Q2)
                            ####Q1 End
                            #### Q3 Start
                            Scenario8=DY[(DY['year']==key)&(DY['Qperiod']=='Q3')]
                            TempDF2=Scenario8[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(TempDF1Q[TempDF1Q['Translated']==row['Translated']])>0:
                                    if row['Translated']!='سرمایه':
                                        Q1Val=TempDF1Q[TempDF1Q['Translated']==row['Translated']].val.values.tolist()[0]
                                        Q3Val=row['val']
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(Q3Val) , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(Q3Val) , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                else:
                                    if row['Translated']=='سرمایه':
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                    
                            # TempDF2.loc[:,'Yearly']=False
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            # Final=Final.append(TempDF2)
                            TempDF1=Scenario8[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            # TempDF1=Scenario8[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            # TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            # if len(TempDF1.val.unique().tolist())>5:
                            #     TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                            #     TempDF1.loc[:,'Yearly']=False
                            #     TempDF_created=[]
                            #     for index,row in TempDF1.iterrows():
                            #         if len(TempDF1Q2[TempDF1Q2['Translated']==row['Translated']])>0:
                            #             if row['Translated']!='سرمایه':
                            #                 LastYearQ1Val=TempDF1Q2[TempDF1Q2['Translated']==row['Translated']].val.values.tolist()[0]
                            #                 LastYearQ3Val=row['val']
                            #                 TempDF_created.append({'Translated':row['Translated'], 'val':(LastYearQ3Val) , 'aggregated':row['aggregated']
                            #                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                 , 'Qperiod':'Q2', 'year':row['year']})
                            #                 TempDF_created.append({'Translated':row['Translated'], 'val':(LastYearQ3Val) , 'aggregated':row['aggregated']
                            #                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                 , 'Qperiod':'Q3', 'year':row['year']})
                            #             else:
                            #                 TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                 , 'Qperiod':'Q2', 'year':row['year']})
                            #                 TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                 , 'Qperiod':'Q3', 'year':row['year']})
                            #         else:
                            #             if row['Translated']=='سرمایه':
                            #                 TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #                                     , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                                     , 'Qperiod':'Q3', 'year':row['year']})
                            #                 TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #                                     , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                                     , 'Qperiod':'Q2', 'year':row['year']})
                            #             else:
                            #                 TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #                                     , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                                     , 'Qperiod':'Q3', 'year':row['year']})
                            #     Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            #     Temp_CreatedDF2.loc[:,'Yearly']=False
                            #     Final=Final.append(Temp_CreatedDF2)
                        #
                        if ('Q1' not in value) and ('Q2' in value) and ('Q3' in value) and ('Q4' not in value):
                            # print('Q2 va Q3')
                            TempDF_created=[]
                            Scenario9=DY[(DY['year']==key)&(DY['Qperiod']=='Q2')]
                            TempDF1=Scenario9[['Translated', 'thisPeriod','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1.loc[:,'Yearly']=False
                            TempDF2QThisyear=TempDF1.copy()
                            for index,row in TempDF1.iterrows():
                                if row['Translated']!='سرمایه':
                                    newVal=row['val']
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q1', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q2', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q2', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario9[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            # TempDF1=Scenario9[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            # TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            # TempDF2QLastyear=TempDF1.copy()
                            # if len(TempDF1.val.unique().tolist())>5:
                            #     TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                            #     TempDF1.loc[:,'Yearly']=False
                            #     TempDF_created=[]
                            #     for index,row in TempDF1.iterrows():
                            #         if row['Translated']!='سرمایه':
                            #             newVal=row['val']
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q1', 'year':row['year']})
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q2', 'year':row['year']})
                            #         else:
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q1', 'year':row['year']})
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q2', 'year':row['year']})
                            #     Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            #     Temp_CreatedDF2.loc[:,'Yearly']=False
                            #     Final=Final.append(Temp_CreatedDF2)
                            ####Q2 End
                            #### Q3 Start
                            Scenario10=DY[(DY['year']==key)&(DY['Qperiod']=='Q3')]
                            TempDF2=Scenario10[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(TempDF2QThisyear[TempDF2QThisyear['Translated']==row['Translated']])>0:
                                    if row['Translated']!='سرمایه':
                                        Q2Val=TempDF2QThisyear[TempDF2QThisyear['Translated']==row['Translated']].val.values.tolist()[0]
                                        Q3Val=row['val']
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(Q3Val), 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'], 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                            # TempDF2.loc[:,'Yearly']=False
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            # Final=Final.append(TempDF2)
                            TempDF1=Scenario10[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            # TempDF1=Scenario10[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            # TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            # if len(TempDF1.val.unique().tolist())>5:
                            #     TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                            #     TempDF1.loc[:,'Yearly']=False
                            #     TempDF_created=[]
                            #     for index,row in TempDF1.iterrows():
                            #         if len(TempDF2QLastyear[TempDF2QLastyear['Translated']==row['Translated']])>0:
                            #             if row['Translated']!='سرمایه':
                            #                 LastYearQ2Val=TempDF2QLastyear[TempDF2QLastyear['Translated']==row['Translated']].val.values.tolist()[0]
                            #                 LastYearQ3Val=row['val']
                            #                 TempDF_created.append({'Translated':row['Translated'], 'val':(LastYearQ3Val) , 'aggregated':row['aggregated']
                            #                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                 , 'Qperiod':'Q3', 'year':row['year']})
                            #             else:
                            #                 TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                 , 'Qperiod':'Q3', 'year':row['year']})
                            #         else:
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #                                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                                 , 'Qperiod':'Q3', 'year':row['year']})
                            #     Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            #     Temp_CreatedDF2.loc[:,'Yearly']=False
                            #     Final=Final.append(Temp_CreatedDF2)
                        #
                        if ('Q1' in value) and ('Q2' in value) and ('Q3' in value) and ('Q4' not in value):
                            # print('Q1 va Q2 va Q3')
                            Scenario11=DY[(DY['year']==key)&(DY['Qperiod']=='Q1')]
                            TempDF1Q=Scenario11[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1Q.loc[:,'Yearly']=False
                            Final=Final.append(TempDF1Q)
                            TempDF1=Scenario11[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            # TempDF1Q2=Scenario11[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            # TempDF1Q2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            # if len(TempDF1Q2.val.unique().tolist())>5:
                            #     TempDF1Q2.loc[:,'year']=str(int(lastYearIn)-1)
                            #     TempDF1Q2.loc[:,'Yearly']=False
                            #     Final=Final.append(TempDF1Q2)
                            ####
                            Scenaro12=DY[(DY['year']==key)&(DY['Qperiod']=='Q2')]
                            TempDF2Q=Scenaro12[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2Q.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF2Q.loc[:,'Yearly']=False
                            TempDF_created=[]
                            for index,row in TempDF2Q.iterrows():
                                if len(TempDF1Q[TempDF1Q['Translated']==row['Translated']])>0:
                                    if row['Translated']!='سرمایه':
                                        Q1Val=TempDF1Q[TempDF1Q['Translated']==row['Translated']].val.values.tolist()[0]
                                        Q2Val=row['val']
                                        TempDF_created.append({'Translated':row['Translated'], 'val':Q2Val , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenaro12[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            # TempDF2Q2LastYearthisPeriod=Scenaro12[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            # TempDF2Q2LastYearthisPeriod.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            # if len(TempDF2Q2LastYearthisPeriod.val.unique().tolist())>5:
                            #     TempDF2Q2LastYearthisPeriod.loc[:,'year']=str(int(lastYearIn)-1)
                            #     TempDF2Q2LastYearthisPeriod.loc[:,'Yearly']=False
                            #     TempDF_created=[]
                            #     for index,row in TempDF2Q2LastYearthisPeriod.iterrows():
                            #         if len(TempDF1Q2[TempDF1Q2['Translated']==row['Translated']])>0:
                            #             if row['Translated']!='سرمایه':
                            #                 LastYearQ1Val=TempDF1Q2[TempDF1Q2['Translated']==row['Translated']].val.values.tolist()[0]
                            #                 LastYearQ2Val=row['val']
                            #                 TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val , 'aggregated':row['aggregated']
                            #                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                 , 'Qperiod':'Q2', 'year':row['year']})
                            #             else:
                            #                 TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                 , 'Qperiod':'Q2', 'year':row['year']})
                            #         else:
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #                                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                                 , 'Qperiod':'Q2', 'year':row['year']})
                            #     Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            #     Temp_CreatedDF2.loc[:,'Yearly']=False
                            #     Final=Final.append(Temp_CreatedDF2)
                            ###
                            Scenario14=DY[(DY['year']==key)&(DY['Qperiod']=='Q3')]
                            TempDF1=Scenario14[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1.loc[:,'Yearly']=False
                            TempDF_created=[]
                            for index,row in TempDF1.iterrows():
                                if len(TempDF2Q[TempDF2Q['Translated']==row['Translated']])>0:
                                    if row['Translated']!='سرمایه':
                                        Q2Val=TempDF2Q[TempDF2Q['Translated']==row['Translated']].val.values.tolist()[0]
                                        Q3Val=row['val']
                                        TempDF_created.append({'Translated':row['Translated'], 'val':Q3Val , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                            # TempDF2.loc[:,'Yearly']=False
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario14[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            # TempDF2Q3LastYearthisPeriod=Scenario14[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            # TempDF2Q3LastYearthisPeriod.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            # if len(TempDF2Q3LastYearthisPeriod.val.unique().tolist())>5:
                            #     TempDF2Q3LastYearthisPeriod.loc[:,'year']=str(int(lastYearIn)-1)
                            #     TempDF2Q3LastYearthisPeriod.loc[:,'Yearly']=False
                            #     TempDF_created=[]
                            #     for index,row in TempDF2Q3LastYearthisPeriod.iterrows():
                            #         if len(TempDF2Q2LastYearthisPeriod[TempDF2Q2LastYearthisPeriod['Translated']==row['Translated']])>0:
                            #             if row['Translated']!='سرمایه':
                            #                 LastYearQ2Val=TempDF2Q2LastYearthisPeriod[TempDF2Q2LastYearthisPeriod['Translated']==row['Translated']].val.values.tolist()[0]
                            #                 LastYearQ3Val=row['val']
                            #                 TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ3Val , 'aggregated':row['aggregated']
                            #                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                 , 'Qperiod':'Q3', 'year':row['year']})
                            #             else:
                            #                 TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                 , 'Qperiod':'Q3', 'year':row['year']})
                            #         else:
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #                                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                                 , 'Qperiod':'Q3', 'year':row['year']})
                            #     Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            #     Temp_CreatedDF2.loc[:,'Yearly']=False
                            #     Final=Final.append(Temp_CreatedDF2)
                    else:
                        if ('Q1' in value) and ('Q2' in value) and ('Q3' in value) and ('Q4' in value):
                            # print('PerfectYear')
                            Scenario4=DY[(DY['year']==key)&(DY['Qperiod']=='Q1')]
                            TempDF1Q=Scenario4[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1Q.loc[:,'Yearly']=False
                            Final=Final.append(TempDF1Q)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            # TempDF1Q2=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            # TempDF1Q2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            # if len(TempDF1Q2.val.unique().tolist())>5:
                            #     TempDF1Q2.loc[:,'year']=str(int(lastYearIn)-1)
                            #     TempDF1Q2.loc[:,'Yearly']=False
                            #     Final=Final.append(TempDF1Q2)
                            
                            #
                            Scenario5=DY[(DY['year']==key)&(DY['Qperiod']=='Q2')]
                            TempDF2=Scenario5[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            Q2Temp=TempDF2.copy()
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(TempDF1Q[TempDF1Q['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه':
                                    Q1Val=TempDF1Q[TempDF1Q['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    # print(Q2Val)
                                    TempDF_created.append({'Translated':row['Translated'], 'val':Q2Val , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            # LastYear6Month=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            # LastYear6Month.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            # if len(LastYear6Month.val.unique().tolist())>5:
                            #     LastYear6Month.loc[:,'year']=str(int(lastYearIn)-1)
                            #     LastYear6Month.loc[:,'Yearly']=False
                            #     Final=Final.append(TempDF1Q2)
                            #     TempDF_created=[]
                            #     for index,row in LastYear6Month.iterrows():
                            #         if len(TempDF1Q2[TempDF1Q2['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                            #             LastYearQ1Val=TempDF1Q2[TempDF1Q2['Translated']==row['Translated']].val.values.tolist()[0]
                            #             LastYearQ2Val=row['val']
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val , 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q2', 'year':row['year']})
                            #         else:
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #                                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                                 , 'Qperiod':'Q2', 'year':row['year']})
                            #     Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            #     Temp_CreatedDF2.loc[:,'Yearly']=False
                            #     Final=Final.append(Temp_CreatedDF2)
                            #
                            Scenario6=DY[(DY['year']==key)&(DY['Qperiod']=='Q3')]
                            TempDF2=Scenario6[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            Q3Temp=TempDF2.copy()
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(Q2Temp[Q2Temp['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه':
                                    Q1Val=Q2Temp[Q2Temp['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    # print(Q2Val)
                                    TempDF_created.append({'Translated':row['Translated'], 'val':Q2Val , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            # LastYearQ3=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            # LastYearQ3.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            # if len(LastYearQ3.val.unique().tolist())>5:
                            #     LastYearQ3.loc[:,'year']=str(int(lastYearIn)-1)
                            #     LastYearQ3.loc[:,'Yearly']=False
                            #     Final=Final.append(TempDF1Q2)
                            #     TempDF_created=[]
                            #     for index,row in LastYearQ3.iterrows():
                            #         if len(LastYear6Month[LastYear6Month['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                            #             LastYearQ1Val=LastYear6Month[LastYear6Month['Translated']==row['Translated']].val.values.tolist()[0]
                            #             LastYearQ2Val=row['val']
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val , 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q3', 'year':row['year']})
                            #         else:
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #                                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                                 , 'Qperiod':'Q3', 'year':row['year']})
                            #     Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            #     Temp_CreatedDF2.loc[:,'Yearly']=False
                            #     Final=Final.append(Temp_CreatedDF2)
                            #
                            Scenario2=DY[(DY['year']==key)&(DY['Qperiod']=='Q4')]
                            TempDF2=Scenario6[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            Q4Temp=TempDF2.copy()
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(Q3Temp[Q3Temp['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه':
                                    Q1Val=Q3Temp[Q3Temp['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    # print(Q2Val)
                                    TempDF_created.append({'Translated':row['Translated'], 'val':Q2Val , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            # LastYearQ4=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            # LastYearQ4.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            # if len(LastYearQ4.val.unique().tolist())>5:
                            #     LastYearQ4.loc[:,'year']=str(int(lastYearIn)-1)
                            #     LastYearQ4.loc[:,'Yearly']=False
                            #     Final=Final.append(TempDF1Q2)
                            #     TempDF_created=[]
                            #     for index,row in LastYearQ4.iterrows():
                            #         if len(LastYearQ3[LastYearQ3['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                            #             LastYearQ1Val=LastYearQ3[LastYearQ3['Translated']==row['Translated']].val.values.tolist()[0]
                            #             LastYearQ2Val=row['val']
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val , 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q4', 'year':row['year']})
                            #         else:
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #                                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                                 , 'Qperiod':'Q4', 'year':row['year']})
                            #     Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            #     Temp_CreatedDF2.loc[:,'Yearly']=False
                            #     Final=Final.append(Temp_CreatedDF2)
                        #
                        if ('Q1' in value) and ('Q2' not in value) and ('Q3' not in value) and ('Q4' in value):
                            # print('no Q2 and Q3')
                            Scenario4=DY[(DY['year']==key)&(DY['Qperiod']=='Q1')]
                            TempDF1Q=Scenario4[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1Q.loc[:,'Yearly']=False
                            Final=Final.append(TempDF1Q)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            # TempDF1Q2=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            # TempDF1Q2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            # if len(TempDF1Q2.val.unique().tolist())>5:
                            #     TempDF1Q2.loc[:,'year']=str(int(lastYearIn)-1)
                            #     TempDF1Q2.loc[:,'Yearly']=False
                            #     Final=Final.append(TempDF1Q2)
                            
                            #
                            Scenario5=DY[(DY['year']==key)&(DY['Qperiod']=='Q4')]
                            TempDF2=Scenario5[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(TempDF1Q[TempDF1Q['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه':
                                    Q1Val=TempDF1Q[TempDF1Q['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    newVal=(Q2Val)
                                    # print(Q2Val)
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q2', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            # LastYear12Month=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            # LastYear12Month.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            # if len(LastYear12Month.val.unique().tolist())>5:
                            #     LastYear12Month.loc[:,'year']=str(int(lastYearIn)-1)
                            #     LastYear12Month.loc[:,'Yearly']=False
                            #     Final=Final.append(TempDF1Q2)
                            #     TempDF_created=[]
                            #     for index,row in LastYear12Month.iterrows():
                            #         if len(TempDF1Q2[TempDF1Q2['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                            #             LastYearQ1Val=TempDF1Q2[TempDF1Q2['Translated']==row['Translated']].val.values.tolist()[0]
                            #             LastYearQ2Val=row['val']
                            #             newVal=(LastYearQ2Val )
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':newVal, 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q2', 'year':row['year']})
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':newVal, 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q3', 'year':row['year']})
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':newVal, 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q4', 'year':row['year']})
                            #         else:
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #                                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                                 , 'Qperiod':'Q2', 'year':row['year']})
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #                                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                                 , 'Qperiod':'Q3', 'year':row['year']})
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #                                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                                 , 'Qperiod':'Q4', 'year':row['year']})
                            #     Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            #     Temp_CreatedDF2.loc[:,'Yearly']=False
                            #     Final=Final.append(Temp_CreatedDF2)
                            #
                        #
                        if ('Q1' not in value) and ('Q2' in value) and ('Q3' not in value) and ('Q4' in value):
                            # print('no Q1 and Q3')
                            Scenario4=DY[(DY['year']==key)&(DY['Qperiod']=='Q2')]
                            TempDF1Q=Scenario4[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1Q.loc[:,'Yearly']=False
                            TempDF_created=[]
                            for index,row in TempDF1Q.iterrows():
                                if row['Translated']!='سرمایه':
                                    newVal=row['val']/2
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q1', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q2', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q1', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q2', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            # Final=Final.append(TempDF1Q)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            # LastYear6Month=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            # LastYear6Month.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            # if len(LastYear6Month.val.unique().tolist())>5:
                            #     LastYear6Month.loc[:,'year']=str(int(lastYearIn)-1)
                            #     LastYear6Month.loc[:,'Yearly']=False
                            #     TempDF_created=[]
                            #     for index,row in LastYear6Month.iterrows():
                            #         if len(TempDF1Q2[TempDF1Q2['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                            #             # LastYearQ1Val=TempDF1Q2[TempDF1Q2['Translated']==row['Translated']].val.values.tolist()[0]
                            #             LastYearQ2Val=row['val']
                            #             newVal=(LastYearQ2Val)
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':newVal, 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q1', 'year':row['year']})
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':newVal, 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q2', 'year':row['year']})
                            #         else:
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #                                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                                 , 'Qperiod':'Q1', 'year':row['year']})
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #                                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                                 , 'Qperiod':'Q2', 'year':row['year']})
                            #     Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            #     Temp_CreatedDF2.loc[:,'Yearly']=False
                            #     Final=Final.append(Temp_CreatedDF2)


                            #
                            Scenario5=DY[(DY['year']==key)&(DY['Qperiod']=='Q4')]
                            TempDF2=Scenario5[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(TempDF1Q[TempDF1Q['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه':
                                    Q1Val=TempDF1Q[TempDF1Q['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    newVal=(Q2Val)
                                    # print(Q2Val)
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            # LastYear12Month=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            # LastYear12Month.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            # if len(LastYear12Month.val.unique().tolist())>5:
                            #     LastYear12Month.loc[:,'year']=str(int(lastYearIn)-1)
                            #     LastYear12Month.loc[:,'Yearly']=False
                            #     TempDF_created=[]
                            #     for index,row in LastYear12Month.iterrows():
                            #         if len(LastYear6Month[LastYear6Month['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                            #             LastYearQ1Val=LastYear6Month[LastYear6Month['Translated']==row['Translated']].val.values.tolist()[0]
                            #             LastYearQ2Val=row['val']
                            #             newVal=(LastYearQ2Val )
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':newVal, 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q3', 'year':row['year']})
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':newVal, 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q4', 'year':row['year']})
                            #         else:
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #                                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                                 , 'Qperiod':'Q3', 'year':row['year']})
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #                                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                                 , 'Qperiod':'Q4', 'year':row['year']})
                            #     Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            #     Temp_CreatedDF2.loc[:,'Yearly']=False
                            #     Final=Final.append(Temp_CreatedDF2)
                        #
                        if ('Q1' not in value) and ('Q2' not in value) and ('Q3' in value) and ('Q4' in value):
                            # print('no Q2 and Q1')
                            Scenario4=DY[(DY['year']==key)&(DY['Qperiod']=='Q3')]
                            TempDF1Q=Scenario4[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1Q.loc[:,'Yearly']=False
                            TempDF_created=[]
                            for index,row in TempDF1Q.iterrows():
                                if row['Translated']!='سرمایه':
                                    newVal=row['val']
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q1', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q2', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q3', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q1', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q2', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q3', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            # Final=Final.append(TempDF1Q)
                            # LastYear9Month=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            # LastYear9Month.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            # if len(LastYear6Month.val.unique().tolist())>5:
                            #     LastYear6Month.loc[:,'year']=str(int(lastYearIn)-1)
                            #     LastYear6Month.loc[:,'Yearly']=False
                            #     TempDF_created=[]
                            #     for index,row in LastYear6Month.iterrows():
                            #         if len(TempDF1Q2[TempDF1Q2['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                            #             # LastYearQ1Val=TempDF1Q2[TempDF1Q2['Translated']==row['Translated']].val.values.tolist()[0]
                            #             LastYearQ2Val=row['val']
                            #             newVal=(LastYearQ2Val)
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':newVal, 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q1', 'year':row['year']})
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':newVal, 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q2', 'year':row['year']})
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':newVal, 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q3', 'year':row['year']})
                            #         else:
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #                                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                                 , 'Qperiod':'Q1', 'year':row['year']})
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #                                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                                 , 'Qperiod':'Q2', 'year':row['year']})
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #                                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                                 , 'Qperiod':'Q3', 'year':row['year']})
                            #     Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            #     Temp_CreatedDF2.loc[:,'Yearly']=False
                            #     Final=Final.append(Temp_CreatedDF2)


                            #
                            Scenario5=DY[(DY['year']==key)&(DY['Qperiod']=='Q4')]
                            TempDF2=Scenario5[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(TempDF1Q[TempDF1Q['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه':
                                    Q1Val=TempDF1Q[TempDF1Q['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    newVal=(Q2Val)
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario5[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            # LastYear12Month=Scenario5[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            # LastYear12Month.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            # if len(LastYear12Month.val.unique().tolist())>5:
                            #     LastYear12Month.loc[:,'year']=str(int(lastYearIn)-1)
                            #     LastYear12Month.loc[:,'Yearly']=False
                            #     TempDF_created=[]
                            #     for index,row in LastYear12Month.iterrows():
                            #         if len(LastYear6Month[LastYear6Month['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                            #             LastYearQ1Val=LastYear6Month[LastYear6Month['Translated']==row['Translated']].val.values.tolist()[0]
                            #             LastYearQ2Val=row['val']
                            #             newVal=(LastYearQ2Val )
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':newVal, 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q4', 'year':row['year']})
                            #         else:
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #                                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                                 , 'Qperiod':'Q4', 'year':row['year']})
                            #     Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            #     Temp_CreatedDF2.loc[:,'Yearly']=False
                            #     Final=Final.append(Temp_CreatedDF2)
                        #
                        if ('Q1' in value) and ('Q2' not in value) and ('Q3' in value) and ('Q4' in value):
                            # print('no Q2')
                            Scenario4=DY[(DY['year']==key)&(DY['Qperiod']=='Q1')]
                            TempDF1Q=Scenario4[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1Q.loc[:,'Yearly']=False
                            Final=Final.append(TempDF1Q)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            # TempDF1Q2=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            # TempDF1Q2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            # if len(TempDF1Q2.val.unique().tolist())>5:
                            #     TempDF1Q2.loc[:,'year']=str(int(lastYearIn)-1)
                            #     TempDF1Q2.loc[:,'Yearly']=False
                            #     Final=Final.append(TempDF1Q2)
                            
                            #
                            Scenario6=DY[(DY['year']==key)&(DY['Qperiod']=='Q3')]
                            TempDF2=Scenario6[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            Q3Temp=TempDF2.copy()
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(TempDF1Q[TempDF1Q['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه':
                                    Q1Val=TempDF1Q[TempDF1Q['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    if row['Translated']!='سرمایه' :
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val), 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val) , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':Q2Val, 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':Q2Val , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                else:
                                    if row['Translated']!='سرمایه' :
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'], 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            # LastYearQ3=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            # LastYearQ3.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            # if len(LastYearQ3.val.unique().tolist())>5:
                            #     LastYearQ3.loc[:,'year']=str(int(lastYearIn)-1)
                            #     LastYearQ3.loc[:,'Yearly']=False
                            #     # Final=Final.append(TempDF1Q2)
                            #     TempDF_created=[]
                            #     for index,row in LastYearQ3.iterrows():
                            #         if len(TempDF1Q2[TempDF1Q2['Translated']==row['Translated']])>0 :
                            #             LastYearQ1Val=TempDF1Q2[TempDF1Q2['Translated']==row['Translated']].val.values.tolist()[0]
                            #             LastYearQ2Val=row['val']
                            #             if row['Translated']!='سرمایه' :
                            #                 TempDF_created.append({'Translated':row['Translated'], 'val':(LastYearQ2Val) , 'aggregated':row['aggregated']
                            #                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                 , 'Qperiod':'Q3', 'year':row['year']})
                            #                 TempDF_created.append({'Translated':row['Translated'], 'val':(LastYearQ2Val) , 'aggregated':row['aggregated']
                            #                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                 , 'Qperiod':'Q2', 'year':row['year']})
                            #             else:
                            #                 TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val , 'aggregated':row['aggregated']
                            #                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                 , 'Qperiod':'Q3', 'year':row['year']})
                            #                 TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val, 'aggregated':row['aggregated']
                            #                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                 , 'Qperiod':'Q2', 'year':row['year']})
                            #         else:
                            #             if row['Translated']!='سرمایه' :
                            #                 TempDF_created.append({'Translated':row['Translated'], 'val':row['val'], 'aggregated':row['aggregated']
                            #                                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                                 , 'Qperiod':'Q3', 'year':row['year']})
                            #                 TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #                                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                                 , 'Qperiod':'Q2', 'year':row['year']})
                            #             else:
                            #                 TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                 , 'Qperiod':'Q3', 'year':row['year']})
                            #                 TempDF_created.append({'Translated':row['Translated'], 'val':row['val'], 'aggregated':row['aggregated']
                            #                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                 , 'Qperiod':'Q2', 'year':row['year']})
                            #     Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            #     Temp_CreatedDF2.loc[:,'Yearly']=False
                            #     Final=Final.append(Temp_CreatedDF2)
                            #
                            Scenario2=DY[(DY['year']==key)&(DY['Qperiod']=='Q4')]
                            TempDF2=Scenario6[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            Q4Temp=TempDF2.copy()
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(Q3Temp[Q3Temp['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه':
                                    Q1Val=Q3Temp[Q3Temp['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    # print(Q2Val)
                                    TempDF_created.append({'Translated':row['Translated'], 'val':Q2Val , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            # LastYearQ4=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            # LastYearQ4.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            # if len(LastYearQ4.val.unique().tolist())>5:
                            #     LastYearQ4.loc[:,'year']=str(int(lastYearIn)-1)
                            #     LastYearQ4.loc[:,'Yearly']=False
                            #     Final=Final.append(TempDF1Q2)
                            #     TempDF_created=[]
                            #     for index,row in LastYearQ4.iterrows():
                            #         if len(LastYearQ3[LastYearQ3['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                            #             LastYearQ1Val=LastYearQ3[LastYearQ3['Translated']==row['Translated']].val.values.tolist()[0]
                            #             LastYearQ2Val=row['val']
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val , 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q4', 'year':row['year']})
                            #         else:
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #                                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                                 , 'Qperiod':'Q4', 'year':row['year']})
                            #     Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            #     Temp_CreatedDF2.loc[:,'Yearly']=False
                            #     Final=Final.append(Temp_CreatedDF2)
                        #
                        if ('Q1' in value) and ('Q2' in value) and ('Q3' not in value) and ('Q4' in value):
                            # print('no Q3')
                            Scenario4=DY[(DY['year']==key)&(DY['Qperiod']=='Q1')]
                            TempDF1Q=Scenario4[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1Q.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            TempDF1Q.loc[:,'Yearly']=False
                            Final=Final.append(TempDF1Q)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            # TempDF1Q2=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            # TempDF1Q2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            # if len(TempDF1Q2.val.unique().tolist())>5:
                            #     TempDF1Q2.loc[:,'year']=str(int(lastYearIn)-1)
                            #     TempDF1Q2.loc[:,'Yearly']=False
                            #     Final=Final.append(TempDF1Q2)
                            
                            #
                            Scenario6=DY[(DY['year']==key)&(DY['Qperiod']=='Q2')]
                            TempDF2=Scenario6[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            Q3Temp=TempDF2.copy()
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(TempDF1Q[TempDF1Q['Translated']==row['Translated']])>0:
                                    Q1Val=TempDF1Q[TempDF1Q['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    if row['Translated']!='سرمایه' :
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val), 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':Q2Val, 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                else:
                                    if row['Translated']!='سرمایه' :
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'], 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            # LastYearQ3=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            # LastYearQ3.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            # if len(LastYearQ3.val.unique().tolist())>5:
                            #     LastYearQ3.loc[:,'year']=str(int(lastYearIn)-1)
                            #     LastYearQ3.loc[:,'Yearly']=False
                            #     # Final=Final.append(TempDF1Q2)
                            #     TempDF_created=[]
                            #     for index,row in LastYearQ3.iterrows():
                            #         if len(TempDF1Q2[TempDF1Q2['Translated']==row['Translated']])>0 :

                            #             LastYearQ1Val=TempDF1Q2[TempDF1Q2['Translated']==row['Translated']].val.values.tolist()[0]
                            #             LastYearQ2Val=row['val']
                            #             if row['Translated']!='سرمایه' :
                            #                 TempDF_created.append({'Translated':row['Translated'], 'val':(LastYearQ2Val) , 'aggregated':row['aggregated']
                            #                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                 , 'Qperiod':'Q2', 'year':row['year']})
                            #             else:
                            #                 TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val, 'aggregated':row['aggregated']
                            #                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                 , 'Qperiod':'Q2', 'year':row['year']})
                            #         else:
                                        
                            #             if row['Translated']!='سرمایه' :
                            #                 TempDF_created.append({'Translated':row['Translated'], 'val':row['val'], 'aggregated':row['aggregated']
                            #                                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                                 , 'Qperiod':'Q2', 'year':row['year']})
                            #             else:
                            #                 TempDF_created.append({'Translated':row['Translated'], 'val':row['val'], 'aggregated':row['aggregated']
                            #                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                 , 'Qperiod':'Q2', 'year':row['year']})
                            #     Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            #     Temp_CreatedDF2.loc[:,'Yearly']=False
                            #     Final=Final.append(Temp_CreatedDF2)
                            #
                            Scenario2=DY[(DY['year']==key)&(DY['Qperiod']=='Q4')]
                            TempDF2=Scenario6[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            Q4Temp=TempDF2.copy()
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(Q3Temp[Q3Temp['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه':
                                    Q1Val=Q3Temp[Q3Temp['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    # print(Q2Val)
                                    if row['Translated']!='سرمایه' :
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val) , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val) , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q4', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val) , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val), 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q4', 'year':row['year']})
                                else:
                                    if row['Translated']!='سرمایه' :
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'], 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q4', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q3', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q4', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            # LastYearQ4=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            # LastYearQ4.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            # if len(LastYearQ4.val.unique().tolist())>5:
                            #     LastYearQ4.loc[:,'year']=str(int(lastYearIn)-1)
                            #     LastYearQ4.loc[:,'Yearly']=False
                            #     Final=Final.append(TempDF1Q2)
                            #     TempDF_created=[]
                            #     for index,row in LastYearQ4.iterrows():
                            #         if len(LastYearQ3[LastYearQ3['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                            #             LastYearQ1Val=LastYearQ3[LastYearQ3['Translated']==row['Translated']].val.values.tolist()[0]
                            #             LastYearQ2Val=row['val']
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val , 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q4', 'year':row['year']})
                            #         else:
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #                                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                                 , 'Qperiod':'Q4', 'year':row['year']})
                            #     Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            #     Temp_CreatedDF2.loc[:,'Yearly']=False
                            #     Final=Final.append(Temp_CreatedDF2)
                        #
                        if ('Q1' not in value) and ('Q2' in value) and ('Q3' in value) and ('Q4' in value):
                            # print('no Q1')
                            Scenario6=DY[(DY['year']==key)&(DY['Qperiod']=='Q2')]
                            TempDF2=Scenario6[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            Q3Temp=TempDF2.copy()
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if row['Translated']!='سرمایه':
                                    Q2Val=row['val']
                                    if row['Translated']!='سرمایه' :
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val), 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q1', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val), 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                    else:
                                        TempDF_created.append({'Translated':row['Translated'], 'val':Q2Val, 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q2', 'year':row['year']})
                                        TempDF_created.append({'Translated':row['Translated'], 'val':Q2Val, 'aggregated':row['aggregated']
                                                                , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                                , 'Qperiod':'Q1', 'year':row['year']})

                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            # LastYearQ3=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            # LastYearQ3.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            # if len(LastYearQ3.val.unique().tolist())>5:
                            #     LastYearQ3.loc[:,'year']=str(int(lastYearIn)-1)
                            #     LastYearQ3.loc[:,'Yearly']=False
                            #     # Final=Final.append(TempDF1Q2)
                            #     TempDF_created=[]
                            #     for index,row in LastYearQ3.iterrows():
                            #         LastYearQ2Val=row['val']
                            #         if row['Translated']!='سرمایه' :
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':(LastYearQ2Val) , 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q2', 'year':row['year']})
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':(LastYearQ2Val) , 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q2', 'year':row['year']})
                            #         else:
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val, 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q2', 'year':row['year']})
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val, 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q2', 'year':row['year']})
                            #     Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            #     Temp_CreatedDF2.loc[:,'Yearly']=False
                            #     Final=Final.append(Temp_CreatedDF2)
                            #
                            Scenario2=DY[(DY['year']==key)&(DY['Qperiod']=='Q3')]
                            TempDF2=Scenario6[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            Q4Temp=TempDF2.copy()
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(Q3Temp[Q3Temp['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه':
                                    Q1Val=Q3Temp[Q3Temp['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    # print(Q2Val)
                                    TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val) , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val) , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q3', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            # LastYearQ4=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            # LastYearQ4.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            # if len(LastYearQ4.val.unique().tolist())>5:
                            #     LastYearQ4.loc[:,'year']=str(int(lastYearIn)-1)
                            #     LastYearQ4.loc[:,'Yearly']=False
                            #     Final=Final.append(TempDF1Q2)
                            #     TempDF_created=[]
                            #     for index,row in LastYearQ4.iterrows():
                            #         if len(LastYearQ3[LastYearQ3['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                            #             LastYearQ1Val=LastYearQ3[LastYearQ3['Translated']==row['Translated']].val.values.tolist()[0]
                            #             LastYearQ2Val=row['val']
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val , 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q3', 'year':row['year']})
                            #         else:
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #                                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                                 , 'Qperiod':'Q3', 'year':row['year']})
                            #     Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            #     Temp_CreatedDF2.loc[:,'Yearly']=False
                            #     Final=Final.append(Temp_CreatedDF2)
                            #
                            Scenario2=DY[(DY['year']==key)&(DY['Qperiod']=='Q4')]
                            TempDF2=Scenario6[['Translated', 'thisPeriod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF2.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            # Q4Temp=TempDF2.copy()
                            TempDF_created=[]
                            for index,row in TempDF2.iterrows():
                                if len(Q4Temp[Q4Temp['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه':
                                    Q1Val=Q4Temp[Q4Temp['Translated']==row['Translated']].val.values.tolist()[0]
                                    Q2Val=row['val']
                                    # print(Q2Val)
                                    TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val) , 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':(Q2Val), 'aggregated':row['aggregated']
                                                            , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                                            , 'Qperiod':'Q4', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario4[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            # LastYearQ5=Scenario4[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            # LastYearQ5.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            # if len(LastYearQ5.val.unique().tolist())>5:
                            #     LastYearQ5.loc[:,'year']=str(int(lastYearIn)-1)
                            #     LastYearQ5.loc[:,'Yearly']=False
                            #     Final=Final.append(TempDF1Q2)
                            #     TempDF_created=[]
                            #     for index,row in LastYearQ5.iterrows():
                            #         if len(LastYearQ4[LastYearQ4['Translated']==row['Translated']])>0 and row['Translated']!='سرمایه' :
                            #             LastYearQ1Val=LastYearQ4[LastYearQ4['Translated']==row['Translated']].val.values.tolist()[0]
                            #             LastYearQ2Val=row['val']
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':LastYearQ2Val , 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q4', 'year':row['year']})
                            #         else:
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #                                 , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #                                 , 'Qperiod':'Q4', 'year':row['year']})
                            #     Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            #     Temp_CreatedDF2.loc[:,'Yearly']=False
                            #     Final=Final.append(Temp_CreatedDF2)
                        #
                        if ('Q1' not in value) and ('Q2' not in value) and ('Q3' not in value) and ('Q4' in value):
                            # print('OnlyQ4')
                            TempDF_created=[]
                            Scenario2=DY[(DY['year']==key)&(DY['Qperiod']=='Q4')]
                            TempDF1=Scenario2[['Translated', 'thisPeriod','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            ##Add Salane
                            TempDF1.loc[:,'Yearly']=True
                            Final=Final.append(TempDF1)
                            ####
                            TempDF1.loc[:,'Yearly']=False
                            for index,row in TempDF1.iterrows():
                                if row['Translated']!='سرمایه':
                                    newVal=row['val']
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q1', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q2', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q3', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q4', 'year':row['year']})
                                else:
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q1', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q2', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q3', 'year':row['year']})
                                    TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                                    , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                                    , 'Qperiod':'Q4', 'year':row['year']})
                            Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            Temp_CreatedDF2.loc[:,'Yearly']=False
                            Final=Final.append(Temp_CreatedDF2)
                            TempDF1=Scenario2[['Translated', 'lastYear','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            if len(TempDF1.val.unique().tolist())>5:
                                TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                                TempDF1.loc[:,'Yearly']=True
                                Final=Final.append(TempDF1)
                            # TempDF1=Scenario2[['Translated', 'lastYearThisperiod','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']]
                            # TempDF1.columns=['Translated', 'val','period','aggregated', 'toDate', 'report_id', 'firm', 'Qperiod', 'year']
                            # if len(TempDF1.val.unique().tolist())>5:
                            #     TempDF1.loc[:,'year']=str(int(lastYearIn)-1)
                            #     TempDF1.loc[:,'Yearly']=False
                            #     TempDF_created=[]
                            #     for index,row in TempDF1.iterrows():
                            #         if row['Translated']!='سرمایه':
                            #             newVal=row['val']/4
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q1', 'year':row['year']})
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q2', 'year':row['year']})
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q3', 'year':row['year']})
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':newVal , 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q4', 'year':row['year']})
                            #         else:
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q1', 'year':row['year']})
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q2', 'year':row['year']})
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q3', 'year':row['year']})
                            #             TempDF_created.append({'Translated':row['Translated'], 'val':row['val'] , 'aggregated':row['aggregated']
                            #             , 'toDate':row['toDate'], 'report_id':row['report_id'], 'firm':row['firm']
                            #             , 'Qperiod':'Q4', 'year':row['year']})
                            #     Temp_CreatedDF2=pd.DataFrame(TempDF_created)
                            #     Temp_CreatedDF2.loc[:,'Yearly']=False
                            #     Final=Final.append(Temp_CreatedDF2)
                Final = Final.sort_values(by='toDate', ascending=False)
                
                Final = Final.drop_duplicates(subset=['Translated','aggregated', 'firm', 'Qperiod', 'year','Yearly'], keep="first")
                Final.sort_values(by=['toDate'],ascending=False,inplace=True)
                Final.loc[:,'Engitem']='BSItems'
                reports=reports.drop_duplicates()
                # FinalDF.dropna(subset=['Translated'],inplace=True)
                insertValues(Final,"BS",reports)
        except Exception as E:
            print(E)
            continue
    print('All took', time.time()-start1, 'seconds.To Add Pre Ratios')