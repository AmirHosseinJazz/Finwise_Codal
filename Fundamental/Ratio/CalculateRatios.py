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
def getRatios(identifier):
    ct=0
    while ct<3:
        head = {'Accept-Profile':'statement'}
        resp = requests.get('http://130.185.74.40:3000/View_RatioItems?firm=eq.'+str(identifier),headers=head)
        if resp.status_code == 200:
            return pd.DataFrame(json.loads(resp.text))
        else:
            time.sleep(2)
            ct=ct+1
    return ("noData")
def InsertRatios (df):
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
                IF NOT EXISTS (select from  statement."Ratio" where "firm"=%(firm)s and "aggregated"=%(aggregated)s and "year"=%(year)s and "quarter"=%(quarter)s and "RatioName"=%(Title)s) THEN
                   INSERT INTO statement."Ratio"(
	                    firm, quarter, year, aggregated, "RatioName", "RatioValue")
	                VALUES (%(firm)s,%(quarter)s,%(year)s,%(aggregated)s,%(Title)s,%(Value)s);
                END IF;
            END
            $$ 

        """
        cursor.executemany(postgres_insert_query_cheif,df.to_dict(orient='records'))
        #######
        
        connection.commit()
        print('Values Inserted')
    except(Exception, psycopg2.Error) as error:
        print(error)
        if(connection):
            cursor.close()
            connection.close()    
    finally:
        if(connection):
            cursor.close()
            connection.close()
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
if __name__=="__main__":
    DX=getAll()
    for index,row in DX.iterrows():
        try:
            print(row['ticker'])
            DD=getRatios(row['ID'])
            CashToDebt=pd.merge(DD.loc[DD['engName']=='Cash'],DD.loc[DD['engName']=='Debt'],on=['quarter','year','aggregated','firm'])
            CashToDebt.loc[:,'Value']=CashToDebt['value_x']/CashToDebt['value_y']
            CashToDebt.loc[:,'Title']='CashToDebt'
            CashToDebt=CashToDebt[['Title','quarter','year','aggregated','firm','Value']]
            InsertRatios(CashToDebt)
        except Exception as E:
            print(E)
        # EquityToAsset=pd.merge(DD.loc[DD['engName']=='Equity'],DD.loc[DD['engName']=='Asset'],on=['quarter','year','aggregated','firm'])
        # DebtToEquity=pd.merge(DD.loc[DD['engName']=='Equity'],DD.loc[DD['engName']=='Debt'],on=['quarter','year','aggregated','firm'])
        # CurrentRatio=pd.merge(DD.loc[DD['engName']=='CurrentDebt'],DD.loc[DD['engName']=='CurrentAsset'],on=['quarter','year','aggregated','firm'])
        # QuickRatio=pd.merge(pd.merge(DD.loc[DD['engName']=='Inventory'],DD.loc[DD['engName']=='CurrentAsset'],on=['quarter','year','aggregated','firm']),DD.loc[DD['engName']=='CurrentDebt'],on=['quarter','year','aggregated','firm'])