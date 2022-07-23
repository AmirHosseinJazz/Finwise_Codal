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
def Calculate_Coeeff(idOf):
    # idOf='637'
    print(idOf)
    DPS=getDPS1(idOf)
    if not DPS.empty:
        DPS['Date']=DPS['AssemblyDate'].apply(lambda x:JalaliDate.strptime(get_TrueValue(x.split(' ')[0]),'%Y/%m/%d'))
        DPS=DPS[['Value','Date']]
        DPS.columns=['DPS','Date']
    else:
        DPS=pd.DataFrame(columns=['DPS','Date'])
    IC1=getIC1(idOf)
    ICTotal=pd.DataFrame(columns=['Date','Alpha','Beta'])
    if not IC1.empty:
        IC1['Date']=IC1['AssemblyDate'].apply(lambda x:JalaliDate.strptime(get_TrueValue(x.split(' ')[0]),'%Y/%m/%d'))
        IC1['Beta']=(IC1['RetainedEarning_Final']/IC1['LastCapital'])+(IC1['Reevaluation_Final']/IC1['LastCapital'])+(IC1['Reserves_Final']/IC1['LastCapital'])
        IC1['Alpha']=(IC1['CashIncoming_Final']/IC1['LastCapital'])
        IC1=IC1[['Date','Alpha','Beta']]
        ICTotal=ICTotal.append(IC1)
    IC2=getIC2(idOf)
    if not IC2.empty:
        IC2['Date']=IC2['LastBoardMemberInvitation'].apply(lambda x:JalaliDate.strptime(get_TrueValue(x.split(' ')[0]),'%Y/%m/%d'))
        for i in ['cashIncoming','RetainedEarning', 'Reserves', 'RevaluationSurplus', 'SarfSaham','LastCapital']:
            IC2[i]=IC2[i].astype(float)
        IC2['Beta']=(IC2['RetainedEarning']/IC2['LastCapital'])+(IC2['Reserves']/IC2['LastCapital'])+(IC2['RevaluationSurplus']/IC2['LastCapital'])  
        IC2['Alpha']=(IC2['cashIncoming']/IC2['LastCapital'])  
        IC2=IC2[['Date','Alpha','Beta']]
        ICTotal=ICTotal.append(IC2)
    Total=ICTotal.append(DPS)
    Total.sort_values(by=['Date'],ascending=False,inplace=True)
    Total.fillna(0,inplace=True)
    for i in ['DPS','Alpha','Beta']:
        Total[i]=Total[i].astype(float)
    Total['Date']=Total['Date'].astype(str)
    Total=Total.groupby(['Date']).sum().reset_index()
    Total['StockID']=idOf
    Total=Total.sort_values(by=['Date'],ascending=True)
    Total.reset_index(inplace=True,drop=True)
    for index,row in Total.iterrows():
        ind1=index
        M=1
        S1=0
        S2=0
        S2C=1
        for index2,row2 in Total[Total.index>=ind1].iterrows():
            # print(row2['DPS'])
            M=M*(1+row2['Alpha']+row2['Beta'])
            S1=S1+1000*row2['Alpha']
            S2C=S2C*(1+row2['Alpha']+row2['Beta'])
        Total.loc[Total.index==ind1,'S2C']=S2C
        Total.loc[Total.index==ind1,'Delta']=S1
        Total.loc[Total.index==ind1,'Omega']=M
    for index,row in Total.iterrows():
        ind1=index
        M=1
        S1=0
        S2=0
        S2C=1
        for index3,row3 in Total[Total.index>=ind1].iterrows():
            S2=S2+row3['S2C']*row3['DPS']
        Total.loc[Total.index==ind1,'Delta']=S2+row['Delta']
    insertCoeff(Total)
def get_TrueValue(ss):
    ss=str(ss)
    ss=ss.replace('١','1')
    ss=ss.replace('٢','2')
    ss=ss.replace('٣','3')
    ss=ss.replace('۴','4')
    ss=ss.replace('۵','5')
    ss=ss.replace('۶','6')
    ss=ss.replace('٧','7')
    ss=ss.replace('٨','8')
    ss=ss.replace('٩','9')
    ss=ss.replace('٠','0')
    ss=ss.replace('۸','8')
    ss=ss.replace('۱','1')
    ss=ss.replace('۳','3')
    ss=ss.replace('۷','7')
    ss=ss.replace('۲','2')
    ss=ss.replace('۹','9')
    ss=ss.replace('۰','0')
    return ss
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
def getIC1(identifier):
    ct=0
    while ct<3:
        head = {'Accept-Profile':'public'}
        resp = requests.get('http://130.185.74.40:3000/rpc/assemblydecision_incrasecapital?a='+str(identifier),headers=head)
        if resp.status_code == 200:
            return pd.DataFrame(json.loads(resp.text))
        else:
            time.sleep(2)
            ct=ct+1
        
    return ("noData")
def getIC2(identifier):
    ct=0
    while ct<3:
        head = {'Accept-Profile':'public'}
        resp = requests.get('http://130.185.74.40:3000/rpc/boarddecision_increasecapital?a='+str(identifier),headers=head)
        if resp.status_code == 200:
            return pd.DataFrame(json.loads(resp.text))
        else:
            time.sleep(2)
            ct=ct+1
    return ("noData")
def getPHistory(identifier):
    ct=0
    while ct<3:
        head = {'Accept-Profile':'marketwatch'}
        resp = requests.get('http://185.231.115.223:3000/View_PriceHistory?firm=eq.'+str(identifier),headers=head)
        if resp.status_code == 200:
            return pd.DataFrame(json.loads(resp.text))
        else:
            time.sleep(2)
            ct=ct+1
    return ("noData")    
def getDPS1(identifier):
    ct=0
    while ct<3:
        head = {'Accept-Profile':'public'}
        resp = requests.get('http://130.185.74.40:3000/rpc/dpsallforadjust?a='+str(identifier),headers=head)
        if resp.status_code == 200:
            return pd.DataFrame(json.loads(resp.text))
        else:
            time.sleep(2)
            ct=ct+1
        
    return ("noData")  
def getallcoeffs(identifier):
    ct=0
    while ct<3:
        head = {'Accept-Profile':'public'}
        resp = requests.get('http://130.185.74.40:3000/View_adjCoeff?stockID=eq.'+str(identifier),headers=head)
        if resp.status_code == 200:
            return pd.DataFrame(json.loads(resp.text))
        else:
            time.sleep(2)
            ct=ct+1
        
    return ("noData")      
def insertCoeff(df):
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
                IF NOT EXISTS (select from public."adjustCoeff" where "stockID"=%(StockID)s and "Date"=%(Date)s) THEN
                    INSERT INTO public."adjustCoeff"(
                    "stockID", "Date", "Alpha", "Beta", "DPS","Delta","Omega")
                    VALUES (%(StockID)s, %(Date)s, %(Alpha)s, %(Beta)s, %(DPS)s,%(Delta)s,%(Omega)s);
                END IF;
            END
            $$ 

        """
        cursor.executemany(postgres_insert_query_cheif,df.to_dict(orient='records'))
        #######
        
        connection.commit()
        print('Coefff Done')
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
    DX['ID'].apply(lambda x: Calculate_Coeeff(x))            