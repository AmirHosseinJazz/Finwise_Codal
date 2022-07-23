import requests
import time
import pandas as pd
import psycopg2
import numpy as np
from scipy.stats import norm
import pandas.io.sql as psql
import json
import statistics
from pandas.io.json import json_normalize
from datetime import datetime
db_username="Jay"
db_pass="Mehrad1"
db_host="127.0.0.1"
db_port="5432"
db_database="FinWisev12"
def get_last():
    try:
        connection = psycopg2.connect(user=db_username,
                                          password=db_pass,
                                          host=db_host,
                                          port=db_port,
                                          database=db_database)
        cursor = connection.cursor()
        df = psql.read_sql("""

        select "SentTime" from codalraw."allrawReports" order by "SentTime" DESC limit 1""", connection)
        return df.SentTime.tolist()[0]
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to read links", error)
    finally:
            if(connection):
                cursor.close()
                connection.close()
def Insert_DF(DF):
    try:
            connection = psycopg2.connect(user=db_username,
                                                  password=db_pass,
                                                  host=db_host,
                                                  port=db_port,
                                                  database=db_database)
            cursor = connection.cursor() 
            postgres_insert_query = """ 
            DO 
            $$
                BEGIN
                    IF NOT EXISTS (select from codalraw."allrawReports" where "Ticker"=%(Symbol)s and "SentTime"=%(SentDateTime)s and "PublishTime"=%(PublishDateTime)s ) THEN
                        INSERT INTO codalraw."allrawReports"(
                            "TracingNo", "Ticker", reported_firm,
                            "underSupervision", title, "LetterCode",
                            "SentTime", "PublishTime", "HasHtml",
                            "HtmlUrl", "HasExcel", "HasPdf", 
                            "HasXbrl", "HasAttachment", "AttachmentUrl", 
                            "PdfUrl", "ExcelUrl", "XbrlUrl", "TedanUrl", 
                            "SuperVision_UnderSupervision", "SuperVision_AdditionalInfo", "SuperVision_Reasons")
                            VALUES (%(TracingNo)s, %(Symbol)s, %(CompanyName)s, %(UnderSupervision)s,
                            %(Title)s, %(LetterCode)s, %(SentDateTime)s, %(PublishDateTime)s,
                            %(HasHtml)s, %(Url)s, %(HasExcel)s, %(HasPdf)s, 
                            %(HasXbrl)s, %(HasAttachment)s, %(AttachmentUrl)s, %(PdfUrl)s,
                            %(ExcelUrl)s, %(XbrlUrl)s, %(TedanUrl)s, %(SuperVision.UnderSupervision)s, 
                            %(SuperVision.AdditionalInfo)s, %(SuperVision.Reasons)s);
                    END IF;
                END
            $$ 
            """
            cursor.executemany(postgres_insert_query,DF.to_dict(orient='records'))
            connection.commit()
    except (Exception, psycopg2.Error) as error :
            if(connection):
                print("Failed to update putOption", error)
    finally:
            if(connection):
                cursor.close()
                connection.close()                
if __name__=="__main__":
    lastUpdated=get_last()
    flag=True
    ct=1
    while flag:
        try:
            headers = {'User-Agent': 'Thunder Client (https://www.thunderclient.com)'}
            resp = requests.get('https://search.codal.ir/api/search/v2/q?&Audited=true&AuditorRef=-1&Category=-1&Childs=true&CompanyState=-1&CompanyType=-1&Consolidatable=true&IsNotAudited=false&Length=-1&LetterType=-1&Mains=true&NotAudited=true&NotConsolidatable=true&PageNumber='+str(ct)+'&Publisher=false&TracingNo=-1&search=true',verify=False,headers=headers,timeout=10)
            if resp.status_code != 200:
                print('Error')
                # raise ApiError('GET /tasks/ {}'.format(resp.status_code))
            else:
                js=json.loads(resp.text)
                DF=pd.json_normalize(js['Letters'])
                Range=js['Page']
                if len(DF[DF["SentDateTime"]==lastUpdated])!=0:
                    flag=False
                Insert_DF(DF)
                print('Page '+str(ct)+' Added')
                ct=ct+1
                
        except:
            continue                