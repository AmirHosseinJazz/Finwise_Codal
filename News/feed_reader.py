import feedparser
import pandas as pd
import webbrowser
from khayyam import *
# from hazm import *
import ssl
import html2text
import datetime
import psycopg2
import pandas.io.sql as psql

db_username="Jay"
db_pass="Mehrad1"
db_host="127.0.0.1"
db_port="5432"
db_database="FinWisev12"
ssl._create_default_https_context = ssl._create_unverified_context
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
                    IF NOT EXISTS (select from news.feed where "source"=%(source)s and "href"=%(href)s and "date"=%(englishDate)s ) THEN
                       INSERT INTO news.feed(
	                        title, source, href, date, summary,"persianDate")
                            VALUES (%(title)s, %(source)s,%(href)s, %(englishDate)s, %(summary)s,
                            %(persianDate)s);
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
    
    links=[
    # "https://donya-e-eqtesad.com/fa/feeds/?",
    # 'https://www.isna.ir/rss/tp/34',
    'https://www.cbi.ir/NewsRss.aspx?ln=fa',
    'https://tse.ir/news/rss.xml',
    'https://www.ifb.ir/RSSForNews.aspx',
    'https://www.sena.ir/rss',
    'http://boursepress.ir/page/rss'
    ]
    AllTitles=[]
    for c in links:
        feed = feedparser.parse(c)
        feed_entries = feed.entries
        for x in feed_entries:
            title=x['title_detail']['value']
            href=x['links'][0]['href']
            date=x['published']
            # summary=html2text.html2text(x['summary'])
            AllTitles.append({'title':title,'href':href,'date':date,'summary':'summary'})
    DF=pd.DataFrame(AllTitles).sort_values(by=['date'])
    DF2=DF[DF['href'].str.contains('ifb.ir')]
    DF3=DF[DF['href'].str.contains('tse.ir')]
    DF3['source']='سازمان بورس'
    DF2['source']='سازمان فرابورس'
    DF2['englishDate']=DF2['date'].apply(lambda x:JalaliDatetime(x.split(' ')[0].split('/')[2],x.split(' ')[0].split('/')[1],x.split(' ')[0].split('/')[0],int(x.split(' ')[1].split(':')[0]),int(x.split(' ')[1].split(':')[1]),int(x.split(' ')[1].split(':')[2])).todatetime())
    DF3['englishDate']=DF3['date'].apply(lambda x: datetime.datetime.strptime(x, '%m/%d/%Y  %I:%M:%S %p'))
    DF4=DF[DF['href'].str.contains('cbi.ir')]
    DF4['source']='بانک مرکزی'
    DF4['englishDate']=DF4['date'].apply(lambda x:datetime.datetime.strptime(x, '%a, %d %b %Y %H:%M:%S %Z'))
    # DF5=DF[DF['href'].str.contains('isna.ir')]
    # DF5['englishDate']=DF5['date'].apply(lambda x:datetime.datetime.strptime(x, '%a, %d %b %Y %H:%M:%S %Z'))
    # DF5['source']='ایسنا'
    # DF6=DF[DF['href'].str.contains('tejaratnews')]
    # DF6['englishDate']=DF6['date'].apply(lambda x:datetime.datetime.strptime(x[:-6], '%a, %d %b %Y %H:%M:%S'))
    # DF6['source']='تجارت نیوز'
    # DF7=DF[DF['href'].str.contains('donya-e-eqtesad')]
    # DF7['englishDate']=DF7['date'].apply(lambda x:datetime.datetime.strptime(x[:-6], '%a, %d %b %Y %H:%M:%S'))
    # DF7['source']='دنیای اقتصاد'
    DF8=DF[DF['href'].str.contains('sena.ir')]
    DF8['englishDate']=DF8['date'].apply(lambda x:datetime.datetime.strptime(x[:-4], '%a, %d %b %Y %H:%M:%S'))
    DF8['source']='پایگاه خبری بازار سرمایه'
    DF9=DF[DF['href'].str.contains('boursepress.ir')]
    DF9['englishDate']=DF9['date'].apply(lambda x:datetime.datetime.strptime(x[:-4], '%a, %d %b %Y %H:%M:%S'))
    DF9['source']='پایگاه اطلاع رسانی بورس پرس'
    DFALL=DF2.append(DF3)
    DFAL=DFALL.append(DF4)
    # DFALL=DFALL.append(DF5)
    # DFALL=DFALL.append(DF6)
    # DFALL=DFALL.append(DF7)        
    DFALL=DFALL.append(DF8)  
    DFALL=DFALL.append(DF9)  
    DFALL.replace('!','',regex=True,inplace=True)
    DFALL.replace('\[]','',regex=True,inplace=True)
    DFALL['summary']=DFALL['summary'].str.replace(r"\([^()]*\)","")
    DFALL['persianDate']=DFALL['englishDate'].apply(lambda x: JalaliDatetime(x).strftime('%Y/%m/%d %H:%M:%S'))
    DFALL['englishDate']=DFALL['englishDate'].astype(str)
    DFALL['persianDate']=DFALL['persianDate'].astype(str)
    Insert_DF(DFALL)
    print('Done')