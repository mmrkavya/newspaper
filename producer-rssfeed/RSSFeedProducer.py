#Importing all required libraries
from kafka import KafkaProducer
import numpy as np
import pandas as pd
from json import dumps
import time
import random
import datetime
import requests
import os
from gnewsclient import gnewsclient
#Declaring Kafka_Broker Port
# KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_BROKER = os.getenv("KAFKA_BROKER")==None ? 'localhost:9092':os.getenv("KAFKA_BROKER")
# #Declaring #Kafka Topic
KAFKA_TOPIC = 'newspaper'
#numpy,pandas,gnewclient,
#pymongo,newspaper3k
try:
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER,value_serializer=lambda x: 
                                     dumps(x).encode('utf-8'))
except Exception as e:
    print(f'Error Connecting to Kafka --> {e}')


end1=datetime.datetime.today()
end=end1
start=end-datetime.timedelta(minutes=14)
categories=['Business','Entertainment','Health','Science','Sports','Technology']

while True:
        count=0
        
        for c in categories:
            client = gnewsclient.NewsClient(language='english',
                                location='india',topic=c,#,'Technology','Business','Sports','Science'],
                                max_results=10)
 
            news_list = client.get_news()
            if count == 0:
                end1=datetime.datetime.today()
            count=count+1
            for article in news_list:
                
                try:
                    print('--------------')
                    a={}
                    a['source']={}
                    a['source']['name']=''
                    a['author']=''
                    a['url']=article['link']
                    a['urlToImage']=''
                    a['content']=''
                    a['cagetory']=c.lower()
                    print(article)
                    producer.send(KAFKA_TOPIC, value=a)
                except Exception as e:
                    print(f'exception-->{e}')
                time.sleep(2)
                
        start=end+datetime.timedelta(seconds=1)
        end=end1
        time.sleep(900)
        

