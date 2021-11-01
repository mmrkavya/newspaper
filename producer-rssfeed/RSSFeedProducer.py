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
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
if(KAFKA_BROKER== None):
    KAFKA_BROKER='kafka:9092'
# #Declaring #Kafka Topic
KAFKA_TOPIC = 'newspaper'
#numpy,pandas,gnewclient,
#pymongo,newspaper3k
try:
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER,value_serializer=lambda x: 
                                     dumps(x).encode('utf-8'))
									 
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
						a['category']=c.lower()
						a['source']={}
						a['source']['name']=''
						a['author']=''
						a['url']=article['link']
						a['urlToImage']=''
						a['content']=''
						a['title']=''
						if(article['title']!= None):
							a['title']=article['title']
						print(article)
						a['publishedAt']=''
						producer.send(KAFKA_TOPIC, value=a)
					except Exception as e:
						print(f'exception-->{e}')
					time.sleep(2)
					
			start=end+datetime.timedelta(seconds=1)
			end=end1
			time.sleep(900)
except Exception as e:
    print('Error Connecting to Kafka')
    print(e)


        

