#Importing all required libraries
from kafka import KafkaProducer
import numpy as np
import pandas as pd
from json import dumps
import time
import random
import datetime
import requests 

#Declaring Kafka_Broker Port
KAFKA_BROKER = 'localhost:9092'
#Declaring #Kafka Topic
KAFKA_TOPIC = 'newspaper'

try:
	producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER,value_serializer=lambda x: 
									 dumps(x).encode('utf-8'))
except Exception as e:
	print(f'Error Connecting to Kafka --> {e}')


#sending  one record at a time
#class NpEncoder(json.JSONEncoder):
 #	 def default(self, obj):
  #		 if isinstance(obj, np.integer):
   #		 return int(obj)
	#	 if isinstance(obj, np.floating):
	 #		 return float(obj)
	  #	 if isinstance(obj, np.ndarray):
	   #	 return obj.tolist()
		#return super(NpEncoder, self).default(obj)

#Reading each Record one by one so we are writing it in a loop
end1=datetime.datetime.today()
end=end1
start=end-datetime.timedelta(minutes=14)
categories=['business','entertainment','general','health','science','sports','technology']

while True:
		count=0
		
		for c in categories:
			response=requests.get("https://newsapi.org/v2/top-headlines?from={startdatetime}&to=(enddatetime)&sortBy=publishedAt&apiKey=fe0e70f78de749aeab74f31c65636ba0&country=in&category={category}".format(startdatetime=start.strftime("%Y-%m-%dT%H:%M:%S"),enddatetime=end.strftime("%Y-%m-%dT%H:%M:%S"),category=c))
			articles=dict(response.json())['articles']
			#start=datetime.datetime.today()
			#end=start+datetime.timedelta(minutes=14)
			if count == 0:
				end1=datetime.datetime.today()
			count=count+1
			for article in articles:
				print(article)
				article['category']=c
				print('--------------')
				producer.send(KAFKA_TOPIC, value=article)
				time.sleep(2)
		start=end+datetime.timedelta(seconds=1)
		end=end1
		time.sleep(900)
