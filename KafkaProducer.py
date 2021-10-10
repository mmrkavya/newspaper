#Importing all required libraries
from kafka import KafkaProducer
import numpy as np
import pandas as pd
from json
import time
from bson import json_util
import datetime
import requests

#Declaring Kafka_Broker Port
KAFKA_BROKER = 'localhost:9092'
#Declaring #Kafka Topic
KAFKA_TOPIC = 'newspaper'

try:
	producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
except Exception as e:
	print(f'Error Connecting to Kafka --> {e}')


#Reading each Record one by one so we are writing it in a loop
end=datetime.datetime.today()
start=end-datetime.timedelta(minutes=14)
while True:
	
	response=requests.get("https://newsapi.org/v2/top-headlines?from={startdatetime}&to=(enddatetime)&sortBy=publishedAt&apiKey=fe0e70f78de749aeab74f31c65636ba0&country=in".format(startdatetime=start.strftime("%Y-%m-%dT%H:%M:%S"),enddatetime=end.strftime("%Y-%m-%dT%H:%M:%S")))
	articles=dict(response.json())['articles']
	start=datetime.datetime.today()
	end=start+datetime.timedelta(minutes=14)
	for article in articles:
		print(article)
		print('--------------')
		producer.send(KAFKA_TOPIC, json.dumps(data, default=json_util.default).encode('utf-8'))
		time.sleep(2)
	time.sleep(900)
