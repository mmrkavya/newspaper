#Consumer.py
import json
from kafka import KafkaConsumer
import pymongo

from news_article import get_insert_doc

import os
#Declaring Kafka_Broker Port
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
if(KAFKA_BROKER== None):
    KAFKA_BROKER='localhost:9092'#Declaring #Kafka Topic
#Declaring #Kafka Topic
KAFKA_TOPIC = 'newspaper'

client = pymongo.MongoClient("mongodb://kavya:BzSz97SmGzU9ZKL6@cluster0-shard-00-00.rhrte.mongodb.net:27017,cluster0-shard-00-01.rhrte.mongodb.net:27017,cluster0-shard-00-02.rhrte.mongodb.net:27017/newspaper?ssl=true&replicaSet=atlas-q05w9f-shard-0&authSource=admin&retryWrites=true&w=majority")
newspaperdb=client['newspaper']
collection=newspaperdb['newspaperFeed']

consumer = KafkaConsumer(bootstrap_servers=KAFKA_BROKER,auto_offset_reset='earliest', value_deserializer=lambda m: json.loads(m.decode('utf-8')))
consumer.subscribe([KAFKA_TOPIC])
#dict_keys(['source', 'author', 'title', 'description', 'url', 'urlToImage', 'publishedAt', 'content','category'])
for message in consumer :
    try:
        article=get_insert_doc(message.value)
        collection.insert_one(article)
        print(article)
    except Exception as e:
        print('error while parsing, not inserted into db ')
        print(e)
        
 