#Consumer.py
import json
from kafka import KafkaConsumer
import pymongo

client = pymongo.MongoClient("mongodb://kavya:BzSz97SmGzU9ZKL6@cluster0-shard-00-00.rhrte.mongodb.net:27017,cluster0-shard-00-01.rhrte.mongodb.net:27017,cluster0-shard-00-02.rhrte.mongodb.net:27017/newspaper?ssl=true&replicaSet=atlas-q05w9f-shard-0&authSource=admin&retryWrites=true&w=majority")
newspaperdb=client['newspaper']
collection=newspaperdb['newspaperFeed']

consumer = KafkaConsumer(bootstrap_servers='localhost:9092',auto_offset_reset='earliest', value_deserializer=lambda m: json.loads(m.decode('utf-8')))
consumer.subscribe(['newspaper'])
for message in consumer :
    collection.insert_one(message.value)
    print(message.value)