#Consumer.py
import json
from kafka import KafkaConsumer
import pymongo
from newspaper import Article
import nltk

#only first time
nltk.download('punkt')
 
client = pymongo.MongoClient("mongodb://kavya:BzSz97SmGzU9ZKL6@cluster0-shard-00-00.rhrte.mongodb.net:27017,cluster0-shard-00-01.rhrte.mongodb.net:27017,cluster0-shard-00-02.rhrte.mongodb.net:27017/newspaper?ssl=true&replicaSet=atlas-q05w9f-shard-0&authSource=admin&retryWrites=true&w=majority")
newspaperdb=client['newspaper']
collection=newspaperdb['newspaperFeed']

consumer = KafkaConsumer(bootstrap_servers='localhost:9092',auto_offset_reset='earliest', value_deserializer=lambda m: json.loads(m.decode('utf-8')))
consumer.subscribe(['newspaper'])
#dict_keys(['source', 'author', 'title', 'description', 'url', 'urlToImage', 'publishedAt', 'content','category'])
for message in consumer :
	try:
		a={}
		insertValue=message.value
		a['source']=insertValue['source']['name']
		a['author']=insertValue['author']
		a['title']=insertValue['title']
		a['publishedAt']=insertValue['publishedAt']
		a['category']=insertValue['category']
		print(f'----------cateogry {a["category"]}')
		url=insertValue['url']
		#print(url)
		article=Article(url)
		article.download()
		article.parse()
		article.nlp()
		#print(article.summary)
		a['summary']=article.summary
		a['article']=article.text
		collection.insert_one(insertValue)
		print(insertValue)
	except Exception as e:
		print(f'error while parsing, not inserted into db --> {e}')
		
 