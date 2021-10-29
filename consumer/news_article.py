#only first time
from newspaper import Article
import nltk
nltk.download('punkt')

def get_article(url):
	a={}
	article=Article(url)
	article.download()
	article.parse()
	article.nlp()
	#print(article.summary)
	a['summary']=article.summary
	a['article']=article.text
	return a

	
def get_insert_doc(article):
		insertValue={}
		insertValue['source']=article['source']['name']
		insertValue['author']=article['author']
		insertValue['title']=article['title']
		insertValue['publishedAt']=article['publishedAt']	
		insertValue['category']=article['category']
		
		print(f'----------category {insertValue["category"]}')
		url=article['url']
		a=get_article(url)
		insertValue['summary']=a['summary']
		insertValue['article']=a['article']
		#tokens=preprocess_article(insertValue['article'])
		#insertValue['tokens']=tokens
		
		return insertValue
		
		#print(url)