#only first time
from newspaper import Article
import nltk
nltk.download('punkt')

def get_article(url):
	article=Article(url)
	article.download()
	article.parse()
	article.nlp()
	#print(article.summary)
	a=article.text
	return a

