import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import nltk
import re

#!pip install translate 
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import string
#import translate
nltk.download('stopwords')
nltk.download('punkt')
nltk.download('wordnet')
#from nltk.stem.porter import PorterStemmer
import string
import sklearn

from tensorflow import keras
from keras.preprocessing.text import text_to_word_sequence
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.model_selection import train_test_split
from sklearn import svm
from sklearn.naive_bayes import MultinomialNB
from sklearn.naive_bayes import GaussianNB
from sklearn.metrics import roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.metrics import classification_report
from sklearn.metrics import accuracy_score

from nltk.stem import WordNetLemmatizer
lemmatizer = WordNetLemmatizer()


def remove_punctuation(text):
   # res = re.sub(r'[^\w\s]', '', text)
   translator = str.maketrans('', '', string.punctuation)
   return text.translate(translator)
   
   
def preprocess(article):   
    #to lower case
    article=article.lower()
    #removing punctuation marks
    article=remove_punctuation(str(article))
    #removing numbers
    article = re.findall(r'\D+', article)     
    tokens=[]
    #stop words list
    stop_words = set(stopwords.words("english"))
    for t in article:
       t2=nltk.word_tokenize(t)
       for i in t2:
         #removing duplicate words and stop words
            if i not in tokens and i not in stop_words:
            #tokens=tokens+[stemmer.stem(i),]
            #storing the words after lemmatizier
                tokens=tokens+[lemmatizer.lemmatize(i, pos ='v')]
    return tokens
def preprocessDataset(train_text):
       
    #word tokenization using text-to-word-sequence
    train_text= str(train_text)
    tokenized_train_set = text_to_word_sequence(train_text,filters='!"#$%&()*+,-./:;<=>?@[\\]^_`{|}~\t\n',lower=True,split=" ")
        
    #stop word removal
    stop_words = set(stopwords.words('english'))
    stopwordremove = [i for i in tokenized_train_set if not i in stop_words]
        
     
    #join words into sentence
    stopwordremove_text = ' '.join(stopwordremove)
        
        
    #remove numbers
    numberremove_text = ''.join(c for c in stopwordremove_text if not c.isdigit())
       
        
    #--Stemming--
    stemmer= PorterStemmer()

    stem_input=nltk.word_tokenize(numberremove_text)
    stem_text=' '.join([stemmer.stem(word) for word in stem_input])
        
        
    lemmatizer = WordNetLemmatizer()

    def get_wordnet_pos(word):
        """Map POS tag to first character lemmatize() accepts"""
        tag = nltk.pos_tag([word])[0][1][0].upper()
        tag_dict = {"J": wordnet.ADJ,
                "N": wordnet.NOUN,
                "V": wordnet.VERB,
                "R": wordnet.ADV}

        return tag_dict.get(tag, wordnet.NOUN)

    lem_input = nltk.word_tokenize(stem_text)
    lem_text= ' '.join([lemmatizer.lemmatize(w, get_wordnet_pos(w)) for w in lem_input])
        
    return lem_text

spark = SparkSession.\
builder.\
appName("pyspark-notebook2").\
config("spark.mongodb.input.uri=mongodb://kavya:BzSz97SmGzU9ZKL6@cluster0-shard-00-00.rhrte.mongodb.net:27017,cluster0-shard-00-01.rhrte.mongodb.net:27017,cluster0-shard-00-02.rhrte.mongodb.net:27017/newspaper?ssl=true&replicaSet=atlas-q05w9f-shard-0&authSource=admin&retryWrites=true&w=majority").\
config("spark.mongodb.output.uri=mongodb://kavya:BzSz97SmGzU9ZKL6@cluster0-shard-00-00.rhrte.mongodb.net:27017,cluster0-shard-00-01.rhrte.mongodb.net:27017,cluster0-shard-00-02.rhrte.mongodb.net:27017/newspaper?ssl=true&replicaSet=atlas-q05w9f-shard-0&authSource=admin&retryWrites=true&w=majority").\
config("spark.mongodb.input.database","newspaper").\
config("spark.mongodb.input.collection","newspaperFeed"). \
config("spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.11:2.3.5").\
getOrCreate()

sc=spark.sparkContext
sqlContext=SQLContext(sc)
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
df.printSchema()
df.registerTempTable("newspaperFeed")
result_data=sqlContext.sql("SELECT * from newspaperFeed")
result_data.show()
result_data_rdd=sc.parallelize(result_data.collect())
transformRDD=result_data_rdd.map(lambda article: (article['category'],preprocess(article['article'])))

try:
	print(type(transformRDD.toDF()))
except Exception as e:
	print(hasattr(transformRDD,"toDF"))
	print(type(transformRDD.toDF()))
#first time the toDF is not working so just catching the exception and doing it again	
t=transformRDD.toDF()

t.show()
text = t['article']
category = t['Category']
print(text.head())
 
X_train, X_test, Y_train, Y_test = train_test_split(text,category, test_size = 0.3, random_state = 60,shuffle=True, stratify=category)

print(len(X_train))
print(len(X_test))
nb = Pipeline([('tfidf', TfidfVectorizer()),
               ('clf', MultinomialNB()),
              ])
nb.fit(X_train,Y_train)

test_predict = nb.predict(X_test)

train_accuracy = round(nb.score(X_train,Y_train)*100)
test_accuracy =round(accuracy_score(test_predict, Y_test)*100)


print("Naive Bayes Train Accuracy Score : {}% ".format(train_accuracy ))
print("Naive Bayes Test Accuracy Score  : {}% ".format(test_accuracy ))
print()
print(classification_report(test_predict, Y_test, target_names=target_category))
'''
sample output of the file
vagrant@vagrant:~$ python3 /vagrant/DataPreprocessing.py
[nltk_data] Downloading package stopwords to
[nltk_data]     /home/vagrant/nltk_data...
[nltk_data]   Package stopwords is already up-to-date!
[nltk_data] Downloading package punkt to /home/vagrant/nltk_data...
[nltk_data]   Package punkt is already up-to-date!
[nltk_data] Downloading package wordnet to /home/vagrant/nltk_data...
[nltk_data]   Package wordnet is already up-to-date!
Ivy Default Cache set to: /home/vagrant/.ivy2/cache
The jars for the packages stored in: /home/vagrant/.ivy2/jars
:: loading settings :: url = jar:file:/home/vagrant/.local/lib/python3.6/site-packages/pyspark/jars/ivy-2.4.0.jar!/org/apache/ivy/core/settings/ivysettings.xml
org.mongodb.spark#mongo-spark-connector_2.11 added as a dependency
:: resolving dependencies :: org.apache.spark#spark-submit-parent-7da73bf9-f0c9-4db4-adf6-3ad26312ffb5;1.0
        confs: [default]
        found org.mongodb.spark#mongo-spark-connector_2.11;2.3.5 in central
        found org.mongodb#mongo-java-driver;3.12.5 in central
:: resolution report :: resolve 617ms :: artifacts dl 19ms
        :: modules in use:
        org.mongodb#mongo-java-driver;3.12.5 from central in [default]
        org.mongodb.spark#mongo-spark-connector_2.11;2.3.5 from central in [default]
        ---------------------------------------------------------------------
        |                  |            modules            ||   artifacts   |
        |       conf       | number| search|dwnlded|evicted|| number|dwnlded|
        ---------------------------------------------------------------------
        |      default     |   2   |   0   |   0   |   0   ||   2   |   0   |
        ---------------------------------------------------------------------
:: retrieving :: org.apache.spark#spark-submit-parent-7da73bf9-f0c9-4db4-adf6-3ad26312ffb5
        confs: [default]
        0 artifacts copied, 2 already retrieved (0kB/23ms)
21/10/17 17:05:36 WARN Utils: Your hostname, vagrant resolves to a loopback address: 127.0.1.1; using 10.0.2.15 instead (on interface eth0)
21/10/17 17:05:36 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
21/10/17 17:05:36 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
root
 |-- _id: struct (nullable = true)
 |    |-- oid: string (nullable = true)
 |-- article: string (nullable = true)
 |-- author: string (nullable = true)
 |-- category: string (nullable = true)
 |-- publishedAt: string (nullable = true)
 |-- source: string (nullable = true)
 |-- summary: string (nullable = true)
 |-- title: string (nullable = true)

+--------------------+--------------------+--------------------+--------+--------------------+------------------+--------------------+--------------------+
|                 _id|             article|              author|category|         publishedAt|            source|             summary|               title|
+--------------------+--------------------+--------------------+--------+--------------------+------------------+--------------------+--------------------+
|[616a6d7c53ea8b35...|Petrol and diesel...|            Livemint|business|2021-10-16T01:51:52Z|          Livemint|Petrol and diesel...|Petrol, diesel pr...|
|[616a6d7d53ea8b35...|The Pandora Paper...|          Ritu Sarin|business|2021-10-16T01:04:50Z|The Indian Express|DHFL promoters Ka...|Pandora Papers: H...|
|[616a6d7e53ea8b35...|Feature China/Bar...|        Isabelle Lee|business|2021-10-15T14:07:12Z|  Business Insider|Feature China/Bar...|China: Evergrande...|
|[616a6d7f53ea8b35...|Smartphone shipme...|             Michail|business|2021-10-15T13:42:01Z|      GSMArena.com|Smartphone shipme...|Canalys: Samsung ...|
|[616a6d7f53ea8b35...|New chassis, perf...|       Pearl Daniels|business|2021-10-15T13:33:17Z|      Rushlane.com|In comparison, th...|New KTM RC 200 Ar...|
|[616a6d8053ea8b35...|Mukesh Ambani-led...|       Sagar Malviya|business|2021-10-15T13:19:00Z|The Times of India|This is the first...|Reliance to acqui...|
|[616a6d8053ea8b35...|Tata Group compan...|                null|business|2021-10-15T13:18:37Z|          CNBCTV18|Tatas are betting...|Bottomline: Big b...|
|[616a6d8153ea8b35...|Representative im...|                null|business|2021-10-15T12:50:29Z|      Moneycontrol|Representative im...|Tala raises $145 ...|
|[616a6d8253ea8b35...|Though cryptocurr...|                null|business|2021-10-15T12:41:58Z|      Moneycontrol|While these figur...|Indian crypto spa...|
|[616a6d8353ea8b35...|Since the announc...|Vaibhav Gautam Ba...|business|2021-10-15T12:31:31Z|      Zee Business|Rakesh Jhunjhunwa...|These two newly b...|
|[616a6d8553ea8b35...|Shiba Inu, one of...|         Bilal Jafar|business|2021-10-15T12:23:00Z|  Finance Magnates|Shiba Inu, one of...|Shiba Inu (SHIB):...|
|[616a6d8653ea8b35...|Looking to cash i...|            Livemint|business|2021-10-15T11:43:42Z|          Livemint|Looking to cash i...|L&T forays into o...|
|[616a6d8853ea8b35...|Check out the bes...|       Kshitij Rawat|business|2021-10-15T11:30:29Z|    GaadiWaadi.com|Maruti Vitara Bre...|Best Discounts On...|
|[616a6d8853ea8b35...|Festive season 20...|        HT Auto Desk|business|2021-10-15T10:45:03Z|   Hindustan Times|Recently, TVS Mot...|New TVS Apache RT...|
|[616a6d8853ea8b35...|Nomura Holdings L...|           Bloomberg|business|2021-10-15T10:21:12Z|          Livemint|Nomura Holdings L...|Nomura sees resil...|
|[616a6d8a53ea8b35...|Check out our on-...|       Kshitij Rawat|business|2021-10-15T10:00:45Z|    GaadiWaadi.com|MG Astor Vs Marut...|MG Astor Vs Marut...|
|[616a6d8b53ea8b35...|live bse live

ns...|                null|business|2021-10-15T09:57:09Z|      Moneycontrol|Going ahead, with...|Dussehra Picks: E...|
|[616a6d8c53ea8b35...|(Image: Reuters)
...|                null|business|2021-10-15T09:51:28Z|      Moneycontrol|(Image: Reuters)B...|Bitcoin tops $60,...|
|[616a6d8d53ea8b35...|US Senator Elizab...|                null|business|2021-10-15T09:35:00Z|         NDTV News|Warren, a promine...|US Senator Seeks ...|
|[616a6d8d53ea8b35...|Nagaraj Shetti, T...|                null|business|2021-10-16T03:23:57Z|      Moneycontrol|Nagaraj Shetti, T...|These 6 stocks lo...|
+--------------------+--------------------+--------------------+--------+--------------------+------------------+--------------------+--------------------+
only showing top 20 rows

True
21/10/17 17:05:59 WARN TaskSetManager: Stage 3 contains a task of very large size (2185 KB). The maximum recommended task size is 100 KB.
<class 'pyspark.sql.dataframe.DataFrame'>
21/10/17 17:06:03 WARN TaskSetManager: Stage 4 contains a task of very large size (2185 KB). The maximum recommended task size is 100 KB.
21/10/17 17:06:07 WARN TaskSetManager: Stage 5 contains a task of very large size (2185 KB). The maximum recommended task size is 100 KB.
+--------+--------------------+
|      _1|                  _2|
+--------+--------------------+
|business|[petrol, diesel, ...|
|business|[pandora, paper, ...|
|business|[feature, chinaba...|
|business|[smartphone, ship...|
|business|[new, chassis, pe...|
|business|[mukesh, ambanile...|
|business|[tata, group, com...|
|business|[representative, ...|
|business|[though, cryptocu...|
|business|[since, announcem...|
|business|[shiba, inu, one,...|
|business|[look, cash, onli...|
|business|[check, best, dea...|
|business|[festive, season,...|
|business|[nomura, hold, lt...|
|business|[check, onpaper, ...|
|business|[live, bse, nse, ...|
|business|[image, reuters, ...|
|business|[us, senator, eli...|
|business|[nagaraj, shetti,...|
+--------+--------------------+
only showing top 20 rows
'''

