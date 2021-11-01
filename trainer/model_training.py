import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from sklearn.preprocessing import LabelEncoder  
import nltk
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.model_selection import train_test_split
from sklearn import svm
from sklearn.naive_bayes import MultinomialNB
from sklearn.naive_bayes import GaussianNB
from sklearn.metrics import roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.metrics import classification_report
from sklearn.metrics import accuracy_score
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from news_article import get_insert_doc,get_article
from Preprocess import preprocess

nltk.download('punkt')

import string
import pandas as pd
import pickle
import Preprocess as pp

X_train=None
Y_train=None
nb=None
model_path='./model/_model.pickle'

def initial_model_training():
    spark = SparkSession.\
    builder.\
    appName("pyspark-notebook2").\
    config("spark.mongodb.input.uri=mongodb://kavya:BzSz97SmGzU9ZKL6@cluster0-shard-00-00.rhrte.mongodb.net:27017,cluster0-shard-00-01.rhrte.mongodb.net:27017,cluster0-shard-00-02.rhrte.mongodb.net:27017/newspaper?ssl=true&replicaSet=atlas-q05w9f-shard-0&authSource=admin&retryWrites=true&w=majority").\
    config("spark.mongodb.output.uri=mongodb://kavya:BzSz97SmGzU9ZKL6@cluster0-shard-00-00.rhrte.mongodb.net:27017,cluster0-shard-00-01.rhrte.mongodb.net:27017,cluster0-shard-00-02.rhrte.mongodb.net:27017/newspaper?ssl=true&replicaSet=atlas-q05w9f-shard-0&authSource=admin&retryWrites=true&w=majority").\
    #config("spark.mongodb.input.database","newspaper").\
    #config("spark.mongodb.input.collection","newspaperFeed"). \
    #config("spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.11:3.1.2").\
    getOrCreate()

    sc=spark.sparkContext
    sqlContext=SQLContext(sc)
    df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database","newspaper").option("collection", "newspaperFeed").load()
    df.printSchema()
    df.registerTempTable("newspaperFeed")
    result_data=sqlContext.sql("SELECT * from newspaperFeed")
    result_data.show()
    result_data_rdd=sc.parallelize(result_data.collect())
    transformRDD=result_data_rdd.map(lambda article: dict({'category':article['category'],'article':pp.preprocess(article['article']+" "+article['summary']+" "+article["title"])}))

    try:
        print(type(transformRDD.toDF()))
    except Exception as e:
        print(hasattr(transformRDD,"toDF"))
        print(type(transformRDD.toDF()))
    #first time the toDF is not working so just catching the exception and doing it again   
    t=transformRDD.toDF().toPandas()
    target_category = t['category'].unique()
    print((t['category']))
    encoder = LabelEncoder()
    t['categoryId'] = encoder.fit_transform(t['category'])
    text = t['article']
    category = t['category']

    print(t)

    t.groupby('category').category.count().plot.bar(ylim=0)

    X_train, X_test, Y_train, Y_test = train_test_split(text,category, test_size = 0.3, random_state = 60,shuffle=True, stratify=category)
    print("--------------------X_train"+X_train)
    print("---------------Y_train-------------")
    print("Y_train")
    nb = Pipeline([('tfidf', TfidfVectorizer()),
                ('clf',MultinomialNB()),
                ])
    nb.fit(X_train,Y_train)

    test_predict = nb.predict(X_test)

    train_accuracy = round(nb.score(X_train,Y_train)*100)
    test_accuracy =round(accuracy_score(test_predict, Y_test)*100)


    print("Decision Tree Train Accuracy Score : {}% ".format(train_accuracy ))
    print("Decision Tree Test Accuracy Score  : {}% ".format(test_accuracy ))

    print(classification_report(test_predict, Y_test, target_names=target_category))
    with open(model_path, "wb") as file:
        pickle.dump(nb, file)
        
    print(nb.predict([pp.preprocess("cricket")]))



def retrain(data):
    preprocessed_data=[]
    try:
        global X_train,Y_train
        
        for d in data: 
            data=get_insert_doc(dict(data))
            preprocessed_data=preprocessed_data+[{'category':d['category'],'article':pp.preprocess(d['article']+" "+d['summary']+" "+d["title"])},]
            X_train=X_train+[d['category'],]
            Y_train=Y_train+[d['article'],]
     
        global nb
        nb.fit(X_train,Y_train)
        with open(model_path, "wb") as file:
            pickle.dump(nb, file)
        #print(nb.predict([pp.preprocess("medicine")]))
        print("retrained successfully")
    except Exception as e:
        print("Error while retraining")

def predict(url):
    if url is not None:
        try:
            model = pickle.load(open(model_path,'rb'))
            article=preprocess(get_article(url)['article'])
        except Exception as e:
            print("exception occured while parsing "+e)
            return {"err": e}
    output = {"category": (model.predict([article,]))[0]}
    return output
	
    


'''
retrain([{'article': "Petrol and diesel prices on Saturday rallied to their highest ever levels across the country, as fuel rates were hiked again by 35 paise a litre.\n\nThe price of petrol in Delhi rose to its highest-ever level of ₹105.49 a litre and ₹111.43 per litre in Mumbai, according to a price notification of state-owned fuel retailers. In Mumbai, diesel now comes for ₹102.15 a litre; while in Delhi, it costs ₹94.22.\n\nThis is the third straight day of 35 paise per litre increase in petrol and diesel prices. There was no change in rates on October 12 and 13.\n\nPetrol and diesel have been priced at ₹106.10 and ₹97.33 respectively in West Bengal's Kolkata and ₹102.70 and ₹98.59 in Chennai respectively.\n\nIn Bengaluru, petrol is available at ₹109.16 per litre and diesel at ₹100.00 and in Hyderabad, one litre of petrol is now available at ₹109.73 and diesel cost ₹102.80 for one litre of diesel.\n\nSince the ending of a three-week-long hiatus in rate revision in the last week of September, this is the 15th increase in petrol price and the 18th time that diesel rates have gone up.\n\nWhile petrol price in most of the country is already above ₹100-a-litre mark, diesel rates have crossed that level in a dozen states, including Madhya Pradesh, Rajasthan, Odisha, Andhra Pradesh, Telangana, Gujarat, Maharashtra, Chhattisgarh, Bihar, Kerala, Karnataka and Leh. Prices differ from state to state depending on the incidence of local taxes.\n\nShedding the modest price change policy, state-owned fuel retailers have since October 6 started passing on the larger incidence of cost to consumers. This is because the international benchmark Brent crude is trading at USD 84.61 per barrel for the first time in seven years.\n\nA month back, Brent was trading at USD 73.51. Being a net importer of oil, India prices petrol and diesel at rates equivalent to international prices. The surge in international oil prices ended a three-week hiatus in rates on September 28 for petrol and September 24 for diesel.\n\nSince then, diesel rates have gone up by ₹5.25 per litre and petrol price has increased by ₹4.25. Before that, the petrol price was increased by ₹11.44 a litre between May 4 and July 17. Diesel rate had gone up by ₹9.14 during this period.\n\nSubscribe to Mint Newsletters * Enter a valid email * Thank you for subscribing to our newsletter.",
 'author': 'Livemint',
 'category': 'business',
 'publishedAt': '2021-10-16T01:51:52Z',
 'source': 'Livemint',
 'summary': 'Petrol and diesel prices on Saturday rallied to their highest ever levels across the country, as fuel rates were hiked again by 35 paise a litre.\nThis is the third straight day of 35 paise per litre increase in petrol and diesel prices.\nBeing a net importer of oil, India prices petrol and diesel at rates equivalent to international prices.\nThe surge in international oil prices ended a three-week hiatus in rates on September 28 for petrol and September 24 for diesel.\nSince then, diesel rates have gone up by ₹5.25 per litre and petrol price has increased by ₹4.25.',
 'title': 'Petrol, diesel prices today: Fuel rates hiked, diesel crosses ₹94 mark in Delhi - Mint'},])
    

2021-10-23 11:24:51.455588: W tensorflow/stream_executor/platform/default/dso_loader.cc:64] Could not load dynamic library 'libcudart.so.11.0'; dlerror: libcudart.so.11.0: cannot open shared object file: No such file or directory
2021-10-23 11:24:51.455804: I tensorflow/stream_executor/cuda/cudart_stub.cc:29] Ignore above cudart dlerror if you do not have a GPU set up on your machine.
[nltk_data] Downloading package stopwords to
[nltk_data]     /home/vagrant/nltk_data...
[nltk_data]   Package stopwords is already up-to-date!
[nltk_data] Downloading package punkt to /home/vagrant/nltk_data...
[nltk_data]   Package punkt is already up-to-date!
[nltk_data] Downloading package wordnet to /home/vagrant/nltk_data...
[nltk_data]   Package wordnet is already up-to-date!
[nltk_data] Downloading package averaged_perceptron_tagger to
[nltk_data]     /home/vagrant/nltk_data...
[nltk_data]   Package averaged_perceptron_tagger is already up-to-
[nltk_data]       date!
Ivy Default Cache set to: /home/vagrant/.ivy2/cache
The jars for the packages stored in: /home/vagrant/.ivy2/jars
:: loading settings :: url = jar:file:/home/vagrant/.local/lib/python3.6/site-packages/pyspark/jars/ivy-2.4.0.jar!/org/apache/ivy/core/settings/ivysettings.xml
org.mongodb.spark#mongo-spark-connector_2.11 added as a dependency
:: resolving dependencies :: org.apache.spark#spark-submit-parent-e4574cce-f685-450e-ad1f-5bec319e5427;1.0
        confs: [default]
        found org.mongodb.spark#mongo-spark-connector_2.11;2.3.5 in central
        found org.mongodb#mongo-java-driver;3.12.5 in central
:: resolution report :: resolve 739ms :: artifacts dl 24ms
        :: modules in use:
        org.mongodb#mongo-java-driver;3.12.5 from central in [default]
        org.mongodb.spark#mongo-spark-connector_2.11;2.3.5 from central in [default]
        ---------------------------------------------------------------------
        |                  |            modules            ||   artifacts   |
        |       conf       | number| search|dwnlded|evicted|| number|dwnlded|
        ---------------------------------------------------------------------
        |      default     |   2   |   0   |   0   |   0   ||   2   |   0   |
        ---------------------------------------------------------------------
:: retrieving :: org.apache.spark#spark-submit-parent-e4574cce-f685-450e-ad1f-5bec319e5427
        confs: [default]
        0 artifacts copied, 2 already retrieved (0kB/17ms)
21/10/23 11:25:18 WARN Utils: Your hostname, vagrant resolves to a loopback address: 127.0.1.1; using 10.0.2.15 instead (on interface eth0)
21/10/23 11:25:18 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
21/10/23 11:25:20 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
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
21/10/23 11:25:49 WARN TaskSetManager: Stage 3 contains a task of very large size (2185 KB). The maximum recommended task size is 100 KB.
/home/vagrant/.local/lib/python3.6/site-packages/pyspark/sql/session.py:366: UserWarning: Using RDD of dict to inferSchema is deprecated. Use pyspark.sql.Row instead
  warnings.warn("Using RDD of dict to inferSchema is deprecated. "
<class 'pyspark.sql.dataframe.DataFrame'>
21/10/23 11:26:06 WARN TaskSetManager: Stage 4 contains a task of very large size (2185 KB). The maximum recommended task size is 100 KB.
21/10/23 11:26:13 WARN TaskSetManager: Stage 5 contains a task of very large size (2185 KB). The maximum recommended task size is 100 KB.
0        business
1        business
2        business
3        business
4        business
          ...
577    technology
578    technology
579    technology
580    technology
581    technology
Name: category, Length: 582, dtype: object
                                               article    category  categoryId
0     petrol diesel price saturday rally highest ev...    business           0
1     pandora paper contain fresh lead indian inves...    business           0
2     feature chinabarcroft media via getty image c...    business           0
3     smartphone shipments julyseptember period fel...    business           0
4     new chassis performance upgrade enhance safet...    business           0
..                                                 ...         ...         ...
577   twitch passwords expose livestreaming service...  technology           6
578   windows insiders get fix amd performance issu...  technology           6
579   nakuul mehtas son sufi wife jankee visit set ...  technology           6
580   animal cross new horizons direct absolutely s...  technology           6
581   htc unveil vive flow portable vr headset use ...  technology           6

[582 rows x 3 columns]
Decision Tree Train Accuracy Score : 95%
Decision Tree Test Accuracy Score  : 89%
               precision    recall  f1-score   support

     business       1.00      0.88      0.94        41
entertainment       1.00      0.84      0.92        32
      general       0.39      0.75      0.51        12
       health       0.92      0.96      0.94        23
      science       1.00      0.88      0.94        26
       sports       1.00      0.88      0.94        26
   technology       0.79      1.00      0.88        15

     accuracy                           0.89       175
    macro avg       0.87      0.89      0.87       175
 weighted avg       0.93      0.89      0.90       175

['sports']
'''
