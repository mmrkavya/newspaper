# newspaper

predicts topic of the newspaper
Newspaper classifier

It predicts (classifies) article, url or any given text . (Report with Architecture and screen shot is given in FinalReport presentation)

    Note: Please note that some parts of docker-compose yaml is  commented to run only the predict part of the code. 

## How it works!!
In the front end of the newspaper classifier will be text box in which either url of the article or the actual text can be entered.
On click of submit the text in the text box will be sent to predict api and response will be displayed to the User below the text box.
To clear the result click on clear button.

# With Docker:
### Run only kafka consumer and producer:
1. Comment predict, webapp, trainer in the docker-compose yaml
2. docker-compose up

### Run only predict and webapp
1. Comment kafka, spark-master, spark-worker, consumer, producer, producer-rssfeed , trainer
2. docker-compose up

### Run only training model
1. Comment producer,producer-rssfeed, predict, webapp
2. docker-compose up

# To run it without docker follow the following steps
### Make sure that kafka host and UI xhrhttprequest url are made localhost
1. Kafka Installation:

    i) download kafka and unzip it
     
         $ wget http://apache.claz.org/kafka/2.2.0/kafka_2.12-2.2.0.tgz
         $ tar -xvf kafka_2.12-2.2.0.tgz
         $ mv kafka_2.12-2.2.0.tgz kafka
    
    ii) install jdk
    
         $ tar -xvf kafka_2.12-2.2.0.tgz
         $ mv kafka_2.12-2.2.0.tgz kafka
    
    iii)install kafka python lib
    
         $ pip3 install kakfa-python
    
2. Spark installation:
   i. download and unzip spark
   
         $ wget https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz
         $ tar -xvf Downloads/spark-2.4.3-bin-hadoop2.7.tgz
         $ mv spark-2.4.3-bin-hadoop2.7.tgz spark
   
   ii. install Scala:
   
       $ sudo apt install scala -y
   
   iii. install pyspark:
   
       $ pip3 install pyspark==2.4.6

   iv update the .bashrc file:
   
       export PATH=$PATH:/home/<USER>/spark/bin
       export PYSPARK_PYTHON=python3
   
   After udpating run the following command:
    
       $ source .bashrc
  
  v. download the following jar and place it in /spark/jars (jars directory of spark):
    
        $ cd usr/spark/jars
        $ wget https://repo1.maven.org/maven2/org/mongodb/bson/3.8.1/bson-3.8.1.jar
        $ wget https://oss.sonatype.org/content/repositories/releases/org/mongodb/mongodb-driver-core/3.8.1/
        $ wget https://oss.sonatype.org/content/repositories/releases/org/mongodb/mongodb-driver/3.8.1/
        $ wget https://search.maven.org/remotecontent?filepath=org/mongodb/spark/mongo-spark-connector_2.11/2.3.5/mongo-spark-connector_2.11-2.3.5.jar
  
  
  3. Make sure the requirements in requirement.txt file in each of the folders are properly installed.
    
  4. For running consumer and producer part
    
    i) Run zookeeper in kafka directory in one terminal
    
        $ cd kafka/
        $ bin/zookeeper-server-start.sh config/zookeeper.properties
    
    ii) Run broker in kafka directory of another terminal
    
        $ cd kafka/
        $ bin/kafka-server-start.sh config/server.properties

    iii) Create topic in new terminal
    
        $ cd kafka/    
        $  bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic newspaper

    iv) Run consumer by
    
        $ python KafkaConsumer.py
    
    v) Run producer by
        
        $ python KafkaProducer.py
    
      if required run the other producer parallely in another terminal
      
        $ python RSSFeedProducer.py
      
   
    5. For running Trainer create a folder model in trainer directory:
    
         $ cd trainer
         $ mkdir model
         $ python main.py
 
   6. For running prediction, go to predict directory:
   
          $ cd predict 
          $ python main.py
   
  7. For running the webapp, please make sure that predict is running and place the predict api's host in the html
    
  



  
