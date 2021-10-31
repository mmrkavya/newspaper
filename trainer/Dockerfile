#FROM alpine:3.7
FROM python:3.8-slim AS py3
WORKDIR /app


COPY . /app


#FROM rappdw/docker-java-python:openjdk1.8.0_171-python3.6.6

FROM openjdk:8-jre-alpine
COPY --from=py3 / /
RUN apk add --update \
    curl \
    && rm -rf /var/cache/apk/*

RUN apk update && apk add wget

RUN apk update && apk add bash



WORKDIR /app

ENV PYTHONUNBUFFERED 1
ENV DISPLAY=:99

# Install apt dependencies
RUN apt-get update
RUN apt install default-jdk scala git -y
#RUN python -v


ENV JAVA_HOME="/usr/lib/jvm/java-1.8-openjdk"
#spark-3.1.2-bin-hadoop3.2.tgz
# SPARK
ARG SPARK_ARCHIVE=https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz
ENV SPARK_HOME /usr/local/spark-3.1.2-bin-hadoop3.2

ENV PATH $PATH:${SPARK_HOME}/bin
RUN curl -s ${SPARK_ARCHIVE} | tar -xz -C /usr/local/

ARG MONGO_DB_JAR=https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/3.0.1/mongo-spark-connector_2.12-3.0.1.jar
ADD ${MONGO_DB_JAR} ${SPARK_HOME}/jars/
ADD https://repo1.maven.org/maven2/org/mongodb/mongodb-driver/3.8.1/mongodb-driver-3.8.1.jar ${SPARK_HOME}/jars/
#ADD https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/3.8.1/mongodb-driver-core-3.8.1.jar ${SPARK_HOME}/jars/
ADD https://repo1.maven.org/maven2/org/mongodb/bson/3.8.1/bson-3.8.1.jar ${SPARK_HOME}/jars/
COPY . /app
WORKDIR  /app


RUN pip install newspaper3k
RUN pip install  nltk
RUN pip install pyspark-stubs==2.3.0
RUN pip install  pymongo
RUN pip install uvicorn
RUN pip install  fastapi
RUN pip install  requests
RUN pip install scikit-learn
RUN pip install --no-cache-dir matplotlib pandas
ENTRYPOINT spark-submit \
--packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 \
main.py --executor-cores  2 --num-executors 1