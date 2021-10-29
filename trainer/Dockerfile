FROM openjdk:8-jre-alpine

RUN apk add --update \
    curl \
    && rm -rf /var/cache/apk/*

RUN apk update && apk add wget

RUN apk update && apk add bash

ENV PYTHONUNBUFFERED=1
RUN apk add --update --no-cache python3 && ln -sf python3 /usr/bin/python
RUN python3 -m ensurepip
RUN pip3 install --no-cache --upgrade pip setuptools

WORKDIR /app

ENV PYTHONUNBUFFERED 1
ENV DISPLAY=:99

# Install apt dependencies
RUN apk update

# Install requirements
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

#spark-3.1.2-bin-hadoop3.2.tgz
# SPARK
ARG SPARK_ARCHIVE=https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz
ENV SPARK_HOME /usr/local/spark-3.1.2-bin-hadoop3.2

ENV PATH $PATH:${SPARK_HOME}/bin
RUN curl -s ${SPARK_ARCHIVE} | tar -xz -C /usr/local/
ARG MONGO_DB_JAR=https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.11/2.3.5/mongo-spark-connector_2.11-2.3.5.jar
RUN curl -s ${MONGO_DB_JAR} | mv ${SPARK_HOME}/jar/
WORKDIR /app

COPY . /app

ENTRYPOINT python3 model_training.py