FROM python:3.8-slim

WORKDIR /app

ENV PYTHONUNBUFFERED 1
ENV DISPLAY=:99

# Install apt dependencies
RUN apt update

# Install requirements
COPY requirement.txt requirement.txt
RUN pip install -r requirement.txt

COPY . /app


ENTRYPOINT python KafkaProducer.py