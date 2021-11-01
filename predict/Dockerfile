FROM python:3.8-slim
WORKDIR /app

ENV PYTHONUNBUFFERED 1
ENV DISPLAY=:99

RUN mkdir -p /usr/share/man/man1

RUN apt update

# Install requirements
COPY requirement.txt requirement.txt
RUN pip install -r requirement.txt
RUN python -m nltk.downloader stopwords
RUN python -m nltk.downloader punkt


COPY . /app

ENTRYPOINT python predict.py