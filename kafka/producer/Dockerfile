FROM python:3.8-slim-buster

RUN apt-get update -y

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY modules/ modules
COPY main.py .