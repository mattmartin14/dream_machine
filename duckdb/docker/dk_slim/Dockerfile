# Author: Matt Martin
# Date: 2023-11-18
# Desc: Docker file for minimal install needs for duckdb python scripts

FROM python:3.11-slim 

WORKDIR /usr/src/app
RUN pip install --upgrade pip
RUN pip install duckdb

RUN mkdir /root/scripts
COPY /scripts/test.py /root/scripts