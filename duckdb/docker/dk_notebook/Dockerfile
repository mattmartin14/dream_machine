# Author: Matt Martin
# Date: 2023-11-18
# Desc: Min requirements for container with juypter notebooks installed

FROM python:3.11-bullseye

WORKDIR /usr/src/app
RUN pip install --upgrade pip
RUN pip install duckdb ipykernel

RUN mkdir /root/scripts
COPY /scripts/duckdb_test.ipynb /root/scripts