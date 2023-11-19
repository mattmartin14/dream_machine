
#slim is 286mb, bullseye is 1GB
#slim did not have the ability to install ipykernel
#FROM python:3.11-slim 
FROM python:3.11-bullseye 
WORKDIR /usr/src/app
RUN pip install --upgrade pip
COPY requirements.txt .
RUN pip install -r requirements.txt

#might want to add this directory as a mount point
RUN mkdir /root/scripts
COPY /scripts/duckdb_test.ipynb /root/scripts