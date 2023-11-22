
#slim is 286mb, bullseye is 1GB
#slim and slim-bullseye can't handle installing the pykernel for notebook stuff

#use slim for just py files
FROM python:3.11-slim 

#use bullseye for notebook files
#FROM python:3.11-bullseye
WORKDIR /usr/src/app
RUN pip install --upgrade pip
COPY requirements.txt .
RUN pip install -r requirements.txt

#might want to add this directory as a mount point
RUN mkdir /root/scripts
COPY /scripts/test.py /root/scripts