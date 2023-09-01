#!/bin/bash

: '
    Author: Matt Martin
    Date: 2023-08-30
    Desc: Installs Airflow locally
    -- Assumptions before you run this:
        1) You have created a directory for airflow to install into
            In the script below, update the AIRFLOW_HOME variable to your airflow directory path
        2) You have Python version 3.10 installed (Airflow currently does not work with 3.11)
'


## note: need to set the AIRFLOW_HOME environ variable; otherwise a ~/airflow folder is automatically created;
## dags folder should be stored in AIRFLOW_HOME path

export AIRFLOW_HOME=~/dream_machine/airflow_workspace

# this flag keeps airflow from showing a bunch of pre-loaded example dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False;

cd;

cd $AIRFLOW_HOME;

## add dags directory (if not exists)
if [ ! -d "dags" ]; then
  mkdir -p "dags"
  echo "Dags directory created"
fi


echo "Building virtual environment";

python3.10 -m venv .venv;

source .venv/bin/activate;

echo "upgrading pip";

pip3.10 install pip --upgrade;

echo "installing airflow";

pip3.10 install apache-airflow  >/dev/null 2>&1;

echo "adding admin user";

echo "initializing airflow db";

airflow db init;

echo "adding user"
airflow users create -u admin -p abc123 -f matt -l martin -r Admin -e bob@gmail.com;

echo "launching scheduler"
airflow scheduler -D;

echo "launching webserver"
airflow webserver -p 8080 -D;

echo "done initializing airflow";

echo "launching web console";
open -a Safari "localhost:8080";