#!/bin/bash

: '
    Author: Matt Martin
    Desc: Nukes Airflow scheduler/webserver process IDs to shut down airflow
'

cd;

cd dream_machine/airflow_workspace;

export AIRFLOW_HOME=~/dream_machine/airflow_workspace;

kill $(cat $AIRFLOW_HOME/airflow-scheduler.pid);

echo "scheduler stopped";

kill $(cat $AIRFLOW_HOME/airflow-webserver.pid);


echo "web server stopped";

echo "Airflow services stopped";
