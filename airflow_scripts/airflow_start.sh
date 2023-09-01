: '
    Author: Matt Martin
    Desc: Launches Airflow >> Assumes you already have airflow installed
'

export AIRFLOW_HOME=~/dream_machine/airflow_workspace
export AIRFLOW__CORE__LOAD_EXAMPLES=False;

cd;

cd dream_machine/airflow_workspace;

echo "Building virtual environment";

source .venv/bin/activate;

airflow db init;

echo "launching scheduler"
airflow scheduler -D;

echo "launching webserver"
airflow webserver -p 8080 -D;

echo "done initializing airflow";

echo "launching web console";
open -a Safari "localhost:8080";