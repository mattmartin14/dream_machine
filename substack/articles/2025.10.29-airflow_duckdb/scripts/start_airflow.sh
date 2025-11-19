#!/bin/bash

AIRFLOW_VERSION=3.1.2

# Extract the version of Python you have installed. If you're currently using a Python version that is not supported by Airflow, you may want to set this manually.
# See above for supported versions.
#PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
PYTHON_VERSION="3.13"

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# For example this would install 3.0.0 with python 3.10: https://raw.githubusercontent.com/apache/airflow/constraints-3.1.2/constraints-3.10.txt

uv pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# Set Airflow home to current project directory
export AIRFLOW_HOME="$(pwd)/airflow_home"

# Set DAGs folder to the dags directory in this project
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/dags"

# Set PYTHONPATH so sitecustomize.py is found (fixes structlog import error)
export PYTHONPATH="$(pwd):${PYTHONPATH:-}"

# Set required JWT secret for api-server
#export AIRFLOW__API_AUTH__JWT_SECRET="$(openssl rand -base64 32)"

# Use uv run to execute airflow in the venv context
uv run airflow standalone