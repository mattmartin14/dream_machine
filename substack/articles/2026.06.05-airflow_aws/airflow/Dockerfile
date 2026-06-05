FROM apache/airflow:2.10.5-python3.12

ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.12
ARG AMAZON_PROVIDER_VERSION=9.2.0

RUN pip install --no-cache-dir \
    "apache-airflow-providers-amazon==${AMAZON_PROVIDER_VERSION}" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
