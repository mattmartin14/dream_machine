from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator


def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def csv_env(name: str) -> list[str]:
    raw_value = env(name)
    if not raw_value:
        return []
    return [item.strip() for item in raw_value.split(",") if item.strip()]


AWS_REGION = env("AIRFLOW_ECS_REGION", env("AWS_REGION", "us-east-1"))
AWS_CONN_ID = env("AIRFLOW_ECS_AWS_CONN_ID", "aws_default")
CLUSTER = env("AIRFLOW_ECS_CLUSTER")
TASK_DEFINITION = env("AIRFLOW_ECS_TASK_DEFINITION")
CONTAINER_NAME = env("AIRFLOW_ECS_CONTAINER_NAME", "duckdb-runner-medium")
SUBNETS = csv_env("AIRFLOW_ECS_SUBNETS")
SECURITY_GROUPS = csv_env("AIRFLOW_ECS_SECURITY_GROUPS")
ASSIGN_PUBLIC_IP = env("AIRFLOW_ECS_ASSIGN_PUBLIC_IP", "ENABLED")
LAUNCH_TYPE = env("AIRFLOW_ECS_LAUNCH_TYPE", "FARGATE")

ORDERS_SCRIPT_S3_URI = env("ORDERS_SCRIPT_S3_URI", "s3://s3-sales-agg-test/scripts/orders_etl.py")
CUST_SCRIPT_S3_URI = env("CUST_SCRIPT_S3_URI", "s3://s3-sales-agg-test/scripts/cust_etl.py")
SEED_SCRIPT_S3_URI = env("SEED_SCRIPT_S3_URI", "s3://s3-sales-agg-test/scripts/seed_tpch_raw_data.py")
LINEITEM_SCRIPT_S3_URI = env("LINEITEM_SCRIPT_S3_URI", "s3://s3-sales-agg-test/scripts/lineitem_etl.py")


def ecs_runner_task(task_id: str, script_s3_uri: str) -> EcsRunTaskOperator:
    return EcsRunTaskOperator(
        task_id=task_id,
        cluster=CLUSTER,
        task_definition=TASK_DEFINITION,
        launch_type=LAUNCH_TYPE,
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        overrides={
            "containerOverrides": [
                {
                    "name": CONTAINER_NAME,
                    "command": [script_s3_uri],
                }
            ]
        },
        network_configuration={
            "awsvpcConfiguration": {
                "subnets": SUBNETS,
                "securityGroups": SECURITY_GROUPS,
                "assignPublicIp": ASSIGN_PUBLIC_IP,
            }
        },
        wait_for_completion=True,
        do_xcom_push=False,
    )


with DAG(
    dag_id="duckdb_ecs_etl",
    description="Seed TPCH raw data, run orders and customer DuckDB ETL jobs in parallel, then run a final lineitem ETL job.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["duckdb", "ecs", "airflow-migration"],
) as dag:
    run_seed_tpch_raw = ecs_runner_task(
        task_id="run_seed_tpch_raw",
        script_s3_uri=SEED_SCRIPT_S3_URI,
    )

    run_orders_etl = ecs_runner_task(
        task_id="run_orders_etl",
        script_s3_uri=ORDERS_SCRIPT_S3_URI,
    )

    run_cust_etl = ecs_runner_task(
        task_id="run_cust_etl",
        script_s3_uri=CUST_SCRIPT_S3_URI,
    )

    run_lineitem_etl_final = ecs_runner_task(
        task_id="run_lineitem_etl_final",
        script_s3_uri=LINEITEM_SCRIPT_S3_URI,
    )

    run_seed_tpch_raw >> [run_orders_etl, run_cust_etl]
    [run_orders_etl, run_cust_etl] >> run_lineitem_etl_final
