#!/usr/bin/env python3
"""Report Cost Explorer actuals and estimates using only supplied price inputs."""

from __future__ import annotations

import argparse
import json
import os
from datetime import date, timedelta
from typing import Any

import boto3

from hardcharger_demo.costs import estimate_costs


def fetch_actual_costs(
    *, client: Any | None = None, today: date | None = None
) -> dict[str, object]:
    if client is None:
        client = boto3.client("ce", region_name="us-east-1")
    end = (today or date.today()).replace(day=1)
    start = (end - timedelta(days=1)).replace(day=1)
    response = client.get_cost_and_usage(
        TimePeriod={"Start": start.isoformat(), "End": end.isoformat()},
        Granularity="MONTHLY",
        Metrics=["UnblendedCost"],
        GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}],
    )
    results = response["ResultsByTime"]
    groups = results[0]["Groups"] if results else []
    return {
        "basis": "actual Cost Explorer billed data",
        "period_start": start.isoformat(),
        "period_end_exclusive": end.isoformat(),
        "services": {
            group["Keys"][0]: {
                "amount": float(group["Metrics"]["UnblendedCost"]["Amount"]),
                "unit": group["Metrics"]["UnblendedCost"]["Unit"],
            }
            for group in groups
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--actuals", action="store_true")
    parser.add_argument("--rows", type=int, default=0)
    args = parser.parse_args()
    rates = {key: value for key, value in os.environ.items() if key.endswith("_USD")}
    report: dict[str, object] = {
        "estimate": estimate_costs(
            fargate_vcpu_hours=float(os.getenv("MEASURED_FARGATE_VCPU_HOURS", "0")),
            fargate_gb_hours=float(os.getenv("MEASURED_FARGATE_GB_HOURS", "0")),
            s3_gb_months=float(os.getenv("MEASURED_S3_GB_MONTHS", "0")),
            s3_get_requests=int(os.getenv("MEASURED_S3_GET_REQUESTS", "0")),
            s3_put_requests=int(os.getenv("MEASURED_S3_PUT_REQUESTS", "0")),
            s3_tables_gb_months=float(os.getenv("MEASURED_S3_TABLES_GB_MONTHS", "0")),
            s3_tables_requests=int(os.getenv("MEASURED_S3_TABLES_REQUESTS", "0")),
            secrets_manager_secret_months=float(
                os.getenv("MEASURED_SECRETS_MANAGER_SECRET_MONTHS", "1")
            ),
            secrets_manager_api_calls=int(
                os.getenv("MEASURED_SECRETS_MANAGER_API_CALLS", "0")
            ),
            cloudwatch_log_ingest_gb=float(os.getenv("MEASURED_CW_LOG_INGEST_GB", "0")),
            cloudwatch_log_storage_gb_months=float(
                os.getenv("MEASURED_CW_LOG_STORAGE_GB_MONTHS", "0")
            ),
            cloudwatch_metrics=int(os.getenv("MEASURED_CW_METRICS", "3")),
            ecr_gb_months=float(os.getenv("MEASURED_ECR_GB_MONTHS", "0")),
            nat_gb=float(os.getenv("MEASURED_NAT_GB", "0")),
            data_transfer_gb=float(os.getenv("MEASURED_DATA_TRANSFER_GB", "0")),
            rows_processed=args.rows,
            rates=rates,
            monthly_etl_runs=int(os.getenv("MONTHLY_ETL_RUNS", "30")),
            hardcharger_active_hours=float(
                os.getenv("MEASURED_HARDCHARGER_ACTIVE_HOURS", "0")
            ),
        )
    }
    if args.actuals:
        report["actuals"] = fetch_actual_costs()
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
