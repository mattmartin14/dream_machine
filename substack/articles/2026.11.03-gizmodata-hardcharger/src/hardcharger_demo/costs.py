"""Cost estimates using measured quantities and user-provided pricing only."""

from __future__ import annotations


def estimate_costs(
    *,
    fargate_vcpu_hours: float,
    fargate_gb_hours: float,
    s3_gb_months: float,
    s3_get_requests: int,
    s3_put_requests: int,
    s3_tables_gb_months: float,
    s3_tables_requests: int,
    secrets_manager_secret_months: float,
    secrets_manager_api_calls: int,
    cloudwatch_log_ingest_gb: float,
    cloudwatch_log_storage_gb_months: float,
    cloudwatch_metrics: int,
    ecr_gb_months: float,
    nat_gb: float,
    data_transfer_gb: float,
    rows_processed: int,
    rates: dict[str, str],
    monthly_etl_runs: int = 30,
    hardcharger_active_hours: float = 0,
) -> dict[str, object]:
    quantities = {
        "fargate_vcpu": (fargate_vcpu_hours, "FARGATE_VCPU_HOUR_USD"),
        "fargate_memory": (fargate_gb_hours, "FARGATE_GB_HOUR_USD"),
        "s3_storage": (s3_gb_months, "S3_GB_MONTH_USD"),
        "s3_get_requests": (s3_get_requests / 1000, "S3_GET_PER_1000_USD"),
        "s3_put_requests": (s3_put_requests / 1000, "S3_PUT_PER_1000_USD"),
        "s3_tables_storage": (s3_tables_gb_months, "S3_TABLES_GB_MONTH_USD"),
        "s3_tables_requests": (
            s3_tables_requests / 1000,
            "S3_TABLES_REQUEST_PER_1000_USD",
        ),
        "secrets_manager_storage": (
            secrets_manager_secret_months,
            "SECRETS_MANAGER_SECRET_MONTH_USD",
        ),
        "secrets_manager_api": (
            secrets_manager_api_calls / 10000,
            "SECRETS_MANAGER_API_PER_10000_USD",
        ),
        "cloudwatch_log_ingestion": (
            cloudwatch_log_ingest_gb,
            "CLOUDWATCH_LOG_INGEST_GB_USD",
        ),
        "cloudwatch_log_storage": (
            cloudwatch_log_storage_gb_months,
            "CLOUDWATCH_LOG_STORAGE_GB_MONTH_USD",
        ),
        "cloudwatch_metrics": (cloudwatch_metrics, "CLOUDWATCH_METRIC_MONTH_USD"),
        "ecr_storage": (ecr_gb_months, "ECR_GB_MONTH_USD"),
        "nat_data": (nat_gb, "NAT_GB_USD"),
        "data_transfer": (data_transfer_gb, "DATA_TRANSFER_GB_USD"),
    }
    aws_items = {
        name: round(quantity * float(rates[rate_key]), 6) if rate_key in rates else None
        for name, (quantity, rate_key) in quantities.items()
    }
    aws_total = (
        round(sum(value for value in aws_items.values() if value is not None), 6)
        if all(value is not None for value in aws_items.values())
        else None
    )

    monthly_price = rates.get("HARDCHARGER_MONTHLY_USD")
    hourly_price = rates.get("HARDCHARGER_HOURLY_USD")
    hardcharger_idle = float(monthly_price) if monthly_price is not None else None
    hardcharger_active = (
        round(float(hourly_price) * hardcharger_active_hours, 6)
        if hourly_price is not None
        else None
    )
    hardcharger_monthly = (
        round((hardcharger_idle or 0) + (hardcharger_active or 0), 6)
        if hardcharger_idle is not None or hardcharger_active is not None
        else None
    )
    monthly_total = (
        round(aws_total + hardcharger_monthly, 6)
        if aws_total is not None and hardcharger_monthly is not None
        else None
    )
    per_execution = (
        round(monthly_total / monthly_etl_runs, 6)
        if monthly_total is not None and monthly_etl_runs > 0
        else None
    )
    per_million_rows = (
        round(monthly_total / (rows_processed / 1_000_000), 6)
        if monthly_total is not None and rows_processed > 0
        else None
    )
    return {
        "basis": "estimate from supplied rates and measured quantities; not billed actuals",
        "aws_line_items_usd": aws_items,
        "aws_estimated_monthly_total_usd": aws_total,
        "hardcharger_pricing_model": "monthly base plus hourly active usage when supplied",
        "hardcharger_estimated_idle_usd": hardcharger_idle,
        "hardcharger_estimated_active_usd": hardcharger_active,
        "hardcharger_estimated_monthly_total_usd": hardcharger_monthly,
        "hardcharger_price_status": (
            "estimated from supplied pricing" if hardcharger_monthly is not None else "unknown"
        ),
        "estimated_combined_monthly_total_usd": monthly_total,
        "estimated_cost_per_etl_execution_usd": per_execution,
        "estimated_cost_per_million_rows_usd": per_million_rows,
        "monthly_etl_runs": monthly_etl_runs,
        "rows_processed_per_month": rows_processed,
        "sponsored_demo_hardcharger_expense_usd": 0,
        "sponsored_demo_note": (
            "Complimentary sponsor access; not a representation of normal customer pricing."
        ),
    }
