from hardcharger_demo.costs import estimate_costs


def test_cost_estimate_does_not_fabricate_missing_rates() -> None:
    estimate = estimate_costs(
        fargate_vcpu_hours=1,
        fargate_gb_hours=2,
        s3_gb_months=3,
        s3_get_requests=1000,
        s3_put_requests=10,
        s3_tables_gb_months=0,
        s3_tables_requests=0,
        secrets_manager_secret_months=1,
        secrets_manager_api_calls=10,
        cloudwatch_log_ingest_gb=0,
        cloudwatch_log_storage_gb_months=0,
        cloudwatch_metrics=3,
        ecr_gb_months=0,
        nat_gb=0,
        data_transfer_gb=0,
        rows_processed=1_000_000,
        rates={"FARGATE_VCPU_HOUR_USD": "0.1"},
    )
    assert estimate["aws_estimated_monthly_total_usd"] is None
    assert estimate["estimated_cost_per_million_rows_usd"] is None
    assert estimate["hardcharger_price_status"] == "unknown"
