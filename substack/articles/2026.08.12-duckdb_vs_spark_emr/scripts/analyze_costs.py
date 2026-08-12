#!/usr/bin/env python3
from __future__ import annotations

import datetime as dt
import json
import os
import subprocess
import sys
from pathlib import Path


def run_aws_json(args: list[str]) -> dict:
    try:
        completed = subprocess.run(
            ["aws", "--no-cli-pager", *args],
            capture_output=True,
            text=True,
            check=True,
        )
        return {"ok": True, "data": json.loads(completed.stdout)}
    except subprocess.CalledProcessError as exc:
        return {
            "ok": False,
            "error": exc.stderr.strip() or exc.stdout.strip() or str(exc),
            "args": args,
        }


def main() -> int:
    summary_path = Path(os.environ["SUMMARY_PATH"])
    output_path = Path(os.environ["OUTPUT_PATH"])
    aws_region = os.environ["AWS_REGION"]

    summary = json.loads(summary_path.read_text())
    run_date = summary["run_date"]
    start_date = dt.date.fromisoformat(run_date)
    end_date = start_date + dt.timedelta(days=2)

    # Runtime-based cost estimates (immediate, benchmark-specific)
    fargate_vcpu_per_second = 0.000011244
    fargate_mem_gb_per_second = 0.000001235
    emr_serverless_vcpu_hour = 0.052624
    emr_serverless_mem_gb_hour = 0.0057785

    duckdb = summary["engines"]["duckdb"]
    spark = summary["engines"]["spark"]

    duckdb_run_seconds = float(duckdb.get("control_plane", {}).get("run_to_stop_seconds") or 0.0)
    duckdb_vcpu = 2.0
    duckdb_mem_gb = 4.0

    duckdb_cpu_cost = duckdb_vcpu * duckdb_run_seconds * fargate_vcpu_per_second
    duckdb_mem_cost = duckdb_mem_gb * duckdb_run_seconds * fargate_mem_gb_per_second
    duckdb_estimated_cost = duckdb_cpu_cost + duckdb_mem_cost

    spark_runtime = spark.get("runtime_diagnostics", {})
    billed = spark_runtime.get("billed_resource_utilization", {}) or {}
    spark_vcpu_hour = float(billed.get("vCPUHour") or 0.0)
    spark_mem_gb_hour = float(billed.get("memoryGBHour") or 0.0)

    spark_vcpu_cost = spark_vcpu_hour * emr_serverless_vcpu_hour
    spark_mem_cost = spark_mem_gb_hour * emr_serverless_mem_gb_hour
    spark_estimated_cost = spark_vcpu_cost + spark_mem_cost

    runtime_ratio = None
    if duckdb_estimated_cost > 0:
        runtime_ratio = spark_estimated_cost / duckdb_estimated_cost

    # Cost Explorer queries (may lag by 24h+ and tags may not be active yet)
    ce_by_service = run_aws_json(
        [
            "ce",
            "get-cost-and-usage",
            "--time-period",
            f"Start={start_date.isoformat()},End={end_date.isoformat()}",
            "--granularity",
            "DAILY",
            "--metrics",
            "UnblendedCost",
            "--group-by",
            "Type=DIMENSION,Key=SERVICE",
        ]
    )

    ce_tags = {}
    for tag_key in ["Project", "ManagedBy", "Benchmark"]:
        ce_tags[tag_key] = run_aws_json(
            [
                "ce",
                "get-tags",
                "--time-period",
                f"Start={start_date.isoformat()},End={end_date.isoformat()}",
                "--tag-key",
                tag_key,
            ]
        )

    analysis = {
        "benchmark_id": summary["benchmark_id"],
        "run_date": run_date,
        "region": aws_region,
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "runtime_estimate_usd": {
            "duckdb_ecs_fargate": {
                "estimated_total_usd": round(duckdb_estimated_cost, 8),
                "cpu_component_usd": round(duckdb_cpu_cost, 8),
                "memory_component_usd": round(duckdb_mem_cost, 8),
                "assumptions": {
                    "task_vcpu": duckdb_vcpu,
                    "task_memory_gb": duckdb_mem_gb,
                    "billable_seconds_used": duckdb_run_seconds,
                    "fargate_vcpu_price_per_second": fargate_vcpu_per_second,
                    "fargate_memory_price_per_gb_second": fargate_mem_gb_per_second,
                },
            },
            "spark_emr_serverless": {
                "estimated_total_usd": round(spark_estimated_cost, 8),
                "vcpu_component_usd": round(spark_vcpu_cost, 8),
                "memory_component_usd": round(spark_mem_cost, 8),
                "assumptions": {
                    "billed_vcpu_hours": spark_vcpu_hour,
                    "billed_memory_gb_hours": spark_mem_gb_hour,
                    "emr_serverless_vcpu_hour_price": emr_serverless_vcpu_hour,
                    "emr_serverless_memory_gb_hour_price": emr_serverless_mem_gb_hour,
                },
            },
            "spark_vs_duckdb_ratio": round(runtime_ratio, 6) if runtime_ratio is not None else None,
        },
        "cost_explorer": {
            "service_breakdown_query": ce_by_service,
            "tag_visibility": ce_tags,
            "notes": [
                "Cost Explorer data can lag by 24h or longer.",
                "Tag-based attribution requires activated cost allocation tags and post-activation usage.",
                "Runtime estimates are benchmark-specific and available immediately.",
            ],
        },
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(analysis, indent=2), encoding="utf-8")
    print(json.dumps(analysis, indent=2))
    print(f"Wrote {output_path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyError as exc:
        print(f"missing required environment variable: {exc}", file=sys.stderr)
        raise SystemExit(2)
