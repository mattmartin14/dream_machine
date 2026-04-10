#!/usr/bin/env python3
"""Benchmark Athena query performance for JSON vs Parquet tables."""

from __future__ import annotations

import argparse
import os
import statistics
import time
from dataclasses import dataclass

import boto3


@dataclass
class QueryMetrics:
    execution_id: str
    execution_ms: int
    scanned_bytes: int


@dataclass
class BenchmarkResult:
    name: str
    json_ms_median: float
    parquet_ms_median: float
    json_bytes_median: float
    parquet_bytes_median: float

    @property
    def speedup(self) -> float:
        if self.parquet_ms_median <= 0:
            return float("inf")
        return self.json_ms_median / self.parquet_ms_median

    @property
    def scan_reduction(self) -> float:
        if self.parquet_bytes_median <= 0:
            return float("inf")
        return self.json_bytes_median / self.parquet_bytes_median


def parse_args() -> argparse.Namespace:
    aws_bucket = os.getenv("aws_bucket")
    if not aws_bucket:
        raise SystemExit("Set environment variable aws_bucket before running benchmarks.")

    parser = argparse.ArgumentParser(
        description="Run repeated Athena benchmarks comparing JSON and Parquet tables.",
    )
    parser.add_argument("--database", default="db1", help="Athena/Glue database name.")
    parser.add_argument("--workgroup", default="primary", help="Athena workgroup.")
    parser.add_argument(
        "--output-location",
        default=f"s3://{aws_bucket}/demo_athena/query_results/",
        help="Athena query output S3 location.",
    )
    parser.add_argument("--json-table", default="bike_orders_json", help="Raw JSON table name.")
    parser.add_argument("--parquet-table", default="bike_orders_parquet", help="Parquet table name.")
    parser.add_argument("--runs", type=int, default=3, help="Measured runs per query pair.")
    parser.add_argument(
        "--warmup-runs",
        type=int,
        default=1,
        help="Unmeasured warmup runs per query pair per format.",
    )
    parser.add_argument("--region", default="us-east-1", help="AWS region for Athena API calls.")
    return parser.parse_args()


def start_query(
    athena,
    sql: str,
    database: str,
    output_location: str,
    workgroup: str,
) -> str:
    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output_location},
        WorkGroup=workgroup,
    )
    return response["QueryExecutionId"]


def wait_for_query(athena, query_execution_id: str, poll_seconds: float = 1.5) -> QueryMetrics:
    while True:
        info = athena.get_query_execution(QueryExecutionId=query_execution_id)["QueryExecution"]
        state = info["Status"]["State"]

        if state == "SUCCEEDED":
            stats = info.get("Statistics", {})
            return QueryMetrics(
                execution_id=query_execution_id,
                execution_ms=int(stats.get("EngineExecutionTimeInMillis", 0)),
                scanned_bytes=int(stats.get("DataScannedInBytes", 0)),
            )

        if state in {"FAILED", "CANCELLED"}:
            reason = info["Status"].get("StateChangeReason", "Unknown Athena error")
            raise RuntimeError(f"Athena query {query_execution_id} ended as {state}: {reason}")

        time.sleep(poll_seconds)


def run_once(
    athena,
    sql: str,
    database: str,
    output_location: str,
    workgroup: str,
) -> QueryMetrics:
    query_id = start_query(
        athena=athena,
        sql=sql,
        database=database,
        output_location=output_location,
        workgroup=workgroup,
    )
    return wait_for_query(athena=athena, query_execution_id=query_id)


def benchmark_pair(
    athena,
    name: str,
    json_sql: str,
    parquet_sql: str,
    database: str,
    output_location: str,
    workgroup: str,
    runs: int,
    warmup_runs: int,
) -> BenchmarkResult:
    for _ in range(max(0, warmup_runs)):
        run_once(athena, json_sql, database, output_location, workgroup)
        run_once(athena, parquet_sql, database, output_location, workgroup)

    json_runs = [run_once(athena, json_sql, database, output_location, workgroup) for _ in range(runs)]
    parquet_runs = [run_once(athena, parquet_sql, database, output_location, workgroup) for _ in range(runs)]

    return BenchmarkResult(
        name=name,
        json_ms_median=statistics.median([m.execution_ms for m in json_runs]),
        parquet_ms_median=statistics.median([m.execution_ms for m in parquet_runs]),
        json_bytes_median=statistics.median([m.scanned_bytes for m in json_runs]),
        parquet_bytes_median=statistics.median([m.scanned_bytes for m in parquet_runs]),
    )


def print_results(results: list[BenchmarkResult]) -> None:
    print("\nBenchmark Results (median values)")

    headers = [
        "Query",
        "JSON ms",
        "Parquet ms",
        "Speedup x",
        "JSON scanned bytes",
        "Parquet scanned bytes",
        "Scan reduction x",
    ]

    rows = []
    for result in results:
        speedup = "inf" if result.speedup == float("inf") else f"{result.speedup:.2f}"
        scan_reduction = "inf" if result.scan_reduction == float("inf") else f"{result.scan_reduction:.2f}"
        rows.append(
            [
                result.name,
                f"{result.json_ms_median:.1f}",
                f"{result.parquet_ms_median:.1f}",
                speedup,
                f"{result.json_bytes_median:.0f}",
                f"{result.parquet_bytes_median:.0f}",
                scan_reduction,
            ]
        )

    widths = [len(h) for h in headers]
    for row in rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))

    def border() -> str:
        return "+" + "+".join("-" * (w + 2) for w in widths) + "+"

    def format_row(cells: list[str], numeric_cols: set[int]) -> str:
        formatted = []
        for idx, cell in enumerate(cells):
            if idx in numeric_cols:
                formatted.append(f" {cell:>{widths[idx]}} ")
            else:
                formatted.append(f" {cell:<{widths[idx]}} ")
        return "|" + "|".join(formatted) + "|"

    numeric_columns = {1, 2, 3, 4, 5, 6}
    print(border())
    print(format_row(headers, set()))
    print(border())
    for row in rows:
        print(format_row(row, numeric_columns))
    print(border())


def build_queries(json_table: str, parquet_table: str) -> list[tuple[str, str, str]]:
    return [
        (
            "row_count",
            f"SELECT count(*) FROM {json_table} WHERE order_id IS NOT NULL",
            f"SELECT count(*) FROM {parquet_table}",
        ),
        (
            "channel_sales",
            (
                "SELECT sales_channel, sum(totals.grand_total) AS gross_sales "
                f"FROM {json_table} "
                "WHERE order_id IS NOT NULL "
                "GROUP BY sales_channel"
            ),
            (
                "SELECT sales_channel, sum(grand_total) AS gross_sales "
                f"FROM {parquet_table} "
                "GROUP BY sales_channel"
            ),
        ),
        (
            "high_value_orders",
            (
                "SELECT count(*) "
                f"FROM {json_table} "
                "WHERE order_id IS NOT NULL AND totals.grand_total >= 2000"
            ),
            (
                "SELECT count(*) "
                f"FROM {parquet_table} "
                "WHERE grand_total >= 2000"
            ),
        ),
    ]


def main() -> None:
    args = parse_args()

    if args.runs < 1:
        raise SystemExit("--runs must be >= 1")

    athena = boto3.client("athena", region_name=args.region)
    pairs = build_queries(json_table=args.json_table, parquet_table=args.parquet_table)

    print("Running Athena benchmarks with:")
    print(f"  database: {args.database}")
    print(f"  workgroup: {args.workgroup}")
    print(f"  output location: {args.output_location}")
    print(f"  json table: {args.json_table}")
    print(f"  parquet table: {args.parquet_table}")
    print(f"  runs/query: {args.runs} (warmup: {args.warmup_runs})")

    results: list[BenchmarkResult] = []
    for name, json_sql, parquet_sql in pairs:
        print(f"\nBenchmarking: {name}")
        result = benchmark_pair(
            athena=athena,
            name=name,
            json_sql=json_sql,
            parquet_sql=parquet_sql,
            database=args.database,
            output_location=args.output_location,
            workgroup=args.workgroup,
            runs=args.runs,
            warmup_runs=args.warmup_runs,
        )
        results.append(result)

    print_results(results)


if __name__ == "__main__":
    main()
