# Spark 4.0 JSON Skew Demo (uv + Python 3.13)

This project demonstrates how skewed JSON object sizes in S3 can slow Spark ETL, then shows a size-aware optimization that reduces stragglers.

## Scenario
- Most chat-log JSON objects are small (3 KB to 8 KB).
- 1 to 2 outlier JSON objects are large (about 20 MB).
- Data models hardware-store return chats, where each order has one prefix.

## S3 Layout
- Bucket: `matt-sbx-bucket-1-us-east-1`
- Raw data prefix:
  - `etl-skew-demo/raw/returns_chat/run_date=YYYY-MM-DD/order_id=.../chat_*.json`
- Normalized optimized prefix:
  - `etl-skew-demo/normalized/returns_chat/run_date=YYYY-MM-DD/.../part-*.jsonl`

## Prerequisites
1. `uv` installed.
2. AWS credentials already available in your shell environment.
3. Java runtime available for Spark.

## Setup
```bash
uv python install 3.13
uv sync --python 3.13
```

## 1) Generate skewed test data
```bash
uv run --python 3.13 skew-generate \
  --bucket matt-sbx-bucket-1-us-east-1 \
  --run-date 2026-08-08 \
  --orders 250 \
  --files-per-order 4 \
  --large-files 2 \
  --small-min-kb 3 \
  --small-max-kb 8 \
  --large-mb 20
```

This writes a manifest locally to `artifacts/manifest.json` and to S3 under `etl-skew-demo/manifests/run_date=.../manifest.json`.

## 2) Baseline job (non-optimized)
Reads multiline JSON directly and performs ETL.

```bash
uv run --python 3.13 skew-baseline \
  --bucket matt-sbx-bucket-1-us-east-1 \
  --run-date 2026-08-08
```

Writes metrics to `artifacts/metrics/baseline_metrics.json`.

## 3) Optimized job (size-aware)
- Lists source objects by size.
- Splits large files into chunked JSONL records.
- Reads chunked data in parallel and unions with small-file path.
- Runs same downstream ETL.

```bash
uv run --python 3.13 skew-optimized \
  --bucket matt-sbx-bucket-1-us-east-1 \
  --run-date 2026-08-08 \
  --large-threshold-mb 10 \
  --chunk-message-count 1500
```

Writes metrics to `artifacts/metrics/optimized_metrics.json`.

## 4) Compare results
```bash
uv run --python 3.13 skew-report
```

Outputs comparison report at `artifacts/metrics/comparison_report.json`.

## 5) DuckDB baseline (direct JSON)
Reads the same raw S3 JSON input and runs equivalent aggregate logic in DuckDB.

```bash
uv run --python 3.13 duckdb-baseline \
  --bucket matt-sbx-bucket-1-us-east-1 \
  --run-date 2026-08-08
```

Writes metrics to `artifacts/metrics/duckdb_baseline_metrics.json`.

## What to look for
- Lower total runtime in optimized run.
- Lower `max_over_median` task duration ratio in optimized run.
- Better partition balance (`largest_over_median`) after normalization.

## Notes
- Spark 4.0 requires Java. Use a compatible JDK in your environment.
- This is a demo pipeline prioritizing clear skew behavior and mitigation clarity.
