# Athena JSON to Parquet Demo (Bike Shop)

This project demonstrates a full Athena workflow:
1. Generate nested JSON purchase order and customer data in S3.
2. Create an Athena external table over raw JSON.
3. UNLOAD transformed data to Parquet.
4. Create an Athena external table over Parquet.
5. Benchmark query performance and scanned bytes for JSON vs Parquet.

The data generation intentionally creates many small JSON files so the Parquet compaction benefit is easy to observe.

## Fixed Demo Targets

- Bucket: from environment variable `aws_bucket`
- Raw JSON prefix: `s3://${aws_bucket}/demo_athena/raw/`
- Parquet prefix: `s3://${aws_bucket}/demo_athena/parquet/`
- Athena query results: `s3://${aws_bucket}/demo_athena/query_results/`
- Database: `db1`
- Workgroup: `primary`

## Scripts

1. `generate_bikeshop_dataset.py`
	- Generates nested NDJSON for orders and customers.
	- Writes day partitions (`dt=YYYY-MM-DD`) under `demo_athena/raw/`.
	- Includes mixed partition behavior: some partitions include customer files, some do not.
	- Reads bucket from `aws_bucket` (or `--bucket`).
	- Parameterized with `--days` (default `5`).

2. `athena_build_tables.sh`
	- Creates JSON external table: `db1.bike_orders_json`.
	- Runs Athena `UNLOAD` to Parquet in `demo_athena/parquet/`.
	- Creates Parquet external table: `db1.bike_orders_parquet`.
	- Reads bucket from `aws_bucket`.

3. `benchmark_athena_formats.py`
	- Runs paired benchmark queries against JSON and Parquet tables.
	- Repeats each benchmark (`--runs`, default `3`) with warmup (`--warmup-runs`, default `1`).
	- Reports median execution time and median scanned bytes, plus speedup factors.
	- Uses `aws_bucket` for default Athena output location.

## End-to-End Run

1. Generate 5 days of raw data (default):

```bash
export aws_bucket=<your-s3-bucket>
```

```bash
uv run python generate_bikeshop_dataset.py
```

2. Build Athena JSON table, UNLOAD to Parquet, and Parquet table:

```bash
bash athena_build_tables.sh
```

3. Run benchmarks:

```bash
uv run python benchmark_athena_formats.py
```

## Dataset Shape

Raw layout example:

```text
s3://${aws_bucket}/demo_athena/raw/
  dt=2026-04-06/
	 orders/part-00001.json
	 orders/part-00002.json
	 ...
	 customers/part-00001.json   # only in selected partitions
  dt=2026-04-07/
	 orders/part-00001.json
	 ...
```

Order JSON contains nested structures such as:
- `fulfillment` (struct)
- `customer_ref` (struct)
- `line_items` (array of structs with nested pricing/discounts)
- `totals`, `payment`, `audit` (structs)
- optional `customer_profile` (struct)

Customer JSON contains nested structures such as:
- `profile`, `loyalty` (structs)
- `addresses` (array)
- `bike_preferences` (struct)

## Observed Benchmark Results

From the most recent run (`runs=3`, `warmup=1`), median values:

| Query | JSON ms | Parquet ms | Speedup x | JSON scanned bytes | Parquet scanned bytes | Scan reduction x |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| row_count | 3696.0 | 406.0 | 9.10 | 17354063 | 0 | inf |
| channel_sales | 4105.0 | 376.0 | 10.92 | 17354063 | 71388 | 243.09 |
| high_value_orders | 4057.0 | 573.0 | 7.08 | 17354063 | 68152 | 254.64 |

Summary:
- Runtime improved by about `7x` to `11x`.
- Data scanned dropped by roughly `243x` to `255x` for the analytic queries.

## Useful Options

Generate more/less data:

```bash
uv run python generate_bikeshop_dataset.py --days 10 --orders-per-day 4000
```

Run heavier benchmarks:

```bash
uv run python benchmark_athena_formats.py --runs 5 --warmup-runs 1
```

## Notes

- The benchmark script compares equivalent query logic across raw JSON and Parquet projections.
- `row_count` may show `0` scanned bytes on Parquet due to Athena optimizations.
- For article reproducibility, keep the same seed and run parameters across benchmark reruns.
