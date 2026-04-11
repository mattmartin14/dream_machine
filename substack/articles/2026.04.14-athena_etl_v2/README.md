# Athena JSON to Parquet Demo (Bike Shop)

This project demonstrates Athena ETL with AWS CLI only:
1. Generate nested JSON purchase order data in S3.
2. Register raw JSON in a staging database.
3. UNLOAD transformed rows to Parquet.
4. Register final Parquet table in an analytics database.
5. Benchmark JSON vs Parquet query performance.

No Glue ETL job and no Python transformation code is required for the conversion step.

## Architecture

- Staging database: `db1_staging` (raw JSON external table)
- Analytics database: `db1` (final Parquet external table)
- Athena orchestration: shell script + AWS CLI polling helpers
- SQL logic: external `.sql` files in `sql/`

The shell runner stays small while ETL logic lives in reusable SQL templates.

## Fixed Demo Targets

- Bucket: from environment variable `aws_bucket`
- Raw JSON prefix: `s3://${aws_bucket}/demo_athena/raw/`
- Parquet prefix: `s3://${aws_bucket}/demo_athena/parquet/`
- Athena query results: `s3://${aws_bucket}/demo_athena/query_results/`
- Staging database default: `db1_staging`
- Analytics database default: `db1`
- Workgroup: `primary`

## Project Files

1. `generate_bikeshop_dataset.py`
   - Generates nested NDJSON for orders and customers.
   - Writes day partitions (`dt=YYYY-MM-DD`) under `demo_athena/raw/`.
   - Reads bucket from `aws_bucket` (or `--bucket`).

2. `athena_common.sh`
   - Reusable Athena helpers for start query, poll, logging, and command checks.
   - Sourced by ETL shell scripts.

3. `athena_build_tables.sh`
   - Thin orchestrator that loads SQL templates, substitutes runtime variables, and executes queries.
   - Creates/uses `db1_staging` for raw table and `db1` for Parquet table.

4. `sql/*.sql`
   - Ordered SQL templates for each ETL step.
   - Keeps transformation logic out of shell code.

5. `benchmark_athena_formats.py`
   - Runs paired benchmark queries against JSON and Parquet tables.
   - Defaults to `--json-database db1_staging` and `--parquet-database db1`.

## SQL Template Flow

SQL files execute in this order:

1. `01_create_staging_database.sql`
2. `02_create_analytics_database.sql`
3. `03_drop_staging_json_table.sql`
4. `04_create_staging_json_table.sql`
5. `05_unload_to_parquet.sql`
6. `06_drop_analytics_parquet_table.sql`
7. `07_create_analytics_parquet_table.sql`
8. `08_sanity_check_counts.sql`

Template placeholders are substituted at runtime by the shell runner:
- `__STAGING_DATABASE__`
- `__ANALYTICS_DATABASE__`
- `__JSON_TABLE__`
- `__PARQUET_TABLE__`
- `__RAW_S3__`
- `__PARQUET_S3__`

## End-to-End Run

1. Set bucket:

```bash
export aws_bucket=<your-s3-bucket>
```

2. Generate raw data:

```bash
uv run python generate_bikeshop_dataset.py
```

3. Run Athena ETL (staging JSON -> Parquet analytics):

```bash
bash athena_build_tables.sh
```

4. Benchmark formats:

```bash
uv run python benchmark_athena_formats.py
```

## Useful Overrides

Change database names at runtime:

```bash
STAGING_DATABASE=my_staging ANALYTICS_DATABASE=my_analytics bash athena_build_tables.sh
```

Run benchmarks against custom database names:

```bash
uv run python benchmark_athena_formats.py --json-database my_staging --parquet-database my_analytics
```

Generate more data:

```bash
uv run python generate_bikeshop_dataset.py --days 10 --orders-per-day 4000
```

## Notes

- The benchmark script compares equivalent logic across raw JSON and Parquet projections.
- `row_count` may show `0` scanned bytes on Parquet due to Athena optimizations.
- Keep seed and run parameters constant for reproducible article numbers.
