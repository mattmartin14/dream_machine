# DuckDB Basics: Python, CLI, SQL Files, and S3

## Goal
Teach SQL fundamentals in DuckDB using three interfaces:

1. Python API
2. DuckDB CLI
3. Reusable `.sql` files

Then extend the workflow to AWS S3 for Parquet push/pull.

## Why DuckDB
DuckDB is ideal for beginner analytics work:

- Local and fast
- No separate database server to manage
- Excellent support for CSV and Parquet
- Works well from Python and command line

## My Practical Preference (From Experience)
For most DuckDB work, I prefer the CLI and `.sql` files before Python.

Why:

- The CLI is self-contained and does not require Python package setup for query work
- The CLI is fast for iterative query development
- The workflow feels lightweight, clean, and easy to reason about

My usual pattern:

1. Start in DuckDB CLI and iterate with `.sql` files
2. Move to Python only when I need non-SQL logic like iteration/control flow
3. Use Python when integrating with AWS services beyond straightforward SQL access patterns (for example, broader boto3 service orchestration outside S3/Glue-style SQL workflows)

This module shows both approaches, but the default mindset is SQL-first in CLI, then Python when orchestration is required.

## Part 1: Python API Basics
Create and use an in-memory DuckDB database from Python.

```python
import duckdb

cn = duckdb.connect()

cn.execute("""
CREATE OR REPLACE TABLE sales AS
SELECT *
FROM (
    VALUES
        (1, 'book', 12.99, '2026-01-10'),
        (2, 'pen', 1.99, '2026-01-10'),
        (3, 'book', 12.99, '2026-01-11'),
        (4, 'notebook', 5.49, '2026-01-11')
) AS t(id, item, amount, sale_date);
""")

cn.sql("SELECT item, COUNT(*) AS n, SUM(amount) AS total FROM sales GROUP BY item ORDER BY total DESC;").show()
```

Run it with:

```bash
uv run python your_script.py
```

## Part 2: Generate CSV and Parquet
Export the table to CSV and Parquet.

```python
cn.execute("COPY sales TO 'sales.csv' (HEADER, DELIMITER ',')")
cn.execute("COPY sales TO 'sales.parquet' (FORMAT PARQUET)")
```

Read them back:

```python
cn.sql("SELECT COUNT(*) AS csv_rows FROM read_csv_auto('sales.csv')").show()
cn.sql("SELECT COUNT(*) AS parquet_rows FROM read_parquet('sales.parquet')").show()
```

## Part 3: DuckDB CLI Basics
You can run the same SQL directly in CLI using an in-memory session.

Start CLI:

```bash
duckdb
```

Inside CLI:

```sql
CREATE OR REPLACE TABLE sales AS
SELECT *
FROM (
  VALUES
    (1, 'book', 12.99, '2026-01-10'),
    (2, 'pen', 1.99, '2026-01-10'),
    (3, 'book', 12.99, '2026-01-11'),
    (4, 'notebook', 5.49, '2026-01-11')
) AS t(id, item, amount, sale_date);

SELECT COUNT(*) FROM sales;
SELECT item, AVG(amount) AS avg_amount FROM sales GROUP BY item ORDER BY avg_amount DESC;
```

Exit with `.quit`.

Because this is in-memory, each new CLI launch starts clean.

## Part 4: Use .sql Files
Create a query file, for example `queries/sales_summary.sql`:

```sql
CREATE OR REPLACE TABLE sales AS
SELECT *
FROM (
  VALUES
    (1, 'book', 12.99, '2026-01-10'),
    (2, 'pen', 1.99, '2026-01-10'),
    (3, 'book', 12.99, '2026-01-11'),
    (4, 'notebook', 5.49, '2026-01-11')
) AS t(id, item, amount, sale_date);

SELECT
  item,
  COUNT(*) AS orders,
  SUM(amount) AS revenue
FROM sales
GROUP BY item
ORDER BY revenue DESC;
```

Execute from CLI using `.read`:

```bash
duckdb -c ".read queries/sales_summary.sql"
```

The `.read` approach is useful because your SQL files can include DuckDB dot commands (for example, print-style output helpers and other CLI directives), not just plain SQL.

This teaches production-style habits early: queries live in files, not only in ad-hoc command history.

## Part 5: AWS Authentication with MFA
Complete MFA setup and credential refresh steps using the dedicated guide:

- [AWS Authentication with MFA](aws_auth_mfa.md)

After completing that guide, return here and continue with S3 push/pull.

## Part 6: AWS S3 Parquet Push/Pull
This step demonstrates data movement patterns a data engineer uses often.

### Prerequisites
- AWS credentials configured via profile or environment variables
- Export your bucket env var before running examples: `export aws_bucket=your-bucket-name`
- For the script-driven CLI flow below, also set: `export AWS_BUCKET=your-bucket-name`
- Never hardcode access keys in scripts or SQL files
- Never hardcode bucket names in training scripts when you can use environment variables

### Option A (Recommended): Scripted DuckDB CLI flow with `.read`
Use the tested script in this repo:

- `scripts/duckdb_aws.sql`

Run it:

```bash
duckdb -c ".read ./scripts/duckdb_aws.sql"
```

Why this pattern is powerful:

- `.print` statements provide clear progress markers while the script runs
- `.shell` commands let you invoke external tools (for example AWS CLI) directly from the DuckDB CLI workflow
- This is especially useful when CLI SQL statements alone are not enough for an operation (for example, dynamic shell-driven upload steps)

This gives you a clean SQL-first orchestration style while still allowing operational steps around the SQL.

### Option B: boto3 upload/download + DuckDB query

```python
import os
import boto3

# Reuse the existing in-memory DuckDB connection from earlier sections.
bucket = os.getenv("aws_bucket")
if not bucket:
  raise ValueError("Set aws_bucket env var before running this example")

s3 = boto3.client("s3")
s3.upload_file("sales.parquet", bucket, "training/sales.parquet")
s3.download_file(bucket, "training/sales.parquet", "sales_from_s3.parquet")

cn.sql("SELECT COUNT(*) AS rows_from_s3 FROM read_parquet('sales_from_s3.parquet')").show()
```

### Option C: DuckDB direct S3 read (advanced follow-up)
Depending on your DuckDB setup and extensions, you can read Parquet directly from S3 paths.

```sql
SELECT COUNT(*)
FROM read_parquet('s3://${aws_bucket}/training/sales.parquet');
```

Use this as an advanced sidebar after the boto3-first approach if you want the beginner path to stay predictable.

## Mentor Talking Points
- Python API, CLI, and `.sql` files are complementary, not competing approaches.
- Use Python for orchestration, SQL files for reusable logic, and CLI for quick exploration.
- CSV and Parquet generation/read-back builds confidence before introducing cloud storage.
- MFA-based authentication teaches secure default habits before cloud data movement.
- S3 push/pull shows how local analytics patterns transition into cloud workflows.
