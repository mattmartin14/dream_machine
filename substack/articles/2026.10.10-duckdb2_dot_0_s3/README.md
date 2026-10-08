# DuckDB 2.0 + S3 benchmark

Generates dummy bicycle shop order data as JSON, uploads it to S3, and queries it with the DuckDB CLI, so the 1.5 and 2.0 CLIs can be compared.

- Bucket: `matt-sbx-bucket-1-us-east-1`
- Prefix: `2_dot_0_benchmark/`
- Requires an authenticated AWS SSO session and [uv](https://docs.astral.sh/uv/).

## Files

### `main.py`

Generates dummy data files and uploads to s3.

```sh
uv run main.py sample                # print one example document
uv run main.py upload -n 1000 -w 32  # generate and upload 1000 files using 32 threads
uv run main.py nuke                  # delete everything under the prefix
```

| Option | Default | Description |
| --- | --- | --- |
| `-n`, `--count` | 100 | Number of files to upload |
| `-w`, `--workers` | 32 | Thread count |

Each document has order number, date, total amount, customer id, store name, a nested `customer` struct (name, address, city, state, zip), and an `order_lines` array (line number, product, quantity, unit price).

### `toggle_version.sh`

Installs a DuckDB CLI version using the official install script.

```sh
./toggle_version.sh 2.0   # 2.0 alpha
./toggle_version.sh 1.5   # 1.5 (default release)
```

### `query.sql`

Run with the DuckDB CLI. It loads the `aws` extension, creates an S3 secret from the credential chain (SSO), prints the DuckDB version, and times a query that unnests `order_lines` to count orders, line items, and average line quantity.

```sh
duckdb -c ".read query.sql"
```

### `benchmark.sh`

Runs `query.sql` on DuckDB 1.5, then 2.0 (switching versions via `toggle_version.sh`). Each version gets one untimed warm-up run, then 5 timed runs by default. Only the `real` time from `.timer` is captured, so the extension install and secret creation are not counted. Prints each run, both averages, and the % decrease in run time.

```sh
./benchmark.sh      # 5 runs per version
./benchmark.sh 10   # 10 runs per version
```

The script leaves DuckDB 2.0 installed; run `./toggle_version.sh 1.5` to switch back.
