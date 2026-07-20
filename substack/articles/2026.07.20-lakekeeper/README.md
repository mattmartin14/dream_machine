# Lakekeeper Raw-to-Iceberg Demo

## Configuration

Bucket names are defined once in `.env`:

```bash
MINIO_BUCKET_NAME="matt-sbx-bucket-minio"
AWS_BUCKET_NAME="matt-sbx-bucket-1-us-east-1"
```

All scripts (`run_raw_load.sh`, `run_workload.sh`) and Docker Compose consume these values.

## Start Local Stack

```bash
docker compose up -d
```

## Raw CSV Load

```bash
# Local MinIO raw load
./run_raw_load.sh dev

# Prod S3 raw load
./run_raw_load.sh prod
```

Expected checkpoint for each command:

- `raw_orders_count = 1500`

## Iceberg Workload

```bash
# Local Lakekeeper + MinIO
./run_workload.sh dev

# Prod S3 Tables
AWS_ACCT_ID=<your-account-id> ./run_workload.sh prod
```

Expected checkpoint for workload script:

- final `count_star() = 195`

