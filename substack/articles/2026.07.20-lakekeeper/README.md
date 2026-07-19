# Lakekeeper Raw-to-Iceberg Demo

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

