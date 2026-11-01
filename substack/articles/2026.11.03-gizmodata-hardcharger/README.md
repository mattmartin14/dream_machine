# Hardcharger on AWS: ETL, read-only user, row and column security

This project:

1. Deploys AWS assets (Terraform) and an ECS Fargate job that tells Hardcharger to read
   Parquet from S3, aggregate it, and keep the result as `order_stats.main.orders_summary`.
2. Creates a read-only user on Hardcharger and applies column-level and row-level security.
3. Queries the data with the Hardcharger ADBC driver.

Older experiments (daily ETL, federation, FinOps, TPC-H validation, notes) are in
[archive/](archive).

## Prerequisites

Python 3.12+, `uv`, Docker, AWS CLI v2, Terraform, `jq`, and Bash. Environment variables
(for example in `~/.zshrc`): `GIZMODATA_API_KEY`, `HC_ADMIN_PASSWORD`, `HC_AWS_PERSONAL_*`,
`HC_READONLY_USERNAME`, `HC_READONLY_PASSWORD`. Run `source ~/.zshrc` after editing it.

```bash
uv sync --extra dev
cp hardcharger/cluster-config.example.env hardcharger/cluster-config.local.env
```

Fill in `hardcharger/cluster-config.local.env`. With `HC_CREATE_ACCESS_CONTROL_CATALOG=true`,
`provision.sh` creates a fresh access-control catalog (needed for users, grants, and row
policies) for each cluster and `destroy.sh` deletes it; a catalog can only be used by one
cluster. The image must be a pinned release such as `hardcharger-2.4.1`, not `latest`.
If the cluster fails to start, `provision.sh` prints the provisioning error, pod logs, and the
create curl (credentials redacted), and saves the record to `hardcharger/cluster-failure.json`.

## 1. Source data in S3

The ETL reads `s3://matt-sbx-bucket-1-us-east-1/tpch/orders.parquet` (`source_s3_uri` in
`terraform/variables.tf`). It is created once with DuckDB using
[sql/load_source_data.sql](sql/load_source_data.sql) (TPC-H `orders` at scale factor 0.001):

```bash
duckdb < sql/load_source_data.sql
```

## 2. Deploy AWS and create the Hardcharger cluster

```bash
./scripts/deploy.sh --apply
```

This applies Terraform (ECR, ECS Fargate task, IAM, secret, lock table, schedule), builds
and pushes the image, runs `hardcharger/provision.sh` (creates the cluster with an empty
`order_stats` database and writes its connection secret), and runs a smoke test.

## 3. Run the ETL job (stand-alone)

```bash
./scripts/run_etl_aws.sh
```

Starts the Fargate task and waits for it. The task entry point is
`hardcharger-orders-summary` ([src/hardcharger_demo/orders_summary.py](src/hardcharger_demo/orders_summary.py));
it creates `order_stats.main.orders_summary` on Hardcharger from the S3 Parquet. Run it
whenever you like after deploy, and re-run it to rebuild the table.

## 4. Read-only user, security, and queries

Scripts find the cluster URI in `hardcharger/cluster-config.env` and the admin
credentials in the Terraform-managed secret. SQL is run one statement at a time as the admin.

```bash
uv run python scripts/run_sql.py sql/readonly_demo/01_create_readonly_user.sql
uv run python scripts/query_orders_summary.py
uv run python scripts/run_sql.py sql/readonly_demo/02_column_level_security.sql
uv run python scripts/query_orders_summary.py #should fail
uv run python scripts/query_orders_summary.py "SELECT order_status FROM order_stats.main.orders_summary" #should succeed
uv run python scripts/run_sql.py sql/readonly_demo/03_row_level_security.sql
uv run python scripts/query_orders_summary.py "SELECT order_status, order_priority, order_count FROM order_stats.main.orders_summary"
```

| Script | Effect for the read-only user |
| --- | --- |
| `01_create_readonly_user.sql` | Creates the role and user and grants `SELECT` on the table. |
| `02_column_level_security.sql` | `total_price` and `average_price` are refused; `SELECT *` fails, the three granted columns work. |
| `03_row_level_security.sql` | Only rows where `order_status` equals the user's `order_status` attribute (`'O'`) are returned. |
| `04_reset_security.sql` | Removes the row policy and restores the table-wide grant. |
| `05_drop_readonly_user.sql` | Drops the policy, user, and role. |

## 5. Tear down

```bash
./scripts/destroy.sh --confirm --delete-hardcharger
```

Terminates the Hardcharger cluster (via the GizmoData API) and destroys the AWS resources.
It also deletes the persisted `order_stats` database, so the next build starts with an empty
database (no `orders_summary`) and the ETL run shows the before/after. The S3 source data is
not touched. `provision.sh` refuses to reuse an existing `order_stats` database.

## Layout

- `terraform/`: AWS infrastructure.
- `docker/`, `src/hardcharger_demo/`: the Fargate ETL image and code.
- `hardcharger/`: cluster create and destroy via the GizmoData API.
- `scripts/`: deploy/destroy, SQL runner, query script.
- `sql/`: source-data load and `readonly_demo/` user and security SQL.
- `tests/`: unit tests (`make test`).
