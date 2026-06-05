# Airflow + ECS/Fargate + DuckDB ETL (Standalone Project)

## What This Project Is

This project demonstrates a practical hybrid data platform pattern:

- Keep Apache Airflow local for fast development and orchestration control.
- Offload ETL compute to AWS ECS/Fargate for serverless, isolated execution.
- Use DuckDB inside ephemeral ECS tasks to run analytics-style SQL transformations.

In short, Airflow decides what to run and when, while Fargate executes each workload without managing EC2 hosts.

## Why It Is Built This Way

This architecture is intentional for teams that want:

- Lower local machine load: orchestration local, heavy compute remote.
- Operational simplicity: no ECS host fleet to patch or scale.
- Repeatable ETL workers: immutable container image + script-driven runtime.
- Cost control: pay-per-task compute with Fargate.
- Flexible orchestration: Airflow dependencies/retries while using cloud-native execution.

## What It Builds

The repository provisions and runs the following:

- Foundational AWS infrastructure in [terraform_infra](terraform_infra):
  - ECR repository for ETL runner image
  - ECS cluster and CloudWatch log group
  - IAM task roles/policies
  - Networking and security group
  - S3 raw and aggregate buckets
  - Slack webhook secret in Secrets Manager

- Airflow-runner task-definition layer in [terraform_ecs_runner_taskdefs](terraform_ecs_runner_taskdefs):
  - Reusable ECS/Fargate task definitions for different runner sizes
  - Optional policy for allowing a local Airflow principal to call ECS RunTask

- Local orchestration layer with Docker Compose:
  - Airflow webserver + scheduler + metadata database

- Runtime ETL scripts in [scripts](scripts):
  - TPCH raw seeding script
  - Orders aggregate ETL
  - Customer aggregate ETL
  - Final lineitem aggregate ETL

## End-to-End Data Flow

The DAG [dags/duckdb_ecs_etl.py](dags/duckdb_ecs_etl.py) runs with this dependency graph:

1. `run_seed_tpch_raw`
2. `run_orders_etl` and `run_cust_etl` in parallel
3. `run_lineitem_etl_final` after both parallel tasks succeed

Outputs:

- Raw bucket:
  - `s3://s3-sales-raw-test/tpch/orders_raw/orders.parquet`
  - `s3://s3-sales-raw-test/tpch/customer_raw/customer.parquet`
  - `s3://s3-sales-raw-test/tpch/lineitem_raw/lineitem.parquet`

- Aggregate bucket:
  - `s3://s3-sales-agg-test/tpch/orders_agg/orders_agg.parquet`
  - `s3://s3-sales-agg-test/tpch/cust_agg/cust_agg.parquet`
  - `s3://s3-sales-agg-test/tpch/lineitem_agg/lineitem_agg.parquet`

## Repository Structure

- [dags](dags): Airflow DAG definitions
- [scripts](scripts): runtime ETL Python scripts executed in ECS
- [deploy](deploy): deployment/teardown orchestration scripts
- [docker](docker): ETL runner image build/push scripts
- [terraform_infra](terraform_infra): foundational infra IaC
- [terraform_ecs_runner_taskdefs](terraform_ecs_runner_taskdefs): runner task defs IaC

## Prerequisites

- AWS CLI configured with access to target account
- Terraform >= 1.5
- Docker + Docker Compose
- Permission to create/destroy IAM, ECS, ECR, S3, networking, and Secrets Manager resources
- `aws_auth` helper available for role + MFA login flow

## Deploy and Run

1. Authenticate (role + MFA):

```bash
aws_auth
```

2. Deploy foundational infrastructure and build/push runner image:

```bash
./deploy/deploy_foundational.sh latest --auto-approve
```

3. Deploy ECS runner task definitions:

```bash
./deploy/deploy_runner_taskdefs.sh --auto-approve -var 'image_tag=latest'
```

Optional (creates attachable IAM policy for local Airflow identity):

```bash
./deploy/deploy_runner_taskdefs.sh --auto-approve -var 'create_airflow_runner_policy=true' -var 'image_tag=latest'
```

4. Upload runtime ETL scripts to S3:

```bash
./deploy/upload_etl_scripts.sh --aws-region us-east-1
```

5. Render local Airflow environment from Terraform outputs:

```bash
export AWS_ACCT_ID=<your-12-digit-account-id>
./deploy/render_airflow_env.sh
```

6. Start Airflow locally:

```bash
./deploy/start_airflow.sh --build
```

7. Open Airflow UI:

- URL: `http://localhost:8080`
- Default credentials: `airflow` / `airflow`

8. Trigger DAG `duckdb_ecs_etl`.

## Teardown (Destroy AWS Assets)

This repository now includes explicit teardown scripts.

### Destroy Task Definitions Only

```bash
./deploy/destroy_runner_taskdefs.sh --auto-approve
```

### Destroy Foundational Infrastructure Only

```bash
./deploy/destroy_foundational.sh --auto-approve
```

### Destroy Everything in Correct Order

```bash
./deploy/destroy_all.sh --auto-approve
```

Order matters. The all-in-one script destroys runner task definitions first, then foundational infrastructure.

## Important Notes

- AWS CLI pager/autoprompt are disabled in deploy/teardown scripts to avoid interactive hangs.
- The foundational module requires `slack_webhook`; destroy script supplies a safe default if unset.
- Cloud resources can take time to fully disappear after Terraform reports completion.
