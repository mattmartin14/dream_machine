# DuckDB vs EMR Serverless Benchmark

This repo benchmarks the same JSON-processing workload on two AWS execution targets:

- ECS Fargate running a Python 3.13 DuckDB container
- EMR Serverless running a PySpark job

Both jobs read the same JSON data from S3, compute the same aggregates, write their results back to S3 as JSON, and emit a shared benchmark result JSON that records logical start and finish timestamps inside the job so service cold-start time is excluded from the headline comparison.

## Repo layout

- `skew_demo/generate_data.py`: synthetic dataset generator that uploads chat transcript JSON to S3
- `skew_demo/duckdb_job.py`: DuckDB benchmark job
- `skew_demo/spark_job.py`: Spark parity job for EMR Serverless
- `terraform/`: ECR, ECS, EMR Serverless, IAM, and CloudWatch infrastructure
- `scripts/deploy.sh`: builds and deploys the benchmark stack
- `scripts/run_benchmarks.sh`: runs both jobs and downloads their benchmark JSON
- `scripts/summarize_benchmark.sh`: emits a consolidated benchmark summary JSON
- `scripts/teardown.sh`: destroys the benchmark infrastructure with Terraform

## Prerequisites

- Python 3.13 with `uv`
- Docker
- Terraform
- AWS CLI authenticated into the target account and region
- An existing S3 bucket to hold source data, scripts, logs, and results
- An infra deployment role that can manage ECR, ECS, IAM, CloudWatch Logs, and EMR Serverless

## EMR Serverless IAM notes

The infra deployment role needs these capabilities in addition to the ECS and ECR permissions already in place:

- `emr-serverless:CreateApplication`
- `emr-serverless:GetApplication`
- `emr-serverless:UpdateApplication`
- `emr-serverless:DeleteApplication`
- `emr-serverless:StartApplication`
- `emr-serverless:StopApplication`
- `emr-serverless:ListApplications`
- `emr-serverless:StartJobRun`
- `emr-serverless:GetJobRun`
- `emr-serverless:ListJobRuns`
- `emr-serverless:CancelJobRun`
- `emr-serverless:GetDashboardForJobRun`
- `emr-serverless:TagResource`
- `emr-serverless:UntagResource`
- `emr-serverless:ListTagsForResource`
- `iam:PassRole` with `iam:PassedToService = emr-serverless.amazonaws.com`
- `iam:CreateServiceLinkedRole` with `iam:AWSServiceName = ops.emr-serverless.amazonaws.com`

The EMR job runtime role is separate. Terraform creates that role for this stack and grants it S3 access to the benchmark bucket and script prefix.

## Local setup

Install base dependencies:

```bash
uv sync --python 3.13
```

Generate data if needed:

```bash
uv run python -m skew_demo.generate_data --bucket matt-sbx-bucket-1-us-east-1 --run-date 2026-08-12
```

Run the DuckDB job locally against S3:

```bash
uv run python -m skew_demo.duckdb_job --bucket matt-sbx-bucket-1-us-east-1 --run-date 2026-08-12 --benchmark-id local-test
```

Run the Spark job locally:

```bash
uv run python -m skew_demo.spark_job --bucket matt-sbx-bucket-1-us-east-1 --run-date 2026-08-12 --benchmark-id local-test
```

## Deploy

The deployment script does four things:

1. Bootstraps the ECR repository with Terraform
2. Builds and pushes the Python 3.13 DuckDB container image
3. Uploads the Spark entrypoint script and zipped Python package to S3
4. Runs a full Terraform apply with the pushed image URI

Example:

```bash
AWS_REGION=us-east-1 \
BUCKET_NAME=matt-sbx-bucket-1-us-east-1 \
./scripts/deploy.sh
```

Optional environment variables:

- `ROOT_PREFIX`
- `DATASET_PREFIX`
- `SCRIPT_PREFIX`
- `PROJECT_NAME`
- `IMAGE_TAG`

## Run the benchmark

The benchmark script:

1. Launches the ECS Fargate DuckDB task
2. Waits for completion
3. Starts the EMR Serverless Spark job
4. Waits for completion
5. Downloads the benchmark JSON from S3 for both engines
6. Downloads Spark skew diagnostics and control-plane timing diagnostics
7. Generates `build/benchmark/benchmark_summary.json` and prints a side-by-side summary

Example:

```bash
RUN_DATE=2026-08-12 ./scripts/run_benchmarks.sh
```

Optional environment variables:

- `BENCHMARK_ID`
- `AWS_REGION`
- `BUCKET_NAME`
- `ROOT_PREFIX`
- `DATASET_PREFIX`
- `SCRIPT_PREFIX`

## Result locations

For a benchmark id like `20260812T190000Z`, results are written to:

- `s3://<bucket>/<root_prefix>/results/run_date=<date>/benchmark_id=<id>/engine=duckdb/aggregate_results.json`
- `s3://<bucket>/<root_prefix>/results/run_date=<date>/benchmark_id=<id>/engine=duckdb/benchmark_result.json`
- `s3://<bucket>/<root_prefix>/results/run_date=<date>/benchmark_id=<id>/engine=spark/aggregate_results/` (Spark JSON dataset directory)
- `s3://<bucket>/<root_prefix>/results/run_date=<date>/benchmark_id=<id>/engine=spark/benchmark_result.json`
- `s3://<bucket>/<root_prefix>/results/run_date=<date>/benchmark_id=<id>/engine=spark/skew_analysis.json`

Each `benchmark_result.json` includes:

- logical start timestamp
- logical finish timestamp
- elapsed processing seconds
- transcript count
- message count
- aggregate row count
- output URI
- metrics URI

Control-plane diagnostics are written locally to:

- `build/benchmark/duckdb_control_plane.json`
- `build/benchmark/spark_control_plane.json`

These report submit API latency, time-to-running (provision/startup), and control-plane wall-clock duration.

Spark runtime utilization diagnostics are written locally to:

- `build/benchmark/spark_runtime_diagnostics.json`

This file includes EMR Serverless-reported resource utilization and an observed executor count inferred from Spark executor log folders.

Spark skew diagnostics are written to:

- S3: `.../engine=spark/skew_analysis.json`
- local: `build/benchmark/spark_skew_analysis.json`

The skew analysis summarizes input file size distribution and largest files to help identify potential skew from oversized files.

A consolidated benchmark summary is written to:

- `build/benchmark/benchmark_summary.json`

## Teardown

Destroy benchmark infrastructure:

```bash
./scripts/teardown.sh
```

## Auth failures

If deployment or benchmark commands fail with AWS auth errors, refresh the assumed-role session before retrying. The scripts do not attempt to repair expired AWS credentials.
