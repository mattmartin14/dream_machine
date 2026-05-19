# ECS Fargate DuckDB ETL (End-to-End Demo)

This project runs Python ETL workloads on AWS ECS Fargate with a clean split between shared infrastructure and job-specific deployment.

## Terraform Structure

- `terraform_infra`: shared foundational infrastructure
- `terraform_etl_job`: generic ETL job deployment root

Module ownership is separated:

- `terraform_infra/modules`: infra-only modules
- `terraform_etl_job/modules`: ETL-job-only modules

## What Foundation Deploys

- ECR repository for runner images
- ECS cluster and CloudWatch log group
- ECS task/execution IAM roles
- Network security group
- S3 buckets:
  - `s3-sales-raw-test`
  - `s3-sales-agg-test`
- Slack webhook secret in Secrets Manager

## What ETL Job Deploys

- ECS task definition for a specific `etl_job_name`
- EventBridge Scheduler role and schedule for that job
- Runtime script S3 URI wired into task command

## Key ETL Job Parameters

The `terraform_etl_job` root accepts:

- `etl_job_name`: used in ECS task family and EventBridge schedule name
- `script_s3_uri`: passed to `runner.py` as the script location
- `container_cpu`
- `container_memory`
- `task_ephemeral_storage_gib`

Note: Local script upload is intentionally **not** done in Terraform. It is done by deploy script via `aws s3 cp`.

## Secure Slack Webhook Setup

Set this in `terraform_infra/terraform.tfvars` (or via env var `TF_VAR_slack_webhook`):

```hcl
slack_webhook_secret_name = "slack_webhook_v1"
slack_webhook             = "https://hooks.slack.com/services/XXX/YYY/ZZZ"
```

## Deploy Flow

### 1. Deploy Foundation

```bash
cp terraform_infra/terraform.tfvars.example terraform_infra/terraform.tfvars
./deploy/deploy_foundational.sh
```

Optional:

```bash
./deploy/deploy_foundational.sh v1.2.3
./deploy/deploy_foundational.sh latest --no-cache --auto-approve
```

### 2. Deploy an ETL Job (Generic)

Use the generic ETL deploy script. It uploads the local script with `aws s3 cp`, then runs Terraform apply in `terraform_etl_job`.

```bash
./deploy/deploy_etl_job.sh \
  --etl-job-name sales-etl \
  --local-script-path scripts/sales_etl.py \
  --script-s3-uri s3://s3-sales-agg-test/scripts/sales_etl.py
```

Optional tuning:

```bash
./deploy/deploy_etl_job.sh \
  --etl-job-name sales-etl-hourly \
  --local-script-path scripts/sales_etl.py \
  --script-s3-uri s3://s3-sales-agg-test/scripts/sales_etl_hourly.py \
  --container-cpu 1024 \
  --container-memory 4096 \
  --schedule-expression 'cron(0 * * * ? *)' \
  --auto-approve
```

### 3. Job-Specific Wrapper Example

`deploy/deploy_sales_job.sh` is a job-specific wrapper that calls `deploy/deploy_etl_job.sh` with prefilled values for the sales ETL job.

```bash
./deploy/deploy_sales_job.sh
./deploy/deploy_sales_job.sh --auto-approve
```

It deploys:
- `scripts/sales_etl.py` to `s3://s3-sales-agg-test/scripts/sales_etl.py`
- CPU `2048` (2 vCPU) and memory `4096` (4 GiB)
- Daily 5 AM schedule via `cron(0 5 * * ? *)`

## Manual Smoke Test (Optional)

```bash
CLUSTER_ARN=$(terraform -chdir=terraform_infra output -raw ecs_cluster_arn)
TASK_DEF_ARN=$(terraform -chdir=terraform_etl_job output -raw ecs_task_definition_arn)
SUBNET_IDS=$(aws ec2 describe-subnets --filters Name=default-for-az,Values=true --query 'Subnets[].SubnetId' --output text)
SG_ID=$(aws ec2 describe-security-groups --filters Name=group-name,Values=*ecs-duckdb-etl-test-sg* --query 'SecurityGroups[0].GroupId' --output text)
SCRIPT_S3_URI="s3://s3-sales-agg-test/scripts/sales_etl.py"

aws ecs run-task \
  --launch-type FARGATE \
  --cluster "$CLUSTER_ARN" \
  --task-definition "$TASK_DEF_ARN" \
  --overrides "$(printf '{\"containerOverrides\":[{\"name\":\"sales-etl\",\"command\":[\"%s\"]}]}' "$SCRIPT_S3_URI")" \
  --network-configuration "awsvpcConfiguration={subnets=[$(echo $SUBNET_IDS | sed 's/ /,/g')],securityGroups=[$SG_ID],assignPublicIp=ENABLED}"
```

## Cleanup

Destroy ETL jobs first, then infra:

```bash
terraform -chdir=terraform_etl_job destroy
terraform -chdir=terraform_infra destroy
```
