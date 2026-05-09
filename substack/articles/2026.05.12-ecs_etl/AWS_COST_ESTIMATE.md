# AWS Infrastructure Cost Estimate (Demo)

This document provides a practical cost estimate for this ECS Fargate ETL demo.

## Scope

This estimate is based on resources configured in Terraform and current example values:

- Region: `us-east-1`
- ECS task size: `0.5 vCPU`, `1 GB memory`
- Scheduler: daily run (`cron(0 5 * * ? *)`)
- Network mode: Fargate task with public IP assigned at runtime
- Existing S3 buckets are reused (not created by Terraform)

## Resources In Scope

- ECS Fargate task runtime (CPU + memory)
- Public IPv4 while task runs
- EventBridge Scheduler
- CloudWatch Logs
- ECR repository image storage
- AWS Secrets Manager secret (Slack webhook)
- S3 read/write requests and storage deltas from ETL I/O

No NAT Gateway or managed database is created in this project, which avoids the largest common always-on costs.

## Pricing Inputs (Ballpark)

These rates are approximate and can change over time. Validate in AWS Pricing Calculator before production use.

- Fargate vCPU: `$0.04048` per vCPU-hour
- Fargate memory: `$0.004445` per GB-hour
- Public IPv4: `$0.005` per hour (when attached)
- Secrets Manager: `$0.40` per secret-month
- ECR storage: about `$0.10` per GB-month

## Runtime Cost Formula

For this demo task size:

$$
\text{Fargate/hour} = (0.5 \times 0.04048) + (1 \times 0.004445) = 0.024685
$$

$$
\text{Task runtime/hour incl. IPv4} = 0.024685 + 0.005 = 0.029685
$$

So the task costs about **$0.0297 per runtime hour**.

## Cost by Service

### 1) ECS Fargate + Public IPv4 (dominant variable)

Estimated runtime cost at current sizing:

- 5 minutes: about `$0.0025`
- 15 minutes: about `$0.0074`
- 30 minutes: about `$0.0148`
- 60 minutes: about `$0.0297`

### 2) Secrets Manager

- 1 secret (Slack webhook): about `$0.40/month`
- If resources exist for 1 day only: about `$0.013`

### 3) ECR

- Charged for image storage only (no hourly charge)
- Example: 1 GB image for a full month is about `$0.10`
- Example: 1 GB image for 1 day is about `$0.003`

### 4) CloudWatch Logs

- Charged by ingestion/storage
- For demo-scale ETL logs, this is usually pennies or less

### 5) EventBridge Scheduler

- 1 scheduled run/day is typically tiny/negligible at this scale

### 6) S3 (existing buckets)

- Standard request and storage pricing applies to ETL reads/writes
- Small demo datasets are usually pennies

### 7) IAM, ECS cluster metadata, security group

- No meaningful direct runtime charge

## Estimated Total: Monthly (If Left Running)

Assuming one scheduled run per day and no unusual data volume:

- 15-minute run/day: about `$0.70 - $1.50` / month
- 30-minute run/day: about `$0.90 - $2.00` / month
- 60-minute run/day: about `$1.30 - $3.00` / month

These totals include rough allowances for Secrets Manager, ECR, CloudWatch Logs, and light S3 usage.

## Estimated Total: Short-Lived Demo (Deploy, Test, Destroy < 1 Day)

Likely total:

- **About `$0.05 - $0.40`**

Typical midpoint for a few short test runs:

- **About `$0.10 - $0.25`**

Primary driver is total task runtime minutes.

## Quick Scenarios

### Scenario A: 3 manual tests, each 10 minutes

- Runtime = 30 minutes total
- Fargate + IPv4 = about `$0.0148`
- Add 1-day prorated secret + small logs/ECR/S3 = still usually under `$0.10`

### Scenario B: 6 tests, each 20 minutes

- Runtime = 120 minutes total
- Fargate + IPv4 = about `$0.0594`
- Add 1-day prorated secret + small logs/ECR/S3 = often around `$0.10 - $0.25`

## How to Keep Cost Low During Demo

- Destroy Terraform resources immediately after screenshots/tests
- Avoid large repeated ETL input files
- Keep manual test runs short
- Do not leave extra image tags/layers in ECR longer than needed

## Accuracy Notes

- This is a ballpark estimate, not a billing guarantee.
- Final cost depends on run duration, data volume, logs, image size, and retries.
- For formal numbers, run AWS Pricing Calculator with your exact expected minutes and data transfer/ingestion.
