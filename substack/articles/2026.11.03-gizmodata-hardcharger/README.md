# Hardcharger AWS Data Platform Demo

A reproducible demonstration of managed GizmoData Hardcharger orchestrated by
AWS, processing synthetic TPC-H Parquet data into DuckLake aggregations. The
project is designed for an enterprise architecture article, not comparative
database benchmarking.

**No resources have been deployed by this repository setup.** AWS deployment is
explicitly gated. The EventBridge schedule defaults to disabled. Do not run the
deployment until you have reviewed the expected AWS spend and filled the missing
GizmoData integration inputs.

## Verified behavior and open integration points

Official Hardcharger documentation reviewed for this project confirms:

- Hardcharger is a managed, remote DuckDB service. Its documented Python driver
  is `adbc-driver-hardcharger`, using the Warp SQL ADBC protocol and Arrow results.
- `hardcharger://host` uses HTTPS/TLS by default (documented default port 31337).
  Credentials are passed separately from the URI; the driver documents TLS CA
  and mutual-TLS options.
- DuckLake is supported by the service. The actual catalog URI, storage access,
  and initialization statement for a customer cluster are configuration-specific
  and must be confirmed with GizmoData.
- SQL access control, row policies, masking, audit/instrumentation, and query
  timeouts are documented. SQL examples here follow the official policy syntax.
- The AWS provider documents S3 Tables bucket, namespace, and Iceberg table
  resources. That does **not** prove that this managed Hardcharger service is
  configured for AWS S3 Tables REST catalog access.

The checked documentation did not identify a GizmoData cluster-management API
endpoint or payload/response schema. The adapter therefore does not guess one.
The response field selectors and optional readiness/deletion calls must come
from the actual GizmoData API response/documentation.

The supplied create curl establishes the management request as `POST
https://app.gizmodata.com/api/database-clusters/?wait=false`, authenticated by a
Bearer token, with a JSON payload and a `cloud_region.id` for GizmoData's
`us-east-2`. It does not include the response schema. The captured request's
`allow_any_ip: true` was intentionally tightened to `false` in the rendered
payload; configure `HC_IP_ACCESS_LIST_JSON` using the exact IP-entry format
GizmoData documents before provisioning. The renderer refuses an empty allowlist
when wildcard access is disabled. Set `HC_ALLOW_ANY_IP=true` only as an
intentional, reviewed choice; it preserves the captured curl's broad ingress.

## Prerequisites

- Python 3.12+, `uv`, Docker, AWS CLI v2, Terraform >= 1.7, `jq`, and Bash.
- An AWS profile/credential chain with permission to create this project's
  resources, use IAM `PassRole` for its ECS/Scheduler roles, push ECR images,
  write its Secrets Manager value, upload its S3 dataset, and read Cost Explorer
  data if actual cost reporting is requested.
- A GizmoData-managed Hardcharger cluster, a supported connection identity, and
  its actual TLS/authentication configuration.
- A confirmed way for the managed service to read the private S3 raw bucket.
  The Fargate task role is **not** inherited by the separately managed
  Hardcharger service. A bucket policy grant is included only when a verified
  `hardcharger_s3_principal_arn` is supplied; GizmoData must also configure that
  principal on its side.

Never add AWS or Hardcharger credentials to Terraform variables, source files,
SQL, or commits. `.env` and local Hardcharger configuration are gitignored.

## Local setup

```bash
uv sync --extra dev
cp .env.example .env
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
cp hardcharger/cluster-config.example.env hardcharger/cluster-config.local.env
chmod 600 .env hardcharger/cluster-config.local.env
uv run pytest
uv run ruff check .
terraform -chdir=terraform fmt -check -recursive
```

The `.env` file is optional for most local unit tests. Fill only the values needed
for the operation you run. AWS CLI and boto3 use the normal AWS profile/credential
chain; no AWS key variables are embedded in this project.

### Application settings

| Name | Required for | Description |
|---|---|---|
| `AWS_REGION` | AWS resources, Secrets Manager | Defaults to `us-east-2`, aligned with the managed service |
| `HARDCHARGER_CLOUD_REGION` | GizmoData cluster request | Fixed to `us-east-2` by the supplied request |
| `HARDCHARGER_URI` | Local connection | TLS-enabled Hardcharger URI; can be read from the secret |
| `HARDCHARGER_USERNAME` | Local connection | Username; can be read from the secret |
| `HARDCHARGER_PASSWORD` | Local connection | Optional local override; prefer Secrets Manager |
| `HARDCHARGER_SECRET_ARN` | ECS connection | Secret JSON containing `uri`, `username`, and `password` |
| `RAW_BUCKET` | ETL/upload | General-purpose S3 bucket name |
| `RAW_PREFIX` | ETL/upload | Defaults to `tpch` |
| `DUCKLAKE_CATALOG` | ETL | Output catalog; defaults to `analytics` |
| `DUCKLAKE_SCHEMA` | ETL | Output schema; defaults to `main` |
| `PROCESSING_DATE` | ETL | One ISO date, e.g. `2026-10-23` |
| `PROCESSING_START_DATE`, `PROCESSING_END_DATE` | Backfill | Inclusive ISO date range; set both |
| `QUERY_TIMEOUT_SECONDS` | ADBC | Documented Hardcharger query timeout setting |
| `DDB_LOCK_TABLE` | ECS ETL | Set by Terraform for exclusive scheduled-run locking |

With no processing date or range, ETL uses the previous UTC calendar date. This
is a deterministic scheduling convention, not a claim that the static TPC-H data
arrives every day.

## Provisioning managed Hardcharger

1. The create request is `POST /api/database-clusters/?wait=false`, with the body in
   `hardcharger/cluster-request.json.template`, sent by `hardcharger/provision.sh`.
2. Fill `hardcharger/cluster-config.local.env`, provide the API key and admin
   password via protected environment/CI secrets, and set jq selectors for the response
   fields holding the cluster ID, TLS URI, username, and password. Selectors are
   supplied by you from the actual response schema; no response field names are
   presumed.
3. Use `HC_API_HEADERS_JSON` for the actual documented headers. This local config
   is a sourced shell file; only put trusted shell assignments in it. Keep it
   mode `0600`.
4. Set readiness endpoint/method/state selector only if GizmoData documents
   those details. Without them, the script explicitly reports readiness polling
   skipped.
5. After Terraform creates the Secrets Manager secret, run
   `./hardcharger/provision.sh`.

The adapter keeps the full response in a mode-0600, gitignored file so a
one-time credential can be extracted. It writes nonsecret local metadata and
stores the selected URI, username, and password in AWS Secrets Manager. A second
run uses the existing local cluster ID and does not issue another create call.
This is local idempotency only. If the API created a cluster but the response
cannot be interpreted, do not blindly retry; inspect the response and confirm
the remote resource state with GizmoData first. Use an API idempotency key only
if the vendor confirms the exact mechanism.

`./hardcharger/destroy.sh --confirm` uses the documented
`DELETE /api/database-clusters/{id}?wait=true` operation and requires
`GIZMODATA_API_KEY` in the protected environment. It waits for the GizmoData
API to report termination before removing local metadata. It does not set
`purge=true`: the documented purge operation is system-admin-only and removes
retained billing/audit history.

## AWS deployment

Review `terraform/terraform.tfvars.example`; optionally set an existing VPC and
at least two subnets. Then:

```bash
./scripts/deploy.sh --apply
```

This explicit flag authorizes creation of AWS project resources. The script:

1. Checks local tools and AWS identity/region.
2. Initializes and validates Terraform and applies project infrastructure.
3. Builds/pushes the ECR image and updates the task definition.
4. Calls the configurable managed Hardcharger provisioning adapter.
5. Runs a connection/S3-caller smoke check.

To also generate/upload the default SF1 data and execute an initial Fargate ETL:

```bash
./scripts/deploy.sh --apply --with-data
```

SF1 is the initial data size; set `TPCH_SCALE_FACTOR` to another positive
integer in `.env` to change it. Generation and upload use local disk, S3 storage,
and request charges, and are not run unless `--with-data` is requested.
If managed-service provisioning fails after AWS succeeds, the AWS resources are
left in place for safe retry or explicit cleanup; the script prints the state of
the partial deployment. It does not destroy data or infrastructure implicitly.

### Networking

With no existing VPC, Terraform creates two public subnets, an internet gateway,
an S3 gateway endpoint, and an outbound-only task security group. Fargate tasks
receive public IPs by default so they can reach the public managed Hardcharger
endpoint without a NAT Gateway. No inbound task rules are created. This avoids
NAT hourly/data processing charges but exposes task egress through public
addresses.

For private subnets, set `assign_public_ip = false` and provide NAT egress or
suitable VPC endpoints. Typical private endpoint needs include ECR API/DKR,
CloudWatch Logs, Secrets Manager, CloudWatch metrics, and DynamoDB (gateway),
plus external network egress to Hardcharger. No private connectivity to
GizmoData is assumed or provisioned. Ensure private DNS, routing, and TLS reach
the actual service endpoint before running the task.

## TPC-H dataset

TPC-H is synthetic data reused here as representative business-shaped data. This
project is not running or publishing a comparative TPC-H benchmark.

```bash
uv run python scripts/generate_tpch.py --scale-factor 1
uv run python scripts/validate_pipeline.py
uv run python scripts/upload_tpch.py --bucket "$RAW_BUCKET"
```

Scale factor 1 is the initial default to keep generated files manageable; larger
scale factors can be selected explicitly when the environment is ready. Generation
and upload are separate, repeatable operations. The generator writes
the eight standard tables as Zstandard Parquet under `data/tpch/`, with row
counts/file sizes in `manifest.json`. The uploader writes deterministic S3 keys
and the manifest under `s3://<bucket>/tpch/`. Data is ignored by Git. Actual
compressed volume and timings must be measured on the chosen machine; no size or
execution time is pre-reported.

## ETL and idempotency

Run locally:

```bash
uv run hardcharger-etl
PROCESSING_DATE=1995-01-01 uv run hardcharger-etl
PROCESSING_START_DATE=1995-01-01 PROCESSING_END_DATE=1995-01-31 uv run hardcharger-etl
```

The Python process orchestrates SQL through the documented Hardcharger ADBC
driver. Hardcharger reads the Parquet files directly from S3; Fargate does not
download the multi-gigabyte dataset. The service-side AWS identity and `READ
FILESYSTEM` grant must be verified with GizmoData. The ETL computes:

- Daily order count and line-item count.
- Gross extended price, discount amount (`extendedprice * discount`), net
  revenue (`extendedprice * (1 - discount)`), and net average order value.
- A second daily aggregate grouped by actual TPC-H nation key and region key.

For each processing date, the application deletes the existing date slice from
both outputs, inserts the recomputed data, reconciles nation totals to daily
totals, validates revenue arithmetic, then commits. Failures roll back. The
integration gate must verify that the specific managed DuckLake catalog supports
the transaction and DML path before recurring execution is enabled. The driver
documents retries only for safe-to-repeat requests; the application does not
blindly retry writes.

The Fargate task uses a DynamoDB conditional-put lock to prevent overlapping
ETL invocations. It has no automatic stale lock expiry, avoiding takeover while
an unusually long prior task may still be running. If a task is forcibly killed,
inspect ECS before manually removing the `daily-etl` item from the lock table.

## EventBridge schedule and observability

The Terraform default is daily at `cron(0 6 * * ? *)` UTC and `DISABLED`.
`enable_schedule` must be deliberately set to `true` in your Terraform variables
before an apply will start recurring runs. Validate manually first.

ECS JSON logs include execution ID, dates, start/end timestamps, source manifest
row counts/bytes, output rows, SQL/total durations, validation status, and
exceptions. CloudWatch alarms cover ETL failures, long runs, and Scheduler target
errors. They have no notification action configured; connect an SNS topic or
your alerting system before production use. Scheduler invocation metrics do not
capture every container failure; inspect the ECS task stopped reason and
CloudWatch logs as well.

## Optional federation

Terraform creates an S3 Tables table bucket, namespace, and an empty Iceberg
`customer_segment` table. The table's current schema is `nationkey` and `segment`.
The small reference data generator uses synthetic segment labels; it does not
claim to represent actual customer segmentation.

The S3 Tables REST catalog and cross-account auth path have not been confirmed
for managed Hardcharger. Do not run the S3 Tables query until GizmoData confirms
catalog support and an administrator configures a supported catalog/auth path.
Provide `FEDERATION_DUCKLAKE_TABLE` and `FEDERATION_ICEBERG_TABLE` as safe,
qualified SQL identifiers and run:

```bash
FEDERATION_MODE=s3tables \
FEDERATION_QUERY_FILE=sql/federation.sql \
uv run python scripts/run_federation.py
```

Fallback, if that integration is unsupported: generate/upload the tiny Parquet
lookup and run a direct S3 Parquet join, which uses the documented DuckDB file
query model and does not require S3 Tables catalog support:

```bash
uv run python scripts/generate_reference.py
uv run python scripts/upload_reference.py --bucket "$RAW_BUCKET"
FEDERATION_MODE=parquet \
FEDERATION_QUERY_FILE=sql/federation_parquet_fallback.sql \
uv run python scripts/run_federation.py
```

In either case, query execution and joins happen in managed Hardcharger. The
server reads data from DuckLake plus the reference location; result rows are
returned to the client as Arrow. Storage does not move merely because the query
joins the sources. The fallback still requires the managed server to have
verified S3 permissions.

## Security demonstration

Review the SQL files in this order, as administrator, after the initial ETL
created its target tables:

1. `sql/create_security_demo_table.sql` materializes the small synthetic
   customer table. Replace `<RAW_BUCKET>` and confirm service-side S3 access.
2. `sql/security_roles.sql` creates separate ETL, finance, regional, and
   reporting roles/users and least-privilege grants. Each access-control
   statement must be submitted separately.
3. `sql/row_policies.sql` applies regional filtering using the actual TPC-H
   `regionkey` and `session_attribute('regionkey')`.
4. `sql/column_masking.sql` masks synthetic customer names, addresses, and phone
   numbers; the finance role is exempt.
5. Set credentials with the supported Hardcharger auth flow. No passwords are
   supplied in the SQL examples.

The example uses actual TPC-H region keys `0` and `3`; query `main.tpch_region`
for their standard labels. It does not fabricate East/West assignments. The
official row-policy documentation says policy-covered users have read-only
tables and that only an administrator manages policies. The example ETL role
requires `READ FILESYSTEM` for external S3 Parquet and create access to its
target schema; verify the exact minimum system privilege with GizmoData.

The gated positive/negative integration tests require configured user variables
and populated tables; local unit tests never connect to AWS or Hardcharger:

```bash
uv run pytest -m integration tests/test_security_integration.py
```

The integration tests check region restrictions, masked output, and denied
reporting-user writes. Run them only after setting up the SQL objects and test
identities.

## FinOps

AWS actuals are retrieved from Cost Explorer only when explicitly requested:

```bash
uv run python scripts/cost_report.py --actuals --rows 1000000
```

Cost Explorer reports the previous complete month by AWS service, subject to
permissions and billing-data availability. Rate-based estimates require user
supplied pricing in `.env` and measured usage values such as task vCPU-hours,
S3 storage/requests, CloudWatch log ingestion/storage, ECR storage, NAT,
networking, and Secrets Manager use. Missing rates remain `null`; they are not
filled with invented prices. The cost model treats a supplied monthly
Hardcharger amount as an idle/base charge and an hourly amount as active usage;
confirm whether those inputs are additive for the actual contract.

Sponsored Hardcharger access is represented as a $0 expense for this specific
demo only. The normal customer price remains **unknown** until GizmoData supplies
published pricing. No competing-database cost or performance comparison is
included.

## Validation and cleanup

```bash
uv run pytest
uv run ruff check .
terraform -chdir=terraform fmt -check -recursive
terraform -chdir=terraform init -backend=false
terraform -chdir=terraform validate
```

Destroy only project-managed resources after stopping tasks and reviewing the
Terraform plan:

```bash
./scripts/destroy.sh --confirm --delete-s3-data
```

The required `--delete-s3-data` flag explicitly authorizes deletion of the
general-purpose bucket contents and Terraform-managed S3 Tables resources. It
sets S3 Tables `force_destroy` before Terraform destroys the project table
bucket, removing all its namespaces, tables, and data.
Delete the managed cluster only with the separate documented API config and
`GIZMODATA_API_KEY` environment variable, then run
`./scripts/destroy.sh --confirm --delete-s3-data --delete-hardcharger`. The
Hardcharger delete runs before AWS object and infrastructure deletion; a failed
API call stops AWS teardown. DuckLake
objects are not deleted by this AWS script; remove them only through a separately
verified Hardcharger cleanup procedure. Existing VPCs/subnets are never managed
or destroyed by this project.

## Troubleshooting

- **Provision request fails:** inspect the protected response file and verify
  the endpoint/method/headers/payload from GizmoData. Do not retry a possibly
  successful create until remote state is checked.
- **ETL cannot access S3:** ECS task IAM applies only to Fargate. Verify the
  managed Hardcharger identity separately, bucket policy, S3 region, and
  documented filesystem permissions.
- **TLS failure:** use the URI and CA/client-certificate settings supplied by
  GizmoData. Never disable certificate verification in production.
- **DuckLake transaction error:** keep the schedule disabled; confirm the
  managed catalog's transaction, `DELETE`, and `INSERT` behavior before retry.
- **S3 Tables catalog error:** the main ETL is independent of this optional
  feature. Use the Parquet federation fallback and report the unsupported
  integration as a limitation.
- **Stale ETL lock:** confirm no task is still running, then remove only the
  project lock item from the DynamoDB table.
