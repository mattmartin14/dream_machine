# Deployment runbook

## Before AWS changes

1. Review README safety/cost notes and fill `terraform/terraform.tfvars`.
2. Select an AWS profile via the normal AWS CLI configuration or `AWS_PROFILE`.
3. Obtain the actual cluster management curl request from GizmoData and complete
   the ignored `hardcharger/cluster-config.local.env` plus payload file.
4. Confirm the Hardcharger service can access the raw S3 prefix.
5. Review `terraform plan`; recurring schedule must remain disabled.

The top-level deployment requires `--apply`; `--with-data` separately authorizes
SF1 generation, S3 upload, and the initial Fargate ETL. Neither this repository
setup nor its tests contacted AWS.

## Terraform

Terraform creates isolated project resources: S3 buckets, S3 Tables bucket and
Iceberg schema, ECR, ECS, Scheduler, CloudWatch, IAM roles, a Secrets Manager
secret, and DynamoDB lock storage. The secret object is created empty and gets a
value only after managed Hardcharger provisioning. No secret value is stored in
Terraform state.

When existing VPC inputs are set, Terraform creates only a task security group
and uses the supplied subnets. Without them, Terraform creates a small public
VPC. A private existing VPC must have routes/endpoints for AWS APIs and the
public Hardcharger TLS endpoint.

## Failure recovery

If AWS provisioning succeeds but managed-service provisioning fails, resources
are intentionally retained. Review the response and remote cluster state before
retrying. Continue with the corrected local config or run the explicit destroy
script. A failed API response cannot be safely auto-cleaned without the vendor's
actual response schema and deletion semantics.

The top-level destroy script supports `--delete-hardcharger`. When enabled, it
calls the documented `DELETE /api/database-clusters/{id}?wait=true` endpoint
using the saved cluster UUID and protected `hardcharger_ci_token` environment
variable before deleting AWS data/resources. The wait parameter makes remote
termination synchronous; API failure stops AWS teardown. The call intentionally
omits `purge=true`, which the API documents as a system-admin-only removal of
retained history.

## Schedule

`enable_schedule` is `false` by default. After manual smoke and backfill
validation, set it to `true` and apply the change deliberately. Use an
appropriate schedule expression and UTC timezone.
