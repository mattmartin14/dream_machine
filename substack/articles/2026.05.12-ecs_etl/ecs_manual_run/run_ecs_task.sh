#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TF_DIR="$PROJECT_ROOT/terraform"

if ! command -v aws >/dev/null 2>&1; then
  echo "Error: aws CLI is required." >&2
  exit 1
fi

if ! command -v terraform >/dev/null 2>&1; then
  echo "Error: terraform is required." >&2
  exit 1
fi

if [[ ! -f "$TF_DIR/terraform.tfvars" ]]; then
  echo "Error: terraform/terraform.tfvars not found. Run from a configured checkout." >&2
  exit 1
fi

normalize_region() {
  local raw="$1"
  local value="${raw// /}"

  # Accept common mistaken format like: aws_region="us-east-1"
  if [[ "$value" == aws_region=* ]]; then
    value="${value#aws_region=}"
  fi

  # Strip optional wrapping quotes.
  value="${value%\"}"
  value="${value#\"}"

  printf '%s' "$value"
}

AWS_REGION="${AWS_REGION:-$(grep -E '^aws_region' "$TF_DIR/terraform.tfvars" | head -n1 | sed -E 's/.*=\s*"([^"]+)"/\1/') }"
AWS_REGION="$(normalize_region "$AWS_REGION")"

if [[ -z "$AWS_REGION" ]]; then
  AWS_REGION="us-east-1"
fi

if [[ ! "$AWS_REGION" =~ ^[a-z]{2}(-gov)?-[a-z]+-[0-9]+$ ]]; then
  echo "Error: AWS_REGION is invalid: '$AWS_REGION'" >&2
  echo "Set AWS_REGION to a value like 'us-east-1'." >&2
  exit 1
fi

CLUSTER_ARN="${CLUSTER_ARN:-$(terraform -chdir="$TF_DIR" output -raw ecs_cluster_arn)}"
TASK_DEF_ARN="${TASK_DEF_ARN:-$(terraform -chdir="$TF_DIR" output -raw ecs_task_definition_arn)}"
TASK_COUNT="${TASK_COUNT:-1}"
SCRIPT_S3_URI="${SCRIPT_S3_URI:-${1:-}}"

if [[ -z "$SCRIPT_S3_URI" ]]; then
  echo "Error: SCRIPT_S3_URI is required (example: s3://bucket/path/to/script.py)." >&2
  echo "Hint: SCRIPT_S3_URI=s3://my-bucket/etl/scripts/job.py ./ecs_manual_run/run_ecs_task.sh" >&2
  exit 1
fi

NAME_PREFIX="$(terraform -chdir="$TF_DIR" console <<'EOF' | tr -d '"\r\n'
local.name_prefix
EOF
)"

SG_NAME="${NAME_PREFIX}-sg"
SG_ID="${SG_ID:-$(aws ec2 describe-security-groups \
  --region "$AWS_REGION" \
  --filters "Name=group-name,Values=$SG_NAME" \
  --query 'SecurityGroups[0].GroupId' \
  --output text)}"

if [[ -z "$SG_ID" || "$SG_ID" == "None" ]]; then
  echo "Error: could not find security group '$SG_NAME' in $AWS_REGION." >&2
  echo "Hint: set SG_ID explicitly, e.g. SG_ID=sg-1234 ./ecs_manual_run/run_ecs_task.sh" >&2
  exit 1
fi

SUBNETS_CSV="${SUBNETS_CSV:-}"

if [[ -z "$SUBNETS_CSV" ]]; then
  DEFAULT_VPC_ID="$(aws ec2 describe-vpcs \
    --region "$AWS_REGION" \
    --filters Name=isDefault,Values=true \
    --query 'Vpcs[0].VpcId' \
    --output text)"

  if [[ -z "$DEFAULT_VPC_ID" || "$DEFAULT_VPC_ID" == "None" ]]; then
    echo "Error: no default VPC found in $AWS_REGION." >&2
    exit 1
  fi

  SUBNETS_CSV="$(aws ec2 describe-subnets \
    --region "$AWS_REGION" \
    --filters Name=vpc-id,Values="$DEFAULT_VPC_ID" Name=default-for-az,Values=true \
    --query 'Subnets[].SubnetId' \
    --output text | tr '\t' ',')"
fi

if [[ -z "$SUBNETS_CSV" ]]; then
  echo "Error: could not resolve default subnets in VPC $DEFAULT_VPC_ID." >&2
  exit 1
fi

echo "Running ECS task with:"
echo "  AWS_REGION=$AWS_REGION"
echo "  CLUSTER_ARN=$CLUSTER_ARN"
echo "  TASK_DEF_ARN=$TASK_DEF_ARN"
echo "  SG_ID=$SG_ID"
echo "  SUBNETS_CSV=$SUBNETS_CSV"
echo "  TASK_COUNT=$TASK_COUNT"
echo "  SCRIPT_S3_URI=$SCRIPT_S3_URI"

OVERRIDES_JSON="$(printf '{\"containerOverrides\":[{\"name\":\"etl\",\"command\":[\"%s\"]}]}' "$SCRIPT_S3_URI")"

aws ecs run-task \
  --region "$AWS_REGION" \
  --cluster "$CLUSTER_ARN" \
  --task-definition "$TASK_DEF_ARN" \
  --launch-type FARGATE \
  --count "$TASK_COUNT" \
  --overrides "$OVERRIDES_JSON" \
  --network-configuration "awsvpcConfiguration={subnets=[$SUBNETS_CSV],securityGroups=[$SG_ID],assignPublicIp=ENABLED}" \
  --output json
