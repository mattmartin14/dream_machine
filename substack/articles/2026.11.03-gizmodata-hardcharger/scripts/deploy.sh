#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ "${1:-}" != "--apply" ]]; then
  echo "No AWS changes were made. Review costs and run deploy.sh --apply to deploy." >&2
  exit 2
fi
shift
WITH_DATA=false
while (($#)); do
  case "$1" in
    --with-data) WITH_DATA=true ;;
    *) echo "Unknown option: $1" >&2; exit 2 ;;
  esac
  shift
done

for command in terraform aws docker jq uv python3; do
  command -v "$command" >/dev/null || { echo "Required command not found: $command" >&2; exit 2; }
done

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

REGION="${AWS_REGION:-us-east-2}"
aws sts get-caller-identity --region "$REGION" --output json >/dev/null

terraform -chdir=terraform init
terraform -chdir=terraform fmt -check -recursive
terraform -chdir=terraform validate
current_image="bootstrap:pending"
if [[ -f .deploy-image-uri ]]; then
  current_image="$(<.deploy-image-uri)"
fi
plan_file="$(mktemp)"
trap 'rm -f "$plan_file"' EXIT
terraform -chdir=terraform plan -out="$plan_file" \
  -var="aws_region=$REGION" \
  -var="container_image=$current_image" \
  -var="enable_schedule=false"
terraform -chdir=terraform apply -auto-approve "$plan_file"

account_id="$(aws sts get-caller-identity --region "$REGION" --query Account --output text)"
repository_url="$(terraform -chdir=terraform output -raw ecr_repository_url)"
image_tag="deployment-$(python3 -c 'import uuid; print(uuid.uuid4().hex[:12])')"
image_uri="${repository_url}:${image_tag}"
aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "$account_id.dkr.ecr.$REGION.amazonaws.com"
docker build --platform linux/amd64 -f docker/Dockerfile -t "$image_uri" .
docker push "$image_uri"
printf '%s\n' "$image_uri" >.deploy-image-uri
chmod 600 .deploy-image-uri

terraform -chdir=terraform apply -auto-approve \
  -var="aws_region=$REGION" \
  -var="container_image=$image_uri" \
  -var="enable_schedule=false"

if [[ ! -f hardcharger/cluster-config.local.env ]]; then
  echo "AWS infrastructure and image are ready; Hardcharger API config is missing." >&2
  echo "Fill hardcharger/cluster-config.local.env, then rerun deploy.sh --apply." >&2
  echo "AWS resources remain available for retry or explicit cleanup." >&2
  exit 3
fi
./hardcharger/provision.sh

export AWS_REGION="$REGION"
export HARDCHARGER_SECRET_ARN="$(terraform -chdir=terraform output -raw hardcharger_secret_arn)"
export RAW_BUCKET="$(terraform -chdir=terraform output -raw raw_bucket_name)"
export HARDCHARGER_URI=""
export HARDCHARGER_USERNAME=""
if [[ -f hardcharger/cluster-config.env ]]; then
  set -a
  # shellcheck disable=SC1091
  source hardcharger/cluster-config.env
  set +a
fi
uv sync --extra dev
uv run python scripts/smoke_test.py

if [[ "$WITH_DATA" == true ]]; then
  uv run python scripts/generate_tpch.py --scale-factor "${TPCH_SCALE_FACTOR:-1}"
  uv run python scripts/upload_tpch.py --bucket "$RAW_BUCKET"
  export ECS_CLUSTER_NAME="$(terraform -chdir=terraform output -raw ecs_cluster_name)"
  export ECS_TASK_DEFINITION_ARN="$(terraform -chdir=terraform output -raw ecs_task_definition_arn)"
  export ECS_SUBNET_IDS="$(terraform -chdir=terraform output -json task_subnet_ids | jq -r 'join(",")')"
  export ECS_SECURITY_GROUP_ID="$(terraform -chdir=terraform output -raw task_security_group_id)"
  if [[ "$(terraform -chdir=terraform output -raw assign_public_ip)" == "true" ]]; then
    export ECS_ASSIGN_PUBLIC_IP=ENABLED
  else
    export ECS_ASSIGN_PUBLIC_IP=DISABLED
  fi
  ./scripts/run_etl_aws.sh
else
  echo "Dataset generation/upload and the initial ETL run were skipped. Use --with-data to request them."
fi

echo "Deployment complete. The recurring EventBridge schedule remains disabled by default."
terraform -chdir=terraform output
