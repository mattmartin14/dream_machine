#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TF_DIR="$ROOT_DIR/terraform"

AWS_REGION="${AWS_REGION:-us-east-1}"
BUCKET_NAME="${BUCKET_NAME:-matt-sbx-bucket-1-us-east-1}"
ROOT_PREFIX="${ROOT_PREFIX:-etl-skew-demo}"
DATASET_PREFIX="${DATASET_PREFIX:-raw/returns_chat}"
SCRIPT_PREFIX="${SCRIPT_PREFIX:-etl/scripts/duckdb-vs-spark-emr}"
PROJECT_NAME="${PROJECT_NAME:-duckdb-vs-spark-emr}"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

require_cmd aws
require_cmd terraform

cd "$TF_DIR"
terraform init

EMR_APP_ID="$(aws --no-cli-pager emr-serverless list-applications \
  --region "$AWS_REGION" \
  --query "applications[?name=='$PROJECT_NAME'].id | [0]" \
  --output text 2>/dev/null || true)"

if [[ "$EMR_APP_ID" == "None" ]]; then
  EMR_APP_ID=""
fi

if [[ -n "$EMR_APP_ID" ]]; then
  ACTIVE_JOB_IDS="$(aws --no-cli-pager emr-serverless list-job-runs \
    --region "$AWS_REGION" \
    --application-id "$EMR_APP_ID" \
    --query "jobRuns[?state=='SUBMITTED' || state=='PENDING' || state=='SCHEDULED' || state=='RUNNING'].id" \
    --output text 2>/dev/null || true)"

  if [[ -n "$ACTIVE_JOB_IDS" && "$ACTIVE_JOB_IDS" != "None" ]]; then
    for job_id in $ACTIVE_JOB_IDS; do
      echo "Cancelling active EMR job $job_id before teardown"
      aws --no-cli-pager emr-serverless cancel-job-run \
        --region "$AWS_REGION" \
        --application-id "$EMR_APP_ID" \
        --job-run-id "$job_id" >/dev/null || true
    done
  fi

  EMR_APP_STATE="$(aws --no-cli-pager emr-serverless get-application --region "$AWS_REGION" --application-id "$EMR_APP_ID" --query 'application.state' --output text 2>/dev/null || true)"
  if [[ "$EMR_APP_STATE" == "STARTED" || "$EMR_APP_STATE" == "STARTING" || "$EMR_APP_STATE" == "STOPPING" ]]; then
    echo "Stopping EMR Serverless application $EMR_APP_ID before terraform destroy"
    if [[ "$EMR_APP_STATE" != "STOPPING" ]]; then
      aws --no-cli-pager emr-serverless stop-application --region "$AWS_REGION" --application-id "$EMR_APP_ID" >/dev/null || true
    fi
    while true; do
      EMR_APP_STATE="$(aws --no-cli-pager emr-serverless get-application --region "$AWS_REGION" --application-id "$EMR_APP_ID" --query 'application.state' --output text 2>/dev/null || true)"
      if [[ "$EMR_APP_STATE" == "STOPPED" || "$EMR_APP_STATE" == "CREATED" || -z "$EMR_APP_STATE" ]]; then
        break
      fi
      echo "Waiting for EMR application to stop; current state: $EMR_APP_STATE"
      sleep 10
    done
  fi
fi

ECR_REPO_EXISTS="$(aws --no-cli-pager ecr describe-repositories \
  --region "$AWS_REGION" \
  --repository-names "$PROJECT_NAME" \
  --query 'repositories[0].repositoryName' \
  --output text 2>/dev/null || true)"

if [[ -n "$ECR_REPO_EXISTS" && "$ECR_REPO_EXISTS" != "None" ]]; then
  while true; do
    DIGESTS="$(aws --no-cli-pager ecr list-images \
      --region "$AWS_REGION" \
      --repository-name "$PROJECT_NAME" \
      --query 'imageIds[].imageDigest' \
      --output text 2>/dev/null | tr '\t' '\n' | sed '/^$/d' | sort -u || true)"

    if [[ -z "$DIGESTS" ]]; then
      break
    fi

    echo "Deleting ECR images from $PROJECT_NAME before terraform destroy"
    image_args=()
    while IFS= read -r digest; do
      [[ -n "$digest" ]] || continue
      image_args+=("imageDigest=$digest")
    done <<< "$DIGESTS"

    aws --no-cli-pager ecr batch-delete-image \
      --region "$AWS_REGION" \
      --repository-name "$PROJECT_NAME" \
      --image-ids "${image_args[@]}" >/dev/null || true

    remaining="$(aws --no-cli-pager ecr list-images \
      --region "$AWS_REGION" \
      --repository-name "$PROJECT_NAME" \
      --query 'length(imageIds)' \
      --output text 2>/dev/null || true)"
    if [[ "$remaining" == "0" || "$remaining" == "None" || -z "$remaining" ]]; then
      break
    fi
    echo "Waiting for ECR repository to empty; remaining image refs: $remaining"
    sleep 2
  done
fi

terraform destroy \
  -auto-approve \
  -var="aws_region=$AWS_REGION" \
  -var="bucket_name=$BUCKET_NAME" \
  -var="root_prefix=$ROOT_PREFIX" \
  -var="dataset_prefix=$DATASET_PREFIX" \
  -var="script_prefix=$SCRIPT_PREFIX" \
  -var="project_name=$PROJECT_NAME"

echo "Teardown complete"
