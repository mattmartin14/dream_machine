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

require_cmd terraform

cd "$TF_DIR"
terraform init
terraform destroy \
  -auto-approve \
  -var="aws_region=$AWS_REGION" \
  -var="bucket_name=$BUCKET_NAME" \
  -var="root_prefix=$ROOT_PREFIX" \
  -var="dataset_prefix=$DATASET_PREFIX" \
  -var="script_prefix=$SCRIPT_PREFIX" \
  -var="project_name=$PROJECT_NAME"

echo "Teardown complete"
