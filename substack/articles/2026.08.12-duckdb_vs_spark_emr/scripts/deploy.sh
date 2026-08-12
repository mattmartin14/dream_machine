#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TF_DIR="$ROOT_DIR/terraform"
BUILD_DIR="$ROOT_DIR/build"

AWS_REGION="${AWS_REGION:-us-east-1}"
BUCKET_NAME="${BUCKET_NAME:-matt-sbx-bucket-1-us-east-1}"
ROOT_PREFIX="${ROOT_PREFIX:-etl-skew-demo}"
DATASET_PREFIX="${DATASET_PREFIX:-raw/returns_chat}"
SCRIPT_PREFIX="${SCRIPT_PREFIX:-etl/scripts/duckdb-vs-spark-emr}"
PROJECT_NAME="${PROJECT_NAME:-duckdb-vs-spark-emr}"
IMAGE_TAG="${IMAGE_TAG:-latest}"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

require_cmd aws
require_cmd docker
require_cmd terraform
require_cmd zip
require_cmd python3

mkdir -p "$BUILD_DIR"

cd "$TF_DIR"
terraform init

terraform apply \
  -target=aws_ecr_repository.duckdb_runner \
  -auto-approve \
  -var="aws_region=$AWS_REGION" \
  -var="bucket_name=$BUCKET_NAME" \
  -var="root_prefix=$ROOT_PREFIX" \
  -var="dataset_prefix=$DATASET_PREFIX" \
  -var="script_prefix=$SCRIPT_PREFIX" \
  -var="project_name=$PROJECT_NAME"

REPO_URL="$(terraform output -raw ecr_repository_url)"
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"

aws ecr get-login-password --region "$AWS_REGION" | docker login --username AWS --password-stdin "$ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com"

rm -f "$BUILD_DIR/skew_demo_package.zip"
cd "$ROOT_DIR"
zip -qr "$BUILD_DIR/skew_demo_package.zip" skew_demo

aws s3 cp "$ROOT_DIR/skew_demo/spark_job.py" "s3://$BUCKET_NAME/$SCRIPT_PREFIX/spark_job.py"
aws s3 cp "$BUILD_DIR/skew_demo_package.zip" "s3://$BUCKET_NAME/$SCRIPT_PREFIX/skew_demo_package.zip"

docker build --platform linux/amd64 -t "$REPO_URL:$IMAGE_TAG" .
docker push "$REPO_URL:$IMAGE_TAG"

cd "$TF_DIR"
terraform apply \
  -auto-approve \
  -var="aws_region=$AWS_REGION" \
  -var="bucket_name=$BUCKET_NAME" \
  -var="root_prefix=$ROOT_PREFIX" \
  -var="dataset_prefix=$DATASET_PREFIX" \
  -var="script_prefix=$SCRIPT_PREFIX" \
  -var="project_name=$PROJECT_NAME" \
  -var="duckdb_image_uri=$REPO_URL:$IMAGE_TAG"

echo "Deployment complete"
echo "ECR image: $REPO_URL:$IMAGE_TAG"
echo "Spark entrypoint: s3://$BUCKET_NAME/$SCRIPT_PREFIX/spark_job.py"
echo "Spark package: s3://$BUCKET_NAME/$SCRIPT_PREFIX/skew_demo_package.zip"