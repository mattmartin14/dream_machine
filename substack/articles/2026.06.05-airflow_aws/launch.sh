#!/usr/bin/env bash
set -euo pipefail

export AWS_PAGER=""
export AWS_CLI_AUTO_PROMPT="off"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"

usage() {
  cat <<'EOF'
Usage:
  ./launch.sh [--image-tag <tag>] [--no-cache] [--create-airflow-runner-policy]

Examples:
  ./launch.sh
  ./launch.sh --image-tag latest
  ./launch.sh --image-tag v1.2.3 --no-cache
  ./launch.sh --create-airflow-runner-policy

Required environment variables:
  SLACK_WEBHOOK   Slack webhook URL for foundational terraform secret
  AWS_ACCT_ID     12-digit AWS account ID used when rendering Airflow .env

Description:
  Runs the full project bootstrap flow:
    1) deploy foundational infra and build/push runner image
    2) deploy ECS runner task definitions
    3) upload ETL scripts to S3
    4) render Airflow .env
    5) start Airflow webserver + scheduler
EOF
}

IMAGE_TAG="latest"
NO_CACHE_FLAG=""
CREATE_AIRFLOW_RUNNER_POLICY="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --image-tag)
      IMAGE_TAG="${2:-}"
      if [[ -z "$IMAGE_TAG" ]]; then
        echo "Error: --image-tag requires a value" >&2
        exit 1
      fi
      shift 2
      ;;
    --no-cache)
      NO_CACHE_FLAG="--no-cache"
      shift
      ;;
    --create-airflow-runner-policy)
      CREATE_AIRFLOW_RUNNER_POLICY="true"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Error: unknown argument '$1'" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "${SLACK_WEBHOOK:-}" ]]; then
  echo "Error: SLACK_WEBHOOK must be set before running launch.sh" >&2
  exit 1
fi

if [[ -z "${AWS_ACCT_ID:-}" ]]; then
  echo "Error: AWS_ACCT_ID must be set before running launch.sh" >&2
  exit 1
fi

if ! [[ "$AWS_ACCT_ID" =~ ^[0-9]{12}$ ]]; then
  echo "Error: AWS_ACCT_ID must be a 12-digit account ID" >&2
  exit 1
fi

if ! command -v aws >/dev/null 2>&1; then
  echo "Error: aws CLI is required." >&2
  exit 1
fi

if ! command -v terraform >/dev/null 2>&1; then
  echo "Error: terraform is required." >&2
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "Error: docker is required." >&2
  exit 1
fi

if ! aws sts get-caller-identity >/dev/null 2>&1; then
  echo "Error: AWS auth/session is not active. Run aws_auth first." >&2
  exit 1
fi

echo "[1/5] Deploying foundational infrastructure and runner image..."
if [[ -n "$NO_CACHE_FLAG" ]]; then
  "$PROJECT_ROOT/deploy/deploy_foundational.sh" "$IMAGE_TAG" "$NO_CACHE_FLAG" --auto-approve
else
  "$PROJECT_ROOT/deploy/deploy_foundational.sh" "$IMAGE_TAG" --auto-approve
fi

echo "[2/5] Deploying ECS runner task definitions..."
if [[ "$CREATE_AIRFLOW_RUNNER_POLICY" == "true" ]]; then
  "$PROJECT_ROOT/deploy/deploy_runner_taskdefs.sh" --auto-approve -var "image_tag=$IMAGE_TAG" -var "create_airflow_runner_policy=true"
else
  "$PROJECT_ROOT/deploy/deploy_runner_taskdefs.sh" --auto-approve -var "image_tag=$IMAGE_TAG"
fi

echo "[3/5] Uploading ETL scripts to S3..."
"$PROJECT_ROOT/deploy/upload_etl_scripts.sh" --aws-region us-east-1

echo "[4/5] Rendering Airflow .env..."
"$PROJECT_ROOT/deploy/render_airflow_env.sh"

echo "[5/5] Starting Airflow services..."
"$PROJECT_ROOT/deploy/start_airflow.sh" --build

echo "Launch complete. Open http://localhost:8080 and trigger DAG duckdb_ecs_etl."
