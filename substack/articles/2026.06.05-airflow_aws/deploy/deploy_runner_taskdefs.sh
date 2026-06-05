#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TF_DIR="$PROJECT_ROOT/terraform_ecs_runner_taskdefs"

usage() {
  cat <<'EOF'
Usage:
  ./deploy/deploy_runner_taskdefs.sh [--auto-approve] [terraform plan/apply args...]

Examples:
  ./deploy/deploy_runner_taskdefs.sh
  ./deploy/deploy_runner_taskdefs.sh --auto-approve
  ./deploy/deploy_runner_taskdefs.sh --auto-approve -var 'image_tag=latest' -var 'create_airflow_runner_policy=true'

Description:
  Deploys reusable ECS/Fargate runner task definitions used by Airflow.
EOF
}

AUTO_APPROVE="false"
TF_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --auto-approve)
      AUTO_APPROVE="true"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      TF_ARGS+=("$1")
      shift
      ;;
  esac
done

if [[ ! -d "$TF_DIR" ]]; then
  echo "Error: terraform directory not found at $TF_DIR" >&2
  exit 1
fi

if ! command -v terraform >/dev/null 2>&1; then
  echo "Error: terraform is required." >&2
  exit 1
fi

terraform -chdir="$TF_DIR" init
terraform -chdir="$TF_DIR" plan "${TF_ARGS[@]}"

if [[ "$AUTO_APPROVE" == "true" ]]; then
  terraform -chdir="$TF_DIR" apply -auto-approve "${TF_ARGS[@]}"
else
  terraform -chdir="$TF_DIR" apply "${TF_ARGS[@]}"
fi
