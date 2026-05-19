#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ETL_JOB_DIR="$PROJECT_ROOT/terraform_etl_job"
INFRA_DIR="$PROJECT_ROOT/terraform_infra"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Error: required command not found: $1" >&2
    exit 1
  fi
}

workspace_exists() {
  local ws="$1"
  terraform -chdir="$ETL_JOB_DIR" workspace list | sed 's/*//g' | tr -d ' ' | grep -Fxq "$ws"
}

destroy_etl_workspace() {
  local job_name="$1"
  local local_script_path="$2"
  local script_s3_uri="$3"

  if ! workspace_exists "$job_name"; then
    echo "Skipping ETL workspace '$job_name' (not found)."
    return 0
  fi

  echo "Destroying ETL workspace '$job_name'..."
  terraform -chdir="$ETL_JOB_DIR" workspace select "$job_name" >/dev/null
  terraform -chdir="$ETL_JOB_DIR" destroy -auto-approve \
    -var "etl_job_name=$job_name" \
    -var "local_script_path=$local_script_path" \
    -var "script_s3_uri=$script_s3_uri"

  terraform -chdir="$ETL_JOB_DIR" workspace select default >/dev/null
  terraform -chdir="$ETL_JOB_DIR" workspace delete "$job_name" >/dev/null || true
}

main() {
  require_cmd terraform

  if [[ ! -d "$ETL_JOB_DIR" ]]; then
    echo "Error: ETL terraform directory not found at $ETL_JOB_DIR" >&2
    exit 1
  fi

  if [[ ! -d "$INFRA_DIR" ]]; then
    echo "Error: infra terraform directory not found at $INFRA_DIR" >&2
    exit 1
  fi

  terraform -chdir="$ETL_JOB_DIR" init
  destroy_etl_workspace "orders-etl" "scripts/orders_etl.py" "s3://s3-sales-agg-test/scripts/orders_etl.py"
  destroy_etl_workspace "cust-etl" "scripts/cust_etl.py" "s3://s3-sales-agg-test/scripts/cust_etl.py"

  echo "Destroying foundational infrastructure..."
  # terraform_infra requires var.slack_webhook even during destroy planning.
  if [[ -z "${TF_VAR_slack_webhook:-}" ]]; then
    if [[ -n "${SLACK_WEBHOOK:-}" ]]; then
      export TF_VAR_slack_webhook="$SLACK_WEBHOOK"
    else
      # Destroy flow does not need the real webhook value; avoid interactive prompt.
      export TF_VAR_slack_webhook="https://example.invalid/teardown-placeholder"
    fi
  fi

  terraform -chdir="$INFRA_DIR" init
  terraform -chdir="$INFRA_DIR" destroy -auto-approve

  echo "Teardown complete."
}

main
