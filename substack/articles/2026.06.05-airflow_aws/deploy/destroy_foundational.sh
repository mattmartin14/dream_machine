#!/usr/bin/env bash
set -euo pipefail

export AWS_PAGER=""
export AWS_CLI_AUTO_PROMPT="off"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
FOUNDATION_DIR="$PROJECT_ROOT/terraform_infra"

usage() {
  cat <<'EOF'
Usage:
  ./deploy/destroy_foundational.sh [--auto-approve] [terraform destroy args...]

Examples:
  ./deploy/destroy_foundational.sh
  ./deploy/destroy_foundational.sh --auto-approve
  ./deploy/destroy_foundational.sh --auto-approve -var 'project_name=ecs-duckdb-etl'

Notes:
  - Destroys foundational resources in terraform_infra.
  - The module requires slack_webhook as an input variable; this script supplies an
    empty value when TF_VAR_slack_webhook/SLACK_WEBHOOK are not set.
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

if [[ ! -d "$FOUNDATION_DIR" ]]; then
  echo "Error: foundation terraform directory not found at $FOUNDATION_DIR" >&2
  exit 1
fi

if ! command -v terraform >/dev/null 2>&1; then
  echo "Error: terraform is required." >&2
  exit 1
fi

if [[ -z "${TF_VAR_slack_webhook:-}" ]]; then
  if [[ -n "${SLACK_WEBHOOK:-}" ]]; then
    export TF_VAR_slack_webhook="$SLACK_WEBHOOK"
  else
    export TF_VAR_slack_webhook=""
  fi
fi

terraform -chdir="$FOUNDATION_DIR" init -input=false

if [[ ${#TF_ARGS[@]} -gt 0 ]]; then
  terraform -chdir="$FOUNDATION_DIR" plan -destroy "${TF_ARGS[@]}"
else
  terraform -chdir="$FOUNDATION_DIR" plan -destroy
fi

if [[ "$AUTO_APPROVE" == "true" ]]; then
  if [[ ${#TF_ARGS[@]} -gt 0 ]]; then
    terraform -chdir="$FOUNDATION_DIR" destroy -auto-approve -input=false "${TF_ARGS[@]}"
  else
    terraform -chdir="$FOUNDATION_DIR" destroy -auto-approve -input=false
  fi
else
  if [[ ${#TF_ARGS[@]} -gt 0 ]]; then
    terraform -chdir="$FOUNDATION_DIR" destroy -input=false "${TF_ARGS[@]}"
  else
    terraform -chdir="$FOUNDATION_DIR" destroy -input=false
  fi
fi
