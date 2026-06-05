#!/usr/bin/env bash
set -euo pipefail

export AWS_PAGER=""
export AWS_CLI_AUTO_PROMPT="off"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TF_DIR="$PROJECT_ROOT/terraform_ecs_runner_taskdefs"

usage() {
  cat <<'EOF'
Usage:
  ./deploy/destroy_runner_taskdefs.sh [--auto-approve] [terraform destroy args...]

Examples:
  ./deploy/destroy_runner_taskdefs.sh
  ./deploy/destroy_runner_taskdefs.sh --auto-approve
  ./deploy/destroy_runner_taskdefs.sh --auto-approve -var 'project_name=ecs-duckdb-etl'

Description:
  Destroys reusable ECS/Fargate runner task definitions used by Airflow.
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

if ! command -v aws >/dev/null 2>&1; then
  echo "Error: aws CLI is required." >&2
  exit 1
fi

terraform -chdir="$TF_DIR" init -input=false

# Detach the optional local-airflow policy from any identities before destroy.
POLICY_ARN="$(terraform -chdir="$TF_DIR" output -raw airflow_runner_policy_arn 2>/dev/null || true)"
POLICY_ARN="$(printf '%s' "$POLICY_ARN" | tr -d '[:space:]')"
if [[ "$POLICY_ARN" == arn:aws:iam::* ]]; then
  while IFS= read -r role_name; do
    [[ -n "$role_name" ]] || continue
    aws iam detach-role-policy --role-name "$role_name" --policy-arn "$POLICY_ARN"
  done < <(aws iam list-entities-for-policy --policy-arn "$POLICY_ARN" --query 'PolicyRoles[].RoleName' --output text | tr '\t' '\n')

  while IFS= read -r user_name; do
    [[ -n "$user_name" ]] || continue
    aws iam detach-user-policy --user-name "$user_name" --policy-arn "$POLICY_ARN"
  done < <(aws iam list-entities-for-policy --policy-arn "$POLICY_ARN" --query 'PolicyUsers[].UserName' --output text | tr '\t' '\n')

  while IFS= read -r group_name; do
    [[ -n "$group_name" ]] || continue
    aws iam detach-group-policy --group-name "$group_name" --policy-arn "$POLICY_ARN"
  done < <(aws iam list-entities-for-policy --policy-arn "$POLICY_ARN" --query 'PolicyGroups[].GroupName' --output text | tr '\t' '\n')
fi

if [[ ${#TF_ARGS[@]} -gt 0 ]]; then
  terraform -chdir="$TF_DIR" plan -destroy "${TF_ARGS[@]}"
else
  terraform -chdir="$TF_DIR" plan -destroy
fi

if [[ "$AUTO_APPROVE" == "true" ]]; then
  if [[ ${#TF_ARGS[@]} -gt 0 ]]; then
    terraform -chdir="$TF_DIR" destroy -auto-approve -input=false "${TF_ARGS[@]}"
  else
    terraform -chdir="$TF_DIR" destroy -auto-approve -input=false
  fi
else
  if [[ ${#TF_ARGS[@]} -gt 0 ]]; then
    terraform -chdir="$TF_DIR" destroy -input=false "${TF_ARGS[@]}"
  else
    terraform -chdir="$TF_DIR" destroy -input=false
  fi
fi
