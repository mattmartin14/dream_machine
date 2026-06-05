#!/usr/bin/env bash
set -euo pipefail

export AWS_PAGER=""
export AWS_CLI_AUTO_PROMPT="off"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
FOUNDATION_DIR="$PROJECT_ROOT/terraform_infra"
BUILD_AND_PUSH_SCRIPT="$PROJECT_ROOT/docker/build_and_push.sh"

usage() {
  cat <<'EOF'
Usage:
  ./deploy/deploy_foundational.sh [image_tag] [--no-cache] [--auto-approve]

Examples:
  ./deploy/deploy_foundational.sh
  ./deploy/deploy_foundational.sh latest
  ./deploy/deploy_foundational.sh v1.2.3 --no-cache --auto-approve

Notes:
  - Runs Terraform init/plan/apply for terraform_infra.
  - Then delegates container build/push to docker/build_and_push.sh.
  - Passes image_tag and --no-cache through to docker/build_and_push.sh.
  - Reads Slack webhook from TF_VAR_slack_webhook or SLACK_WEBHOOK environment variables.
EOF
}

IMAGE_TAG="latest"
NO_CACHE_FLAG=""
AUTO_APPROVE="false"

for arg in "$@"; do
  case "$arg" in
    -h|--help)
      usage
      exit 0
      ;;
    --no-cache)
      NO_CACHE_FLAG="--no-cache"
      ;;
    --auto-approve)
      AUTO_APPROVE="true"
      ;;
    *)
      IMAGE_TAG="$arg"
      ;;
  esac
done

if [[ ! -d "$FOUNDATION_DIR" ]]; then
  echo "Error: foundation terraform directory not found at $FOUNDATION_DIR" >&2
  exit 1
fi

if [[ ! -x "$BUILD_AND_PUSH_SCRIPT" ]]; then
  echo "Error: docker build script not found or not executable at $BUILD_AND_PUSH_SCRIPT" >&2
  exit 1
fi

if ! command -v aws >/dev/null 2>&1; then
  echo "Error: required command not found: aws" >&2
  exit 1
fi

# terraform_infra requires variable "slack_webhook"; map SLACK_WEBHOOK automatically.
if [[ -z "${TF_VAR_slack_webhook:-}" ]]; then
  if [[ -n "${SLACK_WEBHOOK:-}" ]]; then
    export TF_VAR_slack_webhook="$SLACK_WEBHOOK"
  else
    echo "Error: set SLACK_WEBHOOK (or TF_VAR_slack_webhook) before running deploy_foundational.sh" >&2
    exit 1
  fi
fi

# If the target secret is pending deletion, restore it before Terraform apply.
SLACK_SECRET_NAME="${TF_VAR_slack_webhook_secret_name:-slack_webhook_v1}"
DELETED_DATE="$(aws secretsmanager describe-secret --secret-id "$SLACK_SECRET_NAME" --query 'DeletedDate' --output text 2>/dev/null || true)"
if [[ -n "$DELETED_DATE" && "$DELETED_DATE" != "None" ]]; then
  echo "Secret '$SLACK_SECRET_NAME' is scheduled for deletion; restoring it now..."
  aws secretsmanager restore-secret --secret-id "$SLACK_SECRET_NAME" >/dev/null
fi

terraform -chdir="$FOUNDATION_DIR" init

# If the secret already exists but is not in Terraform state, import it.
if aws secretsmanager describe-secret --secret-id "$SLACK_SECRET_NAME" >/dev/null 2>&1; then
  if ! terraform -chdir="$FOUNDATION_DIR" state list | grep -Fxq "module.slack_secret.aws_secretsmanager_secret.slack_webhook"; then
    echo "Importing existing secret '$SLACK_SECRET_NAME' into Terraform state..."
    terraform -chdir="$FOUNDATION_DIR" import \
      module.slack_secret.aws_secretsmanager_secret.slack_webhook \
      "$SLACK_SECRET_NAME"
  fi
fi

terraform -chdir="$FOUNDATION_DIR" plan

if [[ "$AUTO_APPROVE" == "true" ]]; then
  terraform -chdir="$FOUNDATION_DIR" apply -auto-approve
else
  terraform -chdir="$FOUNDATION_DIR" apply
fi

if [[ -n "$NO_CACHE_FLAG" ]]; then
  "$BUILD_AND_PUSH_SCRIPT" "$IMAGE_TAG" "$NO_CACHE_FLAG"
else
  "$BUILD_AND_PUSH_SCRIPT" "$IMAGE_TAG"
fi
