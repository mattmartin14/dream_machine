#!/usr/bin/env bash
set -euo pipefail

export AWS_PAGER=""
export AWS_CLI_AUTO_PROMPT="off"

export AWS_REGION="us-east-1"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DEFAULT_FOUNDATION_TF_DIR="$PROJECT_ROOT/terraform_infra"
LEGACY_TF_DIR="$PROJECT_ROOT/terraform"
TF_DIR="${TF_DIR:-$DEFAULT_FOUNDATION_TF_DIR}"

if [[ ! -d "$TF_DIR" && -d "$LEGACY_TF_DIR" ]]; then
  TF_DIR="$LEGACY_TF_DIR"
fi

usage() {
  cat <<'EOF'
Usage:
  ./docker/build_and_push.sh [image_tag] [--no-cache]

Examples:
  ./docker/build_and_push.sh
  ./docker/build_and_push.sh latest
  ./docker/build_and_push.sh v1.2.3 --no-cache

Environment variables:
  AWS_REGION    Optional. Defaults to region from terraform_infra/terraform.tfvars, then us-east-1.
  ECR_REPO_URL  Optional. Defaults to terraform output ecr_repository_url.
  TF_DIR        Optional. Terraform directory containing ecr_repository_url output.
  DOCKER_PLATFORM Optional. Docker target platform. Defaults to linux/amd64.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

IMAGE_TAG="${1:-latest}"
NO_CACHE_FLAG="${2:-}"

if [[ -n "$NO_CACHE_FLAG" && "$NO_CACHE_FLAG" != "--no-cache" ]]; then
  echo "Error: second argument must be --no-cache when provided." >&2
  usage
  exit 1
fi

if ! command -v aws >/dev/null 2>&1; then
  echo "Error: aws CLI is required." >&2
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "Error: docker is required." >&2
  exit 1
fi

if ! command -v terraform >/dev/null 2>&1; then
  echo "Error: terraform is required." >&2
  exit 1
fi

if [[ ! -d "$TF_DIR" ]]; then
  echo "Error: terraform directory not found at $TF_DIR" >&2
  exit 1
fi

normalize_region() {
  local raw="$1"
  local value="${raw// /}"

  # Accept common mistaken format like: aws_region="us-east-1"
  if [[ "$value" == aws_region=* ]]; then
    value="${value#aws_region=}"
  fi

  # Strip optional wrapping quotes.
  value="${value%\"}"
  value="${value#\"}"

  printf '%s' "$value"
}

AWS_REGION="${AWS_REGION:-}"
AWS_REGION="$(normalize_region "$AWS_REGION")"
if [[ -z "$AWS_REGION" && -f "$TF_DIR/terraform.tfvars" ]]; then
  AWS_REGION="$(grep -E '^aws_region' "$TF_DIR/terraform.tfvars" | head -n1 | sed -E 's/.*=\s*"([^"]+)"/\1/')"
  AWS_REGION="$(normalize_region "$AWS_REGION")"
fi
if [[ -z "$AWS_REGION" ]]; then
  AWS_REGION="us-east-1"
fi

if [[ ! "$AWS_REGION" =~ ^[a-z]{2}(-gov)?-[a-z]+-[0-9]+$ ]]; then
  echo "Error: AWS_REGION is invalid: '$AWS_REGION'" >&2
  echo "Set AWS_REGION to a value like 'us-east-1'." >&2
  exit 1
fi

ECR_REPO_URL="${ECR_REPO_URL:-}"
if [[ -z "$ECR_REPO_URL" ]]; then
  ECR_REPO_URL="$(terraform -chdir="$TF_DIR" output -raw ecr_repository_url)"
fi

if [[ -z "$ECR_REPO_URL" ]]; then
  echo "Error: ECR repository URL is empty." >&2
  exit 1
fi

ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
ECR_REGISTRY="$ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com"
LOCAL_IMAGE="etl:$IMAGE_TAG"
REMOTE_IMAGE="$ECR_REPO_URL:$IMAGE_TAG"
DOCKER_PLATFORM="${DOCKER_PLATFORM:-linux/amd64}"

echo "Logging in to ECR registry: $ECR_REGISTRY"
aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin "$ECR_REGISTRY"

if [[ "$NO_CACHE_FLAG" == "--no-cache" ]]; then
  echo "Building Docker image (no cache): $LOCAL_IMAGE (platform=$DOCKER_PLATFORM)"
  docker build --platform "$DOCKER_PLATFORM" --no-cache -f "$SCRIPT_DIR/Dockerfile" -t "$LOCAL_IMAGE" "$PROJECT_ROOT"
else
  echo "Building Docker image: $LOCAL_IMAGE (platform=$DOCKER_PLATFORM)"
  docker build --platform "$DOCKER_PLATFORM" -f "$SCRIPT_DIR/Dockerfile" -t "$LOCAL_IMAGE" "$PROJECT_ROOT"
fi

echo "Tagging image: $LOCAL_IMAGE -> $REMOTE_IMAGE"
docker tag "$LOCAL_IMAGE" "$REMOTE_IMAGE"

echo "Pushing image: $REMOTE_IMAGE"
docker push "$REMOTE_IMAGE"

echo "Build and push complete: $REMOTE_IMAGE"
