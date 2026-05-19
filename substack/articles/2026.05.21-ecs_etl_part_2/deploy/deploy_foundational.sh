#!/usr/bin/env bash
set -euo pipefail

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

terraform -chdir="$FOUNDATION_DIR" init
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
