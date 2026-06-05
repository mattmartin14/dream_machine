#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

usage() {
  cat <<'EOF'
Usage:
  ./deploy/destroy_all.sh [--no-auto-approve]

Examples:
  ./deploy/destroy_all.sh
  ./deploy/destroy_all.sh --no-auto-approve

Description:
  Destroys all AWS resources created by this project in the correct order:
  1) ECS runner task definitions
  2) Foundational infrastructure
  3) Stops/removes local Docker Compose services when present

Notes:
  - Terraform destroys are auto-approved by default.
EOF
}

AUTO_APPROVE="true"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --auto-approve)
      AUTO_APPROVE="true"
      shift
      ;;
    --no-auto-approve)
      AUTO_APPROVE="false"
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

echo "Destroying runner task definitions..."
if [[ "$AUTO_APPROVE" == "true" ]]; then
  "$SCRIPT_DIR/destroy_runner_taskdefs.sh" --auto-approve
else
  "$SCRIPT_DIR/destroy_runner_taskdefs.sh"
fi

echo "Destroying foundational infrastructure..."
if [[ "$AUTO_APPROVE" == "true" ]]; then
  "$SCRIPT_DIR/destroy_foundational.sh" --auto-approve
else
  "$SCRIPT_DIR/destroy_foundational.sh"
fi

if command -v docker >/dev/null 2>&1; then
  echo "Checking local Docker Compose services..."
  if (cd "$PROJECT_ROOT" && docker compose ps -q >/dev/null 2>&1 && [[ -n "$(cd "$PROJECT_ROOT" && docker compose ps -q)" ]]); then
    echo "Stopping/removing Docker Compose services..."
    (cd "$PROJECT_ROOT" && docker compose down --volumes --remove-orphans)
  else
    echo "No running Docker Compose services found."
  fi
else
  echo "Docker is not installed; skipping local Docker Compose teardown."
fi

echo "Destroy complete."
