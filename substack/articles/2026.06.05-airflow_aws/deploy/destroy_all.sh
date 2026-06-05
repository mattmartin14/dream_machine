#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

usage() {
  cat <<'EOF'
Usage:
  ./deploy/destroy_all.sh [--auto-approve]

Examples:
  ./deploy/destroy_all.sh
  ./deploy/destroy_all.sh --auto-approve

Description:
  Destroys all AWS resources created by this project in the correct order:
  1) ECS runner task definitions
  2) Foundational infrastructure
EOF
}

AUTO_APPROVE="false"

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
      echo "Error: unknown argument '$1'" >&2
      usage
      exit 1
      ;;
  esac
done

AA_FLAG=()
if [[ "$AUTO_APPROVE" == "true" ]]; then
  AA_FLAG=("--auto-approve")
fi

echo "Destroying runner task definitions..."
"$SCRIPT_DIR/destroy_runner_taskdefs.sh" "${AA_FLAG[@]}"

echo "Destroying foundational infrastructure..."
"$SCRIPT_DIR/destroy_foundational.sh" "${AA_FLAG[@]}"

echo "Destroy complete."
