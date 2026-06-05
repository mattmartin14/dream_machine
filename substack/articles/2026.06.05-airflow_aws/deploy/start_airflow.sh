#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

usage() {
  cat <<'EOF'
Usage:
  ./deploy/start_airflow.sh [--build]

Description:
  Initializes Airflow metadata DB/user and starts local webserver + scheduler.
EOF
}

BUILD_FLAG=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build)
      BUILD_FLAG="--build"
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

if ! command -v docker >/dev/null 2>&1; then
  echo "Error: docker is required." >&2
  exit 1
fi

if [[ ! -f "$PROJECT_ROOT/.env" ]]; then
  echo "Error: .env not found. Run ./deploy/render_airflow_env.sh first." >&2
  exit 1
fi

cd "$PROJECT_ROOT"
docker compose run --rm airflow-init

docker compose up -d $BUILD_FLAG airflow-webserver airflow-scheduler

echo "Airflow is starting at http://localhost:8080"
