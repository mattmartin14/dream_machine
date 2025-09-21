#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."  # move to repo root (../ from scripts)

if [ -f .env ]; then
  # shellcheck disable=SC1091
  source .env
fi

: "${PROJECT_ID:?PROJECT_ID not set (export or put in .env)}"
: "${ICEBERG_WAREHOUSE_PATH:?ICEBERG_WAREHOUSE_PATH not set}"

export USE_PROXY=true
export PROXY_PORT=${PROXY_PORT:-8799}

echo "[run_full_attach] Starting embedded proxy + attach (port=$PROXY_PORT)" >&2

PYTHONPATH=. python scripts/attach_biglake.py --embedded-proxy --proxy-port "$PROXY_PORT" "$@"

echo "[run_full_attach] Completed." >&2