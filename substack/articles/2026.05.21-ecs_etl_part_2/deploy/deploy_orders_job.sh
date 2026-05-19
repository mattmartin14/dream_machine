#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DEPLOY_GENERIC_SCRIPT="$PROJECT_ROOT/deploy/deploy_etl_job.sh"

"$DEPLOY_GENERIC_SCRIPT" \
  --etl-job-name "orders-etl" \
  --local-script-path "scripts/orders_etl.py" \
  --script-s3-uri "s3://s3-sales-agg-test/scripts/orders_etl.py" \
  --container-cpu "2048" \
  --container-memory "4096" \
  --schedule-expression 'cron(0 5 * * ? *)' \
  --auto-approve
