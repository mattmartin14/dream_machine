#!/usr/bin/env bash
set -euo pipefail

export AWS_PAGER=""
export AWS_CLI_AUTO_PROMPT="off"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

usage() {
  cat <<'EOF'
Usage:
  ./deploy/seed_tpch_raw_data.sh [--sf 0.01] [--aws-region us-east-1] [--raw-bucket s3-sales-raw-test]

Description:
  Generates TPCH orders/customer parquet files with DuckDB in a disposable Docker
  container and uploads them to the raw bucket prefixes expected by ETL jobs.

Outputs:
  s3://<raw-bucket>/tpch/orders_raw/orders.parquet
  s3://<raw-bucket>/tpch/customer_raw/customer.parquet
EOF
}

SCALE_FACTOR="0.01"
AWS_REGION="${AWS_REGION:-us-east-1}"
RAW_BUCKET="s3-sales-raw-test"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --sf)
      SCALE_FACTOR="${2:-}"
      shift 2
      ;;
    --aws-region)
      AWS_REGION="${2:-}"
      shift 2
      ;;
    --raw-bucket)
      RAW_BUCKET="${2:-}"
      shift 2
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

if ! command -v aws >/dev/null 2>&1; then
  echo "Error: aws CLI is required." >&2
  exit 1
fi

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

echo "Generating TPCH parquet files with sf=$SCALE_FACTOR"
docker run --rm -v "$TMP_DIR:/work" python:3.13-slim /bin/bash -lc "pip install --no-cache-dir duckdb >/dev/null && python - <<'PY'
import duckdb

sf = ${SCALE_FACTOR}
con = duckdb.connect()
con.execute('INSTALL tpch;')
con.execute('LOAD tpch;')
con.execute(f'CALL dbgen(sf={sf});')
con.execute(\"COPY orders TO '/work/orders.parquet' (FORMAT PARQUET);\")
con.execute(\"COPY customer TO '/work/customer.parquet' (FORMAT PARQUET);\")
con.close()
PY"

if [[ ! -f "$TMP_DIR/orders.parquet" || ! -f "$TMP_DIR/customer.parquet" ]]; then
  echo "Error: TPCH parquet generation failed." >&2
  exit 1
fi

ORDERS_S3_URI="s3://${RAW_BUCKET}/tpch/orders_raw/orders.parquet"
CUSTOMER_S3_URI="s3://${RAW_BUCKET}/tpch/customer_raw/customer.parquet"

echo "Uploading $ORDERS_S3_URI"
aws s3 cp "$TMP_DIR/orders.parquet" "$ORDERS_S3_URI" --region "$AWS_REGION"

echo "Uploading $CUSTOMER_S3_URI"
aws s3 cp "$TMP_DIR/customer.parquet" "$CUSTOMER_S3_URI" --region "$AWS_REGION"

echo "Seed complete."
