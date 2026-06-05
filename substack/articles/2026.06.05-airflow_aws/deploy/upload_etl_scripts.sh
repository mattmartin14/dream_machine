#!/usr/bin/env bash
set -euo pipefail

export AWS_PAGER=""
export AWS_CLI_AUTO_PROMPT="off"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

usage() {
  cat <<'EOF'
Usage:
  ./deploy/upload_etl_scripts.sh [--seed-s3-uri s3://...] [--orders-s3-uri s3://...] [--cust-s3-uri s3://...] [--lineitem-s3-uri s3://...] [--aws-region us-east-1]

Examples:
  ./deploy/upload_etl_scripts.sh
  ./deploy/upload_etl_scripts.sh --aws-region us-east-1
EOF
}

ORDERS_S3_URI="s3://s3-sales-agg-test/scripts/orders_etl.py"
CUST_S3_URI="s3://s3-sales-agg-test/scripts/cust_etl.py"
SEED_S3_URI="s3://s3-sales-agg-test/scripts/seed_tpch_raw_data.py"
LINEITEM_S3_URI="s3://s3-sales-agg-test/scripts/lineitem_etl.py"
AWS_REGION="${AWS_REGION:-us-east-1}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --seed-s3-uri)
      SEED_S3_URI="${2:-}"
      shift 2
      ;;
    --orders-s3-uri)
      ORDERS_S3_URI="${2:-}"
      shift 2
      ;;
    --cust-s3-uri)
      CUST_S3_URI="${2:-}"
      shift 2
      ;;
    --lineitem-s3-uri)
      LINEITEM_S3_URI="${2:-}"
      shift 2
      ;;
    --aws-region)
      AWS_REGION="${2:-}"
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

if ! command -v aws >/dev/null 2>&1; then
  echo "Error: aws CLI is required." >&2
  exit 1
fi

if [[ ! -f "$PROJECT_ROOT/scripts/seed_tpch_raw_data.py" || ! -f "$PROJECT_ROOT/scripts/orders_etl.py" || ! -f "$PROJECT_ROOT/scripts/cust_etl.py" || ! -f "$PROJECT_ROOT/scripts/lineitem_etl.py" ]]; then
  echo "Error: expected scripts at scripts/seed_tpch_raw_data.py, scripts/orders_etl.py, scripts/cust_etl.py, and scripts/lineitem_etl.py" >&2
  exit 1
fi

echo "Uploading scripts/seed_tpch_raw_data.py -> $SEED_S3_URI"
aws s3 cp "$PROJECT_ROOT/scripts/seed_tpch_raw_data.py" "$SEED_S3_URI" --region "$AWS_REGION"

echo "Uploading scripts/orders_etl.py -> $ORDERS_S3_URI"
aws s3 cp "$PROJECT_ROOT/scripts/orders_etl.py" "$ORDERS_S3_URI" --region "$AWS_REGION"

echo "Uploading scripts/cust_etl.py -> $CUST_S3_URI"
aws s3 cp "$PROJECT_ROOT/scripts/cust_etl.py" "$CUST_S3_URI" --region "$AWS_REGION"

echo "Uploading scripts/lineitem_etl.py -> $LINEITEM_S3_URI"
aws s3 cp "$PROJECT_ROOT/scripts/lineitem_etl.py" "$LINEITEM_S3_URI" --region "$AWS_REGION"

echo "Upload complete."
