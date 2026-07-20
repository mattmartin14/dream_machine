#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

source "$ROOT_DIR/env_config.sh"
load_project_env "$ROOT_DIR"

MODE="${1:-dev}"
RAW_LOAD_FILE="${2:-load_raw_data.sql}"

usage() {
    cat <<'EOF'
Usage:
    ./run_raw_load.sh dev [load_raw_data.sql]
    ./run_raw_load.sh prod [load_raw_data.sql]

Examples:
    ./run_raw_load.sh dev
    ./run_raw_load.sh prod
    ./run_raw_load.sh dev load_raw_data.sql

Notes:
    - dev: writes TPCH CSV to local MinIO bucket from MINIO_BUCKET_NAME
    - prod: writes TPCH CSV to AWS bucket from AWS_BUCKET_NAME
EOF
}

if [[ "$MODE" != "dev" && "$MODE" != "prod" ]]; then
    echo "Invalid mode: $MODE" >&2
    usage
    exit 1
fi

if [[ ! -f "$RAW_LOAD_FILE" ]]; then
    echo "Raw load SQL file not found: $RAW_LOAD_FILE" >&2
    exit 1
fi

resolve_bucket_for_mode "$MODE"

SETUP_SQL=""
if [[ "$MODE" == "dev" ]]; then
    SETUP_SQL="$(cat <<'EOF'
.print '<< RAW LOAD IN DEV ENVIRONMENT >>'
INSTALL httpfs;
LOAD httpfs;

CREATE OR REPLACE SECRET minio_secret (
    TYPE s3,
    PROVIDER config,
    KEY_ID 'minio',
    SECRET 'minio12345',
    REGION 'us-east-1',
    ENDPOINT 'localhost:9000',
    URL_STYLE 'path',
    USE_SSL false
);
EOF
)"
else
    SETUP_SQL="$(cat <<'EOF'
.print '<< RAW LOAD IN PROD ENVIRONMENT >>'
INSTALL httpfs;
LOAD httpfs;
INSTALL aws;
LOAD aws;

CREATE OR REPLACE SECRET aws_secret (
    TYPE s3,
    PROVIDER credential_chain
);
EOF
)"
fi

RAW_SQL="$(sed "s|__BUCKET__|${BUCKET}|g" "$RAW_LOAD_FILE")"

duckdb <<SQL
${SETUP_SQL}

${RAW_SQL}

SELECT 'raw_orders_count' AS checkpoint, COUNT(*) AS row_count
FROM read_csv_auto('s3://${BUCKET}/tpch/orders.csv');
SQL
