#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

MODE="${1:-dev}"
WORKLOAD_FILE="${2:-iceberg_workload.sql}"

export BUCKET="matt-sbx-bucket-1-us-east-1"
if [[ "$MODE" == "dev" ]]; then
    BUCKET="warehouse-rest"
fi

usage() {
    cat <<'EOF'
Usage:
    ./run_workload.sh dev [workload.sql]
    ./run_workload.sh prod [workload.sql]

Examples:
    ./run_workload.sh dev
    AWS_ACCT_ID=123456789012 ./run_workload.sh prod
    ./run_workload.sh prod iceberg_workload.sql
Notes:
    - dev: attaches Lakekeeper catalog via local MinIO + REST catalog endpoint
    - prod: attaches AWS S3 Tables catalog via AWS credential chain + AWS_ACCT_ID
EOF
}

if [[ "$MODE" != "dev" && "$MODE" != "prod" ]]; then
    echo "Invalid mode: $MODE" >&2
    usage
    exit 1
fi

if [[ ! -f "$WORKLOAD_FILE" ]]; then
    echo "Workload file not found: $WORKLOAD_FILE" >&2
    exit 1
fi

DUCKDB_SETUP_SQL=""
ATTACH_SQL=""

if [[ "$MODE" == "dev" ]]; then

    ATTACH_SQL="$(cat <<EOF

.print '<< RUNNING IN DEV ENVIRONMENT >>'
INSTALL httpfs;
LOAD httpfs;

INSTALL iceberg;
LOAD iceberg;

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

ATTACH 'demo' AS iceberg_cat (
    TYPE iceberg,
    ENDPOINT 'http://localhost:8181/catalog',
    AUTHORIZATION_TYPE 'none',
    ACCESS_DELEGATION_MODE 'none'
);
EOF
)"

else
    if [[ -z "${AWS_ACCT_ID:-}" ]]; then
        echo "AWS_ACCT_ID is not set. Export it first, e.g. export AWS_ACCT_ID=123456789012" >&2
        exit 1
    fi

    ATTACH_SQL="$(cat <<EOF

.print '<< RUNNING IN PROD ENVIRONMENT >>'
INSTALL AWS;
LOAD AWS;
INSTALL ICEBERG;
LOAD ICEBERG;

CREATE OR REPLACE SECRET aws_secret (TYPE S3, PROVIDER CREDENTIAL_CHAIN);

ATTACH 'arn:aws:s3tables:us-east-1:${AWS_ACCT_ID}:bucket/icehouse-tbl-bucket1' AS iceberg_cat (
    TYPE iceberg,
    endpoint_type s3_tables
);
EOF
)"
fi

duckdb <<SQL

${ATTACH_SQL}

.read ${ROOT_DIR}/${WORKLOAD_FILE}
SQL
