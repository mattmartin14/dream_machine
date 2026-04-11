#!/usr/bin/env bash
set -euo pipefail

# Builds Athena objects for the demo flow using external SQL templates:
# 1) Raw JSON table in staging database
# 2) UNLOAD transformed rows to Parquet
# 3) Parquet table in analytics database

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/athena_common.sh"

aws_bucket="${aws_bucket:-}"
if [[ -z "$aws_bucket" ]]; then
  echo "Environment variable aws_bucket is required." >&2
  exit 1
fi

BUCKET="$aws_bucket"
RAW_PREFIX="${RAW_PREFIX:-demo_athena/raw}"
PARQUET_PREFIX="${PARQUET_PREFIX:-demo_athena/parquet}"
RESULTS_PREFIX="${RESULTS_PREFIX:-demo_athena/query_results}"
STAGING_DATABASE="${STAGING_DATABASE:-db1_staging}"
ANALYTICS_DATABASE="${ANALYTICS_DATABASE:-db1}"
RECREATE_STAGING_DB="${RECREATE_STAGING_DB:-false}"
DATABASE="${ANALYTICS_DATABASE}"
WORKGROUP="${WORKGROUP:-primary}"
JSON_TABLE="${JSON_TABLE:-bike_orders_json}"
PARQUET_TABLE="${PARQUET_TABLE:-bike_orders_parquet}"
REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"

RAW_S3="s3://${BUCKET}/${RAW_PREFIX%/}/"
PARQUET_S3="s3://${BUCKET}/${PARQUET_PREFIX%/}/"
RESULTS_S3="s3://${BUCKET}/${RESULTS_PREFIX%/}/"
SQL_DIR="${SCRIPT_DIR}/sql"

load_sql_template() {
  local sql_file="$1"
  local sql
  sql="$(<"$sql_file")"

  sql="${sql//__STAGING_DATABASE__/$STAGING_DATABASE}"
  sql="${sql//__ANALYTICS_DATABASE__/$ANALYTICS_DATABASE}"
  sql="${sql//__JSON_TABLE__/$JSON_TABLE}"
  sql="${sql//__PARQUET_TABLE__/$PARQUET_TABLE}"
  sql="${sql//__RAW_S3__/$RAW_S3}"
  sql="${sql//__PARQUET_S3__/$PARQUET_S3}"
  printf '%s' "$sql"
}

exec_athena_query() {
  local title="$1"
  local file_name="$2"
  local use_db="${3:-yes}"
  local sql_file="${SQL_DIR}/${file_name}"

  if [[ ! -f "$sql_file" ]]; then
    echo "Missing SQL file: $sql_file" >&2
    exit 1
  fi

  local sql
  sql="$(load_sql_template "$sql_file")"
  run_athena_query "$title" "$sql" "$use_db"
}

main() {
  require_cmd aws
  local recreate_staging_db_normalized
  recreate_staging_db_normalized="$(printf '%s' "$RECREATE_STAGING_DB" | tr '[:upper:]' '[:lower:]')"

  log "Using region=${REGION}, workgroup=${WORKGROUP}"
  log "Staging database: ${STAGING_DATABASE}"
  log "Analytics database: ${ANALYTICS_DATABASE}"
  log "Recreate staging database: ${RECREATE_STAGING_DB}"
  log "Raw location: ${RAW_S3}"
  log "Parquet location: ${PARQUET_S3}"
  log "Athena results location: ${RESULTS_S3}"

  case "${recreate_staging_db_normalized}" in
    true|1|yes)
      exec_athena_query "Drop staging database (CASCADE)" "00_drop_staging_database.sql" "no"
      ;;
    false|0|no)
      ;;
    *)
      echo "Invalid RECREATE_STAGING_DB value: ${RECREATE_STAGING_DB}. Use true/false." >&2
      exit 1
      ;;
  esac

  exec_athena_query "Create staging database if missing" "01_create_staging_database.sql" "no"
  exec_athena_query "Create analytics database if missing" "02_create_analytics_database.sql" "no"

  exec_athena_query "Drop existing staging JSON table (if any)" "03_drop_staging_json_table.sql" "no"
  exec_athena_query "Create staging JSON external table" "04_create_staging_json_table.sql" "no"

  log "Clearing existing Parquet UNLOAD output"
  aws s3 rm "${PARQUET_S3}" --recursive --region "$REGION" >/dev/null || true

  exec_athena_query "UNLOAD staging JSON data into Parquet" "05_unload_to_parquet.sql" "no"

  exec_athena_query "Drop existing analytics Parquet table (if any)" "06_drop_analytics_parquet_table.sql" "no"
  exec_athena_query "Create analytics Parquet external table" "07_create_analytics_parquet_table.sql" "no"

  exec_athena_query "Sanity check row counts" "08_sanity_check_counts.sql" "no"

  log "Done."
  log "JSON table: ${STAGING_DATABASE}.${JSON_TABLE}"
  log "Parquet table: ${ANALYTICS_DATABASE}.${PARQUET_TABLE}"
}

main "$@"
