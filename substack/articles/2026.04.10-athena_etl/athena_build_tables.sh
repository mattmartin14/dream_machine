#!/usr/bin/env bash
set -euo pipefail

# Builds Athena objects for the demo flow:
# 1) JSON external table over raw nested files
# 2) UNLOAD transformed rows to Parquet
# 3) Parquet external table over UNLOAD output

aws_bucket="${aws_bucket:-}"
if [[ -z "$aws_bucket" ]]; then
  echo "Environment variable aws_bucket is required." >&2
  exit 1
fi

BUCKET="$aws_bucket"
RAW_PREFIX="${RAW_PREFIX:-demo_athena/raw}"
PARQUET_PREFIX="${PARQUET_PREFIX:-demo_athena/parquet}"
RESULTS_PREFIX="${RESULTS_PREFIX:-demo_athena/query_results}"
DATABASE="${DATABASE:-db1}"
WORKGROUP="${WORKGROUP:-primary}"
JSON_TABLE="${JSON_TABLE:-bike_orders_json}"
PARQUET_TABLE="${PARQUET_TABLE:-bike_orders_parquet}"
REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"

RAW_S3="s3://${BUCKET}/${RAW_PREFIX%/}/"
PARQUET_S3="s3://${BUCKET}/${PARQUET_PREFIX%/}/"
RESULTS_S3="s3://${BUCKET}/${RESULTS_PREFIX%/}/"

log() {
  printf '[%s] %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*"
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    exit 1
  }
}

athena_start_query() {
  local query="$1"
  local use_db="${2:-yes}"
  if [[ "$use_db" == "yes" ]]; then
    aws athena start-query-execution \
      --region "$REGION" \
      --work-group "$WORKGROUP" \
      --query-execution-context "Database=${DATABASE}" \
      --result-configuration "OutputLocation=${RESULTS_S3}" \
      --query-string "$query" \
      --query 'QueryExecutionId' \
      --output text
  else
    aws athena start-query-execution \
      --region "$REGION" \
      --work-group "$WORKGROUP" \
      --result-configuration "OutputLocation=${RESULTS_S3}" \
      --query-string "$query" \
      --query 'QueryExecutionId' \
      --output text
  fi
}

athena_wait_for_query() {
  local query_id="$1"
  local state
  while true; do
    state="$(aws athena get-query-execution \
      --region "$REGION" \
      --query-execution-id "$query_id" \
      --query 'QueryExecution.Status.State' \
      --output text)"

    case "$state" in
      SUCCEEDED)
        return 0
        ;;
      FAILED|CANCELLED)
        aws athena get-query-execution \
          --region "$REGION" \
          --query-execution-id "$query_id" \
          --query 'QueryExecution.Status.StateChangeReason' \
          --output text >&2
        return 1
        ;;
      *)
        sleep 2
        ;;
    esac
  done
}

run_athena_query() {
  local title="$1"
  local sql="$2"
  local use_db="${3:-yes}"

  log "$title"
  local qid
  qid="$(athena_start_query "$sql" "$use_db")"
  log "QueryExecutionId=${qid}"
  athena_wait_for_query "$qid"
}

main() {
  require_cmd aws

  log "Using region=${REGION}, workgroup=${WORKGROUP}, database=${DATABASE}"
  log "Raw location: ${RAW_S3}"
  log "Parquet location: ${PARQUET_S3}"
  log "Athena results location: ${RESULTS_S3}"

  run_athena_query "Create database if missing" "CREATE DATABASE IF NOT EXISTS ${DATABASE}" "no"

  run_athena_query "Drop existing JSON table (if any)" "DROP TABLE IF EXISTS ${JSON_TABLE}"
  run_athena_query "Create JSON external table" "
CREATE EXTERNAL TABLE ${JSON_TABLE} (
  order_id string,
  order_ts string,
  partition_date string,
  sales_channel string,
  fulfillment struct<method:string,warehouse:string,ship_speed:string>,
  customer_ref struct<customer_id:string,segment:string>,
  line_items array<struct<
    sku:string,
    name:string,
    category:string,
    quantity:int,
    pricing:struct<list_price:double,unit_price:double,currency:string>,
    discounts:array<struct<type:string,code:string,amount:double>>
  >>,
  totals struct<subtotal:double,shipping:double,tax:double,grand_total:double,currency:string>,
  payment struct<method:string,auth_code:string,status:string>,
  audit struct<ingested_at:string,source_system:string,trace_id:string>,
  customer_profile struct<
    name:struct<first:string,last:string>,
    contact:struct<email:string,sms_opt_in:boolean>,
    shipping_city:string
  >
)
PARTITIONED BY (dt string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json'='true')
LOCATION '${RAW_S3}'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.dt.type'='date',
  'projection.dt.range'='2020-01-01,NOW',
  'projection.dt.format'='yyyy-MM-dd',
  'storage.location.template'='${RAW_S3}dt=\${dt}/orders/'
)
" 

  log "Clearing existing Parquet UNLOAD output"
  aws s3 rm "${PARQUET_S3}" --recursive --region "$REGION" >/dev/null || true

  run_athena_query "UNLOAD JSON data into Parquet" "
UNLOAD (
  SELECT
    order_id,
    CAST(from_iso8601_timestamp(order_ts) AS timestamp) AS order_ts,
    CAST(dt AS date) AS dt,
    sales_channel,
    customer_ref.customer_id AS customer_id,
    customer_ref.segment AS customer_segment,
    cardinality(line_items) AS item_count,
    totals.subtotal AS subtotal,
    totals.shipping AS shipping,
    totals.tax AS tax,
    totals.grand_total AS grand_total,
    payment.method AS payment_method,
    fulfillment.warehouse AS warehouse,
    fulfillment.ship_speed AS ship_speed,
    customer_profile IS NOT NULL AS has_customer_profile
  FROM ${JSON_TABLE}
  WHERE order_id IS NOT NULL
)
TO '${PARQUET_S3}'
WITH (
  format = 'PARQUET',
  compression = 'SNAPPY'
)
"

  run_athena_query "Drop existing Parquet table (if any)" "DROP TABLE IF EXISTS ${PARQUET_TABLE}"
  run_athena_query "Create Parquet external table" "
CREATE EXTERNAL TABLE ${PARQUET_TABLE} (
  order_id string,
  order_ts timestamp,
  dt date,
  sales_channel string,
  customer_id string,
  customer_segment string,
  item_count int,
  subtotal double,
  shipping double,
  tax double,
  grand_total double,
  payment_method string,
  warehouse string,
  ship_speed string,
  has_customer_profile boolean
)
STORED AS PARQUET
LOCATION '${PARQUET_S3}'
"

  run_athena_query "Sanity check row counts" "
SELECT
  (SELECT COUNT(*) FROM ${JSON_TABLE} WHERE order_id IS NOT NULL) AS json_rows,
  (SELECT COUNT(*) FROM ${PARQUET_TABLE}) AS parquet_rows
"

  log "Done."
  log "JSON table: ${DATABASE}.${JSON_TABLE}"
  log "Parquet table: ${DATABASE}.${PARQUET_TABLE}"
}

main "$@"
