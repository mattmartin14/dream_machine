#!/usr/bin/env bash

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
