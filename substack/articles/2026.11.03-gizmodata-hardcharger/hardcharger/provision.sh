#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

CONFIG_FILE="${HC_CONFIG_FILE:-hardcharger/cluster-config.local.env}"
if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "Missing $CONFIG_FILE. Copy cluster-config.example.env and fill it from the actual GizmoData API request." >&2
  exit 2
fi
# This file is a private local shell config; do not source untrusted files.
set -a
# shellcheck disable=SC1090
source "$CONFIG_FILE"
set +a

for command in curl jq aws; do
  command -v "$command" >/dev/null || { echo "Required command not found: $command" >&2; exit 2; }
done
command -v python3 >/dev/null || { echo "Required command not found: python3" >&2; exit 2; }

response_file="hardcharger/cluster-response.json"
support_curl_file="hardcharger/cluster-support-curl.txt"
metadata_file="hardcharger/cluster-config.env"
umask 077
curl_config="$(mktemp)"
response_tmp="$(mktemp)"
secret_tmp="$(mktemp)"
payload_tmp="$(mktemp)"
trap 'rm -f "$curl_config" "$response_tmp" "$secret_tmp" "$payload_tmp"' EXIT

curl_quote() {
  local value="$1"
  [[ "$value" != *$'\r'* && "$value" != *$'\n'* ]] || {
    echo "HTTP configuration values cannot contain line breaks." >&2
    return 2
  }
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  printf '"%s"' "$value"
}

require_value() {
  local name="$1"
  local value="$2"
  if [[ -z "$value" ]]; then
    echo "Required configuration is empty: $name" >&2
    exit 2
  fi
}

write_request_config() {
  local endpoint="$1"
  local method="$2"
  local payload="$3"
  : >"$curl_config"
  printf 'url = %s\n' "$(curl_quote "$endpoint")" >>"$curl_config"
  printf 'request = %s\n' "$(curl_quote "$method")" >>"$curl_config"
  printf 'silent\nshow-error\nfail-with-body\n' >>"$curl_config"
  if [[ -n "${HC_API_HEADERS_JSON:-}" ]]; then
    local resolved_headers
    resolved_headers="$(python3 -c '
import json, os, sys
headers = json.loads(sys.stdin.read())
if not isinstance(headers, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in headers.items()):
    raise SystemExit("headers must be a JSON object of strings")
api_key = os.getenv("GIZMODATA_API_KEY", "")
headers = {k: v.replace("${GIZMODATA_API_KEY}", api_key) for k, v in headers.items()}
if any("${GIZMODATA_API_KEY}" in v or "\n" in v or "\r" in v for v in headers.values()):
    raise SystemExit("unresolved API key placeholder or invalid header")
print(json.dumps(headers))
' <<<"$HC_API_HEADERS_JSON")" || {
      echo "Could not resolve HTTP headers safely." >&2
      return 2
    }
    jq -e 'type == "object" and all(.[]; type == "string" and (contains("\n") | not) and (contains("\r") | not))' >/dev/null <<<"$resolved_headers" || {
      echo "HC_API_HEADERS_JSON must be a JSON object with string values." >&2
      return 2
    }
    while IFS= read -r header; do
      [[ "$header" != *$'\n'* && "$header" != *$'\r'* ]] || {
        echo "HTTP header values cannot contain line breaks." >&2
        return 2
      }
      printf 'header = %s\n' "$(curl_quote "$header")" >>"$curl_config"
    done < <(jq -r 'to_entries[] | "\(.key): \(.value)"' <<<"$resolved_headers")
  fi
  if [[ -n "${HC_API_USERNAME:-}" ]]; then
    printf 'user = %s\n' "$(curl_quote "${HC_API_USERNAME}:${HC_API_PASSWORD:-}")" >>"$curl_config"
  fi
  if [[ -n "$payload" ]]; then
    printf 'data-binary = %s\n' "$(curl_quote "@$payload")" >>"$curl_config"
  fi
}

perform_request() {
  local endpoint="$1"
  local method="$2"
  local payload="$3"
  local output="$4"
  write_request_config "$endpoint" "$method" "$payload"
  chmod 600 "$curl_config"
  local status
  if ! status="$(curl --config "$curl_config" --output "$output" --write-out '%{http_code}')"; then
    echo "GizmoData API request failed; inspect the protected response file at $response_file." >&2
    return 1
  fi
  if [[ ! "$status" =~ ^2[0-9][0-9]$ ]]; then
    echo "GizmoData API returned HTTP $status; inspect the protected response file at $response_file." >&2
    return 1
  fi
  jq -e . "$output" >/dev/null || {
    echo "GizmoData API response was not valid JSON." >&2
    return 1
  }
}

save_support_curl() {
  python3 scripts/format_hardcharger_curl.py "$1" "$2" "$3" \
    --output "$support_curl_file"
  chmod 600 "$support_curl_file"
  if jq -e 'has("support_request")' "$response_file" >/dev/null 2>&1; then
    jq 'del(.support_request)' "$response_file" >"$response_tmp"
    mv "$response_tmp" "$response_file"
    chmod 600 "$response_file"
  fi
  echo "Saved sanitized support curl to $support_curl_file." >&2
}

secrets_endpoint="${HC_SECRETS_ENDPOINT:-https://app.gizmodata.com/api/secrets/}"

# Creates (or reuses, matched by name) a GizmoData S3 secret from the
# HC_AWS_PERSONAL_* environment variables and exports its ID for the cluster payload.
ensure_s3_secret() {
  [[ -n "${HC_AWS_PERSONAL_KEY:-}" ]] || return 0
  require_value HC_AWS_PERSONAL_SECRET "${HC_AWS_PERSONAL_SECRET:-}"
  require_value HC_AWS_PERSONAL_BUCKET "${HC_AWS_PERSONAL_BUCKET:-}"
  require_value HC_ORGANIZATION_ID "${HC_ORGANIZATION_ID:-}"
  require_value HC_S3_SECRET_TYPE_ID "${HC_S3_SECRET_TYPE_ID:-}"
  local name="${HC_S3_SECRET_NAME:-hardcharger-personal-s3}"
  local secret_payload secret_id
  secret_payload="$(mktemp)"

  perform_request "$secrets_endpoint" GET "" "$response_tmp" || {
    rm -f "$secret_payload"
    return 1
  }
  secret_id="$(jq -r --arg name "$name" --arg org "$HC_ORGANIZATION_ID" \
    'map(select(.name == $name and .organization_id == $org)) | first | .id // empty' "$response_tmp")"

  if [[ -n "$secret_id" ]]; then
    echo "Reusing GizmoData secret '$name' ($secret_id)."
  else
    jq -n \
      --arg name "$name" \
      --arg label "${HC_S3_SECRET_LABEL:-Personal AWS S3 (read-only)}" \
      --arg type_id "$HC_S3_SECRET_TYPE_ID" \
      --arg key "$HC_AWS_PERSONAL_KEY" \
      --arg secret "$HC_AWS_PERSONAL_SECRET" \
      --arg region "${HC_AWS_PERSONAL_REGION:-us-east-1}" \
      --arg scope "s3://$HC_AWS_PERSONAL_BUCKET" \
      --arg region_id "$HC_CLOUD_REGION_ID" \
      --arg org "$HC_ORGANIZATION_ID" \
      '{
        name: $name,
        label: $label,
        secret_type: {id: $type_id},
        value: {provider: "config", key_id: $key, secret: $secret, region: $region, scope: $scope},
        gizmodata_managed: true,
        is_default: false,
        is_public: false,
        cloud_region_id: $region_id,
        scope: "organization",
        organization_id: $org
      }' >"$secret_payload"
    # Error bodies echo the submitted value, so never leave them on disk.
    if ! perform_request "$secrets_endpoint" POST "$secret_payload" "$response_tmp"; then
      jq -r '.detail | if type == "string" then . else "validation error" end' "$response_tmp" 2>/dev/null >&2 || true
      rm -f "$secret_payload" "$response_tmp"
      echo "Could not create the GizmoData S3 secret." >&2
      return 1
    fi
    secret_id="$(jq -er '.id' "$response_tmp")"
    echo "Created GizmoData secret '$name' ($secret_id)."
  fi
  rm -f "$secret_payload"
  export HC_S3_SECRET_ID="$secret_id"
}

databases_endpoint="${HC_DATABASES_ENDPOINT:-https://app.gizmodata.com/api/databases/}"

# Creates a persisted DuckDB database and exports its ID so the cluster mounts it
# read-write. A database with the same name already existing would carry old tables
# into the new cluster, so this fails unless HC_REUSE_PERSIST_DB=true.
ensure_database() {
  [[ -n "${HC_PERSIST_DB_NAME:-}" ]] || return 0
  local name="$HC_PERSIST_DB_NAME" db_payload db_id
  perform_request "$databases_endpoint" GET "" "$response_tmp" || return 1
  db_id="$(jq -r --arg name "$name" --arg env "$HC_PROJECT_ENVIRONMENT_ID" \
    'map(select(.name == $name and .project_environment_id == $env and .active_flag != false)) | first | .id // empty' "$response_tmp")"
  if [[ -n "$db_id" && "${HC_REUSE_PERSIST_DB:-false}" != true ]]; then
    echo "Persisted database '$name' ($db_id) already exists and would carry over old data." >&2
    echo "Delete it first (hardcharger/destroy.sh does this) or set HC_REUSE_PERSIST_DB=true." >&2
    return 1
  elif [[ -n "$db_id" ]]; then
    echo "Reusing persisted Hardcharger database '$name' ($db_id)."
  else
    db_payload="$(mktemp)"
    jq -n --arg name "$name" --arg label "${HC_PERSIST_DB_LABEL:-$name}" --arg env "$HC_PROJECT_ENVIRONMENT_ID" \
      '{name: $name, label: $label, database_engine: {name: "duckdb"},
        project_environment_id: $env, persist_to_cloud: true}' >"$db_payload"
    perform_request "$databases_endpoint" POST "$db_payload" "$response_tmp" || {
      rm -f "$db_payload"
      echo "Could not create the persisted database '$name'." >&2
      return 1
    }
    rm -f "$db_payload"
    db_id="$(jq -er '.id' "$response_tmp")"
    echo "Created persisted Hardcharger database '$name' ($db_id)."
  fi
  export HC_DATABASE_ID="$db_id"
}

if [[ -f "$metadata_file" ]]; then
  echo "Using previously saved cluster metadata; no create request will be sent."
  set -a
  # shellcheck disable=SC1090
  source "$metadata_file"
  set +a
else
  require_value HC_API_ENDPOINT "${HC_API_ENDPOINT:-}"
  require_value HC_API_METHOD "${HC_API_METHOD:-}"
  require_value HC_API_PAYLOAD_TEMPLATE "${HC_API_PAYLOAD_TEMPLATE:-}"
  require_value HC_RESPONSE_ID_JQ "${HC_RESPONSE_ID_JQ:-}"
  require_value HC_RESPONSE_URI_JQ "${HC_RESPONSE_URI_JQ:-}"
  [[ -f "$HC_API_PAYLOAD_TEMPLATE" ]] || {
    echo "Request payload template does not exist: $HC_API_PAYLOAD_TEMPLATE" >&2
    exit 2
  }
  [[ "$HC_API_ENDPOINT" == "https://app.gizmodata.com/api/database-clusters/?wait=false" ]] || {
    echo "The API endpoint differs from the supplied GizmoData curl request." >&2
    exit 2
  }
  [[ "$HC_API_METHOD" == "POST" ]] || {
    echo "The supplied cluster creation curl request uses POST." >&2
    exit 2
  }
  require_value HC_ADMIN_USERNAME "${HC_ADMIN_USERNAME:-}"
  require_value HC_ADMIN_PASSWORD "${HC_ADMIN_PASSWORD:-}"

  reuse_saved_response=false
  if [[ -s "$response_file" ]]; then
    if jq -e "($HC_RESPONSE_ID_JQ | strings | select(length > 0))" "$response_file" >/dev/null 2>&1; then
      echo "Found a successful response from an earlier request; parsing it without creating another cluster."
      reuse_saved_response=true
    else
      api_error="$(jq -er '.detail | strings | select(length > 0)' "$response_file" 2>/dev/null || true)"
      if [[ -n "$api_error" ]]; then
        echo "Discarding saved API error and retrying with the current configuration: $api_error" >&2
        require_value GIZMODATA_API_KEY "${GIZMODATA_API_KEY:-}"
        rm -f "$response_file"
      else
        echo "The saved response does not contain the configured cluster ID and URI; preserving it to prevent duplicate creation." >&2
        echo "Inspect $response_file and resolve it before retrying to avoid duplicate creation." >&2
        exit 2
      fi
    fi
  fi

  if [[ "$reuse_saved_response" != true ]]; then
    require_value GIZMODATA_API_KEY "${GIZMODATA_API_KEY:-}"
    ensure_s3_secret
    ensure_database
    python3 scripts/render_hardcharger_request.py "$HC_API_PAYLOAD_TEMPLATE" "$payload_tmp"
    if ! perform_request "$HC_API_ENDPOINT" "$HC_API_METHOD" "$payload_tmp" "$response_file"; then
      save_support_curl "$HC_API_ENDPOINT" "$HC_API_METHOD" "$payload_tmp"
      exit 1
    fi
  fi
  chmod 600 "$response_file"
  HC_CLUSTER_ID="$(jq -er "$HC_RESPONSE_ID_JQ" "$response_file")"
  HARDCHARGER_URI="$(jq -er "$HC_RESPONSE_URI_JQ" "$response_file" 2>/dev/null || true)"
  HARDCHARGER_USERNAME="${HC_ADMIN_USERNAME}"
  HARDCHARGER_PASSWORD="${HC_ADMIN_PASSWORD}"
  if [[ -n "${HC_RESPONSE_USERNAME_JQ:-}" ]]; then
    HARDCHARGER_USERNAME="$(jq -er "$HC_RESPONSE_USERNAME_JQ" "$response_file")"
  fi
  if [[ -n "${HC_RESPONSE_PASSWORD_JQ:-}" ]]; then
    HARDCHARGER_PASSWORD="$(jq -er "$HC_RESPONSE_PASSWORD_JQ" "$response_file")"
  fi
fi

require_value HC_CLUSTER_ID "${HC_CLUSTER_ID:-}"
require_value HARDCHARGER_USERNAME "${HARDCHARGER_USERNAME:-}"

if [[ -n "${HC_READY_ENDPOINT_TEMPLATE:-}" ]]; then
  require_value HC_READY_METHOD "${HC_READY_METHOD:-}"
  require_value HC_READY_STATE_JQ "${HC_READY_STATE_JQ:-}"
  require_value HC_READY_SUCCESS_VALUE "${HC_READY_SUCCESS_VALUE:-}"
  timeout="${HC_READY_TIMEOUT_SECONDS:-900}"
  interval="${HC_READY_POLL_SECONDS:-10}"
  [[ "$timeout" =~ ^[1-9][0-9]*$ && "$interval" =~ ^[1-9][0-9]*$ ]] || {
    echo "Readiness timeout and poll interval must be positive integers." >&2
    exit 2
  }
  deadline=$(( $(date +%s) + timeout ))
  encoded_cluster_id="$(python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$HC_CLUSTER_ID")"
  ready_endpoint="${HC_READY_ENDPOINT_TEMPLATE//\{cluster_id\}/$encoded_cluster_id}"
  while (( $(date +%s) < deadline )); do
    if perform_request "$ready_endpoint" "$HC_READY_METHOD" "" "$response_tmp"; then
      state="$(jq -er "$HC_READY_STATE_JQ" "$response_tmp" 2>/dev/null || true)"
      if [[ "$state" == "$HC_READY_SUCCESS_VALUE" ]]; then
        if [[ -z "${HARDCHARGER_URI:-}" ]]; then
          HARDCHARGER_URI="$(jq -er "$HC_RESPONSE_URI_JQ" "$response_tmp")"
        fi
        echo "Managed Hardcharger cluster is ready."
        break
      fi
    fi
    sleep "$interval"
  done
  if [[ "${state:-}" != "$HC_READY_SUCCESS_VALUE" ]]; then
    echo "Timed out waiting for cluster readiness; metadata is preserved to prevent duplicate creation." >&2
    exit 1
  fi
else
  echo "Readiness polling skipped: no documented readiness endpoint was configured."
fi

if [[ -z "${HARDCHARGER_URI:-}" && -s "$response_file" ]]; then
  HARDCHARGER_URI="$(jq -er "$HC_RESPONSE_URI_JQ" "$response_file" 2>/dev/null || true)"
fi
require_value HARDCHARGER_URI "${HARDCHARGER_URI:-}"
[[ "$HARDCHARGER_URI" == https://* || "$HARDCHARGER_URI" == hardcharger://* ]] || {
  echo "The selected connection URI must use a TLS-enabled HTTPS or hardcharger scheme." >&2
  exit 2
}

if [[ ! -f "$metadata_file" ]]; then
  printf 'HC_CLUSTER_ID=%q\nHARDCHARGER_URI=%q\nHARDCHARGER_USERNAME=%q\n' \
    "$HC_CLUSTER_ID" "$HARDCHARGER_URI" "$HARDCHARGER_USERNAME" >"$metadata_file"
  chmod 600 "$metadata_file"
fi

if [[ -z "${HC_SECRET_ID:-}" ]]; then
  terraform_outputs="$(terraform -chdir=terraform output -json)" || {
    echo "Could not read Terraform outputs; set HC_SECRET_ID or apply the Terraform stack." >&2
    exit 2
  }
  HC_SECRET_ID="$(jq -er '.hardcharger_secret_arn.value | strings | select(length > 0)' <<<"$terraform_outputs" 2>/dev/null)" || {
    echo "Terraform has no hardcharger_secret_arn output. Apply the Terraform stack or set HC_SECRET_ID to the existing Secrets Manager name/ARN." >&2
    exit 2
  }
fi
if [[ "$HC_SECRET_ID" == arn:* ]]; then
  [[ "$HC_SECRET_ID" =~ ^arn:[A-Za-z0-9-]+:secretsmanager:[A-Za-z0-9-]+:[0-9]{12}:secret:[A-Za-z0-9/_+=.@-]+$ ]] || {
    echo "HC_SECRET_ID is not a valid Secrets Manager ARN." >&2
    exit 2
  }
else
  [[ "$HC_SECRET_ID" =~ ^[A-Za-z0-9/_+=.@-]+$ ]] || {
    echo "HC_SECRET_ID must be a Secrets Manager name or ARN; check Terraform output and shell configuration." >&2
    exit 2
  }
fi

HARDCHARGER_PASSWORD="${HARDCHARGER_PASSWORD:-${HC_ADMIN_PASSWORD:-}}"
if [[ -z "${HARDCHARGER_PASSWORD:-}" && -s "$response_file" && -n "${HC_RESPONSE_PASSWORD_JQ:-}" ]]; then
  HARDCHARGER_PASSWORD="$(jq -er "$HC_RESPONSE_PASSWORD_JQ" "$response_file")"
fi
if [[ -n "${HARDCHARGER_PASSWORD:-}" ]]; then
  jq -n \
    --arg uri "$HARDCHARGER_URI" \
    --arg username "$HARDCHARGER_USERNAME" \
    --arg password "$HARDCHARGER_PASSWORD" \
    '{uri: $uri, username: $username, password: $password}' >"$secret_tmp"
  chmod 600 "$secret_tmp"
  aws secretsmanager put-secret-value \
    --secret-id "$HC_SECRET_ID" \
    --secret-string "file://$secret_tmp" \
    --query VersionId --output text >/dev/null
  unset HARDCHARGER_PASSWORD
  rm -f "$response_file"
else
  aws secretsmanager get-secret-value --secret-id "$HC_SECRET_ID" \
    --query SecretString --output text >/dev/null || {
    echo "No usable credential is stored. Supply the credential securely before retrying." >&2
    exit 2
  }
fi

unset GIZMODATA_API_KEY HC_ADMIN_PASSWORD
echo "Hardcharger cluster metadata saved locally; connection credential stored in Secrets Manager."
