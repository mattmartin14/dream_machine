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

  if [[ -s "$response_file" ]]; then
    echo "Found a saved response from an earlier request; parsing it without creating another cluster."
  else
    require_value GIZMODATA_API_KEY "${GIZMODATA_API_KEY:-}"
    require_value HC_ADMIN_PASSWORD "${HC_ADMIN_PASSWORD:-}"
    python3 scripts/render_hardcharger_request.py "$HC_API_PAYLOAD_TEMPLATE" "$payload_tmp"
    perform_request "$HC_API_ENDPOINT" "$HC_API_METHOD" "$payload_tmp" "$response_file"
  fi
  chmod 600 "$response_file"
  HC_CLUSTER_ID="$(jq -er "$HC_RESPONSE_ID_JQ" "$response_file")"
  HARDCHARGER_URI="$(jq -er "$HC_RESPONSE_URI_JQ" "$response_file")"
  HARDCHARGER_USERNAME="${HC_ADMIN_USERNAME}"
  HARDCHARGER_PASSWORD="${HC_ADMIN_PASSWORD}"
  if [[ -n "${HC_RESPONSE_USERNAME_JQ:-}" ]]; then
    HARDCHARGER_USERNAME="$(jq -er "$HC_RESPONSE_USERNAME_JQ" "$response_file")"
  fi
  if [[ -n "${HC_RESPONSE_PASSWORD_JQ:-}" ]]; then
    HARDCHARGER_PASSWORD="$(jq -er "$HC_RESPONSE_PASSWORD_JQ" "$response_file")"
  fi
  [[ "$HARDCHARGER_URI" == https://* || "$HARDCHARGER_URI" == hardcharger://* ]] || {
    echo "The selected connection URI must use a TLS-enabled HTTPS or hardcharger scheme." >&2
    exit 2
  }

  printf 'HC_CLUSTER_ID=%q\nHARDCHARGER_URI=%q\nHARDCHARGER_USERNAME=%q\n' \
    "$HC_CLUSTER_ID" "$HARDCHARGER_URI" "$HARDCHARGER_USERNAME" >"$metadata_file"
  chmod 600 "$metadata_file"
fi

require_value HC_CLUSTER_ID "${HC_CLUSTER_ID:-}"
require_value HARDCHARGER_URI "${HARDCHARGER_URI:-}"
require_value HARDCHARGER_USERNAME "${HARDCHARGER_USERNAME:-}"
HC_SECRET_ID="${HC_SECRET_ID:-$(terraform -chdir=terraform output -raw hardcharger_secret_arn)}"
require_value HC_SECRET_ID "${HC_SECRET_ID:-}"

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

unset GIZMODATA_API_KEY HC_ADMIN_PASSWORD
echo "Hardcharger cluster metadata saved locally; connection credential stored in Secrets Manager."
