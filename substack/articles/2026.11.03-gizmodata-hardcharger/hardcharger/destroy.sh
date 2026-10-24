#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" != "--confirm" ]]; then
  echo "Refusing to terminate the managed Hardcharger cluster. Re-run with --confirm." >&2
  exit 2
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
METADATA_FILE="hardcharger/cluster-config.env"
[[ -f "$METADATA_FILE" ]] || {
  echo "Saved cluster metadata not found: $METADATA_FILE" >&2
  exit 2
}
set -a
# shellcheck disable=SC1090
source "$METADATA_FILE"
set +a

for command in curl python3; do
  command -v "$command" >/dev/null || {
    echo "Required command not found: $command" >&2
    exit 2
  }
done

if [[ -z "${HC_CLUSTER_ID:-}" ]]; then
  echo "HC_CLUSTER_ID is missing from saved cluster metadata." >&2
  exit 2
fi
if [[ ! "$HC_CLUSTER_ID" =~ ^[[:xdigit:]]{8}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{12}$ ]]; then
  echo "HC_CLUSTER_ID is not a UUID; refusing to call the termination API." >&2
  exit 2
fi
if [[ -z "${GIZMODATA_API_KEY:-}" ]]; then
  echo "Set GIZMODATA_API_KEY through the protected shell/CI environment." >&2
  exit 2
fi

umask 077
curl_config="$(mktemp)"
response_file="$(mktemp)"
trap 'rm -f "$curl_config" "$response_file"' EXIT
cat >"$curl_config" <<EOF
url = "https://app.gizmodata.com/api/database-clusters/${HC_CLUSTER_ID}?wait=true"
request = "DELETE"
header = "Authorization: Bearer ${GIZMODATA_API_KEY}"
header = "Content-Type: application/json"
silent
show-error
fail-with-body
max-time = 1800
EOF
chmod 600 "$curl_config"

if ! status="$(curl --config "$curl_config" --output "$response_file" --write-out '%{http_code}')"; then
  echo "GizmoData cluster termination request failed; AWS teardown has not started." >&2
  exit 1
fi
if [[ "$status" != "200" ]]; then
  echo "GizmoData cluster termination returned HTTP $status; AWS teardown has not started." >&2
  exit 1
fi

rm -f "$METADATA_FILE" hardcharger/cluster-response.json
unset GIZMODATA_API_KEY
echo "GizmoData confirmed cluster termination; local cluster metadata removed."
