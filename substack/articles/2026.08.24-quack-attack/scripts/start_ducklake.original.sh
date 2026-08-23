#!/usr/bin/env bash
# Opens an in-memory DuckDB client attached only to the server-owned Quack catalog.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
terraform_dir="$(cd "$script_dir/../terraform" && pwd)"
client_database="${1:-:memory:}"

if ! command -v duckdb >/dev/null 2>&1; then
  echo "duckdb CLI is required on this laptop."
  exit 1
fi

if [[ ! -f "$terraform_dir/terraform.tfstate" ]]; then
  echo "No Terraform state found. Deploy the Quack infrastructure first."
  exit 1
fi

quack_uri="$(terraform -chdir="$terraform_dir" output -raw quack_uri)"
quack_public_ip="$(terraform -chdir="$terraform_dir" output -raw quack_public_ip)"
quack_token="$(terraform -chdir="$terraform_dir" output -raw quack_token)"
local_quack_port="${QUACK_LOCAL_PORT:-9494}"

if ! command -v ssh >/dev/null 2>&1; then
  echo "SSH is required to tunnel the plain-HTTP Quack connection."
  exit 1
fi

if ! lsof -ti "tcp:$local_quack_port" >/dev/null 2>&1; then
  ssh -f -N \
    -o ExitOnForwardFailure=yes \
    -o StrictHostKeyChecking=accept-new \
    -i "$HOME/.ssh/quack-poc" \
    -L "127.0.0.1:$local_quack_port:127.0.0.1:9494" \
    "ubuntu@$quack_public_ip"
fi

local_quack_uri="quack:localhost:$local_quack_port"

if [[ "$client_database" != ":memory:" ]]; then
  mkdir -p "$(dirname "$client_database")"
fi

init_file="$(mktemp)"
trap 'rm -f "$init_file"' EXIT

cat >"$init_file" <<SQL
INSTALL quack;
LOAD quack;

CREATE OR REPLACE SECRET quack_secret (TYPE quack, TOKEN '$quack_token');
ATTACH 'quack:$local_quack_uri' AS remote;
SQL

cat <<EOF
Remote Quack endpoint: $quack_uri
Local SSH tunnel endpoint: $local_quack_uri
Server-owned DuckLake alias: dl1
Server-owned S3 DATA_PATH: configured on EC2

SQL executed:
  ATTACH 'quack:$local_quack_uri' AS remote;

Copy this into another DuckDB instance on this laptop:
  INSTALL quack;
  LOAD quack;

  CREATE OR REPLACE SECRET quack_secret (
    TYPE quack,
    TOKEN '$quack_token'
  );
  ATTACH 'quack:$local_quack_uri' AS remote;

Run server-owned DuckLake SQL through Quack:
  SELECT * FROM remote.query('SELECT * FROM dl1.some_table');
  SELECT * FROM remote.query('INSERT INTO dl1.some_table VALUES (...)');

This uses the SSH tunnel at $local_quack_uri created by this script.
Client database: $client_database
EOF

duckdb -init "$init_file" "$client_database"