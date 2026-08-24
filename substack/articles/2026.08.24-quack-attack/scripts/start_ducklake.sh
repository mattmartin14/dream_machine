#!/usr/bin/env bash
# Writes and reads a server-owned DuckLake table through Quack's remote.query(...).
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
terraform_dir="$(cd "$script_dir/../terraform" && pwd)"

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
local_quack_host="localhost:$local_quack_port"
local_quack_uri="quack:$local_quack_host"

if ! command -v ssh >/dev/null 2>&1; then
  echo "SSH is required to tunnel the plain-HTTP Quack connection."
  exit 1
fi

"$script_dir/start_ssh.sh" >/dev/null 2>&1 || exit 1

cat <<EOF
Remote Quack endpoint: $quack_uri
Local SSH tunnel endpoint: $local_quack_uri
Server-owned DuckLake alias: dl1
Server-owned S3 DATA_PATH: configured on EC2

Quack client SQL:
  INSTALL quack; LOAD quack;
  CREATE  SECRET quack_secret (TYPE quack, TOKEN '$quack_token');
  ATTACH 'quack:$local_quack_host' AS remote;

Writing and reading dl1.quack_demo on the EC2-hosted DuckLake catalog.
EOF

duckdb :memory: <<SQL
INSTALL quack;
LOAD quack;

CREATE OR REPLACE SECRET quack_secret (TYPE quack, TOKEN '$quack_token');
ATTACH 'quack:$local_quack_host' AS remote;

SELECT * FROM remote.query('
  CREATE TABLE IF NOT EXISTS dl1.quack_demo (
    id BIGINT,
    created_at TIMESTAMPTZ
  )
');

SELECT * FROM remote.query('
  INSERT INTO dl1.quack_demo VALUES (epoch_ms(current_timestamp), current_timestamp)
');

SELECT * FROM remote.query('
  SELECT * FROM dl1.quack_demo ORDER BY created_at
');
SQL