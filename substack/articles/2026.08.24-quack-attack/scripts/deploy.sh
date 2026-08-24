#!/usr/bin/env bash
# Deploys the Quack EC2 POC from the Terraform module in this repository.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
terraform_dir="$(cd "$script_dir/../terraform" && pwd)"

if [[ ! -f "$terraform_dir/terraform.tfvars" ]]; then
  echo "Missing $terraform_dir/terraform.tfvars. Copy terraform.tfvars.example and fill in its values."
  exit 1
fi

cd "$terraform_dir"
terraform init
terraform plan
terraform apply -auto-approve

quack_public_ip="$(terraform output -raw quack_public_ip)"
quack_token="$(terraform output -raw quack_token)"
quack_attach="quack:localhost:9494"

cat <<EOF

Quack client SQL (connect through the SSH tunnel on localhost:9494):
  INSTALL quack; LOAD quack;
  CREATE SECRET quack_secret (TYPE quack, TOKEN '$quack_token');
  ATTACH '$quack_attach' AS remote;

Optional sanity check:
  SELECT * FROM remote.query('SELECT 1');

To open the tunnel manually, run:
  bash scripts/start_ssh.sh

Important: this POC intentionally runs Quack as plain HTTP on EC2. The client must connect via the local SSH tunnel to localhost:9494; a direct public-IP attach will default to HTTPS and fail without a TLS proxy.
EOF