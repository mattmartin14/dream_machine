#!/usr/bin/env bash
# Opens the SSH tunnel that exposes Quack on localhost:9494.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
terraform_dir="$(cd "$script_dir/../terraform" && pwd)"

if ! command -v ssh >/dev/null 2>&1; then
  echo "SSH is required to tunnel the plain-HTTP Quack connection."
  exit 1
fi

if [[ ! -f "$terraform_dir/terraform.tfstate" ]]; then
  echo "No Terraform state found. Deploy the Quack infrastructure first."
  exit 1
fi

quack_public_ip="$(terraform -chdir="$terraform_dir" output -raw quack_public_ip)"
local_quack_port="${QUACK_LOCAL_PORT:-9494}"

if lsof -ti "tcp:$local_quack_port" >/dev/null 2>&1; then
  echo "SSH tunnel already active on localhost:$local_quack_port"
  exit 0
fi

ssh -f -N \
  -o ExitOnForwardFailure=yes \
  -o StrictHostKeyChecking=accept-new \
  -i "$HOME/.ssh/quack-poc" \
  -L "127.0.0.1:$local_quack_port:127.0.0.1:9494" \
  "ubuntu@$quack_public_ip"

echo "SSH tunnel active: quack:localhost:$local_quack_port"
