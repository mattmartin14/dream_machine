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
terraform apply