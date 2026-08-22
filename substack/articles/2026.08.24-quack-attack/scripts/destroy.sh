#!/usr/bin/env bash
# Tears down the Quack EC2 POC, including DuckLake Parquet data in its S3 bucket.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
terraform_dir="$(cd "$script_dir/../terraform" && pwd)"

if [[ ! -f "$terraform_dir/terraform.tfvars" ]]; then
  echo "Missing $terraform_dir/terraform.tfvars. Terraform needs the original deployment values to destroy it."
  exit 1
fi

cd "$terraform_dir"
terraform init
terraform destroy -var='force_destroy_ducklake_bucket=true'