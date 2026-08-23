#!/usr/bin/env bash
# Tears down all Quack POC resources, including DuckLake S3 data and the root EBS volume.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
terraform_dir="$(cd "$script_dir/../terraform" && pwd)"

if [[ ! -f "$terraform_dir/terraform.tfvars" ]]; then
  echo "Missing $terraform_dir/terraform.tfvars. Terraform needs the original deployment values to destroy it."
  exit 1
fi

cd "$terraform_dir"
terraform init

# Persist force_destroy before destroy because a destroy-only plan reads the
# existing bucket setting from Terraform state.
if terraform state list | grep -qx 'aws_s3_bucket.ducklake_data'; then
  terraform apply -target=aws_s3_bucket.ducklake_data -auto-approve
fi

terraform destroy -auto-approve