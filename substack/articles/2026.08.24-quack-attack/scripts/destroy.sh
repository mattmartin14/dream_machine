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

instance_id="$(terraform state show aws_instance.quack 2>/dev/null | awk -F' = ' '$1 ~ /^ *id$/ {gsub(/"/, "", $2); print $2; exit}')"
delete_root_volume=false
root_volume_id=""

if [[ -n "$instance_id" ]]; then
  aws_region="$(terraform console <<< 'var.aws_region' | tr -d '"')"
  root_device_name="$(aws ec2 describe-instances \
    --region "$aws_region" \
    --instance-ids "$instance_id" \
    --query 'Reservations[0].Instances[0].RootDeviceName' \
    --output text)"
  root_volume_id="$(aws ec2 describe-instances \
    --region "$aws_region" \
    --instance-ids "$instance_id" \
    --query "Reservations[0].Instances[0].BlockDeviceMappings[?DeviceName=='$root_device_name'].Ebs.VolumeId | [0]" \
    --output text)"

  echo
  echo "Root EBS volume handling:"
  echo "  1) Retain the root EBS volume and its Quack catalog (default)"
  echo "  2) Delete the root EBS volume after EC2 terminates"
  read -r -p "Choose [1/2]: " volume_choice

  if [[ "$volume_choice" == "2" ]]; then
    delete_root_volume=true
    echo "The root volume $root_volume_id will be permanently deleted after teardown."
  else
    echo "The root volume will be retained as an available EBS volume."
  fi
fi

terraform destroy -var='force_destroy_ducklake_bucket=true'

if [[ "$delete_root_volume" == true && -n "$root_volume_id" && "$root_volume_id" != "None" ]]; then
  echo "Waiting for root EBS volume $root_volume_id to detach."
  aws ec2 wait volume-available --region "$aws_region" --volume-ids "$root_volume_id"
  aws ec2 delete-volume --region "$aws_region" --volume-id "$root_volume_id"
  echo "Deleted root EBS volume $root_volume_id."
fi