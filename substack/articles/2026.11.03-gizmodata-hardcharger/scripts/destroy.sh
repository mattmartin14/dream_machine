#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

CONFIRM=false
DELETE_S3_DATA=false
DELETE_HARDCHARGER=false
for arg in "$@"; do
  case "$arg" in
    --confirm) CONFIRM=true ;;
    --delete-s3-data) DELETE_S3_DATA=true ;;
    --delete-hardcharger) DELETE_HARDCHARGER=true ;;
    *) echo "Unknown option: $arg" >&2; exit 2 ;;
  esac
done
if [[ "$CONFIRM" != true ]]; then
  echo "Refusing to destroy resources. Review the effects, then pass --confirm." >&2
  exit 2
fi
if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi
REGION="${AWS_REGION:-us-east-1}"
command -v terraform >/dev/null || { echo "terraform is required." >&2; exit 2; }
command -v aws >/dev/null || { echo "aws CLI is required." >&2; exit 2; }
terraform -chdir=terraform init

if [[ "$DELETE_HARDCHARGER" == true ]]; then
  ./hardcharger/destroy.sh --confirm
fi

terraform -chdir=terraform destroy -auto-approve \
  -var="aws_region=$REGION"
rm -f .deploy-image-uri
echo "Terraform-managed project resources were destroyed. Existing VPC/subnets were not managed by Terraform."
