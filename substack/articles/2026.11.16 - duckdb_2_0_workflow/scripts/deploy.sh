#!/bin/bash

if [[ -n "${BASH_VERSION:-}" && "${BASH_SOURCE[0]}" == "$0" ]] ||
   [[ -n "${ZSH_VERSION:-}" && "${ZSH_EVAL_CONTEXT:-}" != *:file ]]; then
  printf '%s\n' 'Run this script with: source scripts/deploy.sh' >&2
  exit 1
fi

ducklake_deploy() {
  local script_path project_dir endpoint public_ip cert_dir cert_path cert_tmp
  if [[ -n "${BASH_VERSION:-}" ]]; then
    script_path="${BASH_SOURCE[0]}"
  else
    script_path="${(%):-%x}"
  fi
  project_dir=$(cd -- "$(dirname -- "$script_path")/.." && pwd -P) || return
  local -x TF_VAR_aurora_admin_user="${AURORA_ADMIN_USER:-}"
  local -x TF_VAR_aurora_admin_pwd="${AURORA_ADMIN_PWD:-}"
  local -x TF_VAR_allowed_cidr

  if [[ -z "$TF_VAR_aurora_admin_user" || -z "$TF_VAR_aurora_admin_pwd" ]]; then
    printf '%s\n' 'Set AURORA_ADMIN_USER and AURORA_ADMIN_PWD before deploying (e.g. source ~/.zshrc).' >&2
    return 1
  fi

  unset AURORA_HOST AURORA_SSL_ROOT_CERT
  public_ip=$(curl -4 -fsS --max-time 10 https://checkip.amazonaws.com) || {
    printf '%s\n' 'Failed to determine the public IPv4 address.' >&2
    return 1
  }
  if [[ -z "$public_ip" ]]; then
    printf '%s\n' 'The public IPv4 address lookup returned an empty response.' >&2
    return 1
  fi
  TF_VAR_allowed_cidr="${public_ip}/32"
  cert_dir="$HOME/.aws"
  cert_path="$cert_dir/rds-global-bundle.pem"
  mkdir -p "$cert_dir" || return
  cert_tmp=$(mktemp "$cert_dir/.rds-global-bundle.XXXXXX") || return
  if ! curl -fsS --connect-timeout 10 --max-time 60 \
    https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem -o "$cert_tmp"; then
    printf '%s\n' 'Failed to download the AWS RDS CA bundle.' >&2
    rm -f -- "$cert_tmp"
    return 1
  fi
  if ! openssl x509 -in "$cert_tmp" -noout >/dev/null; then
    printf '%s\n' 'The downloaded AWS RDS CA bundle is invalid.' >&2
    rm -f -- "$cert_tmp"
    return 1
  fi
  if ! mv -f -- "$cert_tmp" "$cert_path"; then
    printf '%s\n' 'Failed to install the AWS RDS CA bundle.' >&2
    rm -f -- "$cert_tmp"
    return 1
  fi
  terraform -chdir="$project_dir/terraform" init -input=false || return
  (umask 077; terraform -chdir="$project_dir/terraform" apply -auto-approve) || return
  endpoint=$(terraform -chdir="$project_dir/terraform" output -raw aurora_host) || return
  if [[ -z "$endpoint" ]]; then
    printf '%s\n' 'Terraform returned an empty Aurora endpoint.' >&2
    return 1
  fi
  export AURORA_HOST="$endpoint"
  export AURORA_SSL_ROOT_CERT="$cert_path"
  printf '%s\n' 'AURORA_HOST and AURORA_SSL_ROOT_CERT are set in the current shell.'
}

ducklake_deploy
