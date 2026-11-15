#!/bin/bash

ducklake_destroy() {
  local script_path project_dir
  if [[ -n "${BASH_VERSION:-}" ]]; then
    script_path="${BASH_SOURCE[0]}"
  else
    script_path="${(%):-%x}"
  fi
  project_dir=$(cd -- "$(dirname -- "$script_path")/.." && pwd -P) || return
  local -x TF_VAR_aurora_admin_user="ducklake_destroy"
  local -x TF_VAR_aurora_admin_pwd="destroy_placeholder"
  local -x TF_VAR_allowed_cidr="127.0.0.1/32"

  terraform -chdir="$project_dir/terraform" init -input=false || return
  (umask 077; terraform -chdir="$project_dir/terraform" destroy -auto-approve)
}

ducklake_destroy
