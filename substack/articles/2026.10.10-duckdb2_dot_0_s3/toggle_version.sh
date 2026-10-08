#!/usr/bin/env bash
# Usage: ./toggle_version.sh <2.0|1.5>
set -euo pipefail

case "${1:-}" in
  2.0) curl https://install.duckdb.org | DUCKDB_VERSION=alpha bash ;;
  1.5) curl https://install.duckdb.org | bash ;;
  *) echo "Usage: $0 <2.0|1.5>" >&2; exit 1 ;;
esac
