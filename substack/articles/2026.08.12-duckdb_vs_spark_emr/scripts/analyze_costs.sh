#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SUMMARY_PATH="${BENCHMARK_SUMMARY_PATH:-$ROOT_DIR/build/benchmark/benchmark_summary.json}"
OUTPUT_PATH="${COST_ANALYSIS_OUTPUT_PATH:-$ROOT_DIR/build/benchmark/benchmark_cost_analysis.json}"
AWS_REGION="${AWS_REGION:-us-east-1}"

if [[ ! -f "$SUMMARY_PATH" ]]; then
  echo "benchmark summary not found at $SUMMARY_PATH" >&2
  exit 1
fi

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

require_cmd aws
require_cmd python3

SUMMARY_PATH="$SUMMARY_PATH" OUTPUT_PATH="$OUTPUT_PATH" AWS_REGION="$AWS_REGION" \
  python3 "$ROOT_DIR/scripts/analyze_costs.py"
