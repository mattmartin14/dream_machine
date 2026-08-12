#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DATE="${RUN_DATE:-$(date +%F)}"
BENCHMARK_ID="${BENCHMARK_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"

export RUN_DATE
export BENCHMARK_ID

echo "Running DuckDB benchmark for $BENCHMARK_ID"
bash "$ROOT_DIR/scripts/run_duckdb_benchmark.sh"

echo "Running Spark benchmark for $BENCHMARK_ID"
bash "$ROOT_DIR/scripts/run_spark_benchmark.sh"

echo "Generating benchmark summary for $BENCHMARK_ID"
bash "$ROOT_DIR/scripts/summarize_benchmark.sh"

echo "Benchmark completed for $BENCHMARK_ID"
