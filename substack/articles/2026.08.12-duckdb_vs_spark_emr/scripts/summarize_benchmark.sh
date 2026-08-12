#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="$ROOT_DIR/build/benchmark"
SUMMARY_FILE="$TMP_DIR/benchmark_summary.json"

for required in \
  "$TMP_DIR/duckdb_benchmark_result.json" \
  "$TMP_DIR/spark_benchmark_result.json" \
  "$TMP_DIR/duckdb_control_plane.json" \
  "$TMP_DIR/spark_control_plane.json"; do
  if [[ ! -f "$required" ]]; then
    echo "missing required benchmark artifact: $required" >&2
    exit 1
  fi
done

python3 - <<'PY'
import json
from pathlib import Path

base = Path("build/benchmark")
duckdb = json.loads((base / "duckdb_benchmark_result.json").read_text())
spark = json.loads((base / "spark_benchmark_result.json").read_text())
duckdb_ctrl = json.loads((base / "duckdb_control_plane.json").read_text())
spark_ctrl = json.loads((base / "spark_control_plane.json").read_text())

runtime = None
runtime_file = base / "spark_runtime_diagnostics.json"
if runtime_file.exists():
    runtime = json.loads(runtime_file.read_text())

skew = None
skew_file = base / "spark_skew_analysis.json"
if skew_file.exists():
    skew = json.loads(skew_file.read_text())

duckdb_elapsed = float(duckdb["elapsed_seconds"])
spark_elapsed = float(spark["elapsed_seconds"])
ratio = round(spark_elapsed / duckdb_elapsed, 6) if duckdb_elapsed > 0 else None

summary = {
    "benchmark_id": duckdb["benchmark_id"],
    "run_date": duckdb["run_date"],
    "engines": {
        "duckdb": {
            "in_job_elapsed_seconds": duckdb_elapsed,
            "counts": duckdb["counts"],
            "output_uri": duckdb["output_uri"],
            "metrics_uri": duckdb["metrics_uri"],
            "control_plane": duckdb_ctrl,
        },
        "spark": {
            "in_job_elapsed_seconds": spark_elapsed,
            "counts": spark["counts"],
            "output_uri": spark["output_uri"],
            "metrics_uri": spark["metrics_uri"],
            "control_plane": spark_ctrl,
            "runtime_diagnostics": runtime,
            "skew_analysis": skew,
        },
    },
    "comparison": {
        "spark_vs_duckdb_in_job_ratio": ratio,
        "in_job_elapsed_delta_seconds": round(spark_elapsed - duckdb_elapsed, 6),
    },
}

(base / "benchmark_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
print(json.dumps(summary, indent=2))
PY

echo "Wrote $SUMMARY_FILE"
