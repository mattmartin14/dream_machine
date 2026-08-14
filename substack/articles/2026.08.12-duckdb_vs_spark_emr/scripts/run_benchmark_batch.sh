#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="$ROOT_DIR/build/benchmark"
RUNS_DIR="$TMP_DIR/runs"
REGISTRY_FILE="$TMP_DIR/run_registry.jsonl"

RUN_COUNT="${RUN_COUNT:-3}"
RUN_DATE="${RUN_DATE:-$(date +%F)}"
BENCHMARK_PREFIX="${BENCHMARK_PREFIX:-$(date -u +%Y%m%dT%H%M%SZ)}"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

require_cmd bash
require_cmd python3
require_cmd terraform

mkdir -p "$RUNS_DIR"

if ! terraform -chdir="$ROOT_DIR/terraform" output -raw ecs_cluster_name >/dev/null 2>&1; then
  echo "terraform outputs are unavailable; deploy the benchmark stack before running a batch" >&2
  echo "hint: bash $ROOT_DIR/scripts/deploy.sh" >&2
  exit 1
fi

archive_run() {
  local benchmark_id="$1"
  local run_dir="$RUNS_DIR/$benchmark_id"
  mkdir -p "$run_dir"

  local files=(
    duckdb_benchmark_result.json
    spark_benchmark_result.json
    duckdb_control_plane.json
    spark_control_plane.json
    spark_runtime_diagnostics.json
    spark_skew_analysis.json
    benchmark_summary.json
  )

  local copied_count=0
  local file
  for file in "${files[@]}"; do
    if [[ -f "$TMP_DIR/$file" ]]; then
      cp "$TMP_DIR/$file" "$run_dir/$file"
      copied_count=$((copied_count + 1))
    fi
  done

  BENCHMARK_ID="$benchmark_id" \
  RUN_DATE="$RUN_DATE" \
  RUN_DIR="$run_dir" \
  COPIED_COUNT="$copied_count" \
  python3 - <<'PY'
import datetime as dt
import json
import os
from pathlib import Path

benchmark_id = os.environ["BENCHMARK_ID"]
run_date = os.environ["RUN_DATE"]
run_dir = Path(os.environ["RUN_DIR"])
copied_count = int(os.environ["COPIED_COUNT"])
summary_path = run_dir / "benchmark_summary.json"

payload = {
    "benchmark_id": benchmark_id,
    "run_date": run_date,
    "archived_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
    "copied_artifact_count": copied_count,
    "summary_present": summary_path.exists(),
}

(run_dir / "manifest.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
print(json.dumps(payload))
PY

  BENCHMARK_ID="$benchmark_id" \
  RUN_DATE="$RUN_DATE" \
  RUN_DIR="$run_dir" \
  REGISTRY_FILE="$REGISTRY_FILE" \
  python3 - <<'PY'
import datetime as dt
import json
import os
from pathlib import Path

benchmark_id = os.environ["BENCHMARK_ID"]
run_date = os.environ["RUN_DATE"]
run_dir = Path(os.environ["RUN_DIR"])
registry_file = Path(os.environ["REGISTRY_FILE"])
summary_file = run_dir / "benchmark_summary.json"

record = {
    "benchmark_id": benchmark_id,
    "run_date": run_date,
    "recorded_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
    "summary_path": str(summary_file),
    "run_dir": str(run_dir),
}

registry_file.parent.mkdir(parents=True, exist_ok=True)
with registry_file.open("a", encoding="utf-8") as fh:
    fh.write(json.dumps(record) + "\n")

print(json.dumps(record))
PY
}

for i in $(seq 1 "$RUN_COUNT"); do
  BENCHMARK_ID="${BENCHMARK_PREFIX}-r${i}"
  echo "[run $i/$RUN_COUNT] starting benchmark id $BENCHMARK_ID for run_date $RUN_DATE"
  RUN_DATE="$RUN_DATE" BENCHMARK_ID="$BENCHMARK_ID" bash "$ROOT_DIR/scripts/run_benchmarks.sh"
  archive_run "$BENCHMARK_ID"
  echo "[run $i/$RUN_COUNT] archived benchmark id $BENCHMARK_ID"
done

echo "Batch complete. Registry: $REGISTRY_FILE"
echo "Run artifacts: $RUNS_DIR"
