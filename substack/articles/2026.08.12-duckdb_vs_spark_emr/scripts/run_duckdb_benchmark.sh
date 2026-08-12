#!/usr/bin/env bash
set -euo pipefail
export AWS_PAGER=""

POLL_SECONDS="${POLL_SECONDS:-10}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TF_DIR="$ROOT_DIR/terraform"
TMP_DIR="$ROOT_DIR/build/benchmark"

AWS_REGION="${AWS_REGION:-$(cd "$TF_DIR" && terraform output -raw aws_region)}"
BUCKET_NAME="${BUCKET_NAME:-$(cd "$TF_DIR" && terraform output -raw bucket_name)}"
ROOT_PREFIX="${ROOT_PREFIX:-$(cd "$TF_DIR" && terraform output -raw root_prefix)}"
DATASET_PREFIX="${DATASET_PREFIX:-$(cd "$TF_DIR" && terraform output -raw dataset_prefix)}"
RUN_DATE="${RUN_DATE:-$(date +%F)}"
BENCHMARK_ID="${BENCHMARK_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
PROJECT_NAME="${PROJECT_NAME:-duckdb-vs-spark-emr}"
MANAGED_BY="${MANAGED_BY:-terraform}"
BENCHMARK_TAG="${BENCHMARK_TAG:-duckdb-vs-spark}"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

require_cmd aws
require_cmd terraform

mkdir -p "$TMP_DIR"

ecs_cluster_name() { (cd "$TF_DIR" && terraform output -raw ecs_cluster_name); }
ecs_task_definition_arn() { (cd "$TF_DIR" && terraform output -raw ecs_task_definition_arn); }
ecs_security_group_id() { (cd "$TF_DIR" && terraform output -raw ecs_security_group_id); }

SUBNETS_JSON="$(cd "$TF_DIR" && terraform output -json ecs_subnet_ids)"
ASSIGN_PUBLIC_IP="$(cd "$TF_DIR" && terraform output -raw ecs_assign_public_ip)"

SUBNETS_CSV="$(SUBNETS_JSON="$SUBNETS_JSON" python3 - <<'PY'
import json
import os
print(",".join(json.loads(os.environ["SUBNETS_JSON"])))
PY
)"

PUBLIC_IP_VALUE="DISABLED"
if [[ "$ASSIGN_PUBLIC_IP" == "true" ]]; then
  PUBLIC_IP_VALUE="ENABLED"
fi

DUCKDB_OVERRIDES_FILE="$TMP_DIR/duckdb_overrides.json"
cat > "$DUCKDB_OVERRIDES_FILE" <<JSON
{
  "containerOverrides": [
    {
      "name": "duckdb-benchmark",
      "command": [
        "--bucket", "$BUCKET_NAME",
        "--root-prefix", "$ROOT_PREFIX",
        "--dataset-prefix", "$DATASET_PREFIX",
        "--run-date", "$RUN_DATE",
        "--region", "$AWS_REGION",
        "--benchmark-id", "$BENCHMARK_ID"
      ]
    }
  ]
}
JSON

echo "Starting ECS DuckDB task for benchmark $BENCHMARK_ID"
duckdb_submit_started_at="$(date +%s)"
DUCKDB_TASK_ARN="$(aws ecs run-task \
  --no-cli-pager \
  --region "$AWS_REGION" \
  --cluster "$(ecs_cluster_name)" \
  --launch-type FARGATE \
  --task-definition "$(ecs_task_definition_arn)" \
  --network-configuration "awsvpcConfiguration={subnets=[$SUBNETS_CSV],securityGroups=[$(ecs_security_group_id)],assignPublicIp=$PUBLIC_IP_VALUE}" \
  --overrides "file://$DUCKDB_OVERRIDES_FILE" \
  --tags "key=Project,value=$PROJECT_NAME" "key=ManagedBy,value=$MANAGED_BY" "key=Benchmark,value=$BENCHMARK_TAG" "key=BenchmarkId,value=$BENCHMARK_ID" \
  --query 'tasks[0].taskArn' \
  --output text)"
duckdb_submit_finished_at="$(date +%s)"

echo "DuckDB task ARN: $DUCKDB_TASK_ARN"

duckdb_poll_started_at="$(date +%s)"
duckdb_first_running_at=""
duckdb_stopped_at=""

while true; do
  TASK_STATUS="$(aws --no-cli-pager ecs describe-tasks --region "$AWS_REGION" --cluster "$(ecs_cluster_name)" --tasks "$DUCKDB_TASK_ARN" --query 'tasks[0].lastStatus' --output text)"
  elapsed="$(( $(date +%s) - duckdb_poll_started_at ))"
  if [[ "$TASK_STATUS" == "RUNNING" && -z "$duckdb_first_running_at" ]]; then
    duckdb_first_running_at="$(date +%s)"
  fi
  if [[ "$TASK_STATUS" == "STOPPED" ]]; then
    duckdb_stopped_at="$(date +%s)"
    DUCKDB_EXIT_CODE="$(aws --no-cli-pager ecs describe-tasks --region "$AWS_REGION" --cluster "$(ecs_cluster_name)" --tasks "$DUCKDB_TASK_ARN" --query 'tasks[0].containers[0].exitCode' --output text)"
    if [[ "$DUCKDB_EXIT_CODE" != "0" ]]; then
      DUCKDB_STOP_REASON="$(aws --no-cli-pager ecs describe-tasks --region "$AWS_REGION" --cluster "$(ecs_cluster_name)" --tasks "$DUCKDB_TASK_ARN" --query 'tasks[0].stoppedReason' --output text || true)"
      echo "DuckDB ECS task failed after ${elapsed}s with exit code $DUCKDB_EXIT_CODE" >&2
      echo "DuckDB task stop reason: ${DUCKDB_STOP_REASON:-unknown}" >&2
      exit 1
    fi
    echo "DuckDB ECS task completed in ${elapsed}s"
    break
  fi
  echo "[${elapsed}s] DuckDB ECS task status: $TASK_STATUS"
  sleep "$POLL_SECONDS"
done

DUCKDB_CONTROL_FILE="$TMP_DIR/duckdb_control_plane.json"
DUCKDB_SUBMIT_STARTED_AT="$duckdb_submit_started_at" \
DUCKDB_SUBMIT_FINISHED_AT="$duckdb_submit_finished_at" \
DUCKDB_FIRST_RUNNING_AT="$duckdb_first_running_at" \
DUCKDB_STOPPED_AT="$duckdb_stopped_at" \
DUCKDB_TASK_ARN="$DUCKDB_TASK_ARN" \
BENCHMARK_ID="$BENCHMARK_ID" \
python3 - <<'PY'
import json
import os
from pathlib import Path

submit_started = int(os.environ["DUCKDB_SUBMIT_STARTED_AT"])
submit_finished = int(os.environ["DUCKDB_SUBMIT_FINISHED_AT"])
first_running_raw = os.environ.get("DUCKDB_FIRST_RUNNING_AT", "")
stopped_raw = os.environ.get("DUCKDB_STOPPED_AT", "")

first_running = int(first_running_raw) if first_running_raw else None
stopped = int(stopped_raw) if stopped_raw else None

payload = {
  "benchmark_id": os.environ["BENCHMARK_ID"],
  "task_arn": os.environ["DUCKDB_TASK_ARN"],
  "submit_api_seconds": submit_finished - submit_started,
  "provision_to_running_seconds": (first_running - submit_finished) if first_running is not None else None,
  "run_to_stop_seconds": (stopped - first_running) if first_running is not None and stopped is not None else None,
  "control_plane_wall_seconds": (stopped - submit_finished) if stopped is not None else None,
}
Path("build/benchmark/duckdb_control_plane.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
PY

DUCKDB_METRICS_KEY="$ROOT_PREFIX/results/run_date=$RUN_DATE/benchmark_id=$BENCHMARK_ID/engine=duckdb/benchmark_result.json"
aws --no-cli-pager s3 cp "s3://$BUCKET_NAME/$DUCKDB_METRICS_KEY" "$TMP_DIR/duckdb_benchmark_result.json"

python3 - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path("build/benchmark/duckdb_benchmark_result.json").read_text())
control = json.loads(Path("build/benchmark/duckdb_control_plane.json").read_text())
print("DuckDB benchmark summary")
print(json.dumps({
    "benchmark_id": payload["benchmark_id"],
    "engine": payload["engine"],
    "status": payload["status"],
    "elapsed_seconds": payload["elapsed_seconds"],
    "counts": payload["counts"],
    "output_uri": payload["output_uri"],
    "metrics_uri": payload["metrics_uri"],
    "control_plane": control,
}, indent=2))
PY

echo "DuckDB benchmark completed for $BENCHMARK_ID"
