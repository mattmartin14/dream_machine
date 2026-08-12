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
SCRIPT_PREFIX="${SCRIPT_PREFIX:-$(cd "$TF_DIR" && terraform output -raw script_prefix)}"
RUN_DATE="${RUN_DATE:-$(date +%F)}"
BENCHMARK_ID="${BENCHMARK_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

require_cmd aws
require_cmd terraform

mkdir -p "$TMP_DIR"

emr_application_id() { (cd "$TF_DIR" && terraform output -raw emr_application_id); }
emr_runtime_role_arn() { (cd "$TF_DIR" && terraform output -raw emr_runtime_role_arn); }

SPARK_JOB_DRIVER_FILE="$TMP_DIR/spark_job_driver.json"
cat > "$SPARK_JOB_DRIVER_FILE" <<JSON
{
  "sparkSubmit": {
    "entryPoint": "s3://$BUCKET_NAME/$SCRIPT_PREFIX/spark_job.py",
    "entryPointArguments": [
      "--bucket", "$BUCKET_NAME",
      "--root-prefix", "$ROOT_PREFIX",
      "--dataset-prefix", "$DATASET_PREFIX",
      "--run-date", "$RUN_DATE",
      "--region", "$AWS_REGION",
      "--benchmark-id", "$BENCHMARK_ID"
    ],
    "sparkSubmitParameters": "--py-files s3://$BUCKET_NAME/$SCRIPT_PREFIX/skew_demo_package.zip --conf spark.executor.cores=1 --conf spark.executor.memory=2g --conf spark.driver.cores=1 --conf spark.driver.memory=2g"
  }
}
JSON

SPARK_OVERRIDES_FILE="$TMP_DIR/spark_overrides.json"
cat > "$SPARK_OVERRIDES_FILE" <<JSON
{
  "monitoringConfiguration": {
    "s3MonitoringConfiguration": {
      "logUri": "s3://$BUCKET_NAME/$ROOT_PREFIX/logs/emr-serverless/"
    }
  }
}
JSON

echo "Starting EMR Serverless Spark job for benchmark $BENCHMARK_ID"
spark_submit_started_at="$(date +%s)"
SPARK_JOB_RUN_ID="$(aws emr-serverless start-job-run \
  --no-cli-pager \
  --region "$AWS_REGION" \
  --application-id "$(emr_application_id)" \
  --execution-role-arn "$(emr_runtime_role_arn)" \
  --name "spark-benchmark-$BENCHMARK_ID" \
  --client-token "$BENCHMARK_ID-spark" \
  --job-driver "file://$SPARK_JOB_DRIVER_FILE" \
  --configuration-overrides "file://$SPARK_OVERRIDES_FILE" \
  --execution-timeout-minutes 60 \
  --query 'jobRunId' \
  --output text)"
spark_submit_finished_at="$(date +%s)"

echo "Spark job run ID: $SPARK_JOB_RUN_ID"

spark_poll_started_at="$(date +%s)"
spark_first_running_at=""
spark_finished_at=""

while true; do
  SPARK_STATE="$(aws --no-cli-pager emr-serverless get-job-run --region "$AWS_REGION" --application-id "$(emr_application_id)" --job-run-id "$SPARK_JOB_RUN_ID" --query 'jobRun.state' --output text)"
  elapsed="$(( $(date +%s) - spark_poll_started_at ))"
  if [[ "$SPARK_STATE" == "RUNNING" && -z "$spark_first_running_at" ]]; then
    spark_first_running_at="$(date +%s)"
  fi
  case "$SPARK_STATE" in
    SUCCESS)
      spark_finished_at="$(date +%s)"
      echo "Spark job completed in ${elapsed}s"
      break
      ;;
    FAILED|CANCELLING|CANCELLED)
      SPARK_FAILURE_REASON="$(aws --no-cli-pager emr-serverless get-job-run --region "$AWS_REGION" --application-id "$(emr_application_id)" --job-run-id "$SPARK_JOB_RUN_ID" --query 'jobRun.stateDetails' --output text || true)"
      echo "Spark job failed after ${elapsed}s with state $SPARK_STATE" >&2
      echo "Spark failure details: ${SPARK_FAILURE_REASON:-unknown}" >&2
      spark_finished_at="$(date +%s)"
      exit 1
      ;;
    *)
      echo "[${elapsed}s] Spark job state: $SPARK_STATE"
      ;;
  esac
  sleep "$POLL_SECONDS"
done

SPARK_CONTROL_FILE="$TMP_DIR/spark_control_plane.json"
SPARK_SUBMIT_STARTED_AT="$spark_submit_started_at" \
SPARK_SUBMIT_FINISHED_AT="$spark_submit_finished_at" \
SPARK_FIRST_RUNNING_AT="$spark_first_running_at" \
SPARK_FINISHED_AT="$spark_finished_at" \
SPARK_JOB_RUN_ID="$SPARK_JOB_RUN_ID" \
BENCHMARK_ID="$BENCHMARK_ID" \
python3 - <<'PY'
import json
import os
from pathlib import Path

submit_started = int(os.environ["SPARK_SUBMIT_STARTED_AT"])
submit_finished = int(os.environ["SPARK_SUBMIT_FINISHED_AT"])
first_running_raw = os.environ.get("SPARK_FIRST_RUNNING_AT", "")
finished_raw = os.environ.get("SPARK_FINISHED_AT", "")

first_running = int(first_running_raw) if first_running_raw else None
finished = int(finished_raw) if finished_raw else None

payload = {
  "benchmark_id": os.environ["BENCHMARK_ID"],
  "job_run_id": os.environ["SPARK_JOB_RUN_ID"],
  "submit_api_seconds": submit_finished - submit_started,
  "provision_to_running_seconds": (first_running - submit_finished) if first_running is not None else None,
  "running_to_finish_seconds": (finished - first_running) if first_running is not None and finished is not None else None,
  "control_plane_wall_seconds": (finished - submit_finished) if finished is not None else None,
}
Path("build/benchmark/spark_control_plane.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
PY

SPARK_METRICS_KEY="$ROOT_PREFIX/results/run_date=$RUN_DATE/benchmark_id=$BENCHMARK_ID/engine=spark/benchmark_result.json"
SPARK_SKEW_KEY="$ROOT_PREFIX/results/run_date=$RUN_DATE/benchmark_id=$BENCHMARK_ID/engine=spark/skew_analysis.json"
aws --no-cli-pager s3 cp "s3://$BUCKET_NAME/$SPARK_METRICS_KEY" "$TMP_DIR/spark_benchmark_result.json"
aws --no-cli-pager s3 cp "s3://$BUCKET_NAME/$SPARK_SKEW_KEY" "$TMP_DIR/spark_skew_analysis.json"

SPARK_RUNTIME_FILE="$TMP_DIR/spark_runtime_diagnostics.json"
SPARK_RUNTIME_RAW_JSON="$(aws --no-cli-pager emr-serverless get-job-run \
  --region "$AWS_REGION" \
  --application-id "$(emr_application_id)" \
  --job-run-id "$SPARK_JOB_RUN_ID" \
  --output json)"

SPARK_EXECUTOR_IDS_RAW="$(aws --no-cli-pager s3 ls "s3://$BUCKET_NAME/$ROOT_PREFIX/logs/emr-serverless/applications/$(emr_application_id)/jobs/$SPARK_JOB_RUN_ID/" --recursive \
  | awk -F'SPARK_EXECUTOR/' 'NF>1 {split($2,a,"/"); print a[1]}' \
  | sort -u \
  || true)"

SPARK_RUNTIME_RAW_JSON="$SPARK_RUNTIME_RAW_JSON" \
SPARK_EXECUTOR_IDS_RAW="$SPARK_EXECUTOR_IDS_RAW" \
BENCHMARK_ID="$BENCHMARK_ID" \
SPARK_JOB_RUN_ID="$SPARK_JOB_RUN_ID" \
python3 - <<'PY'
import json
import os
from pathlib import Path

job_run = json.loads(os.environ["SPARK_RUNTIME_RAW_JSON"])["jobRun"]
executor_ids = [x for x in os.environ.get("SPARK_EXECUTOR_IDS_RAW", "").splitlines() if x.strip()]

payload = {
    "benchmark_id": os.environ["BENCHMARK_ID"],
    "job_run_id": os.environ["SPARK_JOB_RUN_ID"],
    "state": job_run.get("state"),
    "state_details": job_run.get("stateDetails"),
    "attempt": job_run.get("attempt"),
    "queued_duration_milliseconds": job_run.get("queuedDurationMilliseconds"),
    "total_execution_duration_seconds": job_run.get("totalExecutionDurationSeconds"),
    "total_resource_utilization": job_run.get("totalResourceUtilization"),
    "billed_resource_utilization": job_run.get("billedResourceUtilization"),
    "monitoring_configuration": job_run.get("configurationOverrides", {}).get("monitoringConfiguration"),
    "observed_executor_ids": executor_ids,
    "observed_executor_count": len(executor_ids),
}

Path("build/benchmark/spark_runtime_diagnostics.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
PY

python3 - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path("build/benchmark/spark_benchmark_result.json").read_text())
control = json.loads(Path("build/benchmark/spark_control_plane.json").read_text())
skew = json.loads(Path("build/benchmark/spark_skew_analysis.json").read_text())
runtime = json.loads(Path("build/benchmark/spark_runtime_diagnostics.json").read_text())
print("Spark benchmark summary")
print(json.dumps({
    "benchmark_id": payload["benchmark_id"],
    "engine": payload["engine"],
    "status": payload["status"],
    "elapsed_seconds": payload["elapsed_seconds"],
    "counts": payload["counts"],
    "output_uri": payload["output_uri"],
    "metrics_uri": payload["metrics_uri"],
    "control_plane": control,
    "skew_analysis_uri": payload.get("extra", {}).get("skew_analysis_uri"),
    "runtime_diagnostics": {
      "queued_duration_milliseconds": runtime.get("queued_duration_milliseconds"),
      "total_execution_duration_seconds": runtime.get("total_execution_duration_seconds"),
      "total_resource_utilization": runtime.get("total_resource_utilization"),
      "billed_resource_utilization": runtime.get("billed_resource_utilization"),
      "observed_executor_count": runtime.get("observed_executor_count"),
      "observed_executor_ids": runtime.get("observed_executor_ids", []),
    },
    "skew_analysis": {
      "possible_size_skew": skew.get("possible_size_skew"),
      "file_count": skew.get("file_count"),
      "size_bytes": skew.get("size_bytes"),
      "largest_files": skew.get("largest_files", [])[:5],
    },
}, indent=2))
PY

echo "Spark benchmark completed for $BENCHMARK_ID"
