#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ETL_JOB_DIR="$PROJECT_ROOT/terraform_etl_job"

usage() {
  cat <<'EOF'
Usage:
  ./deploy/deploy_etl_job.sh --etl-job-name <name> --local-script-path <path> --script-s3-uri <s3://...> [options]

Required:
  --etl-job-name <name>          Job name used in ECS task definition and scheduler naming.
  --local-script-path <path>     Local ETL script path to upload with aws s3 cp.
  --script-s3-uri <s3://...>     Target S3 URI for uploaded script and ECS runtime command.

Optional:
  --container-cpu <units>                ECS task CPU units (default: 512)
  --container-memory <MiB>               ECS task memory in MiB (default: 1024)
  --task-ephemeral-storage-gib <GiB>     Fargate ephemeral storage in GiB (default: 50)
  --schedule-expression <expr>           EventBridge schedule expression
  --schedule-timezone <tz>               EventBridge schedule timezone
  --schedule-flexible-window-mode <mode> OFF or FLEXIBLE
  --image-tag <tag>                      ECR image tag (default: latest)
  --aws-region <region>                  AWS region override
  --auto-approve                         Pass -auto-approve to terraform apply
  -h, --help                             Show help

Examples:
  ./deploy/deploy_etl_job.sh \
    --etl-job-name sales-etl \
    --local-script-path scripts/sales_etl.py \
    --script-s3-uri s3://s3-sales-agg-test/scripts/sales_etl.py

  ./deploy/deploy_etl_job.sh \
    --etl-job-name sales-etl-hourly \
    --local-script-path scripts/sales_etl.py \
    --script-s3-uri s3://s3-sales-agg-test/scripts/sales_etl_hourly.py \
    --container-cpu 1024 \
    --container-memory 4096 \
    --schedule-expression 'cron(0 * * * ? *)' \
    --auto-approve
EOF
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Error: required command not found: $1" >&2
    exit 1
  fi
}

ETL_JOB_NAME=""
LOCAL_SCRIPT_PATH=""
SCRIPT_S3_URI=""
CONTAINER_CPU="512"
CONTAINER_MEMORY="1024"
TASK_EPHEMERAL_STORAGE_GIB="50"
SCHEDULE_EXPRESSION=""
SCHEDULE_TIMEZONE=""
SCHEDULE_FLEXIBLE_WINDOW_MODE=""
IMAGE_TAG="latest"
AWS_REGION_OVERRIDE=""
AUTO_APPROVE="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --etl-job-name)
      ETL_JOB_NAME="${2:-}"
      shift 2
      ;;
    --local-script-path)
      LOCAL_SCRIPT_PATH="${2:-}"
      shift 2
      ;;
    --script-s3-uri)
      SCRIPT_S3_URI="${2:-}"
      shift 2
      ;;
    --container-cpu)
      CONTAINER_CPU="${2:-}"
      shift 2
      ;;
    --container-memory)
      CONTAINER_MEMORY="${2:-}"
      shift 2
      ;;
    --task-ephemeral-storage-gib)
      TASK_EPHEMERAL_STORAGE_GIB="${2:-}"
      shift 2
      ;;
    --schedule-expression)
      SCHEDULE_EXPRESSION="${2:-}"
      shift 2
      ;;
    --schedule-timezone)
      SCHEDULE_TIMEZONE="${2:-}"
      shift 2
      ;;
    --schedule-flexible-window-mode)
      SCHEDULE_FLEXIBLE_WINDOW_MODE="${2:-}"
      shift 2
      ;;
    --image-tag)
      IMAGE_TAG="${2:-}"
      shift 2
      ;;
    --aws-region)
      AWS_REGION_OVERRIDE="${2:-}"
      shift 2
      ;;
    --auto-approve)
      AUTO_APPROVE="true"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Error: unknown argument '$1'" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$ETL_JOB_NAME" || -z "$LOCAL_SCRIPT_PATH" || -z "$SCRIPT_S3_URI" ]]; then
  echo "Error: --etl-job-name, --local-script-path, and --script-s3-uri are required." >&2
  usage
  exit 1
fi

if [[ ! -d "$ETL_JOB_DIR" ]]; then
  echo "Error: ETL job terraform directory not found at $ETL_JOB_DIR" >&2
  exit 1
fi

if [[ ! -f "$PROJECT_ROOT/$LOCAL_SCRIPT_PATH" ]]; then
  echo "Error: local script path not found: $PROJECT_ROOT/$LOCAL_SCRIPT_PATH" >&2
  exit 1
fi

if [[ "$SCRIPT_S3_URI" != s3://* ]]; then
  echo "Error: --script-s3-uri must start with s3://" >&2
  exit 1
fi

require_cmd aws
require_cmd terraform

AWS_REGION_EFFECTIVE="${AWS_REGION_OVERRIDE:-${AWS_REGION:-}}"
if [[ -z "$AWS_REGION_EFFECTIVE" ]]; then
  AWS_REGION_EFFECTIVE="us-east-1"
fi

echo "Uploading script to S3: $SCRIPT_S3_URI"
aws s3 cp "$PROJECT_ROOT/$LOCAL_SCRIPT_PATH" "$SCRIPT_S3_URI" --region "$AWS_REGION_EFFECTIVE"

TF_ARGS=(
  -var "etl_job_name=$ETL_JOB_NAME"
  -var "local_script_path=$LOCAL_SCRIPT_PATH"
  -var "script_s3_uri=$SCRIPT_S3_URI"
  -var "container_cpu=$CONTAINER_CPU"
  -var "container_memory=$CONTAINER_MEMORY"
  -var "task_ephemeral_storage_gib=$TASK_EPHEMERAL_STORAGE_GIB"
  -var "image_tag=$IMAGE_TAG"
)

if [[ -n "$AWS_REGION_OVERRIDE" ]]; then
  TF_ARGS+=( -var "aws_region=$AWS_REGION_OVERRIDE" )
fi
if [[ -n "$SCHEDULE_EXPRESSION" ]]; then
  TF_ARGS+=( -var "schedule_expression=$SCHEDULE_EXPRESSION" )
fi
if [[ -n "$SCHEDULE_TIMEZONE" ]]; then
  TF_ARGS+=( -var "schedule_timezone=$SCHEDULE_TIMEZONE" )
fi
if [[ -n "$SCHEDULE_FLEXIBLE_WINDOW_MODE" ]]; then
  TF_ARGS+=( -var "schedule_flexible_window_mode=$SCHEDULE_FLEXIBLE_WINDOW_MODE" )
fi

terraform -chdir="$ETL_JOB_DIR" init

# Keep each ETL job in its own Terraform workspace/state to avoid replacing other jobs.
if terraform -chdir="$ETL_JOB_DIR" workspace list | sed 's/*//g' | tr -d ' ' | grep -Fxq "$ETL_JOB_NAME"; then
  terraform -chdir="$ETL_JOB_DIR" workspace select "$ETL_JOB_NAME" >/dev/null
else
  terraform -chdir="$ETL_JOB_DIR" workspace new "$ETL_JOB_NAME" >/dev/null
fi

# If scheduler IAM resources already exist in AWS but are missing from state,
# import them to avoid EntityAlreadyExists errors on reruns.
PROJECT_NAME_EFFECTIVE="${TF_VAR_project_name:-ecs-duckdb-etl}"
NAME_PREFIX="${PROJECT_NAME_EFFECTIVE}-test-${ETL_JOB_NAME}"
SCHEDULER_ROLE_NAME="${NAME_PREFIX}-scheduler-role"
SCHEDULER_POLICY_NAME="${NAME_PREFIX}-scheduler-invoke"
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text 2>/dev/null || true)"
SCHEDULER_POLICY_ARN="arn:aws:iam::${ACCOUNT_ID}:policy/${SCHEDULER_POLICY_NAME}"

if ! terraform -chdir="$ETL_JOB_DIR" state list | grep -Fxq "module.iam_scheduler.aws_iam_role.scheduler_invoke"; then
  if aws iam get-role --role-name "$SCHEDULER_ROLE_NAME" >/dev/null 2>&1; then
    echo "Importing existing IAM role into Terraform state: $SCHEDULER_ROLE_NAME"
    terraform -chdir="$ETL_JOB_DIR" import module.iam_scheduler.aws_iam_role.scheduler_invoke "$SCHEDULER_ROLE_NAME" >/dev/null
  fi
fi

if ! terraform -chdir="$ETL_JOB_DIR" state list | grep -Fxq "module.iam_scheduler.aws_iam_policy.scheduler_invoke"; then
  if [[ -n "$ACCOUNT_ID" && "$ACCOUNT_ID" != "None" ]] && aws iam get-policy --policy-arn "$SCHEDULER_POLICY_ARN" >/dev/null 2>&1; then
    echo "Importing existing IAM policy into Terraform state: $SCHEDULER_POLICY_ARN"
    terraform -chdir="$ETL_JOB_DIR" import module.iam_scheduler.aws_iam_policy.scheduler_invoke "$SCHEDULER_POLICY_ARN" >/dev/null
  fi
fi

if ! terraform -chdir="$ETL_JOB_DIR" state list | grep -Fxq "module.iam_scheduler.aws_iam_role_policy_attachment.scheduler_invoke"; then
  if aws iam get-role --role-name "$SCHEDULER_ROLE_NAME" >/dev/null 2>&1 && [[ -n "$ACCOUNT_ID" && "$ACCOUNT_ID" != "None" ]] && aws iam get-policy --policy-arn "$SCHEDULER_POLICY_ARN" >/dev/null 2>&1; then
    echo "Importing existing IAM role policy attachment into Terraform state: ${SCHEDULER_ROLE_NAME}/${SCHEDULER_POLICY_ARN}"
    terraform -chdir="$ETL_JOB_DIR" import module.iam_scheduler.aws_iam_role_policy_attachment.scheduler_invoke "${SCHEDULER_ROLE_NAME}/${SCHEDULER_POLICY_ARN}" >/dev/null
  fi
fi

terraform -chdir="$ETL_JOB_DIR" plan "${TF_ARGS[@]}"

if [[ "$AUTO_APPROVE" == "true" ]]; then
  terraform -chdir="$ETL_JOB_DIR" apply -auto-approve "${TF_ARGS[@]}"
else
  terraform -chdir="$ETL_JOB_DIR" apply "${TF_ARGS[@]}"
fi
