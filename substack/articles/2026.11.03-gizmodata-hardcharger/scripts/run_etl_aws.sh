#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

CLUSTER_NAME="${ECS_CLUSTER_NAME:?Set ECS_CLUSTER_NAME}"
TASK_DEFINITION="${ECS_TASK_DEFINITION_ARN:?Set ECS_TASK_DEFINITION_ARN}"
SUBNETS="${ECS_SUBNET_IDS:?Set comma-separated ECS_SUBNET_IDS}"
SECURITY_GROUP="${ECS_SECURITY_GROUP_ID:?Set ECS_SECURITY_GROUP_ID}"
REGION="${AWS_REGION:-us-east-2}"
ASSIGN_PUBLIC_IP="${ECS_ASSIGN_PUBLIC_IP:-ENABLED}"

subnet_json="$(python -c 'import json,sys; print(json.dumps(sys.argv[1].split(",")))' "$SUBNETS")"
network_json="$(python -c 'import json,sys; print(json.dumps({"awsvpcConfiguration":{"subnets":json.loads(sys.argv[1]),"securityGroups":[sys.argv[2]],"assignPublicIp":sys.argv[3]}}))' "$subnet_json" "$SECURITY_GROUP" "$ASSIGN_PUBLIC_IP")"
overrides="$(python -c '
import json, os
env = [{"name": key, "value": os.environ[key]} for key in
       ("PROCESSING_DATE", "PROCESSING_START_DATE", "PROCESSING_END_DATE") if os.getenv(key)]
print(json.dumps({"containerOverrides": [{"name": "etl", "environment": env}]}))
')"
task_arn="$(aws ecs run-task \
  --region "$REGION" \
  --cluster "$CLUSTER_NAME" \
  --task-definition "$TASK_DEFINITION" \
  --launch-type FARGATE \
  --network-configuration "$network_json" \
  --overrides "$overrides" \
  --query 'tasks[0].taskArn' \
  --output text)"

if [[ -z "$task_arn" || "$task_arn" == "None" ]]; then
  echo "ECS RunTask did not return a task ARN." >&2
  exit 1
fi
echo "Started ECS task: $task_arn"
aws ecs wait tasks-stopped --region "$REGION" --cluster "$CLUSTER_NAME" --tasks "$task_arn"
exit_code="$(aws ecs describe-tasks \
  --region "$REGION" --cluster "$CLUSTER_NAME" --tasks "$task_arn" \
  --query 'tasks[0].containers[0].exitCode' --output text)"
if [[ "$exit_code" != "0" ]]; then
  aws ecs describe-tasks --region "$REGION" --cluster "$CLUSTER_NAME" --tasks "$task_arn"
  echo "ETL task exited with code $exit_code." >&2
  exit 1
fi
