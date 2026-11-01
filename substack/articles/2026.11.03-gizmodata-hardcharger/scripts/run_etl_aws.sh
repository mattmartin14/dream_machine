#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

REGION="${AWS_REGION:-us-east-1}"
tf_out() { terraform -chdir=terraform output "$@"; }
CLUSTER_NAME="$(tf_out -raw ecs_cluster_name)"
TASK_DEFINITION="$(tf_out -raw ecs_task_definition_arn)"
SECURITY_GROUP="$(tf_out -raw task_security_group_id)"
SUBNETS="$(tf_out -json task_subnet_ids | jq -r 'join(",")')"
if [[ "$(tf_out -raw assign_public_ip)" == "true" ]]; then
  ASSIGN_PUBLIC_IP=ENABLED
else
  ASSIGN_PUBLIC_IP=DISABLED
fi
network_json="$(jq -nc --arg s "$SUBNETS" --arg g "$SECURITY_GROUP" --arg a "$ASSIGN_PUBLIC_IP" \
  '{awsvpcConfiguration: {subnets: ($s | split(",")), securityGroups: [$g], assignPublicIp: $a}}')"
task_arn="$(aws ecs run-task \
  --region "$REGION" \
  --cluster "$CLUSTER_NAME" \
  --task-definition "$TASK_DEFINITION" \
  --launch-type FARGATE \
  --network-configuration "$network_json" \
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
echo "ETL task completed successfully."
