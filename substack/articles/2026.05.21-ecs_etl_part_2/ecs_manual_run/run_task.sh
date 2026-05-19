#!/usr/bin/env bash
SCRIPT_S3_URI="s3://s3-sales-agg-test/etl/scripts/sales_etl.py"
OVERRIDES_JSON="$(printf '{\"containerOverrides\":[{\"name\":\"etl\",\"command\":[\"%s\"]}]}' "$SCRIPT_S3_URI")"

AWS_REGION=us-east-1
SUBNETS_CSV=$(aws ec2 describe-subnets --region "$AWS_REGION" --filters Name=default-for-az,Values=true --query 'Subnets[].SubnetId' --output text | tr '\t' ',')
SG_ID=$(aws ec2 describe-security-groups --region "$AWS_REGION" --filters Name=group-name,Values=ecs-duckdb-etl-test-sg --query 'SecurityGroups[0].GroupId' --output text)
echo "$SUBNETS_CSV"
echo "$SG_ID"

aws ecs run-task \
  --region "$AWS_REGION" \
  --cluster "arn:aws:ecs:$AWS_REGION:$AWS_ACCT_ID:cluster/ecs-duckdb-etl-test-cluster" \
  --task-definition "arn:aws:ecs:$AWS_REGION:$AWS_ACCT_ID:task-definition/ecs-duckdb-etl-test-sales-etl-task:2" \
  --launch-type FARGATE \
  --count "1" \
  --overrides "$OVERRIDES_JSON" \
  --network-configuration "awsvpcConfiguration={subnets=[$SUBNETS_CSV],securityGroups=[$SG_ID],assignPublicIp=ENABLED}" \
  --output json