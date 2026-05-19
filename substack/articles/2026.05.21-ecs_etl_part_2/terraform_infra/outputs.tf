output "aws_account_id" {
  description = "AWS account used by the provider"
  value       = data.aws_caller_identity.current.account_id
}

output "ecr_repository_url" {
  description = "ECR repository URL for container push"
  value       = module.ecr.repository_url
}

output "ecs_cluster_arn" {
  description = "ECS cluster ARN"
  value       = module.ecs_cluster.cluster_arn
}

output "ecs_cluster_name" {
  description = "ECS cluster name"
  value       = module.ecs_cluster.cluster_name
}

output "ecs_log_group_name" {
  description = "CloudWatch log group name for ECS tasks"
  value       = module.ecs_cluster.log_group_name
}

output "execution_role_arn" {
  description = "ECS execution role ARN"
  value       = module.iam_ecs.execution_role_arn
}

output "task_role_arn" {
  description = "ECS task role ARN"
  value       = module.iam_ecs.task_role_arn
}

output "security_group_id" {
  description = "Foundation security group ID"
  value       = module.network.security_group_id
}

output "subnet_ids" {
  description = "Subnet IDs used for ECS networking"
  value       = module.network.subnet_ids
}

output "s3_source_bucket_name" {
  description = "Source bucket name for raw sales data"
  value       = module.s3_buckets.source_bucket_name
}

output "s3_target_bucket_name" {
  description = "Target bucket name for ETL output and scripts"
  value       = module.s3_buckets.target_bucket_name
}

output "s3_source_bucket_arn" {
  description = "Source bucket ARN"
  value       = module.s3_buckets.source_bucket_arn
}

output "s3_target_bucket_arn" {
  description = "Target bucket ARN"
  value       = module.s3_buckets.target_bucket_arn
}

output "slack_webhook_secret_name" {
  description = "Secrets Manager secret name for Slack webhook"
  value       = module.slack_secret.secret_name
}

output "slack_webhook_secret_arn" {
  description = "Secrets Manager secret ARN for Slack webhook"
  value       = module.slack_secret.secret_arn
}
