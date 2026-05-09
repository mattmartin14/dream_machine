output "aws_account_id" {
  description = "AWS account used by the provider"
  value       = data.aws_caller_identity.current.account_id
}

output "s3_source_bucket_name" {
  description = "Existing source bucket name for raw sales data"
  value       = var.s3_source_bucket_name
}

output "s3_target_bucket_name" {
  description = "Existing target bucket name for ETL output"
  value       = var.s3_target_bucket_name
}

output "s3_script_bucket_name" {
  description = "Existing script bucket name for runtime ETL artifacts"
  value       = var.s3_script_bucket_name
}

output "s3_bucket_name" {
  description = "Legacy alias for target bucket name"
  value       = var.s3_target_bucket_name
}

output "ecr_repository_url" {
  description = "ECR repository URL for container push"
  value       = module.ecr.repository_url
}

output "ecs_cluster_arn" {
  description = "ECS cluster ARN"
  value       = module.ecs.cluster_arn
}

output "ecs_task_definition_arn" {
  description = "ECS task definition ARN"
  value       = module.ecs.task_definition_arn
}

output "scheduler_name" {
  description = "EventBridge Scheduler name"
  value       = module.scheduler.name
}
