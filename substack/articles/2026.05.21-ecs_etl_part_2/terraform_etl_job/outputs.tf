output "ecs_cluster_arn" {
  description = "ECS cluster ARN"
  value       = data.aws_ecs_cluster.etl.arn
}

output "ecs_task_definition_arn" {
  description = "ECS task definition ARN"
  value       = module.ecs_task_definition.task_definition_arn
}

output "scheduler_name" {
  description = "EventBridge Scheduler name"
  value       = module.scheduler.name
}

output "etl_job_name" {
  description = "ETL job name used by task definition and schedule"
  value       = var.etl_job_name
}

output "script_s3_uri" {
  description = "S3 URI passed to ECS task as script runtime argument"
  value       = var.script_s3_uri
}
