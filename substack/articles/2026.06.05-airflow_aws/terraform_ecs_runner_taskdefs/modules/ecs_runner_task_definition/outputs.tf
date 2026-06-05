output "task_definition_arn" {
  description = "ECS task definition ARN."
  value       = aws_ecs_task_definition.runner.arn
}

output "container_name" {
  description = "Container name to use in Airflow ECS containerOverrides."
  value       = local.container_name
}
