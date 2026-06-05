output "ecs_cluster_arn" {
  description = "ECS cluster ARN Airflow should use."
  value       = data.aws_ecs_cluster.etl.arn
}

output "ecs_cluster_name" {
  description = "ECS cluster name Airflow should use."
  value       = data.aws_ecs_cluster.etl.cluster_name
}

output "security_group_id" {
  description = "Security group ID for Airflow ECS task network configuration."
  value       = data.aws_security_group.etl_task.id
}

output "subnet_ids" {
  description = "Subnet IDs for Airflow ECS task network configuration."
  value       = data.aws_subnets.default.ids
}

output "runner_task_definition_arns" {
  description = "Task definition ARNs by runner size class."
  value       = { for size, task_definition in module.ecs_runner_task_definition : size => task_definition.task_definition_arn }
}

output "runner_container_names" {
  description = "Container names by runner size class."
  value       = { for size, task_definition in module.ecs_runner_task_definition : size => task_definition.container_name }
}

output "medium_runner_task_definition_arn" {
  description = "Convenience output for the medium runner task definition ARN."
  value       = module.ecs_runner_task_definition["medium"].task_definition_arn
}

output "medium_runner_container_name" {
  description = "Convenience output for the medium runner container name."
  value       = module.ecs_runner_task_definition["medium"].container_name
}

output "airflow_runner_policy_arn" {
  description = "IAM policy ARN to attach to the local Airflow AWS principal, if created."
  value       = var.create_airflow_runner_policy ? aws_iam_policy.airflow_runner[0].arn : null
}

output "airflow_runner_policy_json" {
  description = "IAM policy JSON local Airflow needs if you prefer to attach it manually."
  value       = data.aws_iam_policy_document.airflow_runner.json
}
