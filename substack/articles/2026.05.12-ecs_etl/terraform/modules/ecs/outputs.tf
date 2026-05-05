output "cluster_arn" {
  value = aws_ecs_cluster.etl.arn
}

output "task_definition_arn" {
  value = aws_ecs_task_definition.etl.arn
}

output "log_group_name" {
  value = aws_cloudwatch_log_group.etl.name
}
