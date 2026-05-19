output "cluster_arn" {
  value = aws_ecs_cluster.etl.arn
}

output "cluster_name" {
  value = aws_ecs_cluster.etl.name
}

output "log_group_name" {
  value = aws_cloudwatch_log_group.etl.name
}
