output "aws_region" {
  value = var.aws_region
}

output "hardcharger_region" {
  value = var.hardcharger_region
}

output "raw_bucket_name" {
  value = aws_s3_bucket.raw.bucket
}

output "output_bucket_name" {
  value = aws_s3_bucket.output.bucket
}

output "ecr_repository_url" {
  value = aws_ecr_repository.etl.repository_url
}

output "ecs_cluster_name" {
  value = aws_ecs_cluster.demo.name
}

output "ecs_task_definition_arn" {
  value = aws_ecs_task_definition.etl.arn
}

output "task_security_group_id" {
  value = aws_security_group.task.id
}

output "assign_public_ip" {
  value = var.assign_public_ip
}

output "task_subnet_ids" {
  value = local.task_subnet_ids
}

output "hardcharger_secret_arn" {
  value     = aws_secretsmanager_secret.hardcharger.arn
  sensitive = true
}

output "s3_tables_bucket_arn" {
  value = aws_s3tables_table_bucket.analytics.arn
}

output "scheduler_name" {
  value = aws_scheduler_schedule.etl.name
}
