output "aws_region" {
  value = var.aws_region
}

output "bucket_name" {
  value = var.bucket_name
}

output "root_prefix" {
  value = var.root_prefix
}

output "dataset_prefix" {
  value = var.dataset_prefix
}

output "script_prefix" {
  value = var.script_prefix
}

output "ecr_repository_url" {
  value = aws_ecr_repository.duckdb_runner.repository_url
}

output "ecs_cluster_name" {
  value = aws_ecs_cluster.benchmark.name
}

output "ecs_task_definition_arn" {
  value = aws_ecs_task_definition.duckdb.arn
}

output "ecs_security_group_id" {
  value = aws_security_group.ecs_task.id
}

output "ecs_subnet_ids" {
  value = data.aws_subnets.default.ids
}

output "ecs_assign_public_ip" {
  value = var.ecs_assign_public_ip
}

output "emr_application_id" {
  value = aws_emrserverless_application.spark.id
}

output "emr_runtime_role_arn" {
  value = aws_iam_role.emr_runtime.arn
}

output "ecs_task_role_arn" {
  value = aws_iam_role.ecs_task.arn
}