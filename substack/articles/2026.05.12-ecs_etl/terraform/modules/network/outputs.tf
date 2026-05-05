output "subnet_ids" {
  value = data.aws_subnets.default.ids
}

output "security_group_id" {
  value = aws_security_group.etl_task.id
}
