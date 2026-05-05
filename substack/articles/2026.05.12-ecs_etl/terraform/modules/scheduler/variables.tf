variable "name_prefix" {
  description = "Resource name prefix"
  type        = string
}

variable "schedule_expression" {
  description = "Scheduler expression"
  type        = string
}

variable "schedule_timezone" {
  description = "IANA timezone"
  type        = string
}

variable "schedule_flexible_window_mode" {
  description = "Flexible window mode"
  type        = string
}

variable "ecs_cluster_arn" {
  description = "ECS cluster ARN"
  type        = string
}

variable "ecs_task_definition_arn" {
  description = "ECS task definition ARN"
  type        = string
}

variable "scheduler_role_arn" {
  description = "Scheduler role ARN"
  type        = string
}

variable "subnet_ids" {
  description = "Subnet IDs for ECS task networking"
  type        = list(string)
}

variable "security_group_id" {
  description = "Security group ID for ECS task networking"
  type        = string
}
