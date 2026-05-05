variable "name_prefix" {
  description = "Resource name prefix"
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

variable "ecs_task_role_arn" {
  description = "ECS task role ARN"
  type        = string
}

variable "ecs_execution_role_arn" {
  description = "ECS execution role ARN"
  type        = string
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
}
