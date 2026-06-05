variable "name_prefix" {
  description = "Resource name prefix."
  type        = string
}

variable "runner_size" {
  description = "Runner size class name."
  type        = string
}

variable "container_cpu" {
  description = "Task CPU units."
  type        = number
}

variable "container_memory" {
  description = "Task memory in MiB."
  type        = number
}

variable "task_ephemeral_storage_gib" {
  description = "Ephemeral storage size in GiB for Fargate task scratch space."
  type        = number
}

variable "execution_role_arn" {
  description = "ECS execution role ARN."
  type        = string
}

variable "task_role_arn" {
  description = "ECS task role ARN."
  type        = string
}

variable "ecr_repository_url" {
  description = "ECR repository URL."
  type        = string
}

variable "image_tag" {
  description = "Container image tag."
  type        = string
}

variable "aws_region" {
  description = "AWS region."
  type        = string
}

variable "default_script_s3_uri" {
  description = "Default S3 URI argument passed to runner.py. Airflow overrides this per task."
  type        = string
}

variable "log_level" {
  description = "Application log level."
  type        = string
}

variable "slack_webhook_secret_name" {
  description = "Secrets Manager secret name used by the app to fetch Slack webhook URL."
  type        = string
}

variable "log_group_name" {
  description = "CloudWatch log group name for ECS logs."
  type        = string
}

variable "tags" {
  description = "Resource tags."
  type        = map(string)
}
