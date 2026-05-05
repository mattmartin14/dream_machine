variable "name_prefix" {
  description = "Resource name prefix"
  type        = string
}

variable "app_name" {
  description = "Application/pipeline name used in task definition naming"
  type        = string
}

variable "container_cpu" {
  description = "Task CPU units"
  type        = number
}

variable "container_memory" {
  description = "Task memory in MiB"
  type        = number
}

variable "execution_role_arn" {
  description = "ECS execution role ARN"
  type        = string
}

variable "task_role_arn" {
  description = "ECS task role ARN"
  type        = string
}

variable "ecr_repository_url" {
  description = "ECR repository URL"
  type        = string
}

variable "image_tag" {
  description = "Container image tag"
  type        = string
}

variable "aws_region" {
  description = "AWS region"
  type        = string
}

variable "s3_bucket_name" {
  description = "Default data bucket name for task definition"
  type        = string
}

variable "s3_script_bucket_name" {
  description = "S3 bucket name where ETL scripts are stored"
  type        = string
}

variable "s3_script_key" {
  description = "S3 key for runtime ETL script"
  type        = string
}

variable "normalized_input_prefix" {
  description = "Normalized input prefix"
  type        = string
}

variable "normalized_output_prefix" {
  description = "Normalized output prefix"
  type        = string
}

variable "log_level" {
  description = "Application log level"
  type        = string
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
}
