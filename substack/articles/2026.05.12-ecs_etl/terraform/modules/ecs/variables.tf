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

variable "s3_source_bucket_name" {
  description = "Deprecated: source bucket name is no longer injected into task env"
  type        = string
  default     = ""
}

variable "s3_target_bucket_name" {
  description = "Deprecated: target bucket name is no longer injected into task env"
  type        = string
  default     = ""
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
  description = "Deprecated: input prefix is only used by IAM module"
  type        = string
  default     = ""
}

variable "normalized_output_prefix" {
  description = "Deprecated: output prefix is only used by IAM module"
  type        = string
  default     = ""
}

variable "log_level" {
  description = "Application log level"
  type        = string
}

variable "slack_webhook_secret_name" {
  description = "Secrets Manager secret name used by the app to fetch Slack webhook URL"
  type        = string
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
}
