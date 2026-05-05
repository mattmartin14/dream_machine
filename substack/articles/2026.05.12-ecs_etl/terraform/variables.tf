variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name prefix for resources"
  type        = string
  default     = "ecs-duckdb-etl"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "demo"
}

variable "image_tag" {
  description = "Container image tag in ECR"
  type        = string
  default     = "latest"
}

variable "container_cpu" {
  description = "Task CPU units"
  type        = number
  default     = 512
}

variable "container_memory" {
  description = "Task memory in MiB"
  type        = number
  default     = 1024
}

variable "s3_bucket_name" {
  description = "Optional existing or preferred bucket name; leave empty to auto-generate"
  type        = string
  default     = ""
}

variable "s3_input_prefix" {
  description = "Input prefix containing parquet files"
  type        = string
  default     = "raw/"
}

variable "s3_output_prefix" {
  description = "Output prefix for transformed parquet files"
  type        = string
  default     = "processed/"
}

variable "s3_script_key" {
  description = "S3 key for the ETL script fetched by the runner container"
  type        = string
  default     = "etl/scripts/main_etl.py"
}

variable "log_level" {
  description = "Application log level"
  type        = string
  default     = "INFO"
}

variable "schedule_expression" {
  description = "EventBridge Scheduler expression"
  type        = string
  default     = "cron(0 5 * * ? *)"
}

variable "schedule_timezone" {
  description = "IANA timezone used by EventBridge Scheduler"
  type        = string
  default     = "America/New_York"
}

variable "schedule_flexible_window_mode" {
  description = "OFF for exact schedule, FLEXIBLE for fuzzy execution"
  type        = string
  default     = "OFF"
}

variable "tags" {
  description = "Additional tags"
  type        = map(string)
  default     = {}
}
