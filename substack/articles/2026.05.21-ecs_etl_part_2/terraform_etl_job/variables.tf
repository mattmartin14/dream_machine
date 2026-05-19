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

variable "etl_job_name" {
  description = "ETL job name used in ECS task definition and EventBridge schedule names"
  type        = string
  default     = "sales-etl"
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

variable "task_ephemeral_storage_gib" {
  description = "Fargate ephemeral storage size in GiB for task local staging"
  type        = number
  default     = 50
}

variable "local_script_path" {
  description = "Local ETL script path used by deploy script for aws s3 cp"
  type        = string
  default     = "scripts/sales_etl.py"
}

variable "script_s3_uri" {
  description = "S3 URI to execute on scheduled ECS runs"
  type        = string
  default     = "s3://s3-sales-agg-test/etl/scripts/sales_etl.py"
}

variable "slack_webhook_secret_name" {
  description = "AWS Secrets Manager secret name containing the Slack webhook URL"
  type        = string
  default     = "slack_webhook_test"
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
