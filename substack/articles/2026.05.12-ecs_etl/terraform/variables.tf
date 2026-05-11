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

variable "app_name" {
  description = "Application/pipeline name used for app-specific assets"
  type        = string
  default     = "main-etl"
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

variable "s3_source_bucket_name" {
  description = "Existing source bucket name containing raw sales data"
  type        = string
}

variable "s3_target_bucket_name" {
  description = "Existing target bucket name for aggregated sales output"
  type        = string
}

variable "s3_script_bucket_name" {
  description = "Existing bucket name where runtime ETL script is stored"
  type        = string
}

variable "s3_input_prefix" {
  description = "Input prefix containing parquet files"
  type        = string
  default     = "tpch/orders_raw/"
}

variable "s3_output_prefix" {
  description = "Output prefix for transformed parquet files"
  type        = string
  default     = "tpch/cust_agg/"
}

variable "s3_script_key" {
  description = "S3 key for the ETL script fetched by the runner container"
  type        = string
  default     = "etl/scripts/sales_etl.py"
}

variable "scheduled_script_s3_uri" {
  description = "S3 URI to execute on scheduled ECS runs"
  type        = string
}

variable "runtime_script_allowed_prefixes" {
  description = "Allowed key prefixes (within script bucket) for runtime script downloads"
  type        = list(string)
  default     = ["etl/scripts/"]
}

variable "log_level" {
  description = "Application log level"
  type        = string
  default     = "INFO"
}

variable "slack_webhook_secret_name" {
  description = "AWS Secrets Manager secret name containing the Slack webhook URL"
  type        = string
  default     = "slack_webhook_test"
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
