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

variable "s3_source_bucket_name" {
  description = "Source bucket name containing raw sales data"
  type        = string
  default     = "s3-sales-raw-test"
}

variable "s3_target_bucket_name" {
  description = "Target bucket name for aggregated sales output"
  type        = string
  default     = "s3-sales-agg-test"
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

variable "runtime_script_allowed_prefixes" {
  description = "Allowed key prefixes (within script bucket) for runtime script downloads"
  type        = list(string)
  default     = ["scripts/", "etl/scripts/"]
}

variable "slack_webhook_secret_name" {
  description = "AWS Secrets Manager secret name containing Slack webhook payload"
  type        = string
  default     = "slack_webhook_v1"
}

variable "slack_webhook" {
  description = "Slack webhook URL to store in AWS Secrets Manager as key slack_webhook"
  type        = string
  sensitive   = true
}

variable "tags" {
  description = "Additional tags"
  type        = map(string)
  default     = {}
}
