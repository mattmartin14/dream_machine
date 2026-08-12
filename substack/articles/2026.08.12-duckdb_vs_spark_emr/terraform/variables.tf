variable "aws_region" {
  description = "AWS region for the benchmark stack."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Prefix for benchmark resources."
  type        = string
  default     = "duckdb-vs-spark-emr"
}

variable "bucket_name" {
  description = "Existing S3 bucket that stores source data, scripts, logs, and benchmark results."
  type        = string
  default     = "matt-sbx-bucket-1-us-east-1"
}

variable "root_prefix" {
  description = "Root S3 prefix for data, logs, and results."
  type        = string
  default     = "etl-skew-demo"
}

variable "dataset_prefix" {
  description = "Dataset prefix under the benchmark root."
  type        = string
  default     = "raw/returns_chat"
}

variable "script_prefix" {
  description = "S3 prefix used to upload Spark entrypoint scripts and Python package archives."
  type        = string
  default     = "etl/scripts/duckdb-vs-spark-emr"
}

variable "duckdb_image_uri" {
  description = "ECR image URI for the DuckDB ECS task. Leave empty for the bootstrap apply that only creates ECR."
  type        = string
  default     = "public.ecr.aws/docker/library/python:3.13-slim"
}

variable "ecs_task_cpu" {
  description = "Fargate task CPU units for the DuckDB benchmark."
  type        = number
  default     = 2048
}

variable "ecs_task_memory" {
  description = "Fargate task memory in MiB for the DuckDB benchmark."
  type        = number
  default     = 4096
}

variable "ecs_assign_public_ip" {
  description = "Whether the one-off Fargate task should receive a public IP."
  type        = bool
  default     = true
}

variable "emr_release_label" {
  description = "EMR Serverless release label for Spark."
  type        = string
  default     = "emr-7.1.0"
}

variable "emr_idle_timeout_minutes" {
  description = "How long the EMR Serverless application can sit idle before auto-stopping."
  type        = number
  default     = 15
}

variable "emr_maximum_cpu" {
  description = "Maximum aggregate CPU available to the EMR Serverless application."
  type        = string
  default     = "8 vCPU"
}

variable "emr_maximum_memory" {
  description = "Maximum aggregate memory available to the EMR Serverless application."
  type        = string
  default     = "32 GB"
}

variable "tags" {
  description = "Additional tags applied to all managed resources."
  type        = map(string)
  default     = {}
}