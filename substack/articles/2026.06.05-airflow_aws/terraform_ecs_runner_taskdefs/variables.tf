variable "aws_region" {
  description = "AWS region for all resources."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name prefix used by the referenced ECS ETL foundation."
  type        = string
  default     = "ecs-duckdb-etl"
}

variable "environment" {
  description = "Environment suffix used by the referenced ECS ETL foundation."
  type        = string
  default     = "test"
}

variable "image_tag" {
  description = "Container image tag in the existing ECR repository."
  type        = string
  default     = "latest"
}

variable "default_script_s3_uri" {
  description = "Default runner command argument. Airflow overrides this per task."
  type        = string
  default     = "s3://s3-sales-agg-test/scripts/orders_etl.py"
}

variable "slack_webhook_secret_name" {
  description = "AWS Secrets Manager secret name containing the Slack webhook URL."
  type        = string
  default     = "slack_webhook_v1"
}

variable "log_level" {
  description = "Application log level for ECS runner containers."
  type        = string
  default     = "INFO"
}

variable "runner_size_classes" {
  description = "Reusable ECS/Fargate runner size classes for Airflow to choose from."
  type = map(object({
    cpu                   = number
    memory                = number
    ephemeral_storage_gib = number
  }))
  default = {
    small = {
      cpu                   = 1024
      memory                = 2048
      ephemeral_storage_gib = 50
    }
    medium = {
      cpu                   = 2048
      memory                = 8192
      ephemeral_storage_gib = 50
    }
    large = {
      cpu                   = 4096
      memory                = 16384
      ephemeral_storage_gib = 100
    }
    xl = {
      cpu                   = 8192
      memory                = 32768
      ephemeral_storage_gib = 150
    }
  }
}

variable "create_airflow_runner_policy" {
  description = "Whether to create an IAM policy that can be attached to the local Airflow AWS principal."
  type        = bool
  default     = false
}

variable "tags" {
  description = "Additional tags."
  type        = map(string)
  default     = {}
}
