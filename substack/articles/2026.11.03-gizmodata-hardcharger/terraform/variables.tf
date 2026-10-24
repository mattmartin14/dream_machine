variable "aws_region" {
  description = "AWS deployment region."
  type        = string
  default     = "us-east-2"
}

variable "hardcharger_region" {
  description = "GizmoData-managed Hardcharger service region, separate from AWS infrastructure."
  type        = string
  default     = "us-east-2"

  validation {
    condition     = var.hardcharger_region == "us-east-2"
    error_message = "The supplied GizmoData cluster request targets us-east-2."
  }
}

variable "project" {
  description = "Short project identifier used in resource names."
  type        = string
  default     = "hardcharger-demo"
}

variable "environment" {
  description = "Environment tag and resource-name suffix."
  type        = string
  default     = "dev"
}

variable "existing_vpc_id" {
  description = "Existing VPC ID; leave empty to create a small VPC."
  type        = string
  default     = ""
}

variable "existing_subnet_ids" {
  description = "Subnets in the existing VPC. Existing VPC mode requires at least two."
  type        = list(string)
  default     = []
}

variable "assign_public_ip" {
  description = "Assign a public IP to Fargate tasks. Required for internet access without NAT."
  type        = bool
  default     = true
}

variable "schedule_expression" {
  description = "EventBridge Scheduler expression. Default daily at 06:00 UTC."
  type        = string
  default     = "cron(0 6 * * ? *)"
}

variable "enable_schedule" {
  description = "Enable recurring ETL invocations. Defaults off to avoid unreviewed charges."
  type        = bool
  default     = false
}

variable "container_image" {
  description = "ECR image URI. The bootstrap image is only a placeholder before deploy.sh pushes."
  type        = string
  default     = "bootstrap:pending"
}

variable "hardcharger_s3_principal_arn" {
  description = "Optional GizmoData AWS principal to grant read-only access to raw S3 data."
  type        = string
  default     = ""
}

variable "raw_bucket_name" {
  description = "Optional globally unique name for the raw data bucket."
  type        = string
  default     = ""
}

variable "output_bucket_name" {
  description = "Optional globally unique name for analytical outputs."
  type        = string
  default     = ""
}

variable "tables_bucket_name" {
  description = "Optional S3 Tables bucket name."
  type        = string
  default     = ""
}

variable "allow_s3_tables_force_destroy" {
  description = "Allow irreversible deletion of all S3 Tables namespaces/tables when true."
  type        = bool
  default     = false
}

variable "hardcharger_secret_name" {
  description = "Optional Secrets Manager name. Defaults to a name scoped by project/environment."
  type        = string
  default     = ""
}

variable "etl_lock_table_name" {
  description = "Optional DynamoDB lock table name. Defaults to a name scoped by project/environment."
  type        = string
  default     = ""
}

variable "task_cpu" {
  description = "Fargate task CPU units."
  type        = number
  default     = 1024
}

variable "task_memory" {
  description = "Fargate task memory in MiB."
  type        = number
  default     = 2048
}

variable "log_retention_days" {
  description = "CloudWatch log retention."
  type        = number
  default     = 30
}

variable "common_tags" {
  description = "Additional tags to apply to project resources."
  type        = map(string)
  default     = {}
}
