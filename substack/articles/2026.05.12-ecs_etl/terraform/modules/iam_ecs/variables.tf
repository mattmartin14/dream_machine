variable "name_prefix" {
  description = "Resource name prefix"
  type        = string
}

variable "s3_source_bucket_arn" {
  description = "S3 source bucket ARN"
  type        = string
}

variable "s3_target_bucket_arn" {
  description = "S3 target bucket ARN"
  type        = string
}

variable "s3_script_bucket_arn" {
  description = "S3 bucket ARN where runtime ETL script is stored"
  type        = string
}

variable "runtime_script_allowed_prefixes" {
  description = "Allowed script key prefixes under the script bucket"
  type        = list(string)
}

variable "slack_webhook_secret_arn" {
  description = "Secrets Manager ARN for Slack webhook secret"
  type        = string
}

variable "normalized_input_prefix" {
  description = "Normalized input prefix without leading/trailing slash"
  type        = string
}

variable "normalized_output_prefix" {
  description = "Normalized output prefix without leading/trailing slash"
  type        = string
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
}
