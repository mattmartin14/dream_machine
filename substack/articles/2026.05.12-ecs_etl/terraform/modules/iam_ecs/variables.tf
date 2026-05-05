variable "name_prefix" {
  description = "Resource name prefix"
  type        = string
}

variable "s3_bucket_arn" {
  description = "S3 bucket ARN"
  type        = string
}

variable "s3_script_key" {
  description = "S3 key for runtime ETL script"
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
