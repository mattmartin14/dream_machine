variable "source_bucket_name" {
  description = "Source S3 bucket name"
  type        = string
}

variable "target_bucket_name" {
  description = "Target S3 bucket name"
  type        = string
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
}
