variable "name_prefix" {
  description = "Resource name prefix"
  type        = string
}

variable "app_name" {
  description = "Application/pipeline name used in bucket naming"
  type        = string
}

variable "bucket_name" {
  description = "Optional explicit bucket name"
  type        = string
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
}
