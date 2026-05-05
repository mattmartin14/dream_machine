variable "name_prefix" {
  description = "Resource name prefix"
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
