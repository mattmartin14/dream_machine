variable "name_prefix" {
  description = "Resource name prefix"
  type        = string
}

variable "log_retention_days" {
  description = "CloudWatch log retention for ECS task logs"
  type        = number
  default     = 14
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
}
