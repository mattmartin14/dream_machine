variable "secret_name" {
  description = "Secrets Manager secret name for Slack webhook"
  type        = string
}

variable "slack_webhook" {
  description = "Slack webhook URL value"
  type        = string
  sensitive   = true
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
}
