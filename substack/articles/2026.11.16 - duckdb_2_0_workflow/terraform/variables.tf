variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "allowed_cidr" {
  description = "Public IPv4 address of the DuckDB client, in /32 CIDR notation."
  type        = string

  validation {
    condition     = can(cidrnetmask(var.allowed_cidr)) && try(tonumber(split("/", var.allowed_cidr)[1]) == 32, false)
    error_message = "allowed_cidr must be a single IPv4 address in /32 CIDR notation."
  }
}

variable "aurora_admin_user" {
  description = "Aurora master username."
  type        = string
}

variable "aurora_admin_pwd" {
  description = "Aurora master password. Terraform stores this in its state."
  type        = string
  sensitive   = true
}
