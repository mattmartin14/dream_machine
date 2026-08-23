variable "aws_region" {
  description = "AWS region for all resources."
  type        = string
  default     = "us-east-1"
}

variable "name" {
  description = "Prefix for named AWS resources."
  type        = string
  default     = "quack-poc"
}

variable "vpc_id" {
  description = "VPC that contains the target subnet."
  type        = string
}

variable "subnet_id" {
  description = "Public subnet for the EC2 instance and Elastic IP."
  type        = string
}

variable "key_pair_name" {
  description = "Name for the Terraform-managed EC2 key pair."
  type        = string
  default     = "quack-poc"
}

variable "public_key_path" {
  description = "Path to the local SSH public key Terraform registers as the EC2 key pair."
  type        = string
  default     = "~/.ssh/quack-poc.pub"
}

variable "allowed_cidr_blocks" {
  description = "CIDR blocks permitted to SSH and connect to Quack, for example [\"203.0.113.25/32\"]."
  type        = list(string)
}

variable "ducklake_bucket_name" {
  description = "Globally unique private S3 bucket name for DuckLake Parquet data."
  type        = string
}

variable "quack_token" {
  description = "Shared token clients send to the Quack server."
  type        = string
  sensitive   = true
}

variable "instance_type" {
  description = "Small Graviton instance type suitable for this POC."
  type        = string
  default     = "t4g.small"
}

variable "ami_id" {
  description = "Optional ARM64 Ubuntu AMI ID. Null selects the latest Ubuntu 22.04 ARM64 AMI."
  type        = string
  default     = null
  nullable    = true
}

variable "quack_port" {
  description = "TCP port exposed by Quack."
  type        = number
  default     = 9494
}

variable "root_volume_size_gb" {
  description = "Size of the EBS root volume that stores the Quack DuckDB catalog."
  type        = number
  default     = 20
}

variable "delete_root_volume_on_termination" {
  description = "Whether terminating the instance also deletes its EBS root volume. Enabled by default so POC teardown removes all billable storage."
  type        = bool
  default     = true
}

variable "force_destroy_ducklake_bucket" {
  description = "Allow Terraform to delete the non-empty DuckLake POC bucket during destroy."
  type        = bool
  default     = true
}

variable "tags" {
  description = "Additional tags applied to AWS resources."
  type        = map(string)
  default     = {}
}