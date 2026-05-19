locals {
  environment_rt = "test"
  name_prefix    = "${var.project_name}-${local.environment_rt}"

  tags = merge(
    {
      Project     = var.project_name
      Environment = local.environment_rt
      ManagedBy   = "Terraform"
    },
    var.tags
  )
}
