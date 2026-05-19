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

  normalized_input_prefix  = trim(var.s3_input_prefix, "/")
  normalized_output_prefix = trim(var.s3_output_prefix, "/")
}
