locals {
  name_prefix = "${var.project_name}-${var.environment}"

  tags = merge(
    {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "Terraform"
    },
    var.tags
  )

  normalized_input_prefix  = trim(var.s3_input_prefix, "/")
  normalized_output_prefix = trim(var.s3_output_prefix, "/")
}
