locals {
  name_prefix = "${var.project_name}-${var.environment}"

  tags = merge(
    {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "Terraform"
      Component   = "local-airflow-ecs-runner"
    },
    var.tags
  )
}
