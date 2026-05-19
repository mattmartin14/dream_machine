resource "aws_cloudwatch_log_group" "etl" {
  name              = "/ecs/${var.name_prefix}"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

resource "aws_ecs_cluster" "etl" {
  name = "${var.name_prefix}-cluster"
  tags = var.tags
}
