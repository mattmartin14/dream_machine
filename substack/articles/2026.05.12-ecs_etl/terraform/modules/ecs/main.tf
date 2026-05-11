resource "aws_cloudwatch_log_group" "etl" {
  name              = "/ecs/${var.name_prefix}"
  retention_in_days = 14
  tags              = var.tags
}

locals {
  container_environment = [
    { name = "AWS_REGION", value = var.aws_region },
    { name = "SLACK_WEBHOOK_SECRET_NAME", value = var.slack_webhook_secret_name },
    { name = "LOG_LEVEL", value = var.log_level }
  ]
}

resource "aws_ecs_cluster" "etl" {
  name = "${var.name_prefix}-cluster"
  tags = var.tags
}

resource "aws_ecs_task_definition" "etl" {
  family                   = "${var.name_prefix}-${var.app_name}-task"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = tostring(var.container_cpu)
  memory                   = tostring(var.container_memory)
  execution_role_arn       = var.execution_role_arn
  task_role_arn            = var.task_role_arn

  ephemeral_storage {
    size_in_gib = var.task_ephemeral_storage_gib
  }

  container_definitions = jsonencode([
    {
      name        = "etl"
      image       = "${var.ecr_repository_url}:${var.image_tag}"
      essential   = true
      entryPoint  = ["python3.13", "/app/runner/runner.py"]
      command     = [var.default_script_s3_uri]
      environment = local.container_environment
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.etl.name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = "etl"
        }
      }
    }
  ])

  tags = var.tags
}
