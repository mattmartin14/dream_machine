resource "aws_cloudwatch_log_group" "etl" {
  name              = "/ecs/${var.name_prefix}"
  retention_in_days = 14
  tags              = var.tags
}

resource "aws_ecs_cluster" "etl" {
  name = "${var.name_prefix}-cluster"
  tags = var.tags
}

resource "aws_ecs_task_definition" "etl" {
  family                   = "${var.name_prefix}-task"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = tostring(var.container_cpu)
  memory                   = tostring(var.container_memory)
  execution_role_arn       = var.execution_role_arn
  task_role_arn            = var.task_role_arn

  container_definitions = jsonencode([
    {
      name      = "etl"
      image     = "${var.ecr_repository_url}:${var.image_tag}"
      essential = true
      environment = [
        { name = "AWS_REGION", value = var.aws_region },
        { name = "S3_BUCKET", value = var.s3_bucket_name },
        { name = "S3_SCRIPT_KEY", value = var.s3_script_key },
        { name = "S3_INPUT_PREFIX", value = "${var.normalized_input_prefix}/" },
        { name = "S3_OUTPUT_PREFIX", value = "${var.normalized_output_prefix}/" },
        { name = "LOG_LEVEL", value = var.log_level }
      ]
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
