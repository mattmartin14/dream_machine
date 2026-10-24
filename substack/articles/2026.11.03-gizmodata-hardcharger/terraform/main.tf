locals {
  name_prefix        = "${var.project}-${var.environment}"
  use_existing_vpc   = var.existing_vpc_id != ""
  secret_name        = var.hardcharger_secret_name != "" ? var.hardcharger_secret_name : "${local.name_prefix}/connection"
  lock_table_name    = var.etl_lock_table_name != "" ? var.etl_lock_table_name : "${local.name_prefix}-etl-lock"
  raw_bucket_name    = var.raw_bucket_name != "" ? var.raw_bucket_name : "${local.name_prefix}-${data.aws_caller_identity.current.account_id}-raw"
  output_bucket_name = var.output_bucket_name != "" ? var.output_bucket_name : "${local.name_prefix}-${data.aws_caller_identity.current.account_id}-out"
  tables_bucket_name = var.tables_bucket_name != "" ? var.tables_bucket_name : "${local.name_prefix}-${data.aws_caller_identity.current.account_id}-tables"
  task_subnet_ids    = local.use_existing_vpc ? var.existing_subnet_ids : aws_subnet.public[*].id
  task_vpc_id        = local.use_existing_vpc ? var.existing_vpc_id : aws_vpc.demo[0].id
}

resource "aws_secretsmanager_secret" "hardcharger" {
  name                    = local.secret_name
  description             = "Managed Hardcharger connection details; value is added after provisioning."
  recovery_window_in_days = 7
  tags                    = var.common_tags
}

resource "aws_ecs_cluster" "demo" {
  name = local.name_prefix
  tags = var.common_tags
}

resource "aws_security_group" "task" {
  name        = "${local.name_prefix}-task"
  description = "Outbound-only Fargate ETL access."
  vpc_id      = local.task_vpc_id

  egress {
    description = "HTTPS and other outbound service traffic."
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = var.common_tags
}

resource "aws_cloudwatch_log_group" "etl" {
  name              = "/ecs/${local.name_prefix}"
  retention_in_days = var.log_retention_days
  tags              = var.common_tags
}

resource "aws_ecs_task_definition" "etl" {
  family                   = "${local.name_prefix}-etl"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.task_cpu
  memory                   = var.task_memory
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task.arn

  container_definitions = jsonencode([
    {
      name      = "etl"
      image     = var.container_image
      essential = true
      command   = ["hardcharger-etl"]
      environment = [
        { name = "AWS_REGION", value = var.aws_region },
        { name = "ENVIRONMENT", value = var.environment },
        { name = "RAW_BUCKET", value = aws_s3_bucket.raw.bucket },
        { name = "RAW_PREFIX", value = "tpch" },
        { name = "HARDCHARGER_SECRET_ARN", value = aws_secretsmanager_secret.hardcharger.arn },
        { name = "DDB_LOCK_TABLE", value = aws_dynamodb_table.etl_lock.name }
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
  tags = var.common_tags
}

resource "aws_cloudwatch_metric_alarm" "etl_failure" {
  alarm_name          = "${local.name_prefix}-etl-failure"
  alarm_description   = "An ETL container reported a failed run."
  namespace           = "HardchargerDemo"
  metric_name         = "ETLFailures"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  dimensions          = { Environment = var.environment }
  tags                = var.common_tags
}

resource "aws_cloudwatch_metric_alarm" "etl_duration" {
  alarm_name          = "${local.name_prefix}-etl-long-run"
  alarm_description   = "ETL execution exceeded the configured duration threshold."
  namespace           = "HardchargerDemo"
  metric_name         = "ETLDuration"
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 3600
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  dimensions          = { Environment = var.environment }
  tags                = var.common_tags
}

resource "aws_cloudwatch_metric_alarm" "scheduler_errors" {
  alarm_name          = "${local.name_prefix}-scheduler-errors"
  alarm_description   = "EventBridge Scheduler failed to invoke the ETL task."
  namespace           = "AWS/Scheduler"
  metric_name         = "TargetErrorCount"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  dimensions          = { ScheduleGroup = aws_scheduler_schedule.etl.group_name }
  tags                = var.common_tags
}

resource "aws_cloudwatch_metric_alarm" "validation_failure" {
  alarm_name          = "${local.name_prefix}-validation-failure"
  alarm_description   = "ETL output failed a data correctness validation."
  namespace           = "HardchargerDemo"
  metric_name         = "ValidationFailures"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  dimensions          = { Environment = var.environment }
  tags                = var.common_tags
}

resource "aws_cloudwatch_event_rule" "ecs_task_failure" {
  name        = "${local.name_prefix}-ecs-task-failure"
  description = "Match failed or failed-to-start project ECS tasks."
  event_pattern = jsonencode({
    source        = ["aws.ecs"]
    "detail-type" = ["ECS Task State Change"]
    detail = {
      clusterArn = [aws_ecs_cluster.demo.arn]
      lastStatus = ["STOPPED"]
      "$or" = [
        { stopCode = ["TaskFailedToStart"] },
        { containers = { exitCode = [{ "anything-but" = 0 }] } }
      ]
    }
  })
  tags = var.common_tags
}

resource "aws_cloudwatch_metric_alarm" "ecs_task_failure" {
  alarm_name          = "${local.name_prefix}-ecs-task-failure"
  alarm_description   = "A project ECS task failed to start or exited unsuccessfully."
  namespace           = "AWS/Events"
  metric_name         = "MatchedEvents"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  dimensions = {
    EventBusName = "default"
    RuleName     = aws_cloudwatch_event_rule.ecs_task_failure.name
  }
  tags = var.common_tags
}
