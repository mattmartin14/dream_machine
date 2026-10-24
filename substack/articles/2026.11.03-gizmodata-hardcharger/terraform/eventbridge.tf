resource "aws_scheduler_schedule" "etl" {
  name                         = "${local.name_prefix}-daily-etl"
  description                  = "Daily Hardcharger ETL; disabled until explicitly enabled."
  schedule_expression          = var.schedule_expression
  schedule_expression_timezone = "UTC"
  state                        = var.enable_schedule ? "ENABLED" : "DISABLED"
  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_ecs_cluster.demo.arn
    role_arn = aws_iam_role.scheduler.arn
    ecs_parameters {
      task_definition_arn = aws_ecs_task_definition.etl.arn
      launch_type         = "FARGATE"
      task_count          = 1
      network_configuration {
        subnets          = local.task_subnet_ids
        security_groups  = [aws_security_group.task.id]
        assign_public_ip = var.assign_public_ip
      }
    }
    retry_policy {
      maximum_event_age_in_seconds = 3600
      maximum_retry_attempts       = 0
    }
  }
}
