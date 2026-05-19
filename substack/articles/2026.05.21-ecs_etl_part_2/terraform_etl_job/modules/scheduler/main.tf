resource "aws_scheduler_schedule" "etl" {
  name       = "${var.name_prefix}-${var.etl_job_name}-daily"
  group_name = "default"

  flexible_time_window {
    mode = var.schedule_flexible_window_mode
  }

  schedule_expression          = var.schedule_expression
  schedule_expression_timezone = var.schedule_timezone

  target {
    arn      = var.ecs_cluster_arn
    role_arn = var.scheduler_role_arn

    ecs_parameters {
      task_definition_arn = var.ecs_task_definition_arn
      launch_type         = "FARGATE"
      platform_version    = "LATEST"
      task_count          = 1

      network_configuration {
        assign_public_ip = true
        subnets          = var.subnet_ids
        security_groups  = [var.security_group_id]
      }
    }

    retry_policy {
      maximum_retry_attempts       = 3
      maximum_event_age_in_seconds = 3600
    }
  }

  state = "ENABLED"
}
