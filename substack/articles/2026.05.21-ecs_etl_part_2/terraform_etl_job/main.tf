data "aws_ecr_repository" "etl" {
  name = "${local.name_prefix}-repo"
}

data "aws_ecs_cluster" "etl" {
  cluster_name = "${local.name_prefix}-cluster"
}

data "aws_iam_role" "task" {
  name = "${local.name_prefix}-task-role"
}

data "aws_iam_role" "execution" {
  name = "${local.name_prefix}-execution-role"
}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

data "aws_security_group" "etl_task" {
  filter {
    name   = "group-name"
    values = ["${local.name_prefix}-sg"]
  }

  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

module "ecs_task_definition" {
  source = "./modules/ecs_task_definition"

  name_prefix                = local.name_prefix
  etl_job_name               = var.etl_job_name
  container_cpu              = var.container_cpu
  container_memory           = var.container_memory
  execution_role_arn         = data.aws_iam_role.execution.arn
  task_role_arn              = data.aws_iam_role.task.arn
  ecr_repository_url         = data.aws_ecr_repository.etl.repository_url
  image_tag                  = var.image_tag
  aws_region                 = var.aws_region
  script_s3_uri              = var.script_s3_uri
  task_ephemeral_storage_gib = var.task_ephemeral_storage_gib
  log_level                  = var.log_level
  slack_webhook_secret_name  = var.slack_webhook_secret_name
  log_group_name             = "/ecs/${local.name_prefix}"
  tags                       = local.tags
}

module "iam_scheduler" {
  source = "./modules/iam_scheduler"

  name_prefix             = "${local.name_prefix}-${var.etl_job_name}"
  ecs_cluster_arn         = data.aws_ecs_cluster.etl.arn
  ecs_task_definition_arn = module.ecs_task_definition.task_definition_arn
  ecs_task_role_arn       = data.aws_iam_role.task.arn
  ecs_execution_role_arn  = data.aws_iam_role.execution.arn
  tags                    = local.tags
}

module "scheduler" {
  source = "./modules/scheduler"

  name_prefix                   = local.name_prefix
  etl_job_name                  = var.etl_job_name
  schedule_expression           = var.schedule_expression
  schedule_timezone             = var.schedule_timezone
  schedule_flexible_window_mode = var.schedule_flexible_window_mode
  ecs_cluster_arn               = data.aws_ecs_cluster.etl.arn
  ecs_task_definition_arn       = module.ecs_task_definition.task_definition_arn
  scheduler_role_arn            = module.iam_scheduler.role_arn
  subnet_ids                    = data.aws_subnets.default.ids
  security_group_id             = data.aws_security_group.etl_task.id
}
