data "aws_caller_identity" "current" {}

resource "aws_s3_object" "sales_etl_script" {
  bucket       = var.s3_script_bucket_name
  key          = var.s3_script_key
  source       = "${path.module}/../scripts/sales_etl.py"
  etag         = filemd5("${path.module}/../scripts/sales_etl.py")
  content_type = "text/x-python"
}

module "ecr" {
  source = "./modules/ecr"

  name_prefix = local.name_prefix
  tags        = local.tags
}

module "network" {
  source = "./modules/network"

  name_prefix = local.name_prefix
  tags        = local.tags
}

module "iam_ecs" {
  source = "./modules/iam_ecs"

  name_prefix                     = local.name_prefix
  s3_source_bucket_arn            = "arn:aws:s3:::${var.s3_source_bucket_name}"
  s3_target_bucket_arn            = "arn:aws:s3:::${var.s3_target_bucket_name}"
  s3_script_bucket_arn            = "arn:aws:s3:::${var.s3_script_bucket_name}"
  runtime_script_allowed_prefixes = var.runtime_script_allowed_prefixes
  slack_webhook_secret_arn        = "arn:aws:secretsmanager:${var.aws_region}:${data.aws_caller_identity.current.account_id}:secret:${var.slack_webhook_secret_name}*"
  normalized_input_prefix         = local.normalized_input_prefix
  normalized_output_prefix        = local.normalized_output_prefix
  tags                            = local.tags
}

module "ecs" {
  source = "./modules/ecs"

  name_prefix                = local.name_prefix
  app_name                   = var.app_name
  container_cpu              = var.container_cpu
  container_memory           = var.container_memory
  execution_role_arn         = module.iam_ecs.execution_role_arn
  task_role_arn              = module.iam_ecs.task_role_arn
  ecr_repository_url         = module.ecr.repository_url
  image_tag                  = var.image_tag
  aws_region                 = var.aws_region
  default_script_s3_uri      = var.scheduled_script_s3_uri
  task_ephemeral_storage_gib = var.task_ephemeral_storage_gib
  log_level                  = var.log_level
  slack_webhook_secret_name  = var.slack_webhook_secret_name
  tags                       = local.tags
}

module "iam_scheduler" {
  source = "./modules/iam_scheduler"

  name_prefix             = local.name_prefix
  ecs_cluster_arn         = module.ecs.cluster_arn
  ecs_task_definition_arn = module.ecs.task_definition_arn
  ecs_task_role_arn       = module.iam_ecs.task_role_arn
  ecs_execution_role_arn  = module.iam_ecs.execution_role_arn
  tags                    = local.tags
}

module "scheduler" {
  source = "./modules/scheduler"

  name_prefix                   = local.name_prefix
  app_name                      = var.app_name
  schedule_expression           = var.schedule_expression
  schedule_timezone             = var.schedule_timezone
  schedule_flexible_window_mode = var.schedule_flexible_window_mode
  ecs_cluster_arn               = module.ecs.cluster_arn
  ecs_task_definition_arn       = module.ecs.task_definition_arn
  scheduler_role_arn            = module.iam_scheduler.role_arn
  subnet_ids                    = module.network.subnet_ids
  security_group_id             = module.network.security_group_id
}
