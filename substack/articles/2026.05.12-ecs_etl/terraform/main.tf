data "aws_caller_identity" "current" {}

module "s3" {
  source = "./modules/s3"

  name_prefix = local.name_prefix
  app_name    = var.app_name
  bucket_name = var.s3_bucket_name
  tags        = local.tags
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

  name_prefix              = local.name_prefix
  s3_bucket_arn            = module.s3.bucket_arn
  s3_script_key            = var.s3_script_key
  normalized_input_prefix  = local.normalized_input_prefix
  normalized_output_prefix = local.normalized_output_prefix
  tags                     = local.tags
}

module "ecs" {
  source = "./modules/ecs"

  name_prefix              = local.name_prefix
  app_name                 = var.app_name
  container_cpu            = var.container_cpu
  container_memory         = var.container_memory
  execution_role_arn       = module.iam_ecs.execution_role_arn
  task_role_arn            = module.iam_ecs.task_role_arn
  ecr_repository_url       = module.ecr.repository_url
  image_tag                = var.image_tag
  aws_region               = var.aws_region
  s3_bucket_name           = module.s3.bucket_name
  s3_script_bucket_name    = module.s3.bucket_name
  s3_script_key            = var.s3_script_key
  normalized_input_prefix  = local.normalized_input_prefix
  normalized_output_prefix = local.normalized_output_prefix
  log_level                = var.log_level
  tags                     = local.tags
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
  runtime_s3_bucket_name        = module.s3.bucket_name
  subnet_ids                    = module.network.subnet_ids
  security_group_id             = module.network.security_group_id
}
