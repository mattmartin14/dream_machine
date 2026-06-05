data "aws_caller_identity" "current" {}

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

module "s3_buckets" {
  source = "./modules/s3_buckets"

  source_bucket_name = var.s3_source_bucket_name
  target_bucket_name = var.s3_target_bucket_name
  tags               = local.tags
}

module "slack_secret" {
  source = "./modules/slack_secret"

  secret_name   = var.slack_webhook_secret_name
  slack_webhook = var.slack_webhook
  tags          = local.tags
}

module "iam_ecs" {
  source = "./modules/iam_ecs"

  name_prefix                     = local.name_prefix
  s3_source_bucket_arn            = module.s3_buckets.source_bucket_arn
  s3_target_bucket_arn            = module.s3_buckets.target_bucket_arn
  s3_script_bucket_arn            = module.s3_buckets.target_bucket_arn
  runtime_script_allowed_prefixes = var.runtime_script_allowed_prefixes
  slack_webhook_secret_arn        = module.slack_secret.secret_arn
  normalized_input_prefix         = local.normalized_input_prefix
  normalized_output_prefix        = local.normalized_output_prefix
  tags                            = local.tags
}

module "ecs_cluster" {
  source = "./modules/ecs_cluster"

  name_prefix = local.name_prefix
  tags        = local.tags
}
