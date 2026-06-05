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

module "ecs_runner_task_definition" {
  source = "./modules/ecs_runner_task_definition"

  for_each = var.runner_size_classes

  name_prefix                = local.name_prefix
  runner_size                = each.key
  container_cpu              = each.value.cpu
  container_memory           = each.value.memory
  task_ephemeral_storage_gib = each.value.ephemeral_storage_gib
  execution_role_arn         = data.aws_iam_role.execution.arn
  task_role_arn              = data.aws_iam_role.task.arn
  ecr_repository_url         = data.aws_ecr_repository.etl.repository_url
  image_tag                  = var.image_tag
  aws_region                 = var.aws_region
  default_script_s3_uri      = var.default_script_s3_uri
  log_level                  = var.log_level
  slack_webhook_secret_name  = var.slack_webhook_secret_name
  log_group_name             = "/ecs/${local.name_prefix}"
  tags                       = local.tags
}

data "aws_iam_policy_document" "airflow_runner" {
  statement {
    sid = "RunAndInspectDuckdbRunnerTasks"
    actions = [
      "ecs:RunTask",
      "ecs:DescribeTasks",
      "ecs:DescribeTaskDefinition",
      "ecs:ListTasks",
    ]

    resources = concat(
      [data.aws_ecs_cluster.etl.arn],
      [for task_definition in module.ecs_runner_task_definition : task_definition.task_definition_arn],
    )
  }

  statement {
    sid = "PassEcsTaskRoles"
    actions = [
      "iam:PassRole",
    ]

    resources = [
      data.aws_iam_role.task.arn,
      data.aws_iam_role.execution.arn,
    ]
  }

  statement {
    sid = "ReadEcsTaskLogs"
    actions = [
      "logs:GetLogEvents",
      "logs:DescribeLogStreams",
    ]

    resources = ["*"]
  }
}

resource "aws_iam_policy" "airflow_runner" {
  count = var.create_airflow_runner_policy ? 1 : 0

  name        = "${local.name_prefix}-local-airflow-runner"
  description = "Permissions for local Airflow to launch ECS DuckDB runner tasks."
  policy      = data.aws_iam_policy_document.airflow_runner.json
  tags        = local.tags
}
