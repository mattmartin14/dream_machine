data "aws_caller_identity" "current" {}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

locals {
  common_tags = merge(
    {
      Project   = var.project_name
      ManagedBy = "terraform"
      Benchmark = "duckdb-vs-spark"
    },
    var.tags,
  )

  bucket_arn         = "arn:aws:s3:::${var.bucket_name}"
  bucket_objects     = "arn:aws:s3:::${var.bucket_name}/*"
  root_objects_arn   = "arn:aws:s3:::${var.bucket_name}/${var.root_prefix}/*"
  script_objects_arn = "arn:aws:s3:::${var.bucket_name}/${var.script_prefix}/*"
  emr_log_uri        = "s3://${var.bucket_name}/${var.root_prefix}/logs/emr-serverless/"
  emr_log_group_arn  = "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/emr-serverless/${var.project_name}"
}

resource "aws_ecr_repository" "duckdb_runner" {
  name                 = var.project_name
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = local.common_tags
}

resource "aws_cloudwatch_log_group" "ecs" {
  name              = "/aws/ecs/${var.project_name}"
  retention_in_days = 14
  tags              = local.common_tags
}

resource "aws_cloudwatch_log_group" "emr" {
  name              = "/aws/emr-serverless/${var.project_name}"
  retention_in_days = 14
  tags              = local.common_tags
}

resource "aws_ecs_cluster" "benchmark" {
  name = var.project_name
  tags = local.common_tags
}

resource "aws_security_group" "ecs_task" {
  name_prefix = "${var.project_name}-ecs-"
  description = "Egress-only security group for DuckDB benchmark tasks"
  vpc_id      = data.aws_vpc.default.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.common_tags
}

data "aws_iam_policy_document" "ecs_task_execution_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ecs_task_execution" {
  name               = "${var.project_name}-ecs-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_execution_assume_role.json
  tags               = local.common_tags
}

resource "aws_iam_role_policy_attachment" "ecs_task_execution_managed" {
  role       = aws_iam_role.ecs_task_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

data "aws_iam_policy_document" "ecs_task_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ecs_task" {
  name               = "${var.project_name}-ecs-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_assume_role.json
  tags               = local.common_tags
}

data "aws_iam_policy_document" "ecs_task_access" {
  statement {
    sid = "ListBenchmarkBucket"

    actions = [
      "s3:ListBucket"
    ]

    resources = [local.bucket_arn]
  }

  statement {
    sid = "ReadWriteBenchmarkObjects"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject"
    ]

    resources = [local.root_objects_arn]
  }
}

resource "aws_iam_policy" "ecs_task_access" {
  name   = "${var.project_name}-ecs-task-access"
  policy = data.aws_iam_policy_document.ecs_task_access.json
  tags   = local.common_tags
}

resource "aws_iam_role_policy_attachment" "ecs_task_access" {
  role       = aws_iam_role.ecs_task.name
  policy_arn = aws_iam_policy.ecs_task_access.arn
}

data "aws_iam_policy_document" "emr_runtime_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_iam_role" "emr_runtime" {
  name               = "${var.project_name}-emr-runtime"
  assume_role_policy = data.aws_iam_policy_document.emr_runtime_assume_role.json
  tags               = local.common_tags
}

data "aws_iam_policy_document" "emr_runtime_access" {
  statement {
    sid = "ListBenchmarkBucket"

    actions = [
      "s3:ListBucket"
    ]

    resources = [local.bucket_arn]
  }

  statement {
    sid = "ReadWriteBenchmarkData"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject"
    ]

    resources = [
      local.root_objects_arn,
      local.script_objects_arn,
    ]
  }

  statement {
    sid = "AllowEmrServerlessCloudWatchDiscovery"

    actions = [
      "logs:DescribeLogGroups",
      "logs:DescribeLogStreams"
    ]

    resources = ["*"]
  }

  statement {
    sid = "AllowEmrServerlessCloudWatchPublish"

    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]

    resources = [
      "${local.emr_log_group_arn}:*"
    ]
  }
}

resource "aws_iam_policy" "emr_runtime_access" {
  name   = "${var.project_name}-emr-runtime-access"
  policy = data.aws_iam_policy_document.emr_runtime_access.json
  tags   = local.common_tags
}

resource "aws_iam_role_policy_attachment" "emr_runtime_access" {
  role       = aws_iam_role.emr_runtime.name
  policy_arn = aws_iam_policy.emr_runtime_access.arn
}

resource "aws_ecs_task_definition" "duckdb" {
  family                   = var.project_name
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = tostring(var.ecs_task_cpu)
  memory                   = tostring(var.ecs_task_memory)
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  container_definitions = jsonencode([
    {
      name      = "duckdb-benchmark"
      image     = var.duckdb_image_uri
      essential = true
      cpu       = var.ecs_task_cpu
      memory    = var.ecs_task_memory
      environment = [
        { name = "AWS_DEFAULT_REGION", value = var.aws_region }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.ecs.name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = "duckdb"
        }
      }
    }
  ])

  tags = local.common_tags
}

resource "aws_emrserverless_application" "spark" {
  name          = var.project_name
  release_label = var.emr_release_label
  type          = "spark"
  architecture  = "X86_64"

  auto_start_configuration {
    enabled = true
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = var.emr_idle_timeout_minutes
  }

  maximum_capacity {
    cpu    = var.emr_maximum_cpu
    memory = var.emr_maximum_memory
  }

  monitoring_configuration {
    cloudwatch_logging_configuration {
      enabled                = true
      log_group_name         = aws_cloudwatch_log_group.emr.name
      log_stream_name_prefix = "spark"
    }

    s3_monitoring_configuration {
      log_uri = local.emr_log_uri
    }

    managed_persistence_monitoring_configuration {
      enabled = true
    }
  }

  tags = local.common_tags
}