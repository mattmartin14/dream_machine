data "aws_iam_policy_document" "task_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "task" {
  name               = "${local.name_prefix}-task"
  assume_role_policy = data.aws_iam_policy_document.task_assume.json
  tags               = var.common_tags
}

resource "aws_iam_role" "task_execution" {
  name               = "${local.name_prefix}-execution"
  assume_role_policy = data.aws_iam_policy_document.task_assume.json
  tags               = var.common_tags
}

data "aws_iam_policy_document" "task_execution" {
  statement {
    sid       = "EcrAuthorization"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }
  statement {
    sid = "PullProjectImage"
    actions = [
      "ecr:BatchCheckLayerAvailability",
      "ecr:GetDownloadUrlForLayer",
      "ecr:BatchGetImage"
    ]
    resources = [aws_ecr_repository.etl.arn]
  }
  statement {
    sid = "WriteTaskLogs"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["${aws_cloudwatch_log_group.etl.arn}:*"]
  }
}

resource "aws_iam_role_policy" "task_execution" {
  name   = "${local.name_prefix}-execution-policy"
  role   = aws_iam_role.task_execution.id
  policy = data.aws_iam_policy_document.task_execution.json
}

data "aws_iam_policy_document" "task_access" {
  statement {
    sid       = "ReadRawTpch"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.raw.arn}/tpch/*"]
  }
  statement {
    sid       = "ListRawTpch"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.raw.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["tpch", "tpch/*"]
    }
  }
  statement {
    sid       = "ReadConnectionSecret"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.hardcharger.arn]
  }
  statement {
    sid       = "PublishDemoMetrics"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["HardchargerDemo"]
    }
  }
  statement {
    sid       = "AcquireAndReleaseEtlLock"
    actions   = ["dynamodb:PutItem", "dynamodb:DeleteItem"]
    resources = [aws_dynamodb_table.etl_lock.arn]
  }
}

resource "aws_iam_role_policy" "task_access" {
  name   = "${local.name_prefix}-task-access"
  role   = aws_iam_role.task.id
  policy = data.aws_iam_policy_document.task_access.json
}

data "aws_iam_policy_document" "scheduler_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  name               = "${local.name_prefix}-scheduler"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume.json
  tags               = var.common_tags
}

data "aws_iam_policy_document" "scheduler_run_task" {
  statement {
    actions   = ["ecs:RunTask"]
    resources = [aws_ecs_task_definition.etl.arn]
    condition {
      test     = "ArnEquals"
      variable = "ecs:cluster"
      values   = [aws_ecs_cluster.demo.arn]
    }
  }
  statement {
    actions = ["iam:PassRole"]
    resources = [
      aws_iam_role.task.arn,
      aws_iam_role.task_execution.arn
    ]
    condition {
      test     = "StringLike"
      variable = "iam:PassedToService"
      values   = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy" "scheduler_run_task" {
  name   = "${local.name_prefix}-scheduler-run-task"
  role   = aws_iam_role.scheduler.id
  policy = data.aws_iam_policy_document.scheduler_run_task.json
}
