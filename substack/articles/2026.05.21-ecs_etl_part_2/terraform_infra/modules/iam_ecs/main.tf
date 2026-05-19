data "aws_iam_policy_document" "ecs_task_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "ecs_task_execution" {
  name               = "${var.name_prefix}-execution-role"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_assume_role.json
  tags               = var.tags
}

resource "aws_iam_role_policy_attachment" "ecs_task_execution_managed" {
  role       = aws_iam_role.ecs_task_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "ecs_task" {
  name               = "${var.name_prefix}-task-role"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_assume_role.json
  tags               = var.tags
}

data "aws_iam_policy_document" "ecs_task_s3" {
  statement {
    sid    = "ReadRuntimeScript"
    effect = "Allow"
    actions = [
      "s3:GetObject"
    ]
    resources = [for prefix in var.runtime_script_allowed_prefixes : "${var.s3_script_bucket_arn}/${trim(prefix, "/")}/*"]
  }

  statement {
    sid    = "ReadSlackWebhookSecret"
    effect = "Allow"
    actions = [
      "secretsmanager:DescribeSecret",
      "secretsmanager:GetSecretValue"
    ]
    resources = [var.slack_webhook_secret_arn]
  }

  statement {
    sid    = "ListInputPrefix"
    effect = "Allow"
    actions = [
      "s3:ListBucket"
    ]
    resources = [var.s3_source_bucket_arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values = [
        "${var.normalized_input_prefix}/*",
        var.normalized_input_prefix
      ]
    }
  }

  statement {
    sid    = "ReadInputObjects"
    effect = "Allow"
    actions = [
      "s3:GetObject"
    ]
    resources = ["${var.s3_source_bucket_arn}/${var.normalized_input_prefix}/*"]
  }

  statement {
    sid    = "ListOutputPrefix"
    effect = "Allow"
    actions = [
      "s3:ListBucket"
    ]
    resources = [var.s3_target_bucket_arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values = [
        "${var.normalized_output_prefix}/*",
        var.normalized_output_prefix
      ]
    }
  }

  statement {
    sid    = "ReadWriteOutputObjects"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListBucketMultipartUploads"
    ]
    resources = ["${var.s3_target_bucket_arn}/${var.normalized_output_prefix}/*"]
  }
}

resource "aws_iam_policy" "ecs_task_s3" {
  name   = "${var.name_prefix}-task-s3"
  policy = data.aws_iam_policy_document.ecs_task_s3.json
  tags   = var.tags
}

resource "aws_iam_role_policy_attachment" "ecs_task_s3" {
  role       = aws_iam_role.ecs_task.name
  policy_arn = aws_iam_policy.ecs_task_s3.arn
}
