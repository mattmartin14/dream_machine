data "aws_iam_policy_document" "scheduler_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "scheduler_invoke" {
  name               = "${var.name_prefix}-scheduler-role"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume_role.json
  tags               = var.tags
}

data "aws_iam_policy_document" "scheduler_invoke" {
  statement {
    sid    = "RunSpecificTaskDefinition"
    effect = "Allow"
    actions = [
      "ecs:RunTask"
    ]
    resources = [var.ecs_task_definition_arn]

    condition {
      test     = "ArnEquals"
      variable = "ecs:cluster"
      values   = [var.ecs_cluster_arn]
    }
  }

  statement {
    sid    = "PassOnlyEcsRoles"
    effect = "Allow"
    actions = [
      "iam:PassRole"
    ]
    resources = [
      var.ecs_task_role_arn,
      var.ecs_execution_role_arn
    ]

    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_policy" "scheduler_invoke" {
  name   = "${var.name_prefix}-scheduler-invoke"
  policy = data.aws_iam_policy_document.scheduler_invoke.json
  tags   = var.tags
}

resource "aws_iam_role_policy_attachment" "scheduler_invoke" {
  role       = aws_iam_role.scheduler_invoke.name
  policy_arn = aws_iam_policy.scheduler_invoke.arn
}
