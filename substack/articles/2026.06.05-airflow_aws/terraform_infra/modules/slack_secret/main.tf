resource "aws_secretsmanager_secret" "slack_webhook" {
  name = var.secret_name
  tags = var.tags
}

resource "aws_secretsmanager_secret_version" "slack_webhook" {
  secret_id = aws_secretsmanager_secret.slack_webhook.id
  secret_string = jsonencode({
    slack_webhook = var.slack_webhook
  })
}
