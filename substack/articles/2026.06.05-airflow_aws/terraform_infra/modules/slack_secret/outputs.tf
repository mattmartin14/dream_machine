output "secret_name" {
  value = aws_secretsmanager_secret.slack_webhook.name
}

output "secret_arn" {
  value = aws_secretsmanager_secret.slack_webhook.arn
}
