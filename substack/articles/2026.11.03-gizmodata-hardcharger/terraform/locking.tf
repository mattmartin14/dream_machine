resource "aws_dynamodb_table" "etl_lock" {
  name         = local.lock_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "lock_key"

  attribute {
    name = "lock_key"
    type = "S"
  }

  server_side_encryption {
    enabled = true
  }

  tags = var.common_tags
}
