resource "random_string" "bucket_suffix" {
  count = var.bucket_name == "" ? 1 : 0

  length  = 6
  lower   = true
  upper   = false
  numeric = true
  special = false
}

locals {
  effective_bucket_name = var.bucket_name != "" ? var.bucket_name : "${var.name_prefix}-${var.app_name}-${random_string.bucket_suffix[0].result}"
}

resource "aws_s3_bucket" "etl" {
  bucket = local.effective_bucket_name
  tags   = var.tags
}

resource "aws_s3_bucket_public_access_block" "etl" {
  bucket                  = aws_s3_bucket.etl.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "etl" {
  bucket = aws_s3_bucket.etl.id

  versioning_configuration {
    status = "Enabled"
  }
}
