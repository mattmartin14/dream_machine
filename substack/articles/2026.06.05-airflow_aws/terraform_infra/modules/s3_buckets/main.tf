resource "aws_s3_bucket" "source" {
  bucket        = var.source_bucket_name
  force_destroy = true
  tags          = var.tags
}

resource "aws_s3_bucket" "target" {
  bucket        = var.target_bucket_name
  force_destroy = true
  tags          = var.tags
}

resource "aws_s3_bucket_public_access_block" "source" {
  bucket = aws_s3_bucket.source.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "target" {
  bucket = aws_s3_bucket.target.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "source" {
  bucket = aws_s3_bucket.source.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_versioning" "target" {
  bucket = aws_s3_bucket.target.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "source" {
  bucket = aws_s3_bucket.source.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "target" {
  bucket = aws_s3_bucket.target.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
