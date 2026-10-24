resource "aws_s3_bucket" "raw" {
  bucket        = local.raw_bucket_name
  force_destroy = var.allow_s3_tables_force_destroy
  tags          = var.common_tags
}

resource "aws_s3_bucket" "output" {
  bucket        = local.output_bucket_name
  force_destroy = false
  tags          = var.common_tags
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "output" {
  bucket                  = aws_s3_bucket.output.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "output" {
  bucket = aws_s3_bucket.output.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "output" {
  bucket = aws_s3_bucket.output.id
  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

data "aws_iam_policy_document" "vendor_raw_read" {
  count = var.hardcharger_s3_principal_arn == "" ? 0 : 1
  statement {
    sid       = "GizmoDataReadRawObjects"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.raw.arn}/tpch/*"]
    principals {
      type        = "AWS"
      identifiers = [var.hardcharger_s3_principal_arn]
    }
  }
  statement {
    sid       = "GizmoDataListRawObjects"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.raw.arn]
    principals {
      type        = "AWS"
      identifiers = [var.hardcharger_s3_principal_arn]
    }
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["tpch", "tpch/*"]
    }
  }
}

resource "aws_s3_bucket_policy" "vendor_raw_read" {
  count  = var.hardcharger_s3_principal_arn == "" ? 0 : 1
  bucket = aws_s3_bucket.raw.id
  policy = data.aws_iam_policy_document.vendor_raw_read[0].json
}

resource "aws_s3tables_table_bucket" "analytics" {
  name          = local.tables_bucket_name
  force_destroy = false
  encryption_configuration = {
    kms_key_arn   = null
    sse_algorithm = "AES256"
  }
  tags = var.common_tags
}

resource "aws_s3tables_namespace" "analytics" {
  namespace        = "demo"
  table_bucket_arn = aws_s3tables_table_bucket.analytics.arn
}

resource "aws_s3tables_table" "customer_segment" {
  name             = "customer_segment"
  namespace        = aws_s3tables_namespace.analytics.namespace
  table_bucket_arn = aws_s3tables_table_bucket.analytics.arn
  format           = "ICEBERG"

  metadata {
    iceberg {
      schema {
        field {
          name     = "nationkey"
          type     = "int"
          required = true
        }
        field {
          name     = "segment"
          type     = "string"
          required = true
        }
      }
    }
  }
}
