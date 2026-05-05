output "bucket_name" {
  value = aws_s3_bucket.etl.bucket
}

output "bucket_arn" {
  value = aws_s3_bucket.etl.arn
}
