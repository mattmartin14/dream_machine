output "source_bucket_name" {
  value = aws_s3_bucket.source.bucket
}

output "target_bucket_name" {
  value = aws_s3_bucket.target.bucket
}

output "source_bucket_arn" {
  value = aws_s3_bucket.source.arn
}

output "target_bucket_arn" {
  value = aws_s3_bucket.target.arn
}
