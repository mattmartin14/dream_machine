output "quack_public_ip" {
  description = "Stable Elastic IP for Quack clients."
  value       = aws_eip.quack.public_ip
}

output "quack_uri" {
  description = "Quack URI for a DuckLake ATTACH statement. Quack is plain HTTP; clients must use DISABLE_SSL true unless TLS is added separately."
  value       = "quack:${aws_eip.quack.public_ip}:${var.quack_port}"
}

output "ducklake_s3_uri" {
  description = "S3 URI to use as DuckLake DATA_PATH."
  value       = "s3://${aws_s3_bucket.ducklake_data.bucket}/"
}

output "ducklake_bucket_name" {
  description = "Name of the private S3 bucket holding DuckLake Parquet data."
  value       = aws_s3_bucket.ducklake_data.bucket
}

output "quack_token" {
  description = "Quack authentication token for approved POC clients."
  value       = var.quack_token
  sensitive   = true
}