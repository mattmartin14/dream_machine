output "aurora_host" {
  description = "Aurora PostgreSQL cluster writer endpoint."
  value       = aws_rds_cluster.catalog.endpoint
}
