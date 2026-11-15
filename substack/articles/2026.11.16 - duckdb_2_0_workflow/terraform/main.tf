data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }

  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

resource "aws_db_subnet_group" "catalog" {
  name       = "ducklake-demo"
  subnet_ids = data.aws_subnets.default.ids
}

resource "aws_security_group" "catalog" {
  name        = "ducklake-demo-postgres"
  description = "PostgreSQL access from the demo client only"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
  }
}

resource "aws_rds_cluster" "catalog" {
  cluster_identifier     = "ducklake-demo"
  engine                 = "aurora-postgresql"
  engine_mode            = "provisioned"
  database_name          = "ducklake_catalog"
  master_username        = var.aurora_admin_user
  master_password        = var.aurora_admin_pwd
  db_subnet_group_name   = aws_db_subnet_group.catalog.name
  vpc_security_group_ids = [aws_security_group.catalog.id]
  storage_encrypted      = true
  skip_final_snapshot    = true

  serverlessv2_scaling_configuration {
    min_capacity             = 0
    max_capacity             = 1
    seconds_until_auto_pause = 300
  }
}

resource "aws_rds_cluster_instance" "catalog" {
  identifier           = "ducklake-demo-writer"
  cluster_identifier   = aws_rds_cluster.catalog.id
  instance_class       = "db.serverless"
  engine               = aws_rds_cluster.catalog.engine
  engine_version       = aws_rds_cluster.catalog.engine_version
  publicly_accessible  = true
  db_subnet_group_name = aws_db_subnet_group.catalog.name
}
