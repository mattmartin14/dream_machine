terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"]

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-arm64-server-*"]
  }

  filter {
    name   = "architecture"
    values = ["arm64"]
  }
}

resource "aws_s3_bucket" "ducklake_data" {
  bucket        = var.ducklake_bucket_name
  force_destroy = var.force_destroy_ducklake_bucket

  tags = merge(var.tags, {
    Name = "${var.name}-ducklake-data"
  })
}

resource "aws_s3_bucket_public_access_block" "ducklake_data" {
  bucket = aws_s3_bucket.ducklake_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_security_group" "quack" {
  name_prefix = "${var.name}-"
  description = "Restricted access to the Quack DuckDB remote server"
  vpc_id      = var.vpc_id

  ingress {
    description = "SSH from approved client networks"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
  }

  ingress {
    description = "Quack HTTP from approved client networks"
    from_port   = var.quack_port
    to_port     = var.quack_port
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
  }

  egress {
    description = "Outbound access for package and DuckDB extension downloads"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.name}-quack"
  })
}

resource "aws_key_pair" "quack" {
  key_name   = var.key_pair_name
  public_key = file(pathexpand(var.public_key_path))

  tags = merge(var.tags, {
    Name = var.key_pair_name
  })
}

resource "aws_iam_role" "quack" {
  name_prefix = "${var.name}-quack-"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "ducklake_data" {
  name_prefix = "${var.name}-ducklake-data-"
  role        = aws_iam_role.quack.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:ListBucket"]
      Resource = [aws_s3_bucket.ducklake_data.arn]
      }, {
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
      Resource = ["${aws_s3_bucket.ducklake_data.arn}/*"]
    }]
  })
}

resource "aws_iam_instance_profile" "quack" {
  name_prefix = "${var.name}-quack-"
  role        = aws_iam_role.quack.name
}

resource "aws_instance" "quack" {
  ami                         = var.ami_id != null ? var.ami_id : data.aws_ami.ubuntu.id
  instance_type               = var.instance_type
  key_name                    = aws_key_pair.quack.key_name
  subnet_id                   = var.subnet_id
  vpc_security_group_ids      = [aws_security_group.quack.id]
  iam_instance_profile        = aws_iam_instance_profile.quack.name
  associate_public_ip_address = true

  root_block_device {
    volume_type           = "gp3"
    volume_size           = var.root_volume_size_gb
    delete_on_termination = var.delete_root_volume_on_termination
    encrypted             = true
  }

  user_data = templatefile("${path.module}/templates/bootstrap.sh.tftpl", {
    aws_region         = var.aws_region
    ducklake_data_path = "s3://${aws_s3_bucket.ducklake_data.bucket}/data/"
    quack_port         = var.quack_port
    quack_token        = var.quack_token
  })

  user_data_replace_on_change = false

  tags = merge(var.tags, {
    Name = var.name
  })
}

resource "aws_eip" "quack" {
  domain = "vpc"

  tags = merge(var.tags, {
    Name = "${var.name}-eip"
  })
}

resource "aws_eip_association" "quack" {
  allocation_id = aws_eip.quack.id
  instance_id   = aws_instance.quack.id
}