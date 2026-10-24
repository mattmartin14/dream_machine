resource "aws_ecr_repository" "etl" {
  name                 = "${local.name_prefix}-etl"
  image_tag_mutability = "IMMUTABLE"
  force_delete         = true
  image_scanning_configuration {
    scan_on_push = true
  }
  encryption_configuration {
    encryption_type = "AES256"
  }
  tags = var.common_tags
}

resource "aws_ecr_lifecycle_policy" "etl" {
  repository = aws_ecr_repository.etl.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep the most recent demonstration images."
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 10
      }
      action = { type = "expire" }
    }]
  })
}
