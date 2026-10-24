resource "aws_vpc" "demo" {
  count                = local.use_existing_vpc ? 0 : 1
  cidr_block           = "10.73.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = merge(var.common_tags, { Name = "${local.name_prefix}-vpc" })
}

resource "aws_internet_gateway" "demo" {
  count  = local.use_existing_vpc ? 0 : 1
  vpc_id = aws_vpc.demo[0].id
  tags   = var.common_tags
}

resource "aws_subnet" "public" {
  count                   = local.use_existing_vpc ? 0 : 2
  vpc_id                  = aws_vpc.demo[0].id
  cidr_block              = cidrsubnet(aws_vpc.demo[0].cidr_block, 8, count.index + 1)
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = true
  tags                    = merge(var.common_tags, { Name = "${local.name_prefix}-public-${count.index + 1}" })
}

resource "aws_route_table" "public" {
  count  = local.use_existing_vpc ? 0 : 1
  vpc_id = aws_vpc.demo[0].id
  tags   = var.common_tags
}

resource "aws_route" "internet" {
  count                  = local.use_existing_vpc ? 0 : 1
  route_table_id         = aws_route_table.public[0].id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.demo[0].id
}

resource "aws_route_table_association" "public" {
  count          = local.use_existing_vpc ? 0 : 2
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public[0].id
}

resource "aws_vpc_endpoint" "s3" {
  count             = local.use_existing_vpc ? 0 : 1
  vpc_id            = aws_vpc.demo[0].id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.public[0].id]
  tags              = var.common_tags
}

check "existing_subnets" {
  assert {
    condition     = !local.use_existing_vpc || length(var.existing_subnet_ids) >= 2
    error_message = "existing_subnet_ids must contain at least two subnets when existing_vpc_id is set."
  }
}
