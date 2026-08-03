data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_ssm_parameter" "amazon_linux_2023" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

resource "time_offset" "expiry" {
  offset_hours = var.ttl_hours
}

locals {
  availability_zone = var.availability_zone != "" ? var.availability_zone : data.aws_availability_zones.available.names[0]
  name              = "${var.name_prefix}-${terraform.workspace}"

  common_tags = merge(
    {
      Campaign    = terraform.workspace
      Environment = "aws-smoke"
      ExpiresAt   = time_offset.expiry.rfc3339
      ManagedBy   = "terraform"
      Owner       = var.owner
      Project     = "redis-event-stream-module"
      Repository  = "joshrotenberg/redis-event-stream-module"
    },
    var.extra_tags,
  )

  bootstrap_server = <<-USER_DATA
    #!/usr/bin/env bash
    set -euxo pipefail
    dnf install -y docker jq perf
    systemctl enable --now docker
    systemctl enable --now amazon-ssm-agent
    docker pull '${var.module_image}'
    install -d -m 0755 /var/lib/eventstream-smoke
    touch /var/lib/eventstream-smoke/ready
  USER_DATA

  bootstrap_loadgen = <<-USER_DATA
    #!/usr/bin/env bash
    set -euxo pipefail
    dnf install -y docker jq
    systemctl enable --now docker
    systemctl enable --now amazon-ssm-agent
    docker pull '${var.loadgen_image}'
    install -d -m 0755 /var/lib/eventstream-smoke
    touch /var/lib/eventstream-smoke/ready
  USER_DATA
}

resource "aws_vpc" "lab" {
  cidr_block           = "10.87.0.0/24"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = merge(local.common_tags, {
    Name = "${local.name}-vpc"
  })
}

resource "aws_internet_gateway" "lab" {
  vpc_id = aws_vpc.lab.id

  tags = merge(local.common_tags, {
    Name = "${local.name}-igw"
  })
}

resource "aws_subnet" "lab" {
  vpc_id                  = aws_vpc.lab.id
  availability_zone       = local.availability_zone
  cidr_block              = "10.87.0.0/25"
  map_public_ip_on_launch = true

  tags = merge(local.common_tags, {
    Name = "${local.name}-subnet"
  })
}

resource "aws_route_table" "lab" {
  vpc_id = aws_vpc.lab.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.lab.id
  }

  tags = merge(local.common_tags, {
    Name = "${local.name}-routes"
  })
}

resource "aws_route_table_association" "lab" {
  route_table_id = aws_route_table.lab.id
  subnet_id      = aws_subnet.lab.id
}

resource "aws_security_group" "server" {
  name_prefix = "${local.name}-server-"
  description = "Redis ingress only from the smoke load generator"
  vpc_id      = aws_vpc.lab.id

  tags = merge(local.common_tags, {
    Name = "${local.name}-server"
    Role = "server"
  })

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group" "loadgen" {
  name_prefix = "${local.name}-loadgen-"
  description = "No inbound access; outbound benchmark and SSM traffic only"
  vpc_id      = aws_vpc.lab.id

  tags = merge(local.common_tags, {
    Name = "${local.name}-loadgen"
    Role = "load-generator"
  })

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_vpc_security_group_ingress_rule" "redis_from_loadgen" {
  description                  = "Redis from the dedicated load generator"
  from_port                    = 6379
  ip_protocol                  = "tcp"
  referenced_security_group_id = aws_security_group.loadgen.id
  security_group_id            = aws_security_group.server.id
  to_port                      = 6379
  tags                         = local.common_tags
}

resource "aws_vpc_security_group_egress_rule" "server" {
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
  security_group_id = aws_security_group.server.id
  tags              = local.common_tags
}

resource "aws_vpc_security_group_egress_rule" "loadgen" {
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
  security_group_id = aws_security_group.loadgen.id
  tags              = local.common_tags
}

resource "aws_iam_role" "ssm" {
  name_prefix = "${local.name}-ssm-"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      },
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "ssm" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
  role       = aws_iam_role.ssm.name
}

resource "aws_iam_instance_profile" "ssm" {
  name_prefix = "${local.name}-ssm-"
  role        = aws_iam_role.ssm.name

  tags = local.common_tags
}

resource "aws_instance" "server" {
  ami                         = data.aws_ssm_parameter.amazon_linux_2023.value
  associate_public_ip_address = true
  availability_zone           = local.availability_zone
  iam_instance_profile        = aws_iam_instance_profile.ssm.name
  instance_type               = var.server_instance_type
  subnet_id                   = aws_subnet.lab.id
  user_data                   = local.bootstrap_server
  user_data_replace_on_change = true
  vpc_security_group_ids      = [aws_security_group.server.id]

  metadata_options {
    http_endpoint = "enabled"
    http_tokens   = "required"
  }

  root_block_device {
    delete_on_termination = true
    encrypted             = true
    volume_size           = var.root_volume_gib
    volume_type           = "gp3"
    tags                  = local.common_tags
  }

  tags = merge(local.common_tags, {
    Name = "${local.name}-server"
    Role = "server"
  })
}

resource "aws_instance" "loadgen" {
  ami                         = data.aws_ssm_parameter.amazon_linux_2023.value
  associate_public_ip_address = true
  availability_zone           = local.availability_zone
  iam_instance_profile        = aws_iam_instance_profile.ssm.name
  instance_type               = var.loadgen_instance_type
  subnet_id                   = aws_subnet.lab.id
  user_data                   = local.bootstrap_loadgen
  user_data_replace_on_change = true
  vpc_security_group_ids      = [aws_security_group.loadgen.id]

  metadata_options {
    http_endpoint = "enabled"
    http_tokens   = "required"
  }

  root_block_device {
    delete_on_termination = true
    encrypted             = true
    volume_size           = var.root_volume_gib
    volume_type           = "gp3"
    tags                  = local.common_tags
  }

  tags = merge(local.common_tags, {
    Name = "${local.name}-loadgen"
    Role = "load-generator"
  })
}

# A controller interruption must not leave billable compute running until a
# human notices the expiry tag. EventBridge Scheduler invokes the EC2
# StopInstances API at ExpiresAt using a role restricted to these two instance
# ARNs. Terraform destroy remains the resource-cleanup path; this is the hard
# runtime backstop for the dominant compute and public-IPv4 charges.
resource "aws_iam_role" "expiry_stop" {
  name_prefix = "${local.name}-expiry-stop-"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "scheduler.amazonaws.com"
        }
      },
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy" "expiry_stop" {
  name_prefix = "stop-lab-instances-"
  role        = aws_iam_role.expiry_stop.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "ec2:StopInstances"
        Effect = "Allow"
        Resource = [
          aws_instance.server.arn,
          aws_instance.loadgen.arn,
        ]
      },
    ]
  })
}

resource "aws_scheduler_schedule_group" "lab" {
  name = "${local.name}-expiry"
  tags = local.common_tags
}

resource "aws_scheduler_schedule" "expiry_stop" {
  name                         = "${local.name}-expiry-stop"
  description                  = "Hard-stop the disposable benchmark hosts at ExpiresAt"
  schedule_expression          = "at(${formatdate("YYYY-MM-DD'T'hh:mm:ss", time_offset.expiry.rfc3339)})"
  schedule_expression_timezone = "UTC"
  action_after_completion      = "DELETE"
  group_name                   = aws_scheduler_schedule_group.lab.name
  depends_on                   = [aws_iam_role_policy.expiry_stop]

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = "arn:aws:scheduler:::aws-sdk:ec2:stopInstances"
    role_arn = aws_iam_role.expiry_stop.arn
    input = jsonencode({
      InstanceIds = [
        aws_instance.server.id,
        aws_instance.loadgen.id,
      ]
    })

    retry_policy {
      maximum_event_age_in_seconds = 3600
      maximum_retry_attempts       = 3
    }
  }
}
