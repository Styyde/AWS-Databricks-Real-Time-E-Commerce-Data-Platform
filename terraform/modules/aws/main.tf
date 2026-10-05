# -----------------------------------------------------------------------------
# PROVIDER AWS (hérité)
# -----------------------------------------------------------------------------
data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_caller_identity" "current" {}

# -----------------------------------------------------------------------------
# VPC
# -----------------------------------------------------------------------------
resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = "${var.project_name}-vpc"
  }
}

resource "aws_subnet" "public" {
  count                   = 2
  vpc_id                  = aws_vpc.main.id
  cidr_block              = cidrsubnet(aws_vpc.main.cidr_block, 8, count.index)
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = true

  tags = {
    Name = "${var.project_name}-public-${count.index}"
  }
}

resource "aws_subnet" "private" {
  count             = 2
  vpc_id            = aws_vpc.main.id
  cidr_block        = cidrsubnet(aws_vpc.main.cidr_block, 8, count.index + 2)
  availability_zone = data.aws_availability_zones.available.names[count.index]

  tags = {
    Name = "${var.project_name}-private-${count.index}"
  }
}

resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "${var.project_name}-igw"
  }
}

resource "aws_eip" "nat" {
  domain = "vpc"
}

resource "aws_nat_gateway" "ngw" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public[0].id

  tags = {
    Name = "${var.project_name}-natgw"
  }

  depends_on = [aws_internet_gateway.igw]
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.igw.id
  }

  tags = {
    Name = "${var.project_name}-rt-public"
  }
}

resource "aws_route_table_association" "public" {
  count          = 2
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.ngw.id
  }

  tags = {
    Name = "${var.project_name}-rt-private"
  }
}

resource "aws_route_table_association" "private" {
  count          = 2
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

# VPC Endpoint pour S3 (trafic privé entre Databricks et S3)
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  route_table_ids   = [aws_route_table.private.id]
  vpc_endpoint_type = "Gateway"

  tags = {
    Name = "${var.project_name}-vpce-s3"
  }
}

# -----------------------------------------------------------------------------
# SECURITY GROUP pour Databricks
# -----------------------------------------------------------------------------
resource "aws_security_group" "databricks" {
  name        = "${var.project_name}-databricks-sg"
  description = "Security group for Databricks workspace"
  vpc_id      = aws_vpc.main.id

  # Sortant : tout autoriser
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Entrant
  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 8443
    to_port     = 8443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 6666
    to_port     = 6666
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project_name}-databricks-sg"
  }
}

# -----------------------------------------------------------------------------
# S3 BUCKET - BRONZE (données brutes Kafka)
# -----------------------------------------------------------------------------
resource "aws_s3_bucket" "kafka_raw" {
  bucket        = "${var.project_name}-kafka-raw"
  force_destroy = true

  tags = {
    Name = "${var.project_name}-kafka-raw"
  }
}

resource "aws_s3_bucket_versioning" "kafka_raw" {
  bucket = aws_s3_bucket.kafka_raw.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "kafka_raw" {
  bucket = aws_s3_bucket.kafka_raw.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "kafka_raw" {
  bucket = aws_s3_bucket.kafka_raw.id

  rule {
    id     = "archive_after_30_days"
    status = "Enabled"

    # Uniquement les fichiers bruts du S3 Sink : les tables Delta (uc/, bronze_catalog/)
    # et les checkpoints doivent rester en stockage standard.
    filter {
      prefix = "topics/"
    }

    transition {
      days          = 30
      storage_class = "GLACIER"
    }

    expiration {
      days = 365
    }
  }
}

resource "aws_s3_bucket_public_access_block" "kafka_raw" {
  bucket = aws_s3_bucket.kafka_raw.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# -----------------------------------------------------------------------------
# S3 BUCKET - ARTIFACTS DATABRICKS (Root Storage)
# -----------------------------------------------------------------------------
resource "aws_s3_bucket" "databricks_artifacts" {
  bucket        = "${var.project_name}-databricks-artifacts"
  force_destroy = true

  tags = {
    Name = "${var.project_name}-databricks-artifacts"
  }
}

resource "aws_s3_bucket_versioning" "databricks_artifacts" {
  bucket = aws_s3_bucket.databricks_artifacts.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "databricks_artifacts" {
  bucket = aws_s3_bucket.databricks_artifacts.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "databricks_artifacts" {
  bucket = aws_s3_bucket.databricks_artifacts.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# -----------------------------------------------------------------------------
# BUCKET POLICY - OBLIGATOIRE pour Databricks Root Storage
# -----------------------------------------------------------------------------
resource "aws_s3_bucket_policy" "databricks_artifacts" {
  bucket = aws_s3_bucket.databricks_artifacts.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "GrantDatabricksAccess"
        Effect = "Allow"
        Principal = {
          AWS = [
            "arn:aws:iam::${var.databricks_aws_account_id}:root",
            aws_iam_role.databricks_cross_account.arn
          ]
        }
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.databricks_artifacts.arn,
          "${aws_s3_bucket.databricks_artifacts.arn}/*"
        ]
      },
      {
        Sid    = "AllowBucketOwnerFullControl"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${var.databricks_aws_account_id}:root"
        }
        Action   = "s3:PutObject"
        Resource = "${aws_s3_bucket.databricks_artifacts.arn}/*"
        Condition = {
          StringEquals = {
            "s3:x-amz-acl" = "bucket-owner-full-control"
          }
        }
      }
    ]
  })

  depends_on = [
    aws_s3_bucket_public_access_block.databricks_artifacts
  ]
}

# -----------------------------------------------------------------------------
# IAM USER pour le connecteur Kafka local (écriture S3)
# -----------------------------------------------------------------------------
resource "aws_iam_user" "kafka_s3" {
  name = "${var.project_name}-kafka-s3-writer"
}

resource "aws_iam_user_policy" "kafka_s3" {
  name = "kafka-s3-write-policy"
  user = aws_iam_user.kafka_s3.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:AbortMultipartUpload"
        ]
        Effect = "Allow"
        Resource = [
          aws_s3_bucket.kafka_raw.arn,
          "${aws_s3_bucket.kafka_raw.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_access_key" "kafka_s3" {
  user = aws_iam_user.kafka_s3.name
}

# -----------------------------------------------------------------------------
# IAM ROLE pour Databricks (cross-account) - AVEC SELF-ASSUMING (sans cycle)
# -----------------------------------------------------------------------------
data "aws_iam_policy_document" "databricks_assume_role" {
  # 1. Confiance pour la création du Workspace (Databricks Account)
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      # Remplacez par l'ID de compte AWS de Databricks pour votre région (généralement 414351767826)
      identifiers = ["arn:aws:iam::414351767826:root"] 
    }

    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.databricks_account_id]
    }
  }

  # 2. Confiance spécifique pour Unity Catalog (Storage Credentials) ET Self-Assuming
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      # Autorise le compte AWS de Databricks Unity Catalog
      identifiers = ["arn:aws:iam::414351767826:root"] 
    }
    
    # Unity Catalog utilise sts:ExternalId pour valider le Workspace ou l'Account.
    # Pour un Account-level Storage Credential, c'est l'account_id.
    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.databricks_account_id]
    }
  }

  # 3. LE VRAI SELF-ASSUMING : Le rôle s'autorise lui-même en tant que principal
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      # C'est l'ARN exact du rôle que vous êtes en train de créer
      identifiers = ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.project_name}-databricks-role"]
    }
    
    # Très important : pas de condition 'sts:ExternalId' ici, car lorsque le rôle 
    # s'assume lui-même, il ne passe pas l'ExternalId.
  }
}

resource "aws_iam_role" "databricks_cross_account" {
  name               = "${var.project_name}-databricks-role"
  assume_role_policy = data.aws_iam_policy_document.databricks_assume_role.json

  tags = {
    Name = "${var.project_name}-databricks-role"
  }
}

# -----------------------------------------------------------------------------
# IAM ROLE POLICY (complète et correcte pour Databricks)
# avec depends_on explicite pour garantir l'attachement
# -----------------------------------------------------------------------------
resource "aws_iam_role_policy" "databricks_cross_account" {
  name = "databricks-cross-account-policy"
  role = aws_iam_role.databricks_cross_account.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # ------------------------------------------------------------------
      # S3 - Root Storage + données
      # ------------------------------------------------------------------
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:GetLifecycleConfiguration",
          "s3:PutLifecycleConfiguration"
        ]
        Resource = [
          aws_s3_bucket.databricks_artifacts.arn,
          "${aws_s3_bucket.databricks_artifacts.arn}/*",
          aws_s3_bucket.kafka_raw.arn,
          "${aws_s3_bucket.kafka_raw.arn}/*"
        ]
      },

      # ------------------------------------------------------------------
      # EC2 - Permissions obligatoires (liste explicite)
      # ------------------------------------------------------------------
      {
        Effect = "Allow"
        Action = [
          "ec2:DescribeAvailabilityZones",
          "ec2:DescribeInstances",
          "ec2:DescribeInstanceStatus",
          "ec2:DescribeRouteTables",
          "ec2:DescribeSecurityGroups",
          "ec2:DescribeSubnets",
          "ec2:DescribeVolumes",
          "ec2:DescribeVpcs",
          "ec2:DescribeVpcAttribute",
          "ec2:DescribeNetworkAcls",
          "ec2:DescribeNetworkInterfaces",
          "ec2:DescribeNetworkInterfaceAttribute",
          "ec2:DescribeInternetGateways",
          "ec2:DescribeNatGateways",
          "ec2:DescribeImages",
          "ec2:DescribeKeyPairs",
          "ec2:DescribeLaunchTemplateVersions",
          "ec2:DescribeVpcEndpointServices",
          "ec2:DescribeVpcEndpoints",
          "ec2:DescribePrefixLists",
          "ec2:DescribeSpotInstanceRequests",
          "ec2:DescribeSpotPriceHistory",
          "ec2:CreateTags",
          "ec2:DeleteTags",
          "ec2:CreateNetworkInterface",
          "ec2:DeleteNetworkInterface",
          "ec2:AttachNetworkInterface",
          "ec2:DetachNetworkInterface",
          "ec2:AssignPrivateIpAddresses",
          "ec2:UnassignPrivateIpAddresses",
          "ec2:CreateVolume",
          "ec2:DeleteVolume",
          "ec2:AttachVolume",
          "ec2:DetachVolume",
          "ec2:CreateSecurityGroup",
          "ec2:DeleteSecurityGroup",
          "ec2:AuthorizeSecurityGroupIngress",
          "ec2:AuthorizeSecurityGroupEgress",
          "ec2:RevokeSecurityGroupIngress",
          "ec2:RevokeSecurityGroupEgress",
          "ec2:RunInstances",
          "ec2:TerminateInstances",
          "ec2:RequestSpotInstances",
          "ec2:CancelSpotInstanceRequests",
          "ec2:ModifyInstanceAttribute",
          "ec2:ModifyNetworkInterfaceAttribute"
        ]
        Resource = "*"
      },

      # ------------------------------------------------------------------
      # IAM PassRole (obligatoire)
      # ------------------------------------------------------------------
      {
        Effect   = "Allow"
        Action   = "iam:PassRole"
        Resource = [
          aws_iam_role.databricks_cross_account.arn,
          "arn:aws:iam::*:role/${var.project_name}-databricks-*"
        ]
      }
    ]
  })

  # Garantir que la politique est créée après le rôle
  depends_on = [aws_iam_role.databricks_cross_account]
}

# Instance Profile pour les clusters Databricks
resource "aws_iam_instance_profile" "databricks_worker" {
  name = "${var.project_name}-databricks-worker-profile"
  role = aws_iam_role.databricks_cross_account.name
}