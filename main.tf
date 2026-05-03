terraform {
  required_version = ">= 1.3.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# ─── Provider ────────────────────────────────────────────────────────────────
provider "aws" {
  region = var.aws_region
}

# ─── Variables ────────────────────────────────────────────────────────────────
variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "me-south-1" # Bahrain - closest AWS region to Egypt
}

variable "project_name" {
  description = "Project name used as a prefix for all resources"
  type        = string
  default     = "cdr-telecom"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"
}

# ─── S3 Bucket ───────────────────────────────────────────────────────────────
resource "aws_s3_bucket" "cdr_data_lake" {
  bucket = "${var.project_name}-data-lake-${var.environment}"

  tags = {
    Name        = "${var.project_name}-data-lake"
    Environment = var.environment
    Project     = "CDR Telecom Big Data Platform"
    ManagedBy   = "Terraform"
  }
}

# Block all public access (data lake must be private)
resource "aws_s3_bucket_public_access_block" "cdr_data_lake" {
  bucket = aws_s3_bucket.cdr_data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Enable versioning so we can roll back bad data loads
resource "aws_s3_bucket_versioning" "cdr_data_lake" {
  bucket = aws_s3_bucket.cdr_data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Server-side encryption at rest
resource "aws_s3_bucket_server_side_encryption_configuration" "cdr_data_lake" {
  bucket = aws_s3_bucket.cdr_data_lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# ─── S3 Folder Structure (zone prefixes) ─────────────────────────────────────
# Raw zone  – unprocessed CDR files from the generator
resource "aws_s3_object" "raw_zone" {
  bucket  = aws_s3_bucket.cdr_data_lake.id
  key     = "raw/.keep"
  content = ""
}

# Clean zone – validated & anonymised CDRs after Spark processing
resource "aws_s3_object" "clean_zone" {
  bucket  = aws_s3_bucket.cdr_data_lake.id
  key     = "clean/.keep"
  content = ""
}

# Analytics zone – aggregated Hive/Spark outputs for BI tools
resource "aws_s3_object" "analytics_zone" {
  bucket  = aws_s3_bucket.cdr_data_lake.id
  key     = "analytics/.keep"
  content = ""
}

# ─── IAM: read/write policy for the pipeline ─────────────────────────────────
resource "aws_iam_policy" "cdr_s3_policy" {
  name        = "${var.project_name}-s3-rw-policy"
  description = "Read/Write access to the CDR data lake bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.cdr_data_lake.arn,
          "${aws_s3_bucket.cdr_data_lake.arn}/*"
        ]
      }
    ]
  })
}

# ─── Outputs ─────────────────────────────────────────────────────────────────
output "s3_bucket_name" {
  description = "Name of the CDR data lake S3 bucket"
  value       = aws_s3_bucket.cdr_data_lake.bucket
}

output "s3_bucket_arn" {
  description = "ARN of the CDR data lake S3 bucket"
  value       = aws_s3_bucket.cdr_data_lake.arn
}

output "s3_bucket_region" {
  description = "Region of the CDR data lake S3 bucket"
  value       = var.aws_region
}
