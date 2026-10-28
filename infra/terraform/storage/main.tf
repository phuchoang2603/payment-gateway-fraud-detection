terraform {
  required_version = ">= 1.13.2"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    doppler = {
      source  = "DopplerHQ/doppler"
      version = "1.22.1"
    }
    random = {
      source  = "hashicorp/random"
      version = "3.9.1"
    }
  }
  cloud {
    hostname     = "app.terraform.io"
    organization = "phuchoang2603"
    workspaces {
      name = "payment-gateway-storage"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}
provider "doppler" {}

variable "bucket_name" {
  type    = string
  default = "platform-prod-142737101504-us-east-1"
}

data "aws_s3_bucket" "platform" {
  bucket = var.bucket_name
}

locals {
  prefixes = ["datasets/payment-gateway", "runs/payment-gateway", "lakehouse"]
  object_secrets = {
    OBJECT_GATEWAY_ACCESS_KEY_ID     = aws_iam_access_key.gateway.id
    OBJECT_GATEWAY_SECRET_ACCESS_KEY = aws_iam_access_key.gateway.secret
  }
}

resource "aws_iam_user" "gateway" {
  name = "payment-gateway-prod-storage"
}

data "aws_iam_policy_document" "gateway" {
  statement {
    actions   = ["s3:GetBucketLocation"]
    resources = [data.aws_s3_bucket.platform.arn]
  }
  statement {
    actions   = ["s3:ListBucket", "s3:ListBucketMultipartUploads"]
    resources = [data.aws_s3_bucket.platform.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = flatten([for prefix in local.prefixes : [prefix, "${prefix}/*"]])
    }
  }
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts", "s3:GetObjectTagging", "s3:PutObjectTagging"]
    resources = [for prefix in local.prefixes : "${data.aws_s3_bucket.platform.arn}/${prefix}/*"]
  }
}

resource "aws_iam_user_policy" "gateway" {
  name   = "gateway-object-storage"
  user   = aws_iam_user.gateway.name
  policy = data.aws_iam_policy_document.gateway.json
}
resource "aws_iam_access_key" "gateway" {
  user = aws_iam_user.gateway.name
}
resource "doppler_secret" "objects" {
  for_each = nonsensitive(toset(keys(local.object_secrets)))
  project  = "payment-gateway-fraud-detection"
  config   = "prd"
  name     = each.key
  value    = local.object_secrets[each.key]
}
resource "random_password" "metadata_reader" {
  for_each = toset(["dev", "prd"])
  length   = 32
  special  = false
}
resource "doppler_secret" "metadata_reader" {
  for_each = random_password.metadata_reader
  project  = "payment-gateway-fraud-detection"
  config   = each.key
  name     = "STARROCKS_OPENMETADATA_PASSWORD"
  value    = each.value.result
}
