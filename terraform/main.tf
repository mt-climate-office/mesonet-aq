# ── mesonet-aq: nightly PurpleAir archive → mco-mesonet/air-quality/ ──
#
# A scheduled Fargate one-shot (mco-aws scheduled-fargate-pipeline module) that
# fetches PurpleAir history, rewrites the affected station-month Parquet, and
# publishes the index files behind the data CDN (data2.climate.umt.edu/mesonet).
#
# This stack owns: the ECR repo, ECS cluster/task/schedule, the task and CI
# roles, two Secrets Manager secrets, and the dead-man alarm. It does NOT own
# the bucket (mco-aws stacks/mco-mesonet-bucket, including the lifecycle rule
# for air-quality/) or the CDN (mco-data-cdn) -- both are looked up by name.
#
# Conventions (mco-aws/docs/conventions.md): one flat dir, tags ONLY via
# provider default_tags, no `profile` anywhere (export AWS_PROFILE=mco),
# .terraform.lock.hcl committed, terraform.tfvars not. Applies are manual and
# local (mco-aws decisions/0006): plan -out, read it, apply that plan.

terraform {
  required_version = ">= 1.10, < 2.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  backend "s3" {
    bucket       = "mco-terraform-state"
    key          = "mesonet-aq/terraform.tfstate"
    region       = "us-west-2"
    encrypt      = true
    use_lockfile = true
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project   = var.project_name
      ManagedBy = "terraform"
      Repo      = var.github_repo
    }
  }
}

data "aws_caller_identity" "current" {}

data "aws_s3_bucket" "mesonet" {
  bucket = var.bucket_name # fails at plan time if the name is wrong
}

data "aws_cloudfront_distribution" "data_cdn" {
  id = var.data_cdn_distribution_id
}

data "aws_sns_topic" "ops_alerts" {
  name = var.alarm_sns_topic_name
}

locals {
  prefix           = "air-quality"
  metric_namespace = "MesonetAQ"
}
