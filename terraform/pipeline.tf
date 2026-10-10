# ── The nightly Fargate task ──
#
# 0.5 vCPU / 1 GiB is ample: a night is 13 sensors x 1-2 API calls and a few
# MB of Parquet. `migrate` and `--since` backfills reuse this task definition
# (cpu/memory overrides if ever needed) via .github/workflows/run-task.yml.

module "pipeline" {
  source = "git::https://github.com/mt-climate-office/mco-aws.git//modules/scheduled-fargate-pipeline?ref=v0.1.1"

  name                = var.project_name
  schedule_expression = var.schedule_expression
  schedule_timezone   = "America/Denver"

  cpu                   = 512
  memory                = 1024
  ephemeral_storage_gib = 21

  # Private subnets: egress is the TGW default route; a public IP is useless
  # in a VPC with no Internet Gateway.
  vpc_subnet_ids     = var.subnet_ids
  security_group_ids = var.security_group_ids
  assign_public_ip   = false

  alarm_sns_topic_arn = data.aws_sns_topic.ops_alerts.arn

  environment = {
    TZ                  = "UTC" # the pipeline reasons in UTC days; the app renders Mountain
    AWS_DEFAULT_REGION  = var.aws_region
    S3_BUCKET           = data.aws_s3_bucket.mesonet.id
    AQ_PREFIX           = local.prefix
    CDN_BASE_URL        = "https://data2.climate.umt.edu/mesonet"
    CDN_DISTRIBUTION_ID = var.data_cdn_distribution_id
    METRIC_NAMESPACE    = local.metric_namespace
    AIRTABLE_BASE_ID    = var.airtable_base_id
  }

  secrets = {
    PURPLEAIR_API_KEY = aws_secretsmanager_secret.purpleair.arn
    AIRTABLE_TOKEN    = aws_secretsmanager_secret.airtable.arn
  }

  task_policy_statements = [
    {
      # Put-overwrite of station-months and index files. Delete is for the
      # post-cutover legacy-tree retirement only; the bucket is versioned and
      # the mco-aws lifecycle expires air-quality/ noncurrent versions at 30 d.
      sid       = "AirQualityWrite"
      actions   = ["s3:PutObject", "s3:DeleteObject"]
      resources = ["${data.aws_s3_bucket.mesonet.arn}/${local.prefix}/*"]
    },
    {
      sid       = "AirQualityRead"
      actions   = ["s3:GetObject"]
      resources = ["${data.aws_s3_bucket.mesonet.arn}/${local.prefix}/*"]
    },
    {
      # Prefix-scoped. Also turns a GET of a missing key into 404, not 403
      # (mesonet-cameras terraform/iam.tf), which `get()` relies on.
      sid        = "AirQualityList"
      actions    = ["s3:ListBucket"]
      resources  = [data.aws_s3_bucket.mesonet.arn]
      conditions = { StringLike = { "s3:prefix" = ["${local.prefix}/*", local.prefix] } }
    },
    {
      sid       = "AirQualityCDNInvalidation"
      actions   = ["cloudfront:CreateInvalidation", "cloudfront:GetInvalidation"]
      resources = [data.aws_cloudfront_distribution.data_cdn.arn]
    },
    {
      sid        = "Heartbeat"
      actions    = ["cloudwatch:PutMetricData"]
      resources  = ["*"] # PutMetricData has no resource ARNs; scoped by namespace
      conditions = { StringEquals = { "cloudwatch:namespace" = [local.metric_namespace] } }
    },
  ]
}
