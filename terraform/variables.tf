variable "project_name" {
  description = "Names the ECR repo, cluster, roles, schedule and the state key. Org convention: the repo name."
  type        = string
  default     = "mesonet-aq"
}

variable "aws_region" {
  description = "AWS region."
  type        = string
  default     = "us-west-2"
}

variable "github_repo" {
  description = "GitHub repository, org/name. Scopes the OIDC trust and the Repo tag."
  type        = string
  default     = "mt-climate-office/mesonet-aq"
}

variable "github_repository_id" {
  description = "Immutable numeric repository ID (gh api repos/mt-climate-office/mesonet-aq --jq .id). Keeps CI authenticating across a rename."
  type        = string
}

variable "bucket_name" {
  description = "The shared, private, CDN-fronted bucket (owned by mco-aws stacks/mco-mesonet-bucket)."
  type        = string
  default     = "mco-mesonet"
}

variable "data_cdn_distribution_id" {
  description = "mco-data-cdn distribution (data2.climate.umt.edu). Invalidation paths are cache keys: /air-quality/..., never /mesonet/air-quality/..."
  type        = string
  default     = "E24I4W0YAJ2A27"
}

variable "subnet_ids" {
  description = "Shared-VPC PRIVATE workload subnets (PrivateSubnet1/2 -- they carry the 0.0.0.0/0 route via the Transit Gateway). Never the TGW subnets. The VPC has no Internet Gateway, so a public IP would not help."
  type        = list(string)
}

variable "security_group_ids" {
  description = "Security groups for the task (the shared VPC default SG). This account cannot create SGs."
  type        = list(string)
}

variable "schedule_expression" {
  description = "Nightly run, evaluated in America/Denver. 01:30 is ~7.5 h after the UTC day closes."
  type        = string
  default     = "cron(30 1 * * ? *)"
}

variable "alarm_sns_topic_name" {
  description = "Org-wide ops alert topic (mco-aws stacks/account-baseline)."
  type        = string
  default     = "mco-ops-alerts"
}

variable "airtable_base_id" {
  description = "Airtable base with the Deployments table. An identifier, not a credential (the token is in Secrets Manager); set in terraform.tfvars."
  type        = string
}
