# ── API credentials, resolved into the container env by ECS ──
#
# Plain-string secrets (not JSON): the module grants the execution role read on
# exactly the ARNs in `secrets`, and a `:key::` suffix would no longer match
# that IAM resource. Terraform creates them with a PLACEHOLDER and never reads
# the value back, so the real value is set out of band and never lands in state:
#
#   aws secretsmanager put-secret-value --secret-id mesonet-aq/purpleair-api-key --secret-string "$KEY"
#   aws secretsmanager put-secret-value --secret-id mesonet-aq/airtable-token    --secret-string "$TOKEN"

resource "aws_secretsmanager_secret" "purpleair" {
  name        = "${var.project_name}/purpleair-api-key"
  description = "PurpleAir API read key (X-API-Key) for the nightly history fetch."
}

resource "aws_secretsmanager_secret_version" "purpleair" {
  secret_id     = aws_secretsmanager_secret.purpleair.id
  secret_string = "PLACEHOLDER"

  lifecycle {
    ignore_changes = [secret_string]
  }
}

resource "aws_secretsmanager_secret" "airtable" {
  name        = "${var.project_name}/airtable-token"
  description = "Airtable PAT, read-only on the sensor Deployments base."
}

resource "aws_secretsmanager_secret_version" "airtable" {
  secret_id     = aws_secretsmanager_secret.airtable.id
  secret_string = "PLACEHOLDER"

  lifecycle {
    ignore_changes = [secret_string]
  }
}
