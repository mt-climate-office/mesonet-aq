# ── Dead-man alarm: no successful nightly run in 30 h ──
#
# The module's alarm only sees EventBridge Scheduler failing to START the task.
# A task that starts and exits non-zero -- or never logs at all -- is invisible
# to it. `mesonet-aq run` publishes MesonetAQ/RunSucceeded=1 only after a clean,
# full run, so silence is the signal (the mesonet-db-rds ledger-alarms.tf
# pattern). treat_missing_data = "breaching" is load-bearing: no datapoint IS
# the failure. 30 x 1 h periods: one skipped night pages the next morning,
# while run-length jitter and the Denver DST shift do not.

resource "aws_cloudwatch_metric_alarm" "run_stale" {
  alarm_name        = "${var.project_name}-run-stale"
  alarm_description = "mesonet-aq has not completed a clean nightly run in 30 h. Read /ecs/${var.project_name} logs; the next run resumes from each station's fetched_through, so a re-run (run-task.yml) repairs gaps."

  namespace   = local.metric_namespace
  metric_name = "RunSucceeded"
  statistic   = "SampleCount"

  comparison_operator = "LessThanThreshold"
  threshold           = 1
  period              = 3600
  evaluation_periods  = 30
  datapoints_to_alarm = 30
  treat_missing_data  = "breaching"

  alarm_actions = [data.aws_sns_topic.ops_alerts.arn]
  ok_actions    = [data.aws_sns_topic.ops_alerts.arn]
}
