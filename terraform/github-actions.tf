# ── The role GitHub Actions assumes (OIDC; main branch only) ──
#
# Replaces the console-made `mco_mesonet-aq_s3-access` (unknown trust subject,
# wrote S3 directly). CI no longer touches data: it pushes the image and can
# START the task for migrate/backfill. Everything data-side is the task role.

module "github_oidc" {
  source = "git::https://github.com/mt-climate-office/mco-aws.git//modules/github-oidc-role?ref=v0.1.1"

  project_name         = var.project_name # role: mesonet-aq-github-actions
  github_repo          = var.github_repo
  github_repository_id = var.github_repository_id

  policy_statements = [
    {
      sid       = "ECRAuth"
      actions   = ["ecr:GetAuthorizationToken"]
      resources = ["*"]
    },
    {
      sid = "ECRPush"
      actions = [
        "ecr:GetDownloadUrlForLayer",
        "ecr:BatchGetImage",
        "ecr:BatchCheckLayerAvailability",
        "ecr:PutImage",
        "ecr:InitiateLayerUpload",
        "ecr:UploadLayerPart",
        "ecr:CompleteLayerUpload",
      ]
      resources = [module.pipeline.ecr_repository_arn]
    },
    {
      # run-task.yml: start the pipeline's own task family (any revision), on
      # its own cluster only, and watch it finish.
      sid       = "RunPipelineTask"
      actions   = ["ecs:RunTask"]
      resources = ["arn:aws:ecs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:task-definition/${module.pipeline.task_family}:*"]
      conditions = {
        ArnEquals = { "ecs:cluster" = [module.pipeline.cluster_arn] }
      }
    },
    {
      sid       = "WatchPipelineTask"
      actions   = ["ecs:DescribeTasks"]
      resources = ["arn:aws:ecs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:task/${module.pipeline.cluster_name}/*"]
    },
    {
      sid       = "PassPipelineRoles"
      actions   = ["iam:PassRole"]
      resources = [module.pipeline.task_role_arn, module.pipeline.execution_role_arn]
      conditions = {
        StringEquals = { "iam:PassedToService" = ["ecs-tasks.amazonaws.com"] }
      }
    },
  ]
}
