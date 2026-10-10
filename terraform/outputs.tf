output "github_actions_role_arn" {
  description = "Set as the repo's AWS_ROLE_ARN Actions VARIABLE (not a secret)."
  value       = module.github_oidc.role_arn
}

output "ecr_repository_url" {
  description = "CI pushes :latest and :<sha> here; the schedule runs :latest."
  value       = module.pipeline.ecr_repository_url
}

output "cluster_name" {
  description = "Set as the repo's ECS_CLUSTER Actions variable (run-task.yml)."
  value       = module.pipeline.cluster_name
}

output "task_family" {
  description = "Set as the repo's ECS_TASK_FAMILY Actions variable (run-task.yml)."
  value       = module.pipeline.task_family
}

output "log_group_name" {
  description = "Pipeline logs."
  value       = module.pipeline.log_group_name
}

output "run_task_network" {
  description = "Set as the repo's ECS_NETWORK Actions variable (run-task.yml)."
  value       = "awsvpcConfiguration={subnets=[${join(",", var.subnet_ids)}],securityGroups=[${join(",", var.security_group_ids)}],assignPublicIp=DISABLED}"
}
