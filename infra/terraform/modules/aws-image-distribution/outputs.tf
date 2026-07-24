output "qualified_shards_by_workspace" {
  description = "Secret-free regional shard facts to aggregate into qualification manifests."
  value = {
    for workspace in sort(tolist(var.workspaces)) : workspace => [
      for key in sort(keys(local.qualified_shards)) : local.qualified_shards[key]
      if local.qualified_shards[key].workspace == workspace
    ]
  }
}

output "copy_target_role_arn" {
  value = local.copy_target_role_arn
}

output "lifecycle_target_role_arn" {
  value = local.lifecycle_target_role_arn
}

output "role_fingerprints" {
  description = "Secret-free role and policy fingerprints for qualification."
  value = {
    "${var.region}:copy_role_arn"                     = local.copy_target_role_arn
    "${var.region}:copy_policy_hash"                  = sha256(data.aws_iam_policy_document.copy_permissions.json)
    "${var.region}:lifecycle_role_arn"                = local.lifecycle_target_role_arn
    "${var.region}:lifecycle_policy_hash"             = sha256(data.aws_iam_policy_document.lifecycle_permissions.json)
    "${var.region}:copy_boundary_policy_hash"         = sha256(data.aws_iam_policy_document.copy_role_boundary.json)
    "${var.region}:lifecycle_boundary_policy_hash"    = sha256(data.aws_iam_policy_document.lifecycle_role_boundary.json)
    "${var.region}:qualification_repo_arn"            = local.active_qualification_repository.arn
    "${var.region}:qualification_policy_hash"         = sha256(jsonencode(jsondecode(data.aws_iam_policy_document.qualification.json)))
    "${var.region}:qualification_ownership_tags_hash" = sha256(jsonencode(local.active_qualification_repository.tags_all))
  }
}

output "quota_facts" {
  description = "Applied quota and shared worker-budget facts."
  value = {
    "${var.region}:ecr_api_rate_per_second" = var.applied_ecr_api_rate_per_second
    "${var.region}:ecr_api_burst"           = var.ecr_api_burst
    "${var.region}:images_per_repository"   = local.applied_images_per_repository_quota
    "${var.region}:reserved_headroom"       = var.quota_headroom
  }
}

output "qualification_repository_url" {
  description = "Active qualification repository URL retained for backward-compatible callers."
  value       = local.active_qualification_repository.repository_url
}

output "qualification_repository_generation" {
  description = "Active qualification repository generation."
  value       = var.active_qualification_repository_generation
}

output "qualification_repository_urls_by_generation" {
  description = "Every retained qualification repository URL keyed by generation."
  value = merge(
    { "0" = aws_ecr_repository.qualification.repository_url },
    {
      for key, repository in aws_ecr_repository.qualification_generation :
      tostring(local.qualification_generation_specs[key].generation) => repository.repository_url
    },
  )
}

output "qualification_repository_arns_by_generation" {
  description = "Every retained qualification repository ARN keyed by generation."
  value = merge(
    { "0" = aws_ecr_repository.qualification.arn },
    {
      for key, repository in aws_ecr_repository.qualification_generation :
      tostring(local.qualification_generation_specs[key].generation) => repository.arn
    },
  )
}

output "qualification_repository_policy_modes_by_generation" {
  description = "Repository data-plane policy mode for every retained qualification generation."
  value = merge(
    {
      "0" = var.active_qualification_repository_generation == 0 ? "ACTIVE" : "INACTIVE_DENY"
    },
    {
      for key, _repository in aws_ecr_repository.qualification_generation :
      tostring(local.qualification_generation_specs[key].generation) => (
        local.qualification_generation_specs[key].generation == var.active_qualification_repository_generation ? "ACTIVE" : "INACTIVE_DENY"
      )
    },
  )
}
