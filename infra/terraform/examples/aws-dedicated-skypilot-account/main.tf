provider "aws" {
  alias  = "home"
  region = var.home_region
}

provider "aws" {
  alias  = "secondary"
  region = var.secondary_region
}

data "aws_partition" "current" {
  provider = aws.home
}

module "image_canary_account" {
  source = "../../modules/aws-image-canary-account"

  providers = {
    aws = aws.home
  }
}

locals {
  copy_base_role_name           = "skypilot-image-worker-copy"
  lifecycle_base_role_name      = "skypilot-image-worker-lifecycle"
  home_copy_role_name           = "skypilot-images-${var.home_region}-copy"
  home_lifecycle_role_name      = "skypilot-images-${var.home_region}-lifecycle"
  secondary_copy_role_name      = "skypilot-images-${var.secondary_region}-copy"
  secondary_lifecycle_role_name = "skypilot-images-${var.secondary_region}-lifecycle"
  role_arn_prefix               = "arn:${data.aws_partition.current.partition}:iam::${var.registry_account_id}:role"
}

module "worker_identity" {
  source = "../../modules/aws-image-worker-identity"

  providers = {
    aws = aws.home
  }

  name_prefix               = "skypilot-image-worker"
  oidc_provider_arn         = var.eks_oidc_provider_arn
  oidc_issuer_url           = var.eks_oidc_issuer_url
  kubernetes_namespace      = var.kubernetes_namespace
  copy_service_account      = "skypilot-image-copy-worker"
  lifecycle_service_account = "skypilot-image-lifecycle-worker"
  copy_target_role_arns = [
    "${local.role_arn_prefix}/${local.home_copy_role_name}",
    "${local.role_arn_prefix}/${local.secondary_copy_role_name}",
  ]
  lifecycle_target_role_arns = [
    "${local.role_arn_prefix}/${local.home_lifecycle_role_name}",
    "${local.role_arn_prefix}/${local.secondary_lifecycle_role_name}",
  ]
  canary_target_role_arns = var.canary_target_role_arns
  tags                    = var.tags
}

module "home_distribution" {
  source = "../../modules/aws-image-distribution"

  providers = {
    aws = aws.home
  }

  catalog_authority                   = var.catalog_authority
  catalog_authority_base32            = var.catalog_authority_base32
  realm                               = var.realm
  profile                             = var.profile
  registry_account_id                 = var.registry_account_id
  region                              = var.home_region
  workspaces                          = var.workspaces
  copy_worker_base_role_arns          = [module.worker_identity.copy_role_arn]
  lifecycle_worker_base_role_arns     = [module.worker_identity.lifecycle_role_arn]
  copy_target_role_name               = local.home_copy_role_name
  lifecycle_target_role_name          = local.home_lifecycle_role_name
  applied_images_per_repository_quota = var.applied_images_per_repository_quota
  targets = {
    canonical = {
      canonical                    = true
      shard_count                  = 16
      max_manifests_per_shard      = 90000
      max_declared_bytes_per_shard = 10995116277760
      max_in_flight                = 10
      runtime_pull_principal_arns  = var.home_runtime_pull_principal_arns
    }
  }
  tags = var.tags
}

module "secondary_distribution" {
  source = "../../modules/aws-image-distribution"

  providers = {
    aws = aws.secondary
  }

  catalog_authority                   = var.catalog_authority
  catalog_authority_base32            = var.catalog_authority_base32
  realm                               = var.realm
  profile                             = var.profile
  registry_account_id                 = var.registry_account_id
  region                              = var.secondary_region
  workspaces                          = var.workspaces
  copy_worker_base_role_arns          = [module.worker_identity.copy_role_arn]
  lifecycle_worker_base_role_arns     = [module.worker_identity.lifecycle_role_arn]
  copy_target_role_name               = local.secondary_copy_role_name
  lifecycle_target_role_name          = local.secondary_lifecycle_role_name
  applied_images_per_repository_quota = var.applied_images_per_repository_quota
  targets = {
    "aws-${var.secondary_region}" = {
      canonical                    = false
      shard_count                  = 16
      max_manifests_per_shard      = 90000
      max_declared_bytes_per_shard = 10995116277760
      max_in_flight                = 10
      runtime_pull_principal_arns  = var.secondary_runtime_pull_principal_arns
    }
  }
  tags = var.tags
}

locals {
  qualification_manifests = {
    for workspace in sort(tolist(var.workspaces)) : workspace => {
      schema_version         = 1
      catalog_authority      = var.catalog_authority
      workspace              = workspace
      profile                = var.profile
      profile_revision       = var.profile_revision
      config_hash            = var.profile_config_hash
      physical_manifest_hash = var.physical_manifest_hash
      generated_at           = var.qualification_generated_at
      shards = concat(
        module.home_distribution.qualified_shards_by_workspace[workspace],
        module.secondary_distribution.qualified_shards_by_workspace[workspace],
      )
      role_fingerprints = merge(
        module.home_distribution.role_fingerprints,
        module.secondary_distribution.role_fingerprints,
      )
      quota_facts = merge(
        module.home_distribution.quota_facts,
        module.secondary_distribution.quota_facts,
      )
    }
  }
}
