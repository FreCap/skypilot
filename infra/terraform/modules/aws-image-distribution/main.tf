data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}
data "aws_region" "current" {}

locals {
  workspace_hashes = {
    for workspace in var.workspaces :
    workspace => substr(sha256("v1:${var.catalog_authority}:${lower(trimspace(workspace))}"), 0, 32)
  }

  common_tags = merge(var.tags, {
    "ManagedBy"         = "Terraform"
    "SkyPilotAuthority" = var.catalog_authority
    "SkyPilotRealm"     = var.realm
    "SkyPilotProfile"   = var.profile
    "SkyPilotComponent" = "container-image-distribution"
  })

  shard_specs = flatten([
    for workspace in sort(tolist(var.workspaces)) : [
      for target_name, target in var.targets : [
        for shard_index in range(target.shard_count) : {
          key                = "${local.workspace_hashes[workspace]}:${target_name}:${format("%02x", shard_index)}"
          workspace          = workspace
          workspace_hash     = local.workspace_hashes[workspace]
          target             = target_name
          canonical          = target.canonical
          shard_index        = shard_index
          repository_name    = "${var.repository_prefix}/r${var.catalog_authority_base32}/w${local.workspace_hashes[workspace]}/g00/s${format("%02x", shard_index)}"
          max_manifests      = target.max_manifests_per_shard
          max_declared_bytes = target.max_declared_bytes_per_shard
          max_in_flight      = target.max_in_flight
          pull_principals    = target.runtime_pull_principal_arns
        }
      ]
    ]
  ])
  shards = { for shard in flatten(local.shard_specs) : shard.key => shard }

  qualification_repository_name = "${var.repository_prefix}/r${var.catalog_authority_base32}/qualification/${var.region}"
  create_copy_role              = var.existing_copy_target_role_arn == null
  create_lifecycle_role         = var.existing_lifecycle_target_role_arn == null
}

resource "terraform_data" "validation" {
  input = {
    account = data.aws_caller_identity.current.account_id
    region  = data.aws_region.current.region
  }

  lifecycle {
    precondition {
      condition     = data.aws_caller_identity.current.account_id == var.registry_account_id
      error_message = "The AWS provider is authenticated to a different registry account."
    }
    precondition {
      condition     = data.aws_region.current.region == var.region
      error_message = "The AWS provider region does not match var.region."
    }
    precondition {
      condition     = length(distinct(values(local.workspace_hashes))) == length(local.workspace_hashes)
      error_message = "The declared workspace encoding has a collision."
    }
    precondition {
      condition     = alltrue([for shard in values(local.shards) : length(shard.repository_name) <= 256])
      error_message = "A fixed repository name exceeds the ECR length limit."
    }
    precondition {
      condition     = var.encryption_type == "KMS" ? var.kms_key_arn != null : var.kms_key_arn == null
      error_message = "kms_key_arn must be set only when encryption_type is KMS."
    }
    precondition {
      condition = var.applied_images_per_repository_quota == null || alltrue([
        for target in values(var.targets) :
        target.max_manifests_per_shard + var.quota_headroom <= var.applied_images_per_repository_quota
      ])
      error_message = "A configured manifest ceiling exceeds the verified quota after headroom."
    }
    precondition {
      condition     = !local.create_copy_role || length(var.copy_worker_base_role_arns) > 0
      error_message = "copy_worker_base_role_arns is required when creating the copy target role."
    }
    precondition {
      condition     = !local.create_lifecycle_role || length(var.lifecycle_worker_base_role_arns) > 0
      error_message = "lifecycle_worker_base_role_arns is required when creating the lifecycle target role."
    }
  }
}

resource "aws_ecr_repository" "shard" {
  for_each = local.shards

  name                 = each.value.repository_name
  image_tag_mutability = "IMMUTABLE"
  force_delete         = false

  encryption_configuration {
    encryption_type = var.encryption_type
    kms_key         = var.encryption_type == "KMS" ? var.kms_key_arn : null
  }

  image_scanning_configuration {
    scan_on_push = var.scan_on_push
  }

  tags = merge(local.common_tags, {
    "SkyPilotWorkspaceHash" = each.value.workspace_hash
    "SkyPilotTarget"        = each.value.target
    "SkyPilotGeneration"    = "0"
    "SkyPilotShard"         = format("%02x", each.value.shard_index)
  })

  lifecycle {
    prevent_destroy = true
  }

  depends_on = [terraform_data.validation]
}

resource "aws_ecr_repository" "qualification" {
  name                 = local.qualification_repository_name
  image_tag_mutability = "IMMUTABLE"
  force_delete         = false

  encryption_configuration {
    encryption_type = var.encryption_type
    kms_key         = var.encryption_type == "KMS" ? var.kms_key_arn : null
  }

  image_scanning_configuration {
    scan_on_push = false
  }

  tags = merge(local.common_tags, {
    "SkyPilotQualification" = "true"
  })

  lifecycle {
    prevent_destroy = true
  }

  depends_on = [terraform_data.validation]
}

data "aws_iam_policy_document" "target_role_boundary" {
  statement {
    sid       = "ExactManagedRepositories"
    effect    = "Allow"
    actions   = ["ecr:BatchCheckLayerAvailability", "ecr:GetDownloadUrlForLayer", "ecr:BatchGetImage", "ecr:DescribeImages", "ecr:ListImages", "ecr:InitiateLayerUpload", "ecr:UploadLayerPart", "ecr:CompleteLayerUpload", "ecr:PutImage", "ecr:BatchDeleteImage"]
    resources = concat([for repository in aws_ecr_repository.shard : repository.arn], [aws_ecr_repository.qualification.arn])
  }

  statement {
    sid       = "AuthorizationTokenOnly"
    effect    = "Allow"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "target_role_boundary" {
  count = local.create_copy_role || local.create_lifecycle_role ? 1 : 0

  name        = "${var.copy_target_role_name}-boundary"
  description = "Maximum ECR data-plane permissions for SkyPilot image target roles."
  policy      = data.aws_iam_policy_document.target_role_boundary.json
  tags        = local.common_tags
}

data "aws_iam_policy_document" "copy_trust" {
  count = local.create_copy_role ? 1 : 0

  statement {
    sid     = "ExactCopyWorkerPrincipals"
    effect  = "Allow"
    actions = ["sts:AssumeRole", "sts:TagSession"]

    principals {
      type        = "AWS"
      identifiers = sort(tolist(var.copy_worker_base_role_arns))
    }

    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/SkyPilotCatalog"
      values   = [var.catalog_authority]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/SkyPilotProfile"
      values   = [var.profile]
    }

    dynamic "condition" {
      for_each = var.worker_assume_role_external_id == null ? [] : [var.worker_assume_role_external_id]
      content {
        test     = "StringEquals"
        variable = "sts:ExternalId"
        values   = [condition.value]
      }
    }
  }
}

data "aws_iam_policy_document" "lifecycle_trust" {
  count = local.create_lifecycle_role ? 1 : 0

  statement {
    sid     = "ExactLifecycleWorkerPrincipals"
    effect  = "Allow"
    actions = ["sts:AssumeRole", "sts:TagSession"]

    principals {
      type        = "AWS"
      identifiers = sort(tolist(var.lifecycle_worker_base_role_arns))
    }

    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/SkyPilotCatalog"
      values   = [var.catalog_authority]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/SkyPilotProfile"
      values   = [var.profile]
    }

    dynamic "condition" {
      for_each = var.worker_assume_role_external_id == null ? [] : [var.worker_assume_role_external_id]
      content {
        test     = "StringEquals"
        variable = "sts:ExternalId"
        values   = [condition.value]
      }
    }
  }
}

resource "aws_iam_role" "copy_target" {
  count = local.create_copy_role ? 1 : 0

  name                 = var.copy_target_role_name
  assume_role_policy   = data.aws_iam_policy_document.copy_trust[0].json
  permissions_boundary = aws_iam_policy.target_role_boundary[0].arn
  max_session_duration = 3600
  tags                 = merge(local.common_tags, { "SkyPilotWorkerKind" = "copy" })
}

resource "aws_iam_role" "lifecycle_target" {
  count = local.create_lifecycle_role ? 1 : 0

  name                 = var.lifecycle_target_role_name
  assume_role_policy   = data.aws_iam_policy_document.lifecycle_trust[0].json
  permissions_boundary = aws_iam_policy.target_role_boundary[0].arn
  max_session_duration = 3600
  tags                 = merge(local.common_tags, { "SkyPilotWorkerKind" = "lifecycle" })
}

locals {
  copy_target_role_arn      = var.existing_copy_target_role_arn != null ? var.existing_copy_target_role_arn : aws_iam_role.copy_target[0].arn
  lifecycle_target_role_arn = var.existing_lifecycle_target_role_arn != null ? var.existing_lifecycle_target_role_arn : aws_iam_role.lifecycle_target[0].arn
  noncanonical_repository_arns = [
    for key, repository in aws_ecr_repository.shard : repository.arn
    if !local.shards[key].canonical
  ]
}

data "aws_iam_policy_document" "copy_permissions" {
  statement {
    sid       = "CopyExactManagedContent"
    effect    = "Allow"
    actions   = ["ecr:BatchCheckLayerAvailability", "ecr:GetDownloadUrlForLayer", "ecr:BatchGetImage", "ecr:DescribeImages", "ecr:ListImages", "ecr:InitiateLayerUpload", "ecr:UploadLayerPart", "ecr:CompleteLayerUpload", "ecr:PutImage"]
    resources = concat([for repository in aws_ecr_repository.shard : repository.arn], [aws_ecr_repository.qualification.arn])
  }

  statement {
    sid       = "CopyAuthorizationToken"
    effect    = "Allow"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }
}

data "aws_iam_policy_document" "lifecycle_permissions" {
  statement {
    sid       = "InspectAndDeleteEligibleContent"
    effect    = "Allow"
    actions   = ["ecr:BatchGetImage", "ecr:DescribeImages", "ecr:ListImages", "ecr:BatchDeleteImage"]
    resources = concat(local.noncanonical_repository_arns, [aws_ecr_repository.qualification.arn])
  }

  statement {
    sid       = "LifecycleAuthorizationToken"
    effect    = "Allow"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "copy_target" {
  count  = local.create_copy_role ? 1 : 0
  name   = "copy-managed-image-content"
  role   = aws_iam_role.copy_target[0].id
  policy = data.aws_iam_policy_document.copy_permissions.json
}

resource "aws_iam_role_policy" "lifecycle_target" {
  count  = local.create_lifecycle_role ? 1 : 0
  name   = "lifecycle-managed-image-content"
  role   = aws_iam_role.lifecycle_target[0].id
  policy = data.aws_iam_policy_document.lifecycle_permissions.json
}

data "aws_iam_policy_document" "shard" {
  for_each = local.shards

  statement {
    sid     = "SkyPilotCopyWorker"
    effect  = "Allow"
    actions = ["ecr:BatchCheckLayerAvailability", "ecr:GetDownloadUrlForLayer", "ecr:BatchGetImage", "ecr:DescribeImages", "ecr:ListImages", "ecr:InitiateLayerUpload", "ecr:UploadLayerPart", "ecr:CompleteLayerUpload", "ecr:PutImage"]

    principals {
      type        = "AWS"
      identifiers = [local.copy_target_role_arn]
    }
  }

  dynamic "statement" {
    for_each = length(each.value.pull_principals) == 0 ? [] : [each.value.pull_principals]
    content {
      sid     = "SkyPilotRuntimePull"
      effect  = "Allow"
      actions = ["ecr:BatchCheckLayerAvailability", "ecr:GetDownloadUrlForLayer", "ecr:BatchGetImage"]

      principals {
        type        = "AWS"
        identifiers = sort(tolist(statement.value))
      }
    }
  }

  dynamic "statement" {
    for_each = each.value.canonical ? [] : [local.lifecycle_target_role_arn]
    content {
      sid     = "SkyPilotLifecycleWorker"
      effect  = "Allow"
      actions = ["ecr:BatchGetImage", "ecr:DescribeImages", "ecr:ListImages", "ecr:BatchDeleteImage"]

      principals {
        type        = "AWS"
        identifiers = [statement.value]
      }
    }
  }
}

resource "aws_ecr_repository_policy" "shard" {
  for_each = local.shards

  repository = aws_ecr_repository.shard[each.key].name
  policy     = data.aws_iam_policy_document.shard[each.key].json

  lifecycle {
    precondition {
      condition     = length(data.aws_iam_policy_document.shard[each.key].json) <= var.max_repository_policy_bytes
      error_message = "A rendered ECR repository policy exceeds the configured byte ceiling."
    }
  }
}

locals {
  all_runtime_pull_principals = toset(flatten([
    for target in values(var.targets) : tolist(target.runtime_pull_principal_arns)
  ]))
}

data "aws_iam_policy_document" "qualification" {
  statement {
    sid     = "SkyPilotQualificationWorkers"
    effect  = "Allow"
    actions = ["ecr:BatchCheckLayerAvailability", "ecr:GetDownloadUrlForLayer", "ecr:BatchGetImage", "ecr:DescribeImages", "ecr:ListImages", "ecr:InitiateLayerUpload", "ecr:UploadLayerPart", "ecr:CompleteLayerUpload", "ecr:PutImage"]

    principals {
      type        = "AWS"
      identifiers = [local.copy_target_role_arn]
    }
  }

  statement {
    sid     = "SkyPilotQualificationCleanup"
    effect  = "Allow"
    actions = ["ecr:BatchGetImage", "ecr:DescribeImages", "ecr:ListImages", "ecr:BatchDeleteImage"]

    principals {
      type        = "AWS"
      identifiers = [local.lifecycle_target_role_arn]
    }
  }

  dynamic "statement" {
    for_each = length(local.all_runtime_pull_principals) == 0 ? [] : [local.all_runtime_pull_principals]
    content {
      sid     = "SkyPilotQualificationRuntimePull"
      effect  = "Allow"
      actions = ["ecr:BatchCheckLayerAvailability", "ecr:GetDownloadUrlForLayer", "ecr:BatchGetImage"]

      principals {
        type        = "AWS"
        identifiers = sort(tolist(statement.value))
      }
    }
  }
}

resource "aws_ecr_repository_policy" "qualification" {
  repository = aws_ecr_repository.qualification.name
  policy     = data.aws_iam_policy_document.qualification.json

  lifecycle {
    precondition {
      condition     = length(data.aws_iam_policy_document.qualification.json) <= var.max_repository_policy_bytes
      error_message = "The qualification repository policy exceeds the configured byte ceiling."
    }
  }
}

locals {
  shard_bases = {
    for key, shard in local.shards : key => {
      workspace           = shard.workspace
      target              = shard.target
      partition           = data.aws_partition.current.partition
      account             = var.registry_account_id
      region              = var.region
      shard_generation    = 0
      shard_index         = shard.shard_index
      registry            = split("/", aws_ecr_repository.shard[key].repository_url)[0]
      repository_name     = aws_ecr_repository.shard[key].name
      repository_arn      = aws_ecr_repository.shard[key].arn
      encryption_type     = var.encryption_type
      kms_key_arn         = var.encryption_type == "KMS" ? var.kms_key_arn : null
      tag_immutability    = "IMMUTABLE"
      scanning_mode       = var.scan_on_push ? "SCAN_ON_PUSH" : "MANUAL"
      policy_hash         = sha256(data.aws_iam_policy_document.shard[key].json)
      ownership_tags_hash = sha256(jsonencode(aws_ecr_repository.shard[key].tags))
      max_manifests       = shard.max_manifests
      max_declared_bytes  = shard.max_declared_bytes
      max_in_flight       = shard.max_in_flight
    }
  }
  qualified_shards = {
    for key, shard in local.shard_bases :
    key => merge(shard, { physical_fingerprint = sha256(jsonencode(shard)) })
  }
}
