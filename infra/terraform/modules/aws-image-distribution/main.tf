data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}
data "aws_region" "current" {}

data "aws_servicequotas_service_quota" "ecr_images_per_repository" {
  count        = var.applied_images_per_repository_quota == null ? 1 : 0
  service_code = "ecr"
  quota_code   = "L-03A36CE1"
}

locals {
  applied_images_per_repository_quota = var.applied_images_per_repository_quota != null ? var.applied_images_per_repository_quota : data.aws_servicequotas_service_quota.ecr_images_per_repository[0].value
  base32_alphabet                     = "abcdefghijklmnopqrstuvwxyz234567"
  catalog_authority_padded_integer    = try(parseint(replace(var.catalog_authority, "-", ""), 16) * 4, 0)
  derived_catalog_authority_base32 = join("", [
    for index in range(26) :
    substr(
      local.base32_alphabet,
      floor(local.catalog_authority_padded_integer / pow(32, 25 - index)) % 32,
      1,
    )
  ])
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

  qualification_repository_base = "${var.repository_prefix}/r${var.catalog_authority_base32}/qualification"
  qualification_repository_name = "${local.qualification_repository_base}/${var.region}"
  qualification_generation_specs = {
    for generation in var.qualification_repository_generations :
    format("g%02x", generation) => {
      generation      = generation
      repository_name = "${local.qualification_repository_base}/${format("g%02x", generation)}/${var.region}"
    }
    if generation != 0
  }
  active_qualification_repository_key = format(
    "g%02x",
    var.active_qualification_repository_generation,
  )
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
      condition     = var.catalog_authority_base32 == local.derived_catalog_authority_base32
      error_message = "catalog_authority_base32 must be the lowercase, unpadded base32 encoding of catalog_authority."
    }
    precondition {
      condition = alltrue([
        for arn in var.copy_worker_base_role_arns :
        startswith(arn, "arn:${data.aws_partition.current.partition}:iam::")
      ])
      error_message = "Every copy worker base role must belong to the registry target's AWS partition."
    }
    precondition {
      condition = alltrue([
        for arn in var.lifecycle_worker_base_role_arns :
        startswith(arn, "arn:${data.aws_partition.current.partition}:iam::")
      ])
      error_message = "Every lifecycle worker base role must belong to the registry target's AWS partition."
    }
    precondition {
      condition = alltrue([
        for target in values(var.targets) :
        alltrue([
          for arn in target.runtime_pull_principal_arns :
          startswith(arn, "arn:${data.aws_partition.current.partition}:iam::")
        ])
      ])
      error_message = "Every runtime pull principal must belong to the registry target's AWS partition."
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
      condition = (
        contains(var.qualification_repository_generations, 0) &&
        contains(
          var.qualification_repository_generations,
          var.active_qualification_repository_generation,
        ) &&
        var.active_qualification_repository_generation == max(
          tolist(var.qualification_repository_generations)...
        )
      )
      error_message = "qualification_repository_generations must retain generation 0, and active_qualification_repository_generation must be the highest retained generation."
    }
    precondition {
      condition = (
        length(local.qualification_repository_name) <= 256 &&
        alltrue([
          for spec in values(local.qualification_generation_specs) :
          length(spec.repository_name) <= 256
        ])
      )
      error_message = "A qualification repository name exceeds the ECR length limit."
    }
    precondition {
      condition     = var.encryption_type == "KMS" ? var.kms_key_arn != null : var.kms_key_arn == null
      error_message = "kms_key_arn must be set only when encryption_type is KMS."
    }
    precondition {
      condition = (
        var.kms_key_arn == null ||
        startswith(
          var.kms_key_arn,
          "arn:${data.aws_partition.current.partition}:kms:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:key/",
        )
      )
      error_message = "kms_key_arn must belong to the registry target's AWS partition, account, and region."
    }
    precondition {
      condition = alltrue([
        for target in values(var.targets) :
        target.max_manifests_per_shard + var.quota_headroom <= local.applied_images_per_repository_quota
      ])
      error_message = "A configured manifest ceiling exceeds the verified quota after headroom."
    }
    precondition {
      condition     = length(var.copy_worker_base_role_arns) > 0
      error_message = "copy_worker_base_role_arns is required for the module-owned copy target role."
    }
    precondition {
      condition     = length(var.lifecycle_worker_base_role_arns) > 0
      error_message = "lifecycle_worker_base_role_arns is required for the module-owned lifecycle target role."
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

resource "aws_ecr_repository" "qualification_generation" {
  for_each = local.qualification_generation_specs

  name                 = each.value.repository_name
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
    "SkyPilotQualification"           = "true"
    "SkyPilotQualificationGeneration" = tostring(each.value.generation)
  })

  lifecycle {
    prevent_destroy = true
  }

  depends_on = [terraform_data.validation]
}

locals {
  qualification_repositories = merge(
    { g00 = aws_ecr_repository.qualification },
    aws_ecr_repository.qualification_generation,
  )
  active_qualification_repository = try(
    local.qualification_repositories[local.active_qualification_repository_key],
    aws_ecr_repository.qualification,
  )
  active_qualification_repository_arns = [
    local.active_qualification_repository.arn
  ]
  lifecycle_read_actions = [
    "ecr:BatchGetImage",
    "ecr:DescribeImages",
    "ecr:ListImages",
  ]
  lifecycle_delete_actions = ["ecr:BatchDeleteImage"]
  managed_repository_arns = concat(
    [for repository in aws_ecr_repository.shard : repository.arn],
    local.active_qualification_repository_arns,
  )
  lifecycle_delete_repository_arns = concat(
    [
      for key, repository in aws_ecr_repository.shard : repository.arn
      if !local.shards[key].canonical
    ],
    local.active_qualification_repository_arns,
  )
  iam_managed_policy_max_characters = 6144
  iam_inline_policy_max_characters  = 10240
}

data "aws_iam_policy_document" "copy_role_boundary" {
  statement {
    sid       = "CopyExactManagedRepositories"
    effect    = "Allow"
    actions   = ["ecr:BatchCheckLayerAvailability", "ecr:GetDownloadUrlForLayer", "ecr:BatchGetImage", "ecr:DescribeImages", "ecr:DescribeRepositories", "ecr:GetRepositoryPolicy", "ecr:ListTagsForResource", "ecr:ListImages", "ecr:InitiateLayerUpload", "ecr:UploadLayerPart", "ecr:CompleteLayerUpload", "ecr:PutImage"]
    resources = local.managed_repository_arns
  }

  statement {
    sid       = "AuthorizationTokenOnly"
    effect    = "Allow"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }

  statement {
    sid       = "ReadAppliedEcrQuota"
    effect    = "Allow"
    actions   = ["servicequotas:GetServiceQuota", "servicequotas:GetAWSDefaultServiceQuota"]
    resources = ["*"]
  }
}

data "aws_iam_policy_document" "lifecycle_role_boundary" {
  statement {
    sid       = "LifecycleReadAllManagedRepositories"
    effect    = "Allow"
    actions   = local.lifecycle_read_actions
    resources = local.managed_repository_arns
  }

  statement {
    sid       = "LifecycleDeleteEligibleRepositories"
    effect    = "Allow"
    actions   = local.lifecycle_delete_actions
    resources = local.lifecycle_delete_repository_arns
  }
}

resource "aws_iam_policy" "copy_role_boundary" {
  name        = "${var.copy_target_role_name}-boundary"
  description = "Maximum ECR and quota-read permissions for the SkyPilot image copy role."
  policy      = data.aws_iam_policy_document.copy_role_boundary.json
  tags        = local.common_tags

  lifecycle {
    precondition {
      condition     = length(data.aws_iam_policy_document.copy_role_boundary.minified_json) <= local.iam_managed_policy_max_characters
      error_message = "The rendered copy role boundary exceeds the AWS customer-managed policy size limit."
    }
  }
}

resource "aws_iam_policy" "lifecycle_role_boundary" {
  name        = "${var.lifecycle_target_role_name}-boundary"
  description = "Maximum ECR read and custody-scoped delete permissions for the SkyPilot image lifecycle role."
  policy      = data.aws_iam_policy_document.lifecycle_role_boundary.json
  tags        = local.common_tags

  lifecycle {
    precondition {
      condition     = length(data.aws_iam_policy_document.lifecycle_role_boundary.minified_json) <= local.iam_managed_policy_max_characters
      error_message = "The rendered lifecycle role boundary exceeds the AWS customer-managed policy size limit."
    }
  }
}

data "aws_iam_policy_document" "copy_trust" {
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
  name                 = var.copy_target_role_name
  assume_role_policy   = data.aws_iam_policy_document.copy_trust.json
  permissions_boundary = aws_iam_policy.copy_role_boundary.arn
  max_session_duration = 3600
  tags                 = merge(local.common_tags, { "SkyPilotWorkerKind" = "copy" })

  lifecycle {
    precondition {
      condition     = length(data.aws_iam_policy_document.copy_trust.minified_json) <= var.applied_role_trust_policy_quota
      error_message = "The rendered copy target role trust policy exceeds applied_role_trust_policy_quota."
    }
  }
}

resource "aws_iam_role" "lifecycle_target" {
  name                 = var.lifecycle_target_role_name
  assume_role_policy   = data.aws_iam_policy_document.lifecycle_trust.json
  permissions_boundary = aws_iam_policy.lifecycle_role_boundary.arn
  max_session_duration = 3600
  tags                 = merge(local.common_tags, { "SkyPilotWorkerKind" = "lifecycle" })

  lifecycle {
    precondition {
      condition     = length(data.aws_iam_policy_document.lifecycle_trust.minified_json) <= var.applied_role_trust_policy_quota
      error_message = "The rendered lifecycle target role trust policy exceeds applied_role_trust_policy_quota."
    }
  }
}

locals {
  copy_target_role_arn      = aws_iam_role.copy_target.arn
  lifecycle_target_role_arn = aws_iam_role.lifecycle_target.arn
}

data "aws_iam_policy_document" "copy_permissions" {
  statement {
    sid       = "CopyExactManagedContent"
    effect    = "Allow"
    actions   = ["ecr:BatchCheckLayerAvailability", "ecr:GetDownloadUrlForLayer", "ecr:BatchGetImage", "ecr:DescribeImages", "ecr:DescribeRepositories", "ecr:GetRepositoryPolicy", "ecr:ListTagsForResource", "ecr:ListImages", "ecr:InitiateLayerUpload", "ecr:UploadLayerPart", "ecr:CompleteLayerUpload", "ecr:PutImage"]
    resources = local.managed_repository_arns
  }

  statement {
    sid       = "CopyAuthorizationToken"
    effect    = "Allow"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }

  statement {
    sid       = "CopyReadAppliedEcrQuota"
    effect    = "Allow"
    actions   = ["servicequotas:GetServiceQuota", "servicequotas:GetAWSDefaultServiceQuota"]
    resources = ["*"]
  }
}

data "aws_iam_policy_document" "lifecycle_permissions" {
  statement {
    sid       = "ReadAllManagedContent"
    effect    = "Allow"
    actions   = local.lifecycle_read_actions
    resources = local.managed_repository_arns
  }

  statement {
    sid       = "DeleteEligibleManagedContent"
    effect    = "Allow"
    actions   = local.lifecycle_delete_actions
    resources = local.lifecycle_delete_repository_arns
  }
}

resource "aws_iam_role_policy" "copy_target" {
  name   = "copy-managed-image-content"
  role   = aws_iam_role.copy_target.id
  policy = data.aws_iam_policy_document.copy_permissions.json

  lifecycle {
    precondition {
      condition     = length(data.aws_iam_policy_document.copy_permissions.minified_json) <= local.iam_inline_policy_max_characters
      error_message = "The rendered copy role policy exceeds the AWS inline role policy size limit."
    }
  }
}

resource "aws_iam_role_policy" "lifecycle_target" {
  name   = "lifecycle-managed-image-content"
  role   = aws_iam_role.lifecycle_target.id
  policy = data.aws_iam_policy_document.lifecycle_permissions.json

  lifecycle {
    precondition {
      condition     = length(data.aws_iam_policy_document.lifecycle_permissions.minified_json) <= local.iam_inline_policy_max_characters
      error_message = "The rendered lifecycle role policy exceeds the AWS inline role policy size limit."
    }
  }
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

  statement {
    sid     = "SkyPilotLifecycleRead"
    effect  = "Allow"
    actions = local.lifecycle_read_actions

    principals {
      type        = "AWS"
      identifiers = [local.lifecycle_target_role_arn]
    }
  }

  dynamic "statement" {
    for_each = each.value.canonical ? [] : [local.lifecycle_target_role_arn]
    content {
      sid     = "SkyPilotLifecycleDelete"
      effect  = "Allow"
      actions = local.lifecycle_delete_actions

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
    sid     = "SkyPilotQualificationLifecycleRead"
    effect  = "Allow"
    actions = local.lifecycle_read_actions

    principals {
      type        = "AWS"
      identifiers = [local.lifecycle_target_role_arn]
    }
  }

  statement {
    sid     = "SkyPilotQualificationLifecycleDelete"
    effect  = "Allow"
    actions = local.lifecycle_delete_actions

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

data "aws_iam_policy_document" "qualification_inactive" {
  statement {
    sid    = "SkyPilotInactiveQualificationDataPlaneDeny"
    effect = "Deny"
    actions = [
      "ecr:BatchCheckLayerAvailability",
      "ecr:GetDownloadUrlForLayer",
      "ecr:BatchGetImage",
      "ecr:DescribeImages",
      "ecr:ListImages",
      "ecr:InitiateLayerUpload",
      "ecr:UploadLayerPart",
      "ecr:CompleteLayerUpload",
      "ecr:PutImage",
      "ecr:BatchDeleteImage",
    ]

    principals {
      type        = "*"
      identifiers = ["*"]
    }
  }
}

resource "aws_ecr_repository_policy" "qualification" {
  repository = aws_ecr_repository.qualification.name
  policy = var.active_qualification_repository_generation == 0 ? (
    data.aws_iam_policy_document.qualification.json
  ) : data.aws_iam_policy_document.qualification_inactive.json

  lifecycle {
    precondition {
      condition = max(
        length(data.aws_iam_policy_document.qualification.json),
        length(data.aws_iam_policy_document.qualification_inactive.json),
      ) <= var.max_repository_policy_bytes
      error_message = "The qualification repository policy exceeds the configured byte ceiling."
    }
  }
}

resource "aws_ecr_repository_policy" "qualification_generation" {
  for_each = aws_ecr_repository.qualification_generation

  repository = each.value.name
  policy = local.qualification_generation_specs[each.key].generation == var.active_qualification_repository_generation ? (
    data.aws_iam_policy_document.qualification.json
  ) : data.aws_iam_policy_document.qualification_inactive.json

  lifecycle {
    precondition {
      condition = max(
        length(data.aws_iam_policy_document.qualification.json),
        length(data.aws_iam_policy_document.qualification_inactive.json),
      ) <= var.max_repository_policy_bytes
      error_message = "A generated qualification repository policy exceeds the configured byte ceiling."
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
      policy_hash         = sha256(jsonencode(jsondecode(data.aws_iam_policy_document.shard[key].json)))
      ownership_tags_hash = sha256(jsonencode(aws_ecr_repository.shard[key].tags_all))
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
