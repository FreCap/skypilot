data "aws_caller_identity" "current" {}

data "aws_partition" "current" {}

locals {
  oidc_issuer = trimprefix(var.oidc_issuer_url, "https://")
  expected_oidc_provider_arn = format(
    "arn:%s:iam::%s:oidc-provider/%s",
    data.aws_partition.current.partition,
    data.aws_caller_identity.current.account_id,
    local.oidc_issuer,
  )
  target_role_arns = setunion(
    var.copy_target_role_arns,
    var.lifecycle_target_role_arns,
    var.canary_target_role_arns,
  )
  common_tags = merge(var.tags, {
    "ManagedBy"         = "Terraform"
    "SkyPilotComponent" = "container-image-distribution"
  })
}

resource "terraform_data" "validate_contract" {
  lifecycle {
    precondition {
      condition     = var.oidc_provider_arn == local.expected_oidc_provider_arn
      error_message = "oidc_provider_arn must exactly identify oidc_issuer_url in the active AWS account and partition."
    }

    precondition {
      condition = alltrue([
        for arn in local.target_role_arns :
        startswith(arn, "arn:${data.aws_partition.current.partition}:iam::")
      ])
      error_message = "Every target role ARN must use the active AWS partition."
    }

    precondition {
      condition = var.permissions_boundary_arn == null ? true : (
        startswith(
          var.permissions_boundary_arn,
          "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:policy/",
        )
      )
      error_message = "permissions_boundary_arn must identify a managed policy in the active AWS account and partition."
    }
  }
}

data "aws_iam_policy_document" "copy_trust" {
  statement {
    sid     = "SkyPilotImageCopyWorkerWebIdentity"
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [var.oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_issuer}:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_issuer}:sub"
      values   = ["system:serviceaccount:${var.kubernetes_namespace}:${var.copy_service_account}"]
    }
  }
}

data "aws_iam_policy_document" "lifecycle_trust" {
  statement {
    sid     = "SkyPilotImageLifecycleWorkerWebIdentity"
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [var.oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_issuer}:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_issuer}:sub"
      values   = ["system:serviceaccount:${var.kubernetes_namespace}:${var.lifecycle_service_account}"]
    }
  }
}

data "aws_iam_policy_document" "canary_trust" {
  statement {
    sid     = "SkyPilotImageCanaryWorkerWebIdentity"
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [var.oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_issuer}:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_issuer}:sub"
      values   = ["system:serviceaccount:${var.kubernetes_namespace}:${var.canary_service_account}"]
    }
  }
}

resource "aws_iam_role" "copy" {
  name                 = "${var.name_prefix}-copy"
  assume_role_policy   = data.aws_iam_policy_document.copy_trust.json
  permissions_boundary = var.permissions_boundary_arn
  tags                 = merge(local.common_tags, { "SkyPilotWorkerKind" = "copy" })

  depends_on = [terraform_data.validate_contract]
}

resource "aws_iam_role" "lifecycle" {
  name                 = "${var.name_prefix}-lifecycle"
  assume_role_policy   = data.aws_iam_policy_document.lifecycle_trust.json
  permissions_boundary = var.permissions_boundary_arn
  tags                 = merge(local.common_tags, { "SkyPilotWorkerKind" = "lifecycle" })

  depends_on = [terraform_data.validate_contract]
}

resource "aws_iam_role" "canary" {
  name                 = "${var.name_prefix}-canary"
  assume_role_policy   = data.aws_iam_policy_document.canary_trust.json
  permissions_boundary = var.permissions_boundary_arn
  tags                 = merge(local.common_tags, { "SkyPilotWorkerKind" = "canary" })

  depends_on = [terraform_data.validate_contract]
}

data "aws_iam_policy_document" "copy_assume_targets" {
  count = length(var.copy_target_role_arns) == 0 ? 0 : 1

  statement {
    sid       = "AssumeExactImageCopyRoles"
    effect    = "Allow"
    actions   = ["sts:AssumeRole", "sts:TagSession"]
    resources = sort(tolist(var.copy_target_role_arns))
  }
}

data "aws_iam_policy_document" "lifecycle_assume_targets" {
  count = length(var.lifecycle_target_role_arns) == 0 ? 0 : 1

  statement {
    sid       = "AssumeExactImageLifecycleRoles"
    effect    = "Allow"
    actions   = ["sts:AssumeRole", "sts:TagSession"]
    resources = sort(tolist(var.lifecycle_target_role_arns))
  }
}

data "aws_iam_policy_document" "canary_assume_targets" {
  count = length(var.canary_target_role_arns) == 0 ? 0 : 1

  statement {
    sid       = "AssumeExactImageCanaryRoles"
    effect    = "Allow"
    actions   = ["sts:AssumeRole", "sts:TagSession"]
    resources = sort(tolist(var.canary_target_role_arns))
  }
}

resource "aws_iam_role_policy" "copy_assume_targets" {
  count  = length(var.copy_target_role_arns) == 0 ? 0 : 1
  name   = "assume-image-copy-targets"
  role   = aws_iam_role.copy.id
  policy = data.aws_iam_policy_document.copy_assume_targets[0].json
}

resource "aws_iam_role_policy" "lifecycle_assume_targets" {
  count  = length(var.lifecycle_target_role_arns) == 0 ? 0 : 1
  name   = "assume-image-lifecycle-targets"
  role   = aws_iam_role.lifecycle.id
  policy = data.aws_iam_policy_document.lifecycle_assume_targets[0].json
}

resource "aws_iam_role_policy" "canary_assume_targets" {
  count  = length(var.canary_target_role_arns) == 0 ? 0 : 1
  name   = "assume-image-canary-targets"
  role   = aws_iam_role.canary.id
  policy = data.aws_iam_policy_document.canary_assume_targets[0].json
}
