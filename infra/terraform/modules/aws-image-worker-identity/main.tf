locals {
  oidc_issuer = trimprefix(var.oidc_issuer_url, "https://")
  common_tags = merge(var.tags, {
    "ManagedBy"         = "Terraform"
    "SkyPilotComponent" = "container-image-distribution"
  })
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
}

resource "aws_iam_role" "lifecycle" {
  name                 = "${var.name_prefix}-lifecycle"
  assume_role_policy   = data.aws_iam_policy_document.lifecycle_trust.json
  permissions_boundary = var.permissions_boundary_arn
  tags                 = merge(local.common_tags, { "SkyPilotWorkerKind" = "lifecycle" })
}

resource "aws_iam_role" "canary" {
  name                 = "${var.name_prefix}-canary"
  assume_role_policy   = data.aws_iam_policy_document.canary_trust.json
  permissions_boundary = var.permissions_boundary_arn
  tags                 = merge(local.common_tags, { "SkyPilotWorkerKind" = "canary" })
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
