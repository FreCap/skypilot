data "aws_partition" "current" {}
data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

locals {
  ec2_canary_enabled = length(var.ami_arns) > 0 || length(var.subnet_arns) > 0
  instance_resource_arns = [
    "arn:${data.aws_partition.current.partition}:ec2:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:instance/*",
    "arn:${data.aws_partition.current.partition}:ec2:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:network-interface/*",
    "arn:${data.aws_partition.current.partition}:ec2:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:volume/*",
  ]
  common_tags = merge(var.tags, {
    "ManagedBy"         = "Terraform"
    "SkyPilotComponent" = "container-image-canary"
  })
}

resource "terraform_data" "validate_contract" {
  lifecycle {
    precondition {
      condition = (
        (length(var.ami_arns) > 0 && length(var.subnet_arns) > 0) ||
        (!local.ec2_canary_enabled && length(var.eks_cluster_arns) > 0)
      )
      error_message = "Configure both ami_arns and subnet_arns for EC2 canaries, or at least one eks_cluster_arn for an EKS-only target."
    }
    precondition {
      condition     = !local.ec2_canary_enabled || length(var.canary_instance_types) > 0
      error_message = "EC2 canaries require at least one exact canary_instance_type."
    }
    precondition {
      condition     = !local.ec2_canary_enabled || length(var.security_group_arns) > 0
      error_message = "EC2 canaries require at least one exact security_group_arn and never use an implicit default security group."
    }
  }
}

data "aws_iam_policy_document" "trust" {
  statement {
    sid     = "AssumeFromExactCanaryWorkers"
    effect  = "Allow"
    actions = ["sts:AssumeRole", "sts:TagSession"]

    principals {
      type        = "AWS"
      identifiers = sort(tolist(var.canary_worker_role_arns))
    }

    dynamic "condition" {
      for_each = var.external_id == null ? [] : [var.external_id]
      content {
        test     = "StringEquals"
        variable = "sts:ExternalId"
        values   = [condition.value]
      }
    }
  }
}

data "aws_iam_policy_document" "permissions" {
  dynamic "statement" {
    for_each = local.ec2_canary_enabled ? [1] : []
    content {
      sid     = "LaunchOnlyThroughQualifiedNetworkAndImage"
      effect  = "Allow"
      actions = ["ec2:RunInstances"]
      resources = sort(concat(
        tolist(var.ami_arns),
        tolist(var.subnet_arns),
        tolist(var.security_group_arns),
      ))

      condition {
        test     = "StringEquals"
        variable = "ec2:InstanceType"
        values   = sort(tolist(var.canary_instance_types))
      }
    }
  }

  dynamic "statement" {
    for_each = local.ec2_canary_enabled ? [1] : []
    content {
      sid       = "CreateOnlyCatalogTaggedCanaryResources"
      effect    = "Allow"
      actions   = ["ec2:RunInstances"]
      resources = local.instance_resource_arns

      condition {
        test     = "StringEquals"
        variable = "ec2:InstanceType"
        values   = sort(tolist(var.canary_instance_types))
      }

      condition {
        test     = "StringEquals"
        variable = "aws:RequestTag/SkyPilotCatalog"
        values   = [var.catalog_authority]
      }

      condition {
        test     = "StringLike"
        variable = "aws:RequestTag/SkyPilotCanaryOperation"
        values   = ["????????-????-????-????-????????????"]
      }
    }
  }

  dynamic "statement" {
    for_each = local.ec2_canary_enabled ? [1] : []
    content {
      sid       = "TagOnlyDuringQualifiedCanaryLaunch"
      effect    = "Allow"
      actions   = ["ec2:CreateTags"]
      resources = local.instance_resource_arns

      condition {
        test     = "StringEquals"
        variable = "ec2:CreateAction"
        values   = ["RunInstances"]
      }
    }
  }

  statement {
    sid       = "InspectComputeState"
    effect    = "Allow"
    actions   = ["ec2:DescribeInstances", "ec2:DescribeInstanceStatus", "ec2:DescribeTags"]
    resources = ["*"]
  }

  dynamic "statement" {
    for_each = local.ec2_canary_enabled ? [1] : []
    content {
      sid       = "ObserveAndTerminateCatalogCanaries"
      effect    = "Allow"
      actions   = ["ec2:GetConsoleOutput", "ec2:TerminateInstances"]
      resources = [local.instance_resource_arns[0]]

      condition {
        test     = "StringEquals"
        variable = "ec2:ResourceTag/SkyPilotCatalog"
        values   = [var.catalog_authority]
      }
    }
  }

  dynamic "statement" {
    for_each = local.ec2_canary_enabled ? [1] : []
    content {
      sid       = "PassOnlyQualifiedRuntimeRoles"
      effect    = "Allow"
      actions   = ["iam:PassRole"]
      resources = sort(tolist(var.runtime_role_arns))

      condition {
        test     = "StringEquals"
        variable = "iam:PassedToService"
        values   = ["ec2.amazonaws.com"]
      }
    }
  }

  statement {
    sid       = "InspectOnlyQualifiedInstanceProfiles"
    effect    = "Allow"
    actions   = ["iam:GetInstanceProfile"]
    resources = sort(tolist(var.instance_profile_arns))
  }

  dynamic "statement" {
    for_each = length(var.eks_cluster_arns) > 0 ? [1] : []
    content {
      sid       = "InspectOnlyQualifiedEksClusters"
      effect    = "Allow"
      actions   = ["eks:DescribeCluster"]
      resources = sort(tolist(var.eks_cluster_arns))
    }
  }
}

resource "aws_iam_role" "canary" {
  name                 = var.role_name
  assume_role_policy   = data.aws_iam_policy_document.trust.json
  permissions_boundary = var.permissions_boundary_arn
  tags                 = local.common_tags
}

resource "aws_iam_role_policy" "canary" {
  name   = "bounded-image-canary-launch"
  role   = aws_iam_role.canary.id
  policy = data.aws_iam_policy_document.permissions.json
}
