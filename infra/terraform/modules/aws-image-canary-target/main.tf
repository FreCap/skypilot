data "aws_partition" "current" {}
data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

locals {
  ec2_canary_enabled             = length(var.ami_arns) > 0 || length(var.subnet_arns) > 0
  instance_resource_arn          = "arn:${data.aws_partition.current.partition}:ec2:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:instance/*"
  network_interface_resource_arn = "arn:${data.aws_partition.current.partition}:ec2:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:network-interface/*"
  volume_resource_arn            = "arn:${data.aws_partition.current.partition}:ec2:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:volume/*"
  created_resource_arns = [
    local.instance_resource_arn,
    local.network_interface_resource_arn,
    local.volume_resource_arn,
  ]
  spot_request_resource_arn = "arn:${data.aws_partition.current.partition}:ec2:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:spot-instances-request/*"
  launch_created_resource_arns = concat(
    local.created_resource_arns,
    [local.spot_request_resource_arn],
  )
  qualified_instance_profile_arns = sort(concat(
    tolist(var.ec2_instance_profile_arns),
    tolist(var.eks_node_instance_profile_arns),
  ))
  ec2_service_principal                 = "ec2.${data.aws_partition.current.dns_suffix}"
  expected_spot_service_linked_role_arn = "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:role/aws-service-role/spot.amazonaws.com/AWSServiceRoleForEC2Spot"
  spot_service_linked_role_arn          = var.spot_service_linked_role_arn == null ? local.expected_spot_service_linked_role_arn : var.spot_service_linked_role_arn
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
    precondition {
      condition     = !local.ec2_canary_enabled || length(var.ec2_runtime_role_arns) > 0
      error_message = "EC2 canaries require at least one exact ec2_runtime_role_arn."
    }
    precondition {
      condition     = !local.ec2_canary_enabled || length(var.ec2_instance_profile_arns) > 0
      error_message = "EC2 canaries require at least one exact ec2_instance_profile_arn."
    }
    precondition {
      condition     = length(var.eks_cluster_arns) == 0 || length(var.eks_node_instance_profile_arns) > 0
      error_message = "EKS canaries require at least one inspect-only eks_node_instance_profile_arn."
    }
    precondition {
      condition     = length(setintersection(var.ec2_instance_profile_arns, var.eks_node_instance_profile_arns)) == 0
      error_message = "EC2 launchable and EKS inspect-only instance profiles must be disjoint."
    }
    precondition {
      condition = alltrue([
        for arn in var.canary_worker_role_arns :
        startswith(arn, "arn:${data.aws_partition.current.partition}:iam::")
      ])
      error_message = "Every canary worker role must belong to the target AWS partition."
    }
    precondition {
      condition = alltrue([
        for arn in var.ec2_runtime_role_arns :
        startswith(arn, "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:role/")
      ])
      error_message = "Every EC2 runtime role must belong to the target AWS account and partition."
    }
    precondition {
      condition = alltrue([
        for arn in local.qualified_instance_profile_arns :
        startswith(arn, "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:instance-profile/")
      ])
      error_message = "Every qualified instance profile must belong to the target AWS account and partition."
    }
    precondition {
      condition = alltrue([
        for arn in var.ami_arns :
        startswith(arn, "arn:${data.aws_partition.current.partition}:ec2:${data.aws_region.current.region}::image/ami-")
      ])
      error_message = "Every qualified AMI must belong to the target AWS partition and module region."
    }
    precondition {
      condition = alltrue([
        for arn in var.subnet_arns :
        startswith(arn, "arn:${data.aws_partition.current.partition}:ec2:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:subnet/subnet-")
      ])
      error_message = "Every qualified subnet must belong to the target AWS account, partition, and module region."
    }
    precondition {
      condition = alltrue([
        for arn in var.security_group_arns :
        startswith(arn, "arn:${data.aws_partition.current.partition}:ec2:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:security-group/sg-")
      ])
      error_message = "Every qualified security group must belong to the target AWS account, partition, and module region."
    }
    precondition {
      condition = alltrue([
        for arn in var.eks_cluster_arns :
        startswith(arn, "arn:${data.aws_partition.current.partition}:eks:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:cluster/")
      ])
      error_message = "Every qualified EKS cluster must belong to the target AWS account, partition, and module region."
    }
    precondition {
      condition = (
        var.permissions_boundary_arn == null ||
        startswith(var.permissions_boundary_arn, "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:policy/")
      )
      error_message = "The permissions boundary must belong to the target AWS account and partition."
    }
    precondition {
      condition     = !local.ec2_canary_enabled || var.spot_service_linked_role_arn != null
      error_message = "EC2 canary targets require spot_service_linked_role_arn from the account bootstrap module."
    }
    precondition {
      condition     = var.spot_service_linked_role_arn == null || var.spot_service_linked_role_arn == local.expected_spot_service_linked_role_arn
      error_message = "spot_service_linked_role_arn must belong to the target AWS account and identify its canonical EC2 Spot service-linked role."
    }
    precondition {
      condition     = local.ec2_canary_enabled || length(var.spot_customer_managed_kms_key_arns) == 0
      error_message = "Spot customer-managed KMS keys are valid only for an EC2 canary target."
    }
    precondition {
      condition = alltrue([
        for arn in var.spot_customer_managed_kms_key_arns :
        startswith(arn, "arn:${data.aws_partition.current.partition}:kms:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:key/")
      ])
      error_message = "Every Spot customer-managed KMS key must belong to the target AWS account and module region."
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
    }
  }

  dynamic "statement" {
    for_each = local.ec2_canary_enabled ? [1] : []
    content {
      sid       = "CreateOnlyCatalogTaggedCanaryInstances"
      effect    = "Allow"
      actions   = ["ec2:RunInstances"]
      resources = [local.instance_resource_arn]

      condition {
        test     = "StringEquals"
        variable = "ec2:InstanceType"
        values   = sort(tolist(var.canary_instance_types))
      }

      condition {
        test     = "ArnEquals"
        variable = "ec2:InstanceProfile"
        values   = sort(tolist(var.ec2_instance_profile_arns))
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
      sid       = "CreateOnlyCatalogTaggedCanaryVolumes"
      effect    = "Allow"
      actions   = ["ec2:RunInstances"]
      resources = [local.volume_resource_arn]

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
      sid       = "CreateOnlyQualifiedSubnetCanaryNetworkInterfaces"
      effect    = "Allow"
      actions   = ["ec2:RunInstances"]
      resources = [local.network_interface_resource_arn]

      condition {
        test     = "ArnEquals"
        variable = "ec2:Subnet"
        values   = sort(tolist(var.subnet_arns))
      }
    }
  }

  dynamic "statement" {
    for_each = local.ec2_canary_enabled ? [1] : []
    content {
      sid       = "CreateOnlyCatalogTaggedCanarySupportResources"
      effect    = "Allow"
      actions   = ["ec2:RunInstances"]
      resources = [local.spot_request_resource_arn]

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
      resources = local.launch_created_resource_arns

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
    actions   = ["ec2:DescribeInstances", "ec2:DescribeInstanceStatus", "ec2:DescribeSpotInstanceRequests", "ec2:DescribeTags"]
    resources = ["*"]
  }

  dynamic "statement" {
    for_each = local.ec2_canary_enabled ? [1] : []
    content {
      sid       = "ObserveAndTerminateCatalogCanaries"
      effect    = "Allow"
      actions   = ["ec2:GetConsoleOutput", "ec2:TerminateInstances"]
      resources = [local.instance_resource_arn]

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
      sid       = "CancelOnlyCatalogSpotRequests"
      effect    = "Allow"
      actions   = ["ec2:CancelSpotInstanceRequests"]
      resources = [local.spot_request_resource_arn]

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
      resources = sort(tolist(var.ec2_runtime_role_arns))

      condition {
        test     = "StringEquals"
        variable = "iam:PassedToService"
        values   = [local.ec2_service_principal]
      }
    }
  }

  statement {
    sid       = "InspectOnlyQualifiedInstanceProfiles"
    effect    = "Allow"
    actions   = ["iam:GetInstanceProfile"]
    resources = local.qualified_instance_profile_arns
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

resource "aws_kms_grant" "spot_encrypted_ami" {
  for_each = var.spot_customer_managed_kms_key_arns

  name              = "${var.role_name}-spot-${substr(sha256(each.value), 0, 12)}"
  key_id            = each.value
  grantee_principal = local.spot_service_linked_role_arn
  operations = [
    "CreateGrant",
    "Decrypt",
    "DescribeKey",
    "Encrypt",
    "GenerateDataKey",
    "GenerateDataKeyWithoutPlaintext",
    "ReEncryptFrom",
    "ReEncryptTo",
  ]
}
