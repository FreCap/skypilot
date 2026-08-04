locals {
  create = var.controller_role_arn != null

  base_tags = {
    "ManagedBy" = "terraform"
    "Purpose"   = "skypilot-aws-vm"
  }

  # EC2 actions SkyPilot needs to provision/manage VMs (see
  # docs.skypilot.co/en/latest/cloud-setup/cloud-permissions/aws.html). Kept on
  # "*" for simplicity; tighten per the docs' minimal policy if required.
  ec2_actions = [
    "ec2:RunInstances",
    "ec2:TerminateInstances",
    "ec2:StartInstances",
    "ec2:StopInstances",
    # ModifyInstanceAttribute is needed at teardown: SkyPilot clears the instance's
    # disableApiTermination attribute before TerminateInstances, so without it
    # `sky down` fails with UnauthorizedOperation and leaks the instance.
    "ec2:ModifyInstanceAttribute",
    "ec2:CreateTags",
    "ec2:DeleteTags",
    "ec2:Describe*",
    "ec2:CreateSecurityGroup",
    "ec2:AuthorizeSecurityGroupIngress",
    "ec2:RevokeSecurityGroupIngress",
    # DeleteSecurityGroup is required for both AZ/region failover cleanup (when a
    # launch hits InsufficientInstanceCapacity and retries elsewhere) and `sky down`
    # teardown — without it a transient capacity error aborts the whole launch.
    "ec2:DeleteSecurityGroup",
    "ec2:CreateKeyPair",
    "ec2:DeleteKeyPair",
    "ec2:ImportKeyPair",
  ]
}

# Scopes SkyServe controller SSM ARNs to the active AWS account and partition.
data "aws_caller_identity" "current" {}

data "aws_partition" "current" {}

data "aws_iam_policy_document" "vm_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.${data.aws_partition.current.dns_suffix}"]
    }
  }
}

resource "aws_iam_role" "vm" {
  name                 = var.instance_profile_name
  assume_role_policy   = data.aws_iam_policy_document.vm_assume.json
  permissions_boundary = var.permissions_boundary_arn
  tags                 = merge(var.tags, local.base_tags, { Name = var.instance_profile_name })

  # Fail-closed guard: error if the controller role to trust is unset. Rides on
  # this always-created role (mirrors the EKS workspace pool's precondition on
  # aws_eks_access_entry.pool) rather than a dedicated no-op resource.
  lifecycle {
    precondition {
      condition     = var.controller_role_arn != null && trimspace(coalesce(var.controller_role_arn, " ")) != ""
      error_message = "controller_role_arn is unset — set it to the control plane's api_server_role_arn output."
    }

    precondition {
      condition = startswith(
        coalesce(var.controller_role_arn, "invalid"),
        "arn:${data.aws_partition.current.partition}:iam::",
      )
      error_message = "controller_role_arn must use the active AWS partition."
    }

    precondition {
      condition = var.permissions_boundary_arn == null ? true : startswith(
        var.permissions_boundary_arn,
        "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:policy/",
      )
      error_message = "permissions_boundary_arn must identify a managed policy in the active AWS account and partition."
    }

    precondition {
      condition = alltrue([
        for grant in var.vm_dataset_grants :
        startswith(grant.bucket_arn, "arn:${data.aws_partition.current.partition}:s3:::") &&
        (grant.kms_key_arn == null ? true : startswith(
          grant.kms_key_arn,
          "arn:${data.aws_partition.current.partition}:kms:",
        ))
      ])
      error_message = "Dataset bucket and KMS key ARNs must use the active AWS partition."
    }

    precondition {
      condition = alltrue([
        for arn in var.vm_role_extra_policy_arns :
        startswith(
          arn,
          "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:policy/",
        ) ||
        startswith(
          arn,
          "arn:${data.aws_partition.current.partition}:iam::aws:policy/",
        )
      ])
      error_message = "vm_role_extra_policy_arns must identify AWS-managed policies or customer-managed policies in the active AWS account and partition."
    }
  }
}

resource "aws_iam_instance_profile" "vm" {
  name = var.instance_profile_name
  role = aws_iam_role.vm.name
}

data "aws_iam_policy_document" "vm" {
  statement {
    sid       = "Ec2"
    effect    = "Allow"
    actions   = local.ec2_actions
    resources = ["*"]
  }

  dynamic "statement" {
    for_each = length(var.vm_dataset_grants) > 0 ? [1] : []
    content {
      sid       = "DatasetBuckets"
      effect    = "Allow"
      actions   = ["s3:GetBucketLocation", "s3:ListBucket", "s3:ListBucketMultipartUploads"]
      resources = [for g in var.vm_dataset_grants : g.bucket_arn]
    }
  }

  dynamic "statement" {
    for_each = length(var.vm_dataset_grants) > 0 ? [1] : []
    content {
      sid       = "DatasetObjects"
      effect    = "Allow"
      actions   = ["s3:AbortMultipartUpload", "s3:GetObject", "s3:ListMultipartUploadParts", "s3:PutObject"]
      resources = [for g in var.vm_dataset_grants : "${g.bucket_arn}/*"]
    }
  }

  dynamic "statement" {
    for_each = length([for g in var.vm_dataset_grants : g.kms_key_arn if g.kms_key_arn != null]) > 0 ? [1] : []
    content {
      sid       = "DatasetKms"
      effect    = "Allow"
      actions   = ["kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
      resources = [for g in var.vm_dataset_grants : g.kms_key_arn if g.kms_key_arn != null]
    }
  }
}

resource "aws_iam_role_policy" "vm" {
  name   = "skypilot-vm"
  role   = aws_iam_role.vm.id
  policy = data.aws_iam_policy_document.vm.json
}

resource "aws_iam_role_policy_attachment" "vm_extra" {
  for_each   = toset(var.vm_role_extra_policy_arns)
  role       = aws_iam_role.vm.name
  policy_arn = each.value
}

# Resource-scoped inline grants supplied by the caller.
resource "aws_iam_role_policy" "vm_extra_inline" {
  count  = var.vm_role_extra_policy_json != null ? 1 : 0
  name   = "skypilot-vm-extra"
  role   = aws_iam_role.vm.id
  policy = var.vm_role_extra_policy_json
}

# A SkyServe controller VM can use instance-metadata credentials to launch
# replicas in-account. It needs PassRole/GetInstanceProfile to attach the VM
# role and SSM permissions to reach replicas. EC2 actions come from the policy
# above.
resource "aws_iam_role_policy" "vm_serve_replica" {
  count  = var.enable_serve_controller ? 1 : 0
  name   = "skypilot-vm-serve-replica"
  role   = aws_iam_role.vm.id
  policy = data.aws_iam_policy_document.vm_serve_replica[0].json
}

data "aws_iam_policy_document" "vm_serve_replica" {
  count = var.enable_serve_controller ? 1 : 0

  statement {
    sid       = "PassSkypilotInstanceProfile"
    effect    = "Allow"
    actions   = ["iam:GetInstanceProfile", "iam:PassRole"]
    resources = [aws_iam_role.vm.arn, aws_iam_instance_profile.vm.arn]
  }

  statement {
    sid     = "SsmStartSession"
    effect  = "Allow"
    actions = ["ssm:StartSession"]
    resources = [
      "arn:${data.aws_partition.current.partition}:ec2:*:${data.aws_caller_identity.current.account_id}:instance/*",
      "arn:${data.aws_partition.current.partition}:ssm:*::document/AWS-StartSSHSession",
    ]
  }

  statement {
    sid       = "SsmManageSession"
    effect    = "Allow"
    actions   = ["ssm:TerminateSession", "ssm:ResumeSession"]
    resources = ["arn:${data.aws_partition.current.partition}:ssm:*:${data.aws_caller_identity.current.account_id}:session/*"]
  }
}

# A SkyServe controller assumes the provisioner for storage operations and
# credential-process use. TagSession mirrors the provisioner trust because
# instance-profile sessions can carry transitive tags.
resource "aws_iam_role_policy" "vm_assume_provisioner" {
  count  = var.enable_serve_controller && local.create ? 1 : 0
  name   = "skypilot-vm-assume-provisioner"
  role   = aws_iam_role.vm.id
  policy = data.aws_iam_policy_document.vm_assume_provisioner[0].json
}

data "aws_iam_policy_document" "vm_assume_provisioner" {
  count = var.enable_serve_controller && local.create ? 1 : 0

  statement {
    sid       = "AssumeProvisioner"
    effect    = "Allow"
    actions   = ["sts:AssumeRole", "sts:TagSession"]
    resources = [aws_iam_role.provisioner[0].arn]
  }
}

# Lets the SSM agent dial out so the control plane can SSH over Session Manager (no inbound needed).
resource "aws_iam_role_policy_attachment" "vm_ssm" {
  count      = var.enable_ssm ? 1 : 0
  role       = aws_iam_role.vm.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

data "aws_iam_policy_document" "provisioner_assume" {
  count = local.create ? 1 : 0
  statement {
    effect = "Allow"
    # TagSession: EKS Pod Identity sessions carry transitive tags; sts:TagSession
    # must appear on both the caller's policy and this trust.
    actions = ["sts:AssumeRole", "sts:TagSession"]
    principals {
      type = "AWS"
      # Trust the control-plane role and, optionally, the VM role used by an
      # in-account SkyServe controller.
      identifiers = concat(
        [var.controller_role_arn],
        var.enable_serve_controller ? [aws_iam_role.vm.arn] : [],
      )
    }
    dynamic "condition" {
      for_each = var.external_id != null ? [1] : []
      content {
        test     = "StringEquals"
        variable = "sts:ExternalId"
        values   = [var.external_id]
      }
    }
  }
}

resource "aws_iam_role" "provisioner" {
  count                = local.create ? 1 : 0
  name                 = var.provisioner_role_name
  assume_role_policy   = data.aws_iam_policy_document.provisioner_assume[0].json
  permissions_boundary = var.permissions_boundary_arn
  tags                 = merge(var.tags, local.base_tags, { Name = var.provisioner_role_name })
}

data "aws_iam_policy_document" "provisioner" {
  count = local.create ? 1 : 0

  statement {
    sid       = "Ec2Provisioning"
    effect    = "Allow"
    actions   = local.ec2_actions
    resources = ["*"]
  }

  # S3 for SkyPilot Storage (managed-job logs and file mounts).
  statement {
    sid       = "SkyPilotStorageList"
    effect    = "Allow"
    actions   = ["s3:ListAllMyBuckets"]
    resources = ["*"]
  }

  statement {
    sid     = "SkyPilotStorageBuckets"
    effect  = "Allow"
    actions = ["s3:GetBucketLocation", "s3:ListBucket", "s3:CreateBucket", "s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = [
      "arn:${data.aws_partition.current.partition}:s3:::skypilot-*",
      "arn:${data.aws_partition.current.partition}:s3:::skypilot-*/*",
    ]
  }

  statement {
    sid       = "SpotServiceLinkedRole"
    effect    = "Allow"
    actions   = ["iam:CreateServiceLinkedRole"]
    resources = ["arn:${data.aws_partition.current.partition}:iam::*:role/aws-service-role/spot.amazonaws.com/*"]
    condition {
      test     = "StringEquals"
      variable = "iam:AWSServiceName"
      values   = ["spot.amazonaws.com"]
    }
  }

  # Scoped to skypilot-v1 only — prevents passing any other instance profile.
  statement {
    sid       = "PassSkypilotInstanceProfile"
    effect    = "Allow"
    actions   = ["iam:GetRole", "iam:PassRole", "iam:GetInstanceProfile"]
    resources = [aws_iam_role.vm.arn, aws_iam_instance_profile.vm.arn]
  }

  # SSM SSH access (private-IP→instance-id lookup already covered by ec2:Describe* above).
  dynamic "statement" {
    for_each = var.enable_ssm ? [1] : []
    content {
      sid     = "SsmStartSession"
      effect  = "Allow"
      actions = ["ssm:StartSession"]
      resources = [
        "arn:${data.aws_partition.current.partition}:ec2:*:*:instance/*",
        "arn:${data.aws_partition.current.partition}:ssm:*::document/AWS-StartSSHSession",
      ]
    }
  }
}

resource "aws_iam_role_policy" "provisioner" {
  count  = local.create ? 1 : 0
  name   = "skypilot-provisioner"
  role   = aws_iam_role.provisioner[0].id
  policy = data.aws_iam_policy_document.provisioner[0].json
}
