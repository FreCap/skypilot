# AWS identity for the SkyPilot API-server service account, via EKS Pod Identity.
# This role's ARN is the principal that pool clusters authorize.

locals {
  api_server_role_name = var.api_server_role_name != null ? var.api_server_role_name : "skypilot-api-${var.host_cluster_name}"
}

resource "aws_iam_role" "api_server" {
  name                 = local.api_server_role_name
  permissions_boundary = var.permissions_boundary_arn

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "pods.eks.amazonaws.com" }
      Action    = ["sts:AssumeRole", "sts:TagSession"]
    }]
  })

  tags = merge(var.tags, {
    Name      = local.api_server_role_name
    ManagedBy = "terraform"
    Purpose   = "skypilot-control-plane"
  })

  lifecycle {
    precondition {
      condition     = data.aws_caller_identity.current.account_id == var.aws_account_id
      error_message = "aws_account_id must match the account selected by the AWS provider."
    }
    precondition {
      condition     = data.aws_region.current.region == var.aws_region
      error_message = "aws_region must match the region selected by the AWS provider."
    }
    precondition {
      condition     = length(local.api_server_role_name) <= 64 && can(regex("^[A-Za-z0-9+=,.@_-]+$", local.api_server_role_name))
      error_message = "The derived API-server role name is invalid or exceeds IAM's 64-character limit; set api_server_role_name explicitly."
    }
    precondition {
      condition = var.permissions_boundary_arn == null || startswith(
        var.permissions_boundary_arn,
        "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:policy/",
      )
      error_message = "permissions_boundary_arn must identify a managed policy in the active AWS account and partition."
    }
  }
}

# Host cluster must run the eks-pod-identity-agent addon (the cluster substrate guarantees this).
resource "aws_eks_pod_identity_association" "api_server" {
  cluster_name    = var.host_cluster_name
  namespace       = var.namespace
  service_account = local.api_service_account_name
  role_arn        = aws_iam_role.api_server.arn
}

# Baseline permissions: discover EKS clusters (pools) and EC2 capacity. Intentionally
# minimal; broaden via var.extra_policy_json when VM-based pools or other clouds are enabled.
data "aws_iam_policy_document" "api_server" {
  statement {
    sid    = "DiscoverEks"
    effect = "Allow"
    actions = [
      "eks:DescribeCluster",
      "eks:ListClusters",
    ]
    resources = ["*"]
  }

  # SkyServe runs a chain of preflight EC2 *describe* calls with the api-server's OWN
  # identity (not the assumed cross-account provisioner) before it hands off to the
  # provisioner for the actual RunInstances. These cascade — first the AMI root-device
  # lookup (`ec2:DescribeImages`, else "Image <ami> not found"), then a VPC lookup
  # (`ec2:DescribeVpcs`, surfaced as the UnauthorizedOperation in `sky status`/`sky
  # serve up`), then subnets / security-groups / etc. Granting read-only `ec2:Describe*`
  # ends the cascade in one step; mutations still flow through the provisioner
  # role, so this remains read-only in the control-plane account.
  statement {
    sid       = "Ec2PreflightDescribe"
    effect    = "Allow"
    actions   = ["ec2:Describe*"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "api_server" {
  name   = "skypilot-api-baseline"
  role   = aws_iam_role.api_server.id
  policy = data.aws_iam_policy_document.api_server.json
}

resource "aws_iam_role_policy" "api_server_extra" {
  count = var.extra_policy_json != null ? 1 : 0

  name   = "skypilot-api-extra"
  role   = aws_iam_role.api_server.id
  policy = var.extra_policy_json
}
