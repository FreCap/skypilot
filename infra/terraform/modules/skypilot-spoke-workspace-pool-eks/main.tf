# These values are retained for Terragrunt callers that generate an EKS
# exec-auth Kubernetes provider alongside this module. The module itself does
# not configure providers; ordinary Terraform callers may configure and pass a
# Kubernetes provider by any supported authentication method.
data "aws_eks_cluster" "target" {
  name = var.eks_cluster_name
}

data "aws_partition" "current" {}

locals {
  exec_env = var.aws_profile != null && trimspace(var.aws_profile) != "" ? {
    AWS_PROFILE = trimspace(var.aws_profile)
  } : {}

  exec_args = ["eks", "get-token", "--cluster-name", var.eks_cluster_name, "--region", var.aws_region]

  partitions = [for partition in var.partitions : merge(partition, {
    group = coalesce(partition.group, partition.namespace)
  })]

  partitions_by_namespace = {
    for partition in local.partitions : partition.namespace => partition
  }
  partitions_with_identity = {
    for partition in local.partitions : partition.namespace => partition
    if trimspace(partition.pod_identity_role_arn) != ""
  }
}

# EKS access entries support cross-account principals within the active AWS
# partition. The mapped principal receives every configured partition group.
resource "aws_eks_access_entry" "pool" {
  cluster_name      = var.eks_cluster_name
  principal_arn     = var.controller_role_arn
  type              = "STANDARD"
  kubernetes_groups = [for partition in local.partitions : partition.group]

  tags = merge(var.tags, {
    Name      = "${var.eks_cluster_name}-skypilot-pool"
    ManagedBy = "terraform"
    Purpose   = "skypilot-pool-access"
  })

  lifecycle {
    precondition {
      condition     = startswith(var.controller_role_arn, "arn:${data.aws_partition.current.partition}:iam::")
      error_message = "controller_role_arn must belong to the active AWS partition."
    }
  }
}

module "rbac" {
  source   = "../skypilot-spoke-workspace-pool-rbac"
  for_each = local.partitions_by_namespace

  providers = {
    kubernetes = kubernetes
  }

  name                 = each.value.group
  namespace            = each.value.namespace
  manage_namespace     = each.value.manage_namespace
  service_account_name = each.value.pod_identity_service_account
  allow_pvc_read       = length(each.value.fsx_volumes) > 0
  allow_self_teardown  = each.value.allow_self_teardown

  subjects = [{
    kind = "Group"
    name = each.value.group
  }]
}

# A Pod Identity association is created only for partitions that supply a role.
# This module does not establish a tenant boundary by itself: other
# associations may already exist in the namespace, and the SkyPilot workspace
# must separately be pinned to the intended namespace.
resource "aws_eks_pod_identity_association" "pool_sa" {
  for_each = local.partitions_with_identity

  cluster_name    = var.eks_cluster_name
  namespace       = each.value.namespace
  service_account = each.value.pod_identity_service_account
  role_arn        = each.value.pod_identity_role_arn

  tags = merge(var.tags, {
    ManagedBy = "terraform"
    Purpose   = "skypilot-pool-pod-identity"
  })

  lifecycle {
    precondition {
      condition     = startswith(each.value.pod_identity_role_arn, "arn:${data.aws_partition.current.partition}:iam::")
      error_message = "Each pod_identity_role_arn must belong to the active AWS partition."
    }
  }

  depends_on = [module.rbac]
}
