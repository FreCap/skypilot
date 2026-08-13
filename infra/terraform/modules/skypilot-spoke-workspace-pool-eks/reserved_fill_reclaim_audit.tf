# One optional audit identity proves the deployment-owned reclaim topology
# without broadening the ordinary writer or workload ServiceAccount. The
# source role is exact, and its EKS Pod Identity attributes survive role
# chaining as transitive principal tags.
resource "aws_iam_role" "reserved_fill_reclaim_audit" {
  count = local.reclaim_audit_enabled ? 1 : 0

  name = local.reclaim_audit_identity
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "ExactControlPlanePodIdentity"
      Effect    = "Allow"
      Principal = { AWS = var.controller_role_arn }
      Action    = ["sts:AssumeRole", "sts:TagSession"]
      Condition = {
        StringEquals = {
          "aws:PrincipalTag/eks-cluster-arn"            = var.reserved_fill_reclaim_audit.source_identity.eks_cluster_arn
          "aws:PrincipalTag/kubernetes-namespace"       = var.reserved_fill_reclaim_audit.source_identity.namespace
          "aws:PrincipalTag/kubernetes-service-account" = var.reserved_fill_reclaim_audit.source_identity.service_account
        }
      }
    }]
  })

  tags = merge(var.tags, {
    Name      = local.reclaim_audit_identity
    ManagedBy = "terraform"
    Purpose   = "skypilot-reserved-fill-reclaim-audit"
  })

  lifecycle {
    precondition {
      condition     = startswith(var.reserved_fill_reclaim_audit.source_identity.eks_cluster_arn, "arn:${data.aws_partition.current.partition}:eks:")
      error_message = "The reclaim-audit source EKS cluster must belong to the active AWS partition."
    }

    precondition {
      condition     = try(local.reclaim_audit_partition != null && local.reclaim_audit_partition.priority_class != null && local.reclaim_audit_partition.kueue != null, false)
      error_message = "reserved_fill_reclaim_audit.partition_namespace must select one configured partition with priority and Kueue contracts."
    }
  }
}

resource "aws_iam_role_policy" "reserved_fill_reclaim_audit" {
  count = local.reclaim_audit_enabled ? 1 : 0

  name = local.reclaim_audit_identity
  role = aws_iam_role.reserved_fill_reclaim_audit[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ReadExactEksClusterAndAssociationIndex"
        Effect   = "Allow"
        Action   = ["eks:DescribeCluster", "eks:ListPodIdentityAssociations"]
        Resource = data.aws_eks_cluster.target.arn
      },
      {
        Sid      = "ReadExactEksPodIdentityAssociations"
        Effect   = "Allow"
        Action   = "eks:DescribePodIdentityAssociation"
        Resource = "arn:${data.aws_partition.current.partition}:eks:${var.aws_region}:${data.aws_caller_identity.current[0].account_id}:podidentityassociation/${var.eks_cluster_name}/*"
      },
    ]
  })
}

resource "aws_eks_access_entry" "reserved_fill_reclaim_audit" {
  count = local.reclaim_audit_enabled ? 1 : 0

  cluster_name      = var.eks_cluster_name
  principal_arn     = aws_iam_role.reserved_fill_reclaim_audit[0].arn
  type              = "STANDARD"
  kubernetes_groups = [local.reclaim_audit_identity]

  tags = merge(var.tags, {
    Name      = "${var.eks_cluster_name}-reserved-fill-reclaim-audit"
    ManagedBy = "terraform"
    Purpose   = "skypilot-reserved-fill-reclaim-audit"
  })
}

resource "kubernetes_cluster_role_v1" "reserved_fill_reclaim_audit" {
  count = local.reclaim_audit_enabled ? 1 : 0

  metadata {
    name   = local.reclaim_audit_cluster_resource_name
    labels = local.reclaim_audit_labels
  }

  # Kubernetes RBAC cannot restrict list by label selector. This is the only
  # non-exact-name read, and the plugin validates every returned Node locally.
  rule {
    api_groups = [""]
    resources  = ["nodes"]
    verbs      = ["list"]
  }

  rule {
    api_groups     = [""]
    resources      = ["namespaces"]
    resource_names = ["kube-system", local.reclaim_audit_partition_namespace]
    verbs          = ["get"]
  }

  rule {
    api_groups     = ["scheduling.k8s.io"]
    resources      = ["priorityclasses"]
    resource_names = [local.reclaim_audit_priority_class_name]
    verbs          = ["get"]
  }

  rule {
    api_groups     = ["kueue.x-k8s.io"]
    resources      = ["clusterqueues"]
    resource_names = sort(tolist(local.reclaim_audit_cluster_queues))
    verbs          = ["get"]
  }

  rule {
    api_groups     = ["kueue.x-k8s.io"]
    resources      = ["workloadpriorityclasses"]
    resource_names = sort(tolist(var.reserved_fill_reclaim_audit.workload_priority_class_names))
    verbs          = ["get"]
  }

  rule {
    api_groups     = ["kueue.x-k8s.io"]
    resources      = ["resourceflavors"]
    resource_names = sort(tolist(var.reserved_fill_reclaim_audit.resource_flavor_names))
    verbs          = ["get"]
  }

  rule {
    api_groups     = ["admissionregistration.k8s.io"]
    resources      = ["validatingadmissionpolicies"]
    resource_names = sort(tolist(var.reserved_fill_reclaim_audit.admission_policy_names))
    verbs          = ["get"]
  }

  rule {
    api_groups     = ["admissionregistration.k8s.io"]
    resources      = ["validatingadmissionpolicybindings"]
    resource_names = sort(tolist(var.reserved_fill_reclaim_audit.admission_policy_binding_names))
    verbs          = ["get"]
  }

  rule {
    api_groups     = ["admissionregistration.k8s.io"]
    resources      = ["validatingwebhookconfigurations"]
    resource_names = sort(tolist(var.reserved_fill_reclaim_audit.validating_webhook_names))
    verbs          = ["get"]
  }

  rule {
    api_groups     = ["admissionregistration.k8s.io"]
    resources      = ["mutatingwebhookconfigurations"]
    resource_names = sort(tolist(var.reserved_fill_reclaim_audit.mutating_webhook_names))
    verbs          = ["get"]
  }
}

resource "kubernetes_cluster_role_binding_v1" "reserved_fill_reclaim_audit" {
  count = local.reclaim_audit_enabled ? 1 : 0

  metadata {
    name   = local.reclaim_audit_cluster_resource_name
    labels = local.reclaim_audit_labels
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = kubernetes_cluster_role_v1.reserved_fill_reclaim_audit[0].metadata[0].name
  }

  subject {
    kind      = "Group"
    name      = local.reclaim_audit_identity
    api_group = "rbac.authorization.k8s.io"
  }
}

resource "kubernetes_role_v1" "reserved_fill_reclaim_partition_audit" {
  count = local.reclaim_audit_enabled ? 1 : 0

  metadata {
    name      = local.reclaim_audit_partition_resource_name
    namespace = local.reclaim_audit_partition_namespace
    labels    = local.reclaim_audit_labels
  }

  rule {
    api_groups     = [""]
    resources      = ["serviceaccounts"]
    resource_names = [local.reclaim_audit_service_account]
    verbs          = ["get"]
  }

  rule {
    api_groups     = ["kueue.x-k8s.io"]
    resources      = ["localqueues"]
    resource_names = [local.reclaim_audit_local_queue]
    verbs          = ["get"]
  }
}

resource "kubernetes_role_binding_v1" "reserved_fill_reclaim_partition_audit" {
  count = local.reclaim_audit_enabled ? 1 : 0

  metadata {
    name      = local.reclaim_audit_partition_resource_name
    namespace = local.reclaim_audit_partition_namespace
    labels    = local.reclaim_audit_labels
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role_v1.reserved_fill_reclaim_partition_audit[0].metadata[0].name
  }

  subject {
    kind      = "Group"
    name      = local.reclaim_audit_identity
    api_group = "rbac.authorization.k8s.io"
  }
}

resource "kubernetes_role_v1" "reserved_fill_reclaim_scheduler_audit" {
  count = local.reclaim_audit_enabled ? 1 : 0

  metadata {
    name      = local.reclaim_audit_scheduler_resource_name
    namespace = var.reserved_fill_reclaim_audit.scheduler_namespace
    labels    = local.reclaim_audit_labels
  }

  rule {
    api_groups     = ["apps"]
    resources      = ["deployments"]
    resource_names = [var.reserved_fill_reclaim_audit.scheduler_deployment_name]
    verbs          = ["get"]
  }
}

resource "kubernetes_role_binding_v1" "reserved_fill_reclaim_scheduler_audit" {
  count = local.reclaim_audit_enabled ? 1 : 0

  metadata {
    name      = local.reclaim_audit_scheduler_resource_name
    namespace = var.reserved_fill_reclaim_audit.scheduler_namespace
    labels    = local.reclaim_audit_labels
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role_v1.reserved_fill_reclaim_scheduler_audit[0].metadata[0].name
  }

  subject {
    kind      = "Group"
    name      = local.reclaim_audit_identity
    api_group = "rbac.authorization.k8s.io"
  }
}

resource "kubernetes_role_v1" "reserved_fill_reclaim_kueue_audit" {
  count = local.reclaim_audit_enabled ? 1 : 0

  metadata {
    name      = local.reclaim_audit_kueue_resource_name
    namespace = var.reserved_fill_reclaim_audit.kueue_namespace
    labels    = local.reclaim_audit_labels
  }

  rule {
    api_groups     = ["apps"]
    resources      = ["deployments"]
    resource_names = [var.reserved_fill_reclaim_audit.kueue_deployment_name]
    verbs          = ["get"]
  }

  rule {
    api_groups     = [""]
    resources      = ["configmaps"]
    resource_names = [var.reserved_fill_reclaim_audit.kueue_config_map_name]
    verbs          = ["get"]
  }
}

resource "kubernetes_role_binding_v1" "reserved_fill_reclaim_kueue_audit" {
  count = local.reclaim_audit_enabled ? 1 : 0

  metadata {
    name      = local.reclaim_audit_kueue_resource_name
    namespace = var.reserved_fill_reclaim_audit.kueue_namespace
    labels    = local.reclaim_audit_labels
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role_v1.reserved_fill_reclaim_kueue_audit[0].metadata[0].name
  }

  subject {
    kind      = "Group"
    name      = local.reclaim_audit_identity
    api_group = "rbac.authorization.k8s.io"
  }
}
