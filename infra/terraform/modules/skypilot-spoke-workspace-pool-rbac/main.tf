locals {
  labels = merge(var.labels, {
    "app.kubernetes.io/managed-by" = "Terraform"
    "app.kubernetes.io/part-of"    = "skypilot-control-plane"
  })
}

resource "kubernetes_namespace_v1" "pool" {
  count = var.manage_namespace ? 1 : 0

  metadata {
    name   = var.namespace
    labels = local.labels
  }
}

# The ServiceAccount SkyPilot runs pods as (the control plane pins
# kubernetes.pod_config serviceAccountName to it). SkyPilot does NOT self-provision
# it, so the pool must. Its AWS identity, if any, comes from an EKS Pod Identity
# association on (this namespace, this SA) in the parent module — a namespace with
# no association mints no credentials for it.
resource "kubernetes_service_account_v1" "pool_sa" {
  metadata {
    name      = var.service_account_name
    namespace = var.namespace
    labels    = local.labels
  }

  depends_on = [kubernetes_namespace_v1.pool]
}

# Cluster-scoped reads only — SkyPilot creates no cluster-scoped objects (the pod uses
# a pre-created SA, so its self-provisioning of ClusterRoles/namespaces is skipped):
#  - nodes: `list` feeds the credential check + resource-fitting (launch hard-fails
#    without it); `get` (read_node) is the node-health status read.
#  - pods (all namespaces): the realtime GPU-availability view (`sky show-gpus` and the
#    dashboard Infra page) lists pods cluster-wide to subtract in-use GPUs from node
#    allocatable. Read-only. Launch/scheduling does NOT need this — it's the
#    capacity-reporting path only; without it those views 403 "cannot list pods at the
#    cluster scope" (the pod-lifecycle pods perms in the Role below are namespaced).
resource "kubernetes_cluster_role_v1" "pool" {
  metadata {
    name   = var.name
    labels = local.labels
  }

  rule {
    api_groups = [""]
    resources  = ["nodes"]
    verbs      = ["get", "list"]
  }

  rule {
    api_groups = [""]
    resources  = ["pods"]
    verbs      = ["get", "list"]
  }

  # runtimeclasses: SkyPilot lists these on launch to detect the GPU RuntimeClass.
  # Source review marked this "optional/warn", but on the deployed chart version its
  # denial fails the launch (verified live) — so it's REQUIRED here. `list` only.
  rule {
    api_groups = ["node.k8s.io"]
    resources  = ["runtimeclasses"]
    verbs      = ["list"]
  }
}

resource "kubernetes_cluster_role_binding_v1" "pool" {
  metadata {
    name   = var.name
    labels = local.labels
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = kubernetes_cluster_role_v1.pool.metadata[0].name
  }

  dynamic "subject" {
    for_each = var.subjects
    content {
      kind      = subject.value.kind
      name      = subject.value.name
      api_group = subject.value.api_group
    }
  }
}

# Least-privilege namespaced ops — exactly what SkyPilot's pool lifecycle calls (derived
# from the sky/provision/kubernetes source + verified by a live launch). Confined to the
# pool namespace, which is the isolation boundary. The pod uses a pre-created
# ServiceAccount, so SkyPilot does NOT self-provision serviceaccounts/roles/rolebindings.
resource "kubernetes_role_v1" "pool" {
  metadata {
    name      = var.name
    namespace = var.namespace
    labels    = local.labels
  }

  # Pod lifecycle: create (launch), get/list (status/scheduling), patch (labels + Kueue
  # finalizer strip on down), delete (down).
  rule {
    api_groups = [""]
    resources  = ["pods"]
    verbs      = ["get", "list", "create", "patch", "delete"]
  }

  # exec = command/file transport (setup, sky exec, sky logs, rsync); portforward = the
  # default SSH tunnel SkyPilot wires on every launch (omitting it hangs wait-for-ssh).
  rule {
    api_groups = [""]
    resources  = ["pods/exec", "pods/portforward"]
    verbs      = ["create"]
  }

  # Head + head-ssh Services: create/patch on launch, delete (+ deletecollection
  # label-selector cleanup fallback) on down.
  rule {
    api_groups = [""]
    resources  = ["services"]
    verbs      = ["get", "list", "create", "patch", "delete", "deletecollection"]
  }

  # Read pod scheduling events to surface Pending/Evicted/OOM reasons during launch.
  rule {
    api_groups = [""]
    resources  = ["events"]
    verbs      = ["list"]
  }

  # PVC reads for tiers that mount pre-existing claims (FSx): SkyPilot GETs each
  # referenced PVC to check its phase before creating the pod. Read-only.
  dynamic "rule" {
    for_each = var.allow_pvc_read ? [1] : []
    content {
      api_groups = [""]
      resources  = ["persistentvolumeclaims"]
      verbs      = ["get", "list"]
    }
  }

  depends_on = [kubernetes_namespace_v1.pool]
}

resource "kubernetes_role_binding_v1" "pool" {
  metadata {
    name      = var.name
    namespace = var.namespace
    labels    = local.labels
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role_v1.pool.metadata[0].name
  }

  dynamic "subject" {
    for_each = var.subjects
    content {
      kind      = subject.value.kind
      name      = subject.value.name
      api_group = subject.value.api_group
    }
  }

  depends_on = [kubernetes_namespace_v1.pool]
}
