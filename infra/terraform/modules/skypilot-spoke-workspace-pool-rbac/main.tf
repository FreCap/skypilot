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
#  - kube-system Namespace: protocol-v2 reserved fill reads this one immutable
#    Namespace UID to deduplicate aliases and fence a context retarget. No Namespace
#    list/watch or access to other Namespace objects is needed.
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

  rule {
    api_groups     = [""]
    resources      = ["namespaces"]
    resource_names = ["kube-system"]
    verbs          = ["get"]
  }

  # Required-Kueue preflight follows LocalQueue -> ClusterQueue and evaluates
  # the latter's selector against this Namespace on every launch. Both reads
  # remain exact-name and cluster-scoped; no list or mutation is needed.
  dynamic "rule" {
    for_each = var.kueue == null ? [] : [var.kueue.cluster_queue_name]
    content {
      api_groups     = ["kueue.x-k8s.io"]
      resources      = ["clusterqueues"]
      resource_names = [rule.value]
      verbs          = ["get"]
    }
  }

  dynamic "rule" {
    for_each = var.kueue == null ? [] : [var.namespace]
    content {
      api_groups     = [""]
      resources      = ["namespaces"]
      resource_names = [rule.value]
      verbs          = ["get"]
    }
  }

  # API discovery selects v1beta2 with a v1beta1 compatibility fallback. The
  # generated Python client currently calls /apis/; grant both exact spellings
  # to remain compatible without exposing discovery subpaths. Most clusters
  # already grant these through system:discovery, but hardened clusters may not.
  dynamic "rule" {
    for_each = var.kueue == null ? [] : [1]
    content {
      non_resource_urls = ["/apis", "/apis/"]
      verbs             = ["get"]
    }
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

  # SkyPilot checks this exact queue before Pod creation. It does not need to
  # enumerate or mutate queues, and workload Pods must not receive this grant.
  dynamic "rule" {
    for_each = var.kueue == null ? [] : [var.kueue.local_queue_name]
    content {
      api_groups     = ["kueue.x-k8s.io"]
      resources      = ["localqueues"]
      resource_names = [rule.value]
      verbs          = ["get"]
    }
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

# Self-teardown for the pod ServiceAccount.
#
# Everything above is bound to `var.subjects` -- the CONTROL PLANE's identity.
# That covers every operation the API server drives. It does not cover the one
# SkyPilot operation that runs from inside the node, as the pod's own
# ServiceAccount: the idle-timer teardown. `StopEvent` -> `terminate_instances`
# lists the cluster's pods and deletes them plus their head Services, using
# in-cluster credentials. With no binding for the ServiceAccount those calls
# 403 every 60s forever; SkyPilot swallows the error to keep the skylet alive
# and the API server keeps reporting a serene `AUTOSTOP Nm (down)`.
#
# Deliberately a SEPARATE, smaller Role rather than adding the ServiceAccount to
# the subjects of the one above: that Role carries pods `create`, `pods/exec`
# and `pods/portforward`, which would let any workload pod start pods and exec
# into its neighbours. Teardown needs none of that.
#
# Unconditional. It briefly shipped as an opt-in flag defaulting to false,
# because RBAC cannot scope `delete pods` to "pods this cluster owns" (names
# are dynamic; verbs take no label selector), so in a namespace shared by
# several users one SkyPilot workload can delete another's SkyPilot pods. That
# reservation was misplaced: a pool namespace is not a tenant boundary and this
# module never claimed it was -- the caller's `partitions` documentation says
# so outright, and the control-plane subjects already hold pods create/exec/
# portforward here. Meanwhile a pool without this grant cannot honour
# `-i N --down` at all, and an operator has no way to notice until a cluster
# has been idling for hours. A knob whose only correct value is `true` is a
# footgun, so there is no knob.
#
# Verbs derived from sky/provision/kubernetes/instance.py:
#   pods         list (filter_pods) + get/delete (_terminate_node)
#   services     delete (_delete_services) + deletecollection over a label
#                selector (_delete_cluster_services), which also reads
#   events       create -- the best-effort "Cluster is autodowning." breadcrumb
#                the server reads back to attribute the termination
#   deployments  list only -- the high-availability-controller probe, which is
#                already wrapped in try/except; granting it just removes 403
#                noise. No delete: HA controllers are control-plane managed.
resource "kubernetes_role_v1" "pool_sa_self_teardown" {
  metadata {
    name      = "${var.name}-self-teardown"
    namespace = var.namespace
    labels    = local.labels
  }

  rule {
    api_groups = [""]
    resources  = ["pods"]
    verbs      = ["get", "list", "delete"]
  }

  rule {
    api_groups = [""]
    resources  = ["services"]
    verbs      = ["get", "list", "delete", "deletecollection"]
  }

  rule {
    api_groups = [""]
    resources  = ["events"]
    verbs      = ["create"]
  }

  rule {
    api_groups = ["apps"]
    resources  = ["deployments"]
    verbs      = ["list"]
  }

  depends_on = [kubernetes_namespace_v1.pool]
}

resource "kubernetes_role_binding_v1" "pool_sa_self_teardown" {
  metadata {
    name      = "${var.name}-self-teardown"
    namespace = var.namespace
    labels    = local.labels
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role_v1.pool_sa_self_teardown.metadata[0].name
  }

  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account_v1.pool_sa.metadata[0].name
    namespace = var.namespace
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

# The self-teardown pair shipped behind `count = var.allow_self_teardown ? 1 : 0`
# and is now unconditional, which renames the state addresses from `[0]` to
# unindexed. Without these, an already-applied pool would destroy and recreate
# its Role and RoleBinding -- a window in which a node that hits its idle timer
# cannot delete itself. A pool that never opted in has nothing at `[0]`, so the
# blocks are a no-op there.
moved {
  from = kubernetes_role_v1.pool_sa_self_teardown[0]
  to   = kubernetes_role_v1.pool_sa_self_teardown
}

moved {
  from = kubernetes_role_binding_v1.pool_sa_self_teardown[0]
  to   = kubernetes_role_binding_v1.pool_sa_self_teardown
}
