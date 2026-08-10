# A LocalQueue is the namespace-local admission entry point used by SkyPilot.
# ClusterQueue quota, cohort, flavor, TAS, and preemption remain operator-owned
# policy and must exist before this module is applied.
data "kubernetes_resource" "partition_cluster_queue" {
  for_each = local.kueue_partitions

  api_version = "kueue.x-k8s.io/v1beta2"
  kind        = "ClusterQueue"

  metadata {
    name = each.value.cluster_queue_name
  }
}

locals {
  # LocalQueue Active=True only mirrors the target ClusterQueue's Active
  # condition in Kueue 0.18. The scheduler checks namespaceSelector later, so
  # an active LocalQueue can still be permanently inadmissible. Accept only the
  # two selector shapes whose result this module can prove without guessing
  # externally managed Namespace labels: match all, or this exact namespace.
  kueue_cluster_queue_selector_is_proven = {
    for namespace, queue in local.kueue_partitions : namespace => (
      try(data.kubernetes_resource.partition_cluster_queue[namespace].object.metadata.name, null) == queue.cluster_queue_name &&
      try(data.kubernetes_resource.partition_cluster_queue[namespace].object.spec.namespaceSelector, null) != null &&
      (
        (
          try(length(keys(data.kubernetes_resource.partition_cluster_queue[namespace].object.spec.namespaceSelector.matchLabels)), 0) == 0 &&
          try(length(data.kubernetes_resource.partition_cluster_queue[namespace].object.spec.namespaceSelector.matchExpressions), 0) == 0
          ) || (
          try(length(keys(data.kubernetes_resource.partition_cluster_queue[namespace].object.spec.namespaceSelector.matchLabels)), 0) == 1 &&
          try(data.kubernetes_resource.partition_cluster_queue[namespace].object.spec.namespaceSelector.matchLabels["kubernetes.io/metadata.name"], null) == namespace &&
          try(length(data.kubernetes_resource.partition_cluster_queue[namespace].object.spec.namespaceSelector.matchExpressions), 0) == 0
        )
      )
    )
  }

  kueue_cluster_queue_is_currently_active = {
    for namespace, queue in local.kueue_partitions : namespace => (
      try(data.kubernetes_resource.partition_cluster_queue[namespace].object.metadata.name, null) == queue.cluster_queue_name &&
      try(data.kubernetes_resource.partition_cluster_queue[namespace].object.metadata.deletionTimestamp, null) == null &&
      try(length([
        for condition in data.kubernetes_resource.partition_cluster_queue[namespace].object.status.conditions : condition
        if condition.type == "Active"
      ]) == 1, false) &&
      try(length([
        for condition in data.kubernetes_resource.partition_cluster_queue[namespace].object.status.conditions : condition
        if condition.type == "Active" &&
        condition.status == "True" &&
        try(condition.observedGeneration, -1) == data.kubernetes_resource.partition_cluster_queue[namespace].object.metadata.generation
      ]) == 1, false)
    )
  }
}

# Waiting for Active=True catches an absent or inactive ClusterQueue after the
# stronger namespace-selector precondition above has proven eligibility.
resource "kubernetes_manifest" "partition_local_queue" {
  for_each = local.kueue_partitions

  manifest = {
    apiVersion = "kueue.x-k8s.io/v1beta2"
    kind       = "LocalQueue"
    metadata = {
      name      = each.value.local_queue_name
      namespace = each.key
    }
    spec = {
      clusterQueue = each.value.cluster_queue_name
    }
  }

  wait {
    condition {
      type   = "Active"
      status = "True"
    }
  }

  lifecycle {
    precondition {
      condition     = local.kueue_cluster_queue_is_currently_active[each.key]
      error_message = "ClusterQueue ${each.value.cluster_queue_name} must exist, not be deleting, and report current-generation Active=True before creating LocalQueue ${each.key}/${each.value.local_queue_name}."
    }

    precondition {
      condition     = local.kueue_cluster_queue_selector_is_proven[each.key]
      error_message = "ClusterQueue ${each.value.cluster_queue_name} must select partition namespace ${each.key} with either an empty namespaceSelector or exactly kubernetes.io/metadata.name=${each.key}."
    }
  }

  depends_on = [module.rbac]
}

# Protect clients that do not implement SkyPilot's synchronous Kueue
# attestation. The policy requires the exact configured queue; merely supplying
# a nonempty but misspelled queue name still fails admission.
resource "kubernetes_manifest" "partition_kueue_policy" {
  for_each = local.kueue_partitions

  manifest = {
    apiVersion = "admissionregistration.k8s.io/v1"
    kind       = "ValidatingAdmissionPolicy"
    metadata   = { name = "${each.key}-kueue-queue" }
    spec = {
      failurePolicy = "Fail"
      matchConstraints = {
        resourceRules = [{
          apiGroups   = [""]
          apiVersions = ["v1"]
          operations  = ["CREATE", "UPDATE"]
          resources   = ["pods"]
        }]
      }
      validations = [{
        expression = "has(object.metadata.labels) && 'kueue.x-k8s.io/queue-name' in object.metadata.labels && object.metadata.labels['kueue.x-k8s.io/queue-name'] == '${each.value.local_queue_name}' && 'kueue.x-k8s.io/managed' in object.metadata.labels && object.metadata.labels['kueue.x-k8s.io/managed'] == 'true'"
        message    = "pods in ${each.key} must be admitted by Kueue through LocalQueue '${each.value.local_queue_name}'"
      }]
    }
  }
}

resource "kubernetes_manifest" "partition_kueue_binding" {
  for_each = local.kueue_partitions

  manifest = {
    apiVersion = "admissionregistration.k8s.io/v1"
    kind       = "ValidatingAdmissionPolicyBinding"
    metadata   = { name = "${each.key}-kueue-queue" }
    spec = {
      policyName        = "${each.key}-kueue-queue"
      validationActions = ["Deny"]
      matchResources = {
        namespaceSelector = {
          matchLabels = { "kubernetes.io/metadata.name" = each.key }
        }
      }
    }
  }

  # Do not activate the fail-closed binding until the queue has reconciled.
  depends_on = [
    kubernetes_manifest.partition_kueue_policy,
    kubernetes_manifest.partition_local_queue,
  ]
}
