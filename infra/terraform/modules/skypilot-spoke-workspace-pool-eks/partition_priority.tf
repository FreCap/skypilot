# A partition can opt into an exact PriorityClass. The admission policy does not
# inject or default the class: it rejects pods that omit it or name another
# class. The target cluster must support ValidatingAdmissionPolicy v1.

locals {
  priority_partitions = {
    for partition in var.partitions : partition.namespace => {
      value = partition.priority_class.value
      name  = coalesce(partition.priority_class.name, "${partition.namespace}-low")
    } if partition.priority_class != null
  }
}

resource "kubernetes_priority_class_v1" "partition" {
  for_each = local.priority_partitions

  metadata {
    name = each.value.name
  }
  value             = each.value.value
  preemption_policy = "Never"
  description       = "SkyPilot ${each.key} partition: low, preemptible priority."
}

resource "kubernetes_manifest" "partition_priority_policy" {
  for_each = local.priority_partitions

  manifest = {
    apiVersion = "admissionregistration.k8s.io/v1"
    kind       = "ValidatingAdmissionPolicy"
    metadata   = { name = "${each.key}-priority" }
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
        expression = "has(object.spec.priorityClassName) && object.spec.priorityClassName == '${each.value.name}'"
        message    = "pods in ${each.key} must set priorityClassName '${each.value.name}'"
      }]
    }
  }

  depends_on = [kubernetes_priority_class_v1.partition]
}

resource "kubernetes_manifest" "partition_priority_binding" {
  for_each = local.priority_partitions

  manifest = {
    apiVersion = "admissionregistration.k8s.io/v1"
    kind       = "ValidatingAdmissionPolicyBinding"
    metadata   = { name = "${each.key}-priority" }
    spec = {
      policyName        = "${each.key}-priority"
      validationActions = ["Deny"]
      matchResources = {
        namespaceSelector = {
          matchLabels = { "kubernetes.io/metadata.name" = each.key }
        }
      }
    }
  }

  depends_on = [kubernetes_manifest.partition_priority_policy]
}
