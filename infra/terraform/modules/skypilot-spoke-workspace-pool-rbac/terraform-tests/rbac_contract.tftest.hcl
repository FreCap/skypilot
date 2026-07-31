mock_provider "kubernetes" {
  override_during = plan
}

variables {
  name                 = "skypilot-gpu-pool"
  namespace            = "skypilot-gpu"
  service_account_name = "skypilot-pool-sa"
  subjects = [{
    kind = "Group"
    name = "skypilot:gpu-pool"
  }]
}

run "default_permissions_match_the_skypilot_pool_contract" {
  command = plan

  assert {
    condition = (
      length(kubernetes_namespace_v1.pool) == 1 &&
      kubernetes_namespace_v1.pool[0].metadata[0].name == var.namespace &&
      kubernetes_service_account_v1.pool_sa.metadata[0].namespace == var.namespace
    )
    error_message = "The default contract must create the namespace and workload service account."
  }

  assert {
    condition = length([
      for rule in kubernetes_cluster_role_v1.pool.rule : rule
      if toset(rule.api_groups) == toset([""]) &&
      toset(rule.resources) == toset(["nodes"]) &&
      toset(rule.verbs) == toset(["get", "list"])
    ]) == 1
    error_message = "The cluster role must preserve node get/list access."
  }

  assert {
    condition = length([
      for rule in kubernetes_cluster_role_v1.pool.rule : rule
      if toset(rule.api_groups) == toset([""]) &&
      toset(rule.resources) == toset(["pods"]) &&
      toset(rule.verbs) == toset(["get", "list"])
    ]) == 1
    error_message = "The cluster role must preserve cluster-wide pod get/list access."
  }

  assert {
    condition = length([
      for rule in kubernetes_cluster_role_v1.pool.rule : rule
      if toset(rule.api_groups) == toset(["node.k8s.io"]) &&
      toset(rule.resources) == toset(["runtimeclasses"]) &&
      toset(rule.verbs) == toset(["list"])
    ]) == 1
    error_message = "The cluster role must preserve RuntimeClass list access."
  }

  assert {
    condition = length([
      for rule in kubernetes_role_v1.pool.rule : rule
      if toset(rule.api_groups) == toset([""]) &&
      toset(rule.resources) == toset(["pods"]) &&
      toset(rule.verbs) == toset(["get", "list", "create", "patch", "delete"])
    ]) == 1
    error_message = "The namespaced role must preserve the exact pod lifecycle permissions."
  }

  assert {
    condition = length([
      for rule in kubernetes_role_v1.pool.rule : rule
      if toset(rule.api_groups) == toset([""]) &&
      toset(rule.resources) == toset(["pods/exec", "pods/portforward"]) &&
      toset(rule.verbs) == toset(["create"])
    ]) == 1
    error_message = "The namespaced role must preserve exec and port-forward creation."
  }

  assert {
    condition = length([
      for rule in kubernetes_role_v1.pool.rule : rule
      if toset(rule.api_groups) == toset([""]) &&
      toset(rule.resources) == toset(["services"]) &&
      toset(rule.verbs) == toset(["get", "list", "create", "patch", "delete", "deletecollection"])
    ]) == 1
    error_message = "The namespaced role must preserve the exact service lifecycle permissions."
  }

  assert {
    condition = length([
      for rule in kubernetes_role_v1.pool.rule : rule
      if toset(rule.api_groups) == toset([""]) &&
      toset(rule.resources) == toset(["events"]) &&
      toset(rule.verbs) == toset(["list"])
    ]) == 1
    error_message = "The namespaced role must preserve event-list permission."
  }

  assert {
    condition = length([
      for rule in kubernetes_role_v1.pool.rule : rule
      if contains(rule.resources, "persistentvolumeclaims")
    ]) == 0
    error_message = "PVC reads must remain disabled by default."
  }

  assert {
    condition = (
      output.namespace == "skypilot-gpu" &&
      output.service_account_name == "skypilot-pool-sa" &&
      output.cluster_role_name == "skypilot-gpu-pool" &&
      output.role_name == "skypilot-gpu-pool"
    )
    error_message = "The module must expose the managed workload and RBAC identities."
  }
}

run "pvc_reads_are_opt_in_and_read_only" {
  command = plan

  variables {
    allow_pvc_read = true
  }

  assert {
    condition = length([
      for rule in kubernetes_role_v1.pool.rule : rule
      if toset(rule.api_groups) == toset([""]) &&
      toset(rule.resources) == toset(["persistentvolumeclaims"]) &&
      toset(rule.verbs) == toset(["get", "list"])
    ]) == 1
    error_message = "Enabling PVC support must add only get/list on namespaced claims."
  }
}

run "an_external_namespace_preserves_the_other_resource_shapes" {
  command = plan

  variables {
    manage_namespace = false
  }

  assert {
    condition = (
      length(kubernetes_namespace_v1.pool) == 0 &&
      kubernetes_service_account_v1.pool_sa.metadata[0].namespace == var.namespace &&
      kubernetes_role_v1.pool.metadata[0].namespace == var.namespace
    )
    error_message = "Disabling namespace ownership must not disable the namespaced service account or RBAC."
  }
}
