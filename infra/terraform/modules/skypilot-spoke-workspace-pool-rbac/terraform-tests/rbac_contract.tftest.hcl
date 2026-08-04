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
      for rule in kubernetes_cluster_role_v1.pool.rule : rule
      if toset(rule.api_groups) == toset([""]) &&
      toset(rule.resources) == toset(["namespaces"]) &&
      toset(rule.resource_names) == toset(["kube-system"]) &&
      toset(rule.verbs) == toset(["get"])
    ]) == 1
    error_message = "The cluster role must grant only named kube-system Namespace get for reserved-fill physical identity."
  }

  assert {
    condition = length([
      for rule in kubernetes_cluster_role_v1.pool.rule : rule
      if contains(rule.resources, "namespaces")
    ]) == 1
    error_message = "The module must not add a second or broader Namespace permission."
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

# Autodown runs INSIDE the node, as the pool ServiceAccount. Before this the
# module created that ServiceAccount and bound nothing to it, so `-i N --down`
# 403'd forever while `sky status` still advertised the autostop.
run "self_teardown_is_off_by_default" {
  command = plan

  assert {
    condition = (
      length(kubernetes_role_v1.pool_sa_self_teardown) == 0 &&
      length(kubernetes_role_binding_v1.pool_sa_self_teardown) == 0 &&
      output.self_teardown_role_name == null
    )
    error_message = "Self-teardown must be opt-in: it lets any pod in the namespace delete its neighbours' pods."
  }
}

run "self_teardown_binds_the_service_account_to_teardown_verbs_only" {
  command = plan

  variables {
    allow_self_teardown = true
  }

  # The binding must name the ServiceAccount itself. Binding only the control
  # plane's Group -- which is what shipped -- leaves the node unable to delete
  # itself, which is the whole bug.
  assert {
    condition = (
      length(kubernetes_role_binding_v1.pool_sa_self_teardown) == 1 &&
      length(kubernetes_role_binding_v1.pool_sa_self_teardown[0].subject) == 1 &&
      kubernetes_role_binding_v1.pool_sa_self_teardown[0].subject[0].kind == "ServiceAccount" &&
      kubernetes_role_binding_v1.pool_sa_self_teardown[0].subject[0].name == var.service_account_name &&
      kubernetes_role_binding_v1.pool_sa_self_teardown[0].subject[0].namespace == var.namespace
    )
    error_message = "The self-teardown binding must name the pool ServiceAccount in the pool namespace."
  }

  assert {
    condition     = kubernetes_role_binding_v1.pool_sa_self_teardown[0].role_ref[0].name == kubernetes_role_v1.pool_sa_self_teardown[0].metadata[0].name
    error_message = "The self-teardown binding must reference the self-teardown role."
  }

  # terminate_instances: filter_pods (list) then delete each pod.
  assert {
    condition = length([
      for rule in kubernetes_role_v1.pool_sa_self_teardown[0].rule : rule
      if toset(rule.api_groups) == toset([""]) &&
      toset(rule.resources) == toset(["pods"]) &&
      toset(rule.verbs) == toset(["get", "list", "delete"])
    ]) == 1
    error_message = "Self-teardown must grant exactly get/list/delete on namespaced pods."
  }

  # _delete_services, plus the label-selector deletecollection fallback.
  assert {
    condition = length([
      for rule in kubernetes_role_v1.pool_sa_self_teardown[0].rule : rule
      if toset(rule.api_groups) == toset([""]) &&
      toset(rule.resources) == toset(["services"]) &&
      toset(rule.verbs) == toset(["get", "list", "delete", "deletecollection"])
    ]) == 1
    error_message = "Self-teardown must let the node delete its own head Services."
  }

  # Nothing that would let a workload pod start or enter other pods.
  assert {
    condition = length([
      for rule in kubernetes_role_v1.pool_sa_self_teardown[0].rule : rule
      if length(setintersection(toset(rule.verbs), toset(["create", "patch", "update"]))) > 0 &&
      length(setintersection(toset(rule.resources), toset(["pods", "pods/exec", "pods/portforward"]))) > 0
    ]) == 0
    error_message = "Self-teardown must never grant create/patch on pods or any pods/exec, pods/portforward."
  }

  # The control-plane Role is untouched by the opt-in.
  assert {
    condition = length([
      for rule in kubernetes_role_v1.pool.rule : rule
      if toset(rule.resources) == toset(["pods"]) &&
      toset(rule.verbs) == toset(["get", "list", "create", "patch", "delete"])
    ]) == 1
    error_message = "Enabling self-teardown must not alter the control-plane role."
  }
}
