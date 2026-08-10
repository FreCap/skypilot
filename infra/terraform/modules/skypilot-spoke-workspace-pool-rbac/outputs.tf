output "namespace" {
  description = "Namespace SkyPilot launches pool workloads into."
  value       = var.namespace
}

output "service_account_name" {
  description = "Name of the service account created for SkyPilot workloads."
  value       = kubernetes_service_account_v1.pool_sa.metadata[0].name
}

output "cluster_role_name" {
  description = "Name shared by the cluster role and its binding."
  value       = kubernetes_cluster_role_v1.pool.metadata[0].name
}

output "role_name" {
  description = "Name shared by the namespaced role and its binding."
  value       = kubernetes_role_v1.pool.metadata[0].name
}

output "self_teardown_role_name" {
  description = <<-EOT
    Name shared by the pod ServiceAccount's self-teardown role and its binding
    -- the grant that lets a node honour `sky launch -i N --down`.
  EOT
  value       = kubernetes_role_v1.pool_sa_self_teardown.metadata[0].name
}

output "kueue" {
  description = "Exact Kueue LocalQueue and ClusterQueue names readable by the control-plane subjects, or null."
  value       = var.kueue
}
