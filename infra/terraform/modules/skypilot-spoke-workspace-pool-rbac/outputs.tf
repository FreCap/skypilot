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
