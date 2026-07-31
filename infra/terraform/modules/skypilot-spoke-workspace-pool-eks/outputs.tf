output "partitions" {
  description = "Partition namespace to RBAC group mapping."
  value       = { for partition in local.partitions : partition.namespace => partition.group }
}

output "access_entry_id" {
  description = "ID of the EKS access entry that maps the controller principal."
  value       = aws_eks_access_entry.pool.id
}

output "partition_service_accounts" {
  description = "Partition namespace to workload service-account name mapping."
  value = {
    for partition in local.partitions :
    partition.namespace => partition.pod_identity_service_account
  }
}

output "fsx_claims" {
  description = "Static FSx volume identity keys to namespaced PVC names."
  value = {
    for key, volume in local.fsx_volumes :
    key => {
      namespace  = volume.namespace
      claim_name = volume.claim_name
    }
  }
}

output "priority_classes" {
  description = "Partition namespace to enforced PriorityClass name mapping."
  value       = { for namespace, priority in local.priority_partitions : namespace => priority.name }
}
