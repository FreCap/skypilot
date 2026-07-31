output "api_server_role_arn" {
  description = <<-EOT
    Role ARN of the SkyPilot API server service account. Feed this into a pool
    module's `controller_role_arn` so the pool grants it an EKS access entry or
    cross-account AssumeRole.
  EOT
  value       = aws_iam_role.api_server.arn
}

output "api_server_role_name" {
  description = "Name of the API-server EKS Pod Identity role."
  value       = aws_iam_role.api_server.name
}

output "api_service_account_name" {
  description = "Kubernetes service account name the API server runs as."
  value       = local.api_service_account_name
}

output "namespace" {
  description = "Namespace the SkyPilot API server is deployed into."
  value       = var.namespace
}

output "api_server_oidc_issuer" {
  description = <<-EOT
    OIDC issuer URL of the host EKS cluster the API server runs on. Feed this into a
    GCP VM pool's `controller_oidc_issuer` so it can federate the API server's
    projected Kubernetes ServiceAccount token. Pair it with the API server
    subject `system:serviceaccount:<namespace>:<api_service_account_name>`.
  EOT
  value       = data.aws_eks_cluster.host.identity[0].oidc[0].issuer
}

output "release_name" {
  description = "Helm release name."
  value       = var.release_name
}

output "config_generation" {
  description = "Hash identifying the desired DB-backed config seed generation."
  value       = local.config_hash
}

output "host_cluster_provider_config" {
  description = "EKS endpoint, CA data, and AWS exec arguments used by root Kubernetes and Helm providers."
  value = {
    endpoint = local.host_endpoint
    ca_data  = local.host_ca_b64
    exec = {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "aws"
      args        = local.exec_args
      env         = local.provider_exec_env
    }
  }
}
