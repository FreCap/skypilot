# Discover the existing EKS cluster used to host the SkyPilot control plane.
#
# The host data source is UNCONDITIONAL (host_cluster_name is required and points
# at a live cluster). Root callers configure and pass the Kubernetes and Helm
# providers; the endpoint, CA, and exec-auth values below are exposed as outputs
# so Terragrunt roots can generate the same short-lived EKS authentication.

data "aws_eks_cluster" "host" {
  name = var.host_cluster_name
}

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}
data "aws_region" "current" {}

locals {
  host_endpoint = data.aws_eks_cluster.host.endpoint
  host_ca_b64   = data.aws_eks_cluster.host.certificate_authority[0].data

  # The chart names the API server service account <release_name>-api-sa.
  api_service_account_name = "${var.release_name}-api-sa"

  exec_args = ["eks", "get-token", "--cluster-name", var.host_cluster_name, "--region", var.aws_region]

  # Lets operators select an AWS profile when reaching the host cluster
  # (empty by default means ambient credentials).
  provider_exec_env = var.aws_profile != null && trimspace(var.aws_profile) != "" ? {
    AWS_PROFILE = trimspace(var.aws_profile)
  } : {}

}
