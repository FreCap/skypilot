mock_provider "aws" {
  override_during = plan

  mock_data "aws_partition" {
    defaults = {
      dns_suffix         = "amazonaws.com"
      id                 = "aws-us-gov"
      partition          = "aws-us-gov"
      reverse_dns_prefix = "com.amazonaws"
    }
  }

  mock_data "aws_eks_cluster" {
    defaults = {
      arn      = "arn:aws-us-gov:eks:us-gov-west-1:210987654321:cluster/gov-pool"
      endpoint = "https://example.eks.amazonaws.com"
      id       = "gov-pool"
    }
  }
}

mock_provider "kubernetes" {
  override_during = plan
}

variables {
  aws_region          = "us-gov-west-1"
  eks_cluster_name    = "gov-pool"
  controller_role_arn = "arn:aws-us-gov:iam::123456789012:role/skypilot-api"
  partitions = [{
    namespace             = "training"
    pod_identity_role_arn = "arn:aws-us-gov:iam::210987654321:role/skypilot-training"
    fsx_volumes = [{
      claim_name    = "training-data"
      volume_handle = "fs-0123456789abcdef0"
      storage_class = "fsx-lustre"
      capacity      = "1200Gi"
      mountname     = "abcd1234"
    }]
  }]
}

run "govcloud_uses_partition_arns_and_dns" {
  command = plan

  assert {
    condition     = aws_eks_access_entry.pool.principal_arn == "arn:aws-us-gov:iam::123456789012:role/skypilot-api"
    error_message = "GovCloud must preserve the exact controller role ARN."
  }

  assert {
    condition     = aws_eks_pod_identity_association.pool_sa["training"].role_arn == "arn:aws-us-gov:iam::210987654321:role/skypilot-training"
    error_message = "GovCloud must preserve the exact Pod Identity role ARN."
  }

  assert {
    condition     = local.fsx_volumes["training/training-data"].dnsname == "fs-0123456789abcdef0.fsx.us-gov-west-1.amazonaws.com"
    error_message = "GovCloud FSx DNS names must use the active partition suffix."
  }
}
