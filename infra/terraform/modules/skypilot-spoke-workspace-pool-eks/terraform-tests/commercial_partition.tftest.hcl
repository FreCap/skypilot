mock_provider "aws" {
  override_during = plan

  mock_data "aws_partition" {
    defaults = {
      dns_suffix         = "amazonaws.com"
      id                 = "aws"
      partition          = "aws"
      reverse_dns_prefix = "com.amazonaws"
    }
  }

  mock_data "aws_eks_cluster" {
    defaults = {
      arn      = "arn:aws:eks:us-east-2:210987654321:cluster/gpu-pool"
      endpoint = "https://example.eks.amazonaws.com"
      id       = "gpu-pool"
      vpc_config = [{
        cluster_security_group_id = "sg-0fedcba9876543210"
      }]
    }
  }
}

mock_provider "kubernetes" {
  override_during = plan

  mock_data "kubernetes_resource" {
    defaults = {
      object = {
        metadata = {
          generation = 4
          name       = "inference-borrower"
        }
        spec = {
          namespaceSelector = {
            matchLabels = {
              "kubernetes.io/metadata.name" = "inference"
            }
          }
        }
        status = {
          conditions = [{
            observedGeneration = 4
            status             = "True"
            type               = "Active"
          }]
        }
      }
    }
  }
}

variables {
  aws_region          = "us-east-2"
  eks_cluster_name    = "gpu-pool"
  controller_role_arn = "arn:aws:iam::123456789012:role/skypilot-api"
  cluster_api_ingress_cidrs = [
    "10.20.0.0/16",
    "10.30.0.0/16",
  ]
  partitions = [
    {
      namespace                    = "training"
      group                        = "training"
      pod_identity_role_arn        = "arn:aws:iam::210987654321:role/skypilot-training"
      pod_identity_service_account = "skypilot-pool-sa"
      priority_class = {
        value = -1000
        name  = "training-low"
      }
      fsx_volumes = [{
        claim_name    = "training-data"
        volume_handle = "fs-0123456789abcdef0"
        storage_class = "fsx-lustre"
        capacity      = "1200Gi"
        mountname     = "abcd1234"
      }]
    },
    {
      namespace        = "inference"
      manage_namespace = false
      kueue = {
        cluster_queue_name = "inference-borrower"
      }
      fsx_volumes = [{
        claim_name    = "models"
        volume_handle = "fs-0fedcba9876543210"
        storage_class = "fsx-openzfs"
        capacity      = "1024Gi"
        driver        = "fsx.openzfs.csi.aws.com"
      }]
    },
  ]
  serve_probe_ingress = {
    node_security_group_id = "sg-0123456789abcdef0"
    control_plane_cidr     = "10.20.0.0/16"
    port                   = 8000
  }
}

run "commercial_pool_preserves_partition_identity_shapes" {
  command = plan

  assert {
    condition     = aws_eks_access_entry.pool.principal_arn == var.controller_role_arn
    error_message = "The access entry must preserve the exact controller role ARN."
  }

  assert {
    condition     = toset(keys(module.rbac)) == toset(["training", "inference"])
    error_message = "RBAC module instances must remain keyed by partition namespace."
  }

  assert {
    condition     = toset(keys(aws_eks_pod_identity_association.pool_sa)) == toset(["training"])
    error_message = "Only partitions with a nonempty role ARN may create Pod Identity associations."
  }

  assert {
    condition     = aws_eks_pod_identity_association.pool_sa["training"].role_arn == "arn:aws:iam::210987654321:role/skypilot-training"
    error_message = "The Pod Identity association must preserve the exact commercial AWS role ARN."
  }

  assert {
    condition     = toset(keys(kubernetes_persistent_volume_v1.fsx)) == toset(["training/training-data", "inference/models"])
    error_message = "FSx resources must remain keyed by namespace/claim name."
  }

  assert {
    condition     = local.fsx_volumes["training/training-data"].dnsname == "fs-0123456789abcdef0.fsx.us-east-2.amazonaws.com"
    error_message = "Commercial AWS FSx DNS names must use the active partition suffix."
  }

  assert {
    condition     = local.fsx_volumes["inference/models"].dnsname == "fs-0fedcba9876543210.fsx.us-east-2.amazonaws.com"
    error_message = "OpenZFS must use the same partition-aware FSx endpoint derivation."
  }

  assert {
    condition     = kubernetes_priority_class_v1.partition["training"].metadata[0].name == "training-low"
    error_message = "PriorityClass instances must remain keyed by partition namespace."
  }

  assert {
    condition     = aws_security_group_rule.serve_probe_from_control_plane[0].description == "SkyServe probe traffic from the control plane"
    error_message = "The probe rule must use the reusable default description."
  }

  assert {
    condition = (
      aws_security_group_rule.cluster_api_from_control_plane[0].security_group_id == "sg-0fedcba9876543210" &&
      aws_security_group_rule.cluster_api_from_control_plane[0].cidr_blocks == tolist(["10.20.0.0/16", "10.30.0.0/16"])
    )
    error_message = "Private API ingress must target the EKS-managed cluster security group and preserve the reviewed CIDRs."
  }

  assert {
    condition = output.partitions == {
      inference = "inference"
      training  = "training"
    }
    error_message = "The partition output must expose the effective RBAC groups."
  }

  assert {
    condition     = toset(keys(kubernetes_manifest.partition_local_queue)) == toset(["inference"])
    error_message = "Only partitions that configure Kueue may create a LocalQueue."
  }

  assert {
    condition = (
      kubernetes_manifest.partition_local_queue["inference"].manifest["apiVersion"] == "kueue.x-k8s.io/v1beta2" &&
      kubernetes_manifest.partition_local_queue["inference"].manifest["kind"] == "LocalQueue" &&
      kubernetes_manifest.partition_local_queue["inference"].manifest["metadata"]["name"] == "default" &&
      kubernetes_manifest.partition_local_queue["inference"].manifest["metadata"]["namespace"] == "inference" &&
      kubernetes_manifest.partition_local_queue["inference"].manifest["spec"]["clusterQueue"] == "inference-borrower"
    )
    error_message = "The LocalQueue must preserve the exact configured namespace, name, API, and ClusterQueue."
  }

  assert {
    condition = (
      length(kubernetes_manifest.partition_local_queue["inference"].wait) == 1 &&
      length(kubernetes_manifest.partition_local_queue["inference"].wait[0].condition) == 1 &&
      kubernetes_manifest.partition_local_queue["inference"].wait[0].condition[0].type == "Active" &&
      kubernetes_manifest.partition_local_queue["inference"].wait[0].condition[0].status == "True"
    )
    error_message = "Terraform must not finish the partition apply until the LocalQueue reports Active=True."
  }

  assert {
    condition = (
      toset(keys(kubernetes_manifest.partition_kueue_policy)) == toset(["inference"]) &&
      toset(keys(kubernetes_manifest.partition_kueue_binding)) == toset(["inference"]) &&
      strcontains(
        kubernetes_manifest.partition_kueue_policy["inference"].manifest["spec"]["validations"][0]["expression"],
        "object.metadata.labels['kueue.x-k8s.io/queue-name'] == 'default'",
      ) &&
      strcontains(
        kubernetes_manifest.partition_kueue_policy["inference"].manifest["spec"]["validations"][0]["expression"],
        "object.metadata.labels['kueue.x-k8s.io/managed'] == 'true'",
      )
    )
    error_message = "A configured Kueue partition must reject Pods that omit/misname its exact queue or bypass Kueue mutation."
  }

  assert {
    condition = output.kueue_local_queues == {
      inference = {
        api_version        = "kueue.x-k8s.io/v1beta2"
        cluster_queue_name = "inference-borrower"
        local_queue_name   = "default"
      }
    }
    error_message = "The Kueue output must expose only the verified namespace-to-queue contract."
  }

  assert {
    condition = (
      module.rbac["inference"].kueue.local_queue_name == "default" &&
      module.rbac["inference"].kueue.cluster_queue_name == "inference-borrower" &&
      module.rbac["training"].kueue == null
    )
    error_message = "Only the configured partition may receive LocalQueue preflight RBAC."
  }
}

# The pool ServiceAccount is what a node runs as, so it is the identity that
# performs an idle teardown. Every partition gets it: a pool namespace whose
# nodes cannot delete themselves silently breaks `sky launch -i N --down`.
run "every_partition_gets_node_self_teardown" {
  command = plan

  assert {
    condition = (
      module.rbac["training"].self_teardown_role_name == "training-self-teardown" &&
      module.rbac["inference"].self_teardown_role_name == "inference-self-teardown"
    )
    error_message = "Every partition must receive the node self-teardown grant."
  }
}
