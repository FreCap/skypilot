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
          generation = 1
          name       = "training-all"
        }
        spec = {
          namespaceSelector = {}
        }
        status = {
          conditions = [{
            observedGeneration = 1
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
  partitions = [{
    namespace = "training"
  }]
}

run "accepts_an_existing_namespace" {
  command = plan

  variables {
    partitions = [{
      namespace        = "training"
      manage_namespace = false
    }]
  }

  assert {
    condition     = output.partitions == { training = "training" }
    error_message = "Disabling namespace ownership must retain the partition identity."
  }
}

run "kueue_is_a_backward_compatible_noop_by_default" {
  command = plan

  assert {
    condition = (
      length(data.kubernetes_resource.partition_cluster_queue) == 0 &&
      length(kubernetes_manifest.partition_local_queue) == 0 &&
      length(kubernetes_manifest.partition_kueue_policy) == 0 &&
      length(kubernetes_manifest.partition_kueue_binding) == 0 &&
      output.kueue_local_queues == {} &&
      module.rbac["training"].kueue == null
    )
    error_message = "Omitting partition.kueue must create no Kueue reads, resources, admission policy, or RBAC."
  }
}

run "rejects_an_invalid_local_queue_name" {
  command = plan

  variables {
    partitions = [{
      namespace = "training"
      kueue = {
        local_queue_name   = "Invalid_Queue"
        cluster_queue_name = "training"
      }
    }]
  }

  expect_failures = [var.partitions]
}

run "rejects_a_blank_cluster_queue_name" {
  command = plan

  variables {
    partitions = [{
      namespace = "training"
      kueue = {
        local_queue_name   = "training"
        cluster_queue_name = ""
      }
    }]
  }

  expect_failures = [var.partitions]
}

run "accepts_a_63_character_cluster_queue_dns_label" {
  command = plan

  variables {
    partitions = [{
      namespace = "training"
      kueue = {
        local_queue_name   = "training"
        cluster_queue_name = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      }
    }]
  }

  override_data {
    target = data.kubernetes_resource.partition_cluster_queue["training"]
    values = {
      object = {
        metadata = {
          generation = 1
          name       = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        }
        spec = {
          namespaceSelector = {}
        }
        status = {
          conditions = [{
            observedGeneration = 1
            status             = "True"
            type               = "Active"
          }]
        }
      }
    }
  }

  assert {
    condition     = kubernetes_manifest.partition_local_queue["training"].manifest["spec"]["clusterQueue"] == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    error_message = "A 63-character DNS-1123 label must remain a valid ClusterQueue name."
  }
}

run "rejects_a_dotted_cluster_queue_name" {
  command = plan

  variables {
    partitions = [{
      namespace = "training"
      kueue = {
        local_queue_name   = "training"
        cluster_queue_name = "training.reserved"
      }
    }]
  }

  expect_failures = [var.partitions]
}

run "rejects_an_uppercase_cluster_queue_name" {
  command = plan

  variables {
    partitions = [{
      namespace = "training"
      kueue = {
        local_queue_name   = "training"
        cluster_queue_name = "Training"
      }
    }]
  }

  expect_failures = [var.partitions]
}

run "rejects_an_underscore_cluster_queue_name" {
  command = plan

  variables {
    partitions = [{
      namespace = "training"
      kueue = {
        local_queue_name   = "training"
        cluster_queue_name = "training_reserved"
      }
    }]
  }

  expect_failures = [var.partitions]
}

run "rejects_an_overlength_cluster_queue_name" {
  command = plan

  variables {
    partitions = [{
      namespace = "training"
      kueue = {
        local_queue_name   = "training"
        cluster_queue_name = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      }
    }]
  }

  expect_failures = [var.partitions]
}

run "rejects_a_trailing_hyphen_cluster_queue_name" {
  command = plan

  variables {
    partitions = [{
      namespace = "training"
      kueue = {
        local_queue_name   = "training"
        cluster_queue_name = "training-"
      }
    }]
  }

  expect_failures = [var.partitions]
}

run "rejects_a_cluster_queue_that_cannot_prove_namespace_eligibility" {
  command = plan

  variables {
    partitions = [{
      namespace = "training"
      kueue = {
        local_queue_name   = "training"
        cluster_queue_name = "research-only"
      }
    }]
  }

  override_data {
    target = data.kubernetes_resource.partition_cluster_queue["training"]
    values = {
      object = {
        metadata = {
          generation = 1
          name       = "research-only"
        }
        spec = {
          namespaceSelector = {
            matchLabels = {
              "kubernetes.io/metadata.name" = "research"
            }
          }
        }
        status = {
          conditions = [{
            observedGeneration = 1
            status             = "True"
            type               = "Active"
          }]
        }
      }
    }
  }

  expect_failures = [kubernetes_manifest.partition_local_queue["training"]]
}

run "accepts_an_explicit_match_all_cluster_queue_selector" {
  command = plan

  variables {
    partitions = [{
      namespace = "training"
      kueue = {
        local_queue_name   = "training"
        cluster_queue_name = "training-all"
      }
    }]
  }

  assert {
    condition     = kubernetes_manifest.partition_local_queue["training"].manifest["spec"]["clusterQueue"] == "training-all"
    error_message = "An explicit empty namespaceSelector legitimately admits every namespace."
  }
}

run "rejects_an_omitted_cluster_queue_selector" {
  command = plan

  variables {
    partitions = [{
      namespace = "training"
      kueue = {
        local_queue_name   = "training"
        cluster_queue_name = "training-none"
      }
    }]
  }

  override_data {
    target = data.kubernetes_resource.partition_cluster_queue["training"]
    values = {
      object = {
        metadata = {
          generation = 1
          name       = "training-none"
        }
        spec = {}
        status = {
          conditions = [{
            observedGeneration = 1
            status             = "True"
            type               = "Active"
          }]
        }
      }
    }
  }

  expect_failures = [kubernetes_manifest.partition_local_queue["training"]]
}

run "rejects_a_stale_cluster_queue_active_condition" {
  command = plan

  variables {
    partitions = [{
      namespace = "training"
      kueue = {
        local_queue_name   = "training"
        cluster_queue_name = "training-all"
      }
    }]
  }

  override_data {
    target = data.kubernetes_resource.partition_cluster_queue["training"]
    values = {
      object = {
        metadata = {
          generation = 2
          name       = "training-all"
        }
        spec = {
          namespaceSelector = {}
        }
        status = {
          conditions = [{
            observedGeneration = 1
            status             = "True"
            type               = "Active"
          }]
        }
      }
    }
  }

  expect_failures = [kubernetes_manifest.partition_local_queue["training"]]
}

run "rejects_duplicate_cluster_queue_active_conditions" {
  command = plan

  variables {
    partitions = [{
      namespace = "training"
      kueue = {
        local_queue_name   = "training"
        cluster_queue_name = "training-all"
      }
    }]
  }

  override_data {
    target = data.kubernetes_resource.partition_cluster_queue["training"]
    values = {
      object = {
        metadata = {
          generation = 1
          name       = "training-all"
        }
        spec = {
          namespaceSelector = {}
        }
        status = {
          conditions = [
            {
              observedGeneration = 1
              status             = "True"
              type               = "Active"
            },
            {
              observedGeneration = 1
              status             = "False"
              type               = "Active"
            },
          ]
        }
      }
    }
  }

  expect_failures = [kubernetes_manifest.partition_local_queue["training"]]
}

run "rejects_a_deleting_cluster_queue" {
  command = plan

  variables {
    partitions = [{
      namespace = "training"
      kueue = {
        local_queue_name   = "training"
        cluster_queue_name = "training-all"
      }
    }]
  }

  override_data {
    target = data.kubernetes_resource.partition_cluster_queue["training"]
    values = {
      object = {
        metadata = {
          deletionTimestamp = "2026-08-10T00:00:00Z"
          generation        = 1
          name              = "training-all"
        }
        spec = {
          namespaceSelector = {}
        }
        status = {
          conditions = [{
            observedGeneration = 1
            status             = "True"
            type               = "Active"
          }]
        }
      }
    }
  }

  expect_failures = [kubernetes_manifest.partition_local_queue["training"]]
}

run "rejects_an_inactive_cluster_queue" {
  command = plan

  variables {
    partitions = [{
      namespace = "training"
      kueue = {
        local_queue_name   = "training"
        cluster_queue_name = "training-all"
      }
    }]
  }

  override_data {
    target = data.kubernetes_resource.partition_cluster_queue["training"]
    values = {
      object = {
        metadata = {
          generation = 1
          name       = "training-all"
        }
        spec = {
          namespaceSelector = {}
        }
        status = {
          conditions = [{
            observedGeneration = 1
            reason             = "FlavorNotFound"
            status             = "False"
            type               = "Active"
          }]
        }
      }
    }
  }

  expect_failures = [kubernetes_manifest.partition_local_queue["training"]]
}

run "rejects_an_empty_partition_set" {
  command = plan

  variables {
    partitions = []
  }

  expect_failures = [var.partitions]
}

run "rejects_duplicate_namespaces" {
  command = plan

  variables {
    partitions = [
      {
        namespace = "training"
        group     = "training-a"
      },
      {
        namespace = "training"
        group     = "training-b"
      },
    ]
  }

  expect_failures = [var.partitions]
}

run "rejects_duplicate_groups" {
  command = plan

  variables {
    partitions = [
      {
        namespace = "training-a"
        group     = "training"
      },
      {
        namespace = "training-b"
        group     = "training"
      },
    ]
  }

  expect_failures = [var.partitions]
}

run "rejects_lustre_without_a_mount_name" {
  command = plan

  variables {
    partitions = [{
      namespace = "training"
      fsx_volumes = [{
        claim_name    = "data"
        volume_handle = "fs-0123456789abcdef0"
        storage_class = "fsx-lustre"
        capacity      = "1200Gi"
      }]
    }]
  }

  expect_failures = [var.partitions]
}

run "rejects_openzfs_with_a_mount_name" {
  command = plan

  variables {
    partitions = [{
      namespace = "training"
      fsx_volumes = [{
        claim_name    = "data"
        volume_handle = "fs-0123456789abcdef0"
        storage_class = "fsx-openzfs"
        capacity      = "1200Gi"
        driver        = "fsx.openzfs.csi.aws.com"
        mountname     = "not-used"
      }]
    }]
  }

  expect_failures = [var.partitions]
}

run "rejects_an_unsupported_fsx_driver" {
  command = plan

  variables {
    partitions = [{
      namespace = "training"
      fsx_volumes = [{
        claim_name    = "data"
        volume_handle = "fs-0123456789abcdef0"
        storage_class = "unsupported"
        capacity      = "1200Gi"
        driver        = "example.csi.invalid"
      }]
    }]
  }

  expect_failures = [var.partitions]
}

run "rejects_public_cluster_api_ingress" {
  command = plan

  variables {
    cluster_api_ingress_cidrs = ["0.0.0.0/0"]
  }

  expect_failures = [var.cluster_api_ingress_cidrs]
}

run "rejects_invalid_cluster_api_ingress" {
  command = plan

  variables {
    cluster_api_ingress_cidrs = ["not-a-cidr"]
  }

  expect_failures = [var.cluster_api_ingress_cidrs]
}

run "rejects_duplicate_cluster_api_ingress" {
  command = plan

  variables {
    cluster_api_ingress_cidrs = ["10.30.0.0/16", "10.30.0.0/16"]
  }

  expect_failures = [var.cluster_api_ingress_cidrs]
}

run "rejects_duplicate_claims_in_one_namespace" {
  command = plan

  variables {
    partitions = [{
      namespace = "training"
      fsx_volumes = [
        {
          claim_name    = "data"
          volume_handle = "fs-0123456789abcdef0"
          storage_class = "fsx-lustre"
          capacity      = "1200Gi"
          mountname     = "abcd1234"
        },
        {
          claim_name    = "data"
          volume_handle = "fs-0fedcba9876543210"
          storage_class = "fsx-lustre"
          capacity      = "1200Gi"
          mountname     = "efgh5678"
        },
      ]
    }]
  }

  expect_failures = [var.partitions]
}

run "rejects_public_probe_ingress_by_default" {
  command = plan

  variables {
    serve_probe_ingress = {
      node_security_group_id = "sg-0123456789abcdef0"
      control_plane_cidr     = "0.0.0.0/0"
      port                   = 8000
    }
  }

  expect_failures = [var.serve_probe_ingress]
}

run "rejects_a_noncanonical_public_probe_ingress_by_default" {
  command = plan

  variables {
    serve_probe_ingress = {
      node_security_group_id = "sg-0123456789abcdef0"
      control_plane_cidr     = "10.20.30.40/0"
      port                   = 8000
    }
  }

  expect_failures = [var.serve_probe_ingress]
}

run "accepts_explicit_public_probe_ingress" {
  command = plan

  variables {
    serve_probe_ingress = {
      node_security_group_id = "sg-0123456789abcdef0"
      control_plane_cidr     = "0.0.0.0/0"
      port                   = 8000
      allow_public_cidr      = true
      description            = "Explicitly reviewed public SkyServe probe"
    }
  }

  assert {
    condition     = aws_security_group_rule.serve_probe_from_control_plane[0].cidr_blocks == tolist(["0.0.0.0/0"])
    error_message = "An explicit opt-in must preserve the requested public source."
  }
}

run "additional_ports_default_to_creating_no_extra_rule" {
  command = plan

  variables {
    serve_probe_ingress = {
      node_security_group_id = "sg-0123456789abcdef0"
      control_plane_cidr     = "10.30.0.0/16"
      port                   = 8080
    }
  }

  assert {
    condition     = length(aws_security_group_rule.serve_control_plane_additional) == 0
    error_message = "Omitting additional_ports must not widen an existing spoke."
  }
}

run "grants_the_ssh_port_a_kubernetes_launch_needs" {
  command = plan

  variables {
    serve_probe_ingress = {
      node_security_group_id = "sg-0123456789abcdef0"
      control_plane_cidr     = "10.30.0.0/16"
      port                   = 8080
      additional_ports       = [22]
    }
  }

  # The serving port keeps index [0] so an existing rule is not re-keyed, and
  # the SSH port arrives as a separate, independently addressed rule.
  assert {
    condition     = aws_security_group_rule.serve_probe_from_control_plane[0].from_port == 8080
    error_message = "The serving-port rule must keep its address and port."
  }

  assert {
    condition     = aws_security_group_rule.serve_control_plane_additional["22"].from_port == 22
    error_message = "A Kubernetes replica launch SSHes to the Pod IP; 22 must be granted."
  }

  assert {
    condition     = aws_security_group_rule.serve_control_plane_additional["22"].cidr_blocks == tolist(["10.30.0.0/16"])
    error_message = "The extra rule must reuse the reviewed control-plane CIDR."
  }
}

run "rejects_an_additional_port_repeating_the_serving_port" {
  command = plan

  variables {
    serve_probe_ingress = {
      node_security_group_id = "sg-0123456789abcdef0"
      control_plane_cidr     = "10.30.0.0/16"
      port                   = 8080
      additional_ports       = [22, 8080]
    }
  }

  expect_failures = [var.serve_probe_ingress]
}

run "rejects_an_out_of_range_additional_port" {
  command = plan

  variables {
    serve_probe_ingress = {
      node_security_group_id = "sg-0123456789abcdef0"
      control_plane_cidr     = "10.30.0.0/16"
      port                   = 8080
      additional_ports       = [70000]
    }
  }

  expect_failures = [var.serve_probe_ingress]
}

run "an_adopted_port_keeps_its_existing_aws_description" {
  command = plan

  variables {
    serve_probe_ingress = {
      node_security_group_id = "sg-0123456789abcdef0"
      control_plane_cidr     = "10.30.0.0/16"
      port                   = 8080
      additional_ports       = [22]
      additional_port_descriptions = {
        "22" = "SSH access from admin subnets (same VPC, different SG)"
      }
    }
  }

  # AWS identifies a rule by (protocol, ports, CIDR) and rejects a duplicate,
  # so a spoke that already carries the grant must import it. The import only
  # plans clean if the description matches the live rule byte for byte.
  assert {
    condition     = aws_security_group_rule.serve_control_plane_additional["22"].description == "SSH access from admin subnets (same VPC, different SG)"
    error_message = "An adopted rule must keep the description AWS already holds."
  }
}

run "an_unadopted_port_keeps_the_derived_description" {
  command = plan

  variables {
    serve_probe_ingress = {
      node_security_group_id = "sg-0123456789abcdef0"
      control_plane_cidr     = "10.30.0.0/16"
      port                   = 8080
      additional_ports       = [22, 2222]
      additional_port_descriptions = {
        "22" = "adopted"
      }
      description = "base"
    }
  }

  assert {
    condition     = aws_security_group_rule.serve_control_plane_additional["22"].description == "adopted"
    error_message = "An override must apply to exactly its own port."
  }

  assert {
    condition     = aws_security_group_rule.serve_control_plane_additional["2222"].description == "base (port 2222)"
    error_message = "A port without an override must keep the derived description."
  }
}

run "rejects_a_description_for_a_port_that_is_not_granted" {
  command = plan

  variables {
    serve_probe_ingress = {
      node_security_group_id       = "sg-0123456789abcdef0"
      control_plane_cidr           = "10.30.0.0/16"
      port                         = 8080
      additional_ports             = [22]
      additional_port_descriptions = { "443" = "typo" }
    }
  }

  expect_failures = [var.serve_probe_ingress]
}

run "rejects_a_controller_role_from_another_partition" {
  command = plan

  variables {
    controller_role_arn = "arn:aws-us-gov:iam::123456789012:role/skypilot-api"
  }

  expect_failures = [aws_eks_access_entry.pool]
}

run "rejects_a_pod_role_from_another_partition" {
  command = plan

  variables {
    partitions = [{
      namespace             = "training"
      pod_identity_role_arn = "arn:aws-cn:iam::210987654321:role/skypilot-training"
    }]
  }

  expect_failures = [aws_eks_pod_identity_association.pool_sa]
}
