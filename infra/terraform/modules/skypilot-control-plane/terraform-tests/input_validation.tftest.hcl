mock_provider "aws" {
  override_during = plan

  mock_data "aws_caller_identity" {
    defaults = {
      account_id = "123456789012"
      arn        = "arn:aws:iam::123456789012:role/terraform-test"
      id         = "123456789012"
      user_id    = "terraform-test"
    }
  }

  mock_data "aws_partition" {
    defaults = {
      dns_suffix         = "amazonaws.com"
      id                 = "aws"
      partition          = "aws"
      reverse_dns_prefix = "com.amazonaws"
    }
  }

  mock_data "aws_region" {
    defaults = {
      description = "US East (N. Virginia)"
      endpoint    = "ec2.us-east-1.amazonaws.com"
      id          = "us-east-1"
      name        = "us-east-1"
      region      = "us-east-1"
    }
  }

  mock_data "aws_eks_cluster" {
    defaults = {
      arn      = "arn:aws:eks:us-east-1:123456789012:cluster/platform-eks"
      endpoint = "https://example.eks.amazonaws.com"
      id       = "platform-eks"
      certificate_authority = [{
        data = "dGVzdC1jYQ=="
      }]
      identity = [{
        oidc = [{
          issuer = "https://oidc.eks.us-east-1.amazonaws.com/id/example"
        }]
      }]
    }
  }

  mock_data "aws_iam_policy_document" {
    defaults = {
      id            = "terraform-test-policy"
      json          = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
      minified_json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }
}

mock_provider "kubernetes" {
  override_during = plan
}

mock_provider "helm" {
  override_during = plan
}

mock_provider "time" {
  override_during = plan
}

variables {
  aws_region                = "us-east-1"
  aws_account_id            = "123456789012"
  host_cluster_name         = "platform-eks"
  chart_version             = "1.1.0"
  operations_helper_image   = "registry.example/skypilot-ops@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  db_connection_secret_name = "skypilot-postgres"
  oauth_enabled             = false
}

run "rejects_account_mismatch" {
  command = plan

  variables {
    aws_account_id = "210987654321"
  }

  expect_failures = [aws_iam_role.api_server]
}

run "rejects_wrong_partition_boundary" {
  command = plan

  variables {
    permissions_boundary_arn = "arn:aws-us-gov:iam::123456789012:policy/platform"
  }

  expect_failures = [aws_iam_role.api_server]
}

run "rejects_missing_helper_image" {
  command = plan

  variables {
    operations_helper_image = null
    api_server_image        = null
  }

  expect_failures = [kubernetes_job_v1.seed_config]
}

run "rejects_oauth_without_client_secret" {
  command = plan

  variables {
    oauth_enabled            = true
    oauth_client_secret_name = null
  }

  expect_failures = [helm_release.skypilot]
}

run "rejects_public_ingress_from_escape_hatch" {
  command = plan

  variables {
    extra_helm_values = <<-EOT
      ingress:
        annotations:
          service.beta.kubernetes.io/aws-load-balancer-scheme: internet-facing
    EOT
  }

  expect_failures = [helm_release.skypilot]
}

run "rejects_unknown_request_store_backend" {
  command = plan

  variables {
    request_store = {
      backend = "memory"
    }
  }

  expect_failures = [var.request_store]
}

run "rejects_empty_request_store_cutover_gate_path" {
  command = plan

  variables {
    request_store = {
      cutover_gate_path = "   "
    }
  }

  expect_failures = [var.request_store]
}

run "rejects_quiescence_enforcement_with_sqlite" {
  command = plan

  variables {
    request_store = {
      backend                                       = "sqlite"
      enforce_builtin_execution_quiescence_backends = true
    }
  }

  expect_failures = [var.request_store]
}

run "rejects_request_store_from_escape_hatch" {
  command = plan

  variables {
    extra_helm_values = <<-EOT
      requestStore:
        backend: sqlite
    EOT
  }

  expect_failures = [helm_release.skypilot]
}

run "rejects_request_store_null_without_prior_capture" {
  command = plan

  variables {
    request_store = {
      backend                                       = "postgres"
      enforce_builtin_execution_quiescence_backends = true
    }
    extra_helm_values = <<-EOT
      requestStore: null
    EOT
  }

  expect_failures = [helm_release.skypilot]
}

run "rejects_request_store_null_when_prior_capture_omits_backend" {
  command = plan

  variables {
    prior_helm_release_values = {
      yaml   = <<-EOT
        auth:
          retainedOAuthSecret: live-oauth
      EOT
      sha256 = "d6bcbb03c60b0b4bf4f60c44f727ac20fbb69339b27d29ee36e3361e231cb4b6"
    }
    request_store = {
      backend                                       = "postgres"
      enforce_builtin_execution_quiescence_backends = true
    }
    extra_helm_values = <<-EOT
      requestStore: null
    EOT
  }

  expect_failures = [helm_release.skypilot]
}

run "rejects_request_store_null_when_prior_backend_is_sqlite" {
  command = plan

  variables {
    prior_helm_release_values = {
      yaml   = <<-EOT
        requestStore:
          backend: sqlite
      EOT
      sha256 = "d8c91e4bc2983a864ccc5d4962004ecf8e7f0ec4fca761633d5d0f251e156647"
    }
    request_store = {
      backend                                       = "postgres"
      enforce_builtin_execution_quiescence_backends = true
    }
    extra_helm_values = <<-EOT
      requestStore: null
    EOT
  }

  expect_failures = [helm_release.skypilot]
}

run "rejects_request_store_null_when_prior_quiescence_fence_is_absent" {
  command = plan

  variables {
    prior_helm_release_values = {
      yaml   = <<-EOT
        requestStore:
          backend: postgres
      EOT
      sha256 = "376bb1eea709416d4e59ec9a776a1b54a09f34ba2cc947a2a7b2f70ff3c8f16b"
    }
    request_store = {
      backend                                       = "postgres"
      enforce_builtin_execution_quiescence_backends = true
    }
    extra_helm_values = <<-EOT
      requestStore: null
    EOT
  }

  expect_failures = [helm_release.skypilot]
}

run "rejects_request_store_null_when_typed_backend_is_sqlite" {
  command = plan

  variables {
    prior_helm_release_values = {
      yaml   = <<-EOT
        requestStore:
          backend: postgres
          enforceBuiltinExecutionQuiescenceBackends: true
      EOT
      sha256 = "4c1443efbd95d40dffe898049b8e0a89eaa1cf9f81ecf89e298aa1e892e3dc11"
    }
    extra_helm_values = <<-EOT
      requestStore: null
    EOT
  }

  expect_failures = [helm_release.skypilot]
}

run "rejects_request_store_null_without_typed_quiescence_fence" {
  command = plan

  variables {
    prior_helm_release_values = {
      yaml   = <<-EOT
        requestStore:
          backend: postgres
          enforceBuiltinExecutionQuiescenceBackends: true
      EOT
      sha256 = "4c1443efbd95d40dffe898049b8e0a89eaa1cf9f81ecf89e298aa1e892e3dc11"
    }
    request_store = {
      backend = "postgres"
    }
    extra_helm_values = <<-EOT
      requestStore: null
    EOT
  }

  expect_failures = [helm_release.skypilot]
}

run "rejects_request_store_map_even_with_transition_evidence" {
  command = plan

  variables {
    prior_helm_release_values = {
      yaml   = <<-EOT
        requestStore:
          backend: postgres
          enforceBuiltinExecutionQuiescenceBackends: true
      EOT
      sha256 = "4c1443efbd95d40dffe898049b8e0a89eaa1cf9f81ecf89e298aa1e892e3dc11"
    }
    request_store = {
      backend                                       = "postgres"
      enforce_builtin_execution_quiescence_backends = true
    }
    extra_helm_values = <<-EOT
      requestStore:
        backend: postgres
    EOT
  }

  expect_failures = [helm_release.skypilot]
}

run "rejects_request_store_scalar_even_with_transition_evidence" {
  command = plan

  variables {
    prior_helm_release_values = {
      yaml   = <<-EOT
        requestStore:
          backend: postgres
          enforceBuiltinExecutionQuiescenceBackends: true
      EOT
      sha256 = "4c1443efbd95d40dffe898049b8e0a89eaa1cf9f81ecf89e298aa1e892e3dc11"
    }
    request_store = {
      backend                                       = "postgres"
      enforce_builtin_execution_quiescence_backends = true
    }
    extra_helm_values = <<-EOT
      requestStore: postgres
    EOT
  }

  expect_failures = [helm_release.skypilot]
}

run "rejects_request_store_list_even_with_transition_evidence" {
  command = plan

  variables {
    prior_helm_release_values = {
      yaml   = <<-EOT
        requestStore:
          backend: postgres
          enforceBuiltinExecutionQuiescenceBackends: true
      EOT
      sha256 = "4c1443efbd95d40dffe898049b8e0a89eaa1cf9f81ecf89e298aa1e892e3dc11"
    }
    request_store = {
      backend                                       = "postgres"
      enforce_builtin_execution_quiescence_backends = true
    }
    extra_helm_values = <<-EOT
      requestStore:
        - postgres
    EOT
  }

  expect_failures = [helm_release.skypilot]
}

run "rejects_fullname_override_from_escape_hatch" {
  command = plan

  variables {
    extra_helm_values = <<-EOT
      fullnameOverride: renamed-control-plane
    EOT
  }

  expect_failures = [helm_release.skypilot]
}

run "rejects_prior_helm_values_digest_mismatch" {
  command = plan

  variables {
    prior_helm_release_values = {
      yaml   = <<-EOT
        databaseConnection:
          retainedSecretName: live-postgres
      EOT
      sha256 = "0000000000000000000000000000000000000000000000000000000000000000"
    }
  }

  expect_failures = [var.prior_helm_release_values]
}

run "rejects_prior_helm_values_non_map" {
  command = plan

  variables {
    prior_helm_release_values = {
      yaml   = "- not-a-map\n"
      sha256 = "f058b49087222c604bb2024ecbb2c8a9b090a94c960fc496dad101f156f0b757"
    }
  }

  expect_failures = [var.prior_helm_release_values]
}
