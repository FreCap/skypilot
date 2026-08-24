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

run "default_contract_is_provider_neutral_and_stable" {
  command = plan

  assert {
    condition     = aws_iam_role.api_server.name == "skypilot-api-platform-eks"
    error_message = "The default API role name must preserve the deployed naming contract."
  }

  assert {
    condition     = aws_iam_role.api_server.permissions_boundary == null
    error_message = "The permissions boundary must remain opt-in."
  }

  assert {
    condition = (
      jsondecode(aws_iam_role.api_server.assume_role_policy).Statement[0].Principal.Service ==
      "pods.eks.amazonaws.com"
    )
    error_message = "The API role must trust the EKS Pod Identity service."
  }

  assert {
    condition = (
      aws_eks_pod_identity_association.api_server.cluster_name == "platform-eks" &&
      aws_eks_pod_identity_association.api_server.namespace == "skypilot" &&
      aws_eks_pod_identity_association.api_server.service_account == "skypilot-api-sa"
    )
    error_message = "The Pod Identity association must preserve cluster, namespace, and service-account defaults."
  }

  assert {
    condition     = length(kubernetes_namespace_v1.skypilot) == 1
    error_message = "The module must preserve namespace ownership by default."
  }

  assert {
    condition     = helm_release.skypilot.version == "1.1.0"
    error_message = "The Helm release must use the exact caller-supplied chart version."
  }

  assert {
    condition = (
      yamldecode(helm_release.skypilot.values[0]).requestStore == {
        backend                                   = "sqlite"
        cutoverGatePath                           = "/root/.sky/api-request-cutover.json"
        enforceBuiltinExecutionQuiescenceBackends = false
      }
    )
    error_message = "The module must render the chart-compatible request-store defaults explicitly."
  }

  assert {
    condition     = output.api_server_role_name == "skypilot-api-platform-eks"
    error_message = "The module must expose the effective API role name."
  }

  assert {
    condition = (
      output.host_cluster_provider_config.endpoint == "https://example.eks.amazonaws.com" &&
      output.host_cluster_provider_config.exec.args == [
        "eks",
        "get-token",
        "--cluster-name",
        "platform-eks",
        "--region",
        "us-east-1",
      ]
    )
    error_message = "The output must expose the root provider's exact EKS exec-auth contract."
  }
}

run "postgres_request_store_is_rendered" {
  command = plan

  variables {
    request_store = {
      backend                                       = "postgres"
      enforce_builtin_execution_quiescence_backends = true
      cutover_gate_path                             = "/var/lib/skypilot/request-cutover.json"
    }
  }

  assert {
    condition = (
      yamldecode(helm_release.skypilot.values[0]).requestStore == {
        backend                                   = "postgres"
        cutoverGatePath                           = "/var/lib/skypilot/request-cutover.json"
        enforceBuiltinExecutionQuiescenceBackends = true
      }
    )
    error_message = "The module must map request_store into the chart's requestStore contract."
  }
}

run "existing_release_values_are_reused_in_helm_order" {
  command = plan

  variables {
    prior_helm_release_values = {
      yaml   = <<-EOT
        databaseConnection:
          retainedSecretName: live-postgres
        auth:
          retainedOAuthSecret: live-oauth
      EOT
      sha256 = "163ffc4cb6866bc8a91767abedf526fcbdce18ac28be8a6629f8fc8e84810708"
    }
    extra_helm_values = <<-EOT
      apiService:
        replicas: 2
    EOT
  }

  assert {
    condition     = helm_release.skypilot.reuse_values
    error_message = "An immutable prior-values capture must enable Helm reuse_values."
  }

  assert {
    condition = (
      length(helm_release.skypilot.values) == 3 &&
      yamldecode(helm_release.skypilot.values[0]).databaseConnection.retainedSecretName == "live-postgres" &&
      yamldecode(helm_release.skypilot.values[0]).auth.retainedOAuthSecret == "live-oauth" &&
      !contains(keys(yamldecode(helm_release.skypilot.values[1])), "databaseConnection") &&
      !contains(keys(yamldecode(helm_release.skypilot.values[1]).auth), "retainedOAuthSecret") &&
      yamldecode(helm_release.skypilot.values[2]).apiService.replicas == 2
    )
    error_message = "Helm values must preserve release-only database/auth settings in the first layer, followed by generated values and planned overrides."
  }
}

run "postgres_request_store_null_tombstone_is_last" {
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
      apiService:
        replicas: 2
      requestStore: null
    EOT
  }

  assert {
    condition     = helm_release.skypilot.reuse_values
    error_message = "The requestStore tombstone must run only with Helm reuse_values enabled by the immutable capture."
  }

  assert {
    condition = (
      length(helm_release.skypilot.values) == 3 &&
      yamldecode(helm_release.skypilot.values[0]).requestStore.backend == "postgres" &&
      yamldecode(helm_release.skypilot.values[1]).requestStore == {
        backend                                   = "postgres"
        cutoverGatePath                           = "/root/.sky/api-request-cutover.json"
        enforceBuiltinExecutionQuiescenceBackends = true
      } &&
      contains(keys(yamldecode(helm_release.skypilot.values[2])), "requestStore") &&
      yamldecode(helm_release.skypilot.values[2]).requestStore == null &&
      yamldecode(helm_release.skypilot.values[2]).apiService.replicas == 2
    )
    error_message = "The exact null tombstone must remain the last Helm values layer after the proven and typed PostgreSQL contracts."
  }
}

run "custom_role_and_boundary_are_additive" {
  command = plan

  variables {
    api_server_role_name     = "skypilot-control-plane"
    permissions_boundary_arn = "arn:aws:iam::123456789012:policy/boundaries/platform"
  }

  assert {
    condition = (
      aws_iam_role.api_server.name == "skypilot-control-plane" &&
      aws_iam_role.api_server.permissions_boundary == var.permissions_boundary_arn
    )
    error_message = "The caller must be able to select a safe role name and active-account boundary."
  }
}

run "helper_image_does_not_advance_config_generation" {
  command = plan

  variables {
    operations_helper_image = "registry.example/skypilot-ops@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
  }

  assert {
    condition = (
      output.config_generation ==
      run.default_contract_is_provider_neutral_and_stable.config_generation
    )
    error_message = "A helper-image-only change must not trigger an API-server config reconcile."
  }
}

run "config_change_advances_config_generation" {
  command = plan

  variables {
    config_extra = { jobs = { controller = { consolidation_mode = true } } }
  }

  assert {
    condition = (
      output.config_generation !=
      run.default_contract_is_provider_neutral_and_stable.config_generation
    )
    error_message = "A DB-backed config change must still trigger an API-server config reconcile."
  }
}
