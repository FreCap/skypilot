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
    condition = (
      !contains(keys(yamldecode(helm_release.skypilot.values[0])), "extraInitContainers") &&
      alltrue([
        for volume in try(yamldecode(helm_release.skypilot.values[0]).apiService.extraVolumes, []) :
        volume.name != "skypilot-rwx-authority-fence"
      ])
    )
    error_message = "The default contract must not render an RWX authority verifier or authority volume."
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

run "rwx_authority_fence_is_typed_and_composed" {
  command = plan

  variables {
    request_store = {
      backend                                       = "postgres"
      enforce_builtin_execution_quiescence_backends = true
    }
    extra_helm_values = <<-EOT
      storage:
        enabled: true
        accessMode: ReadWriteMany
        existingClaim: skypilot-state-rwx
    EOT
    rwx_authority_fence = {
      authority_claim_name              = "skypilot-state-authority"
      state_claim_name                  = "skypilot-state-rwx"
      expected_sha256                   = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      expected_postgres_evidence_sha256 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
      identity = {
        source = {
          pvc_name      = "skypilot-state"
          pvc_uid       = "11111111-1111-1111-1111-111111111111"
          pv_name       = "pvc-11111111-1111-1111-1111-111111111111"
          pv_uid        = "22222222-2222-2222-2222-222222222222"
          ebs_volume_id = "vol-11111111111111111"
        }
        target = {
          filesystem_id             = "fs-11111111111111111"
          state_access_point_id     = "fsap-11111111111111111"
          state_pv_name             = "skypilot-state-rwx-pv"
          state_pv_uid              = "33333333-3333-3333-3333-333333333333"
          state_pvc_uid             = "44444444-4444-4444-4444-444444444444"
          authority_access_point_id = "fsap-22222222222222222"
          authority_pv_name         = "skypilot-state-authority-pv"
          authority_pv_uid          = "55555555-5555-5555-5555-555555555555"
          authority_pvc_uid         = "66666666-6666-6666-6666-666666666666"
        }
      }
    }
  }

  assert {
    condition = one([
      for volume in yamldecode(helm_release.skypilot.values[0]).apiService.extraVolumes : volume
      if volume.name == "skypilot-rwx-authority-fence"
      ]).persistentVolumeClaim == {
      claimName = "skypilot-state-authority"
      readOnly  = true
    }
    error_message = "The module must render the dedicated authority PVC as a read-only pod volume."
  }

  assert {
    condition = alltrue([
      for mount in try(yamldecode(helm_release.skypilot.values[0]).apiService.extraVolumeMounts, []) :
      mount.name != "skypilot-rwx-authority-fence"
    ])
    error_message = "The module must not expose the authority PVC to long-running role containers."
  }

  assert {
    condition = (
      length([
        for container in yamldecode(helm_release.skypilot.values[0]).extraInitContainers : container
        if container.name == "verify-rwx-authority-fence"
      ]) == 1 &&
      one([
        for container in yamldecode(helm_release.skypilot.values[0]).extraInitContainers : container
        if container.name == "verify-rwx-authority-fence"
      ]).image == var.operations_helper_image &&
      !one([
        for container in yamldecode(helm_release.skypilot.values[0]).extraInitContainers : container
        if container.name == "verify-rwx-authority-fence"
      ]).securityContext.allowPrivilegeEscalation &&
      one([
        for container in yamldecode(helm_release.skypilot.values[0]).extraInitContainers : container
        if container.name == "verify-rwx-authority-fence"
      ]).securityContext.readOnlyRootFilesystem &&
      one([
        for container in yamldecode(helm_release.skypilot.values[0]).extraInitContainers : container
        if container.name == "verify-rwx-authority-fence"
      ]).securityContext.capabilities.drop == ["ALL"] &&
      one([
        for container in yamldecode(helm_release.skypilot.values[0]).extraInitContainers : container
        if container.name == "verify-rwx-authority-fence"
        ]).volumeMounts == [{
        mountPath = "/var/run/skypilot/rwx-authority"
        name      = "skypilot-rwx-authority-fence"
        readOnly  = true
      }] &&
      strcontains(one([
        for container in yamldecode(helm_release.skypilot.values[0]).extraInitContainers : container
        if container.name == "verify-rwx-authority-fence"
      ]).command[2], "O_NOFOLLOW")
    )
    error_message = "The module must render one hardened, no-follow authority verifier init container."
  }

  assert {
    condition = (
      jsondecode(one([
        for env in one([
          for container in yamldecode(helm_release.skypilot.values[0]).extraInitContainers : container
          if container.name == "verify-rwx-authority-fence"
        ]).env : env.value
        if env.name == "SKYPILOT_RWX_AUTHORITY_FENCE_EXPECTED_IDENTITY"
        ])) == {
        namespace    = "skypilot"
        release_name = "skypilot"
        source = {
          pvc_namespace = "skypilot"
          pvc_name      = "skypilot-state"
          pvc_uid       = "11111111-1111-1111-1111-111111111111"
          pv_name       = "pvc-11111111-1111-1111-1111-111111111111"
          pv_uid        = "22222222-2222-2222-2222-222222222222"
          ebs_volume_id = "vol-11111111111111111"
        }
        target = {
          state_pvc_namespace       = "skypilot"
          state_claim_name          = "skypilot-state-rwx"
          filesystem_id             = "fs-11111111111111111"
          state_access_point_id     = "fsap-11111111111111111"
          state_pv_name             = "skypilot-state-rwx-pv"
          state_pv_uid              = "33333333-3333-3333-3333-333333333333"
          state_pvc_uid             = "44444444-4444-4444-4444-444444444444"
          authority_claim_name      = "skypilot-state-authority"
          authority_pvc_namespace   = "skypilot"
          authority_access_point_id = "fsap-22222222222222222"
          authority_pv_name         = "skypilot-state-authority-pv"
          authority_pv_uid          = "55555555-5555-5555-5555-555555555555"
          authority_pvc_uid         = "66666666-6666-6666-6666-666666666666"
        }
      } &&
      one([
        for env in one([
          for container in yamldecode(helm_release.skypilot.values[0]).extraInitContainers : container
          if container.name == "verify-rwx-authority-fence"
        ]).env : env.value
        if env.name == "SKYPILOT_RWX_AUTHORITY_FENCE_EXPECTED_SHA256"
      ]) == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" &&
      one([
        for env in one([
          for container in yamldecode(helm_release.skypilot.values[0]).extraInitContainers : container
          if container.name == "verify-rwx-authority-fence"
        ]).env : env.value
        if env.name == "SKYPILOT_RWX_AUTHORITY_FENCE_EXPECTED_POSTGRES_EVIDENCE_SHA256"
      ]) == "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    )
    error_message = "The verifier must bind the exact release, source, state, and authority identities."
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
