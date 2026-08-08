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
  request_store = {
    backend                                       = "postgres"
    enforce_builtin_execution_quiescence_backends = true
  }
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

run "rejects_mutable_helper_image_with_authority_fence" {
  command = plan
  variables {
    operations_helper_image = "registry.example/skypilot-ops:mutable"
    extra_helm_values       = <<-EOT
      storage: {enabled: true, accessMode: ReadWriteMany, existingClaim: skypilot-state-rwx}
    EOT
  }
  expect_failures = [helm_release.skypilot]
}

run "rejects_chart_created_state_claim_with_authority_fence" {
  command = plan
  variables {
    extra_helm_values = <<-EOT
      storage: {enabled: true, accessMode: ReadWriteMany}
    EOT
  }
  expect_failures = [helm_release.skypilot]
}

run "rejects_unenforced_execution_backends_with_authority_fence" {
  command = plan
  variables {
    request_store = {
      backend                                       = "postgres"
      enforce_builtin_execution_quiescence_backends = false
    }
    extra_helm_values = <<-EOT
      storage: {enabled: true, accessMode: ReadWriteMany, existingClaim: skypilot-state-rwx}
    EOT
  }
  expect_failures = [helm_release.skypilot]
}

run "rejects_api_sidecar_with_authority_fence" {
  command = plan
  variables {
    extra_helm_values = <<-EOT
      storage: {enabled: true, accessMode: ReadWriteMany, existingClaim: skypilot-state-rwx}
      apiService:
        sidecarContainers: [{name: bypass, image: busybox}]
    EOT
  }
  expect_failures = [helm_release.skypilot]
}

run "rejects_database_extra_volume_with_authority_fence" {
  command = plan
  variables {
    extra_helm_values = <<-EOT
      storage: {enabled: true, accessMode: ReadWriteMany, existingClaim: skypilot-state-rwx}
      databaseConnection:
        extraVolumes: [{name: bypass, emptyDir: {}}]
    EOT
  }
  expect_failures = [helm_release.skypilot]
}

run "rejects_database_extra_mount_with_authority_fence" {
  command = plan
  variables {
    extra_helm_values = <<-EOT
      storage: {enabled: true, accessMode: ReadWriteMany, existingClaim: skypilot-state-rwx}
      databaseConnection:
        extraVolumeMounts: [{name: skypilot-rwx-authority-fence, mountPath: /bypass}]
    EOT
  }
  expect_failures = [helm_release.skypilot]
}

run "rejects_executor_extra_volume_with_authority_fence" {
  command = plan
  variables {
    extra_helm_values = <<-EOT
      storage: {enabled: true, accessMode: ReadWriteMany, existingClaim: skypilot-state-rwx}
      executorService:
        extraVolumes: [{name: bypass, emptyDir: {}}]
    EOT
  }
  expect_failures = [helm_release.skypilot]
}

run "rejects_executor_extra_mount_with_authority_fence" {
  command = plan
  variables {
    extra_helm_values = <<-EOT
      storage: {enabled: true, accessMode: ReadWriteMany, existingClaim: skypilot-state-rwx}
      executorService:
        extraVolumeMounts: [{name: skypilot-rwx-authority-fence, mountPath: /bypass}]
    EOT
  }
  expect_failures = [helm_release.skypilot]
}

run "rejects_controller_extra_volume_with_authority_fence" {
  command = plan
  variables {
    extra_helm_values = <<-EOT
      storage: {enabled: true, accessMode: ReadWriteMany, existingClaim: skypilot-state-rwx}
      controllerService:
        extraVolumes: [{name: bypass, emptyDir: {}}]
    EOT
  }
  expect_failures = [helm_release.skypilot]
}

run "rejects_controller_extra_mount_with_authority_fence" {
  command = plan
  variables {
    extra_helm_values = <<-EOT
      storage: {enabled: true, accessMode: ReadWriteMany, existingClaim: skypilot-state-rwx}
      controllerService:
        extraVolumeMounts: [{name: skypilot-rwx-authority-fence, mountPath: /bypass}]
    EOT
  }
  expect_failures = [helm_release.skypilot]
}
