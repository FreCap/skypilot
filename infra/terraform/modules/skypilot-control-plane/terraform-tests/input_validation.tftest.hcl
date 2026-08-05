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
  operations_helper_image   = "registry.example/skypilot-ops:1.1.0"
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
