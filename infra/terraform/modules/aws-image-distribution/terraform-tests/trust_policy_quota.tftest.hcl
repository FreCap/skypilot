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
      endpoint    = "ecr.us-east-1.amazonaws.com"
      id          = "us-east-1"
      name        = "us-east-1"
      region      = "us-east-1"
    }
  }

  mock_data "aws_servicequotas_service_quota" {
    defaults = {
      adjustable    = true
      arn           = "arn:aws:servicequotas:us-east-1:123456789012:ecr/L-03A36CE1"
      default_value = 100000
      global_quota  = false
      id            = "ecr/L-03A36CE1"
      quota_code    = "L-03A36CE1"
      quota_name    = "Images per repository"
      service_code  = "ecr"
      service_name  = "Amazon Elastic Container Registry"
      value         = 100000
    }
  }

  mock_data "aws_iam_policy_document" {
    defaults = {
      id            = "terraform-test-policy"
      json          = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
      minified_json = <<-EOT
        0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
        0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
        0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
        0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
        0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
        0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
        0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
        0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
        0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
      EOT
    }
  }

  override_resource {
    target          = aws_ecr_repository.shard
    override_during = plan
    values = {
      arn            = "arn:aws:ecr:us-east-1:123456789012:repository/skypilot/images/test-shard"
      registry_id    = "123456789012"
      repository_url = "123456789012.dkr.ecr.us-east-1.amazonaws.com/skypilot/images/test-shard"
    }
  }

  override_resource {
    target          = aws_ecr_repository.qualification
    override_during = plan
    values = {
      arn            = "arn:aws:ecr:us-east-1:123456789012:repository/skypilot/images/test-qualification"
      registry_id    = "123456789012"
      repository_url = "123456789012.dkr.ecr.us-east-1.amazonaws.com/skypilot/images/test-qualification"
    }
  }

  override_resource {
    target          = aws_iam_policy.copy_role_boundary
    override_during = plan
    values = {
      arn = "arn:aws:iam::123456789012:policy/image-copy-target-boundary"
      id  = "arn:aws:iam::123456789012:policy/image-copy-target-boundary"
    }
  }

  override_resource {
    target          = aws_iam_policy.lifecycle_role_boundary
    override_during = plan
    values = {
      arn = "arn:aws:iam::123456789012:policy/image-lifecycle-target-boundary"
      id  = "arn:aws:iam::123456789012:policy/image-lifecycle-target-boundary"
    }
  }

  override_resource {
    target          = aws_iam_role.copy_target
    override_during = plan
    values = {
      arn = "arn:aws:iam::123456789012:role/image-copy-target"
      id  = "image-copy-target"
    }
  }

  override_resource {
    target          = aws_iam_role.lifecycle_target
    override_during = plan
    values = {
      arn = "arn:aws:iam::123456789012:role/image-lifecycle-target"
      id  = "image-lifecycle-target"
    }
  }
}

variables {
  catalog_authority                   = "00000000-0000-4000-8000-000000000001"
  catalog_authority_base32            = "aaaaaaaaaaaaaaaaaaaaaaaaaa"
  realm                               = "terraform-test"
  profile                             = "default"
  registry_account_id                 = "123456789012"
  region                              = "us-east-1"
  workspaces                          = ["workspace-a"]
  copy_worker_base_role_arns          = [for index in range(4) : "arn:aws:iam::123456789012:role/${join("", [for _ in range(510) : "p"])}/copy-${index}"]
  lifecycle_worker_base_role_arns     = [for index in range(4) : "arn:aws:iam::123456789012:role/${join("", [for _ in range(510) : "p"])}/lifecycle-${index}"]
  copy_target_role_name               = "image-copy-target"
  lifecycle_target_role_name          = "image-lifecycle-target"
  applied_images_per_repository_quota = 1000
  quota_headroom                      = 100
  targets = {
    canonical = {
      canonical                    = true
      shard_count                  = 1
      max_manifests_per_shard      = 800
      max_declared_bytes_per_shard = 1099511627776
      max_in_flight                = 10
      runtime_pull_principal_arns  = ["arn:aws:iam::123456789012:role/runtime-pull"]
    }
  }
}

run "accepts_long_path_trust_policies_with_applied_custom_quota" {
  command = plan

  variables {
    applied_role_trust_policy_quota = 4096
  }
}

run "rejects_long_path_trust_policies_over_default_quota" {
  command = plan

  expect_failures = [
    aws_iam_role.copy_target,
    aws_iam_role.lifecycle_target,
  ]
}
