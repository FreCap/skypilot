mock_provider "aws" {
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

  mock_data "aws_iam_policy_document" {
    defaults = {
      id            = "terraform-test-policy"
      json          = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
      minified_json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }

  override_resource {
    target          = aws_iam_policy.copy_role_boundary
    override_during = plan
    values = {
      arn              = "arn:aws:iam::123456789012:policy/image-copy-target-boundary"
      attachment_count = 0
      id               = "arn:aws:iam::123456789012:policy/image-copy-target-boundary"
      name_prefix      = ""
      policy_id        = "copy-target-boundary"
      tags_all         = {}
    }
  }

  override_resource {
    target          = aws_iam_policy.lifecycle_role_boundary
    override_during = plan
    values = {
      arn              = "arn:aws:iam::123456789012:policy/image-lifecycle-target-boundary"
      attachment_count = 0
      id               = "arn:aws:iam::123456789012:policy/image-lifecycle-target-boundary"
      name_prefix      = ""
      policy_id        = "lifecycle-target-boundary"
      tags_all         = {}
    }
  }

  override_resource {
    target          = aws_iam_role.copy_target
    override_during = plan
    values = {
      arn                 = "arn:aws:iam::123456789012:role/image-copy-target"
      create_date         = "2026-07-24T00:00:00Z"
      id                  = "image-copy-target"
      managed_policy_arns = []
      name_prefix         = ""
      tags_all            = {}
      unique_id           = "copy-target"
    }
  }

  override_resource {
    target          = aws_iam_role.lifecycle_target
    override_during = plan
    values = {
      arn                 = "arn:aws:iam::123456789012:role/image-lifecycle-target"
      create_date         = "2026-07-24T00:00:00Z"
      id                  = "image-lifecycle-target"
      managed_policy_arns = []
      name_prefix         = ""
      tags_all            = {}
      unique_id           = "lifecycle-target"
    }
  }
}

variables {
  catalog_authority                   = "00000000-0000-4000-8000-000000000001"
  catalog_authority_base32            = "aaaaaaaaabaabaaaaaaaaaaaae"
  realm                               = "terraform-test"
  profile                             = "default"
  registry_account_id                 = "123456789012"
  region                              = "us-east-1"
  workspaces                          = ["workspace-a"]
  copy_worker_base_role_arns          = ["arn:aws:iam::123456789012:role/image-copy-worker"]
  lifecycle_worker_base_role_arns     = ["arn:aws:iam::123456789012:role/image-lifecycle-worker"]
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
      runtime_pull_principal_arns  = []
    }
  }
}

run "apply_generation_zero" {
  command   = apply
  state_key = "qualification-generation-removal"
}

run "add_and_activate_generation_one" {
  command   = apply
  state_key = "qualification-generation-removal"

  variables {
    qualification_repository_generations       = [0, 1]
    active_qualification_repository_generation = 1
  }
}

# This plan must fail. verify_prevent_destroy.sh turns only the exact lifecycle
# rejection into a passing acceptance check.
run "attempt_to_remove_generation_one" {
  command   = plan
  state_key = "qualification-generation-removal"

  variables {
    qualification_repository_generations       = [0]
    active_qualification_repository_generation = 0
  }
}
