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

  mock_data "aws_iam_policy_document" {
    defaults = {
      id            = "terraform-test-policy"
      json          = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
      minified_json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }

  override_data {
    target          = data.aws_iam_policy_document.qualification
    override_during = plan
    values = {
      id            = "qualification-active-policy"
      json          = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Sid\":\"Active\",\"Effect\":\"Allow\",\"Principal\":\"*\",\"Action\":\"ecr:BatchGetImage\",\"Resource\":\"*\"}]}"
      minified_json = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Sid\":\"Active\",\"Effect\":\"Allow\",\"Principal\":\"*\",\"Action\":\"ecr:BatchGetImage\",\"Resource\":\"*\"}]}"
    }
  }

  override_data {
    target          = data.aws_iam_policy_document.qualification_inactive
    override_during = plan
    values = {
      id            = "qualification-inactive-policy"
      json          = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Sid\":\"Inactive\",\"Effect\":\"Deny\",\"Principal\":\"*\",\"Action\":\"ecr:BatchGetImage\",\"Resource\":\"*\"}]}"
      minified_json = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Sid\":\"Inactive\",\"Effect\":\"Deny\",\"Principal\":\"*\",\"Action\":\"ecr:BatchGetImage\",\"Resource\":\"*\"}]}"
    }
  }

  override_resource {
    target          = aws_ecr_repository.shard
    override_during = plan
    values = {
      arn            = "arn:aws:ecr:us-east-1:123456789012:repository/skypilot/images/test-shard"
      id             = "skypilot/images/test-shard"
      region         = "us-east-1"
      registry_id    = "123456789012"
      repository_url = "123456789012.dkr.ecr.us-east-1.amazonaws.com/skypilot/images/test-shard"
      tags_all       = {}
      encryption_configuration = {
        encryption_type = "AES256"
        kms_key         = null
      }
    }
  }

  override_resource {
    target          = aws_ecr_repository.qualification
    override_during = plan
    values = {
      arn            = "arn:aws:ecr:us-east-1:123456789012:repository/skypilot/images/test-qualification-g00"
      id             = "skypilot/images/test-qualification-g00"
      region         = "us-east-1"
      registry_id    = "123456789012"
      repository_url = "123456789012.dkr.ecr.us-east-1.amazonaws.com/skypilot/images/test-qualification-g00"
      tags_all = {
        SkyPilotQualification = "true"
        ProviderDefault       = "inherited-g00"
      }
      encryption_configuration = {
        encryption_type = "AES256"
        kms_key         = null
      }
    }
  }

  override_resource {
    target          = aws_ecr_repository.qualification_generation
    override_during = plan
    values = {
      arn            = "arn:aws:ecr:us-east-1:123456789012:repository/skypilot/images/test-qualification-g01"
      id             = "skypilot/images/test-qualification-g01"
      region         = "us-east-1"
      registry_id    = "123456789012"
      repository_url = "123456789012.dkr.ecr.us-east-1.amazonaws.com/skypilot/images/test-qualification-g01"
      tags_all = {
        SkyPilotQualification           = "true"
        SkyPilotQualificationGeneration = "1"
        ProviderDefault                 = "inherited-g01"
      }
      encryption_configuration = {
        encryption_type = "AES256"
        kms_key         = null
      }
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

run "defaults_to_the_legacy_generation_zero_state" {
  command   = apply
  state_key = "qualification-generation-rollout"

  assert {
    condition     = length(aws_ecr_repository.qualification_generation) == 0
    error_message = "The backward-compatible default must create only the legacy generation-zero qualification repository."
  }

  assert {
    condition     = output.qualification_repository_generation == 0
    error_message = "The backward-compatible default must select generation zero."
  }

  assert {
    condition     = output.role_fingerprints["us-east-1:qualification_policy_hash"] == sha256(jsonencode(jsondecode(data.aws_iam_policy_document.qualification.json)))
    error_message = "The generation-zero handoff must fingerprint the canonical active qualification policy."
  }

  assert {
    condition = (
      aws_ecr_repository.qualification.tags_all["ProviderDefault"] == "inherited-g00" &&
      output.role_fingerprints["us-east-1:qualification_ownership_tags_hash"] == sha256(jsonencode(aws_ecr_repository.qualification.tags_all)) &&
      output.role_fingerprints["us-east-1:qualification_ownership_tags_hash"] != sha256(jsonencode(aws_ecr_repository.qualification.tags))
    )
    error_message = "The generation-zero handoff must canonically fingerprint active qualification tags_all, including provider defaults."
  }

  assert {
    condition     = output.qualification_repository_policy_modes_by_generation == { "0" = "ACTIVE" }
    error_message = "The selected generation-zero repository must retain the active data-plane policy."
  }

  assert {
    condition     = aws_ecr_repository_policy.qualification.policy == data.aws_iam_policy_document.qualification.json
    error_message = "The selected generation-zero repository must use the active qualification policy."
  }
}

run "retains_generation_zero_and_activates_generation_one" {
  command   = apply
  state_key = "qualification-generation-rollout"

  variables {
    qualification_repository_generations       = [0, 1]
    active_qualification_repository_generation = 1
  }

  assert {
    condition     = aws_ecr_repository.qualification.name == "skypilot/images/raaaaaaaaabaabaaaaaaaaaaaae/qualification/us-east-1"
    error_message = "Generation zero must retain the exact legacy qualification repository path."
  }

  assert {
    condition     = aws_ecr_repository.qualification_generation["g01"].name == "skypilot/images/raaaaaaaaabaabaaaaaaaaaaaae/qualification/g01/us-east-1"
    error_message = "Generation one must use its own deterministic repository path."
  }

  assert {
    condition     = output.qualification_repository_generation == 1
    error_message = "The active qualification generation output must identify generation one."
  }

  assert {
    condition     = output.qualification_repository_arns_by_generation["0"] == run.defaults_to_the_legacy_generation_zero_state.qualification_repository_arns_by_generation["0"]
    error_message = "The generation-one rollout must retain the exact generation-zero repository identity from the prior apply."
  }

  assert {
    condition     = output.qualification_repository_url == aws_ecr_repository.qualification_generation["g01"].repository_url
    error_message = "The compatibility URL output must point at the active qualification repository."
  }

  assert {
    condition = output.qualification_repository_urls_by_generation == {
      "0" = aws_ecr_repository.qualification.repository_url
      "1" = aws_ecr_repository.qualification_generation["g01"].repository_url
    }
    error_message = "The URL map must expose every retained qualification generation."
  }

  assert {
    condition = output.qualification_repository_arns_by_generation == {
      "0" = aws_ecr_repository.qualification.arn
      "1" = aws_ecr_repository.qualification_generation["g01"].arn
    }
    error_message = "The ARN map must expose every retained qualification generation."
  }

  assert {
    condition     = output.role_fingerprints["us-east-1:qualification_repo_arn"] == aws_ecr_repository.qualification_generation["g01"].arn
    error_message = "The handoff fingerprint must identify the active qualification repository."
  }

  assert {
    condition     = output.role_fingerprints["us-east-1:qualification_policy_hash"] == sha256(jsonencode(jsondecode(data.aws_iam_policy_document.qualification.json)))
    error_message = "The generation-one handoff must retain the canonical active qualification policy fingerprint."
  }

  assert {
    condition = (
      aws_ecr_repository.qualification_generation["g01"].tags_all["ProviderDefault"] == "inherited-g01" &&
      output.role_fingerprints["us-east-1:qualification_ownership_tags_hash"] == sha256(jsonencode(aws_ecr_repository.qualification_generation["g01"].tags_all)) &&
      output.role_fingerprints["us-east-1:qualification_ownership_tags_hash"] != sha256(jsonencode(aws_ecr_repository.qualification_generation["g01"].tags))
    )
    error_message = "The generation-one handoff must canonically fingerprint active qualification tags_all, including provider defaults."
  }

  assert {
    condition = output.qualification_repository_policy_modes_by_generation == {
      "0" = "INACTIVE_DENY"
      "1" = "ACTIVE"
    }
    error_message = "Only the selected generation may retain qualification data-plane authority."
  }

  assert {
    condition     = aws_ecr_repository_policy.qualification.policy == data.aws_iam_policy_document.qualification_inactive.json
    error_message = "The inactive generation-zero repository must receive the explicit data-plane deny policy."
  }

  assert {
    condition     = aws_ecr_repository_policy.qualification_generation["g01"].policy == data.aws_iam_policy_document.qualification.json
    error_message = "The selected generation-one repository must receive the active qualification policy."
  }

  assert {
    condition     = aws_ecr_repository_policy.qualification_generation["g01"].repository == aws_ecr_repository.qualification_generation["g01"].name
    error_message = "Every generated qualification repository must receive the qualification policy."
  }

  assert {
    condition = (
      length(data.aws_iam_policy_document.qualification_inactive.statement) == 1 &&
      one(data.aws_iam_policy_document.qualification_inactive.statement).effect == "Deny" &&
      toset(one(data.aws_iam_policy_document.qualification_inactive.statement).actions) == toset([
        "ecr:BatchCheckLayerAvailability",
        "ecr:GetDownloadUrlForLayer",
        "ecr:BatchGetImage",
        "ecr:DescribeImages",
        "ecr:ListImages",
        "ecr:InitiateLayerUpload",
        "ecr:UploadLayerPart",
        "ecr:CompleteLayerUpload",
        "ecr:PutImage",
        "ecr:BatchDeleteImage",
      ]) &&
      length(one(data.aws_iam_policy_document.qualification_inactive.statement).principals) == 1 &&
      one(one(data.aws_iam_policy_document.qualification_inactive.statement).principals).type == "*" &&
      toset(one(one(data.aws_iam_policy_document.qualification_inactive.statement).principals).identifiers) == toset(["*"])
    )
    error_message = "The inactive policy must explicitly deny every image data-plane action to every principal while leaving metadata and management actions available for Terraform reconciliation."
  }

  assert {
    condition = toset(one([
      for statement in data.aws_iam_policy_document.copy_role_boundary.statement : statement
      if statement.sid == "CopyExactManagedRepositories"
      ]).resources) == toset(concat(
      [for repository in aws_ecr_repository.shard : repository.arn],
      [aws_ecr_repository.qualification_generation["g01"].arn],
    ))
    error_message = "The copy boundary must grant only shards and the active qualification generation."
  }

  assert {
    condition = toset(one([
      for statement in data.aws_iam_policy_document.lifecycle_role_boundary.statement : statement
      if statement.sid == "LifecycleReadAllManagedRepositories"
      ]).resources) == toset(concat(
      [for repository in aws_ecr_repository.shard : repository.arn],
      [aws_ecr_repository.qualification_generation["g01"].arn],
    ))
    error_message = "The lifecycle boundary must read only shards and the active qualification generation."
  }

  assert {
    condition = toset(one([
      for statement in data.aws_iam_policy_document.lifecycle_role_boundary.statement : statement
      if statement.sid == "LifecycleDeleteEligibleRepositories"
      ]).resources) == toset([
      aws_ecr_repository.qualification_generation["g01"].arn,
    ])
    error_message = "The lifecycle boundary must delete only noncanonical shards and the active qualification generation."
  }

  assert {
    condition = toset(one([
      for statement in data.aws_iam_policy_document.copy_permissions.statement : statement
      if statement.sid == "CopyExactManagedContent"
      ]).resources) == toset(concat(
      [for repository in aws_ecr_repository.shard : repository.arn],
      [aws_ecr_repository.qualification_generation["g01"].arn],
    ))
    error_message = "The copy inline policy must grant only shards and the active qualification generation."
  }

  assert {
    condition = toset(one([
      for statement in data.aws_iam_policy_document.lifecycle_permissions.statement : statement
      if statement.sid == "ReadAllManagedContent"
      ]).resources) == toset(concat(
      [for repository in aws_ecr_repository.shard : repository.arn],
      [aws_ecr_repository.qualification_generation["g01"].arn],
    ))
    error_message = "The lifecycle inline policy must read only shards and the active qualification generation."
  }

  assert {
    condition = toset(one([
      for statement in data.aws_iam_policy_document.lifecycle_permissions.statement : statement
      if statement.sid == "DeleteEligibleManagedContent"
      ]).resources) == toset([
      aws_ecr_repository.qualification_generation["g01"].arn,
    ])
    error_message = "The lifecycle inline policy must delete only noncanonical shards and the active qualification generation."
  }
}

run "cleanup_stateful_generation_rollout" {
  command   = apply
  state_key = "qualification-generation-rollout"

  # Terraform cleans test state with the configuration from its most recent run.
  # An empty module intentionally owns cleanup so prevent_destroy continues to
  # protect normal module plans without making the successful test leak state.
  module {
    source = "./terraform-tests/modules/empty"
  }
}

run "rejects_an_active_generation_that_is_not_retained" {
  command = plan

  variables {
    qualification_repository_generations       = [0]
    active_qualification_repository_generation = 1
  }

  expect_failures = [terraform_data.validation]
}

run "rejects_reselecting_an_older_retained_generation" {
  command = plan

  variables {
    qualification_repository_generations       = [0, 1]
    active_qualification_repository_generation = 0
  }

  expect_failures = [terraform_data.validation]
}

run "rejects_removing_legacy_generation_zero" {
  command = plan

  variables {
    qualification_repository_generations       = [1]
    active_qualification_repository_generation = 1
  }

  expect_failures = [terraform_data.validation]
}

run "rejects_an_out_of_range_retained_generation" {
  command = plan

  variables {
    qualification_repository_generations = [0, 256]
  }

  expect_failures = [var.qualification_repository_generations]
}

run "rejects_a_fractional_active_generation" {
  command = plan

  variables {
    active_qualification_repository_generation = 1.5
  }

  expect_failures = [var.active_qualification_repository_generation]
}

run "rejects_a_mismatched_catalog_authority_encoding" {
  command = plan

  variables {
    catalog_authority_base32 = "aaaaaaaaaaaaaaaaaaaaaaaaaa"
  }

  expect_failures = [terraform_data.validation]
}

run "accepts_a_nontrivial_catalog_authority_encoding" {
  command = plan

  variables {
    catalog_authority        = "12345678-9abc-4def-8abc-0123456789ab"
    catalog_authority_base32 = "ci2fm6e2xrg67cv4aerukz4jvm"
  }
}
