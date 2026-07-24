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

  mock_data "aws_iam_policy" {
    defaults = {
      arn         = "arn:aws:iam::123456789012:policy/boundaries/organization"
      description = "Organization-managed permissions boundary"
      id          = "arn:aws:iam::123456789012:policy/boundaries/organization"
      name        = "organization"
      path        = "/boundaries/"
      policy      = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Sid\":\"OrganizationBoundary\",\"Effect\":\"Allow\",\"Action\":[\"ecr:*\",\"servicequotas:GetAWSDefaultServiceQuota\",\"servicequotas:GetServiceQuota\"],\"Resource\":\"*\"}]}"
      policy_id   = "ANPAEXTERNALBOUNDARY"
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
      arn            = "arn:aws:ecr:us-east-1:123456789012:repository/skypilot/images/test-qualification"
      id             = "skypilot/images/test-qualification"
      region         = "us-east-1"
      registry_id    = "123456789012"
      repository_url = "123456789012.dkr.ecr.us-east-1.amazonaws.com/skypilot/images/test-qualification"
      tags_all       = {}
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

run "null_preserves_module_managed_boundaries_and_fingerprints" {
  command   = apply
  state_key = "external-boundary-rollout"

  assert {
    condition = (
      length(data.aws_iam_policy.external_role_boundary) == 0 &&
      aws_iam_role.copy_target.permissions_boundary == aws_iam_policy.copy_role_boundary.arn &&
      aws_iam_role.lifecycle_target.permissions_boundary == aws_iam_policy.lifecycle_role_boundary.arn
    )
    error_message = "The null default must keep both existing module-managed boundary attachments without reading an external policy."
  }

  assert {
    condition = (
      output.role_fingerprints["us-east-1:copy_boundary_policy_hash"] == sha256(data.aws_iam_policy_document.copy_role_boundary.json) &&
      output.role_fingerprints["us-east-1:lifecycle_boundary_policy_hash"] == sha256(data.aws_iam_policy_document.lifecycle_role_boundary.json)
    )
    error_message = "The null default must preserve the existing module-managed boundary fingerprints byte for byte."
  }

  assert {
    condition = toset(keys(output.role_fingerprints)) == toset([
      "us-east-1:copy_role_arn",
      "us-east-1:copy_policy_hash",
      "us-east-1:lifecycle_role_arn",
      "us-east-1:lifecycle_policy_hash",
      "us-east-1:copy_boundary_policy_hash",
      "us-east-1:lifecycle_boundary_policy_hash",
      "us-east-1:qualification_repo_arn",
      "us-east-1:qualification_policy_hash",
      "us-east-1:qualification_ownership_tags_hash",
    ])
    error_message = "Adding the optional boundary must not change the role_fingerprints output schema."
  }
}

run "null_default_is_a_state_noop" {
  command   = plan
  state_key = "external-boundary-rollout"

  assert {
    condition = (
      output.copy_target_role_arn == run.null_preserves_module_managed_boundaries_and_fingerprints.copy_target_role_arn &&
      output.lifecycle_target_role_arn == run.null_preserves_module_managed_boundaries_and_fingerprints.lifecycle_target_role_arn &&
      output.role_fingerprints == run.null_preserves_module_managed_boundaries_and_fingerprints.role_fingerprints
    )
    error_message = "Replanning the null default must retain the exact role identities and fingerprint map from the existing state."
  }
}

run "external_boundary_is_attached_and_its_document_is_fingerprinted" {
  command   = plan
  state_key = "external-boundary-rollout"

  variables {
    permissions_boundary_arn = "arn:aws:iam::123456789012:policy/boundaries/organization"
  }

  assert {
    condition = (
      length(data.aws_iam_policy.external_role_boundary) == 1 &&
      data.aws_iam_policy.external_role_boundary[0].arn == var.permissions_boundary_arn &&
      aws_iam_role.copy_target.permissions_boundary == var.permissions_boundary_arn &&
      aws_iam_role.lifecycle_target.permissions_boundary == var.permissions_boundary_arn
    )
    error_message = "A configured external boundary must be read by exact ARN and attached to both target roles."
  }

  assert {
    condition = (
      aws_iam_role_policy.copy_target.policy == data.aws_iam_policy_document.copy_permissions.json &&
      aws_iam_role_policy.lifecycle_target.policy == data.aws_iam_policy_document.lifecycle_permissions.json &&
      one(values(aws_ecr_repository_policy.shard)).policy == one(values(data.aws_iam_policy_document.shard)).json &&
      aws_ecr_repository_policy.qualification.policy == data.aws_iam_policy_document.qualification.json
    )
    error_message = "Selecting an external boundary must not weaken or replace the exact inline or repository policies."
  }

  assert {
    condition = (
      aws_iam_policy.copy_role_boundary.policy == data.aws_iam_policy_document.copy_role_boundary.json &&
      aws_iam_policy.lifecycle_role_boundary.policy == data.aws_iam_policy_document.lifecycle_role_boundary.json
    )
    error_message = "The existing module-managed boundary resources and state addresses must remain intact for compatibility."
  }

  assert {
    condition = (
      output.role_fingerprints["us-east-1:copy_boundary_policy_hash"] == sha256(jsonencode(jsondecode(data.aws_iam_policy.external_role_boundary[0].policy))) &&
      output.role_fingerprints["us-east-1:lifecycle_boundary_policy_hash"] == sha256(jsonencode(jsondecode(data.aws_iam_policy.external_role_boundary[0].policy))) &&
      output.role_fingerprints["us-east-1:copy_boundary_policy_hash"] != sha256(data.aws_iam_policy_document.copy_role_boundary.json) &&
      output.role_fingerprints["us-east-1:lifecycle_boundary_policy_hash"] != sha256(data.aws_iam_policy_document.lifecycle_role_boundary.json) &&
      output.role_fingerprints["us-east-1:copy_boundary_policy_hash"] != sha256(var.permissions_boundary_arn)
    )
    error_message = "Qualification must fingerprint the attached external policy document, not either unattached module boundary or the ARN alone."
  }

  assert {
    condition = toset(keys(output.role_fingerprints)) == toset([
      "us-east-1:copy_role_arn",
      "us-east-1:copy_policy_hash",
      "us-east-1:lifecycle_role_arn",
      "us-east-1:lifecycle_policy_hash",
      "us-east-1:copy_boundary_policy_hash",
      "us-east-1:lifecycle_boundary_policy_hash",
      "us-east-1:qualification_repo_arn",
      "us-east-1:qualification_policy_hash",
      "us-east-1:qualification_ownership_tags_hash",
    ])
    error_message = "Using an external boundary must preserve every role_fingerprints key."
  }
}

run "rejects_cross_account_external_boundary" {
  command = plan

  variables {
    permissions_boundary_arn = "arn:aws:iam::210987654321:policy/boundaries/organization"
  }

  expect_failures = [terraform_data.validation]
}

run "rejects_cross_partition_external_boundary" {
  command = plan

  variables {
    permissions_boundary_arn = "arn:aws-us-gov:iam::123456789012:policy/boundaries/organization"
  }

  expect_failures = [terraform_data.validation]
}

run "defers_external_boundary_path_validation_to_the_provider" {
  command = plan

  variables {
    permissions_boundary_arn = "arn:aws:iam::123456789012:policy/delegated!boundaries/organization"
  }

  assert {
    condition     = aws_iam_role.copy_target.permissions_boundary == var.permissions_boundary_arn
    error_message = "The module must not reject provider-resolvable IAM policy path characters beyond the exact ARN boundary."
  }
}

run "rejects_non_policy_external_boundary_arn" {
  command = plan

  variables {
    permissions_boundary_arn = "arn:aws:iam::123456789012:role/organization-boundary"
  }

  expect_failures = [var.permissions_boundary_arn]
}
