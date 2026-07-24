mock_provider "aws" {
  override_during = plan

  mock_data "aws_caller_identity" {
    defaults = {
      account_id = "123456789012"
      arn        = "arn:aws-us-gov:iam::123456789012:role/terraform-test"
      id         = "123456789012"
      user_id    = "terraform-test"
    }
  }

  mock_data "aws_partition" {
    defaults = {
      dns_suffix         = "amazonaws.com"
      id                 = "aws-us-gov"
      partition          = "aws-us-gov"
      reverse_dns_prefix = "com.amazonaws"
    }
  }

  mock_data "aws_region" {
    defaults = {
      description = "AWS GovCloud (US-West)"
      endpoint    = "ecr.us-gov-west-1.amazonaws.com"
      id          = "us-gov-west-1"
      name        = "us-gov-west-1"
      region      = "us-gov-west-1"
    }
  }

  mock_data "aws_iam_policy" {
    defaults = {
      arn         = "arn:aws-us-gov:iam::123456789012:policy/boundaries/organization"
      description = "Organization-managed GovCloud permissions boundary"
      id          = "arn:aws-us-gov:iam::123456789012:policy/boundaries/organization"
      name        = "organization"
      path        = "/boundaries/"
      policy      = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Sid\":\"OrganizationBoundary\",\"Effect\":\"Allow\",\"Action\":[\"ecr:*\",\"servicequotas:GetAWSDefaultServiceQuota\",\"servicequotas:GetServiceQuota\"],\"Resource\":\"*\"}]}"
      policy_id   = "ANPAGOVEXTERNALBOUNDARY"
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
      arn            = "arn:aws-us-gov:ecr:us-gov-west-1:123456789012:repository/skypilot/images/test-shard"
      registry_id    = "123456789012"
      repository_url = "123456789012.dkr.ecr.us-gov-west-1.amazonaws.com/skypilot/images/test-shard"
    }
  }

  override_resource {
    target          = aws_ecr_repository.qualification
    override_during = plan
    values = {
      arn            = "arn:aws-us-gov:ecr:us-gov-west-1:123456789012:repository/skypilot/images/test-qualification"
      registry_id    = "123456789012"
      repository_url = "123456789012.dkr.ecr.us-gov-west-1.amazonaws.com/skypilot/images/test-qualification"
    }
  }

  override_resource {
    target          = aws_iam_policy.copy_role_boundary
    override_during = plan
    values = {
      arn = "arn:aws-us-gov:iam::123456789012:policy/image-copy-target-boundary"
      id  = "arn:aws-us-gov:iam::123456789012:policy/image-copy-target-boundary"
    }
  }

  override_resource {
    target          = aws_iam_policy.lifecycle_role_boundary
    override_during = plan
    values = {
      arn = "arn:aws-us-gov:iam::123456789012:policy/image-lifecycle-target-boundary"
      id  = "arn:aws-us-gov:iam::123456789012:policy/image-lifecycle-target-boundary"
    }
  }

  override_resource {
    target          = aws_iam_role.copy_target
    override_during = plan
    values = {
      arn = "arn:aws-us-gov:iam::123456789012:role/image-copy-target"
      id  = "image-copy-target"
    }
  }

  override_resource {
    target          = aws_iam_role.lifecycle_target
    override_during = plan
    values = {
      arn = "arn:aws-us-gov:iam::123456789012:role/image-lifecycle-target"
      id  = "image-lifecycle-target"
    }
  }
}

variables {
  catalog_authority                   = "00000000-0000-4000-8000-000000000001"
  catalog_authority_base32            = "aaaaaaaaabaabaaaaaaaaaaaae"
  realm                               = "terraform-test"
  profile                             = "default"
  registry_account_id                 = "123456789012"
  region                              = "us-gov-west-1"
  workspaces                          = ["workspace-a"]
  copy_worker_base_role_arns          = ["arn:aws-us-gov:iam::123456789012:role/workers/image-copy-worker"]
  lifecycle_worker_base_role_arns     = ["arn:aws-us-gov:iam::123456789012:role/workers/image-lifecycle-worker"]
  copy_target_role_name               = "image-copy-target"
  lifecycle_target_role_name          = "image-lifecycle-target"
  encryption_type                     = "KMS"
  kms_key_arn                         = "arn:aws-us-gov:kms:us-gov-west-1:123456789012:key/mrk-00000000000040008000000000000001"
  applied_images_per_repository_quota = 1000
  quota_headroom                      = 100
  targets = {
    canonical = {
      canonical                    = true
      shard_count                  = 1
      max_manifests_per_shard      = 800
      max_declared_bytes_per_shard = 1099511627776
      max_in_flight                = 10
      runtime_pull_principal_arns  = ["arn:aws-us-gov:iam::210987654321:role/runtime/image-pull"]
    }
  }
}

run "govcloud_accepts_exact_principals_and_kms_key" {
  command = plan

  assert {
    condition     = aws_ecr_repository.qualification.encryption_configuration[0].kms_key == "arn:aws-us-gov:kms:us-gov-west-1:123456789012:key/mrk-00000000000040008000000000000001"
    error_message = "GovCloud must preserve the exact target-scoped multi-Region KMS key ARN."
  }

  assert {
    condition = toset(flatten([
      for principal in one([
        for statement in data.aws_iam_policy_document.copy_trust.statement : statement
        if statement.sid == "ExactCopyWorkerPrincipals"
      ]).principals : principal.identifiers
      ])) == toset([
      "arn:aws-us-gov:iam::123456789012:role/workers/image-copy-worker",
    ])
    error_message = "GovCloud target-role trust must preserve the exact worker principal."
  }
}

run "govcloud_accepts_same_account_external_boundary" {
  command = plan

  variables {
    permissions_boundary_arn = "arn:aws-us-gov:iam::123456789012:policy/boundaries/organization"
  }

  assert {
    condition = (
      aws_iam_role.copy_target.permissions_boundary == var.permissions_boundary_arn &&
      aws_iam_role.lifecycle_target.permissions_boundary == var.permissions_boundary_arn
    )
    error_message = "GovCloud target roles must attach an exact same-account GovCloud boundary."
  }

  assert {
    condition = (
      output.role_fingerprints["us-gov-west-1:copy_boundary_policy_hash"] == sha256(jsonencode(jsondecode(data.aws_iam_policy.external_role_boundary[0].policy))) &&
      output.role_fingerprints["us-gov-west-1:lifecycle_boundary_policy_hash"] == sha256(jsonencode(jsondecode(data.aws_iam_policy.external_role_boundary[0].policy)))
    )
    error_message = "GovCloud qualification must fingerprint the attached external boundary document."
  }
}
