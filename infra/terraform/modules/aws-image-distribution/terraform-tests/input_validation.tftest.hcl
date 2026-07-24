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
      minified_json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
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
      runtime_pull_principal_arns  = ["arn:aws:iam::123456789012:role/runtime-pull"]
    }
  }
}

run "accepts_exact_iam_and_uuid_kms_boundaries" {
  command = plan

  variables {
    copy_worker_base_role_arns = [
      "arn:aws:iam::123456789012:role/${join("", [for _ in range(510) : "p"])}/${join("", [for _ in range(64) : "c"])}",
    ]
    lifecycle_worker_base_role_arns = [
      "arn:aws:iam::123456789012:role/${join("", [for _ in range(510) : "p"])}/${join("", [for _ in range(64) : "l"])}",
    ]
    copy_target_role_name      = join("", [for _ in range(64) : "c"])
    lifecycle_target_role_name = join("", [for _ in range(64) : "l"])
    encryption_type            = "KMS"
    kms_key_arn                = "arn:aws:kms:us-east-1:123456789012:key/00000000-0000-4000-8000-000000000001"
    targets = {
      canonical = {
        canonical                    = true
        shard_count                  = 1
        max_manifests_per_shard      = 800
        max_declared_bytes_per_shard = 1099511627776
        max_in_flight                = 10
        runtime_pull_principal_arns = [
          "arn:aws:iam::123456789012:role/${join("", [for _ in range(510) : "p"])}/${join("", [for _ in range(64) : "r"])}",
        ]
      }
    }
  }

  assert {
    condition     = aws_ecr_repository.qualification.encryption_configuration[0].kms_key == "arn:aws:kms:us-east-1:123456789012:key/00000000-0000-4000-8000-000000000001"
    error_message = "The exact boundary UUID KMS key ARN must reach the ECR repository."
  }
}

run "accepts_exact_mrk_kms_identifier" {
  command = plan

  variables {
    encryption_type = "KMS"
    kms_key_arn     = "arn:aws:kms:us-east-1:123456789012:key/mrk-00000000000040008000000000000001"
  }
}

run "accepts_minimum_worker_assume_role_external_id" {
  command = plan

  variables {
    worker_assume_role_external_id = "x-"
  }
}

run "accepts_maximum_worker_assume_role_external_id_and_allowed_characters" {
  command = plan

  variables {
    worker_assume_role_external_id = "Az09_+=,.@:/-${join("", [for _ in range(605) : "x"])}${join("", [for _ in range(606) : "x"])}"
  }
}

run "rejects_worker_assume_role_external_id_below_minimum_length" {
  command = plan

  variables {
    worker_assume_role_external_id = "x"
  }

  expect_failures = [var.worker_assume_role_external_id]
}

run "rejects_worker_assume_role_external_id_above_maximum_length" {
  command = plan

  variables {
    worker_assume_role_external_id = "Az09_+=,.@:/-${join("", [for _ in range(606) : "x"])}${join("", [for _ in range(606) : "x"])}"
  }

  expect_failures = [var.worker_assume_role_external_id]
}

run "rejects_worker_assume_role_external_id_characters_outside_sts_set" {
  command = plan

  variables {
    worker_assume_role_external_id = "not valid?"
  }

  expect_failures = [var.worker_assume_role_external_id]
}

run "rejects_role_trust_policy_quota_below_aws_default" {
  command = plan

  variables {
    applied_role_trust_policy_quota = 2047
  }

  expect_failures = [var.applied_role_trust_policy_quota]
}

run "rejects_role_trust_policy_quota_above_aws_maximum" {
  command = plan

  variables {
    applied_role_trust_policy_quota = 8193
  }

  expect_failures = [var.applied_role_trust_policy_quota]
}

run "rejects_overlong_iam_terminal_names" {
  command = plan

  variables {
    copy_worker_base_role_arns = [
      "arn:aws:iam::123456789012:role/${join("", [for _ in range(65) : "c"])}",
    ]
    lifecycle_worker_base_role_arns = [
      "arn:aws:iam::123456789012:role/${join("", [for _ in range(65) : "l"])}",
    ]
    targets = {
      canonical = {
        canonical                    = true
        shard_count                  = 1
        max_manifests_per_shard      = 800
        max_declared_bytes_per_shard = 1099511627776
        max_in_flight                = 10
        runtime_pull_principal_arns = [
          "arn:aws:iam::123456789012:role/${join("", [for _ in range(65) : "r"])}",
        ]
      }
    }
  }

  expect_failures = [
    var.copy_worker_base_role_arns,
    var.lifecycle_worker_base_role_arns,
    var.targets,
  ]
}

run "rejects_overlong_iam_paths" {
  command = plan

  variables {
    copy_worker_base_role_arns = [
      "arn:aws:iam::123456789012:role/${join("", [for _ in range(511) : "p"])}/copy",
    ]
    lifecycle_worker_base_role_arns = [
      "arn:aws:iam::123456789012:role/${join("", [for _ in range(511) : "p"])}/lifecycle",
    ]
    targets = {
      canonical = {
        canonical                    = true
        shard_count                  = 1
        max_manifests_per_shard      = 800
        max_declared_bytes_per_shard = 1099511627776
        max_in_flight                = 10
        runtime_pull_principal_arns = [
          "arn:aws:iam::123456789012:role/${join("", [for _ in range(511) : "p"])}/runtime",
        ]
      }
    }
  }

  expect_failures = [
    var.copy_worker_base_role_arns,
    var.lifecycle_worker_base_role_arns,
    var.targets,
  ]
}

run "rejects_wildcard_and_policy_variable_principals" {
  command = plan

  variables {
    copy_worker_base_role_arns      = ["*"]
    lifecycle_worker_base_role_arns = ["arn:aws:iam::123456789012:role/*"]
    targets = {
      canonical = {
        canonical                    = true
        shard_count                  = 1
        max_manifests_per_shard      = 800
        max_declared_bytes_per_shard = 1099511627776
        max_in_flight                = 10
        runtime_pull_principal_arns  = ["arn:aws:iam::123456789012:role/$${aws:username}"]
      }
    }
  }

  expect_failures = [
    var.copy_worker_base_role_arns,
    var.lifecycle_worker_base_role_arns,
    var.targets,
  ]
}

run "rejects_principal_collection_overflow" {
  command = plan

  variables {
    copy_worker_base_role_arns = [
      for index in range(65) :
      "arn:aws:iam::123456789012:role/copy-${index}"
    ]
    lifecycle_worker_base_role_arns = [
      for index in range(65) :
      "arn:aws:iam::123456789012:role/lifecycle-${index}"
    ]
    targets = {
      canonical = {
        canonical                    = true
        shard_count                  = 1
        max_manifests_per_shard      = 800
        max_declared_bytes_per_shard = 1099511627776
        max_in_flight                = 10
        runtime_pull_principal_arns = [
          for index in range(101) :
          "arn:aws:iam::123456789012:role/runtime-${index}"
        ]
      }
    }
  }

  expect_failures = [
    var.copy_worker_base_role_arns,
    var.lifecycle_worker_base_role_arns,
    var.targets,
  ]
}

run "rejects_invalid_target_role_names" {
  command = plan

  variables {
    copy_target_role_name      = join("", [for _ in range(65) : "c"])
    lifecycle_target_role_name = "invalid/role"
  }

  expect_failures = [
    var.copy_target_role_name,
    var.lifecycle_target_role_name,
  ]
}

run "rejects_malformed_kms_key_identifiers" {
  command = plan

  variables {
    encryption_type = "KMS"
    kms_key_arn     = "arn:aws:kms:us-east-1:123456789012:key/not-a-real-key"
  }

  expect_failures = [var.kms_key_arn]
}

run "rejects_kms_alias_arns" {
  command = plan

  variables {
    encryption_type = "KMS"
    kms_key_arn     = "arn:aws:kms:us-east-1:123456789012:alias/skypilot-images"
  }

  expect_failures = [var.kms_key_arn]
}

run "rejects_cross_partition_principals" {
  command = plan

  variables {
    copy_worker_base_role_arns      = ["arn:aws-cn:iam::123456789012:role/image-copy-worker"]
    lifecycle_worker_base_role_arns = ["arn:aws-cn:iam::123456789012:role/image-lifecycle-worker"]
    targets = {
      canonical = {
        canonical                    = true
        shard_count                  = 1
        max_manifests_per_shard      = 800
        max_declared_bytes_per_shard = 1099511627776
        max_in_flight                = 10
        runtime_pull_principal_arns  = ["arn:aws-cn:iam::123456789012:role/runtime-pull"]
      }
    }
  }

  expect_failures = [terraform_data.validation]
}

run "rejects_cross_partition_kms_keys" {
  command = plan

  variables {
    encryption_type = "KMS"
    kms_key_arn     = "arn:aws-cn:kms:us-east-1:123456789012:key/00000000-0000-4000-8000-000000000001"
  }

  expect_failures = [terraform_data.validation]
}

run "rejects_cross_account_kms_keys" {
  command = plan

  variables {
    encryption_type = "KMS"
    kms_key_arn     = "arn:aws:kms:us-east-1:210987654321:key/00000000-0000-4000-8000-000000000001"
  }

  expect_failures = [terraform_data.validation]
}

run "rejects_cross_region_kms_keys" {
  command = plan

  variables {
    encryption_type = "KMS"
    kms_key_arn     = "arn:aws:kms:us-west-2:123456789012:key/00000000-0000-4000-8000-000000000001"
  }

  expect_failures = [terraform_data.validation]
}
