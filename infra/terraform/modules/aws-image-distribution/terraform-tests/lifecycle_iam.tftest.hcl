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

run "canonical_is_readable_but_never_deletable" {
  command = plan

  assert {
    condition     = aws_iam_role.copy_target.permissions_boundary == aws_iam_policy.copy_role_boundary.arn
    error_message = "The module-owned copy target role must attach the module-owned copy permissions boundary."
  }

  assert {
    condition     = aws_iam_role.lifecycle_target.permissions_boundary == aws_iam_policy.lifecycle_role_boundary.arn
    error_message = "The module-owned lifecycle target role must attach the module-owned lifecycle permissions boundary."
  }

  assert {
    condition     = aws_iam_role_policy.copy_target.role == aws_iam_role.copy_target.id
    error_message = "The copy identity policy must attach to the module-owned copy target role."
  }

  assert {
    condition     = aws_iam_role_policy.lifecycle_target.role == aws_iam_role.lifecycle_target.id
    error_message = "The lifecycle identity policy must attach to the module-owned lifecycle target role."
  }

  assert {
    condition     = output.copy_target_role_arn == aws_iam_role.copy_target.arn
    error_message = "The copy role output must identify the role whose boundary and inline policy are managed here."
  }

  assert {
    condition     = output.lifecycle_target_role_arn == aws_iam_role.lifecycle_target.arn
    error_message = "The lifecycle role output must identify the role whose boundary and inline policy are managed here."
  }

  assert {
    condition = toset(one([
      for statement in data.aws_iam_policy_document.lifecycle_role_boundary.statement : statement
      if statement.sid == "LifecycleReadAllManagedRepositories"
      ]).actions) == toset([
      "ecr:BatchGetImage",
      "ecr:DescribeImages",
      "ecr:ListImages",
    ])
    error_message = "The lifecycle permissions boundary must allow the exact read surface on every managed repository."
  }

  assert {
    condition = toset(one([
      for statement in data.aws_iam_policy_document.lifecycle_role_boundary.statement : statement
      if statement.sid == "LifecycleReadAllManagedRepositories"
      ]).resources) == toset(concat(
      [for repository in aws_ecr_repository.shard : repository.arn],
      [aws_ecr_repository.qualification.arn],
    ))
    error_message = "The lifecycle permissions boundary must allow reads on canonical shards and qualification."
  }

  assert {
    condition = toset(one([
      for statement in data.aws_iam_policy_document.lifecycle_role_boundary.statement : statement
      if statement.sid == "LifecycleDeleteEligibleRepositories"
      ]).actions) == toset([
      "ecr:BatchDeleteImage",
    ])
    error_message = "The lifecycle permissions boundary must isolate delete into its own statement."
  }

  assert {
    condition = toset(one([
      for statement in data.aws_iam_policy_document.lifecycle_role_boundary.statement : statement
      if statement.sid == "LifecycleDeleteEligibleRepositories"
      ]).resources) == toset([
      aws_ecr_repository.qualification.arn,
    ])
    error_message = "The lifecycle permissions boundary must exclude canonical shards from delete authority."
  }

  assert {
    condition = toset(one([
      for statement in data.aws_iam_policy_document.lifecycle_permissions.statement : statement
      if statement.sid == "ReadAllManagedContent"
      ]).actions) == toset([
      "ecr:BatchGetImage",
      "ecr:DescribeImages",
      "ecr:ListImages",
    ])
    error_message = "The lifecycle identity policy must isolate exact reads from delete authority."
  }

  assert {
    condition = toset(one([
      for statement in data.aws_iam_policy_document.lifecycle_permissions.statement : statement
      if statement.sid == "ReadAllManagedContent"
      ]).resources) == toset(concat(
      [for repository in aws_ecr_repository.shard : repository.arn],
      [aws_ecr_repository.qualification.arn],
    ))
    error_message = "The lifecycle identity policy must allow canonical exact-manifest reads."
  }

  assert {
    condition = toset(one([
      for statement in data.aws_iam_policy_document.lifecycle_permissions.statement : statement
      if statement.sid == "DeleteEligibleManagedContent"
      ]).actions) == toset([
      "ecr:BatchDeleteImage",
    ])
    error_message = "The lifecycle identity policy must isolate delete into its own statement."
  }

  assert {
    condition = toset(one([
      for statement in data.aws_iam_policy_document.lifecycle_permissions.statement : statement
      if statement.sid == "DeleteEligibleManagedContent"
      ]).resources) == toset([
      aws_ecr_repository.qualification.arn,
    ])
    error_message = "The lifecycle identity policy must exclude canonical shards from delete authority."
  }

  assert {
    condition = length([
      for statement in one(values(data.aws_iam_policy_document.shard)).statement : statement
      if statement.sid == "SkyPilotLifecycleRead" && toset(statement.actions) == toset([
        "ecr:BatchGetImage",
        "ecr:DescribeImages",
        "ecr:ListImages",
      ])
    ]) == 1
    error_message = "A canonical repository policy must grant lifecycle read authority."
  }

  assert {
    condition = length([
      for statement in one(values(data.aws_iam_policy_document.shard)).statement : statement
      if statement.sid == "SkyPilotLifecycleDelete" || contains(statement.actions, "ecr:BatchDeleteImage")
    ]) == 0
    error_message = "A canonical repository policy must never grant lifecycle delete authority."
  }

  assert {
    condition = length([
      for statement in data.aws_iam_policy_document.qualification.statement : statement
      if statement.sid == "SkyPilotQualificationLifecycleRead" && toset(statement.actions) == toset([
        "ecr:BatchGetImage",
        "ecr:DescribeImages",
        "ecr:ListImages",
      ])
    ]) == 1
    error_message = "The qualification repository policy must grant lifecycle read authority."
  }

  assert {
    condition = length([
      for statement in data.aws_iam_policy_document.qualification.statement : statement
      if statement.sid == "SkyPilotQualificationLifecycleDelete" && toset(statement.actions) == toset([
        "ecr:BatchDeleteImage",
      ])
    ]) == 1
    error_message = "The qualification repository policy must grant lifecycle delete authority."
  }
}

run "regional_is_readable_and_deletable" {
  command = plan

  variables {
    targets = {
      aws-us-east-1 = {
        canonical                    = false
        shard_count                  = 1
        max_manifests_per_shard      = 800
        max_declared_bytes_per_shard = 1099511627776
        max_in_flight                = 10
        runtime_pull_principal_arns  = []
      }
    }
  }

  assert {
    condition = toset(one([
      for statement in data.aws_iam_policy_document.lifecycle_role_boundary.statement : statement
      if statement.sid == "LifecycleDeleteEligibleRepositories"
      ]).resources) == toset(concat(
      [for repository in aws_ecr_repository.shard : repository.arn],
      [aws_ecr_repository.qualification.arn],
    ))
    error_message = "The lifecycle permissions boundary must allow delete on regional shards and qualification."
  }

  assert {
    condition = toset(one([
      for statement in data.aws_iam_policy_document.lifecycle_permissions.statement : statement
      if statement.sid == "DeleteEligibleManagedContent"
      ]).resources) == toset(concat(
      [for repository in aws_ecr_repository.shard : repository.arn],
      [aws_ecr_repository.qualification.arn],
    ))
    error_message = "The lifecycle identity policy must allow delete on regional shards and qualification."
  }

  assert {
    condition = length([
      for statement in one(values(data.aws_iam_policy_document.shard)).statement : statement
      if statement.sid == "SkyPilotLifecycleRead" && toset(statement.actions) == toset([
        "ecr:BatchGetImage",
        "ecr:DescribeImages",
        "ecr:ListImages",
      ])
    ]) == 1
    error_message = "A regional repository policy must grant lifecycle read authority."
  }

  assert {
    condition = length([
      for statement in one(values(data.aws_iam_policy_document.shard)).statement : statement
      if statement.sid == "SkyPilotLifecycleDelete" && toset(statement.actions) == toset([
        "ecr:BatchDeleteImage",
      ])
    ]) == 1
    error_message = "A regional repository policy must grant lifecycle delete authority."
  }
}
