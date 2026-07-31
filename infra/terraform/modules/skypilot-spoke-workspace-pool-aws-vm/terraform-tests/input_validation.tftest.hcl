mock_provider "aws" {
  override_during = plan

  mock_data "aws_caller_identity" {
    defaults = {
      account_id = "444455556666"
      arn        = "arn:aws:iam::444455556666:role/terraform-test"
      id         = "444455556666"
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

  mock_data "aws_iam_policy_document" {
    defaults = {
      id            = "terraform-test-policy"
      json          = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
      minified_json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }
}

variables {
  controller_role_arn = "arn:aws:iam::111122223333:role/skypilot-api"
}

run "rejects_an_empty_controller_role_arn" {
  command = plan

  variables {
    controller_role_arn = ""
  }

  expect_failures = [var.controller_role_arn]
}

run "rejects_a_controller_from_another_partition" {
  command = plan

  variables {
    controller_role_arn = "arn:aws-cn:iam::111122223333:role/skypilot-api"
  }

  expect_failures = [aws_iam_role.vm]
}

run "rejects_an_invalid_permissions_boundary" {
  command = plan

  variables {
    permissions_boundary_arn = "arn:aws:iam::444455556666:role/not-a-policy"
  }

  expect_failures = [var.permissions_boundary_arn]
}

run "rejects_a_boundary_from_another_account" {
  command = plan

  variables {
    permissions_boundary_arn = "arn:aws:iam::111122223333:policy/skypilot-boundary"
  }

  expect_failures = [aws_iam_role.vm]
}

run "rejects_an_extra_policy_from_another_account" {
  command = plan

  variables {
    vm_role_extra_policy_arns = [
      "arn:aws:iam::111122223333:policy/not-attachable-in-this-account",
    ]
  }

  expect_failures = [aws_iam_role.vm]
}

run "rejects_an_invalid_role_name" {
  command = plan

  variables {
    instance_profile_name = "invalid/name"
  }

  expect_failures = [var.instance_profile_name]
}

run "rejects_invalid_extra_policy_json" {
  command = plan

  variables {
    vm_role_extra_policy_json = "{not-json"
  }

  expect_failures = [var.vm_role_extra_policy_json]
}

run "rejects_a_dataset_from_another_partition" {
  command = plan

  variables {
    vm_dataset_grants = [{
      bucket_arn  = "arn:aws-cn:s3:::example-training-data"
      kms_key_arn = null
    }]
  }

  expect_failures = [aws_iam_role.vm]
}
