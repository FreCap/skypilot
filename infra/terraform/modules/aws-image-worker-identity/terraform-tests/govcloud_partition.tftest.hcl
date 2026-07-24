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

  mock_data "aws_iam_policy_document" {
    defaults = {
      id            = "terraform-test-policy"
      json          = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
      minified_json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }

  override_resource {
    target          = aws_iam_role.copy
    override_during = plan
    values = {
      arn = "arn:aws-us-gov:iam::123456789012:role/skypilot-image-copy"
      id  = "skypilot-image-copy"
    }
  }

  override_resource {
    target          = aws_iam_role.lifecycle
    override_during = plan
    values = {
      arn = "arn:aws-us-gov:iam::123456789012:role/skypilot-image-lifecycle"
      id  = "skypilot-image-lifecycle"
    }
  }

  override_resource {
    target          = aws_iam_role.canary
    override_during = plan
    values = {
      arn = "arn:aws-us-gov:iam::123456789012:role/skypilot-image-canary"
      id  = "skypilot-image-canary"
    }
  }
}

variables {
  name_prefix       = "skypilot-image"
  oidc_provider_arn = "arn:aws-us-gov:iam::123456789012:oidc-provider/oidc.eks.us-gov-west-1.amazonaws.com/id/EXAMPLE"
  oidc_issuer_url   = "https://oidc.eks.us-gov-west-1.amazonaws.com/id/EXAMPLE"
  copy_target_role_arns = [
    "arn:aws-us-gov:iam::210987654321:role/registries/image-copy",
  ]
  lifecycle_target_role_arns = [
    "arn:aws-us-gov:iam::210987654321:role/registries/image-lifecycle",
  ]
  canary_target_role_arns = [
    "arn:aws-us-gov:iam::345678901234:role/compute/image-canary",
  ]
  permissions_boundary_arn = "arn:aws-us-gov:iam::123456789012:policy/boundaries/skypilot-image-workers"
}

run "govcloud_accepts_exact_partition_account_and_issuer" {
  command = plan

  assert {
    condition     = local.expected_oidc_provider_arn == var.oidc_provider_arn
    error_message = "The GovCloud partition must derive the exact configured OIDC provider ARN."
  }

  assert {
    condition = alltrue([
      for arn in local.target_role_arns : startswith(arn, "arn:aws-us-gov:iam::")
    ])
    error_message = "GovCloud target roles must remain in the aws-us-gov partition."
  }
}
