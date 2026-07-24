mock_provider "aws" {
  override_during = plan

  mock_data "aws_caller_identity" {
    defaults = {
      account_id = "123456789012"
      arn        = "arn:aws-cn:iam::123456789012:role/terraform-test"
      id         = "123456789012"
      user_id    = "terraform-test"
    }
  }

  mock_data "aws_partition" {
    defaults = {
      dns_suffix         = "amazonaws.com.cn"
      id                 = "aws-cn"
      partition          = "aws-cn"
      reverse_dns_prefix = "cn.com.amazonaws"
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
      arn = "arn:aws-cn:iam::123456789012:role/skypilot-image-copy"
      id  = "skypilot-image-copy"
    }
  }

  override_resource {
    target          = aws_iam_role.lifecycle
    override_during = plan
    values = {
      arn = "arn:aws-cn:iam::123456789012:role/skypilot-image-lifecycle"
      id  = "skypilot-image-lifecycle"
    }
  }

  override_resource {
    target          = aws_iam_role.canary
    override_during = plan
    values = {
      arn = "arn:aws-cn:iam::123456789012:role/skypilot-image-canary"
      id  = "skypilot-image-canary"
    }
  }
}

variables {
  name_prefix       = "skypilot-image"
  oidc_provider_arn = "arn:aws-cn:iam::123456789012:oidc-provider/oidc.eks.cn-north-1.amazonaws.com.cn/id/EXAMPLE"
  oidc_issuer_url   = "https://oidc.eks.cn-north-1.amazonaws.com.cn/id/EXAMPLE"
  copy_target_role_arns = [
    "arn:aws-cn:iam::210987654321:role/registries/image-copy",
  ]
  lifecycle_target_role_arns = [
    "arn:aws-cn:iam::210987654321:role/registries/image-lifecycle",
  ]
  canary_target_role_arns = [
    "arn:aws-cn:iam::345678901234:role/compute/image-canary",
  ]
  permissions_boundary_arn = "arn:aws-cn:iam::123456789012:policy/boundaries/skypilot-image-workers"
}

run "china_accepts_exact_partition_account_and_issuer" {
  command = plan

  assert {
    condition     = local.expected_oidc_provider_arn == var.oidc_provider_arn
    error_message = "The China partition must derive the exact configured OIDC provider ARN."
  }

  assert {
    condition = alltrue([
      for arn in local.target_role_arns : startswith(arn, "arn:aws-cn:iam::")
    ])
    error_message = "China target roles must remain in the aws-cn partition."
  }
}
