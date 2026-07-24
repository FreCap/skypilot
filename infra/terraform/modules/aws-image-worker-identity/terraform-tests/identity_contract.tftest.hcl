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
      arn = "arn:aws:iam::123456789012:role/skypilot-image-copy"
      id  = "skypilot-image-copy"
    }
  }

  override_resource {
    target          = aws_iam_role.lifecycle
    override_during = plan
    values = {
      arn = "arn:aws:iam::123456789012:role/skypilot-image-lifecycle"
      id  = "skypilot-image-lifecycle"
    }
  }

  override_resource {
    target          = aws_iam_role.canary
    override_during = plan
    values = {
      arn = "arn:aws:iam::123456789012:role/skypilot-image-canary"
      id  = "skypilot-image-canary"
    }
  }
}

variables {
  name_prefix               = "skypilot-image"
  oidc_provider_arn         = "arn:aws:iam::123456789012:oidc-provider/oidc.eks.us-east-1.amazonaws.com/id/EXAMPLE"
  oidc_issuer_url           = "https://oidc.eks.us-east-1.amazonaws.com/id/EXAMPLE"
  kubernetes_namespace      = "skypilot-system"
  copy_service_account      = "image-copy"
  lifecycle_service_account = "image-lifecycle"
  canary_service_account    = "image-canary"
  copy_target_role_arns = [
    "arn:aws:iam::210987654321:role/registries/image-copy",
  ]
  lifecycle_target_role_arns = [
    "arn:aws:iam::210987654321:role/registries/image-lifecycle",
  ]
  canary_target_role_arns = [
    "arn:aws:iam::345678901234:role/compute/image-canary",
  ]
  permissions_boundary_arn = "arn:aws:iam::123456789012:policy/boundaries/skypilot-image-workers"
}

run "exact_irsa_subjects_and_target_roles_are_isolated" {
  command = plan

  assert {
    condition = alltrue([
      for trust in [
        data.aws_iam_policy_document.copy_trust,
        data.aws_iam_policy_document.lifecycle_trust,
        data.aws_iam_policy_document.canary_trust,
      ] :
      toset(one(one(trust.statement).principals).identifiers) == toset([
        var.oidc_provider_arn,
      ])
    ])
    error_message = "Every worker trust policy must use the exact correlated OIDC provider."
  }

  assert {
    condition = toset(flatten([
      for condition in one(data.aws_iam_policy_document.copy_trust.statement).condition :
      condition.values
      if condition.variable == "${local.oidc_issuer}:sub"
      ])) == toset([
      "system:serviceaccount:${var.kubernetes_namespace}:${var.copy_service_account}",
    ])
    error_message = "The copy role must trust only the configured copy service account."
  }

  assert {
    condition = toset(flatten([
      for condition in one(data.aws_iam_policy_document.lifecycle_trust.statement).condition :
      condition.values
      if condition.variable == "${local.oidc_issuer}:sub"
      ])) == toset([
      "system:serviceaccount:${var.kubernetes_namespace}:${var.lifecycle_service_account}",
    ])
    error_message = "The lifecycle role must trust only the configured lifecycle service account."
  }

  assert {
    condition = toset(flatten([
      for condition in one(data.aws_iam_policy_document.canary_trust.statement).condition :
      condition.values
      if condition.variable == "${local.oidc_issuer}:sub"
      ])) == toset([
      "system:serviceaccount:${var.kubernetes_namespace}:${var.canary_service_account}",
    ])
    error_message = "The canary role must trust only the configured canary service account."
  }

  assert {
    condition = alltrue([
      for trust in [
        data.aws_iam_policy_document.copy_trust,
        data.aws_iam_policy_document.lifecycle_trust,
        data.aws_iam_policy_document.canary_trust,
      ] :
      toset(flatten([
        for condition in one(trust.statement).condition :
        condition.values
        if condition.variable == "${local.oidc_issuer}:aud"
      ])) == toset(["sts.amazonaws.com"])
    ])
    error_message = "Every worker trust policy must require the STS audience."
  }

  assert {
    condition = (
      toset(one(one(data.aws_iam_policy_document.copy_assume_targets).statement).resources) == var.copy_target_role_arns &&
      toset(one(one(data.aws_iam_policy_document.lifecycle_assume_targets).statement).resources) == var.lifecycle_target_role_arns &&
      toset(one(one(data.aws_iam_policy_document.canary_assume_targets).statement).resources) == var.canary_target_role_arns
    )
    error_message = "Each worker must receive only its own exact target-role set."
  }

  assert {
    condition = alltrue([
      toset(one(one(data.aws_iam_policy_document.copy_assume_targets).statement).actions) == toset([
        "sts:AssumeRole",
        "sts:TagSession",
      ]),
      toset(one(one(data.aws_iam_policy_document.lifecycle_assume_targets).statement).actions) == toset([
        "sts:AssumeRole",
        "sts:TagSession",
      ]),
      toset(one(one(data.aws_iam_policy_document.canary_assume_targets).statement).actions) == toset([
        "sts:AssumeRole",
        "sts:TagSession",
      ]),
    ])
    error_message = "Target policies must grant only the two required STS actions."
  }

  assert {
    condition = alltrue([
      for role in [
        aws_iam_role.copy,
        aws_iam_role.lifecycle,
        aws_iam_role.canary,
      ] : role.permissions_boundary == var.permissions_boundary_arn
    ])
    error_message = "Every worker role must use the exact validated permissions boundary."
  }
}
