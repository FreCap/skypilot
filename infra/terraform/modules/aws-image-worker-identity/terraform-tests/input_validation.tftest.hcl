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
}

variables {
  name_prefix       = "skypilot-image"
  oidc_provider_arn = "arn:aws:iam::123456789012:oidc-provider/oidc.eks.us-east-1.amazonaws.com/id/EXAMPLE"
  oidc_issuer_url   = "https://oidc.eks.us-east-1.amazonaws.com/id/EXAMPLE"
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

run "accepts_exact_input_boundaries" {
  command = plan

  variables {
    kubernetes_namespace = join("", [for _ in range(63) : "n"])
    copy_service_account = join(".", [
      join("", [for _ in range(63) : "a"]),
      join("", [for _ in range(63) : "b"]),
      join("", [for _ in range(63) : "c"]),
      join("", [for _ in range(61) : "d"]),
    ])
    copy_target_role_arns = toset([
      for i in range(64) :
      "arn:aws:iam::210987654321:role/image-copy-${format("%02d", i)}"
    ])
    lifecycle_target_role_arns = [
      "arn:aws:iam::210987654321:role/${join("", [for _ in range(510) : "p"])}/${join("", [for _ in range(64) : "r"])}",
    ]
    permissions_boundary_arn = "arn:aws:iam::123456789012:policy/${join("", [for _ in range(510) : "p"])}/${join("", [for _ in range(128) : "b"])}"
  }
}

run "accepts_no_permissions_boundary" {
  command = plan

  variables {
    permissions_boundary_arn = null
  }
}

run "accepts_percent_encoded_oidc_path" {
  command = plan

  variables {
    oidc_provider_arn = "arn:aws:iam::123456789012:oidc-provider/issuer.example.com/tenant%2Fone/id/%7eexample"
    oidc_issuer_url   = "https://issuer.example.com/tenant%2Fone/id/%7eexample"
  }
}

run "rejects_bare_percent_in_oidc_path" {
  command = plan

  variables {
    oidc_provider_arn = "arn:aws:iam::123456789012:oidc-provider/issuer.example.com/tenant%/id"
    oidc_issuer_url   = "https://issuer.example.com/tenant%/id"
  }

  expect_failures = [
    var.oidc_provider_arn,
    var.oidc_issuer_url,
  ]
}

run "rejects_one_digit_percent_escape_in_oidc_path" {
  command = plan

  variables {
    oidc_provider_arn = "arn:aws:iam::123456789012:oidc-provider/issuer.example.com/tenant%A/id"
    oidc_issuer_url   = "https://issuer.example.com/tenant%A/id"
  }

  expect_failures = [
    var.oidc_provider_arn,
    var.oidc_issuer_url,
  ]
}

run "rejects_non_hex_percent_escape_in_oidc_path" {
  command = plan

  variables {
    oidc_provider_arn = "arn:aws:iam::123456789012:oidc-provider/issuer.example.com/tenant%GG/id"
    oidc_issuer_url   = "https://issuer.example.com/tenant%GG/id"
  }

  expect_failures = [
    var.oidc_provider_arn,
    var.oidc_issuer_url,
  ]
}

run "rejects_wildcards_policy_variables_and_malformed_paths" {
  command = plan

  variables {
    oidc_provider_arn = "arn:aws:iam::123456789012:oidc-provider/oidc.eks.us-east-1.amazonaws.com/id/*"
    copy_target_role_arns = [
      "arn:aws:iam::210987654321:role/*",
    ]
    lifecycle_target_role_arns = [
      "arn:aws:iam::210987654321:role/$${aws:username}",
    ]
    canary_target_role_arns = [
      "arn:aws:iam::345678901234:role/team//image-canary",
    ]
    permissions_boundary_arn = "arn:aws:iam::123456789012:policy/*"
  }

  expect_failures = [
    var.oidc_provider_arn,
    var.copy_target_role_arns,
    var.lifecycle_target_role_arns,
    var.canary_target_role_arns,
    var.permissions_boundary_arn,
  ]
}

run "rejects_overlong_iam_terminal_names" {
  command = plan

  variables {
    copy_target_role_arns = [
      "arn:aws:iam::210987654321:role/${join("", [for _ in range(65) : "r"])}",
    ]
    lifecycle_target_role_arns = [
      "arn:aws:iam::210987654321:role/${join("", [for _ in range(65) : "r"])}",
    ]
    canary_target_role_arns = [
      "arn:aws:iam::345678901234:role/${join("", [for _ in range(65) : "r"])}",
    ]
    permissions_boundary_arn = "arn:aws:iam::123456789012:policy/${join("", [for _ in range(129) : "b"])}"
  }

  expect_failures = [
    var.copy_target_role_arns,
    var.lifecycle_target_role_arns,
    var.canary_target_role_arns,
    var.permissions_boundary_arn,
  ]
}

run "rejects_overlong_iam_paths" {
  command = plan

  variables {
    copy_target_role_arns = [
      "arn:aws:iam::210987654321:role/${join("", [for _ in range(511) : "p"])}/image-copy",
    ]
    lifecycle_target_role_arns = [
      "arn:aws:iam::210987654321:role/${join("", [for _ in range(511) : "p"])}/image-lifecycle",
    ]
    canary_target_role_arns = [
      "arn:aws:iam::345678901234:role/${join("", [for _ in range(511) : "p"])}/image-canary",
    ]
    permissions_boundary_arn = "arn:aws:iam::123456789012:policy/${join("", [for _ in range(511) : "p"])}/image-workers"
  }

  expect_failures = [
    var.copy_target_role_arns,
    var.lifecycle_target_role_arns,
    var.canary_target_role_arns,
    var.permissions_boundary_arn,
  ]
}

run "rejects_more_than_64_target_roles" {
  command = plan

  variables {
    copy_target_role_arns = toset([
      for i in range(65) :
      "arn:aws:iam::210987654321:role/image-copy-${format("%02d", i)}"
    ])
  }

  expect_failures = [var.copy_target_role_arns]
}

run "rejects_target_roles_over_the_inline_policy_budget" {
  command = plan

  variables {
    copy_target_role_arns = toset([
      for i in range(64) :
      "arn:aws:iam::210987654321:role/${join("", [for _ in range(200) : "p"])}/image-copy-${format("%02d", i)}"
    ])
  }

  expect_failures = [var.copy_target_role_arns]
}

run "rejects_non_https_issuer" {
  command = plan

  variables {
    oidc_issuer_url = "http://oidc.eks.us-east-1.amazonaws.com/id/EXAMPLE"
  }

  expect_failures = [var.oidc_issuer_url]
}

run "rejects_issuer_port" {
  command = plan

  variables {
    oidc_issuer_url = "https://oidc.eks.us-east-1.amazonaws.com:443/id/EXAMPLE"
  }

  expect_failures = [var.oidc_issuer_url]
}

run "rejects_issuer_query" {
  command = plan

  variables {
    oidc_issuer_url = "https://oidc.eks.us-east-1.amazonaws.com/id/EXAMPLE?tenant=one"
  }

  expect_failures = [var.oidc_issuer_url]
}

run "rejects_issuer_fragment" {
  command = plan

  variables {
    oidc_issuer_url = "https://oidc.eks.us-east-1.amazonaws.com/id/EXAMPLE#fragment"
  }

  expect_failures = [var.oidc_issuer_url]
}

run "rejects_issuer_trailing_slash" {
  command = plan

  variables {
    oidc_issuer_url = "https://oidc.eks.us-east-1.amazonaws.com/id/EXAMPLE/"
  }

  expect_failures = [var.oidc_issuer_url]
}

run "rejects_issuer_with_an_overlong_dns_label" {
  command = plan

  variables {
    oidc_issuer_url = "https://${join("", [for _ in range(64) : "a"])}.example.com/id/EXAMPLE"
  }

  expect_failures = [var.oidc_issuer_url]
}

run "rejects_overlong_oidc_identifiers" {
  command = plan

  variables {
    oidc_issuer_url   = "https://issuer.example.com/${join("", [for _ in range(232) : "p"])}"
    oidc_provider_arn = "arn:aws:iam::123456789012:oidc-provider/issuer.example.com/${join("", [for _ in range(280) : "p"])}"
  }

  expect_failures = [
    var.oidc_issuer_url,
    var.oidc_provider_arn,
  ]
}

run "rejects_invalid_kubernetes_dns_names" {
  command = plan

  variables {
    kubernetes_namespace      = "skypilot.system"
    copy_service_account      = "Image-Copy"
    lifecycle_service_account = join("", [for _ in range(64) : "a"])
    canary_service_account    = "image..canary"
  }

  expect_failures = [
    var.kubernetes_namespace,
    var.copy_service_account,
    var.lifecycle_service_account,
    var.canary_service_account,
  ]
}

run "rejects_overlong_kubernetes_dns_names" {
  command = plan

  variables {
    kubernetes_namespace = join("", [for _ in range(64) : "n"])
    copy_service_account = join(".", [
      join("", [for _ in range(63) : "a"]),
      join("", [for _ in range(63) : "b"]),
      join("", [for _ in range(63) : "c"]),
      join("", [for _ in range(62) : "d"]),
    ])
  }

  expect_failures = [
    var.kubernetes_namespace,
    var.copy_service_account,
  ]
}

run "rejects_oidc_provider_from_another_account" {
  command = plan

  variables {
    oidc_provider_arn = "arn:aws:iam::210987654321:oidc-provider/oidc.eks.us-east-1.amazonaws.com/id/EXAMPLE"
  }

  expect_failures = [terraform_data.validate_contract]
}

run "rejects_oidc_provider_from_another_partition" {
  command = plan

  variables {
    oidc_provider_arn = "arn:aws-cn:iam::123456789012:oidc-provider/oidc.eks.us-east-1.amazonaws.com/id/EXAMPLE"
  }

  expect_failures = [terraform_data.validate_contract]
}

run "rejects_oidc_provider_for_another_issuer_path" {
  command = plan

  variables {
    oidc_provider_arn = "arn:aws:iam::123456789012:oidc-provider/oidc.eks.us-east-1.amazonaws.com/id/OTHER"
  }

  expect_failures = [terraform_data.validate_contract]
}

run "rejects_oidc_provider_for_another_issuer_authority" {
  command = plan

  variables {
    oidc_provider_arn = "arn:aws:iam::123456789012:oidc-provider/issuer.example.com/id/EXAMPLE"
  }

  expect_failures = [terraform_data.validate_contract]
}

run "rejects_target_roles_from_another_partition" {
  command = plan

  variables {
    canary_target_role_arns = [
      "arn:aws-us-gov:iam::345678901234:role/compute/image-canary",
    ]
  }

  expect_failures = [terraform_data.validate_contract]
}

run "rejects_permissions_boundary_from_another_account" {
  command = plan

  variables {
    permissions_boundary_arn = "arn:aws:iam::210987654321:policy/boundaries/skypilot-image-workers"
  }

  expect_failures = [terraform_data.validate_contract]
}

run "rejects_permissions_boundary_from_another_partition" {
  command = plan

  variables {
    permissions_boundary_arn = "arn:aws-cn:iam::123456789012:policy/boundaries/skypilot-image-workers"
  }

  expect_failures = [terraform_data.validate_contract]
}
