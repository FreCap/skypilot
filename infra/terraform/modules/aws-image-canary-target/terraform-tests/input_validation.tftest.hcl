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
    target          = aws_iam_role.canary
    override_during = plan
    values = {
      arn = "arn:aws:iam::123456789012:role/skypilot-image-canary"
      id  = "skypilot-image-canary"
    }
  }
}

variables {
  role_name                      = "skypilot-image-canary"
  canary_worker_role_arns        = ["arn:aws:iam::123456789012:role/skypilot-image-canary-worker"]
  catalog_authority              = "00000000-0000-4000-8000-000000000001"
  ec2_runtime_role_arns          = ["arn:aws:iam::123456789012:role/skypilot-runtime"]
  ec2_instance_profile_arns      = ["arn:aws:iam::123456789012:instance-profile/skypilot-runtime"]
  eks_node_instance_profile_arns = ["arn:aws:iam::123456789012:instance-profile/skypilot-eks-node"]
  ami_arns                       = ["arn:aws:ec2:us-east-1::image/ami-00000000000000001"]
  subnet_arns                    = ["arn:aws:ec2:us-east-1:123456789012:subnet/subnet-00000000000000001"]
  security_group_arns            = ["arn:aws:ec2:us-east-1:123456789012:security-group/sg-00000000000000001"]
  canary_instance_types          = ["g5.xlarge"]
  spot_service_linked_role_arn   = "arn:aws:iam::123456789012:role/aws-service-role/spot.amazonaws.com/AWSServiceRoleForEC2Spot"
}

run "accepts_exact_aws_identifier_boundaries" {
  command = plan

  variables {
    role_name = join("", [for _ in range(64) : "r"])
    canary_worker_role_arns = [
      "arn:aws:iam::123456789012:role/${join("", [for _ in range(510) : "p"])}/${join("", [for _ in range(64) : "r"])}",
    ]
    ec2_runtime_role_arns = [
      "arn:aws:iam::123456789012:role/${join("", [for _ in range(510) : "p"])}/${join("", [for _ in range(64) : "r"])}",
    ]
    ec2_instance_profile_arns = [
      "arn:aws:iam::123456789012:instance-profile/${join("", [for _ in range(510) : "p"])}/${join("", [for _ in range(128) : "i"])}",
    ]
    eks_node_instance_profile_arns = [
      "arn:aws:iam::123456789012:instance-profile/${join("", [for _ in range(510) : "p"])}/${join("", [for _ in range(128) : "n"])}",
    ]
    ami_arns = [
      "arn:aws:ec2:us-east-1::image/ami-01234567",
      "arn:aws:ec2:us-east-1::image/ami-0123456789abcdef0",
    ]
    subnet_arns = [
      "arn:aws:ec2:us-east-1:123456789012:subnet/subnet-01234567",
      "arn:aws:ec2:us-east-1:123456789012:subnet/subnet-0123456789abcdef0",
    ]
    security_group_arns = [
      "arn:aws:ec2:us-east-1:123456789012:security-group/sg-01234567",
      "arn:aws:ec2:us-east-1:123456789012:security-group/sg-0123456789abcdef0",
    ]
    spot_customer_managed_kms_key_arns = [
      "arn:aws:kms:us-east-1:123456789012:key/00000000-0000-4000-8000-000000000001",
      "arn:aws:kms:us-east-1:123456789012:key/mrk-00000000000040008000000000000001",
    ]
    eks_cluster_arns = [
      "arn:aws:eks:us-east-1:123456789012:cluster/c${join("", [for _ in range(99) : "x"])}",
    ]
    permissions_boundary_arn = "arn:aws:iam::123456789012:policy/${join("", [for _ in range(510) : "p"])}/${join("", [for _ in range(128) : "b"])}"
  }
}

run "accepts_same_region_cross_account_spot_kms_key" {
  command = plan

  variables {
    spot_customer_managed_kms_key_arns = [
      "arn:aws:kms:us-east-1:210987654321:key/00000000-0000-4000-8000-000000000001",
    ]
  }

  assert {
    condition     = aws_kms_grant.spot_encrypted_ami["arn:aws:kms:us-east-1:210987654321:key/00000000-0000-4000-8000-000000000001"].key_id == "arn:aws:kms:us-east-1:210987654321:key/00000000-0000-4000-8000-000000000001"
    error_message = "A same-partition, same-region key owned by the AMI source account must reach the Spot grant unchanged."
  }
}

run "accepts_minimum_external_id" {
  command = plan

  variables {
    external_id = "x-"
  }
}

run "accepts_maximum_external_id_and_allowed_characters" {
  command = plan

  variables {
    external_id = "Az09_+=,.@:/-${join("", [for _ in range(605) : "x"])}${join("", [for _ in range(606) : "x"])}"
  }
}

run "rejects_external_id_below_minimum_length" {
  command = plan

  variables {
    external_id = "x"
  }

  expect_failures = [var.external_id]
}

run "rejects_external_id_above_maximum_length" {
  command = plan

  variables {
    external_id = "Az09_+=,.@:/-${join("", [for _ in range(606) : "x"])}${join("", [for _ in range(606) : "x"])}"
  }

  expect_failures = [var.external_id]
}

run "rejects_external_id_characters_outside_sts_set" {
  command = plan

  variables {
    external_id = "not valid?"
  }

  expect_failures = [var.external_id]
}

run "rejects_cross_partition_spot_kms_key" {
  command = plan

  variables {
    spot_customer_managed_kms_key_arns = [
      "arn:aws-cn:kms:us-east-1:210987654321:key/00000000-0000-4000-8000-000000000001",
    ]
  }

  expect_failures = [terraform_data.validate_contract]
}

run "rejects_cross_region_spot_kms_key" {
  command = plan

  variables {
    spot_customer_managed_kms_key_arns = [
      "arn:aws:kms:us-west-2:210987654321:key/00000000-0000-4000-8000-000000000001",
    ]
  }

  expect_failures = [terraform_data.validate_contract]
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
    role_name = join("", [for _ in range(65) : "r"])
    canary_worker_role_arns = [
      "arn:aws:iam::123456789012:role/${join("", [for _ in range(65) : "r"])}",
    ]
    ec2_runtime_role_arns = [
      "arn:aws:iam::123456789012:role/${join("", [for _ in range(65) : "r"])}",
    ]
    ec2_instance_profile_arns = [
      "arn:aws:iam::123456789012:instance-profile/${join("", [for _ in range(129) : "i"])}",
    ]
    eks_node_instance_profile_arns = [
      "arn:aws:iam::123456789012:instance-profile/${join("", [for _ in range(129) : "i"])}",
    ]
    permissions_boundary_arn = "arn:aws:iam::123456789012:policy/${join("", [for _ in range(129) : "b"])}"
  }

  expect_failures = [
    var.role_name,
    var.canary_worker_role_arns,
    var.ec2_runtime_role_arns,
    var.ec2_instance_profile_arns,
    var.eks_node_instance_profile_arns,
    var.permissions_boundary_arn,
  ]
}

run "rejects_overlong_iam_paths" {
  command = plan

  variables {
    canary_worker_role_arns = [
      "arn:aws:iam::123456789012:role/${join("", [for _ in range(511) : "p"])}/worker",
    ]
    ec2_runtime_role_arns = [
      "arn:aws:iam::123456789012:role/${join("", [for _ in range(511) : "p"])}/runtime",
    ]
    ec2_instance_profile_arns = [
      "arn:aws:iam::123456789012:instance-profile/${join("", [for _ in range(511) : "p"])}/runtime",
    ]
    eks_node_instance_profile_arns = [
      "arn:aws:iam::123456789012:instance-profile/${join("", [for _ in range(511) : "p"])}/eks-node",
    ]
    permissions_boundary_arn = "arn:aws:iam::123456789012:policy/${join("", [for _ in range(511) : "p"])}/boundary"
  }

  expect_failures = [
    var.canary_worker_role_arns,
    var.ec2_runtime_role_arns,
    var.ec2_instance_profile_arns,
    var.eks_node_instance_profile_arns,
    var.permissions_boundary_arn,
  ]
}

run "rejects_malformed_iam_resource_paths" {
  command = plan

  variables {
    canary_worker_role_arns        = ["arn:aws:iam::123456789012:role/team/"]
    ec2_runtime_role_arns          = ["arn:aws:iam::123456789012:role/runtime name"]
    ec2_instance_profile_arns      = ["arn:aws:iam::123456789012:instance-profile/runtime?"]
    eks_node_instance_profile_arns = ["arn:aws:iam::123456789012:instance-profile/$${aws:username}"]
    permissions_boundary_arn       = "arn:aws:iam::123456789012:policy/team//boundary"
  }

  expect_failures = [
    var.canary_worker_role_arns,
    var.ec2_runtime_role_arns,
    var.ec2_instance_profile_arns,
    var.eks_node_instance_profile_arns,
    var.permissions_boundary_arn,
  ]
}

run "rejects_ec2_resource_id_wrong_lengths" {
  command = plan

  variables {
    ami_arns            = ["arn:aws:ec2:us-east-1::image/ami-0123456789abcdef"]
    subnet_arns         = ["arn:aws:ec2:us-east-1:123456789012:subnet/subnet-0123456789abcdef01"]
    security_group_arns = ["arn:aws:ec2:us-east-1:123456789012:security-group/sg-0123456"]
  }

  expect_failures = [
    var.ami_arns,
    var.subnet_arns,
    var.security_group_arns,
  ]
}

run "rejects_ec2_resource_id_non_lowercase_hex" {
  command = plan

  variables {
    ami_arns            = ["arn:aws:ec2:us-east-1::image/ami-0123456789abcdeFG"]
    subnet_arns         = ["arn:aws:ec2:us-east-1:123456789012:subnet/subnet-0123456789abcdeFG"]
    security_group_arns = ["arn:aws:ec2:us-east-1:123456789012:security-group/sg-0123456789abcdeFG"]
  }

  expect_failures = [
    var.ami_arns,
    var.subnet_arns,
    var.security_group_arns,
  ]
}

run "rejects_overlong_eks_cluster_names" {
  command = plan

  variables {
    eks_cluster_arns = [
      "arn:aws:eks:us-east-1:123456789012:cluster/c${join("", [for _ in range(100) : "x"])}",
    ]
  }

  expect_failures = [var.eks_cluster_arns]
}

run "rejects_malformed_eks_cluster_names" {
  command = plan

  variables {
    eks_cluster_arns = [
      "arn:aws:eks:us-east-1:123456789012:cluster/invalid.cluster",
    ]
  }

  expect_failures = [var.eks_cluster_arns]
}

run "rejects_malformed_kms_uuid_key_ids" {
  command = plan

  variables {
    spot_customer_managed_kms_key_arns = [
      "arn:aws:kms:us-east-1:123456789012:key/not-a-real-key",
    ]
  }

  expect_failures = [var.spot_customer_managed_kms_key_arns]
}

run "rejects_malformed_kms_mrk_key_ids" {
  command = plan

  variables {
    spot_customer_managed_kms_key_arns = [
      "arn:aws:kms:us-east-1:123456789012:key/mrk-0000000000004000800000000000001",
    ]
  }

  expect_failures = [var.spot_customer_managed_kms_key_arns]
}

run "rejects_malformed_arn_partition_and_region_components" {
  command = plan

  variables {
    canary_worker_role_arns = [
      "arn:aws--cn:iam::123456789012:role/skypilot-image-canary-worker",
    ]
    ami_arns = [
      "arn:aws:ec2:us--east-1::image/ami-00000000000000001",
    ]
  }

  expect_failures = [
    var.canary_worker_role_arns,
    var.ami_arns,
  ]
}
