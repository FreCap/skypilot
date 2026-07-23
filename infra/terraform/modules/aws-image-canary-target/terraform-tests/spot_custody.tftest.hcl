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

  override_resource {
    target          = aws_kms_grant.spot_encrypted_ami["arn:aws:kms:us-east-1:123456789012:key/00000000-0000-4000-8000-000000000001"]
    override_during = plan
    values = {
      grant_id = "terraform-test-grant"
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
  eks_cluster_arns               = ["arn:aws:eks:us-east-1:123456789012:cluster/skypilot-runtime"]
  spot_service_linked_role_arn   = "arn:aws:iam::123456789012:role/aws-service-role/spot.amazonaws.com/AWSServiceRoleForEC2Spot"
  spot_customer_managed_kms_key_arns = [
    "arn:aws:kms:us-east-1:123456789012:key/00000000-0000-4000-8000-000000000001",
  ]
}

run "ec2_target_rejects_a_missing_account_bootstrap" {
  command = plan

  variables {
    spot_service_linked_role_arn = null
  }

  expect_failures = [terraform_data.validate_contract]
}

run "spot_requests_are_tagged_and_reclaimable" {
  command = plan

  assert {
    condition = contains(one([
      for statement in data.aws_iam_policy_document.permissions.statement : statement
      if statement.sid == "CreateOnlyCatalogTaggedCanarySupportResources"
    ]).resources, "arn:aws:ec2:us-east-1:123456789012:spot-instances-request/*")
    error_message = "RunInstances must authorize operation-tagged Spot request creation."
  }

  assert {
    condition = contains(one([
      for statement in data.aws_iam_policy_document.permissions.statement : statement
      if statement.sid == "TagOnlyDuringQualifiedCanaryLaunch"
    ]).resources, "arn:aws:ec2:us-east-1:123456789012:spot-instances-request/*")
    error_message = "CreateTags must authorize the Spot request resource during RunInstances."
  }

  assert {
    condition = contains(one([
      for statement in data.aws_iam_policy_document.permissions.statement : statement
      if statement.sid == "InspectComputeState"
    ]).actions, "ec2:DescribeSpotInstanceRequests")
    error_message = "The canary role must discover every operation-tagged Spot request."
  }

  assert {
    condition = toset(one([
      for statement in data.aws_iam_policy_document.permissions.statement : statement
      if statement.sid == "CancelOnlyCatalogSpotRequests"
      ]).actions) == toset(["ec2:CancelSpotInstanceRequests"]) && toset(one([
      for statement in data.aws_iam_policy_document.permissions.statement : statement
      if statement.sid == "CancelOnlyCatalogSpotRequests"
      ]).resources) == toset([
      "arn:aws:ec2:us-east-1:123456789012:spot-instances-request/*",
    ])
    error_message = "Spot cancellation must be isolated to the regional request resource."
  }

  assert {
    condition = one(one([
      for statement in data.aws_iam_policy_document.permissions.statement : statement
      if statement.sid == "CancelOnlyCatalogSpotRequests"
      ]).condition).test == "StringEquals" && toset(one(one([
        for statement in data.aws_iam_policy_document.permissions.statement : statement
        if statement.sid == "CancelOnlyCatalogSpotRequests"
      ]).condition).values) == toset(["00000000-0000-4000-8000-000000000001"]) && one(one([
      for statement in data.aws_iam_policy_document.permissions.statement : statement
      if statement.sid == "CancelOnlyCatalogSpotRequests"
    ]).condition).variable == "ec2:ResourceTag/SkyPilotCatalog"
    error_message = "Spot cancellation must require the exact catalog ownership tag."
  }
}

run "customer_managed_ami_keys_are_granted_to_the_spot_service_role" {
  command = plan

  assert {
    condition     = aws_kms_grant.spot_encrypted_ami["arn:aws:kms:us-east-1:123456789012:key/00000000-0000-4000-8000-000000000001"].grantee_principal == "arn:aws:iam::123456789012:role/aws-service-role/spot.amazonaws.com/AWSServiceRoleForEC2Spot"
    error_message = "The regional KMS grant must target the account's EC2 Spot service-linked role."
  }

  assert {
    condition = toset(aws_kms_grant.spot_encrypted_ami["arn:aws:kms:us-east-1:123456789012:key/00000000-0000-4000-8000-000000000001"].operations) == toset([
      "CreateGrant",
      "Decrypt",
      "DescribeKey",
      "Encrypt",
      "GenerateDataKey",
      "GenerateDataKeyWithoutPlaintext",
      "ReEncryptFrom",
      "ReEncryptTo",
    ])
    error_message = "The KMS grant must contain the exact AWS-documented Spot encrypted-AMI operations."
  }
}

run "mixed_ec2_eks_target_never_passes_node_identity" {
  command = plan

  assert {
    condition = toset(one([
      for statement in data.aws_iam_policy_document.permissions.statement : statement
      if statement.sid == "PassOnlyQualifiedRuntimeRoles"
      ]).resources) == toset([
      "arn:aws:iam::123456789012:role/skypilot-runtime",
    ])
    error_message = "PassRole must include only EC2 runtime roles, never EKS node roles."
  }

  assert {
    condition = (
      toset(one([
        for statement in data.aws_iam_policy_document.permissions.statement : statement
        if statement.sid == "CreateOnlyCatalogTaggedCanaryInstances"
      ]).resources) == toset(["arn:aws:ec2:us-east-1:123456789012:instance/*"]) &&
      toset(flatten([
        for condition in one([
          for statement in data.aws_iam_policy_document.permissions.statement : statement
          if statement.sid == "CreateOnlyCatalogTaggedCanaryInstances"
        ]).condition : condition.values
        if condition.variable == "ec2:InstanceProfile"
        ])) == toset([
        "arn:aws:iam::123456789012:instance-profile/skypilot-runtime",
      ])
    )
    error_message = "The mandatory instance-resource authorization must constrain the exact EC2 instance profile."
  }

  assert {
    condition = alltrue([
      for statement in data.aws_iam_policy_document.permissions.statement :
      !contains(flatten([
        for condition in statement.condition : condition.values
      ]), "arn:aws:iam::123456789012:instance-profile/skypilot-eks-node")
      if contains(statement.actions, "ec2:RunInstances")
    ])
    error_message = "No RunInstances authorization may reference an EKS node profile."
  }

  assert {
    condition = alltrue([
      for statement in data.aws_iam_policy_document.permissions.statement :
      length([
        for condition in statement.condition : condition
        if contains(["ec2:InstanceProfile", "ec2:InstanceType"], condition.variable)
      ]) == 0
      if contains(statement.actions, "ec2:RunInstances") && statement.sid != "CreateOnlyCatalogTaggedCanaryInstances"
    ])
    error_message = "Instance-only condition keys must not be attached to other RunInstances resource types."
  }

  assert {
    condition = toset(one([
      for statement in data.aws_iam_policy_document.permissions.statement : statement
      if statement.sid == "InspectOnlyQualifiedInstanceProfiles"
      ]).resources) == toset([
      "arn:aws:iam::123456789012:instance-profile/skypilot-runtime",
      "arn:aws:iam::123456789012:instance-profile/skypilot-eks-node",
    ])
    error_message = "EC2 and EKS profiles may both be inspected without making the EKS profile launchable."
  }
}

run "eks_only_target_has_no_ec2_launch_or_pass_role_authority" {
  command = plan

  variables {
    ec2_runtime_role_arns              = []
    ec2_instance_profile_arns          = []
    ami_arns                           = []
    subnet_arns                        = []
    security_group_arns                = []
    canary_instance_types              = []
    spot_service_linked_role_arn       = null
    spot_customer_managed_kms_key_arns = []
  }

  assert {
    condition = length([
      for statement in data.aws_iam_policy_document.permissions.statement : statement
      if contains(statement.actions, "ec2:RunInstances") || contains(statement.actions, "iam:PassRole")
    ]) == 0
    error_message = "An EKS-only target must not carry EC2 launch or PassRole authority."
  }
}
