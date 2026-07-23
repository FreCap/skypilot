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
  role_name                    = "skypilot-image-canary"
  canary_worker_role_arns      = ["arn:aws:iam::123456789012:role/skypilot-image-canary-worker"]
  catalog_authority            = "00000000-0000-4000-8000-000000000001"
  runtime_role_arns            = ["arn:aws:iam::123456789012:role/skypilot-runtime"]
  instance_profile_arns        = ["arn:aws:iam::123456789012:instance-profile/skypilot-runtime"]
  ami_arns                     = ["arn:aws:ec2:us-east-1::image/ami-00000000000000001"]
  subnet_arns                  = ["arn:aws:ec2:us-east-1:123456789012:subnet/subnet-00000000000000001"]
  security_group_arns          = ["arn:aws:ec2:us-east-1:123456789012:security-group/sg-00000000000000001"]
  canary_instance_types        = ["g5.xlarge"]
  spot_service_linked_role_arn = "arn:aws:iam::123456789012:role/aws-service-role/spot.amazonaws.com/AWSServiceRoleForEC2Spot"
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
      if statement.sid == "CreateOnlyCatalogTaggedCanaryResources"
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
