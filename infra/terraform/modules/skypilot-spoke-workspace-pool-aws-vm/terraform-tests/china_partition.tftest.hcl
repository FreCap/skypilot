mock_provider "aws" {
  override_during = plan

  mock_data "aws_caller_identity" {
    defaults = {
      account_id = "444455556666"
      arn        = "arn:aws-cn:iam::444455556666:role/terraform-test"
      id         = "444455556666"
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
}

variables {
  controller_role_arn      = "arn:aws-cn:iam::111122223333:role/skypilot-api"
  permissions_boundary_arn = "arn:aws-cn:iam::444455556666:policy/skypilot-boundary"
  enable_serve_controller  = true
  vm_dataset_grants = [{
    bucket_arn  = "arn:aws-cn:s3:::example-training-data"
    kms_key_arn = "arn:aws-cn:kms:cn-north-1:444455556666:key/00000000-0000-4000-8000-000000000001"
  }]
}

run "china_arns_and_service_principals_follow_the_active_partition" {
  command = plan

  assert {
    condition = one(one([
      for statement in data.aws_iam_policy_document.vm_assume.statement : statement
      if length(statement.principals) == 1
    ]).principals).identifiers == toset(["ec2.amazonaws.com.cn"])
    error_message = "The EC2 service principal must use the AWS China DNS suffix."
  }

  assert {
    condition = toset(one([
      for statement in data.aws_iam_policy_document.provisioner[0].statement : statement
      if statement.sid == "SkyPilotStorageBuckets"
      ]).resources) == toset([
      "arn:aws-cn:s3:::skypilot-*",
      "arn:aws-cn:s3:::skypilot-*/*",
    ])
    error_message = "SkyPilot storage resources must use the AWS China partition."
  }

  assert {
    condition = (
      aws_iam_role_policy_attachment.vm_ssm[0].policy_arn ==
      "arn:aws-cn:iam::aws:policy/AmazonSSMManagedInstanceCore"
    )
    error_message = "The managed SSM policy must use the AWS China partition."
  }

  assert {
    condition = (
      toset(one([
        for statement in data.aws_iam_policy_document.provisioner[0].statement : statement
        if statement.sid == "SpotServiceLinkedRole"
        ]).resources) == toset([
        "arn:aws-cn:iam::*:role/aws-service-role/spot.amazonaws.com/*",
      ]) &&
      toset(one(one([
        for statement in data.aws_iam_policy_document.provisioner[0].statement : statement
        if statement.sid == "SpotServiceLinkedRole"
        ]).condition).values) == toset([
        "spot.amazonaws.com",
      ])
    )
    error_message = "The EC2 Spot service-linked role must use the AWS China ARN partition and global service name."
  }

  assert {
    condition = (
      aws_iam_role.vm.permissions_boundary == var.permissions_boundary_arn &&
      aws_iam_role.provisioner[0].permissions_boundary == var.permissions_boundary_arn
    )
    error_message = "The AWS China boundary must be attached unchanged to both roles."
  }
}
