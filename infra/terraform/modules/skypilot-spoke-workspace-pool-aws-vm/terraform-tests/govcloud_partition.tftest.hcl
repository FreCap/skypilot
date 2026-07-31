mock_provider "aws" {
  override_during = plan

  mock_data "aws_caller_identity" {
    defaults = {
      account_id = "444455556666"
      arn        = "arn:aws-us-gov:iam::444455556666:role/terraform-test"
      id         = "444455556666"
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
}

variables {
  controller_role_arn      = "arn:aws-us-gov:iam::111122223333:role/skypilot-api"
  permissions_boundary_arn = "arn:aws-us-gov:iam::444455556666:policy/skypilot-boundary"
  enable_serve_controller  = true
  vm_dataset_grants = [{
    bucket_arn  = "arn:aws-us-gov:s3:::example-training-data"
    kms_key_arn = "arn:aws-us-gov:kms:us-gov-west-1:444455556666:key/mrk-00000000000040008000000000000001"
  }]
}

run "govcloud_arns_follow_the_active_partition" {
  command = plan

  assert {
    condition = one(one([
      for statement in data.aws_iam_policy_document.vm_assume.statement : statement
      if length(statement.principals) == 1
    ]).principals).identifiers == toset(["ec2.amazonaws.com"])
    error_message = "The EC2 service principal must use the GovCloud DNS suffix."
  }

  assert {
    condition = toset(one([
      for statement in data.aws_iam_policy_document.provisioner[0].statement : statement
      if statement.sid == "SkyPilotStorageBuckets"
      ]).resources) == toset([
      "arn:aws-us-gov:s3:::skypilot-*",
      "arn:aws-us-gov:s3:::skypilot-*/*",
    ])
    error_message = "SkyPilot storage resources must use the GovCloud partition."
  }

  assert {
    condition = (
      aws_iam_role_policy_attachment.vm_ssm[0].policy_arn ==
      "arn:aws-us-gov:iam::aws:policy/AmazonSSMManagedInstanceCore"
    )
    error_message = "The managed SSM policy must use the GovCloud partition."
  }

  assert {
    condition = contains(one([
      for statement in data.aws_iam_policy_document.vm_serve_replica[0].statement : statement
      if statement.sid == "SsmStartSession"
    ]).resources, "arn:aws-us-gov:ssm:*::document/AWS-StartSSHSession")
    error_message = "The SkyServe SSM document must use the GovCloud partition."
  }

  assert {
    condition = (
      aws_iam_role.vm.permissions_boundary == var.permissions_boundary_arn &&
      aws_iam_role.provisioner[0].permissions_boundary == var.permissions_boundary_arn
    )
    error_message = "The GovCloud boundary must be attached unchanged to both roles."
  }
}
