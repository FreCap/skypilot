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

  override_resource {
    target          = aws_iam_role.vm
    override_during = plan
    values = {
      arn = "arn:aws:iam::444455556666:role/skypilot-v1"
      id  = "skypilot-v1"
    }
  }

  override_resource {
    target          = aws_iam_role.provisioner[0]
    override_during = plan
    values = {
      arn = "arn:aws:iam::444455556666:role/skypilot-provisioner"
      id  = "skypilot-provisioner"
    }
  }

  override_resource {
    target          = aws_iam_instance_profile.vm
    override_during = plan
    values = {
      arn = "arn:aws:iam::444455556666:instance-profile/skypilot-v1"
      id  = "skypilot-v1"
    }
  }
}

variables {
  controller_role_arn      = "arn:aws:iam::111122223333:role/skypilot-api"
  permissions_boundary_arn = "arn:aws:iam::444455556666:policy/platform/skypilot-boundary"
  enable_serve_controller  = true
  vm_role_extra_policy_arns = [
    "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy",
  ]
  vm_dataset_grants = [{
    bucket_arn  = "arn:aws:s3:::example-training-data"
    kms_key_arn = "arn:aws:kms:us-east-1:444455556666:key/00000000-0000-4000-8000-000000000001"
  }]
}

run "commercial_partition_preserves_the_vm_pool_contract" {
  command = plan

  assert {
    condition = (
      length(aws_iam_role.provisioner) == 1 &&
      length(aws_iam_role_policy.provisioner) == 1 &&
      length(data.aws_iam_policy_document.provisioner) == 1
    )
    error_message = "The required controller ARN must retain the counted provisioner resource addresses."
  }

  assert {
    condition = (
      aws_iam_role.vm.permissions_boundary == var.permissions_boundary_arn &&
      aws_iam_role.provisioner[0].permissions_boundary == var.permissions_boundary_arn
    )
    error_message = "The permissions boundary must be attached to both roles."
  }

  assert {
    condition = toset(one([
      for statement in data.aws_iam_policy_document.vm.statement : statement
      if statement.sid == "Ec2"
      ]).actions) == toset([
      "ec2:RunInstances",
      "ec2:TerminateInstances",
      "ec2:StartInstances",
      "ec2:StopInstances",
      "ec2:ModifyInstanceAttribute",
      "ec2:CreateTags",
      "ec2:DeleteTags",
      "ec2:Describe*",
      "ec2:CreateSecurityGroup",
      "ec2:AuthorizeSecurityGroupIngress",
      "ec2:RevokeSecurityGroupIngress",
      "ec2:DeleteSecurityGroup",
      "ec2:CreateKeyPair",
      "ec2:DeleteKeyPair",
      "ec2:ImportKeyPair",
    ])
    error_message = "The established SkyPilot EC2 lifecycle permission set must remain exact."
  }

  assert {
    condition = toset(one([
      for statement in data.aws_iam_policy_document.vm.statement : statement
      if statement.sid == "DatasetBuckets"
      ]).resources) == toset([
      "arn:aws:s3:::example-training-data",
    ])
    error_message = "The exact dataset bucket grant must reach the VM policy."
  }

  assert {
    condition = toset(one([
      for statement in data.aws_iam_policy_document.provisioner[0].statement : statement
      if statement.sid == "SkyPilotStorageBuckets"
      ]).resources) == toset([
      "arn:aws:s3:::skypilot-*",
      "arn:aws:s3:::skypilot-*/*",
    ])
    error_message = "SkyPilot storage ARNs must use the active commercial partition."
  }

  assert {
    condition = (
      aws_iam_role_policy_attachment.vm_ssm[0].policy_arn ==
      "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
    )
    error_message = "The SSM managed policy must use the active commercial partition."
  }

  assert {
    condition = (
      output.vm_role_arn == "arn:aws:iam::444455556666:role/skypilot-v1" &&
      output.instance_profile_arn == "arn:aws:iam::444455556666:instance-profile/skypilot-v1" &&
      output.provisioner_role_arn == "arn:aws:iam::444455556666:role/skypilot-provisioner"
    )
    error_message = "The module must expose the identities needed by its callers."
  }
}
