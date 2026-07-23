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

  mock_data "aws_region" {
    defaults = {
      description = "China (Beijing)"
      endpoint    = "ec2.cn-north-1.amazonaws.com.cn"
      id          = "cn-north-1"
      name        = "cn-north-1"
      region      = "cn-north-1"
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
      arn = "arn:aws-cn:iam::123456789012:role/skypilot-image-canary"
      id  = "skypilot-image-canary"
    }
  }
}

variables {
  role_name                    = "skypilot-image-canary"
  canary_worker_role_arns      = ["arn:aws-cn:iam::123456789012:role/skypilot-image-canary-worker"]
  catalog_authority            = "00000000-0000-4000-8000-000000000001"
  ec2_runtime_role_arns        = ["arn:aws-cn:iam::123456789012:role/skypilot-runtime"]
  ec2_instance_profile_arns    = ["arn:aws-cn:iam::123456789012:instance-profile/skypilot-runtime"]
  ami_arns                     = ["arn:aws-cn:ec2:cn-north-1::image/ami-00000000000000001"]
  subnet_arns                  = ["arn:aws-cn:ec2:cn-north-1:123456789012:subnet/subnet-00000000000000001"]
  security_group_arns          = ["arn:aws-cn:ec2:cn-north-1:123456789012:security-group/sg-00000000000000001"]
  canary_instance_types        = ["g5.xlarge"]
  spot_service_linked_role_arn = "arn:aws-cn:iam::123456789012:role/aws-service-role/spot.amazonaws.com/AWSServiceRoleForEC2Spot"
}

run "china_target_uses_partition_ec2_service_principal" {
  command = plan

  assert {
    condition = toset(flatten([
      for condition in one([
        for statement in data.aws_iam_policy_document.permissions.statement : statement
        if statement.sid == "PassOnlyQualifiedRuntimeRoles"
      ]).condition : condition.values
      if condition.variable == "iam:PassedToService"
    ])) == toset(["ec2.amazonaws.com.cn"])
    error_message = "PassRole must use the EC2 service principal for the active AWS partition."
  }
}
