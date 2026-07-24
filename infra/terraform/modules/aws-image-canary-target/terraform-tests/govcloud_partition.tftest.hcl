mock_provider "aws" {
  override_during = plan

  mock_data "aws_caller_identity" {
    defaults = {
      account_id = "123456789012"
      arn        = "arn:aws-us-gov:iam::123456789012:role/terraform-test"
      id         = "123456789012"
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

  mock_data "aws_region" {
    defaults = {
      description = "AWS GovCloud (US-West)"
      endpoint    = "ec2.us-gov-west-1.amazonaws.com"
      id          = "us-gov-west-1"
      name        = "us-gov-west-1"
      region      = "us-gov-west-1"
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
      arn = "arn:aws-us-gov:iam::123456789012:role/skypilot-image-canary"
      id  = "skypilot-image-canary"
    }
  }
}

variables {
  role_name                      = "skypilot-image-canary"
  canary_worker_role_arns        = ["arn:aws-us-gov:iam::123456789012:role/workers/skypilot-image-canary-worker"]
  catalog_authority              = "00000000-0000-4000-8000-000000000001"
  ec2_runtime_role_arns          = ["arn:aws-us-gov:iam::123456789012:role/runtime/skypilot-runtime"]
  ec2_instance_profile_arns      = ["arn:aws-us-gov:iam::123456789012:instance-profile/runtime/skypilot-runtime"]
  eks_node_instance_profile_arns = ["arn:aws-us-gov:iam::123456789012:instance-profile/eks/skypilot-node"]
  ami_arns                       = ["arn:aws-us-gov:ec2:us-gov-west-1::image/ami-00000000000000001"]
  subnet_arns                    = ["arn:aws-us-gov:ec2:us-gov-west-1:123456789012:subnet/subnet-00000000000000001"]
  security_group_arns            = ["arn:aws-us-gov:ec2:us-gov-west-1:123456789012:security-group/sg-00000000000000001"]
  canary_instance_types          = ["g5.xlarge"]
  spot_service_linked_role_arn   = "arn:aws-us-gov:iam::123456789012:role/aws-service-role/spot.amazonaws.com/AWSServiceRoleForEC2Spot"
  spot_customer_managed_kms_key_arns = [
    "arn:aws-us-gov:kms:us-gov-west-1:123456789012:key/mrk-00000000000040008000000000000001",
  ]
  eks_cluster_arns         = ["arn:aws-us-gov:eks:us-gov-west-1:123456789012:cluster/skypilot-runtime"]
  permissions_boundary_arn = "arn:aws-us-gov:iam::123456789012:policy/boundaries/skypilot-canary"
}

run "govcloud_target_accepts_exact_partition_arns" {
  command = plan

  assert {
    condition = toset(flatten([
      for condition in one([
        for statement in data.aws_iam_policy_document.permissions.statement : statement
        if statement.sid == "PassOnlyQualifiedRuntimeRoles"
      ]).condition : condition.values
      if condition.variable == "iam:PassedToService"
    ])) == toset(["ec2.amazonaws.com"])
    error_message = "PassRole must use the EC2 service principal for the GovCloud partition."
  }

  assert {
    condition     = aws_kms_grant.spot_encrypted_ami["arn:aws-us-gov:kms:us-gov-west-1:123456789012:key/mrk-00000000000040008000000000000001"].key_id == "arn:aws-us-gov:kms:us-gov-west-1:123456789012:key/mrk-00000000000040008000000000000001"
    error_message = "GovCloud must preserve the exact multi-Region KMS key ARN."
  }
}
