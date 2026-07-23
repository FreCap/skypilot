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
      arn = "arn:aws:iam::123456789012:role/image-canary"
      id  = "image-canary"
    }
  }
}

variables {
  role_name               = "image-canary"
  canary_worker_role_arns = ["arn:aws:iam::123456789012:role/image-canary-worker"]
  catalog_authority       = "00000000-0000-4000-8000-000000000001"
  runtime_role_arns       = ["arn:aws:iam::123456789012:role/runtime"]
  instance_profile_arns   = ["arn:aws:iam::123456789012:instance-profile/runtime"]
  ami_arns                = ["arn:aws:ec2:us-east-1:123456789012:image/ami-0123456789abcdef0"]
  subnet_arns             = ["arn:aws:ec2:us-east-1:123456789012:subnet/subnet-0123456789abcdef0"]
  security_group_arns     = ["arn:aws:ec2:us-east-1:123456789012:security-group/sg-0123456789abcdef0"]
  canary_instance_types   = ["t3.micro"]
}

run "ec2_launch_authority_matches_each_resource_context" {
  command = plan

  assert {
    condition = toset(one([
      for statement in data.aws_iam_policy_document.permissions.statement : statement
      if statement.sid == "LaunchOnlyThroughQualifiedNetworkAndImage"
      ]).resources) == toset(concat(
      tolist(var.ami_arns),
      tolist(var.subnet_arns),
      tolist(var.security_group_arns),
    ))
    error_message = "The launch must require the exact AMI, subnet, and security group."
  }

  assert {
    condition = length(one([
      for statement in data.aws_iam_policy_document.permissions.statement : statement
      if statement.sid == "LaunchOnlyThroughQualifiedNetworkAndImage"
    ]).condition) == 0
    error_message = "Exact existing resources must not require instance-only context keys."
  }

  assert {
    condition = toset(one([
      for statement in data.aws_iam_policy_document.permissions.statement : statement
      if statement.sid == "CreateOnlyCatalogTaggedCanaryInstances"
      ]).resources) == toset([
      "arn:aws:ec2:us-east-1:123456789012:instance/*",
    ])
    error_message = "The instance authorization must cover only created instances."
  }

  assert {
    condition = length([
      for condition in one([
        for statement in data.aws_iam_policy_document.permissions.statement : statement
        if statement.sid == "CreateOnlyCatalogTaggedCanaryInstances"
      ]).condition : condition
      if condition.test == "StringEquals" &&
      condition.variable == "ec2:InstanceType" &&
      toset(condition.values) == toset(var.canary_instance_types)
    ]) == 1
    error_message = "The instance authorization must require an exact canary instance type."
  }

  assert {
    condition = length([
      for condition in one([
        for statement in data.aws_iam_policy_document.permissions.statement : statement
        if statement.sid == "CreateOnlyCatalogTaggedCanaryInstances"
      ]).condition : condition
      if condition.test == "ArnEquals" &&
      condition.variable == "ec2:InstanceProfile" &&
      toset(condition.values) == toset(var.instance_profile_arns)
    ]) == 1
    error_message = "The instance authorization must require an exact runtime instance profile."
  }

  assert {
    condition = length([
      for condition in one([
        for statement in data.aws_iam_policy_document.permissions.statement : statement
        if statement.sid == "CreateOnlyCatalogTaggedCanaryInstances"
      ]).condition : condition
      if startswith(condition.variable, "aws:RequestTag/")
    ]) == 2
    error_message = "The instance authorization must require catalog and operation request tags."
  }

  assert {
    condition = toset(one([
      for statement in data.aws_iam_policy_document.permissions.statement : statement
      if statement.sid == "CreateOnlyCatalogTaggedCanaryVolumes"
      ]).resources) == toset([
      "arn:aws:ec2:us-east-1:123456789012:volume/*",
    ])
    error_message = "The volume authorization must cover only created EBS volumes."
  }

  assert {
    condition = length([
      for condition in one([
        for statement in data.aws_iam_policy_document.permissions.statement : statement
        if statement.sid == "CreateOnlyCatalogTaggedCanaryVolumes"
      ]).condition : condition
      if startswith(condition.variable, "aws:RequestTag/")
    ]) == 2
    error_message = "The volume authorization must require catalog and operation request tags."
  }

  assert {
    condition = alltrue([
      for condition in one([
        for statement in data.aws_iam_policy_document.permissions.statement : statement
        if statement.sid == "CreateOnlyCatalogTaggedCanaryVolumes"
      ]).condition : condition.variable != "ec2:InstanceType"
    ])
    error_message = "The volume statement must not require a condition key absent from its RunInstances resource context."
  }

  assert {
    condition = toset(one([
      for statement in data.aws_iam_policy_document.permissions.statement : statement
      if statement.sid == "CreateOnlyQualifiedSubnetCanaryNetworkInterfaces"
      ]).resources) == toset([
      "arn:aws:ec2:us-east-1:123456789012:network-interface/*",
    ])
    error_message = "The network-interface authorization must cover only created interfaces."
  }

  assert {
    condition = length([
      for condition in one([
        for statement in data.aws_iam_policy_document.permissions.statement : statement
        if statement.sid == "CreateOnlyQualifiedSubnetCanaryNetworkInterfaces"
      ]).condition : condition
      if condition.test == "ArnEquals" &&
      condition.variable == "ec2:Subnet" &&
      toset(condition.values) == toset(var.subnet_arns)
      ]) == 1 && length(one([
        for statement in data.aws_iam_policy_document.permissions.statement : statement
        if statement.sid == "CreateOnlyQualifiedSubnetCanaryNetworkInterfaces"
    ]).condition) == 1
    error_message = "The implicit network interface must be restricted by its exact available subnet context."
  }

  assert {
    condition = toset(one([
      for statement in data.aws_iam_policy_document.permissions.statement : statement
      if statement.sid == "TagOnlyDuringQualifiedCanaryLaunch"
      ]).resources) == toset([
      "arn:aws:ec2:us-east-1:123456789012:instance/*",
      "arn:aws:ec2:us-east-1:123456789012:network-interface/*",
      "arn:aws:ec2:us-east-1:123456789012:volume/*",
    ])
    error_message = "Tag-on-create authority must cover exactly the three resources emitted by the canary worker."
  }
}
