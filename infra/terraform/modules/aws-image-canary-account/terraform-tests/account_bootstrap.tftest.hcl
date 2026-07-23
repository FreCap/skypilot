mock_provider "aws" {
  override_during = plan

  override_resource {
    target          = aws_iam_service_linked_role.ec2_spot
    override_during = plan
    values = {
      arn = "arn:aws:iam::123456789012:role/aws-service-role/spot.amazonaws.com/AWSServiceRoleForEC2Spot"
    }
  }
}

run "clean_account_creates_the_spot_service_role" {
  command = plan

  assert {
    condition     = aws_iam_service_linked_role.ec2_spot.aws_service_name == "spot.amazonaws.com"
    error_message = "The account bootstrap must create the EC2 Spot service-linked role."
  }

  assert {
    condition     = output.spot_service_linked_role_arn == aws_iam_service_linked_role.ec2_spot.arn
    error_message = "The output must expose the exact managed Spot service-linked role."
  }
}
