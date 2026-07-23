resource "aws_iam_service_linked_role" "ec2_spot" {
  aws_service_name = "spot.amazonaws.com"
  description      = "Allows EC2 Spot to launch managed SkyPilot image canaries."
}
