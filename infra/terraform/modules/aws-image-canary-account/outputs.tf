output "spot_service_linked_role_arn" {
  description = "Pass to each regional aws-image-canary-target module."
  value       = aws_iam_service_linked_role.ec2_spot.arn
}
