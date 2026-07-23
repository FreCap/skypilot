output "role_arn" {
  description = "Use as the aws_assume_role canary_launch authority."
  value       = aws_iam_role.canary.arn
}

output "binding" {
  description = "Secret-free access-binding fragment for SkyPilot config."
  value = merge({
    kind      = "aws_assume_role"
    authority = aws_iam_role.canary.arn
    purposes  = ["canary_launch"]
    }, var.external_id == null ? {} : {
    external_id = var.external_id
  })
}

output "spot_service_linked_role_arn" {
  description = "Canonical EC2 Spot service-linked role used by this target."
  value       = local.spot_service_linked_role_arn
}

output "spot_kms_grant_ids" {
  description = "KMS grants created for customer-managed encrypted Spot AMIs."
  value       = { for arn, grant in aws_kms_grant.spot_encrypted_ami : arn => grant.grant_id }
}
