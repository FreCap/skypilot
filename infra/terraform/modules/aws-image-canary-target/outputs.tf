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
