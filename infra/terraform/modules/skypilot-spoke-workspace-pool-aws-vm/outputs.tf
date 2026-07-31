output "provisioner_role_arn" {
  description = <<-EOT
    ARN of the role the control plane assumes to launch EC2 here. Use it in the
    API server's ~/.aws/config profile (role_arn=...) and grant the control-plane
    Pod Identity role sts:AssumeRole on it.
  EOT
  value       = local.create ? aws_iam_role.provisioner[0].arn : null
}

output "provisioner_role_name" {
  description = "Name of the role the SkyPilot control plane assumes to manage EC2 resources."
  value       = local.create ? aws_iam_role.provisioner[0].name : null
}

output "instance_profile_name" {
  description = "Name of the instance profile attached to launched VMs (skypilot-v1)."
  value       = aws_iam_instance_profile.vm.name
}

output "instance_profile_arn" {
  description = "ARN of the instance profile attached to launched VMs."
  value       = aws_iam_instance_profile.vm.arn
}

output "vm_role_name" {
  description = "Name of the IAM role attached to launched VMs through the instance profile."
  value       = aws_iam_role.vm.name
}

output "vm_role_arn" {
  description = "ARN of the IAM role attached to launched VMs through the instance profile."
  value       = aws_iam_role.vm.arn
}
