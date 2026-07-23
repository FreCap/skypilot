output "copy_role_arn" {
  description = "Base role ARN for the copy-worker Kubernetes service account."
  value       = aws_iam_role.copy.arn
}

output "lifecycle_role_arn" {
  description = "Base role ARN for the lifecycle-worker Kubernetes service account."
  value       = aws_iam_role.lifecycle.arn
}

output "canary_role_arn" {
  description = "Base role ARN for the canary-worker Kubernetes service account."
  value       = aws_iam_role.canary.arn
}

output "helm_service_account_annotations" {
  description = "Values to pass to the three Helm worker service accounts."
  value = {
    imageCopyWorker = {
      "eks.amazonaws.com/role-arn" = aws_iam_role.copy.arn
    }
    imageLifecycleWorker = {
      "eks.amazonaws.com/role-arn" = aws_iam_role.lifecycle.arn
    }
    imageCanaryWorker = {
      "eks.amazonaws.com/role-arn" = aws_iam_role.canary.arn
    }
  }
}
