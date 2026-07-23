output "qualification_config_map_data" {
  description = "Use directly as the data field of the qualification ConfigMap."
  value = {
    for workspace, manifest in local.qualification_manifests :
    "${substr(sha256(lower(trimspace(workspace))), 0, 16)}-${var.profile}.json" => jsonencode(manifest)
  }
}

output "helm_image_worker_values" {
  description = "Non-secret Helm values for the separately permissioned workers."
  value = {
    imageCopyWorker = {
      enabled      = true
      replicaCount = 1
      maxInFlight  = 4
      serviceAccount = {
        create      = true
        name        = "skypilot-image-copy-worker"
        annotations = module.worker_identity.helm_service_account_annotations.imageCopyWorker
      }
    }
    imageLifecycleWorker = {
      enabled      = true
      replicaCount = 1
      maxInFlight  = 4
      serviceAccount = {
        create      = true
        name        = "skypilot-image-lifecycle-worker"
        annotations = module.worker_identity.helm_service_account_annotations.imageLifecycleWorker
      }
    }
    imageCanaryWorker = {
      enabled      = true
      replicaCount = 1
      maxInFlight  = 2
      serviceAccount = {
        create      = true
        name        = "skypilot-image-canary-worker"
        annotations = module.worker_identity.helm_service_account_annotations.imageCanaryWorker
      }
    }
  }
}
