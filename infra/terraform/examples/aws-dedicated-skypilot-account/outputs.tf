output "qualification_config_map_data" {
  description = "Use directly as the data field of the qualification ConfigMap."
  value = {
    for workspace, manifest in local.qualification_manifests :
    "${substr(sha256(lower(trimspace(workspace))), 0, 16)}-${var.profile}.json" => jsonencode(manifest)
  }
}

output "qualification_config_map_name" {
  description = "Content-addressed name for the immutable qualification ConfigMap consumed by the copy worker."
  value       = local.qualification_config_map_name
}

output "qualification_repositories_by_region" {
  description = "Active and retained qualification repository facts for rollout verification."
  value = {
    (var.home_region) = {
      active_generation          = module.home_distribution.qualification_repository_generation
      active_ownership_tags_hash = module.home_distribution.role_fingerprints["${var.home_region}:qualification_ownership_tags_hash"]
      active_policy_hash         = module.home_distribution.role_fingerprints["${var.home_region}:qualification_policy_hash"]
      active_url                 = module.home_distribution.qualification_repository_url
      urls_by_generation         = module.home_distribution.qualification_repository_urls_by_generation
      arns_by_generation         = module.home_distribution.qualification_repository_arns_by_generation
      policy_modes_by_generation = module.home_distribution.qualification_repository_policy_modes_by_generation
    }
    (var.secondary_region) = {
      active_generation          = module.secondary_distribution.qualification_repository_generation
      active_ownership_tags_hash = module.secondary_distribution.role_fingerprints["${var.secondary_region}:qualification_ownership_tags_hash"]
      active_policy_hash         = module.secondary_distribution.role_fingerprints["${var.secondary_region}:qualification_policy_hash"]
      active_url                 = module.secondary_distribution.qualification_repository_url
      urls_by_generation         = module.secondary_distribution.qualification_repository_urls_by_generation
      arns_by_generation         = module.secondary_distribution.qualification_repository_arns_by_generation
      policy_modes_by_generation = module.secondary_distribution.qualification_repository_policy_modes_by_generation
    }
  }
}

output "helm_image_worker_values" {
  description = "Steady-state or fresh-install Helm values only. Do not apply directly for an existing installation's staged protocol-2 upgrade."
  value = {
    imageCopyWorker = {
      enabled                            = true
      replicaCount                       = 1
      maxInFlight                        = 4
      qualificationManifestConfigMapName = local.qualification_config_map_name
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
