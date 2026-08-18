# Config-seeding Job: writes inline_config into the DB-backed config_yaml table.
#
# The chart forbids helm-inline config when DB mode is on — config lives ONLY in postgres.
# Without this Job a fresh deploy comes up with no allowed_clouds/workspaces and SkyPilot
# auto-creates a wide-open skypilot-vpc, bypassing the spoke's SSH lockdown.
# Deep-merge: IaC keys win and runtime-only keys survive. `workspaces` is replaced wholesale.
# Retired SkyServe topology keys are pruned only when the caller explicitly opts in. See
# scripts/seed_config.py for merge/locking.
locals {
  seed_config_script = file("${path.module}/scripts/seed_config.py")
  # Keep the post-seed restart on the same readiness budget as Helm. The 1.1.159
  # rollout exceeded the old 180-second gate, then became healthy without any
  # intervention, leaving an otherwise successful apply marked failed.
  api_server_rollout_timeout_seconds = 600
  # Existing callers retain api_server_image as the helper fallback. The empty
  # fallback lets the lifecycle precondition return a targeted error.
  seed_image = var.operations_helper_image != null ? var.operations_helper_image : (
    var.api_server_image != null ? var.api_server_image : ""
  )
  # The API server only needs a post-seed restart when the desired DB-backed
  # configuration changes. Keep its generation independent from the helper
  # image so a normal runtime image rollout is not followed by a second,
  # redundant rollout during Terraform reconciliation.
  config_hash = substr(sha256(jsonencode({
    script                              = local.seed_config_script
    config                              = local.inline_config
    prune_retired_serve_controller_keys = var.prune_retired_serve_controller_keys
  })), 0, 12)
  # The completed Job has an immutable pod template, so its generation still
  # includes the helper image. Preserve the legacy object shape to avoid
  # replacing Jobs whose script, config, behavior, and image are unchanged.
  seed_job_hash = substr(sha256(jsonencode({
    script                              = local.seed_config_script
    config                              = local.inline_config
    prune_retired_serve_controller_keys = var.prune_retired_serve_controller_keys
    image                               = local.seed_image
  })), 0, 12)
}

resource "kubernetes_config_map_v1" "seed_config" {
  metadata {
    name      = "skypilot-seed-config"
    namespace = var.namespace
    labels    = { "app.kubernetes.io/managed-by" = "terraform" }
  }

  data = { "config.yaml" = local.inline_config }

  depends_on = [kubernetes_namespace_v1.skypilot]
}

# A Job's pod template is immutable, and the kubernetes provider does NOT force-new on a
# template change — so bumping a pod annotation is silently dropped and the seed never re-runs.
# Encode the config hash in the Job NAME instead: a new config yields a new name, which the
# provider treats as replace (destroy old, create new), so the seed actually re-runs on change.
resource "kubernetes_job_v1" "seed_config" {
  metadata {
    name      = "skypilot-seed-config-${local.seed_job_hash}"
    namespace = var.namespace
    labels    = { "app.kubernetes.io/managed-by" = "terraform" }
  }

  spec {
    backoff_limit = 5

    template {
      metadata {
        labels = { "app.kubernetes.io/managed-by" = "terraform" }
      }
      spec {
        restart_policy = "Never"

        container {
          name    = "seed"
          image   = local.seed_image
          command = ["python", "-c", local.seed_config_script]

          env {
            name = "SKYPILOT_DB_CONNECTION_URI"
            value_from {
              secret_key_ref {
                name = var.db_connection_secret_name
                key  = "connection_string"
              }
            }
          }

          env {
            name  = "SKYPILOT_PRUNE_RETIRED_SERVE_CONTROLLER_KEYS"
            value = tostring(var.prune_retired_serve_controller_keys)
          }

          volume_mount {
            name       = "desired-config"
            mount_path = "/seed"
            read_only  = true
          }
        }

        volume {
          name = "desired-config"
          config_map {
            name = kubernetes_config_map_v1.seed_config.metadata[0].name
          }
        }
      }
    }
  }

  # Keep config reconciliation independent from runtime Helm ownership. The
  # seed script waits for the API migration to create config_yaml, so a fresh
  # install remains ordered by readiness without pulling helm_release into a
  # targeted config-only plan. This also lets operators fix forward the live
  # runtime with Helm while Terraform continues to own the DB-backed config.
  depends_on          = [kubernetes_config_map_v1.seed_config]
  wait_for_completion = true

  timeouts {
    create = "10m"
    update = "10m"
  }

  lifecycle {
    precondition {
      condition     = trimspace(local.seed_image) != ""
      error_message = "Set operations_helper_image or api_server_image to an image containing the config-seed runtime."
    }
  }
}

# Reconcile the running server roles after a config change. The seed writes config_yaml, but the
# processes load workspaces/RBAC (casbin) into memory at boot and do NOT re-read on a raw DB write,
# so a privacy/allowlist change silently has no effect until a restart. Compatibility mode rolls
# only the all-role API Deployment. Guarded HA rolls the API, executor, and controller Deployments
# once per config generation, after the seed lands, so every role re-reconciles from the new config.
# Operators already have `aws eks get-token`/kubectl access to the host cluster (see main.tf); a
# temp kubeconfig keeps this off the caller's default context.
resource "terraform_data" "reconcile_api_server" {
  triggers_replace = local.config_hash

  provisioner "local-exec" {
    interpreter = ["bash", "-c"]
    environment = merge(local.provider_exec_env, {
      SKYPILOT_HIGH_AVAILABILITY_ENABLED = tostring(local.split_role_high_availability_enabled)
    })
    command = <<-EOT
      set -euo pipefail
      KUBECONFIG_TMP="$(mktemp)"
      trap 'rm -f "$KUBECONFIG_TMP"' EXIT
      proxy_args=()
      if [[ -n "$${KUBE_PROXY_URL:-}" ]]; then
        proxy_args=(--proxy-url "$KUBE_PROXY_URL")
      fi
      aws eks update-kubeconfig --name ${var.host_cluster_name} --region ${var.aws_region} --kubeconfig "$KUBECONFIG_TMP" "$${proxy_args[@]}" >/dev/null
      deployment_suffixes=(api-server)
      if [[ "$${SKYPILOT_HIGH_AVAILABILITY_ENABLED:-false}" == "true" ]]; then
        deployment_suffixes+=(executor controller)
      fi
      for deployment_suffix in "$${deployment_suffixes[@]}"; do
        kubectl --kubeconfig "$KUBECONFIG_TMP" -n ${var.namespace} rollout restart "deployment/${var.release_name}-$deployment_suffix"
      done
      for deployment_suffix in "$${deployment_suffixes[@]}"; do
        kubectl --kubeconfig "$KUBECONFIG_TMP" -n ${var.namespace} rollout status "deployment/${var.release_name}-$deployment_suffix" --timeout=${local.api_server_rollout_timeout_seconds}s
      done
    EOT
  }

  depends_on = [kubernetes_job_v1.seed_config]
}
