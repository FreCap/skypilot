# Source the OIDC client id/secret from AWS Secrets Manager via External Secrets
# Operator (ESO, authed by Pod Identity on the host cluster) instead of a hand-created
# plaintext Secret. ESO materializes the `oauth_client_secret_name` Secret the chart reads.

# The shared ESO controller authenticates ClusterSecretStore reads with its OWN identity,
# so it needs read access to this secret. Own the grant here (an inline policy on the
# host's ESO role) rather than in the host module, so the host stays SkyPilot-agnostic.
locals {
  # Every Secrets Manager key ESO reads on SkyPilot's behalf. The single
  # inline policy below covers them all (same resource name as before, so
  # adding a key is a policy update, not an IAM object churn).
  eso_read_secret_keys = compact([
    var.oauth_secretsmanager_key,
    try(var.catalog_mirror.token_secretsmanager_key, null),
    var.nebius_credentials_secretsmanager_key,
    var.azure_credentials_secretsmanager_key,
  ])
}

# NOTE: the resource label says "oauth" for historical reasons but the policy
# now covers every key in eso_read_secret_keys (oauth + catalog PAT + nebius).
# Keeping the label avoids a Terraform address change/state move; the inline
# policy name was always the generic "${release}-eso-read-secrets".
resource "aws_iam_role_policy" "eso_read_oauth_secret" {
  count = length(local.eso_read_secret_keys) > 0 && var.eso_secrets_reader_role_name != null ? 1 : 0

  name = "${var.release_name}-eso-read-secrets"
  role = var.eso_secrets_reader_role_name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
      Resource = [for k in local.eso_read_secret_keys : "arn:${data.aws_partition.current.partition}:secretsmanager:${var.aws_region}:${var.aws_account_id}:secret:${k}-*"]
    }]
  })
}

resource "kubernetes_manifest" "oauth_external_secret" {
  count = var.oauth_secretsmanager_key != null ? 1 : 0

  # ESO must be allowed to read the secret before it can sync it.
  depends_on = [
    aws_iam_role_policy.eso_read_oauth_secret,
    kubernetes_namespace_v1.skypilot,
  ]

  manifest = {
    apiVersion = "external-secrets.io/v1"
    kind       = "ExternalSecret"
    metadata = {
      name      = var.oauth_client_secret_name
      namespace = var.namespace
    }
    spec = {
      refreshInterval = "1h"
      secretStoreRef = {
        name = var.oauth_cluster_secret_store
        kind = "ClusterSecretStore"
      }
      target = {
        name           = var.oauth_client_secret_name
        creationPolicy = "Owner"
        deletionPolicy = "Retain"
      }
      data = [
        { secretKey = "client-id", remoteRef = { key = var.oauth_secretsmanager_key, property = "client_id" } },
        { secretKey = "client-secret", remoteRef = { key = var.oauth_secretsmanager_key, property = "client_secret" } },
      ]
    }
  }
}

# The chart does a template-time lookup of the Secret and fails if its keys are absent,
# so give ESO a moment to populate it after the ExternalSecret is created.
resource "time_sleep" "wait_oauth_secret" {
  count           = var.oauth_secretsmanager_key != null ? 1 : 0
  depends_on      = [kubernetes_manifest.oauth_external_secret]
  create_duration = "25s"
}


# GitHub PAT for the private catalog mirror: plain-string secret -> single
# `token` key consumed as SKYPILOT_HOSTED_CATALOG_TOKEN (skypilot.tf).
resource "kubernetes_manifest" "catalog_token_external_secret" {
  count = try(var.catalog_mirror.token_secretsmanager_key, null) != null ? 1 : 0

  depends_on = [
    aws_iam_role_policy.eso_read_oauth_secret,
    kubernetes_namespace_v1.skypilot,
  ]

  manifest = {
    apiVersion = "external-secrets.io/v1"
    kind       = "ExternalSecret"
    metadata = {
      name      = "skypilot-catalog-token"
      namespace = var.namespace
    }
    spec = {
      refreshInterval = "1h"
      secretStoreRef = {
        name = var.oauth_cluster_secret_store
        kind = "ClusterSecretStore"
      }
      target = {
        name           = "skypilot-catalog-token"
        creationPolicy = "Owner"
        deletionPolicy = "Retain"
      }
      # Plain-string secret: no `property` — the whole value is the token.
      data = [
        { secretKey = "token", remoteRef = { key = var.catalog_mirror.token_secretsmanager_key } },
      ]
    }
  }
}

# Nebius service-account bundle -> Secret whose data KEYS are the exact
# filenames the SkyPilot Nebius adaptor expects under ~/.nebius (mounted in
# skypilot.tf). SM properties are underscored (credentials_json/tenant_id):
# ESO's gjson `property` treats dots as path separators.
resource "kubernetes_manifest" "nebius_credentials_external_secret" {
  count = var.nebius_credentials_secretsmanager_key != null ? 1 : 0

  depends_on = [
    aws_iam_role_policy.eso_read_oauth_secret,
    kubernetes_namespace_v1.skypilot,
  ]

  manifest = {
    apiVersion = "external-secrets.io/v1"
    kind       = "ExternalSecret"
    metadata = {
      name      = "skypilot-nebius-credentials"
      namespace = var.namespace
    }
    spec = {
      refreshInterval = "1h"
      secretStoreRef = {
        name = var.oauth_cluster_secret_store
        kind = "ClusterSecretStore"
      }
      target = {
        name           = "skypilot-nebius-credentials"
        creationPolicy = "Owner"
        deletionPolicy = "Retain"
      }
      data = [
        { secretKey = "credentials.json", remoteRef = { key = var.nebius_credentials_secretsmanager_key, property = "credentials_json" } },
        { secretKey = "NEBIUS_TENANT_ID.txt", remoteRef = { key = var.nebius_credentials_secretsmanager_key, property = "tenant_id" } },
      ]
    }
  }
}

# Azure service-principal bundle -> Secret consumed as env by the
# `az login --service-principal` init container (skypilot.tf). SP secrets are
# injected via secretKeyRef, never written to the pod spec/args. SM properties
# are the JSON keys client_id/client_secret/tenant_id/subscription_id.
resource "kubernetes_manifest" "azure_credentials_external_secret" {
  count = var.azure_credentials_secretsmanager_key != null ? 1 : 0

  depends_on = [
    aws_iam_role_policy.eso_read_oauth_secret,
    kubernetes_namespace_v1.skypilot,
  ]

  manifest = {
    apiVersion = "external-secrets.io/v1"
    kind       = "ExternalSecret"
    metadata = {
      name      = "skypilot-azure-credentials"
      namespace = var.namespace
    }
    spec = {
      refreshInterval = "1h"
      secretStoreRef = {
        name = var.oauth_cluster_secret_store
        kind = "ClusterSecretStore"
      }
      target = {
        name           = "skypilot-azure-credentials"
        creationPolicy = "Owner"
        deletionPolicy = "Retain"
      }
      data = [
        { secretKey = "client-id", remoteRef = { key = var.azure_credentials_secretsmanager_key, property = "client_id" } },
        { secretKey = "client-secret", remoteRef = { key = var.azure_credentials_secretsmanager_key, property = "client_secret" } },
        { secretKey = "tenant-id", remoteRef = { key = var.azure_credentials_secretsmanager_key, property = "tenant_id" } },
        { secretKey = "subscription-id", remoteRef = { key = var.azure_credentials_secretsmanager_key, property = "subscription_id" } },
      ]
    }
  }
}
