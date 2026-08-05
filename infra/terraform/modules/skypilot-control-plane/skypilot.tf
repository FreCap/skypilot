# SkyPilot API server (shared control plane) — skypilot-nightly Helm chart.
# Helm does not reject unknown keys: reconcile value paths against your pinned
# chart version's values.yaml before applying. Secrets (OAuth client, pool
# kubeconfig, Postgres DSN) must be pre-created in `namespace`; they never
# enter TF state.

resource "kubernetes_namespace_v1" "skypilot" {
  count = var.manage_namespace ? 1 : 0

  metadata {
    name = var.namespace
    labels = {
      "app.kubernetes.io/managed-by" = "Terraform"
      "app.kubernetes.io/part-of"    = "skypilot-control-plane"
    }
  }
}

locals {
  # The chart forbids inline apiService.config with an external DB; config_seed.tf
  # seeds this into the DB (config_yaml table) instead.
  inline_config = yamlencode(merge(
    {
      rbac           = { default_role = var.rbac_default_role }
      allowed_clouds = var.allowed_clouds
    },
    var.config_extra,
    {
      # Deep-merge the kubernetes block LAST so it wins: a plain merge() would let
      # config_extra.kubernetes (pod_config, provision_timeout, ...) REPLACE allowed_contexts,
      # dropping the contexts SkyPilot needs to reach the pools.
      kubernetes = merge({ allowed_contexts = var.allowed_contexts }, try(var.config_extra.kubernetes, {}))
    },
  ))

  # config is omitted (forbidden with dbConnectionSecretName); DB-seeded via config_seed.tf.
  api_service_values = merge({
    enableUserManagement   = true
    dbConnectionSecretName = var.db_connection_secret_name
    },
    var.api_server_image != null ? { image = var.api_server_image } : {},
  )

  oauth_values = merge(
    {
      enabled           = var.oauth_enabled
      "oidc-issuer-url" = var.oidc_issuer_url
    },
    var.oauth_client_secret_name != null ? { "client-details-from-secret" = var.oauth_client_secret_name } : {},
    var.workspace_email_domain != null ? { "email-domain" = var.workspace_email_domain } : {},
  )

  # NOTE: keep useKubeconfig (a bool) in the static object, not in the ternary.
  # A ternary whose branches are objects of different shapes
  # (`{useKubeconfig=bool, kubeconfigSecretName=string}` vs `{}`) unifies to
  # map(string), which coerces the bool to the string "true" — the chart then
  # rejects it ("/kubernetesCredentials/useKubeconfig: got string, want boolean").
  # Only the string-valued kubeconfigSecretName is added conditionally.
  kube_creds = merge(
    {
      useApiServerCluster = var.include_host_cluster_as_pool
      inclusterNamespace  = var.namespace
      useKubeconfig       = var.kubeconfig_secret_name != null
    },
    var.kubeconfig_secret_name != null ? {
      kubeconfigSecretName = var.kubeconfig_secret_name
    } : {},
  )


  # True if generated or escape-hatch ingress annotations request a public load
  # balancer. Checking both prevents extra_helm_values from bypassing the guard.
  ingress_scheme_is_public = anytrue([
    for v in concat(
      values(var.ingress_annotations),
      try(values(local.extra_helm_values_decoded.ingress.annotations), []),
    ) : can(regex("internet-facing", tostring(v)))
  ])

  # Isolated posture: unique class, namespace-scoped watch, webhook off — never
  # adopts other teams' ingresses on a shared cluster.
  ingress_nginx_values = merge(
    { enabled = var.install_bundled_ingress_nginx },
    var.install_bundled_ingress_nginx ? {
      controller = {
        scope = { enabled = true }
        ingressClassResource = {
          name            = var.ingress_class_name
          controllerValue = "k8s.io/${var.ingress_class_name}"
          default         = false
        }
        admissionWebhooks        = { enabled = false }
        watchIngressWithoutClass = false
      }
    } : {},
  )

  # aws-credentials secret mounts /root/.aws read-only; emptyDir overlay at
  # /root/.aws/cli lets the AWS CLI (SSM) write its cache.
  aws_creds_enabled = var.aws_credentials_secret_name != null
  # Keep enabled/useCredentialsFile as BOOLS — same map(string)-coercion trap as kube_creds.
  # Build statically; gate inclusion in helm_values on local.aws_creds_enabled.
  aws_credentials_values = {
    enabled            = true
    useCredentialsFile = true
    awsSecretName      = local.aws_creds_enabled ? var.aws_credentials_secret_name : ""
  }
  # `[for x in [...] : x if cond]` avoids the ternary same-length-tuple type error.
  aws_volumes       = [for v in [{ name = "aws-cli-cache", emptyDir = {} }] : v if local.aws_creds_enabled]
  aws_volume_mounts = [for v in [{ name = "aws-cli-cache", mountPath = "/root/.aws/cli" }] : v if local.aws_creds_enabled]

  # Keyless GCP VM provisioner via WIF — one toggle: var.gcp_provisioner.
  gcp_provisioner_enabled = var.gcp_provisioner != null
  # The config seed and optional cloud-login init containers use one explicitly
  # pinned helper image. Existing callers retain api_server_image as the fallback.
  init_helper_image = var.operations_helper_image != null ? var.operations_helper_image : (
    var.api_server_image != null ? var.api_server_image : ""
  )
  gcp_login_image = local.gcp_provisioner_enabled ? local.init_helper_image : null
  # gcp-cred: keyless external_account cred-config (no key).
  # gcp-token: projected K8s SA token whose audience is the WIF provider; cred-config
  #   exchanges it at Google STS to impersonate the provisioner SA.
  # gcloud-config: writable CLOUDSDK_CONFIG populated by the init container.
  gcp_volumes = [for v in [
    { name = "gcp-cred", secret = { secretName = try(var.gcp_provisioner.cred_secret_name, "") } },
    { name = "gcp-token", projected = { sources = [{ serviceAccountToken = {
      audience          = try(var.gcp_provisioner.wif_audience, "")
      expirationSeconds = 3600
      path              = "token"
    } }] } },
    { name = "gcloud-config", emptyDir = {} },
  ] : v if local.gcp_provisioner_enabled]
  gcp_volume_mounts = [for v in [
    { name = "gcp-cred", mountPath = "/var/secrets/gcp", readOnly = true },
    { name = "gcp-token", mountPath = "/var/run/secrets/gcp", readOnly = true },
    { name = "gcloud-config", mountPath = "/var/gcloud" },
  ] : v if local.gcp_provisioner_enabled]
  # GOOGLE_CLOUD_PROJECT: keyless cred-config carries no project; must be explicit.
  # CLOUDSDK_CONFIG: points `gcloud auth list` (sky check gcp) at the init-container login.
  # (CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE registers no active account — init login wins.)
  gcp_envs = [for v in [
    { name = "GOOGLE_APPLICATION_CREDENTIALS", value = "/var/secrets/gcp/gcp-cred.json" },
    { name = "GOOGLE_CLOUD_PROJECT", value = try(var.gcp_provisioner.project, "") },
    { name = "CLOUDSDK_CONFIG", value = "/var/gcloud" },
  ] : v if local.gcp_provisioner_enabled]
  # Top-level extraInitContainers — apiService.extraInitContainers is silently dropped.
  # `gcloud auth login --cred-file` registers the SA in CLOUDSDK_CONFIG; the chart's
  # gcpCredentials init rejects external_account configs and crash-loops.
  gcp_init_containers = [for c in [{
    name    = "gcp-gcloud-login"
    image   = local.gcp_login_image
    command = ["bash", "-c"]
    args    = ["set -e\ngcloud auth login --cred-file=/var/secrets/gcp/gcp-cred.json\ngcloud config set project ${try(var.gcp_provisioner.project, "")}\n"]
    env     = [{ name = "CLOUDSDK_CONFIG", value = "/var/gcloud" }]
    volumeMounts = [
      { name = "gcp-cred", mountPath = "/var/secrets/gcp", readOnly = true },
      { name = "gcp-token", mountPath = "/var/run/secrets/gcp", readOnly = true },
      { name = "gcloud-config", mountPath = "/var/gcloud" },
    ]
  }] : c if local.gcp_provisioner_enabled]

  # --- Self-hosted catalog mirror ---
  catalog_mirror_enabled = var.catalog_mirror != null
  catalog_token_enabled  = try(var.catalog_mirror.token_secretsmanager_key, null) != null
  catalog_envs = concat(
    [for v in [{ name = "SKYPILOT_HOSTED_CATALOG_DIR_URL", value = try(var.catalog_mirror.url, "") }] : v if local.catalog_mirror_enabled],
    # valueFrom entry has a different object shape than {name, value}; the
    # env lists are jsonencode-normalized below before concat so Terraform
    # does not demand a common object type.
    [for v in [{ name = "SKYPILOT_HOSTED_CATALOG_TOKEN", valueFrom = { secretKeyRef = { name = "skypilot-catalog-token", key = "token" } } }] : v if local.catalog_token_enabled],
  )

  # --- Nebius compute credentials ---
  # Secret keys are the exact filenames the adaptor reads under ~/.nebius
  # (credentials.json + NEBIUS_TENANT_ID.txt); container HOME is /root, same
  # convention as the aws-credentials mount above.
  nebius_enabled = var.nebius_credentials_secretsmanager_key != null
  nebius_volumes = [for v in [
    { name = "nebius-credentials", secret = { secretName = "skypilot-nebius-credentials" } },
  ] : v if local.nebius_enabled]
  nebius_volume_mounts = [for v in [
    { name = "nebius-credentials", mountPath = "/root/.nebius", readOnly = true },
  ] : v if local.nebius_enabled]

  # --- Azure service principal ---
  # SkyPilot's Azure adaptor authenticates off the `az` CLI profile (needs
  # ~/.azure/azureProfile.json + msal_token_cache.json + a working `az`), not a
  # credential file. So we mirror the keyless GCP init-container pattern: an
  # `az login --service-principal` init container populates a shared /root/.azure
  # emptyDir that the main container reads. The emptyDir also retains the SP
  # secret entry, so the long-running server refreshes tokens without re-login.
  azure_enabled = var.azure_credentials_secretsmanager_key != null
  # Reuse the pinned api-server image (azure-cli is bundled — see Dockerfile).
  azure_login_image = local.azure_enabled ? local.init_helper_image : null
  # azure-config: writable ~/.azure (AZURE_CONFIG_DIR default), shared init->main.
  azure_volumes = [for v in [
    { name = "azure-config", emptyDir = {} },
  ] : v if local.azure_enabled]
  azure_volume_mounts = [for v in [
    { name = "azure-config", mountPath = "/root/.azure" },
  ] : v if local.azure_enabled]
  # SP creds injected as env (secretKeyRef) — never rendered into the pod spec.
  # `az login` acquires a token (populates azureProfile.json + msal cache);
  # `get-access-token` + `touch` guarantee the msal_token_cache.json that
  # SkyPilot's _check_credentials strictly requires exists even if a given
  # az build defers writing it. HOME=/root so ~/.azure resolves to the mount.
  azure_init_containers = [for c in [{
    name    = "azure-cli-login"
    image   = local.azure_login_image
    command = ["bash", "-c"]
    args    = ["set -e\naz login --service-principal -u \"$AZURE_CLIENT_ID\" -p \"$AZURE_CLIENT_SECRET\" --tenant \"$AZURE_TENANT_ID\" >/dev/null\naz account set -s \"$AZURE_SUBSCRIPTION_ID\"\naz account get-access-token -o none\ntouch /root/.azure/msal_token_cache.json\n"]
    env = [
      { name = "HOME", value = "/root" },
      { name = "AZURE_CLIENT_ID", valueFrom = { secretKeyRef = { name = "skypilot-azure-credentials", key = "client-id" } } },
      { name = "AZURE_CLIENT_SECRET", valueFrom = { secretKeyRef = { name = "skypilot-azure-credentials", key = "client-secret" } } },
      { name = "AZURE_TENANT_ID", valueFrom = { secretKeyRef = { name = "skypilot-azure-credentials", key = "tenant-id" } } },
      { name = "AZURE_SUBSCRIPTION_ID", valueFrom = { secretKeyRef = { name = "skypilot-azure-credentials", key = "subscription-id" } } },
    ]
    volumeMounts = [
      { name = "azure-config", mountPath = "/root/.azure" },
    ]
  }] : c if local.azure_enabled]

  # Helm REPLACES arrays (only deep-merges maps): full arrays assembled here, never split
  # with var.extra_helm_values. Keys omitted when array is empty.
  all_extra_volumes       = concat(local.aws_volumes, local.gcp_volumes, local.nebius_volumes, local.azure_volumes)
  all_extra_volume_mounts = concat(local.aws_volume_mounts, local.gcp_volume_mounts, local.nebius_volume_mounts, local.azure_volume_mounts)
  # gcp and azure both use top-level extraInitContainers (keyless CLI logins).
  # jsondecode(jsonencode(...)) erases per-container types before concat: the
  # gcp init env is all {name,value} while the azure init env mixes {name,value}
  # and {name,valueFrom} — the same heterogeneous-object trap handled for
  # all_extra_envs below; without it concat demands one common element type.
  all_init_containers = concat(
    jsondecode(jsonencode(local.gcp_init_containers)),
    jsondecode(jsonencode(local.azure_init_containers)),
  )
  # jsondecode(jsonencode(...)) erases the per-list object types: env entries
  # with `value` and with `valueFrom` cannot otherwise share one concat().
  all_extra_envs = concat(
    jsondecode(jsonencode(local.gcp_envs)),
    jsondecode(jsonencode(var.api_server_extra_envs)),
    jsondecode(jsonencode(local.catalog_envs)),
  )
  all_extra_env_names = [
    for env in local.all_extra_envs : env.name
  ]
  duplicate_extra_env_names = toset([
    for env_name in local.all_extra_env_names : env_name
    if length([
      for candidate in local.all_extra_env_names : candidate
      if candidate == env_name
    ]) > 1
  ])
  api_service_extra = merge(
    length(local.all_extra_volumes) > 0 ? { extraVolumes = local.all_extra_volumes } : {},
    length(local.all_extra_volume_mounts) > 0 ? { extraVolumeMounts = local.all_extra_volume_mounts } : {},
    length(local.all_extra_envs) > 0 ? { extraEnvs = local.all_extra_envs } : {},
  )
  # Helm replaces arrays instead of deep-merging them. These arrays are fully
  # assembled above so provider credentials and operator-supplied env knobs
  # cannot be silently discarded by the last-applied escape-hatch values.
  module_owned_api_service_array_keys = toset([
    "extraEnvs",
    "extraVolumeMounts",
    "extraVolumes",
  ])
  module_owned_top_level_array_keys = toset([
    "extraInitContainers",
  ])
  # Decode the escape hatch into a safe fallback. Separate validity and shape
  # guards below reject invalid YAML, non-map top-level values, and a null or
  # non-map apiService before its protected keys are inspected.
  extra_helm_values_normalized = (
    trimspace(var.extra_helm_values) == "" ? "{}" : var.extra_helm_values
  )
  extra_helm_values_valid_yaml = can(
    yamldecode(local.extra_helm_values_normalized)
  )
  extra_helm_values_decoded = try(
    yamldecode(local.extra_helm_values_normalized),
    {},
  )
  extra_helm_values_is_map = can(keys(local.extra_helm_values_decoded))
  extra_helm_top_level_keys = toset(try(
    keys(local.extra_helm_values_decoded),
    [],
  ))
  extra_helm_request_store_present = contains(
    local.extra_helm_top_level_keys,
    "requestStore",
  )
  extra_helm_api_service_present = contains(
    local.extra_helm_top_level_keys,
    "apiService",
  )
  extra_helm_api_service_is_map = (
    !local.extra_helm_api_service_present ||
    can(keys(try(local.extra_helm_values_decoded.apiService, null)))
  )
  extra_helm_api_service_keys = toset(try(
    keys(try(local.extra_helm_values_decoded.apiService, null)),
    [],
  ))
  redefined_api_service_array_keys = setintersection(
    local.module_owned_api_service_array_keys,
    local.extra_helm_api_service_keys,
  )
  redefined_top_level_array_keys = setintersection(
    local.module_owned_top_level_array_keys,
    local.extra_helm_top_level_keys,
  )

  helm_values = merge({
    apiService = merge(local.api_service_values, local.api_service_extra)
    requestStore = {
      backend                                   = var.request_store.backend
      enforceBuiltinExecutionQuiescenceBackends = var.request_store.enforce_builtin_execution_quiescence_backends
      cutoverGatePath                           = var.request_store.cutover_gate_path
    }
    auth = {
      oauth          = local.oauth_values
      serviceAccount = { enabled = true }
    }
    # No SA annotation: Pod Identity binds the role via the association in iam.tf.
    kubernetesCredentials = local.kube_creds
    # Disabled ({} = chart default): GCP uses keyless WIF init container, not SA keys.
    gcpCredentials = {}
    ingress = merge(
      {
        enabled          = var.ingress_enabled
        ingressClassName = var.ingress_class_name
      },
      length(var.ingress_annotations) > 0 ? { annotations = var.ingress_annotations } : {},
    )
    "ingress-nginx" = local.ingress_nginx_values
    # storage: chart default (PVC on). Never disable it — in-flight requests
    # only survive a pod replacement if their state outlives the pod.
    },
    # Top-level, not under apiService (nesting there is silently dropped).
    length(local.all_init_containers) > 0 ? { extraInitContainers = local.all_init_containers } : {},
    local.aws_creds_enabled ? { awsCredentials = local.aws_credentials_values } : {},
  )
}

resource "helm_release" "skypilot" {
  name             = var.release_name
  repository       = var.chart_repository
  chart            = var.chart_name
  version          = var.chart_version
  namespace        = var.namespace
  create_namespace = false
  devel            = true # chart is published as a pre-release (-dev) version
  timeout          = local.api_server_rollout_timeout_seconds
  replace          = true # Required to recover from failed releases (house convention)
  wait             = true # Host cluster has nodes; surface install failures at apply time

  values = compact([
    yamlencode(local.helm_values),
    var.extra_helm_values,
  ])

  # The catalog-token and nebius ExternalSecrets are not read at helm template
  # time (secretKeyRef/volume are resolved by the kubelet), but ordering them
  # before the release keeps a first apply on a fresh namespace from spending
  # its helm timeout in ContainerCreating while ESO does its initial sync.
  depends_on = [
    kubernetes_namespace_v1.skypilot,
    time_sleep.wait_oauth_secret,
    kubernetes_manifest.catalog_token_external_secret,
    kubernetes_manifest.nebius_credentials_external_secret,
    kubernetes_manifest.azure_credentials_external_secret,
  ]

  lifecycle {
    precondition {
      condition     = var.chart_version != null
      error_message = "chart_version is null: pin one exact SkyPilot chart version before applying. A floating prerelease can change cluster-scoped resources and auth/RBAC behavior without a code diff."
    }
    precondition {
      condition     = !var.oauth_enabled || var.oauth_client_secret_name != null
      error_message = "oauth_client_secret_name is required when oauth_enabled is true."
    }
    precondition {
      condition     = var.oauth_secretsmanager_key == null || var.oauth_client_secret_name != null
      error_message = "oauth_client_secret_name is required when oauth_secretsmanager_key is set."
    }
    precondition {
      condition     = length(local.eso_read_secret_keys) == 0 || trimspace(var.oauth_cluster_secret_store) != ""
      error_message = "oauth_cluster_secret_store must be nonempty when module-managed ExternalSecrets are enabled."
    }
    precondition {
      condition     = var.allow_public_ingress || !local.ingress_scheme_is_public
      error_message = "ingress_annotations requests an internet-facing load balancer but allow_public_ingress is false. The SkyPilot control plane should be VPN-internal; set allow_public_ingress=true only if a public endpoint is genuinely intended."
    }
    precondition {
      condition     = length(local.duplicate_extra_env_names) == 0
      error_message = "apiService.extraEnvs contains duplicate names after assembling generated and user-provided values: ${join(", ", sort(tolist(local.duplicate_extra_env_names)))}."
    }
    precondition {
      condition     = local.extra_helm_values_valid_yaml
      error_message = "extra_helm_values must be valid YAML."
    }
    precondition {
      condition     = local.extra_helm_values_is_map
      error_message = "extra_helm_values must decode to a top-level YAML map."
    }
    precondition {
      condition     = local.extra_helm_api_service_is_map
      error_message = "extra_helm_values.apiService must be a YAML map when present; null, scalar, and list values are not allowed."
    }
    precondition {
      condition     = !local.extra_helm_request_store_present
      error_message = "extra_helm_values must not redefine requestStore; use the typed request_store input so Terraform cannot silently override the selected persistence contract."
    }
    precondition {
      condition     = length(local.redefined_api_service_array_keys) == 0 && length(local.redefined_top_level_array_keys) == 0
      error_message = "extra_helm_values must not redefine apiService.extraEnvs, apiService.extraVolumes, apiService.extraVolumeMounts, or top-level extraInitContainers; the module assembles these arrays and Helm would replace them."
    }
  }
}
