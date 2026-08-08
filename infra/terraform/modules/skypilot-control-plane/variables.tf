# Inputs for a SkyPilot control plane hosted on an existing EKS cluster.

variable "aws_region" {
  description = "AWS region of the host cluster."
  type        = string

  validation {
    condition     = can(regex("^[a-z]{2}(-[a-z]+)+-[0-9]+$", var.aws_region))
    error_message = "aws_region must be a valid AWS region identifier."
  }
}

variable "aws_account_id" {
  description = "AWS account ID that owns the host cluster (used for provider guardrails)."
  type        = string

  validation {
    condition     = can(regex("^[0-9]{12}$", var.aws_account_id))
    error_message = "aws_account_id must contain exactly 12 digits."
  }
}

variable "aws_profile" {
  description = "Optional AWS profile passed to `aws eks get-token` when the provider reaches the host cluster. Null = ambient credentials (the terragrunt-assumed role)."
  type        = string
  default     = null
}

variable "aws_credentials_secret_name" {
  description = <<-EOT
    Name of a pre-created secret holding the API server's ~/.aws/config (the
    cross-account VM-pool assume-role profile). When set, the chart mounts it
    read-only at /root/.aws (awsCredentials.useCredentialsFile) and the module
    overlays a writable emptyDir at /root/.aws/cli so the AWS CLI (used for SSM)
    can write its assume-role cache. Null = no AWS credentials file mounted.
  EOT
  type        = string
  default     = null
}

variable "host_cluster_name" {
  description = <<-EOT
    Name of the EXISTING EKS cluster the SkyPilot API server is deployed onto.
    The caller's AWS identity must already have kubectl/EKS access. The root
    caller owns Kubernetes and Helm provider configuration.
  EOT
  type        = string

  validation {
    condition     = can(regex("^[0-9A-Za-z][0-9A-Za-z_-]{0,99}$", var.host_cluster_name))
    error_message = "host_cluster_name must be a valid EKS cluster name of at most 100 characters."
  }
}

variable "namespace" {
  description = "Namespace for the SkyPilot API server."
  type        = string
  default     = "skypilot"

  validation {
    condition     = can(regex("^[a-z0-9]([-a-z0-9]*[a-z0-9])?$", var.namespace)) && length(var.namespace) <= 63
    error_message = "namespace must be a Kubernetes DNS label of at most 63 characters."
  }
}

variable "release_name" {
  description = "Helm release name. The chart derives the API service account as <release_name>-api-sa."
  type        = string
  default     = "skypilot"

  validation {
    condition     = can(regex("^[a-z0-9]([-a-z0-9]*[a-z0-9])?$", var.release_name)) && length(var.release_name) <= 53
    error_message = "release_name must be a Helm-compatible DNS label of at most 53 characters."
  }
}

variable "chart_version" {
  description = <<-EOT
    Version of the SkyPilot Helm chart. Pin to a known-good version; a null value
    hard-fails the plan. Version ranges and wildcard selectors are rejected.
  EOT
  type        = string
  default     = null

  validation {
    condition = var.chart_version == null || (
      trimspace(var.chart_version) != "" &&
      !can(regex("[*<>=~^,[:space:]]", var.chart_version))
    )
    error_message = "chart_version must be null or one exact chart version, not a range or wildcard."
  }
}

variable "chart_repository" {
  description = <<-EOT
    Helm chart repository. Defaults to the public SkyPilot chart. Private OCI
    repositories require matching credentials on the root Helm provider.
  EOT
  type        = string
  default     = "https://helm.skypilot.co"
}

variable "chart_name" {
  description = "Chart name within chart_repository."
  type        = string
  default     = "skypilot-nightly"
}

variable "chart_registry_login_url" {
  description = <<-EOT
    Private OCI registry login URL. This compatibility input is consumed by
    Terragrunt root-provider generation; ordinary child-module callers configure
    the Helm provider themselves. Leave null for a public HTTPS chart.
  EOT
  type        = string
  default     = null
}

variable "api_server_image" {
  description = <<-EOT
    Optional exact API-server image override. When operations_helper_image is
    null, this image is also used by the config seed and enabled cloud-login
    helpers. Leave null to retain the chart's API-server image default, but then
    operations_helper_image is required.
  EOT
  type        = string
  default     = null

  validation {
    condition     = var.api_server_image == null || trimspace(var.api_server_image) != ""
    error_message = "api_server_image must be null or a nonempty image reference."
  }
}

variable "operations_helper_image" {
  description = <<-EOT
    Exact image used for PostgreSQL config seeding and optional GCP/Azure login
    initialization. It must contain Python, SQLAlchemy, PyYAML, and the
    PostgreSQL driver; enabled cloud logins additionally require Bash and the
    corresponding gcloud or az CLI. Defaults to api_server_image.
  EOT
  type        = string
  default     = null

  validation {
    condition     = var.operations_helper_image == null || trimspace(var.operations_helper_image) != ""
    error_message = "operations_helper_image must be null or a nonempty image reference."
  }
}

variable "api_server_extra_envs" {
  description = <<-EOT
    Non-secret extra environment variables set on the API-server pod. Values are
    stored in Terraform state; use Kubernetes Secrets for credentials.
  EOT
  type        = list(object({ name = string, value = string }))
  default     = []

  validation {
    condition = length(var.api_server_extra_envs) == length(toset([
      for env in var.api_server_extra_envs : env.name
    ]))
    error_message = "api_server_extra_envs must not contain duplicate names; Kubernetes resolves duplicate env entries ambiguously."
  }
}

variable "catalog_mirror" {
  description = <<-EOT
    Optional self-hosted SkyPilot catalog mirror. `url` becomes
    SKYPILOT_HOSTED_CATALOG_DIR_URL. When token_secretsmanager_key is set, ESO
    materializes it into the `skypilot-catalog-token` Secret and injects
    SKYPILOT_HOSTED_CATALOG_TOKEN.
  EOT
  type = object({
    url                      = string
    token_secretsmanager_key = optional(string)
  })
  default = null
}

variable "nebius_credentials_secretsmanager_key" {
  description = <<-EOT
    Secrets Manager key holding the Nebius service-account bundle (JSON with
    properties `credentials_json` — the SDK authorized-key file content — and
    `tenant_id`). ESO-materializes the `skypilot-nebius-credentials` Secret,
    mounted at /root/.nebius as credentials.json + NEBIUS_TENANT_ID.txt, which
    is exactly where the SkyPilot Nebius adaptor looks. Null disables Nebius.
  EOT
  type        = string
  default     = null
}

variable "azure_credentials_secretsmanager_key" {
  description = <<-EOT
    Secrets Manager key holding the Azure service-principal bundle (JSON with
    properties `client_id`, `client_secret`, `tenant_id`, `subscription_id`).
    Unlike AWS/Nebius (credential files), SkyPilot's Azure adaptor authenticates
    off the `az` CLI profile: it requires `az --version`, a populated
    `~/.azure/azureProfile.json`, and `~/.azure/msal_token_cache.json` to exist.
    So instead of mounting files we ESO-materialize the SP into the
    `skypilot-azure-credentials` Secret and run an `az login --service-principal`
    init container that writes the CLI profile into a shared /root/.azure
    emptyDir (mirrors the keyless GCP `gcloud auth login` init container). The
    emptyDir also retains the SP secret entry, so the long-running server
    refreshes its own tokens. Null disables Azure.
  EOT
  type        = string
  default     = null
}

# --- Authentication (OIDC) --------------------------------------------------

variable "oauth_enabled" {
  description = "Enable OIDC/SSO auth on the API server ingress. Required for RBAC; the default basic-auth path does not support roles."
  type        = bool
  default     = true
}

variable "oidc_issuer_url" {
  description = "OIDC issuer URL."
  type        = string
  default     = "https://accounts.google.com"
}

variable "oauth_client_secret_name" {
  description = <<-EOT
    Name of a pre-created Kubernetes secret in `namespace` holding the OIDC
    client id/secret (consumed by the chart as
    auth.oauth.client-details-from-secret). Create it out of band so the client
    secret never lands in Terraform state. Null disables OIDC wiring.
  EOT
  type        = string
  default     = null
}

variable "workspace_email_domain" {
  description = "Restrict logins to this email domain (auth.oauth.email-domain). Null relies on the OIDC client's audience restriction."
  type        = string
  default     = null
}

variable "rbac_default_role" {
  description = <<-EOT
    Default role for newly auto-provisioned SSO users. SkyPilot ships this as
    `admin` to ease setup; we default to `user` for least privilege. NOTE: verify
    it actually takes effect on your chart version (see skypilot issue #9271).
  EOT
  type        = string
  default     = "user"
}

# --- Compute pools ----------------------------------------------------------

variable "include_host_cluster_as_pool" {
  description = "Expose the host cluster itself as a SkyPilot pool (kubernetesCredentials.useApiServerCluster)."
  type        = bool
  default     = false
}

variable "kubeconfig_secret_name" {
  description = <<-EOT
    Name of a pre-created Kubernetes secret in `namespace` holding a kubeconfig
    whose contexts point at external pools. Consumed via
    kubernetesCredentials.useKubeconfig + kubeconfigSecretName.
    The kubeconfig should authenticate via `aws eks get-token` using the API
    server's Pod Identity role. Null = host cluster only.
  EOT
  type        = string
  default     = null
}

variable "allowed_contexts" {
  description = "Kubernetes contexts SkyPilot may schedule onto (kubernetes.allowed_contexts), in failover order."
  type        = list(string)
  default     = []
}

variable "allowed_clouds" {
  description = "Clouds the API server is allowed to use (config allowed_clouds)."
  type        = list(string)
  default     = ["aws", "kubernetes"]
}

variable "config_extra" {
  description = <<-EOT
    Non-secret top-level keys merged into the DB-backed API-server config.
    Mappings deep-merge, lists/scalars replace, and workspaces is replaced
    wholesale. The Kubernetes block is merged with allowed_contexts. This value
    enters Terraform state and a ConfigMap; never put credentials here.
  EOT
  type        = any
  default     = {}
}

variable "prune_retired_serve_controller_keys" {
  description = <<-EOT
    Remove the retired serve.controller.consolidation_mode and
    serve.controller.external_load_balancer keys from the DB-backed config during
    seeding. This is a one-way cutover aid and is disabled by default so public-chart
    consumers retain their existing config behavior.
  EOT
  type        = bool
  default     = false
}

# --- GCP VM provisioner (keyless OIDC Workload Identity Federation) ----------
#
# GCP onboarding is keyless-only: the chart's service-account-key path is not
# exposed and remains disabled in skypilot.tf.
#
# `gcp_provisioner` (presence = enabled) groups ALL the GCP wiring behind one toggle. It
# provisions GCE VMs keylessly: the pod's projected K8s SA token (audience = the GCP WIF provider)
# is exchanged at Google STS to impersonate the provisioner SA — no key in the loop. The module
# mounts the cred-config + projected token and runs `gcloud auth login --cred-file` in an init
# container (the chart's key-only init would crash-loop on an external_account config).
#
# The operator must pre-create `cred_secret_name` in `namespace` — a keyless external_account
# cred-config (gcp-cred.json, NO key); see the README.
variable "gcp_provisioner" {
  description = "Keyless GCP VM provisioner wiring (Workload Identity Federation). Null disables it. The login init container uses operations_helper_image."
  type = object({
    project          = string
    cred_secret_name = string
    wif_audience     = string
  })
  default = null

  validation {
    condition = var.gcp_provisioner == null || try(
      can(regex("^[a-z][a-z0-9-]{4,28}[a-z0-9]$", var.gcp_provisioner.project)) &&
      can(regex("^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$", var.gcp_provisioner.cred_secret_name)) &&
      trimspace(var.gcp_provisioner.wif_audience) != "",
      false,
    )
    error_message = "gcp_provisioner requires a valid GCP project ID, Kubernetes secret name, and nonempty WIF audience."
  }
}

# --- State / persistence ----------------------------------------------------

variable "db_connection_secret_name" {
  description = <<-EOT
    Name of a Kubernetes secret (key `connection_string`) with an external
    PostgreSQL connection string (apiService.dbConnectionSecretName). REQUIRED —
    the control plane is DB-only: HA / zero-downtime RollingUpdate upgrades and a
    durable cross-cloud user+job ledger. The chart then requires apiService.config
    to be null, so the whole SkyPilot config is seeded into the DB by the
    config-seeding Job (config_seed.tf) rather than rendered inline.
  EOT
  type        = string

  validation {
    condition     = var.db_connection_secret_name != null && trimspace(var.db_connection_secret_name) != ""
    error_message = "db_connection_secret_name is required — the control plane is DB-only (external Postgres / in-cluster StatefulSet)."
  }
}

variable "request_store" {
  description = <<-EOT
    API request-envelope persistence settings rendered as the chart's
    requestStore values. The SQLite defaults preserve the chart's compatibility
    behavior; select PostgreSQL explicitly only after completing the chart's
    one-way request-store cutover procedure. Enabling built-in execution
    quiescence enforcement requires the PostgreSQL backend.
  EOT
  type = object({
    backend                                       = optional(string, "sqlite")
    enforce_builtin_execution_quiescence_backends = optional(bool, false)
    cutover_gate_path                             = optional(string, "/root/.sky/api-request-cutover.json")
  })
  default  = {}
  nullable = false

  validation {
    condition     = contains(["sqlite", "postgres"], var.request_store.backend)
    error_message = "request_store.backend must be either sqlite or postgres."
  }

  validation {
    condition     = trimspace(var.request_store.cutover_gate_path) != ""
    error_message = "request_store.cutover_gate_path must be nonempty."
  }

  validation {
    condition = (
      !var.request_store.enforce_builtin_execution_quiescence_backends ||
      var.request_store.backend == "postgres"
    )
    error_message = "request_store.enforce_builtin_execution_quiescence_backends requires request_store.backend=postgres."
  }
}

variable "rwx_authority_fence" {
  description = <<-EOT
    Optional steady-state verifier for a completed migration to static RWX
    storage. Null disables it. When set, the module mounts authority_claim_name
    read-only into a fail-closed init container on every API, executor, and
    controller pod. The long-running containers never receive this mount.

    The authority claim must be a dedicated static PVC backed by an EFS access
    point distinct from the writable state claim/access point. expected_sha256
    is the SHA-256 of the exact digest-sealed fence bytes emitted by the accepted
    finalizer and deliberately has no default. The PostgreSQL evidence digest
    is independently supplied and has no default. identity binds those bytes
    to the exact source, replacement-state, and authority Kubernetes/AWS
    objects.
  EOT
  type = object({
    authority_claim_name              = string
    state_claim_name                  = string
    expected_sha256                   = string
    expected_postgres_evidence_sha256 = string
    identity = object({
      source = object({
        pvc_name      = string
        pvc_uid       = string
        pv_name       = string
        pv_uid        = string
        ebs_volume_id = string
      })
      target = object({
        filesystem_id             = string
        state_access_point_id     = string
        state_pv_name             = string
        state_pv_uid              = string
        state_pvc_uid             = string
        authority_access_point_id = string
        authority_pv_name         = string
        authority_pv_uid          = string
        authority_pvc_uid         = string
      })
    })
  })
  default = null

  validation {
    condition = var.rwx_authority_fence == null || try(
      can(regex("^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)*$", var.rwx_authority_fence.authority_claim_name)) &&
      length(var.rwx_authority_fence.authority_claim_name) <= 253 &&
      can(regex("^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)*$", var.rwx_authority_fence.state_claim_name)) &&
      length(var.rwx_authority_fence.state_claim_name) <= 253 &&
      var.rwx_authority_fence.authority_claim_name != var.rwx_authority_fence.state_claim_name,
      false,
    )
    error_message = "rwx_authority_fence requires distinct, valid Kubernetes authority_claim_name and state_claim_name values."
  }

  validation {
    condition = var.rwx_authority_fence == null || try(
      can(regex("^[0-9a-f]{64}$", var.rwx_authority_fence.expected_sha256)),
      false,
    )
    error_message = "rwx_authority_fence.expected_sha256 must be exactly 64 lowercase hexadecimal characters."
  }

  validation {
    condition = var.rwx_authority_fence == null || try(
      can(regex("^[0-9a-f]{64}$", var.rwx_authority_fence.expected_postgres_evidence_sha256)),
      false,
    )
    error_message = "rwx_authority_fence.expected_postgres_evidence_sha256 must be exactly 64 lowercase hexadecimal characters."
  }

  validation {
    condition = var.rwx_authority_fence == null || try(
      alltrue([
        for name in [
          var.rwx_authority_fence.identity.source.pvc_name,
          var.rwx_authority_fence.identity.source.pv_name,
          var.rwx_authority_fence.identity.target.state_pv_name,
          var.rwx_authority_fence.identity.target.authority_pv_name,
        ] : can(regex("^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)*$", name)) && length(name) <= 253
      ]) &&
      alltrue([
        for uid in [
          var.rwx_authority_fence.identity.source.pvc_uid,
          var.rwx_authority_fence.identity.source.pv_uid,
          var.rwx_authority_fence.identity.target.state_pv_uid,
          var.rwx_authority_fence.identity.target.state_pvc_uid,
          var.rwx_authority_fence.identity.target.authority_pv_uid,
          var.rwx_authority_fence.identity.target.authority_pvc_uid,
        ] : can(regex("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", uid))
      ]) &&
      can(regex("^vol-[0-9a-f]{8}([0-9a-f]{9})?$", var.rwx_authority_fence.identity.source.ebs_volume_id)) &&
      can(regex("^fs-[0-9a-f]{8}([0-9a-f]{9})?$", var.rwx_authority_fence.identity.target.filesystem_id)) &&
      can(regex("^fsap-[0-9a-f]{8}([0-9a-f]{9})?$", var.rwx_authority_fence.identity.target.state_access_point_id)) &&
      can(regex("^fsap-[0-9a-f]{8}([0-9a-f]{9})?$", var.rwx_authority_fence.identity.target.authority_access_point_id)),
      false,
    )
    error_message = "rwx_authority_fence.identity must use valid Kubernetes object names, canonical lowercase Kubernetes UIDs, and AWS EBS/EFS resource IDs."
  }

  validation {
    condition = var.rwx_authority_fence == null || try(
      var.rwx_authority_fence.identity.target.state_access_point_id != var.rwx_authority_fence.identity.target.authority_access_point_id &&
      var.rwx_authority_fence.identity.target.state_pv_name != var.rwx_authority_fence.identity.target.authority_pv_name &&
      var.rwx_authority_fence.identity.target.state_pv_uid != var.rwx_authority_fence.identity.target.authority_pv_uid &&
      var.rwx_authority_fence.identity.target.state_pvc_uid != var.rwx_authority_fence.identity.target.authority_pvc_uid &&
      !contains([
        var.rwx_authority_fence.state_claim_name,
        var.rwx_authority_fence.authority_claim_name,
      ], var.rwx_authority_fence.identity.source.pvc_name) &&
      !contains([
        var.rwx_authority_fence.identity.target.state_pv_name,
        var.rwx_authority_fence.identity.target.authority_pv_name,
      ], var.rwx_authority_fence.identity.source.pv_name) &&
      !contains([
        var.rwx_authority_fence.identity.target.state_pv_uid,
        var.rwx_authority_fence.identity.target.authority_pv_uid,
      ], var.rwx_authority_fence.identity.source.pv_uid) &&
      !contains([
        var.rwx_authority_fence.identity.target.state_pvc_uid,
        var.rwx_authority_fence.identity.target.authority_pvc_uid,
      ], var.rwx_authority_fence.identity.source.pvc_uid),
      false,
    )
    error_message = "rwx_authority_fence must use distinct source, state, and authority claims, access points, PVs, and PVCs."
  }
}

# --- Ingress ----------------------------------------------------------------

variable "ingress_enabled" {
  description = "Create SkyPilot's own Ingress object (routes to the API server)."
  type        = bool
  default     = true
}

variable "install_bundled_ingress_nginx" {
  description = <<-EOT
    Install SkyPilot's bundled ingress-nginx controller. The module ships it in an
    ISOLATED posture on a shared cluster: a unique IngressClass (var.ingress_class_name)
    used on both the controller and SkyPilot's Ingress, the controller scoped to
    watch only its own namespace, and the cluster-wide admission webhook DISABLED —
    so it cannot adopt or reject ingresses belonging to other teams on the prod
    cluster. Set false to skip the bundled controller and front the API server with
    the platform's existing ingress instead (wire that via extra_helm_values).
  EOT
  type        = bool
  default     = true
}

variable "ingress_class_name" {
  description = <<-EOT
    IngressClass name used by BOTH SkyPilot's Ingress and the bundled controller.
    MUST be unique on a shared cluster (never plain "nginx") so this controller
    never collides with or hijacks other teams' ingresses.
  EOT
  type        = string
  default     = "skypilot-nginx"
}

variable "allow_public_ingress" {
  description = "Safety guard: the module refuses an internet-facing LB scheme unless this is true. Keep false in production — the endpoint should be reachable only over the VPN."
  type        = bool
  default     = false
}

variable "manage_namespace" {
  description = <<-EOT
    Create the SkyPilot namespace in Terraform. Set FALSE when the operator
    pre-creates the namespace and the OAuth/pool/DB secrets in it out of band:
    the Helm release consumes those secrets, so on first apply they must already
    exist — which is impossible if the same apply is also creating the namespace.
    Recommended prod flow: create ns + secrets first, then apply with
    manage_namespace=false. (With true, bootstrap the namespace via a targeted
    apply before creating secrets — see the README.)
  EOT
  type        = bool
  default     = true
}

variable "ingress_annotations" {
  description = <<-EOT
    Annotations for the ingress Service/Ingress. Defaults to an INTERNAL load
    balancer so the API/SSO surface is reachable only over the VPN, never the
    public internet. Override to add external-dns hostnames etc., but keep an
    internal scheme.
  EOT
  type        = map(string)
  default = {
    "service.beta.kubernetes.io/aws-load-balancer-scheme" = "internal"
  }
}

# --- IAM (workload identity for AWS) ----------------------------------------

variable "extra_policy_json" {
  description = "Optional additional IAM policy JSON to attach to the API server role (e.g. broader EC2 perms for VM-based pools)."
  type        = string
  default     = null
}

variable "extra_helm_values" {
  description = "Non-secret escape hatch: extra Helm values merged last. This value is stored in Terraform state; never place credentials here."
  type        = string
  default     = ""
}

variable "oauth_secretsmanager_key" {
  description = "AWS Secrets Manager secret name holding the OIDC client (JSON with client_id and client_secret). ESO materializes it into oauth_client_secret_name."
  type        = string
  default     = null
}

variable "oauth_cluster_secret_store" {
  description = "ESO ClusterSecretStore name used by every module-managed ExternalSecret."
  type        = string
  default     = "aws-secrets-manager"
}

variable "eso_secrets_reader_role_name" {
  description = <<-EOT
    Name of the host cluster's shared ESO controller IAM role to grant read access to
    this module's Secrets Manager secret(s). ESO authenticates ClusterSecretStore reads
    with its own (ambient Pod Identity) identity, so it must be allowed to read SkyPilot's
    secrets — this module attaches that grant itself, keeping the host module unaware of
    SkyPilot. Null = don't manage the grant (e.g. the secret is pre-created out of band).
  EOT
  type        = string
  default     = null
}

variable "tags" {
  description = "Tags applied to AWS resources created by this module."
  type        = map(string)
  default     = {}
}

variable "api_server_role_name" {
  description = "Optional API-server IAM role name. Null preserves the derived skypilot-api-<host_cluster_name> name."
  type        = string
  default     = null

  validation {
    condition = var.api_server_role_name == null || (
      length(var.api_server_role_name) >= 1 &&
      length(var.api_server_role_name) <= 64 &&
      can(regex("^[A-Za-z0-9+=,.@_-]+$", var.api_server_role_name))
    )
    error_message = "api_server_role_name must be null or a valid IAM role name of at most 64 characters."
  }
}

variable "permissions_boundary_arn" {
  description = "Optional organization-managed permissions boundary attached to the API-server IAM role."
  type        = string
  default     = null

  validation {
    condition = var.permissions_boundary_arn == null || can(regex(
      "^arn:aws(-[a-z0-9]+)*:iam::[0-9]{12}:policy/([A-Za-z0-9+=,.@_-]+/)*[A-Za-z0-9+=,.@_-]{1,128}$",
      var.permissions_boundary_arn,
    ))
    error_message = "permissions_boundary_arn must be null or an exact IAM managed-policy ARN."
  }
}
