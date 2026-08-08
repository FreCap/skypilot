# SkyPilot control plane on EKS

Installs a pinned SkyPilot Helm release on an existing EKS cluster, creates the
API-server EKS Pod Identity role, and seeds PostgreSQL-backed SkyPilot
configuration. The module is provider-neutral: the root caller configures and
passes AWS, Kubernetes, Helm, and Time providers.

## Prerequisites

- Terraform or OpenTofu 1.5 or newer.
- An existing EKS cluster with the EKS Pod Identity agent.
- Root AWS, Kubernetes, and Helm providers targeting the same account, region,
  and cluster.
- A pinned SkyPilot chart version.
- A PostgreSQL connection Kubernetes Secret with key `connection_string`.
- An immutable helper image containing Python, SQLAlchemy, PyYAML, and the
  PostgreSQL driver. GCP or Azure login also requires Bash and `gcloud` or `az`.
- On the machine running Terraform: authenticated `aws` and `kubectl`, Bash,
  `mktemp`, registry access, and network access to the EKS API.
- For proxied EKS API access, set `KUBE_PROXY_URL` in the Terraform process
  environment. At execution time the reconciliation command adds it to
  `aws eks update-kubeconfig` as `--proxy-url`, without storing the URL in
  Terraform configuration or state. Leaving it unset preserves direct access.
- When an External Secret input is enabled: External Secrets Operator CRDs and
  the configured ClusterSecretStore. If `eso_secrets_reader_role_name` is set,
  this state also owns an inline read policy on that existing role.

Bootstrap the namespace and required Secrets before the main apply. For an
operator-managed namespace, create it first and set `manage_namespace = false`.
For a module-managed namespace, create it in a targeted first stage, create the
Secrets, and then run the complete plan. Do not put credentials in
`config_extra`, `extra_helm_values`, or `api_server_extra_envs`; these inputs are
stored in Terraform state and configuration also enters a ConfigMap.

The module explicitly renders the chart's `requestStore` block. It defaults to
SQLite so adopting the module cannot silently perform the chart's one-way
request-store cutover. New installations may select `backend = "postgres"`
once the chart's fresh-schema bootstrap/migration can complete. Existing
installations must first complete the release's documented cutover procedure
and verify its durable gate before changing this input. Set
`enforce_builtin_execution_quiescence_backends = true` for a PostgreSQL
production control plane only when every execution path uses the built-in
PostgreSQL storage and queue backends; the module rejects that guard with
SQLite. `cutover_gate_path` defaults to the chart's durable gate location.
`requestStore` is module-owned and cannot be redefined through
`extra_helm_values`. Existing callers using that escape hatch must move the
same effective values into `request_store` and remove the old block in one
plan; this changes ownership without changing the rendered release contract.

## RWX cutover authority fence

After an existing control plane completes a reviewed migration from its legacy
RWO state volume to static RWX storage, set `rwx_authority_fence`. Leave it null
before that boundary and on installations which have not migrated. The input
has no default digest: it needs the SHA-256 of the exact digest-sealed
`fence.json` bytes emitted by the accepted finalizer, the independently
accepted PostgreSQL-evidence SHA-256, and the exact source, replacement, and
authority object identities.

Fence mode requires `storage.enabled=true`, `storage.accessMode=ReadWriteMany`,
and an explicit nonempty `storage.existingClaim` equal to `state_claim_name`;
the chart-created default claim is not an accepted static migration target. The
effective operations helper (or API-image fallback) must be pinned by an exact
`@sha256` digest because its Python executable enforces the startup gate. It
also requires PostgreSQL plus
`enforce_builtin_execution_quiescence_backends=true`, so the accepted evidence
covers every execution storage and queue backend.

The authority fence must live on a dedicated, statically bound EFS access
point/PV/PVC which is distinct from the writable state access point/PV/PVC.
The module exposes that PVC only to one read-only verifier init container. It
does not add the authority mount to the API, executor, or controller container.
The verifier is composed onto all three role templates and remains enabled in
both one-pod compatibility and split-role HA modes. The pinned chart must
therefore propagate top-level `extraInitContainers` and
`apiService.extraVolumes` to all three role Deployments, as the chart in this
repository does.

```hcl
rwx_authority_fence = {
  authority_claim_name = "skypilot-state-authority"
  state_claim_name     = "skypilot-state-rwx"
  expected_sha256      = "<64 lowercase hex characters>"
  expected_postgres_evidence_sha256 = "<64 lowercase hex characters>"
  identity = {
    source = {
      pvc_name      = "<legacy PVC name>"
      pvc_uid       = "<legacy PVC UID>"
      pv_name       = "<legacy PV name>"
      pv_uid        = "<legacy PV UID>"
      ebs_volume_id = "vol-..."
    }
    target = {
      filesystem_id            = "fs-..."
      state_access_point_id     = "fsap-..."
      state_pv_name             = "<state PV name>"
      state_pv_uid              = "<state PV UID>"
      state_pvc_uid             = "<state PVC UID>"
      authority_access_point_id = "fsap-..."
      authority_pv_name         = "<authority PV name>"
      authority_pv_uid          = "<authority PV UID>"
      authority_pvc_uid         = "<authority PVC UID>"
    }
  }
}
```

The exact schema-v1 fence object has these top-level fields:
`schema_version`, `status`, `identity`, `snapshots`, `manifest`,
`postgres_evidence`, `postgres_evidence_sha256`,
`generation_intent_sha256`, `attempt_generation`, `zero_at`, `work_cutoff`,
`api_ready_deadline`, and `completed_at`. `identity` is the rendered
expected identity above plus the module-owned namespace/release, the two claim
names, and source/state/authority PVC namespace fields. All three PVC
namespaces equal the release namespace because a pod cannot mount a claim from
another namespace. `snapshots` contains the distinct `baseline_source_id`,
`baseline_encrypted_id`, `quiesced_source_id`, and `quiesced_encrypted_id`.
`manifest` contains a lowercase SHA-256, positive entry count, and nonnegative
byte count. The entire fence uses the same sorted-key, compact, `allow_nan=false`,
`ensure_ascii=true` UTF-8 canonical encoding described below, with no trailing
newline or alternate whitespace.

`postgres_evidence` is a sanitized, path-free exact-schema object produced by
one `REPEATABLE READ READ ONLY` validation transaction. It binds the canonical
cutover-marker hash/format/timestamp/counts/logical hash to the observed
database schema revision, current row/logical hashes, and integer-zero queue,
nonterminal, and claimed counts. `postgres_evidence_sha256` is the SHA-256 of
its UTF-8 canonical JSON (`allow_nan=false`, `ensure_ascii=true`, compact
separators, sorted keys), and must also match the independently supplied typed
input. No source path, request ID, or credential is stored in this evidence.
The object's fields are exactly `schema_version` (integer 1), `metadata_key`
(`sqlite-to-postgres-cutover.v1`), `cutover_marker_sha256`,
`cutover_format_version` (integer 1), `cutover_completed_at`,
`cutover_request_count`, `cutover_queue_count`, `cutover_logical_sha256`,
`observed_at`, `database_schema_revision`, `current_request_count`,
`current_queue_count`, `current_nonterminal_count`, `current_claimed_count`,
and `current_logical_sha256`. The current request count cannot be lower than
the historical cutover count; each of the three current work counts must be
the JSON integer `0` (not a boolean).
The status must be `complete` and timestamps must be RFC 3339 UTC. Unknown or
duplicate keys, a writable/symlinked/non-regular/hardlinked file, a digest or
identity mismatch, or a file replacement during verification prevents every
affected pod from starting. `attempt_generation` is a positive integer,
`work_cutoff - zero_at` is exactly 2,700 seconds,
`api_ready_deadline - zero_at` is exactly 7,200 seconds, and `completed_at`
must be strictly inside the API-zero work window. Historical queue count cannot
exceed historical request count, and the timestamps must satisfy
`cutover_completed_at <= zero_at < observed_at < completed_at < work_cutoff`.
When the fence is enabled, nonempty escape-hatch API sidecars and
database/executor/controller extra volume or mount arrays are rejected so no
long-running container can reference the init-only authority volume.

## Provider ownership

As a normal child module, configure providers in the root:

```hcl
data "aws_eks_cluster" "host" {
  name = var.host_cluster_name
}

provider "kubernetes" {
  host                   = data.aws_eks_cluster.host.endpoint
  cluster_ca_certificate = base64decode(data.aws_eks_cluster.host.certificate_authority[0].data)

  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args        = ["eks", "get-token", "--cluster-name", var.host_cluster_name, "--region", var.aws_region]
  }
}

provider "helm" {
  kubernetes = {
    host                   = data.aws_eks_cluster.host.endpoint
    cluster_ca_certificate = base64decode(data.aws_eks_cluster.host.certificate_authority[0].data)
    exec = {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "aws"
      args        = ["eks", "get-token", "--cluster-name", var.host_cluster_name, "--region", var.aws_region]
    }
  }
}

module "skypilot_control_plane" {
  source = "git::https://github.com/boltz-bio/skypilot.git//infra/terraform/modules/skypilot-control-plane?ref=<full-commit-sha>"

  providers = {
    aws        = aws
    kubernetes = kubernetes
    helm       = helm
    time       = time
  }

  aws_account_id                  = "123456789012"
  aws_region                      = "us-east-1"
  host_cluster_name               = "platform-eks"
  chart_version                   = "1.1.0"
  db_connection_secret_name       = "skypilot-postgres"
  request_store = {
    backend                                        = "postgres"
    enforce_builtin_execution_quiescence_backends = true
  }
  operations_helper_image         = "registry.example/skypilot-ops@sha256:<digest>"
  oauth_enabled                   = false
}
```

Terragrunt may instead download this directory as its root source and generate
the provider blocks beside it. `chart_registry_login_url` exists for that
compatibility flow; private OCI authentication belongs to the root Helm
provider. A short-lived ECR authorization data source used there is sensitive
but is still represented in Terraform state.

## Configuration ownership

The Helm chart forbids inline API config when an external database is selected.
The module therefore runs a Kubernetes Job that locks and updates the
`api_server_config` row in `config_yaml`:

- IaC mappings recursively override persisted mappings.
- Lists and scalars replace.
- `workspaces` is replaced wholesale.
- Runtime-only keys outside the desired tree survive.
- `prune_retired_serve_controller_keys` performs an opt-in, one-way removal.

Any script, desired configuration, pruning-mode, or helper-image change creates
a new immutable seed Job. Only script, configuration, or pruning-mode changes
advance `config_generation`; after a successful seed, a bounded local-exec
restarts and waits for `<release_name>-api-server` in compatibility mode. When
`apiService.highAvailability.enabled=true`, it restarts all three split-role
Deployments (`api-server`, `executor`, and `controller`) and waits for each
within the same 600-second per-Deployment budget. Helper-image-only changes
therefore update the Job and any enabled login init containers without causing
a second runtime rollout after Helm has already rolled the pod templates.
The reconciler issues all selected restarts before waiting, and Helm may also
roll the three Deployments concurrently. HA rollout preflight must therefore
prove aggregate cluster headroom for one temporary surge pod per role (up to
three temporary pods), not one surge pod total.

The module owns the chart workload names as well as their cloud identities.
Set `release_name` to choose that name; `extra_helm_values.fullnameOverride` is
rejected so the rendered service account and Deployments cannot diverge from
the identities and post-seed reconciliation targets managed by Terraform.

## Security and lifecycle notes

- The API role can describe EKS clusters and EC2 resources. Broader authority is
  opt-in through `extra_policy_json`.
- `permissions_boundary_arn` must belong to the active account and partition.
- Supported secret integrations accept names/identifiers, not secret payloads.
- Ingress defaults to internal and public schemes require
  `allow_public_ingress = true`, including annotations supplied through
  `extra_helm_values`.
- Changing the cluster, release, namespace, or role name can replace resources.
  Review plans before adoption or rename.
- Private catalog, Nebius, Azure, and OIDC ExternalSecrets have fixed
  SkyPilot-specific object names within the module namespace.

## Module reference

<!-- BEGIN_TF_DOCS -->
## Requirements

| Name | Version |
|------|---------|
| <a name="requirement_terraform"></a> [terraform](#requirement\_terraform) | >= 1.5.0 |
| <a name="requirement_aws"></a> [aws](#requirement\_aws) | >= 6.24.0 |
| <a name="requirement_helm"></a> [helm](#requirement\_helm) | >= 3.0 |
| <a name="requirement_kubernetes"></a> [kubernetes](#requirement\_kubernetes) | >= 2.20 |
| <a name="requirement_time"></a> [time](#requirement\_time) | >= 0.9 |

## Providers

| Name | Version |
|------|---------|
| <a name="provider_aws"></a> [aws](#provider\_aws) | >= 6.24.0 |
| <a name="provider_helm"></a> [helm](#provider\_helm) | >= 3.0 |
| <a name="provider_kubernetes"></a> [kubernetes](#provider\_kubernetes) | >= 2.20 |
| <a name="provider_terraform"></a> [terraform](#provider\_terraform) | n/a |
| <a name="provider_time"></a> [time](#provider\_time) | >= 0.9 |

## Modules

No modules.

## Resources

| Name | Type |
|------|------|
| [aws_eks_pod_identity_association.api_server](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/eks_pod_identity_association) | resource |
| [aws_iam_role.api_server](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role) | resource |
| [aws_iam_role_policy.api_server](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role_policy) | resource |
| [aws_iam_role_policy.api_server_extra](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role_policy) | resource |
| [aws_iam_role_policy.eso_read_oauth_secret](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role_policy) | resource |
| [helm_release.skypilot](https://registry.terraform.io/providers/hashicorp/helm/latest/docs/resources/release) | resource |
| [kubernetes_config_map_v1.seed_config](https://registry.terraform.io/providers/hashicorp/kubernetes/latest/docs/resources/config_map_v1) | resource |
| [kubernetes_job_v1.seed_config](https://registry.terraform.io/providers/hashicorp/kubernetes/latest/docs/resources/job_v1) | resource |
| [kubernetes_manifest.azure_credentials_external_secret](https://registry.terraform.io/providers/hashicorp/kubernetes/latest/docs/resources/manifest) | resource |
| [kubernetes_manifest.catalog_token_external_secret](https://registry.terraform.io/providers/hashicorp/kubernetes/latest/docs/resources/manifest) | resource |
| [kubernetes_manifest.nebius_credentials_external_secret](https://registry.terraform.io/providers/hashicorp/kubernetes/latest/docs/resources/manifest) | resource |
| [kubernetes_manifest.oauth_external_secret](https://registry.terraform.io/providers/hashicorp/kubernetes/latest/docs/resources/manifest) | resource |
| [kubernetes_namespace_v1.skypilot](https://registry.terraform.io/providers/hashicorp/kubernetes/latest/docs/resources/namespace_v1) | resource |
| [terraform_data.reconcile_api_server](https://registry.terraform.io/providers/hashicorp/terraform/latest/docs/resources/data) | resource |
| [time_sleep.wait_oauth_secret](https://registry.terraform.io/providers/hashicorp/time/latest/docs/resources/sleep) | resource |
| [aws_caller_identity.current](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/caller_identity) | data source |
| [aws_eks_cluster.host](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/eks_cluster) | data source |
| [aws_iam_policy_document.api_server](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |
| [aws_partition.current](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/partition) | data source |
| [aws_region.current](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/region) | data source |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_allow_public_ingress"></a> [allow\_public\_ingress](#input\_allow\_public\_ingress) | Safety guard: the module refuses an internet-facing LB scheme unless this is true. Keep false in production — the endpoint should be reachable only over the VPN. | `bool` | `false` | no |
| <a name="input_allowed_clouds"></a> [allowed\_clouds](#input\_allowed\_clouds) | Clouds the API server is allowed to use (config allowed\_clouds). | `list(string)` | <pre>[<br/>  "aws",<br/>  "kubernetes"<br/>]</pre> | no |
| <a name="input_allowed_contexts"></a> [allowed\_contexts](#input\_allowed\_contexts) | Kubernetes contexts SkyPilot may schedule onto (kubernetes.allowed\_contexts), in failover order. | `list(string)` | `[]` | no |
| <a name="input_api_server_extra_envs"></a> [api\_server\_extra\_envs](#input\_api\_server\_extra\_envs) | Non-secret extra environment variables set on the API-server pod. Values are<br/>stored in Terraform state; use Kubernetes Secrets for credentials. | `list(object({ name = string, value = string }))` | `[]` | no |
| <a name="input_api_server_image"></a> [api\_server\_image](#input\_api\_server\_image) | Optional exact API-server image override. When operations\_helper\_image is<br/>null, this image is also used by the config seed and enabled cloud-login<br/>helpers. Leave null to retain the chart's API-server image default, but then<br/>operations\_helper\_image is required. | `string` | `null` | no |
| <a name="input_api_server_role_name"></a> [api\_server\_role\_name](#input\_api\_server\_role\_name) | Optional API-server IAM role name. Null preserves the derived skypilot-api-<host\_cluster\_name> name. | `string` | `null` | no |
| <a name="input_aws_account_id"></a> [aws\_account\_id](#input\_aws\_account\_id) | AWS account ID that owns the host cluster (used for provider guardrails). | `string` | n/a | yes |
| <a name="input_aws_credentials_secret_name"></a> [aws\_credentials\_secret\_name](#input\_aws\_credentials\_secret\_name) | Name of a pre-created secret holding the API server's ~/.aws/config (the<br/>cross-account VM-pool assume-role profile). When set, the chart mounts it<br/>read-only at /root/.aws (awsCredentials.useCredentialsFile) and the module<br/>overlays a writable emptyDir at /root/.aws/cli so the AWS CLI (used for SSM)<br/>can write its assume-role cache. Null = no AWS credentials file mounted. | `string` | `null` | no |
| <a name="input_aws_profile"></a> [aws\_profile](#input\_aws\_profile) | Optional AWS profile passed to `aws eks get-token` when the provider reaches the host cluster. Null = ambient credentials (the terragrunt-assumed role). | `string` | `null` | no |
| <a name="input_aws_region"></a> [aws\_region](#input\_aws\_region) | AWS region of the host cluster. | `string` | n/a | yes |
| <a name="input_azure_credentials_secretsmanager_key"></a> [azure\_credentials\_secretsmanager\_key](#input\_azure\_credentials\_secretsmanager\_key) | Secrets Manager key holding the Azure service-principal bundle (JSON with<br/>properties `client_id`, `client_secret`, `tenant_id`, `subscription_id`).<br/>Unlike AWS/Nebius (credential files), SkyPilot's Azure adaptor authenticates<br/>off the `az` CLI profile: it requires `az --version`, a populated<br/>`~/.azure/azureProfile.json`, and `~/.azure/msal_token_cache.json` to exist.<br/>So instead of mounting files we ESO-materialize the SP into the<br/>`skypilot-azure-credentials` Secret and run an `az login --service-principal`<br/>init container that writes the CLI profile into a shared /root/.azure<br/>emptyDir (mirrors the keyless GCP `gcloud auth login` init container). The<br/>emptyDir also retains the SP secret entry, so the long-running server<br/>refreshes its own tokens. Null disables Azure. | `string` | `null` | no |
| <a name="input_catalog_mirror"></a> [catalog\_mirror](#input\_catalog\_mirror) | Optional self-hosted SkyPilot catalog mirror. `url` becomes<br/>SKYPILOT\_HOSTED\_CATALOG\_DIR\_URL. When token\_secretsmanager\_key is set, ESO<br/>materializes it into the `skypilot-catalog-token` Secret and injects<br/>SKYPILOT\_HOSTED\_CATALOG\_TOKEN. | <pre>object({<br/>    url                      = string<br/>    token_secretsmanager_key = optional(string)<br/>  })</pre> | `null` | no |
| <a name="input_chart_name"></a> [chart\_name](#input\_chart\_name) | Chart name within chart\_repository. | `string` | `"skypilot-nightly"` | no |
| <a name="input_chart_registry_login_url"></a> [chart\_registry\_login\_url](#input\_chart\_registry\_login\_url) | Private OCI registry login URL. This compatibility input is consumed by<br/>Terragrunt root-provider generation; ordinary child-module callers configure<br/>the Helm provider themselves. Leave null for a public HTTPS chart. | `string` | `null` | no |
| <a name="input_chart_repository"></a> [chart\_repository](#input\_chart\_repository) | Helm chart repository. Defaults to the public SkyPilot chart. Private OCI<br/>repositories require matching credentials on the root Helm provider. | `string` | `"https://helm.skypilot.co"` | no |
| <a name="input_chart_version"></a> [chart\_version](#input\_chart\_version) | Version of the SkyPilot Helm chart. Pin to a known-good version; a null value<br/>hard-fails the plan. Version ranges and wildcard selectors are rejected. | `string` | `null` | no |
| <a name="input_config_extra"></a> [config\_extra](#input\_config\_extra) | Non-secret top-level keys merged into the DB-backed API-server config.<br/>Mappings deep-merge, lists/scalars replace, and workspaces is replaced<br/>wholesale. The Kubernetes block is merged with allowed\_contexts. This value<br/>enters Terraform state and a ConfigMap; never put credentials here. | `any` | `{}` | no |
| <a name="input_db_connection_secret_name"></a> [db\_connection\_secret\_name](#input\_db\_connection\_secret\_name) | Name of a Kubernetes secret (key `connection_string`) with an external<br/>PostgreSQL connection string (apiService.dbConnectionSecretName). REQUIRED —<br/>the control plane is DB-only: HA / zero-downtime RollingUpdate upgrades and a<br/>durable cross-cloud user+job ledger. The chart then requires apiService.config<br/>to be null, so the whole SkyPilot config is seeded into the DB by the<br/>config-seeding Job (config\_seed.tf) rather than rendered inline. | `string` | n/a | yes |
| <a name="input_eso_secrets_reader_role_name"></a> [eso\_secrets\_reader\_role\_name](#input\_eso\_secrets\_reader\_role\_name) | Name of the host cluster's shared ESO controller IAM role to grant read access to<br/>this module's Secrets Manager secret(s). ESO authenticates ClusterSecretStore reads<br/>with its own (ambient Pod Identity) identity, so it must be allowed to read SkyPilot's<br/>secrets — this module attaches that grant itself, keeping the host module unaware of<br/>SkyPilot. Null = don't manage the grant (e.g. the secret is pre-created out of band). | `string` | `null` | no |
| <a name="input_extra_helm_values"></a> [extra\_helm\_values](#input\_extra\_helm\_values) | Non-secret escape hatch: extra Helm values merged last. This value is stored in Terraform state; never place credentials here. | `string` | `""` | no |
| <a name="input_extra_policy_json"></a> [extra\_policy\_json](#input\_extra\_policy\_json) | Optional additional IAM policy JSON to attach to the API server role (e.g. broader EC2 perms for VM-based pools). | `string` | `null` | no |
| <a name="input_gcp_provisioner"></a> [gcp\_provisioner](#input\_gcp\_provisioner) | Keyless GCP VM provisioner wiring (Workload Identity Federation). Null disables it. The login init container uses operations\_helper\_image. | <pre>object({<br/>    project          = string<br/>    cred_secret_name = string<br/>    wif_audience     = string<br/>  })</pre> | `null` | no |
| <a name="input_host_cluster_name"></a> [host\_cluster\_name](#input\_host\_cluster\_name) | Name of the EXISTING EKS cluster the SkyPilot API server is deployed onto.<br/>The caller's AWS identity must already have kubectl/EKS access. The root<br/>caller owns Kubernetes and Helm provider configuration. | `string` | n/a | yes |
| <a name="input_include_host_cluster_as_pool"></a> [include\_host\_cluster\_as\_pool](#input\_include\_host\_cluster\_as\_pool) | Expose the host cluster itself as a SkyPilot pool (kubernetesCredentials.useApiServerCluster). | `bool` | `false` | no |
| <a name="input_ingress_annotations"></a> [ingress\_annotations](#input\_ingress\_annotations) | Annotations for the ingress Service/Ingress. Defaults to an INTERNAL load<br/>balancer so the API/SSO surface is reachable only over the VPN, never the<br/>public internet. Override to add external-dns hostnames etc., but keep an<br/>internal scheme. | `map(string)` | <pre>{<br/>  "service.beta.kubernetes.io/aws-load-balancer-scheme": "internal"<br/>}</pre> | no |
| <a name="input_ingress_class_name"></a> [ingress\_class\_name](#input\_ingress\_class\_name) | IngressClass name used by BOTH SkyPilot's Ingress and the bundled controller.<br/>MUST be unique on a shared cluster (never plain "nginx") so this controller<br/>never collides with or hijacks other teams' ingresses. | `string` | `"skypilot-nginx"` | no |
| <a name="input_ingress_enabled"></a> [ingress\_enabled](#input\_ingress\_enabled) | Create SkyPilot's own Ingress object (routes to the API server). | `bool` | `true` | no |
| <a name="input_install_bundled_ingress_nginx"></a> [install\_bundled\_ingress\_nginx](#input\_install\_bundled\_ingress\_nginx) | Install SkyPilot's bundled ingress-nginx controller. The module ships it in an<br/>ISOLATED posture on a shared cluster: a unique IngressClass (var.ingress\_class\_name)<br/>used on both the controller and SkyPilot's Ingress, the controller scoped to<br/>watch only its own namespace, and the cluster-wide admission webhook DISABLED —<br/>so it cannot adopt or reject ingresses belonging to other teams on the prod<br/>cluster. Set false to skip the bundled controller and front the API server with<br/>the platform's existing ingress instead (wire that via extra\_helm\_values). | `bool` | `true` | no |
| <a name="input_kubeconfig_secret_name"></a> [kubeconfig\_secret\_name](#input\_kubeconfig\_secret\_name) | Name of a pre-created Kubernetes secret in `namespace` holding a kubeconfig<br/>whose contexts point at external pools. Consumed via<br/>kubernetesCredentials.useKubeconfig + kubeconfigSecretName.<br/>The kubeconfig should authenticate via `aws eks get-token` using the API<br/>server's Pod Identity role. Null = host cluster only. | `string` | `null` | no |
| <a name="input_manage_namespace"></a> [manage\_namespace](#input\_manage\_namespace) | Create the SkyPilot namespace in Terraform. Set FALSE when the operator<br/>pre-creates the namespace and the OAuth/pool/DB secrets in it out of band:<br/>the Helm release consumes those secrets, so on first apply they must already<br/>exist — which is impossible if the same apply is also creating the namespace.<br/>Recommended prod flow: create ns + secrets first, then apply with<br/>manage\_namespace=false. (With true, bootstrap the namespace via a targeted<br/>apply before creating secrets — see the README.) | `bool` | `true` | no |
| <a name="input_namespace"></a> [namespace](#input\_namespace) | Namespace for the SkyPilot API server. | `string` | `"skypilot"` | no |
| <a name="input_nebius_credentials_secretsmanager_key"></a> [nebius\_credentials\_secretsmanager\_key](#input\_nebius\_credentials\_secretsmanager\_key) | Secrets Manager key holding the Nebius service-account bundle (JSON with<br/>properties `credentials_json` — the SDK authorized-key file content — and<br/>`tenant_id`). ESO-materializes the `skypilot-nebius-credentials` Secret,<br/>mounted at /root/.nebius as credentials.json + NEBIUS\_TENANT\_ID.txt, which<br/>is exactly where the SkyPilot Nebius adaptor looks. Null disables Nebius. | `string` | `null` | no |
| <a name="input_oauth_client_secret_name"></a> [oauth\_client\_secret\_name](#input\_oauth\_client\_secret\_name) | Name of a pre-created Kubernetes secret in `namespace` holding the OIDC<br/>client id/secret (consumed by the chart as<br/>auth.oauth.client-details-from-secret). Create it out of band so the client<br/>secret never lands in Terraform state. Null disables OIDC wiring. | `string` | `null` | no |
| <a name="input_oauth_cluster_secret_store"></a> [oauth\_cluster\_secret\_store](#input\_oauth\_cluster\_secret\_store) | ESO ClusterSecretStore name used by every module-managed ExternalSecret. | `string` | `"aws-secrets-manager"` | no |
| <a name="input_oauth_enabled"></a> [oauth\_enabled](#input\_oauth\_enabled) | Enable OIDC/SSO auth on the API server ingress. Required for RBAC; the default basic-auth path does not support roles. | `bool` | `true` | no |
| <a name="input_oauth_secretsmanager_key"></a> [oauth\_secretsmanager\_key](#input\_oauth\_secretsmanager\_key) | AWS Secrets Manager secret name holding the OIDC client (JSON with client\_id and client\_secret). ESO materializes it into oauth\_client\_secret\_name. | `string` | `null` | no |
| <a name="input_oidc_issuer_url"></a> [oidc\_issuer\_url](#input\_oidc\_issuer\_url) | OIDC issuer URL. | `string` | `"https://accounts.google.com"` | no |
| <a name="input_operations_helper_image"></a> [operations\_helper\_image](#input\_operations\_helper\_image) | Exact image used for PostgreSQL config seeding and optional GCP/Azure login<br/>initialization. It must contain Python, SQLAlchemy, PyYAML, and the<br/>PostgreSQL driver; enabled cloud logins additionally require Bash and the<br/>corresponding gcloud or az CLI. Defaults to api\_server\_image. | `string` | `null` | no |
| <a name="input_permissions_boundary_arn"></a> [permissions\_boundary\_arn](#input\_permissions\_boundary\_arn) | Optional organization-managed permissions boundary attached to the API-server IAM role. | `string` | `null` | no |
| <a name="input_prune_retired_serve_controller_keys"></a> [prune\_retired\_serve\_controller\_keys](#input\_prune\_retired\_serve\_controller\_keys) | Remove the retired serve.controller.consolidation\_mode and<br/>serve.controller.external\_load\_balancer keys from the DB-backed config during<br/>seeding. This is a one-way cutover aid and is disabled by default so public-chart<br/>consumers retain their existing config behavior. | `bool` | `false` | no |
| <a name="input_rbac_default_role"></a> [rbac\_default\_role](#input\_rbac\_default\_role) | Default role for newly auto-provisioned SSO users. SkyPilot ships this as<br/>`admin` to ease setup; we default to `user` for least privilege. NOTE: verify<br/>it actually takes effect on your chart version (see skypilot issue #9271). | `string` | `"user"` | no |
| <a name="input_release_name"></a> [release\_name](#input\_release\_name) | Helm release name. The chart derives the API service account as <release\_name>-api-sa. | `string` | `"skypilot"` | no |
| <a name="input_request_store"></a> [request\_store](#input\_request\_store) | API request-envelope persistence settings rendered as the chart's<br/>requestStore values. The SQLite defaults preserve the chart's compatibility<br/>behavior; select PostgreSQL explicitly only after completing the chart's<br/>one-way request-store cutover procedure. Enabling built-in execution<br/>quiescence enforcement requires the PostgreSQL backend. | <pre>object({<br/>    backend                                        = optional(string, "sqlite")<br/>    enforce_builtin_execution_quiescence_backends = optional(bool, false)<br/>    cutover_gate_path                              = optional(string, "/root/.sky/api-request-cutover.json")<br/>  })</pre> | `{}` | no |
| <a name="input_rwx_authority_fence"></a> [rwx\_authority\_fence](#input\_rwx\_authority\_fence) | Optional steady-state verifier for a completed migration to static RWX storage. The object binds a dedicated authority PVC, exact fence and PostgreSQL-evidence SHA-256 values, and exact source/state/authority identities. | <pre>object({<br/>    authority_claim_name = string<br/>    state_claim_name     = string<br/>    expected_sha256      = string<br/>    expected_postgres_evidence_sha256 = string<br/>    identity = object({<br/>      source = object({<br/>        pvc_name = string<br/>        pvc_uid = string<br/>        pv_name = string<br/>        pv_uid = string<br/>        ebs_volume_id = string<br/>      })<br/>      target = object({<br/>        filesystem_id = string<br/>        state_access_point_id = string<br/>        state_pv_name = string<br/>        state_pv_uid = string<br/>        state_pvc_uid = string<br/>        authority_access_point_id = string<br/>        authority_pv_name = string<br/>        authority_pv_uid = string<br/>        authority_pvc_uid = string<br/>      })<br/>    })<br/>  })</pre> | `null` | no |
| <a name="input_tags"></a> [tags](#input\_tags) | Tags applied to AWS resources created by this module. | `map(string)` | `{}` | no |
| <a name="input_workspace_email_domain"></a> [workspace\_email\_domain](#input\_workspace\_email\_domain) | Restrict logins to this email domain (auth.oauth.email-domain). Null relies on the OIDC client's audience restriction. | `string` | `null` | no |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_api_server_oidc_issuer"></a> [api\_server\_oidc\_issuer](#output\_api\_server\_oidc\_issuer) | OIDC issuer URL of the host EKS cluster the API server runs on. Feed this into a<br/>GCP VM pool's `controller_oidc_issuer` so it can federate the API server's<br/>projected Kubernetes ServiceAccount token. Pair it with the API server<br/>subject `system:serviceaccount:<namespace>:<api_service_account_name>`. |
| <a name="output_api_server_role_arn"></a> [api\_server\_role\_arn](#output\_api\_server\_role\_arn) | Role ARN of the SkyPilot API server service account. Feed this into a pool<br/>module's `controller_role_arn` so the pool grants it an EKS access entry or<br/>cross-account AssumeRole. |
| <a name="output_api_server_role_name"></a> [api\_server\_role\_name](#output\_api\_server\_role\_name) | Name of the API-server EKS Pod Identity role. |
| <a name="output_api_service_account_name"></a> [api\_service\_account\_name](#output\_api\_service\_account\_name) | Kubernetes service account name the API server runs as. |
| <a name="output_config_generation"></a> [config\_generation](#output\_config\_generation) | Hash identifying the desired DB-backed config seed generation. |
| <a name="output_host_cluster_provider_config"></a> [host\_cluster\_provider\_config](#output\_host\_cluster\_provider\_config) | EKS endpoint, CA data, and AWS exec arguments used by root Kubernetes and Helm providers. |
| <a name="output_namespace"></a> [namespace](#output\_namespace) | Namespace the SkyPilot API server is deployed into. |
| <a name="output_release_name"></a> [release\_name](#output\_release\_name) | Helm release name. |
<!-- END_TF_DOCS -->
