# Reusable SkyPilot Terraform modules

- **Status:** Review-28 correction independently accepted; implementation and
  production handoff remain pending
- **Last updated:** 2026-08-08
- **Authoritative repository:** `boltz-bio/skypilot`, branch `improvements`

## Context

SkyPilot deployment Terraform currently lives in `boltz-bio/boltz-platform`.
That makes generally useful control-plane and spoke-workspace-pool building
blocks depend on an application repository, encourages consumers to copy them,
and prevents a SkyPilot release from carrying the infrastructure contract needed
to deploy it. The reusable package owns supporting infrastructure only. The
SkyPilot control-plane application is deployed by a human operator directly
through Helm; it is never owned or rolled out by this Terraform module.

The modules move to SkyPilot's existing `infra/terraform/modules` tree. The
first consumer remains `boltz-platform`, but the module APIs and documentation
must not encode deployment-specific Boltz accounts, hostnames, images,
repositories, workspaces, or environment topology. The canonical
`boltz-bio/skypilot` distribution URL and repository-authority instruction are
intentional exceptions.

For this fork, `boltz-bio/skypilot:improvements` is the sole source of truth.
Modules consumed across repositories are pinned to an immutable commit from
that branch. Upstream `skypilot-org/skypilot` is not an implementation,
comparison, or history source unless a user explicitly requests it.

## Goals

- Publish the four currently deployed modules from stable, reusable paths:
  - `infra/terraform/modules/skypilot-control-plane`
  - `infra/terraform/modules/skypilot-spoke-workspace-pool-aws-vm`
  - `infra/terraform/modules/skypilot-spoke-workspace-pool-eks`
  - `infra/terraform/modules/skypilot-spoke-workspace-pool-rbac`
- Preserve the existing `boltz-platform` Terraform infrastructure resource
  addresses and effective provider configuration and defaults while changing
  the module package source, except for the four intentional state-only
  removals defined below.
- Hand the existing Helm release and configuration seed/reconcile path from
  Terraform state to direct human Helm operation without changing or deleting
  the live release or legacy seed objects during the state operation, and make
  that separation permanent.
- Retain the control-plane's supporting IAM, EKS Pod Identity, optional
  External Secrets Operator resources, and optional namespace. Move the stable
  digest-pinned PostgreSQL configuration seed and role reload into the
  revision-scoped direct-Helm application bundle.
- Remove deployment-specific examples and assumptions from module code and
  documentation.
- Make IAM roles compatible with organization-managed permissions boundaries
  and AWS partitions without changing the commercial-AWS default behavior.
- Make the published modules provider-neutral child modules; provider
  configuration belongs to the root caller.
- Document prerequisites, inputs, outputs, upgrade behavior, rollback, and an
  immutable Git source example for every module.
- Exercise every published module with formatting, initialization, validation,
  and mocked Terraform contract tests in CI.

## Non-goals

- Provision an EKS cluster, VPC, PostgreSQL database, External Secrets Operator,
  ingress controller outside the chart, or cloud credentials.
- Install, upgrade, roll back, or otherwise own the SkyPilot Helm release.
- Seed the SkyPilot database, own a seed ConfigMap/Job, or restart application
  Deployments from Terraform.
- Publish these modules through the Terraform Registry.
- Introduce a Terragrunt API in the SkyPilot repository.
- Generalize the EKS-specific control-plane module to arbitrary Kubernetes
  distributions.
- Let pull-request automation change live infrastructure or run
  `terraform apply`; the only state transition in scope is the separately
  reviewed, human-applied forget of the four exact root state addresses below,
  each with `destroy = false`.
- Generalize state migration machinery or rename any other existing Terraform
  resource label.
- Move the separate GCP VM-pool module from `boltz-platform`; it is not part of
  the four-module AWS/Kubernetes package being transferred.

## Public contract

### Package and source contract

The module directories above are the public package paths. “Spoke workspace
pool” is package terminology for spoke-side infrastructure where workspace
workloads execute. It does not imply a one-to-one binding: one pool can serve
multiple workspaces, and one workspace can target more than one pool. Existing
Terraform labels, public inputs, persisted names, and SkyPilot `pool` vocabulary
remain unchanged. “Spoke” describes the logical workload side of the topology;
these modules do not require a physically separate account or cluster. A spoke
workspace pool is infrastructure packaging and is unrelated to a managed Job
Pool operated through `sky jobs pool`.

This package-directory rename happens before the draft module stack merges or
becomes a supported consumer contract. It does not rename module/resource labels
or create Terraform `moved` blocks. Existing immutable commits that expose the
earlier paths remain valid for callers pinned to those commits; the draft
`boltz-platform` migration is repinned atomically to a commit containing the new
paths. No moving-branch source or compatibility symlink is introduced.

Cross-repository callers use an immutable Git source:

```hcl
source = "git::https://github.com/boltz-bio/skypilot.git//infra/terraform/modules/skypilot-spoke-workspace-pool-aws-vm?ref=<full-commit-sha>"
```

The EKS pool module's relative
`../skypilot-spoke-workspace-pool-rbac` dependency is part of the package
contract. Callers must use Git's `//subdirectory` syntax so Terraform downloads
the repository package and the sibling module remains available.

Moving a caller between a local and remote source does not change Terraform
addresses. Resource and nested-module labels therefore remain unchanged in
this release. No `moved` blocks are required for the `boltz-platform`
migration.

The published modules contain provider requirements but no provider
configuration blocks. A normal Terraform caller inherits or explicitly passes
`aws` and/or `kubernetes` providers as each module requires. The control-plane
module has no Helm or Time provider requirement after the ownership handoff; the
former Time sleep existed only to order Terraform's Helm installation after an
ESO Secret. A Terragrunt caller generates the root provider configuration next
to the downloaded module. The `boltz-platform` migration moves the existing EKS
exec-auth configuration into generated root files while retaining the same AWS
and Kubernetes provider addresses.

The supported consumer language floor is Terraform or OpenTofu `>= 1.7.0`,
because the permanent control-plane-module-root `removed` blocks are part of
the safety contract. SkyPilot CI tests Terraform 1.14.8. The first consumer
additionally tests OpenTofu 1.11.5 through Terragrunt. Provider floors remain AWS
`>= 6.24.0` and Kubernetes `>= 2.20`;
the three committed consumer lock files retain their exact selected versions
and hashes. Historical unused Helm and Time selections may remain locked during
the no-mutation handoff, but neither is a required provider. Helm is an operator
CLI, not a Terraform provider, in the revised contract.

### `skypilot-control-plane`

This module prepares supporting infrastructure for a SkyPilot control plane on
an existing EKS cluster. It creates the namespace when requested, the API-server
EKS Pod Identity role and association, and optional External Secrets resources
and read policy. It does not install, read, update, or delete the Helm release;
mutate the PostgreSQL database; create seed Jobs or ConfigMaps; or restart and
wait for application Deployments. A human Helm operator owns every application
deployment and configuration reconciliation after the one-time state handoff.

The caller supplies:

- the existing EKS cluster, AWS region and account;
- the existing host-cluster and release identity used to name supporting IAM,
  Pod Identity, namespace, and Secret objects;
- optional ESO source-secret identifiers, target Secret names, and the named
  ClusterSecretStore; and
- any reusable IAM boundary and supporting-infrastructure options.

PostgreSQL connection Secret, chart version, API-server and operations-helper
image, database-backed SkyPilot configuration, pruning mode, OAuth and cloud
login wiring, ingress, request-store, role topology, readiness budgets, and
additional chart values are direct-Helm inputs. They are not module variables,
outputs, locals, data sources, or Terraform state. An ordinary application or
configuration rollout therefore requires no `boltz-platform` change or
Terraform plan.

Supported infrastructure secret integrations accept Kubernetes secret names or
cloud secret identifiers rather than secret payloads. Direct Helm operators
follow the chart's Secret-reference contract. Terraform does not ingest the
database URI, chart registry authorization, application configuration, or
other application credential payloads.

The host cluster needs the EKS Pod Identity agent. Enabled External Secret
features require the ESO CRDs and named ClusterSecretStore. Module-managed
namespaced objects explicitly depend on the optional module-managed namespace.
Direct Helm preflight, not a Terraform Time sleep, verifies that referenced
Secrets are materialized before chart rendering or upgrade.

`aws_account_id` is an expected-account guard and is checked against the active
AWS identity; the region is likewise checked against the active provider.
The API-server role name is configurable and defaults to the current
`skypilot-api-${host_cluster_name}` value. The resulting name is validated
against IAM's 64-character limit.

#### Direct-Helm configuration reconciliation

Removing Terraform's seed objects is allowed only because the direct-Helm
chart/release bundle replaces their behavior. The chart takes a dedicated
operations-helper image pinned by `@sha256:<64-lowercase-hex>`; there is no
fallback to the API-server image. Every configuration operation has an
operation ID and a full seed-generation SHA-256 over the helper digest, seed
script, desired configuration, pruning policy, database contract, and role
topology.

Configuration reconciliation has three separate, ordered Helm phases. The
existing database migration is a weighted `pre-install,pre-upgrade` Job. After
it succeeds, a later-weighted revision-and-generation-scoped seed Job commits
and reads back the database generation. The digest-pinned operations image
contains the seed code; a secret-free canonical-JSON desired configuration is
embedded directly in the Job manifest and is rejected above 262,144 bytes.
There is no new seed-input hook ConfigMap: Helm does not manage hook resources,
and deleting a successful ConfigMap hook can race the later Job that consumes
it. The Job never adopts or overwrites the legacy Terraform-created
`skypilot-seed-config` ConfigMap or completed Job.

The seed transaction preserves the prior behavior contract:

- recursively deep-merge mappings with deployment-supplied keys winning;
- replace lists and scalars rather than appending them;
- preserve persisted runtime-only keys absent from the deployment input;
- replace `workspaces` wholesale when deployment input supplies it; and
- prune retired keys only through an explicit, reviewed, one-way pruning list.

The transaction serializes concurrent seeders, records the generation, and is
idempotent: retrying the same operation or a later Helm revision with the same
generation produces the same database value and no duplicate side effect. It
fails before mutation on a missing schema, invalid stored/configured shape,
helper mismatch, or migration failure. A readback hash after commit must equal
the deterministic expected merge result.

The first production takeover, H0, has an additional fail-closed
`configReconciliation.handoffGuard`. Its reviewed bundle sets both
`expectedRawConfigSha256`, over the exact live
`config_yaml.api_server_config` text bytes, and `expectedConfigSha256`, over
canonical JSON decoded from that row. Both are 64-character lowercase hashes
and are included in the seed generation. `requiredPaths` is a string array
containing `/gcp/vpc_name`, `/aws/ingress_source_ranges`, and
`/kubernetes/allowed_contexts` plus an exact, exclusive enumeration of every
live workspace. For workspace name `N`, its RFC 6901 component `E(N)` escapes
`~` as `~0` and `/` as `~1`. A disabled workspace
contributes only `/workspaces/E(N)/kubernetes/disabled`, whose value must be the
JSON Boolean `true`; an enabled workspace contributes exactly
`/workspaces/E(N)/kubernetes/namespace` and
`/workspaces/E(N)/kubernetes/allowed_contexts`. Missing, extra, mixed, or
incomplete workspace coverage fails closed. Both row hashes bind the exact
pointed values, and every pointer must resolve. Before mutation, the seed Job
requires both digests and every pointed value to match its captured evidence.
It also
requires the deterministic merge to be a complete configuration no-op. It
writes only the separate `api_server_config_seed_generation` row, then proves
the raw configuration-row bytes, canonical digest, and required values are
unchanged after commit. The post-upgrade verifier repeats both raw and
canonical digests plus the required-path checks in a new read-only transaction.
H0 therefore proves that taking over
reconciliation did not alter the security-load-bearing configuration; a new
generation marker alone is insufficient evidence.

The transition supports only the built-in config schema. Every enabled bundle
sets literal `configReconciliation.pluginsUnsupported: true`; that attestation
is part of the generation and is allowed only after proving Rainier has no API
config plugins. The helper rejects a nonempty stored top-level `plugins` value
and applies strict built-in unknown-field validation to the complete row. It
never loads or installs plugins, and any plugin-provided config field fails
closed rather than being preserved without validation.

Setting only one hash or paths without both hashes is invalid. Helm persists the
armed guard through the ownership handoff and later no-pod-change H2/H0-C
operations must retain it. It may be explicitly cleared only by a reviewed
application operation after the state handoff whose contract already permits a
generation rollout. That operation then uses the ordinary deterministic merge
contract. A stale H0 guard inherited through `--reuse-values` fails closed
instead of silently permitting configuration drift or causing an unreviewed
rollout merely to clear itself.

After the pre-upgrade seed commits, regular rendered Deployments carry the
generation annotation and Helm applies them under bounded `--wait`. Compatibility
topology rolls the single all-role `${release_name}-api-server` Deployment.
Split topology rolls the complete exact set `${release_name}-api-server`,
`${release_name}-executor`, and `${release_name}-controller`. A distinct
`post-install,post-upgrade` verifier Job then checks the exact database
generation and readiness/generation of every topology-selected Deployment.
Its release-managed service account and Role can only read those exact
Deployments; it cannot restart or patch them. Empty, partial, inferred, or stale
Terraform suffix sets are impossible because Terraform no longer carries this
input.

Seed and verifier Jobs use `before-hook-creation` for same-name retries and a
configurable diagnostic TTL constrained to 86,400--604,800 seconds for both
success and failure. Successful verifier Jobs additionally use
`hook-succeeded` for eager cleanup. This leaves no non-Job hook input to leak indefinitely,
retains failures long enough for diagnosis, and bounds residue even after
uninstall. The verifier RBAC is an ordinary chart-managed resource and is
removed by uninstall. Tests exercise install, upgrade, retry, seed failure,
rollout failure, verifier failure, TTL cleanup, and uninstall residue.

The direct-Helm bundle must prove migration-before-seed, merge parity,
runtime-only preservation, wholesale workspace replacement, explicit pruning,
same-generation retry idempotency, and both all-role and split-role reloads
plus the separate post-rollout verifier and bounded hook lifecycle before the
ownership handoff can be human-applied.

#### One-time Helm ownership handoff

The first consumer currently has four application-owned objects at root
Terraform state addresses because Terragrunt downloads the control-plane module
as its root module:

- `helm_release.skypilot`
- `kubernetes_config_map_v1.seed_config`
- `kubernetes_job_v1.seed_config`
- `terraform_data.reconcile_api_server`

The reusable module revision deletes those resource blocks and permanently
retains these exact tombstones in the same downloaded control-plane module
root:

```hcl
removed {
  from = helm_release.skypilot

  lifecycle {
    destroy = false
  }
}

removed {
  from = kubernetes_config_map_v1.seed_config

  lifecycle {
    destroy = false
  }
}

removed {
  from = kubernetes_job_v1.seed_config

  lifecycle {
    destroy = false
  }
}

removed {
  from = terraform_data.reconcile_api_server

  lifecycle {
    destroy = false
  }
}
```

All four `removed` blocks are permanent control-plane module policy. They are
not generated beside the downloaded module by Terragrunt: that would conflict
with the predecessor module's still-declared resources and would silently lose
the tombstones on a later source-pin change. They are not deleted after the
handoff, and neither this module nor later consumer code may reuse those or
alternate Terraform addresses for the release, seed ConfigMap, seed Job, or
rollout reconciler. A repository guard asserts that every supported
control-plane module revision contains exactly these four `destroy = false`
tombstones and none of the retired resource blocks. A
`manage_release = false` count toggle,
`lifecycle.ignore_changes`, manual `state rm`, and repinning the old module
after the forget are not valid alternatives: each either retains unsafe
ownership, plans destruction, evades review, or can recreate live objects.

Before planning the handoff, the operator proves all four addresses exist in
the expected state and captures the three corresponding live Kubernetes/Helm
objects. The evidence includes release name, namespace, revision, status,
complete `helm get values --all`, manifest, history, chart identity, every
image digest, seed ConfigMap content hash, completed seed Job identity/status,
the canonical pre-H0 `api_server_config` digest and required-path projection,
and a secret-redacted SHA-256 for the bundle. `terraform_data` has no live
object, so its state identity is the proof. If any address or live object is
missing or divergent, stop; do not weaken the four-address contract.

The reviewed direct-Helm artifact contains an operation ID, exact chart
artifact, exact image digests, complete captured values, rendered manifests,
the revision-scoped migration/seed/reload hooks, and a diff against the live
release. Ordinary future upgrades use `helm upgrade --reuse-values` with a
reviewed overlay. A `--reset-values` operation is allowed only as a separately
documented, fully rendered exception that intentionally scrubs a retired value.

The handoff plan must contain exactly four non-no-op managed-resource actions:
`["forget"]` for the four root addresses above. It must contain no create,
update, replace, or delete action for any AWS, Kubernetes, Helm, or other
managed resource and no unreviewed output delta. A human operator applies that
exact saved plan once. Immediately afterward, proof must show all four state
addresses absent, a second plan empty, and the live Helm release, seed
ConfigMap, and completed seed Job byte-for-byte/identity unchanged. Refresh-only
reads are not mutations, but they cannot obscure the exact four-action plan
assertion.

The immutable SkyPilot commit used for this handoff is shared by every
`boltz-platform` production module consumer. Before the control-plane plan can
be accepted, the same source-pin revision must produce a reviewed saved plan
with zero managed-resource actions for each other consumer of that pin. A
source pin cannot be called state-only merely because the Rainier unit's own
plan is state-only.

After that proof, every application change is a direct human Helm operation.
The infrastructure module continues to own only its IAM, Pod Identity, ESO,
and optional namespace resources. The forgotten legacy seed ConfigMap and
completed Job remain inert and unmanaged; they are not deleted during the state
handoff. A separately stacked direct-Helm cleanup removes them only after the
revision-scoped hook path has passed parity and retry evidence in production.
The four tombstones remain after that live-object cleanup. No SkyPilot
application rollout or cleanup may require a `boltz-platform` pull request or
apply.

### `skypilot-spoke-workspace-pool-aws-vm`

This module prepares a spoke AWS account in which SkyPilot workspace VMs run by
creating the provisioner role, VM role and instance profile, optional Session
Manager access, optional SkyServe controller permissions, and explicitly
supplied dataset and KMS grants. It does not create a VM during Terraform apply;
SkyPilot launches VMs into the prepared account later.

Role names retain their existing defaults but are caller-configurable. A
nullable permissions-boundary ARN is accepted and attached to every role the
module creates. IAM and managed-policy ARNs derive from the active AWS
partition. The EC2 Spot service-linked role ARN keeps its documented global
`spot.amazonaws.com` service path in every partition while its ARN prefix
follows the active partition; it must not be synthesized from the partition DNS
suffix.

The controller role ARN is a required, nonempty input, while the existing
counted provisioner addresses remain unchanged. The module intentionally grants
broad EC2 provisioning actions on `*`, access to `skypilot-*` storage buckets,
permission to create the EC2 Spot service-linked role, and optional Session
Manager access. Dataset grants and extra policy inputs can broaden that surface
and must be reviewed by the caller.

### `skypilot-spoke-workspace-pool-eks`

This module connects workspace workloads to an existing spoke EKS cluster, maps
a control-plane IAM principal through an EKS access entry, creates one RBAC
partition per namespace, and optionally creates Pod Identity associations,
exact-priority admission policies, static FSx volumes, and a SkyServe probe
ingress rule. It does not provision the EKS cluster.

The controller role ARN is a required, nonempty input.

Partition namespaces, groups, claim names, and role names are durable identity
keys. Changing them can replace infrastructure and is documented as an explicit
migration, not a routine update.

Partitions are workload credential and storage partitions, not independent
controller or tenant trust boundaries. One controller principal receives every
partition group. A namespace without a module-created Pod Identity association
has no identity from this module, but another pre-existing service-account
association can still provide credentials, and this module does not enforce the
SkyPilot workspace's namespace choice. Callers must audit other associations
and pin workspace namespaces.

The priority policy does not inject or default a PriorityClass. It denies a pod
unless the pod explicitly names the configured class. The cluster must support
`admissionregistration.k8s.io/v1` ValidatingAdmissionPolicy.

Probe ingress rejects public CIDRs unless the caller explicitly opts in. Its
description is configurable with a generic default; `boltz-platform` supplies
the exact legacy description during migration so the persisted rule does not
change. EKS access-entry mode, the Pod Identity agent, CSI drivers, storage
classes, VPC CNI routing, and service availability remain caller
prerequisites.

### `skypilot-spoke-workspace-pool-rbac`

This cloud-neutral module creates the namespace when requested, the workload
service account, read-only cluster visibility, namespaced workload lifecycle
permissions, and bindings to caller-supplied Kubernetes subjects.

The caller owns provider configuration and identity mapping. The module never
creates cloud IAM.

The module grants cluster-wide read access to nodes, pods, and RuntimeClasses,
plus namespaced pod, exec, port-forward, service, event, and optional PVC
permissions. Its ClusterRole and ClusterRoleBinding names are cluster-wide;
callers must choose names that do not collide with another module instance.
Changing namespace or service-account ownership flags after adoption requires a
state-aware migration.

## Architecture and invariants

```text
existing EKS host
  ├── human-operated direct Helm release (application owner)
  │     └── revision-scoped migration → config seed → all/split reload
  └── skypilot-control-plane (supporting-infrastructure owner)
        ├── optional namespace + ESO
        └── API-server Pod Identity role
              ├── assumes skypilot-spoke-workspace-pool-aws-vm provisioner role
              └── maps into skypilot-spoke-workspace-pool-eks access entry
                    └── skypilot-spoke-workspace-pool-rbac[partition namespace]
```

- Remaining infrastructure resource and module labels are preserved from the
  deployed modules. The four application/reconciliation labels are retired
  together through permanent control-plane-module-root `removed` blocks, not
  renamed.
- Existing `count` and `for_each` shapes, partition keys, and volume keys are
  preserved.
- The package-directory rename does not rename remaining internal Terraform
  labels, infrastructure variables/outputs, physical defaults, tags, service
  accounts, or namespaces. Application and seed inputs/outputs are deleted.
- Defaults produce the same commercial-AWS names, policies, and tags as the
  local modules they replace. Terraform has no Helm-value defaults after the
  handoff.
- Direct Helm exclusively owns chart values, request-store settings, image
  digests, release history, application migrations, configuration seeding,
  role reload, and workload topology.
- The only intentional state-address changes are the one-time forgets of root
  `helm_release.skypilot`, `kubernetes_config_map_v1.seed_config`,
  `kubernetes_job_v1.seed_config`, and
  `terraform_data.reconcile_api_server`; every infrastructure address remains
  stable.
- All four permanent control-plane-module-root `removed` tombstones use
  `destroy = false`,
  and no Terraform resource may subsequently own the application release,
  seed artifacts, or rollout reconciliation.
- Optional permissions boundaries default to `null`, preserving existing plans.
- Supported named-secret integrations do not ingest long-lived secret payloads.
- Every cross-repository module source is pinned; no moving branch or
  prerelease lookup is accepted by production callers. The separate direct
  Helm artifact pins the chart and images independently.
- Namespace and persistent-volume resources retain their existing ownership and
  `Retain` behavior.
- Provider credentials are ambient or short-lived `aws eks get-token` results;
  the EKS token is not persisted in Terraform state.
- Caller-owned objects are explicit: the control-plane module can attach an
  inline read policy to a named ESO role, and the EKS module can mutate a named
  node security group. Those ownership edges and their broad IAM/RBAC surfaces
  are documented.
- Inputs interpolated into local commands and physical identifiers are
  validated for their AWS or Kubernetes grammar. Cross-field checks require
  coherent ESO settings, a nonempty EKS partition set, valid FSx
  driver/mount-name combinations, non-public probe sources absent an explicit
  opt-in, and a permissions boundary in the active account and partition.
- Direct-Helm hook ordering enforces migration before the idempotent seed and
  seed commit before the topology-complete reload. No Terraform value or state
  participates in configuration seeding or role selection.

## Implementation phases

### Phase 1: publish infrastructure-only modules in SkyPilot

1. Add the four modules and their operator documentation.
2. Add the revision-scoped direct-Helm migration, configuration-seed, and
   all-role/split-role reload contract. Merge it as an application PR and have
   a human deploy it directly with Helm; it does not wait for or modify
   `boltz-platform`.
3. Move provider configuration out of the published modules and into
   caller-owned root configuration.
4. Remove the Helm and Time provider requirements, chart-value assembly, chart
   registry lookup, all four application/reconciliation resources, the seed
   script and local-exec restart, and every application/seed input and output
   from the reusable module. In their place, add the exact four permanent
   `destroy = false` tombstones to the control-plane module root; they are the
   only application-address declarations retained by the package.
5. Remove deployment-specific prose and examples.
6. Add a configurable API role name, optional
   permissions boundaries, partition-aware IAM/FSx values, validations, and
   tests without renaming resource labels.
7. Expand `infra/terraform/README.md` into a module catalog and state plainly
   that application deployment is direct Helm.
8. Add the authoritative-fork agent rule and align the formatter baseline with
   `origin/improvements`.

### Phase 2: migrate the first consumer and hand off Helm ownership

1. Centralize one full 40-hex commit SHA from the Phase 1 pull request for
   review. Before Phase 2 can merge or apply, Phase 1 must merge and the pin
   must be updated to a commit reachable from `origin/improvements`.
2. Require evidence that the revision-scoped direct-Helm path is already
   deployed and has passed the migration/seed/reload parity and idempotency
   contract. Then capture and review the live release and legacy seed-object
   evidence described above. The handoff itself is not an application rollout
   and must not issue a Helm mutation.
3. Generate the existing EKS exec-auth provider only where used: Kubernetes in
   the control-plane and EKS-pool roots, and no additional provider in the
   AWS-VM root. Change all four production units to the corresponding remote
   module source. Do not generate or pass a Helm provider.
4. Remove chart/application/seed/restart inputs, Helm provider generation,
   registry-auth data, and application-value guards from the control-plane
   consumer. Pin the module revision that carries the four permanent root
   tombstones exactly as written above. Replace obsolete application-value
   assertions with an ownership guard scoped to the SkyPilot control-plane
   consumer that rejects any Helm provider, release, seed/restart resource, and
   application input there; unrelated platform-owned Helm releases remain in
   their existing infrastructure roots. Application and security-config
   assertions move to the reviewed direct-Helm
   H0 artifact and canonical database parity evidence. These edits must leave
   the live release, legacy seed ConfigMap, and completed seed Job untouched.
5. Synchronize and retain the local module copies temporarily so a source
   rollback remains valid with caller-owned providers. The retained local
   control-plane copy must also be infrastructure-only and contain the same
   four permanent tombstones. The local copy must never be a path back to
   Terraform Helm or seed ownership. Preserve the legacy
   underscore directory layout and the EKS module's valid
   `../skypilot_pool_rbac` sibling path; initialize and validate every retained
   local source as well as every remote source.
6. Split tests: generic module and chart-hook tests live in SkyPilot; Rainier
   pin, paid-capacity, infrastructure ownership, module-tombstone, and exact-plan
   assertions remain in `boltz-platform` at an environment-owned path. Update
   pre-commit, CI, the GCP pool README, and runbook references.
7. Keep all three committed environment `.terraform.lock.hcl` files
   byte-for-byte unchanged. An unused historical Helm-provider selection may
   remain in a lock file; no root configuration or module may require it.
8. Run Terragrunt/OpenTofu source initialization, validation, provider
   comparison, and read-only old-source versus new-source plans. The old-source
   baseline is the untouched module tree from pre-Phase-2 `origin/main`, not the
   synchronized compatibility copies on the migration branch. The
   control-plane plan's only non-no-op managed-resource actions must be four
   `["forget"]` actions at the exact root addresses in the handoff contract,
   with zero AWS, Kubernetes, or Helm mutations. Every other production unit
   consuming the shared SkyPilot module pin--currently the research-production
   EKS pool, research-usw2 spoke-workspace EKS pool, and multi-tenant AWS-VM
   account--must have its own saved plan with no managed-resource actions.
   Output-only changes, if any, are enumerated and reviewed separately and may
   not mask a resource action.
9. After review, a human applies each plan. Automation and agents do not apply
   production Terraform/OpenTofu. The control-plane handoff is applied exactly
   once and immediately followed by the state/live-release proof below.

### Phase 3: remove compatibility copies

Open a stacked draft that deletes the four unused local module directories and
updates stale local-path documentation. It retains the four permanent
tombstones in the synchronized and remote control-plane module roots. It must
not merge until Phase 2 is merged, human-applied, and operationally verified
across all four production units.

### Phase 4: remove inert legacy seed objects through direct Helm operations

Open the application cleanup as a stacked draft with the direct-Helm hook
change. It deletes only the forgotten legacy seed ConfigMap and completed Job;
`terraform_data.reconcile_api_server` has no live object and the Helm release
remains in place. The cleanup stays blocked until Phase 2 proves all four state
addresses absent and the deployed revision-scoped hook path proves migration,
merge/readback parity, same-generation retry idempotency, and both topology
reload modes in the target environment. It is executed as a reviewed human
direct-Helm cleanup, never a Terraform action. The cleanup artifact carries the
captured exact legacy names, UIDs, content/generation hashes, and terminal Job
status; it fails closed instead of deleting an object when any identity differs.
Its temporary RBAC is scoped to those object names and removed with the cleanup
hook. The control-plane module-root tombstones are retained permanently after
cleanup.

### Pull-request stack and cross-repository gates

The SkyPilot stack contains: (S1) the direct-Helm hook transition and parity
tests; (S2) the infrastructure-only reusable module; and (S3) the draft legacy
seed-object cleanup. S3 names S1's production parity evidence as its merge gate.
The first-consumer stack contains: (P1) the remote pin, inert application-input
retirement, and ownership/plan guards and (P2) the draft local-module-copy
deletion. The four permanent root tombstones ship in S2. P1 cannot merge until
S2 is reachable from `origin/improvements`; its human apply is additionally
blocked on S1's direct-Helm production proof. P2 cannot merge until P1 has been
human-applied and verified across all four production units. S3 cannot merge
merely because P1 merged: it requires the successful P1 apply, exact state-absence evidence,
and S1 target parity. These are explicit cross-repository gates, not a reason to
route an ordinary SkyPilot rollout through `boltz-platform`.

## Deployment and rollback

No infrastructure is applied by these pull requests.

Before the Phase 2 handoff apply, rollback is a source change to the synchronized
infrastructure-only local copy, which retains all four tombstones. After the
four addresses have been forgotten, Terraform ownership
is not rolled back: operators must not import any of them, repin a module that
contains any retired resource, or remove a tombstone. A module defect is
corrected forward with a new pinned module commit that still excludes the
release and seed/restart resources. The production pin always names a commit
reachable from `improvements`, so deleting the feature branch cannot remove it.

A direct Helm rollout or rollback is separate from this infrastructure apply.
It uses a new reviewed `helm upgrade` operation against the existing release,
normally with `--reuse-values`; it does not use Terraform and does not require a
`boltz-platform` change. If an intentional retired-value scrub requires
`--reset-values`, the operator must first review the complete rendered values
and manifests. Native `helm rollback` is not used across application schema,
taint, or migration boundaries; recovery is a reviewed fix-forward upgrade that
preserves the durable database contract.

If any consumer plan proposes replacement, deletion, an address move, IAM
policy broadening, namespace recreation, or persistent-volume recreation, stop
the rollout. Correct the module contract first; do not approve the plan or add
ad hoc state commands. The sole exception is the reviewed `forget` action for
each of the four exact root addresses; the handoff stops unless all four and no
other non-no-op resource actions appear.

Application one-way boundaries, including request-store and database
migrations, belong to the direct Helm rollout design. Removing them from
Terraform does not make them reversible: every fix-forward Helm artifact must
retain the selected durable backend and completed migration gates.

## Verification

Required before opening the pull requests:

- `terraform fmt -check -recursive infra/terraform`
- `terraform init`, `terraform validate`, and
  `terraform test -test-directory=terraform-tests` for every new module
- the direct-Helm config-seed unit and chart-rendering suites, including
  migration-before-seed ordering, helper digest pinning, deep-merge winner
  direction, runtime-only preservation, list/scalar replacement, wholesale
  `workspaces`, explicit pruning, database/readback failure, generation
  idempotency, the 262,144-byte canonical input bound, complete all-role/split-
  role generation reloads, the separate post-rollout verifier, same-name retry,
  failure retention, bounded Job TTLs, interrupted-client-after-success, and
  uninstall residue; the suite also requires literal
  `pluginsUnsupported: true` in every enabled contract and its generation,
  rejects omitted/false attestation, a nonempty stored `plugins` value, and an
  ordinary or plugin-provided unknown field under strict built-in validation,
  and proves no plugin loader, import, or installer is invoked
- Terraform 1.14.8 and OpenTofu 1.11.5 tests proving the language floor is
  `>= 1.7.0` and the control-plane module root contains exactly four permanent
  `removed` blocks with `destroy = false`
- CI searches proving the reusable control-plane module has no Helm or Time
  provider and no `resource` block for `helm_release`, seed ConfigMap/Job, or
  `terraform_data` reconciler,
  local-exec restart, chart lookup, values rendering, application/seed input,
  or application/seed output
- tests proving the direct-Helm revision-scoped objects cannot collide with the
  forgotten legacy ConfigMap/Job and that their eventual cleanup is gated
- a guard that every module directory has a Terraform test suite
- commercial AWS, GovCloud, and China mocked contract tests for IAM, Secrets
  Manager, S3, SSM, EC2, managed-policy, service-linked-role, service-principal,
  and FSx DNS partition values; correct syntax does not promise each service is
  available in every region or partition
- automated checks for unchanged remaining resource/module labels,
  `count`/`for_each` shapes and identity keys, exact retirement of only the four
  named labels, full 40-hex source pins, and unchanged consumer lock files
- Terragrunt/OpenTofu 1.11.5 source initialization and validation for all four
  `boltz-platform` production units consuming the shared module pin
- fixture-state plan JSON proving exactly four root `forget` actions for the
  named addresses, zero mutation actions, and an empty follow-up plan
- old-source and new-source plan JSON comparison where the required backend and
  cloud credentials are available
- repository searches proving no deployment-specific account, hostname,
  repository, workspace, or image remains in the reusable module package
- a stale-reference gate proving published module code, the module catalog and
  READMEs, Terraform tests, and CI contain none of the old hyphenated package
  paths `skypilot-pool-aws-vm`, `skypilot-pool-eks`,
  `skypilot-pool-rbac`, or the old nested source
  `../skypilot-pool-rbac`; the temporary underscore-form `boltz-platform`
  rollback directories and `../skypilot_pool_rbac` source are the only
  pre-Phase-3 exception and are validated separately

The SkyPilot stale-reference gate is:

```bash
if rg -n 'skypilot-pool-(aws-vm|eks|rbac)|\.\./skypilot-pool-rbac' \
    infra/terraform .github/workflows; then
  exit 1
fi
```

Manual verification for the eventual rollout:

1. Verify the revision-scoped direct-Helm hook path is deployed and its target
   parity/idempotency evidence is accepted, including identical canonical and
   raw `api_server_config` SHA-256 before and after H0 and identical
   `gcp.vpc_name`, `aws.ingress_source_ranges`, global
   `kubernetes.allowed_contexts`, and complete per-workspace Kubernetes-boundary
   projections: either exact disabled state or the namespace/allowed-context
   pair. Prove the live row has no configured API plugins, set literal
   `pluginsUnsupported: true`, and retain evidence that the helper used strict
   built-in validation without loading, importing, or installing plugin code.
   Confirm all four Terraform state
   addresses exist, then capture the release and legacy seed-object evidence
   bundle: redacted values/manifest hashes, revision, status, chart identity,
   image digests, seed ConfigMap content hash, and completed seed Job identity
   and status.
2. Plan the control-plane unit using the approved non-admin production identity.
   Inspect plan JSON and require exactly four root forgets for
   `helm_release.skypilot`, `kubernetes_config_map_v1.seed_config`,
   `kubernetes_job_v1.seed_config`, and
   `terraform_data.reconcile_api_server`, with no
   create/update/replace/delete action and no unreviewed output change.
3. Have a human apply that exact saved plan once. Verify all four state
   addresses are absent, an immediate new plan is empty, and the live release,
   legacy seed ConfigMap, and completed seed Job match the pre-apply evidence.
   For the release, compare revision, Helm release Secret identity/resource
   version, redacted values/manifest hashes, Helm-owned workload object
   UIDs/generations, pod-template hashes, and image digests.
4. Plan every other production unit that consumes the shared module pin. For
   the research-production and research-usw2 EKS pools, verify the access
   entry, every
   `module.rbac[namespace]`, admission object, FSx PV/PVC, Pod Identity
   association, and security-group rule remain at their existing addresses.
5. Plan the multi-tenant AWS VM pool and verify both IAM roles, the instance
   profile,
   policies, and attachments remain in place.
6. Human-apply only the reviewed zero-mutation consumer plans, then verify the
   SkyPilot API health endpoint and read-only workspace/cloud checks. Do not
   activate paid capacity merely to prove the ownership handoff.
7. Keep the legacy seed ConfigMap and Job until the separately stacked direct
   Helm cleanup gate passes; then delete only those two objects and retain all
   four root tombstones.

## Verification evidence

The pre-rename Phase 1 implementation was verified on 2026-07-31 with Terraform
1.14.8:

- `terraform fmt -check -recursive infra/terraform` passed.
- `terraform init -backend=false -test-directory=terraform-tests`,
  `terraform validate`, and
  `terraform test -test-directory=terraform-tests` passed for all eight
  modules under `infra/terraform/modules` (155 tests total; the four modules in
  this design contributed 46 tests).
- `uv run --no-project --with 'PyYAML>=6,<7' --with 'SQLAlchemy>=2,<3'
  python -m unittest -v test_seed_config.py` passed all 24 tests.
- An independent comparison against untouched `boltz-platform@origin/main`
  confirmed identical managed-resource/module labels, collection identity
  shapes, commercial-AWS effective values, and seed-script bytes. The seed
  script SHA-256 is
  `b87765a0f58db47b8cade97a8ebf6224e7558b31ba105478e3493f4615a1ca74`.
- The pre-rename design passed adversarial review after the Spot service-name
  and output-only plan contracts were clarified.

The rename-specific verification also completed on 2026-07-31:

- Terraform 1.14.8 formatting, initialization, validation, and all 155 tests
  passed across the eight published module roots. The four modules in this
  design contributed 9 control-plane, 11 AWS VM, 16 EKS, and 10 RBAC tests.
- Terraform-docs 0.20.0 checks passed for all four modules, and no module-root
  dependency lockfiles remain in the package tree.
- The seed suite passed all 24 tests and retained the SHA-256 recorded above.
- The stale-reference gate, actionlint 1.7.7, and working-tree/index diff checks
  passed.
- An independent comparison against both pre-rename SkyPilot commit
  `fe2938b71cd0559199909d5a31147cd93fad8c5d` and untouched
  `boltz-platform@origin/main` confirmed that the only executable rename change
  is the EKS sibling module source. Managed resource/module labels,
  `count`/`for_each` expressions, variable defaults, outputs, tags, physical
  names, commercial-AWS effective values, and seed bytes are unchanged.
- The rename contract passed adversarial review before implementation and again
  against the completed verification evidence.

This evidence predates the direct-Helm correction. It remains evidence for the
unchanged IAM, Pod Identity, ESO, namespace, pool, and historical seed-merge
foundation, but it does not validate the revised four-address ownership
handoff, language floor, chart-hook ordering/idempotency, or topology-complete
reload contract.

The now-superseded Terraform-owned request-store contract was verified on
2026-08-05 with Terraform 1.15.8: formatting and all 16 control-plane mocked
contract/validation tests passed. That result is historical only. Request-store
values now belong exclusively to the direct Helm artifact, so those tests must
be removed or relocated to chart/direct-Helm coverage rather than used as
evidence for this module.

No revised implementation, fixture-state handoff plan, live production plan,
or apply evidence is claimed yet. Phase 1 and Phase 2 commands and pull-request
descriptions must record their exact results when they run.

## Review history

- 2026-07-31: the package rename and reusable-module preservation contract
  passed adversarial review.
- 2026-08-05: the then-current Terraform-owned request-store extension was
  verified and accepted.
- 2026-08-08: the organization SkyPilot operating contract established direct
  human Helm ownership. That invalidated the module's Helm-release ownership
  and Terraform seed/restart ownership as well as the earlier accepted status.
  This revision specifies a permanent, four-address non-destructive state
  handoff and revision-scoped direct-Helm configuration reconciliation.
- 2026-08-08: exact review rejected a one-address stack-map remnant, a SkyPilot
  control-plane Helm provider, and a single-hook/ConfigMap seed lifecycle that
  could neither verify the later rollout nor bound orphaned hook objects. The
  correction requires four forgets, separate direct-Helm authentication, a
  weighted pre-upgrade seed Job with embedded bounded input, regular
  generation-annotated Deployments, and a distinct read-only post-upgrade
  verifier with bounded Job retention. A follow-up exact pass required that
  TTL on successful verifier Jobs too, because an interrupted Helm client can
  miss `hook-succeeded` eager cleanup.
  Independent exact review accepted commit
  `e7d484f85571e89887ad903d8d59df9b9681e437` with no remaining blocker.
- 2026-08-08: cross-repository implementation review found that Terragrunt
  downloads the control-plane module as the root, so platform-generated
  tombstones would conflict before the pin change and disappear after a later
  pin. Review 28 moves the four permanent tombstones into the SkyPilot module,
  requires a guard against their removal, expands shared-pin planning to every
  production consumer, retires inert platform application inputs and guards,
  and makes H0 prove raw/canonical database-config and security-key parity.
  Independent exact cross-repository review accepted the correction with no
  remaining blocker.

## Open gates

- Preserve the accepted contract while implementing the ownership correction;
  any behavioral departure updates this file and reopens exact review first.
- Phase 1 must implement and test the direct-Helm migration/seed/reload path,
  Terraform/OpenTofu `>= 1.7.0` floor, infrastructure-only module, and absence
  of all module-owned Helm, seed, and restart behavior. It must retain the exact
  four module-root tombstones and implement the H0 no-op config parity guard.
- Phase 2 may use a pushed Phase 1 SHA for draft review, but cannot merge or
  apply until Phase 1 is merged and the pin is reachable from
  `origin/improvements`.
- The direct-Helm path must be human-deployed and its target parity evidence,
  exact live-object/state inventory, and an approved non-admin production
  identity must exist before the handoff plan. The plan must prove the exact
  four-forget/zero-mutation contract, and every other shared-pin production
  consumer must have a saved zero-managed-resource-action plan. Only a human
  may apply it.
- Phase 3 remains a stacked draft until Phase 2 is merged, human-applied, and
  verified across all four production units, including state absence and
  unchanged live Helm proof for the control plane.
- Phase 4 stays blocked until the direct-Helm hook path and the state handoff
  pass their production gates. It removes only the two inert live seed objects;
  all four `removed` tombstones are permanent and are not cleanup gates.
- No future application rollout or seed-object cleanup is gated on a
  `boltz-platform` change.
- Any intentional resource-label change requires updating this design first and
  adding a reviewed address migration; none except the four explicit
  state-only forgets is currently planned.
