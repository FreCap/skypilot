# Managed Container Image Distribution

Status: control-plane implementation complete; production activation pending

Last updated: 2026-07-18

Implementation: [PR #368](https://github.com/boltz-bio/skypilot/pull/368),
branch `feat/managed-container-image-distribution`

## Outcome

SkyPilot will resolve an OCI image once to an immutable digest, track it in a
workspace-scoped PostgreSQL catalog, materialize verified copies through a
configured canonical registry and optional regional registries, and persist an
immutable image snapshot with every launched workload. VM, Kubernetes,
managed-job recovery, and SkyServe updates then use the same verified artifact
identity without storing registry credentials in durable state.

The control-plane contract and runtime integration are implemented. Production
activation remains deliberately gated on repository and IAM bootstrap,
cloud-provider operations, deployment of the copy worker, and canary evidence.

## Goals

- Pin mutable OCI references to a verified immutable digest before persistence.
- Reuse the same artifact across clouds, clusters, regions, restarts, and
  SkyServe versions while preserving workspace isolation.
- Prefer a verified local registry copy without making locality a hard
  availability dependency unless policy explicitly requires it.
- Keep registry profiles and durable records free of credential values.
- Make publication, copying, verification, retry, lease recovery, reference
  tracking, and eviction durable and safe under concurrent workers.
- Support catalogs with millions of artifacts through bounded indexed queries,
  leases, and reconciliation batches.
- Preserve existing deployments until a workload explicitly opts into managed
  distribution.

## Non-goals

- Automatically converting existing `image_id: docker:...` workloads.
- Automatically deploying a copy-worker fleet or creating repositories and IAM
  in this implementation PR.
- Reinterpreting the task-level `setup` command as image-build work.
- Building ARM64 images or any unused platform proactively.
- Starting one model process per GPU. SkyPilot shares one node image pull; the
  workload remains responsible for its per-GPU process topology.
- Treating SQLite as a supported central/API-server catalog database. Local and
  controller databases retain their existing, separate compatibility policy.

## Current status

Completed in PR #368:

- [x] Add the `resources.container_image` scalar and object contracts.
- [x] Add administrator registry profiles and workspace activation/locality
  policy.
- [x] Resolve OCI tags to immutable descriptors and validate platform metadata.
- [x] Add the workspace-scoped artifact, release, location, intent, request,
  reference, and event state in PostgreSQL.
- [x] Add lease-fenced reconciliation, retry, verification, and bounded catalog
  operations.
- [x] Add versioned SDK, CLI, REST, and request-executor surfaces.
- [x] Persist immutable image snapshots across launch, managed-job recovery,
  and every SkyServe version.
- [x] Preserve heterogeneous legacy Docker `image_id` candidates outside the
  managed catalog.
- [x] Make existing SkyServe workspace adoption null-only, evidence-based, and
  fenced by the exact service incarnation.
- [x] Document the task and administrator configuration contracts.
- [x] Verify the implementation with targeted local tests and all 20 GitHub
  checks on code-bearing implementation head
  `62cd18974af18b34c8e5c3846fc1010ed0257e40`.

Required before production activation:

- [ ] Bootstrap managed ECR, GAR, Nebius, or generic OCI repositories and the
  required pull/copy IAM bindings, or register pre-created external locations.
- [ ] Implement and validate provider operations for repository provisioning
  and short-lived, destination-scoped copy credentials.
- [ ] Deploy a separately scaled, resource-bounded reconciliation/copy worker.
- [ ] Run VM and Kubernetes canaries covering import, cross-cloud copy, local
  pull, fallback, restart, update, rollback, and digest mismatch.
- [ ] Measure cold-start latency, source-registry egress, worker throughput, and
  registry throttling at the intended fleet scale.
- [ ] Migrate `boltz-l4-fleet.serve.yaml` in a companion `boltz-platform` PR
  after the operational gates pass.

## Public task contract

The scalar form selects an OCI source reference:

```yaml
resources:
  container_image: ghcr.io/my-org/model:2026-07-18
```

The object form selects a source, immutable release alias, exact catalog
artifact, or registry distribution profile:

```yaml
resources:
  container_image:
    ref: ghcr.io/my-org/model:2026-07-18
    release: model-production-2026-07-18
    distribution: production
```

Supported fields:

- `ref`: OCI source reference. A selected managed profile resolves a mutable tag
  to a digest before the workload is persisted.
- `release`: workspace-scoped immutable human-readable alias. With `ref`, first
  use binds the alias; alone, it selects an existing release.
- `artifact_id`: SkyPilot-generated UUID for an exact catalog artifact. It is
  mutually exclusive with `ref` and `release`.
- `distribution`: administrator-configured registry profile. `direct` is an
  explicit bypass under `managed_preferred` and is rejected under
  `managed_required`.

When `distribution` is omitted, resolution uses the workspace default and then
the API-server default. If no profile is configured, a `ref` retains direct
pull behavior. `release` and `artifact_id` require a managed profile.

Every `any_of` or `ordered` candidate in one managed workload must resolve to
the same immutable artifact. Legacy `image_id: docker:...` remains supported
and deprecated, and does not opt into managed distribution.

## Administrator contract

Registry profiles are revisioned, atomic, and secret-free:

```yaml
container_registries:
  default_profile: production
  profiles:
    production:
      revision: 1
      ownership: managed
      realm: production
      namespace: skypilot/{workspace}
      require_digest_at_runtime: true
      canonical:
        provider: aws
        account: '123456789012'
        region: us-east-1
        pull_auth: ecr_runtime_identity
      targets:
        - name: gcp-us-central1
          provider: gcp
          project: my-gcp-project
          region: us-central1
          pull_auth: gar_runtime_identity

workspaces:
  research:
    container_images:
      mode: managed_required
      default_profile: production
      allowed_profiles: [production]
      locality: prefer
      regional_cache_retention_weeks: 8
```

Profile revisions must increase when endpoints, identities, namespaces, or
policy fields change. `ownership: managed` requires a workspace-partitioned
namespace; `ownership: external` leaves repository lifecycle outside SkyPilot.

Workspace policy has two activation modes:

- `managed_preferred` uses a verified local or canonical copy when possible and
  may use an authenticated source fallback.
- `managed_required` rejects direct references and legacy Docker `image_id`
  workloads.

Locality policy is independent:

- `prefer` permits canonical or authenticated source fallback.
- `require` waits for a verified local target.
- `canonical` always uses the canonical registry target.

## Architecture and responsibility split

```text
Task selector
    -> immutable OCI resolution and policy admission
    -> workspace artifact and release catalog
    -> canonical and regional distribution intents
    -> lease-fenced reconciliation/copy worker
    -> digest verification
    -> placement-specific runtime reference
    -> immutable workload snapshot
    -> VM or Kubernetes launcher
```

| Component | Responsibility |
|---|---|
| Selector/resolver | Normalize task syntax, resolve immutable identity, and enforce workspace policy. |
| Catalog | Own workspace-scoped artifact identity, aliases, references, and lifecycle state. |
| Registry profile | Describe canonical and locality targets without credential values. |
| Reconciler | Claim bounded work, copy content, verify digests, retry, and publish readiness. |
| Placement resolver | Choose a verified local, canonical, or allowed source fallback. |
| Launcher | Pull the selected digest-pinned reference and run the task's runtime setup. |
| Image builder | Future, separate producer; it is not part of distribution reconciliation. |

## Data and safety invariants

- The central catalog is PostgreSQL-only and rejects non-PostgreSQL engines.
- Artifact identity is workspace-scoped and digest-based; mutable tags are not
  workload identity.
- A release alias is immutable after its first successful binding.
- Profile revision and realm are persisted with work so queued operations cannot
  be reinterpreted after configuration changes.
- Durable state contains identity references and provider metadata, never
  credential values.
- Workers use expiring leases and fencing tokens; stale workers cannot commit a
  result after ownership changes.
- A location is launchable only after its digest has been verified.
- SkyServe workspace backfill only fills null or empty values, validates the
  controller incarnation first, and fails closed on conflicting evidence.
- One node-level pull serves all GPUs on a multi-GPU instance.

## Compatibility and deployment behavior

Checking in or merging the code has no effect on a running deployment. Deploying
the API-server code adds the PostgreSQL migrations and new control-plane
surfaces, emits deprecation warnings for legacy Docker syntax, and permits safe
workspace backfill for existing service rows. It does not restart replicas,
replace images, or opt existing workloads into managed distribution.

The latest checked Boltz L4 fleet configuration contains 254 candidates: two
legacy Kubernetes image candidates and 252 host-mode candidates. It produces no
managed artifact snapshot, so its VM R2 download/`docker load`, Kubernetes ECR
pull, and per-GPU launcher behavior remain unchanged.

Managed behavior begins only when a task uses `resources.container_image` and a
registry profile is selected. Keep workspaces on `managed_preferred` during
migration. Enable `managed_required` only after every workload in that workspace
has been converted and rollback has been exercised.

## Rollout and rollback

1. Provision or register the canonical repository, regional repositories, and
   runtime identities in a staging workspace.
2. Deploy the reconciliation worker with bounded concurrency, rate limits,
   metrics, and dead-letter visibility.
3. Configure a revisioned profile under `managed_preferred` and `locality:
   prefer`.
4. Import a digest-pinned canary image and prove canonical and regional digest
   verification.
5. Launch on one multi-GPU VM and one Kubernetes cluster. Prove one node pull,
   correct runtime identity, per-GPU workload startup, restart recovery, and
   safe fallback.
6. Convert one low-risk SkyServe service and compare cold-start latency, egress,
   and error rate against its legacy path.
7. Expand by workspace and region. Switch to `managed_required` only after all
   legacy selectors are removed and rollback tests pass.

Rollback does not require deleting catalog state. Under `managed_preferred`, a
workload can return to its prior YAML or explicitly use `distribution: direct`.
Disable the migrated service version, restore the prior service YAML, and keep
the additive PostgreSQL catalog for later diagnosis or retry. Do not use direct
fallback under `managed_required`; change workspace policy first.

## Verification evidence

- Managed image, resource parsing, YAML parsing, API, and PostgreSQL suites pass.
- Launch, managed-jobs, Serve state, Serve controller, replica-manager, update,
  and stale-controller compatibility coverage pass.
- PostgreSQL 16 migration reconciliation passes for both former revision-016
  preview layouts.
- The exact latest Boltz L4 fleet YAML parses all 254 candidates unchanged and
  produces `artifact_ids=[]`.
- YAPF, isort, mypy, Pylint, Ruff, dashboard lint, Prettier, Python compilation,
  and `git diff --check` pass.
- All 20 visible GitHub checks passed on code-bearing implementation head
  `62cd18974af18b34c8e5c3846fc1010ed0257e40` before this plan-only update.
  The PR check rollup is authoritative for the latest exact-head status, and
  every subsequent plan or code update must pass it before merge.

## Open decisions and follow-up plans

- The provider bootstrap and worker deployment must receive their own finalized
  implementation plan before production work begins.
- The companion Boltz fleet conversion plan must specify the replacement for
  the current host-mode R2 tarball path and preserve the deliberate per-GPU
  process launcher.
- Image construction remains in the separate, unimplemented
  `sky/design_docs/proposals/managed_container_image_builder.md` proposal. If
  accepted, it must become its own feature plan rather than expanding this
  distribution worker's responsibilities.

## Change log

- 2026-07-18: Recorded the implemented control-plane contract, compatibility
  behavior, exact-head verification, and outstanding production activation
  gates for PR #368.
