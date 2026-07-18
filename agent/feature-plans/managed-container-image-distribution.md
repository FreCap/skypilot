# Managed Container Image Distribution

Status: control-plane implementation complete; infrastructure and provider
activation pending

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

Distribution must not make the normal deployment path wait for repository
creation or image replication. Infrastructure is provisioned ahead of time,
publication can warm routes asynchronously, and placement selects an already
verified local route or immediately falls back to an immutable canonical/source
route under the default policy.

The control-plane contract and runtime integration are implemented. Production
activation remains deliberately gated on repository and IAM bootstrap,
cloud-provider operations, deployment of the copy worker, and canary evidence.

## Goals

- Pin mutable OCI references to a verified immutable digest before persistence.
- Reuse the same artifact across clouds, clusters, regions, restarts, and
  SkyServe versions while preserving workspace isolation.
- Prefer a verified local registry copy without making locality a hard
  availability dependency unless policy explicitly requires it.
- Add no synchronous repository provisioning, registry copy, or verification
  work to placement or workload admission.
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
- Treating a raw S3-compatible bucket, including Cloudflare R2, as an OCI
  registry. An R2-backed service that implements the OCI Distribution API is a
  registry; an R2 bucket containing image tarballs is not.
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
- [x] Reconcile the feature with the current `improvements` base and review the
  async-lifecycle baseline shifts introduced by the feature's added lines.
- [x] Verify the implementation with targeted local tests and all 20 GitHub
  checks on code-bearing implementation head
  `62cd18974af18b34c8e5c3846fc1010ed0257e40`.

Required before production activation:

- [ ] Extract reusable AWS bootstrap modules from the current Boltz Platform
  ECR, SkyPilot control-plane, and VM-pool Terraform without carrying over
  Boltz-specific names, account topology, or retention policy.
- [ ] Bootstrap managed ECR, GAR, Nebius, Cloudflare, or generic OCI targets and
  the required pull/copy IAM bindings, or register pre-created external
  locations.
- [ ] Implement and validate provider operations for repository provisioning
  and short-lived, destination-scoped copy credentials.
- [ ] Deploy a separately scaled, resource-bounded reconciliation/copy worker.
- [ ] Run VM and Kubernetes canaries covering import, cross-cloud copy, local
  pull, fallback, restart, update, rollback, and digest mismatch.
- [ ] Measure cold-start latency, source-registry egress, worker throughput, and
  registry throttling at the intended fleet scale.
- [ ] Migrate `boltz-l4-fleet.serve.yaml` in a companion `boltz-platform` PR
  after the operational gates pass.
- [ ] Canary external digest pulls, large model images, credential renewal, and
  throttling against Cloudflare's managed R2-backed registry before adding a
  native Cloudflare adapter.

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

The operational phase should extend a target with an explicit materialization
strategy rather than pretending a lazy proxy is already a verified local copy:

```yaml
container_registries:
  profiles:
    production:
      revision: 2
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
        - name: aws-us-west-2-cache
          provider: aws
          account: '210987654321'
          region: us-west-2
          pull_auth: ecr_runtime_identity
          materialization: pull_through
        - name: cloudflare-global
          provider: cloudflare
          registry: registry.cloudflare.com/0123456789abcdef
          region: global
          manager_identity: cloudflare-production
          pull_auth: cloudflare_short_lived
          materialization: copy
          localities:
            - {provider: nebius, region: eu-north1}
```

When introduced, `materialization` should default to `copy` for compatibility
with the current contract.
`pull_through` means the endpoint can proxy a cold immutable digest and lazily
create its local repository. It is usable without a control-plane copy wait but
must not be reported as a verified local copy until the digest has actually
been observed there. `provider: cloudflare` and
`cloudflare_short_lived` are planned contracts, not implemented by PR #368.
The manager identity names the credential broker; generated credentials remain
ephemeral and are never stored in the profile or catalog.

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

## Deployment latency contract

The fastest deployment path selects an existing `release` or `artifact_id`.
That path performs PostgreSQL reads only before ordinary cloud provisioning. A
new mutable `ref` still requires one bounded source-registry manifest lookup to
establish immutable identity; build or release CI should publish the artifact
first when even that lookup is undesirable.

Placement and admission may read catalog state and enqueue an idempotent intent.
They must not create a repository, copy layers, wait for reconciliation, or
perform a cloud control-plane write. Copy workers, cache prewarming, digest
verification, and retries run outside the request path and can overlap VM or
Kubernetes provisioning.

Route selection under `managed_preferred` and `locality: prefer` is ordered:

1. Use a verified local copy.
2. Use a verified canonical copy or authenticated digest-pinned source while
   local warming continues.
3. Use a cold pull-through route only when policy prefers lazy locality over the
   lower-latency canonical/source fallback.

The container runtime must still download missing layers. ECR pull-through
cache removes a separate orchestration wait and creates repositories lazily, but
its first pull can still pay the upstream transfer cost. Hot images should be
prewarmed asynchronously by pulling their platform-specific digest through the
cache while capacity is provisioning. `managed_required` and locality
`require` may wait or fail intentionally and are opt-in only after readiness
has been proven.

The API server should expose timing for selector resolution, placement
resolution, intent enqueue, copy queue delay, copy duration, and runtime pull.
The regression gate is that managed-image placement adds no registry or cloud
network write to the direct deployment critical path.

## Infrastructure bootstrap

Reusable infrastructure should live in a versioned
`terraform-aws-skypilot` module repository, with one thin root composition and
three independently consumable lifecycle boundaries:

| Module | Responsibility |
|---|---|
| `modules/control-plane` | Deploy or configure the SkyPilot Helm release on an existing EKS cluster, bind Pod Identity, PostgreSQL, secrets, and worker configuration. |
| `modules/vm-pool` | Register one target AWS account for direct EC2: provisioner role, instance profile, SSM access, and least-privilege ECR pull permissions. |
| `modules/image-distribution` | Create canonical immutable ECR namespaces, cross-account/Region pull-through cache rules, repository-creation templates, copy/prewarm IAM, and registry-profile outputs. |

An example composition can add optional VPC, EKS, and PostgreSQL modules, but
the reusable modules should accept those resources rather than owning an entire
AWS estate. Each account/Region instance receives a caller-supplied aliased AWS
provider; credentials never become module inputs or outputs. Outputs are
secret-free role ARNs, registry endpoints, instance-profile names, and a
registry-profile fragment that can be merged into SkyPilot configuration.

The starting point is Boltz Platform `origin/main` at
`5331f1505c842bce9a45200d99cb49e358bd50f3`: `ecr-distribution`,
`ecr-pull-through-cache`, `skypilot_control_plane`, and
`skypilot_pool_aws_vm`. Extraction must parameterize Boltz-specific repository
names, accounts, IAM boundaries, retention, and workspace wiring. Boltz
Platform should then pin the reusable module and migrate existing resources
with explicit `moved` or import guidance instead of remaining a forked source
of truth.

Cloudflare remains a separate optional module/provider boundary so AWS-only
installations do not inherit Cloudflare credentials or Terraform dependencies.
Cloudflare's managed registry is the preferred R2-backed OCI option if its
canary passes. The current `cloudflare-r2` module and
`images/<model>/<tag>.tar.zst` layout remain an archive/emergency path; they are
not reused as the registry implementation. Self-hosting an OCI service on
Workers plus R2 is a fallback only if the managed registry cannot satisfy the
external-pull contract.

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
- A cold pull-through endpoint is addressable but is not labeled as a verified
  local location until the expected digest is observed at the downstream
  endpoint.
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

1. Apply the reusable Terraform modules to provision the canonical repository,
   AWS account/Region cache scaffolding, worker identity, VM pull identity, and
   registry-profile outputs in a staging workspace.
2. Deploy the reconciliation worker with bounded concurrency, rate limits,
   metrics, and dead-letter visibility.
3. Configure a revisioned profile under `managed_preferred` and `locality:
   prefer`.
4. Import a digest-pinned canary image and prove canonical, copied regional,
   and cold/warm pull-through behavior without making placement wait.
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
- Serve-controller tests restore the in-process controller marker after each
  test. The exact worker-order reproducer and the combined Serve-controller and
  managed-image suites pass, so catalog-authority checks are no longer affected
  by unrelated test process state.
- The exact latest Boltz L4 fleet YAML parses all 254 candidates unchanged and
  produces `artifact_ids=[]`.
- YAPF, isort, mypy, Pylint, Ruff, BasedPyright, the flake8 async-lifecycle
  ratchet, dashboard lint, Prettier, Python compilation, and `git diff --check`
  pass.
- All 20 visible GitHub checks passed on code-bearing implementation head
  `62cd18974af18b34c8e5c3846fc1010ed0257e40` before this plan-only update.
  The PR check rollup is authoritative for the latest exact-head status, and
  every subsequent plan or code update must pass it before merge.

## Open decisions and follow-up plans

- The provider bootstrap and worker deployment must receive their own finalized
  implementation plan before production work begins. The AWS work should use
  the module split above and begin from the current Boltz Platform resources.
- The companion Boltz fleet conversion plan must specify the replacement for
  the current host-mode R2 tarball path and preserve the deliberate per-GPU
  process launcher.
- A Cloudflare canary must decide whether the managed registry is promoted to a
  native adapter. Raw R2 and a self-hosted Workers registry are not the default.
- Image construction remains in the separate, unimplemented
  `sky/design_docs/proposals/managed_container_image_builder.md` proposal. If
  accepted, it must become its own feature plan rather than expanding this
  distribution worker's responsibilities.

## Change log

- 2026-07-18: Defined raw R2 versus R2-backed OCI support, made deployment-path
  non-blocking behavior an explicit invariant, modeled lazy pull-through routes
  separately from verified copies, and specified reusable AWS Terraform module
  boundaries based on current Boltz Platform infrastructure.
- 2026-07-18: Scoped the controller process marker to each Serve-controller
  unit test after the exact-head full suite exposed a pytest-worker environment
  leak. Production controller behavior is unchanged.
- 2026-07-18: Merged the current `improvements` base and refreshed only shifted
  line numbers in the new async-lifecycle baseline after confirming identical
  files, rules, columns, and findings.
- 2026-07-18: Recorded the implemented control-plane contract, compatibility
  behavior, exact-head verification, and outstanding production activation
  gates for PR #368.
