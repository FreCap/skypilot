# Managed container image distribution

Status: accepted reshape, implementation in progress

Owner: SkyPilot control plane

Last updated: 2026-07-20

## Decision

SkyPilot will provide a small, portable image-distribution control plane built
on standard OCI registries. An image is identified by an immutable digest. An
explicit publication operation adopts that digest, prepares one canonical
registry location asynchronously, and makes an optional human-readable release
visible only after the canonical location is verified READY. Workload
deployment consumes published identity and may request one placement-specific
cache intent. Deployment never discovers a mutable tag, registers a source, or
publishes a release.

The first managed provider is AWS ECR. GCP, Kubernetes, Nebius, and other
providers use externally provisioned OCI profiles until their own managed
adapter, infrastructure, IAM, and negative tests qualify. Cloudflare R2 and
other S3-compatible object stores may hold build contexts, logs, attestations,
or model artifacts, but are not represented as OCI registries.

This is intentionally not a clone of Modal's runtime. Modal controls a global
content-addressed filesystem, lazy loading, and snapshot mechanisms. Portable
SkyPilot v0 provides safer immutable publication, placement-aware caching, and
operational automation. It makes a cold-start performance claim only after a
representative fleet benchmark proves one.

## Why this is worth building

Large services currently pay repeatedly for cross-region pulls and ad hoc image
preparation. Operators cannot tell whether a digest is ready in a target region,
which identity may copy or delete it, or which deployment still references it.
The smallest useful product centralizes those answers while leaving bytes in
ordinary registries:

1. publish one exact digest before deployment;
2. materialize only observed or explicitly requested locations;
3. keep copy work off API and placement workers;
4. pin one verified location into each durable deployment;
5. make warming, failure, retry, and worker readiness visible; and
6. evict only unreferenced regional cache manifests with a separate identity.

The honest baseline remains CI-built digest-pinned images in an existing
registry. A managed profile must beat that baseline in either measured startup
performance or operator effort before activation.

## Release boundaries

### V0

V0 contains:

- digest-only source adoption and immutable release publication;
- account-level default profiles and workspace policy/allowlists;
- asynchronous canonical and just-in-time regional materialization;
- one managed AWS ECR adapter and externally provisioned generic profiles;
- fixed, Terraform-created AWS repository shards;
- separate API, copy-worker, lifecycle-worker, and workload identities;
- durable deployment references and reference-aware regional eviction;
- node-scoped image resolution for multi-GPU and multi-node workloads;
- bounded paginated APIs and a complete operational Images UI;
- copy and lifecycle worker Helm deployments, health, metrics, and recovery;
- PostgreSQL-only central image state; and
- fleet measurements as an activation gate, not a merge prerequisite.

### Explicit post-v0 seams

The design preserves interfaces for, but v0 does not productize:

- a managed builder with content-addressed layer cache and late-bound source;
- R2-backed build contexts, logs, attestations, and model objects;
- credential-aware mutable-tag resolution in an isolated metadata worker;
- managed GAR, Nebius, or other provider provisioning;
- mutable named channels with one generationed deployment snapshot;
- repository-generation expansion beyond the fixed Terraform layout;
- OCI indexes and additional architectures selected from declared targets;
- lazy snapshotter integrations and a separate model-data plane; and
- memory or GPU snapshots.

The linked
[`managed-container-image-builder.md`](managed-container-image-builder.md)
owns the builder seam. It may produce only the READY artifact contract defined
here.

### Not part of this product

- raw R2 bucket references as runtime images;
- eager copy to every configured region;
- transparent mid-pull registry failover;
- build inference from arbitrary task `setup` commands;
- one image pull or copy per GPU process;
- speculative ARM64 or GPU-specific builds;
- browser-executed Terraform;
- an internal proprietary registry; or
- claims of Modal-style lazy startup on an ordinary OCI runtime.

## Terminology and ownership

| Concept | Meaning | Owner |
| --- | --- | --- |
| Artifact | Workspace-scoped immutable OCI digest | catalog repository |
| Source | Exact digest-pinned OCI reference used to import an artifact | publication service |
| Publication | Requested adoption and optional release reservation | publication service |
| Release | Human-readable immutable alias visible only after verification | publication service |
| Profile | Complete registry topology and policy snapshot | server configuration |
| Location | One digest in one physical registry target | materialization service |
| Pull plan | Secret-free, placement-specific READY location snapshot | runtime resolver |
| Reference | Durable consumer fence preventing location eviction | reference service |
| Copy worker | Claims copy/verify work and can write manifests | materialization worker |
| Lifecycle worker | Claims eligible regional eviction work and can delete manifests | lifecycle worker |

The implementation is split along those boundaries:

```text
sky/container_images/
  models.py                 value objects and validators
  config.py                 profile and workspace policy snapshots
  catalog_state.py          artifact, source, publication, release persistence
  materialization_state.py  location, lease, verification, and retry persistence
  reference_state.py        durable consumer references and eviction eligibility
  publication.py            explicit publication service
  runtime.py                read-only workload resolution and warming dependency
  providers.py              portable adapter contracts
  aws.py                    qualified ECR adapter
  copy_worker_service.py    independently deployed copy loop
  lifecycle_worker_service.py independently deployed deletion loop
  api.py                    typed direct reads and asynchronous mutations
  state.py                  temporary compatibility facade only
```

Business services never issue raw SQL outside the repository module they own.
Provider adapters do no catalog writes. API handlers create intent or project
state, but never copy or delete content.

## User interface

### Workload YAML

The public field is `resources.container_image`. A scalar is the direct OCI
source form:

```yaml
resources:
  container_image: ghcr.io/boltz-bio/boltz@sha256:<64-hex-digest>
```

The object form selects one immutable identity and optionally a distribution:

```yaml
resources:
  container_image:
    release: boltz-2.4.1
    distribution: gpu-production
```

Supported keys are exactly:

- `ref`: one digest-pinned OCI reference;
- `release`: one immutable workspace release;
- `artifact_id`: one SkyPilot artifact UUID; and
- `distribution`: one configured profile or the explicit `direct` escape hatch.

`artifact_id` is exclusive with `ref` and `release`. `ref` and `release` may be
combined only to prove that an already published release has the expected
source. A task cannot create that release. The unreleased `profile` and
`version` aliases are removed.

The legacy `image_id: docker:...` form retains direct-pull behavior. It does not
opt into managed distribution and is not silently reinterpreted by a server
default.

### CLI and SDK

```text
sky image publish SOURCE@sha256:DIGEST \
    --release NAME --distribution PROFILE [--no-wait]
sky image status [SELECTOR] [--workspace W]
sky image prepare SELECTOR --distribution PROFILE --target TARGET... [--no-wait]
sky image retry SELECTOR --distribution PROFILE --target TARGET [--no-wait]
```

`publish` is the only public source-adoption operation. There is no `register`
alias. With `--no-wait`, it returns the asynchronous request and publication
identifier immediately. Without it, the client waits for canonical verification
and returns the public release. Request retry uses an idempotency key.

`prepare` creates only the explicitly selected target intent. If the target
depends on a canonical location that is not READY, it returns the canonical
dependency and does not create a second intent in that call.

### Defaults

Selection precedence is:

1. task `distribution`;
2. workspace `container_images.default_profile`; and
3. server `container_registries.default_profile`.

Profiles are complete atomic objects. Workspace configuration may select a
default, restrict `allowed_profiles`, and choose:

- `managed_required`: unknown or unready managed identity waits or fails closed;
- `managed_preferred`: an exact request-supplied digest may pull directly while
  a known artifact location warms; or
- absence of image policy: preserve direct behavior.

Locality is `prefer`, `require`, or `canonical`. `direct` is allowed only as an
explicit task choice under `managed_preferred`.

## Publication contract

Publication is independent of workload deployment.

1. Validate a digest-pinned source, release, workspace, and complete active
   profile before persistence.
2. In one transaction, converge the artifact and source, reserve the release as
   PENDING, and create or reuse one canonical location intent.
3. A copy worker claims the canonical location with a random fenced lease,
   obtains short-lived credentials, copies the exact digest, and verifies the
   destination digest and OCI platform metadata.
4. The completion transaction marks the location READY and changes matching
   PENDING release reservations to READY. Only then can release lookup return
   the artifact.
5. A terminal pre-READY copy failure marks the reservation FAILED without
   exposing the release. Retry returns the same reservation to PENDING.

An existing READY release is immutable. A conflicting digest is rejected. A
failed replacement never changes another release or any deployment already
pinned to an older artifact.

Mutable tags are rejected in v0. Documentation must not promise tag resolution.
A later isolated resolver may accept a credential reference, resolve a tag to a
digest, discard the credential, and submit the same publication transaction.

## Deployment and warming contract

Workload resolution is read-only with respect to artifacts, sources,
publications, and releases.

- An unknown `release` or `artifact_id` fails with `IMAGE_NOT_PUBLISHED`.
- An unknown digest `ref` under `managed_required` fails with the same closed
  error and points to `sky image publish`.
- An unknown digest `ref` under `managed_preferred` may remain a direct pull. It
  does not create catalog state.
- A known artifact may create at most one missing location intent for the
  selected placement in one resolution attempt.
- Provider calls and OCI transfers occur only in workers.

When `locality: require` has no READY local route, the resolver raises the typed
`ContainerImageWarmingError`. It carries artifact, profile, target, location,
and retry metadata but no credential or raw untrusted value. The provisioning
loop treats it as a pending execution dependency with `no_failover=True`.
SkyServe and managed jobs keep retrying the same selected placement. A normal
launch uses the existing `--retry-until-up` behavior or reports the precise
prepare command. The dashboard and cluster events say `IMAGE_WARMING`, not
`resources unavailable`.

With `managed_preferred` plus `locality: prefer`, the exact request-supplied
digest can be used immediately if its pull authentication is valid for the
placement. Release-only and artifact-only selectors never infer or expose a
source fallback.

The runtime commits a secret-free pull plan only after a READY route is selected.
It rechecks profile revision, target fingerprint, digest, platform, auth strategy,
and location state at the durable cluster commit. Restarts keep a still-valid
plan or atomically replace it with another READY plan. They never persist a
WARMING fallback as managed locality.

## Multi-node, multi-GPU, and architecture behavior

Image distribution is node-scoped. One EC2 instance with eight GPUs pulls one
image through its container runtime cache. Starting one process per GPU is the
workload or serving runtime's responsibility. Distribution does not multiply
copy work by GPU count, replica process count, or task rank.

A multi-node task stores one pull plan per placement shape, not per GPU. All
nodes in that placement use the same digest. Kubernetes still creates pods
normally and relies on node-level containerd caching.

V0 supports only a single verified image manifest per publication. The worker
records its nonempty OCI platform set. Placement fails closed when a known
runtime architecture is not covered. Unknown architecture may use an artifact
only when the cloud/backend binding declares a finite platform set and the
artifact covers it. V0 does not build ARM64 speculatively. A later OCI-index
feature must model parent and child manifest ownership before publication.

## PostgreSQL data model

Central image state is PostgreSQL-only. Local and controller databases retain
their existing SQLite support.

Migration 023 is a literal additive migration. It does not import live ORM
metadata. It creates only:

```text
container_image_catalog
container_image_profile_revisions
container_images
container_image_sources
container_image_releases
container_image_locations
container_image_references
container_image_workers
```

The catalog singleton contains only a stable authority UUID and creation time.
There is no forced RLS policy, API-version GUC, database-wide advisory lock,
global configuration apply ledger, realm generation, dynamic shard allocation,
catalog projection, facet table, or custom lifetime quota in v0.

Important constraints include:

- unique `(workspace, source_digest)` artifact identity;
- unique `(workspace, source_ref)` source alias;
- unique `(workspace, release)` reservation;
- release state in `PENDING|READY|FAILED`;
- READY release requires a canonical location ID for the same artifact;
- unique physical location identity for artifact/profile/target/fingerprint;
- canonical versus regional dependency checks;
- closed location state and lease combinations;
- unique durable consumer reference; and
- worker kind in `COPY|LIFECYCLE` with bounded heartbeat metadata.

All queue discovery is bounded and indexed by state, retry time, lease expiry,
and ID. Claim uses `FOR UPDATE SKIP LOCKED`. Provider I/O occurs outside the
claim transaction. Completion validates the random lease token after acquiring
the row lock and reading the current clock.

The downgrade drops the tables only when all are empty and the feature has no
active profile. Otherwise it fails closed. Because the feature has not shipped,
there is no compatibility reason to preserve the earlier branch-only schema.

## Registry profiles

A profile is a complete immutable revision:

```yaml
container_registries:
  default_profile: gpu-production
  profiles:
    gpu-production:
      revision: 1
      ownership: managed
      provider: aws
      canonical:
        account: "123456789012"
        region: us-east-1
        registry: 123456789012.dkr.ecr.us-east-1.amazonaws.com
        repository_prefix: skypilot-images
        shard_count: 16
        pull_auth: ecr_runtime_identity
      targets:
        - name: us-west-2
          account: "123456789012"
          region: us-west-2
          registry: 123456789012.dkr.ecr.us-west-2.amazonaws.com
          repository_prefix: skypilot-images
          shard_count: 16
          pull_auth: ecr_runtime_identity
```

Externally provisioned profiles use `ownership: external`, an OCI registry
provider binding, and no deletion authority. Kubernetes contexts explicitly map
to one registry target and pull-auth strategy. VM targets declare the provider
and region localities they satisfy. A generic endpoint is never declared local
merely because it is reachable.

Semantic changes require a higher explicit revision. Existing durable pull
plans remain valid while their exact target and auth contract remains usable.
New placement uses only the active revision. Config validation checks every
profile atomically before activation and rejects ambiguous locality, duplicate
targets, or incompatible authentication. V0 uses the existing server config
reload path and feature-gated rolling deployment. It does not create a second
global configuration transaction protocol.

## AWS managed slice

### Fixed repository layout

Terraform creates every v0 repository before profile activation. For each
declared workspace, region, and shard index, the name is deterministic:

```text
<prefix>/<workspace>/s<two-hex-index>
```

The shard is selected by stable digest hashing. `shard_count` is immutable for a
profile revision and must be between 1 and 256. Capacity is checked during
Terraform planning against the applied ECR repository and images-per-repository
quotas with explicit headroom. Supporting a million retained manifests normally
requires multiple fixed shards. Expanding beyond the layout creates a new
post-v0 profile revision and explicit migration; v0 does not create repositories
during placement or copy.

No Terraform action copies image content. Canonical and regional manifests are
created only from durable intents.

### IAM boundary

The module creates or accepts four distinct roles:

- API role: metadata reads and intent writes in PostgreSQL, no ECR writes;
- copy-worker role: ECR read, layer upload, and `PutImage` in the fixed prefix,
  no manifest deletion or repository administration;
- lifecycle-worker role: describe and `BatchDeleteImage` for eligible regional
  cache manifests, no push or repository deletion; and
- workload role: token and pull operations only.

Repository creation/deletion, registry-policy mutation, IAM, KMS, and account
administration stay with Terraform. The module may manage an ECR registry V2
policy only when it is the declared sole owner. Otherwise the profile is
external and SkyPilot makes no managed-custody claim.

Negative tests grant a probe principal broad identity permissions and prove the
resource boundary still rejects writes outside the copy role, deletes outside
the lifecycle role, and every repository deletion path.

### Terraform deliverables

```text
infra/terraform/modules/aws-image-distribution
infra/terraform/modules/aws-image-worker-identity
infra/terraform/examples/aws-dedicated-skypilot-account
```

The distribution module accepts account ID, regions, workspaces, prefix, fixed
shard count, encryption/scanning settings, quota headroom, and optional existing
role ARNs. It outputs secret-free profile YAML, repository ARNs, role ARNs, and
readiness checks. The example composes PostgreSQL/API infrastructure already
owned by the platform with these modules. It does not duplicate database state.

## Worker services

Helm exposes two disabled-by-default deployments:

```text
imageCopyWorker.enabled
imageCopyWorker.replicaCount
imageCopyWorker.maxInFlight
imageCopyWorker.serviceAccount

imageLifecycleWorker.enabled
imageLifecycleWorker.replicaCount
imageLifecycleWorker.maxInFlight
imageLifecycleWorker.serviceAccount
```

Each pod registers a random worker ID and periodically upserts a bounded
heartbeat with kind, version, started time, last-success time, and current
in-flight count. The UI treats a heartbeat as stale after three periods. Stale
rows are compacted. Heartbeats contain no hostname, token, ARN, or credential.

Copy-worker concurrency is bounded by its pod setting and provider throttling.
Adding replicas increases claim throughput safely because leases and
`SKIP LOCKED` prevent duplicate authority. Lifecycle workers claim only
reference-free, noncanonical, managed locations past retention. They inspect
the exact digest after ambiguous deletion and never delete a repository.

Shutdown stops new claims, cancels work that has not started provider I/O, and
lets leases expire after ambiguous I/O. Restart recovery verifies actual
registry state before completion or retry.

## Dashboard

The Dashboard contains a first-class Images navigation item and two complete
surfaces.

### Images catalog and detail

All authorized users can:

- select a workspace;
- page artifacts with digest, releases, platforms, size, and updated time;
- filter by release, digest, distribution, target, and location state;
- open artifact detail with sources, release reservations, locations, errors,
  references, and publication history;
- start Publish, Prepare, Retry publication, and Retry location actions when
  authorized;
- follow asynchronous request progress without duplicate submission; and
- see empty, loading, stale-cursor, permission, old-server, and error states.

The UI never fetches credentials or raw server configuration. It displays
bounded, code-valued errors and copyable remediation commands.

### Image distribution readiness

Administrators get a read-only Settings panel showing:

- active secret-free profile revisions and workspace defaults/allowlists;
- Terraform-produced repository and role readiness;
- copy and lifecycle worker healthy/stale counts;
- queue depth and oldest pending/retry age by profile/target; and
- capability failures that prevent managed-profile activation.

V0 deliberately has no browser profile editor. Operators change versioned
configuration and Terraform through normal GitOps, then use the panel to verify
convergence. This removes a second configuration transaction system without
making the feature raw-YAML-only operationally.

### Direct read API

```text
GET /images/catalog?workspace=W&limit=50&cursor=C
GET /images/artifacts/{id}?workspace=W
GET /images/artifacts/{id}/releases?workspace=W&limit=50&cursor=C
GET /images/artifacts/{id}/locations?workspace=W&limit=50&cursor=C
GET /images/profiles?workspace=W
GET /images/workers?workspace=W
```

Reads use opaque versioned keyset cursors bound to workspace and filters. Limit
is 1 through 100. A cursor from another workspace, filter, profile revision, or
server version fails closed. Responses bound associations; detail collections
remain paginated. No dashboard read creates a generic request row.

## Security and privacy invariants

- Runtime references are digest-pinned.
- Credential values never enter YAML, PostgreSQL, request rows, logs, command
  arguments, API responses, or dashboard state.
- Source authentication is a named secret reference resolved only inside the
  isolated worker. V0 supports only source paths with a qualified implementation.
- Provider errors are mapped to bounded codes before persistence.
- API, copy, lifecycle, and workload identities are non-interchangeable.
- Managed deletion is allowed only for noncanonical regional content with no
  live durable reference.
- External profiles never grant SkyPilot deletion authority.
- Workspace authorization is checked before selector lookup to avoid existence
  disclosure.

## Rollout

1. Merge literal schema, typed API, worker images, UI, Terraform, and tests with
   managed profiles disabled.
2. Deploy API replicas and run migration 023.
3. Apply Terraform in a dedicated AWS account or an isolated fixed prefix.
4. Deploy one copy worker and one lifecycle worker with separate identities.
5. Activate one profile for one test workspace and publish one digest.
6. Verify publish, warming, pull, restart, retry, and reference-fenced eviction.
7. Convert the Boltz L4 test fleet and compare direct cross-region pulls,
   operator-prewarmed images, and managed JIT locations.
8. Enable production only if the operations or performance gate passes.

Rollback disables new publication and worker claims, keeps existing digest pull
plans usable, and preserves PostgreSQL intent. External digest-pinned profiles
remain the escape hatch. A migration downgrade is permitted only before any
image row exists.

## Acceptance gates

### Core invariants

- A workload using a release performs no source registration or release
  mutation, and no provider call in the API or placement process.
- A release lookup returns nothing until its canonical location is READY.
- A failed publication leaves every prior release and deployment launchable.
- One placement attempt creates at most one location intent and warming never
  causes cloud or region failover.
- Copy crashes before and after manifest publication converge to one verified
  digest.
- At 1,000 replicas and eight GPUs per node, copy cardinality equals requested
  physical targets, not replicas, nodes, or GPUs.
- Regional eviction cannot pass a concurrent reference acquisition or canonical
  dependency fence.

### Required verification

- real PostgreSQL migration, concurrency, lease, retry, and downgrade tests;
- old-server/new-client and new-server/old-client feature-gate tests;
- AWS integration plus negative IAM tests;
- `terraform fmt -check`, `terraform validate`, and plans for one and multiple
  regions with fixed shards;
- worker kill/restart tests around every provider-I/O boundary;
- Jest interaction, pagination, permission, responsive, and stale-state tests;
- a production Next.js build;
- repository formatting and focused backend tests; and
- 100, 500, and 1,000-replica timing evidence before a speed claim.

Managed production activation requires either:

- image-ready p95 at least 40 percent lower than direct cross-region pull with
  no regression above 10 percent versus operator-prewarmed nodes; or
- no material speed claim, but verified automatic preparation, zero credential
  leakage, bounded recovery, and at least 95 percent reduction in manual image
  preparation steps.

## Adversarial review protocol

Each material design or implementation change is reviewed at one immutable git
commit by both Codex 5.6 and Claude Fable. A round counts only when both inspect
the same commit and return `PURSUE`, `RESHAPE`, or `DROP`. An unavailable model is
reported and is not replaced. Findings are fixed as one coherent batch before
the next round. Completion requires three consecutive paired `PURSUE` rounds,
with no more than six additional paired rounds in this review cycle.

Round 1 at `c1a8ce729c82cc606eef5cfd62abdf9fa9d7fd8e` returned Codex `RESHAPE`.
Fable was unavailable because its CLI account reported no usage credits, so the
round is not counted as paired. This document incorporates the decision-changing
Codex findings: narrow v0, read-only deployment publication, typed warming,
READY-gated releases, fixed Terraform repositories, early component separation,
removal of unreleased aliases, and a narrow complete UI.
