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
| Publication | Durable adoption attempt and optional release reservation | publication service |
| Release | Human-readable immutable alias created only after verification | publication service |
| Profile | Complete registry topology and policy snapshot | server configuration |
| Registry shard | One preprovisioned physical repository and its hard admission budget | shard repository |
| Location | One digest in one physical registry target | materialization service |
| Dependency | Durable placement pin while a logical workload waits for one location | runtime transaction service |
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
  shard_state.py            physical repository admission and drift persistence
  materialization_state.py  location, lease, verification, and retry persistence
  dependency_state.py       durable warming pins and READY pull plans
  reference_state.py        durable consumer references and eviction eligibility
  transactions.py           the two cross-repository PostgreSQL transitions
  publication.py            explicit publication service
  runtime.py                read-only workload resolution and warming dependency
  providers.py              portable adapter contracts
  aws.py                    qualified ECR adapter
  copy_worker_service.py    independently deployed copy loop
  lifecycle_worker_service.py independently deployed deletion loop
  api.py                    typed direct reads and asynchronous mutations
  state.py                  temporary compatibility facade only
```

Repository functions accept a caller-owned SQLAlchemy session and never commit
it. `transactions.py` is the only cross-repository transaction boundary. Its
small public surface owns publication creation/convergence, dependency
creation/READY commit, and reference-fenced eviction. It owns no tables and does
no provider I/O. Repositories do not call each other, which prevents the split
from recreating a cyclic monolith. Business services never issue raw SQL.
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
and returns the artifact ID, publication ID, and optional public release. Request
retry uses an idempotency key; `--release` is optional.

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

1. Validate a digest-pinned source, optional release, workspace, and complete
   active profile before persistence. The request hash covers all of those
   fields, including the exact profile revision.
2. In one transaction, converge the artifact and source, create a durable
   PENDING publication, reserve its optional release name through a unique
   publication constraint, and create or reuse one canonical location intent.
   If that location is already READY, the same transaction completes the
   publication and inserts the release.
3. A copy worker claims the canonical location with a random fenced lease,
   obtains short-lived credentials, copies the exact digest, and verifies the
   destination digest and OCI platform metadata.
4. `transactions.converge_canonical()` locks the location, records READY or
   terminal FAILED, then locks dependent PENDING publications in ascending ID
   batches. Each publication becomes READY and gains its immutable release, or
   becomes FAILED, in the same transaction that rechecks the exact canonical
   result. Release lookup therefore cannot observe a pending alias. Remaining
   publications stay on an indexed reconciliation queue, so a crash or a fan-out
   larger than one batch resumes deterministically without repeating provider
   I/O.
5. Retry locks the shared canonical location before its dependent publications.
   It returns the same publication to PENDING and reuses the location. A retry of
   one failed dependent makes every still-retained dependent eligible to
   converge from the shared result; it never creates a second physical copy.

An existing READY release is immutable. A conflicting digest is rejected. A
failed replacement never changes another release or any deployment already
pinned to an older artifact.

Publication collision behavior is complete and server-enforced:

| Condition | Result |
| --- | --- |
| Same `(workspace, idempotency_key)` and request hash | Return the same publication in its current state |
| Same idempotency key and a different request hash | Reject with `IDEMPOTENCY_KEY_REUSED` |
| Same release reservation, digest, and profile revision | Return the reserving publication, even with a different idempotency key |
| Same release with another digest or profile revision | Reject with `RELEASE_CONFLICT`; use `prepare` for another distribution |
| Different releases for the same digest and profile revision | Create distinct publications sharing one canonical location |
| No release and a new idempotency key | Create one retention-bounded publication sharing any existing canonical location |

`(workspace, idempotency_key)` is unique and keys are 16 through 128 bytes.
Release-backed successful publications are retained catalog facts. An active
failed release reservation expires after 30 days without retry; its
`reservation_active` flag is cleared, so a name that was never publicly visible
can be requested again. The failed publication remains visible for 90 days and
is then compacted. Terminal release-less publications are retained for 30 days,
which exceeds the seven-day idempotency replay guarantee. The lifecycle worker
processes at most 500 expirations or deletions per sweep. There is no separate
unbounded publication-attempt table.

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

When `locality: require` has no READY local route, resolution first persists a
server-owned dependency for the logical provisioning attempt. It contains the
catalog authority, artifact, exact profile revision, target fingerprint,
location, a bounded placement constraint, consumer generation, and retry epoch.
It contains no credential or raw untrusted value. Consumer identities are
derived by the server from the cluster launch generation, managed-job recovery
generation, or Serve replica generation; users cannot supply them in YAML.

Only after that commit does the resolver raise the typed
`ContainerImageWarmingError`. The same-call provisioning loop also sets
`no_failover=True`, but the durable dependency is authoritative. Before every
new optimization, normal launch, SkyServe, and managed-job controllers reload
it and restrict candidates to its target. An API or controller restart therefore
cannot reoptimize into another cloud and create another warming intent. The
dashboard and events say `IMAGE_WARMING`, not `resources unavailable`.

If materialization fails terminally, the controller reports
`IMAGE_PREPARATION_FAILED` for that target. It does not reinterpret that failure
as capacity. After a READY plan, a genuine capacity failure may explicitly
supersede the dependency, increment the consumer generation, and optimize a new
placement. This distinction preserves ordinary recovery without allowing image
warming itself to cause failover.

With `managed_preferred` plus `locality: prefer`, the exact request-supplied
digest can be used immediately if its pull authentication is valid for the
placement. Release-only and artifact-only selectors never infer or expose a
source fallback.

The runtime commits a secret-free pull plan only after a READY route is selected.
`transactions.commit_ready_dependency()` locks the location, then the dependency,
inserts or converges its reference, and stores the plan in one PostgreSQL
transaction. It rechecks profile revision, target fingerprint, digest, platform,
auth strategy, lease-free READY state, and consumer epoch. Central dependency
state is the durable source for normal launch, Serve, and managed-job controllers,
so their own SQLite-compatible state stores only the dependency ID and generation.
Restarts keep a still-valid plan or explicitly supersede it after a real capacity
failure. They never persist a WARMING fallback as managed locality.

Eviction treats a live WARMING dependency as a fence before a reference exists.
Reference acquisition and eviction both lock the same location row. Consumer
terminal handling releases dependency and reference together. A reconciler may
expire a WARMING normal-request dependency only after the request is terminal
and at least 24 hours old; active job, service, or cluster dependencies require
an observed terminal consumer generation and are never removed by age alone.

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
container_image_publications
container_image_releases
container_image_registry_shards
container_image_locations
container_image_dependencies
container_image_references
container_image_workers
```

The catalog singleton contains only a stable authority UUID and creation time.
There is no forced RLS policy, API-version GUC, runtime-wide advisory lock,
global configuration apply ledger, realm generation, dynamic repository
creation, catalog projection, or facet table in v0. A physical shard row is the
small admission primitive required to prove that a fixed repository cannot be
overfilled; it is not a workspace billing or product quota.

Important constraints include:

- unique `(workspace, source_digest)` artifact identity;
- unique `(workspace, source_ref)` source alias;
- unique `(workspace, idempotency_key)` plus a bounded request hash;
- unique non-null `(workspace, requested_release)` while
  `reservation_active`, retained forever for READY publications and expiring
  after 30 days for unretried FAILED publications;
- publication state in `PENDING|READY|FAILED`, with one canonical location and
  the collision behavior above;
- every release row points to the READY publication and artifact that created
  it, and no release row exists before that transaction;
- one row per physical repository shard with immutable fingerprint, hard
  manifest ceiling, reserved count, observed count, and
  `READY|FULL|DRIFTED|DISABLED` admission state;
- unique physical location identity for artifact/profile/target/fingerprint;
- canonical versus regional dependency checks;
- closed location state and lease combinations;
- one server-owned dependency per consumer generation and placement slot, with
  `WARMING|READY|FAILED|RELEASED` state and a bounded secret-free plan;
- unique durable consumer reference; and
- worker kind in `COPY|LIFECYCLE` with bounded heartbeat metadata.

All queue discovery is bounded and indexed by state, retry time, lease expiry,
and ID. Claim uses `FOR UPDATE SKIP LOCKED`. Provider I/O occurs outside the
claim transaction. Completion validates the random lease token after acquiring
the row lock and reading the current clock.

Every command that locks more than one image row uses this order:

1. profile revision and physical shard rows, ordered by ID;
2. artifact and source rows, ordered by ID;
3. canonical location before regional location, then location ID;
4. publication and release rows, ordered by ID;
5. dependency and reference rows, ordered by ID; and
6. a central durable consumer row, when normal cluster state participates.

Initial insert races rely on unique constraints and restart the transaction.
No repository function acquires an earlier class after a later one. Canonical
completion and publication retry both lock location before publication.
Reference acquisition and lifecycle eviction both lock location before checking
dependencies or references. This is the executable ownership contract for the
component split.

Migration 023 is run under a PostgreSQL migration-scoped advisory lock, not a
runtime control-plane lock. The downgrade itself can inspect only database
state, so it drops the tables only when every image table is empty. Draining all
023 processes is a separately verified operator precondition. Normal rollback
never downgrades. Because the feature has not shipped, there is no compatibility
reason to preserve the earlier branch-only schema.

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
        max_manifests_per_shard: 90000
        pull_auth: ecr_runtime_identity
      targets:
        - name: us-west-2
          account: "123456789012"
          region: us-west-2
          registry: 123456789012.dkr.ecr.us-west-2.amazonaws.com
          repository_prefix: skypilot-images
          shard_count: 16
          max_manifests_per_shard: 90000
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

`shard_count` is immutable for a physical target and must be between 1 and 256.
Terraform reads the applied ECR images-per-repository quota, reserves explicit
headroom, and emits `max_manifests_per_shard`. AWS documents a default adjustable
limit of 100,000 images per repository in its
[ECR service quotas](https://docs.aws.amazon.com/AmazonECR/latest/userguide/service-quotas.html).
A managed target activates
only when every Terraform-owned repository is empty, its fingerprint matches,
and its hard ceiling is no greater than the verified applied quota minus
headroom. A repository containing preexisting content must use external
ownership instead.

On first location creation, the transaction starts from a stable digest-derived
shard index and probes the fixed ring. It locks one physical shard row, rechecks
whether the location already exists, and reserves one slot only when
`reserved_count < max_manifests_per_shard`. The chosen shard is stored on the
location forever. A full ring fails closed with `REGISTRY_CAPACITY_EXHAUSTED`;
it does not try an undeclared repository. Reserved count is decremented only
after exact provider inspection proves that no manifest exists and no retained
publication or dependency can recreate it.

The lifecycle worker periodically compares paginated ECR inventory with state.
An in-flight or not-yet-written location consumes a reservation but is not
expected in inventory. An unexplained manifest, an observed count above reserved
count, or a missing manifest for a location recorded as present marks the shard
`DRIFTED` and stops new admission without breaking existing pulls. This makes
capacity a transactionally enforced boundary, rather than a probabilistic
hashing claim. At a 90,000 ceiling, twelve shards admit at least one million
manifests; the example uses sixteen for headroom. Expanding the fixed layout
requires Terraform plus a new profile revision. V0 never creates a repository
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
role ARNs. It reads applied repository and images-per-repository quotas when
permitted; otherwise it requires explicit validated quota inputs and makes the
readiness check fail until they are supplied. It outputs secret-free profile
YAML, immutable repository fingerprints and ceilings, repository ARNs, role
ARNs, and readiness checks. The example composes PostgreSQL/API infrastructure
already owned by the platform with these modules. It does not duplicate database
state.

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
rows older than 24 hours are deleted by the lifecycle worker in batches of at
most 500 every five minutes. Heartbeats contain no hostname, token, ARN, or
credential. Worker reads are keyset-paginated, so a restart storm cannot produce
an unbounded API response while compaction catches up.

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
GET /images/artifacts/{id}/sources?workspace=W&limit=50&cursor=C
GET /images/artifacts/{id}/publications?workspace=W&limit=50&cursor=C
GET /images/artifacts/{id}/locations?workspace=W&limit=50&cursor=C
GET /images/artifacts/{id}/references?workspace=W&limit=50&cursor=C
GET /images/profiles?workspace=W
GET /images/workers?workspace=W&limit=50&cursor=C
GET /images/readiness?workspace=W
```

Reads use opaque versioned keyset cursors bound to workspace and filters. Limit
is 1 through 100. A cursor from another workspace, filter, profile revision, or
server version fails closed. Responses bound associations; detail collections
remain paginated. Profile validation permits at most 128 profiles and 256 targets
per profile. Readiness counts scan at most 10,001 indexed queue rows per target
and report `at_least: 10000` above that cap; oldest-age lookup is index-bounded.
No dashboard read creates a generic request row.

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
   the image feature and managed profiles disabled.
2. Run migration 023 once in a Helm migration Job. The Job holds the PostgreSQL
   migration advisory lock. Helm API pods run in `verify` mode, which refuses to
   start below 023 but never races to upgrade. Local single-server development
   may retain `auto` mode under the same PostgreSQL lock.
3. While old 022 API replicas still serve traffic, confirm they ignore the
   additive 023 tables. Roll every API replica to the new binary, then prove no
   old pod remains before feature activation.
4. Apply Terraform in a dedicated AWS account or an isolated fixed prefix and
   pass repository inventory, quota, fingerprint, and IAM readiness checks.
5. Deploy one copy worker and one lifecycle worker with separate identities.
6. Activate one profile for one test workspace and publish one digest.
7. Verify publish, warming, pull, API/controller restart, retry, capacity
   admission, drift fail-closed behavior, and reference-fenced eviction.
8. Convert the Boltz L4 test fleet and compare direct cross-region pulls,
   operator-prewarmed images, and managed JIT locations.
9. Enable production only if the operations or performance gate passes.

Rollback first disables profile activation and new publication, then stops
worker claims, and only then rolls old API binaries. Existing digest pull plans
remain usable and PostgreSQL intent is preserved. Old binaries tolerate the
additive 023 schema. External digest-pinned profiles remain the escape hatch.
Downgrade is a separate manual operation allowed only after every new process is
drained and every image table is empty; it is never part of Helm rollback.

## Acceptance gates

### Core invariants

- A workload using a release performs no source registration or release
  mutation, and no provider call in the API or placement process.
- A release lookup returns nothing until its canonical location is READY.
- A failed publication leaves every prior release and deployment launchable.
- One placement attempt creates at most one location intent and warming never
  causes cloud or region failover.
- `IMAGE_WARMING` survives API and controller restart with the same consumer
  generation, profile revision, target, and location.
- Copy crashes before and after manifest publication converge to one verified
  digest.
- At 1,000 replicas and eight GPUs per node, copy cardinality equals requested
  physical targets, not replicas, nodes, or GPUs.
- Regional eviction cannot pass a concurrent reference acquisition or canonical
  dependency fence.
- Every physical shard refuses admission at its hard ceiling, and provider drift
  stops new writes before the database can claim additional capacity.

### Required verification

- real PostgreSQL migration, concurrency, lease, retry, and downgrade tests;
- fresh-through-023 and literal 022-to-023 schema equivalence, concurrent
  migration-lock, and mixed-022/023 feature-disabled tests;
- old-server/new-client and new-server/old-client feature-gate tests;
- AWS integration plus negative IAM tests;
- `terraform fmt -check`, `terraform validate`, and plans for one and multiple
  regions with fixed shards;
- worker kill/restart tests around every provider-I/O boundary;
- idempotency collision-matrix, canonical publication fan-out, controller
  restart, shard-ceiling, and inventory-drift tests;
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

Round 2 at `64b6a6f8a7a402b33f8b9d9626fb60d71f47ff1d` returned Codex `RESHAPE`.
Fable again reported no usage credits, so the round is not paired. This revision
adds the exact cross-repository transaction owner and lock order, complete
publication convergence matrix, durable warming dependencies, singleton
migration rollout, hard physical-shard admission, bounded worker and publication
retention, and the builder handoff correction required by that review.
