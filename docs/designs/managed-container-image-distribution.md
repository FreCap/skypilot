# Managed container image distribution

Status: adversarial review in progress, implementation realignment pending

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

The v0 managed product is deliberately narrower: one dedicated-account AWS ECR
profile serving qualified EC2 and EKS runtimes. GCP, Nebius, generic Kubernetes,
and non-cloud servers retain unchanged direct digest-pinned OCI behavior. Their
provider-neutral adapters remain internal seams until one complete private-OCI
binding schema, installation flow, qualification protocol, and negative suite
qualifies. Cloudflare R2 and other S3-compatible object stores may hold build
contexts, logs, attestations, or model artifacts, but are not OCI registries.

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
- one managed AWS ECR adapter for qualified EC2 and EKS runtimes;
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
- external private OCI profiles for GAR, Nebius, Kubernetes, and non-cloud
  runtimes after one end-to-end binding qualifies;
- managed GAR, Nebius, or other provider provisioning;
- mutable named channels with one generationed deployment snapshot;
- repository-generation expansion beyond the fixed Terraform layout;
- full multi-platform OCI-index publication and additional architectures;
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
| Artifact | Workspace-scoped immutable runtime digest and platform | catalog repository |
| Source | Exact root reference and selected runtime manifest used to import an artifact | publication service |
| Publication | Durable adoption attempt and optional release reservation | publication service |
| Release | Human-readable immutable alias created only after verification | publication service |
| Profile | Complete registry topology and policy snapshot | server configuration |
| Access binding | Credential-free reference to one qualified read, write, pull, or delete authority | provider adapter |
| Qualification | Timestamped proof that one profile revision and its access bindings are usable | background worker |
| Registry shard | One preprovisioned physical repository and its hard admission budget | shard repository |
| Location | One digest in one physical registry target | materialization service |
| Demand | Durable placement pin shared by one logical deployment and target | demand transaction service |
| Pull plan | Secret-free, placement-specific READY location snapshot | runtime resolver |
| Reference | Durable consumer fence preventing location eviction | reference service |
| Copy worker | Claims copy/verify work and can write manifests | materialization worker |
| Lifecycle worker | Claims eligible regional eviction work and can delete manifests | lifecycle worker |

The implementation is split along those boundaries:

```text
sky/container_images/
  models.py                 value objects and validators
  config.py                 profile and workspace policy snapshots
  catalog_state.py          artifact, source, publication, and release aggregate
  topology_state.py         profiles, budgets, shards, locations, leases, workers
  demand_state.py           durable demands, pull plans, references, tombstones
  transactions.py           cross-repository PostgreSQL transitions
  publication.py            explicit publication service
  runtime.py                read-only workload resolution and warming demand
  providers.py              portable access, content-graph, and adapter contracts
  aws.py                    qualified ECR adapter
  copy_worker_service.py    independently deployed copy loop
  lifecycle_worker_service.py independently deployed deletion loop
  api.py                    typed direct reads and asynchronous mutations
  state.py                  temporary compatibility facade only
```

Repository functions accept a caller-owned SQLAlchemy session and never commit
it. `transactions.py` is the only cross-repository transaction boundary. Its
small public surface owns publication creation/convergence, demand
creation/READY commit, and reference-fenced eviction. It owns no tables and does
no provider I/O. Repositories do not call each other, which prevents the split
from recreating a cyclic monolith. Business services never issue raw SQL.
Provider adapters do no catalog writes. API handlers create intent or project
state, but never copy or delete content.

## User interface

### Workload YAML

The public field is `resources.container_image`. A scalar is the OCI `ref` form:

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

`distribution: direct` requires one digest-pinned `ref` and rejects `release` or
`artifact_id`; managed identity cannot be bypassed without its source. A scalar
does not itself opt into managed distribution. It follows the explicit activation
rules below.

The legacy `image_id: docker:...` form retains direct-pull behavior. It does not
opt into managed distribution and is not silently reinterpreted by a server
default.

### CLI and SDK

```text
sky image publish SOURCE@sha256:DIGEST \
    [--source-auth BINDING] [--release NAME] \
    --distribution PROFILE [--platform linux/amd64] [--no-wait]
sky image status [SELECTOR] [--workspace W]
sky image prepare SELECTOR --distribution PROFILE --target TARGET... [--no-wait]
sky image retry SELECTOR --distribution PROFILE --target TARGET [--no-wait]
sky image profile qualify PROFILE --manifest TERRAFORM.json [--no-wait]
```

`publish` is the only public source-adoption operation. There is no `register`
alias. `status` and every Dashboard catalog/readiness query are synchronous,
paginated reads that create no generic request row.

`--source-auth` names an allowed credential resolver binding, never a secret.
The publication persists only its binding ID and qualification fingerprint. A
public source omits the option. The Dashboard offers the same authorized binding
names without reading or returning their values.

`profile qualify` is an administrator-only, bounded upload of the secret-free
Terraform handoff. Helm deployments normally mount that handoff from a ConfigMap
for automatic background ingestion; the command supports non-Kubernetes control
planes. Neither path runs Terraform or waits for provider qualification inline.

`prepare` accepts only a READY artifact whose canonical location is verified. It
validates every selected target before creating any regional intent. A pending
publication fails with `ARTIFACT_NOT_READY` and a status remediation; callers do
not orchestrate a hidden two-call canonical protocol.

### Mutation contract

`publish`, `prepare`, `retry`, and `profile qualify` require a client-generated
idempotency key in the SDK or `Idempotency-Key` header. The CLI and Dashboard
generate a random key once per submitted form and reuse it after lost responses.
One bounded image-operation row is unique by catalog authority, workspace or
administrator scope, actor hash, mutation kind, and key. It stores the request
hash and only a typed, secret-free result projection. Same key and body returns
that operation; a body mismatch returns `IDEMPOTENCY_KEY_REUSED`.

Each mutation has one versioned typed result:

| Mutation | Stable result |
| --- | --- |
| Publish | operation, publication, optional artifact/release, state |
| Prepare | operation, artifact, ordered target/location states |
| Retry | operation, retried publication or location, state |
| Profile qualify | operation, profile/desired revision, manifest hash, state |

`--no-wait` returns the same result in its current nonterminal state. The default
wait polls by operation ID and returns the terminal form. A cancellation before
intent commit may cancel the request. After commit, Ctrl-C or request cancellation
only detaches the waiter: copy, verification, qualification, and ambiguous
provider I/O continue to convergence. Retry never guesses whether an external
write happened.

Terminal errors are code-valued and value-free: `IMAGE_NOT_PUBLISHED`,
`ARTIFACT_NOT_READY`, `PROFILE_NOT_ACTIVE`, `PLATFORM_UNSUPPORTED`,
`IMAGE_LOCALITY_UNSUPPORTED`,
`RELEASE_CONFLICT`, `IDEMPOTENCY_KEY_REUSED`, `AUTH_BINDING_UNAVAILABLE`,
`REGISTRY_CAPACITY_EXHAUSTED`, `IMAGE_PREPARATION_FAILED`,
`QUALIFICATION_FAILED`, and `PERMISSION_DENIED`. `IMAGE_WARMING` and provider
throttling are nonterminal states with bounded retry/ETA metadata. CLI and UI map
each code to one copyable remediation without reflecting provider or secret
values.

### API version and trust boundary

V0 raises the remote API version to 62. Every image SDK method uses the minimum
version decorator. Before sending a request, the new client recursively scans all
tasks and resource alternatives used by launch/exec, managed jobs, Serve
up/update, pools, and nested DAGs. If any explicit `container_image` crosses an
API 61 server, it fails locally with a capability error. The remediation may use
legacy `image_id` only when the user intentionally wants its unchanged direct
behavior; managed release semantics are never downgraded silently.

An old client talking to an API 62 server remains byte-for-byte unchanged and
continues to use `image_id`. New standalone image reads and mutations are simply
unavailable to it. Launch response shapes do not change for clients that did not
opt into the feature.

Server request validation rejects client-supplied pull plans, resolved runtime
digests, demand IDs, references, qualification records, and any field prefixed as
server-owned. It applies to REST bodies, serialized YAML, every resource
alternative, and persisted request revalidation. Request-scoped config overrides
cannot define or replace `container_registries`, access bindings, or workspace
image policy. Managed identity and authority are always selected from active
server state after workspace authorization.

### Defaults

Managed selection is explicit:

1. task `distribution: direct` bypasses managed state and requires a digest ref;
2. an explicit task profile opts in when the workspace allowlist permits it;
3. only a workspace in `managed_preferred` or `managed_required` may use its
   `container_images.default_profile`, then the server default; and
4. without a managed workspace mode or explicit task profile, a ref keeps direct
   OCI behavior and release/artifact selectors fail with `PROFILE_NOT_ACTIVE`.

A server default is therefore a default for opted-in workspaces, not a global
behavior switch. Profiles are complete atomic objects. Workspace configuration
may restrict `allowed_profiles` and choose:

- `managed_required`: unknown or unready managed identity waits or fails closed;
- `managed_preferred`: an exact request-supplied digest may pull directly while
  a known artifact location warms; or
- `direct` or absence of image policy: preserve direct behavior.

Locality is `prefer`, `require`, or `canonical` only for managed selection.
`distribution: direct` is allowed under `direct` or `managed_preferred`, never
under `managed_required`.

The workspace opt-in is explicit configuration, not task YAML:

```yaml
workspaces:
  production:
    container_images:
      mode: managed_required
      default_profile: gpu-production
      allowed_profiles: [gpu-production]
```

## Publication contract

Publication is independent of workload deployment. Managed v0 accepts either a
single image manifest or one selected child of an OCI index. `--platform`
defaults to `linux/amd64`; additional platforms are explicit, never speculative.

1. Validate a digest-pinned source root, requested platform, optional release,
   workspace, source access binding, and complete active profile. The request
   hash includes every field and exact profile revision.
2. In one transaction, create a durable PENDING publication and reserve its
   optional release. The canonical location remains null until source inspection;
   deployment still cannot observe a release.
3. A copy worker claims the publication with a random fenced inspection lease,
   moves it to INSPECTING, authenticates only to the source, and builds
   `OciContentGraph` before destination authority or I/O. A single manifest must
   match the requested platform. An index must contain exactly one matching
   runnable child. In one transaction, the worker rechecks the inspection token,
   persists the immutable source-root digest, selected child/runtime digest, and
   platform, converges the artifact/source, reserves one shard, creates or reuses
   its canonical intent, clears the inspection lease, and returns the bound
   publication to PENDING.
4. The worker separately acquires destination authority, copies only the selected
   manifest and referenced layers, then verifies the destination child digest and
   platform. It never uploads the source index in v0.
5. `transactions.converge_canonical()` locks the location, records READY or
   terminal FAILED, then locks dependent PENDING publications in ascending ID
   batches. Each publication becomes READY and gains its immutable release, or
   becomes FAILED, in the same transaction that rechecks the exact canonical
   result. Release lookup therefore cannot observe a pending alias. Remaining
   publications stay on an indexed reconciliation queue, so a crash or a fan-out
   larger than one batch resumes deterministically without repeating provider
   I/O.
6. A worker crash or ambiguous source read leaves INSPECTING until its lease
   expires; another worker then reinspects from the immutable source root. Retry
   of an unbound pre-inspection failure locks only its publication and requeues
   inspection. Once bound, retry locks the shared canonical location before
   dependent publications, returns retained failures to PENDING, and reuses the
   location. It never creates a second physical copy.

An existing READY release is immutable. A conflicting digest is rejected. A
failed replacement never changes another release or any deployment already
pinned to an older artifact.

Publication collision behavior is complete and server-enforced:

| Condition | Result |
| --- | --- |
| Same operation scope/key and request hash | Return the same publication operation in its current state |
| Same idempotency key and a different request hash | Reject with `IDEMPOTENCY_KEY_REUSED` |
| Same release reservation, source root, platform, and profile revision | Return the reserving publication, even with a different idempotency key |
| Same release with another source root, platform, or profile revision | Reject with `RELEASE_CONFLICT`; use `prepare` for another distribution |
| Different releases selecting the same runtime digest and profile revision | Create distinct publications sharing one canonical location |
| No release and a new idempotency key | Create one retention-bounded publication sharing any existing canonical location |

Keys are 16 through 128 bytes. Terminal operation rows are retained for 30 days,
which exceeds the seven-day replay guarantee, then compacted in batches of 500.
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

Before the first optimization, a metadata-only eligibility pass maps each
candidate placement to the active profile's declared runtime binding, locality,
and selected artifact platform. `locality: require` removes unsupported
candidates; `prefer` ranks a READY local route ahead of canonical or permitted
direct fallback. This pass makes no provider call. No eligible target fails with
`IMAGE_LOCALITY_UNSUPPORTED` before provisioning rather than warming an
impossible placement.

When a selected managed target has no READY route, resolution first persists one
server-owned demand for the logical deployment target. Identity is:

- cluster launch generation;
- managed-job recovery generation plus target; or
- Serve service version plus target.

Serve replicas, task ranks, nodes, and GPU processes point to that demand and do
not create independent rows or eviction fences. The demand contains catalog
authority, artifact/runtime digest, exact profile revision, target fingerprint,
location, bounded placement constraint, owner epoch, and retry epoch. It contains
no credential or raw untrusted value, and users cannot supply it in YAML.

Only after that commit does the resolver raise the typed
`ContainerImageWarmingError`. The same-call provisioning loop also sets
`no_failover=True`, but the durable demand is authoritative. Before every
new optimization, normal launch, SkyServe, and managed-job controllers reload
it and restrict candidates to its target. An API or controller restart therefore
cannot reoptimize into another cloud and create another warming intent. The
dashboard and events say `IMAGE_WARMING`, not `resources unavailable`.

If materialization fails terminally, the controller reports
`IMAGE_PREPARATION_FAILED` for that target. It does not reinterpret that failure
as capacity. After a READY plan, a genuine capacity failure may explicitly
supersede the demand, increment the consumer generation, and optimize a new
placement. This distinction preserves ordinary recovery without allowing image
warming itself to cause failover.

With `managed_preferred` plus `locality: prefer`, the exact request-supplied
digest can be used immediately if its pull authentication is valid for the
placement. Release-only and artifact-only selectors never infer or expose a
source fallback.

The runtime commits a secret-free pull plan only after a READY route is selected.
`transactions.commit_ready_demand()` locks the location, then the demand,
inserts or converges its single logical reference, and stores the plan in one
PostgreSQL transaction. It rechecks profile revision, target fingerprint, digest,
platform, auth strategy, lease-free READY state, and consumer epoch. Central
demand state is the durable source for normal launch, Serve, and managed-job
controllers, so their own SQLite-compatible state stores only the demand ID and
generation.
Restarts keep a still-valid plan or explicitly supersede it after a real capacity
failure. They never persist a WARMING fallback as managed locality.

Eviction treats a live WARMING demand as a fence before a reference exists.
Reference acquisition and eviction both lock the same location row. Consumer
terminal or supersede handling writes a central tombstone and releases demand
and reference together. A WARMING request demand may expire only when its request
is terminal, no durable consumer attached, and it is at least 24 hours old. For
clusters, jobs, and services, reconciliation requires two authoritative terminal
consumer observations separated by an hour before writing a missing tombstone;
absence or an unreachable consumer store never releases a fence. Such rows are
shown as orphan candidates for administrator review rather than deleted by age.

SkyServe keeps the prior healthy version routed and its demands referenced until
the new version has a READY registry plan, at least one replica reports node pull
completion, and that replica passes health checks. Registry READY, node pull
complete, and replica healthy are three distinct states and timestamps. Only
then may traffic shift and the old version drain. Spot or capacity failover after
READY creates a new version-target demand and tombstones the superseded one only
after the replacement is healthy.

## Multi-node, multi-GPU, and architecture behavior

Image distribution is node-scoped. One EC2 instance with eight GPUs pulls one
image through its container runtime cache. Starting one process per GPU is the
workload or serving runtime's responsibility. Distribution does not multiply
copy work by GPU count, replica process count, or task rank.

A multi-node task stores one demand and pull plan per placement target, not per
node or GPU. All nodes in that placement use the same runtime digest. Kubernetes
still creates pods normally and relies on node-level containerd caching.

V0 stores one verified runtime manifest per artifact. Its source may be that
manifest or an index whose exact platform child was selected at publication.
`container_image_sources` preserves root digest, root media type, requested
platform, and selected child digest; `container_images` is keyed by runtime child
digest and platform. Pull plans use only the child digest. Placement fails closed
when the runtime architecture does not match. V0 defaults publication to AMD64
and never builds or selects ARM64 speculatively. Full index replication remains
post-v0 because parent and every child then need capacity, reference, and deletion
ownership.

## PostgreSQL data model

Central image state is PostgreSQL-only. Local and controller databases retain
their existing SQLite support.

Migration 023 is a literal additive migration. It does not import live ORM
metadata. It creates only:

```text
container_image_catalog
container_image_profile_revisions
container_image_operations
container_images
container_image_sources
container_image_publications
container_image_releases
container_image_provider_budgets
container_image_registry_shards
container_image_locations
container_image_demands
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

- unique `(workspace, runtime_digest, platform)` artifact identity;
- unique `(workspace, source_ref, requested_platform)` source selection, with
  immutable source-root and selected-child digests;
- unique operation `(authority, scope, actor_hash, kind, idempotency_key)` plus a
  bounded request hash, `PENDING|RUNNING|SUCCEEDED|FAILED` state, result
  projection, and 30-day terminal expiry;
- unique non-null `(workspace, requested_release)` while
  `reservation_active`, retained forever for READY publications and expiring
  after 30 days for unretried FAILED publications;
- publication state in `PENDING|INSPECTING|READY|FAILED`, with an inspection
  lease token and expiry only in INSPECTING, one canonical location, and the
  collision behavior above; canonical location is null only before source
  inspection and becomes immutable when bound;
- every release row points to the READY publication and artifact that created
  it, and no release row exists before that transaction;
- one provider budget row per provider, partition, account, region, and API
  family, with an applied rate, token state, and persisted throttle backoff;
- profile revision state in `QUALIFYING|ACTIVE|FAILED|RETIRED`, with at most one
  active revision per profile selection scope and a bounded desired-config and
  qualification hash;
- one row per physical repository shard with immutable fingerprint, hard
  manifest ceiling, reserved count, observed count, qualification timestamp,
  reconciliation epoch/cursor, and `READY|FULL|DRIFTED|DISABLED` admission state;
- unique physical location identity for artifact/profile/target/fingerprint;
- canonical versus regional location relationship checks;
- closed location state and lease combinations;
- an inventory epoch marker on each manifest-present location;
- one server-owned demand per cluster generation, job recovery target, or Serve
  version target, with immutable owner identity/generation, target, terminal
  observation/tombstone fields, and
  `WARMING|READY|FAILED|SUPERSEDED|RELEASED` state plus a bounded secret-free plan;
- one durable reference per demand; and
- worker kind in `COPY|LIFECYCLE` with bounded heartbeat metadata.

All queue discovery is bounded and indexed by state, retry time, inspection or
location lease expiry, and ID. Claim uses `FOR UPDATE SKIP LOCKED`. Provider I/O
occurs outside the claim transaction. Completion validates the applicable random
lease token after acquiring the row lock and reading the current clock.

Every command that locks more than one image row uses this order:

1. profile revision and physical shard rows, ordered by ID;
2. artifact and source rows, ordered by ID;
3. canonical location before regional location, then location ID;
4. publication, release, and operation rows, ordered by ID;
5. demand and reference rows, ordered by ID; and
6. a central durable consumer row, when normal cluster state participates.

Initial insert races rely on unique constraints and restart the transaction.
No repository function acquires an earlier class after a later one. Canonical
completion and publication retry both lock location before publication.
Reference acquisition and lifecycle eviction both lock location before checking
demands or references. This is the executable ownership contract for the
component split.

Migration 023 is run under a PostgreSQL migration-scoped advisory lock, not a
runtime control-plane lock. The downgrade itself can inspect only database
state, so it drops the tables only when every image table is empty. Draining all
023 processes is a separately verified operator precondition. Normal rollback
never downgrades. Because the feature has not shipped, there is no compatibility
reason to preserve the earlier branch-only schema.

## Registry profiles

### Provider-neutral access contract

A provider adapter consumes credential-free bindings rather than provider fields
from a pull plan. Every source or target resolves the following fixed roles:

- `source_read`: optional authority used only to inspect and copy the published
  source digest;
- `destination_write`: authority to inspect and write one declared target;
- `runtime_pull`: backend-specific strategy used by the actual container-runtime
  principal;
- `lifecycle_delete`: optional authority for reference-safe deletion; and
- `localities`: finite provider/backend/region bindings the target truly serves.

An access binding has an immutable ID, provider kind, nonsecret authority or
secret-resolver reference, allowed purposes, and qualification fingerprint.
Adapters mint short-lived credentials inside the relevant worker or runtime.
Secret values never enter a profile, pull plan, database row, command argument,
or API response. A binding qualified for source read cannot be reused for write
or delete.

Before any destination write, the adapter returns an `OciContentGraph` containing
the root digest, media type, child descriptors, runnable platforms, and manifest
unit count. V0 accepts one runnable image manifest, or an OCI index/manifest list
only to select exactly one declared platform child. It rejects artifact manifests,
nested indexes, zero or ambiguous platform matches, and non-runnable children
before upload. Only the selected manifest consumes destination capacity. The graph
type and persisted source root preserve a later path to full parent/child
publication without replacing the provider interface.

The v0 public schema accepts only `aws_assume_role`,
`aws_ec2_instance_identity`, `aws_eks_kubelet_identity`, and a
`kubernetes_dockerconfig_secret` restricted to source read. All other binding
kinds fail validation. The Docker config reference names namespace, Secret, and
key; only the copy-worker service account can read its value.

The internal post-v0 external-target seam has two modes:

- `write_through`: SkyPilot may inspect and materialize through qualified
  destination-write authority, but never provisions or deletes infrastructure;
- `read_only`: SkyPilot may verify an already present digest and route pulls, but
  `prepare` fails with `TARGET_READ_ONLY` instead of pretending JIT copy works.

Private external OCI will require explicit write and runtime-pull bindings.
Generic OCI compatibility is not enough. Qualification must record
digest-preserving manifest read/write, accepted media types, credential refresh,
throttling, declared locality, and optional deletion. GAR, Nebius, generic
Kubernetes, and non-cloud Docker binding kinds are rejected by the v0 public
schema until an adapter passes that contract. A generic endpoint is never
declared local merely because it is reachable.

### Profile syntax and activation

A profile is a complete immutable revision. The managed AWS example uses one
dedicated registry account in one AWS partition, with many target regions:

```yaml
container_registries:
  default_profile: gpu-production
  access_bindings:
    private-source:
      kind: kubernetes_dockerconfig_secret
      reference:
        namespace: skypilot
        name: image-source-credentials
        key: .dockerconfigjson
      purposes: [source_read]
    registry-copy:
      kind: aws_assume_role
      authority: arn:aws:iam::123456789012:role/SkyPilotImageCopy
      purposes: [source_read, destination_write, verify]
    registry-lifecycle:
      kind: aws_assume_role
      authority: arn:aws:iam::123456789012:role/SkyPilotImageLifecycle
      purposes: [verify, lifecycle_delete]
    aws-vm-pullers:
      kind: aws_ec2_instance_identity
      purposes: [runtime_pull]
      principals:
        - arn:aws:iam::210987654321:role/SkyPilotNodeRole
    aws-eks-pullers:
      kind: aws_eks_kubelet_identity
      purposes: [runtime_pull]
      principals:
        - arn:aws:iam::210987654321:role/EksNodeRole
  profiles:
    gpu-production:
      revision: 1
      ownership: managed
      provider: aws
      partition: aws
      registry_account: "123456789012"
      realm: skypilot-production
      canonical:
        region: us-east-1
        registry: 123456789012.dkr.ecr.us-east-1.amazonaws.com
        repository_prefix: skypilot-images
        shard_count: 16
        max_manifests_per_shard: 90000
        write_authority: registry-copy
        delete_authority: disabled
        runtime_pull:
          aws_vm: aws-vm-pullers
          aws_eks: aws-eks-pullers
      targets:
        - name: us-west-2
          region: us-west-2
          registry: 123456789012.dkr.ecr.us-west-2.amazonaws.com
          repository_prefix: skypilot-images
          shard_count: 16
          max_manifests_per_shard: 90000
          write_authority: registry-copy
          delete_authority: registry-lifecycle
          runtime_pull:
            aws_vm: aws-vm-pullers
            aws_eks: aws-eks-pullers
```

Semantic changes require a higher explicit revision. Existing durable pull
plans remain valid while their exact target and auth contract remains usable.
Config reload stages a desired profile revision as `QUALIFYING`; it never makes
provider calls or blocks deployment. The copy worker validates the secret-free
Terraform handoff, assumes each access binding, probes settings and capabilities,
and persists a qualification hash and timestamp. One transaction promotes the
revision to `ACTIVE` only when every target is fresh and matches the desired
config. The previous active revision remains selectable until then. New
placement uses only the active revision; existing plans retain their exact old
revision until references drain.

Qualification refreshes every ten minutes. After one hour without a successful
refresh, new publication and materialization stop for that target while existing
verified pull plans remain usable. This is a scoped profile state machine in
`container_image_profile_revisions`, not a second global configuration ledger.
Config validation still rejects ambiguous locality, duplicate targets,
cross-partition managed profiles, or incompatible access bindings before staging.

## AWS managed slice

### Fixed repository layout

Terraform creates every v0 repository before profile activation. For each
declared workspace, region, and shard index, the name is deterministic:

```text
<prefix>/r<authority-base32>/w<workspace-hash>/g00/s<two-hex-index>
```

`authority-base32` encodes the 128-bit catalog authority. `workspace-hash` is a
128-bit, versioned hash of authority plus normalized workspace name. Terraform
and the API reject any collision across the declared workspace set and validate
the final ECR name and length. `g00` is the fixed v0 repository generation. This
prevents two SkyPilot control planes sharing an account from colliding and avoids
placing workspace display names in registry paths. A later shard expansion uses
a new explicit generation and profile revision; it never changes old paths.

A shard's physical identity includes AWS partition, account, region, registry
authority, repository ARN and name, catalog authority/realm, workspace encoding
version, generation/index, encryption type and KMS key ARN, tag immutability,
scanning mode, and Terraform ownership tags. Endpoint plus namespace alone is
not a sufficient fingerprint. Setting drift marks the shard `DRIFTED`; it does
not silently create a second physical identity.

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
publication or demand can recreate it.

Each shard stores a durable inventory epoch, provider cursor, started time, and
last-completed time. One reconciliation claim reads at most ten provider pages
or runs for ten seconds, then commits its cursor. An invalid or expired provider
cursor restarts the epoch safely. Observed managed digests update the matching
location's epoch marker. Only a completed epoch may diagnose a missing manifest.
An in-flight or not-yet-written location consumes a reservation but is not
expected in inventory. An unexplained manifest, observed count above reserved
count, or manifest-present location absent from a complete epoch marks the shard
`DRIFTED` and stops new admission without breaking existing pulls.

Failed canonical reservations are reaped only after every dependent publication
reservation has expired, no demand or reference remains, no lease is live,
and exact inspection proves the digest absent. The same transaction deletes the
empty location and decrements the shard count. V0 never deletes a READY canonical
manifest. An ambiguous write that left content therefore keeps its honest
capacity reservation until an operator resolves it.

This makes capacity a transactionally enforced boundary rather than a
probabilistic claim. At a 90,000 ceiling, twelve shards admit at least one million
manifests; the example uses sixteen for headroom. A complete million-manifest
scan is resumable and off the API path. Expanding the fixed layout requires
Terraform plus a new profile revision. V0 never creates a repository during
placement or copy.

No Terraform action copies image content. Canonical and regional manifests are
created only from durable intents.

### IAM boundary

V0 managed custody uses exactly one dedicated registry account per profile and
any number of regions in the same AWS partition. Source registries and compute
accounts may differ. Multiple destination registry accounts remain outside
managed v0 until a later topology is reviewed.

The module creates or accepts these non-interchangeable identities:

- API: PostgreSQL metadata and intent only, with no ECR, Service Quotas, KMS, or
  data-role `sts:AssumeRole` permission;
- copy worker base: may assume only the exact registry copy role;
- registry copy role: `GetAuthorizationToken` on the required wildcard resource,
  source/repository reads, metadata qualification, layer upload, and `PutImage`
  only for fixed destination repositories, with no deletion or administration;
- lifecycle worker base: may assume only the exact lifecycle role;
- lifecycle role: describe all fixed managed repositories and
  `BatchDeleteImage` only for eligible regional repositories, with no push or
  repository deletion; and
- runtime pull principals: the actual EC2 instance-profile role, EKS kubelet
  node role, Fargate execution role, or declared kubelet credential provider,
  with token plus repository-scoped pull only.

A pod service account is not treated as the EKS image-pull principal. V0 supports
the EKS node role, Fargate execution role, or an explicitly installed kubelet ECR
credential provider. Generic `imagePullSecret` and non-cloud Docker helper
bindings remain post-v0. Credentials refresh at pull time and are never
serialized into a pull plan.

Target-role trust names only the worker base principals and constrains session
duration, external ID, and catalog/profile session tags. Cross-account repository
policies grant exact copy or pull principals. Target roles have Terraform-managed
permissions boundaries, so accidentally broad identity policy on those roles
cannot escape the fixed repository set. SkyPilot cannot constrain a dedicated
account administrator; that administrative trust is explicit and all mutations
are drift-checked and CloudTrail-audited.

Repository creation/deletion, repository policies, IAM, KMS, and account
administration stay with Terraform. ECR registry V2 replication-policy management
is not part of v0. With customer-managed encryption, keys are regional and ECR
owns the required grants; workers receive no direct KMS data permission. The
default module path uses standard ECR encryption, while optional CMKs add key and
grant-quota qualification.

### Terraform deliverables

```text
infra/terraform/modules/aws-image-distribution
infra/terraform/modules/aws-image-worker-identity
infra/terraform/examples/aws-dedicated-skypilot-account
```

The distribution module accepts partition, dedicated registry account ID,
regional provider aliases, catalog authority/realm, declared workspaces, prefix,
fixed shard count/generation, encryption/scanning settings, quota headroom,
exact compute pull-principal ARNs, and optional existing worker role ARNs. It
reads applied repository and images-per-repository quotas when permitted;
otherwise it requires explicit validated inputs and leaves readiness false.

Its secret-free qualification manifest contains desired config hash, timestamp,
workspace encoding version, repository fingerprints and ceilings, role and
permissions-boundary ARNs, repository-policy hashes, applied quotas, KMS/grant
facts, and Terraform ownership tags. The background worker compares this handoff
with live provider state before activation. Terraform output alone never claims
live readiness.

Import/adoption accepts only empty repositories with exact immutable settings
and ownership tags. Nonempty adoption remains external. Repositories use
`force_delete = false` and explicit destroy protection. Terraform destroy fails
while content exists; profile retirement additionally requires the Dashboard to
show zero demands, references, and pull plans. Policies and access bindings
for an old revision remain configured until that revision drains. The example
composes PostgreSQL/API infrastructure already owned by the platform and does not
duplicate database state.

### Why workers, not ECR replication or pull-through cache

ECR replication is push-triggered, preserves repository names, does not backfill
preexisting images, and is capped at 25 unique destinations. Pull-through cache
has a bounded upstream set and makes the workload's first pull perform the fill.
Neither gives SkyPilot per-digest JIT placement, READY-before-deploy, adoption of
arbitrary existing digests, durable copy recovery, or reference-aware regional
deletion. V0 therefore uses portable workers and does not configure either AWS
feature. They remain possible future optimizations behind the same verified
location contract, never alternative sources of truth.

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
`SKIP LOCKED` prevent duplicate authority. Before each provider API family, a
worker acquires from the PostgreSQL account-region token bucket. Applied quota,
refill rate, and burst are qualification inputs; provider throttles persist one
shared exponential `blocked_until`, so scaling pods cannot multiply past the
account limit. ECR's default `PutImage` rate is only 10 per second, and the UI
reports a quota-bound ETA rather than implying worker replicas can exceed it.

Worker budgets do not pretend to control calls made by remote container
runtimes. Node pulls use per-node credential reuse plus bounded exponential
backoff and jitter against qualified pull quotas. A thousand service replicas may
still cause many node layer downloads; registry locality avoids cross-region
transfer but does not claim that an OCI registry prewarms each node cache.

The copy worker also owns bounded background profile qualification and shard
inventory claims. It validates `OciContentGraph` before destination I/O and uses
separate source-read and destination-write sessions. Lifecycle workers claim only
reference-free, noncanonical, managed locations past retention, plus provably
empty failed canonical reservations for counter reclamation. They inspect the
exact digest after ambiguous deletion and never delete a repository or a READY
canonical manifest.

Shutdown stops new claims, cancels work that has not started provider I/O, and
lets leases expire after ambiguous I/O. Restart recovery verifies actual
registry state before completion or retry.

## Dashboard

The Dashboard contains one first-class Images navigation item with Catalog and
Readiness tabs. It does not add a registry editor or Terraform surface.

### Images catalog and detail

Authorization maps to three explicit capabilities:

| Capability | Existing authorization | Actions |
| --- | --- | --- |
| `images:read` | user may view the workspace | Catalog, detail, bounded operation status |
| `images:write` | user may launch/manage resources in the workspace | Publish, Prepare, Retry within that workspace |
| `images:admin` | server administrator | Qualification ingestion/status, all-workspace readiness and remediation |

Every lookup checks workspace access before selector resolution. Binding names
appear only when the caller can use them; credential values never do.

Authorized users can:

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

Mutation forms validate source digest, release, platform, distribution, binding,
and target combinations locally and again on the server. One idempotency key is
retained for the form submission until terminal state. Polls are keyed by
operation and view generation; navigation aborts the request and discards late
responses. Stop waiting is labelled `Detach`, because it never cancels committed
provider work. A stale cursor prompts a clean first-page reload without replaying
an action.

Status labels never conflate layers:

- registry `PENDING|INSPECTING|READY|FAILED` and a queue/quota-based preparation
  ETA;
- deployment node pull `UNKNOWN|IN_PROGRESS|COMPLETE|FAILED`; and
- SkyServe replica health, linked from Serve rather than synthesized by Images.

Registry ETA does not predict node cache fill or replica health. Old API servers
show a capability callout, hide mutation controls, and keep the rest of the
Dashboard usable. Catalog/detail/action dialogs and tables support keyboard
navigation, labelled controls, screen readers, narrow viewports, and reduced
motion.

### Image distribution readiness

Administrators get an operational Settings panel showing:

- active secret-free profile revisions and workspace defaults/allowlists;
- desired versus active revisions, qualification hash/age, repository and role
  readiness, reconciliation progress, quota backoff, and drift;
- copy and lifecycle worker healthy/stale counts;
- queue depth and oldest pending/retry age by profile/target; and
- capability failures that prevent managed-profile activation.

V0 deliberately has no browser profile editor. Operators change versioned
configuration and Terraform through normal GitOps, then use the panel to verify
convergence. This removes a second configuration transaction system without
making the feature raw-YAML-only operationally.

The only infrastructure-adjacent UI mutation is the same bounded, secret-free
qualification-manifest upload as `sky image profile qualify`. It stages a hash
for background verification; it cannot edit a profile, run Terraform, assume a
role, or mark itself qualified.

Every readiness response is a projection of PostgreSQL state written by bounded
background work. A Dashboard request never assumes a role, calls STS/KMS/ECR,
resumes inventory, or refreshes qualification. Stale timestamps are shown as
stale rather than synchronously repaired.

### Direct read API

```text
GET /images/catalog?workspace=W&limit=50&cursor=C
GET /images/artifacts/{id}?workspace=W
GET /images/artifacts/{id}/releases?workspace=W&limit=50&cursor=C
GET /images/artifacts/{id}/sources?workspace=W&limit=50&cursor=C
GET /images/artifacts/{id}/publications?workspace=W&limit=50&cursor=C
GET /images/artifacts/{id}/locations?workspace=W&limit=50&cursor=C
GET /images/artifacts/{id}/references?workspace=W&limit=50&cursor=C
GET /images/operations/{id}?workspace=W
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
- Source authentication is a named access-binding reference resolved only inside
  the isolated worker. V0 supports only source paths with a qualified resolver.
- Provider errors are mapped to bounded codes before persistence.
- API, copy base/target, lifecycle base/target, and runtime-pull identities are
  non-interchangeable.
- Managed deletion is allowed only for noncanonical regional content with no
  live durable reference.
- External profiles never grant SkyPilot deletion authority.
- OCI indexes and manifest lists may select one exact platform child; nested
  indexes, ambiguous matches, unselected children, and artifact manifests never
  reach destination writes in v0.
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
4. Apply Terraform in one dedicated registry account, import its qualification
   manifest, and stage the desired profile revision without activating it.
5. Deploy one copy worker and one lifecycle worker with separate identities.
6. Let background qualification prove repository inventory, settings, quota,
   KMS, access bindings, runtime pull principals, and fingerprints, then
   atomically activate one profile for one test workspace.
7. Verify publish, warming, pull, API/controller restart, retry, capacity
   admission, drift fail-closed behavior, and reference-fenced eviction.
8. Convert the Boltz L4 test fleet and compare direct cross-region pulls,
   operator-prewarmed images, and managed JIT locations.
9. Enable production only if the operations or performance gate passes.

Rollback first disables profile activation and new publication, then stops
worker claims, and only then rolls old API binaries. Existing digest pull plans
remain usable and PostgreSQL intent is preserved. Old binaries tolerate the
additive 023 schema. Unchanged direct digest-pinned OCI behavior remains the
escape hatch.
Downgrade is a separate manual operation allowed only after every new process is
drained and every image table is empty; it is never part of Helm rollback.

## Acceptance gates

### Core invariants

- A workload using a release performs no source registration or release
  mutation, and no provider call in the API or placement process.
- A server default never opts a workspace into managed behavior; direct and
  managed selector combinations follow the explicit activation matrix.
- A release lookup returns nothing until its canonical location is READY.
- A failed publication leaves every prior release and deployment launchable.
- One placement attempt creates at most one location intent and warming never
  causes cloud or region failover.
- `IMAGE_WARMING` survives API and controller restart with the same consumer
  generation, profile revision, target, and location.
- Copy crashes before and after manifest publication converge to one verified
  digest.
- At 1,000 replicas and eight GPUs per node, copy cardinality equals requested
  physical targets and demand/reference cardinality equals service-version
  targets, not replicas, nodes, or GPUs.
- A Serve update keeps the prior healthy version routed until the replacement has
  registry READY, node pull complete, and a healthy replica.
- Regional eviction cannot pass a concurrent reference acquisition, warming
  demand, or canonical-location fence.
- Every physical shard refuses admission at its hard ceiling, and provider drift
  stops new writes before the database can claim additional capacity.
- No API, placement, or Dashboard read performs registry, STS, KMS, or Terraform
  I/O.
- V0 resolves an index to one declared platform child before destination
  authority, and never uploads the parent or an unselected child.
- Every mutation is idempotent, typed, and detachable without abandoning durable
  or ambiguous provider work; every read bypasses generic request rows.

### Required verification

- real PostgreSQL migration, concurrency, lease, retry, and downgrade tests;
- fresh-through-023 and literal 022-to-023 schema equivalence, concurrent
  migration-lock, and mixed-022/023 feature-disabled tests;
- old-server/new-client and new-server/old-client feature-gate tests;
- API 61/62 behavior across launch/exec, jobs, Serve up/update, pools, nested DAGs,
  resource alternatives, forged private fields, and request config overrides;
- scalar/object parsing, every selector combination, explicit opt-in/defaults,
  allowlists, direct restrictions, and byte-for-byte legacy `image_id` tests;
- AWS integration plus negative IAM tests;
- EC2 instance and EKS kubelet/Fargate runtime pull-auth refresh tests, plus
  unchanged direct OCI behavior on other runtimes;
- `terraform fmt -check`, `terraform validate`, and plans for one and multiple
  regions with fixed shards;
- worker kill/restart tests around every provider-I/O boundary;
- replay after lost mutation responses, key/body collision, detach before/after
  intent commit, stable result shape, bounded error, and CLI remediation tests;
- idempotency collision-matrix, canonical publication fan-out, controller
  restart, shard-ceiling, and inventory-drift tests;
- source/destination account separation, permissions-boundary, repository-policy,
  KMS grant, protected destroy, empty import, and old-revision drain tests;
- one-million-row resumable inventory, durable cursor, API token-bucket,
  throttling, and empty failed-reservation reclamation tests;
- demand aggregation/tombstone/orphan tests for cluster, job recovery, Serve
  version-target, controller loss, supersede, and unreachable consumer stores;
- single AMD64 manifest, selected AMD64 index child, ambiguous/wrong platform,
  nested index, and artifact reject-before-write tests;
- Jest interaction, pagination, permission, responsive, and stale-state tests;
- Dashboard capability matrix, action validation/idempotency, stale poll and
  navigation suppression, detach semantics, old-server deep links, keyboard,
  screen-reader, reduced-motion, ETA-layer, and secret-absence tests;
- a production Next.js build;
- repository formatting and focused backend tests; and
- 100, 500, and 1,000-replica timing evidence before a speed claim.

Performance reports registry-ready, node-pull-complete, and replica-healthy
separately. They record physical copies, logical demands, uncached node layer
downloads, quota backoff, and ETA error. Ordinary OCI locality is never reported
as node prewarming or Modal-style lazy loading.

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
publication convergence matrix, durable warming demands, singleton
migration rollout, hard physical-shard admission, bounded worker and publication
retention, and the builder handoff correction required by that review.

Round 3 at `bebb29dbbff51783def0dc578cb50181448263b1` returned Codex `RESHAPE`.
Fable again reported no usage credits, so the round is not paired. This revision
adds the provider-neutral access and content-graph contracts, selects one
dedicated-account AWS topology, binds pulls to the actual EC2 or kubelet
principal, removes registry V2 policy management, defines exact repository
identity and durable rate-limited reconciliation, rejects indexes before writes,
and makes Terraform/background qualification a nonblocking durable handoff.

Round 4 at `63d51d0614ba1dd80b7c8cfabd36e5283bef08e5` returned Codex `RESHAPE`.
Fable again reported no usage credits, so the round is not paired. This revision
narrows public v0 to qualified AWS EC2/EKS plus unchanged direct OCI, defines
explicit activation and API 62 behavior, makes every mutation typed and
idempotent, aggregates logical demands, filters locality before optimization,
selects one platform child from ordinary indexes, specifies the operational UI,
uses explicit builder step inputs, adds bounded replayable operation state and
fenced source inspection, and collapses persistence into three aggregates plus
the transaction coordinator.
