# Managed container image distribution

Status: implementation and verification in progress, feature disabled by default

Owner: SkyPilot control plane

Last updated: 2026-07-21

## Decision

SkyPilot will provide a small, portable image-distribution control plane built
on standard OCI registries. An image is identified by an immutable digest. An
explicit publication operation adopts that digest under one required immutable
release, prepares one canonical registry location asynchronously, and makes the
release visible only after the canonical location is verified READY. Workload
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
- separate API, copy-worker, lifecycle-worker, canary-worker, and workload
  identities;
- durable deployment demands that fence regional eviction;
- node-scoped image resolution for multi-GPU and multi-node workloads;
- bounded paginated APIs and a complete operational Images UI;
- copy, lifecycle, and runtime-canary worker Helm deployments, health, metrics,
  and recovery;
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
| Publication | Durable adoption attempt and required release reservation | publication service |
| Release | Immutable published name projected only from a READY publication | publication service |
| Profile | Complete registry topology and policy snapshot | server configuration |
| Access binding | Credential-free reference to one qualified read, write, pull, or delete authority | provider adapter |
| Qualification | Timestamped proof that one profile revision and its access bindings are usable | background worker |
| Registry shard | One preprovisioned physical repository and its hard admission budget | shard repository |
| Location | One digest in one physical registry target | materialization service |
| Demand | Durable placement pin shared by one logical deployment and target | demand transaction service |
| Pull plan | Secret-free, placement-specific READY location snapshot | runtime resolver |
| Copy worker | Claims copy/verify work and can write manifests | materialization worker |
| Lifecycle worker | Claims eligible regional eviction work and can delete manifests | lifecycle worker |
| Canary worker | Runs bounded EC2/EKS pulls through the declared runtime identity | qualification worker |

The implementation is split along those boundaries:

```text
sky/container_images/
  models.py                 value objects and validators
  config.py                 profile and workspace policy snapshots
  catalog_state.py          artifact, source, publication, and release aggregate
  topology_state.py         profiles, budgets, shards, locations, leases, workers
  demand_state.py           durable demands, pull plans, tombstones, watermarks
  transactions.py           cross-repository PostgreSQL transitions
  publication.py            explicit publication service
  runtime.py                read-only workload resolution and warming demand
  providers.py              portable access, content-graph, and adapter contracts
  aws.py                    qualified ECR adapter
  copy_worker_service.py    independently deployed copy loop
  lifecycle_worker_service.py independently deployed deletion loop
  canary_worker_service.py  independently deployed runtime-canary loop
  api_models.py             closed direct-API request and response models
  server.py                 typed direct reads and asynchronous mutations
  builder_prototype.py      disabled post-v0 BuildKit/R2/S3 evidence harness
```

Repository functions accept a caller-owned SQLAlchemy session and never commit
it. `transactions.py` is the only cross-repository transaction boundary. Its
small public surface owns publication creation/convergence, demand
creation/READY commit, and demand-fenced eviction. It owns no tables and does
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
    [--source-auth BINDING] --release NAME \
    --distribution PROFILE [--platform linux/amd64] [--no-wait]
sky image status [SELECTOR] [--workspace W]
sky image prepare SELECTOR --distribution PROFILE --target TARGET [--no-wait]
sky image retry SELECTOR --distribution PROFILE --target TARGET [--no-wait]
sky image profile qualify PROFILE --manifest TERRAFORM.json [--no-wait]
sky image profile canary PROFILE --target TARGET \
    --backend aws_vm|aws_eks [--no-wait]
```

`publish` is the only public source-adoption operation and requires one release.
There is no `register` alias or release-less permanent artifact path. `status`
and every Dashboard catalog/readiness query are synchronous, paginated reads that
create no generic request row.

`--source-auth` names an allowed credential resolver binding, never a secret.
The publication persists only its binding ID and qualification fingerprint. A
public source omits the option. The Dashboard offers the same authorized binding
names without reading or returning their values.

`profile qualify` is an administrator-only, bounded upload of the secret-free
Terraform handoff. Helm deployments normally mount that handoff from a ConfigMap
for automatic background ingestion; the command supports non-Kubernetes control
planes. Neither path runs Terraform or waits for provider qualification inline.
`profile canary` is an administrator-only asynchronous proof using the actual
declared EC2 instance-profile or EKS node-role pull path. It is the only public
operation allowed to resolve a desired but not-yet-active revision, and only for
the fixed qualification artifact and one-time nonce.

`prepare` accepts only a READY artifact whose canonical location is verified and
one qualified target. It validates that target before creating a regional intent.
A pending publication fails with `ARTIFACT_NOT_READY` and a status remediation;
callers do not orchestrate a hidden two-call canonical protocol. V0 has no
standard-user bulk fan-out; normal deployment may prepare only its selected
placement, and administrators repeat the single-target operation explicitly.

### Mutation contract

`publish`, `prepare`, `retry`, `profile qualify`, and `profile canary` require a
client-generated idempotency key in the SDK or `Idempotency-Key` header. The CLI
and Dashboard generate a random key once per submitted form and reuse it after
lost responses.
One bounded image-operation row is unique by catalog authority, workspace or
administrator scope, actor hash, mutation kind, and key. It stores the request
hash and only a typed, secret-free result projection. Same key and body returns
that operation; a body mismatch returns `IDEMPOTENCY_KEY_REUSED`.

Each mutation has one versioned typed result:

| Mutation | Stable result |
| --- | --- |
| Publish | operation, publication, requested release, optional inspected artifact, READY-only published release, state |
| Prepare | operation, artifact, target/location state |
| Retry | operation, retried publication or location, state |
| Profile qualify | operation, profile/desired revision, manifest hash, state |
| Profile canary | operation, profile/desired revision, target/backend, attestation state |

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
`REGISTRY_CAPACITY_EXHAUSTED`, `REGISTRY_SHARD_UNAVAILABLE`,
`REGISTRY_LOCATION_QUARANTINED`, `IMAGE_LIMIT_EXCEEDED`, `TARGET_READ_ONLY`,
`IMAGE_PREPARATION_FAILED`, `QUALIFICATION_FAILED`, `CANARY_FAILED`, and
`PERMISSION_DENIED`.
`IMAGE_WARMING` and provider
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
digests, demand IDs, demand fences, qualification records, and any field prefixed as
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
      publishers:
        - 1a2b3c4d5e6f7890
```

`publishers` contains stable SkyPilot user IDs, not display names. Administrators
always have `images:publish`; other users require their exact ID in this list.
Workspace access alone grants `images:use`, while the viewer role remains
read-only even if its ID is accidentally listed. An absent list grants no
non-administrator publication or preparation mutations.

## Publication contract

Publication is independent of workload deployment. Managed v0 accepts either a
single image manifest or one selected child of an OCI index. `--platform`
defaults to `linux/amd64`; additional platforms are explicit, never speculative.

1. Validate a digest-pinned source root, requested platform, required release,
   workspace, source access binding, and complete active profile. The request
   hash includes every field and exact profile revision.
2. In one transaction, create a durable PENDING publication and reserve its
   release. The canonical location remains null until source inspection;
   deployment still cannot observe a release.
3. A copy worker claims the publication with a random fenced inspection lease,
   moves it to INSPECTING, authenticates only to the source, and builds
   `OciContentGraph` before destination authority or I/O. A single manifest must
   match the requested platform. An index must contain exactly one matching
   runnable child. In one transaction, the worker rechecks the inspection token,
   persists the immutable source-root digest, selected child/runtime digest, and
   platform, converges the artifact/source, reserves one shard, creates or reuses
   its canonical intent, enforces the artifact's release ceiling while holding
   the artifact lock, clears the inspection lease, and returns the bound
   publication to PENDING.
4. The worker separately acquires destination authority, copies only the selected
   exact raw manifest and referenced distributable layers, then verifies the
   destination child digest, config digest, and platform. It never uploads the
   source index in v0.
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
   location. It never creates a second physical copy. Exceeding the per-artifact
   release ceiling is a typed `IMAGE_LIMIT_EXCEEDED` failure rather than a
   source-content validation failure.

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

Keys are 16 through 128 bytes. Terminal operation rows are retained for 30 days,
which exceeds the seven-day replay guarantee, then compacted in batches of 500.
Release-backed successful publications are retained catalog facts. An active
failed release reservation expires after 30 days without retry; its
`reservation_active` flag is cleared, so a name that was never publicly visible
can be requested again. The failed publication remains visible for 90 days and
is then compacted. The lifecycle worker processes at most 500 expirations or
deletions per sweep. There is no release-less publication or separate unbounded
publication-attempt table.

A READY canonical artifact is permanent in v0, so custody is bounded before
source inspection can reserve it. The active profile sets a maximum artifact
size, maximum releases per artifact, maximum regional locations per artifact,
and conservative declared-byte ceiling per physical shard. Descriptor sizes are
charged without assuming cross-artifact layer deduplication. The existing shard
manifest ceiling bounds artifact count. A concurrent reservation locks the chosen
shard and fails with `REGISTRY_CAPACITY_EXHAUSTED` before destination authority
when either count or declared bytes would exceed its limit. Publication requires
the explicit `images:publish` capability; ordinary workload permission cannot
fill canonical custody.

Mutable tags are rejected in v0. Documentation must not promise tag resolution.
A later isolated resolver may accept a credential reference, resolve a tag to a
digest, discard the credential, and submit the same publication transaction.

## Durable transition and ECR ambiguity contract

The asynchronous state machines are closed. Publication moves from unbound
PENDING to INSPECTING, back to bound PENDING, then to READY or FAILED. An
explicit retained FAILED retry returns it to PENDING. Operation moves from
PENDING to RUNNING, then SUCCEEDED or FAILED. Cancellation before the intent
transaction deletes PENDING; after commit it only detaches.

Location transitions are:

| From | To | Guard |
| --- | --- | --- |
| PENDING | COPYING | Fenced worker claim |
| COPYING | VERIFYING | `PutImage` returned or its outcome is ambiguous |
| COPYING | PENDING | Retryable pre-manifest failure with backoff |
| COPYING | FAILED | Closed permanent error |
| VERIFYING | READY | Exact destination content verifies |
| VERIFYING | PENDING | Exact manifest is absent or read is retryable |
| VERIFYING | FAILED | Exact mismatch or closed permanent error |
| READY | MISSING | Completed inventory plus exact digest absence |
| READY | EVICTING | Regional, automatic eviction enabled, past the workspace retention anchor, and no live demand |
| FAILED, MISSING, EVICTED | PENDING | Explicit prepare/retry or new authorized demand |
| EVICTING (`EVICT`) | EVICTING (`DELETE`) | Same fenced owner durably authorizes the first destructive provider call |
| EVICTING | EVICTED | Exact digest absence after delete |
| EVICTING (`DELETE`) | READY | Exact digest remains after a successful provider conclusion or an explicit no-mutation rejection |
| EVICTING (`EVICT`) | READY or EVICTING (`EVICT`) | Expired pre-delete claim, restore for live demand or retry the still-demand-free eviction |
| EVICTING (`DELETE`) | QUARANTINED | Delete outcome is ambiguous or the destructive-intent lease expires |

Only INSPECTING publications carry an inspection token and expiry. Only
COPYING, VERIFYING, or EVICTING locations carry a random location token, expiry,
and matching lease kind. `EVICT` means no destructive provider call may have
started; `DELETE` is the durable point after which a delete may already be in
flight. PENDING, READY, FAILED, MISSING, EVICTED, and QUARANTINED carry no lease.
Canonical locations never enter EVICTING, EVICTED, or QUARANTINED. Every retry
records a bounded attempt count, code, and `next_retry_at`; throttles and
timeouts before provider I/O remain retryable. An unknown delete outcome is
terminal for that physical location because ECR has no conditional-delete token.

A PROFILE_CANARY operation is the only operation row that also acts as its work
queue. RUNNING then carries a random lease, expiry, one bounded child launch ID,
and teardown deadline. The canary resource is tagged with operation/profile/
generation, may exercise only one target/backend, and always auto-terminates.
After a crash, the next claimant reads the child launch and qualification
repository before retrying or teardown; it never launches a second live canary
for the same operation. Other operation kinds project their publication,
location, or profile work and carry no provider lease.

Canary intent creation locks the desired profile row and reserves its conservative
worst-case cost in a UTC daily window before committing the operation. Concurrent
automatic or manual canaries cannot exceed the configured hard cap; increasing
the cap requires a new profile revision.

An ECR destination claim executes this fenced algorithm:

1. Claim one PENDING or expired COPYING/VERIFYING location with `FOR UPDATE SKIP
   LOCKED`, a new token, and COPYING state. An expired claim first follows the same
   destination inspection path as a fresh claim; it never resumes from process
   memory.
2. Re-read the selected source by digest and preserve its exact raw manifest
   bytes. Fetch and hash the config blob, prove its OS/architecture matches the
   requested platform, and validate every descriptor digest and declared size.
   Reject
   foreign or nondistributable media types, external layer URLs, excessive
   manifest/config size, excessive layer count, or the artifact byte limit before
   acquiring destination authority. The lease heartbeat is active before source
   credential acquisition or network reads. Generic OCI inspection remains
   read-only and streamed blob transfer observes cancellation; destination
   authority cannot be acquired after that source work loses its lease. Managed
   regional-source ECR credential acquisition and SDK calls are synchronously
   fenced by the exact lease.
3. Re-prove the exact lease immediately before destination credential
   acquisition and again after it returns. Every destination ECR SDK hook
   re-proves ownership before and after any provider-budget wait, leaving no
   unbounded interval in which an expired worker can begin a later call. Then
   call destination `BatchGetImage` for the exact child digest. If the returned
   raw bytes, media type, config, platform, and referenced layers verify, skip all
   writes and converge through VERIFYING to READY.
4. Use `BatchCheckLayerAvailability` and transfer only missing blobs. Each source
   download is streamed through a digest verifier. After an ambiguous ECR upload
   initiation, part, or completion, check exact layer availability; if absent,
   start a new upload. Abandoned ECR upload IDs are not catalog identity.
5. Submit the unchanged selected manifest bytes with untagged `PutImage`. A
   success or already-exists response moves the fenced row to VERIFYING. A timeout
   or disconnect also moves it to VERIFYING with an ambiguous-outcome code.
6. VERIFYING performs exact `BatchGetImage` plus config and layer checks. Exact
   presence commits READY. Confirmed absence returns to PENDING with backoff.
   Digest/media/config mismatch is terminal and never overwrites the destination.
7. SQL completion locks the location and validates the token and database clock.
   A worker that lost its lease cannot begin another credential or ECR call and
   cannot complete state even if an already-started same-digest call finished.
   The next claimant's exact reads make that harmless immutable write converge.
   Canonical READY then fans out dependent publications in bounded batches
   without repeating provider I/O.

Inventory is advisory. A completed list epoch may nominate a READY location as
absent, but only an exact digest read under a reconciliation lease can move it to
MISSING or mark a managed shard drifted. This rule also applies after an invalid
or expired provider cursor.

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

The location-intent transaction converges on the immutable target-ring
fingerprint before selecting a physical shard, locks the selected shard, then
the artifact, enforces its regional-location ceiling, reserves count and
conservative declared bytes once, and inserts or converges the location. A
unique artifact, target-ring fingerprint, and runtime-digest constraint closes
concurrent shard-selection races. The loser rolls back its shard reservation
and reloads the winning location. An implicit workload request cannot fan out
beyond the one selected placement.

Before the first optimization, a metadata-only eligibility pass maps each
candidate placement to the active profile's declared runtime binding, locality,
and selected artifact platform. `locality: require` removes unsupported
candidates. `prefer` is a lexicographic class ahead of the ordinary optimizer:
READY managed routes rank first, an authorized direct source fallback ranks
second, and a managed route that still needs warming ranks third. Cost, time,
reservations, and egress preserve their existing ordering within the winning
class. Exact indexed location reads supply this rank without provider calls. No
eligible target fails with `IMAGE_LOCALITY_UNSUPPORTED` before provisioning
rather than warming an impossible placement.

For managed EC2, that metadata includes the exact planned host AMI and instance
profile. Both must match the binding's qualified regional AMI and principal; a
request with no host image is pinned to the qualified regional AMI before
optimization, while a user-supplied host image or role outside that tuple is not
silently trusted. EKS eligibility maps the selected SkyPilot Kubernetes context
to one exact cluster ARN, node role, namespace, and immutable nonempty node
selector. The canary and every managed workload pod receive that same selector.
Qualification enumerates every schedulable node matching the selector, resolves
each node's EC2 instance profile, and requires the declared role for the complete
eligible set before recording READY. A selector that matches zero nodes, more
than the bounded qualification page, or heterogeneous roles fails closed. This
binds the selected cluster and node pool to the attestation instead of proving
one fortuitously scheduled canary node. `managed_required` fails closed on a
mismatch, while
`managed_preferred` may use only its otherwise-authorized direct digest path.

Before optimization, the execution path derives one restart-stable logical
consumer identity and loads its current live demand from PostgreSQL. A live
demand restricts the metadata pass to its stored provider, region, backend,
profile revision, target fingerprint, digest, and platform. A request or
controller restart therefore cannot select a second target while the first is
warming. When a selected managed target has no READY route, resolution first
persists one server-owned demand for that logical deployment target. Identity
is a stable owner plus an explicit controller epoch:

- a named-cluster owner combines the reusable cluster name with a digest of the
  durable API request epoch for one launch incarnation and reloads that epoch
  from its persisted pull plan on replay;
- a managed-job owner uses stable job and task IDs plus the recovery generation
  derived atomically from the authoritative job status and recovery count; and
- a Serve owner uses the service incarnation and version plus a normalized
  provider, region, backend, and platform target scope.

The Serve incarnation is part of the stable owner key, not only its controller
epoch. Recreating a service under the same name therefore starts a distinct
version-target owner and cannot collide with a watermark left by the prior
incarnation. All replicas of one incarnation, version, and target still share
exactly one owner and demand.

The controller epoch is not hashed into an arbitrary generation. The consumer
watermark stores the bounded controller epoch and maps it, under row lock, to a
monotonically increasing owner epoch. It also stores an optional monotonic
controller sequence. Serve uses its durable version, managed jobs use their
durable recovery generation, and clusters deliberately use no sequence because
only the named-cluster lifecycle lock may authorize a new request epoch.
Replaying the same controller epoch requires the same sequence and reuses the
mapping. Advancing to a different controller epoch is allowed only when the
caller presents an authoritative lifecycle transition. For sequenced owners,
the new sequence must be strictly greater, so a delayed recovery cannot take
ownership back. That same transaction supersedes any older live demand before
publishing the new owner epoch. The cluster INIT transaction stores the exact
validated consumer kind and owner beside the authoritative cluster row, rather
than relying on a later handle decode. A first-party named-cluster deletion uses
that binding to release every live demand and permanently retire the launch
incarnation in the same PostgreSQL transaction that deletes the cluster row.
If a pre-binding handle is unreadable, deletion never guesses an owner: it
removes the row and leaves the fence for the independent two-observation
reconciler. Every managed pull plan is revalidated against READY catalog state
before persistence; there is no persisted-plan validation bypass.
Managed-job and Serve replica cluster rows carry their shared non-cluster
binding and never retire it during replica teardown. Recreating the same named
cluster receives a new request epoch and therefore a new stable owner.

The two-observation reconciler compares both the reusable cluster name and its
stored consumer binding. A same-name row with a different binding is proof that
the old incarnation is absent; it cannot mask the old demand indefinitely.
Rows and demands created before incarnation-scoped bindings retain the explicit
legacy name-only compatibility path. A false `binding_known` scalar marks a
pre-binding or indeterminate row and conservatively keeps every same-name demand
live until a current writer backfills it or the row disappears. Current writers
set that scalar true and leave both consumer fields `NULL` when the handle proves
there is no managed consumer, so an unrelated direct-image recreation does not
mask an old managed incarnation. A current binding, an authoritative
nonterminal state, or an unknown lifecycle result clears any partial terminal
confirmation. The separate confirmation-delay rotation preserves its first
observation, so
retirement requires two uninterrupted authoritative terminal observations at
least one hour apart. The delay applies only to reconciliation that infers a
missing owner, never to an authoritative `sky down` transition. When
reconciliation proves a cluster, managed-job task, or Serve version terminal,
its final observation terminalizes the last demand and retires that owner in
one watermark-then-demand transaction.

Cluster-row absence is serialized rather than inferred from an unlocked
snapshot. On PostgreSQL, every cluster INIT/upsert, direct deletion, and final
cluster reconciliation acquires the same transaction-scoped advisory lock keyed
by the globally unique cluster name. The final reconciler re-reads the binding
and performs its demand observation in that transaction; the earlier bounded
batch lookup is only a safe fast-path hint. Thus either INIT commits first and
reconciliation observes the exact live binding, or reconciliation commits first
and a later INIT fails READY-demand validation. The lock order is cluster
lifecycle advisory lock, cluster row when present, consumer watermark, then
demand rows. The advisory lock has no persistent per-cluster table cardinality.

Serve replicas, task ranks, nodes, and GPU processes point to that demand and do
not create independent rows or eviction fences. The demand contains catalog
authority, artifact/runtime digest, exact profile revision, target fingerprint,
location, bounded placement constraint, owner epoch, retry epoch, and a bounded
server request ID used only for unattached cluster cleanup. The request ID has a
partial PostgreSQL index; terminal request handling never scans or parses every
live demand. The row contains no credential or raw user-controlled registry
value, and users cannot supply it in YAML.

Only after that commit does the resolver raise the typed
`ContainerImageWarmingError`. The PostgreSQL demand and consumer watermark are
the authoritative image-placement state. Normal launch, SkyServe, and
managed-job recovery establish the same consumer context before both the normal
optimizer and the under-lock planner, so either path reloads and restricts to
the durable target. The resolved pull plan persists the controller epoch, owner
epoch, demand ID, and demand generation in the cluster's INIT handle before
provider provisioning. The same INIT transaction persists `binding_known` plus
the nullable consumer kind and owner in three cluster-row scalar columns on both
supported cluster-state dialects. A false bit is pre-feature or indeterminate;
a true bit with both consumer fields `NULL` is a current, validated absence; and
a true bit with both fields populated is an exact binding. Only the managed
image catalog remains PostgreSQL-only. The per-controller SQLite-compatible
state may retain the demand ID as a hint, but correctness does not depend on a
second database commit. The dashboard and events say `IMAGE_WARMING`, not
`resources unavailable`.

If materialization fails terminally, the controller atomically supersedes that
demand before permitting a new candidate and reports
`IMAGE_PREPARATION_FAILED` for the failed target. Transient warming never causes
failover. A genuine post-READY capacity failure may use the same explicit
supersession transaction. READY commit locks the consumer watermark and requires
the demand to remain its current maximum generation, so a stale worker or retry
cannot publish a pull plan after supersession.

With `managed_preferred` plus `locality: prefer`, the exact request-supplied
digest can be used immediately if its pull authentication is valid for the
placement. Release-only and artifact-only selectors never infer or expose a
source fallback.

The runtime commits a secret-free pull plan only after a READY route is selected.
`transactions.commit_ready_demand()` locks the artifact before the location,
then the consumer watermark and demand. It marks the demand itself as the
durable eviction fence and stores the plan in one PostgreSQL transaction. It
rechecks profile revision, target fingerprint, reference, digest, platform,
auth strategy, credential-helper class, lease-free READY state, and consumer
epoch. If eviction changed a metadata snapshot from READY before demand
creation, that locked recheck returns typed warming state; the new durable demand
keeps the location fenced, an expired eviction is reclaimed, and the attempt
never degrades to a generic resolution failure. A new authorized demand
re-admits FAILED, MISSING, or EVICTED state before creating its fence.
QUARANTINED is never re-admitted to the same physical reference. A replaying
live demand re-admits MISSING or EVICTED state, including from its exact retired
profile snapshot, while a FAILED or QUARANTINED live demand is superseded and
reported as terminal. Central demand state is the durable source for normal
launch, Serve, and managed-job controllers, so their own SQLite-compatible state
stores only the demand ID and generation.
Restarts keep a still-valid plan or explicitly supersede it after a real capacity
failure. They never persist a WARMING fallback as managed locality.
An owner epoch with a live demand reloads that demand's exact immutable profile
snapshot and target even after the revision becomes RETIRED. It evaluates
qualification freshness at the demand creation timestamp and accepts only an
exact replay. A retired revision cannot admit a new owner or select a new target,
so a profile rollout cannot strand an in-flight deployment or reopen old
capacity.

Eviction treats every WARMING or READY demand as the fence and locks its shard,
location, and demand state in the canonical order. Retention is evaluated per
workspace from `last_used_at`, or from `last_verified_at`/`created_at` for a
location that has never been consumed. Thus explicit prepare retains newly
materialized bytes for the full configured interval. `last_used_at` records
registry admission, when the READY pull plan is committed, rather than consumer
termination. A live demand remains an independent hard fence, but once a
long-lived consumer terminates its location is immediately eligible when the
interval since its last registry admission has already elapsed. A null
`regional_cache_retention_weeks` disables new automatic eviction claims for that
workspace. Each qualified shard persists its physical target fingerprint, exact
activated profile revision, and current `eviction_enabled` policy, so the global
SQL queue can exclude disabled targets without scanning locations or searching
profile history. If demand
appears after the provider deletion began and exact readback proves absence,
completion changes
the location to `PENDING`, preserves its existing capacity reservation, and lets
the copy queue rematerialize it. It never records READY for absent bytes. If no
demand exists, exact absence changes the location to `EVICTED`, decrements the
reservation exactly once, and zeros the location's charged bytes. Re-admission
or explicit retry atomically restores count and bytes before changing an
EVICTED location to `PENDING`. A provider operation known not to have started may
restore READY only while the exact current lease still has `EVICT` kind.
Immediately before the first ECR delete call, the SDK hook atomically changes
that lease from `EVICT` to `DELETE`; database completion rejects any provider
result that lacks this durable intent. A successful ECR response, or an explicit
provider rejection that proves no mutation was accepted, may be followed by
exact readback. A transport failure, timeout, ambiguous server response, or
failure without a provider conclusion is never read back as proof of that
delete's terminal state; it immediately yields an ambiguous outcome. After
provider I/O, only exact presence following a concluded call may restore READY
and only exact absence following a concluded call may requeue or release
capacity.

An expired `EVICT` lease proves that no destructive call could have passed the
hook. A live demand therefore restores READY without provider I/O; otherwise a
new owner may retry the eviction. An ambiguous result or expired `DELETE` lease
cannot be made safe by later readback: an old process may still resume after the
read and send, or complete, its delete. The location is therefore atomically
changed to `QUARANTINED`, its in-flight slot is released, and its capacity
reservation remains charged. Inventory may observe the digest but never clears
QUARANTINED, and prepare, demand replay, and explicit retry cannot reuse the same
repository/digest reference. Recovery requires a qualified profile target with
a different repository-ring fingerprint. Other digests on the shard remain
admissible, limiting the exceptional blast radius to the one ambiguous digest.
The bounded readiness projection reports both quarantined-location count and
their retained declared bytes per target, so operators can size and verify a
ring rotation without scanning the catalog.
Every lifecycle claim uses the same durable background lease heartbeat as copy
work. The worker proves the exact location token before assuming its provider
role, synchronously re-proves ownership in the hook immediately before every ECR
delete begins, records `DELETE` intent in that hook, and rechecks ownership again
before database completion. Lease loss sets cancellation state and starts no
later provider call. A call that may already have started is never treated as
cancelled, present, or absent; it converges only to QUARANTINED. The same
unexpired owner may use exact readback only after the provider successfully
concludes the delete request or explicitly rejects it before mutation. Failed
explicit retries bind a terminal idempotent operation before returning a typed
conflict, so replay cannot leave or conceal an unattached nonterminal operation.
When exact absence releases the final capacity reservation on a `FULL` shard,
the same locked transaction changes it to `READY` if both reservation ceilings
are now below their activated limits. It does not wait for the next inventory
epoch to reopen admission.

Consumer terminal or supersede handling writes a tombstone and advances one
stable-owner generation watermark in the same transaction. Demand creation
locks that watermark, validates the explicit controller epoch, and rejects a
generation below maximum seen, at/below maximum terminal, or owned by a consumer
that was authoritatively deleted. Owner deletion is irreversible for that stable
owner key; a later incarnation uses a new stable owner. A replay of the exact
live maximum-seen generation converges the existing demand. A WARMING request
demand may expire
only when its request is terminal, no durable consumer attached, and it is at
least 24 hours old. For clusters, jobs, and services, reconciliation requires two
authoritative terminal observations separated by an hour before advancing a
missing tombstone; absence or an unreachable consumer store never releases a
fence.
All demand creation, supersession, failure, and authoritative terminal paths use
the same watermark-then-demand lock order. This prevents inverse-lock deadlocks
under concurrent controller replay and lifecycle reconciliation.

Task request errors are classified by typed image-plane exceptions at the
runtime boundary, never by inspecting whether the request body happens to
contain an image field. Bounded code-valued errors such as `IMAGE_WARMING`,
`IMAGE_NOT_PUBLISHED`, and `IMAGE_PREPARATION_FAILED` cross the API unchanged.
An unexpected exception caught inside the image boundary becomes one generic
typed image error selected from a static value-free message table. Request
failure persistence receives the original exception only long enough to locate
that typed marker through bounded cause and failover wrappers, then stores a
fresh built-in error with no inherited traceback. Errors from legacy `image_id`,
ordinary provisioning, quotas, setup, or user code are not rewritten by the
image feature.

For `locality: prefer`, candidate generation assigns READY managed, authenticated
direct, and WARMING managed paths locality ranks 0, 1, and 2. It selects the best
rank across every resource alternative for the task before the cost optimizer
runs. A cheap cross-region or warming option therefore cannot defeat a READY
regional image merely because it originated from another `any_of` resource.

Terminal demand rows compact after 30 days only when the authoritative consumer
lifecycle has permanently retired their incarnation and the owner watermark
prevents resurrection. The watermark is the durable nonresurrection fence and
never compacts automatically. Because demand creation always rechecks that
permanent deleted-owner marker under row lock, controller credential expiry is
not a safety precondition for deleting the retained demand payload.
Authoritative owner retirement locks the fence and succeeds only when the owner
has no live demand. Deleting the fence would let a controller transaction that
had already passed authorization lose the row while waiting on its unique key
and recreate the owner on retry. If authoritative retirement proof is
unavailable, the newest tombstone also remains for administrator review. This
retains at most one high-watermark row per stable consumer owner rather than
every historical generation. At million-owner scale these narrow rows remain
ordinary indexed PostgreSQL state; any future archival scheme must preserve an
equivalent non-deletable owner fence.

Compaction discovers a bounded page of terminal candidate keys without taking
row locks, groups those keys by owner, and processes owners in deterministic key
order. For each owner it locks the consumer watermark first, rechecks deletion,
and terminal-generation proof, then locks and rechecks only that page's eligible
demand rows before deletion. It retains the watermark after the last demand is
removed. It never locks a demand before its watermark, so concurrent replay,
supersession, authoritative release, or demand creation cannot form an
inverse-lock cycle with lifecycle maintenance or erase the nonresurrection
fence.

The image plane gates only whether a new SkyServe replica is eligible to become
READY. Registry READY, node pull complete, and replica healthy remain three
distinct states and timestamps. SkyServe's existing capacity-aware rolling
update remains the sole owner of routing, coverage, rollback, and old-version
drain. The old version's demand stays live until Serve declares that version
terminal after its own drain. Spot or capacity failover after READY creates a new
version-target demand; image code never initiates a traffic shift or replica
retirement.

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
post-v0 because parent and every child then need capacity, demand, and deletion
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
container_image_provider_budgets
container_image_registry_shards
container_image_locations
container_image_demands
container_image_consumer_watermarks
container_image_workers
```

The catalog singleton contains only a stable authority UUID and creation time.
There is no forced RLS policy, API-version GUC, runtime-wide advisory lock,
global configuration apply ledger, realm generation, dynamic repository
creation, catalog projection, or facet table in v0. A physical shard row is the
small admission primitive required to enforce the profile's permanent artifact
count and conservative declared-byte ceilings. It is a custody limit, not a
billing ledger, and does not require a separate workspace-quota table.

Important constraints include:

- unique `(workspace, runtime_digest, platform)` artifact identity;
- unique `(workspace, source_ref, requested_platform)` source selection, with
  immutable source-root and selected-child digests;
- unique operation `(authority, scope, actor_hash, kind, idempotency_key)` plus a
  bounded request hash, `PENDING|RUNNING|SUCCEEDED|FAILED` state, result
  projection, and 30-day terminal expiry, with a lease/child/teardown tuple only
  for RUNNING PROFILE_CANARY;
- unique `(workspace, requested_release)` while
  `reservation_active`, retained forever for READY publications and expiring
  after 30 days for unretried FAILED publications;
- publication state in `PENDING|INSPECTING|READY|FAILED`, with an inspection
  lease token and expiry only in INSPECTING, one canonical location, and the
  collision behavior above; canonical location is null only before source
  inspection and becomes immutable when bound;
- release lookup is an indexed projection of READY publications and returns no
  reservation or failed row;
- one provider budget row per provider, partition, account, region, and API
  family, with an applied rate, token state, and persisted throttle backoff;
- profile revision state in
  `QUALIFYING|ACTIVE|FAILED|SUPERSEDED|RETIRED`, a monotonically increasing
  desired generation, partial unique desired and active revisions per profile
  selection scope, a bounded secret-free immutable config snapshot and its
  hash, Terraform and capability-attestation hashes, plus a daily canary-cost
  reservation window;
- one row per physical repository shard with immutable fingerprint, hard
  manifest and declared-byte ceilings, reserved/observed counters,
  qualification timestamp, fair-dispatch timestamp and in-flight ceiling,
  reconciliation epoch/cursor, and `READY|FULL|DRIFTED|DISABLED` admission state;
- unique logical location identity for artifact, immutable target-ring
  fingerprint, and runtime digest, independent of profile revision, plus a
  separately persisted physical repository-shard fingerprint;
- canonical versus regional location relationship checks;
- the exact location transitions and lease combinations above;
- an inventory epoch marker on each manifest-present location;
- one server-owned demand per cluster generation, job recovery target, or Serve
  version target, with immutable owner identity/generation, target, terminal
  observation/tombstone fields, and
  `WARMING|READY|FAILED|SUPERSEDED|RELEASED` state plus a bounded secret-free plan;
- one maximum seen/terminal generation watermark per stable consumer owner; and
- worker kind in `COPY|LIFECYCLE|CANARY` with bounded heartbeat metadata and
  bounded provider-token grants.

All queue discovery is bounded and indexed by state, retry time, inspection or
location lease expiry, and ID. Claim uses `FOR UPDATE SKIP LOCKED`. Provider I/O
occurs outside the claim transaction. Completion validates the applicable random
lease token after acquiring the row lock and reading the current clock.
Global eviction discovery orders the oldest eligible location per shard, then
locks the shard itself with `SKIP LOCKED` before selecting its location. A busy
oldest shard therefore cannot stop independent shards, while the shard-before-
location lock order remains intact. A partial PostgreSQL index over shard,
state, the effective retention timestamp, and location ID keeps this discovery
bounded by the fixed shard ring rather than sorting a million-location cache.

Every command that locks more than one participating row uses this order:

1. a central durable consumer row, when normal cluster state participates;
2. all revisions for one profile ordered by ID, provider budgets ordered by
   provider scope, physical shards ordered by ID, and worker rows;
3. artifact and source rows, ordered by ID;
4. canonical location before regional location, then location ID;
5. publication and operation rows, ordered by ID; and
6. consumer watermark and demand rows, ordered by ID.

Initial insert races rely on unique constraints and restart the transaction.
No ordinary repository function acquires an earlier class after a later one.
Canonical completion and publication retry both lock location before
publication. The PROFILE_CANARY queue is the one explicit class-order
exception: claim and
completion both lock their operation row before the referenced profile revision.
No profile-locking transaction acquires a canary operation, so this consistent
one-way edge cannot close a cycle.
Inspection completion reads its publication optimistically, locks profile/shard,
artifact/source, and location rows first, then locks and rechecks the publication
token; an invalid token rolls the entire transaction back. Demand READY commit
locks artifact before location and then watermark/demand. Lifecycle eviction
locks shard and location before checking demands and does not acquire an
artifact row. Location re-admission and explicit retry lock shard, artifact,
then location, so a stale READY commit cannot form a reverse edge. Authoritative
cluster deletion already owns the central consumer row before it locks the image
watermark and demand. This is the executable ownership contract for the
component split.

Migration 023 is run under a PostgreSQL migration-scoped advisory lock, not a
runtime control-plane lock. The downgrade itself can inspect only database state.
It requires every operational image table to be empty and the catalog table to
contain exactly the expected singleton authority row, then drops the singleton
with the schema. Draining all 023 processes, removing profile configuration,
revoking controller/canary credentials, and running the bounded teardown command
that empties operational rows are separately verified operator preconditions.
Normal rollback never downgrades. Because the feature has not shipped, there is
no compatibility reason to preserve the earlier branch-only schema.

## Registry profiles

### Provider-neutral access contract

A provider adapter consumes credential-free bindings rather than provider fields
from a pull plan. Every source or target resolves the following fixed roles:

- `source_read`: optional authority used only to inspect and copy the published
  source digest;
- `destination_write`: authority to inspect and write one declared target;
- `runtime_pull`: backend-specific strategy used by the actual container-runtime
  principal;
- `lifecycle_delete`: optional authority for demand-safe deletion; and
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
      instance_profile: SkyPilotNodeProfile
      credential_helper: amazon-ecr-credential-helper
      qualified_node_images:
        us-east-1: ami-0123456789abcdef0
        us-west-2: ami-0fedcba9876543210
      canary_authority: compute-canary
      canary_instance_type: t3.micro
      canary_subnets:
        us-east-1: [subnet-0123456789abcdef0]
        us-west-2: [subnet-0fedcba9876543210]
      canary_security_groups:
        us-east-1: [sg-0123456789abcdef0]
        us-west-2: [sg-0fedcba9876543210]
    aws-eks-pullers:
      kind: aws_eks_kubelet_identity
      purposes: [runtime_pull]
      canary_authority: compute-canary
      qualified_clusters:
        - context: boltz-gpu
          cluster_arn: arn:aws:eks:us-west-2:210987654321:cluster/boltz-gpu
          node_role: arn:aws:iam::210987654321:role/EksNodeRole
          namespace: skypilot-image-canaries
          node_selector:
            skypilot.co/image-pull-role: eks-node
    compute-canary:
      kind: aws_assume_role
      authority: arn:aws:iam::210987654321:role/SkyPilotImageCanary
      purposes: [canary_launch]
  profiles:
    gpu-production:
      revision: 1
      ownership: managed
      provider: aws
      partition: aws
      registry_account: "123456789012"
      realm: skypilot-production
      limits:
        max_artifact_bytes: 107374182400
        max_releases_per_artifact: 32
        max_regional_locations_per_artifact: 16
      qualification:
        runtime_attestation_max_age_seconds: 86400
        automatic_canaries: true
        max_daily_canary_cost_usd: 5
        canary_worst_case_cost_usd: 0.10
        canary_timeout_seconds: 900
        canary_ref: public.ecr.aws/skypilot/image-canary@sha256:<64-hex>
        canary_platform: linux/amd64
      canonical:
        region: us-east-1
        registry: 123456789012.dkr.ecr.us-east-1.amazonaws.com
        repository_prefix: skypilot-images
        shard_count: 16
        max_manifests_per_shard: 90000
        max_declared_bytes_per_shard: 10995116277760
        max_in_flight: 16
        write_authority: registry-copy
        delete_authority: disabled
        qualification_delete_authority: registry-lifecycle
        runtime_pull:
          aws_vm: aws-vm-pullers
      targets:
        - name: us-west-2
          region: us-west-2
          registry: 123456789012.dkr.ecr.us-west-2.amazonaws.com
          repository_prefix: skypilot-images
          shard_count: 16
          max_manifests_per_shard: 90000
          max_declared_bytes_per_shard: 10995116277760
          max_in_flight: 16
          write_authority: registry-copy
          delete_authority: registry-lifecycle
          qualification_delete_authority: registry-lifecycle
          runtime_pull:
            aws_vm: aws-vm-pullers
            aws_eks: aws-eks-pullers
```

Semantic changes require a higher explicit revision. Existing durable pull plans
remain valid while their exact target and auth contract remains usable. In one
transaction, config reload locks the current desired and active rows, increments
the desired generation, marks an older unfinished desired revision SUPERSEDED,
and stages the new revision as QUALIFYING. It never makes provider calls or
blocks deployment.

Qualification is a bounded aggregation of revision-scoped, secret-free
attestations, not one powerful worker assuming every identity:

- the Terraform handoff attests desired repository, policy, quota, KMS, and role
  fingerprints but cannot by itself claim runtime readiness;
- a copy worker uses only the copy role to probe destination read/write and copy
  the fixed canary artifact;
- a lifecycle worker uses only the lifecycle role to delete and exactly verify a
  regional canary with no demand;
- an EC2 canary pulls through the declared instance-profile and exact regional
  host AMI whose ECR helper was present before workload start; and
- an EKS canary pulls through the declared kubelet node role.

Every declared EC2 region/AMI/role tuple and EKS cluster/role tuple needs its own
attestation; success for one tuple never qualifies another. Target qualification
orders canary copy, actual runtime pulls, then lifecycle deletion and exact
absence, so activation never races cleanup.

The runtime binding also declares the minimum launch tuple needed for an
automatic canary. EC2 qualification pins one IAM role, instance-profile name,
AMI, instance type, and bounded regional subnet/security-group allowlist. EKS
qualification pins one kubeconfig context, cluster ARN, node role, and dedicated
namespace. The separately referenced `canary_launch` authority may only launch,
inspect, and tear down these tagged canaries; it has no ECR write or lifecycle
permission. The fixed digest-pinned `canary_ref` is copied by the copy worker
into Terraform's non-catalog qualification repository. The canary worker pulls
that regional digest through the declared runtime identity, and the lifecycle
worker deletes it through `qualification_delete_authority`. A target cannot use
its ordinary `delete_authority: disabled` setting to skip qualification cleanup.

`canary_worst_case_cost_usd` is reserved atomically with the operation lease
before any child launch. It is a conservative operator-set ceiling for one run,
not an observed bill. `canary_timeout_seconds` bounds launch, pull, observation,
and teardown. V0 supports only `linux/amd64`; configuration rejects another
canary platform rather than building or launching an unused architecture.

Each service writes a bounded attestation through its authenticated internal
endpoint. A canary result includes a single-use nonce and actual-principal
evidence, never credentials: the canonicalized EC2 STS role ARN, or the EKS node
UID/node-group role paired with successful kubelet pull and pod start. Copy
workers may coordinate aggregation but cannot assume or simulate lifecycle or
runtime roles. `transactions.activate_profile()` locks the
current desired row and promotes it only after rechecking desired generation,
config and Terraform hashes, target fingerprints, and fresh required
attestations. A late older qualifier becomes SUPERSEDED and cannot activate. The
previous active revision remains selectable until the new one commits ACTIVE;
existing plans retain their exact old revision until their demands drain. New
runtime resolution, publication, and prepare requests likewise use that ACTIVE
revision's immutable snapshot while the configured successor is QUALIFYING.
Current workspace allowlists and authorization still apply, but a desired config
reload cannot create a deployment outage before its provider proofs converge.

Profile revision is routing and authority metadata, not physical image identity.
If a new active revision names the same qualified physical shard fingerprint, it
reuses the existing READY location after rechecking its new runtime binding. A
v0 revision that would change a canonical physical fingerprint after any release
exists fails validation. A later repository-generation feature must first define
copy-then-cutover custody and superseded-canonical deletion. This prevents
policy-only revisions from reserving or copying the same ECR manifest twice while
each pull plan still pins the exact authority revision it used.

Infrastructure and copy attestations refresh every ten minutes. A live runtime
pull refreshes its binding attestation. An actual-principal canary is required at
activation, after an AMI/helper, cluster/node role, repository-policy, or binding
fingerprint change, and before the configured maximum age. The background
scheduler creates the same idempotent PROFILE_CANARY operation ahead of expiry
when automatic canaries are enabled; its daily cost cap is hard, and exhaustion
makes the binding stale rather than spending more. Copy staleness stops new
publication and materialization for that target. Runtime staleness stops new
placement on that binding. Lifecycle staleness stops deletion only. Existing
verified pull plans remain usable. This is one scoped state machine in
`container_image_profile_revisions`, not a second configuration ledger. Config
validation still rejects ambiguous locality, duplicate targets, cross-partition
managed profiles, unbounded policies, or incompatible bindings before staging.

A workload never runs a qualification canary inline. A stale binding is removed
by the metadata eligibility pass; background or explicit admin canaries restore
it without extending an already-started deployment request.

## AWS managed slice

### Fixed repository layout

Terraform creates every v0 repository before profile activation. For each
declared workspace, region, and shard index, the name is deterministic:

```text
<prefix>/r<authority-base32>/w<workspace-hash>/g00/s<two-hex-index>
```

It also creates one fixed, non-catalog qualification repository per region. That
repository uses the same encryption and rendered access-policy template but is
outside workspace capacity. Copy, actual-runtime-pull, and lifecycle canaries use
only bounded untagged digests there, and the lifecycle attestation verifies their
removal. A qualification digest is never returned in a workload pull plan.

`authority-base32` encodes the 128-bit catalog authority. `workspace-hash` is a
128-bit, versioned hash of authority plus normalized workspace name. Terraform
and the API reject any collision across the declared workspace set and validate
the final ECR name and length. `g00` is the fixed v0 repository generation. This
prevents two SkyPilot control planes sharing an account from colliding and avoids
placing workspace display names in registry paths. A post-v0 shard expansion uses
a new explicit generation, profile revision, and reviewed custody migration; it
never changes old paths in place.
Adding a workspace likewise requires Terraform to create its fixed repositories
and a new qualified profile revision before that workspace can opt in. Until then
its direct OCI behavior is unchanged.

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
A Terraform handoff creates a previously unseen bootstrap shard as `PENDING`;
desired Terraform output is never live qualification. Reingesting unchanged
bootstrap facts does not reset a completed epoch. Changing limits on an unowned
bootstrap row waits for any inventory lease, resets it to `PENDING`, and requires
a new complete scan. Before validation, handoff ingestion locks every existing
physical shard for the profile in ID order. Once a shard has an operational
revision pointer, a later handoff validates immutable identity and reservation
floors but does not update its limits, inventory cursor, timestamps, state,
dispatch ceiling, or revision pointer. Those candidate limits remain in the
candidate revision's Terraform attestation. The copy worker assumes only the regional
copy/verify role and reads every physical shard's live ARN, URI, tag immutability,
encryption/KMS setting, scanning setting, repository policy, ownership tags, and
applied images-per-repository service quota. It canonicalizes the policy and
tags and compares them to the handoff fingerprints. The first complete inventory
must be empty. Only that live proof may promote the shard to READY and write the
profile's per-shard attestation. Reingesting a handoff cannot turn a DRIFTED shard
READY.

Provider API budgets follow the same boundary. Qualification may create a
missing account-region budget so first activation can make bounded probes, but
it never rewrites an existing live budget. The candidate's applied rate and
burst remain revision-scoped Terraform facts. Activation locks provider scopes
in deterministic order and applies those facts in its transaction while
preserving the persisted throttle backoff and clamping existing tokens to a
smaller burst.

The shard row also stores the target fingerprint. Its operational profile-
revision pointer and automatic-eviction bit remain on the current active values
(or unset/false before first activation) while another revision is merely
desired or QUALIFYING. They change only in the same transaction that activates
the fully attested revision. Copy and lifecycle workers resolve that
exact ACTIVE or RETIRED snapshot and recheck its target fingerprint against the
physical shard and location. Inventory qualification remains separately scoped
to the candidate revision and requires its exact per-shard Terraform
attestation. A newer same-named target cannot lend credentials or policy to
bytes in a different repository ring.

Inventory has three explicit authority modes. Before first activation, a
QUALIFYING revision may inventory an unowned PENDING shard and promote it after
the complete physical scan. Once a shard has an operational revision pointer,
resumable inventory resolves only that exact ACTIVE or RETIRED snapshot and is
the only path that can change shared shard or location state. A later
QUALIFYING policy revision instead performs a read-only per-shard probe with its
own verify authority and exact Terraform expectations. It may record candidate
evidence only while the shard still points to the same ACTIVE revision and its
READY/FULL operational inventory epoch remains fresh and unchanged. Candidate
credential, policy, or quota mismatch leaves the operational shard untouched
and blocks only candidate activation. Each maintenance pass probes at most 16
missing shards per target through the persisted provider budget; the current
ACTIVE revision remains available while larger rings converge in the
background.

Activation locks every revision for the profile in ID order, then every shared
provider budget in provider-scope order, then every physical shard in ID order.
For each shard it rechecks the candidate Terraform limits, exact target and
physical fingerprints, a fresh live proof for the shard's current inventory
epoch, the absence of an inventory lease, and either an unowned bootstrap slot
or the still-ACTIVE operational pointer. It then applies candidate capacity,
dispatch, eviction, and revision values, retires the old revision, and promotes
the candidate in the same transaction. Any stale epoch, failed reservation
floor, mismatched ring, or invalid budget rolls back every operational update.

A managed target first activates only when every workspace shard and the cleaned
qualification repository are empty, fingerprints match, and hard ceilings are no
greater than verified applied quotas minus headroom. Activation requires a fresh
live attestation for every exact physical fingerprint. Later revisions reconcile
the already managed inventory instead of requiring it to disappear. A repository
containing unexplained preexisting content must use external ownership instead.

After first activation, a revision may not lower manifest, declared-byte,
release, or regional-location ceilings below current reservations. It may raise a
physical ceiling only when a new Terraform handoff and live quota proof agree;
configuration alone never creates capacity.

On first location creation, the transaction starts from a stable digest-derived
shard index and probes the fixed ring. Admission uses an ordinary PostgreSQL row
lock rather than `SKIP LOCKED`: brief contention waits and rechecks the same
capacity predicate instead of being misreported as exhaustion. If PostgreSQL
READ COMMITTED EvalPlanQual drops a candidate that filled while the selector
waited behind `LIMIT 1`, admission executes a fresh statement snapshot so the
next eligible ring member can be selected. It locks one physical shard row,
rechecks
whether the location already exists, and reserves one slot only when
`reserved_count < max_manifests_per_shard` and conservative declared bytes fit.
The chosen shard is stored on the location forever. A full ring fails closed with
`REGISTRY_CAPACITY_EXHAUSTED`; it does not try an undeclared repository. A READY
canonical reservation is permanent in v0. Failed reservations decrement count
and bytes only after exact provider inspection proves that no manifest exists and
no retained publication or demand can recreate it.

Each shard stores a durable inventory epoch, provider cursor, started time, and
last-completed time. One reconciliation claim reads at most ten provider pages
or runs for ten seconds, then commits its cursor. An invalid or expired provider
cursor restarts the epoch safely. Observed managed digests update the matching
location's epoch marker. Only a completed epoch may nominate a missing manifest
for the exact confirmation required by the transition contract.
An in-flight or not-yet-written location consumes a reservation but is not
expected in inventory. An unexplained manifest, observed count above reserved
count, or manifest-present location absent from a complete epoch marks the shard
`DRIFTED` and stops new admission without breaking existing pulls.

Failed canonical reservations are reaped only after every dependent publication
reservation has expired, no demand remains, no lease is live,
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
  `BatchDeleteImage` only for qualification repositories and eligible regional
  workspace repositories, never canonical workspace repositories, with no push
  or repository deletion; and
- runtime pull principals: the actual EC2 instance-profile role, EKS kubelet
  node role, with token plus repository-scoped pull only.

A pod service account is not treated as the EKS image-pull principal. V0 supports
only the EKS node role for Kubernetes. Fargate, custom kubelet credential
providers, generic `imagePullSecret`, and non-cloud Docker helper bindings remain
post-v0. Credentials refresh at pull time and are never serialized into a pull
plan.

Each v0 qualified EKS cluster declares one node role shared by every node on
which SkyPilot may schedule these pods. Qualification fails for a heterogeneous
eligible node-role set rather than proving one node and trusting the others.
Multiple clusters are listed independently; a later node-pool binding may add
selector-to-role cardinality without changing the pull-plan contract.

A managed EC2 target is eligible only when its qualified node image already
contains the Amazon ECR credential helper, or an equivalent reviewed native
helper, before workload deployment. The pull plan configures the endpoint and
digest; the helper uses the instance profile and refreshes tokens. Managed launch
never downloads AWS CLI, runs per-deployment `docker login`, or persists an ECR
bearer token. EKS uses the kubelet's native ECR path. Legacy direct `image_id`
keeps its existing behavior and is not evidence that a managed runtime binding
is qualified.

Target-role trust names only the worker base principals and constrains session
duration, external ID, and catalog/profile session tags. Cross-account repository
policies grant exact copy or pull principals. Target roles have Terraform-managed
permissions boundaries, so accidentally broad identity policy on those roles
cannot escape the fixed repository set. SkyPilot cannot constrain a dedicated
account administrator; that administrative trust is explicit and all mutations
are drift-checked and CloudTrail-audited.

The module renders every repository policy before apply, caps explicit pull
principals, and fails when the provider policy-byte limit would be exceeded. A
single organization-and-principal-tag statement may replace an ARN list only
when the operator explicitly supplies the organization ID and the dedicated
compute accounts enforce that immutable role tag. Qualification compares the
exact rendered and live policy hashes; it never truncates principals or broadens
to account root silently.

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
regional registry and compute-account provider aliases, catalog authority/realm,
declared workspaces, prefix,
fixed shard count/generation, manifest and declared-byte ceilings,
encryption/scanning settings, quota headroom, exact compute pull-principal ARNs,
qualified EC2 AMI IDs/helper fingerprint, optional organization/tag policy
conditions, and optional existing worker role ARNs. It
reads applied repository and images-per-repository quotas when permitted;
otherwise it requires explicit validated inputs and leaves readiness false.

Its secret-free qualification manifest contains desired config hash, timestamp,
workspace encoding version, repository fingerprints and ceilings, role and
permissions-boundary ARNs, repository-policy hashes, applied quotas, KMS/grant
facts, rendered policy byte sizes, EC2 AMI/helper facts, and Terraform ownership
tags. Background
attesters compare this handoff with live provider state and actual-principal
canaries before activation. Terraform output alone never claims live readiness.

Import/adoption accepts only empty repositories with exact immutable settings
and ownership tags. Nonempty adoption remains external. Repositories use
`force_delete = false` and explicit destroy protection. Terraform destroy fails
while content exists; profile retirement additionally requires the Dashboard to
show zero live demands and pull plans and zero locations requiring old-only
authority. Policies and access bindings
for an old revision remain configured until that revision drains. The example
composes PostgreSQL/API infrastructure already owned by the platform and does not
duplicate database state.

### Why workers, not ECR replication or pull-through cache

ECR replication is push-triggered, preserves repository names, does not backfill
preexisting images, and is capped at 25 unique destinations. Pull-through cache
has a bounded upstream set and makes the workload's first pull perform the fill.
Neither gives SkyPilot per-digest JIT placement, READY-before-deploy, adoption of
arbitrary existing digests, durable copy recovery, or demand-aware regional
deletion. V0 therefore uses portable workers and does not configure either AWS
feature. They remain possible future optimizations behind the same verified
location contract, never alternative sources of truth.

## Worker services

Helm exposes three disabled-by-default deployments:

```text
imageCopyWorker.enabled
imageCopyWorker.replicaCount
imageCopyWorker.maxInFlight
imageCopyWorker.serviceAccount

imageLifecycleWorker.enabled
imageLifecycleWorker.replicaCount
imageLifecycleWorker.maxInFlight
imageLifecycleWorker.serviceAccount

imageCanaryWorker.enabled
imageCanaryWorker.replicaCount
imageCanaryWorker.maxInFlight
imageCanaryWorker.serviceAccount
```

Each pod uses its Kubernetes pod UID as a stable process-lifetime worker ID and
periodically upserts a bounded
heartbeat with kind, version, started time, last-success time, and current
in-flight count. The UI treats a heartbeat as stale after three periods. Stale
rows older than 24 hours are deleted by the lifecycle worker in batches of at
most 500 every five minutes. Heartbeats contain no hostname, token, ARN, or
credential. Worker reads are keyset-paginated, so a restart storm cannot produce
an unbounded API response while compaction catches up.

Every worker also exposes a dependency-free HTTP health and Prometheus text
surface on a dedicated port. Liveness fails when the main claim loop has not
ticked within its bounded deadline; readiness requires successful registration
and a recent PostgreSQL heartbeat. Helm configures startup, readiness, and
liveness probes against these distinct signals and annotates the metrics port.
A deadlocked loop is therefore restarted even when the Python process still
exists.

`kubernetes_dockerconfig_secret` is fail-closed. Helm accepts an explicit bounded
allowlist of source credential Secret namespace/name pairs, renders one
least-privilege Role and RoleBinding per namespace for a chart-managed service
account, and passes the same allowlist to the copy process. The process refuses a
configured Secret outside that list even if broader ambient RBAC exists. An
externally managed service account still supplies the allowlist and owns matching
RBAC itself.

Copy-worker concurrency is bounded by its pod setting and provider throttling.
Adding replicas increases claim throughput safely because leases and
`SKIP LOCKED` prevent duplicate authority. A grant transaction locks one
PostgreSQL account-region-API budget row and the worker row, deducts at most one
second or 64 calls of capacity, and records an expiring grant. The worker spends
that batch locally, so layer APIs do not update one hot row per call; a crash only
loses a bounded grant until refill. Applied quota, refill rate, and burst are
qualification inputs. A provider throttle writes one shared exponential
`blocked_until`, so scaling pods cannot multiply past the account limit. ECR's
default `PutImage` rate is only 10 per second, and the UI reports a quota-bound ETA
rather than implying worker replicas can exceed it.

Location dispatch is two-level and no-starvation: select an eligible physical
shard by oldest `last_dispatch_at` under its target in-flight ceiling, then claim
its oldest eligible location. Source-inspection claims rotate by
profile/workspace. If another target has eligible work, one target cannot consume
every consecutive claim. `FULL` is an admission state, not a dispatch stop:
already-reserved `PENDING` work remains claimable on `READY` and `FULL` shards.
An expired `COPYING` or `VERIFYING` lease remains reclaimable even if the shard
later becomes `DRIFTED` or `DISABLED`; recovery performs an exact destination
read and repairs the in-flight counter, but fresh writes remain blocked outside
`READY|FULL`. Re-admitting a `FAILED`, `MISSING`, or `EVICTED` location is new
write admission and therefore also requires `READY|FULL`, including canonical
publication retry. QUARANTINED is not retryable on its original physical
reference. The dispatcher rechecks shard state from its locked row after
selecting a location, so an expired-lease heartbeat cannot make a `DRIFTED`
shard fall through to fresh work. Lease reconciliation repairs in-flight
counters after a worker expires.

The lifecycle worker reloads workspace retention policy behind a failure
boundary. A malformed or temporarily unreadable configuration keeps the last
valid cutoff map and logs a bounded warning instead of terminating eviction,
lease reconciliation, or compaction. Before the first valid map has loaded,
retention eviction is disabled for every workspace while the worker continues
non-destructive reconciliation and retries configuration refresh.

Worker budgets do not pretend to control calls made by remote container
runtimes. Node pulls use per-node credential reuse plus bounded exponential
backoff and jitter against qualified pull quotas. A thousand service replicas may
still cause many node layer downloads; registry locality avoids cross-region
transfer but does not claim that an OCI registry prewarms each node cache.

The copy worker owns only its copy attestation, bounded aggregation of independent
attestations, and shard inventory claims. It validates `OciContentGraph` before
destination I/O and uses separate source-read and destination-write sessions.
Lifecycle workers own their lifecycle attestation and claim only demand-free,
noncanonical, managed locations past retention, plus provably
empty failed canonical reservations for counter reclamation. They inspect the
exact digest after ambiguous deletion and never delete a repository or a READY
canonical manifest. Copy and lifecycle use one shared lease-heartbeat primitive.
Copy synchronously proves ownership around managed source/destination ECR
credential acquisition and around every provider-budget wait, observes
cancellation while transferring immutable content, and still relies on
token-checked state convergence for a call already in flight. External source
inspection is read-only, and destination authority remains fenced after it.
Lifecycle additionally makes durable `DELETE` intent a synchronous precondition
of every destructive provider call.

Shutdown stops new claims, cancels work that has not started provider I/O, and
lets leases expire after ambiguous I/O. Restart recovery verifies actual
registry state for immutable copy work and pre-delete claims. An expired
destructive intent quarantines its exact physical reference without trusting a
read to bound an older delete.

## Dashboard

The Dashboard contains one first-class Images navigation item with Catalog and
Readiness tabs. It does not add a registry editor or Terraform surface.

### Images catalog and detail

Authorization maps to four explicit capabilities:

| Capability | Existing authorization | Actions |
| --- | --- | --- |
| `images:read` | user may view the workspace | Catalog, detail, bounded operation status |
| `images:use` | user may launch/manage resources in the workspace | Resolve a published image and implicitly prepare only the selected placement |
| `images:publish` | explicit workspace owner/operator grant | Publish, single-target Prepare, and Retry within that workspace |
| `images:admin` | server administrator | Qualification ingestion/canaries, all-workspace readiness and remediation |

Every lookup checks workspace access before selector resolution. Binding names
appear only when the caller can use them; credential values never do.

Authorized users can:

- select a workspace;
- page artifacts with digest, releases, platforms, size, and updated time;
- filter by release, digest, distribution, target, and location state;
- open artifact detail with sources, release reservations, locations, errors,
  demands, and publication history;
- start Publish, Prepare, Retry publication, and Retry location actions when
  authorized;
- follow asynchronous request progress without duplicate submission; and
- see empty, loading, stale-cursor, permission, old-server, and error states.

The UI never fetches credentials or raw server configuration. It displays
bounded, code-valued errors and copyable remediation commands.

An explicit Retry location request distinguishes a concurrent successful retry
from a still-failed location on a shard outside `READY|FULL`. The former returns
the refreshed row; the latter fails with `REGISTRY_SHARD_UNAVAILABLE` instead of
recording a misleading nonterminal operation against an unchanged failed row.

`REGISTRY_SHARD_UNAVAILABLE` is a typed 409 conflict from the persistence fence
through the direct REST API. Client-safe error text and the dashboard direct the
operator to repair shard drift or activate a qualified revision; arbitrary
provider text never crosses the boundary.
`REGISTRY_LOCATION_QUARANTINED` is the corresponding typed 409 for an exact
physical reference that may still be affected by a late delete. Its remediation
requires a newly qualified repository-ring fingerprint, not an unsafe retry.

Mutation forms validate source digest, release, platform, distribution, binding,
and target combinations locally and again on the server. One idempotency key is
retained for the form submission until terminal state. Polls are keyed by
operation and view generation; navigation aborts the request and discards late
responses. Stop waiting is labelled `Detach`, because it never cancels committed
provider work. A stale cursor prompts a clean first-page reload without replaying
an action.

Status labels never conflate layers:

- publication `PENDING|INSPECTING|READY|FAILED`;
- registry location
  `PENDING|COPYING|VERIFYING|READY|FAILED|MISSING|EVICTING|EVICTED|QUARANTINED`
  and a
  queue/quota-based preparation ETA;
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
- desired generation versus active revision, per-capability attestation hash/age,
  repository and role readiness, reconciliation progress, count/declared-byte
  headroom, quota backoff, and drift;
- copy and lifecycle worker healthy/stale counts;
- queue depth and oldest pending/retry age by profile/target; and
- capability failures that prevent managed-profile activation.

V0 deliberately has no browser profile editor. Operators change versioned
configuration and Terraform through normal GitOps, then use the panel to verify
convergence. This removes a second configuration transaction system without
making the feature raw-YAML-only operationally.

The infrastructure-adjacent UI mutations are the bounded, secret-free
qualification-manifest upload and the asynchronous one-target EC2/EKS canary,
with explicit cost/target confirmation. They stage evidence for background
verification; the browser cannot edit a profile, run Terraform, assume a role,
or mark itself qualified.

Every readiness response is a projection of PostgreSQL state written by bounded
background work. A Dashboard request never assumes a role, calls STS/KMS/ECR,
resumes inventory, or refreshes qualification. Stale timestamps are shown as
stale rather than synchronously repaired. Queue counts are capped index-backed
queries and oldest ages take one indexed head per physical shard before a final
minimum; neither operation scans or sorts the full queue. The browser advances
its own clock, keeps refresh errors visible beside cached data, and disables
prepare, retry, and qualification mutations while the snapshot is stale.

### Mutation API

```text
POST /images/publications
POST /images/artifacts/{id}/prepare
POST /images/publications/{id}/retry
POST /images/locations/{id}/retry
POST /images/profiles/{name}/qualification
POST /images/profiles/{name}/canaries
```

Each accepts `Idempotency-Key`, returns its versioned mutation result directly,
and authorizes workspace/profile scope before looking up the named object. It
returns the promptly committed current state; the CLI/SDK default waiter polls
the read API. It never wraps the result in SkyPilot's generic request-row
response. Detaching leaves the same operation ID available through the read API.

### Direct read API

```text
GET /images/catalog?workspace=W&limit=50&cursor=C
GET /images/publications?workspace=W&state=S&release=R&limit=50&cursor=C
GET /images/artifacts/{id}?workspace=W
GET /images/artifacts/{id}/releases?workspace=W&limit=50&cursor=C
GET /images/artifacts/{id}/sources?workspace=W&limit=50&cursor=C
GET /images/artifacts/{id}/publications?workspace=W&limit=50&cursor=C
GET /images/artifacts/{id}/locations?workspace=W&limit=50&cursor=C
GET /images/artifacts/{id}/demands?workspace=W&limit=50&cursor=C
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

The workspace publication feed is required for recovery, not a duplicate
catalog. A publication that fails while inspecting its source has no artifact
ID yet, so an artifact-scoped endpoint cannot rediscover it after the initiating
client detaches. The feed returns bounded reservation and operation projections,
including unbound PENDING, INSPECTING, and FAILED rows, and is the only way the
CLI or Dashboard locates such a publication for explicit retry.

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
  live WARMING or READY demand.
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
5. Deploy one copy worker, one lifecycle worker, and one runtime-canary worker
   with separate identities.
6. Let each worker attest only its own capability, run actual-principal EC2 and
   EKS canaries, and atomically activate the desired generation only after all
   repository, quota, KMS, policy, runtime, and fingerprint evidence is fresh.
7. Verify publish, warming, pull, API/controller restart, retry, capacity
   admission, drift fail-closed behavior, and demand-fenced eviction.
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
- Every publication has a release, requires `images:publish`, and reserves
  bounded permanent canonical count and declared bytes before destination I/O.
- A failed publication leaves every prior release and deployment launchable.
- One placement attempt creates at most one location intent and warming never
  causes cloud or region failover.
- `IMAGE_WARMING` survives API and controller restart with the same consumer
  generation, profile revision, target, and location.
- Copy crashes before and after manifest publication converge to one verified
  digest.
- At 1,000 replicas and eight GPUs per node, copy cardinality equals requested
  physical targets and demand cardinality equals service-version targets, not
  replicas, nodes, or GPUs.
- Image readiness only gates new Serve replica eligibility; Serve's existing
  capacity-aware controller exclusively owns traffic, rollback, and drain.
- Regional eviction cannot pass a concurrent WARMING or READY demand or a
  canonical-location fence.
- Every physical shard refuses admission at its hard ceiling, and provider drift
  stops new writes before the database can claim additional capacity.
- No API, placement, or Dashboard read performs registry, STS, KMS, or Terraform
  I/O.
- V0 resolves an index to one declared platform child before destination
  authority, and never uploads the parent or an unselected child.
- Managed EC2 pulls use a prequalified native helper and never install AWS CLI or
  run per-deployment `docker login` on the image hot path.
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
- EC2 instance and EKS kubelet runtime pull-auth refresh tests, preinstalled
  helper/AMI enforcement, homogeneous EKS-node-role validation, multi-cluster
  attestation, no managed-path CLI install/login, plus
  unchanged direct OCI behavior on other runtimes;
- `terraform fmt -check`, `terraform validate`, and plans for one and multiple
  regions with fixed shards;
- worker kill/restart and ambiguous-outcome tests around source reads, layer
  availability/download/upload/complete, `PutImage`, exact verification, SQL
  completion, publication fan-out, eviction, and attestation activation;
- replay after lost mutation responses, key/body collision, detach before/after
  intent commit, stable result shape, bounded error, and CLI remediation tests;
- canary nonce/principal proof, child-launch crash deduplication, forced teardown,
  automatic refresh, concurrent daily-cost reservation, and stale-binding tests;
- idempotency collision-matrix, canonical publication fan-out, controller
  restart, shard-ceiling, and inventory-drift tests;
- source/destination/compute account separation, cross-identity attestation,
  negative STS trust, permissions-boundary, repository-policy size/principal,
  KMS grant, protected destroy, empty import, desired-generation fencing, and
  old-revision drain tests;
- policy-only profile revision location reuse and physical-layout-change
  rejection after first release;
- one-million-row resumable inventory, exact missing confirmation, durable cursor,
  batched token grants, hot/cold target no-starvation, throttling, count/byte
  ceilings, and empty failed-reservation reclamation tests;
- demand aggregation/tombstone/orphan tests for cluster, job recovery, Serve
  version-target, controller loss, supersede, generation watermark,
  interrupted terminal confirmation, INIT-versus-reconciliation absent-row
  serialization, authoritative owner retirement, compaction, and unreachable
  consumer stores;
- single AMD64 manifest, selected AMD64 index child, ambiguous/wrong platform,
  nested index, artifact, nondistributable/foreign layer, external URL, config
  platform, raw-byte digest, and size-bound reject-before-write tests;
- Jest interaction, pagination, permission, responsive, and stale-state tests;
- Dashboard capability matrix, action validation/idempotency, stale poll and
  navigation suppression, detach semantics, old-server deep links, keyboard,
  screen-reader, reduced-motion, separate publication/location states,
  attestation/canary flow, ETA-layer, and secret-absence tests;
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
with no more than six additional paired rounds in one review cycle. A verified
blocker resets the consecutive count. If a blocker is found too late to complete
the streak inside that cycle, the blocker is fixed and one new bounded review
cycle starts; the round budget never waives the three-consecutive requirement.

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

Round 5 at `a3884f19dc6c8c67aea314d7563b4162794f631c` returned Codex `RESHAPE`.
Fable again reported no usage credits, so the round is not paired. This revision
replaces cross-identity qualification with capability-specific attestations,
requires release-backed and quota-bounded permanent custody, defines every
destination/ECR ambiguity transition, fences desired revisions and consumer
generations, folds release/reference projections out of separate tables, batches
provider tokens with fair dispatch, requires exact inventory confirmation, keeps
AWS CLI off managed launch, and leaves Serve as the sole drain owner.

Implementation review round 1 at
`26cdfc40d3ba2160b09bdd17c98448c8269213f1` returned paired Codex `RESHAPE`
and Fable `RESHAPE`. Both confirmed the overall split and digest-custody model,
but found contract breaches in eviction re-admission, restart-stable demand
ownership, READY locality ranking, worker health, and acceptance proof. Codex
also found transient shard-lock misclassification, single-node EKS proof,
Terraform-intent trust, unbounded readiness aggregation, missing source-Secret
RBAC, and stale UI health. This revision incorporates the union as one
architecture correction before implementation round 2. The correction also
makes terminal request lookup index-bounded, applies locality ranking globally
across task alternatives, replays retired immutable profile snapshots, splits
copy and lifecycle IAM boundaries, discovers applied ECR quota during planning,
and makes PostgreSQL concurrency and scale tests mandatory in CI.

Implementation review round 2 at
`7c1caac00649b9afc82d9e722482fec5ea0b8059` returned paired Codex `RESHAPE`
and Fable `RESHAPE`. Both found the exact-head acceptance run invalid while
mandatory CI was red and required controller-owned deployment epochs rather
than stable strings hashed into database integers. Fable additionally found
that request-body-based sanitization erased legacy and code-valued errors, that
authoritative cluster deletion waited on the missing-owner reconciliation
delay, and that shard admission retained one PostgreSQL fill-during-wait race.
This revision replaces the hashed token with a locked controller-epoch mapping
and optional monotonic controller sequence, scopes Serve ownership by version
target, and derives managed-job recovery epochs from durable job state. It makes
first-party cluster teardown atomic with demand release, uses typed
image-boundary errors without rewriting legacy or provisioning failures,
revalidates the exact profile-derived READY pull plan, and retries shard
selection from fresh READ COMMITTED snapshots until a separate snapshot proves
capacity exhaustion after EvalPlanQual drops filled candidates. Runtime
placement is a complete immutable demand field, including backend and platform;
EC2 and EKS plans reject every unqualified helper, principal, instance profile,
node selector, or extra field. Error-marker traversal is bounded and cycle-safe,
multi-row request termination locks deterministically, and both copy and
lifecycle workers can resume bounded publication fanout. It also reconciles the
Serve migration chain at revision 021, regenerates the Helm schema, restores
immutable YAML fixtures, removes the duplicate test-module basename, and
updates Python 3.14 static-analysis contracts. Activation remains disabled until
the resulting exact head passes every operational gate.

Implementation review round 3 at
`21958988d04e0f9fb66862dc309e1fc2d66d58ad` returned paired Codex `RESHAPE`
and Fable `RESHAPE`. Both found that READY demand commit acquired a location
before its artifact while regional admission used the opposite order. Fable
also found that Serve omitted its incarnation from the stable owner key, so a
same-name recreation at version one could collide permanently with the prior
watermark. The next revision moved the artifact lock before location and added
a deterministic real-PostgreSQL interleaving proof. It also put the durable
service incarnation into the version-target owner while preserving one shared
owner for 1,000 replicas.

Implementation review round 4 at
`2464318ba8bd3726cadd73145c6662a0af38d712` returned paired Codex `PURSUE`
and Fable `PURSUE`. Both verified the repaired lock graph, service-incarnation
ownership, prior blocker set, and disabled rollout boundary with no blocking
finding.

Implementation review round 5 repeated the exact immutable
`2464318ba8bd3726cadd73145c6662a0af38d712` tree and returned Codex `RESHAPE`
and Fable `PURSUE`. The deeper Codex pass found that terminal compaction still
locked demand rows before their watermark, opposite every controller terminal
path, so concurrent idempotent release could deadlock and abort the synchronous
lifecycle maintenance loop. This revision makes compaction discover candidates
without locks, then process each owner in deterministic watermark-before-demand
order with under-lock revalidation and a real-PostgreSQL concurrency proof.

Implementation review round 6 at
`7cc35c1c6eb214eb84bdca71f1bdaeee28adc8b1` returned Codex `RESHAPE` and
Fable `PURSUE`. Codex found that deleting an empty owner watermark could race a
creator already waiting in `INSERT ... ON CONFLICT DO NOTHING`. The real
PostgreSQL reproduction refined the exact outcome: the waiter first observed a
raw missing-row failure after the conflicting row disappeared, and a later retry
could then recreate a fresh watermark without the terminal fence. This revision
keeps one permanent watermark per stable owner, rejects creation after
authoritative deletion under that row lock, removes orphan-watermark discovery,
and proves both the blocked creator and its retry cannot resurrect the owner.

Implementation review round 7 at
`8ed518c0a8a8f4940017c087555258e7a02604e0` returned Codex `RESHAPE` and
Fable `PURSUE`. Both found that `mark_owner_deleted()` had no production caller,
so the safe compactor was inert and terminal demand payloads grew without
bound. Wiring reusable cluster names directly to irreversible deletion would
also prevent same-name recreation. This revision makes cluster owners
launch-incarnation-safe, retires named clusters atomically with cluster-row
deletion, retires inferred cluster, managed-job, and Serve owners on their final
authoritative observation, and proves expired demand compaction through each
production lifecycle. Since the permanent fence rejects every later creator,
the obsolete credential-expiry gate and column are removed rather than
inventing a provider-specific credential lifetime.

Implementation review round 8 at
`54d94afcc7e7b33f42cc37dc079627b2e719a136` returned Codex `RESHAPE` and
Fable `PURSUE`. Codex proved that cluster reconciliation compared only the
reusable name, so an orphaned incarnation A could remain live forever when
incarnation B recreated the same name before A's second terminal observation.
This revision persists the validated consumer kind and owner on the cluster row
from INIT, compares that exact binding during reconciliation, and uses it for
authoritative direct retirement. It also proves that a same-name replacement
releases and compacts the old incarnation without affecting the replacement.
Any intervening current or indeterminate lifecycle observation now clears stale
terminal evidence, while the confirmation timer uses a distinct rotation that
preserves it. The PostgreSQL proof covers missing A, reappearance of the exact A
binding, replacement by B, and two fresh observations before A is retired.
Rows with a false binding-known bit remain conservative during mixed rollout,
while current direct-image rows record a known absence and cannot mask an old
managed incarnation.

Implementation review round 9 at
`438c03a8af1707610635c186e01fffea0e4a0bbe` returned Codex `RESHAPE` and
Fable `PURSUE`. Codex proved an absent-row race: reconciliation could classify a
cluster as missing, then a concurrent INIT could validate its READY demand and
insert the exact live binding before terminal observation retired that owner.
This revision serializes every INIT/upsert, deletion, and final reconciliation
with one transaction-scoped PostgreSQL advisory lock keyed by cluster name. The
reconciler now performs its final binding read and demand mutation in the same
transaction, and a deterministic PostgreSQL interleaving proves it waits behind
uncommitted INIT and preserves the live fence. It also requires durable terminal
request evidence before the 24-hour unattached-demand fallback, makes corrupt
legacy handles fall back to conservative reconciliation, and removes the unused
persisted-plan bypass scaffolding identified by Fable.

Implementation review rounds 10 and 11 independently repeated the exact
immutable `1c88a6f9b5a0bacdc645d0cfb6eb558633d1e7cf` tree and both returned paired
Codex `PURSUE` and Fable `PURSUE`. Round 10 established the first acceptance in
that streak, and round 11 deliberately retraced the cluster-incarnation and
owner-retirement schedules without finding another blocker.

Implementation review round 12 at
`c53a1d4b527927ef32d6aaf2032baf6d4fb0b0b0` returned Codex `PURSUE` and Fable
`RESHAPE`, resetting the streak. Fable found that an expired eviction with a
new live demand could remain unreclaimable, and that a new authorized demand
could find an EVICTED or MISSING row without re-admitting it. The next revision
made expired work exact-read reclaimable and routed new and replayed demand
through the fenced re-admission transaction.

Implementation review round 13 at
`2df8f2d7e935b2cf02d82c83ec96dfe3855d6e03` returned paired Codex `RESHAPE` and
Fable `RESHAPE`. The union identified lifecycle authority selected from a newer
revision, global eviction starvation behind one locked shard, immediate eviction
of never-used prepared bytes, an inert per-workspace retention opt-out, and a
false no-I/O restore after an earlier provider attempt. The next revision pinned
lifecycle to the shard's operational revision, made dispatch shard-first with
`SKIP LOCKED`, used the verified/created retention anchor, honored workspace
opt-out, and separated fresh EVICT, VERIFY, and RECLAIM completion semantics.

Implementation review round 14 at
`ecd3d1f1c16f2d306afa971f64f2021715ed5da2` returned Codex `RESHAPE` and Fable
`PURSUE`. Codex proved that inventory could borrow a same-ring QUALIFYING
revision's authority and mark the ACTIVE shard DRIFTED. The next revision bound
operational inventory to the shard's exact ACTIVE or RETIRED pointer and added
the read-only, epoch-fenced candidate probe described above.

Implementation review round 15 at
`afde6539019587b7512488be9a08a67b3cb359d4` returned Codex `RESHAPE` and Fable
`PURSUE`. Codex proved that Terraform handoff ingestion still rewrote ACTIVE
shard limits, inventory timestamps, and shared provider budgets before candidate
activation. This revision keeps those facts revision-scoped, makes existing
budgets create-only during qualification, rechecks live quota and inventory
epoch under locks, and applies capacity, dispatch, budget, eviction, and revision
changes atomically during activation. It also aligns profile-row lock order and
enforces the fresh-EVICT-only no-I/O restore rule in the database transaction.

Implementation review round 16 at
`da540abf24efa2770eb6550e3436437971028793` returned Codex `PURSUE` and Fable
`RESHAPE`. Fable proved that a shard becoming `FULL` after its final reservation
could permanently strand that already-admitted `PENDING` location and every
expired copy lease on the shard. The next revision separates admission from
dispatch, reclaims expired leases across later admission-state changes, fences
re-admission to `READY|FULL`, and preserves the last valid workspace-retention
policy when configuration refresh fails.

Implementation review round 17 at
`d82b953f1c7013aa01348b2f2477e8ae0363129d` returned paired Codex `PURSUE` and
Fable `PURSUE`. Both independently traced the repaired FULL-shard dispatch,
expired-lease recovery, candidate isolation, activation fencing, and retention
reload behavior without finding a blocker.

Implementation review round 18 repeated the exact immutable
`d82b953f1c7013aa01348b2f2477e8ae0363129d` tree and returned Codex `RESHAPE`
and Fable `PURSUE`, resetting the acceptance streak. Codex proved that a stale
lifecycle worker could resume a destructive delete after its lease was
reassigned, after a verifier restored the location to READY, and after live
demand committed. This revision therefore shares the copy heartbeat with
lifecycle work and makes exact lease ownership a precondition of every provider
call. It also incorporates Fable's bounded findings by reopening a FULL shard in
the reservation-release transaction, preserving the typed release-limit error,
and making explicit retry report an unavailable shard instead of binding an
unchanged failed row. The resulting full-suite run also exposed that Python's
`pathlib` treats a terminal `/**` as directory-only; the disabled builder harness
now expands that documented recursive include, including bare `**`, portably
instead of omitting late-bound source files. The unavailable-shard fence is also
now typed end to end as an HTTP 409 conflict, with bounded client and dashboard
remediation, rather than degrading into `INVALID_IMAGE_REQUEST`.

The focused round-18 repair review then split: Fable accepted the synchronous
lease fences, while Codex rejected a remaining in-flight-delete schedule. A
worker can durably pass the last lease check, pause while its ECR request is in
flight, lose the lease, and complete its delete after a successor has read
presence and restored READY. Because ECR offers no conditional delete keyed by
the database lease, exact readback cannot close that interval. This revision
therefore adds the durable `EVICT` to `DELETE` intent boundary and permanent
per-location quarantine described above. It deliberately preserves the capacity
charge and requires a different repository-ring fingerprint rather than
guessing that an old request has stopped. The acceptance streak remains reset
until both reviewers approve this exact implementation.

The third focused repair review returned Codex `ACCEPT_REPAIR` and Fable
`REJECT_REPAIR`. Fable proved that a transport timeout after ECR accepted a
delete could be followed by a temporarily-present readback and an unsafe READY
restore before the delayed delete landed. This revision permits delete readback
only after a successful provider conclusion or an explicit no-mutation
rejection; transport, timeout, and ambiguous server failures now enter the
existing quarantine path without readback. It also restricts no-I/O restoration
to pre-delete `EVICT` leases, fixes the race test's logical heartbeat clock,
binds typed retry conflicts to terminal idempotent operations, and projects
bounded quarantine-retained capacity in the readiness UI.

Final acceptance round 1 at
`6f73ba70cb073d8cdd2e2ae75b19f057f598fcee` returned Codex `RESHAPE` and
Fable `PURSUE`, resetting the streak. Codex proved that a copy worker could wait
inside the shared provider budget, lose its location lease, and then begin a
destination ECR call because its hook did not re-prove ownership after the wait.
Destination STS acquisition also preceded heartbeat ownership. This revision
therefore starts the heartbeat before source or destination credentials, moves
destination authority acquisition after source validation, fences credential
acquisition on both sides, and binds synchronous lease checks before and after
every copy ECR provider-budget wait.
