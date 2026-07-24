# Managed container image distribution

Status: implementation and verification in progress, feature disabled by default

Owner: SkyPilot control plane

Last updated: 2026-07-24

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
- additive, retained qualification-repository generations for destructive
  protocol upgrades and quarantine recovery;
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
- workload-shard generation expansion beyond the fixed Terraform layout;
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
| Qualification mutation barrier | The one catalog-wide durable fence around qualification-digest deletion and restoration | qualification workers |
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
  topology_state.py         profiles, global qualification barrier, shards, leases
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
   OCI behavior and release/artifact selectors fail immediately with
   `PROFILE_NOT_ACTIVE`, before consumer-demand lookup or any PostgreSQL-only
   managed-image state access, unless the resolver is replaying an already
   durable consumer demand whose immutable profile snapshot remains authority.

A server default is therefore a default for opted-in workspaces, not a global
behavior switch. Profiles are complete atomic objects. Workspace configuration
may restrict `allowed_profiles` and choose:

- `managed_required`: unknown or unready managed identity waits or fails closed;
- `managed_preferred`: an exact request-supplied digest may pull directly while
  a known artifact location warms; or
- `direct` or absence of image policy: preserve direct behavior.

Resources without `container_image` bypass this subsystem before cloud or
Kubernetes classification. Enabling the code therefore cannot add an image
precondition to an ordinary launch or dry run.

An exact direct ref is self-contained. It remains usable through the existing
direct OCI path even when the central database is SQLite, while release and
artifact selectors never enter that direct path. The resolver rejects those
managed-only selectors once per request instead of cycling cloud candidates or
reaching INIT persistence without a runnable container identity.

Locality is `prefer`, `require`, or `canonical` only for managed selection. On a
placement with a supported managed runtime binding, `distribution: direct` is
allowed under `direct` or `managed_preferred`, never under `managed_required`.

Managed policy applies only after the selected placement is proven to use a
supported managed runtime binding. An exact request-supplied digest `ref` on
GCP, Nebius, generic Kubernetes, or another unsupported runtime keeps direct OCI
behavior even when the workspace is `managed_required`; a `release` or
`artifact_id` selector still fails closed because it has no direct identity.
Runtime and platform classification precedes consumer-demand, profile, catalog,
and central-database access. The same rule applies to an AWS placement whose
architecture is unsupported by v0: an exact ref with no release or artifact
selector returns through the direct path immediately, whereas managed-only
selectors fail closed. A prior AMD64 demand for the same controller epoch
cannot turn an ARM64 candidate into a managed lookup or revoke that direct
compatibility path.
In a `direct` workspace these unsupported candidates keep the ordinary direct
locality rank, so enabling this code does not bias a multicloud optimization
toward AWS. Under a managed policy they form the direct fallback class behind a
READY qualified managed route.
An AWS `managed_preferred` candidate whose managed location is not READY joins
that same direct fallback class. Its optimizer candidate and eventual direct
launch retain the exact original resources, including the original host-image
selection. The qualified AMI and runtime binding are applied only if the
managed location is READY by the final locked resolution. A direct GCP or
generic-Kubernetes alternative therefore does not incorrectly eliminate an
equally executable AWS direct fallback, and a warming managed attempt cannot
mutate the host AMI of the direct path. If readiness is lost while the final
pull plan is being committed, the new managed demand is superseded and that
same launch resumes with the original direct resources.
Kubernetes is classified as managed EKS only when its exact selected context is
declared by an active EKS binding and that binding qualifies the cluster ARN,
node role, namespace, and node selector. A Kubernetes placement is never
inferred to be EKS merely because it uses the Kubernetes cloud abstraction.
Optimizer admission and the final pre-provisioning resolver use one shared
placement classifier, so candidate ranking and launch cannot disagree. That
classifier determines architecture and runtime platform before consulting any
registry profile. For an exact digest-pinned `ref` with no `release` or
`artifact_id`, a Kubernetes context is generic direct unless configuration can
positively prove the exact managed EKS binding. Missing, disallowed, or
malformed profile configuration therefore preserves the exact-ref direct path;
it is not an image-plane failure on generic Kubernetes. The same configuration
errors remain fail-closed for `release`, `artifact_id`, and combined managed
selectors because those requests have no self-contained direct identity. Final
placement classification executes inside the typed image error boundary, so a
managed-only classification failure is sanitized consistently with resolver
failures before reaching request state or logs.
Workspace policy parsing independently validates that `allowed_profiles` and
`publishers` are bounded collections of strings before normalizing them. It
therefore reports every malformed collection shape as a value error rather than
leaking an incidental iterator or hashing exception. The shared classifier can
preserve the exact-ref generic-Kubernetes fallback even when it receives a raw
snapshot that bypassed normal configuration-schema validation, while managed-only
selectors still fail closed. That exact-ref boundary treats both value and type
errors from configuration-only profile classification as a negative managed-EKS
proof. Downstream metadata resolution does not then reintroduce the same policy
failure while computing locality: it keeps the executable direct candidate in
the conservative managed-fallback rank and returns before managed database or
profile access. These boundaries do not catch provider, database, or arbitrary
runtime failures.
An absent workspace `container_images` key alone selects the unchanged default
policy. The parser distinguishes that absence with a private sentinel; an
explicit `container_images: null` is malformed and cannot silently become the
default. The model constructor is independently total over raw snapshots: it
validates list or tuple shape, size, string members, and uniqueness before any
`len`, iteration, or hashing that could leak `TypeError`.

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
2. In one transaction, create a durable unbound PENDING publication and reserve
   its release. The canonical location remains null until source inspection;
   deployment still cannot observe a release. A stored generated
   `inspection_claimable_at` projects only unbound PENDING and INSPECTING rows
   into one exact partial queue index.
3. A copy worker claims only an unbound publication from that projection with a
   random fenced inspection lease, moves it to INSPECTING, authenticates only to
   the source, and builds
   `OciContentGraph` before destination authority or I/O. A single manifest must
   match the requested platform. An index must contain exactly one matching
   runnable child. In one transaction, the worker rechecks the inspection token,
   persists the immutable source-root digest, selected child/runtime digest, and
   platform, converges the artifact/source, reserves one shard, creates or reuses
   its canonical intent, enforces the artifact's release ceiling while holding
   the artifact lock, clears the inspection lease, and returns the publication
   to bound PENDING. Binding removes it from the inspection queue in the same
   transaction.
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
6. A worker crash or ambiguous source read leaves unbound INSPECTING until its
   lease expires; another worker then reinspects from the immutable source root.
   Retry of an unbound pre-inspection failure locks only its publication and
   requeues inspection. A terminal pre-inspection failure atomically marks both
   the publication and its bound operation FAILED, so synchronous waiters cannot
   remain PENDING after the work has ended. Once bound, retry locks the shared
   canonical location before dependent publications, returns retained failures
   to PENDING, and reuses the location. A bound PENDING publication is never
   eligible for source inspection again; only canonical convergence may finish
   it. It never creates a second physical copy. Exceeding the per-artifact
   release ceiling is a typed `IMAGE_LIMIT_EXCEEDED` failure rather than a
   source-content validation failure.

Source adoption is also a network trust boundary. Publication request handling
validates syntax only and performs no DNS or registry I/O. The isolated copy
worker accepts only credential-free HTTPS source and redirect URLs, disables
environment proxy inheritance, rejects localhost and non-public IP literals,
and checks the actual connected peer immediately after every TCP connection and
before TLS, HTTP bytes, or credentials. This closes direct private addresses,
DNS rebinding, bearer realms, and signed-URL redirects over private, link-local,
or multicast IPv4 and IPv6 space. Multicast is rejected explicitly instead of
depending on Python's version-specific `is_global` classification.

One narrow AWS exception supports ECR PrivateLink and private DNS. When and only
when an `aws_assume_role` source binding matches the source's exact validated
`<account>.dkr.ecr.<region>.amazonaws.com` authority, the worker may accept a
private connected peer for that exact registry hostname. The adapter first
assumes the configured role and mints an ECR token for the same account and
authority. HTTPS certificate and hostname verification, digest verification,
lease fencing, body bounds, and disabled proxy inheritance remain mandatory.
The exception is hostname-exact: a different bearer realm or blob redirect
still uses the public connected-peer guard and receives no source
authorization. Thus an AWS service endpoint reached privately is supported, but
an arbitrary private-network OCI registry is not.

Basic source credentials may authenticate a bearer realm only on the same
normalized authority. Token requests never follow redirects. A blob may follow
at most one public HTTPS redirect without forwarding source authorization.
Manifest, token, config, and blob inspection bodies are streamed under explicit
byte limits. Other private-network source registries require a future qualified
operator-controlled network policy and are not accepted by v0.

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
PENDING to unbound INSPECTING, back to bound PENDING, then to READY or FAILED.
The database rejects a bound INSPECTING row, and the inspection claim projection
contains only unbound rows. An
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
| EVICTING (`DELETE`) | EVICTING (`READBACK`) | The destructive request conclusively returned, or the provider explicitly rejected it before mutation |
| EVICTING (`READBACK`) | EVICTED | Exact digest absence and no live demand |
| EVICTING (`READBACK`) | PENDING | Exact digest absence and live demand |
| EVICTING (`READBACK`) | READY | Exact digest remains |
| EVICTING (`EVICT`) | READY or EVICTING (`EVICT`) | Expired pre-delete claim, restore for live demand or retry the still-demand-free eviction |
| EVICTING (`DELETE`) | EVICTING (`EVICT`) | The same live owner proves in-process that the provider wrapper did not start a call |
| EVICTING (`DELETE`) | QUARANTINED | Delete outcome is ambiguous or the destructive-intent lease expires |
| EVICTING (`READBACK`) | EVICTING (`READBACK`) | Readback is transiently unavailable or its lease expires; a new fenced owner retries readback without repeating delete |

Only INSPECTING publications carry an inspection token and expiry. Only
COPYING, VERIFYING, or EVICTING locations carry a random location token, expiry,
and matching lease kind. `EVICT` means no destructive provider call may have
started; `DELETE` is the durable point after which a delete may already be in
flight; `READBACK` durably proves that no delete remains in flight and that only
exact presence resolution remains. PENDING, READY, FAILED, MISSING, EVICTED, and
QUARANTINED carry no lease. Canonical locations never enter EVICTING, EVICTED,
or QUARANTINED. Every retry records a bounded attempt count, code, and
`next_retry_at`; throttles and timeouts before provider I/O remain retryable. An
unknown delete outcome is terminal for that physical location because ECR has
no conditional-delete token. A failed read after a concluded delete is not an
unknown delete outcome and cannot quarantine the location.

Application wall time is never lease authority. Every mutation that records a
provider or inspection result after a potentially blocking row or advisory lock
checks the exact token, state, and expiry in the same SQL mutation using
PostgreSQL `clock_timestamp()`. Transaction-stable `now()`, statement time, and
a Python timestamp sampled before lock acquisition are insufficient. This rule
applies uniformly to publication inspection, canonical and regional copy
completion, inventory pages and finalization, eviction completion, canary child
attachment, success, failure, and timeout. Tests may substitute an explicit
literal epoch, but production finalizers use the database wall clock after the
wait and roll back every earlier write when the fence fails.

PostgreSQL also owns every qualification freshness decision and every persisted
qualification observation time. The shared profile-attestation transaction
resolves the revision's immutable workspace/profile identity, acquires that
profile's transaction advisory lock, locks the candidate revision, samples
`clock_timestamp()`, and overwrites any producer-supplied `observed_at` before
hashing and storing the evidence. Candidate-shard and inventory finalizers take
the profile lock before their revision and shard rows. Generic non-copy
attestations may still be recorded while the catalog mutation barrier exists;
they cannot become actionable because staging, activation, canary admission,
and qualification-copy publication perform the later barrier check. Canary
claim retains the documented operation-before-profile order and then acquires
the shared catalog qualification lock as its final coordination lock.

Producer clocks remain useful only for local diagnostics and never determine
freshness, ordering, or whether provider work is due. Copy-role infrastructure,
candidate-shard, and canary-copy reconciliation sample the database clock before
comparing database-stamped evidence or inventory epochs. Automatic-canary
scheduling and activation preflight do the same; the final activation
transaction still repeats freshness validation after acquiring its complete
lock set. A slow or fast worker therefore cannot make a fresh proof immediately
stale, place a proof in the future, repeatedly trigger paid/provider work, or
wedge a profile in QUALIFYING.

The same database-clock rule governs shared work admission and retention, not
only result finalization. Copy and eviction claims use the database clock for
their indexed candidate scan, then sample it again after locking the selected
shard and location and recheck lease and retention eligibility. Production
eviction callers provide retention durations, and production retry callers
provide delays; the transaction derives absolute cutoffs and `next_retry_at`
instead of trusting a worker-computed epoch. Provider budgets and worker grants
sample database time after locking the budget and worker rows, never move a
future refill anchor backward, and return only a remaining grant duration to the
process. The process maps that duration onto `time.monotonic()` and uses
monotonic time for housekeeping intervals, so host wall-clock jumps cannot spend
a database grant longer or bypass a shared throttle.

Consumer safety fences are database-clock decisions too. Demand creation,
attachment, terminal observation, the one-hour second-observation interval, the
24-hour unattached-cluster proof window, authoritative owner retirement, worker
heartbeats, and terminal compaction all derive their persisted timestamps after
the rows that authorize the mutation are locked. A scheduler may decide when to
attempt reconciliation using monotonic process time, but it does not pass that
time into the final PostgreSQL decision.

A new managed demand additionally validates the exact runtime attestation under
the locked profile revision and consumer watermark. The transaction samples one
database epoch only after those locks and the existing-demand lookup, validates
the target, binding fingerprint, runtime identity, and proof age at that epoch,
then persists that same value as `demand.created_at`. If the proof expires while
the transaction waits, the complete transaction rolls back, including a newly
inserted watermark. An exact existing demand returns before this new-admission
gate and keeps its original creation-time authorization, so an in-flight
deployment remains replayable after the current proof ages out.

Canary failure classification follows the same rule. The worker does not choose
ordinary failure versus deadline-expired timeout from an application timestamp.
One transaction locks the exact operation, samples the database wall clock, and
then attempts the matching live-lease mutation. If the lease or deadline crosses
its fence before the SQL update, the update fails and a successor reclaims the
operation. Inventory abandonment likewise requires the exact inventory epoch,
token, and still-live lease in one database-clock mutation; a delayed exception
handler cannot clear a successor claim or rewind its cursor.

A PROFILE_CANARY operation is the only operation row that also acts as its work
queue. RUNNING then carries a random lease, expiry, one bounded child launch ID,
and teardown deadline. Every canary child is tagged with operation/profile/
generation and may exercise only one target/backend. EC2 guest exit traps request
shutdown even when pull or inspection fails, while provider teardown remains the
authoritative removal proof. A
continuous lease heartbeat starts before credential acquisition. The actual STS,
EC2, EKS, IAM, and Kubernetes call boundaries synchronously prove ownership both
before and after each call. A provider create uses a stronger ordered fence.
Immediately before every EC2 or EKS create attempt, one transaction locks the
operation and validates the exact child ID, lease token, live lease, RUNNING
state, and future teardown deadline against `clock_timestamp()`. It returns only
the conservative remaining duration, which the worker maps from the start of
that database round trip onto `time.monotonic()`. The fenced provider wrapper
rechecks ownership and that monotonic deadline immediately before the raw
create, then rechecks ownership afterward. Host wall-clock skew cannot extend
the database deadline. Initial child attachment and successful evidence require
the exact live lease and a future teardown deadline; terminal failure requires
the live lease, while an already-expired deadline has a separate timeout/
teardown-only transition that cannot launch or qualify a child. An initial persisted intent
does not falsely imply a provider child: if the same owner has not attempted a
create, a client or discovery failure can terminalize without teardown. After a
crash, the next claimant reads the persisted child identity before deciding
between resume and teardown. An unverified or failed teardown after a possible
create remains RUNNING for successor cleanup and never discards the deterministic
child identity by terminalizing the operation. EC2 `RunInstances` uses one stable
operation-derived `ClientToken`, and EKS uses one deterministic namespaced pod
name, so a lost response or successor replay converges on the same child rather
than launching a second paid resource. A Spot launch tags the
`spot-instances-request` itself with the same catalog, operation, and profile
identity. Cleanup discovers the complete tagged request inventory, cancels every
open, active, or disabled request, retains every associated instance ID before
and after cancellation, and requires both a terminal request state and exact
instance termination. Since cancellation does not terminate an already launched
instance, neither proof can substitute for the other. An ambiguous launch
response remains reclaimable until the tagged Spot request is cancelled and
terminal. A terminal request without a concrete request-to-instance association
does not collapse the absence window: every request transition or newly visible
child resets bounded settling, cleanup repeatedly exact-reads every known
request and rediscovers every page of tagged instances, and a late association
is retained and terminated. Malformed, empty, or cyclic pagination fails closed.
Only a concrete association to an exactly terminated instance may finish early;
otherwise the terminal no-instance outcome must remain stable for the complete
60-observation cadence. On-demand ambiguity still requires the complete
instance-absence window. EC2 cleanup follows every known child through
`shutting-down` to `terminated`; EKS cleanup accounts for a timed-out create
that becomes visible after the first delete. A persisted child also
remains reclaimable if its immutable contract cannot be reloaded, because no
terminal transition may substitute for provider teardown; an incompatible
worker rotates that expired queue row without clearing its child identity so a
compatible worker or rollback can reclaim it. EC2 canaries set guest-initiated
shutdown behavior to terminate, not stop. Other operation kinds project their
publication, location, or profile work and carry no provider lease.
An EC2 create response confirms a launch only after its one child record yields
a nonempty string `InstanceId` that is retained in the teardown set. A missing,
non-string, or otherwise malformed child record leaves the create ambiguous even
when AWS returned a nominal response. Cleanup must then replay the stable client
token or run the full repeated tag-absence settling window; one empty discovery
cannot terminalize the operation. A child that becomes visible during that
window is added to the retained set and followed through verified termination.
An operation-tag inventory is exact absence only when its complete child list is
empty. Any nonempty record whose `InstanceId` is missing, empty, or non-string is
unidentifiable child evidence, never absence and never part of an entirely
identified terminal set. The worker retains this ambiguity from every tag read,
including discovery, polling, and the immediate `finally` read, and passes it
into teardown. A clean proof after such a read requires a new complete settling
window after the latest ambiguity: every observation in that window must contain
only concrete retained IDs, and the final ID-scoped response must cover that
entire set in `terminated`; if the inventories are all empty, the window is the
repeated exact-absence proof. A later malformed inventory restarts that proof,
which cannot finish inside the current bounded invocation and therefore remains
successor-owned. This rule also disables the ordinary confirmed-child early
return when an unidentifiable tagged record has been observed.
Ambiguity settling consumes every bounded discovery attempt even after the
currently retained IDs have all reached `terminated`; ordinary confirmed-child
teardown may still return as soon as exact termination is proved. Every settling
attempt refreshes the complete operation-tag inventory, retains and terminates
new IDs, and reads the exact state of the complete retained set. The final
attempt succeeds only when the clean-window requirement is satisfied and either
no child appeared during that whole window, or the exact-state response covers
every retained ID and every state is `terminated`. A child that appears too late
to prove termination, or any unidentifiable child whose clean window is
incomplete, makes cleanup fail closed, so `CANARY_TEARDOWN_FAILED` preserves the
RUNNING operation for a successor instead of discarding custody.

Canary intent creation locks the desired profile row and reserves its conservative
worst-case cost in a UTC daily window before committing the operation. Concurrent
automatic or manual canaries cannot exceed the configured hard cap; increasing
the cap requires a new profile revision.
The claim samples the database clock only after taking the profile cost lock,
so a blocked reservation cannot return an already-expired initial lease or
charge the previous UTC window.

EC2 canary bindings require at least one explicit security group in every
qualified AMI region. The worker always sends those groups and applies the
catalog, operation, and profile tag specifications to the instance, every
created EBS volume, every created network interface, and, for Spot, the request
itself. The generated IAM role authorizes `RunInstances` against the exact AMI,
subnet, security groups, instance type, runtime instance profile, and created
resource classes. AWS evaluates those classes separately. Instance and
EBS-volume authorization requires the catalog and operation request tags. The
implicit network-interface authorization context exposes neither those tags nor
`ec2:InstanceType`, so its separate statement instead requires the exact
declared subnet. That statement cannot authorize an independent interface
create because the role has no `CreateNetworkInterface` action, and the complete
`RunInstances` call must still satisfy every exact image, network, instance,
profile, and tagged-resource statement. Spot-request authorization requires the
same catalog and operation tags. Exact AMI policy resources use EC2's
accountless authorization ARN,
`arn:<partition>:ec2:<region>::image/<ami-id>`; the separately qualified AMI ID,
observed owner account, and compute-account binding retain ownership identity.

EC2 passable roles and instance profiles are separate inputs from EKS
inspect-only node profiles. `iam:PassRole` names only EC2 runtime roles, and the
mandatory instance-resource authorization uses `ArnEquals` on
`ec2:InstanceProfile` with only the EC2 launchable profiles. Instance-only
condition keys are not attached to the AMI, network, volume, or Spot-request
authorizations. EKS node profiles receive `iam:GetInstanceProfile` only and the
two profile sets are disjoint. Every role, profile, image, network, cluster,
key, and permissions-boundary input is an exact ARN with no IAM wildcard or
policy-variable syntax. Target resources are constrained to the active
partition, account, and region as appropriate. `iam:PassedToService` is derived
from the partition DNS suffix, including `ec2.amazonaws.com.cn` in `aws-cn`.
The role may list Spot requests but may cancel only regional request resources
carrying the exact catalog ownership tag. A separate `ec2:CreateTags` statement
is limited by `ec2:CreateAction=RunInstances`; it is not combined with launch
conditions. The role never relies on an implicit VPC default security group.
EC2 qualification canaries use one-time Spot capacity with terminate-on-
interruption by default. `canary_use_spot: false` is the explicit escape hatch
for an account or region that requires on-demand qualification. A failed Spot
launch remains a retryable canary operation; the worker does not silently
change market type under the same idempotent client token or launch a second
fallback child. The configured worst-case reservation remains the cost ceiling,
so Spot savings never weaken the daily budget fence. EKS qualification uses an
existing declared cluster and does not choose that cluster's capacity type.
Serve and job placement remain owned by their normal SkyPilot resource policy,
not by the image plane. Newly parsed profiles always project the default
`true`, so changing from the pre-field on-demand behavior to Spot changes both
the profile hash and binding fingerprint and requires the normal profile
revision increment. A legacy persisted snapshot with no field restores an
internal unspecified state, preserves its old fingerprint projection, and
continues to replay on-demand under its existing idempotent token. Explicit
`false` is also projected and selects on-demand for a newly staged revision.
Public config and new snapshots accept only booleans, never the legacy internal
unspecified value.
Each compute account applies `aws-image-canary-account` exactly once before
planning any EC2 canary target. That account-scoped module owns
`AWSServiceRoleForEC2Spot`; every regional `aws-image-canary-target` requires
and shares its output instead of racing to create an account-global role. This
keeps a target ready for the public Spot default even when a staged profile
revision explicitly selects on-demand. An existing role is imported into the
account module with the active AWS partition. The account-global role is
destruction-protected because every region and any other Spot workload shares
it; deliberate decommission removes regional targets and grants first. If a
qualified AMI or snapshot is encrypted with a customer-managed KMS key, the
regional target module receives that exact key ARN and grants the Spot
service-linked role only the AWS-documented launch operations. The key must be
in the compute target's partition and region, but it may belong to the source
account that owns a shared encrypted AMI. The Terraform caller then requires
cross-account `kms:CreateGrant`, `kms:ListGrants`, and `kms:RevokeGrant`
authority from both that key policy and its own IAM policy so create, refresh,
and teardown all remain usable. AWS-managed EBS keys need no grant. Omitting a
required customer-managed key makes qualification fail closed; the canary
worker never receives direct KMS authority.

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
   read-only. The exact durable lease is re-proved before and after credential
   resolution and every HTTP request, immediately before advancing each streamed
   response iterator, and immediately after that blocking advance returns. A
   source stream is acquired lazily on its first iterator advance, only after
   the final destination-exists check and destination upload initiation, and is
   explicitly closed on every early return and exception. Closing a stream that
   was never advanced has no response to release; the implementation never
   relies on generator finalization to own an eagerly opened response. Lease loss closes the
   response immediately; destination authority cannot be acquired after that
   source work loses its lease. The terminal `StopIteration` advance is fenced
   after it returns too, before completed source bytes can drive a destination
   write. Managed
   regional-source ECR credential acquisition, SDK calls, signed download-URL
   issuance, and each downloaded chunk are synchronously fenced by the exact
   lease. Both generic registry and signed ECR downloads use the same no-proxy,
   public-connected-peer HTTPS guard and reject redirects that would escape the
   validated request boundary.
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

Before the first optimization, placement architecture is classified without
managed state, then a metadata-only eligibility pass maps each supported
candidate to the active profile's declared runtime binding, locality, and
selected artifact platform. `locality: require` removes unsupported candidates.
`prefer` is a lexicographic class ahead of the ordinary optimizer: READY managed
routes rank first, an authorized direct source fallback ranks second, and a
managed route that still needs warming ranks third. Cost, time, reservations,
and egress preserve their existing ordering within the winning class. Exact
indexed location reads supply this rank without provider calls. No eligible
target fails with `IMAGE_LOCALITY_UNSUPPORTED` before provisioning rather than
warming an impossible placement.

For a new demand, the metadata pass samples PostgreSQL `clock_timestamp()` once
per optimization request and reuses that epoch across candidates. Runtime proof
age is checked at that epoch, so an already-expired managed route cannot win the
READY locality class or suppress a direct alternative. An exact existing-demand
replay deliberately uses structural matching without an age check because its
durable creation-time admission remains authoritative. The locked demand
transaction still samples a later database epoch and is the final authority. If
qualification expires between ranking and that transaction, it returns a typed
qualification-stale result; `managed_preferred` resumes with the byte-for-byte
original direct resources after rollback, while strict managed resolution fails
closed.

For managed EC2, that metadata includes the exact planned host AMI and instance
profile. Both must match the binding's qualified regional AMI and principal; a
request with no host image is pinned to the qualified regional AMI before
optimization, while a user-supplied host image or role outside that tuple is not
silently trusted. EKS eligibility maps the selected SkyPilot Kubernetes context
to one exact cluster ARN, node role, namespace, and immutable nonempty node
selector. The canary and every managed workload pod receive that same selector.
When one EKS binding serves clusters in multiple target regions, resolution
parses the configured EKS ARN and requires its region to equal the candidate
registry target region. A context present in a shared binding therefore cannot
make every target appear eligible or select the first cluster in that binding.
Qualification enumerates every schedulable node matching the selector, resolves
each node's EC2 instance profile, and requires the declared role for the complete
eligible set before recording READY. A selector that matches zero nodes, more
than the bounded qualification page, or heterogeneous roles fails closed. This
binds the selected cluster and node pool to the attestation instead of proving
one fortuitously scheduled canary node. `managed_required` fails closed on a
mismatch, while
`managed_preferred` may use only its otherwise-authorized direct digest path.
An unbound Kubernetes context is a generic Kubernetes placement and may use only
an exact direct digest ref; managed release or artifact selectors cannot use it.

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
location, runtime-binding fingerprint, exact qualified EC2 AMI/principal/profile
or EKS cluster/role/selector tuple, bounded placement constraint, owner epoch,
retry epoch, and a bounded server request ID used only for unattached cluster
cleanup. The request ID has a
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
placement. A non-READY managed route remains eligible as that original direct
candidate at the same fallback locality rank as another provider's direct
candidate. Only a READY managed route outranks it. Release-only and
artifact-only selectors never infer or expose a source fallback.

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
snapshot and target even after the revision becomes RETIRED. It evaluates the
immutable authorization recorded when the demand was created and accepts only
an exact placement replay. Resolution rechecks the structural runtime identity,
target, binding fingerprint, principal, host image, and exact EKS tuple, but it
does not re-evaluate proof age from the mutable profile attestation map. This is
important both after proof expiry and after a successful automatic canary
replaces the attestation with a later `observed_at`: neither event can revoke an
already-authorized deployment. Current proof age is a new-admission condition in
both the request-cached metadata eligibility check and the locked demand
transaction. Only the latter authorizes and persists the demand. A retired
revision cannot admit a new owner or select a new target, so a profile rollout
cannot strand an in-flight deployment or reopen old capacity.

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
provider rejection that proves no mutation was accepted, atomically advances
the same token from `DELETE` to `READBACK` before exact readback. A transport
failure, timeout, ambiguous server response, or failure without a provider
conclusion is never read back as proof of that delete's terminal state; it
immediately yields an ambiguous outcome. A transient readback failure leaves
`READBACK` intact. The same owner may retry while its lease is live, and an
expired `READBACK` claim may be fenced to a new owner that repeats only the
exact read. It never repeats the delete. Only exact presence may restore READY
and only exact absence may requeue or release capacity.

An expired `EVICT` lease proves that no destructive call could have passed the
hook. A live demand therefore restores READY without provider I/O; otherwise a
new owner may retry the eviction. An expired `READBACK` lease is safely
reclaimable because the preceding transaction proved that no destructive call
can arrive later. An ambiguous result or expired `DELETE` lease
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
work. The AWS adapter invokes the exact lease fence immediately before and after
its STS `AssumeRole` request; caller-side checks alone are not credential
fencing. Both STS authority acquisition and the assumed service client use an
explicit 10-second connect timeout, 60-second read timeout, and one total
attempt. The botocore session that resolves the worker's ambient credentials
inherits that same default client configuration before its credential-provider
chain is constructed. Deferred IRSA `AssumeRoleWithWebIdentity`, profile-role,
and other nested credential clients therefore cannot silently restore the SDK's
longer retry policy while signing the explicit STS request. A hidden credential
refresh or SDK retry cannot consume the canary custody budget or continue
indefinitely after worker drain. The worker synchronously re-proves
ownership in the hook immediately before every ECR call, records `DELETE` intent
before the destructive call, and rechecks ownership again before database
completion. Lease loss sets
cancellation state and starts no later provider call. A call that may already
have started is never treated as cancelled, present, or absent; it converges
only to QUARANTINED unless its conclusive response was durably recorded as
`READBACK`. Readback failure after that durable conclusion remains retryable
across workers without another delete. Failed explicit retries bind a terminal
idempotent operation before returning a typed conflict, so replay cannot leave
or conceal an unattached nonterminal operation.
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
demand may expire only when its request is terminal, no durable consumer
attached, and it is at least 24 hours old. The one-shot request-terminal
observation is preserved while that 24-hour age gate is pending; rotating the
reconciliation candidate cannot clear it. A current binding, an authoritative
nonterminal result, or an unknown lifecycle result still clears partial terminal
proof. For clusters, jobs, and services, reconciliation requires two
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
Exception-envelope decoding is itself a bounded compatibility boundary. The
decoder accepts only a string exception type, a list or tuple of positional
arguments, and a dictionary with string attribute keys. Unknown types and known
objects that are not exception classes both return the same fixed, value-free
`RuntimeError`; neither the supplied type nor message is reflected. Malformed
fields or a constructor that rejects the supplied shape return that generic
built-in error too. Invalid envelopes never raise a secondary decoder exception
or reflect the complete untrusted payload into an error message.

Built-in exceptions are constructed from positional arguments only, after which
ordinary string-key attributes are restored independently. For a known SkyPilot
exception, the serialized arguments, rendered message, and ordinary attributes
are the canonical wire state; `BaseException.args` is not assumed to have the
same shape as the subclass constructor. Reconstruction first binds matching
ordinary attributes to named constructor parameters, then binds serialized
arguments only to remaining positional parameters. This preserves subclasses
such as validation errors whose constructor-only context is stored in ordinary
attributes. If a current subclass constructor transforms its input into an
already-rendered message, the decoder may restore the validated `BaseException`
state without invoking that transformation a second time, but only when the
serialized arguments themselves reproduce the separately serialized message.
Subclasses that intentionally exclude arguments remain constructor-restored from
their named attributes. The decoder verifies the reconstructed type, arguments,
and rendered message before accepting it. Remaining forward-version attributes
are restored after construction, so an older client preserves the known
exception type when a newer server adds an ordinary attribute. An attribute that
is read-only, slotted, or otherwise unsettable is skipped without replacing the
original error. `args` is reserved wire state and is never admissible inside the
ordinary attribute map; a contradictory duplicate is malformed rather than
being allowed to replace the constructed exception's message. Dunder attributes
are never constructor arguments. Valid Python 3.11 exception notes are emitted
outside the attribute map for old-client compatibility, while the decoder also
accepts the legacy attribute form and restores notes only after construction. A
forward attribute that shadows `add_note` is dropped and cannot be invoked;
notes fall back to the validated `__notes__` slot so malformed input never
raises a secondary exception. The real `BaseException.__context__`
slot is a separate optional envelope field rather than an ordinary attribute.
Context serialization applies the same safe-exception wrapping and canonical
decoder rules recursively, stops at eight levels, and replaces a cycle,
malformed nested envelope, or over-depth tail with the same fixed value-free
`RuntimeError`. Restoring or reserializing a valid raised-inside-except chain
therefore preserves its exact safe types and state without permitting an
attacker-controlled type name or unbounded graph.

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
when the runtime architecture does not match. V0 managed AWS routes support only
`linux/amd64`: an EC2 placement must report that exact platform, while an EKS
placement may omit architecture only because its exact qualified node selector
must contain `kubernetes.io/arch: amd64`. An explicit non-AMD64 EKS placement is
still rejected. The same EKS selector is proved by the canary and injected into
every workload pull plan. Unknown or ARM64 EC2 placements, heterogeneous EKS
selectors, and selectors without the AMD64 label cannot use managed locality.
An exact-reference direct route remains available where workspace policy allows
it. V0 never builds or selects ARM64 speculatively. Full index replication and
architecture-specific AMIs and canaries remain post-v0 because every platform
then needs its own capacity, demand, qualification, and deletion ownership.

## PostgreSQL data model

Central image state is PostgreSQL-only. Local and controller databases retain
their existing SQLite support.

Migration 024 is a literal additive migration. It does not import live ORM
metadata. It creates only:

```text
container_image_catalog
container_image_profile_revisions
container_image_profile_custody
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

Global-state revision 025 adds only restart-safe canary child evidence to the
operation row. Revision 026 adds
`container_image_qualification_mutation`, whose primary key is constrained to
the single literal `global` row, and the partial index over `result_id` for
`RUNNING` `PROFILE_CANARY` operations. Its phase-less compatibility shape has
only `DELETING` and `RESTORING`.

Revision 027 adds the durable `delete_phase` and `quarantine_reason` columns,
admits `QUARANTINED`, and creates
`container_image_qualification_repository_quarantines`. Upgrade adoption is
forward-safe: an empty 026 singleton remains empty, while every phase-less
legacy `DELETING` or `RESTORING` row becomes lease-free `QUARANTINED` and the
same transaction inserts its exact repository-ARN tombstone. One database-clock
sample timestamps both the singleton transition and tombstone. The two source
states retain distinct reasons: an unknown delete outcome versus incomplete
restoration evidence. Neither can safely resume because revision 026 did not
persist the strict repository policy and tag facts required by the new handoff.
The mutation table is empty while qualification mutation is idle and contains
exactly one owner while the catalog-wide barrier is active.

The catalog singleton contains only a stable authority UUID and creation time.
Migration/bootstrap is its sole creator. Runtime reads require exactly the
fixed singleton row, a valid UUID, and a positive creation time; missing,
additional, or malformed authority state fails closed instead of silently
creating a new realm identity.
Apart from the one qualification-only catalog shared/exclusive advisory lock,
there is no forced RLS policy, API-version GUC, general runtime-wide advisory
lock, global configuration apply ledger, realm generation, dynamic repository
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
  for RUNNING PROFILE_CANARY. A generated `canary_claimable_at` is `updated_at`
  for pending canaries, the later of lease expiry and `updated_at` for running
  canaries, and null for every unrelated or terminal operation;
- zero or one catalog-wide qualification mutation row with literal ID `global`,
  immutable owner revision/target/repository/digest/proof identity across
  `DELETING`, `RESTORING`, and `QUARANTINED`; a required
  `PRE_INTENT|IN_FLIGHT|READBACK` phase and unexpired lease only in `DELETING`;
  a nonempty reason only in `QUARANTINED`; and row deletion as the only idle
  representation;
- one permanent qualification-repository quarantine per exact repository ARN,
  with immutable owner revision, target fingerprint, digest, lifecycle proof,
  reason, and database timestamp. It is inserted atomically before an ambiguous
  singleton can clear, is never removed by cutover, and makes that physical ARN
  ineligible across every workspace, profile, revision, and future process;
- one permanent custody row per `(workspace, profile)`, written atomically with
  the first READY publication and carrying the immutable physical-manifest hash
  plus its first profile revision. Profile staging and activation use this
  primary-key probe rather than joining retained revisions and publications;
- unique `(workspace, requested_release)` while
  `reservation_active`, retained forever for READY publications and expiring
  after 30 days for unretried FAILED publications;
- publication state in `PENDING|INSPECTING|READY|FAILED`, with an inspection
  lease token and expiry only in unbound INSPECTING, one canonical location, a
  generated `inspection_claimable_at` only for unbound PENDING or INSPECTING,
  and the collision behavior above; canonical location is null only before
  source inspection and becomes immutable when bound;
- a nullable publication-to-operation audit link with `ON DELETE SET NULL` and
  a reverse index. Terminal operations compact directly from their expiry index
  after 30 days even when a successful publication is retained forever; the
  retained publication simply stops linking to the expired operation;
- release lookup is an indexed projection of READY publications and returns no
  reservation or failed row;
- workspace publication recovery uses `(workspace, created_at, id)` for its
  bounded newest-first keyset page, so a page limit also bounds database work;
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
  qualification timestamp, fair-dispatch timestamp, in-flight ceiling, a
  nullable `copy_next_at` projection, reconciliation epoch/cursor, a durable
  inventory-finalization bit, persisted inventory interval and exact
  `inventory_next_at`, and `READY|FULL|DRIFTED|DISABLED` admission state;
- unique logical location identity for artifact, immutable target-ring
  fingerprint, and runtime digest, independent of profile revision, plus a
  separately persisted physical repository-shard fingerprint;
- canonical versus regional location relationship checks;
- the exact location transitions and lease combinations above, plus a generated
  `copy_claimable_at` for PENDING or COPY/VERIFY lease recovery;
- an inventory epoch marker on each manifest-present location;
- one server-owned demand per cluster generation, job recovery target, or Serve
  version target, with immutable owner identity/generation, target, terminal
  observation/tombstone fields, and
  `WARMING|READY|FAILED|SUPERSEDED|RELEASED` state plus a bounded secret-free plan;
- one maximum seen/terminal generation watermark per stable consumer owner; and
- worker kind in `COPY|LIFECYCLE|CANARY` with bounded heartbeat metadata and
  bounded provider-token grants.

All queue discovery is bounded and uses a persisted or generated due-time
projection whose partial index exactly matches the claim predicate and order.
Publication inspection scans `(inspection_claimable_at, id)` only for unbound
work. Copy dispatch scans `(copy_next_at, id)` only for shards with projected
work, then uses the shard-local generated location projection. Inventory scans
`(inventory_finalizing DESC, inventory_next_at, id)` only for operational shard
states. Canary workers scan `(canary_claimable_at, id)` only where that
projection is nonnull and due, excluding unrelated and retained terminal
operations. Rotating an incompatible expired child advances `updated_at`, which
also moves that row behind older due recovery work. Claim uses `FOR UPDATE SKIP
LOCKED`. Provider I/O occurs outside the
claim transaction. Completion validates the applicable random lease token after
acquiring the row lock and reading the current clock. Terminal publication
history, idle shards, and future inventory epochs are absent from these hot
claim indexes rather than filtered after a global sort.
Operational reads follow the same rule. ACTIVE/QUALIFYING profile readiness,
live and terminal demand pages, expired reservations, canonical publication
fan-out, worker heartbeat cleanup, and state-filtered workspace publication
history each use an exact partial or composite index matching their predicate
and ordering. Artifact demand history uses a bidirectionally scanned B-tree on
`(workspace, image_id, created_at, id)`, matching its newest-first keyset page
without a population sort for either sparse or dense artifacts. Inventory
matching has a shard-and-runtime-digest lookup index. Its 100,000-row scale test
may raise the statement timeout only inside the committed fixture-population
transaction. Sparse and dense `EXPLAIN ANALYZE` proofs run in a new transaction
that inherits and explicitly asserts the engine's 15-second statement timeout;
the fixture allowance cannot weaken the bounded-query gate.
Cross-state pages use one matching partial index for the precise state set or a
fixed number of per-state indexed heads followed by an in-memory bounded merge;
they never sort the full table to return a bounded page. Canonical completion
drives fan-out from indexed PENDING publications rather than globally scanning
READY locations.
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

Qualification claim, authorization, copy, staging, activation, and lifecycle
transitions acquire the catalog qualification advisory lock in the required
shared or exclusive mode only after their applicable ordered rows above, then
inspect the singleton barrier. It is their final coordination lock. Generic
non-copy attestation writes do not acquire it.

Initial insert races rely on unique constraints and restart the transaction.
No ordinary repository function acquires an earlier class after a later one.
Canonical completion and publication retry both lock location before
publication. The PROFILE_CANARY queue is the one explicit class-order
exception: claim and completion both lock their operation row before the
referenced profile revision. Claim then takes the shared catalog qualification
lock. A lifecycle transition locks its owner profile/revision, takes the
exclusive catalog lock, and reads the catalog-wide running-canary predicate
without locking operation rows. No catalog-lock owner requests an earlier lock,
so this consistent one-way edge cannot close a cycle. On the same profile,
either lifecycle waits for claim's profile lock without holding the catalog
lock, or claim waits for lifecycle's profile lock without holding the catalog
lock. Across profiles, a claim may wait for lifecycle's exclusive catalog lock,
but lifecycle performs only the nonlocking indexed running-canary read and can
commit without requesting the claim's operation or profile row.
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

Migration 024 is run under a PostgreSQL migration-scoped advisory lock, not a
runtime control-plane lock. The downgrade itself can inspect only database state.
It requires every operational image table to be empty and the catalog table to
contain exactly the expected singleton authority row, then drops the singleton
with the schema. Draining all 023 processes, removing profile configuration,
revoking controller/canary credentials, and running the bounded teardown command
that empties operational rows are separately verified operator preconditions.
Normal rollback never downgrades. Because the feature has not shipped, there is
no compatibility reason to preserve image-plane state during an explicit
downgrade, while upgrade continuity for preview deployments remains required.

The shared auth-session migration merged first at revision 023, which was also
the revision used by managed-image preview deployments. Revision 024 therefore
creates the literal `auth_sessions` predecessor table when it is absent and
adopts an already-complete preview image schema instead of replaying its DDL.
Before adoption, it creates the same literal image DDL in the connection's
transaction-local `pg_temp` namespace and compares the exact owned table set,
column order/types/nullability/defaults/generated expressions, constraints and
validation flags, foreign keys, and complete index definitions and validity
flags on the same PostgreSQL server. The same literal-reference check covers
the type, nullability, and default of the three adopted cluster-binding
columns. It also requires exactly one catalog row with the fixed singleton ID,
a UUID authority, and a positive creation time. The reference tables are
discarded before adoption. This imports no live ORM metadata and requires no
database-level `CREATE SCHEMA` grant. Missing, extra, or structurally different
preview state fails closed in the migration transaction. Downgrade 024 never
drops `auth_sessions`. Preview builds predated a fixed allowlist of read-only
performance indexes introduced before release. Adoption may create only those
literal known-safe indexes before comparison. A missing allowlisted index is
added transactionally, a malformed same-name index still fails exact comparison,
and every adoption write rolls back with any other drift.

The revision-024 Helm Job cannot coordinate with an old binary whose migration
path predates the PostgreSQL advisory lock. Ordinary `auto` and `upgrade` modes
therefore accept only a database already at revision 023 or later and fail
without DDL when the effective schema is unversioned or below 023, including an
empty schema. Operators first stage an existing deployment through revision
023, drain binaries older than 023, and only then run the 024 Job. A new database
may opt into the distinct `bootstrap` mode only when the operator has isolated
that effective schema from every other migration or DDL writer. `bootstrap`
still proves from PostgreSQL catalogs that the exact schema selected by the
connection search path owns no relation, view, materialized view, sequence,
user type, routine, or other user object before starting the migration chain.
The explicit mode is the isolation assertion; it is never selected by API
replicas or workers and is not a general empty-schema shortcut. New Job,
`bootstrap`, and `auto` processes share the advisory lock once every participant
is at least revision 023.

The independent Serve database has one linear head at revision 026. Upstream
owns revision 022 response-time history, revision 023 prediction-time history,
and revision 024 version quarantine. Managed image distribution follows with
revision 025 workspace convergence and revision 026 exact replica-version
lookup. Preview builds used the same 022, 023, and 024 numbers for workspace,
the replica index, and response history respectively. Revision 025 is therefore
the first common successor for every ambiguous stamp. It idempotently restores
both upstream history tables, quarantine columns, the earlier accelerator and
placement collision state, and workspace state. Revision 026 also repairs the
complete replica JSON state and both replica lookup indexes for predecessor-
stamped preview databases that skipped canonical revision 010, then creates the
exact service-version index. This convergence is required for local/controller
SQLite as well as central PostgreSQL; skipping only the index would leave those
legacy replicas unreadable by the current model. Empty projection-only preview
tables gain the complete current column set. A nonempty row missing both
authoritative JSON and the legacy pickle fails closed because its replica state
cannot be reconstructed without inventing control-plane state. The migration
owns a frozen legacy-pickle projection instead of importing the live Serve
write helper. On PostgreSQL it detects an INVALID same-name concurrent-index
residue, drops it concurrently, and rebuilds it; a valid same-name index with
unexpected columns fails closed instead of being mistaken for the required
lookup. Real-PostgreSQL tests construct
upstream-022 and managed-preview 022/023/024 layouts independently, upgrade each
through 026, and prove the full model projection is readable. A SQLite
regression starts with a legacy three-column replica table stamped at revision
025, upgrades through 026, and proves the JSON backfill plus both indexes. The
matrix covers upstream 022, 023, and 024 plus managed-preview 022, 023, and 024
independently. Revision-only verification is safe only because all known same-
numbered shapes converge through that common successor.

The independent Managed Jobs database advances from revision 024 to 025 with
the exact `(spot_job_id, task_id)` identity index used by bounded terminal
consumer reconciliation. On PostgreSQL, companion migrations inspect the
current-schema catalog rather than treating an index name as proof. The index
must belong to the expected table, be valid and ready, use nonunique,
expression-free, unfiltered B-tree keys, contain no included attributes, use
default ordering, and match the exact ordered columns. An INVALID or not-ready
same-table residue is dropped concurrently and rebuilt; a valid incompatible
same-name index fails closed before Alembic can stamp the revision. The same
exact-shape contract applies to Serve revision 026's status and service-version
indexes, while SQLite validates every shape available from its inspector.

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
      canary_use_spot: true  # defaults to true; set false for on-demand
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
            kubernetes.io/arch: amd64
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
        qualification_repository_generation: 1
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
          qualification_repository_generation: 1
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

`qualification_repository_generation` is optional and defaults to the legacy
generation-zero path. A positive value requires the exact matching
Terraform-qualified repository ARN in the handoff.

Semantic changes require a higher explicit revision. Existing durable pull plans
remain valid while their exact target and auth contract remains usable. In one
transaction, config reload acquires a transaction-scoped PostgreSQL advisory
lock keyed by workspace and profile, reads only an exact idempotency candidate,
the primary-key custody marker, and an indexed scalar maximum desired
generation, marks an older unfinished desired revision SUPERSEDED, and stages
the new revision as QUALIFYING. Activation acquires the same lock, reads only
the exact desired row, indexed ACTIVE row, and custody marker, and updates those
rows directly. Neither mutation materializes retained profile or publication
history. It never makes provider calls or blocks deployment.

Canonical READY commit, reconciliation, and retry acquire the same profile
advisory lock before their shard and location locks, then insert the custody
marker and re-prove its physical-manifest hash in the publication transaction.
This closes READY-versus-stage races without reversing the profile-before-shard
lock order. If staging wins immediately before the first READY commit, later
activation rechecks the newly committed marker and rejects an incompatible
candidate. The marker is permanent even after operational history compaction.

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
absence, so activation never races cleanup. That deletion is a one-time
lifecycle-authority proof, not a permanent desired state for the qualification
artifact. Once exact absence is attested, the copy worker restores the same
fixed digest for later manual and automatic canaries. A lifecycle attestation
for that digest remains the completed deletion proof and must not delete the
restored copy again.

The qualification repository and its fixed digest are physical catalog state,
not profile-revision state. Every workspace and every old, desired, or active
profile revision may refer to the same repository. A revision-local lifecycle
epoch therefore cannot fence deletion: a worker for workspace A could otherwise
delete the digest while a canary for workspace B or an older revision is using
it. Protocol 2 instead has one durable catalog-wide qualification mutation row.
V0 deliberately serializes every physical qualification target through this
single row; qualification is infrequent, and one conservative global fence is
smaller and safer than a partially keyed lock hierarchy.

The singleton row exists while a destructive mutation is active or its outcome
is unsafe to recover automatically. It stores the owner profile revision,
target fingerprint, repository ARN, runtime digest, opaque proof ID, and one
state: `DELETING`, `RESTORING`, or `QUARANTINED`. `DELETING` also stores one
durable provider phase, `PRE_INTENT`, `IN_FLIGHT`, or `READBACK`, plus a
database-clock lease token and expiry. `RESTORING` carries no phase or lease.
`QUARANTINED` carries neither and records a nonempty reason. Row absence means
idle. The proof, physical identity, and owner revision remain stable through
every transition.

`QUARANTINED` also has a catalog-wide physical tombstone keyed by exact
repository ARN. The tombstone is not a temporary copy of singleton state. It
survives audited cutover and permanently prevents that physical repository from
being selected, copied, deleted, armed, restored, canaried, or activated by any
workspace or later profile. Every admission transaction acquires its shared or
exclusive catalog lock before its final tombstone check. Therefore another
workspace cannot insert the tombstone and clear the singleton in the gap
between a stale check and provider admission.

The participating transactions share one transaction-scoped advisory lock
keyed by the catalog authority. Ordinary canary claim, launch authorization,
qualification copy snapshot/publication, profile staging, and activation use
the shared form and require the singleton row to be absent, except that the
exact recorded owner copy may operate during `RESTORING` and a same-logical-
profile higher-generation handoff may complete quarantine cutover. Lifecycle
state transitions, owner restoration publication, and quarantine cutover use
the exclusive form. Generic non-copy attestation writes do not need the barrier
because none can admit work or activate a revision by itself. The advisory lock
is held only for PostgreSQL decisions, never over provider I/O; the durable row
keeps the interval closed after the transaction commits.

The catalog advisory lock is the last lock in every participating transaction,
after the operation row when present, profile advisory lock and revision row,
provider budget when present, and physical shard rows when present. No
transaction acquires one of those earlier locks after it holds the catalog
lock. A lifecycle transition first locks its owner profile and revision, takes
the exclusive catalog lock, and evaluates the catalog-wide zero-running
predicate without locking canary operation rows. This preserves the existing
operation-to-profile order while preventing an inverse cycle.

To begin deletion, the lifecycle worker locks its owner profile and revision,
takes the exclusive catalog lock, requires the barrier to be absent, revalidates
the exact owner and physical target, and proves that no `PROFILE_CANARY`
operation is `RUNNING` anywhere in the catalog. The zero-running predicate is
catalog-wide, not scoped to a workspace, profile, revision, target, or result
projection: every `PROFILE_CANARY` row in `RUNNING` counts even when an older
or future binary left its result kind or ID null or unknown. The matching
partial index has that exact kind-and-state predicate. It then commits
`DELETING/PRE_INTENT`, a new proof ID, and an execution lease before acquiring
lifecycle credentials or starting provider I/O. Every lease comparison samples
the database clock only after the final catalog lock has been acquired. A claim
that has not yet reached the final shared barrier check cannot commit
`RUNNING`; either a completed claim prevents deletion, or the barrier commits
first and makes that claim roll back without reserving cost.

Every lifecycle credential acquisition, delete request, and exact readback is
fenced by process-drain state and exact database ownership of the singleton
row, owner revision, target fingerprint, repository ARN, digest, proof, state,
phase, unexpired lease, and lease token. A background heartbeat and the
synchronous pre-call fence renew that lease; the interval exceeds the adapter's
bounded, single-attempt AWS connect and read timeouts. The ECR call hook commits
`PRE_INTENT` to `IN_FLIGHT` after the provider-budget wait and immediately
before the SDK can start `BatchDeleteImage`. A provider response classified as
conclusive commits `IN_FLIGHT` to `READBACK` before any presence read.

Recovery is phase-specific. An expired `PRE_INTENT` claim may rotate its token
and attempt deletion because no provider request could have started. An expired
`READBACK` claim may rotate its token but may only repeat exact reads. An
expired `IN_FLIGHT` claim atomically becomes `QUARANTINED`; it is never taken
over, reread, or retried. A request the adapter proves never started returns to
`PRE_INTENT`. An ambiguous result becomes `QUARANTINED`. Exact presence observed
from `READBACK` returns to `PRE_INTENT`; exact absence alone advances to
`RESTORING`. A proven not-started request or present readback shortens its safe
`PRE_INTENT` lease to a one-second retry delay instead of waiting for the
five-minute failure lease. Thus a timed-out request that arrives after its
worker disappears can never delete a newly restored digest. Quarantine retains
the global barrier and requires an audited cutover to a new physical
qualification-repository generation. A later read of the quarantined repository
is not sufficient to clear it.

Cutover is narrowly scoped. Only a later revision of the same logical profile
may stage through the temporary `QUARANTINED` singleton, and only when it keeps
the same target fingerprint and physical manifest while selecting a strictly
higher qualification-repository generation. Terraform ingestion must attest a
different exact ARN derived for that generation, the new ARN must not itself be
tombstoned, and the catalog must have zero `RUNNING` canaries. The handoff
transaction records an audited old-to-new repository cutover and deletes only
the singleton. The old physical tombstone remains forever. No copy or lifecycle
worker clears a quarantine.

After exact absence is proved, the current owner locks its profile and revision,
takes the exclusive catalog lock, and CASes `DELETING` to `RESTORING` without
changing the owner revision, physical identity, or proof. The provider
identities remain split: the lifecycle worker cannot write ECR, and only the
copy worker reconciling that recorded owner may snapshot the exact `RESTORING`
proof and perform its copy. No ordinary or other revision's copy may run while
the row exists. The restoration owner copies and exactly verifies the recorded
digest, then reacquires its profile/revision locks followed by the exclusive
catalog lock and in one transaction CASes the exact `RESTORING` row, records the
owner revision's restored-copy evidence against the same lifecycle proof, and
deletes the singleton row. Admission opens only at that atomic commit. A failed
CAS may leave a physically restored digest, but the row remains and the
recorded owner must reverify it before clearing the barrier.
Worker restart recovery reads the singleton owner directly and places that
revision and target ahead of the ordinary bounded eight-revision page:
`DELETING` routes first to lifecycle recovery and `RESTORING` routes first to
copy recovery. `QUARANTINED` performs no provider work. Unrelated repeatedly
failing revisions therefore cannot strand or bypass the catalog-wide barrier.
Ordinary reconciliation queries `QUALIFYING` work in its own indexed statement
and returns that page without filling it from `ACTIVE` maintenance. Only when no
fresh candidate exists does a second statement inspect eligible `ACTIVE`
revisions. This keeps a million older ACTIVE rows that share a tombstoned ARN
from delaying a new generation; tombstoned active history is never parsed on
the fresh-candidate path.

Migration 027 follows the same recovery contract. A phase-less 026 `DELETING`
row has an unknowable provider outcome, and a phase-less 026 `RESTORING` row
lacks the strict repository policy and tag facts needed to authorize its exact
recopy. Upgrade atomically converts either state to `QUARANTINED` with a
state-specific reason, clears its lease, and persists the exact repository
tombstone at the same newly sampled database timestamp before any new binary can
admit work. Recovery always uses the audited fresh-generation handoff.

A canary request may be durably queued while another qualification mutation is
active, including for the mutation owner whose local copy evidence temporarily
reflects `DELETING` or exact absence, but claim is authoritative. Owner
queueing requires the exact singleton owner, target, repository, digest, proof,
and state; unrelated missing-copy evidence still fails. Claim locks its
operation, profile revision, and cost row, then takes the shared catalog lock,
requires no barrier, checks exact revision evidence, and reserves worst-case
daily cost in the same transaction. Persisting `child_launch_id` records only
intent. Immediately before the first billable provider create, launch
authorization repeats the shared-lock barrier check after its
operation/profile locks and rechecks the operation lease, child ID, teardown
deadline, revision, target, and copy evidence. Qualification copy snapshot and
publication, profile staging, and activation also repeat the shared check after
their ordinary locks. Thus a provider readback, a recovered launch intent, a
new workspace, or another revision cannot cross `DELETING`, `RESTORING`, or
`QUARANTINED`. Existing workload pull plans, generic non-copy evidence, and
ordinary catalog image copies do not use the qualification repository and
remain usable.

Legacy revision-local lifecycle proofs are evidence only and never substitute
for the catalog barrier. Legacy `READY` exact-absence evidence may be adopted
into a new protocol-2 proof and `RESTORING` only when the desired profile
selects a nonzero, freshly provisioned qualification-repository generation.
Generation zero is never adopted during the first protocol-2 rollout because
an old request against that physical repository may still arrive. While a
logical profile has a `QUALIFYING` candidate, workers suppress ordinary
maintenance of its preceding `ACTIVE` revision; an existing singleton owner is
still recovered directly. The copy worker must restore and verify the exact
digest against the adopted proof and atomically clear the barrier.
Activation independently requires every target's copy attestation to
acknowledge its exact protocol-2 lifecycle proof; generic freshness alone is
insufficient. The immutable lifecycle proof does not expire: lifecycle
restoration may legitimately exceed the automatic ten-minute copy window, and
the proof records a completed one-time destructive transition rather than
current provider state. The matching copy proof remains freshness-bounded, so
activation still requires a recent physical restoration that acknowledges that
exact lifecycle epoch. The expected attestation hash and final shared barrier
lock make that preflight relationship authoritative at activation commit. Helm
renders the lifecycle worker Deployment with `Recreate`, which drains the old
destructive worker before a new worker can create or advance the singleton
mutation. Copy and canary Deployments remain `RollingUpdate`; the durable
barrier and provider leases fence their cross-version work.

The runtime binding also declares the minimum launch tuple needed for an
automatic canary. EC2 qualification pins one IAM role, instance-profile name,
AMI, instance type, and bounded regional subnet/security-group allowlist. EKS
qualification pins one kubeconfig context, cluster ARN, node role, and dedicated
namespace. Every qualified EKS node selector must include the exact
`kubernetes.io/arch: amd64` label in addition to any operator isolation labels.
The separately referenced `canary_launch` authority may only launch, inspect,
and tear down these tagged canaries; it has no ECR write or lifecycle permission.
The fixed digest-pinned `canary_ref` is copied by the copy worker into
Terraform's non-catalog qualification repository. The canary worker pulls that
regional digest through the declared runtime identity, and the lifecycle worker
deletes it through `qualification_delete_authority`. Runtime evidence records
the fixed `linux/amd64` platform. An EC2 canary must rediscover its exact tagged
instance through `DescribeInstances` and observe `Architecture: x86_64` before
it may publish success. A missing or different architecture fails
qualification even when the pull marker is present. The resulting attestation
persists `instance_architecture: x86_64`, and runtime matching requires that
field together with the qualified AMI and instance profile. Existing preview
attestations without the observed architecture are intentionally invalid and
must be refreshed by a new canary. A pull-only marker or the configured canary
platform is not architecture evidence. A target cannot use its ordinary
`delete_authority: disabled` setting to skip qualification cleanup.
The operation-tag instance query remains the duplicate detector and attestation
inventory, but it cannot supply attestation fields. The independent
operation-tag Spot-request query is teardown-only custody inventory. Every
polling instance read must return exactly the launched or replay-discovered child
ID. The worker then performs an ID-scoped `DescribeInstances` read, verifies that
same ID and the exact operation, catalog, and profile tags, and derives host AMI,
architecture, instance profile, and lifecycle state from that one response. The
observed AMI must equal the qualified regional AMI. The console marker is read
from that same child.
An absent instance-profile ARN while that exact child is still pending or
running is retried within the same bounded deadline because EC2 may expose the
instance before its profile attachment. The exact expected ARN from the
`RunInstances` response or a later ID-scoped read is lease-fenced into a
dedicated nullable child-evidence field on the running operation before the
worker performs more ordinary provider work. The field is separate from the
closed canary request payload so a rolled-back worker can ignore the new column
and continue to parse, reclaim, and clean up the operation. New-code terminal
completion clears the internal evidence. A successor restores the latch for the
same persisted child because EC2 may omit the attachment from a later stopped
or terminated response. A conflicting nonempty ARN at any point fails
qualification.

A process can still die after AWS accepts the launch but before that first
observation commits. For that narrow replay case, a terminal response with no
profile ARN is accepted only after all of the following hold: the child ID and
operation/catalog/profile tags match, the AMI and architecture match, a fresh
IAM read returns the exact expected instance-profile ARN, including its path,
and maps that profile to the one configured role, and the exact nonce-bearing
success marker proves the guest completed the private-registry pull through the
requested profile's IMDS-only credential-helper path. The canary pull clears
environment, shared-file, web-identity, and ECS credential sources, points
shared config files at `/dev/null`, disables the ECR helper's filesystem token
cache, and removes any preexisting Docker auth entry for the target registry. A
credential or token baked into the AMI therefore cannot satisfy this recovery
proof. The worker then records the expected profile ARN as recovered evidence.
A path-qualified or otherwise different IAM ARN, or a marker-free terminal
child without a durable profile latch, fails with
`QUALIFIED_RUNTIME_PRINCIPAL_REQUIRED`; a conflicting ARN can never use this
recovery path.
Before the pull, EC2 canary user data configures the same exact value-free
`credHelpers[registry] = ecr-login` route used by normal managed workload
initialization, then invokes the helper with every non-IMDS AWS credential
source disabled. Merely finding the helper binary in the AMI is not runtime-pull
evidence. The guest script uses an exit trap to invoke the instance's configured
terminate-on-shutdown path after either success or failure, so a failed pull
does not consume the entire canary deadline. Console marker inspection accepts
the exact nonce-bearing marker in the SDK response first, then supports a
strictly valid base64-encoded response for compatible EC2 API implementations.
It never loosely decodes malformed console output. EC2 may expose the stopped
or terminated instance before posting the transition's buffered console output.
After the first terminal observation, the worker therefore keeps requesting
`GetConsoleOutput(Latest=True)` through its fenced provider client for a bounded
settling window inside the original canary deadline. An empty, partial, or
marker-free response is not an immediate pull failure. The exact marker
completes the proof when it appears; exhausting the settling window without the
marker records `CANARY_PULL_FAILED`. This is qualification-only waiting and
does not add a blocking operation to ordinary workload deployment.
An unexpected operation-tagged ID, missing exact child, or tag mismatch can never
be spliced into evidence. Every child ID observed through launch, replay, polling,
or cleanup is retained in the teardown set, and an identity mismatch fails only
after all observed children are verified terminated.

`canary_worst_case_cost_usd` is reserved atomically with the operation lease
before any child launch. It is a conservative operator-set ceiling for one run,
not an observed bill. `canary_timeout_seconds` bounds ordinary launch, pull, and
observation. Mandatory custody teardown has the separate absolute EC2 or EKS
cleanup budget below, so an ordinary timeout cannot prevent child removal. Once
the ordinary deadline passes, an exact live owner may only discover and tear
down the already-persisted deterministic child and record
`CANARY_TIMEOUT` after verified teardown; an unverified teardown remains
reclaimable work. It cannot attach a new child, publish success, or perform
another launch. V0 supports only `linux/amd64`;
configuration rejects another canary platform rather than building or launching
an unused architecture.

Each service writes a bounded attestation through its authenticated internal
endpoint. A canary result includes a single-use nonce and actual-principal
evidence, never credentials: the canonicalized EC2 STS role ARN, or the EKS node
UID/node-group role paired with successful kubelet pull and pod start. EC2
evidence must also equal the configured regional AMI and exact
compute-account instance-profile ARN. EKS evidence must equal the configured
context, cluster ARN, role, selector, positive qualified-node count, and bounded
node-set hash. The binding fingerprint alone never substitutes for those
observed identity fields. Copy workers may coordinate aggregation but cannot
assume or simulate lifecycle or
runtime roles. `transactions.activate_profile()` locks the
current desired row and promotes it only after rechecking desired generation,
config and Terraform hashes, target fingerprints, and fresh required
attestations. Production callers do not pass a pre-lock application timestamp.
The lifecycle scheduler may use its sampled time for interval bookkeeping and
deterministic evidence tests, but neither reconciliation nor activation forwards
that value as database-time authority.
Activation samples the database wall clock only after its profile advisory lock,
candidate and active profile rows, provider-budget rows, and physical shard rows
have been acquired, then revalidates attestation freshness and budget facts
before applying any promotion. A late older qualifier becomes SUPERSEDED and
cannot activate. The
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

It also creates retained, non-catalog qualification-repository generations per
region. Generation zero preserves the original deterministic path:

```text
<prefix>/r<authority-base32>/qualification/<region>
```

Nonzero generations are additive:

```text
<prefix>/r<authority-base32>/qualification/g<two-hex-generation>/<region>
```

Terraform keeps generation zero and every explicitly retained generation with
`prevent_destroy` and emits one active generation in the handoff. Retained
repositories remain Terraform resources, but exactly one has the full
qualification allow policy. Every inactive retained generation has a deny-only
repository policy whose wildcard-principal explicit deny covers every ECR image
pull, content inspection, upload, publication, and digest-delete action. It has
no runtime-pull, copy, or lifecycle allow statement. The deny deliberately
excludes `ecr:DescribeRepositories`, `ecr:GetRepositoryPolicy`,
`ecr:ListTagsForResource`, and repository, policy, and tag management actions so
Terraform can continue refreshing and reconciling the retained resource. The
module-managed copy and lifecycle permissions boundaries and the exact inline
policies likewise contain shard ARNs plus only the active
qualification-generation ARN. An optional organization-managed boundary may be
attached instead, but it never replaces the exact inline or repository
policies. A stale identity policy, resource-based runtime grant, or independently
retained role session therefore cannot use an inactive or tombstoned generation.
Retaining up to 256 generations does not grow role policies. Terraform fails
locally when a rendered module-managed boundary exceeds AWS's 6,144-character
limit or an inline role policy exceeds 10,240 characters. A generation bump
creates a repository, changes the previous generation to deny-only, and updates
active role policies in place; it never replaces or deletes an earlier
repository.
The handoff identifies the active repository with canonical hashes of its
rendered allow policy and complete `tags_all` set, including provider-level
default tags. The copy worker compares those hashes plus exact ARN, URI,
immutability, the shard ring's Terraform-attested encryption type and KMS key,
and the qualification repository's fixed `MANUAL` scanning mode against one
bounded live metadata read before it may record `infrastructure:*` as `READY`.
A same-name repository recreation or any policy, custody, encryption, or scan
drift therefore fails closed before canary content transfer.
The profile target's optional
`qualification_repository_generation` defaults to zero. Generation zero is
omitted from persisted snapshots for backward-compatible hashes. A nonzero
value changes the profile config hash but not the workload target fingerprint
or physical shard manifest hash because qualification storage is outside shard
custody. Successive revisions cannot decrease the generation for an unchanged
target fingerprint. Removing a target or changing its physical target identity
does not create a false monotonic dependency; Terraform's highest-retained
active-generation rule and exact handoff validation remain authoritative.
The dedicated-account example derives the qualification ConfigMap name from the
complete handoff content, emits that name in its Helm values, and requires the
operator-created ConfigMap to set `immutable: true`. A changed handoff therefore
creates and mounts a new object instead of mutating data observed by an older
worker.

Each qualification repository uses the same encryption settings but is outside
workspace capacity. Only the active repository receives the rendered
qualification allow policy. Copy, actual-runtime-pull, and lifecycle canaries
use only bounded untagged digests there, and the lifecycle attestation verifies
their removal. The selected repository is intentionally shared by all declared
workspaces using that profile generation, which is why its delete/restore
barrier is catalog-wide rather than revision-scoped. The manifest parser derives
the exact expected repository path from catalog authority, target, generation,
and region and rejects any other ARN. A qualification digest is never returned
in a workload pull plan.

`authority-base32` encodes the 128-bit catalog authority. `workspace-hash` is a
128-bit, versioned hash of authority plus normalized workspace name. Terraform
and the API reject any collision across the declared workspace set. Handoff
ingestion independently derives and requires the complete authority base32,
workspace hash, `g00`, two-hex shard index, repository name, and partition-aware
ARN for every declared slot; a self-consistent substituted repository and
recomputed physical fingerprint are not accepted. `g00` is the fixed v0
repository generation. This prevents two SkyPilot control planes sharing an
account from colliding and avoids placing workspace display names in registry
paths. A post-v0 shard expansion uses a new explicit generation, profile
revision, and reviewed custody migration; it never changes old paths in place.
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

Activation takes the per-profile transaction advisory lock, locks only the exact
candidate and at most one indexed ACTIVE row, then acquires every shared provider
budget in provider-scope order and every physical shard in ID order.
It takes the shared catalog qualification lock only after those rows, rejects
the singleton and any selected exact repository tombstone, and samples the
database clock after that final coordination lock. It then revalidates the
freshness and qualified values before updating only the budget, shard, and
profile rows already locked by the transaction.
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

Each shard stores a durable inventory epoch, provider cursor, started time,
last-completed time, `inventory_finalizing` bit, interval, and exact
`inventory_next_at`. A live claim projects its lease expiry; a released listing
or finalization continuation projects immediate work; successful attestation
projects `inventory_completed_at + inventory_interval_seconds`. The partial
queue index orders finalization first and then `(inventory_next_at, id)`, so idle
polling does not scan or sort not-due shards. One reconciliation claim reads at
most ten provider pages or runs for ten seconds, then commits its cursor. An
invalid or expired provider cursor restarts the epoch safely. The terminal
provider page durably sets `inventory_finalizing`; while that bit is set, every
released or expired claim is immediately eligible and resumes the same completed
epoch instead of relisting the repository. Observed managed
digests update the matching location's epoch marker. Only a completed epoch may
nominate a missing manifest for the exact confirmation required by the
transition contract. List absence alone never marks a shard `DRIFTED` or a
location `MISSING`.

One finalization claim exactly confirms at most 100 nominated locations. An
exact-present result advances both the epoch marker and verification timestamp,
so the ordered partial-index scan never rewalks already confirmed rows. It then
enters a profile-before-shard transaction that independently proves no
nominations remain. If another page remains, that transaction releases the
token but keeps `inventory_finalizing`, so another worker can resume immediately
without repeating the provider listing. Only the zero-candidate transaction may
record the revision-scoped attestation, clear `inventory_finalizing`, and release
the lease atomically. The reconciliation lease remains live through credential
acquisition, every ECR and service-quota call, and its bounded confirmation
page. A transient failure before final attestation leaves no activatable
evidence. A shard is inventory-active from the durable start of listing through
the atomic end of finalization, including between page leases. Bootstrap
ceilings cannot change while inventory-active. A finalizing shard cannot provide
candidate evidence or participate in activation. An inventory-active shard
admits no fresh or expired lifecycle claim and cannot restore a pre-delete
`EVICT` to READY without provider I/O. A pre-existing exact `READBACK` may record
presence or demand-backed absence because both retain the reservation; an
absence that would release capacity waits until the inventory epoch is idle.
Continuation pages release their token only after durably committing the
cursor. A partial million-row absence sweep is therefore bounded, interruptible,
and successor-resumable.
An in-flight or not-yet-written location consumes a reservation but is not
expected in inventory. An unexplained manifest or observed count above reserved
count marks the shard `DRIFTED` and stops new admission without breaking
existing pulls. A managed location absent from a complete list remains unchanged
until exact readback proves presence or moves it to `MISSING` for rematerialization.

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

The module creates and owns the registry copy and lifecycle roles below,
including their trust and inline policies. By default it also owns one exact
permissions boundary per role. An operator may instead supply one exact
organization-managed customer policy in the registry account and partition as
the boundary attached to both roles. AWS supports one permissions boundary per
role, so this is an attachment replacement, not a second boundary. The exact
inline and repository policies remain authoritative least-privilege layers. The
module validates the managed-policy ARN shape plus active account and partition,
then delegates detailed path/name syntax and existence to the provider's
exact-ARN lookup so its input parser does not reject an otherwise resolvable IAM
policy.
Worker base roles and runtime pull principals are exact input ARNs. These
identities are non-interchangeable:

- API: PostgreSQL metadata and intent only, with no ECR, Service Quotas, KMS, or
  data-role `sts:AssumeRole` permission;
- copy worker base: may assume only the exact registry copy role;
- registry copy role: `GetAuthorizationToken` on the required wildcard resource,
  source/repository reads, metadata qualification, layer upload, and `PutImage`
  only for fixed destination repositories, with no deletion or administration;
- lifecycle worker base: may assume only the exact lifecycle role;
- lifecycle role: `BatchGetImage`, `DescribeImages`, and `ListImages` on all
  fixed workspace shard repositories, including canonical, plus only the active
  qualification generation, so exact absence can be proved without retaining
  provider authority over an older qualification generation;
  `BatchDeleteImage` only for the active qualification repository and eligible
  regional workspace repositories, never canonical workspace repositories,
  with the same read/delete split enforced by its identity policy, permissions
  boundary, and repository policies, and with no push or repository deletion;
- runtime pull principals: the actual EC2 instance-profile role and EKS kubelet
  node role, with token plus repository-scoped pull only on workspace shards and
  the active qualification generation. Every inactive qualification repository
  explicitly denies their image data-plane requests.

The null boundary input preserves the original resource graph, state addresses,
attachments, output keys, and hashes. The module-managed boundary resources stay
unconditional when an external boundary is selected because adding `count` or
`for_each` would rewrite established state addresses and force migration. They
become unattached compatibility resources in that path. Qualification never
attests those dormant policies: Terraform reads the attached external policy by
exact ARN through the AWS provider, obtains its current default-version
document, normalizes the JSON, and places that document hash in both existing
boundary fingerprint keys. Hashing only the ARN would miss a same-ARN policy
version change, while hashing either dormant module policy would attest a
control that AWS does not apply. A changed external document forces
requalification. Selecting, removing, or changing the external boundary is
therefore an additive profile-revision rollout, never an in-place mutation of an
active revision's immutable handoff. The apply identity must be able to read
that managed policy and its default version, and the operator must ensure the
organization boundary allows the exact copy and lifecycle actions because a
boundary can deny permissions that the inline and repository policies allow.

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
An optional external ID is either absent or satisfies the AWS STS request
contract exactly: 2-1,224 characters containing only letters, digits, and
`_+=,.@:/-`. Terraform rejects an invalid external ID before it can create a
trust policy that workers cannot use at runtime.

Terraform measures each rendered minified target-role trust policy against
`applied_role_trust_policy_quota`. The account default is 2,048 characters; an
operator may declare only an integer through AWS's 8,192-character maximum and
must raise it above the default only after the account quota increase is
applied. This keeps a long exact-principal list from failing for the first time
during IAM role creation.

The module renders every repository policy before apply, caps explicit pull
principals, and fails when the provider policy-byte limit would be exceeded. A
single organization-and-principal-tag statement may replace an ARN list only
when the operator explicitly supplies the organization ID and the dedicated
compute accounts enforce that immutable role tag. Qualification compares the
exact rendered and live policy and complete ownership-tag hashes; it never
truncates principals, ignores provider default tags, or broadens to account root
silently. The live verifier uses Terraform `jsonencode`-compatible UTF-8 and
legacy character escaping, so valid non-ASCII or provider-default tag values do
not create cross-language hash drift.

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
infra/terraform/modules/aws-image-canary-account
infra/terraform/modules/aws-image-canary-target
infra/terraform/examples/aws-dedicated-skypilot-account
```

The distribution module derives the partition from its regional AWS provider
and verifies that provider's account and region against explicit inputs. It
accepts the catalog authority, realm and profile, declared workspaces and
repository prefix, exactly one regional target with fixed shard and capacity
ceilings, exact runtime pull principals, exact worker base roles and target-role
names, an optional same-account and same-partition organization-managed
permissions boundary, an optional AWS-valid STS external ID, encryption and
scanning settings, repository-policy and quota ceilings, and the applied
trust-policy quota. It reads the applied ECR images-per-repository quota when
permitted; otherwise the operator supplies that verified value explicitly.

The worker-identity module binds three exact Kubernetes service accounts to
separate IRSA roles and exact same-partition target-role sets. The account module
owns the account-global EC2 Spot service-linked role. One canary-target module
per compute account and region owns the bounded launch and inspection role plus
any same-partition, same-region customer-managed AMI key grants. The companion
example composes worker identity and two distribution regions; compute-account
canary targets remain separate provider-scoped module calls as its README
describes.

The example's secret-free qualification manifest contains desired config and
timestamp fields, regional repository identities and fingerprints, encryption
and KMS-key facts, immutability and scanning settings, policy and Terraform-tag
hashes, capacity ceilings, target-role and boundary-policy fingerprints, and
ECR quota and worker-rate facts. Compute AMI, credential-helper, and Spot KMS
grant facts remain in the profile binding and canary-target outputs rather than
being invented by the distribution module. Background attesters compare the
handoff with live provider state and actual-principal canaries before activation.
Terraform output alone never claims live readiness.

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
imageCopyWorker.terminationGracePeriodSeconds
imageCopyWorker.serviceAccount

imageLifecycleWorker.enabled
imageLifecycleWorker.replicaCount
imageLifecycleWorker.maxInFlight
imageLifecycleWorker.terminationGracePeriodSeconds
imageLifecycleWorker.serviceAccount

imageCanaryWorker.enabled
imageCanaryWorker.replicaCount
imageCanaryWorker.maxInFlight
imageCanaryWorker.terminationGracePeriodSeconds
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

Location dispatch is two-level and no-starvation. Every location has a generated
`copy_claimable_at`: immediate or retry time for PENDING, lease expiry for
COPYING/VERIFYING, and null otherwise. Every transaction that changes location
queue membership or shard capacity refreshes the locked shard's `copy_next_at`
from the first shard-local indexed candidate. The global claim locks the oldest
due `(copy_next_at, id)` shard with `SKIP LOCKED`, then claims its oldest eligible
location. After dispatch, remaining due work rotates the shard projection to the
current claim time so another already-due shard runs first. The persisted
`last_dispatch_at` is a floor on every later refresh, so a heartbeat cannot undo
that rotation by rediscovering an older due location. A heartbeat refreshes the
projection under shard-before-location lock; a stale projection is repaired
synchronously without provider I/O before claim returns. The hot path
therefore examines one indexed shard and one indexed local location rather than
correlating every shard with the million-location table. Source-inspection claims
use their exact generated due-time projection. `FULL` is an admission state, not a dispatch stop:
already-reserved `PENDING` work remains claimable on `READY` and `FULL` shards.
An expired `COPYING` or `VERIFYING` lease remains reclaimable in every shard
state, including when the shard later becomes `PENDING`, `DRIFTED`, or
`DISABLED`; recovery performs an exact destination read and repairs the
in-flight counter, but fresh writes remain blocked outside `READY|FULL`.
Re-admitting a `FAILED`, `MISSING`, or `EVICTED` location is new
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
Explicit `container_images: null` is one such malformed refresh. It cannot be
interpreted as the eight-week default and cannot replace a prior null retention
opt-out; only a genuinely absent key produces the default policy.

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

Shutdown is a no-new-work fence. Every worker rechecks its process stop event
after heartbeat or configuration work and immediately before each maintenance
call, queue claim, and executor submission. Copy and lifecycle route every
executor submission through one shared stop-aware admission helper. The helper
submits only an entry wrapper, whose single stop-event read is the admission
linearization point. If shutdown wins that read, the wrapper returns before
invoking the task, so no database or provider boundary starts and the durable
claim remains safely lease-recoverable. If the wrapper wins, the operation was
admitted before shutdown and is treated as already in flight. This closes a
signal arriving after the caller's final check or inside `Executor.submit`
without holding a lock across provider work. Copy checks before and after every
independently blocking inventory, publication, and location claim; it cannot
begin a later claim after an earlier claim observes shutdown. It also checks
between configuration reload and qualification-manifest ingestion, and the
ingestor checks again before every independently persisted file. Lifecycle
checks between each independent maintenance substep. Synchronously invoked
bounded helpers receive the same stop predicate and check it before their first
bulk state query and before every per-location or per-demand transaction. A stop
observed at any boundary finishes only an already-open atomic transaction and
exits without beginning more database or provider work. Submitted individual
lease-owned operations drain through the executor; bounded qualification pages
also stop between independent profile, target, reservation, and automatic
runtime-canary scheduling items. Copy-role qualification threads the same live
predicate into each per-target and per-shard helper, its source-registry reader,
every ECR provider fence, and the transfer cancellation event. Lifecycle-role
qualification applies the predicate inside exact-delete request and readback,
so a stop observed after a delete response cannot begin the later presence
proof. Both roles recheck stop between provider acquisition, provider proof,
and the next database transaction. Lease fencing and durable state still own
any ambiguous provider result.

The canary worker additionally passes the same stop event into every submitted
canary. A task that has not created a provider child exits without launching
one. A task that may own a child observes the event between bounded provider
calls and on every runtime poll, then enters its existing `finally` teardown.
Every ordinary provider client acquisition and SDK call uses a drain-aware lease
fence: it proves the lease, then rechecks drain immediately before the raw call.
Compound EC2 and EKS helpers repeat that fence before every page, authority
acquisition, log read, node read, and role lookup. Cancellation never interrupts
teardown itself. Teardown uses separate explicit cleanup calls that deliberately
ignore drain while retaining lease and deadline fencing. A persisted child
claimed before shutdown therefore runs in cleanup-only mode and cannot be
recreated or continue ordinary qualification reads. This reduces shutdown
observation from the 3,600-second runtime maximum to one bounded provider call
or the 10-second poll interval, followed by custody cleanup. Verified cleanup
rechecks the process stop event before any success or failure terminalization.
If shutdown arrived during teardown, that exact owner releases the still-RUNNING
token instead, so successful cleanup cannot race into terminal qualification.
Once an unstarted task or verified teardown has no live provider child, the
exact owner advances its lease expiry to PostgreSQL's current time under the same
token. The replacement worker can reclaim it immediately without waiting for the
normal 15-minute lease. A failed release remains safe and falls back to ordinary
lease-expiry recovery.

EC2 teardown uses one absolute 480-second deadline within the 600-second
shutdown grace. Its terminal no-child proof remains 60 observations at a
five-second absolute cadence, so provider-call latency consumes the current
interval instead of accumulating after every fixed sleep. The remaining budget
is bounded headroom for credential acquisition, complete paginated discovery,
and provider calls. Teardown establishes that deadline before acquiring a fresh
cleanup-only EC2 client and never reuses the ordinary runtime client, whose
fixed one-hour assumed credentials may expire at the allowed 3,600-second
runtime limit. Ambient credential materialization, STS role assumption, and
service-client creation all receive the same lease and cleanup deadline fence.
Teardown checks that deadline immediately before and after every preliminary
discovery, every page of instance and Spot-request discovery, request
cancellation, post-cancel exact request read, instance termination, and
exact-state provider call. A cancellation-versus-fulfillment race therefore
adds the returned instance ID to the retained set before teardown can succeed.
A Spot cleanup is verified only when every discovered request is `cancelled`,
`closed`, or `failed`, and every associated or operation-tagged instance is
`terminated`.
The shared deadline is passed into the provider wrapper. That wrapper
proves the lease first, rechecks the deadline after any blocking heartbeat
database round trip, and only then invokes the raw SDK call. A call that crosses
the deadline makes cleanup unverified and no later provider call starts. EKS
cleanup applies the same call-start ordering to its own bounded delete/read
settling deadline. Its canary-specific Kubernetes client executes kubeconfig
`exec` credentials with a 15-second subprocess timeout in an isolated process
group. Concurrent bounded readers retain at most 1 MiB each from stdout and
stderr. Each reader reports clean EOF, overflow, and read failure separately;
overflow or any read failure terminates the complete group and fails closed.
Timeout, overflow, and read failure use bounded terminate-then-kill waits and
always reap the direct child. Reader threads catch every exception category, so
no unexpected reader failure can accept a buffered prefix or reach
`threading.excepthook` with plugin diagnostics.

Subprocess capture, complete `ExecCredential` validation, declared-expiration
parsing, loader application, Kubernetes client construction, and the final
provider fence form one credential-isolation boundary. It accepts only the
requested exact `apiVersion`, `kind: ExecCredential`, a mapping `status`, and
either one nonempty token or a complete nonempty certificate and key pair.
Malformed expiration values and credentials without enough declared lifetime
for one bounded API call fail with a fixed value-free classification. On any
failure the boundary clears byte buffers and credential-bearing references,
detaches control exceptions from sensitive tracebacks, exits every active
exception handler, and returns only a value-free failure classification or the
original sanitized drain/deadline control exception. The outer caller alone
raises that result. No transient plugin stdout, stderr, decoded payload,
credential value generated or parsed inside the boundary, decoder or loader
exception, or prior credential failure may survive in the public exception
message, cause, context, traceback-frame object graph, serialized envelope, or
logs. This does not prohibit a successfully returned Kubernetes client from
owning its installed credential while the client remains usable. Before any
refresh, provider, or control exception escapes a provider-wrapper frame, that
wrapper scrubs installed API keys, authentication prefixes, username/password,
certificate/key references, cookies, and authentication headers, closes the
client, and replaces it with a credential-free invalid state. The next
lease-valid call must rebuild it under the existing deadline. Provider
exceptions are detached from dependency tracebacks before propagation, so
request-local authorization headers cannot remain reachable. A later
drain/deadline fence that wins during refresh is invoked with no credential
exception active, so it cannot acquire that failure as hidden context. A
descendant that inherited a pipe therefore cannot extend the credential budget
or leak a credential or diagnostic secret.
When the Kubernetes wrapper is composed with the canary's generic fenced
client, the generic wrapper delegates its initial lease, drain, and deadline
fence to `ProviderFencedCoreApi` instead of running the same fence while its own
traceback frame still owns the live credential-bearing wrapper. The inner
wrapper captures an initial control failure, invalidates and scrubs the client,
then propagates the detached original control type. Non-Kubernetes providers
retain the generic wrapper's direct pre-call fence.
The EKS canary also owns one explicit outer client lifetime. Its `finally`
block completes any required child teardown first, then closes and scrubs the
Kubernetes wrapper before any success, validation failure, provider failure,
drain, lease loss, cleanup failure, or other exception leaves
`_run_eks_canary`. No outer canary traceback frame may therefore retain a live
installed credential after the function exits.
EC2 and EKS also separate work failure from the final cleanup winner. If
teardown failure, drain, or lease loss replaces an active provider failure, the
worker exits the losing exception handler, drops its traceback and response
state, and only then raises the deterministic winning error with no cause or
context. An internal typed teardown signal remains distinct through
`run_canary` terminalization, so only a genuine unverified teardown preserves
the durable child for successor cleanup. An unrelated provider `ValueError`
with the same message remains an ordinary provider failure and terminalizes as
`CANARY_FAILED` after successful cleanup. The same boundary preserves the
original provider exception when teardown succeeds, so useful ordinary
diagnostics are not erased. Cleanup may therefore determine custody without
making a losing provider response, request-local authorization header, or other
provider state reachable from the winning exception or its serialized envelope.
Transparent library-side refresh is removed from the raw API call path. The
wrapper converts a kubeconfig credential's declared wall-clock expiry into one
monotonic refresh deadline when the credential is accepted, preserving the
existing API-timeout headroom. Configured refresh intervals are monotonic too.
Later wall-clock jumps cannot extend or prematurely reset either deadline.
In-cluster projected service-account credentials have no exposed token expiry,
so the wrapper explicitly rebuilds that client on the Kubernetes library's
one-minute monotonic token-refresh cadence under the same fence.
The wrapper revalidates the monotonic deadline after the final acquisition
fence, after refresh admission, and immediately before the raw method. A
credential that loses its API-call headroom during loader or fence work is
scrubbed and rejected without a provider call.
Every refresh receives the current ordinary drain fence or cleanup deadline,
rechecks that fence after credential acquisition, and only then invokes the raw
Kubernetes method. Cleanup establishes its one 60-second deadline before any
replacement client acquisition, so a missing or expired client cannot create a
fresh auth budget outside teardown. Unsupported legacy auth-provider commands
are rejected on this bounded EKS canary path rather than executed without a
timeout. Every canary provider wrapper also leaves a failed SDK call's exception
handler before rechecking drain, lease, or deadline state. If that control fence
wins, the wrapper clears the losing provider error and response before raising
the detached control exception, so it cannot acquire provider diagnostics as
hidden context. The canary Deployment enforces a termination grace of at least
600 seconds. The grace covers the 480-second absolute EC2 cleanup budget,
including the 60-observation settling cadence and bounded provider-call
overhead, plus process-exit margin. The process exits immediately when cleanup
completes, so the
configured ceiling does not add a fixed rollout delay. The same drain applies to
disable and scale-down. For `helm upgrade --reuse-values`, legacy value maps that
omit the three worker grace keys render copy/lifecycle/canary defaults of
30/30/600 seconds. Null is treated as a legacy omission, while an explicit zero,
non-integral value, string, or canary value below 600 remains invalid in both
schema validation and the independent template guard. If the
grace expires or bounded provider cleanup itself fails, the operation and
concrete child identity remain RUNNING for lease-expiry recovery rather than
being falsely terminalized.

Restart recovery verifies actual registry state for immutable copy work and
pre-delete claims. An expired
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

Operation polling is capability-scoped before row lookup. Workspace readers may
query only `PUBLISH`, `PREPARE`, `RETRY_PUBLICATION`, and `RETRY_LOCATION` rows.
`PROFILE_QUALIFY` and `PROFILE_CANARY` are administrator-only and are excluded
from a non-admin query in SQL, so a known UUID is not an authorization bypass.
Operation responses use a per-kind result-key allowlist. Public kinds expose
only their publication or location identity and state; administrator profile
operations expose only the documented qualification or canary projection, with
the raw canary nonce replaced by its hash. Arbitrary durable `result_json` is
never copied directly into an API response.

Authorized users can:

- select a workspace;
- page artifacts with digest, releases, platforms, size, and updated time;
- filter by release, digest, and exact source reference;
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

Workspace capability state is request-scoped, not merely generation-fenced.
Changing the workspace immediately invalidates the previous capability object,
aborts every catalog, failed-publication, readiness, and capability request,
closes mutation dialogs, clears retries and scoped errors/data, and resets each
cursor history. Rendering and mutations require the capability response's
request workspace to equal the current route workspace exactly. A pending or
failed A-to-B capability request therefore cannot leave workspace A controls,
rows, dialogs, or errors visible or actionable, even when the same user is
authorized in both workspaces.

Artifact-detail state is scoped by the compound route identity
`(requested_workspace, artifact_id)`. Detail data, capabilities, collection
pages and cursors, errors, tabs, and mutation dialogs render only while that
identity exactly matches the current route. A route-identity change fences the
old state during render, before effects run, then aborts old detail and
collection requests, closes dialogs, and resets scoped navigation. A failed
replacement request may retain cached data only for the same compound identity;
it can never restore data or controls from the previous identity. Every
asynchronous branch checks generation, compound scope, and controller ownership
before its first state write, including the nested stale-cursor first-page
fallback and its notice.

Workspace selection is a recoverable navigation transaction. The Dashboard
hides the old scope before requesting the route change, but owns the
`router.replace` result. A rejected navigation or an explicit `false` result
reloads the unchanged route workspace and restores its selector value. Applying
the current workspace is a no-op only while current capabilities exist;
otherwise it restarts capability loading. A superseded navigation result cannot
reload an older route.

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
- the credential-free catalog mutation state and owner target, with distinct
  instructions for `DELETING`, exact-owner `RESTORING`, and fresh-generation
  `QUARANTINED` recovery;
- the bounded permanent repository-tombstone count and truncation flag, plus an
  exact lookup for every repository ARN in the operational profile page so a
  tombstone beyond the global display cap cannot be shown as healthy;
- each target's configured generation, exact Terraform-attested repository ARN,
  attested/not-attested state, quarantine reason and time, and the minimum next
  generation remediation when its selected ARN is tombstoned;
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
GET /images/catalog?workspace=W&release=R&digest=D&source_ref=S&limit=50&cursor=C
GET /images/publications?workspace=W&state=S&release=R&limit=50&cursor=C
GET /images/artifacts/{id}?workspace=W
GET /images/artifacts/{id}/releases?workspace=W&limit=50&cursor=C
GET /images/artifacts/{id}/sources?workspace=W&limit=50&cursor=C
GET /images/artifacts/{id}/publications?workspace=W&limit=50&cursor=C
GET /images/artifacts/{id}/locations?workspace=W&limit=50&cursor=C
GET /images/artifacts/{id}/demands?workspace=W&limit=50&cursor=C
GET /images/operations/{id}?workspace=W
GET /images/profiles?workspace=W&limit=50&cursor=C
GET /images/workers?workspace=W&limit=50&cursor=C
GET /images/readiness?workspace=W
```

Reads use opaque versioned keyset cursors bound to workspace and filters. Limit
is 1 through 100. A cursor from another workspace, filter, profile revision, or
server version fails closed. Responses bound associations; detail collections
remain paginated. Profile validation permits at most 128 profiles and 256 targets
per profile. Profile-history reads use an indexed, newest-first keyset page and
never materialize the durable revision history. Capabilities first derive the
at-most-128 configured and allowed profile names, then query only their ACTIVE
rows through the partial unique index; it does not scan historical revisions.
Readiness similarly uses a UNION of only ACTIVE and QUALIFYING branches, each
served by its partial unique index. It fetches at most 1,001 operational rows,
never lets SUPERSEDED or RETIRED history consume the window, and removes the
last profile as a complete boundary if the cap splits its ACTIVE/QUALIFYING
pair.
The v0 catalog exposes only release, digest, and exact source-reference filters,
whose unique or prefix-bounded identity paths lead directly to an artifact.
Distribution, registry target, and location-state remain visible in bounded
page summaries and paginated artifact detail, but are not catalog filters in v0.
They require a transactionally maintained artifact-ordered facet projection
before becoming filterable; a child-table `EXISTS` plus response `LIMIT` is not
an acceptable million-artifact access path.
Each catalog page loads at most ten active-publication, source, and location
samples per artifact through three fixed lateral-limit statements and matching
artifact indexes. `publications_truncated`, `sources_truncated`, and
`locations_truncated` mark partial summaries; the UI labels them as partial and
uses the paginated artifact endpoints for complete detail. Every artifact
collection has independent Previous and Next controls backed by its own fixed-
window opaque cursor history. Each history retains at most 20 `{cursor, page}`
entries, preserves the absolute page number after trimming, and can continue
moving forward without a page-count limit. Once older cursors leave the window,
Previous stops at the oldest retained entry and an explicit First control
reloads page 1. Navigating one collection does not eagerly materialize or refetch
the others. The catalog and workspace failed-publication feed use the same
bounded helper and independent histories, so an unbound failure remains
discoverable and retryable beyond the first page. Stale collection or
publication cursors reset only their own view to its first page with an explicit
notice. Summary cards say that their counts cover the visible page rather than
presenting a bounded page as a total.

Catalog and detail reads remain available to workspace readers, but source and
publication projections reveal credential-binding names and fingerprints only
to callers with the workspace publish capability. Other readers receive null
for those fields. Full profile history and its Terraform, role, repository, and
attestation evidence is administrator-only, like worker and readiness state.
Authorization and workspace resolution happen before object lookup, and the
Dashboard treats redacted binding fields as absent rather than rendering an
identifier placeholder.

Readiness first selects at most 1,001 shards, excludes any boundary target group
that could be partial, and projects at most the first 100 complete
profile/target/account/region groups. One fixed PostgreSQL statement serves all
selected groups. Each group scans at most 10,001 indexed queue rows per state
class and reports `at_least: 10000` above that cap; oldest age reads one indexed
head per shard and pending state. `queues_truncated` marks either shard or group
truncation, so aggregate counts and the table are displayed as lower bounds.
The query never materializes every durable group, uses no global `array_agg`, and
does not issue per-group round trips. No dashboard read creates a generic
request row.

The SDK catalog APIs retain their explicit keyset cursor. The convenience
`status` command remains bounded to one 100-row page and must disclose a
non-null next cursor instead of silently presenting the page as complete; users
then narrow an explicit selector rather than materializing a million-row catalog
in one client process.

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
- Source registry connections require a public HTTPS peer at connect time.
  Proxy inheritance, private or link-local peers, cross-authority basic
  credential forwarding, bearer-token redirects, redirect chains, and
  unbounded source response bodies fail closed before destination authority.
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
2. Run the global-state, Serve, and managed-jobs Alembic heads once in a
   revision-scoped Helm Kubernetes Job. It is a
   normal chart resource rather than a hook, so chart-managed database Secrets
   exist before it starts and Helm adds no hook wait to deployment latency. API
   pods start concurrently in `verify` mode, refuse to serve unless all three
   central schemas are current, and become ready after the Job commits. Every
   enabled copy, lifecycle, and runtime-canary worker performs the same eager
   three-schema verification before constructing or advertising health and
   before any provider work. For a nonempty global-state database below 023, first
   deploy revision 023 and drain every older binary; the 024 Job refuses that
   unsafe starting state. A fresh isolated database must explicitly set
   `databaseMigration.bootstrapFreshSchema: true`; the resulting `bootstrap`
   mode checks every schema-owned relation, sequence, type, and routine, not
   only tables. The default `upgrade` mode refuses every unversioned schema,
   including an empty one. From revision 023 onward, the Job and transitional
   `auto` pods share the same per-schema cross-host PostgreSQL advisory locks,
   so only one Alembic process can mutate each schema. Global state, Serve, and
   managed jobs all honor the same migration mode. API replicas and every
   enabled image worker run in `verify` mode while the Job owns migration; a
   worker can never win startup and perform the Job's DDL. Disabling
   `databaseMigration` explicitly puts both API and workers in the lock-protected
   `auto` fallback for local or operator-owned migration workflows.
   Database CA or client-certificate volumes are declared once through the
   chart's shared `databaseConnection.extraVolumes` and
   `databaseConnection.extraVolumeMounts` values. The API, migration Job, and
   all image workers mount that exact set. The migration Job receives global
   and database-connection inputs only; API-only environments, Secrets, and
   volumes are not leaked into its narrower identity. Helm rendering fails on
   duplicate or chart-reserved environment names, volume names, or mount paths
   across each produced Pod instead of emitting a manifest Kubernetes rejects.
   Source-secret Roles and RoleBindings reserve their suffix before truncation
   and include a stable namespace/secret hash, so valid long release names
   cannot collapse distinct grants onto one Kubernetes object.
   Ordinary request completion checks the central database dialect before
   issuing a cluster-image terminal hint. A local SQLite API therefore performs
   no managed-image query and emits no PostgreSQL-only warning when the feature
   is unavailable.
3. While old 023 API replicas still serve traffic, confirm they ignore the
   additive 024 tables. Roll every API replica to the new binary, then prove no
   old pod remains before feature activation.
4. Adopt the existing deterministic generation-zero resources into the
   dedicated registry account's Terraform state before planning a generation
   bump. An empty state is a hard stop, not permission to recreate resources
   with colliding names. Require a saved plan with zero deletes and zero
   replacements. In each compute account, apply the account-global Spot
   bootstrap once, then every regional canary target and any customer-managed
   AMI KMS grants. Keep EC2 passable roles and profiles separate from EKS
   inspect-only node profiles.
5. Deploy one copy worker, one lifecycle worker, and one runtime-canary worker
   with separate identities. The lifecycle Deployment uses `Recreate`, so
   normal upgrades never overlap lifecycle binaries with destructive provider
   authority. Copy and runtime-canary Deployments use `RollingUpdate`.
   An existing installation's first protocol-2 upgrade uses these explicit
   gates:

   1. Immediately before mutation, capture the actual live Helm revision, chart
      and app version, API and image-worker image references, worker replica
      counts, worker rollout strategies, and central schema heads as preflight
      baseline `P`. Never substitute a historical release number for this live
      evidence. Probe the installed chart schema rather than assuming whether it
      accepts a zero canary replica count. Through Helm, disable copy, lifecycle,
      and canary workers; a chart whose canary schema rejects zero replicas must
      use `enabled=false`. Wait for all worker Deployments and Pods to disappear,
      zero catalog-wide `RUNNING` `PROFILE_CANARY` operations, and verified
      cleanup of every provider child. Record the resulting Helm revision,
      chart, app version, API image, absent worker workloads, and schema heads as
      rollback boundary `Q`.
   2. Deploy the immutable target chart and image with all workers still
      disabled. Require the migration Job to succeed, PostgreSQL to report the
      revision-027 schema, every API Pod to use the target image, and no old Pod
      to remain. Inspect the singleton instead of assuming it is empty. An empty
      026 row remains empty; any legacy `DELETING` or `RESTORING` row is
      atomically migrated to `QUARANTINED` with its state-specific reason and a
      permanent exact-ARN tombstone. Record this inert target Helm revision,
      chart and app version, exact API image, disabled worker values and absent
      worker workloads, and revision-027 schema heads as boundary `T0`. Never
      re-enable a pre-027 image worker after this point. If T0 contains
      `QUARANTINED`, perform no provider work on the old ARN and continue only
      through the fresh-generation cutover.
   3. Apply the additive Terraform generation bump after quiescence. Require a
      zero-delete, zero-replacement plan, creation of the new repository after
      old-worker termination, retained old repositories with `prevent_destroy`,
      all four effective copy/lifecycle role policies scoped to only the new
      active qualification ARN, the full qualification allow policy on only the
      new active repository, deny-only image data-plane policies on every
      inactive retained repository, and a new immutable, content-digest-derived
      handoff ConfigMap name. Terraform and AWS policy updates are not one atomic
      operation, so keep every image worker disabled until the complete apply
      succeeds and exact policy readback proves that final state. A partial
      apply is a hard stop and must be retried to convergence before handoff
      ingestion. Publish a later profile revision selecting the nonzero
      generation. Never replace the ConfigMap mounted by an older worker in
      place.
   4. Ingest that exact handoff. For a migrated `QUARANTINED` row, ingestion
      must prove the same logical profile and target fingerprint, a strictly
      higher generation, a different non-tombstoned Terraform ARN, and zero
      running canaries before recording the cutover and clearing the singleton.
      The old repository tombstone remains. For an empty singleton, ingestion
      simply stages the fresh candidate.
   5. Enable the target copy worker only. Keep lifecycle disabled and canary
      explicitly enabled at zero replicas. Prove the new `QUALIFYING` revision,
      exact config and Terraform hashes, generation-scoped repository ARN, and
      fixed digest presence. The old revision remains `ACTIVE` but is suppressed
      from ordinary worker maintenance while the candidate exists.
   6. Enable the target lifecycle worker with canary still at zero. Wait for its
      `Recreate` rollout, protocol-2 `ARMED` evidence for the fresh generation,
      and a copy attestation acknowledging that exact proof. The worker must not
      adopt or mutate legacy generation-zero evidence.
   7. With the canary worker still at zero, require zero catalog-wide `RUNNING`
      canaries and zero unrelated `PENDING` canaries, then enqueue only the
      intended profile, target, backend, and runtime proof. Enable the target
      canary worker with exactly `replicaCount=1` and `maxInFlight=1` until that
      proof reaches a verified terminal state. Actual-principal pulls create
      fresh runtime evidence, after which lifecycle may advance through
      `PRE_INTENT`, `IN_FLIGHT`, `READBACK`, `RESTORING`, owner recopy, atomic
      barrier removal, and activation. Require the physical digest to be
      restored, the singleton row absent, and no unrelated canary to have
      launched before restoring the reviewed steady-state canary concurrency. A
      newly `QUARANTINED` row stops provider work on that ARN and repeats the
      higher-generation cutover.

   Every Helm revision uses `--reuse-values`, pins the exact image, waits for
   migrations and workloads, and preserves the last safe boundary. Re-read live
   state before every transition and stop if it no longer matches the recorded
   predecessor boundary. This one-time quiescing plus the fresh physical
   identity prevents a pre-protocol request from deleting target-generation
   bytes.

The 2026-07-24 read-only review observed Helm revision 260 on chart/app
`1.1.784`: the API, copy, lifecycle, and canary workloads used image `1.1.784`;
all three image workers had one replica; the canary worker admitted two
concurrent operations; and the lifecycle Deployment still used
`RollingUpdate`. This is historical evidence that the protocol-boundary
quiescence remains necessary, not a reusable rollout baseline. Repeat step 5.1
immediately before deployment.
6. Let each worker attest only its own capability, run actual-principal EC2 and
   EKS canaries, and atomically activate the desired generation only after all
   repository, quota, KMS, policy, runtime, and fingerprint evidence is fresh.
7. Verify publish, warming, pull, API/controller restart, retry, capacity
   admission, drift fail-closed behavior, and demand-fenced eviction.
8. Convert the Boltz L4 test fleet and compare direct cross-region pulls,
   operator-prewarmed images, and managed JIT locations.
9. Enable production only if the operations or performance gate passes.

Rollback first disables profile activation and new publication, then stops
worker claims, and only then rolls API binaries. `T0` may roll back to `Q`
because both keep workers disabled and the migration is additive. Copy-only may
return to `T0` only while the singleton row is absent. During `DELETING` or
`RESTORING`, keep the exact target lifecycle and copy binaries running until
the row clears; do not terminate the owner or start an older worker.
`QUARANTINED` is roll-forward-only to a fresh physical generation. From the
canary stage, first return canary replicas to zero and prove zero running
operations and child cleanup.

A blind `helm rollback` that can restore any pre-protocol image worker is
forbidden. Keep all image workers quiesced and roll forward, or perform the
documented empty-database downgrade before starting the older workers.
Existing digest pull plans remain usable and PostgreSQL intent is preserved.
Feature-disabled old API binaries tolerate the additive tables, but pre-027
lifecycle/copy workers are not write-compatible with the phase and tombstone
contract and must never be re-enabled. Unchanged direct digest-pinned OCI
behavior remains the escape hatch.
Downgrade is a separate manual operation allowed only after every new process is
drained and both the mutation and permanent tombstone tables are empty; it is
never part of Helm rollback.

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
- Terminal job-task and Serve-version reconciliation issues exact bounded tuple
  lookups; Serve projects the bounded requested tuples through `VALUES` and
  indexed `EXISTS` probes, returning at most one row per requested identity.
  Unrelated task, version, or replica history is never loaded and filtered in
  Python.
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
- A qualification digest delete is globally serialized across all workspaces,
  profiles, revisions, and physical targets; no claim, provider create,
  qualification copy, stage, or activation can cross its durable
  `DELETING`, `RESTORING`, or `QUARANTINED` barrier except the exact recorded
  owner's CAS-protected restoration and the audited same-profile
  fresh-generation handoff that clears only the temporary quarantine
  singleton. Every provider admission rechecks the permanent exact-ARN
  tombstone after acquiring the catalog lock, and cutover never deletes that
  tombstone.
- Every mutation is idempotent, typed, and detachable without abandoning durable
  or ambiguous provider work; every read bypasses generic request rows.

### Required verification

- real PostgreSQL migration through global-state revision 027, concurrency,
  lease, retry, and downgrade tests;
- fresh-through-024 and literal 023-to-024 exact schema equivalence, preview
  adoption rejection for table, column, default, generated-expression,
  cluster-binding-column, constraint, foreign-key, index, and catalog-singleton
  drift, concurrent migration-lock, and mixed-023/024 feature-disabled tests;
- 024-to-027 migration tests proving restart-safe child evidence, the exact
  reserved running-canary index shape, and the singleton
  qualification-mutation table and constraints; exact 026-to-027 tests cover an
  empty singleton, legacy `DELETING` and `RESTORING` adoption into lease-free
  `QUARANTINED` with distinct reasons and permanent exact-ARN tombstones,
  one shared database transition timestamp for each adopted singleton and its
  tombstone, runtime schema equivalence, empty downgrade, and downgrade refusal
  with durable residue;
- old-server/new-client and new-server/old-client feature-gate tests;
- API 61/62 behavior across launch/exec, jobs, Serve up/update, pools, nested DAGs,
  resource alternatives, forged private fields, and request config overrides;
- scalar/object parsing, every selector combination, explicit opt-in/defaults,
  allowlists, direct restrictions, and byte-for-byte legacy `image_id` tests;
- ordinary Kubernetes launches and dry runs with no `container_image`, plus
  multicloud optimizer parity proving a warming AWS preferred route retains its
  original direct resources and rank beside GCP or generic Kubernetes;
- AWS integration plus mocked canonical-read, canonical-delete-denial,
  regional-delete, identity-policy, repository-policy, and
  permissions-boundary tests, including null compatibility, exact external
  attachment, attached-document fingerprinting, and negative account and
  partition cases, via `terraform test -test-directory=terraform-tests`; a
  stateful verbose-plan harness additionally proves the boundary switch updates
  exactly two roles in place with zero creates or destroys;
- clean-account Spot service-linked-role planning, existing-role import
  instructions, regional customer-managed AMI KMS grants, and least-privilege
  Spot request tag, describe, and cancel policy tests; mixed EC2/EKS plans prove
  only EC2 runtime roles are passable, the required instance-resource
  permission pins `ec2:InstanceProfile`, instance-only conditions do not poison
  other resource authorizations, EKS node profiles remain inspect-only, and an
  EKS-only target has no EC2 launch or `PassRole` authority;
- EC2 instance and EKS kubelet runtime pull-auth refresh tests, preinstalled
  helper/AMI enforcement, homogeneous EKS-node-role validation, multi-cluster
  attestation, no managed-path CLI install/login, plus
  unchanged direct OCI behavior on other runtimes;
- `terraform fmt -check`, `terraform validate`, and plans for one and multiple
  regions with fixed shards;
- Helm rendering that migrates global state, Serve, and managed jobs in the
  Job, forces API and all image workers to `verify` while the Job is enabled,
  restores `auto` only when it is disabled, defaults the Job to `upgrade`, emits
  `bootstrap` only for an explicit isolated-fresh flag, rejects reserved and
  duplicate environment, volume, and mount-path collisions, excludes API-only
  inputs from the Job, mounts shared database TLS material into every database
  consumer, and keeps two same-namespace source-secret grants distinct under a
  63-character fullname; asserts `RollingUpdate` for copy and canary workers
  and `Recreate` for the destructive lifecycle worker; schema-bypassed template
  tests independently reject explicit zero, sub-600, non-integral, and string
  canary grace values;
- worker kill/restart and ambiguous-outcome tests around source reads, layer
  availability/download/upload/complete, `PutImage`, exact verification, SQL
  completion, publication fan-out, eviction, and attestation activation;
- EC2 canary profile-latch tests proving the launch response and later
  ID-scoped observation are lease-fenced into the durable operation, a
  successor accepts a terminal AWS record that omits the profile only with
  either that exact latch or the nonce-bearing private-pull recovery proof, and
  a mismatched observed ARN, a path-qualified IAM lookup ARN that differs from
  the expected profile ARN, or a marker-free terminal record still fails
  closed;
- real-PostgreSQL catalog-barrier tests proving the singleton schema and
  `DELETING` phase, `RESTORING`, and `QUARANTINED` constraints, shared-reader
  versus exclusive-transition advisory-lock ordering, post-lock database-clock
  sampling, and the catalog-wide zero-`RUNNING` predicate through its exact
  partial index even for null or unknown result projections;
- deterministic two-workspace tombstone races pausing stale copy, provider,
  lifecycle, restoration, deletion, and copy-publication admission around the
  catalog lock while another session quarantines and cuts over; the exclusive
  transition cannot pass a held shared admission lock, and every resumed or
  later admission rejects the permanent old ARN;
- a 100,000-row tombstoned-ACTIVE PostgreSQL fixture proving one fresh
  `QUALIFYING` candidate returns after exactly one statement whose analyzed plan
  uses `ix_container_image_profile_state` with the `QUALIFYING` index condition
  and never scans `ix_container_image_profile_qualification_queue`; a separate
  zero-candidate case proves ACTIVE fallback and same-profile suppression;
- deterministic cross-workspace and cross-revision interleavings in separate
  PostgreSQL sessions: a `RUNNING` canary for workspace A/revision 1 prevents
  workspace B/revision 2 from inserting `DELETING`; if B commits `DELETING`
  first, A cannot claim, reserve cost, authorize a persisted child, start or
  complete a qualification copy, stage a successor, or activate until the same
  mutation reaches `RESTORING` and is cleared; generic non-copy evidence may
  still be recorded but remains non-actionable;
  repeat the schedule with old and desired revisions of one profile and with
  different physical targets to prove that the intentionally global row, not a
  coincidentally matching revision key, supplies exclusion;
- delete-owner phase tests proving `PRE_INTENT` is committed before credential
  acquisition, `IN_FLIGHT` commits in the final ECR hook before the raw delete,
  and `READBACK` commits before any presence read; an expired `PRE_INTENT`
  rotates its token, an expired `READBACK` rotates into read-only recovery, and
  an expired or ambiguous `IN_FLIGHT` atomically quarantines instead of taking
  over; the old token cannot issue another delete/readback or transition exact
  absence, and an unrelated workspace/revision can neither take over nor
  rewrite the owner identity;
- deterministic late-arrival tests pausing the old provider request after its
  final fence, expiring and quarantining the claim, then allowing that request
  to land; no successor delete, readback restoration, copy publication,
  activation, or canary claim may cross the retained barrier;
- restoration tests proving exact absence CASes the same owner/proof from
  `DELETING/READBACK` to lease-free `RESTORING`, only the recorded owner
  revision may copy, and exact copy verification plus attestation and singleton
  deletion commit atomically under the exclusive catalog lock; a stale owner,
  unrelated copy, mismatched target/repository/digest/proof, or lost CAS leaves
  the row present and every admission path closed even when bytes happen to
  exist;
  legacy exact-absence evidence is eligible for atomic restoration adoption
  only on a nonzero qualification-repository generation, generation-zero
  evidence remains untouched, and activation rejects a fresh copy/lifecycle
  pair whose restoration proof does not match;
  eight older failing revisions cannot hide the direct `DELETING` lifecycle
  owner or `RESTORING` copy owner from worker recovery;
- deterministic claim/delete interleavings proving claim takes the shared
  catalog lock after its operation/profile/cost locks and reserves worst-case
  cost only after the barrier check: deletion wins with zero cost reserved, or
  claim wins and the catalog-wide `RUNNING` predicate prevents deletion;
  recovered `child_launch_id` tests prove launch authorization repeats the
  shared barrier check immediately before EC2 or Kubernetes create; canary
  requests for the exact mutation owner remain durably PENDING and cost-free
  through every barrier state, then become claimable after owner restoration;
- protocol-boundary Helm rehearsals starting from an immediately captured live
  `P`, not a hardcoded chart version, and proving that the installed chart
  quiesces copy, lifecycle, and canary workers with the schema-valid disabled
  form; the rehearsal records exact `Q` and `T0` Helm revisions, chart/app
  versions, images, worker replicas and strategies, and schema heads. The target
  chart then proves migration in an all-off revision, consumes a versioned
  generation-one handoff only after a zero-delete Terraform apply, enables copy,
  enables lifecycle with no old/new lifecycle overlap, and finally restores
  canary only after proving zero unrelated `PENDING` and zero `RUNNING`
  operations, enqueueing the intended target alone, and setting
  `enabled=true, replicaCount=1, maxInFlight=1` through its verified terminal
  proof; rollback rehearsals preserve `Q` and `T0` rather than starting
  pre-protocol binaries;
- Terraform generation tests proving defaults preserve the unchanged
  generation-zero resource address and path, a shared-state generation-zero
  apply followed by `[0, 1]` adds rather than replaces a repository, retained
  outputs expose both ARNs while copy/lifecycle boundaries and inline policies
  expose only the active generation-one ARN, only generation one has the full
  qualification allow policy, generation zero has a wildcard-principal
  deny-only policy for every image data-plane action, the active output selects
  generation one, active policy and complete `tags_all` hashes select only the
  active generation and include provider default tags, omitting zero or the
  active generation fails validation, qualification rejects wrong live policy,
  ownership tags, encryption/KMS, or scanning mode before recording readiness,
  and
  `terraform-tests/verify_prevent_destroy.sh` proves a later removal plan fails
  on the retained generation's `lifecycle.prevent_destroy`; rollout readback
  proves the same policy state before any worker or handoff ingestion is
  enabled;
- lost-response Spot tests where cancellation first reports a terminal request
  without an instance, a request-to-instance edge appears on a later exact
  read, a late request transition cannot reuse an earlier absence interval, and
  a true terminal no-child outcome consumes the complete settling window;
  paginated instance discovery must terminate a running child found only on a
  later page and reject malformed or cyclic cursors; the production 60-poll
  absolute cadence must complete with nonzero provider latency inside its
  distinct 480-second deadline;
- deterministic stop-during-heartbeat tests for copy, lifecycle, and canary
  workers proving that shutdown begins no later maintenance, internal copy
  claim, manifest-ingestion item, lifecycle-maintenance item, publication fanout
  transaction, terminal-consumer query or transaction, qualification item, or
  executor submission; stop-aware admission tests set shutdown inside the
  submission boundary after the caller's final check and prove the entry wrapper
  never invokes the claimed task; per-target copy and per-shard qualification
  tests prove
  stop after a provider proof cannot start the next database transaction or
  shard, the source reader receives the live predicate, transfer cancellation
  observes it without a snapshot, and lifecycle stop after a concluded delete
  cannot begin exact readback; provider-level canary drain tests proving
  cancellation cannot create a new child, cannot begin an ordinary authority
  acquisition or provider read after a lease heartbeat observes drain, always
  enters uncancelled teardown for an owned child, and cannot terminalize success or
  failure when shutdown arrives during verified cleanup; slow-provider tests
  proving EC2 settling passes one absolute deadline through the real fenced
  client, rechecks it after a heartbeat that crosses the deadline, treats an
  over-budget result as unverified, cannot start that or any later provider call
  after its 480-second wall-clock budget, and acquires fresh cleanup credentials
  under that deadline instead of reusing a one-hour runtime session;
  PostgreSQL tests proving a verified drain is immediately reclaimable while a
  stale token cannot release its successor; plus Helm rendering and schema tests
  enforcing the canary's 600-second minimum custody-cleanup grace and legacy
  `--reuse-values` defaults of 30/30/600;
- real deferred-IRSA credential-provider tests proving its nested unsigned STS
  client inherits 10/60/one-attempt defaults without making a network request;
  real kubeconfig exec tests proving bounded success, whole-process-group
  termination when a descendant inherits a pipe, early stdout and stderr flood
  rejection, fail-closed injected stdout and stderr reader failures before and
  after partial or syntactically valid output, exact API version and kind,
  complete credential shape, fixed invalid-expiration handling, and value-free
  failures with empty cause and context chains; tests traverse every public
  exception's traceback locals through a bounded cycle-safe object-graph walk,
  plus the nested exception graph, serialized envelope, and captured logs for
  unpredictable child-generated secrets; installed-client invalidation on
  refresh, provider, and control failures, including an initial canary lease,
  drain, or deadline failure before Kubernetes refresh, plus explicit
  outer-canary invalidation after success, validation failure, provider failure,
  drain, lease loss, or teardown; direct-child reaping, no transparent
  API-client refresh hook, one-minute monotonic
  in-cluster projected-token rotation, monotonic declared-expiry conversion,
  rejection when a final fence or pre-call callback consumes the remaining
  headroom, backward and forward wall-clock jump immunity,
  ordinary refresh stopped before a raw call, cleanup refresh stopped at the
  shared deadline, and persisted-child drain entering cleanup-only before
  ordinary auth acquisition;
- worker-clock skew and blocking-lock tests proving that copy and eviction
  claims, provider grants and throttles, retry delays, consumer terminal
  confirmation, unattached-cluster retention, worker cleanup, and terminal
  compaction derive shared epochs from PostgreSQL, while local grant expiry and
  worker housekeeping use only monotonic process time;
- fast and slow qualification-producer tests proving profile-attestation
  `observed_at`, automatic-canary scheduling, activation preflight, and final
  freshness checks use PostgreSQL time rather than any worker or scheduler wall
  clock, plus copy-role due-check tests proving fast and slow worker clocks
  cannot re-probe fresh copy or inventory evidence and a real-PostgreSQL
  blocking test proving every attestation writer serializes behind the exact
  profile advisory lock;
- locked new-demand tests proving that runtime-proof validation and
  `demand.created_at` use the same database epoch, stale proof rejection rolls
  back both demand and watermark, and an exact existing replay survives both
  later proof expiry and replacement by a newer successful attestation without
  changing its original creation time;
- source-reader tests for private and multicast literals, DNS rebinding to
  private or multicast peers before TLS bytes,
  off-authority bearer realms, private and chained redirects, disabled token
  redirects, absent proxy inheritance, credential non-forwarding, streamed
  token/manifest size ceilings, pre/post-request lease fencing, and lease loss
  during both generic registry and signed ECR chunk streaming, including loss
  immediately before a blocking iterator advance and destination-race paths
  that never acquire or leak a source response, plus upload-initiation failures
  proved against both production readers before their first iterator advance;
- replay after lost mutation responses, key/body collision, detach before/after
  intent commit, stable result shape, bounded error, and CLI remediation tests;
- canary nonce/principal proof, child-launch crash deduplication, forced teardown,
  automatic refresh, concurrent daily-cost reservation, stale-binding tests,
  exact shared EC2 helper-route configuration, plaintext and strict-base64
  console marker responses, malformed console rejection, guest failure
  self-termination, delayed EC2 instance-profile visibility, terminal EC2
  profile omission after an exact match, delayed terminal console publication
  and bounded marker-free console exhaustion,
  expired-owner/successor interleavings at attach/fail/provider boundaries,
  pre-create client failure, stable EC2 `ClientToken` replay, provider-call
  pre/post lease fences, database authorization immediately before both EC2 and
  EKS creates, slow-host-clock denial after database expiry,
  renewal-crosses-deadline rejection for EC2/EKS, and a
  production failure caller blocked past lease expiry without stale-clock
  terminalization; Spot-specific regressions lose both accepted launch
  responses, discover an open operation-tagged request with no instance, race
  fulfillment against cancellation, retain the late instance ID, and require
  terminal request plus terminated instance proof before terminalization;
- idempotency collision-matrix, canonical publication fan-out, controller
  restart, shard-ceiling, and inventory-drift tests;
- source/destination/compute account separation, cross-identity attestation,
  negative STS trust, permissions-boundary, repository-policy size/principal,
  KMS grant, protected destroy, empty import, exact-ARN wildcard and
  policy-variable rejection, target account/region/partition validation,
  partition-derived China PassRole service names, desired-generation fencing,
  and old-revision drain tests;
- policy-only profile revision location reuse and physical-layout-change
  rejection after first release, including a constant-row custody marker plan,
  READY-versus-stage interleavings, and activation revalidation;
- one-million-row resumable inventory, exact missing confirmation, durable cursor,
  batched token grants, hot/cold target no-starvation, throttling, count/byte
  ceilings, expired blocked abandonment fencing, and empty failed-reservation
  reclamation tests;
- PostgreSQL `EXPLAIN (FORMAT JSON)` scale fixtures proving that publication
  inspection, copy-shard dispatch, inventory claims and runtime-digest matches,
  canary pending and expired-lease claims, readiness, live/terminal demand
  pages, expired reservations, canonical
  publication fan-out, terminal operation compaction, worker cleanup, and
  state-filtered history use their exact indexes with large terminal or idle
  populations present;
- staged-migration tests proving that tables, views, materialized views,
  sequences, user-defined types, and routines each make an unversioned target
  schema nonempty and block revision 024 before DDL, while a separate empty
  search-path schema upgrades directly;
- Serve migration convergence tests constructing all six known upstream and
  managed-preview layouts stamped 022, 023, and 024, then proving each reaches
  the sole revision-026 head with response and prediction history, quarantine,
  workspace, and exact replica-version lookup intact, plus a predecessor-
  stamped legacy SQLite replica layout proving revision 026 restores the full
  JSON state and both lookup indexes. Real-PostgreSQL collision tests reject
  valid same-name partial, expression, included-column, method, and wrong-column
  shapes and rebuild only INVALID or not-ready same-table residue;
- Managed Jobs revision-025 collision tests proving malformed same-name
  identity indexes fail without advancing the revision, interrupted concurrent
  residue is rebuilt, the final catalog shape is exact, and a large tuple-IN
  terminal lookup uses `ix_spot_job_task`;
- workspace-publication history and operational-profile readiness scale fixtures
  proving the former uses its `(workspace, created_at, id)` keyset index and the
  latter uses both ACTIVE and QUALIFYING partial indexes despite more than 1,001
  historical revisions;
- hot-artifact catalog fixtures proving three fixed summary statements, ten-row
  child caps, explicit truncation flags, and publication/source/location index
  plans, plus many-target readiness fixtures proving one statement, a 100-group
  cap, queue lower-bound flags, and no per-group query growth;
- Dashboard pagination tests proving independent artifact-collection and failed-
  publication cursor histories, a 20-entry maximum across arbitrarily many
  forward pages, preserved absolute page numbers, the First-page escape after
  history trimming, bounded page replacement rather than eager row
  accumulation, local stale-cursor recovery, and continued retry access beyond
  the first failed-publication page;
- worker-entrypoint tests proving copy, lifecycle, and runtime-canary processes
  verify global state, Serve, and Managed Jobs before health construction and
  fail before health or provider work when any central schema is stale;
- exception-envelope tests proving unknown and known non-exception types share
  one value-free sanitized result, forward-version ordinary attributes preserve
  a known SkyPilot exception type, unsettable attributes cannot replace the
  original error, a duplicate `attributes.args` cannot replace canonical wire
  state, a shadowed `add_note` cannot raise or reflect its value, and every
  currently defined SkyPilot exception class has an exact serialize/deserialize
  round trip for type, arguments, message, notes,
  restorable ordinary state, and the real raised-inside-except context slot;
  unsafe nested types use the safe wrapper, while cycles, malformed nested
  envelopes, and chains beyond eight levels end in the fixed sanitized error;
- Dashboard A-to-B workspace-switch tests with workspace A already rendered,
  proving both pending and failed capability replacement immediately remove A's
  data, mutation controls, open dialogs, retries, and hidden errors, plus
  rejected and `false` route-navigation tests proving the unchanged workspace
  reloads instead of remaining blank;
- Dashboard artifact-detail route tests proving workspace-only, artifact-only,
  and compound identity changes synchronously remove the previous data,
  collection state, mutation controls, and open dialogs, and that a replacement
  failure cannot restore them;
- managed-runtime architecture tests rejecting ARM64 and unknown EC2
  placements, rejecting missing or non-AMD64 EKS selectors, accepting an
  unknown EKS placement only with its exact qualified AMD64 selector, and
  preserving policy-allowed exact-reference direct fallback, plus fresh-launch
  and replay tests that reject a different operation-tagged EC2 child, consume
  no attestation fields from it, and verify teardown of every observed child,
  plus persistent malformed-tag and terminated-known-plus-malformed schedules
  proving that nonempty unidentifiable inventory cannot terminalize or clear the
  operation owner, while one full clean window can resolve earlier ambiguity;
- profile-history pagination beyond one page plus PostgreSQL plan evidence that
  the newest-first workspace query uses its exact keyset index, and capability
  tests proving it requests only the bounded configured ACTIVE profile set;
- endpoint-level viewer, publisher, and administrator tests proving source-auth
  redaction, profile-history denial, and full publisher/admin projections;
- demand aggregation/tombstone/orphan tests for cluster, job recovery, Serve
  version-target, controller loss, supersede, generation watermark,
  interrupted terminal confirmation, one-shot request-terminal proof preserved
  across the pre-24-hour rotation, INIT-versus-reconciliation absent-row
  serialization, authoritative owner retirement, compaction, and unreachable
  consumer stores, plus query-shape and plan evidence that exact bounded job-task
  and Serve-version tuples do not read unrelated history;
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
Serve migration chain at revision 022, regenerates the Helm schema, restores
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

Restarted final acceptance round 1 at
`dcdd54dd92baadd94bac4e5c49b4f413086ea7a0` returned paired `RESHAPE` verdicts.
Codex found that inventory had neither a heartbeat nor synchronous fences around
STS, ECR, and service-quota calls, and that lifecycle STS could begin after its
location lease was lost. Fable found that a conclusive delete followed by a
transient exact-read failure was collapsed into delete ambiguity and permanently
quarantined. The adjacent audit also found that list absence directly marked a
shard `DRIFTED`, preventing the exact confirmation required by this contract.
This revision therefore keeps inventory authority through atomic attestation,
fences all of its provider calls, fences lifecycle credential acquisition, and
separates durable `READBACK` recovery from genuinely ambiguous `DELETE` intent.

The next focused repair review rejected the inventory finalization path because
it confirmed only the first 100 absent READY locations before publishing live
evidence. This revision adds the durable `inventory_finalizing` phase described
above, a partial PostgreSQL index for exact-confirmation candidates, an atomic
zero-candidate attestation fence, and a 101-location interruption and successor
recovery proof. Bootstrap handoff, candidate evidence, and activation all reject
the between-page finalization state.

The following focused gate split: Fable returned `ACCEPT_REPAIR`, while Codex
rejected caller-side STS checks as weaker than a fence at the actual
`AssumeRole` boundary. The adjacent audit also found that an eviction could move
a nominated READY row to `EVICTING` between exact read and database completion,
then restore it without provider I/O, and that bootstrap ceiling handoff could
reset a listing epoch between continuation claims. This revision moves the
optional fence into the shared AWS credential adapter, pauses unsafe lifecycle
transitions during inventory finalization, and treats the entire durable
inventory phase as busy for bootstrap handoff.

The next focused Codex gate rejected finalization-only lifecycle fencing. A
multi-page provider list can observe a digest before a concurrent eviction
deletes it and releases its reservation, causing the terminal page to compare
pre-delete observations with post-delete accounting. This revision uses the
same durable inventory-active predicate for lifecycle discovery, no-I/O restore,
and capacity-releasing READBACK completion across both listing and finalization.

The focused repair gate at `4c4a0525710a1406c7988f94ad0dc64911284f9b`
returned paired `ACCEPT_REPAIR`, but the restarted full-feature round 1 returned
paired `RESHAPE`. Codex found that copy-shard dispatch, publication inspection,
and inventory scheduling sorted hot global populations with indexes that did not
match their predicates and order. Fable found that bound PENDING publications
could re-enter source inspection and that the pre-24-hour unattached-cluster
delay erased one-shot request-terminal proof, leaking demand and eviction fences
forever. This revision adds the exact generated and persisted queue projections
described above, closes inspection to unbound publications, and preserves
terminal request evidence while only the age gate is pending. The acceptance
streak remains reset until both reviewers accept the complete repaired feature.

The first full-feature probe at
`d230a3ef183639910ce4cfa1dcf18342f87cd9c2` used a contradictory review brief
that promoted the explicit post-v0 builder and non-AWS managed-registry seams
into v0 requirements. Codex therefore returned `RESHAPE` for their intentional
absence, while Fable reviewed the canonical v0 boundary and returned `PURSUE`.
That probe does not count toward the acceptance streak. Fable also reproduced a
real legacy-upgrade edge: pre-v35 Docker references whose registry authority is
canonicalized during unpickle left the raw runtime string inconsistent with the
new selector. This revision stores the canonical reference on migration, avoids
repeating the legacy deprecation warning during internal copies through an
identity-only private copy marker, and corrects the public YAML examples so
workload selection cannot be mistaken for publication. Subsequent gates must
treat this document's V0 and explicit post-v0 sections as the authoritative
release boundary.

Valid full-feature round 1 at
`209eb83c2974901e4e90f56dffe67c59c2e4adc1` returned Codex `RESHAPE` and Fable
`PURSUE`, resetting the acceptance streak. Codex proved that both capabilities
and the profile-history endpoint materialized every durable profile revision,
so repeated qualification could make ordinary Images reads consume memory and
latency linearly with retained history. This revision separates the two read
models: capabilities query only the bounded configured ACTIVE set, while the
operator history endpoint uses an indexed, opaque-cursor keyset page with a
1-through-100 public limit. The response remains additively compatible through
the existing version, items, and optional next-cursor envelope.

Valid full-feature round 2 at
`33697358d134c85bcf08bb438792fb9ebddd0be4` returned Codex `RESHAPE` and Fable
`PURSUE`, resetting the acceptance streak. Codex proved that a canary worker
whose lease had expired but had not yet been replaced could still attach its
synthetic child, acquire provider sessions, launch an EC2 instance without an
idempotency token, or irreversibly fail recoverable work. A successor could then
launch a second paid instance. This revision moves the shared continuous
heartbeat and synchronous fences across the complete canary provider boundary,
requires live database ownership for child and terminal transitions, uses a
stable operation-derived EC2 client token, and makes deadline-expired recovery
teardown-only as specified above.

The first focused repair gate at
`58efc07064ec6d89f7a741057b185dd1b404a4d44d1d474e0048066879c2ea9e`
returned Codex `REJECT_REPAIR` and Fable `ACCEPT_REPAIR`. Codex proved that an
initial intent followed by client-construction failure was treated as an
unverified provider child and could remain reclaimable forever. It also proved
that the generic provider pre-call lease renewal could block across the teardown
deadline, allowing EC2 or Kubernetes create to begin late. This revision
separates current-attempt create possibility from the durable intent and adds
the ordered create fence described above.

Valid full-feature round 2 at
`3abcf1561eb0ab20e4e9b87c7663b84e68822cfb` returned Codex `PURSUE` and Fable
`RESHAPE`, resetting the acceptance streak. Fable proved that failed canonical
reservation reclamation performs an exact `BatchGetImage`, while every
lifecycle grant omitted canonical repositories and the permissions boundary
simultaneously allowed `BatchDeleteImage` on them. Access denial was retried
indefinitely, leaking the canonical reservation. This revision splits lifecycle
read from delete in the role identity policy, permissions boundary, shard
repository policies, and qualification policy. Read covers every fixed managed
repository; delete covers only noncanonical and qualification repositories.
Mocked Terraform assertions prove both the positive regional grants and the
negative canonical delete boundary.

Restarted full-feature round 1 at
`6b852c271ac6573a1ff9fec1b642f2eae53e424c` returned Codex `RESHAPE` and Fable
`PURSUE`, resetting the acceptance streak. Codex proved that either externally
managed target-role input could bypass the module's identity policy and
permissions boundary while its output still attested hashes for unattached
documents. This revision removes both target-role escape hatches. Managed v0
owns the copy and lifecycle roles, their trust, inline policies, and boundaries;
existing deterministic names must be imported into the exact Terraform resource
addresses before convergence. Fable also observed an exact-head AWS adaptor
memory check failure. The public image SDK facade is now lazy, and a clean
subprocess regression test prevents its client and API model graph from loading
during ordinary `import sky`.

The parallel final-acceptance attempt at
`24431c27d9e4201aba906c561715d6d440b1bbbe` was halted with the streak at zero
when an independent Codex round returned `RESHAPE`. It proved that revision 024
treated the presence of all owned table names as an exact preview schema without
checking columns, constraints, generated expressions, indexes, or catalog data.
This revision adds the transaction-local literal `pg_temp` comparison and UUID
singleton proof described above. Real PostgreSQL mutation tests retain every
table name while independently changing a column type, named check constraint,
named index, and generated expression; further cases cover missing and extra
tables, changed cluster-binding columns, and invalid or non-singleton catalog
state. Each case must fail atomically before preview adoption.

The restarted parallel final-acceptance attempt at
`b335624f6fc7d47283c14e787957317e3b4e8fcb` was halted before pairing and the
streak remained zero. One Codex round returned `PURSUE`, while two independent
Codex rounds returned `RESHAPE`; the pending Fable processes were stopped once
the blockers were confirmed. The first blocker allowed a digest-pinned source,
bearer realm, redirect, or rebound hostname to reach private network peers and
could forward basic credentials across authorities. The second found that a hot
artifact catalog summary scanned all child rows and that readiness materialized
all shard groups followed by four database round trips per group. This revision
adds the connect-time public-peer and credential-confinement contract, bounded
source bodies, fixed ten-row catalog child samples with truncation markers, and
a single-statement readiness projection over at most 100 preselected complete
target groups. Hot-artifact index plans, fixed statement counts, many-group
truncation, and UI lower-bound behavior are executable acceptance proofs before
the next paired round starts.

Restarted paired final-acceptance round 1 at
`3b948a565ef0da00c3a7cc5c27f1a872ccc0e471` returned Codex `RESHAPE` and Fable
`PURSUE`, resetting the streak. Codex found that the empty-schema migration
exception was proved on a different connection from Alembic, the migration Job
inherited API-only inputs and allowed Pod field collisions, Python could classify
multicast peers as globally routable, Serve returned every matching replica row,
and profile staging and activation locked all retained revisions. Exact-head CI
also exposed three stale Serve migration fixtures. This revision replaces the
general empty-schema exception with explicit isolated `bootstrap` intent,
isolates and validates every rendered database consumer, rejects multicast,
uses bounded `VALUES` plus indexed `EXISTS` Serve probes, serializes profile
mutation with per-profile advisory locks and indexed scalar reads, and advances
the migration fixtures through the current Serve revision. The acceptance
streak remains zero until the repaired immutable head passes CI and both
reviewers accept it.

Restarted paired final-acceptance round 1 at
`8628e97db3fc797ab2c577374364c37a1a4f511e` returned Codex `RESHAPE` and Fable
`PURSUE`, resetting the streak. Codex proved that the Helm Job migrated only
global state while Serve and managed jobs still executed DDL from ordinary
processes; workspace readers received source-binding and infrastructure
attestation metadata; profile staging used an unbounded retained-history join;
terminal operation compaction lacked a reverse publication index; long Helm
fullnames collapsed distinct source-secret RBAC objects; and runtime silently
replaced a missing catalog authority. This revision gives all central schemas
one migration-mode contract, applies capability-sensitive read projections and
an administrator profile-history gate, introduces an atomic permanent profile
custody marker, makes publication audit links nullable with indexed
`ON DELETE SET NULL` compaction, hashes length-safe RBAC names, and makes the
catalog singleton migration-owned and runtime fail-closed. The acceptance
streak remains zero until both reviewers accept one immutable repaired head.

Restarted paired final-acceptance round 1 at
`4b2ec71d408a0254587a3024cf2cde4fae6e8978` returned Codex `RESHAPE` and Fable
`PURSUE`, resetting the streak. Codex proved that result finalizers compared
lease expiry against application time sampled before blocking locks, generic
operation polling exposed administrator-only canary payloads to workspace
readers, and the generated EC2 canary role could not authorize the worker's
instance-only tag request. Exact-head CI also found one YAPF mismatch and one
stale Jobs migration mock. Fable additionally found direct-mode managed-only
selectors cycling candidates before their eventual INIT rejection, an
inconsistent SQLite direct-ref path, and a Dashboard label that rendered a
redacted binding as public. This revision moves every result fence into the
post-lock SQL mutation with `clock_timestamp()`, filters operation kinds before
lookup and allowlists result projections, makes EC2 canary networking and
tag-on-create authority explicit, rejects managed-only selectors before demand
lookup, treats redacted bindings as absent, and repairs the exact-head gates.
The acceptance streak remains zero until both reviewers accept the same new
immutable head three consecutive times.

The next paired round at
`a19d9d015a20a2b5cf71b1a544730135d3066150` returned Fable `PURSUE` and Codex
`RESHAPE`. Codex found no managed-image defect and made the exact-head random
optimizer DAG failure its sole blocker; the same test passed locally. During
that review, `origin/improvements` advanced to `7815fe09f0`, independently
invalidating the reviewed base and resetting the streak. This revision merges
that base, revalidates its Dashboard refresh-ownership change, preserves an
expired canary with a persisted child when a worker cannot decode a future
contract shape, and makes guest-initiated EC2 canary shutdown terminate rather
than stop the instance. The full managed-image suites and the random optimizer
DAG test pass locally. The acceptance streak remains zero until both reviewers
accept one immutable current-base head three consecutive times.

Restarted paired final-acceptance round 1 at
`6c4129ce1157cf7b22ba924242329a9930001e95` returned Fable `PURSUE` and Codex
`RESHAPE`, resetting the streak. Codex reproduced two PostgreSQL stale-clock
races: the production canary worker passed a timestamp sampled before its row
lock into terminal failure, and profile activation reused a timestamp sampled
before its transaction advisory lock, allowing an attestation to expire while
the transaction waited. This revision makes one locked canary transaction choose
ordinary failure versus verified timeout with database time, makes activation
acquire its bounded lock set before its final database-time freshness and budget
validation, and applies the same epoch, token, live-lease, and database-clock
fence to inventory abandonment. Blocking PostgreSQL regressions exercise the
production caller paths. The acceptance streak remains zero until both reviewers
accept one immutable current-base head three consecutive times.

Restarted paired final-acceptance round 1 at
`49698aeb766f5e56e497ccea2268942264fd641a` returned Fable `PURSUE` and Codex
`RESHAPE`, resetting the streak. Codex proved that the lifecycle scheduler still
passed its pre-lock application timestamp into reconciliation and therefore
through both nominally post-lock activation clock reads. It also proved that the
PROFILE_CANARY claim OR-query had no index-bounded pending branch and globally
sorted retained unrelated operations, contradicting the million-row queue
contract. This revision removes timestamp authority at both production caller
boundaries, adds a blocking regression through lifecycle reconciliation, and
replaces the split canary predicate with one generated due-time projection and
exact partial index. Migration preview adoption upgrades the former projection-
less operation table explicitly, and large-population plan coverage proves both
pending and expired-running canaries stay index-bounded. The acceptance streak
remains zero until both reviewers accept one immutable current-base head three
consecutive times.

Restarted paired final-acceptance round 1 at
`cdf7139377afdac40e6cd1a10fbf151bf2b50176` returned Fable `PURSUE` and Codex
`RESHAPE`, resetting the streak. Codex grouped the remaining blockers under one
clock-authority defect: copy and eviction claims could steal live leases when a
worker clock was fast; shared provider grants and throttles mixed database epochs
with application wall time; and consumer retirement reused scheduler time for
the one-hour and 24-hour safety fences. This revision makes shared claims,
retention, retries, grants, throttles, heartbeats, terminal observation, and
compaction database-authoritative after their lock sets, while local grant and
housekeeping deadlines use monotonic time. Blocking PostgreSQL and skew
regressions cover the production paths. During repair, `origin/improvements`
advanced to `3e036aa341`; integrating it linearized the new SkyServe
response-time-history migration as revision 024 after this feature's Serve
workspace and replica-lookup revisions 022 and 023. The real-PostgreSQL
migration-chain test now proves the revision-023 index, revision-024 response
history table, and final declared Serve head together. The acceptance streak
remains zero until both reviewers accept one immutable current-base head three
consecutive times.

Restarted paired final-acceptance round 1 at
`0c1256e78f037c3df6d28bd315c8a97369aac11c` returned Codex `RESHAPE` and Fable
`RESHAPE`, resetting the streak. The paired review proved four cross-component
blockers: new runtime demand admission trusted host time instead of validating
the exact locked proof at its persisted creation epoch; EC2 and EKS canary
creates compared a durable deadline to host wall time; a shared multi-region EKS
binding could make the wrong registry target eligible; and `origin/improvements`
had independently assigned Serve revisions 022 through 024, colliding with this
feature's deployed preview lineage. This revision adds the database-authorized
demand and provider-create fences, strict observed EC2/EKS identity matching,
target-region EKS ARN resolution, and the 025 convergence plus 026 index chain
described above. Real PostgreSQL boundary tests prove stale new admission rolls
back without durable residue while exact replay survives, and provider tests
prove expired database authorization cannot reach either create API despite a
slow host clock. During repair, `origin/improvements` advanced again to
`fd3ee5d7c3d9814248027a3da83c23a41360e647`; integrating that exact base also
invalidated the earlier review identity. The acceptance streak remains zero
until both reviewers accept one immutable current-base head three consecutive
times.

The paired review at `8283818d8f7cd0740b6bc248564d8274068fcabc`
returned Codex `RESHAPE` and Fable `PURSUE`, so the acceptance streak remained
zero. Codex found that qualification evidence still retained producer wall
time, v0 runtime qualification was not structurally bound to AMD64 placements,
and Dashboard cursor histories grew once per forward page despite the bounded-
memory contract. This revision makes the shared attestation transaction replace
producer observation time with its locked PostgreSQL epoch, uses PostgreSQL time
for automatic scheduling and activation preflight, requires exact AMD64 EC2 or
selector-bound EKS managed placements, and gives every image pager a shared
20-entry cursor-history window with an absolute page number and First-page
escape. During repair, `origin/improvements` advanced to `252c30d4e4`; that base
was integrated before implementation and independently invalidated the reviewed
identity. The acceptance streak remains zero until both reviewers accept one
immutable current-base head three consecutive times.

Paired final-acceptance round 1 at
`53378acf19284a66c2ba94e3ebf8fb5e88040c69` returned Codex `RESHAPE` and Fable
`PURSUE`, so the acceptance streak remained zero. Both reviewers independently
proved the PostgreSQL-owned stored observation time, AMD64-bound managed runtime,
and bounded cursor-history repairs. Codex additionally reproduced three
cross-component gaps: copy-role qualification still compared database-stamped
proofs and inventory epochs to worker wall time; copy and runtime-canary
entrypoints could advertise health without eagerly verifying the Serve and
Managed Jobs schemas; and an already-rendered workspace capability object
remained actionable while replacement workspace capabilities were pending or
failed. This revision extends the database-clock and profile-lock contract to
all qualification due checks and attestation writers, makes all three worker
entrypoints verify every central schema before health, and request-scopes plus
synchronously invalidates all Dashboard workspace state. The acceptance streak
remains zero until both reviewers accept one immutable current-base head three
consecutive times.

Paired final-acceptance round 1 at
`ade35661185121fb02ac465b4230d6c40d84de09` returned Codex `RESHAPE`; Fable
exhausted its usage quota after independently verifying the immutable identity,
reading the complete design, and reproducing both backend suite counts, but
before issuing a verdict. Codex re-proved the database-clock, profile-lock,
worker-schema, AMD64 runtime, migration, provider-boundary, bounded-work, Helm,
and deployment repairs. It reproduced two remaining Dashboard gaps:
artifact-detail data and dialogs were generation-fenced but not bound to the
current compound route identity, and Images cleared the current workspace before
an unhandled rejected or `false` route replacement, leaving no dependency
change to restart capabilities. This revision extends request scoping to the
detail route and treats workspace navigation as a recoverable transaction. The
acceptance streak remains zero.

Codex final-acceptance round 1 at
`294b03e7de41800c5d84d4961fe0d5843824e68a` returned `RESHAPE`; Fable could
not start because its zero-token quota probe returned HTTP 429. Codex re-proved
all backend, migration, provider, architecture, bounded-work, Helm, Terraform,
and Dashboard gates, then reproduced two remaining defects. The nested
stale-cursor fallback wrote its notice before checking the compound route
scope, so a late A fallback could annotate artifact B. Managed Jobs revision
025 also accepted any same-name identity index, allowing Alembic to stamp a
malformed or partial production shape. This revision fences every asynchronous
detail state write before mutation and gives both new companion migration heads
exact PostgreSQL index-shape validation, invalid-residue recovery, collision
tests, and production-plan evidence. The acceptance streak remains zero.

Three independent Codex final-acceptance rounds at
`53430099fe6d5dac8bd4bf0ea9e455a6084de31f` returned `RESHAPE`; Fable still
could not start because its zero-token quota probe returned HTTP 429. All three
rounds independently re-proved the Dashboard stale-cursor fence and exact
companion-migration index contracts. Round 1 found that expired runtime
qualification could win metadata ranking and then bypass the saved
managed-preferred direct fallback at locked demand admission. Rounds 2 and 3
both found the missing artifact-demand history index and the unbounded catalog
distribution, target, and location-state facets. Round 3 additionally proved
that AWS ARM64 exact refs consulted managed demand and profile state before
being classified as direct. Exact-head CI exposed an adjacent Python 3.14
compatibility change that serializes exception notes into `__dict__`; old
deserialization treated those notes as constructor keywords. This revision
classifies unsupported exact refs before managed state, checks new-demand proof
age at both metadata and locked admission with typed fallback, adds and validates
the exact artifact-demand index, removes the three unbounded v0 facets, and
round-trips exception notes outside constructor kwargs. The acceptance streak
remains zero until both reviewers accept one immutable repaired head.

Codex final-acceptance round 1 at
`8b1280a6a68a8852ff9675c8a0b67128109c75d5` returned `RESHAPE`; Fable could
not start because its zero-token quota probe returned HTTP 429. Codex re-proved
the complete PostgreSQL, compatibility, Serve, Jobs, Dashboard, Helm, and
Terraform gates, then reproduced two remaining admission defects. Generic
Kubernetes exact refs reached profile parsing in both optimizer and final
backend wrappers before the direct-compatibility guard, so a missing,
disallowed, or malformed profile could reject a runnable direct image. EC2
canaries also persisted the configured AMD64 platform without observing the
launched instance architecture, allowing an ARM64 instance and AMI tuple to
qualify as `linux/amd64`. The review additionally found that malformed exception
envelopes could raise secondary decoder errors. This revision centralizes
placement classification with exact-ref generic-Kubernetes fallback, requires
observed `x86_64` EC2 canary evidence at qualification and runtime matching, and
makes exception decoding total over untrusted envelope shapes. The acceptance
streak remains zero.

Codex final-acceptance round 1 at
`ddf055c7ff84ee1d7452c427b48bc09230dfc534` returned `RESHAPE`; Fable could
not start because its zero-token quota probe returned HTTP 429. Codex re-proved
the shared placement, EC2 architecture, exception-envelope, PostgreSQL,
provider, Dashboard, Helm, and bounded-work contracts, then found one remaining
qualification identity splice. An operation-tag query could return one different
instance from the launched or replay-discovered child. Architecture, instance
profile, and state would come from that instance while the pull marker and
persisted child ID came from the original child, allowing a mixed authorization
proof. A production-path probe also showed that an out-of-schema null
`allowed_profiles` value raised `TypeError`; deployment schema validation made
that nonblocking in the final verdict, but it violated the classifier's
independent totality contract. This revision binds every EC2 evidence field and
tag to one exact child ID, retains all observed IDs through verified teardown,
validates workspace policy collection shapes before normalization, and preserves
the direct fallback through downstream locality ranking. The acceptance streak
remains zero.

Codex final-acceptance round 1 at
`8f1d516a7b03b2191f3a94b82b241a1e7a269526` returned `RESHAPE`; Fable again
could not start because its exact-model request returned HTTP 429 with zero
tokens. Codex re-proved the immutable head and newest repair paths, then found
two remaining boundary failures. Explicit `container_images: null` still became
the default policy and could replace the lifecycle worker's last valid retention
opt-out, while direct model construction could raise incidental type errors.
Separately, EC2 launch confirmation preceded child-ID validation, so a malformed
successful `RunInstances` response could skip the ambiguous-launch settling
window and strand a paid child during tag-visibility delay. This revision makes
missing versus explicit-null policy state unambiguous, validates model
collections before shape-dependent operations, and treats every create without
a retained concrete instance ID as ambiguous until replay or bounded verified
absence. The acceptance streak remains zero.

Codex final-acceptance round 1 at
`b192aa8579bf6e1659f4d6450145dad61ab3690f` returned `RESHAPE`; Fable again
could not start because its exact-model request returned HTTP 429 with zero
tokens. Codex re-proved the policy-custody, direct-fallback, PostgreSQL,
migration, provider, UI, Helm, and Terraform contracts and found one remaining
EC2 custody race. Ambiguity settling returned as soon as the first retained
child reached `terminated`, so another operation-tagged child becoming visible
on the next poll could escape discovery before `run_canary()` terminalized the
ordinary qualification failure. This revision requires the entire bounded
settling window after an ambiguous create, accumulates every late child, and
preserves successor custody whenever the final poll cannot prove the complete
retained set terminated. The acceptance streak remains zero.

Codex final-acceptance round 1 at
`16ea1a2ddb6c569e9d0c3e7f8d4fc0fa901742a0` returned `RESHAPE`; Fable again
could not start because its exact-model request returned HTTP 429 with zero
tokens. Codex independently passed the repaired EC2 custody schedules and found
no additional functional blocker, but proved that the 60-second local timeout
for the 100,000-row artifact-demand fixture remained active for the sparse and
dense `EXPLAIN ANALYZE` statements in the same transaction. The exact index and
query shape were correct, but the documented 15-second bounded-query acceptance
proof was not. This revision commits the relaxed fixture transaction before
opening a new default-timeout transaction, asserts the restored 15-second
ceiling, and only then executes both plan proofs. The acceptance streak remains
zero.

Codex final-acceptance round 1 at
`1c3d60891e263ab2bb8db5f56bab05d11e94baa5` returned `RESHAPE`; Fable again
could not start because its exact-model request returned HTTP 429 with zero
tokens. Codex re-proved the PostgreSQL bounded-plan repair and the complete
backend, Dashboard, Helm, Terraform-validation, and 26-check GitHub gates, then
reproduced one remaining EC2 custody defect. Teardown silently discarded
nonempty operation-tag records with a missing or non-string `InstanceId`, so a
persistent malformed child, or a malformed child beside one known terminated
child, could be certified as absence and clear the durable owner. This revision
distinguishes exact empty inventory from unidentifiable child evidence, carries
tag-read ambiguity through teardown, requires a complete clean settling window
after the latest ambiguity, and preserves the RUNNING operation whenever that
proof is incomplete. During repair, `origin/improvements` advanced to
`1bce497a7b`; integrating it preserves the new Serve autoscaling contracts and
reconciles built-in exception attribute restoration with this feature's
constructor-safe note serialization and total malformed-envelope decoder. The
acceptance streak remains zero.

Codex final-acceptance round 1 at
`d01d7e3cefeec17778ce3d0eb056555aa538c4c8` returned `PURSUE`; Fable again
could not start because its exact-model request returned HTTP 429 with zero
tokens, so no paired acceptance was recorded. Codex independently re-proved
the malformed EC2 inventory repair, all 155 PostgreSQL tests, all 83 worker
tests, the combined 352-test feature matrix, and the complete 26-check GitHub
rollup without finding a blocker. During that round, `origin/improvements`
advanced to `1f34d599a1`, independently invalidating the reviewed base. This
revision integrates that base's GCP capacity classification, SQLite contention
retry, and landed exception-compatibility fixes while retaining strict envelope
shape validation, constructor-safe note serialization, tolerant attribute
restoration, and sanitized fallbacks. The acceptance streak remains zero.

Codex final-acceptance round 1 at
`e6d46103b872433ad504517ba11db76f8fa5c967` returned `RESHAPE`; Fable again
could not start because its exact `claude-fable-5` request reached the account
limit before using any token. Codex re-proved the latest EC2 custody repair and
the complete 26-check GitHub rollup, then reproduced two independent boundary
defects. A stop signal arriving during a worker heartbeat could still begin
maintenance, claim, and submit new work; the canary Deployment's 30-second
termination grace could then kill a paid canary before its bounded runtime and
teardown completed. Separately, unknown exception types reflected untrusted type
and message values, while a forward-version ordinary attribute could replace a
known SkyPilot exception with a generic fallback. This revision makes shutdown
a no-new-work fence for all three workers, actively cancels canary runtime waits
into uncancelled custody teardown, enforces a 600-second cleanup grace, sanitizes
unknown exception identities, and partitions known constructor fields from
restorable forward-version attributes. The acceptance streak remains zero.

PR #368 merged as `e712186072fede57d08841e0ac7e0d184b174c18` before the
round-one repairs above were committed. The corrections therefore continue as
a focused follow-up from `966a8d014ca81610f3b24ceb35e221384b0aab2b`; they do
not rewrite the merge or treat the merged state as reviewed acceptance.

Codex final-acceptance round 1 at
`71c6dca4f5b91facca3a8696fea4d0692003c249` returned `RESHAPE`; the exact
`claude-fable-5` plan-mode request again returned HTTP 429 before using any
token, so no paired acceptance was recorded. Codex reproduced five blocking
boundaries on the otherwise green 26-check head: EC2 teardown could start calls
after its deadline, shutdown during verified cleanup could still terminalize a
canary, copy could begin later internal claims after observing shutdown, legacy
Helm reuse values omitted required grace keys, and several current SkyPilot
exceptions did not round-trip their canonical state. The adjacent audit also
made STS retry and timeout bounds, configuration-to-ingestion shutdown, and
lifecycle maintenance substep shutdown explicit. This revision closes those
boundaries as one batch. The acceptance streak remains zero.

Codex final-acceptance round 1 at
`b1ebe1a9594cc7a696afcfaf5675ec1de2e43092` returned `RESHAPE`; the exact
`claude-fable-5` max-effort plan-mode request reached its weekly limit before
using any token, so no paired acceptance was recorded. Codex independently
re-ran the 352-test feature matrix, 108 worker tests, real PostgreSQL drain proof,
all 191 Helm tests, and the complete 26-check GitHub rollup, then reproduced
three compound boundary defects. An EC2 cleanup heartbeat could cross the shared
deadline before an unfenced raw AWS call, ordinary EKS and EC2 qualification
could acquire a later authority or begin a later provider read after drain, and
synchronous manifest, publication-fanout, and terminal-consumer loops could
start later independent transactions after stop. This revision moves deadline
and drain checks inside the provider fence and threads the stop predicate through
every independently startable synchronous item. The acceptance streak remains
zero.

Codex final-acceptance round 1 at
`15db7837245a07a632b1587ba8a0b0fbe649c58c` returned `RESHAPE`; the exact
`claude-fable-5` max-effort plan-mode request again reached its seven-day limit
before using any token, so no paired acceptance was recorded. Codex verified
the repaired canary deadline fence, compound maintenance stops, all 191 Helm
tests, and the complete 26-check GitHub rollup, then reproduced five remaining
cross-boundary defects. Per-target copy qualification dropped the live stop
predicate inside provider and database work; deferred IRSA credential refresh
could use botocore's longer retry defaults; kubeconfig exec acquisition and
transparent refresh were outside EKS drain and cleanup fences; the real
`BaseException.__context__` slot was not serialized; and the Helm template
silently treated explicit zero as an omitted canary grace. This revision closes
those boundaries together with one bounded credential design rather than adding
caller-only checks. The acceptance streak remains zero.

Codex final-acceptance round 1 at
`9f722c8ecd2f7e06cb1775dc2e84c211d944b6ab` returned `RESHAPE`; the exact
`claude-fable-5` max-effort plan-mode request returned HTTP 429 before using any
token, so no paired acceptance was recorded. Codex independently confirmed the
complete 26-check GitHub rollup and prior five-boundary repair, then reproduced
two P1 gaps: a kubeconfig exec descendant could retain a pipe beyond the nominal
timeout while unbounded stdout or stderr exhausted memory, and copy or lifecycle
could submit a claimed operation when shutdown arrived after the caller's final
check. The adjacent exact-tree audit found that in-cluster projected tokens lost
rotation when transparent refresh was removed, EC2 cleanup could reuse an
expired one-hour runtime session, and malformed exception attributes could
replace canonical `args` or crash a shadowed `add_note` call. This revision
defines one stop-aware executor admission point, bounded process-group exec,
explicit in-cluster rotation, fresh deadline-fenced cleanup credentials, and a
total canonical exception boundary as one repair batch. The acceptance streak
remains zero.

Codex final-acceptance round 2 at
`0d139ff186e1558fedb2185e32eaaa83b4b738b8` was stopped before verdict after its
adversarial malformed-output probe exposed raw credential bytes retained by the
JSON decoder in both `__cause__` and `__context__`, despite the top-level error
message being fixed. The exact `claude-fable-5` max-effort plan-mode request
returned HTTP 429 with zero input or output tokens, so no paired acceptance was
recorded. This revision makes malformed output leave the exception handler
before raising the fixed error and requires an empty exception chain as part of
the value-free credential boundary. The acceptance streak remains zero.

Codex final-acceptance round 3 at
`6b50f1d10b87ddb32d4a8ffbaa21105881a6b971` returned `RESHAPE`; the exact
`claude-fable-5` max-effort plan-mode request again returned HTTP 429 with zero
input or output tokens, so no paired acceptance was recorded. Codex confirmed
all 26 exact-head checks and the empty decoder cause/context repair, then
reproduced one P1 and two P2 residual boundaries. Credential bytes, decoded
status, loader state, and a losing refresh exception remained reachable through
public traceback locals or hidden context; untrusted API-version and expiration
values could also enter public or serialized errors. Pipe-reader failures were
treated as clean EOF, allowing a valid prefix to be accepted, and projected
token rotation used adjustable wall time instead of a monotonic deadline. This
revision defines the single credential-isolation boundary, fail-closed reader
state, exact response validation, and monotonic refresh contract above. The
acceptance streak remains zero.

Codex precommit review round 4 reproduced three remaining boundary failures:
an installed exec credential remained reachable through the later wrapper
exception's `self` object graph, reader exceptions outside `OSError` and
`ValueError` could accept a valid buffered prefix and reach
`threading.excepthook`, and loader or fence time could consume a credential's
API-call headroom before the raw call. Its verdict was `RESHAPE`. This revision
adds credential-scrubbing invalidation on every exceptional provider-wrapper
exit, detaches provider dependency tracebacks, makes reader failures total and
fail closed, and revalidates monotonic headroom at client admission, refresh,
and immediately before the raw method. Tests now use bounded cycle-safe
object-graph traversal rather than shallow local-value representations. This
was a precommit review of a moving worktree, so it is repair evidence rather
than an exact-head acceptance; the acceptance streak remains zero.

Codex exact-head review round 5 at
`9ffa23a6e4ce560acee8e04404aceb0876ce7fa8` was stopped without a verdict when
the GitHub async-lifecycle check rejected the deliberate synchronous
`BaseException` isolation boundaries. The exact `claude-fable-5` max-effort
plan-mode request returned HTTP 429 with zero input or output tokens, so it was
neither acceptance nor rejection. Round 6 at
`2294fda8b3566270702def6453d8823532d10cd7` confirmed the immutable head facts
and then reproduced one residual boundary before it was stopped: the generic
canary wrapper ran its own initial provider fence before delegating to
`ProviderFencedCoreApi`, so the escaping control exception's traceback could
still reach the installed credential through `_FencedClient.self`. This
revision delegates that initial Kubernetes fence to the credential-owning inner
wrapper, which scrubs and invalidates before propagation, and adds an
object-graph regression for the exact pre-fence schedule. The acceptance streak
remains zero.

Codex exact-head review round 7 at
`1d62e047b831d124656759f9bf33788498c8275d` was stopped without a verdict after
its closure-aware reproduction confirmed the initial lease, drain, and deadline
repair, then found an adjacent residual. A validation failure after successful
Kubernetes client acquisition, reproduced with
`CANARY_PRINCIPAL_UNVERIFIED`, escaped `_run_eks_canary` while that outer
traceback frame still owned the live installed credential. The client had not
been invalidated or closed. This revision defines an explicit outer EKS client
lifetime that always scrubs after required teardown and before any result or
exception leaves, with closure-aware regressions for both successful and
exceptional exits. The acceptance streak remains zero.

Codex exact-head review round 8 at
`89c0c48f0c50b2f684b15bd102d3acecf88c0777` was stopped without a verdict by
the platform safety filter after it independently reproduced one adjacent
exception-precedence leak. When provider work failed after EKS pod creation and
teardown then failed, `CANARY_TEARDOWN_FAILED` retained the losing provider
error as implicit context. The serialized envelope therefore retained that
error's response body and request-local authorization header even though the
installed Kubernetes credential was correctly scrubbed and closed. This
revision puts both EC2 and EKS canary lifetimes behind one detached cleanup
winner contract. Successful cleanup preserves the ordinary provider exception;
teardown failure, cleanup lease loss, and cleanup-time drain discard the losing
provider graph before raising their deterministic winner. A typed internal
teardown signal remains distinct through durable terminalization, preventing an
unrelated provider `ValueError` with the same message from preserving the child
as an unverified teardown. Exact object-graph, serialized-envelope, and
end-to-end terminalization regressions cover the EKS precedence and
message-collision outcomes plus the equivalent EC2 teardown-failure schedule.
Round 10 at `a2eaa6410939ce32b4dec6c043c7919370e093f8` was intentionally
stopped before a verdict when this remaining downstream string discriminator
was found. The acceptance streak remains zero.
Round 11 at `0ec9c51c4775748402ef0f59093553169b4b08eb` was intentionally
stopped before a verdict when rollout analysis found that preserving the old
binding identity while changing its absent-field default from on-demand to Spot
could replay one existing EC2 client token with different launch parameters.
The revised three-state snapshot contract preserves old missing-field
on-demand replay, while every new profile projects its boolean market policy
and therefore requires an identity-changing revision. The acceptance streak
remains zero.
Before the repaired-head acceptance runs, `origin/improvements` advanced
through `0758b9b32e5828a0befd6cde1fe09dce62e6f605` and
`742315b8f847b2c414b2a25cf25c30863feb9c82`, preserving the managed-job
cancellation attribution, transient-INIT recovery, and recovery signal-parent
contracts. It then advanced to
`cd52f3e099dad3e63741cc6d8317eceb342d16da`, whose inherited change uses slim
Serve status snapshots for control writes. The exact round-13 candidate had
that commit as both base and merge base, a 39-file base-relative diff, and a
24-test inherited Serve status suite. No semantic overlap or conflict survived
review. All gates are rerun against each actual merge candidate.

Codex exact-head review round 12 at
`795b0e561d3fc7f2af8b44bb7dd21f3671db5826` returned `RESHAPE`; the exact
`claude-fable-5` max-effort request returned HTTP 429 before inference, so it
was neither acceptance nor rejection. Codex confirmed the three-state market
identity contract and all 26 GitHub checks, then independently reproduced two
Spot activation blockers. A one-time request was not tagged, discovered, or
cancelled, so two lost accepted `RunInstances` responses could be followed by
five minutes of empty instance inventory, false teardown success, and a later
orphan launch. Separately, the generated fresh-account path did not provision
the mandatory `AWSServiceRoleForEC2Spot` or customer-managed AMI KMS grant.
This revision makes the request a first-class custody child, proves both request
and instance teardown across cancellation-versus-fulfillment, adds one
account-scoped Spot bootstrap and regional conditional KMS grants, and keeps the
legacy on-demand replay contract unchanged. The acceptance streak remains zero
until both reviewers accept one immutable repaired head.

Codex exact-head review round 13 at
`7623fb052641734faa57ad7668dbbee5d16af437` returned `RESHAPE`; the separate
`claude-fable-5` max-effort request again returned HTTP 429 with zero tokens and
provided no verdict. Codex verified the exact 39-file candidate, all 26 GitHub
checks, the 671-test feature matrix, PostgreSQL, managed-job, inherited Serve,
Terraform, Helm, and static-analysis gates, then reproduced three blockers in
one transitive pass. First, an immediate post-cancel read could report a
terminal Spot request before eventual consistency exposed its running
`InstanceId`, allowing false teardown success. Second, the target module
combined EC2 runtime and EKS node roles under one `iam:PassRole` grant and did
not pin `ec2:InstanceProfile`. Third, this integration history still described
the prior base and 26-file candidate. This revision gives every terminal
unassociated request its own restartable full settling window, splits EC2
passable identities from EKS inspect-only profiles, pins every launch to the
exact EC2 profile set, and synchronizes this canonical design. It also makes the
partition-neutral account import and destruction protection explicit. The
acceptance streak remains zero.

Codex exact-head review round 14 at
`cc024e434dd1662d2302d9505ead48b50ac38c6c` returned `RESHAPE`; the separate
`claude-fable-5` max-effort request again returned HTTP 429 with zero tokens and
provided no verdict. Codex verified all 26 GitHub checks, the 674-test feature
matrix, PostgreSQL, managed-job, inherited Serve, Terraform, Helm, and static
analysis, then reproduced four blockers in one transitive pass. First,
`DescribeInstances` did not follow `NextToken`, so a paid child on a later page
could be omitted while cleanup returned success. Second, the 300-second
deadline left only five seconds for roughly 180 provider calls after its 59
fixed sleeps, making the terminal no-child proof permanently fail under normal
latency. Third, exact-ARN Terraform inputs accepted IAM wildcards and policy
variables. Fourth, `iam:PassedToService` hardcoded the standard-partition EC2
principal and broke China targets. This revision paginates and validates the
complete instance inventory, uses a 480-second cleanup budget with an absolute
60-observation cadence, validates every policy-bearing ARN and target scope,
and derives the EC2 service principal from the active partition. The acceptance
streak remains zero.

Codex exact-head review round 15 at
`017de8ce3ab8a617f35ee54f00a6024504b51407` returned `RESHAPE`; the separate
`claude-fable-5` maximum-effort request again returned HTTP 429 with zero tokens
and provided no verdict. Codex re-proved the worker, PostgreSQL, Terraform,
Helm, Dashboard, Serve, managed-job, and static-analysis gates, then demonstrated
that the exact-ARN input boundary still admitted impossible AWS resource
identifiers. Mocked plans accepted overlong IAM terminal names and paths,
arbitrary-length EC2 IDs, a 600-character EKS cluster name, and an invalid KMS
key ID even though those values flowed directly into launch, inspection,
boundary, and grant policies. The intermediate candidate subsequently merged as
`9e1b6ff8d06de533503662c518e2dc64f42c60d9`.

The follow-up audit then expanded beyond the canary target. The worker-identity
module also admitted wildcard target roles, unrelated or malformed OIDC
providers, unbounded Kubernetes subjects, and cross-account permissions
boundaries. The distribution module admitted wildcard trust and repository
principals, overlong target role names, and malformed or cross-scope KMS keys.
The corrective branch now validates all three Terraform boundaries:

- the canary target enforces AWS path and terminal-name ceilings, exact legacy
  or long lowercase EC2 IDs, bounded EKS cluster names, and UUID or `mrk-` KMS
  key identifiers in the target partition and region, including keys owned by a
  shared encrypted AMI's source account. It also fails planning when the
  rendered, minified trust policy exceeds the account's applied quota, which
  defaults to 2,048 characters and may be declared up to AWS's 8,192-character
  adjustable maximum, and rejects optional external IDs outside the exact AWS
  STS length and character contract;
- the worker identity correlates the exact OIDC provider ARN with the canonical
  HTTPS issuer in the active account and partition, bounds Kubernetes subjects,
  rejects malformed percent escapes, and accepts only bounded exact
  same-partition target roles and an optional same-account permissions
  boundary. Each worker target set is capped at 64 roles and an 8,000-character
  serialized-ARN budget below the 10,240-character role inline-policy ceiling;
  and
- the distribution target accepts only bounded exact same-partition worker and
  runtime principal roles, AWS-valid role names, and an exact KMS key in the
  active partition, account, and region. Its copy and lifecycle trust policies
  are checked against the same applied quota before either target role can be
  created, and its shared optional external ID must satisfy the exact AWS STS
  request contract.

Negative mocked plans cover wildcard, variable, malformed, overlong, overflow,
cross-partition, cross-account, cross-region, and invalid external-ID inputs.
Standard, China, and GovCloud plans prove the partition-neutral contract remains
usable. The canary-target, worker-identity, and distribution suites pass 35, 30,
and 25 mocked plans respectively. The acceptance streak remains zero until the
corrective head passes a new exact-head review.

The intermediate merge was published as immutable release `1.1.749`. Its image
digest is
`sha256:e2fdd7401cc4a53a878e1cfc37c329caa4b324ce8b7be259d37714e4b9b4d8f3`,
and tag `v1.1.749` resolves directly to
`9e1b6ff8d06de533503662c518e2dc64f42c60d9`. Production Helm revision 232
completed its revision-scoped PostgreSQL migration Job and converged the API,
copy, lifecycle, and canary Deployments to one Ready replica each on that exact
digest. The API reported version `1.1.749` and the exact merge commit, while
`boltz-l4-fleet` remained `READY` with 18 of 18 replicas. The normal
improvements release lane subsequently advanced production to Helm revision 233
and release `1.1.750` at
`d27c165ebc6edf9d74c2cbdb72ae406ffabe73bf`. That commit changes only a legacy
SSM unit-test expectation on top of the intermediate merge.

The lane then advanced to Helm revision 234 and release `1.1.751`. Its copy
worker deterministically found an existing `(workspace, profile, revision)`
tuple with a different immutable payload, fell through to `INSERT`, and crashed
on `uq_container_image_profile_revision`. The independently committed repair
checks the locked candidate first: an exact `QUALIFYING` or `ACTIVE` replay is
idempotent, while an immutable mismatch or non-operational replay raises a
sanitized `ValueError` before custody lookup or DML. Qualification ingestion
then terminalizes the durable operation as `FAILED`, and the per-file worker
boundary continues to later manifests. Real PostgreSQL tests cover all three
immutable fields in both operational states, durable failure terminalization,
and worker continuation.

Production was stabilized on release `1.1.752` and digest
`sha256:a15552696d49121315e722e92f4bc782526e1a3bab5c5bfac1efdf273264a02d`.
Helm revisions 235 and 236 completed their migration Jobs with the copy worker
disabled in stored release values. The API, lifecycle worker, and canary worker
were Ready on that exact digest. The stranded provider-free
`PROFILE_QUALIFY` operation was moved through the normal application state
transition from `PENDING` to `FAILED` with `QUALIFICATION_FAILED`; the
operation table then contained no `PENDING` or `RUNNING` rows. Helm revision
237 restored the copy worker on `1.1.752`; its failed-operation replay remained
stable and did not repeat the unique-key crash.

The ordinary release lane advanced to `1.1.753`. Its first upgrade attempt,
Helm revision 238, failed after partially updating the workers because two
stored indexed `extraInitContainers[index].image` values replaced the complete
strategic-merge list entries with image-only maps. Kubernetes correctly rejected
those entries because they lacked the required `name` merge key. Revision 239
supplied the complete init-container objects and converged `1.1.753`. Future
upgrades must set only the top-level
`apiService.image`, or replace each list item in full; indexed list-field
overrides are not safe with `--reuse-values`.

The immutable conflict repair merged in PR 904 as
`ecb41022a9a9ab2b805cb4560a1f2febea33f95d` and was published as release
`1.1.754`. The image digest is
`sha256:ee78c0b3e4e2b90bfb64462e9ea2cf1b959a7245b01b3584785da4d609d99fe4`;
the chart digest is
`sha256:c46a2f26cb42f3700925ebec4d83185480831b78eda4455e39969f50f0fdbb87`.
Production Helm revision 240 completed its revision-scoped migration Job and
converged the API, copy, lifecycle, and canary Deployments to one Ready replica
each on that exact image digest with zero restarts. The API reports version
`1.1.754` and the exact merge commit. Multiple readiness refreshes show fresh,
idle `1.1.754` worker heartbeats and zero queued, in-flight, failed, or
quarantined image work. The repaired provider-free operation remains the same
terminal `FAILED` record with no lease or child.

Qualification then completed for profile revision 2, whose EC2 runtime binding
uses the new default `canary_use_spot: true`. Revision 2 is now `ACTIVE` with
all required infrastructure, copy, lifecycle, and runtime attestations Ready;
revision 1 is `RETIRED`. Existing services and managed jobs retain their own
capacity policies. Post-rollout verification kept `boltz-l4-fleet` `READY` at
18 of 18 replicas, and its load-balancer health and liveness endpoints both
returned HTTP 200.

The normal improvements lane subsequently published release `1.1.755` at
`c21d7e5e5011605c471568a9ad6598b219dec779`. Its image digest is
`sha256:1d43416933904f694474a644be5bfaddc12dd3981dff0c0165ba3ed0e7768c67`,
and its chart digest is
`sha256:6187278300994576df2e71896646f0684145a53d437e3877296dd3e76c50624a`.
Helm revision 241 preserved both complete named init-container objects,
completed its migration Job, and converged the same four Deployments to one
Ready replica each on that exact image digest with zero restarts. The API
reports the exact release and commit; all three image workers remain fresh and
idle, profile revision 2 remains `ACTIVE`, and the repaired qualification
operation remains unchanged.

The rollout deliberately scaled all three old worker Deployments to zero before
the Helm upgrade. This prevented the legacy worker binary from overlapping a
binary that understands `canary_use_spot`. Durable revision 1 omitted that
field and therefore retained the legacy on-demand interpretation until revision
2 qualified. The compute-account role now exercises the bounded Spot request,
tagging, inspection, cancellation, and cleanup contract for new EC2
qualification canaries.

### Live prototype verification, July 23, 2026

The first managed OpenDDE cold launch reached `READY` in 482.65 seconds, versus
434.55 seconds for its paired baseline. This is not a performance improvement
and does not support a faster-deployment claim. Phase logs exposed an
implementation defect before the image pull: the AWS Ray template omitted the
resolved `credential_helper: ecr-login` field. The node therefore installed the
AWS CLI, requested an ECR bearer token, persisted Docker login state, and spent
about 34 seconds in an authentication path that the qualified host contract is
designed to avoid. The image pull itself took about 100.8 seconds, and the
service subsequently staged about 9.9 GB of workload data.

The repair preserves the optional credential-helper field in the rendered AWS
cluster configuration and has a regression test for both helper-backed and
ordinary login configurations. A fresh cold launch is required after deploying
that repair. The remaining image pull, instance bootstrap, and workload-data
staging must be measured separately; this result does not justify moving a
lazy OCI snapshotter or a separate model-data distribution plane into v0.

The repair was subsequently deployed from
`8481a272ee1afea77d9c9d21804a33ab63df9efe`. A fresh direct pull preserved
`credential_helper: ecr-login`, performed no Docker login or ECR token command,
and brought the managed container up after about 102.25 seconds of cold image
pull. A paired Serve comparison used Spot by default with dynamic on-demand
fallback because all `g6.xlarge` Spot zones were initially exhausted. On the
same on-demand `g6.xlarge` fallback shape, source-image provision-to-readiness
was 351.09 seconds and built-image provision-to-readiness was 375.19 seconds.
The built image was about 24.10 seconds slower. Post-cluster readiness was
effectively identical, 140.78 versus 141.29 seconds, because both paths still
staged 10,036,350,627 bytes (9.35 GiB) of workload data.

The credential-helper and concurrent-controller repairs are accepted, but the
builder's 120-second deployment-speed gate failed. V0 remains an ordinary OCI
distribution plane. A node-cache, snapshot, lazy snapshotter, or model-data
locality feature requires a separate design and runtime capability gate rather
than expansion of this PR.

The Boltz prototype release separately resolved to a 4,888,012,862-byte
digest-pinned manifest and passed a fresh L4 container/runtime smoke test.
Its admitted on-demand fallback preserved `credential_helper: ecr-login`,
started the managed container 204.71 seconds after provisioning began, and
passed HTTP readiness at 301.20 seconds. The test did not receive the
production fleet's R2 and payload-encryption secrets, so it proves the managed
pull and executable image boundary but not model readiness or inference.

Before round 15, `origin/improvements` advanced through
`8e37b6dfc27849d09a7a94e323da2c8e5a34de98` to
`cb8e0f125ec27329b2da6c31b08e3fcd38e50031`, then to
`a9eb25b514c9d841c622ad17dbceb9f1e7119548`. The merges bring in the executable
builder prototype, credential-helper preservation, delayed EC2 console-output
proof, managed-job refresh fencing, Serve controller changes, per-sweep YAML
read deduplication, boot-time provider-lookup removal, Kubernetes autodown
reconciliation, and the empty-SSM-target guard. The overlapping canary path
retains the worker-only ambient identity, bounded STS and service clients,
drain-safe provider fences, Spot request custody, 480-second teardown budget,
and exact IAM inputs. It also uses the shared value-free ECR credential-helper
command, strictly decodes both documented console response shapes, waits for
delayed terminal console output inside the original canary deadline, and
applies the corrected per-resource `RunInstances` IAM contexts. Every
exact-head gate and acceptance review is rerun against the resulting merge
commit.
