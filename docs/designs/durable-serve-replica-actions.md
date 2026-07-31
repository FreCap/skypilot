# Durable SkyServe Replica Actions

Status: M0 contract corrected for the shared action-kernel and placement-offer
boundaries at `origin/improvements@7aaa99041065a57c6f733ceed04f025520bac871`;
M1 implementation may not merge until an independent adversarial review
accepts these exact bytes and the reviewed pre-commit content SHA-256 is
recorded in the carrying commit/PR metadata; deployment and removal gates
remain open

Last updated: 2026-07-31

Canonical owner: this file. External plans, pull request descriptions, and
rollout notes must link here instead of restating a divergent contract.

## Summary

SkyPilot's PostgreSQL API request store durably delivers and leases individual
requests, but a SkyServe replica launch or teardown is not yet a stable
database resource across request replacement and controller handoff. The
replica manager currently owns launch and down threads, the replica-to-request
mapping, and cleanup retry clocks in process memory. Recovery reconstructs
intent from persisted replica state and provider observations.

This design adds the first domain adapter to one shared PostgreSQL
resource-action store and dispatcher. A resource action is stable domain
intent; its deterministic correlated API request is one immutable execution
attempt. The generic action row owns runnable state, the database due time,
attempt numbering, backoff, and generic result disposition. A short
`FOR UPDATE SKIP LOCKED` dispatcher transaction materializes a due attempt and
its queue delivery, then releases the action row. The existing API-request
claim/lease/heartbeat remains the sole long-lived external-execution lease.
There is no action lease, second delivery queue, or Serve-specific fetcher.
The shared dispatcher reuses PR #1070's low-level PostgreSQL claim and fencing
mechanics; Serve supplies only domain admission, planning, reduction, and
projection.

For the v1-eligible central-PostgreSQL service set, the migration is complete
only after the durable action is authoritative,
fault injection and live Kubernetes conformance pass, and the process-local
ownership paths in the Removal Map are deleted. Leaving the legacy paths
dormant does not complete the migration.

## Current Behavior and Failure Window

PR #1070 established:

- transactional PostgreSQL request creation and queue delivery;
- `FOR UPDATE SKIP LOCKED` claims;
- database-clock claim leases and heartbeats;
- claim-token and execution-generation write fencing;
- controller leadership generations; and
- ambiguous-outcome handling for non-replayable requests.

SkyServe replica actuation remains controller-process scoped:

1. `_launch_replica()` persists a replica row and constructs a `SafeThread`.
2. The thread calls `sdk.launch()`, then stores the returned request ID only in
   `_replica_to_request_id` and waits for the request.
3. Launch retries and backoff run inside that thread.
4. `_terminate_replica()` constructs a separate down thread and persists a
   coarse `ProcessStatus` before and after thread admission.
5. Failed cleanup retry attempts and `time.monotonic()` deadlines live in
   `_failed_cleanup_retry_attempts` and `_failed_cleanup_retry_at`.

The nested API request survives an API or executor restart. The durable
relationship between that request and the replica action does not. A
controller handoff can therefore lose the request association and backoff
schedule and must infer whether to attach, retry, adopt, cancel, or clean up.

## Goals

- Give each Serve replica launch and committed down one stable logical identity
  across request attempts and controller generations.
- Extend PR #1070 with one shared action store/dispatcher while reusing its
  PostgreSQL request queue, claim lease, heartbeat, cancellation, and
  stale-write fencing for each materialized attempt.
- Make request admission idempotent for one action attempt, including a lost
  HTTP response after the API committed the request.
- Persist attempt number, next reconciliation time, each attempt's provider
  operation identity when available, and a normalized domain outcome.
- Adopt a launch that succeeded before a crash instead of blindly launching
  again.
- Keep teardown fail-closed: deletion is successful only after absence is
  proven, and uncertainty remains visible and retryable indefinitely.
- Let a promoted controller reconcile all nonterminal actions without
  inheriting process-local threads or clocks.
- Make the shared dispatcher, rather than a Serve controller loop, the sole
  owner of the generic due query and deterministic attempt materialization.
- Preserve existing autoscaling, placement, rolling-update, reserved-capacity,
  and load-balancer safety policy.
- Keep every stacked implementation milestone independently testable,
  deployable, and revertible.
- Finish by deleting legacy operation ownership from the eligible authoritative
  manager while retaining only the named, construction-fenced adapters in the
  Removal Map.

## Non-Goals

- A second generic queue, worker pool, action lease, or domain-specific due
  scanner.
- Porting dstack's rolling-update algorithm or cleanup give-up deadline.
- Replacing SkyPilot's provider-specific placement and capacity policy.
- A public fleet or resource-action API.
- Migrating managed jobs, image workers, pool-level scheduling/capacity
  ownership, or storage actions in the first implementation. V1 covers pool
  replica side effects only when the pool is consolidated onto the central
  PostgreSQL Serve manager, where replica intent and an action can share one
  transaction. A non-consolidated pool runs its Serve database in the remote
  jobs controller and cannot atomically write the central action/request
  database; it remains on the explicitly retained legacy adapter and cannot
  enter `shadow` or `authoritative`. Pools still have no inference endpoint or
  load-balancer drain.
- Making provider operation IDs mandatory where the provider has no usable
  idempotency or operation token.
- Adding SQLite support for central resource actions. This is a PostgreSQL-only
  API-server feature.

## Public Contract

There is no new user-facing CLI, SDK, or service YAML field.

For existing APIs:

- `sky serve up`, update, autoscaling, and down retain their current behavior.
- Existing service and replica identifiers remain stable.
- API request lookup continues to show each individual attempt.
- A controller or API handoff may delay reconciliation but must not duplicate a
  logical replica or falsely complete cleanup.
- Existing persisted services are migrated in place; an unrelated update must
  not silently change rollout, load-balancer, placement, or capacity policy.

Internal request correlation is versioned and accepted only for an
authenticated current SkyServe service owner whose hash/lifecycle epoch and
random durable owner token revalidate under lock. PID/IP are diagnostic and API
request-controller leadership is not
planner authority. A public client must not claim or replace another action's
request.

## Core Model

The two durable objects have deliberately different lifetimes and one
hierarchical ownership boundary:

```text
Serve desired state
        |
        v  domain admission / planner / reducer
api_resource_actions
one shared-kernel logical action
kernel_state + next_attempt_at + attempt/effect/result
        |
        | short due/materialization transaction
        v
api_requests / api_request_queue
one deterministic immutable action attempt
the sole execution claim + lease + heartbeat
        |
        v
provider mutation
        |
        v
Serve replica observed-state projection
```

The action table is the generic due-work index, but it is not a second
delivery queue. The dispatcher holds no action claim across request execution.
Candidate discovery is nonlocking; a per-candidate transaction locks the
generic ownership-epoch row before acquiring the action with
`FOR UPDATE SKIP LOCKED`. At most one transaction owns a due row; after it atomically creates or reuses
the deterministic `(action_id, attempt)` request, installs queue delivery, and
advances the row to `QUEUED`, only PR #1070's request lease owns execution.

### Shared action-kernel boundary

`api_resource_actions` and its dispatcher are shared infrastructure, not a
Serve-owned queue under a generic table name. The kernel owns exactly:

- the one nonlocking indexed candidate query over `kernel_state=READY` and
  `next_attempt_at`, returning only ownership scope/epoch/action IDs;
- a short per-candidate transaction that locks the exact ownership-epoch row,
  then takes the action with `FOR UPDATE SKIP LOCKED`, obtains a fresh
  PostgreSQL statement clock, and CAS-revalidates active/current epoch,
  `READY`, descriptor hash, and due time before materialization;
- contiguous action-attempt numbering and the deterministic request binding;
- generic runnable/effect state, bounded database-clock backoff, and
  crash-resumable discovery of the immutable terminal attempt result;
- idempotent recovery of a committed attempt whose admission response was
  lost; and
- the connection-borrowing reducer boundary through which a domain adapter
  commits its state, reservation, action result, and event atomically after
  request terminalization.

It owns no controller leadership, placement choice, Serve lifecycle state,
capacity reservation, retry classification, compensation policy, resource
observation interpretation, or deletion proof. The Serve adapter owns those
domain decisions. It admits an action in the same transaction as replica and
capacity intent, supplies the immutable Serve spec/fences, classifies the
attempt evidence, and reduces it to the legal Serve state-machine edge. It
never supplies custom due-work SQL, claims a due row itself, or creates a
domain worker/fetcher.

The dispatcher materializes, rather than executes, one attempt. The request ID
is derived deterministically from the action namespace and attempt number; the
unique `(action_id, attempt)` binding is idempotent only when action kind,
spec/payload hashes, workspace, actor, and domain-fence projection all match.
The `api_request_queue` row is therefore a bound delivery record for that
action attempt, not the outer logical action and not an independently
replaceable nested request. If a future action effect genuinely needs another
SkyPilot request, that child uses a separate deterministic
`(action_id, attempt, effect_index, child_slot)` binding and the child UUIDv5
namespace defined below; it can never become the action's due, retry, or lease
authority.

The kernel borrows the caller's SQLAlchemy `Connection` at domain admission
and reduction boundaries. Domain admission owns the transaction that inserts
the action. The shared dispatcher owns a mechanics-only short transaction that
locks the exact generic ownership epoch then a due generic action, creates or
reuses the correlated request and queue row from the row's fully materialized
descriptor after matching its registered descriptor digest, and advances the
generic row to `kernel_state=QUEUED`. It invokes no domain callback and locks no projection,
Serve, capacity, or domain-owner row. Correlated request terminalization
uses nonlocking correlation discovery, then reacquires only generic ownership
and action/request/queue/effects; it freezes the attempt and advances only
`kernel_state` to `REDUCING`. It never locks an executor registration or
Serve/domain row. A result scan discovers kernel `REDUCING` IDs without
retaining locks; a later reducer transaction locks the executor registration,
the full domain order, generic action, then terminal request/effects and invokes
the Serve reducer on the supplied connection. No callback opens, commits,
closes, or nests a session, and no database transaction or action-row lock
spans provider I/O.

All paths share one universal row-lock order after nonlocking key discovery:

```text
writer advisory lock
-> current ownership epoch (when scoped)
-> executor registration / adapter claim authority (when present)
-> Serve/domain rows in their declared order
-> generic action
-> request
-> queue/effects
-> api_server_instances (only if needed)
-> operational event
```

A path omits absent classes but never acquires an earlier class after a later
one. Claim has no Serve/domain row and uses the two-stage no-I/O hook. The
API005 correlated terminalizer is the explicit generic exception: it uses
`ownership epoch -> action -> request -> queue/effects -> event` and never
locks registration or domain state. Promotion, expiry, release, pre-call validation,
mutation-window issue/result fill, reducer, observation, and retirement all
perform nonlocking key discovery and then obey this order. A pre-call
transaction commits complete effect/window intent, releases all locks, and
only then may provider I/O begin; result fill starts a new transaction and
reacquires the same order.

### Resource identity

`replica_id` is a display and placement identifier, not a durable incarnation
identifier. Today a restarted controller derives the next ID from the maximum
remaining replica row, so deletion of the highest row can make that integer
reusable. An action keyed only by service hash and replica ID could therefore
adopt or tear down a later replica.

Serve schema 032 adds `replicas.replica_incarnation_id UUID`. New replica rows
receive it in the same transaction that creates the row. Existing rows are
backfilled once while row-locked; application reads during the additive
rollout use a fenced get-or-assign helper until the backfill gate is closed.
The UUID never changes during the row's lifetime and is retained in action
history after the replica row is deleted.

The display form may be:

```text
service_hash : replica_id : replica_incarnation_id : launch
service_hash : replica_id : replica_incarnation_id : down
```

The database uses structured columns and a unique constraint, not string
parsing:

```text
UNIQUE(resource_kind, service_hash, replica_id,
       replica_incarnation_id, action_type)
```

For the first implementation:

- `resource_kind` is `serve_replica`;
- `service_name` is stored for bounded operator queries, while `service_hash`,
  `replica_id`, and `replica_incarnation_id` identify exactly one persisted
  replica incarnation;
- `action_type` is `launch` or `down`; and
- `action_spec` is immutable, bounded, redacted JSON in one of the two exact
  v1 schemas below. It contains only typed values or content-addressed
  references needed to reconstruct the mutation. It never contains a generic
  provider input bag.

#### Closed JSON value domain and canonicalization

Every resource-action JSON value (spec, admission envelope, outcome, evidence,
effect intent/result, and provider details) is constructed as a typed
Python value, not accepted as caller-supplied serialized JSON, and must be in
this domain before PostgreSQL sees it:

- `null` and booleans are allowed only at fields that explicitly name them;
- an integer must have `type(value) is int`, fit signed 64-bit range, and meet
  the narrower field bound below; booleans are not integers;
- floats, decimals, non-finite numbers, and exponent-form numbers are never in
  the JSON domain. A resource accelerator quantity that originated as a Python
  float is represented by the exact tagged IEEE-754 binary64 bits below, with
  no decimal scaling or rounding;
- a string must be NFC-normalized Unicode scalar text, contain neither U+0000
  nor surrogate code points nor C0/C1 control characters, and fit the field's
  UTF-8 byte bound. This rejects PostgreSQL JSONB's U+0000 failure before a
  cast;
- every array has the exact element schema and field-specific count limit
  below; and
- every object has exactly the required keys plus only the explicitly marked
  nullable values. There are no optional or extension keys in v1. The whole
  value is limited to eight object/array levels and 4,096 aggregate nodes,
enough for each declared locator, observation, spec, effect, and evidence
envelope. The 2,048-effect attempt bound is relational; effects are not nested
into one JSON value.

`schema_version` and `version` dispatch use exact integer equality. V1 accepts
only `1`; an unknown positive version is rejected instead of being interpreted
as v1. All object keys below are ASCII literals, so callers cannot introduce a
new dynamic key namespace. A recursive secret-shaped-key blacklist remains as
defense in depth, but it is not the schema or the primary redaction boundary.

Canonicalization is PostgreSQL-owned rather than an imitation of JSONB in
Python. After the closed-domain walk, the store serializes the value once,
binds it as `TEXT`, and, in the same transaction that will write it, executes
the equivalent of:

```sql
WITH normalized(value) AS (SELECT CAST(:candidate_text AS jsonb))
SELECT value,
       value::text AS canonical_text,
       octet_length(convert_to(value::text, 'UTF8')) AS canonical_bytes,
       encode(sha256(convert_to(value::text, 'UTF8')), 'hex') AS digest
FROM normalized;
```

The returned `value` is the value inserted. `spec_hash` is the returned digest,
and the 65,536-byte spec limit is on `canonical_bytes`. The database repeats
both `octet_length(convert_to(action_spec::text, 'UTF8')) <= 65536` and
`spec_hash = encode(sha256(convert_to(action_spec::text, 'UTF8')), 'hex')` as
CHECK constraints. Outcomes and normalized effect envelopes use the same
normalization query and their own limits, so application preflight and the
database CHECK measure identical bytes. PostgreSQL never receives a float,
exponent number, or U+0000, avoiding exponent expansion and JSONB Unicode
ambiguity. Existing rows are compared by their stored JSONB value and database
digest; Python `json.dumps()` output is never a persisted identity.

The canonical action spec and `last_result` are each at most 65,536 canonical
UTF-8 bytes. Each effect intent, result, and evidence object is independently
limited to 65,536 canonical UTF-8 bytes; the request stores only the ordered
effect count and aggregate digest. `spec_hash` is immutable. A change
to immutable launch inputs creates a new replica incarnation; it is never
silently treated as another retry. A down action references the same
incarnation and may only be superseded by a durable logical-target or terminal
service transition.

The following aliases make the schemas concise:

- `Text[N]`: nonempty text in the domain above, at most `N` UTF-8 bytes;
- `Token[N]`: `Text[N]` additionally matching
  `[A-Za-z0-9][A-Za-z0-9._:/@+~-]*`;
- `Sha256`: exactly 64 lowercase hexadecimal ASCII characters;
- `CatalogUuid`: canonical lowercase RFC 4122 UUID text;
- `OciSha256`: `sha256:` followed by exactly 64 lowercase hexadecimal ASCII
  characters;
- `OciPlatform`: lowercase ASCII `os/architecture` or
  `os/architecture/variant`, at most 128 bytes total, where every component
  matches `[a-z0-9]+(?:[._-][a-z0-9]+)*`;
- `WorkspaceName`: nonempty ASCII matching
  `[a-z]([-_a-z0-9]*[a-z0-9])?`, at most 63 characters;
- `PositiveI64` / `NonnegativeI64`: signed-64-bit integer greater than zero /
  greater than or equal to zero; and
- `UtcTimestamp`: exactly `YYYY-MM-DDTHH:MM:SS.ffffffZ`, representing a valid
  UTC instant. Inputs with offsets or fewer/more fractional digits are parsed
  outside the journal and re-rendered before validation.

`AcceleratorQuantity` is this exact union-shaped object with all keys present:

```text
{
  encoding: "integer" | "ieee754_binary64",
  integer: null | PositiveI64,
  binary64_hex: null | exactly 16 lowercase hexadecimal characters
}
```

`encoding=integer` requires only `integer`; it round-trips an original exact
Python integer. `encoding=ieee754_binary64` requires only `binary64_hex`, the
big-endian `struct.pack('>d', value)` bits of an original Python float. Its
decoded value must be finite and strictly positive; positive subnormals, as
well as ordinary small values such as a valid `0.0001` count, are preserved
exactly, while positive/negative zero, NaN, and infinities are rejected. The
handler reconstructs the same Python numeric type before `Resources.copy()`.
Decimal `count_milli` is forbidden.

`LogicalTargetV1` is also an exact object:

```text
{
  mode: "physical" | "aggregate" | "exact_card",
  service_version: PositiveI64,
  reconcile_generation: null | NonnegativeI64,
  target_capacity: null | NonnegativeI64,
  target_by_accelerator: [                           # at most 64
    {name: Text[128], target_capacity: PositiveI64}
  ],
  accelerator_shapes: [                             # at most 64
    {name: Text[128], slots_per_replica: PositiveI64}
  ],
  planned_capacity: PositiveI64
}
```

`physical` requires null generation/target and empty arrays. `aggregate`
requires generation and target but empty arrays. `exact_card` requires both,
case-insensitive unique names in each array, every target name in shapes, and
the sum of per-card targets equal to `target_capacity`; zero targets are
omitted, so a zero total has an empty target array. Array order is significant
and preserves the exact `LogicalAcceleratorState` tuple published by the
replica manager; it is not silently sorted. `planned_capacity` is the selected
physical backend's immutable logical width. Immediately before launch or down,
all fields are compared with the currently published five-component logical
target fence, including target total, target-by-card tuple, and shape tuple.

`ContainerImageSelectorV1` is the exact four-key object shown as `selector`
below. `ContainerImageActionV1` pairs it with immutable content identity:

```text
{
  selector: {
    ref: null | Text[1024],
    release: null | Text[128],
    artifact_id: null | CatalogUuid,
    distribution: null | Text[128]
  },
  content: {
    source_reference: Text[1024],
    source_root_digest: OciSha256,
    runtime: null | {
      artifact_id: CatalogUuid,
      platform: OciPlatform,
      digest: OciSha256
    }
  }
}
```

The selector obeys `ContainerImage`: at least one identity field is non-null,
`artifact_id` is exclusive with `ref`/`release`, every non-null `ref` is
digest-pinned, and `distribution=direct` requires only `ref`.
`content.source_reference` is always a validated digest-pinned OCI reference;
its embedded digest equals `source_root_digest`. `runtime` is an all-or-none
bundle. A selector with `release` or selector `artifact_id` requires it; a
ref-only selector, with any distribution value including null, requires it to
be null. This distinction is syntactic and deterministic at action creation:
a ref-only selector may later take either a direct or managed route after the
optimizer chooses a concrete placement, so admission does not guess its future
platform.

For a catalog selector (`release` or selector `artifact_id`), action creation
locks and resolves one immutable `READY` publication plus its artifact and
source. `source_reference` and `source_root_digest` equal both the publication
and source row; `runtime.artifact_id` equals the artifact ID;
`runtime.platform` equals the publication/source requested platform and
artifact platform; and `runtime.digest` equals both the source selected-child
digest and artifact runtime digest. The publication's `image_id` and
`source_id`, the source's `image_id`, selector release/artifact ID, and any
selector ref must form that same exact chain. For a ref-only selector,
`source_reference` equals the selector ref and `runtime` remains null even when
retry-time policy later chooses managed distribution. A mutable tag, an
unpinned legacy `docker:` image, a non-`READY` release, or conflicting catalog
content is not representable.

Selection is deterministic under the same locks. A release selects its unique
active READY publication; a release plus ref must select that publication's
bound source. An artifact-only selector chooses the first active READY
publication for the artifact by `(created_at ASC, id ASC)` and its non-null
bound source. A ref accompanying an artifact selector is forbidden by
`ContainerImage`. The chosen publication/source/artifact rows are retained at
least until every nonterminal action or shadow sample that snapshots them is
terminal, so retry cannot silently advance to another source chain.

This lookup must use a new caller-session catalog resolver that accepts the
action transaction's existing PostgreSQL `Connection` and locks the `READY`
publication, immutable artifact row, and source row in that transaction before
returning the snapshot. Existing catalog helpers that open their own session or
commit independently are forbidden in action creation, even when they return
the same digest, because they leave a decision-to-action race. The connection-
accepting resolver and its lock-order/race tests are an M1 merge prerequisite.

This content object, not the placement-specific pull plan, is immutable action
identity. On every attempt, after concrete placement is fenced, the image
resolver may choose a different secret-free distribution target, pinned target
reference, or runtime authentication strategy and obtains credentials only
from the retry-time workspace adapter. A catalog selector must reproduce the
exact non-null runtime bundle. For a ref-only selector, a direct pull remains
pinned to `source_root_digest`; a managed selection must lock a source and
artifact whose source reference/root digest equal `content`, whose requested
platform equals the concrete placement platform, and whose selected-child
digest/artifact ID/platform form one valid catalog lineage. That derived bundle
is recorded on the attempt's operation evidence but does not rewrite the null
action runtime. Any failed lineage check performs no cloud mutation and returns
a classified retry/verification outcome. Target IDs, credential helpers,
runtime principals, registry credentials, and auth tokens are not persisted in
the action. Strictly pinning one ref-only runtime child would require moving
action creation after placement and is outside v1.

`ResourceOverrideV1` is the following exact tagged union. All keys shown are
present, including null branches:

```text
{
  source: "none" | "direct" | "location",
  direct: null | {
    use_spot_present: boolean,
    use_spot: null | boolean,
    accelerators_present: boolean,
    accelerators: null | [                           # at most 16
      {name: Text[128], quantity: AcceleratorQuantity}
    ]
  },
  location: null | {
    cloud: Token[64],
    region: Text[128],
    zone: null | Text[128],
    use_spot: boolean,
    accelerators: null | [                           # at most 16
      {name: Text[128], quantity: AcceleratorQuantity}
    ],
    image_ids: null | [                              # at most 32
      {region: null | Text[128], image_id: Text[1024]}
    ],
    container_image: null | ContainerImageSelectorV1,
    disk_tier: null | "low" | "medium" | "high" | "ultra" |
                         "best" | "none",
    ephemeral_storage_gib: null | NonnegativeI64,
    instance_type: null | Text[128]
  }
}
```

`source=none` requires both branches null. `source=direct` requires only the
direct branch and at least one presence bit. A false presence bit requires its
value null; a present `use_spot` requires a boolean, while a present
`accelerators` may be null to clear inherited accelerators. `source=location`
requires only the location branch, whose ten values are the lossless typed
form of every key unconditionally emitted by `Location.to_dict()`; its nulls
are explicit `Resources.copy()` clears. Accelerator names are unique and
arrays are sorted by canonical name. Image regions are unique and sorted with
null first. Image IDs include the legacy `docker` region spelling only up to
1,024 bytes; when `region == "docker"`, `image_id` must itself be a validated
digest-pinned OCI reference and must resolve to the top-level effective
container-content root below; any placement-time managed selection must then
pass the root-to-runtime lineage rule above. A mutable legacy Docker tag cannot
be normalized and is not v1-capable.

There are no v1 CPU, memory, disk-size, network-tier, local-disk, port,
reserved-capacity, cost-rebalance, or generic assignment fields. The reserved
fill/cost-rebalance sentinels and policy provenance are consumed before action
creation. The locked `ReplicaInfo`/capacity-claim rows record whether the
surviving input came from no override, a direct exact-card decision, or a
selected `Location`; action creation derives this union from that provenance
in the same transaction as replica intent. `exact_resources_override` is not a
separate mutable input: the handler derives it as `source == location`.
Unknown legacy arbitrary overrides or rows whose provenance cannot be proved
remain on legacy authority, record shadow divergence, and block
`authoritative`; v1 is never widened to guess them.

`ActionDecisionProvenanceV1` is an exact six-key audit projection:

```text
{
  capacity_context: "ordinary" | "reserved_fill" |
                    "paid_capacity_claim" | "not_applicable",
  retirement_context: "not_applicable" | "replica" |
                      "full_service" | "ownerless",
  execution_context: "non_pool" | "consolidated_pool",
  resource_override_source: "none" | "direct" | "location" |
                            "not_applicable",
  logical_target_source: "physical" | "aggregate" | "exact_card",
  retirement_reason: null | "scale_down" | "rolling_update" |
                     "service_down" | "failed_cleanup" |
                     "orphan_cleanup"
}
```

For launch, capacity context is exactly one of its first three values,
retirement context is `not_applicable`, override/target sources equal their spec
discriminants, and retirement reason is null. For down, capacity is
`not_applicable`, retirement context is exactly `replica`, `full_service`, or
`ownerless`, override is `not_applicable`, target source equals its mode, and
retirement reason equals the retirement object. `logical_target.mode` records
exact-card independently, while `pool` equals whether execution context is
`consolidated_pool`; reserved/paid, exact-card, full-service, and pool are
therefore orthogonal rather than competing enum precedence. A native decision
producer captures this object, the full target
arrays/generation, and the exact override values in the same caller-owned
transaction as replica/capacity intent and action creation. Pre-M1/backfill may
not infer provenance from the eventual placement or cluster record: missing
provenance creates the closed shadow divergence or quarantine and never a
guessed native action.

V1 authoritative provider actuation is deliberately Kubernetes-only.
`KubernetesProviderLocatorV1` is created after concrete placement resolves but
before the first Kubernetes API write and is the exact secret-free cleanup
contract:

```text
KubernetesNamedTargetV1 = {
  schema_version: 1,
  kind: "configmaps" | "ingresses.networking.k8s.io" |
        "pods" | "services",
  namespace: Dns1123Label,
  name: Text[253]
}

KubernetesMutationPlanSummaryV1 = {
  schema_version: 1,
  renderer: "kubernetes-pod-cluster-mutation-plan-v1",
  complete_plan_sha256: Sha256,
  complete_plan_canonical_bytes: PositiveI64,
  named_target_count: PositiveI64,
  maximum_effect_count: PositiveI64,
  maximum_mutation_window_count: PositiveI64,
  maximum_mutation_call_count: PositiveI64,
  launch_call_count: NonnegativeI64,
  failover_call_count: NonnegativeI64,
  compensation_cleanup_call_count: NonnegativeI64,
  down_cleanup_call_count: NonnegativeI64
}

KubernetesProviderLocatorV1 = {
  schema_version: 1,
  provider: "kubernetes",
  adapter: "kubernetes-observation-v1",
  cluster_name_on_cloud: Dns1123Label,
  cluster_record_hash: CatalogUuid,
  scope: {
    context: Text[253],
    cluster_fingerprint_sha256: Sha256,
    kube_system_uid: Text[128],
    namespace: Dns1123Label,
    namespace_uid: Text[128]
  },
  selector: {
    key: "skypilot-cluster-hash",
    value: CatalogUuid
  },
  profile: "pod_cluster_v1",
  bootstrap: {
    mode: "verify_only_preprovisioned_v1",
    service_account_name: Dns1123Label,
    service_account_uid: Text[128],
    namespace_preexisting: true,
    fuse_device_required: false
  },
  expected_nodes: PositiveI64,
  port_mode: "podip" | "loadbalancer" | "ingress",
  ports: [integer 1..65535],                    # at most 128
  named_targets: [KubernetesNamedTargetV1],     # 1..1024, sorted/unique
  named_target_set_sha256: Sha256
}
```

The renderer deterministically produces the complete secret-free
`KubernetesCompleteMutationPlanV1` before action creation. It enumerates every
possible fixed and per-node object, exact kind/namespace/name/verb, branch and
dependency, launch/failover call, same-attempt compensation cleanup, and later
down-cleanup obligation. Generated names and fixed objects (Services,
Ingresses, the ray-ports ConfigMap, and every Pod) count exactly like worker
Pods; no “nodes only” estimate is accepted. Request bodies are represented by
their secret-free canonical digest, while a delete UID is the literal
`from_strong_observation` placeholder resolved only from matching-label
evidence. The plan contains no credential or token.

The complete plan is canonicalized by PostgreSQL during domain admission. Its
summary and digest are stored in both launch and corresponding down specs;
pre-call reconstruction from the immutable version/resource/placement inputs
must produce the byte-equal digest and counts. The complete plan is limited to
16,777,216 canonical bytes, 2,048 effects including exactly one executor-fence
effect and at least one reserved observation effect, at most 2,046 mutation
windows, and at most 16,368 mutation call slots because each window has at most
eight. The sum of every mutually possible create, patch, failover, and
same-attempt compensation call—not merely the selected happy path—must fit the
call bound. The complete down cleanup list independently fits it. Actual
normalized effects must be a branch-valid ordered subset of this sealed plan
and can never exceed the stored maxima.

The previous 512-node aggregate was not an authority bound: 64 in-request
reference entries could encode at most 504 call slots and did not reserve
fixed-object or cleanup work. V1 now retains a schema-level sum of
`expected_nodes <= 512` only as an input-size guard, while authoritative
eligibility is exclusively the exact complete-plan test above. A topology with
one node may fail if its fixed/branch/cleanup plan is too large; a topology
with hundreds may pass only when every exact planned call fits. There is no
silent truncation or optimistic node-to-call conversion.

`Dns1123Label` is nonempty lowercase ASCII matching
`[a-z0-9](?:[-a-z0-9]*[a-z0-9])?` and at most 63 bytes. Context is explicit and
non-null; it is not the account proof. The cluster fingerprint hashes the
canonical API-server authority and public CA material, and the observer also
verifies both kube-system and namespace UIDs. The selector value equals
`cluster_record_hash`. Ports are sorted and unique. The exact physical
`cluster_name_on_cloud` is persisted after provider/max-length/user-hash
derivation and is never recomputed from the display name. `named_targets`
contains every exact object name that the closed renderer can create, patch, or
delete for this locator, including all future failover/cleanup slots; its
canonical hash is immutable. Every target namespace equals `scope.namespace`.
An adapter cannot predeclare a mutation call slot whose exact
kind/namespace/name is absent from this inventory, and it cannot add a target
after PREPARED.

The structural maxima are conjunctive with exact aggregate budgets. Across one
retained locator array, the sum of `expected_nodes` is at most 512, the sum of
port entries is at most 256, the sum of named targets is at most 1,024, and the
PostgreSQL-canonical locator-array bytes are at most 24,576. The complete down
spec, including its duplicated terminal snapshot, must still fit the universal
65,536-byte spec bound; the complete observation set has its own 65,536-byte
bound and the ordered normalized-effect aggregate has the separate
16,777,216-byte bound. A value satisfying an individual
32-locator, 128-port, or 1,024-target cap but violating an aggregate/final bound
is deterministically rejected before action/sample creation or provider I/O.
No writer truncates a locator, target, port, proof, or reference to fit.

`pod_cluster_v1` is eligible only with the built-in reviewed Kubernetes facet
and the exact verification-only bootstrap mode above. Before PREPARED, one
read-only preflight proves the target Namespace already exists with the stored
UID and the named ServiceAccount already exists with the stored UID. The
ServiceAccount name must differ from SkyPilot's code-owned default service
account constant, and every rendered node Pod must use that exact name. The
workspace owns those shared prerequisites; the action neither labels nor
mutates them. The preflight also proves `fuse_device_required=false`.

In that mode `bootstrap_instances` is a verifier, not a shared-resource writer:
it must prove the rendered plan will issue no create, patch, replace, or delete
for Namespace, ServiceAccount, Role, RoleBinding, ClusterRole,
ClusterRoleBinding, or DaemonSet. A missing prerequisite or any planned shared
write rejects authoritative execution before PREPARED. Force deletion, a
NotReady control-plane node, FUSE, SkyPilot-default service-account bootstrap,
system-namespace creation, deployment/PVC mode, SSH node-pool mode, and
ephemeral-volume profiles are outside v1.

After PREPARED, the code-owned per-cluster cleanup-obligation set is exhaustive:
Pods, Services, `networking.k8s.io/v1` Ingresses, and the per-cluster ray-ports
ConfigMap. This includes bootstrap Services, generated per-port Services,
LoadBalancer Services, Ingresses, and the ray-ports ConfigMap. Every obligation
is namespaced, and provisioning injects and protects the exact selector on each
one. An existing same-name object without the matching selector is a conflict,
not something bootstrap may patch or adopt. User metadata cannot override or
remove the identity label. The preflight renders the entire deterministic
named-target inventory, proves it covers this exhaustive obligation set and all
branch/failover names, and performs a strong exact-name GET for every entry
before PREPARED. Any pre-existing entry is a conflict unless it is already a
byte-equal retained object for this locator.

Custom/legacy Kubernetes provisioner plugins, templates that introduce another
kind, templates that replace the protected label, and any provider path whose
bootstrap cannot prove the exact no-shared-write plan are unsupported profiles.
They remain on the legacy/shadow boundary until a later reviewed facet covers
their full inventory. Name-only legacy objects can never be MATCHED or
absence-proven.

The locator is a `PREPARED` provider step. The worker appends its complete value
and canonical hash to the claim-fenced request attempt and to
`replicas.resource_action_provider_locators` through the shared central
PostgreSQL connection, then commits before the first
`sky.execution.launch`/`down_expected_generation` mutating entry, initial
cluster-record write, mutating target-adapter entry, or mutating Kubernetes
request byte. Deterministic normalization and the exact read-only eligibility
preflight happen before this boundary; that preflight grants no mutation
authority. Once either direct mutating primitive is entered, a complete
nonempty locator is therefore durably retained. The
authoritative wrapper additionally requires immutable executor-fence effect
zero and a current precommitted mutation window; no target
adapter/callback is
reachable before both commits. Shadow does the same
with an `INPUT_MATCHED` child under its sample token. The array is prefix-only,
contains at most 32 distinct locators, and is retained until every locator is
proven absent; no 33rd mutation starts. A down spec and terminal snapshot copy
the complete array under lock. `provisioner.bulk_provision()` must plumb the
UUID label and durable pre-call callback into `ProvisionConfig` instead of
rebuilding `tags={}` and calling the provider directly. A mutation that bypasses
the callback, current claim, executor fence, or mutation window is
`unsampled_mutation` and cannot be accepted.

The recursively closed all-locator proof is the only v1 presence/absence
evidence:

```text
KubernetesCollectionReadV1 = {
  schema_version: 1,
  kind: "configmaps" | "ingresses.networking.k8s.io" |
        "pods" | "services",
  request: {
    consistency: "MostRecent",
    resource_version: "",
    resource_version_match: null,
    limit: 256
  },
  collection_resource_version: Text[128],
  page_count: PositiveI64,
  continue_chain_sha256: Sha256,
  final_continue_empty: true,
  count: NonnegativeI64,
  snapshot_sha256: Sha256
}

KubernetesNamedTargetReadV1 = {
  schema_version: 1,
  target_index: NonnegativeI64,
  target_sha256: Sha256,
  request: {
    consistency: "MostRecent",
    resource_version: "",
    resource_version_match: null
  },
  result: "present_matching_label" | "present_mismatched_label" |
          "not_found" | "unverifiable",
  observed_uid: null | Text[128],
  observed_label_value: null | Text[128],
  deletion_timestamp_present: null | boolean,
  finalizers_present: null | boolean,
  observed_at: UtcTimestamp,
  response_evidence_sha256: Sha256
}

KubernetesLocatorObservationV1 = {
  schema_version: 1,
  locator_index: NonnegativeI64,
  reference_kind: "current_prepared" | "retained_locator" |
                  "uncorrelated_locator",
  effect_index: null | NonnegativeI64,
  locator_hash: Sha256,
  named_target_set_sha256: Sha256,
  profile: "pod_cluster_v1",
  observed_at: UtcTimestamp,
  scope: "matched" | "namespace_generation_gone" |
         "mismatch" | "unverifiable",
  scope_evidence_sha256: Sha256,
  enumeration: "complete" | "incomplete",
  disposition: "present" | "absent" | "uncertain",
  counts: {
    pods: NonnegativeI64,
    services: NonnegativeI64,
    ingresses: NonnegativeI64,
    configmaps: NonnegativeI64
  },
  collections: [KubernetesCollectionReadV1],   # 0..4, sorted/unique by kind
  named_targets: [KubernetesNamedTargetReadV1],
  topology_match: boolean,
  snapshot_sha256: Sha256
}

KubernetesObservationRoundV1 = {
  schema_version: 1,
  locator_set_hash: Sha256,
  started_at: UtcTimestamp,
  completed_at: UtcTimestamp,
  observations: [KubernetesLocatorObservationV1]  # 1..32
}

KubernetesObservationSetV1 = {
  schema_version: 1,
  locator_set_hash: Sha256,
  basis: {
    intent_kind: "resource_action" | "shadow_sample",
    intent_id: canonical lowercase UUID text,
    intent_revision: NonnegativeI64,
    request_id: null | canonical lowercase UUID text,
    attempt: NonnegativeI64,
    request_state: "live_claim" | "terminal_frozen" | "uncorrelated",
    effect_count: null | PositiveI64,
    effects_sha256: null | Sha256,
    execution_quiescence_sha256: null | Sha256,
    not_before: UtcTimestamp
  },
  observed_at: UtcTimestamp,
  rounds: [KubernetesObservationRoundV1]          # exactly 1 or 2
}
```

Every round's `observations` has exactly the same length and order as the
locked locator array; entry `i` has `locator_index=i` and the exact canonical
hash of locator `i`.
Its `named_target_set_sha256` equals the locator's field. For
`scope=matched`, `named_targets` has exactly the same length/order as the
locator inventory; target entry `j` has `target_index=j` and the canonical hash
of inventory entry `j`. For verified `namespace_generation_gone` it is empty.
An incomplete entry may retain only a sorted unique prefix/subset of completed
target reads, with each index/hash still exact, but cannot prove absence.
`locator_set_hash` is the SHA-256 of the PostgreSQL-canonical exact object
`{schema_version: 1, locators: [{locator_index: i, locator_hash: h_i}]}`.
The outer and every round hash equal that value. In a two-round set, locator
index, hash, profile, reference kind, and effect index are
byte-equal at each position across both rounds; a later round cannot change the
evidence source.
There is no subset, merge, last-locator shortcut, duplicate, or reordered set.
The locator-source union is exact. `current_prepared` requires a non-null
`effect_index` naming the current request's exact locator-bearing
`prepared` normalized effect; all such indexes are unique. It is used if and
only if such an effect exists. `retained_locator` requires a null effect index
and a
byte-equal locator copied from the locked ordered replica/action array at that
same `locator_index`; it is used only when the current request has no matching
prepared effect. The normalized effect store's existing pre-I/O insert guard,
protected label,
and activation/backfill gates are its durable origin contract. This form is
therefore constructible after a later attempt crashes before re-preparing old
locators without inventing provider mutation evidence.
`uncorrelated_locator` requires a null effect index and is allowed only while
proving a shadow sample whose locked locator array is the evidence source.

For each round, the observer obtains `started_at` from PostgreSQL
`clock_timestamp()` in a short read-fence transaction immediately before its
first provider read and obtains `completed_at` from PostgreSQL immediately
after its last read; every child `observed_at` lies within that closed interval.
The first authoritative read fence performs nonlocking key discovery and locks
`current ownership epoch -> executor registration when present ->
service/owner when present -> replica/capacity -> Serve projection -> action
-> request when present -> effects` (or the declared ownerless
tombstone variant), while shadow uses its declared domain suffix after any
registration. It copies
the exact intent revision, request/attempt state,
current-or-frozen normalized-effect count and aggregate hash, immutable
quiescence hash, and
database-derived `not_before` into `basis`, and locks/recomputes every locator
and named-target-inventory hash before releasing its locks for I/O. The second
round's start transaction must revalidate the same basis and inventories. A
two-round set requires
`rounds[1].started_at >= rounds[0].completed_at + interval '5 seconds'`.
The outer `observed_at` equals the last round's `completed_at`. Before an
outcome is committed, one CAS transaction reacquires that same domain-first
order through the current request when present, revalidates the complete
basis, locked locator-set hash, and every named-target-set hash, then recomputes
every canonical round hash.
Any intervening intent revision, request/attempt/claim state, effect/result
fill, quiescence, or locator change invalidates both rounds; a stale observer
cannot transition the action. `live_claim` requires a current claim and null
quiescence; `terminal_frozen` requires the frozen effect count and aggregate
hash and, whenever any window was ambiguous, the byte-equal quiescence hash;
`uncorrelated` requires null effect count/hash and quiescence hash and is
limited to shadow. For `live_claim`, `not_before` equals the first
read-fence database timestamp; for a terminal attempt with only
no-bytes/definitive results it equals request `finished_at`; for
`uncorrelated` it equals the locked first read-fence time. When quiescence is
required, `not_before` equals its
SQL-derived `settle_after` and the first round's
`started_at` must be at or after the SQL-derived `settle_after`; a
pre-settlement round cannot be retained as either round of terminal evidence.

For each locator, one client bound to its stored endpoint/CA first verifies
kube-system and namespace identity with authenticated default/MostRecent GETs
and no cache/resource-version override, then both LISTs every obligation by the
exact selector and GETs every named-target inventory entry by exact
kind/namespace/name. Every first-page LIST uses literal `resourceVersion=""`,
omits `resourceVersionMatch`, uses `limit=256`, and is a non-watch request;
every exact-name GET uses the same resource-version/match/non-watch contract
without a limit. On an
allowlisted Kubernetes build that is the exact `MostRecent` consistent-read
semantic; `resourceVersion=0`, `Any`, `NotOlderThan`, a cache-only option,
watch, or streaming-initial-events path is forbidden. Every continuation
request uses only the server's preceding opaque token plus the same selector
and limit. Every page must return the same nonempty collection
`resourceVersion` as page one, and the last token must be empty.

The `limit=256` field applies only to LIST; exact-name GET has no limit or
continuation parameter. A target 404 is `not_found`. A complete object with the
exact protected label/value is `present_matching_label`; a complete same-name
object with the label absent or different is `present_mismatched_label`.
Authentication/transport/decode/identity failure is `unverifiable`.
UID/label/deletion/finalizer projection, response evidence, target hash, and
SQL-owned `observed_at` are retained. A matching-label object must also occur
byte-consistently in its kind's LIST snapshot. A mismatched-label object is
never owned/deleted/adopted, but it prevents absence and retry; it maps the
locator to `uncertain`, not `absent`.

`scope_evidence_sha256` hashes the authenticated kube-system plus Namespace
GET status/UID projection. A complete `scope=matched` entry has exactly the
four sorted `KubernetesCollectionReadV1` entries and every ordered exact-name
read. A verified
`namespace_generation_gone` entry has an empty collection array and zero
counts and an empty named-target-read array; its scope evidence is the strong
Namespace 404 or changed-UID proof. An incomplete/uncertain entry may retain
the zero through four completed collection projections and the bounded exact
target reads obtained before failure, but it can never prove absence.
`continue_chain_sha256` hashes the ordered page number, response collection
resourceVersion, incoming-token hash, outgoing-token hash, count, and
per-page snapshot hash; raw continuation tokens and objects are not retained.
Each collection count/hash must equal its projection in the locator-level
counts and aggregate snapshot. The aggregate snapshot hash covers the sorted
canonical `{kind, namespace, name, uid, deletion_timestamp_present,
finalizers_present}` objects. More than 1,024 objects across the complete
observation set, a 401/403, transport failure, cluster 404, pagination gap,
changed/missing collection resourceVersion, exhausted 410 restart,
fingerprint/UID mismatch, or unsupported kind makes the affected entry
`incomplete/uncertain`, never a truncated success. A 410 restarts that entire
kind from a new MostRecent first page; no page from the abandoned chain enters
the round. A namespace 404 after the same cluster identity, or a different
current namespace UID, is `namespace_generation_gone`; mutation against a
replacement namespace is forbidden.

Per-locator confirmed absence requires exactly two rounds and, at that
position in both, complete enumeration, all four counts zero, disposition
`absent`, the canonical empty snapshot, and either `scope=matched` with all
four complete MostRecent collection proofs plus every named target
`not_found`, or
`scope=namespace_generation_gone` with the empty collection array and exact
scope evidence plus an empty target-read array. Thus the terminal object
carries both complete sweeps; it never
contains only a timestamp/hash assertion about evidence stored elsewhere.
There is no UID-ack field or one-sweep shortcut. UID-precondition delete
responses are mutation acknowledgements only and never shorten absence proof.
A terminating or finalized object is still present.

Launch adoption requires the final round to contain exactly one entry with
`scope=matched`, complete enumeration, exactly `expected_nodes` labeled Pods
with exactly one head, `topology_match=true`, and every network object required
by its port mode/ports. Every labeled object must have the byte-equal
matching-label exact-name read, and every inventory target required by the
rendered topology must be present with that label. If the set has two rounds,
the same locator must be the sole matching live location in both. Every other
retained locator must have confirmed absence across both rounds; a one-locator
launch with no absence obligation may use one round. Down success requires
exactly two rounds and confirmed absence at every position. A complete down
round with at least one owned matching-label present entry is
`resource_present`; any mismatched-label exact-name object, uncertain entry,
live location that changes between rounds, multiple launch locations, or
partial topology makes the aggregate `observation_uncertain`. Empty local
cluster/history/YAML/handle state is never provider proof.

Cleanup never uses an unqualified collection delete. It first completes the
all-kind exact-label LIST, then deletes each observed object with its UID
precondition. A 409 or replacement UID returns to uncertain observation. A 410
restarts the entire all-kind sweep; every kind must reach an empty continuation
token. Delete request and response data are not accepted as terminal evidence;
two later complete empty sweeps are still mandatory. A recreated same-name
namespace is never mutated. Because the profile is entirely namespaced,
verified same-cluster namespace absence or a changed namespace UID proves only
that the old namespace generation is gone and can support the confirmation
two-round protocol above.

The locator's explicit context and scope are an active workspace resource.
Workspace deletion or a provider-scope-changing context edit is blocked while
any nonterminal locator exists, including ownerless cleanup. Credential values
remain external. Missing/rotated credentials yield `unverifiable`; same-scope
rotation is usable only after re-verifying the stored endpoint/CA and UIDs.
Mixed/multi-cloud candidate sets may run shadow input auditing but cannot be
promoted and must not be silently filtered. Only services whose complete
candidate set and live inventory fit `pod_cluster_v1` may enter authoritative
mode.

#### Exact launch spec v1

A launch spec has exactly these keys; every nested object also has exactly the
shown keys:

```text
{
  schema_version: 1,
  cluster_name: Token[256],
  cluster_record_hash: CatalogUuid,
  workspace: WorkspaceName,
  service_version: PositiveI64,
  service_lifecycle_epoch: NonnegativeI64,
  version_spec_ref: {
    kind: "serve_version",
    service_hash: Text[256],
    service_version: PositiveI64,
    execution_yaml_sha256: Sha256,
    service_spec_sha256: Sha256,
    submitted_yaml_sha256: null | Sha256
  },
  decision_provenance: ActionDecisionProvenanceV1,
  resource_override: ResourceOverrideV1,
  system_transforms: {
    version: 1,
    replica_id_env: "serve_replica_id_v1",
    security_group_scope: "service_security_group_v1",
    tls: {
      adapter: "serve_replica_tls_v1",
      mode: "disabled" | "unverified" | "pinned",
      binding_fingerprint: null | Sha256
    }
  },
  effective_container_image: null | ContainerImageActionV1,
  kubernetes_mutation_plan: null | KubernetesMutationPlanSummaryV1,
  resource_scope: {
    kind: "incarnation" | "legacy",
    value: null | Text[256]
  },
  logical_target: LogicalTargetV1,
  pool: boolean
}
```

The three service-version values and the outer action identity's service hash
must agree. `service_lifecycle_epoch` equals the locked service row and is the
durable planner epoch; it is not an API request-controller generation.
Workspace and resource scope are independent identities.
Action creation pre-generates `cluster_record_hash` and stores it in the
replica intent. The initial `INIT` write cannot use the existing
`existing_cluster_hash` API, which is update-only. Instead
`global_user_state.add_or_update_cluster()` gains a mutually exclusive
`new_cluster_hash` path: while the cluster-name row is locked, an absent row is
inserted with exactly the preallocated UUID, an existing row with the same UUID
is an idempotent update, and a same-name row with a different UUID is rejected.
It never falls back to a random hash. After that reserved insert, every refresh,
handle/status/event write, teardown, post-cleanup, and removal passes the UUID
as `existing_cluster_hash`; a zero-row conditional update is an ownership loss,
not permission to recreate by name.

The internal launch plumbing carries this exact trusted object, never a public
YAML field or generic provider bag:

```text
ServeReplicaLaunchContextV1 = {
  version: 1,
  intent_kind: "action" | "shadow_sample",
  intent_id: canonical lowercase UUID text,
  service_name: Text[256],
  service_hash: Text[256],
  service_version: PositiveI64,
  service_lifecycle_epoch: NonnegativeI64,
  replica_id: NonnegativeI64,
  replica_incarnation_id: canonical lowercase UUID text,
  cluster_record_hash: CatalogUuid,
  workspace: WorkspaceName,
  resource_scope: {
    kind: "incarnation" | "legacy",
    value: null | Text[256]
  }
}
```

The action handler reconstructs it from the locked action; shadow ordinary
admission reconstructs it from the locked sample. Service name/hash/version
must equal the action or sample identity/spec and remain the workload
attribution and managed-image demand owner; no generic cluster fallback is
allowed. The object travels under the one namespaced nested key
`sky_serve_resource_action_context`; it does not reuse any individual key in
the legacy `REPLICA_LAUNCH_FENCE_KEYS` set. One strict parser is shared by
execution, backend, attribution, and image-demand code. An authoritative action
uses the nested context without inventing a legacy PID/IP owner fence; a shadow
ordinary launch carries both its complete valid legacy owner fence and the
nested sample context, and the two identities must agree. Execution threads it through
`execution.launch`, `CloudVmRayBackend._extra_launch_context`, and
`RetryingVmProvisioner` to the first global-state write. Every layer rechecks
intent, workspace, scope, and cluster hash rather than dropping the context.
Launch refuses if the same-name current cluster row has a different hash; after
mutation it requires the row's `cluster_hash` to equal this ID. Thus name reuse
cannot attach an old action to a new cluster.
`workspace` is the nonempty durable user workspace resolved from
`services.workspace` (including the default/backfill protocol); it is never
derived from resource scope. `resource_scope` is the external resource
namespace stored in `services.resource_scope`: `kind=incarnation` requires a
non-null `value` equal to both that column and the service hash, while
`kind=legacy` requires null and is allowed only when the locked source service
row's scope is null. `origin` and namespace are independent: a new action on a
retained pre-scope service is `origin=native` with legacy scope, while an
operation reconstructed from old state is `origin=legacy_backfill`; either
origin uses incarnation scope when the service row has it. Every service
created by an action-capable image stores its hash as scope and therefore every
native action for such a new service uses
`{kind: incarnation, value: <service_hash>}`. No action path changes a legacy
service's external names merely to journal it. Any non-null scope that differs
from the service hash is a migration error, not a workspace or global scope.
`pool=true` is accepted only when the locked service manager is the
consolidated central-PostgreSQL pool manager and replica/action writes use the
same physical database connection. A remote-controller/non-consolidated pool
cannot normalize either v1 spec and remains legacy.

An authoritative launch requires non-null `kubernetes_mutation_plan` and exact
pre-admission/pre-call plan verification. Null is permitted only in a shadow
sample recording that the input/profile was not representable; such a sample
can never be `MATCHED` or promoted. A legacy service may bypass this model only
through its named legacy adapter. If an already-authoritative service update
would produce null, an unknown renderer, a plan/hash mismatch, or any
effect/window/call/byte overflow, the API rejects the update before mutating
the service version, desired replica count, route, capacity reservation, or
other desired state. It cannot demote the monotonic service mode or fall back
to legacy after committing the update.

Accelerator and image arrays are sorted and contain no duplicates;
accelerator names and image regions occur once. Accelerator quantities use the
exact tagged representation above. `image_ids` is the lossless normalized
form of `dict[str | None, str]`. Ephemeral storage is the exact integer GiB
already held by `Resources`, not a string or JSON float. A non-null container
image override preserves its four-key selector. `effective_container_image`
is computed from the locked version task after applying the exact override and
is present for every effective container image, including a base-task image
when the override source is `none` or `direct`; it is null only when the
effective resources contain no container image. A location selector must equal
the snapshot selector, while an explicit-null location value requires a null
snapshot. A `docker` image-ID entry must normalize to the same selector/content
snapshot. This is the one immutable image identity across every service
candidate, not merely an attribute of a location override.

The reference resolves exactly one existing
`version_specs(service_name, version)` row under the action identity's service
name/hash fence. Creation row-locks that row and hashes the UTF-8 bytes of
`yaml_content`, the exact stored bytes of `spec`, and, when non-null, the UTF-8
bytes of `submitted_yaml_content`. Those digests must equal
`execution_yaml_sha256`, `service_spec_sha256`, and
`submitted_yaml_sha256`, respectively. Nullability must also agree. The row is
retained only while a referencing launch action is nonterminal. Atomic action
terminalization is the dereference gate: after `SUCCEEDED`,
`TERMINAL_FAILED`, or `SUPERSEDED`, reconciliation never reloads mutation input
from that version row, and service purge/name reuse may delete all
`version_specs` rows immediately even though action history and its three
digests remain under longer retention. A nonterminal action blocks only the
specific destructive service purge that would remove its referenced input; it
does not extend version-row retention after completion.
Immediately before mutation the handler reloads the same row, rechecks all
three digests, deserializes `spec` with the existing trusted internal loader,
and invokes launch with that exact execution YAML/spec plus only the closed
override union above. The normalizer either emits no kwargs, emits the
presence-marked direct `use_spot`/`accelerators` kwargs, or losslessly maps all
ten persisted `spot_placer.Location.to_dict()` values. It verifies that the
resulting effective selector equals `effective_container_image.selector` (or
that both are absent), then resolves and attaches a placement-specific pull
plan under the immutable content fence. It then re-applies the exact versioned
system transforms in legacy order: set `SKYPILOT_SERVE_REPLICA_ID` from the
fenced replica ID, resolve TLS through the named server-secret adapter, and
apply the service-scoped security-group transform. `pinned` requires a non-null
binding fingerprint; the other modes require null. The adapter returns the
certificate/key only at retry time and must prove that fingerprint; neither
secret value enters action, request, shadow, event, or logs. Missing material
is a classified pre-mutation retry, and rotation to a different fingerprint
requires a new service version/replica incarnation rather than mutating this
action. The security-group transform is the one additional audited
`Resources.copy(_cluster_config_overrides=...)`: it preserves an explicit user
group or deterministically installs `sky-sg-<service_name>`, and is a no-op for
providers that do not consume it. No other post-normalization copy is legal.
Shadow compares the descriptors and actual secret fingerprint while redacting
the material.

The correlated handler invokes the server-internal in-process primitive
`sky.execution.launch` exactly once with `retry_until_up=False`, using the
locked reconstruction above. It never calls `sdk.launch`, another SDK/client
function, HTTP/FastAPI admission, `schedule_request`, or any path that creates,
claims, or waits for a nested API request. The one correlated request remains
the sole attempt identity, execution generation, claim/lease, and request
terminalization boundary for all provider I/O in the attempt. Its normalized
effect rows own the executor fence, mutation intents/windows, operation IDs,
and results. The primitive receives that already-claimed
request context and may call only its direct backend/provisioner stack; a
nested request is neither an execution adapter nor a child operation. It has
no controller-local `max_retry` or `availability_max_retry` loop, and its
request has `should_retry=false`. Bounded optimizer/provider failover inside
that one invocation is allowed only when every provider callback writes its
sealed-plan PREPARED/effect intent to the normalized effect store under the
same claim and atomically advances the request effect count/aggregate digest.
The action's database retry clock alone creates attempt N+1. Internal
fill/cost-rebalance sentinels
are consumed into typed
replica/logical-target and capacity-claim fields before action creation and are
not mutation inputs. M1 tests audit every launch producer and reject action
creation if any other `Resources.copy()` override survives normalization. This
is the complete reconstruction rule for v1. The YAML/spec may remain in their
existing access-controlled store; neither they nor user environment values are
copied into the journal. Adding another input requires action-spec v2 and
review; it may not be smuggled through a provider dictionary.

#### Exact down spec v1

A down spec has exactly these keys:

```text
{
  schema_version: 1,
  cluster_name: Token[256],
  cluster_record_hash: CatalogUuid,
  provider_locators: [KubernetesProviderLocatorV1],     # 1..32
  workspace: WorkspaceName,
  service_version: PositiveI64,
  service_lifecycle_epoch: NonnegativeI64,
  decision_provenance: ActionDecisionProvenanceV1,
  kubernetes_mutation_plan: KubernetesMutationPlanSummaryV1,
  resource_scope: {
    kind: "incarnation" | "legacy",
    value: null | Text[256]
  },
  logical_target: LogicalTargetV1,
  owner_fence: {
    kind: "live_service" | "terminal_tombstone",
    terminal_intent_id: null | canonical lowercase UUID text,
    terminal_snapshot: null | TerminalOwnerSnapshotV1
  },
  retirement: {
    reason: "scale_down" | "rolling_update" | "service_down" |
            "failed_cleanup" | "orphan_cleanup",
    purge: boolean,
    reconcile_generation: null | NonnegativeI64,
    target_capacity: null | NonnegativeI64,
    target_by_accelerator: [
      {name: Text[128], target_capacity: PositiveI64}
    ],
    accelerator_shapes: [
      {name: Text[128], slots_per_replica: PositiveI64}
    ],
    planned_capacity: PositiveI64
  },
  pool: boolean,
  drain_deadline: null | UtcTimestamp
}
```

The logical target's service version equals the top-level service version.
Every retirement target field—generation, total, ordered per-accelerator
targets, ordered shapes, and planned capacity—is an exact copy of the
corresponding `logical_target` field for all three modes; any mismatch is
rejected. The duplication is an audited retirement-policy projection, not a
second target. Both retirement arrays retain the same 64-entry bounds.
The independent workspace and incarnation/legacy resource-scope invariants are
identical to launch and are retained in the action after the service row is
removed so ownerless cleanup addresses the original namespace. An eligible
consolidated-pool down requires a null deadline and is immediately eligible; a non-pool action uses
null only when existing policy proves no drain wait is required.
`cluster_name`, the structured resource-action identity/resource scope, the
separately protected SkyPilot cluster record, and the complete immutable
provider-locator array together identify the resource. The database hash or
local inventory alone is not a provider reference. There is no
`provider_inputs` field in either v1 schema.
Credentials, raw request bodies, environment values, exception text, and
arbitrary provider metadata are unrepresentable.

`TerminalOwnerSnapshotV1` is the following exact object, with `logical_target`
and `retirement` using the complete objects above:

```text
{
  schema_version: 1,
  intent_id: canonical lowercase UUID text,
  service_name: Text[256],
  service_hash: Text[256],
  replica_id: NonnegativeI64,
  replica_incarnation_id: canonical lowercase UUID text,
  cluster_name: Token[256],
  cluster_record_hash: CatalogUuid,
  provider_locators: [KubernetesProviderLocatorV1],     # 1..32
  workspace: WorkspaceName,
  service_version: PositiveI64,
  service_lifecycle_epoch: NonnegativeI64,
  resource_scope: {
    kind: "incarnation" | "legacy",
    value: null | Text[256]
  },
  logical_target: LogicalTargetV1,
  retirement: <the exact retirement object above>,
  pool: boolean,
  drain_deadline: null | UtcTimestamp,
  routed: false
}
```

Every duplicated value must equal the action or shadow-sample identity and
top-level spec. A
`live_service` fence requires both intent ID and snapshot null. A
`terminal_tombstone` requires both non-null, snapshot intent ID equal to the
pre-generated action ID in authoritative mode or sample ID in shadow mode, and
a byte-equal copy in
`replicas.resource_action_terminal_snapshot`.

A down action copies `cluster_record_hash` and the canonical locator array from
the locked launch/replica intent, never from a same-name lookup. Before any
provider or global-state mutation it requires
`replicas.resource_action_cluster_hash`,
`replicas.resource_action_provider_locators`, and any current cluster row
selected for the operation to match. A current same-name row with a different
hash belongs to a successor and is never passed to teardown; an absent current
row is not deletion proof and uses the complete locator-observation path.
Global-state updates always pass `existing_cluster_hash=cluster_record_hash`,
so their conditional write cannot touch a successor. The replica binding and
locator inventory are retained through down success; action history retains
both afterward.

The dedicated action handler does not call the public user-down or
priority-down branch. It may cancel or supersede only the request ID correlated
to this incarnation's launch action; a claimed or ambiguous launch first puts
down in `VERIFYING` until complete locator observation closes that mutation. It
is reachable only for a nonempty locator array because the planner has already
executed or rejected the zero-locator retirement branch. It
then acquires normal name-scoped status/resource locks without `force_unlock`,
rereads `(cluster_name, cluster_record_hash)`, and calls an internal
`down_expected_generation` path. That typed path carries the action workspace
and expected hash through refresh, status/event, handle, teardown,
post-cleanup, and removal. A different current hash permits no provider call;
a missing current row enters `VERIFYING` and locator observation rather than
the existing `teardown_no_lock` missing-row success path. Public user down
retains its existing behavior and cannot be selected by a correlated handler.

`owner_fence=live_service` requires a null intent ID and snapshot. Its handler
locks the current service and replica rows and requires service hash, mode,
workspace, scope, replica incarnation, cluster name, and full logical target
to match. The service row is retained until that down succeeds.

Full-service, failed-service, and known orphan cleanup first restarts through
the branch-specific universal lock order and evaluates the zero-locator
retirement branch below. Only a nonempty locator array creates a
`terminal_tombstone`. Authoritative mode then
pre-generates an action ID; shadow mode pre-generates a sample ID and never
creates an action. The transaction locks/writes the replica tombstone and
Serve projection before inserting the authoritative down action or the one
shadow `controller_direct` down sample. The selected UUID is stored as both
`owner_fence.terminal_intent_id` and
`replicas.resource_action_terminal_intent_id`, and the exact terminal snapshot
uses that UUID as `intent_id`. One caller-owned PostgreSQL `Connection`
transaction commits route removal, `routed=false`, retirement/capacity intent,
the copied cluster hash and locator array, the byte-equal replica terminal
snapshot and projection, and then the down action or sample. Only after that commit may the service
row be deleted. The action or sample is the immutable authorization tombstone;
the replica and parent row are retained through every cleanup child and until
complete provider absence closes the retirement.

#### Zero-locator retirement (no provider action)

An empty locator array cannot satisfy a down spec or observation set, and v1
never fabricates an empty provider target. Every scale-down, rolling,
service-down, failed-cleanup, and orphan-cleanup planner evaluates this branch
before constructing any down action/sample. It instead permits one
`ZeroLocatorRetirementV1` transaction only when durable evidence proves that
the launch never crossed the PREPARED boundary. After nonlocking discovery of
the launch epoch and every attempt's registration key, the authoritative
transaction uses `writer lock -> ownership epoch -> executor registrations in
registration-ID order -> service/owner -> referenced version_specs ->
replica/capacity/route/usage -> Serve projection -> launch action ->
linked requests in attempt order -> current queue delivery/effects
-> operational event`; the shadow transaction uses
`writer lock -> service -> referenced version_specs ->
replica/capacity/route/usage -> launch sample -> ordinary request/queue ->
operational event`.
These are the declared domain-first and shadow orders, not a new mixed order.
The transaction rechecks the exact service hash, replica incarnation, cluster
hash, mode/owner or retained terminal authority, and requires all of the
following:

```text
ZeroLocatorAttemptProofV1 = {
  schema_version: 1,
  attempt: PositiveI64,
  request_id: canonical lowercase UUID text,
  proof: "queued_unclaimed" | "unclaimed_terminal" |
         "claimed_not_submitted",
  frozen_effect_count: PositiveI64,
  frozen_effects_sha256: Sha256,
  execution_quiescence_sha256: null | Sha256
}

ZeroLocatorRetirementProofV1 = {
  schema_version: 1,
  basis: "native_action" | "shadow_sample",
  launch_action_id: null | canonical lowercase UUID text,
  launch_sample_id: null | canonical lowercase UUID text,
  service_hash: Text[256],
  replica_id: NonnegativeI64,
  replica_incarnation_id: canonical lowercase UUID text,
  cluster_record_hash: CatalogUuid,
  provider_locators_sha256: Sha256,
  checked_at: UtcTimestamp,
  attempt_count: NonnegativeI64,
  attempts_sha256: Sha256,
  shadow_attempt: null | ZeroLocatorAttemptProofV1
}
```

The two parent IDs are an exact XOR fixed by `basis`.
`provider_locators_sha256` is the PostgreSQL-canonical hash of the literal
empty array, `checked_at` is the transaction's `clock_timestamp()`, and
`attempt_count` is the number of contiguous attempt rows selected in order.
`attempts_sha256` is computed inside PostgreSQL from the canonical exact object
`{schema_version: 1, attempts: [<every ZeroLocatorAttemptProofV1 in attempt
order>]}` while all source rows are locked; an empty projection means only
native action attempt zero/no request or the exact never-admitted shadow shape.
The compact count/digest, rather than an unbounded copied array, keeps the
retirement proof within its fixed 65,536-byte outcome/event budget no matter
how many pre-entry retries occurred. Authoritative request history is retained
with the launch action and must recompute to the digest through its normal
retention interval, and `shadow_attempt` must be null. For `shadow_sample`,
`attempt_count` is exactly zero or one. Zero requires null `shadow_attempt`;
one requires a non-null byte-equal copy of its sole
`ZeroLocatorAttemptProofV1`, and `attempts_sha256` must recompute from the
one-element canonical array. Thus the event remains self-contained after the
shadow transaction deletes its owned sample/request; no digest loses its only
preimage.
`queued_unclaimed` is allowed only on the final current attempt: the request is
`PENDING`, generation zero, has a wholly null claim tuple and exact queued
delivery, zero normalized effects, and null quiescence. The retirement transaction
atomically writes its exact before-start `CANCELLED` result with literal
`interrupted_reason=resource_action_zero_locator_retirement`, deletes that
delivery through the terminal-delete guard, and records the pre-transition
proof. That retained literal makes the `queued_unclaimed` projection
recomputable after terminalization. `unclaimed_terminal` requires
`frozen_effect_count=0`, the canonical empty-set aggregate hash, and null
quiescence; `claimed_not_submitted` requires `frozen_effect_count=1`, the
aggregate hash of exact executor-fence effect zero, and the hash of the
byte-equal immutable `not_submitted` proof. The
complete compact proof is at most 65,536 canonical bytes.

- the replica's locator value is byte-equal to `[]`, every launch
  action/sample and normalized request-effect source also contains no locator,
  no terminal intent/snapshot exists, and no down action/sample exists;
- no same-hash global cluster record, provider operation ID, PREPARED child,
  provider callback child, or durable provider mutation effect exists; and
- every possible launch execution is closed by one of the exact never-started
  forms below. Absence of a thread, process, queue row, local handle, or cluster
  status is not a proof.

For an authoritative launch, every correlated attempt before the current one
must be terminal and must be either (a) generation zero with the complete
unclaimed tuple, zero effects, and the ordinary atomic before-start terminal
result, or (b) generation one with exactly its executor-fence effect zero and the
immutable `not_submitted`, `mutation_boundary=not_entered` request quiescence
proof. The current attempt must have one of those terminal shapes or the exact
`queued_unclaimed` shape above. Attempt zero with a `PLANNED` action and no
request is also eligible. There may be no running/claimed live request, and no
request remains live after the transaction. The transaction moves a
nonterminal launch from `PLANNED`, an exactly unstarted `QUEUED`, `VERIFYING`,
or `RETRY_WAIT` state to `SUPERSEDED`. For `PLANNED`/`QUEUED` it installs the
exact
`admission_fence/mutation_not_started/superseded_before_start` attempt result
with `interrupted/resource`, `observed_at=checked_at`, phase `pre_mutation`,
all provider-specific details null, empty index/window arrays, and the
zero-locator provider state; for
`VERIFYING`/`RETRY_WAIT` it preserves the immutable attempt result and any
execution quiescence, and replaces only provider state/top-level projections
as the compositional rules require.
An already
`TERMINAL_FAILED` or `SUPERSEDED` launch is accepted only when its retained
outcome and all attempts satisfy the same never-started proof. `RUNNING`,
`SUCCEEDED`, `deleted` certainty, an entered handler, `worker_exited`, a
PREPARED reference, or a missing prior attempt is ineligible.

For shadow, the sample is eligible when it has the exact existing
never-admitted `PLANNED` shape, or when its sole linked ordinary request is
either terminal generation zero with a wholly null claim tuple, no queue
delivery, and an exact before-dispatch cancellation/submit-failure result, or
is the exact generation-zero `PENDING`/queued/unclaimed shape that this
transaction terminalizes and removes atomically. Both request shapes require
no operation child, no PREPARED callback, and empty retained locator evidence.
A claimed legacy request has no authoritative quiescence column and is
therefore ineligible, even if its process is gone. An ineligible empty-locator
replica remains
locked/quarantined with `never_started_proof_missing`; cleanup does not create
a down sample, observe an empty set, delete by name, or remove the service.

On success the same transaction removes routing, closes the capacity and usage
intervals, removes the replica and any never-admitted shadow sample/ordinary
request in their declared retention order, and records the bounded
zero-locator-retirement operational event carrying the complete exact proof.
For a nonterminal authoritative launch, its `SUPERSEDED` outcome carries the
same proof under `provider_state.zero_locator_retirement`; an already terminal
launch remains immutable and the event is the retirement record. It may
conditionally remove only a
same-hash local reservation row whose state proves it was never published; a
different-hash row is untouched. It creates no down spec, down action, down
sample, terminal intent, terminal snapshot, observation, or provider call.
The retained authoritative launch action/request history (or the event after
the exact shadow deletion) is the audit proof. Full-service and failed-service
cleanup must select this branch before service deletion; only the nonempty
locator branch above may install a terminal tombstone and continue after the
service row is gone.

PREPARED and zero-locator retirement use the same domain/projection/action/
effect/request fences in the same order. If PREPARED commits first, the locator prefix is nonempty and
retirement must restart through normal down/observation cleanup. If retirement
commits first, the launch is terminal or removed and the pre-call append CAS
fails before `sky.execution.launch` or any request byte. No interleaving can
both retire the empty journal and enter the provider.

When the service row is absent, only this closed authority union may call the
provider. Authoritative reconciliation locks the retained replica, Serve
projection, then action and requires the intent ID to equal the action ID and
replica tombstone column.
Shadow reconciliation locks retained replica then sample in the declared
shadow order and requires the intent ID to equal the sample ID and replica
tombstone column. Both branches recheck all immutable identity, scope,
workspace, cluster, locator, and retirement fields against the byte-equal
committed snapshot, prove `routed=false`, and fence the separately protected
cluster record to that same incarnation. They compare the logical target to
the retained retirement snapshot rather than to a missing current service. A
new same-name service with another hash grants no authority and is not locked
as the owner. If any parent, tombstone, replica, locator, or cluster fence is
missing or inconsistent, reconciliation remains observation-only and performs
no mutation.

Loss of the live parent is never itself ownerless authority. A retained cleanup
may proceed only when its preexisting action or sample ID, exact terminal
snapshot, replica intent, and complete locator array satisfy the branch above.
A pre-M1 or truly unscoped orphan with no trustworthy service hash or
replica-incarnation UUID cannot fit action, sample, or divergence identity.
Recovery quarantines it, emits a bounded operator event/metric without the raw
provider record, and performs observation only; it never guesses from cluster
name or calls provider deletion. Operator repair must first establish the
scoped identity/tombstone or use the separately audited legacy manual-cleanup
path.

### PostgreSQL schema and migration ordering

Three additive migrations are required in exact order:
API-request 005 (independent generic kernel), Serve 032 (Serve identity/mode
dependency), then API-request 006 (Serve adapter projection/evidence).
API005 is independently installable and has no import, catalog check, or
foreign key to a Serve schema. Serve032 requires the exact API005 generic
capability marker. API006 requires both exact heads/markers and composes Serve
with the kernel. The composite runner uses
the exact advisory-lock key
`skypilot:alembic:resource-actions:v1`. PostgreSQL
`server_encoding=UTF8` is a hard storage invariant: before setting a GUC,
acquiring an advisory lock, opening a migration transaction, or executing any
DDL, the runner executes `SHOW server_encoding` and requires exactly `UTF8`.
`SQL_ASCII` and every other value are hard errors with zero schema changes.

After the process-wide Alembic lock, the runner checks out one dedicated
SQLAlchemy/DBAPI session from a `NullPool` engine in `AUTOCOMMIT` mode and polls
this nonblocking statement on that same session every 100 ms, up to an exact
600-second monotonic deadline:

```sql
SELECT pg_try_advisory_lock(
  hashtextextended('skypilot:alembic:resource-actions:v1', 0));
```

A false result retains neither a transaction nor a database lock. Cancellation
or expiry at 600 seconds raises a migration-lock-timeout error and closes only
that dedicated session. The winner keeps the DBAPI
session open, sets the session GUC
`skypilot.resource_action_migration_runner` to literal `v1`, switches that
same checked-out connection back to the engine's transactional isolation, and
places the same SQLAlchemy `Connection` in all three ordered Alembic Config
`attributes['connection']` entries. Each environment uses that supplied connection and
commits its own migration transaction; neither creates an engine or closes the
connection. The session-level exclusive migration advisory lock therefore
survives both commits and all postcondition checks. In a `finally` block the
runner returns the connection to `AUTOCOMMIT`, resets the GUC, calls
`pg_advisory_unlock(hashtextextended(<key>, 0))` on that same session and
requires a true result, then closes it.

The process-wide Alembic lock is always acquired before this database lock; no
section-specific migration lock may be acquired first. All PostgreSQL upgrade,
verify, and exceptional downgrade entry points that compose Serve 032/API006
use this runner. API005 may upgrade independently under the same generic lock;
a direct section-only crossing of Serve032 or API006 is unsupported and
refused by revision preflight. The GUC is a misuse guard, while the session
advisory lock provides serialization.

Each PostgreSQL revision preflight requires both the GUC and proof that its own
backend holds the exact exclusive advisory lock. For
`key = hashtextextended(<key text>, 0)`, the required granted `pg_locks` row has
`pid=pg_backend_pid()`, `locktype='advisory'`,
`classid=((key >> 32) & 4294967295)`,
`objid=(key & 4294967295)`, `objsubid=1`, `mode='ExclusiveLock'`, and
`granted=true`. The revision evaluates the key in PostgreSQL, compares those
unsigned 32-bit halves in the catalog, and refuses before its first DDL unless
exactly one matching current-backend row exists. A copied GUC on an unlocked
session is insufficient.

Every transaction that writes any API005/Serve032/API006-owned artifact first obtains
the writer shared transaction advisory lock
(`pg_advisory_xact_lock_shared(hashtextextended(<key>, 0))`) before any row
lock. This includes Serve mode/incarnation/cluster-hash/provider-locator/
owner-token/terminal-intent/parity fields,
actions/effects/ownership/adapters, shadow samples/operations/divergences,
correlated request fields, effect inserts/result fills, executor registration/freeze-ledger
transitions, and runtime instance registration/heartbeats.
Thus the exclusive migration lock either observes the writer's commit or runs
before the writer; it cannot split a cross-schema write. An ordinary request
that touches none of those artifacts does not pay this cost.
After taking the shared lock, every Serve032/API006 protected path also
requires the maintenance-owned, Serve032-created durable downgrade fence to be
absent. This includes
Serve adapter token publication, executor registration, promotion, claim,
window issue/result, shadow writes, projection reduction, and Serve-mode
changes. The Serve032/API006 guards repeat the current-schema-OID-resolved
check, so direct protected-row SQL cannot bypass it. API005 generic functions
never query that Serve-owned relation; their independent fence is the exact
current ownership activation epoch and API005 marker. The maintenance protocol
first drains/closes the Serve ownership scope and empties Serve generic history
before removing API006, so a later API005-only database remains valid for
unrelated domains. Runtime-instance publication/heartbeat additionally goes only
through its reviewed shared-lock store and cannot carry the v1 token while the
row exists. The structural verifier and direct-SQL truth tables require these
exact checks.

API-request 005 runs first and creates only the generic marker, ownership/
legacy-intent/runtime-adapter ledgers, action/effect/child-binding tables,
generic request-correlation/count/hash columns, kernel guards, indexes, and
dispatcher contract specified below. Its postverifier proves those objects
without opening or inspecting a Serve database. Only after that transaction
and postverification succeed does Serve schema 032 run second. In one
PostgreSQL transaction Serve032 adds:

Before its first DDL, Serve032 resolves the API Alembic history using its own
`ScriptDirectory`, proves exact API005 ancestry/head, and verifies the exact
`api_schema_capabilities` row
`('durable_resource_action_kernel', '005', 1)` plus all independent API005
postconditions. It does not infer readiness from a table name. This is the
one-way dependency: API005 never imports the Serve environment or checks a
Serve head.

```text
replicas.replica_incarnation_id UUID nullable during backfill
replicas.resource_action_cluster_hash UUID nullable
replicas.resource_action_provider_locators JSONB nullable
replicas.resource_action_terminal_intent_id UUID nullable
replicas.resource_action_terminal_snapshot JSONB nullable
services.resource_action_mode   TEXT NOT NULL DEFAULT 'legacy'
services.resource_action_owner_token UUID nullable
services.resource_action_parity_window_started_at TIMESTAMPTZ nullable

serve_schema_capabilities:
  capability            TEXT not null
  introduced_revision   TEXT not null
  contract_version      INTEGER not null
  installed_at          TIMESTAMPTZ not null default clock_timestamp()

serve_resource_action_downgrade_guard_state:
  singleton                         BOOLEAN primary key
  phase                             TEXT not null
  process_quiesce                   JSONB not null
  process_quiesce_sha256            TEXT not null
  deployment_inventory              JSONB not null
  deployment_inventory_sha256       TEXT not null
  guarded_backend_inventory         JSONB not null
  guarded_backend_inventory_sha256  TEXT not null
  guard_disable_evidence             JSONB nullable
  guard_disable_evidence_sha256      TEXT nullable
  opened_at                          TIMESTAMPTZ not null
  verified_at                        TIMESTAMPTZ nullable
```

Within that transaction the downgrade-state table and constraints are created
before either Serve trigger function, so the mode guard's fail-closed lookup
never references an absent relation. Backfill and marker insertion occur only
after both guard pairs are installed.

The mode constraint admits only `legacy`, `shadow`, or `authoritative`.
Backfill completion makes the incarnation non-null for every retained replica;
application writes require it from the first 032-capable image. The column is
not dropped on ordinary application rollback.

`resource_action_owner_token` is a random UUID minted whenever an HA parent
claims service ownership, retained across child-controller respawn, and
atomically replaced on parent recovery. Existing rows remain null/legacy until
a 032-aware owner claim. The named validated CHECK
`services_resource_action_owner_required` has exact expression
`resource_action_mode = 'legacy' OR resource_action_owner_token IS NOT NULL`;
PID/IP fields remain diagnostics. Shadow/authoritative transitions and every
private sample/action admission compare the current token under the service-row
lock. No token value is copied into durable samples, actions, requests, or
events.

The owner writer protocol is exact. Initial parent claim generates a fresh UUID
and installs it with PID/IP/port under the existing service hash/lifecycle
fence. Child-controller respawn retains that UUID. HA parent recovery performs
one CAS `WHERE hash = :hash AND resource_action_owner_token = :old_token AND
controller_pid = :old_pid AND controller_ip = :old_ip`, setting a fresh UUID,
new PID/IP, and null port before publishing a child. Every later owner/port
publication includes the new token predicate. In shadow/authoritative, clearing
or replacing a token is legal only through that handoff CAS; a zero-row result
grants no authority.

Serve032 also installs the exact PostgreSQL guard below, stored as the literal
`SERVE_RESOURCE_ACTION_MODE_GUARD_DDL_V1` in the checked-in versioned Serve
migration module. It makes mode monotonic in the database, not merely in store
code, and owns parity-window timestamps:

```sql
CREATE FUNCTION serve_resource_action_mode_guard_v1()
RETURNS trigger
LANGUAGE plpgsql VOLATILE SECURITY INVOKER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
  v_downgrade_guard_active boolean;
BEGIN
  EXECUTE pg_catalog.format(
    'SELECT EXISTS (SELECT 1 FROM %I.serve_resource_action_downgrade_guard_state)',
    TG_TABLE_SCHEMA)
    INTO v_downgrade_guard_active;
  IF v_downgrade_guard_active THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'resource action downgrade guard is active';
  END IF;
  IF TG_OP = 'INSERT' THEN
    IF NEW.resource_action_mode IS DISTINCT FROM 'legacy'
       OR NEW.resource_action_parity_window_started_at IS NOT NULL THEN
      RAISE EXCEPTION USING ERRCODE = '23514',
        MESSAGE = 'new service must start in legacy mode';
    END IF;
    RETURN NEW;
  ELSIF NEW.resource_action_mode = OLD.resource_action_mode THEN
    IF NEW.resource_action_mode = 'shadow' THEN
      IF OLD.hash IS NULL OR OLD.hash = '' THEN
        RAISE EXCEPTION USING ERRCODE = '23514',
          MESSAGE = 'shadow mode requires a stable service hash';
      ELSIF current_setting(
              'skypilot.resource_action_mode_transition', true)
            IS NOT DISTINCT FROM 'block:' || OLD.hash THEN
        NEW.resource_action_parity_window_started_at := NULL;
      ELSIF current_setting(
              'skypilot.resource_action_mode_transition', true)
            IS NOT DISTINCT FROM 'start:' || OLD.hash THEN
        IF OLD.resource_action_parity_window_started_at IS NOT NULL THEN
          RAISE EXCEPTION USING ERRCODE = '23514',
            MESSAGE = 'clean parity window already has a start';
        END IF;
        NEW.resource_action_parity_window_started_at := clock_timestamp();
      ELSIF NEW.resource_action_parity_window_started_at IS DISTINCT FROM
            OLD.resource_action_parity_window_started_at THEN
        RAISE EXCEPTION USING ERRCODE = '23514',
          MESSAGE = 'shadow parity timestamp is trigger-owned';
      END IF;
    ELSIF NEW.resource_action_parity_window_started_at IS NOT NULL THEN
      RAISE EXCEPTION USING ERRCODE = '23514',
        MESSAGE = 'parity window belongs only to shadow mode';
    END IF;
  ELSIF OLD.resource_action_mode = 'legacy'
        AND NEW.resource_action_mode = 'shadow' THEN
    IF OLD.hash IS NULL OR OLD.hash = ''
       OR current_setting('skypilot.resource_action_mode_transition', true)
         IS DISTINCT FROM 'activate:' || OLD.hash THEN
      RAISE EXCEPTION USING ERRCODE = '23514',
        MESSAGE = 'legacy to shadow requires fenced activation';
    END IF;
    NEW.resource_action_parity_window_started_at := NULL;
  ELSIF OLD.resource_action_mode = 'shadow'
        AND NEW.resource_action_mode = 'authoritative' THEN
    IF OLD.hash IS NULL OR OLD.hash = ''
       OR current_setting('skypilot.resource_action_mode_transition', true)
         IS DISTINCT FROM 'promote:' || OLD.hash
       OR OLD.resource_action_parity_window_started_at IS NULL
       OR clock_timestamp() <
          OLD.resource_action_parity_window_started_at + interval '86400 seconds'
       OR NEW.resource_action_parity_window_started_at IS NOT NULL THEN
      RAISE EXCEPTION USING ERRCODE = '23514',
        MESSAGE = 'shadow promotion lacks fenced parity window';
    END IF;
  ELSE
    RAISE EXCEPTION USING ERRCODE = '23514',
      MESSAGE = 'resource action mode cannot decrement or skip';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER trg_services_resource_action_mode_guard_v1
BEFORE INSERT OR UPDATE ON services
FOR EACH ROW EXECUTE FUNCTION serve_resource_action_mode_guard_v1();
```

Every inserted service therefore starts in `legacy`, regardless of application
defaults, deployment flags, or a supplied owner token. Shadow and
authoritative are reachable only through the explicit locked UPDATE
transitions below; an INSERT may not combine row creation with activation.
The transition store first takes the shared migration lock and service row
lock, sets the transaction-local hash-qualified GUC, and performs all blocker
writes/audits in that transaction. `block` is legal only for a new divergence,
unsampled mutation, or parity regression and clears clean-since. After every
blocker is resolved, `start` performs the full locked zero-blocker audit and
starts a new database-clock window. Direct SQL cannot pick a timestamp or
lower/skip mode. The final promotion audit uses `promote:<service_hash>` and
the same locked row. A null/empty service hash rejects every transition.

The table's primary key is named `pk_serve_schema_capabilities`, and its exact
validated positive CHECK is named
`ck_serve_schema_capabilities_contract_version_positive` with expression
`contract_version > 0`. Revision 032
inserts exactly this durable marker in the same transaction as its DDL:

```text
capability:          durable_serve_replica_actions
introduced_revision: 032
contract_version:    1
```

The marker is not an application environment variable or an inferred pair of
column names. Ordinary rollback retains it. The API and controller advertise
the orthogonal runtime capabilities `resource_action_kernel/v1` and
`serve_replica_action_adapter/v1`, plus the exact
`resource_action_adapter_manifest/v1:sha256:<digest>` attestation, only after
startup verification of this database marker and API-request 005. The kernel token requires the generic
store, dispatcher, deterministic attempt binding, and request integration. The
Serve token additionally requires the built-in exact projection/adapters,
locator/observer, protected pre-call callback, dedicated executor,
mutation-window/quiescence, RBAC, and effect-settlement implementations
described below to be registered with their literal v1 schemas. Per-service
promotion requires both and still re-verifies the selected target and bootstrap
profile against those facets and a recomputed manifest containing the exact
two Serve entries. No independent `serve_resource_locator/v1`
token exists, and no build version, action-kind string, or partial facet may
issue either token.

Serve 032 also creates the empty, singleton maintenance table
`serve_resource_action_downgrade_guard_state`. Its only legal row is the exact
closed `ResourceActionGuardDowngradeStateV1` object specified in the
exceptional-downgrade protocol below. `singleton` is literal true and has the
named primary key
`pk_serve_resource_action_downgrade_guard_state` and validated
`ck_serve_resource_action_downgrade_guard_singleton` with exact
`CHECK (singleton)`; `phase` is exactly `DRAINING` or `DISABLED`. Process quiesce, the
two inventories, and disable evidence use the PostgreSQL canonical bytes/hash
contract and are each at most 1,048,576 bytes. `DRAINING` has null disable
evidence/hash/verified time; `DISABLED` has all three non-null.
The named validated phase/shape constraint is
`ck_serve_resource_action_downgrade_guard_state_shape`.
No column has a default, generation, or identity expression. The table is
empty during every supported running deployment. Any row makes every v1-aware
API, controller, and executor refuse startup, runtime-capability advertisement,
and resource-action writes.

The exact checked-in `SERVE_RESOURCE_ACTION_DOWNGRADE_GUARD_DDL_V1` installs a
function `serve_resource_action_downgrade_guard_v1()` and one enabled
row-level `BEFORE INSERT OR UPDATE OR DELETE` trigger
`trg_serve_resource_action_downgrade_guard_v1`. INSERT is legal only to
the draining phase from the maintenance entry point while its current backend
holds the exclusive migration advisory lock and transaction-local
`skypilot.resource_action_downgrade_phase` equals literal
`OPEN_GUARD_DRAINING_V1`; the trigger owns `opened_at`. UPDATE is only the
draining-to-disabled evidence fill, requires that GUC equal literal
`VERIFY_GUARD_DISABLED_V1`, and owns `verified_at`.
DELETE is legal only from disabled after API 005 and the rest of Serve 032 are
already absent, with transaction-local GUC
`skypilot.resource_action_downgrade_finalize` equal to literal
`API004_SERVE031_GUARD_DISABLED_V1` and the same exclusive lock. Every
timestamp comes from one trigger-side `v_now`; a same-state write,
replacement/clearing, reverse edge, caller time, direct DELETE, or ordinary
migration is rejected. This guard pair deliberately survives the Serve 032
down transaction until the final maintenance transaction, so a crash cannot
erase the complete affected-target inventory before postverification.

Serve's official SQLite migration remains supported but is deliberately not a
resource-action capability. Its normal Serve migration runner adds compatible
nullable incarnation/tombstone storage and a legacy-only mode/default needed to read the
same model, but it does not use this PostgreSQL GUC or advisory lock, does not
create or populate `serve_schema_capabilities` or the downgrade-state
table/guards, and never permits `shadow` or `authoritative`. API-request 005
rejects every non-PostgreSQL dialect before
DDL, so a SQLite installation remains on the legacy Serve paths.

API-request schema 006 performs the following compositional structural verifier while the
exclusive migration advisory lock is held, before doing any DDL. Every catalog
lookup is restricted to `current_schema()` and resolved by relation OID, not
by an unqualified search-path match:

1. `SHOW server_encoding` is exactly `UTF8`.
2. `alembic_version_serve_state_db` contains exactly one row. The verifier
   builds `alembic.script.ScriptDirectory` from the same supplied Serve
   Alembic Config, requires exactly one script head, resolves the observed row
   to a real revision, and proves revision `032` is that revision or occurs in
   its `iterate_revisions(observed, 'base')` ancestry. Numeric/string `>= 032`
   comparison is forbidden.
3. `serve_schema_capabilities` has exactly the four columns above:
   `TEXT NOT NULL` primary-key capability, `TEXT NOT NULL` revision,
   `INTEGER NOT NULL` contract version with a validated `> 0` CHECK, and
   `TIMESTAMPTZ NOT NULL` installed time whose default expression is
   `clock_timestamp()`. The primary key and CHECK have exactly the names above,
   the expected constraint types/columns/normalized expressions, and
   `convalidated=true`; lookalike or additional PK/CHECK definitions fail.
4. The exact marker row above exists, with no duplicate possible under the
   primary key.
5. `replicas.replica_incarnation_id` has PostgreSQL type `uuid`, is nullable
   for the additive backfill phase, has no default, and is neither generated
   nor an identity column.
6. `replicas.resource_action_cluster_hash` has PostgreSQL type `uuid`, is
   nullable for legacy backfill, and has no default/generation/identity. Every
   v1 action-capable replica requires it and its value must equal the launch and
   down specs' `cluster_record_hash`.
7. `replicas.resource_action_provider_locators` has PostgreSQL type `jsonb`, is
   nullable for legacy backfill, and has no default/generation/identity. A new
   v1 replica starts with the exact empty array; later writes are canonical
   prefix appends of at most 32 exact `KubernetesProviderLocatorV1` objects.
   Every down spec copies the nonempty final array.
8. `replicas.resource_action_terminal_intent_id` has PostgreSQL type `uuid`, is
   nullable, has no default, and is neither generated nor an identity column.
9. `replicas.resource_action_terminal_snapshot` has PostgreSQL type `jsonb`, is
   nullable with no default/generation/identity, and when non-null passes the
   exact v1 object/hash/projection checks above and accompanies the same row's
   terminal intent ID. The named validated CHECK
   `replicas_resource_action_terminal_pair` has exact expression
   `(resource_action_terminal_intent_id IS NULL) =
   (resource_action_terminal_snapshot IS NULL)`; the verifier requires its
   normalized expression, columns, name, type, and `convalidated=true`.
10. `services.resource_action_mode` has PostgreSQL type `text`, is `NOT NULL`,
   has the catalog-normalized default `'legacy'::text`, and is neither
   generated nor an identity column. The named, validated CHECK
   `services_resource_action_mode_values` admits exactly `legacy`, `shadow`,
   and `authoritative` and no null or fourth value.
11. `services.resource_action_owner_token` has PostgreSQL type `uuid`, is
   nullable with no default/generation/identity. The verifier requires the
   exact named, validated `services_resource_action_owner_required` CHECK above.
12. `services.resource_action_parity_window_started_at` has PostgreSQL type
   `timestamptz`, is nullable with no default, and is neither generated nor an
   identity column. Named, validated CHECK
   `services_resource_action_parity_window_shadow_only` requires it null unless
   mode is `shadow`; promotion requires at least 86,400 database-clock seconds
   since its value.
13. `serve_resource_action_mode_guard_v1` has exact `prosrc` and catalog
   attributes, and exactly one enabled, non-internal row-level BEFORE INSERT OR
   UPDATE trigger (`tgtype=23`) with the exact name/function OID and normalized
   trigger definition above.
14. `serve_resource_action_downgrade_guard_state` has exactly the twelve columns,
    types, nullability, no-default/no-generation contract, named primary key,
    canonical JSON/hash/size constraints, and phase/shape CHECK above, and is
    empty for ordinary startup. Its exact checked-in guard function has the
    same immutable PL/pgSQL/search-path attributes as the mode guard and
    exactly one enabled, non-internal row-level BEFORE INSERT OR UPDATE OR
    DELETE trigger (`tgtype=31`) with the expected name/function OID and
    normalized definition. Maintenance resume uses the same structural
    verification but permits exactly one row in one of the two closed phases;
    an ordinary action-capable startup refuses rather than ignoring that row.

The same verifier, including `server_encoding=UTF8`, runs on every
action-capable API, executor, and controller process startup. A marker with
wrong structure, a pair of lookalike columns, an unvalidated constraint, a
wrong type/nullability/default, or a marker without the Serve Alembic head is a
hard error. API-request 006 therefore fails closed when ordering is violated.
Serve 032 writes its marker last inside its transactional migration, and
Alembic advances its version row in that same transaction, so the verifier can
never accept a partially installed Serve capability. API006 additionally
requires the exact API005 generic marker/head and its independent postverifier.

API-request schema 005 is the independently installable generic foundation. It
first creates exactly:

```text
api_schema_capabilities
  capability             TEXT primary key
  introduced_revision    TEXT not null
  contract_version       INTEGER not null check (contract_version > 0)
  installed_at           TIMESTAMPTZ not null default clock_timestamp()
```

It inserts
`('durable_resource_action_kernel', '005', 1)` only after every API005
object and structural check succeeds. All schema/version discriminator columns
are `INTEGER`. Attempts, generations, indexes, epochs, counters, transition
ordinals, and row revisions are `BIGINT`. API005 has no Serve import,
literal, catalog lookup, foreign key, trigger dependency, or downgrade-fence
lookup.

API005 creates these exact generic tables and extensions. Foreign keys are
added in dependency-safe order; the nullable legacy `action_id` reference is
added only after the action table exists.

```text
api_resource_action_ownership_epochs
  domain                         TEXT not null
  operation_subset               TEXT not null
  store_mode                     TEXT not null
  epoch                          BIGINT not null
  phase                          TEXT not null
  minimum_reader_schema          INTEGER not null
  adapter_name                   TEXT nullable
  adapter_version                INTEGER nullable
  adapter_implementation_sha256  TEXT nullable
  adapter_descriptor_sha256      TEXT nullable
  activation_evidence            JSONB nullable
  activation_evidence_sha256     TEXT nullable
  opened_at                      TIMESTAMPTZ not null
  closed_at                      TIMESTAMPTZ nullable
  row_revision                   BIGINT not null
  primary key (domain, operation_subset, store_mode, epoch)

api_resource_action_legacy_intents
  legacy_intent_id               UUID primary key
  domain                         TEXT not null
  operation_subset               TEXT not null
  store_mode                     TEXT not null
  ownership_epoch                BIGINT not null
  intent_token                   UUID not null
  intent_token_sha256            TEXT not null
  resource_identity_type         TEXT not null
  resource_identity_version      INTEGER not null
  resource_identity              JSONB not null
  resource_identity_sha256       TEXT not null
  request_id                     TEXT nullable references api_requests
  action_id                      UUID nullable references api_resource_actions
  readback_locator_type          TEXT nullable
  readback_locator_version       INTEGER nullable
  readback_locator               JSONB nullable
  readback_locator_sha256        TEXT nullable
  effect_certainty               TEXT nullable
  state                          TEXT not null
  row_revision                   BIGINT not null
  created_at                     TIMESTAMPTZ not null
  updated_at                     TIMESTAMPTZ not null
  completed_at                   TIMESTAMPTZ nullable

api_resource_actions
  action_id                      UUID primary key
  domain                         TEXT not null
  operation_subset               TEXT not null
  store_mode                     TEXT not null
  ownership_epoch                BIGINT not null
  resource_kind                  TEXT not null
  desired_generation             BIGINT not null
  action_kind                    TEXT not null
  adapter_name                   TEXT not null
  adapter_version                INTEGER not null
  adapter_implementation_sha256  TEXT not null
  adapter_descriptor_sha256      TEXT not null
  resource_identity_type         TEXT not null
  resource_identity_version      INTEGER not null
  resource_identity              JSONB not null
  resource_identity_sha256       TEXT not null
  domain_fence_type              TEXT not null
  domain_fence_version           INTEGER not null
  domain_fence                   JSONB not null
  domain_fence_sha256            TEXT not null
  reservation_identity_type      TEXT nullable
  reservation_identity_version   INTEGER nullable
  reservation_identity           JSONB nullable
  reservation_identity_sha256    TEXT nullable
  action_spec_type               TEXT not null
  action_spec_version            INTEGER not null
  action_spec                    JSONB not null
  action_spec_sha256             TEXT not null
  priority                       BIGINT not null
  requested_at                   TIMESTAMPTZ not null
  next_attempt_type              TEXT nullable
  next_attempt_version           INTEGER nullable
  next_attempt                   JSONB nullable
  next_attempt_sha256            TEXT nullable
  kernel_state                   TEXT not null
  current_attempt                BIGINT not null
  current_request_id             TEXT nullable
  correlation_root_request_id    TEXT nullable
  next_attempt_at                TIMESTAMPTZ nullable
  last_result_type               TEXT nullable
  last_result_version            INTEGER nullable
  last_result                    JSONB nullable
  last_result_sha256             TEXT nullable
  cleanup_required               BOOLEAN not null
  transition_ordinal             BIGINT not null
  row_revision                   BIGINT not null
  created_at                     TIMESTAMPTZ not null
  updated_at                     TIMESTAMPTZ not null
  completed_at                   TIMESTAMPTZ nullable

api_resource_action_effects
  action_id                      UUID not null references api_resource_actions
  attempt                        BIGINT not null
  effect_index                   BIGINT not null
  request_id                     TEXT not null
  parent_effect_index            BIGINT nullable
  facet_name                     TEXT not null
  facet_version                  INTEGER not null
  facet_implementation_sha256    TEXT not null
  effect_kind                    TEXT not null
  phase                          TEXT not null
  intent_type                    TEXT not null
  intent_version                 INTEGER not null
  intent                         JSONB not null
  intent_sha256                  TEXT not null
  idempotency_key                TEXT not null
  readback_locator_type          TEXT nullable
  readback_locator_version       INTEGER nullable
  readback_locator               JSONB nullable
  readback_locator_sha256        TEXT nullable
  state                          TEXT not null
  effect_certainty               TEXT nullable
  provider_request_id            TEXT nullable
  provider_operation_id          TEXT nullable
  result_type                    TEXT nullable
  result_version                 INTEGER nullable
  result                         JSONB nullable
  result_sha256                  TEXT nullable
  evidence_type                  TEXT nullable
  evidence_version               INTEGER nullable
  evidence                       JSONB nullable
  evidence_sha256                TEXT nullable
  execution_generation           BIGINT not null
  claim_token_sha256             TEXT not null
  worker_instance_id             UUID not null references api_server_instances
  claim_authority_evidence_sha256 TEXT not null
  row_revision                   BIGINT not null
  created_at                     TIMESTAMPTZ not null
  updated_at                     TIMESTAMPTZ not null
  completed_at                   TIMESTAMPTZ nullable
  primary key (action_id, attempt, effect_index)

api_resource_action_child_requests
  action_id                      UUID not null references api_resource_actions
  attempt                        BIGINT not null
  effect_index                   BIGINT not null
  child_slot                     BIGINT not null
  parent_attempt_request_id      TEXT not null
  child_request_id               TEXT not null unique references api_requests
  payload_sha256                 TEXT not null
  workspace_sha256               TEXT not null
  actor_sha256                   TEXT not null
  created_at                     TIMESTAMPTZ not null
  primary key (action_id, attempt, effect_index, child_slot)

api_resource_action_runtime_adapters
  instance_id                    UUID not null references api_server_instances
  adapter_name                   TEXT not null
  adapter_version                INTEGER not null
  implementation_sha256          TEXT not null
  descriptor_sha256              TEXT not null
  manifest_sha256                TEXT not null
  action_kinds                   JSONB not null
  capabilities                   JSONB not null
  registered_at                  TIMESTAMPTZ not null
  primary key (instance_id, adapter_name, adapter_version)
```

API005 extends `api_requests` with these nullable, no-default columns:

```text
resource_action_id                    UUID nullable
resource_action_attempt               BIGINT nullable
resource_action_payload_sha256        TEXT nullable
resource_action_effect_count          BIGINT nullable
resource_action_effects_sha256        TEXT nullable
resource_action_quiescence_type       TEXT nullable
resource_action_quiescence_version    INTEGER nullable
resource_action_execution_quiescence  JSONB nullable
resource_action_quiescence_sha256     TEXT nullable
```

Every request-ID-valued column is `TEXT`, matching PR #1070's
`api_requests.request_id`; a correlated value is additionally constrained to
the canonical lowercase UUIDv5 text derived below. API005 adds the
request-to-action foreign key on `api_requests.resource_action_id`, a partial
unique constraint on `(resource_action_id, resource_action_attempt)` where
`resource_action_id IS NOT NULL`, and a unique constraint on
`(resource_action_id, resource_action_attempt, request_id)` as the composite
foreign-key target. `api_resource_action_effects` has the composite foreign key
`(action_id, attempt, request_id) ->
api_requests(resource_action_id, resource_action_attempt, request_id)`.
`api_resource_action_child_requests` has both
`(action_id, attempt, effect_index) ->
api_resource_action_effects(action_id, attempt, effect_index)` and
`(action_id, attempt, parent_attempt_request_id) ->
api_requests(resource_action_id, resource_action_attempt, request_id)`; its
unique `child_request_id` separately references `api_requests.request_id`.
These constraints make a cross-action or cross-attempt effect/child binding
structurally impossible. The action's `current_request_id` remains a guarded
partial-unique pointer, not a reverse foreign key.

It also adds only
`api_server_instances.runtime_capabilities JSONB NOT NULL DEFAULT '[]'`.
This is generic fleet attestation, not a worker lease. Runtime adapter rows
have no heartbeat or expiry; freshness and draining come only from the
referenced instance row. Their `capabilities` are a sorted unique exact subset
of `dispatch|claim_authority|execute|reduce|reconcile|read`; action kinds are
also sorted and unique. The instance must publish the kernel runtime token and
a digest-bearing manifest attestation whose SHA equals every selected
registration's `manifest_sha256`. There is no separate instance manifest
digest column or third representation.

An ownership epoch is activation authority, never execution ownership. Its
phase is exactly `LEGACY_OPEN|DRAINING|ACTION_OPEN`; a partial unique index
permits one `closed_at IS NULL` row per
`(domain, operation_subset, store_mode)`. A phase change closes epoch N and
inserts N+1 atomically. Adapter identity, implementation digest, descriptor
digest, and activation evidence are all-null in `LEGACY_OPEN`, follow the
closed DRAINING matrix, and are all non-null in `ACTION_OPEN`. An action binds
that exact scope and epoch.

Legacy intents are typed cutover proofs, not another queue. Their only states
are `ACTIVE|READBACK|TERMINAL`. Identity/token/scope are immutable.
The locator quartet is all-null before `READBACK`, then all-nonnull and
append-once. Optional request/action IDs become immutable when set.
`completed_at` is non-null exactly in `TERMINAL`; effect certainty follows
the closed state matrix. A matching nonterminal legacy intent blocks action
admission.

The action owns generic mechanics only. Every typed value uses its adjacent
type/version/JSON/SHA quartet; each nullable quartet is all-null or
all-nonnull. The exact natural unique key is
`(domain, operation_subset, store_mode, action_kind,
resource_identity_sha256, desired_generation)`. `current_request_id` has a
partial unique index when non-null but no reverse foreign key to
`api_requests`; the request-to-action foreign key is the sole direction, so
there is no cycle. The generic row has no action-level effect phase. Provider
progress exists only in normalized effect rows. `last_result` is a bounded,
typed mechanics disposition and evidence digest; it never contains a Serve
state or Serve outcome.

The generic state graph is exactly:

```text
READY       -> QUEUED | BLOCKED | TERMINAL
QUEUED      -> RUNNING | REDUCING
RUNNING     -> REDUCING
REDUCING    -> READY | BLOCKED | TERMINAL
BLOCKED     -> READY | TERMINAL
TERMINAL    -> (none)
```

`READY` has a complete next-attempt quartet and non-null
`next_attempt_at`. `QUEUED|RUNNING|REDUCING|BLOCKED|TERMINAL` have the
next-attempt quartet and due time all-null. `BLOCKED` means no automatic safe
progress: no dispatcher, timer, or observation worker may act until a separate
domain transaction supplies new evidence and performs `BLOCKED -> READY` with
a complete primary-attempt descriptor, or `BLOCKED -> TERMINAL`.
`QUEUED|RUNNING` name the current correlated request. `REDUCING` requires
that request terminal with frozen effect count/hash. `TERMINAL` requires
completion time and a typed final mechanics result; every other state has null
`completed_at`. Every state edge increments `transition_ordinal`; every row
mutation increments `row_revision`.

Every automatic provider observation or readback is a new primary correlated
`READY` attempt. It uses the same request row, queue, generation-one claim,
lease, effects, terminalizer, and reducer as a mutation attempt; Serve may keep
its projection in `VERIFYING` while generic state advances
`READY -> QUEUED -> RUNNING -> REDUCING`. API005 has no readback-child
scheduler, and Serve v1 creates no nested child request.

Primary attempt IDs are exact UUIDv5 values:

```text
namespace = action_id
name = "skypilot.resource-action.attempt.v1/" +
       canonical_positive_decimal(attempt)
```

A true child's ID is:

```text
namespace = UUID(parent_attempt_request_id)
name = "skypilot.resource-action.child-request.v1/" +
       canonical_nonnegative_decimal(effect_index) + "/" +
       canonical_nonnegative_decimal(child_slot)
```

API005 installs checked-in, extension-free PostgreSQL SHA-1 and UUIDv5 helper
functions solely to recompute these identities in guards. PostgreSQL 14 has no
core SHA-1 function, and the central database must not depend on optional
`uuid-ossp` or `pgcrypto` installation/extension privileges. The literal
helpers are immutable and strict, use no relation or dynamic SQL, are covered
by RFC/Python cross-language vectors, and are exact-definition artifacts of
the API005 structural verifier. They are not general credential or content
hashing APIs; persisted content continues to use SHA-256.

The namespace conversion occurs only after validating the parent as canonical
lowercase UUID text. Canonical decimal text has no sign, leading zero,
whitespace, exponent, or alternate Unicode digits. A child request's own
`resource_action_*` columns
are all null. Binding, child request, and queue delivery commit atomically;
reuse requires byte-equal payload/workspace/actor digests. Serve v1 creates no
child.

For an ordinary request all nine correlation/quiescence fields are null. A
primary correlated request has the first five non-null at admission:
positive attempt, `resource_action_effect_count=0`, and the canonical empty
array SHA-256
`4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945`.
The quiescence quartet starts all-null and may make one append-only
all-null-to-all-nonnull transition only after terminal effect freeze. A
correlated request has `execution_generation=0` before claim and exactly
`0 -> 1` at claim; generation two is forbidden. It always has
`should_retry=false` and `retryable=false`, is never requeued, and is never
reopened. The action reducer creates a new immutable primary request for a
later attempt.

Effects are contiguous `0..count-1`, with at most 2,048 rows
(`effect_index 0..2047`) and at most 16,777,216 PostgreSQL-canonical aggregate
bytes. Their composite primary key is
`(action_id, attempt, effect_index)`; there is no effect ID or plan index.
`request_id` equals that attempt's primary request. A non-null parent index is
strictly lower. State is exactly
`PRE_INTENT|IN_FLIGHT|READBACK|SETTLED|QUARANTINED`.
Intent and idempotency are immutable. The readback-locator quartet is optional
for fences and non-provider effects, but a mutating effect must have it
all-nonnull before entering `IN_FLIGHT`. Certainty, provider IDs, result
quartet, and evidence quartet are null until their exact append-once edge.
Every effect has `execution_generation=1`, exact claim-token SHA, worker
instance, and claim-authority-evidence SHA. Insert/result/evidence fills and
the request effect count/aggregate update commit atomically.

After API005 and Serve032 both postverify, API006 creates the Serve adapter
marker and 1:1 projection:

```text
api_serve_replica_action_projections
  action_id                    UUID primary key
                               references api_resource_actions ON DELETE RESTRICT
  service_name                 TEXT not null
  service_hash                 TEXT not null
  replica_id                   BIGINT not null
  replica_incarnation_id       UUID not null
  action_type                  TEXT not null
  origin                       TEXT not null
  service_lifecycle_epoch      BIGINT not null
  serve_state                  TEXT not null
  serve_last_outcome           JSONB nullable
  serve_completed_at           TIMESTAMPTZ nullable
  row_revision                 BIGINT not null
  created_at                   TIMESTAMPTZ not null
```

All Serve identity, planning fences, lifecycle states
(`PLANNED|QUEUED|RUNNING|VERIFYING|RETRY_WAIT|SUCCEEDED|TERMINAL_FAILED|
SUPERSEDED`), and typed Serve outcomes live only in this projection. API006
maps its exact typed Serve intent to the generic effect intent quartet, its
optional provider readback locator to the generic locator quartet, and its
append-once provider response and observation proof to the generic result and
evidence quartets plus the generic provider request/operation IDs,
facet identity, effect kind, and phase. There is no mutable combined
`domain_projection` column or object.

The dispatcher never imports or queries the projection. The sole due index is
partial on `kernel_state='READY'` and ordered exactly:

```text
(next_attempt_at ASC, priority DESC, requested_at ASC, action_id ASC)
```

Candidate discovery considers only locally registered action kinds. Before
materialization it nonlockingly requires the dispatching instance's own fresh,
ready, non-draining instance row and byte-equal action-bound adapter
registration/manifest with `dispatch`. Independently, it requires at least one
fresh compatible registration with both `execute` and `claim_authority`; role
split is valid and the dispatcher itself need not advertise `execute`. If
either condition is absent, the action remains `READY`.
The short materialization transaction then locks only
`current ownership epoch -> action`, revalidates `ACTION_OPEN`, descriptor
digest, `kernel_state=READY`, due time, revision, and next-attempt quartet,
and creates/reuses the deterministic request plus queue. It never takes a
registration lock after the action. Claim later rediscovers and locks its
chosen adapter authority before action through the two-stage hook.

Required generic uniqueness and guard coverage includes the ownership partial
unique index, exact action natural key, partial unique current request ID,
request-to-action correlation, contiguous effects and aggregate limits, child
identity, exact state graph/shapes, typed quartet all-or-none/hash/size checks,
and the sole partial READY due index above. API006 separately guards its
projection identity and the allowed lag matrix; it cannot weaken API005.

`ResourceActionAdapterV1` is the only generic-to-domain plug point. A
process-local registry accepts one exact adapter per `action_kind`; duplicate
or alias registration is fatal. Each adapter provides closed envelope/schema
validators, one immutable request-materialization descriptor, a
two-stage connection-borrowing claim-authority hook, a pre-call fence
validator, an attempt-result reducer, and a projection/event callback. It
provides no worker, due-admission callback, query, session, clock, lease, or
provider registry. The shared dispatcher imports only that registry interface
and can read only the immutable materialization descriptor; registration
imports the built-in Serve adapters from the composition root.

The claim hook exists because API005 cannot import or understand API006's
one-use Serve executor-registration ledger, yet consuming that authority and
creating effect zero must be atomic with request claim. After a nonlocking
candidate/adapter resolution, the generic claim transaction locks the current
ownership epoch, then calls
`lock_claim_authority(connection, candidate_ref)`. That hook may lock only the
adapter-declared claim-authority row and returns an opaque prelocked handle.
The kernel then locks `action -> request -> queue`, CAS-validates the exact
attempt, generation-zero request, current epoch, adapter implementation and
claim predicate, and calls
`consume_claim_authority(connection, prelocked_authority, locked_rows)`.
The second hook takes no new lock and returns an exact bounded
`ClaimEffectMaterializationV1` containing the typed effect-zero envelopes,
facet identity, claim-token digest, worker instance, and canonical SHA. The
kernel validates the generic envelope/hash/size contract, inserts effect zero,
updates the request effect count/aggregate, installs generation one and the
request lease, advances `kernel_state` to `RUNNING`, and commits all changes
together. API006 guards validate the opaque Serve
`ProviderExecutorFenceV1` content and consume its ledger row. Any CAS failure,
hook error, or transaction rollback leaves both the registration and request
unconsumed. The hook performs no I/O, opens no session, and owns no lease.

Thus the exact correlated claim lock order is
`ownership epoch -> adapter claim authority -> action -> request -> queue ->
effects`. An ordinary generic non-provider effect write may use
`ownership epoch -> action -> request -> effects`. Every authoritative Serve
mutation-window issue or result fill instead uses
`ownership epoch -> executor registration -> Serve/domain rows -> action ->
request -> effects` (and instance last if needed), after nonlocking key
discovery. Heartbeat uses the request claim predicate alone. The registered descriptor digest covers both hook
implementations and `ClaimEffectMaterializationV1`; the runtime adapter must
publish both `execute` and `claim_authority` before the kernel will select it
for claim.

The registry is extensible but never last-registration-wins. It permits future
non-Serve kinds with distinct names and includes every exact
`(adapter_name, adapter_version, implementation_sha256, descriptor_sha256,
sorted_action_kinds, sorted_capabilities)` tuple in one sorted
adapter-manifest digest.
Each instance publishes
`resource_action_adapter_manifest/v1:sha256:<digest>` alongside the kernel
token. Unknown action kinds remain untouched by that instance's due query and
emit a fail-closed health signal; they do not make the generic base
Serve-only. Issuing `serve_replica_action_adapter/v1` requires that the
manifest contain exactly one byte-matching registration for each of the two
required Serve kinds and no alias or duplicate for either. Extra disjoint
future-domain entries are allowed and change the manifest digest; they cannot
replace, broaden, or impersonate a Serve adapter.

Runtime publication uses two orthogonal capabilities plus the manifest
attestation described above:
`resource_action_kernel/v1` proves the generic table/dispatcher, deterministic
attempt binding, and PR #1070 request integration; and
`serve_replica_action_adapter/v1` proves the exact projection, Serve adapters,
outcome matrix, provider-evidence prerequisites, and schema guards in this
design. Authoritative promotion requires both on every relevant API,
controller, and executor role, plus a fresh adapter-manifest token whose digest
recomputes from that process's registry. Neither token implies the other, and
a generic kernel may not advertise the Serve token merely because an action
kind string is registered.

API-request 006 later adds
`api_server_instances.resource_action_executor_registration_id UUID NULL` with
no default or generated value and a foreign key to the durable registration
ledger below.
The database CHECK enforces an array of at most 32 `Token[128]` strings;
existing rows backfill to empty. Registration/heartbeat code canonicalizes it
to sorted unique order. Because PostgreSQL CHECK expressions cannot expand an
array to prove ordering/uniqueness, activation treats values as a set and
rejects a duplicate, unsorted, malformed, or over-limit row; sorted/unique is
not falsely claimed as CHECK-enforced. This is the sole durable
fleet-advertisement surface. A
process includes both `resource_action_kernel/v1` and
`serve_replica_action_adapter/v1` plus its exact adapter-manifest attestation
in its registration and each heartbeat only
after their respective startup verifiers pass; failure or later loss of either
verification makes the process unready and removes the affected token rather
than leaving a stale claim. Activation queries fresh, non-draining instance
rows for every controller/executor-capable role and requires all three
publications. Build
versions, environment variables, and HTTP self-claims are not capability
evidence.
The nullable registration ID is non-null only on a dedicated executor instance
that advertises that token. Registration, promotion, and every
claim/mutation-window transaction lock its referenced ledger row and perform
the full closed-schema/hash/freshness/purpose/consumption verification.
Controller-only rows keep it null. The API005 structural verifier requires the
generic runtime-capability and manifest-attestation columns; the API006
verifier requires the exact registration-ID
type/nullability/default/generation/foreign-key contract.
Downgrade removes the registration ID/FK with API006 and retains
`runtime_capabilities` until generic API005 is removed last.

API-request 006 creates the durable Serve executor-consumption ledger before adding
that foreign key:

```text
api_resource_action_executor_registrations
  registration_id          UUID primary key
  worker_instance_id       UUID not null unique
  pod_namespace            TEXT not null
  pod_name                 TEXT not null
  node_name                TEXT not null
  node_boot_id_sha256      TEXT not null
  purpose                  TEXT not null
  target_cluster_sha256    TEXT not null
  target_kube_system_uid   TEXT not null
  nonce_sha256             TEXT not null unique
  nonce_issued_at          TIMESTAMPTZ not null
  admission_freeze_id      UUID not null unique
  admission_freeze_opened_at TIMESTAMPTZ not null
  admission_freeze_mode    TEXT not null
  admission_freeze_prefreeze_settlement_seconds BIGINT not null
  admission_freeze_snapshot_not_before TIMESTAMPTZ not null
  postwait_challenge_sha256 TEXT nullable unique
  postwait_challenge_issued_at TIMESTAMPTZ nullable
  admission_barrier_drained_at TIMESTAMPTZ nullable
  admission_barrier_evidence_set_sha256 TEXT nullable
  admission_cache_snapshot_not_before TIMESTAMPTZ nullable
  postbarrier_challenge_sha256 TEXT nullable unique
  postbarrier_challenge_issued_at TIMESTAMPTZ nullable
  admission_freeze_release_not_before TIMESTAMPTZ nullable
  admission_freeze_released_at TIMESTAMPTZ nullable
  state                    TEXT not null
  registered_at            TIMESTAMPTZ nullable
  capability               JSONB nullable
  capability_sha256        TEXT nullable
  consumed_at              TIMESTAMPTZ nullable
  consumption              JSONB nullable
  expired_at               TIMESTAMPTZ nullable
  expiration_reason        TEXT nullable
  release_evidence         JSONB nullable
  release_evidence_sha256  TEXT nullable
  created_at               TIMESTAMPTZ not null
  updated_at               TIMESTAMPTZ not null
```

`worker_instance_id` equals the executor Pod UID and the referenced
`api_server_instances.instance_id`. The ledger has exact unique
`(registration_id, worker_instance_id)`, and the composite FK from instance
columns `(resource_action_executor_registration_id, instance_id)` is named
`fk_api_server_instances_resource_action_executor_registration_v1` and is
`ON DELETE RESTRICT`; a mismatched worker pairing is structurally impossible.
Purpose is exactly `readiness` or `action`; target,
identity, nonce, freeze mode/timing, and purpose are immutable. SHA fields have
the exact lowercase 64-hex CHECK, identity/name fields have the shared bounded
domains, capability, consumption, and release evidence are each at most 65,536
PostgreSQL-canonical bytes. Consumption/release evidence are the exact closed objects in
`ResourceActionExecutorCapabilityRegistrationV1` and
`ExecutorRegistrationReleaseEvidenceV1`; capability and release evidence have
their sibling canonical SHA fields checked.
`state=VERIFYING` has the freeze identity/mode/open/snapshot fields non-null.
Its verification prefix is exact: the post-wait challenge pair is installed
first; the barrier-drained/evidence/cache-not-before triple next; and the
post-barrier challenge pair last. Each suffix is either wholly null or all
prior stages are non-null. Release-not-before/released,
registered/capability/consumption/expiration/release-evidence fields are null.
`READY` has the complete two-challenge/barrier prefix,
registered/capability/hash, and the SQL-derived
freeze release-not-before non-null, with
consumption/expiration/released/release-evidence fields null.
`CONSUMED` adds the exact non-null consumed-at/consumption pair and keeps
expiration/released/release-evidence null. `EXPIRED` is reachable only from unconsumed
`VERIFYING` or `READY`, has SQL-owned non-null `expired_at`, freeze
release-not-before, and literal reason `nonce_timeout`, `capability_timeout`,
or `executor_drained_before_consume`, and keeps
consumption/released/release-evidence null; its
capability triple is respectively all null or the immutable prior READY
triple. `RELEASED` is reachable only from `CONSUMED` or `EXPIRED`, preserves
every prior byte, and adds only the SQL-owned
`admission_freeze_released_at=v_now` and exact release-evidence/hash pair after
the release protocol below proves its complete time, Pod, credential, and
request conditions.

The unique Pod-UID row is a tombstone, not an instance-heartbeat cache. A
restart in the same still-live Pod may rediscover the existing row but cannot
insert, reset, or replace it, reread a projected token for another
registration, or claim after `CONSUMED`/`EXPIRED`/`RELEASED`. V1 never deletes
an executor-registration row; its unique Pod-UID/nonce consumption tombstone
is retained indefinitely, including after instance-row GC and action/service
history retention.

Promotion additionally requires
at least one fresh, non-draining, single-use readiness-executor row for the
selected target. Every selected row must carry an unconsumed
`state=READY,purpose=readiness` registration whose exact capability has that target
fingerprint/profile. The promotion transaction locks at least one such row and
atomically changes only `state` from `READY` to `CONSUMED`, `consumed_at` from
null to its one SQL-owned `v_now`, and `consumption` from null to
the exact `service_promotion` object with current service hash/lifecycle epoch,
null action/request/attempt, and `audit_sha256` over the complete locked
promotion audit plus registration ID/nonce/capability hash/target; every other
registration byte is immutable. That consumed ledger row is the durable
promotion record. Their verification timestamps, watch bases, and hashes remain
per-executor and need not be byte-equal. A runtime token without that nonempty
target-bound consumed set is insufficient, and one consumed readiness nonce
cannot promote another service or be reset. Those rows prove only the locked
promotion audit and drain immediately afterward. A later action claim must
register and lock its own new unconsumed `purpose=action` one-request executor
row and atomically consume it for the exact service/action/request/attempt; it
stores the current lifecycle epoch and an audit hash over those locked rows,
registration identity, credential, and capability. It cannot use or rewrite a
readiness executor. Every mutation-window transaction
requires that byte-equal `request_claim` consumption. Cross-purpose, duplicate,
null-to-different, consumption clearing, and second-consumer writes are
database-guarded rejections.
Registration is a closed sequence of fenced transitions. A short transaction locks the
fresh instance row, fixes purpose and target, obtains a cryptographically
random nonce and freeze UUID from PostgreSQL, returns the nonce's one-use
preimage, and stores only its hash with
`nonce_issued_at=admission_freeze_opened_at=v_now`, the exact allowlisted freeze
mode and a SQL-owned pre-freeze settlement bound of exactly zero seconds for
`dynamic_engines_disabled` or exactly 120 seconds for
`static_manifest_guard`,
`admission_freeze_snapshot_not_before=v_now + prefreeze_settlement_seconds`,
`state=VERIFYING`, and all challenge/registered/capability/consumption fields
null. The verifier may use that first nonce only to identify the open freeze;
it cannot collect finalizable provider evidence yet.

At or after `admission_freeze_snapshot_not_before`, a second short SQL
transaction locks the same still-`VERIFYING` row, captures its own `v_now`,
generates a new cryptographically random one-use post-wait challenge, stores
only its hash with `postwait_challenge_issued_at=v_now`, and returns the raw
challenge exactly once. It also requires
`v_now <= nonce_issued_at +
make_interval(secs => prefreeze_settlement_seconds + 60)`; the row lock
serializes challenge issuance against every later stage/expiry edge, so a late verifier cannot
win after the 180-second first-stage maximum. The guard permits that sole null-to-pair
same-state edge and rejects a late issuance, replay, or replacement. The
supervisor reads its one fixed Pod-bound token after this edge, opens direct
barrier connections, and binds every per-backend barrier request/response to
the nonce and post-wait challenge.

After every guarded backend reports its sealed old epoch drained, another short
transaction locks the row, receives the bounded raw barrier/audit projections,
recomputes the exact sorted set hash including one byte-equal fixed-token hash
across every backend, captures `v_now`, and requires
`v_now <= nonce_issued_at +
make_interval(secs => prefreeze_settlement_seconds + 60)`. Guarded mode requires
exactly the pinned backend set; disabled mode requires the canonical empty set.
It installs only
`admission_barrier_drained_at=v_now`,
`admission_barrier_evidence_set_sha256`, and
`admission_cache_snapshot_not_before=v_now + interval '60 seconds'` in guarded
mode or `=v_now` in disabled mode. The extra 60 seconds is the allowlisted
maximum admission-cache propagation interval measured from actual barrier
drain, not freeze open.

At or after that cache deadline, a fourth short transaction locks the
still-`VERIFYING` row, requires
`v_now <= nonce_issued_at + interval '300 seconds'`, issues a fresh
cryptographically random post-barrier challenge, persists only its hash and
SQL issuance time, and returns its preimage once. Every TLS connection used for
finalizable verification is opened after this edge. Every TokenReview,
backend/version/configz, EndpointSlice, admission snapshot, dry-run, guard
probe, and watch-basis request carries a unique marker derived from the
registration nonce and both challenges; each authenticated response/audit
projection must echo and bind it. LIST/GET reads use the literal
MostRecent/no-cache contract. Finalization receives the bounded raw
response/audit projections and recomputes their canonical hashes and all
challenge derivations server-side. Because the final preimage does not exist
until after both the barrier drain and its cache wait, holding any earlier
response and submitting it later cannot satisfy finalization.
A finalization transaction locks the same still-fresh row, requires the same
purpose/target/nonce/two challenges/barrier set and live gap-free watches, captures one new SQL
`v_now`, fills the exact capability and canonical hash, sets
`registered_at=verification.verified_at=v_now`, derives
`verification.expires_at` and the admission-freeze release lower bound as
specified below, and changes only
`VERIFYING -> READY`. It additionally requires
`v_now <= nonce_issued_at + interval '300 seconds'`; the closed budget is
120 seconds to first challenge, up to 60 seconds to drain/record the barrier,
60 seconds of post-drain cache settlement, and up to 60 seconds for final
evidence/finalization. An expired nonce is terminally discarded, never
refreshed in place. `READY` has null consumption.
Promotion or claim changes
only `READY -> CONSUMED` and the two null consumption fields using its one
transaction timestamp. The separate exact release edge changes
`CONSUMED/EXPIRED -> RELEASED` only after its safety checks. No heartbeat can
alter nonce, purpose, target, freeze identity/timing/barrier/challenges,
capability, registration time, expiry, state, or consumption.
The object is target-bound evidence folded under the Serve-adapter capability,
not a third runtime capability or an independent activation/claim authority.

API-request 006 creates the outer-attempt journal
`api_resource_action_shadow_samples` with exactly these columns:

```text
sample_id                  UUID primary key
service_name               TEXT not null
service_hash               TEXT not null
replica_id                 BIGINT not null
replica_incarnation_id     UUID not null
action_type                TEXT not null
capacity_context           TEXT not null
retirement_context         TEXT not null
execution_context          TEXT not null
sample_origin              TEXT not null
legacy_attempt             BIGINT not null
attempt_driver             TEXT not null
legacy_request_id          TEXT nullable references api_requests ON DELETE RESTRICT
legacy_request_status      TEXT nullable
legacy_request_input_hash    TEXT nullable
legacy_request_terminal_hash TEXT nullable
action_spec                JSONB nullable
spec_hash                  TEXT nullable
legacy_input_hash          TEXT nullable
legacy_outcome             JSONB nullable
parity_state               TEXT not null
row_revision               BIGINT not null
created_at                 TIMESTAMPTZ not null default clock_timestamp()
updated_at                 TIMESTAMPTZ not null default clock_timestamp()
completed_at               TIMESTAMPTZ nullable
```

The identity/name bounds, exact launch/down schema, PostgreSQL canonical byte
limit/hash, and exact typed outcome contract are the same as an action row.
The three context columns use the exact `ActionDecisionProvenanceV1` axes and
must equal the spec; their action-type all-or-none rules above eliminate
overlap.
`sample_origin` is exactly `audited` or `pre_m1_recovery`, and
`attempt_driver` is exactly `ordinary_request`, `controller_direct`, or
`observation_only`. `legacy_request_status` is null or `PENDING`, `WAITING`,
`RUNNING`, `SUCCEEDED`, `FAILED`, or `CANCELLED`; all hashes are `Sha256`. A
non-null request ID is unique across samples. A unique constraint covers
resource/action identity plus `legacy_attempt`; a partial unique index permits
at most one nonterminal audited sample for that identity. `row_revision` starts
at zero and every permitted sample update increments it by exactly one.

An `audited` row has a positive `legacy_attempt`, non-null exact action
spec/hash, and begins `PLANNED`. Attempts are contiguous from one under the
resource-identity lock. Inserting `N` locks `N-1` and is legal only when `N-1`
is terminal and has the same immutable spec/hash plus the provider certainty
that would permit a future action retry. The caller preallocates `sample_id`
before its state transaction. Recovery that finds the partial-unique
nonterminal sample compares every immutable identity, context axis, spec, and
hash and reuses that ID; it cannot allocate `N+1`. A conflict is a divergence,
not permission to create a replacement. An `ordinary_request` sample owns
exactly one ordinary request. ID, status, and input hash are all null before
binding and all non-null afterward; terminal hash is null until
the request becomes terminal and is then non-null. A `controller_direct` or
`observation_only` sample always has all four null. The input hash covers
the immutable ordinary request fields listed under request binding below. The
terminal hash additionally covers exact status/result evidence as defined below;
request retention cannot remove or rewrite that row while the sample remains.

A `pre_m1_recovery` row is the one exact exception: attempt is zero, driver is
`observation_only`, all request/fingerprint and input-hash fields are null, and action spec
and spec hash are either both null or both an exact reconstructable pair. It is
inserted `OBSERVING` with an open `pre_m1_ambiguous_recovery` divergence; direct
terminal insertion is forbidden. Uncertain observation remains `OBSERVING`.
Fresh conclusive provider observation may advance it to terminal `RECOVERED`
only when the exact current spec was reconstructable; otherwise it advances to
terminal `DIVERGED` with the open episode. Both terminal states require
`completed_at`. `RECOVERED` is never `MATCHED`, never counts as parity coverage,
and cannot transition.

For audited rows, `parity_state` is exactly `PLANNED`, `INPUT_MATCHED`,
`OBSERVING`, `MATCHED`, or `DIVERGED`, with graph `PLANNED -> INPUT_MATCHED |
DIVERGED`, `INPUT_MATCHED -> OBSERVING | DIVERGED`, and `OBSERVING -> OBSERVING
| MATCHED | DIVERGED`. The same-state `OBSERVING` edge is only a monotonic
refresh of request status or fresh observation evidence. `MATCHED` and
`DIVERGED` are terminal and immutable, and only terminal states have
`completed_at`. `MATCHED` requires `legacy_input_hash = spec_hash`, a typed
aggregate outcome, and at least one matched child operation; there is no
operation-free success.

The outer boundary is one would-be future action attempt. One top-level legacy
launch decision is one `ordinary_request` sample. One durable retirement
decision with a nonempty locator is one `controller_direct` down sample
allocated when retirement/route removal is first persisted, before any idle
deferral; an empty locator is first closed by zero-locator retirement and
allocates no down sample. That same nonempty-locator sample survives
defer-to-idle, later termination, every failed-cleanup reconciliation tick, and
controller recovery; each actual `core.down` call is a child, not a new outer
sample. An internal down used to compensate a failed launch is always a cleanup
child of that launch sample and can never allocate a down sample. A streaming
retry or retrieval after a lost admission response also remains the same launch
sample. Only a genuinely new launch policy decision after retry-safe certainty
may allocate `N+1`.

Provider/direct evidence below an outer attempt is stored in the child table
`api_resource_action_shadow_operations`:

```text
shadow_operation_id       UUID primary key
sample_id                 UUID not null references shadow_samples ON DELETE RESTRICT
operation_index           BIGINT not null
phase                     TEXT not null
operation_kind            TEXT not null
execution_surface         TEXT not null
expected_input_hash       TEXT not null
legacy_input_hash         TEXT nullable
outcome                   JSONB nullable
parity_state              TEXT not null
created_at                TIMESTAMPTZ not null default clock_timestamp()
updated_at                TIMESTAMPTZ not null default clock_timestamp()
completed_at              TIMESTAMPTZ nullable
```

`operation_index` is contiguous from one and unique with `sample_id`. Phase is
exactly `submission`, `provision`, `failover`, `cleanup`, or `observation`;
operation kind is `launch`, `down`, or `observe`; and `execution_surface` is
exactly `provisioner_adapter`, `core_direct`, or `provider_observation`.
`provisioner_adapter` and `core_direct` require mutation kind `launch` or
`down`; `provider_observation` requires `observe`. Input hashes are `Sha256`
and outcome is the exact typed outcome with the same 65,536-byte canonical
bound. Children deliberately have no API
request ID: the outer sample owns the request, while children journal each
provider adapter call, direct call, cleanup, and observation it causes.

At most 32 mutation children may exist per sample. These are shadow-operation
rows, never `api_resource_action_child_requests`. One additional
`provider_observation` shadow child is allowed and is unique per sample. That one row
stores a complete `KubernetesObservationSetV1`, never a proof for only the last
locator. Its `OBSERVING -> OBSERVING` update atomically replaces the entire
typed observation outcome only when the locator-set hash and one-to-one
coverage are unchanged, every `reference_kind`/provider index remains valid
and identical, both new rounds are self-contained and satisfy their
PostgreSQL-clock ordering, and the set `observed_at` strictly increases. It
does not preserve or refer to a round from the replaced value. A partial set,
cross-row prior-round link, or per-locator merge is forbidden. This makes
full-set uncertainty refreshable
without an unbounded row stream. Mutation
children cannot use a same-state update. All children use `PLANNED ->
INPUT_MATCHED -> OBSERVING -> MATCHED | DIVERGED`, with `PLANNED -> DIVERGED`
and `INPUT_MATCHED -> DIVERGED` for early mismatches. Terminal children are
immutable. Inserting any child locks its parent, rejects a terminal parent,
enforces the next contiguous index, and enforces the mutation/observation
limits before mutation authority proceeds. After the retention interval, one
transaction may delete only eligible resolved divergence episodes first, then
child operations, the outer sample, and finally its uniquely owned ordinary
request. Any unresolved or non-resolvable episode blocks retention of the
referenced sample; no foreign-key edge is deferred or bypassed.

Both operation hashes cover the PostgreSQL-canonical exact object
`ShadowOperationEnvelopeV1`:

```text
{
  version: 1,
  outer_spec_hash: Sha256,
  phase: "submission" | "provision" | "failover" | "cleanup" |
         "observation",
  operation_kind: "launch" | "down" | "observe",
  execution_surface: "provisioner_adapter" | "core_direct" |
                     "provider_observation",
  cluster_name: Token[256],
  workspace: WorkspaceName,
  resource_scope: {kind: "incarnation" | "legacy", value: null | Text[256]},
  placement: {
    cloud: null | Token[64],
    region: null | Text[128],
    zone: null | Text[128]
  },
  effective_container_root: null | {
    source_reference: Text[1024],
    source_root_digest: OciSha256
  },
  resolved_runtime: null | {
    artifact_id: CatalogUuid,
    platform: OciPlatform,
    digest: OciSha256
  },
  purge: boolean
}
```

The outer spec hash binds version bytes, override, target, retirement, and
container selector/content. A launch operation requires the byte-equal root
object from that spec when present. Its resolved runtime equals the spec bundle
for a catalog selector, is null for a direct ref-only pull, and is the exact
locked root-to-platform-to-child lineage for a managed ref-only pull.
Down/observe require both fields null. `purge` is false except for a
down/cleanup whose immutable policy requests it. The object contains no
credentials, request body, provider response, or auth target. The tables have
no delivery, claim, lease, retry, or provider mutation authority.

The exact successful shadow sequence is:

1. The producer opens one caller-owned SQLAlchemy `Connection` transaction on
   the central PostgreSQL database, takes the writer shared transaction
   advisory lock, then locks `services -> referenced version_specs ->
   replicas/capacity claims -> prior shadow sample` in that order. It verifies
   the current service hash, lifecycle epoch, owner token, and `shadow` mode,
   normalizes and PostgreSQL-canonicalizes the would-be action, and inserts or
   reuses the preallocated `PLANNED` sample. The Serve state store, ordinary and
   reserved/paid spot-placer capacity-claim paths, retirement/full-service
   path, consolidated-pool path, and shadow store expose caller-session forms
   that accept this exact `Connection`; none may open, nest, commit, or roll
   back its own session. The one transaction persists the replica row and
   incarnation, preallocated cluster-record hash, initial empty locator array,
   ordinary/reserved/paid capacity claim or retirement intent, and sample. For
   full-service retirement with a nonempty locator array it additionally
   persists route removal and the sample-ID terminal tombstone described above;
   an empty array uses the exact zero-locator transaction and creates no down
   sample/tombstone. It commits before any legacy admission or direct mutation.
   This boundary prevents promotion or recovery from racing an unjournaled
   decision.
2. Immediately before the outer entry point and every provider, failover,
   cleanup, or direct-call entry point, the audit hook serializes the *actual*
   cluster name, version bytes/digests, resources and override provenance,
   container content, workspace, scope, logical target, and retirement inputs.
   PostgreSQL computes the outer and operation hashes. Equality advances to
   `INPUT_MATCHED`; mismatch atomically makes the affected row `DIVERGED` and
   inserts the divergence episode before legacy authority may proceed.
3. For `ordinary_request`, the preallocated sample ID is the idempotency token
   to the ordinary admission path. That path may discover the existing binding
   without retaining a lock, then opens one caller-owned `Connection`, takes
   the writer shared transaction advisory lock, and locks `services ->
   referenced version_specs -> replica/capacity claim -> shadow sample ->
   ordinary request/queue`. It revalidates the admin ring, current owner token,
   service hash/lifecycle epoch/mode, replica/incarnation and claim, expected
   sample revision, exact spec hash, and request input hash from
   `ShadowOrdinaryAdmissionContextV1`. It inserts or retrieves the ordinary
   request, verifies its immutable fingerprint, binds request ID/status/input
   hash exactly once, and commits before returning its canonical request ID.
   Retrying after a lost response repeats all authority checks and returns that
   same ID; a stale owner receives no ID, and controller recovery redrives the
   existing sample rather than allocating another. The request keeps all action
   correlation columns null and never receives the action codec/handler. For
   `controller_direct`, the committed indexed `core_direct` child precedes each
   call. For `observation_only`, no mutation call is allowed.
4. Each actual provider/direct invocation and launch compensation is a child.
   A fresh provider observation child must hold the exact full locator set and
   prove the single-live-location launch predicate or all-locator absence for
   down success; the absence of a local cluster record is not provider proof.
   Failures use the same cleanup-certainty rules as the action
   matrix. Driver-specific predicates below alone advance the outer sample to
   `MATCHED`; uncertainty remains `OBSERVING`, and any mismatch creates a
   terminal `DIVERGED` row and durable divergence episode.

The exact outer `MATCHED` predicates are closed:

- `ordinary_request`: the unique bound request input hash is unchanged, it has
  terminal status, the terminal hash recomputes exactly, that
  status/result maps exactly to `legacy_outcome`, every mutation child is
  `MATCHED`, and the matched observation child proves the outcome's provider
  certainty;
- `controller_direct`: there is no request; the inspected row is the one
  durable sample for its retirement decision, and zero or more contiguous,
  indexed `core_direct` down children represent all cleanup ticks. Every
  mutation child is `MATCHED`, its aggregate typed outcome equals
  `legacy_outcome`, and the matched observation child proves confirmed absence
  for every retained locator. Zero
  direct children is legal only when the first complete
  observation already proves absence. Before every direct child after the
  first, the refreshable observation child must have advanced and proved the
  prior call left a present resource; and
- `observation_only`: there is no request or mutation child, and the single
  matched provider-observation child carries exact full-set coverage and proves
  the typed single-location-present or all-locators-deleted result.

Before any service enters shadow, the eligible path refactors
`terminate_cluster(max_retry=3)` into a one-call `terminate_once` adapter. The
reconciler, not an in-function loop, schedules another cleanup tick only after
the complete locator observation above proves the prior call left a present
resource. This preserves every invocation as an indexed child and removes the
false exactly-one-call assumption. Explicitly out-of-scope legacy provider
adapters may retain their old local loop only while the service remains
`legacy`; it is unreachable from shadow or authoritative execution.

A lost HTTP/provider response does not itself create outer attempt `N+1`.
Observation and cleanup remain children of `N`; a stream transport retry is not
another provider invocation. A replacement mutation is another child only when
fresh observation proves the prior invocation did not start or left no
resource. Otherwise `N` stays nonterminal and blocks succession. If legacy code
nevertheless proceeds, it first records `unsafe_legacy_replay`; that reason is
non-resolvable and permanently makes this service incarnation ineligible for
authoritative mode. A 33rd mutation similarly records the non-resolvable
`shadow_operation_limit_exceeded` episode before legacy authority proceeds; it
is never silently omitted.

The ordinary-request binding above is also the exact durable launch-admission
marker: the sole shadow launch endpoint must bind the sample before it creates
queue delivery or crosses any provider callback. The only launch sample shapes
that may be deleted without provider observation are the two shadow forms in
`ZeroLocatorRetirementProofV1`: the still-`PLANNED`, wholly unbound,
never-admitted shape, or the sole linked generation-zero request with its exact
terminal before-dispatch proof. Both require empty locators, no child,
PREPARED callback, delivery, or provider reference. Under the same writer lock
and row order, one transaction persists the exact proof/event and removes the
sample, owned request when present, and replica/capacity intent. This proves the
callback boundary was never crossed. Every other launch sample,
including a lost admission response, requires complete provider evidence before
release. An input-valid row advances through `INPUT_MATCHED` to `OBSERVING` and
records `never_started_proof_missing`; an input-invalid row takes the normal
`DIVERGED` edge. Neither permits a replacement mutation until the declared
observation/divergence protocol closes the uncertainty.

Every shadow-mode path that would directly delete a failed/retired replica row
first evaluates zero-locator retirement. If that exact proof does not apply, a
nonempty locator makes it an eligible down decision and it must first write the
exact down sample. If the provider resource is already absent,
`INPUT_MATCHED -> MATCHED` and replica-row deletion/usage closure commit
together on the same central PostgreSQL connection. A replica row is never
deleted merely because the legacy thread is missing or its coarse status says
complete.

Shadow never creates an `api_resource_actions` row or a correlated API request.
Thus it does not invent a native attempt-zero result, reserve an action attempt,
or interfere with legacy retries. On authoritative cutover, all in-flight
legacy samples must first become terminal. A `DIVERGED` or `RECOVERED` sample
may remain only when every linked resolvable episode was later resolved by
typed evidence; it never counts as matched coverage. A non-resolvable episode
blocks cutover forever.
Any still-live legacy replica operation is conservatively imported after old
workers drain as Serve `VERIFYING` plus kernel `READY` with a primary
attempt-one observation descriptor. Completed samples are evidence only and
are not converted into actions. New decisions after cutover also begin with a
primary positive attempt.

API-request 006 also creates the shadow-only diagnostic table
`api_resource_action_shadow_divergences` with exactly these columns:

```text
divergence_id             UUID primary key
service_name              TEXT not null
service_hash              TEXT not null
replica_id                BIGINT not null
replica_incarnation_id    UUID not null
action_type               TEXT not null
reason                    TEXT not null
capacity_context          TEXT not null
retirement_context        TEXT not null
execution_context         TEXT not null
episode_sequence          BIGINT not null
sample_id                 UUID nullable references shadow_samples ON DELETE RESTRICT
operation_id              UUID nullable references shadow_operations ON DELETE RESTRICT
detected_at               TIMESTAMPTZ not null default clock_timestamp()
resolved_at               TIMESTAMPTZ nullable
resolved_by_sample_id      UUID nullable references shadow_samples
resolved_by_operation_id   UUID nullable references shadow_operations
resolution_evidence        JSONB nullable
```

Names/hashes are limited to 256 canonical UTF-8 bytes, replica ID is
nonnegative, episode sequence is positive and contiguous per resource/action
identity, and resolution is not earlier than detection. `action_type` is
`launch` or `down`; the context axes use the same closed values and
action-specific rules as samples.
`reason` is exactly `unsupported_override`,
`override_provenance_missing`, `canonicalization_rejected`,
`version_reference_unavailable`, `image_identity_unavailable`,
`scope_mismatch`, `legacy_input_mismatch`, `legacy_outcome_mismatch`,
`operation_input_mismatch`, `operation_outcome_mismatch`,
`request_binding_conflict`, `request_input_hash_mismatch`,
`request_terminal_hash_mismatch`, `request_status_mismatch`,
`provider_evidence_missing`, `provider_capability_unavailable`,
`never_started_proof_missing`,
`immutable_action_conflict`, `pre_m1_ambiguous_recovery`,
`attempt_sequence_gap`, `unsampled_mutation`, `parity_regression`,
`unsafe_legacy_replay`, `shadow_operation_limit_exceeded`, or
`atomic_store_unavailable`. A unique constraint over resource/action identity
and `episode_sequence` makes the journal append-only. `sample_id` and
`operation_id` are each optional, but an operation requires its parent sample
to equal `sample_id`; a normalization failure may have neither. The table stores no raw
override, YAML, image credential/reference beyond the action-safe fields,
exception text, or user data.

An unresolved episode has all resolution fields null. For a resolvable reason,
resolution sets `resolved_at`, `resolution_evidence`, and exactly one reference
to a later `MATCHED` audited sample or its matched `provider_observation`
operation. `resolution_evidence` is the exact closed object
`{version: 1, kind: "later_matched_sample" | "matched_provider_observation",
observed_at: UtcTimestamp, evidence_hash: Sha256}`; its kind must match the
chosen reference and its hash is over that immutable row's canonical evidence.
`unsafe_legacy_replay`, `shadow_operation_limit_exceeded`, and
`unsampled_mutation` reject resolution fields. A
`DIVERGED` sample is terminal and never changes to `MATCHED`; repair creates a
later attempt/sample or observation child and resolves the separate divergence
episode. Every recurrence allocates the next episode sequence and inserts a new
row, whether or not an earlier episode is still open; no row is incremented,
reopened, or overwritten.

In `shadow`, deterministic normalization failure does not seize execution
authority. Before the existing legacy thread/request is admitted, the caller
inserts an episode through the same caller-owned central PostgreSQL connection
as the locked replica intent. Pre-mutation input or terminal-outcome mismatch
performs the same append together with the sample's
`DIVERGED` transition. Only after that commit may legacy authority proceed.
Failure to durably record the sample/divergence fails the decision closed rather
than performing an unobserved mutation. A periodic shadow parity pass retries
normalization/observation but never queues an action attempt. It sets
`resolved_at` only through the later matched reference above. An unresolved row is
never retention-purged; a resolved row is retained for at least the
action-history retention interval.

If the same structured action/sample identity already exists with a different
spec hash, origin, or action type projection, the producer never updates it,
never admits a request, and records `immutable_action_conflict` while still in
shadow. Promotion/backfill treats the conflict as unresolved divergence. If it
is discovered after a service is authoritative, the service fails closed in
operator-visible reconciliation; it does not weaken immutability or create a
second identity. The conflict can be resolved only by proving the persisted
row correct or creating a new replica incarnation under normal policy.
Service purge and name reuse may remove version rows but do not remove an open
divergence, because service hash and incarnation keep it unambiguous.

The `shadow -> authoritative` gate performs nonlocking key discovery, then a
registration-first preflight transaction in the universal order through the
service/domain rows. It captures the service revision, activation basis,
registration set, parity window, and provider-observation basis, then commits
and releases every lock. The fresh provider observation runs outside all
database transactions. A final registration-first transaction reacquires the
same order, revalidates the byte-equal basis and service revision, and only
then performs the promotion audit and mode transition. It requires zero
unresolved or non-resolvable
divergence rows, no divergence insertion, unsampled mutation, or parity
regression since a non-null `parity_window_started_at` at least 86,400
database-clock seconds old, every audited sample and child
  terminal, every `DIVERGED`/`RECOVERED` sample's linked resolvable episodes
resolved, and at least one later `MATCHED` launch and down sample for every
eligible context-axis combination that the promoted service may exercise. If
production policy does not naturally produce one of those samples, an
intentional readiness exercise must traverse the same ordinary-request or
controller-direct admission, real provider callback, observation, and terminal
transactions and leave an ordinary `audited` `MATCHED` sample/child chain.
There is no special canary row, flag, external assertion, or synthetic
observation that satisfies this gate. It recomputes every request
input/terminal hash and driver-specific match predicate, requires no attempt
gap and no live/un-sampled legacy operation, and verifies that fresh
observation against the locked basis. New correctly
`MATCHED` samples during the window do not reset or invalidate
`parity_window_started_at`; durable parity does not require 24-hour quiescence.
Only divergence, unsampled mutation, or a recomputation/observation regression
clears it; after all blockers resolve, the locked zero-blocker audit starts a
new window. Non-consolidated pools are
explicitly ineligible rather than hidden as divergences. API-request 006
exceptional downgrade requires all three shadow tables, as well as
action/correlated-request history, to be empty.

Activation-trusted shadow history has database guards too. API006 creates
functions `api_resource_action_shadow_samples_guard_write_v1()` and
`api_resource_action_shadow_operations_guard_write_v1()` and
`api_resource_action_shadow_divergences_guard_write_v1()` plus respective row
triggers
`trg_api_resource_action_shadow_samples_guard_write_v1` and
`trg_api_resource_action_shadow_operations_guard_write_v1` and
`trg_api_resource_action_shadow_divergences_guard_write_v1`. The sample trigger
is BEFORE INSERT OR UPDATE. INSERT locks the resource identity and prior
attempt, validates the exact audited/pre-M1 initial shape, and rejects a gap or
directly manufactured `MATCHED`/`RECOVERED` row. UPDATE makes the ID, identity,
  context axes, origin, attempt/driver, spec/hash, and creation time immutable; allows
the request ID/status/input hash to be installed exactly once by the atomic
ordinary-admission transaction; permits the terminal hash once with terminal
status/outcome; freezes and revalidates both hashes thereafter; enforces the
exact graph; allows only a refresh that mirrors an actually locked ordinary
request's legal status (including its existing `WAITING` lifecycle) and a
fresh observation in `OBSERVING`; and rejects any terminal update.

The operation trigger is BEFORE INSERT OR UPDATE. INSERT locks and rejects a
terminal parent, verifies the next index and closed kind/surface pairing, and
enforces 32 mutation rows plus one observation row. UPDATE makes operation ID,
parent, index, phase, kind/surface, expected hash, and creation time immutable;
installs the actual hash once; enforces the exact graph; allows only the
strictly-newer observation refresh described above; and rejects terminal-row
updates. The divergence trigger is BEFORE INSERT OR UPDATE. INSERT locks the
resource identity and requires the next episode sequence and valid linked
sample/operation. UPDATE freezes every identity/detection field and permits
only the one unresolved-to-resolved transition with the exact later matched
reference/evidence tuple; resolved and non-resolvable rows reject updates.

Their catalog signatures and normalized definitions are verified exactly like
the action guard. Promotion does not trust `MATCHED` text alone: its preflight
and final transactions re-canonicalize every retained action spec, recheck
spec/input hashes and contiguous attempts, and reread each immutable ordinary
legacy request/result. The fresh final-attempt provider observation occurs
between those transactions with no database lock held; the final transaction
accepts it only after byte-equal basis and revision revalidation. Any mismatch
atomically clears the service's parity-window start, inserts the next
divergence episode, and refuses promotion. Direct rewriting therefore cannot
manufacture activation evidence.

The API005 request correlation contract is exactly the nine nullable,
no-default columns listed in the generic schema above, including
`resource_action_quiescence_type`, version, JSON and
`resource_action_quiescence_sha256`. Ordinary requests have all nine null.
Primary correlated requests have the first five non-null, use a UUID request
ID, positive attempt, `should_retry=false`, `retryable=false`, effect count
zero and canonical empty-array SHA
`4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945`.
The quiescence quartet is append-once only after terminal freeze. The request
payload SHA covers the exact seven-key mutation/observation envelope above.

Shadow adds no reciprocal sample/operation column to `api_requests`. The outer
sample is the single owner: its unique nullable `legacy_request_id` is the only
shadow foreign key and points `ON DELETE RESTRICT` to the ordinary request.
This deliberately avoids a cyclic foreign key and prevents one request from
being attached to several child tokens. The sample input hash is the
PostgreSQL-canonical SHA-256 of an
exact object containing `name`, `handler_name`, `payload_type`,
`payload_format`, `payload_version`, `producer_version`, `payload_json`,
`execution_class`, `cluster_name`, `schedule_type`, `user_id`,
`file_mounts_blob_id`, `ignore_return_value`, `retryable`, `event_context`, and
the three precondition fields; no raw copy is placed in a shadow table. The
terminal hash is installed once by the parity pass after it locks the terminal
request. It is the PostgreSQL-canonical SHA-256 of the exact closed v1 object
containing `request_id`, the input hash, terminal `status`,
`execution_generation`, `return_value`, `error`, `finished_at`,
`interrupted_reason`, and `cancel_requested_at`.
`cancel_acknowledged_at` is deliberately excluded so
the one legal post-terminal claim-fenced acknowledgement does not invalidate
evidence. Before
calling `sdk.launch`, the authenticated shadow producer passes the preallocated
sample ID as a trusted admission argument. One transaction locks the sample,
inserts the ordinary request and sets the sample request ID/input hash/status,
or, if already bound, verifies the stored request/input hash and returns the
canonical ID without inserting. A lost response repeats the sample token and
therefore cannot launch an unlinked replacement. The sample binding and every
input-hash field are immutable afterward. Joint terminal observation sets
status, normalized outcome, and terminal hash once; the reverse-lookup request
guard makes the ordinary request terminal row immutable immediately upon its
terminal transition (before the parity pass can bless it), with only the
existing cancel-acknowledgement exception, and then freezes every terminal-hash
input. Direct and
observation-only samples reject a request binding. Retention locks the sample and request, then
deletes operations, the sample, and finally the now-unreferenced ordinary
request in one transaction.

The authoritative executor identity, provider-side settlement capability, and
precommitted mutation window are recursively closed:

```text
KubernetesRequestExecutionChainV1 = {
  schema_version: 1,
  service_account_authenticator: "builtin_local_jwt_v1",
  authorizers: ["Node", "RBAC"],
  admission_plugins_sha256: Sha256,
  protected_object_identity_contract:
      "kind_namespace_name_and_unknown_labels_preserved_v1",
  dynamic_admission:
      "disabled_immutable" | "enabled_static_freeze_guarded_v1",
  allowlisted_static_freeze_guard_count: integer 0..1,
  external_authorizer_count: 0,
  external_authenticator_count: 0,
  external_audit_backend_count: 0,
  external_kms_storage_plugin_count: 0,
  aggregated_obligation_api_count: 0,
  storage_backend: "builtin_etcd_v1",
  static_config_reload: "disabled",
  referenced_config_set_sha256: Sha256,
  chain_sha256: Sha256
}

KubernetesApiServerBackendV1 = {
  schema_version: 1,
  backend_uid: Text[128],
  endpoint_host: Text[253],
  endpoint_port: integer 1..65535,
  tls_server_name: Text[253],
  tls_peer_spki_sha256: Sha256,
  git_version: Text[128],
  git_commit: Text[64],
  configz_sha256: Sha256,
  request_execution_chain: KubernetesRequestExecutionChainV1,
  request_execution_chain_sha256: Sha256
}

KubernetesGuardDisabledApiServerBackendV1 = {
  schema_version: 1,
  backend_uid: Text[128],
  endpoint_host: Text[253],
  endpoint_port: integer 1..65535,
  tls_server_name: Text[253],
  tls_peer_spki_sha256: Sha256,
  git_version: Text[128],
  git_commit: Text[64],
  configz_sha256: Sha256,
  resource_action_authority: "disabled",
  static_freeze_guard_count: 0,
  resource_action_ledger_reader_count: 0,
  immutable_process_config_sha256: Sha256,
  complete_request_chain_sha256: Sha256
}

KubernetesAdmissionRegistrationV1 = {
  schema_version: 1,
  kind: "ValidatingWebhookConfiguration",
  configuration_name: Text[253],
  configuration_uid: Text[128],
  configuration_resource_version: Text[128],
  webhook_name: Text[253],
  applicable_rules_sha256: Sha256,
  client_config_sha256: Sha256,
  failure_policy: "Fail" | "Ignore",
  timeout_seconds: PositiveI64,                 # <= 30
  side_effects: "None"
}

SkyPilotResourceActionDowngradeProcessQuiesceV1 = {
  schema_version: 1,
  api_owners: [{                                 # 0..16, identity sort
    kind: "Deployment" | "StatefulSet",
    namespace: Dns1123Label,
    name: Text[253],
    uid: Text[128],
    generation: NonnegativeI64,
    observed_generation: NonnegativeI64,          # equals generation
    immutable_spec_sha256: Sha256,
    running_image_set_sha256: Sha256,
    mode: "maintenance_read_only_no_resource_action_writes_v1",
    rollback_and_scale_up_disabled: true
  }],
  mutator_owners: [{                             # 0..32, identity sort
    role: "controller" | "executor",
    kind: "Deployment" | "StatefulSet" | "Job",
    namespace: Dns1123Label,
    name: Text[253],
    uid: Text[128],
    generation: NonnegativeI64,
    observed_generation: NonnegativeI64,          # equals generation
    immutable_spec_sha256: Sha256,
    replicas_or_active_jobs: 0,
    rollback_and_scale_up_disabled: true
  }],
  live_mutator_pod_count: 0,
  old_or_unlisted_binary_process_count: 0
}

StaticAdmissionGuardDeploymentInventoryV1 = {
  schema_version: 1,
  authoritative_database_identity_sha256: Sha256,
  installations: [{                             # 0..64, installation_id sort
    installation_id: canonical lowercase UUID text,
    target: {
      cluster_fingerprint_sha256: Sha256,
      kube_system_uid: Text[128]
    },
    control_plane_owner: {
      kind: "StaticPodOwner",
      namespace: Dns1123Label,
      name: Text[253],
      uid: Text[128],
      immutable_installation_spec_sha256: Sha256,
      guard_free_revision_sha256: Sha256,
      rollout_transport: "out_of_band_owner_change_v1"
    },
    module_binary_sha256: Sha256,
    manifest_and_config_sha256: Sha256,
    dependency_inventory_sha256: Sha256
  }]
}

StaticAdmissionGuardBackendInventoryV1 = {
  schema_version: 1,
  deployment_inventory_sha256: Sha256,
  targets: [{                                   # exact installation order
    installation_id: canonical lowercase UUID text,
    target: <the exact matching deployment-inventory target>,
    control_plane_owner_sha256: Sha256,
    guarded_backends: [KubernetesApiServerBackendV1],  # 1..16, sorted
    guarded_backend_set_sha256: Sha256,
    endpoint_slice_snapshot_sha256: Sha256,
    owner_snapshot_sha256: Sha256
  }]
}

StaticAdmissionGuardDisableEvidenceV1 = {
  schema_version: 1,
  deployment_inventory_sha256: Sha256,
  guarded_backend_inventory_sha256: Sha256,
  targets: [{                                   # exact installation order
    installation_id: canonical lowercase UUID text,
    rollout_quiesce: {
      kind: "resource_action_guard_rollout_quiesce_v1",
      owner_uid: Text[128],
      pin_uid: Text[128],
      pin_spec_sha256: Sha256,
      desired_guard_free_revision_sha256: Sha256,
      owner_evidence_sha256: Sha256
    },
    guard_free_backends: [KubernetesGuardDisabledApiServerBackendV1],
                                                    # 1..16, sorted
    guard_free_backend_set_sha256: Sha256,
    guard_free_chain_sha256: Sha256,
    endpoint_slice_first_snapshot_sha256: Sha256,
    endpoint_slice_second_snapshot_sha256: Sha256,
    endpoint_slice_watch_evidence_sha256: Sha256,
    old_backend_absence_evidence_sha256: Sha256,
    verified_at: UtcTimestamp                      # SQL-owned top-level v_now
  }]
}

ResourceActionGuardDowngradeStateV1 = {
  singleton: true,
  phase: "DRAINING" | "DISABLED",
  process_quiesce: SkyPilotResourceActionDowngradeProcessQuiesceV1,
  process_quiesce_sha256: Sha256,
  deployment_inventory: StaticAdmissionGuardDeploymentInventoryV1,
  deployment_inventory_sha256: Sha256,
  guarded_backend_inventory: StaticAdmissionGuardBackendInventoryV1,
  guarded_backend_inventory_sha256: Sha256,
  guard_disable_evidence: null | StaticAdmissionGuardDisableEvidenceV1,
  guard_disable_evidence_sha256: null | Sha256,
  opened_at: UtcTimestamp,
  verified_at: null | UtcTimestamp
}

AdmissionConfigurationFreezeV1 = {
  schema_version: 1,
  freeze_id: canonical lowercase UUID text,
  registration_nonce_sha256: Sha256,
  opened_at: UtcTimestamp,
  mode: "dynamic_engines_disabled" | "static_manifest_guard",
  prefreeze_settlement_seconds: NonnegativeI64,  # <= 120
  snapshot_not_before: UtcTimestamp,
  postwait_challenge_sha256: Sha256,
  postwait_challenge_issued_at: UtcTimestamp,
  barrier_drained_at: UtcTimestamp,
  barrier_evidence_set_sha256: Sha256,
  cache_snapshot_not_before: UtcTimestamp,
  postbarrier_challenge_sha256: Sha256,
  postbarrier_challenge_issued_at: UtcTimestamp,
  guard: null | {
    kind: "allowlisted_static_apiserver_freeze_guard_v1",
    installation_id: canonical lowercase UUID text,
    deployment_inventory_sha256: Sha256,
    deployment_inventory_entry_sha256: Sha256,
    module_binary_sha256: Sha256,
    manifest_and_config_sha256: Sha256,
    backend_set_sha256: Sha256,
    side_effect_contract: "read_only_fail_closed_v1",
    timeout_seconds: PositiveI64,              # <= 5
    protected_resource_rules_sha256: Sha256,
    inflight_barrier_contract:
        "per_backend_pre_freeze_protected_request_drain_v1",
    dependency_inventory_sha256: Sha256,
    authoritative_database_identity_sha256: Sha256,
    database_query_contract:
        "primary_read_committed_freeze_and_downgrade_state_fail_closed_v1",
    live_probe_contract:
        "dry_run_matching_preconditions_fixed_sentinel_delete_v1"
  },
  backend_rejections: [{
    backend_uid: Text[128],
    barrier_audit_id: Text[128],
    barrier_epoch_sha256: Sha256,
    barrier_credential_token_sha256: Sha256,
    barrier_evidence_sha256: Sha256,
    audit_id: Text[128],
    request_sha256: Sha256,
    response_evidence_sha256: Sha256
  }],                                          # conditional cardinality below
  frozen_admission_snapshot_sha256: Sha256,
  release_not_before: UtcTimestamp
}

FixedExecutorCredentialV1 = {
  kind: "fixed_projected_pod_uid_bound_sa_token_v1",
  token_sha256: Sha256,
  audience: "https://kubernetes.default.svc",
  issuer: Text[512],
  subject: Text[512],
  bound_pod_uid: CatalogUuid,
  issued_at: UtcTimestamp,
  expires_at: UtcTimestamp,
  signed_claims_sha256: Sha256,
  token_review_authenticated: true,
  token_review_audiences: ["https://kubernetes.default.svc"],
  token_review_username: Text[512],
  token_review_user_uid: Text[512],
  token_review_groups_sha256: Sha256,
  token_review_extras_sha256: Sha256,
  token_review_pod_uid: CatalogUuid,
  token_review_evidence_sha256: Sha256,
  token_review_verified_at: UtcTimestamp,
  authentication_clock_skew_seconds: NonnegativeI64  # <= 60
}

KubernetesEffectSettlementCapabilityV1 = {
  schema_version: 1,
  provider: "kubernetes",
  profile: "pod_cluster_v1",
  target: {
    cluster_fingerprint_sha256: Sha256,
    kube_system_uid: Text[128]
  },
  mechanism: {
    kind: "kube_apiserver_configz_request_timeout_v1",
    version: 1,
    backends: [KubernetesApiServerBackendV1],   # 1..16, sorted by backend_uid
    backend_set_sha256: Sha256,
    apiserver_build_set_sha256: Sha256,
    admission_registrations: [
      KubernetesAdmissionRegistrationV1        # 0..128, canonical sort
    ],
    admission_registration_set_sha256: Sha256,
    admission_freeze: AdmissionConfigurationFreezeV1,
    mutating_webhook_collection_sha256: Sha256,
    applicable_mutating_webhook_count: 0,
    mutating_registration_collection_rv: Text[128],
    validating_registration_collection_rv: Text[128],
    mutating_policy_api: "allowlisted_not_served" | "served",
    mutating_policy_collection_rv: null | Text[128],
    mutating_policy_binding_collection_rv: null | Text[128],
    mutating_policy_collection_sha256: null | Sha256,
    mutating_policy_binding_collection_sha256: null | Sha256,
    applicable_mutating_policy_count: 0,
    validating_policy_api: "allowlisted_not_served" | "served",
    validating_policy_collection_rv: null | Text[128],
    validating_policy_binding_collection_rv: null | Text[128],
    validating_policy_collection_sha256: null | Sha256,
    validating_policy_binding_collection_sha256: null | Sha256,
    request_timeout_seconds: PositiveI64,       # <= 60
    admission_cache_propagation_bound_seconds: NonnegativeI64,  # <= 60
    adapter_transport_timeout_seconds: 10,
    authentication_clock_skew_seconds: NonnegativeI64,  # <= 60
    storage_contract: "synchronous_etcd_commit_before_response_v1",
    response_matrix_sha256: Sha256,
    obligation_kinds: [
      "configmaps", "ingresses.networking.k8s.io", "pods", "services"
    ]
  },
  verification: {
    verifier: "skypilot.kubernetes.effect-settlement.v1",
    verified_at: UtcTimestamp,
    expires_at: UtcTimestamp,
    configz_evidence_sha256: Sha256,
    dry_run_probe_evidence_sha256: Sha256,
    endpoint_slice_watch_basis_sha256: Sha256,
    admission_watch_basis_sha256: Sha256,
    registration_nonce_sha256: Sha256,
    postwait_challenge_sha256: Sha256,
    postbarrier_challenge_sha256: Sha256,
    fixed_credential: FixedExecutorCredentialV1
  },
  max_effect_settlement_seconds: PositiveI64   # <= 60
}

ResourceActionExecutorCapabilityRegistrationV1 = {
  schema_version: 1,
  state: "VERIFYING" | "READY" | "CONSUMED" | "EXPIRED" | "RELEASED",
  purpose: "readiness" | "action",
  target: {
    cluster_fingerprint_sha256: Sha256,
    kube_system_uid: Text[128]
  },
  nonce_sha256: Sha256,
  nonce_issued_at: UtcTimestamp,
  admission_freeze_id: canonical lowercase UUID text,
  admission_freeze_opened_at: UtcTimestamp,
  admission_freeze_mode:
      "dynamic_engines_disabled" | "static_manifest_guard",
  admission_freeze_prefreeze_settlement_seconds: NonnegativeI64,
  admission_freeze_snapshot_not_before: UtcTimestamp,
  postwait_challenge_sha256: null | Sha256,
  postwait_challenge_issued_at: null | UtcTimestamp,
  admission_barrier_drained_at: null | UtcTimestamp,
  admission_barrier_evidence_set_sha256: null | Sha256,
  admission_cache_snapshot_not_before: null | UtcTimestamp,
  postbarrier_challenge_sha256: null | Sha256,
  postbarrier_challenge_issued_at: null | UtcTimestamp,
  admission_freeze_release_not_before: null | UtcTimestamp,
  admission_freeze_released_at: null | UtcTimestamp,
  registered_at: null | UtcTimestamp,
  capability: null | KubernetesEffectSettlementCapabilityV1,
  capability_sha256: null | Sha256,
  consumed_at: null | UtcTimestamp,
  consumption: null | {
    kind: "service_promotion" | "request_claim",
    service_hash: Text[256],
    service_lifecycle_epoch: NonnegativeI64,
    action_id: null | canonical lowercase UUID text,
    request_id: null | canonical lowercase UUID text,
    attempt: null | PositiveI64,
    audit_sha256: Sha256
  },
  expired_at: null | UtcTimestamp,
  expiration_reason: null | "nonce_timeout" | "capability_timeout" |
                     "executor_drained_before_consume",
  release_evidence: null | ExecutorRegistrationReleaseEvidenceV1,
  release_evidence_sha256: null | Sha256
}

ProviderExecutorFenceV1 = {
  schema_version: 1,
  request_id: canonical lowercase UUID text,
  action_id: canonical lowercase UUID text,
  attempt: PositiveI64,
  execution_generation: 1,
  claim_token_sha256: Sha256,
  worker_instance_id: CatalogUuid,
  executor_registration: {
    registration_id: canonical lowercase UUID text,
    purpose: "action",
    nonce_sha256: Sha256,
    postwait_challenge_sha256: Sha256,
    postbarrier_challenge_sha256: Sha256,
    capability_sha256: Sha256,
    consumed_at: UtcTimestamp,
    consumption_audit_sha256: Sha256
  },
  control_plane: {
    cluster_fingerprint_sha256: Sha256,
    namespace: Dns1123Label,
    pod_name: Dns1123Label,
    pod_uid: CatalogUuid,
    node_name: Text[253],
    node_boot_id_sha256: Sha256,
    credential: FixedExecutorCredentialV1,
    settlement_capability: KubernetesEffectSettlementCapabilityV1,
    settlement_capability_sha256: Sha256,
    max_effect_settlement_seconds: PositiveI64
  },
  claimed_at: UtcTimestamp
}

KubernetesMutationCallSlotV1 = {
  schema_version: 1,
  call_index: PositiveI64,
  verb: "create" | "patch" | "delete",
  kind: "configmaps" | "ingresses.networking.k8s.io" |
        "pods" | "services",
  namespace: Dns1123Label,
  name: Text[253],
  uid_precondition: null | Text[128],
  request_sha256: Sha256
}

KubernetesPodUidAbsenceEvidenceV1 = {
  schema_version: 1,
  target: {
    cluster_fingerprint_sha256: Sha256,
    kube_system_uid: Text[128],
    namespace: Dns1123Label,
    pod_name: Dns1123Label,
    expected_uid: CatalogUuid
  },
  request: {
    consistency: "MostRecent",
    resource_version: "",
    resource_version_match: null
  },
  result: "not_found" | "different_uid",
  observed_uid: null | CatalogUuid,
  observed_at: UtcTimestamp,
  response_evidence_sha256: Sha256
}

ExecutorRegistrationReleaseEvidenceV1 = {
  schema_version: 1,
  registration_id: canonical lowercase UUID text,
  worker_instance_id: CatalogUuid,
  purpose: "readiness" | "action",
  authority_case: "unconsumed_no_request" | "readiness_no_request" |
                  "action_no_window_terminal" |
                  "action_definitive_terminal" | "action_quiesced",
  request_id: null | canonical lowercase UUID text,
  request_terminal_projection_sha256: null | Sha256,
  frozen_effect_count: null | PositiveI64,
  frozen_effects_sha256: null | Sha256,
  request_execution_quiescence_sha256: null | Sha256,
  instance_row: "present_draining_no_capability" | "absent",
  instance_projection_sha256: null | Sha256,
  pod_uid_absence: KubernetesPodUidAbsenceEvidenceV1,
  credential_deadline_basis:
      "verifying_worst_case_v1" | "registered_fixed_credential_v1",
  credential_deadline_at: UtcTimestamp,
  release_not_before: UtcTimestamp,
  recorded_at: UtcTimestamp
}

ProviderMutationWindowV1 = {
  schema_version: 1,
  window_id: CatalogUuid,
  request_nonce_sha256: Sha256,
  claim_token_sha256: Sha256,
  locator_hash: Sha256,
  phase: "prepared" | "submission" | "provision" | "failover" | "cleanup",
  max_calls: PositiveI64,                       # <= 8
  call_slots: [KubernetesMutationCallSlotV1],   # exactly max_calls
  settlement_capability_sha256: Sha256,
  backend_index: NonnegativeI64,
  backend_descriptor_sha256: Sha256,
  backend_connection_sha256: Sha256,
  admission_registration_set_sha256: Sha256,
  issued_at: UtcTimestamp,
  call_start_deadline_at: UtcTimestamp,
  call_complete_deadline_at: UtcTimestamp
}

ProviderMutationWindowResultV1 = {
  schema_version: 1,
  window_id: CatalogUuid,
  disposition: "no_bytes_sent" | "definitive_response" | "ambiguous",
  completed_at: UtcTimestamp,
  watch_validation_sha256: null | Sha256,
  calls: [{                                    # exactly window.max_calls
    call_index: PositiveI64,
    call_slot_sha256: Sha256,
    result: "skipped_before_transport" | "definitive_accepted" |
            "definitive_rejected" | "ambiguous",
    response_received_at: null | UtcTimestamp,
    http_status: null | integer 100..599,
    status_reason: null | Text[64],
    operation_id: null | Text[512],
    response_evidence_sha256: null | Sha256
  }]
}

ProviderExecutionQuiescenceV1 = {
  schema_version: 1,
  request_id: canonical lowercase UUID text,
  action_id: canonical lowercase UUID text,
  attempt: PositiveI64,
  execution_generation: 1,
  claim_token_sha256: Sha256,
  worker_instance_id: CatalogUuid,
  executor: <the exact ProviderExecutorFenceV1.control_plane object>,
  frozen_effect_count: PositiveI64,
  frozen_effects_sha256: Sha256,
  settlement_capability_sha256: Sha256,
  mutation_boundary: "not_entered" | "prepared_or_later",
  method: "not_submitted" | "worker_exited" | "pod_token_revoked" |
          "fixed_token_expired",
  revoked_at: UtcTimestamp,
  last_mutation_window_complete_deadline_at: null | UtcTimestamp,
  max_effect_settlement_seconds: PositiveI64,
  settle_after: UtcTimestamp,
  process_exit: null | {
    pid: PositiveI64,
    process_start_ticks: PositiveI64,
    signal: null | 9,
    exit_code: integer -255..255,
    joined_at: UtcTimestamp,
    no_provider_call_in_flight: true
  },
  pod_token_revocation: null | {
    evidence_recorded_at: UtcTimestamp,
    uid_precondition: CatalogUuid,
    grace_period_seconds: 30,
    delete_response_evidence_sha256: Sha256,
    old_uid_absence: KubernetesPodUidAbsenceEvidenceV1,
    token_review_evidence_sha256: Sha256,
    token_authenticated: false,
    token_error: "bound_object_not_found"
  },
  fixed_token_expiry: null | {
    token_sha256: Sha256,
    signed_claims_sha256: Sha256,
    bound_pod_uid: CatalogUuid,
    token_expires_at: UtcTimestamp,
    authentication_clock_skew_seconds: NonnegativeI64,
    evidence_recorded_at: UtcTimestamp,
    uid_precondition: CatalogUuid,
    delete_response_evidence_sha256: Sha256,
    old_uid_absence: KubernetesPodUidAbsenceEvidenceV1,
    expiry_not_before: UtcTimestamp,
    verified_at: UtcTimestamp
  }
}
```

The downgrade-state store, not an operator payload, constructs
`ResourceActionGuardDowngradeStateV1`. It canonicalizes the complete live
projections, owns `opened_at` on INSERT, and on the sole evidence-fill edge
sets the top-level `verified_at` and every nested target `verified_at` to one
identical trigger-side SQL `v_now`. Rollout-owner timestamps, local clocks, and
future-dated evidence are neither stored nor accepted as authority; the
authenticated owner projection is bound only by its content hash and is
re-read live at each later gate.

`ExecutorRegistrationReleaseEvidenceV1` is filled only by the guarded RELEASED
edge. `unconsumed_no_request` and `readiness_no_request` require all three
request fields null; every action case requires the exact consumed request ID,
terminal-projection hash, and frozen normalized-effect count/aggregate hash.
`request_execution_quiescence_sha256` is non-null if and only if the case is
`action_quiesced`. `instance_projection_sha256` is non-null if and only if the
instance row remains. For `verifying_worst_case_v1`,
`credential_deadline_at=nonce_issued_at + interval '1020 seconds'`; this
conservatively permits credential issuance at the full 300-second verification
deadline, then adds future-iat skew, token lifetime, and latest-acceptance skew.
It therefore does not depend on reconstructing whether a failed disabled-mode
verifier had already read its token. For
`registered_fixed_credential_v1` it equals the stored credential expiry plus
the stored authentication skew. `release_not_before` equals the locked ledger
field, and `recorded_at` is the RELEASED transaction's SQL time. The guard
recomputes these projections; they are not caller assertions.

The sole supported settlement mechanism is literal, not an adapter assertion.
The startup verifier authenticates to the exact stored Kubernetes endpoint/CA,
enumerates every kube-apiserver backend behind `kubernetes.default.svc` with
the same complete MostRecent EndpointSlice LIST rules, and
uses its non-resource-URL RBAC to read each backend's `/configz` and `/version`
over a separately authenticated, non-reconnecting TLS connection. It records
the complete sorted `KubernetesApiServerBackendV1` descriptors, including the
TLS peer and direct connection target, requires the same finite
`requestTimeout` in the inclusive range 1..60 seconds, and requires an exact
reviewed build whose API storage path satisfies the literal synchronous
commit/cancellation contract. For that fixed credential and the four
obligation API paths it also projects the complete request execution chain:
local service-account JWT authentication; exact built-in Node/RBAC
authorization; admission/defaulting; audit; aggregation; storage transform;
and etcd commit. External authentication/authorization/audit webhooks,
external KMS/storage plugins, aggregated replacements for an obligation API,
reloadable referenced files, and any control-plane callback outside the exact
frozen validating-admission registration set below are unsupported. The only
additional callback is the reviewed read-only static admission-freeze guard
below when its count is exactly one. The set/hash rejects
an unreachable, unidentifiable, unreviewed, differently configured, or
additional backend; a Service/VIP address is never itself a backend handle.

Static-guard installation has a complete inventory independent of successful
executor registration. The checked-in, content-addressed
`STATIC_ADMISSION_GUARD_DEPLOYMENT_INVENTORY_V1` is the exact
`StaticAdmissionGuardDeploymentInventoryV1` for every guard process configured
to read this authoritative database, including an installation that has never
opened a registration, one whose registration expired before capability
finalization, and one currently carrying no Serve workload. Each immutable
guard config embeds the full inventory hash and its unique entry/installation
ID, and the control-plane owner refuses to make that backend Ready unless both
match. Per-request primary reads also require the entry to remain current.
`control_plane_owner.uid` is the stable identity of the out-of-band
node/static-manifest owner, not a replaceable mirror-Pod or backend-process UID;
the latter identities are captured separately in backend inventories.
Adding, retiring, or moving an installation requires a new complete inventory,
an all-backend rollout, and fresh review/evidence; a ledger-target projection
is never treated as the complete installation set. M3 activation requires
every static-guard capability entry to match this inventory, and requires the
set of all ledger static-guard targets to be a covered subset. A missing,
duplicate, extra-live, or uninventoryable installation keeps the entire target
shadow. This complete deployment inventory is the authority used by the
exceptional guard teardown below, so a guard installed but never used cannot
be stranded by dropping its database table.

Dynamic admission needs a happens-before fence; an independent watch is not
one. V1 admits exactly two execution-chain modes:

- `dynamic_engines_disabled`: the allowlisted immutable backend config has
  dynamic mutating/validating webhooks and mutating/validating admission-policy
  engines disabled for the process lifetime. Configuration objects may exist
  but are inert. The freeze proof uses
  `mode=dynamic_engines_disabled`, zero pre-freeze wait,
  `snapshot_not_before=opened_at`, null guard, and an exact empty
  backend-rejection array. It still uses the post-wait SQL challenge in a
  transaction after the freeze INSERT commits.
- `static_manifest_guard`: every backend contains the exact allowlisted
  apiserver-native/static-manifest freeze module in its immutable request
  execution chain. This is not an ordinary API-registered webhook or policy:
  Kubernetes excludes circular admission-configuration paths from those
  mechanisms. The module is loaded before the dynamic engines, cannot reload,
  has no external side effect, and performs one bounded uncached PostgreSQL
  `READ COMMITTED` statement with a new snapshot for each protected request.
  Its immutable dependency configuration pins the same authoritative central
  PostgreSQL cluster/database and TLS identity used by the journal, verifies
  the exact database-identity hash and `pg_is_in_recovery()=false`, and forbids
  a load-balancing pool or readable-replica endpoint. A primary-role/identity
  mismatch, stale replica, reused transaction snapshot, connection/statement
  timeout, or unreadable freeze row fails closed. On the primary, a guard
  statement starting after the freeze INSERT commits must observe it; a
  statement that won the race immediately before commit is covered by the
  pre-freeze settlement wait.

  While any unreleased executor-registration freeze exists, the module rejects
  before dispatch every create/update/patch/named delete or collection delete
  of
  `MutatingWebhookConfiguration`, `ValidatingWebhookConfiguration`,
  `MutatingAdmissionPolicy`, `MutatingAdmissionPolicyBinding`,
  `ValidatingAdmissionPolicy`, and `ValidatingAdmissionPolicyBinding`. It also
  rejects mutation of
  its own exact manifest/config/binary, database credential, Namespace,
  ServiceAccount/RBAC, Pod/Deployment, Service/EndpointSlice, Secret, and
  ConfigMap dependency inventory. The same named and collection-delete coverage
  applies to those dependencies; a collection selector that can include one is
  rejected conservatively. DB timeout, missing row, malformed epoch, or
  guard dependency failure rejects the protected request. The exact static
  module binary/config/rules/dependency/database-identity/visibility hashes are
  identical across the pinned backend set and are checked-in allowlist inputs.
  Its installation ID and complete deployment-inventory/entry hashes are also
  identical to the matching inventory entry and are part of every backend
  request-chain hash.
  DeleteCollection/name-empty handling is an explicit rule; it cannot fall
  through the named-delete path.

  A well-formed downgrade row is deliberately not interpreted as another
  unreleased registration freeze. Every registration is already RELEASED
  before that row can be inserted, and v1 supports only a `StaticPodOwner`
  whose exact guarded-to-guard-free replacement uses the declared out-of-band
  node/control-plane channel and sends no Kubernetes API owner, template,
  manifest, or configuration write. In-band
  StatefulSet, DaemonSet, Deployment, operator, or self-hosted Kubernetes-API
  rollout is ineligible for this branch. The required target maintenance
  quiesce revokes human/operator protected-write credentials and pins the exact
  kubelet/control-plane identities to only the inventory-matching mirror-Pod
  and `kubernetes` Service/EndpointSlice churn caused by that out-of-band
  replacement. An unrelated admission/dependency write is denied before
  dispatch and invalidates the teardown evidence. This avoids both a broad
  DRAINING bypass and a guard that deadlocks its own replacement.

  The module also wraps the full lifetime of every protected mutation handler
  in a per-backend in-flight epoch. Every protected request increments the
  current epoch under the gate lock before its primary DB read and leaves only
  after rejection or synchronous handler completion. Once the module observes
  the open freeze it closes that epoch to new entrants; an entrant racing the
  close is either counted in the sealed epoch or rejected before dispatch. Its
  read-only barrier endpoint rechecks the primary freeze, atomically seals the
  epoch, and returns the
  challenge-bound barrier evidence only after every old-epoch handler has
  exited. The reviewed module hashes the authenticated request's literal bearer
  token before header disposal and includes only that SHA-256 in the response/
  audit projection; it never returns or logs the token. A build that cannot
  expose that exact binding is ineligible. A failed registration or later
  freeze-row release never discards a nonzero sealed epoch; a subsequent
  barrier waits the union of every earlier unresolved epoch plus its newly
  sealed epoch. A backend restart kills its own handlers and changes the pinned
  backend/connection identity rather than fabricating a drain. This, not
  `requestTimeout`, handles Kubernetes DeleteCollection,
  which has no finite generic request timeout. Failure to drain within the
  registration deadline prevents capability finalization and leaves the target
  shadow.

Registration-ledger table absence, an unknown downgrade phase, or a
missing/malformed downgrade-state structure is never an allow condition inside
a guard-enabled process. A recognized DRAINING/DISABLED row changes no
registration-freeze decision; the narrow out-of-band rollout contract above is
enforced by the maintenance quiesce and evidence. The ordinary guard query
continues to fail closed if the executor-registration ledger has been removed.
Only a replacement backend whose immutable chain is an exact
entry in
`KUBERNETES_RESOURCE_ACTION_GUARD_DISABLED_BUILD_ALLOWLIST_V1`, contains no
static freeze module or other ledger reader, advertises no resource-action
runtime capability, and is proven by the exceptional teardown may run after
the ledger is dropped. This v1-disabled guard-free chain is not either
authority-capable execution-chain mode and may restore the deployment's normal
dynamic admission behavior because resource-action mutation is globally
quiesced.

The registration INSERT opens the freeze before any capability evidence is
collected and stores its immutable mode/timing tuple. The client chooses no
numeric wait: SQL maps disabled mode to literal zero and guarded mode to the
literal conservative 120-second v1 bound, then derives
`snapshot_not_before=opened_at+prefreeze_settlement_seconds`. Post-challenge
live evidence must prove the actual
`request_timeout_seconds + admission_cache_propagation_bound_seconds <= 120`;
otherwise finalization rejects. This supplies the initial conservative wait
without a circular or client-selected `/configz` estimate; the barrier and
post-drain wait below remain mandatory for a handler that outlives it. At or
after that database time, the locked SQL edge above issues
the second one-use challenge. Only challenge-bound requests made afterward may
enter the barrier proof: the verifier opens new direct TLS connections to every
backend and invokes the read-only guard barrier on each. Every barrier must
return the exact backend/freeze/challenge epoch and drained evidence. SQL then
records that complete set and its database time, waits the additional literal
60-second cache bound from that drain time, and issues the post-barrier
challenge. Only nonce-and-two-challenge-bound requests made after that final
edge may supply `/configz`, backend enumeration, complete MostRecent admission
snapshots, watch bases, TokenReview/dry-run evidence, or the live guard probe.

The probe is a non-persisting, idempotent `dryRun=All` DELETE of one
preprovisioned inert `ValidatingWebhookConfiguration` sentinel. The sentinel
has a fixed reserved name, no applicable resource rules, literal
`sideEffects=None`, and an exact allowlisted immutable spec/dependency hash.
Its UID, resourceVersion, and spec must be present and matching in the
post-barrier MostRecent snapshot; absence or drift rejects capability. Only after
the complete snapshot has also proved zero applicable mutators and safe
validating admission does the verifier send `DeleteOptions` with that matching
UID and resourceVersion. This ensures the Kubernetes delete path reaches
admission rather than returning before it, while server-side dry-run forbids a
storage change even if the guard is missing.

The fixed credential has exactly the least-privilege delete permission for
that one sentinel `resourceName` in addition to its read permissions; it has no
create/update/patch or other admission-configuration delete permission. The
request carries the nonce-and-two-challenge Audit-ID, and its complete literal request,
dry-run/matching-precondition values, sentinel snapshot/spec, RBAC projection,
and response/audit projection are hashed. Correct guard execution returns its
exact checked-in denial before dynamic admission or storage. With a missing
guard, any 2xx/404/409/dry-run response is non-matching and the dry-run still
cannot create, change, or delete an object; a generic successful mutation is
never a probe. The sentinel and every dependency are themselves in the static
guard's protected inventory, so no post-freeze writer can swap the object used
by the probe.

Guarded mode stores exactly one barrier-plus-rejection record for every backend
in backend order;
disabled mode stores exactly zero. The freeze object rejects every other
guard/backend-rejection cardinality. A dynamic engine without the static
guard, a normal webhook pretending to be the guard, a missing/different backend
rejection, a response without the challenge marker, or any pre-wait snapshot
keeps the target shadow.

The verifier also performs MostRecent LISTs of every
`MutatingWebhookConfiguration` and `ValidatingWebhookConfiguration` using the
same empty-resourceVersion/no-`0`/complete-pagination rules, stores both
collection resource versions, and evaluates the Kubernetes rule matching
algorithm against every v1 verb/kind/scope combination. Applicability is
deliberately conservative: any webhook whose rules can cover a supported
combination is applicable regardless of its namespace selector, object
selector, match conditions, or current labels. Those fields cannot exclude it
from the audited set.

An applicable `MutatingWebhookConfiguration` rejects the capability even when
it declares `sideEffects=None`: that declaration does not prevent mutation of
the admitted object and therefore cannot prove preservation of the protected
locator label. The capability stores the complete mutating-configuration
collection hash and literal zero applicable count. Every applicable validating
webhook is represented by one sorted exact registration descriptor and must
declare literal `sideEffects=None`; `NoneOnDryRun`, omitted/unknown side
effects, an unbounded timeout, or an unsupported validating registration keeps
the target in shadow. The descriptor hashes the complete applicable rules and
client config, and its configuration resource version changes on any
selector/condition or other same-name rewrite.

Discovery and the checked-in build entry also close Kubernetes mutating
admission policies. If the allowlisted build does not serve both
`MutatingAdmissionPolicy` and `MutatingAdmissionPolicyBinding`, the capability
uses `mutating_policy_api=allowlisted_not_served` and all four policy
RV/snapshot fields are null. If it serves them, the verifier performs complete
MostRecent LISTs of both collections, stores their nonempty collection RVs and
complete hashes, evaluates every policy/binding whose resource rules can cover
the four obligation kinds regardless of selectors/conditions/parameters, and
requires literal zero applicable policies. A served/absent mismatch, only one
served API, unreadable or incomplete collection, applicable policy, or another
discovered dynamic object-mutation admission surface rejects the capability.
The verifier likewise discovers and completely MostRecent-LISTs
`ValidatingAdmissionPolicy` and `ValidatingAdmissionPolicyBinding` when served,
stores both RV/hash pairs, and includes their full snapshots in the frozen
admission hash. They cannot mutate the object, but an unreadable, partially
served, or post-freeze-inconsistent surface still rejects exact verification.

For every backend, `/configz` evidence must project the exact enabled built-in
and static admission chain plus the complete content hashes of every referenced
configuration file into the backend's
`request_execution_chain.referenced_config_set_sha256` and
`request_execution_chain_sha256`. The checked-in build entry admits only a
reviewed immutable chain that preserves arbitrary unknown metadata labels on
all four obligation kinds and has no external/static object mutator capable of
rewriting the protected label. It also proves that the chain and referenced
configuration cannot reload in place; changing them requires a backend
process/connection replacement, which the pinned connection and EndpointSlice
watch invalidate. Unreadable/reloadable/unreviewed plugin state,
`ImagePolicyWebhook` or another external static admission dependency, other
than the exact freeze guard, and a backend whose complete request execution
chain differs from the allowlisted hash remain shadow.

Through the same fixed credential and a verified backend connection, startup
sends the checked-in
server-side dry-run probe matrix and hashes the authenticated response/audit
projection; no probe object is persisted. Dry-run success is supporting
evidence only and never substitutes for the exact real-call side-effect
registration audit.

The four sorted literal obligation kinds are exhaustive, every adapter mutation
is a synchronous API-server object write, and the adapter cannot start a
controller, background task, exec stream, or deferred writer that could create
one of those objects later.

The reviewed build set is the checked-in exact constant
`KUBERNETES_EFFECT_SETTLEMENT_BUILD_ALLOWLIST_V1`, whose entries bind
`gitVersion`, `gitCommit`, every accepted `configz_sha256` and
`request_execution_chain_sha256`, the static freeze-module binary/config/rules
and installation/deployment-inventory hashes plus the per-backend
in-flight/primary-visibility barrier when present, the complete
dynamic mutation-policy discovery surface,
protected object identity/no-reload contract, storage contract,
exact MostRecent LIST/GET semantics, maximum authentication-clock skew,
maximum request timeout, and the checked-in
`KUBERNETES_MUTATION_RESPONSE_MATRIX_V1` hash. An empty set advertises no
capability. Adding or widening an entry requires code review plus the
delayed-response, concurrent-admission-change, protected-label, and
late-MostRecent-LIST/GET conformance suites; it cannot be supplied by Helm
values, a ConfigMap, or an environment variable.

The separate
`KUBERNETES_RESOURCE_ACTION_GUARD_DISABLED_BUILD_ALLOWLIST_V1` is usable only
by exceptional downgrade. Each entry binds the exact guard-free backend
binary/config/request-chain hash, proves that neither that process nor any
sidecar/init/reload path contains the static guard or reads a resource-action
table, and advertises literal `resource_action_authority=disabled`. It confers
no settlement capability and can never satisfy M3 activation.

`max_effect_settlement_seconds` equals the verified
`request_timeout_seconds`, never a client-selected value.
Every `settlement_capability_sha256` is the PostgreSQL-canonical SHA-256 of the
complete object above, and every copied maximum equals its field.
The verifier proposes evidence and the provider-signed credential projection,
never authoritative wall-clock fields. Capability-finalization SQL captures
one `v_now`, sets both `registered_at` and `verified_at` exactly to it, validates
the registration nonce and both challenges in every final evidence hash, requires
`postwait_challenge_issued_at >= admission_freeze_snapshot_not_before` and
`postwait_challenge_issued_at <= admission_barrier_drained_at`, requires the
barrier set/hash to cover exactly the guarded backend set (or be canonically
empty in disabled mode), requires
`admission_cache_snapshot_not_before =
 admission_barrier_drained_at + interval '60 seconds'` in guarded mode or equal
to the barrier time in disabled mode, requires
`postbarrier_challenge_issued_at >= admission_cache_snapshot_not_before` and
`postbarrier_challenge_issued_at <= v_now <=
 nonce_issued_at + interval '300 seconds'`, requires
the final `FixedExecutorCredentialV1.token_sha256` to equal every guarded
barrier's credential-token hash, requires
the complete ledger freeze ID/mode/open/snapshot/two-challenge/barrier/cache
tuple to be byte-equal to `AdmissionConfigurationFreezeV1` and the verified
execution chain (`0` and `snapshot_not_before=opened_at` for disabled mode; literal
`120` and `snapshot_not_before=opened_at+interval '120 seconds'` for guarded
mode), and in guarded mode requires the post-challenge live
`request_timeout_seconds + admission_cache_propagation_bound_seconds <= 120`,
and requires
`credential.issued_at >= nonce_issued_at -
make_interval(secs => authentication_clock_skew_seconds)`,
`credential.issued_at <= v_now +
make_interval(secs => authentication_clock_skew_seconds)`, and
`credential.expires_at <= credential.issued_at + interval '600 seconds'`, and
derives
`expires_at = least(v_now + interval '60 seconds',
credential.expires_at -
make_interval(secs => authentication_clock_skew_seconds))`. It requires that
derived expiry be at least `v_now + interval '30 seconds'`; otherwise the row
never reaches `READY`. SQL also sets
`admission_freeze_release_not_before =
greatest(expires_at,
         credential.expires_at +
         make_interval(secs => authentication_clock_skew_seconds)) +
make_interval(secs => max_effect_settlement_seconds)` and requires the
byte-equal value in `AdmissionConfigurationFreezeV1`. Future-dated/stale client
verification time cannot extend the capability. This conservative release
bound outlives both every permitted window deadline and the fixed credential's
latest acceptance time, then adds the verified maximum effect-settlement
interval.

Before finalization, the executor starts gap-free watches at the stored
EndpointSlice, both webhook-configuration collection resource versions, and,
when served, all mutating/validating policy/binding collection resource
versions. Every
watch basis binds the database nonce, both challenges, fixed credential
projection, target, backend/request-chain set, and complete collection hashes. Any event,
compaction/410, disconnect, resource-version gap, or set/hash mismatch
atomically invalidates every process-local backend connection, withdraws
readiness/runtime token, and closes every affected open window under the exact
classification below: before any slot enters transport it is
`no_bytes_sent`; after any slot enters it is `ambiguous`. Because watch
delivery can lag the admission write it reports, exact-name observations remain
mandatory and never rely on watch silence to prove provider absence. A fresh
complete verification, not watch resumption by guess, is required.

The freeze remains open across readiness consumption or the complete action
claim/window lifetime; heartbeat loss never releases it. In guarded mode the
static module continues rejecting protected configuration/dependency writes.
The release store first discovers all keys without locks. It may take
`CONSUMED/EXPIRED -> RELEASED` only in a transaction that locks current
ownership epoch when scoped, registration, required Serve/domain rows,
consumed action, request/effects, and only then its instance row if still
present. The PostgreSQL guard obtains one `v_now` itself and
requires `v_now >= admission_freeze_release_not_before`; a caller timestamp
cannot satisfy the edge. It also requires a fresh exact
`KubernetesPodUidAbsenceEvidenceV1` for the registration's target,
namespace/name, and worker/Pod UID. Thus an absent instance row is not guessed
to be drained: it is accepted only with the same strong MostRecent Pod-UID
absence proof. If the instance row remains, it must already be draining, omit
the runtime capability, and match the immutable registration/worker pair.
Strong Pod absence closes every local call gate; a process heartbeat or
`draining=true` assertion alone never does.

The transaction accepts exactly one release authority case:

- `unconsumed_no_request` is an `EXPIRED` registration with no consumption and
  no resource-action request/fence that names its registration or worker;
- `readiness_no_request` is a `CONSUMED,purpose=readiness` promotion record with
  no request/fence and therefore no mutation window;
- `action_no_window_terminal` locks the exact consumed request/action/attempt,
  requires both terminal, `frozen_effect_count=1`, an aggregate hash covering
  exactly executor-fence effect zero and zero mutation-window effects, and no
  remaining claim/window authority;
- `action_definitive_terminal` requires both terminal and every normalized
  frozen window effect
  to have a complete non-ambiguous `no_bytes_sent` or `definitive_response`
  result with every slot closed; this is the legal path when
  `execution_quiescence` is null; or
- `action_quiesced` requires the byte-equal append-once
  `ProviderExecutionQuiescenceV1`, frozen effect count/aggregate hash, and SQL
  `v_now >= quiescence.settle_after` for any missing/ambiguous window.

Every action case requires no later request/window for this one-use
registration. The release store recomputes the terminal/effect/quiescence
count and hashes under those locks, validates the release evidence's exact case and
Pod/instance projections, and supplies only those database-owned values to the
guard. The guard checks the same relational predicates, canonical evidence
hash, and nullability matrix; direct UPDATE cannot manufacture an edge.
It writes `release_evidence`,
`release_evidence_sha256`, `release_evidence.recorded_at=v_now`, and
`admission_freeze_released_at=v_now` together. In guarded mode the static
guard's next primary `READ COMMITTED` statement then permits protected writes.
The registration remains an immutable `RELEASED` tombstone forever. A stuck
reconciler therefore blocks admission-configuration administration rather than
releasing an uncertified window; this operational cost is an explicit
authoritative-v1 eligibility condition. Disabled-engine mode follows the same
ledger transition for uniform fencing even though configuration objects are
inert.

An abandoned `VERIFYING` row cannot strand the freeze forever: after SQL time
`nonce_issued_at + interval '300 seconds'` (the complete first-wait/barrier,
post-drain cache, and final-evidence budget), the HA reconciler locks the
registration and any extant instance, proves no capability/consumption or
request fence ever existed, drains the one-use Pod, and writes
`VERIFYING -> EXPIRED` with `expiration_reason=nonce_timeout` and
the SQL-derived conservative
`release_not_before=nonce_issued_at + interval '1080 seconds'`. The 1080 seconds
cover the maximum 300-second verification lifetime, a provider `issued_at` up
to 60 seconds ahead, the 600-second projected token lifetime, the further
60-second latest-acceptance skew, and 60-second maximum effect settlement even
when finalization never stored a credential projection. It may
reach `RELEASED` only after that bound and the strong Pod-UID absence/no-request
release case above. An unconsumed READY row expires only after its
capability expiry and retains the already conservative
SQL-derived credential/capability release bound. The
subsequent RELEASED edge still requires the no-authority/drain checks above.
No crash path shortens either bound or reopens the nonce.

The dedicated one-request executor never replaces its registered capability
object: heartbeats only prove the same object remains unexpired and both
watches gap-free. Here “both” means the EndpointSlice watch family and the
complete admission watch family above. It stops minting windows and drains
after expiry; a newly
verified executor Pod with a new object serves a later attempt. Thus a benign
timestamp refresh cannot silently change the hash under an open window. If `/configz`,
backend enumeration, applicable-admission enumeration/watch, required RBAC,
the dry-run path, exact response matrix, or provider-side bound is unavailable,
the capability is not issued and the target remains shadow. A
transport/connect/read timeout, Kubernetes client setting, webhook dry-run
declaration alone, or successful sample call is explicitly insufficient.

Authoritative v1 runs a correlated mutator only in a dedicated, single-request
executor Pod in the HA Kubernetes API-server control plane. The Pod's canonical
UID equals `worker_instance_id`; it has no detached child/container capable of
provider I/O, and it is not reused. The provider target is that same management
cluster. After the database has issued the registration's post-wait challenge
and before the barrier probe, the supervisor reads exactly one projected
service-account token bound to the executor Pod UID and uses that same literal
token for every barrier connection. After SQL issues the post-barrier
challenge, it obtains the successful nonce-and-two-challenge-bound TokenReview
and validates its signed
issuer/subject/audience/iat/exp/bound-UID projection and allowlisted clock-skew
bound, hashes the complete TokenReview response evidence, and keeps that
literal token fixed. The startup verifier uses that same token for every
authenticated probe and hashes the full `FixedExecutorCredentialV1` projection
plus both database challenges into each proof/watch basis. Capability finalization,
not the verifier, sets
`credential.token_review_verified_at=verification.verified_at=registered_at`
to its one SQL `v_now`; all other credential fields are verified provider
inputs. The later claim copies that byte-equal credential and capability into
the executor fence. The supervisor then gives the token to the child through a
sealed one-read credential handle. The child has no projected-volume mount,
refresh callback, kubeconfig fallback, or token reload path; every target API
call in the whole claim uses that one token. The raw token is never durable;
only the exact credential projection and evidence hashes are. The TokenRequest
asks for exactly 600 seconds and the
returned `expires_at` may not exceed `issued_at + 600 seconds`; audience,
bound Pod UID, issuer/subject, issued/expiry times, TokenReview evidence,
token/claims hashes, and `authentication_clock_skew_seconds` must be byte-equal
between the capability credential, wrapper evidence bases, and executor-fence
credential. Failure before the atomic claim leaves no claimed request, and the
single-use executor is discarded. Static kubeconfig, user credential, remote
cluster, role=`all`, local-server, and non-role-split execution are ineligible.
The dedicated executor completes the exact registration protocol in its
executor-registration row and advertises it from `api_server_instances`.
After nonlocking key discovery, the claim transaction locks current ownership
epoch and that fresh `state=READY,purpose=action` registration before action;
it locks the referenced non-draining instance row only after request,
queue/effects. It requires both runtime capability
tokens and the recomputed adapter-manifest attestation, exact
target/control-plane fingerprints, healthy gap-free watches, byte-equal
credential/capability hashes, and one unconsumed nonce. With one SQL `v_now` it
atomically consumes the registration for the exact
service/action/request/attempt, installs `claimed_at=v_now`, and appends the
fence whose registration `consumed_at` equals `claimed_at` and whose
registration/audit/capability fields are byte-equal to the ledger; it requires
`capability.expires_at > v_now`. Every mutation-window issue/result
transaction rediscovers keys without locks, then locks current ownership epoch,
the same `CONSUMED/request_claim` registration, required Serve/domain rows,
generic action, request/effects, and instance last if needed. It additionally
requires both `capability.expires_at` and
`credential.expires_at - authentication_clock_skew_seconds` to be at least
`call_complete_deadline_at`. It copies the admission hash and the
index/canonical hash of one byte-equal member of the verified backend set and
binds the already authenticated non-reconnecting TLS connection for that
backend. A call may use
only that connection; it cannot reconnect through the Service/VIP or select a
different endpoint. A target whose managed Kubernetes endpoint cannot expose
and pin this exact connection/backend contract remains shadow. Losing/changing
the heartbeat or either watch prevents another window and invalidates an open
one as specified below, but cannot erase the historical capability that
authorized earlier bytes.

The claim transaction obtains the Pod and node identity from the trusted
executor launcher, hashes the canonical claim-token and fixed-token texts,
validates the credential projection above, and atomically appends the fence as
normalized effect zero while moving generic ownership/action/request/queue to
their first claimed/RUNNING shape. A claimed request with effect count zero is
therefore structurally impossible. The
executor Pod runs one supervised `DisposableExecutor` child with an exact
PID/start-ticks handle, not the shared `ProcessPoolExecutor`. The supervisor
renews the database claim on a bounded call independently of the child and,
before the lease-minus-five-second monotonic guard, either confirms renewal or
SIGKILLs and `waitpid`/joins that exact start-ticks identity. A signal-send
acknowledgement, raw PID, or PID reuse is never exit proof.

API006 adds no effect column or combined mutable projection object. It maps
Serve provider data into API005's generic typed fields exactly: the
`effect_kind`/`phase` columns carry the closed Serve kind and phase; the intent
quartet carries either `ProviderExecutorFenceV1`,
`ProviderMutationWindowV1`, or the observation-read intent; the readback
locator quartet carries `KubernetesProviderLocatorV1`; the two provider-ID
columns carry only IDs learned for that effect; the result quartet carries
`ProviderMutationWindowResultV1`; and the evidence quartet carries the closed
provider-observation evidence. The generic facet identity binds provider,
adapter, version, and implementation hash. No Serve object duplicates generic
effect state, IDs, or typed quartets.

`execution_fence` is exactly effect zero. It requires provider `kubernetes`,
adapter `ha-kubernetes-executor-v1`, the complete non-null executor fence, and
null operation, token, window/result, locator/hash, region, and zone; `observed_at`
equals `claimed_at`. Every later effect has null executor fence and its claim
hash must equal effect zero.

`prepared` requires the two operation fields null, the complete non-null
locator and exact canonical hash, and Kubernetes
provider/observation-adapter literals, `region=locator.scope.context`, and null
zone. It atomically carries the first non-null mutation window for that locator
with `phase=prepared`, but may not carry a result in its initial write. An
attempt contains at most one prepared effect for a locator hash; re-preparing the same retained
target is forbidden. Submission/provision/failover/cleanup require null
locator, a non-null locator hash equal to an earlier prepared effect, and a
non-null mutation window whose phase equals the outer phase. Observation
requires the same earlier locator hash but null window/result; if present it
commits before the observation read fence and is included in the basis hash,
never appended after a sweep. Kubernetes
operation/idempotency IDs in an initial effect projection are limited to values
known before I/O; an operation ID learned from a response appears only in the
non-authorizing result. Thus a locator-bearing PREPARED element, rather than an
invented operation ID, is the durable target fence.

Every mutation window is issued in one new contiguous normalized effect only by one live
claim-fenced database transaction. Immediately before sending that transaction
request, the executor generates a fresh nonce and captures
`m0 = clock_gettime_ns(CLOCK_BOOTTIME)`. The transaction verifies the current
claim, executor fence, fresh matching settlement-capability heartbeat, healthy
watch cursors, locator and phase. It validates 1..8 exact canonical call slots,
requires their indexes to be the contiguous sequence `1..max_calls` and
`max_calls` to equal their count, selects a byte-equal verified backend
descriptor by index/hash and its still-open non-reconnecting TLS connection,
assigns a fresh `window_id`, and stores the nonce/connection hashes;
captures one SQL `v_now`, sets `issued_at=v_now`,
`call_start_deadline_at=v_now+20 seconds`, and
`call_complete_deadline_at=v_now+30 seconds`; requires the latter not later
than the lease, capability `expires_at`, or
`credential.expires_at - authentication_clock_skew_seconds`; and commits the
complete normalized window effect plus the request's new effect count and
ordered aggregate SHA before returning.
Each slot's `request_sha256` is the PostgreSQL-canonical hash of the exact
verb, direct backend-relative path/query, content type, bounded body bytes, and
UID/resource-version preconditions. Authorization and raw token bytes are
excluded; the fixed credential hash is already in the executor fence.
Every slot target must be a byte-equal member of the prepared locator's
immutable named-target inventory. Every create/patch body must carry that
inventory entry's exact `metadata.name`/namespace with no `generateName`, must
carry the locator's exact protected label, and no patch operation may rename
the target or remove/replace that label. The database validates membership,
name, and body-label projection before
committing the window.
The executor verifies the returned nonce/hash and derives its local absolute
deadlines as `m0+20 seconds` and `m0+30 seconds`, not from response-receipt
time. Database/RPC latency therefore consumes both budgets. A transaction
retry, lost/unknown response, nonce mismatch, process resume on another boot,
or expired local deadline discards the window; it is never reused.
The supervisor passes those absolute `CLOCK_BOOTTIME` nanosecond deadlines and
requires the current Linux boot-ID hash to equal the executor fence in both
processes; neither converts a deadline to a duration or resets it after IPC.

Before any mutating request byte is written, the adapter rechecks the
`CLOCK_BOOTTIME` start deadline, capability expiry, complete committed
locator/effect/window, exact backend connection hash, and both gap-free
watch cursors. It may enter only the already authenticated connection; socket
creation, TLS handshake, reconnect, Service/VIP fallback, or another backend
after window commit is forbidden. Immediately before each call it repeats the
deadline/watch/connection checks and executes the matching predeclared slot.
It sets the per-call total transport timeout to the lesser of ten seconds or
the remaining completion budget. All slots are sequential and must close
within the same local deadline. A watch event/gap, heartbeat/capability
invalidation, or connection loss before the first slot enters transport closes
every slot as `skipped_before_transport` and the window as `no_bytes_sent`.
Once any slot has entered transport, any such invalidation before the window is
sealed reclassifies the most recently entered slot as `ambiguous`, even if it
had received a complete response, and closes every later slot as
`skipped_before_transport`; all earlier completed slots retain their exact
response projections. More work first commits another claim-fenced window. A
database partition cannot mint or renew one.

`KUBERNETES_MUTATION_RESPONSE_MATRIX_V1` is a checked-in exact constant whose
hash is bound by the capability. It admits only these complete responses:

- create: HTTP 201 with a fully decoded bounded Kubernetes object of the
  requested kind/name/namespace, a nonempty UID, and the byte-equal protected
  locator label key/value;
- patch: HTTP 200 or 201 with that same exact identity/label projection;
- UID-precondition delete: HTTP 200 with either the exact deleted-object UID or
  a complete `Status{status=Success}` for that target; and
- definitive rejection: HTTP 400, 401, 403, 404, 405, 406, 409, 410, 413, 415,
  422, or 429 with a complete bounded Kubernetes `Status` whose reason and
  details are the exact checked-in status/verb pairing.

Each accepted/rejected entry requires authenticated TLS end-of-stream, a
supported JSON content type, complete body decode of at most 1,048,576 bytes,
matching audit ID,
and the applicable `sideEffects=None` admission snapshot. Every redirect,
HTTP 202/204, 408, 5xx, unlisted status/reason, webhook timeout, malformed or
truncated body, decode mismatch, partial request write, HTTP/2 reset, transport
timeout/disconnect, or any watch/backend-connection invalidation after a slot
enters transport is ambiguous. A watch/backend invalidation before the first
slot enters follows the all-skipped `no_bytes_sent` rule above. No status code
alone is definitive.

After the adapter stops, the live claimant may fill that normalized effect's
`window_result` exactly once under the claim. This replaces only null with one
closed result; all other effect bytes and effect ordering remain identical.
The same transaction recomputes the request's ordered aggregate SHA, and
`completed_at` is the result-fill transaction's PostgreSQL
`clock_timestamp()`. “Stops” means the sequential adapter frame returned its
sealed complete slot vector, the supervisor closed that window's local call
gate, and no child frame or transport callback can use another slot; result
fill is not concurrent with adapter code. The result `window_id` equals its parent window,
`completed_at >= issued_at`, and its call array has exactly `max_calls`
contiguous entries indexed `1..max_calls`. Entry `i` hashes and matches call
slot `i`.
`skipped_before_transport` means that slot was never handed to the HTTP
transport: no request stream, header, or data byte was created or submitted.
Its response/operation fields are null. Once result fill commits, every
skipped slot is durably closed and can never execute.

`no_bytes_sent` requires every slot skipped and null watch validation.
`definitive_response` requires at least one accepted/rejected slot, no
ambiguous slot, every other slot durably skipped, and a non-null evidence hash
from a gap-free watch through a final MostRecent backend/admission-set
revalidation. That hash covers the capability's two watch-basis hashes, window
ID, backend index/hash/connection hash, ordered event-free watch cursor
intervals, fixed-credential and registration-nonce hashes, static admission
freeze/barrier/cache/two-challenge and complete request-chain hashes, final
EndpointSlice/webhook/policy/binding collection resource versions,
and byte-equal final set hashes. `ambiguous` requires exactly the first
ambiguous slot after zero
or more definitive slots; every later slot is skipped. It retains any bounded
status/evidence actually received, but that data cannot become definitive.
Missing/partial result data or a failed result write leaves the stored result
null and is normalized as ambiguous at terminalization.

For a definitive result, database `completed_at` is not later than
`call_complete_deadline_at`. Immediately before submitting result fill, both
supervisor and child recheck the fenced boot ID and prove
`clock_gettime_ns(CLOCK_BOOTTIME) <= m0 + 30 seconds`; a local deadline
overrun is ambiguous even if a response later arrives. The learned operation
ID is evidence only and grants no new mutation authority.

Duplicate canonical window objects are rejected, effect indexes are contiguous
from zero, and `observed_at` is nondecreasing across normalized effects. The
sole non-insert change is the claim-fenced append-once null-to-result fill
above; it is forbidden after terminal freeze. Effects never contain credentials, request
bodies, environment values, raw tokens, or raw exceptions. Application code
rejects an unknown version or over-limit insert before the claim-fenced update;
PostgreSQL repeats the exact-version, effect-count/aggregate-hash,
per-effect-object,
executor/claim/fixed-credential, window-deadline/capability-expiry,
backend-index/descriptor hash, admission-set, call-slot/result-matrix,
completed-or-skipped result-fill, prepared-locator uniqueness,
locator-hash/prefix, and database-canonical serialized-size checks. The trusted
executor additionally proves the live connection/watch hashes at issue,
pre-byte, per-slot, and definitive-result boundaries.

Terminalizing or revoking a claimed attempt freezes the complete normalized
effect count and PostgreSQL-canonical ordered aggregate SHA-256. No absence
decision, replacement
attempt, or provider mutation may occur merely because the claim expired.
Instead the existing owner under its retained claim tuple, or the HA reconciler
after owner loss, installs exactly one `ProviderExecutionQuiescenceV1` in that
immutable request attempt's
`resource_action_execution_quiescence` column. The action outcome carries a
byte-equal decision-time copy while that attempt is current; it is not the
durable source. External supervisors/reconcilers submit identities and bounded
evidence hashes, never authoritative event times. After all required external
checks have completed, the proof-publication transaction locks the exact
request/fence, captures one PostgreSQL `v_now=clock_timestamp()`, assigns every
method's authority/receipt time as specified below, derives `revoked_at` and
`settle_after` in SQL, and writes the complete proof once:

- `not_submitted` requires `frozen_effect_count=1` and an aggregate containing
  only executor-fence effect zero, the parent still holds the
  disposable-executor dispatch lock,
  and the child was never spawned. All three optional proof objects are null,
  `mutation_boundary=not_entered`, `revoked_at=v_now`, and
  `settle_after=revoked_at`.
- `worker_exited` requires the exact PID plus start-ticks handle to have exited
  and been joined with no provider call in flight. Only `process_exit` is
  non-null; SQL sets `process_exit.joined_at=revoked_at=v_now` after validating
  the exit/join evidence. An execution-fence-only effect set means
  `not_entered`; any prepared or later target effect means
  `prepared_or_later`.
- `pod_token_revoked` is the fast remote fail-stop available only while a
  surviving claim-time supervisor still holds the exact volatile fixed-token
  preimage. The reconciler DELETEs the exact executor Pod with its UID
  precondition and 30-second grace, stores the exact old-UID absence proof
  below, and TokenReviews that preimage as `authenticated=false` with exact
  `bound_object_not_found`. SQL sets
  `pod_token_revocation.evidence_recorded_at`,
  `old_uid_absence.observed_at`, and `revoked_at` all to `v_now`; delete and
  TokenReview response hashes are verifier inputs. Only
  `pod_token_revocation` is non-null.
- `fixed_token_expired` is the owner-loss path constructible from durable state
  alone. A fresh HA reconciler DELETEs the exact Pod with its UID precondition,
  stores the exact old-UID absence proof, and locks the
  request/executor-fence credential. SQL sets
  `expiry_not_before = token_expires_at +
  make_interval(secs => authentication_clock_skew_seconds)`, requires the
  signed-claims/token/bound-UID/skew projection byte-equal to the executor
  fence, and requires `v_now >= expiry_not_before`. It sets
  `fixed_token_expiry.evidence_recorded_at`,
  `old_uid_absence.observed_at`, `verified_at`, and `revoked_at` all to that
  same `v_now`. Only
  `fixed_token_expiry` is non-null. Because the child could use only that one
  non-refreshing token, no secret preimage is needed after this certified
  expiry.

`KubernetesPodUidAbsenceEvidenceV1` is obtained through the reconciler's own
credential against the exact management-cluster fingerprint. It uses an
authenticated default/MostRecent Pod GET with literal `resourceVersion=""`,
omitted `resourceVersionMatch`, and no cache, `0`, `Any`, `NotOlderThan`,
watch, or streaming override. `not_found` requires a strong 404 and null
`observed_uid`; `different_uid` requires a successful complete object whose
name/namespace match and whose non-null UID differs from `expected_uid`.
Target fingerprint, kube-system UID, namespace/name, expected UID, and
response projection must equal the executor fence. `observed_at` is the proof
publication transaction's SQL receipt time, not a client/provider timestamp. A
same-name/new-UID Pod without this exact proof, delete acknowledgement alone,
an unbound 404, force deletion alone, a projected-token reload path, an
unverified JWT claim, or database time before the allowlisted skew margin is
not quiescence. If the fast TokenReview proof is unavailable, the action waits
for `fixed_token_expired`; it does not remain permanently blocked merely
because the original owner and raw token were lost.

For `prepared_or_later`,
`last_mutation_window_complete_deadline_at` equals the maximum complete
deadline in the frozen normalized effects. Every window capability hash must
equal the executor fence and quiescence hashes, and
`max_effect_settlement_seconds` is copied from that registered capability.
The database function, never the client, derives
`settle_after = greatest(revoked_at,
last_mutation_window_complete_deadline_at) +
make_interval(secs => max_effect_settlement_seconds)`. For `not_entered` the
last deadline is null and SQL sets `settle_after=revoked_at`; its capability
hash/maximum still equal the executor fence. Correlated terminalization first freezes and
terminalizes the request with a null proof; no proof is accepted on a
nonterminal request or its terminal transition. The proof is installed once
afterward by a registration-first then request/effect-only transaction,
following nonlocking key discovery. That path requires the frozen
binding/attempt, generation-one claim token hash, worker ID, executor fence,
effect count/aggregate hash, and the null-to-non-null proof transition while
permitting only `updated_at` to advance. A non-null request proof is immutable.
It does not lock or transition the action.

The result scan then discovers the proof without a retained lock. Its Serve
reducer transaction reacquires the full domain-first order through action and
request, requires kernel `REDUCING` and Serve `RUNNING` or `VERIFYING` for that
attempt, copies the proof byte-for-byte, and may reduce to kernel `READY` with
Serve `VERIFYING` or `RETRY_WAIT`, kernel `BLOCKED` with Serve `VERIFYING`, or
the matching terminal pair. Every subsequent outcome about that attempt
carries it byte-for-byte. Thus request publication never reaches backward into
domain state and a reducer crash is harmless: the same terminal request/proof
is rediscovered. Admission of attempt N+1
clears only `last_result`; terminal request N and its proof remain under
action-history retention, so retry admission or a later crash cannot erase
quiescence. This append-once evidence is not a lease.

Observation after terminalization starts only at PostgreSQL time
`>= settle_after`; every older round is discarded. An execution-fence-only
attempt plus a valid `not_entered`
quiescence proof is durable `mutation_not_started` evidence and may become
retryable without a locator. A `prepared_or_later` attempt first requires that
proof and then a fresh complete `KubernetesObservationSetV1`; absence may permit
a launch retry or complete a down, presence is adopted/retried according to
action type; uncertainty leaves Serve `VERIFYING` with kernel `READY` and a
new primary observation descriptor, or kernel `BLOCKED`. If process exit, Pod UID,
either exact TokenReview revocation or fixed-token expiry, RBAC,
target/control-plane identity, the byte-equal durable capability that covered
every frozen window's full deadline, or its provider-side settlement bound
cannot be proven, kernel remains `REDUCING` and Serve remains `VERIFYING`.
Capability expiry blocks a new
window; it does not discard historical certification.

While the original claim is still live, observation may inform a terminal
transition only when every precommitted mutation window has a non-null
`no_bytes_sent` or `definitive_response` result and every definitive call has
its complete response evidence. The observer locks and revalidates that exact
normalized effect set before its first round and in the final CAS. A missing or
`ambiguous` result, timed-out/lost response, expired claim, or result whose
write is uncertain forbids live-claim provider certainty: joint
terminalization freezes the effects and the exact per-window result vector,
moves the kernel to `REDUCING` while Serve may project `VERIFYING`, then
requires immutable quiescence, the
SQL-derived settlement wait, and wholly fresh rounds. No empty/present sweep
performed before that sequence is reusable.

The HA chart exposes this path only for role-split, one-use executor Pods in the
release namespace. Its exact least-privilege roles grant the reconciler bounded
get/list/UID-precondition-delete on those Pods; TokenReview; direct backend
`/configz`, `/version`, and the read-only freeze-barrier endpoint; MostRecent
EndpointSlice and all six webhook/policy/
binding configuration LIST/watch; exact provider-obligation LIST/GET/mutation;
and, only in guarded mode, `dryRun` DELETE on the one fixed admission-freeze
sentinel `resourceName`. The sentinel permission has no create/update/patch or
other delete authority. The central PostgreSQL role grants only the reviewed
registration/challenge/finalization/consumption/release functions and journal
writes. The static guard, when that branch is selected, uses a distinct
read-only, primary-pinned freeze-query credential whose identity and
configuration are in its immutable dependency inventory.

Startup verifies those RBAC/DB projections, the fixed full Pod-UID-bound
credential, two staged challenges/barrier/cache proof, same-management-cluster identity, complete
request execution chain, admission-freeze branch, and fresh provider-side
settlement capability before advertising v1. The chart cannot install an
ordinary webhook/policy and call it the static guard; it may select guarded
mode only when immutable control-plane evidence already proves the allowlisted
module and central-primary consistency contract.
Graceful shutdown stops new
claims, kills and joins every disposable child or completes remote token
revocation/expiry proof, persists quiescence, and only then stops the
executor's fleet-registration heartbeat. That freshness record grants no work
authority; the request claim remains the sole execution lease.

Requests that predate this design or are unrelated to resource actions keep
the correlation/quiescence fields null and own no normalized resource-action
effects.

#### PostgreSQL immutability guards

Store-only conventions are insufficient for replay identity. Guard ownership
follows migration ownership exactly.

API005 ships one checked-in, literal generic DDL module,
`sky/server/requests/resource_action_schema_v1.py`. It contains independently
named function/trigger constants for:

- ownership activation epochs;
- typed legacy-intent cutover/readback rows;
- runtime adapter registrations and manifest uniqueness;
- generic action identity, typed envelopes, state, and due shape;
- true nested-child bindings;
- normalized effects and aggregate hashes;
- correlated `api_requests`; and
- correlated `api_request_queue` delivery.

The API005 application kernel also ships exact Python store entry points for
generic materialization, the two-stage claim transaction,
correlated terminalization/reaping, effect insert/result fill, and quiescence
publication. These are caller-transaction operations rather than PostgreSQL
stored functions: claim must invoke the process-local adapter's prelock and
consume callbacks in one SQLAlchemy transaction, and no database function may
dynamically select or impersonate an adapter callback. Their transaction,
lock-order, CAS, and result contracts are the ones specified below. The schema
marker proves only the literal PostgreSQL structures, helpers, and guards; the
kernel runtime token and adapter-manifest digest prove the matching Python
store/registry implementation before any process may dispatch or claim.

Neither the API005 DDL nor those generic Python entry points have a Serve
import, Serve table lookup, Serve foreign key, Serve action-kind literal, or
conditional catalog reference to a Serve schema. The generic structural
postverifier loads only API005's expected PostgreSQL definitions and therefore
succeeds against an API database in which Serve has never been installed.

After API005 and Serve032 are both verified, API006 installs a separate literal
Serve adapter DDL module. It owns:

- the 1:1 Serve projection guard and reducer/read-path compatibility rules;
- shadow sample, operation, and divergence guards;
- Serve executor registration, admission-freeze, challenge, consumption, and
  release guards;
- API006's typed Serve evidence extensions; and
- the exact link between API006's one-use executor registration and API005's
  two-stage claim-authority hook.

No API005 function is replaced with a Serve-aware body. API006 adds only
extension triggers/functions whose dependencies are declared and removed with
API006.

Every PostgreSQL function installed by either literal DDL module is
`SECURITY INVOKER`, has
`SET search_path = pg_catalog, <exact_api_schema>`, uses schema-qualified
relations, has no default arguments, and has its executable-bit and owner
verified. Every trigger name, timing, event set, granularity, enabled state,
function OID, normalized `pg_get_functiondef`, and normalized
`pg_get_triggerdef` is structural-marker evidence. Application roles receive
only the reviewed function privileges; direct writes that could manufacture an
edge fail closed.

The generic ownership-epoch guard enforces:

- the exact scope key
  `(domain, operation_subset, store_mode, epoch)`;
- phases only `LEGACY_OPEN|DRAINING|ACTION_OPEN`;
- a partial unique current row per scope;
- epoch zero at first insert and contiguous `N -> N+1` changes;
- phase change by closing the old row and inserting the next epoch in one
  transaction;
- immutable adapter/activation evidence with their all-or-none shape; and
- no owner instance, owner token, heartbeat, expiry, or lease field.

An action references one exact epoch. It can become `READY` or remain
`QUEUED|RUNNING|REDUCING` only while that epoch is the current
`ACTION_OPEN` row and its adapter identity matches. Closing an epoch prevents
new materialization/claim but does not erase a terminal/reduction history.
Legacy-intent guards enforce the typed envelope/hash shapes and only
`ACTIVE -> READBACK -> TERMINAL` (with the exact never-entered
`ACTIVE -> TERMINAL` exception). A matching nonterminal legacy intent blocks
runnable action admission.

The action guard enforces immutable identity, ownership epoch, resource kind,
desired generation, action kind, adapter identity, priority/request time,
cleanup bit's closed transitions, and the registered type/version/JSON/SHA
shape of every envelope. It repeats the exact generic state graph and shape:

- `READY`: fully materialized next-attempt envelope, non-null database due
  time, no live request;
- `QUEUED`: deterministic current correlated request and queue row, no due or
  next-attempt envelope;
- `RUNNING`: generation-one live correlated request and effect zero;
- `REDUCING`: terminal current correlated request, frozen effects, no due or
  next-attempt envelope;
- `BLOCKED`: no provider-execution authority, next-attempt descriptor, or due
  time; only a separate domain evidence transaction may move it; and
- `TERMINAL`: typed final result and completion time, no due/descriptor.

`transition_ordinal` increments exactly once per state edge and
`row_revision` exactly once per row mutation. Unknown state, envelope type,
version, oversized canonical bytes, hash mismatch, skipped attempt, or
noncurrent ownership epoch is rejected.

Correlated request columns are exactly:

```text
resource_action_id                    UUID nullable
resource_action_attempt               BIGINT nullable
resource_action_payload_sha256        TEXT nullable
resource_action_effect_count          BIGINT nullable
resource_action_effects_sha256        TEXT nullable
resource_action_quiescence_type       TEXT nullable
resource_action_quiescence_version    INTEGER nullable
resource_action_execution_quiescence  JSONB nullable
resource_action_quiescence_sha256     TEXT nullable
```

All nine columns are nullable and have no default. They are all null for an
ordinary request. The first five are all non-null for a correlated attempt;
the quiescence quartet is initially all null and is append-once only after
terminal freeze. The effect count/hash pair is authoritative; there is no
redundant action-attempt binding table.

A correlated admission transaction locks current ownership epoch then action,
derives the exact request ID from `(action_id, attempt)`, inserts or retrieves
the byte-equal generation-zero request, inserts queue delivery, clears the
consumed next-attempt envelope, and advances `READY -> QUEUED`. Collision
with any different payload, codec, handler, schedule, workspace, actor,
correlation root, action, or attempt is terminal conflict. The queue guard
allows exactly that correlated insert and later terminal delete; generic retry
or queue re-enqueue of the same correlated request is forbidden.

The two-stage correlated claim transaction uses the exact lock order
`ownership epoch -> adapter claim authority -> action -> request -> queue ->
effects`. The adapter-specific first hook may lock only its declared authority
row. After the kernel has locked and CAS-validated the generic rows, the second
hook may consume that already-locked authority and return bounded opaque
`ClaimEffectMaterializationV1` bytes but may acquire no lock. The guard then
requires atomically:

- request generation `0 -> 1`, a fresh PR #1070 claim token/worker, database
  heartbeat and live request lease;
- generic `QUEUED -> RUNNING`;
- insertion of exact normalized effect zero with
  `execution_generation=1`, claim-token SHA, worker instance, facet identity,
  typed intent/result/evidence shapes, and adapter implementation digest;
- request `resource_action_effect_count=1` and the recomputed ordered
  aggregate SHA; and
- for Serve under API006, one-use executor-registration consumption and the
  byte-equal `ProviderExecutorFenceV1` evidence.

A rollback or failed CAS consumes nothing. A request heartbeat subsequently
checks only PR #1070's request predicate; it does not mutate action, ownership,
adapter registration, or effect rows.

Effects have contiguous indexes beginning at zero, exact optional
parent-before-child order, one request/action/attempt, a registered facet
name/version/implementation digest, typed intent and optional readback locator,
closed certainty, pre-I/O idempotency key, optional provider request/operation
IDs, generation one and exact claim/worker fence, plus all-or-none typed
result/evidence. Every insert and permitted null-to-result/evidence fill locks
the action, request, and existing effect prefix, recomputes the request count
and ordered aggregate SHA, and commits them together. It enforces the exact
2,048-effect, 2,046-mutation-window, 16,368-call-slot, and
16,777,216-canonical-byte limits. No effect row, count, hash, result, or
certainty may change after terminal freeze except the separately specified
append-once quiescence proof on the request.

A true nested child is keyed by
`(action_id, attempt, effect_index, child_slot)`. Its parent attempt request
must be the correlated request; its distinct deterministic child request has
all `resource_action_*` columns null. Binding, child request, and child queue
row commit together, and reuse requires byte-equal payload/workspace/actor
digests. Serve v1 creates no nested mutator.

Ordinary `_terminalize_locked_request()` rejects correlated rows. The special
correlated terminalizer uses
`ownership epoch -> action -> request -> queue -> effects`, freezes exact
effect count/aggregate and terminal request bytes, deletes delivery, and
advances generic `QUEUED|RUNNING -> REDUCING` atomically. It cannot touch a
Serve/domain result. The reaper uses the same special path. Quiescence
publication is a later request/effect-only append-once transition requiring the
frozen attempt, effect count/hash, generation-one claim, worker and executor
fence; it never moves the action. Request cancellation acknowledgement retains
the same immutable correlation/effect/quiescence fields.

API006's Serve guard never fires an action-to-projection write. Generic
materialization, claim, terminalization, reaping, heartbeat, and effect writes
therefore remain API005-only and action-first. The closed compatibility matrix
allows a nonterminal Serve projection to lag generic execution state:

- generic `READY`: Serve `PLANNED`, `RETRY_WAIT`, or `VERIFYING`;
- generic `QUEUED`: Serve `PLANNED`, `RETRY_WAIT`, `VERIFYING`, or `QUEUED`;
- generic `RUNNING`: Serve `PLANNED`, `RETRY_WAIT`, `VERIFYING`, `QUEUED`, or
  `RUNNING`;
- generic `REDUCING`: any nonterminal Serve state;
- generic `BLOCKED`: Serve `VERIFYING`; and
- generic `TERMINAL`: the matching Serve terminal state only.

Every read and reducer treats generic `REDUCING` as pending reduction and must
ignore any apparently running/queued projection as a resource result. A
nonlocking API006 catch-up scan may find `QUEUED`/`RUNNING` lag, then lock the
full Serve/domain order and finally the action, revalidate the generic state,
and advance only the display/execution projection. It never changes generic
state. The actual reducer similarly discovers `REDUCING` IDs without locks,
then locks full domain order and finally the action and atomically maps
`REDUCING -> READY|BLOCKED|TERMINAL` with the Serve outcome. No action-first
path writes or locks a Serve row.

The checked-in truth-table suite issues direct SQL as both application and
maintenance roles. It covers every permitted edge and one-at-a-time mutation
of every other column; candidate/CAS races; hook rollback after authority
consumption; claim lock order; ordinary-versus-correlated terminalization;
effect 2,048/2,049 and aggregate-byte boundaries; nested-child separation;
every allowed/forbidden base-projection lag pair and read-path suppression of a
stale projection while generic is `REDUCING`; unknown future action kinds; duplicate adapter
registration; and API005 operation against a database with no Serve catalog.
The structural postverifiers compare the literal definitions and constraints,
not only marker rows.

#### Cross-schema upgrade and exceptional downgrade protocol

The common session-level advisory-lock key is exactly
`skypilot:alembic:resource-actions:v1`. The supported composite upgrade is
strictly:

```text
API request 004
  -> API005 generic kernel (independently verified)
  -> Serve032 identity/mode/capability fields (requires API005 marker)
  -> API006 Serve adapter/projection/shadow/executor artifacts
     (requires both verified heads/markers)
```

The runner uses one PostgreSQL connection, acquires the exclusive advisory
lock before either Alembic environment, and performs three separately
transactional phases:

1. upgrade API requests through 005. It creates only generic ownership epochs,
   typed legacy intents, runtime adapter registrations, actions, nested-child
   bindings, normalized effects, request correlation/quiescence columns,
   generic queue integration, guards/indexes, runtime-capability column, and
   marker. Its postverifier runs successfully without a Serve schema;
2. upgrade Serve through 032. It verifies API005, creates only Serve database
   identity/mode/capability and downgrade-maintenance fields, writes its marker
   last, and runs the Serve032 postverifier; and
3. upgrade API requests through 006. It verifies both earlier markers/heads,
   creates the 1:1 Serve projection, shadow tables, typed Serve evidence
   extensions, executor-registration/freeze ledgers, instance registration-ID
   foreign key, API006 guards, and marker, then runs the API006 postverifier.

No later phase is attempted after a failed verifier. A database at the durable
intermediate API005/Serve031 state may run generic-kernel processes but cannot
advertise the Serve adapter. API006 cannot be installed before Serve032, and
Serve032 cannot be installed before API005. Capability publication begins only
after the applicable phase's complete structural postverification. The
exclusive lock is released only after the final verifier or a classified
failure.

Application rollback never runs down migrations. Exceptional downgrade exists
only in the reviewed maintenance command.

The command requires explicit confirmation
`API006_TO005_THEN_SERVE032_TO031_THEN_API005_TO004`, pins every API,
controller, executor, and static admission-guard owner against restart/scale-up,
proves no mutator remains, and holds the same exclusive advisory lock. Direct
Alembic downgrade, Helm hook, startup code, and HTTP APIs cannot invoke these
revisions.

Its crash-resume classifier accepts exactly seven states and the six listed
edges; no state may be skipped or combined:

| State | Exact verified structure | Only legal next transaction |
|---|---|---|
| `INITIAL` | API006 + Serve032 + API005 heads, markers, and structures exact; maintenance fence absent | insert the fence in phase `DRAINING` |
| `GUARD_DRAINING` | the same three heads/structures; one exact `DRAINING` fence row | after the external guard-free rollout and its complete evidence, update only that row to `DISABLED` |
| `GUARD_DISABLED` | the same three heads/structures; one exact `DISABLED` fence row | downgrade API006 to API005 while retaining the fence |
| `AFTER_API006_DOWN` | API005 + Serve032 exact; every API006 artifact absent; the same `DISABLED` fence row present | downgrade Serve032 to Serve031 while retaining the fence |
| `AFTER_SERVE_DOWN` | API005 + Serve031 exact; every non-fence Serve032 and API006 artifact absent; the same `DISABLED` fence row present | downgrade API005 to API004 without inspecting or changing the fence |
| `AFTER_API005_DOWN` | API004 + Serve031 exact; every API005, API006, and non-fence Serve032 artifact absent; the same `DISABLED` fence row present | run the final verifier, then guarded-delete the row and drop the maintenance fence table/function/trigger |
| `FINAL` | API004 + Serve031 exact; every resource-action and maintenance-fence artifact absent | no DDL |

Every other head/fence pair or partial structure is rejected. A fault-injection
and crash-resume test is mandatory immediately before and immediately after
each of the six edges. External deployment or static-guard rollout is permitted
only on `GUARD_DRAINING -> GUARD_DISABLED`; all other edges are database-only
maintenance transactions.

Before the first edge the command requires every Serve service in legacy mode,
no action/projection/shadow/correlated-request/effect/child history, every
executor registration and freeze released, retained executor Pods strongly
absent, and no fresh Serve adapter advertisement. Every rerun revalidates that
drain proof and the pinned guard-free backend evidence.

API006 down removes only API006-owned guards, projection/shadow/evidence
extensions, executor ledgers, and the instance registration-ID foreign
key/column, then verifies their absence while preserving every API005 generic
artifact, the exact API005 capability row, and the maintenance fence. Serve032 down
removes only non-fence Serve032-owned identity/mode/capability fields and its
marker, verifies Serve031 and absence, and preserves the maintenance fence.
API005 down requires its generic tables/history empty, no current
`ACTION_OPEN` epoch, no runtime adapter publication, and no correlated
request/queue/effect/child row. It removes API005 guards in reverse dependency
order, request correlation and quiescence columns, runtime capabilities,
generic tables and marker, then verifies exact API004 absence; it never
inspects, changes, or removes the maintenance fence. Only the final
maintenance verifier in `AFTER_API005_DOWN` may delete and drop the fence.

Every resource-action writer takes the shared transaction form of the common
advisory lock before its row locks. A race therefore either commits wholly
before the exclusive migration and makes a downgrade gate refuse, or resumes
after migration and fails its marker/capability check before writing. The
durable downgrade fence remains fail-closed across process/command crashes
until `FINAL` is verified; only then may rollout pins be released.

### Why the shared action kernel has no independent lease

The generic action row owns due state and attempt materialization, not
long-lived execution. The request row already persists execution generation,
claim token, worker instance, lease expiry, and heartbeat. Copying those fields
into the action would create two clocks and two ownership authorities. The
shared dispatcher takes a row lock only for its short
`FOR UPDATE SKIP LOCKED` transaction and leaves no claim token, lease expiry,
or heartbeat on the action. The authoritative action view joins its
`current_request_id` to the request claim.

The action update is fenced by:

- expected `kernel_state` and row revision;
- expected `current_attempt` and `current_request_id`;
- the request backend's current claim predicates when the worker publishes an
  outcome; and
- the service hash, replica incarnation, logical target, and planning
  lifecycle fence when the controller projects an outcome into replica state.

This is an intentional refinement of both the research note's suggestion to
persist a second lease on the action row and the draft generic-kernel lease
wording in
`docs/designs/provider-lifecycle-actuation.md@7aaa99041065a57c6f733ceed04f025520bac871`.
The shared ownership payoff is one due query and one materializer across
domains, while PR #1070 remains the single execution-lease implementation.

`current_attempt` starts at zero with no request. The first materialized
request is attempt one; every later request is exactly the prior attempt plus
one. An attempt number is never reused. `current_request_id` continues to point
to the latest immutable request after it becomes terminal so the shared kernel
and domain reducer can inspect its result and provider operation ID.

### Relationship to `api_controller_action_reservations`

`api_controller_action_reservations` remains a per-request fence for
non-replayable controller-class handlers. Its current logical action ID is the
request ID and its owner is a controller generation. Those semantics are not
changed or overloaded.

`api_resource_actions` is the shared cross-owner mechanics journal, and
`api_serve_replica_action_projections` is its Serve domain projection.
Correlated Serve handlers are registered outside controller reservation logic
and never create or reference an `api_controller_action_reservations` row. The
reservation table remains only for its existing unrelated handlers.

## State Machine

The shared row column is `kernel_state`, with exactly:

```text
READY       -> QUEUED | BLOCKED | TERMINAL
QUEUED      -> RUNNING | REDUCING
RUNNING     -> REDUCING
REDUCING    -> READY | BLOCKED | TERMINAL
BLOCKED     -> READY | TERMINAL
TERMINAL    -> (none)
```

These are generic mechanics states, not resource outcomes. Their shapes are
closed:

- kernel `READY`: complete next-attempt quartet and database
  `next_attempt_at`, no live request;
- kernel `QUEUED`: deterministic generation-zero primary correlated request
  and queue row, with no descriptor or due time;
- kernel `RUNNING`: that request at generation one under PR #1070's sole
  request lease and at least normalized effect zero;
- kernel `REDUCING`: terminal frozen request/effects, no queue, descriptor,
  or due time;
- kernel `BLOCKED`: no automatic safe progress, no descriptor or due time,
  and a typed mechanics reason in `last_result`; and
- kernel `TERMINAL`: typed final mechanics result and completion time, with no
  descriptor, due time, queue, or live request.

Every automatic observation or readback is a new primary attempt: the reducer
moves kernel `REDUCING -> READY` with an immutable observation descriptor.
That attempt uses the same deterministic request, queue, request claim/lease,
normalized effects, terminalizer, and reducer as a mutation attempt. Kernel
`BLOCKED` never drives a timer or readback. It can leave only when a separate
domain transaction supplies new evidence and creates a complete primary
descriptor with `BLOCKED -> READY`, or establishes a final
`BLOCKED -> TERMINAL` result. There is no `BLOCKED -> REDUCING` edge and no
readback child in Serve v1.

`PLANNED|QUEUED|RUNNING|VERIFYING|RETRY_WAIT|SUCCEEDED|TERMINAL_FAILED|
SUPERSEDED` are exclusively `serve_state` values in the API006 Serve
projection. `serve_last_outcome` is the sole typed resource outcome;
generic `last_result` remains mechanics-only. The Serve projection graph is:

```text
Serve PLANNED       -> Serve QUEUED | Serve SUPERSEDED
Serve QUEUED        -> Serve RUNNING | Serve VERIFYING | Serve RETRY_WAIT |
                       Serve TERMINAL_FAILED | Serve SUPERSEDED
Serve RUNNING       -> Serve VERIFYING | Serve RETRY_WAIT |
                       Serve SUCCEEDED | Serve TERMINAL_FAILED
Serve VERIFYING     -> Serve RETRY_WAIT | Serve SUCCEEDED |
                       Serve TERMINAL_FAILED | Serve SUPERSEDED
Serve RETRY_WAIT    -> Serve QUEUED | Serve VERIFYING | Serve SUPERSEDED
Serve SUCCEEDED     -> (none)
Serve TERMINAL_FAILED -> (none)
Serve SUPERSEDED    -> (none)
```

API005 action-first paths never write or lock the API006 projection. The closed
lag matrix is:

- kernel `READY`: Serve `PLANNED|RETRY_WAIT|VERIFYING`;
- kernel `QUEUED`: Serve `PLANNED|RETRY_WAIT|VERIFYING|QUEUED`;
- kernel `RUNNING`: Serve
  `PLANNED|RETRY_WAIT|VERIFYING|QUEUED|RUNNING`;
- kernel `REDUCING`: any nonterminal Serve projection state;
- kernel `BLOCKED`: Serve `VERIFYING`; and
- kernel `TERMINAL`: the matching Serve
  `SUCCEEDED|TERMINAL_FAILED|SUPERSEDED`.

Thus kernel `QUEUED` plus Serve `PLANNED`, and kernel `REDUCING` plus any
prior nonterminal Serve state, are deliberate crash-closed pairs. Every reader
joins the generic row; while kernel is `REDUCING`, no stale Serve
`QUEUED|RUNNING|VERIFYING` value is a result. A nonlocking projection catch-up
scan may advance display-only Serve `QUEUED|RUNNING` under the full domain
lock order and then the generic action, but execution never waits for it.

The reducer discovers kernel `REDUCING` IDs without locks, then takes the
registration-first full Serve/domain order and finally action/request/effects.
It atomically chooses exactly one pair:

- kernel `READY` plus a complete next primary descriptor; Serve becomes or
  remains `RETRY_WAIT` for a mutation retry, or remains Serve `VERIFYING`
  for an observation/readback attempt;
- kernel `BLOCKED` plus Serve `VERIFYING`, with no due time/descriptor; or
- kernel `TERMINAL` plus Serve
  `SUCCEEDED|TERMINAL_FAILED|SUPERSEDED`.

Native admission inserts kernel `READY` and Serve `PLANNED` together.
Legacy import inserts kernel `READY`, Serve `VERIFYING`, and
`current_attempt=0` with a complete primary attempt-one observation
descriptor; its first automatic observation is the positive correlated attempt
one. It does not invent a generic `VERIFYING` state. Serve `SUPERSEDED` is legal only with
kernel `TERMINAL` and proof that mutation could not start or residue is
absent. Serve `SUCCEEDED` down requires authoritative provider absence.
Serve `TERMINAL_FAILED` launch requires impossible residue or proven absence;
down never uses Serve `TERMINAL_FAILED`.

Identity, action kind/spec, desired generation, and ownership epoch are
immutable. All due times use PostgreSQL `clock_timestamp()`. Every generic
state edge increments `transition_ordinal`; every generic or projection row
mutation increments its own `row_revision`.

### Launch

1. Existing Serve policy chooses immutable replica identity and placement.
2. One domain transaction commits replica/capacity intent, Serve projection
   `serve_state=PLANNED`, and generic `kernel_state=READY` with the complete
   immutable primary mutation descriptor and PostgreSQL due time. Repeating it
   returns the same action.
3. Candidate discovery considers only local action kinds and nonlockingly
   verifies the dispatching instance's fresh, ready, non-draining row plus its
   byte-matching action-bound adapter registration/manifest with `dispatch`.
   It separately requires some fresh compatible registration with
   `execute+claim_authority`; without either, the action stays `READY`. The
   materializer then locks only
   `current ownership epoch -> generic action`, CAS-revalidates
   `kernel_state=READY`, descriptor/due/revision, creates or retrieves the
   deterministic primary request and queue, clears descriptor/due, and sets
   kernel `QUEUED`. It neither queries nor writes Serve state and holds no
   lease after commit.
4. Claim locks
   `ownership epoch -> selected adapter claim authority -> action -> request ->
   queue -> effects`. The two-stage no-I/O hook consumes the prelocked
   one-use authority and returns opaque effect-zero bytes. The kernel installs
   generation one, PR #1070's sole request claim/lease, exact effect zero, and
   kernel `RUNNING` atomically. Heartbeat is request-only.
5. Immediately before provider bytes, the handler follows the universal
   registration-first order and revalidates the claim, service hash/mode,
   workspace/scope, replica incarnation, desired generation, logical target,
   cluster, spec and domain fences. It commits the typed locator and complete
   mutation-window effect before releasing locks. Provider I/O occurs only
   afterward.
6. Operation/result evidence fills only the normalized effect's generic
   provider IDs, typed result quartet, and typed evidence quartet under the
   active claim; it never writes a combined mutable domain projection.
7. Every completion routes through the correlated terminalizer, which freezes
   the request/effects and sets only kernel `REDUCING`. A definitive mutation
   acknowledgement is not Serve success. The reducer normally sets Serve
   `VERIFYING` and kernel `READY` with a new primary observation descriptor.
   If external execution is ambiguous, the action stays kernel `REDUCING`
   until append-once quiescence permits that reduction; lack of constructible
   safe proof maps to kernel `BLOCKED` plus Serve `VERIFYING`.
8. A primary observation attempt proves exactly one live launch location and
   absence at every other retained locator before the reducer may set kernel
   `TERMINAL`, Serve `SUCCEEDED`, and `serve_last_outcome`. Confirmed
   absence plus a retryable mutation classification sets kernel `READY` with
   the next mutation descriptor and Serve `RETRY_WAIT`; uncertainty creates
   another primary observation attempt only when its reducer has a
   deterministic safe descriptor, otherwise kernel `BLOCKED`.
9. Exact pre-entry not-started evidence may reduce a retryable classification
   directly to kernel `READY` plus Serve `RETRY_WAIT`, or a permanent
   classification to kernel `TERMINAL` plus Serve `TERMINAL_FAILED`.
   After PREPARED, provider certainty must first be established by a primary
   observation attempt.

### Down

1. Existing Serve routing, target-generation, idle and replacement-safety
   policy chooses a victim.
2. One transaction commits route removal/drain fields, immutable retirement
   intent, Serve `PLANNED`, and generic `READY` immediately. The primary
   down descriptor is complete at admission; `next_attempt_at` is the durable
   drain-eligibility time. No domain scheduler later changes PLANNED into
   runnable state, and no executor sleeps for drain.
3. The READY-only generic dispatcher materializes the attempt when that due time
   arrives. Immediately before provider bytes, the handler takes the universal
   registration-first order and obtains a fresh load-balancer idle proof or
   revalidates the durable drain deadline plus every service/incarnation/
   logical-target fence. Failure is a terminalized not-started attempt whose
   reducer may create another kernel READY primary descriptor; it never grants
   provider authority.
4. A down mutation commits locator/window intent before I/O and terminalizes to
   kernel `REDUCING`. A delete response is not resource absence. Its reducer
   creates kernel `READY` with a primary observation descriptor and Serve
   `VERIFYING`, subject to required quiescence for ambiguity.
5. Only a primary observation attempt whose complete set proves every locator
   absent may reduce to kernel `TERMINAL`, Serve `SUCCEEDED`, close the
   replica/usage interval and write `serve_last_outcome`. Presence reduces to
   kernel `READY` plus Serve `RETRY_WAIT` with a new down descriptor.
   Uncertainty produces a safe new primary observation descriptor or kernel
   `BLOCKED` plus Serve `VERIFYING`; kernel BLOCKED has no due time.
6. Cleanup has no give-up deadline.

For an eligible consolidated-pool replica, the pool policy commits the same
Serve `PLANNED` plus kernel `READY` pair with an immediately due descriptor;
routing, load-balancer and drain checks are omitted. A non-consolidated pool
remains on its construction-fenced legacy adapter.

### Ambiguous owner loss

Lease expiry never replays a mutator. The special API005 terminalizer takes
exactly
`ownership epoch -> generic action -> request -> queue/effects -> request
event`, freezes the generation-one request/effects, deletes delivery and sets
only kernel `REDUCING`. It takes no executor-registration or Serve/domain
lock and never requeues that request.

Database revocation is not external-I/O quiescence. After nonlocking key
discovery, the quiescence writer locks
`executor registration -> request/effects`, publishes the append-once
quiescence quartet, and does not move the action. Kernel state remains
`REDUCING` until that proof and its SQL-derived settlement time permit the
domain reducer. An effect set containing only executor-fence effect zero plus
`not_entered` quiescence is durable never-started evidence. Any PREPARED or
later locator-bearing effect remains ambiguous until quiescence.

The reducer then chooses exactly:

- kernel `READY` with a new primary observation descriptor and Serve
  `VERIFYING`;
- kernel `READY` with a new mutation descriptor and Serve `RETRY_WAIT` when
  exact never-started evidence already permits retry;
- kernel `BLOCKED` with Serve `VERIFYING` when safe progress is not
  constructible; or
- kernel `TERMINAL` with the exact Serve terminal outcome when evidence is
  already conclusive.

Provider observation never occurs inside that reducer transaction. A later
primary observation attempt obtains the full locator set outside SQL locks and
its terminal reduction adopts/retries/completes according to launch/down
policy. A stale worker cannot publish after claim expiry and cannot later
mutate until its disposable child is joined or Pod-bound token revocation/
expiry is proven. A paused or partitioned worker, signal acknowledgement,
client timeout, or pre-quiescence empty sweep is not proof.

### Durable request terminalization and action reduction

Ordinary requests retain the existing request-first
`_terminalize_locked_request()`; that function rejects correlated rows.
Because `PostgresRequestBackend.update_request()` locks `api_requests`
before yielding, every write entry point first performs a nonlocking
correlation lookup. A correlated terminal success, failure, cancellation,
timeout or reaper expiry exits the ordinary context and routes to the one
special API005 store. Correlated nonterminal progress uses a separate
request-only claim-fenced CAS that cannot terminalize or move the action.

For each correlated terminal ID, the special store takes exactly:

```text
current ownership epoch
-> generic action
-> request
-> queue/effects
-> request-terminal operational event
```

It revalidates `kernel_state=QUEUED|RUNNING`, current attempt/request and the
applicable generation-zero or claim predicate, then atomically:

1. writes the terminal request result while retaining any generation-one claim;
2. validates and freezes exact effect count/ordered aggregate, with terminal
   uncertainty represented in typed mechanics result/evidence;
3. deletes queue delivery through the terminal-delete guard;
4. sets only `kernel_state=REDUCING`, clearing no domain evidence; and
5. writes the request event last.

It locks no executor registration, service, replica, projection, reservation or
other domain row and invokes no adapter. All direct correlated terminal APIs
and the reaper use this store. The request row's correlation columns are the
attempt binding; no separate attempt table exists.

Kernel `REDUCING` plus the terminal request/effects is the sole reducer
signal. A nonlocking scan discovers IDs. If ambiguity requires quiescence, the
action remains `REDUCING`; the registration-first request/effect-only
quiescence writer appends its typed quartet and moves no action. Once reduction
is safe, the API006 Serve reducer performs nonlocking key discovery, then locks
`current ownership epoch -> executor registration when present -> Serve/domain
rows -> generic action -> request/effects -> api_server_instances when needed
-> domain event`. It revalidates the current attempt, frozen hashes, owner,
incarnation, desired generation, reservation and domain fences and atomically:

- sets kernel `READY` with a fully materialized next primary mutation or
  observation descriptor and database due time;
- or sets kernel `BLOCKED` with no descriptor/due and Serve `VERIFYING`;
- or sets kernel `TERMINAL` and the exact Serve
  `SUCCEEDED|TERMINAL_FAILED|SUPERSEDED` projection;
- writes only a typed mechanics disposition to generic `last_result`;
- writes the typed resource outcome only to `serve_last_outcome`; and
- releases/transfers reservation and writes the domain event last when allowed.

A crash before terminalization leaves a live request governed by the request
lease/reaper. A crash after it leaves kernel `REDUCING` plus the prior
nonterminal Serve projection; the scan retries reduction idempotently. A
terminal correlated request can never coexist with kernel `RUNNING`.
Request `SUCCEEDED` means only invocation completion. Resource success comes
only from a provider-observation primary attempt and its Serve reduction.
Attempt N+1 cannot materialize until the reducer has committed kernel `READY`
with a complete descriptor.

### Action-aware cancellation, shutdown, and retention

Every correlated terminal path—normal completion, explicit/internal cancel,
disconnect, shutdown, submit failure, broken worker, handler error and
reaper—first performs the nonlocking correlation read and then uses the special
epoch/action/request terminalizer above. It is not request-only: it always
locks the generic action and commits kernel `REDUCING`. It never locks Serve
or executor-registration rows. Correlated nonterminal progress and heartbeat
remain request-only claim-fenced CAS operations.

Every correlated request has `should_retry=false` and `retryable=false`.
No generic request code requeues it or creates generation two. Only a later
kernel `READY` descriptor can materialize attempt N+1.

The closed non-handler matrix is:

| Initiator / proof | Atomic special-terminalizer result | Later kernel / Serve projection result |
|---|---|---|
| cancel or graceful shutdown, kernel `QUEUED`, generation zero and zero effects | request `CANCELLED`, queue removed, kernel `REDUCING` | kernel `READY` + Serve `RETRY_WAIT` with new primary descriptor, or kernel `TERMINAL` + exact Serve terminal outcome |
| cancel/shutdown/broken worker after claim | terminal request retains generation-one claim and frozen effects; kernel `REDUCING` | after quiescence, kernel `READY` + Serve `VERIFYING` with primary observation descriptor, or kernel `BLOCKED` + Serve `VERIFYING` |
| pre-call fence or submit failure before claim | request `FAILED`, zero effects, kernel `REDUCING` | kernel `READY` + Serve `RETRY_WAIT`, or kernel `TERMINAL` + Serve `TERMINAL_FAILED` for an exact permanent launch classification |
| pre-call fence or retryable handler failure after claim but before provider entry | request `FAILED`, frozen effect-zero-only set, kernel `REDUCING` | only after exact not-entered quiescence: kernel `READY` + Serve `RETRY_WAIT`; otherwise primary observation or BLOCKED as above |
| completion/error after PREPARED or any entered/ambiguous call | terminal frozen request/effects, kernel `REDUCING` | primary observation descriptor with Serve `VERIFYING`, or kernel `BLOCKED`; never direct resource success |
| expired request lease | request `FAILED`, retained claim/executor/effects, kernel `REDUCING` | quiescence first, then primary observation descriptor or kernel `BLOCKED`, both with Serve `VERIFYING` |

Direct supersession of an unclaimed generation-zero attempt is not routed
through the generic terminalizer. It is one separate domain-first transaction:
after nonlocking discovery it locks the declared service/replica/projection
order, then action, request, queue/effects and events; revalidates that provider
entry was impossible; writes request `CANCELLED`; and commits kernel
`TERMINAL`, Serve `SUPERSEDED`, and `serve_last_outcome` atomically.
Attempt-zero supersession uses the same domain transaction without a request.
Zero-locator retirement is the separately specified domain-first variant. A
claimed or possibly started attempt cannot use either shortcut.

Request and domain events remain in their respective atomic transactions:
request event last in the special terminalizer; domain event last in reduction
or supersession. Retention performs no terminalization. It cannot delete a
current request of a nonterminal kernel action or clear correlation. After
kernel `TERMINAL` and retention closure, deletion follows the universal
domain-first order and removes events, queue remnant, effects, correlated
requests, Serve projection and action in foreign-key-safe order. Quiescence is
retained with the whole action history.

## Idempotent Attempt Materialization and Pre-Call Admission

The authoritative due transaction carries no Serve authority object. Its
request bytes come only from the stored next-attempt quartet and byte-equal
registered descriptor. The transient pre-call and shadow-admission objects are
recursively exact:

```text
AuthoritativePreCallContextV1 = {
  version: 1,
  authority_kind: "live_service" | "terminal_tombstone",
  action_id: canonical lowercase UUID text,
  request_id: canonical lowercase UUID text,
  action_type: "launch" | "down" | "observe",
  attempt: PositiveI64,
  expected_action_revision: NonnegativeI64,
  spec_hash: Sha256,
  domain_fence_hash: Sha256,
  reservation_identity_hash: null | Sha256,
  claim_token_sha256: Sha256,
  worker_instance_id: CatalogUuid,
  service_name: Text[256],
  service_hash: Text[256],
  service_lifecycle_epoch: NonnegativeI64,
  owner_token: null | canonical lowercase UUID text
}

ShadowOrdinaryAdmissionContextV1 = {
  version: 1,
  sample_id: canonical lowercase UUID text,
  service_name: Text[256],
  service_hash: Text[256],
  service_lifecycle_epoch: NonnegativeI64,
  owner_token: canonical lowercase UUID text,
  replica_id: NonnegativeI64,
  replica_incarnation_id: canonical lowercase UUID text,
  legacy_attempt: PositiveI64,
  expected_sample_revision: NonnegativeI64,
  spec_hash: Sha256,
  request_input_hash: Sha256
}

ShadowDirectMutationContextV1 = {
  version: 1,
  authority_kind: "live_service" | "terminal_tombstone",
  sample_id: canonical lowercase UUID text,
  action_type: "down",
  proposed_operation_index: PositiveI64,
  expected_sample_revision: NonnegativeI64,
  spec_hash: Sha256,
  operation_input_hash: Sha256,
  service_name: Text[256],
  service_hash: Text[256],
  service_lifecycle_epoch: NonnegativeI64,
  replica_id: NonnegativeI64,
  replica_incarnation_id: canonical lowercase UUID text,
  owner_token: null | canonical lowercase UUID text
}
```

For authoritative `live_service`, the pre-call validator requires a non-null
owner token. After nonlocking key discovery it locks ownership epoch, executor
registration, service/owner, version, replica/capacity/reservation, Serve
projection, generic action, live claimed request/effects, and instance last if
needed. Every identity/fence/hash above must match.
`terminal_tombstone` is accepted only for down and requires null owner token:
the service for that hash is absent; after registration-first discovery the
retained replica, projection, action, request/effects are locked in that order;
action ID equals both
terminal intent IDs; and the byte-equal terminal snapshot/locator/cluster-hash/
routed-false fence passes. A same-name different-hash successor is irrelevant.
The current claim, single-use executor registration, action revision, and
domain-fence CAS authorize this internal pre-call branch; PID/IP, service name,
and an unguessable ID do not.

The shadow direct-down pre-call hook uses
`ShadowDirectMutationContextV1`: its live branch requires the locked current
service hash/epoch/mode/token, while its null-token tombstone branch locks
retained replica then sample and requires sample ID, exact terminal
intent/snapshot, locator array, cluster hash, routed-false state, expected row
revision, next operation index, and operation hash. The child insert and this
authority check commit before the direct call.

There is no authoritative admission HTTP route or controller-supplied
attempt context. The shared dispatcher materializes attempts in-process from
the generic row and registered descriptor. Shadow still calls the ordinary
FastAPI `POST /launch` route with normal service-account/user `Authorization`
and carries the admin-ring token only in
`X-Skypilot-Serve-Resource-Action-Admin`, plus the base64url-unpadded
PostgreSQL-canonical JSON bytes of its context in
`X-Skypilot-Serve-Shadow-Admission-V1`.
`ServeResourceActionAdminMiddleware` runs before body construction, compares
every live overlap token in constant time, rejects either header on every other
route/context, parses exact v1, stores only
`request.state.serve_shadow_admission_v1`, and removes both headers from the
ASGI scope before access logging/auth body construction. `/launch` strips the
transient context before `RequestBody` construction and hash/persistence.
Headers, admin token, owner token, and transient objects are excluded from
request payloads, input/terminal hashes, events, logs, and specs. A public
client cannot self-assign action correlation; only the shared materializer can
insert the correlated request columns that constitute the attempt binding.

The shadow `/launch` route uses the one-connection transaction and exact lock
order in the shadow sequence. It validates every field in
`ShadowOrdinaryAdmissionContextV1` against the current service, referenced
version, replica/capacity claim, and sample before inserting or retrieving and
binding the ordinary request. A repeat after a lost response reauthenticates
and revalidates authority before returning the stored canonical request ID.

The shared materializer first performs a nonlocking indexed candidate query
over the partial READY due index, restricted to locally registered kinds. It
returns only ownership scope/epoch/action IDs. Before opening a materialization
transaction, it nonlockingly verifies a fresh, ready, non-draining instance,
the dispatching instance's byte-equal action-bound adapter
registration/manifest with `dispatch`, and, independently, at least one fresh
compatible `execute+claim_authority` registration. The dispatcher itself need
not advertise `execute`; if either registration condition is absent, it leaves
the action `READY`. For each eligible candidate it then performs one
mechanics-only transaction:

1. lock and verify the exact current `ACTION_OPEN` ownership epoch;
2. lock the generic action with `FOR UPDATE SKIP LOCKED` and CAS-require
   `kernel_state=READY`, exact epoch/revision/due time, and a complete
   next-attempt quartet;
3. derive the next attempt, request ID, exact payload, handler,
   workspace/actor projection, and hashes only from that envelope and the
   byte-equal registered descriptor;
4. insert or retrieve the exact correlated request whose columns themselves
   are the attempt binding;
5. insert durable queue delivery if this is the winning insert;
6. set `current_attempt`, `current_request_id`, clear the consumed next-attempt
   quartet/due time, and set `kernel_state=QUEUED`; and
7. commit and release every lock without writing the Serve projection.

It never locks a runtime registration after the action. It does not lock the
Serve projection/domain rows or decide whether the
persisted intent remains desirable. Domain admission already committed the
immutable fences. The full pre-call validator detects any later invalidation
and fails the attempt before provider bytes; the reducer then supersedes,
replans, or defers it through the domain state machine.

The correlated request body is not a second provider-input container. It is
the following exact closed envelope; the selected handler reconstructs its
mutation or observation only from the locked action and content-addressed
version data:

```text
{
  schema_version: 1,
  action_id: canonical lowercase UUID text,
  attempt: PositiveI64,
  action_type: "launch" | "down",
  attempt_kind: "mutation" | "observation",
  handler: "serve.resource_action.launch.v1" |
           "serve.resource_action.down.v1" |
           "serve.resource_action.observe.v1",
  spec_hash: Sha256
}
```

The registered immutable materialization descriptor uses a dedicated
server-owned
`serve.resource_action.envelope.v1` registry codec that emits exactly those
seven JSON keys. The observe handler is legal only with
`attempt_kind=observation`; launch/down handlers require `mutation`. It does
not instantiate or dump the generic `RequestBody`, so it
cannot inherit client environment, config, file mounts, or other request-body
fields. The registry binds this payload type only to the three dedicated
handler names above and the existing `normal` execution class. Correlated dispatch also
bypasses generic claim-time `event_context` target-ID enrichment. Its bounded
event context is the fully populated, frozen existing six-key `EventContext`
object shown in the request matrix above. The operational-event request-kind
map binds the two mutation request names to the existing cluster launch/down
event kinds; observation has no mutation event and is represented by the
reducer's domain event. Action/attempt/incarnation data is
not smuggled into `EventContext`; provider operation identity is recorded only
in normalized effects and emitted as separate operational events.

The action type, attempt kind and handler must be the matching tuple. The request payload
hash is the database-canonical SHA-256 of this envelope under the same closed
domain and normalization query as the action spec. There is no raw task YAML,
provider dictionary, credential, or independently variable mutation body in a
correlated request. An unknown positive envelope version is a hard error.

If a dispatcher crashes after commit or loses its internal acknowledgement,
the next due scan observes kernel `QUEUED` plus the deterministic binding and performs
no insert. A same `(action_id, attempt)` request with byte-different binding,
payload, fence, workspace, or actor is a hard conflict. There is no exchange
request ID, controller retry, or middleware response override in the
authoritative path.

A service lifecycle-epoch or owner-token change after materialization is
detected by the full domain-first pre-call fence. Before bytes it terminalizes
the attempt as not started for later reduction. Once a mutation window is
committed and bytes may start, owner handoff does not synthetically revoke
external execution; the exact claim/quiescence/observation rules apply.

The action dispatcher capability-checks API-request schema 005, the generic
kernel token, its own exact adapter registration/manifest with `dispatch`, and
the independently available compatible `execute+claim_authority` registration
before use. An old server accepting the
underlying launch or down as an uncorrelated ordinary request is a hard error,
not a fallback.

The generic stores refuse:

- a different payload or spec hash for an existing action attempt;
- a proposed attempt other than the existing idempotent attempt or exactly one
  higher than the durable attempt;
- materialization unless `kernel_state=READY`, due, and locally registered;
- materialization without the dispatching instance's own fresh byte-matching
  registration/manifest with `dispatch`, or without a separately available
  fresh compatible `execute+claim_authority` registration, both discovered
  before the transaction;
- any post-action registration lock in the materialization transaction;
- materialization for a terminal/superseded/non-due action or unregistered
  kind;
- reuse of one request by two actions/bindings; and
- any attempt to make the dispatcher parse or override a domain fence.

## Reconciliation and Scheduling

The active controller remains the Serve desired-state policy owner, but it
does not query due actions or terminal requests. The shared kernel owns:

- the sole partial-index due query for local kinds with
  `kernel_state=READY`, ordered
  `next_attempt_at ASC, priority DESC, requested_at ASC, action_id ASC`;
- primary attempt materialization for mutation and observation descriptors;
- the terminal-binding result scan that routes IDs to the registered reducer.

An observation is never a nested child or an action-local poll. It advances
`current_attempt`, receives a new deterministic primary request, queue row and
generation-one lease, and commits READBACK effects. Serve may remain
`serve_state=VERIFYING` throughout that kernel attempt. Thus the action table is
the one generic due index while `api_request_queue` remains the only delivery
queue and its claim remains the sole execution lease.

There is no resource action for an autoscaling decision, rolling-update wave,
load-balancer cutover, or drain wave. Their existing Serve replica/service
fields remain authoritative. The journal covers only per-replica external
launch and down side effects.

The controller publishes desired-state changes and consumes projected domain
state; it does not create one polling thread per replica. API/kernel handoff
resumes the generic due/result scans, while a Serve owner handoff replaces its
owner token and later pre-call/reducer transactions revalidate that fence.

Launch and down execute only as the two dedicated one-attempt Serve action
handlers. The launch handler calls `sky.execution.launch` directly and the
down handler calls `down_expected_generation` directly; neither calls an
existing HTTP/SDK endpoint or schedules another request. They contain no
controller-local retry loop. Backend-internal bounded placement/failover may
remain within the sole correlated request attempt and is reflected in
normalized effect rows plus the request's frozen aggregate hash/count. A failed
launch does not admit another mutation until cleanup/absence is proven;
uncertainty is Serve `VERIFYING` paired with kernel READY observation,
REDUCING, or BLOCKED according to the generic graph.

Existing global and per-service provisioning/down concurrency rules remain in
force. During migration, admission counts both legacy threads and active
actions. In steady state, durable active-action counts replace thread-map
counts; no controller-process file lock is an ownership primitive.

### Relationship to provider lifecycle actuation

This design and `docs/designs/provider-lifecycle-actuation.md` are jointly
canonical and must be amended together in the same carrying commit whenever
their shared kernel, facet, offer-handoff, or lock contract changes. Commit
`7aaa99041065a57c6f733ceed04f025520bac871` is only the upstream baseline used
for this reconciliation; it is not the current exact provider contract.
This design implements the anticipated
single shared action store/kernel, with one deliberate correction: the action
row owns due state/materialization but no long-lived claim/lease. PR #1070's
bound request remains the sole execution lease. Serve is one domain adapter
and keeps its exact incarnation, desired target, reservation, compensation,
outcome, and deletion-proof policy.

Execution calls the existing provisioner facade. Exactly one selected
`ProvisionerBundleV1` lifecycle facet owns each provider mutation and
observation. Neither the generic action-adapter registry nor the
Kubernetes-evidence adapter is a provider registry, lifecycle dispatcher, or
fallback owner. No action kind may route the same effect through a Cloud
method, plugin, and provisioner implementation competitively; canonical
provider resolution chooses the one facet before the effect intent is sealed.

The current upstream `PlacementOfferV1` S1 contract explicitly supports only
an ordinary direct-Pod cluster create. It excludes Serve, pool and managed-job
controllers, and controller/HA Deployment/PVC actuation. Consequently v1
Serve launch/down specs, request envelopes, domain fences, effects, and shadow
operation envelopes contain no `PlacementOfferV1`, offer ID, observation ID,
or placement-attempt fence. V1 records the already selected legacy placement
and exact resource override as domain decision provenance; it does not
relabel that projection as an offer or call an offer source from retry.

A later Serve offer integration requires reviewed action-spec/attempt schema
v2. It may bind and persist one exact immutable offer envelope plus its
attempt fence only for the first provider mutation. Once any provider mutation
may have occurred, that offer is never rebound or replayed. Recovery first
uses the same lifecycle facet and persisted locator/operation evidence to prove
the complete old attempt absent. Only after that fenced absence may
orchestration emit exact
`NOT_REPRESENTABLE(RETRY_AFTER_PROVIDER_ATTEMPT)` and enter
`LEGACY_RETRY_AFTER_PROVIDER_ATTEMPT`; any different-offer or ordinary legacy
handoff before absence is forbidden. Schema v2 must be adversarially reviewed
with the provider lifecycle design before either authority is enabled.

M1 must add the exact `kubernetes-observation-v1` locator,
verification-only preprovisioned bootstrap, pre-call callback, label injection,
exhaustive named-target inventory/GETs, UID-precondition deletion, complete
observation, dedicated role-split executor, durable one-use
registration/consumption/release ledger, fixed non-refreshing full
Pod-UID-bound credential, two database challenges, bounded precommitted
mutation-window/call slots, complete request-execution-chain/admission-freeze
proof and watches, and live-verified provider-side effect-settlement facets
above to the Kubernetes bundle before any provider can be authoritative.
Generic `InstanceLifecycleV1` name/config lookup and its
unqualified status map are insufficient. These bounded synchronous facets are
M1 prerequisites, but all are methods/evidence of the one lifecycle owner; none
registers a competing owner.
Providers/profiles without an equivalent later reviewed facet remain on the
explicit legacy execution boundary (with shadow input diagnostics only) and
cannot contribute matched provider parity or enter authoritative mode.

## Typed Outcomes

The resource outcome is `ServeReplicaActionOutcomeV1`, stored only in
`api_serve_replica_action_projections.serve_last_outcome`. The generic
`api_resource_actions.last_result_*` quartet stores only bounded kernel
mechanics such as terminal request/effect hashes, reducer disposition and
descriptor provenance. No Serve kind, certainty, state or provider observation
is stored as generic `last_result`.

`serve_last_outcome` is the last committed domain reduction. Generic
materialization and claim do not clear or mutate it because they do not lock
the projection. A first Serve `PLANNED` action begins with it null; later
Serve `QUEUED|RUNNING` projection values may retain the previous retry or
verification outcome until the next domain reducer atomically replaces it.
This is historical evidence, not current execution authority; readers always
join `kernel_state`.

The exact v1 object is:

```text
ServeReplicaActionOutcomeV1 = {
  version: 1,
  kind: "success" | "no_capacity" | "quota" | "auth" |
        "invalid_config" | "transient" | "interrupted" | "unknown",
  scope: "resource" | "zone" | "region" | "cloud" | "account" | "unknown",
  retry_after: null | UtcTimestamp,
  cleanup_certainty: "not_started" | "uncertain" | "present" | "deleted",
  failover_safe: boolean,
  attempt_result: {
    source: "handler_result" | "request_lease_reaper" |
            "request_cancellation" | "executor_shutdown" |
            "request_dispatcher" | "admission_fence" | "legacy_import",
    kind: "no_capacity" | "quota" | "auth" | "invalid_config" |
          "transient" | "interrupted" | "unknown",
    scope: "resource" | "zone" | "region" | "cloud" | "account" | "unknown",
    proof: "mutation_ack" | "classified_failure" |
           "ambiguous_owner_loss" | "mutation_not_started" |
           "ambiguous_interruption" | "imported_ambiguous_state",
    observed_at: UtcTimestamp,
    effect_indexes: [NonnegativeI64],                 # <= 2048, sorted/unique
    window_results: [{
      effect_index: NonnegativeI64,
      window_id: CatalogUuid,
      disposition: "no_bytes_sent" | "definitive_response" | "ambiguous",
      result_sha256: null | Sha256
    }],
    provider_details: {
      provider: null | Token[64],
      adapter: null | Token[128],
      region: null | Text[128],
      zone: null | Text[128],
      code: null | Text[128],
      http_status: null | integer 100..599,
      operation_id: null | Text[512],
      idempotency_token: null | Text[512],
      phase: "admission" | "claim" | "pre_mutation" | "mutation" |
             "cleanup" | "backfill",
      reason: "handler_returned_requires_observation" |
              "execution_lease_expired" | "dispatcher_submit_failed" |
              "broken_process_pool" | "precondition_failed" |
              "retryable_failure" | "terminal_failure" |
              "legacy_imported_ambiguous" | "superseded_before_start" |
              "explicit_cancel_before_start" |
              "explicit_cancel_after_claim" |
              "executor_shutdown_before_start" |
              "executor_shutdown_after_claim" | "worker_lost"
    }
  },
  provider_state: {
    certainty: "not_started" | "uncertain" | "present" | "deleted",
    source: "attempt_result" | "provider_observation" |
            "executor_quiescence" | "legacy_import" |
            "zero_locator_journal",
    proof: "mutation_not_started" | "resource_present" |
           "resource_absent" | "observation_uncertain" |
           "ambiguous_interruption" | "imported_ambiguous_state" |
           "resource_never_prepared",
    observed_at: UtcTimestamp,
    effect_indexes: [NonnegativeI64],                 # <= 2048, sorted/unique
    observation: null | KubernetesObservationSetV1,
    zero_locator_retirement: null | ZeroLocatorRetirementProofV1,
    provider_details: {
      provider: null | Token[64],
      adapter: null | Token[128],
      region: null | Text[128],
      zone: null | Text[128],
      code: null | Text[128],
      http_status: null | integer 100..599,
      operation_id: null | Text[512],
      idempotency_token: null | Text[512],
      phase: "claim" | "observation" | "backfill" | "retirement",
      reason: "attempt_result_not_started" |
              "attempt_result_uncertain" |
              "provider_observation_present" |
              "provider_observation_absent" |
              "provider_observation_uncertain" |
              "executor_quiesced_before_prepared" |
              "legacy_imported_ambiguous" |
              "zero_locator_resource_never_prepared"
    }
  },
  execution_quiescence: null | ProviderExecutionQuiescenceV1
}
```

Every effect index is below the frozen request effect count.
`attempt_result.effect_indexes` equals the sorted indexes in
`window_results`. Each window result is the typed projection of the generic
effect result/evidence quartets; missing completion is `ambiguous` with null
result SHA. Provider/idempotency values must equal the generic effect fields.
No outcome contains or authorizes a mutable combined effect projection.

The closed source rules remain:

- handler mutation acknowledgement is
  `mutation_ack/handler_returned_requires_observation` and has uncertain
  provider state;
- a classified failure may claim `mutation_not_started` only before provider
  entry, with zero effects if unclaimed or effect zero plus exact quiescence if
  claimed;
- lease reaper is
  `ambiguous_owner_loss/{execution_lease_expired|worker_lost}`;
- cancellation/shutdown distinguish exact before-start from ambiguous
  after-claim;
- dispatcher/pre-call failures claim not-started only under the same exact
  entry fence;
- legacy import is
  `imported_ambiguous_state/legacy_imported_ambiguous`; and
- handler/delete acknowledgements never assert `present` or `deleted`.

Provider observation evidence comes only from a primary observation attempt.
Its generic effects carry typed Serve readback intent, locator, result and
evidence quartets. `resource_absent` requires two complete rounds and absence
for every locator. Launch `resource_present` requires exactly one live
matching topology and confirmed absence elsewhere. Down `resource_present`
requires at least one retained locator present. Anything else is uncertain.
A local-row miss, subset, one-round absence or unqualified status map is never
absence.

Execution quiescence is required after owner loss, lease expiry, claimed
cancellation/shutdown, or any missing/ambiguous window. The outcome copy must
be byte-equal to the request's append-once quiescence quartet and frozen
effect count/hash. It is evidence only; the request remains its authority.
Attempt classification remains immutable across later primary observation
attempts, while `provider_state` may be replaced by the new observation.

The exhaustive projection/kernel matrix is:

| Serve projection state | Required kernel state | Outcome contract |
|---|---|---|
| Serve `PLANNED` | kernel `READY|QUEUED|RUNNING|REDUCING` under the lag matrix | null for a native first attempt |
| Serve `QUEUED|RUNNING` | corresponding or later nonterminal kernel state under the lag matrix | null initially or retained previous outcome; never execution authority |
| Serve `VERIFYING` | kernel `READY|QUEUED|RUNNING|REDUCING|BLOCKED` under the lag matrix | non-null uncertain/verification outcome; kernel READY alone has `retry_after=next_attempt_at`, all other pairs have null retry_after |
| Serve `RETRY_WAIT` | kernel `READY|QUEUED|RUNNING|REDUCING` under the lag matrix | non-null retry outcome; `retry_after` equals kernel due time only while READY and becomes null after materialization |
| Serve `SUCCEEDED` launch | kernel `TERMINAL` | success/present from complete primary observation |
| Serve `SUCCEEDED` down | kernel `TERMINAL` | success/deleted from complete all-locator primary observation |
| Serve `TERMINAL_FAILED` launch | kernel `TERMINAL` | terminal classification with not-started or proven-deleted certainty |
| Serve `TERMINAL_FAILED` down | forbidden | cleanup never gives up |
| Serve `SUPERSEDED` | kernel `TERMINAL` | interrupted with not-started or proven-deleted certainty |

Top-level `cleanup_certainty` equals provider-state certainty. Observation
success alone changes kind/scope to `success/resource`; observation-proven
absence after a classified terminal failure preserves that failure
classification. `failover_safe=true` is restricted to a launch retry outcome
with not-started/deleted certainty, kind no-capacity/quota/transient and
zone/region/cloud scope. Unknown keys/versions, raw messages, exceptions,
bodies, credentials and environment values are rejected.

A legacy import creates Serve `VERIFYING` plus kernel `READY` with primary
attempt one observation descriptor. It never performs an automatic attempt-zero
observation. Inconclusive observation reduces to another primary observation
descriptor or kernel `BLOCKED`; conclusive evidence follows the same terminal
matrix as native actions.

## Migration Plan and Stacked Commits

### M0: Canonical design

- Commit this design before implementation.
- Run adversarial review against this exact file.
- Resolve every blocking review finding in place and re-review.

### M1: Additive schema and shadow journal

- Add the shared resource-action migration runner and install/verify API005
  first with only the independent generic ownership/action/effect/child/
  correlation/quiescence/adapter kernel and marker.
- Install/verify Serve032 second with only Serve identity/mode/capability and
  downgrade-maintenance fields, then API006 with the Serve projection, three
  shadow tables, executor registration/freeze ledger, registration-ID field,
  typed Serve evidence extensions, and Serve-specific guards. Exercise the
  exact composite order `005 -> Serve032 -> 006`.
- Assign and backfill stable replica incarnation UUIDs and add the per-service
  activation mode.
- Add the exact launch/down v1 models, PostgreSQL-owned canonicalizer, exact
  admission envelope, typed outcome/evidence model, and fenced store. Delete
  the provisional `provider_inputs` bag; reject every unknown version/key.
- Add the built-in `pod_cluster_v1` locator/observation facet, exact
  verification-only preprovisioned bootstrap gate, protected-label injection,
  and complete all-kind observation. Add the dedicated role-split HA executor,
  fixed snapshot of its projected Pod-UID-bound credential, disposable child
  supervision, precommitted mutation-window/call slots and response matrix,
  reconstructible token-expiry quiescence proof, and pinned
  backend/full-request-chain/primary-visible admission-freeze/live-settlement
  gate, complete static-guard deployment inventory, and the distinct
  downgrade-only guard-free backend allowlist. No
  shadow/provider callback may
  claim v1 capability until these startup verifiers pass.
- Shadow-create outer diagnostic samples and provider/direct child operations,
  never action rows or correlated requests, for new eligible Serve launch/down
  decisions. Persist canonical input/terminal hashes, the sample-owned ordinary
  request link/result, provider observation, and append-only divergence episodes
  as specified above while the existing thread implementation remains
  authoritative.
- Add schema, structural-marker, cross-schema migration race,
  state/outcome-matrix, canonical-domain, shadow input/outcome parity,
  idempotent-admission, and PostgreSQL concurrency tests.
- Add and subprocess-test the single exceptional downgrade maintenance module,
  including explicit confirmation, process/write quiesce, empty-history gates,
  durable stored API/controller/executor owner quiesce,
  guard-draining/disabled phases, complete installed-but-never-
  registered target coverage, owner rollout pins, old-backend absence,
  common-lock serialization, exact API006-then-Serve032-then-API005 down order,
  all seven durable states and six edges including final fence removal, and the
  core/full postverifiers.
- Deploy to the isolated `skypilot-ha` test release and prove migrations,
  shadow parity, rollback, and cleanup.

Rollback: retain both additive schemas. A pre-M1 application image is permitted
only after the common lock and provider-mutation quiesce prove that every
service is still `legacy`, no retained shadow terminal tombstone can authorize
cleanup, and no shadow sample, callback, or provider mutation is in flight. If
any service is `shadow` or any retained shadow tombstone still owns cleanup,
rollback must use an M1-compatible sampler image that continues the mandatory
one-connection journal and pre-call hooks; rollout may pause mutation but may
not stop the journal and then allow legacy mutation. Because mode cannot
decrement, such a row never becomes eligible for a pre-M1 binary. Schema
downgrade is an exceptional operator action allowed only when there is no
action, shadow, or correlated history and every service is `legacy`; ordinary
app rollback does not drop incarnation identity or action history. If
explicitly approved, schema downgrade uses the common exclusive lock and the
closed resume-state protocol above.

### M2: Dark recovery and retry evaluator

- Read immutable shadow samples, their ordinary request links/results, and
  provider observations through bounded snapshots and derive the exact
  hypothetical kernel/Serve state pair, attempt, ambiguity decision, and
  PostgreSQL-clock
  retry time.
- Do not create an action, correlated request, queue delivery, provider
  operation, or durable retry schedule. Legacy threads remain the sole
  identity, mutation, completion, and retry authority; M2's evaluator is
  read-only and may emit only bounded diagnostic metrics/logs.
- Assert as a deployment invariant that action/correlation history stays empty
  while no service is authoritative, and compare every dark derivation with
  the matching shadow outcome.
- Add controller-handoff and crash-boundary tests proving a fresh evaluator
  derives the same result without performing or scheduling work.
- Deploy, kill evaluators/controllers at every boundary, and verify unchanged
  legacy execution plus stable dark results.

Rollback: deploy an M1-compatible image and continue mandatory shadow
journaling. There is no action projection or durable authority to unwind.
A pre-M1 image is subject to the stricter M1 rollback gate above.

### M3: Durable action, retry, and request execution authority

- In the first authority-capable image, create launch/down actions for
  authoritative services, recover their current request IDs/results, schedule
  attempts and cleanup from PostgreSQL timestamps, and reconcile ambiguous
  attempts before any replacement request.
- Submit provider work only as the correlated API request attempts defined
  here; action reconciliation owns logical identity and retry, and no legacy
  thread is an execution adapter for an authoritative service.
- Admit down immediately as Serve `PLANNED` plus kernel `READY` with its
  complete descriptor and database due time; perform the idle/drain check only
  in the protected pre-call hook.
- Replace per-replica thread polling with batched action/request
  reconciliation and count durable active actions for admission.
- Advance each service from `shadow` to `authoritative` only after every
  controller-capable old binary has drained, all live replicas have incarnation
  IDs, bound cluster hashes, pre-mutation Kubernetes locator labels, and
  immutable exhaustive named-target inventories whose exact GET/LIST evidence
  is consistent; the
  current service owner token is non-null; every locator names a pre-existing
  Namespace and pre-existing non-default ServiceAccount by UID, FUSE is false,
  and the bootstrap verifier proves no shared-resource write; the complete
  candidate set fits the four-kind `pod_cluster_v1` obligation set; every
  mutator is a dedicated single-request Pod in the same management cluster
  under the role-split HA topology, uses only one fixed snapshot of its
  projected Pod-UID-bound service-account token with no refresh path, and has
  the exact disposable-executor, precommitted call slots/response matrix,
  fixed-token-expiry quiescence, full credential projection, SQL nonce plus
  the barrier/cache/two-challenge sequence, complete request execution chain, an exact
  dynamic-engines-disabled or primary-backed static-freeze branch, the complete
  installed-guard deployment inventory and matching per-backend installation
  entry plus its out-of-band `StaticPodOwner` when that branch is used, watches,
  RBAC, and fresh provider-side settlement capability; at least one
  target-bound `purpose=readiness` registration is atomically consumed into the
  promotion audit, and later actions can use only newly registered
  `purpose=action` executors; the Serve
  API005, Serve032, and API006 heads, markers, and exact structures all pass
  structural verification;
  and the target image advertises both `resource_action_kernel/v1` and
  `serve_replica_action_adapter/v1`.
- Deploy and run the full fault-injection matrix.

Rollback: activation is one-way (`legacy -> shadow -> authoritative`) for a
service. Fleet rollback keeps the schema and marker, pauses new admission,
quiesces or reconciles current attempts, and deploys only a designated
M3-compatible rollback image that understands authoritative actions,
correlated requests, locators, and immutable quiescence and can continue their
safe reconciliation. The read-only M2 image and every pre-capability controller
are forbidden while an authoritative service or retained authoritative
tombstone exists.

### M4: Remove legacy ownership

- Delete every steady-state legacy path from the v1-eligible authoritative
  central manager in the Removal Map. Extract the explicitly retained central
  legacy, central shadow, local SQLite, and non-consolidated-pool adapters so no
  shared branch can route an eligible authoritative service back to threads.
- Route full-service, failed-service, and exact v1-tombstoned orphan teardown
  through the same locked factory matrix; authoritative cleanup with a
  nonempty locator becomes a durable terminal action, while the exact
  never-prepared empty-locator branch closes atomically without down authority.
  A truly unscoped pre-M1 orphan remains on the
  separately audited legacy manual-cleanup/quarantine boundary and cannot be
  routed into this v1 factory by name.
- Delete compatibility tests that only instantiate removed maps or threads and
  replace them with durable action invariants.
- Keep only explicitly fleet-gated readers needed for old persisted replica
  rows.
- Deploy the removal image, roll back to the last M3-compatible image, and
  re-upgrade.
- Append exact test and live evidence only through the sentinel-delimited
  evidence ledger after the carrying contract commit is accepted.

### M5a: Authoritative-reader cleanup

- After every shared autoscaling, reserved-capacity, history, launch-budget,
  lifecycle, and teardown consumer can read authoritative action/lifecycle
  state without `sky_launch_status` or `sky_down_status`, remove those reads and
  projection writes only from the durable authoritative manager.
- Retain the v1 JSON fields, `ReplicaStatusProperty` support, shared SQLAlchemy
  metadata, and physical `replicas.sky_down_status` column for central
  legacy/shadow, local SQLite, remote non-consolidated pools, and orphan v1
  rows. Decoding follows the row's retained execution format/evidence, never a
  guessed current service mode.
- Record the authoritative-reader fleet query and date. This milestone does not
  claim global persisted-format deletion.
- Retain the Serve capability marker and both Alembic histories while any
  action-capable binary or retained action history exists. They are removed
  only by the exceptional full schema downgrade, not by normal M5 cleanup.

### Later M5b: Global legacy-format deletion — outside this design

Physical removal of the shared status column/codec/Python fields requires zero
central legacy or shadow services, zero local SQLite/remote-pool deployments,
zero orphan v1 rows, complete provider/profile coverage, and a separately
reviewed dialect/metadata migration. Public unsupported-provider support means
that gate is intentionally not claimed by this feature.

## Existing-Service Backfill

M1 does not manufacture completed action history for every healthy replica.
It creates actions on the next relevant transition.

A provider object created before M1 normally has neither the immutable
`skypilot-cluster-hash` label nor a locator persisted before its first provider
call. A later name lookup, local cluster row, or observation cannot manufacture
that pre-call fact. Such a resource may remain on legacy execution with a
pre-M1 observation-only sample, but it cannot be adopted into a v1 action and
does not count toward provider parity. It must be legacy-roll-replaced by an
M1-created, labeled resource before authoritative activation.

Only after the M3 authoritative transition, nonterminal replicas with every
exact v1 source field are classified conservatively. M2 never creates or
attaches an action. On M3 restart, an already correlated row is reused; a row
missing one source follows the observation-only divergence rule below instead
of creating an action:

- pending/provisioning with a correlated active request: attach to it;
- pending/provisioning without correlation: create Serve `VERIFYING` plus
  kernel `READY` with an immediately due primary attempt-one observation
  descriptor, and never immediately relaunch;
- shutting down or failed cleanup: create the same Serve `VERIFYING` plus
  kernel `READY` pair for an attempt-one primary down observation;
- ready replicas: no active action is required, but a name-only pre-M1 replica
  remains ineligible for authoritative activation until legacy-roll-replaced;
- terminal failed replicas without possible provider residue: no active action
  is required; and
- malformed service hash/replica incarnation or a non-null resource scope that
  differs from the service hash: retain the existing fail-closed recovery path
  and emit an operator-visible migration error. A null resource scope alone is
  valid legacy namespace evidence, not malformed identity.

While still in `shadow`, a pre-M1 replica whose persisted process state says a
launch/down may have started but has no trustworthy request link did not pass a
shadow input audit. With a valid service hash/incarnation, recovery inserts the
next `pre_m1_ambiguous_recovery` episode and attempt-zero `OBSERVING` recovery
sample, performs no replacement mutation, and follows the legacy/provider
observation path. Conclusive evidence with a reconstructable exact spec makes
that sample `RECOVERED`; conclusive evidence without a reconstructable exact
spec makes it `DIVERGED`; uncertainty leaves it `OBSERVING`. It may resolve the divergence only after
fresh provider evidence proves the old mutation's present/absent result and
the exact current version/override identity is reconstructable; otherwise it
blocks authoritative activation. It never fabricates a `MATCHED` sample. If
hash or incarnation is also missing, the truly ownerless quarantine rule above
applies and no divergence row is guessed.

Backfill is idempotent and fenced by service hash, replica row lock,
incarnation ID, and lifecycle epoch. It never guesses identity from cluster
name alone. Before creating an action it resolves/backfills the independent
durable workspace. It copies a null service scope as `{kind: legacy,
value: null}` and a scope equal to service hash as `{kind: incarnation,
value: <service_hash>}`; it rejects every other combination. The copied scope
survives later service-row deletion.

Backfill creates a v1 action only if it can also prove an existing immutable
cluster-record UUID; a nonempty exact `KubernetesProviderLocatorV1` array that
was persisted before the original provider call; the matching immutable UUID
label and `pod_cluster_v1` profile; exact decision provenance/override source;
the complete logical-target tuple (including exact-card arrays); image content
snapshot when applicable; and the complete retirement/terminal snapshot. It
copies those values; it never reconstructs them from eventual placement,
coarse process status, a current object listing, or cluster name. In practice
this action-backfill branch is for an M1-created mutation whose durable row
outlived its process, not for a name-only pre-M1 resource. A row missing any
required value remains observation-only and records the corresponding closed
divergence/quarantine, blocking promotion until legacy replacement supplies
native audited coverage.

Backfilled actions use `origin=legacy_backfill` and start with Serve
`VERIFYING`, kernel `READY`, `current_attempt=0`, and a complete immediately
due primary attempt-one observation descriptor. The dispatcher materializes
that observation as the normal positive correlated attempt and request; there
is no automatic attempt-zero result, provider call, or child request.
Observation then follows the ordinary reducer contract: conclusive present or
absent evidence maps to the action-kind result, a safe mutation if needed
becomes a new kernel `READY` primary attempt descriptor, and uncertainty
becomes another safe primary observation descriptor or kernel `BLOCKED`.

Every `provider-observation evidence` phrase in this backfill contract
means a complete `KubernetesObservationSetV1` with exact one-to-one ordered
coverage of the stored locator array, labels, scopes, profiles, and all-locator
launch/down predicate. A local inventory miss, subset, name-only pre-M1 lookup,
or delete acknowledgement is uncertain and cannot take either success edge.
The outcome records that success was observation-proven and the provider
certainty used. This preserves the no-manufactured-history rule while keeping
native action/request invariants strict.

## Activation and Mixed-Version Protocol

`services.resource_action_mode` is durable and monotonic:

- `legacy`: existing threads and fields are authoritative; no action is
  required;
- `shadow`: existing execution remains authoritative and every eligible
  launch/down transition writes a diagnostic sample, links only its ordinary
  uncorrelated legacy request, and is input/outcome/observation parity-checked;
  no action attempt exists; and
- `authoritative`: action/request state owns admission, retry, ambiguity, and
  completion, while legacy fields are compatibility projections only.

Entering `shadow` makes the mode trigger clear
`resource_action_parity_window_started_at`. Inserting a divergence episode,
detecting an unsampled mutation, or observing a parity regression also clears
it in the same locked service transaction. After every blocker resolves, a
full locked zero-blocker audit uses `start:<service_hash>` and the trigger sets
it to PostgreSQL `clock_timestamp()`. Correctly matched activity, including a
new outer attempt, does not clear or restart it. Promotion re-runs the full locked evidence
audit and requires the timestamp at least 86,400 database-clock seconds old,
zero blocker since it, and every currently relevant sample/child terminal with
the exact resolved/matched rules above. Thus a live nonterminal attempt blocks
the final audit but does not erase prior clean time. Promotion sets mode to
`authoritative` and clears the shadow-only timestamp in the same transaction.

Every newly inserted service is database-guarded to `legacy`; neither a fleet
default nor an INSERT-time application choice can activate it. Adding the
column also leaves every existing service in `legacy`. The current token-owning
SkyServe controller performs each later transition under a service-row lock
and records an event. No code path decrements the mode.

The locked `legacy -> shadow` audit is a fail-closed journaling readiness gate.
It requires a non-null current owner token; a stable service hash/workspace/
lifecycle epoch; exact incarnation and bound `resource_action_cluster_hash` on
every live eligible replica; the one-connection sample/replica/capacity-claim
transaction for ordinary, reserved-fill, paid-claim, retirement, full-service,
and consolidated-pool paths; sample-token ordinary admission; and every
provider/direct pre-call hook. A pre-M1 cluster hash may be copied only after a
fresh global-state plus provider observation ties that exact current resource
to the replica/service scope. Otherwise the replica is quarantined or
legacy-roll-replaced before shadow; the migration never guesses by name.

Readiness canaries must show that instrumentation preserves the legacy policy
choice and does not filter a provider/candidate. If a shadow journal write or
required callback fails after activation, the decision stops before external
mutation and records/alerts `atomic_store_unavailable`; it does not silently
fall back to unjournaled legacy behavior. A non-Kubernetes or unsupported
profile may still execute its unchanged legacy choice after a successfully
recorded `provider_capability_unavailable` divergence, but it cannot promote
while that candidate/resource remains unsupported or the episode is open. The
episode may resolve only after the adapter becomes exact-v1 capable and a later
matched provider observation supplies the normal immutable evidence; replacing
the candidate does not retroactively claim parity for the unsupported call.
This exception is explicit input-only shadow coverage, not provider parity.

The locked `shadow -> authoritative` audit additionally requires every live
replica to have a nonempty exact Kubernetes locator array whose UUID label was
installed before its mutation and whose immutable exhaustive named-target
inventory has complete consistent exact-name evidence; name-only pre-M1 resources must be
legacy-roll-replaced. The complete feasible candidate set must be Kubernetes
`pod_cluster_v1`; every locator's Namespace and non-default ServiceAccount must
already exist with the stored UIDs, FUSE must be false, and the verification-
only bootstrap plan must contain no Namespace, ServiceAccount, RBAC, DaemonSet,
or other shared-resource write. The owner token must still be current. Every
authoritative executor must use the role-split HA topology, a dedicated
single-request Pod in the target management cluster, only one fixed
non-refreshing snapshot of its projected Pod-UID-bound service-account token,
the exact bounded mutation-window/call-slot/disposable-child protocol,
startup-verified RBAC, full credential projection, SQL nonce and
barrier/cache/two-challenge sequence, durable unconsumed one-use registration, complete request execution
chain, exact disabled-engine or primary-backed static admission freeze, and,
for the latter, an exact out-of-band `StaticPodOwner` entry in the complete
immutable deployment inventory, its gap-free watches, and the fresh
provider-side effect-settlement capability.
The promotion transaction consumes the exact readiness registration(s);
ordinary runtime-token advertisement cannot replace that durable audit. Every provider
proof/callback/capability test above must pass. Mixed,
remote, static-kubeconfig, shared-bootstrap, custom-kind, or otherwise
unsupported candidates remain shadow and are never silently narrowed.

Before the first authoritative transition, the rollout gate proves that all
controller-capable pods and controller subprocess launchers advertise Serve
032/API 005 support and that old consumers are drained. “Support” means the
process advertises both `resource_action_kernel/v1` and
`serve_replica_action_adapter/v1` only after their exact generic-kernel and
Serve marker/projection/API 005 startup verifiers pass; a build-version string
or column-exists probe is insufficient. During an application rollback,
database migrations and the capability marker normally remain applied. The
designated M3-compatible rollback image can read authoritative rows, stop new
admission, reconcile every active action, and preserve safety while the fault
is repaired. The dark M2 image and any image that ignores the marker are
forbidden while authoritative rows or tombstones exist.

## Deployment and Rollback Protocol

M1/M2 shadow deployment uses the guarded HA release described in
`docs/designs/multi-replica-api-server.md`:

- Kubernetes context `boltz-test`;
- namespace and Helm release `skypilot-ha`;
- PostgreSQL request backend;
- blocking migration hook before target-image pods; and
- `--reuse-values` on upgrades.

The canonical shadow/fail-closed target remains
`boltz-test/skypilot-ha/skypilot-ha`.
The 2026-07-31 recheck found rootless Buildah available for local image
construction, but SSO access to the isolated target and registry push
credentials remain unverified. Production
`gitops-hub-rainier/skypilot/skypilot` is explicitly out of scope for iterative
fault injection. No milestone may substitute production for the missing test
target; test-cluster access and an immutable-image publication path are an open
deployment prerequisite. Before M3, live preflight classifies that managed
control plane. If it lacks an exact authoritative branch, M1/M2 and shadow
fault injection continue there, its missing runtime token is the expected
fail-closed result, and M3 is not declared deployed on it. M3 must instead name
an explicitly approved isolated role-split HA target with the same
PostgreSQL/migration/immutable-image controls and the exact eligible execution
chain, or wait for a separately reviewed contract change. The evidence ledger
records which target satisfied each gate; an ineligible shadow result is never
relabelled authoritative completion. The selected eligible M3 target remains
the M4/M5 rollback, removal, and verification target.

Normal `helm rollback` only selects a milestone image proven compatible with
the retained Serve/API heads; Helm rollback does not execute migration hooks
and is never a schema-downgrade mechanism. The first M1 deployment that installs
API005/Serve032/API006 must omit `--atomic`: automatic image rollback after the
migration hook is unsafe until the prior image has passed ahead-of-head startup
tests or the maintenance command has passed full quiesced restoration tests.
Failure is handled manually by deploying the compatible M1 repair image, or by
running the exceptional maintenance command after its gates pass. A later
milestone may use `--atomic` only when its automatically selected rollback image
has been proven compatible with the retained heads.

For every image:

1. preserve the current Helm values and immutable image digest;
2. build and push the exact commit;
3. run the shared resource-action migration hook, which holds the common
   advisory lock while it installs and structurally postverifies API005 first,
   Serve032 second, and API006 third on one physical PostgreSQL connection;
4. wait for two Ready API and controller replicas plus the milestone's declared
   executor topology;
5. verify both Alembic heads, the exact API005 generic marker/structures,
   Serve032 marker/structures, API006 marker/structures, the action activation
   phase, and either the eligible
   target's advertised `/v1` runtime capability or the shadow target's expected
   fail-closed absence;
6. run milestone canaries and controlled controller eviction;
7. capture logs, database evidence, request/action rows, and provider state;
8. remove canaries, failed revisions, temporary RBAC/secrets, and test
   workloads; and
9. verify the declared clean namespace state.

A failed milestone is repaired in a new stacked commit and redeployed. It is
not hidden by rewriting evidence.

## Fault-Injection and Test Plan

The mandatory crash points for both launch and down are:

1. before action creation;
2. after action creation but before request admission;
3. after API request commit but before the controller receives its ID;
4. after request claim but before provider mutation;
5. after the mutation window commits and request bytes may have been
   accepted/applied, but before the caller receives a response or fills the
   non-authorizing window result;
6. after a definitive window-result fill or ambiguous handler return but
   before the single request/action terminal transaction;
7. after atomic request/action terminalization but before replica projection;
8. after lease expiry, with no observation or reassignment until exact
   execution quiescence is persisted and `settle_after` has elapsed;
9. during SkyServe PID/IP owner handoff at the same lifecycle epoch and during
   a lifecycle-epoch change;
10. during API and executor pod eviction; and
11. during rollback and re-upgrade.

For every point, assert:

- one logical action and at most one active request attempt;
- no duplicate logical replica;
- a stale worker cannot commit;
- ambiguous launch is observed/adopted before retry;
- down never succeeds without deletion proof;
- retry attempt and `next_attempt_at` survive every restart;
- no old-generation resurrection;
- old serving capacity remains until existing replacement gates permit
  retirement; and
- final request, action, replica, usage, and provider state agree.

Required automated coverage:

- migration upgrade/downgrade and schema constraints on real PostgreSQL;
- `server_encoding=SQL_ASCII` refusal before the first DDL, exact 600-second
  nontransactional `pg_try_advisory_lock` timeout, GUC-without-current-backend
  lock rejection, same-winning-connection reuse across all ordered Alembic
  phases, and shared-writer serialization for every API005/Serve032/API006
  artifact class;
- two-host/process upgrade serialization at both `005 -> Serve032` and
  `Serve032 -> 006` boundaries; API005 independently succeeds without a Serve
  catalog, while direct Serve032/API006 crossing without exact prerequisites
  fails without DDL;
- generic-kernel tests for nonlocking due discovery, epoch-first CAS,
  byte-identical descriptor materialization, exact action-kind uniqueness,
  coexistence with a future non-Serve adapter, two-stage claim-authority
  rollback/CAS races, the universal registration-first lock order, request-only
  heartbeat, and no action lease/second queue;
- request-backend integration tests that perform the nonlocking correlation
  read before entering `update_request()`, route every direct correlated
  terminal API and the reaper to the special `REDUCING` store, retain ordinary
  request-first terminalization, and restrict correlated nonterminal progress
  to its claim-fenced request-only CAS;
- placement tests proving Serve/pools/controllers/HA are excluded from
  `PlacementOfferV1` S1 and no offer field exists in either v1 action spec;
  reviewed-v2 tests must enforce offer use only before the first provider
  mutation, fenced absence before exact retry handoff, and one lifecycle facet
  owner with no competing provider registry;
- structural-verifier rejection for each independently corrupted property:
  missing/wrong marker, Serve head below 032, wrong UUID/text type,
  nullability, default, unvalidated or widened mode constraint, generated
  column, wrong capability-table column/constraint, or a missing/widened/
  defaulted downgrade-state column, phase/shape/size/hash constraint, primary
  key, or unexpected ordinary-startup row; independently corrupt
  the executor registration ledger's state/purpose/nonce/freeze mode and
  timing/two challenges/barrier/cache/release-evidence/unique pairs and the
  instance-to-registration composite FK/worker pairing;
- exact `prosrc`/catalog and trigger-definition rejection for every API005
  generic and API006/Serve032 guard, including Serve mode `tgtype=23`,
  Serve downgrade state `tgtype=31`,
  executor registration `tgtype=31`, action `tgtype=19`, Serve projection
  `tgtype=31`, four shadow/request `tgtype=23` values, queue `tgtype=31`,
  disabled/duplicate/lookalike triggers,
  changed search path, and changed protected body. INSERT with
  shadow/authoritative mode or a non-null parity timestamp must fail regardless
  of application/fleet defaults. Direct downgrade-state SQL rejects a
  same-state/reverse/combined write, caller or future authority time, evidence
  replacement/clear, removal before the exact reverse-order
  API006/Serve032/API005 head-and-absence predicates, a wrong seven-state or
  six-edge classifier transition, and any copied GUC without the current-backend
  exclusive lock;
- API005 verifier rejection when the request quiescence column is missing,
  non-JSONB, non-nullable, defaulted/generated, lacks or widens its
  uncorrelated-null/version/size checks, or is omitted from the exact append-
  once request-guard body; independently corrupt the executor-registration-ID
  column's UUID type, nullability/default/generation, composite FK, and
  downgrade absence, plus every ledger JSONB/type/size/state CHECK;
- direct-SQL mode tests for every same/forward/backward/skipped edge, null/empty
  service hash, hash-mismatched GUC, blocker clear, zero-blocker start, clean
  activity that preserves the timestamp, premature promotion, and the exact
  trigger-owned database timestamps;
- runtime-capability tests for wrong JSON type, count/token bound, duplicate,
  unsorted, and stale instance rows, plus missing/malformed/expired or
  target/hash-mismatched executor registration/capability rows. Direct SQL
  covers every VERIFYING/READY/CONSUMED/EXPIRED/RELEASED edge, purpose swap,
  nonce/challenge/capability rewrite, every partial/combined/reordered/replayed
  post-wait-challenge, barrier-triple, and post-barrier-challenge fill,
  challenge-before-deadline and challenge-after-deadline cases, pre-wait or
  pre-barrier response replay under a later challenge, client-supplied authority timestamps,
  consumption/release-evidence clear/reuse, delete, mismatched
  instance/worker pair, instance-row delete/re-upsert, and same-Pod process
  restart. Premature `nonce_timeout`, `capability_timeout`, and every
  `RELEASED` edge before the SQL bound fail. Release tests cover strong Pod-UID
  absence with both a retained draining/no-capability instance and a GC'd
  instance; absence without that proof fails. They cover unconsumed,
  readiness/no-request, action effect-zero-only/no-window,
  definitive-terminal with null quiescence, and ambiguous-terminal with exact
  quiescence, and reject every cross-case shape. All forbidden writes fail and
  the consumed/released tombstone remains.
  Activation must reject zero selected readiness-executor rows, a reused
  readiness executor, and all malformed rows even though only the stated
  structural bounds are CHECK-enforced; a later action must claim through a
  newly registered one-request executor;
- executor-capability registration tests prove the database issues the
  registration nonce and both raw challenge preimages once, persists only
  their hashes, owns nonce/challenge/barrier/cache/
  registered/verified/consumed/expired/released/evidence times, and rejects
  future-dated or replayed client evidence. TokenReview coverage binds exact
  issuer, subject, audience array, username, user UID, groups/extras hashes,
  Pod UID, iat/exp, token/claims hashes, and every response/audit marker to both
  challenges; the final token hash must equal every guarded backend barrier's
  authentication token hash. Any partial projection or credential substitution fails.
  Abandoned VERIFYING recovery races challenge issuance under the row lock,
  expires no earlier than 300 seconds and retains the 1,080-second conservative
  release bound, drains the Pod, and cannot release without strong absence;
- admission-freeze conformance covers both exact modes. Disabled mode requires
  zero pre-freeze wait, `snapshot_not_before=opened_at`, null guard, a post-
  commit SQL challenge, SQL-owned empty barrier record/cache time, a fresh
  post-barrier challenge, and an empty backend-rejection vector. Guarded mode
  requires the SQL-owned literal 120-second wait and rejects any live
  request-timeout-plus-propagation sum above it or any caller-selected bound.
  It admits a protected configuration write just before freeze and proves the
  SQL wait observes its final state; simultaneous and post-freeze writes are
  rejected on every backend, including name-empty DeleteCollection. A
  pre-freeze DeleteCollection that outlives `requestTimeout` must remain in the
  sealed backend epoch: finalization waits for its completion, records the
  database drain time, waits the additional 60-second cache bound, issues a
  fresh challenge, and observes the final snapshot, or misses the nonce
  deadline and stays shadow; the
  120-second timer alone never certifies it. It fails closed on a missing,
  mismatched, or prematurely acknowledged backend barrier, guard DB timeout/error,
  missing freeze row, stale-readable replica, pool routing, primary-role or
  database-identity mismatch, a missing/different backend guard, mutation of a
  guard/sentinel/dependency, or a rejection without the fresh nonce-and-two-challenge
  audit marker. The fixed preprovisioned sentinel is MostRecent-GET and
  spec/UID/resourceVersion matched before its matching-precondition
  `dryRun=All` DELETE; exact resourceName RBAC lets it reach the guard, an
  authorization-layer 403 does not pass, and both guard-present and
  guard-absent paths leave the sentinel byte-equal. Complete request-chain
  tests reject an external authenticator/authorizer/audit/KMS callback,
  aggregated obligation API, reloadable config, unallowlisted static plugin,
  ordinary webhook/policy pretending to be the guard, or any applicable
  mutator—including one that rewrites both object name and protected label.
  A managed-EKS target without the exact disabled-engine or immutable-static-
  guard/primary-evidence branch must remain shadow and cannot satisfy M3;
- exceptional downgrade serialization with concurrent action insert, request
  correlation, replica-incarnation write, and mode advance. Each writer must
  either commit before the locked gate and make downgrade refuse, or block and
  fail capability validation after downgrade; no partial row/schema is legal.
  Subprocess tests cover the exact maintenance-module confirmation literal,
  fresh-process/write-quiesce and empty-history refusals, exact linked-request
  queue/event detection, preservation of unrelated requests/deliveries/events,
  rejection of raw or reversed down migration, and normal Helm rollback
  retaining both heads. Once the draining row commits, every startup/token/
  registration/challenge/finalization/promotion/claim/window/action/shadow/
  correlated-request writer must fail closed, including after process restart.
  The stored process-quiesce object must cover every API/controller/executor
  owner, remain live byte-equal in every nonfinal state, and prevent an old or
  pre-M1 mutator process from restarting between maintenance invocations;
- guard-teardown tests use a complete immutable installation inventory whose
  entries include an active guard that never inserted a registration, an
  expired pre-capability registration, and a normal released registration.
  Missing/duplicate/unlisted entries, ledger-only target derivation,
  wrong central-DB identity, a mixed guarded/guard-free backend set, Service-
  only enumeration, stale/readable-primary evidence, an in-band
  StatefulSet/DaemonSet/Deployment/operator owner, or a StaticPodOwner without
  the durable out-of-band rollout pin must block. A positive test commits
  DRAINING, replaces an exact StaticPod backend without a Kubernetes API
  owner/template/configuration mutation, permits only the inventory-matching kubelet/control-plane
  mirror-Pod and EndpointSlice churn, and proves an unrelated protected write
  is denied by the target maintenance quiesce. The disabled fill requires two complete
  sequential MostRecent EndpointSlice snapshots, one shared SQL `verified_at`,
  a gap-free watch, new allowlisted
  `resource_action_authority=disabled` backends, and exact absence of every old
  guarded process. An old backend rejoin, pin/revision change, caller/future
  time, a backend whose authority type is falsely represented by
  `KubernetesApiServerBackendV1`, or any remaining ledger reader blocks every
  down step. Missing ledger/table remains fail-closed inside a stale guard;
- injected failures before and after each of the six edges must classify
  exactly as `INITIAL`, `GUARD_DRAINING`, `GUARD_DISABLED`,
  `AFTER_API006_DOWN`, `AFTER_SERVE_DOWN`, `AFTER_API005_DOWN`, and `FINAL`.
  In particular, post-DRAINING is `GUARD_DRAINING`, post-disable-evidence is
  `GUARD_DISABLED`, and post-API005-down retains the fence and is
  `AFTER_API005_DOWN`; only the guarded final fence deletion/drop reaches
  `FINAL`. Each nonfinal rerun revalidates the durable target pins and complete
  live backend set. `AFTER_SERVE_DOWN` must verify API005+Serve031 and exact
  absence of API006/Serve032 before API005 removal. `FINAL` performs no DDL and
  succeeds idempotently. Every other revision/structure/phase pair rejects
  before DDL.
  Serve032 down cannot run before API006 down, and API005 down cannot run
  before Serve032 down; final artifact removal cannot run
  before the live scan/postverifiers, and neither down path can race an API
  upgrade;
- canonical-domain vectors for booleans-as-integers, signed-64-bit boundaries,
  floats, decimals, exponent-form input, non-finite input, U+0000, surrogates,
  non-NFC text, eight-level/4,096-node and field array/string bounds, exact byte
  boundaries, unknown keys, and unknown positive versions. Application
  canonical bytes/hash must
  equal the PostgreSQL query and CHECK result for every accepted vector;
- exact accelerator quantity vectors (integer and binary64 hex, including
  fractional values), every direct/location override presence permutation,
  all ten location fields, full exact-card/card-shape logical targets, immutable
  launch/down decision provenance and retirement copies, caller-session
  `READY` image publication/artifact/source locking, immutable container
  root identity with nullable runtime for direct ref-only selectors, locked
  root/platform/child lineage for managed ref-only pulls, multi-architecture
  platform mismatch, artifact/digest selection across retry, terminal
  version-row dereference/name reuse, initial preallocated `new_cluster_hash`
  absent/same/different insert behavior, and cluster-record UUID threading and
  fencing across replica/service name reuse;
- exact `KubernetesProviderLocatorV1`/`KubernetesObservationSetV1` tests for
  PREPARED-before-I/O callback bypass; immutable labels on every obligated
  object; a sorted/unique exhaustive named-target inventory and immutable set
  hash; exact `resourceVersion=""` MostRecent LIST and exact-name GET reads;
  full pagination and scope identity; count/snapshot bounds; HTTP 410 and
  credential uncertainty; UID-precondition deletion; and two complete absence
  sweeps for every locator.
  Reject `resourceVersion=0`, `Any`, `NotOlderThan`, cache-only reads, a
  changed/missing collection RV on a later page, a stale empty cache response,
  incomplete continuation, and reuse of a pre-410 page. One- and 32-locator
  vectors must prove
  ordered one-to-one coverage, exact locator/set hashes, exact if-and-only-if
  `current_prepared` indexes, byte-equal cross-attempt `retained_locator`
  references, and `uncorrelated_locator` shadow/backfill entries. A claim-
  before-PREPARED crash on attempt two with locator A retained from attempt one
  must construct and validate the complete A set without mutating request two.
  Reject a subset, duplicate, reorder, last-locator shortcut, retained-array
  mismatch, cross-row prior-round link, malformed round ordering, or
  per-locator partial refresh; prove
  single-live-location launch adoption and all-locator absence for down. A
  same-name object with a missing/different protected label must be the exact
  `present_mismatched_label` uncertainty result, never absence or adoption.
  Every matching exact-name GET must agree byte-for-byte with its LIST entry;
  name change, delete/recreate, label stripping, a target omitted from the
  inventory, and a LIST/GET race each invalidate the round or remain uncertain.
  Concurrent name-plus-label mutation cannot evade both surfaces or produce an
  empty proof.
  A
  delete/UID acknowledgement plus one empty sweep must remain uncertain, and a
  down handler acknowledgement must never produce deleted certainty or
  success. Crash between the two rounds, a round-one start before settlement,
  and a stale final CAS after attempt/action revision, frozen refs, or locator
  hash changes must reject the set; restart performs two wholly fresh rounds;
- boundary vectors for 32 minimum-size locators, aggregate expected-node,
  port, and 1,024 named-target limits, the 24,576-byte locator-array limit, the complete duplicated
  terminal down-spec 65,536-byte limit, a complete two-round 32-locator
  observation set, and the 2,048 normalized-effect/16,777,216-byte boundary
  with every predeclared/completed-or-skipped call slot. Exact-limit
  values pass; each
  independently over-limit count or canonical byte value rejects before
  action/sample creation or I/O, with no truncation;
- `pod_cluster_v1` bootstrap tests prove a pre-existing Namespace UID,
  pre-existing non-default ServiceAccount UID, every rendered Pod's exact
  service account, FUSE false, and a verification-only plan with no Namespace,
  ServiceAccount, Role, RoleBinding, ClusterRole, ClusterRoleBinding, or
  DaemonSet write. Missing/default prerequisites, FUSE, system-namespace
  creation, deployment/PVC, SSH node-pool, ephemeral-volume, custom plugin,
  protected-label override, extra object kind, or shared-resource mutation must
  reject shadow parity/authoritative activation before PREPARED. The accepted
  renderer mutates only labeled Pods, Services, Ingresses, and the per-cluster
  ray-ports ConfigMap and observes that exhaustive set;
- executor/quiescence fault tests stop at claim-before-child-spawn,
  child-spawn-before-PID publication, claim-to-PREPARED, PREPARED-to-first-call,
  window-commit-to-first-byte, accepted-byte-to-response, response-to-result
  fill, and result-to-terminalization. They
  cover a delayed mutation-window RPC consuming the pre-send
  `CLOCK_BOOTTIME` budget,
  transaction retry/lost response/nonce mismatch forbidding window reuse,
  SIGSTOP across the start deadline, an explicit all-slots-skipped
  before-transport result versus a crash after window commit with no result
  (the latter is ambiguous), accepted bytes with no response, definitive
  response with no terminal commit,
  delayed Kubernetes response, database blackhole, lease loss, capability
  expiry after window commit but before first byte, and backend, complete
  request-chain, admission-freeze, or applicable registration/policy change
  before first byte, between calls, during a
  call, and after the last response but before sealing. Expiry/change before
  the first transport entry must close every slot skipped as `no_bytes_sent`;
  every after-entry case must make the most recently entered slot and the
  window `ambiguous`, with all later slots skipped,
  SIGKILL plus exact PID/start-ticks join, PID reuse, graceful shutdown,
  executor Pod/PID-1 death, token expiry/rejection, and a stale worker attempting
  mutation after an old empty round. A capability
  expiry/hash/fingerprint/backend/admission mismatch before first transport
  entry may produce only the exact all-skipped result; after entry it prevents
  a definitive result. Every missing/ambiguous window result must prevent live
  observation. No post-terminal observation/retry may start until the
  exact frozen-ref quiescence proof and `settle_after`; every pre-settlement
  round is discarded, execution-fence-only proof maps only to not-started, and
  PREPARED-or-later requires a wholly fresh complete set. A proof published after
  terminalization must survive `VERIFYING -> RETRY_WAIT`, admission of attempt
  N+1, and a controller crash as the byte-equal immutable column on request N;
  clearing or rewriting either its source or a relying outcome copy is rejected;
- remote fail-stop tests reject delete acknowledgement, force deletion, a
  cached/unbound 404, `resourceVersion=0`/`Any`/`NotOlderThan`, node partition,
  same-name/new-UID Pod without the exact complete MostRecent proof, missing or
  mismatched old-UID evidence, TokenReview success/unknown error, early
  DB-clock expiry, token/claims/skew mismatch, a token reload/refresh path, and
  RBAC denial. Fast revocation requires UID-precondition delete, exact
  old-UID-absence evidence, and exact bound-token rejection.
  A fresh reconciler with only durable DB state must complete the
  `fixed_token_expired` branch after exact old-UID absence and
  `exp + allowlisted skew`, with no raw-token preimage. Configz backend
  enumeration, direct non-reconnecting backend binding, request-timeout range,
  exact build/response-matrix/storage contract, EndpointSlice or complete
  admission-watch invalidation, literal `sideEffects=None` versus rejected
  `NoneOnDryRun`, dry-run evidence, full-window expiry coverage,
  barrier/cache/two-challenge and capability-hash binding are
  independently falsified. Exhaustive call-matrix tests cover every listed
  verb/status/Status-reason, all 3xx/5xx, 202/204/408, truncated/malformed
  bodies, partial writes, webhook timeout, HTTP/2 reset, and every unused-slot
  order. A create/patch response is definitive only when its complete decoded
  kind/namespace/name/UID and protected-label key/value equal the predeclared
  call slot and locator; a missing/changed label or admission-renamed target is
  ambiguous even with 2xx. Only an exact full response is definitive and every
  unused slot is durably skipped. The conformance test delays/cancels writes and proves no
  newly matching object appears in later MostRecent LISTs after the certified
  bound. Static
  kubeconfig, user credential, remote target cluster, role=`all`, local server,
  non-role-split topology, shared/reused executor, detached provider-I/O child,
  missing/unreadable/inconsistent/exceeded-60-second provider configuration, or
  missing startup RBAC
  rejects capability advertisement and authoritative promotion;
- exact scope vectors proving that native actions require incarnation scope
  equal to service hash for newly created scoped services, scoped legacy
  backfill retains that same value, both native and backfilled actions on a
  retained null-scoped service preserve `{kind: legacy, value: null}`, non-null
  mismatches fail closed, and default/non-default workspace remains independent
  and nonempty in every case;
- exhaustive action-state × action-type × compositional-outcome matrix tests,
  including `success`, every reason code, immutable attempt classification plus
  later provider absence, missing/ambiguous window result, forbidden down
  terminal failure, and primary attempt-one observation success;
- concurrent action upsert and concurrent idempotent request admission;
- exhaustive correlated-request direct-SQL tests for every initial output,
  claim, cancellation, retry, user, event-context, codec/handler/name, queue,
  and timestamp column; every legal matrix edge; generation 2/reclaim/token
  replacement; heartbeat regression; ref rewrite/reorder/truncate, valid
  one-result fill, missing/duplicate/reordered call slot, illegal skipped/
  definitive/ambiguous ordering, second/malformed/combined append-and-fill
  rejection; action-
  request-delivery cross-check; queued/running cancellation; submit failure,
  `BrokenProcessPool`, `ExecutionRetryableError`, lease expiry, shutdown,
  supersession, post-terminal cancel acknowledgement, append-once quiescence
  publication/survival, and event-last rollback;
- registry/codec tests proving exactly three dedicated handler names with no
  aliases, exact seven-key payloads without `RequestBody` environment/config,
  existing six-key `EventContext`, and the two operational-event mappings.
  The launch handler must call `sky.execution.launch` directly exactly once;
  monkeypatched SDK/client/HTTP/`schedule_request` entry points fail the test,
  and no second request ID/claim/lease/effect owner may appear;
- shadow tests for every context-axis combination and all three attempt drivers:
  one physical connection and rollback across ordinary, reserved-fill,
  paid-claim, full-service, and consolidated-pool producers; recovery reuse of
  the same sample/request; defer-to-idle and cleanup ticks under one retirement
  sample; multiple provider calls in one launch request; normal direct
  `core.down`; launch compensation as a launch child; safe deletion of only the
  exact never-admitted launch shape; provider-absent observation-only down;
  lost admission/provider response; stream retry; crash/deletion;
  uncertainty-blocked succession; unsafe replay; the 32-mutation bound with
  refreshable observation; terminal-parent child insertion rejection; pre-M1
  attempt-zero recovery; request input/terminal hash rewrite; resolved
  `DIVERGED` history; recurrence episodes; activity during a clean window;
  final locked promotion audit; and exact divergence/operation/sample/request
  retention order;
- zero-locator retirement tests cover native attempt zero, an exact current
  generation-zero queued/unclaimed request, every prior unclaimed-terminal
  attempt, current versus prior claimed `not_submitted` quiescence (only the
  former must remain in the outcome), shadow never-admitted plus
  generation-zero queued and terminal request proofs, and
  full/failed-service cleanup. They assert one atomic replica/capacity/usage
  closure and exact event/outcome proof with no down spec/action/sample,
  observation set, terminal tombstone, SDK/internal down, or provider call.
  A large pre-entry attempt history proves the PostgreSQL count/digest remains
  fixed-size and recomputes from every contiguous retained request without a
  payload-size cleanup cliff. After shadow retirement deletes its sole
  request/sample, a restart must recompute the event digest from the embedded
  exact `shadow_attempt`; a missing/mismatched preimage rejects the proof.
  A claimed shadow request, `worker_exited`, missing prior attempt, global
  cluster row, operation ID, PREPARED/window reference, nonempty locator, or
  backfill-only guess rejects. A barrier race proves PREPARED-first selects
  normal nonempty cleanup while retirement-first terminalizes the journal and
  makes the pre-call CAS fail before bytes; a second barrier proves
  claim-first makes queued retirement ineligible while retirement-first
  atomically cancels/deletes delivery and makes claim return no work;
- exhaustive `ReplicaExecutionAuthorityV1` factory tests for central
  PostgreSQL legacy, shadow, and complete-authoritative modes; consolidated
  pools; remote non-consolidated pools; and local SQLite. Unsupported/mixed,
  stale owner/mode/topology/terminal-parent objects and illegal central/remote
  connection combinations fail closed. Live versus exact shadow-sample/action
  tombstone cases cover both token shapes, null/all-or-none terminal projection,
  parent kind/ID, replica/incarnation, intent ID, and snapshot hash; central
  legacy cleanup retains its service authority. Promotion must quiesce and destroy the legacy or
  shadow adapter before committing/constructing the durable manager; crash
  recovery after that commit can construct only the durable manager. Full-
  service, failed-service, exact zero-locator retirement, and exact
  v1-tombstoned orphan teardown must dispatch through the same matrix,
  including crash recovery with absent service/null owner. A pre-M1 unscoped
  orphan remains quarantined/manual-only, and retained
  adapters must never be importable as an authoritative fallback;
- M4/M5a tests prove only the authoritative manager loses thread ownership and
  legacy status reads/projections. Central legacy/shadow, local SQLite, remote
  non-consolidated-pool, and orphan-v1 fixtures continue to decode and update
  the retained fields/column/codecs through their named adapters; no test or
  fleet query claims the later global M5b deletion;
- request-lease expiry and stale claim-token rejection;
- atomic request terminal status, queue deletion, action outcome, and
  operational-event commit/rollback;
- owner-token ABA, parent handoff CAS, child respawn retention, stale-token
  rejection, admin-token overlap, middleware stripping/log redaction, lost-
  response reauthorization, and live-service versus action/sample terminal-
  tombstone admission across same-name service reuse;
- controller leadership handoff;
- launch reconstruction tests for replica-ID environment, exact TLS binding
  fingerprint/rotation failure, service security-group transform, and rejection
  of every additional `Resources.copy()` mutation;
- launch/down state-machine unit tests;
- retry timing with the database clock;
- existing Serve autoscaling, rolling, logical-replica, reserved-capacity,
  load-balancer, and cleanup regression suites;
- Helm migration ordering, first-M1 no-`--atomic` rendering, compatible-image
  normal rollback with retained schemas, and mixed-version rendering. A
  pre-M1 image is accepted only at the exact all-legacy/no-shadow-authority/
  provider-quiesced gate; a shadow service or retained shadow tombstone forces
  an M1-compatible image whose journal/pre-call hooks remain active; and
- the live HA conformance harness extended with resource-action evidence.

## Observability

Existing Datadog and operational-event paths remain authoritative. Add bounded
metrics and structured events for:

- actions by kind and state;
- action age;
- attempt count;
- time in `VERIFYING` and `RETRY_WAIT`;
- controller adoption after handoff;
- idempotent admission hits;
- stale action/result write rejection;
- cleanup uncertainty and deletion proof; and
- action/request/replica divergence.

No provider credentials, raw exception payloads, request bodies, or service
secrets may be included.

## Legacy Code Removal Map

For the eligible authoritative manager, these paths must be deleted, not merely
bypassed, when their gate closes. Code explicitly assigned to a named retained
adapter by the factory matrix is an out-of-scope execution boundary, not stale
authoritative ownership.

### Execution factory and retained legacy adapters

The parent service process selects execution through one closed, mode-fenced
factory input; a child never infers consolidation, database topology, or
authority from its spec:

```text
ReplicaExecutionAuthorityV1 = {
  version: 1,
  state_backend: "central_postgresql" | "remote_controller" | "sqlite",
  topology: "central_non_pool" | "central_consolidated_pool" |
            "remote_nonconsolidated_pool" | "local",
  mode: "legacy" | "shadow" | "authoritative",
  authority_kind: "live_service" | "terminal_tombstone",
  candidate_capability: "complete_authoritative_v1" |
                        "unsupported_or_mixed",
  service_name: Text[256],
  service_hash: Text[256],
  service_lifecycle_epoch: NonnegativeI64,
  owner_token: null | canonical lowercase UUID text,
  terminal_authority: null | {
    parent_kind: "shadow_sample" | "resource_action",
    parent_id: canonical lowercase UUID text,
    replica_id: NonnegativeI64,
    replica_incarnation_id: canonical lowercase UUID text,
    terminal_intent_id: canonical lowercase UUID text,
    terminal_snapshot_sha256: Sha256
  },
  central_caller_connection: boolean
}
```

The parent derives this object while holding the exact service/hash/owner lock
and explicit topology handle, or from the already locked immutable terminal
action/sample plus its byte-equal replica tombstone. Every admission re-locks
that same authority source and compares all fields; a
mode/topology/owner/parent mismatch fail-stops the stale object. Cross-field
combinations are closed: `central_postgresql` requires one of the two central
topologies. Its `live_service` authority requires a non-null owner token;
`terminal_tombstone` requires a null token and is allowed only for `shadow`
with the exact retained sample or `authoritative` with the exact retained
action. Live authority requires `terminal_authority=null`. Tombstone authority
requires the complete projection: parent and terminal-intent IDs are equal;
replica identity and canonical terminal-snapshot hash equal the retained
replica row and parent spec/sample; `shadow` requires
`parent_kind=shadow_sample`, while `authoritative` requires
`parent_kind=resource_action` and a down action whose action ID is that parent
ID. The parent re-locks authoritative `replica -> Serve projection -> action` or shadow
`replica -> sample` in the declared order and recomputes the snapshot hash
before construction and every admission. Central legacy retains its service
row through adapter-owned cleanup and therefore cannot construct a terminal
tombstone. `remote_controller`
requires `remote_nonconsolidated_pool`; `sqlite` requires `local`; both require
`authority_kind=live_service` and null terminal authority. Remote and local objects require
`central_caller_connection=false`, `mode=legacy`, null central owner token, and
`candidate_capability=unsupported_or_mixed`. A central legacy object requires
`central_caller_connection=false`; central shadow and authoritative objects
require it true. Any other combination is rejected before adapter
construction. The factory matrix is exhaustive:

- central PostgreSQL plus `legacy` constructs
  `CentralLegacyReplicaExecutionAdapter`; its existing threads, request IDs,
  retry clocks, and v1 replica status fields remain authoritative and it writes
  no action/shadow row;
- central PostgreSQL plus `shadow` constructs
  `CentralShadowReplicaExecutionAdapter`; the same legacy execution owns
  mutation, but every eligible decision/callback must use the mandatory
  one-connection shadow journal. An unsupported/mixed choice records
  `provider_capability_unavailable` before unchanged legacy execution;
- central PostgreSQL plus `authoritative` constructs only
  `DurableResourceActionReplicaManager`, requires a caller-owned central
  connection and `complete_authoritative_v1`, and contains no legacy thread map
  or fallback. Unsupported/mixed construction is an invariant failure;
- `remote_nonconsolidated_pool` constructs only
  `NonConsolidatedPoolLegacyReplicaAdapter`; that topology is the explicit pool
  discriminator and requires `state_backend=remote_controller`, no central
  caller connection, and `mode=legacy`; and
- SQLite is legacy-only and uses a named `LocalLegacyReplicaExecutionAdapter`;
  it requires topology `local` and rejects shadow/authoritative and central
  action calls.

Consolidated pools use the central matrix. Every new central service starts in
database-guarded `legacy`; any later `shadow` audit mode requires the explicit
guarded transition, never a constructor/fleet/INSERT default. An authoritative
update that adds any unsupported candidate is rejected rather than filtering,
downgrading, or retaining a hidden thread path. During promotion the parent
quiesces every legacy thread and sample, destroys the legacy/shadow adapter,
advances the locked mode, commits, and constructs the durable manager. A crash
after the mode commit is recovered by constructing only the durable manager;
no live legacy object may survive authoritative promotion.

M4 extracts the minimum process-local launch/down/status/retry implementation
from the generic replica manager into these four named retained legacy
adapters. Full-service, failed-service, and exact v1-tombstoned orphan teardown
dispatch by the same locked authority object: legacy uses only live-service
authority and its retained adapter; shadow uses live or exact sample-tombstone
authority and its shadow adapter; authoritative uses live or exact action-
tombstone authority and terminal durable actions. Before any tombstone
construction, an exact empty-locator replica dispatches the live-service
zero-locator transaction and constructs no terminal authority. A pre-M1 orphan
without the exact hash/incarnation/tombstone prerequisites follows only the
quarantine/manual-cleanup rule above; a reusable service name cannot construct
authority. The retained adapters are not a dormant fallback for an eligible
authoritative service. Removing them requires later provider coverage and, for
remote pools, either centralization or a reviewed cross-database protocol.

### Provisional untyped action metadata — remove before M1 merges

- Remove `provider_inputs` and every arbitrary-dictionary parser or producer
  for action specs. Launch and down must emit only their exact v1 schemas.
- Remove parsers that accept any positive spec, admission-envelope, outcome,
  or effect-envelope version; dispatch must reject everything except exact
  v1 until a reviewed v2 exists.
- Remove Python-only compact-JSON hashing/sizing for persisted action values;
  use the PostgreSQL-owned normalization result and matching CHECKs.
- Remove outcome writes without exact evidence and provider-details records,
  and remove any implicit “success” encoded as a failure kind.

These are correction gates, not compatibility paths, and receive no rollout
window.

### Process-local launch ownership — remove from authoritative manager in M4

From the generic/authoritative path in `sky/serve/replica_managers.py`:

- `_launch_thread_pool` and every snapshot, membership, pop, and admission
  branch that treats it as durable ownership;
- `_replica_to_request_id`;
- `_replica_to_launch_cancelled` after cancellation is action/request based;
- `_replica_to_logical_launch_fence` after the immutable action spec and
  pre-provider validation carry the same fence;
- `SafeThread` construction in `_launch_replica()`;
- the `replica_to_request_id` and `replica_to_launch_cancelled` parameters of
  `launch_cluster()`;
- `_stream_with_owner_watchdog()` and map-based
  `_cancel_request_for_ownership_loss()`;
- all three replica-intent persistence variants around reserved-capacity fill,
  paid-capacity claim, and ordinary `_persist_replica()` after they accept the
  caller-owned action transaction;
- launch-pool installation of an unstarted thread after replica persistence;
- thread-local launch retry counters, `Backoff`, monotonic sleep loop, and
  cleanup-before-retry loop after durable attempt scheduling owns them;
- the launch-thread completion/admission sections of
  `_refresh_thread_pool()`; and
- the TODO that asks to persist launch/down request IDs.

Equivalent thread/status mechanics needed by the named legacy/shadow adapters
move behind their construction fence and retain dedicated regression tests; the
durable manager cannot import or instantiate them. The provider launch
implementation, task construction, security-group scoping,
placement policy, capacity classification, and service-owner precondition
remain. `ServiceReplicaLaunchPrecondition`'s PID/IP ownership form is removed;
its service hash/status checks move into the correlated action fence together
with replica incarnation and current attempt.

### Process-local down ownership — remove from authoritative manager in M4

From `sky/serve/replica_managers.py`:

- `_down_thread_pool` and `_MAX_CONCURRENT_DOWNS_PER_SERVICE` thread counting;
- `SafeThread` construction in `_terminate_replica()`;
- the down-thread completion/admission sections of `_refresh_thread_pool()`;
- special recovery for a committed database write followed by
  `Thread.start()` failure;
- in-thread drain sleep/polling after drain eligibility is action based;
- `terminate_cluster()`'s controller-local retry counter, `Backoff`, and sleep
  loop after each provider attempt is an immutable request; and
- `_thread_pool_refresher()` when no remaining responsibility uses it.

The named legacy/shadow adapters retain their fenced down owner and retry
mechanics. The fail-closed provider deletion check, load-balancer idle proof,
durable drain start, lifecycle fences, and provider cleanup implementation
remain.

### Full-service and ownerless teardown bypasses — remove in M4

In authoritative mode the journal owns committed per-replica retirement even
outside the steady-state replica manager. Remove or route through durable down
actions for nonempty locators or the exact zero-locator transaction:

- the `SafeThread` replica teardown pool and status polling in
  `sky/serve/service.py`'s full-service cleanup path; legacy/shadow dispatches
  that responsibility to its named adapter instead;
- the direct failed-service and orphan-service `terminate_cluster()` parallel
  loops in `sky/serve/serve_utils.py`; legacy/shadow dispatches them to its
  named adapter;
- any full-service cleanup that deletes a `version_specs` row while a
  nonterminal launch action still requires it. Once all referencing launches
  are terminal, purge deletes the row normally; retained action-history digests
  do not block service-name reuse;
- the request-name/cluster-name launch-quiescence scan in
  `quiesce_service_replica_launch_requests()` after action correlation provides
  an exact incarnation-scoped barrier; and
- `_recover_replica_operations()` branches in
  `sky/serve/replica_managers.py` whose sole purpose is reconstructing
  process-local launch/down ownership.

Service teardown remains owner-fenced and must first durably commit its
terminal intent. Ownerless reconciliation still retains rows until provider
absence is proven. Storage and external load-balancer cleanup stay outside the
replica action journal unless a later canonical design explicitly migrates
them.

### Process-local cleanup scheduling — remove from authoritative path in M3/M4

From `sky/serve/replica_managers.py`:

- `_failed_cleanup_retry_attempts`;
- `_failed_cleanup_retry_at`;
- `_failed_cleanup_retry_state()`;
- `_clear_failed_cleanup_retry()`;
- `_schedule_failed_cleanup_retry()`; and
- monotonic-clock eligibility in `_reconcile_failed_cleanup()`.

The authoritative replacement reads `attempt`, `next_attempt_at`, and outcome
from the durable down action. Named legacy/shadow adapters retain their local
schedule until later provider migration. Infinite fail-closed retry remains
steady-state behavior.

### Replica persisted compatibility state — authoritative-only M5a cleanup

After the M5a reader gate, the durable manager removes its own
`ReplicaStatusProperty.sky_launch_status`/`sky_down_status` reads and projection
writes plus recovery branches whose only input is legacy process status.
Shared codecs, Python fields, SQLAlchemy metadata, the physical
`replicas.sky_down_status` column, and named-adapter reads/writes remain. Their
global deletion is later M5b and outside this design. Replica lifecycle status,
logical-retirement commitments, drain timestamps, placement attribution,
planned capacity, and failure evidence remain.

### Schema capability artifacts — retain

`serve_schema_capabilities`, its
`durable_serve_replica_actions/032/v1` marker, the Serve/API Alembic histories,
replica incarnation/terminal-intent/mode/parity fields, both Serve guards and
the normally empty downgrade-state table,
API005 generic action/effect/child/correlation/runtime-adapter schema and
API006 projection/shadow/executor ledgers and guards, plus retained
action/request/shadow/consumption history, are not
removed by M4 or normal M5a cleanup. Executor registration tombstones remain
indefinitely under normal operation. These artifacts are required for
mixed-version refusal and durable history interpretation. Only the explicitly
approved, empty-history,
all-services-legacy exceptional downgrade removes them, in API006-then-
Serve032-then-API005 order under the common exclusive
migration lock and durable target rollout pins.

### Process-local resource admission lock — conditional M4 removal

Remove `controller_utils.get_resources_lock_path()` use from authoritative
replica admission once durable active-action accounting owns that scope.
Retained adapters keep a narrowly named lock while their thread admission still
uses it; they cannot share that lock as action ownership.

### Tests — remove or rewrite with their production paths

Delete authoritative-manager tests whose only contract is manual construction
or cleanup of:

- `_launch_thread_pool`;
- `_down_thread_pool`;
- `_replica_to_request_id`;
- `_replica_to_launch_cancelled`;
- `_failed_cleanup_retry_attempts`; or
- `_failed_cleanup_retry_at`.

Replace them with tests over durable action identity, request correlation,
database retry time, ambiguous reconciliation, and stale-write rejection.
Retain equivalent named-adapter tests. Preserve tests of placement, capacity,
rolling safety, drain correctness, provider deletion proof, and service
lifecycle fencing.

### Steady-state mechanisms that must not be removed

- `api_request_queue`, request claim leases, heartbeats, and claim-token
  fencing;
- `api_controller_leadership` and controller generations for their existing
  non-Serve-planner consumers;
- `api_controller_action_reservations` for non-replayable controller requests;
- service hash, lifecycle epoch, current PID/IP owner, controller-admin token
  ring, and launch preconditions;
- the external `services.resource_scope` incarnation namespace and independent
  durable service workspace;
- ownerless cluster reconciliation from PR #1071;
- PR #1074's atomic terminal request event and audit history;
- fail-closed cleanup and provider absence proof;
- version quarantine and rolling replacement safety;
- load-balancer routing, idle, drain, cutover, and rollback fences; and
- managed-job, pool-level scheduling/capacity, and image-worker ownership
  systems outside this scope. Consolidated central-PG pool replica launch/down
  side effects are in scope; the named non-consolidated-pool adapter is the
  explicit v1 exception above.

## Rejected Alternatives

### Put another lease on each action

Rejected because request execution would then have two ownership clocks. A
joined view exposes the request lease without duplicating its authority.

### Use the request ID as the permanent action ID

Rejected because a request is one immutable attempt and terminal requests are
never reopened. A logical action must survive attempt replacement.

### Overload `api_controller_action_reservations`

Rejected because it is controller-generation scoped and currently represents
one non-replayable request. Changing its lifetime would weaken PR #1070's
handoff fence and complicate rollback.

### Store only the latest request ID in the replica JSON

Rejected as the final architecture because it does not provide atomic
idempotent admission, attempt history, due-action scans, or a stable result
contract. It is acceptable only as a short-lived shadow compatibility
projection during M1/M2.

### Automatically replay every expired launch/down request

Rejected because the provider mutation may have succeeded after the worker
lost its database lease. Reconciliation must precede another mutation.

### Keep the thread implementation permanently as an adapter

Rejected for a v1-eligible authoritative central service because logical
ownership, retry timing, and completion would remain split between database
rows and controller-process objects. Named central legacy/shadow, local SQLite,
and remote non-consolidated-pool adapters are deliberate retained boundaries
for unsupported topology/provider profiles; their factory fences prevent them
from serving an authoritative service.

## Verification Evidence

The first adversarial review of this exact design on 2026-07-30 returned
`RESHAPE`. It found the reusable-replica-ID identity bug, missing Serve 032 and
activation schema, an incomplete attempt model, a split completion boundary,
under-specified canonical admission, a direct-down bypass, unsafe mixed-version
rollback, and incomplete teardown removal coverage. Those findings were
incorporated in place. A second review found pool/shared-manager and
attempt-zero backfill contradictions plus a missing lost-provider-response
fault point; those were also resolved in place. The third review returned
`ACCEPT` for content SHA-256
`f03f909870dcf1ebc8a8af4bc859b348d73f5e9310601059b5db2f6925cce3c2`.
The M1 implementation map then found that “bounded” provider references and
the “closed” transition set lacked executable numeric/edge contracts. This
revision added those contracts and its adversarial re-review returned `ACCEPT`
for content SHA-256
`2646ea5c2a994ad35cd2afb8767e872d9e40e69aaa4ea9a589259e05dc31ef34`.
That hash is historical and does not cover the current file. A subsequent
implementation-driven adversarial review returned blocking findings: the v1
spec was only top-level closed and accepted unknown positive versions plus a
raw secret-capable `provider_inputs` bag; Python canonicalization did not
define PostgreSQL exponent/U+0000 behavior or one shared size/hash
representation; outcomes had no `success` kind or exact evidence/reason and
state/action certainty matrix; and API 005 inferred Serve 032 from lookalike
columns without a capability marker or a serialized cross-schema
upgrade/downgrade protocol. This revision replaces those contracts with exact
recursive launch/down schemas, PostgreSQL-owned canonicalization, an exhaustive
typed outcome/evidence matrix, and a real Serve marker plus common advisory
lock, structural verifier, reverse-order downgrade, and race tests. The
implementation audit then found that the first exact schema incorrectly treated
`services.resource_scope` as a global/workspace selector. This revision models
its actual external incarnation namespace losslessly (`service_hash` for scoped
rows, null for retained legacy rows), keeps the independently resolved
workspace nonempty, and adds native/backfill cross-product tests. Later
implementation audits added exact image root/runtime identity, initial cluster
hash insertion, Kubernetes-only provider locators and observation, owner-token
authority, one-connection shadow admission, terminal action/sample tombstones,
and the exceptional downgrade entry point. A fresh exact-file review of
historical content SHA-256
`4302ef93046fcdbedab040bb1ac62818df907b14d3008c76745e448a594e60c0`
returned `RESHAPE`: it found singular observation evidence despite retained
multi-locator state, no deletion proof for down acknowledgement, no exact
claim-to-PREPARED stale-executor quiescence, an incomplete
`pod_cluster_v1` bootstrap surface, M4/M5 deletion that stranded retained
legacy topologies, and a self-referential evidence protocol. This revision
replaces those contracts with full ordered observation sets and
provider-observation-only down success; exact disposable-executor, Pod-token
revocation, precommitted mutation-window, provider-side settlement, and
quiescence rules; verification-only
preprovisioned bootstrap; a closed construction-fenced adapter matrix plus
authoritative-only M5a; and the append-only descendant evidence protocol below.
The next independent exact-file review of historical content SHA-256
`4be8adf2ef392242d3d466c062bd247c843d8cd0dcbc6b623faca4d4003fccd1`
returned `RESHAPE`. It found that the correlated launch handler created a
nested SDK request; empty pre-PREPARED replicas had no legal cleanup; handler
acknowledgements could claim locator state; the predecessor's call-permit
clocks, ambiguous live
observation, and settlement certification were not executable; INSERT/default,
canary, and capability paths bypassed activation; M2 claimed authority over
nonexistent shadow actions; M1 rollback could mutate without its journal; and
the downgrade resume protocol omitted the final committed pair. The correction
pass also made the two-round absence evidence self-contained and split immutable
attempt classification from refreshable provider state so later absence cannot
erase why an attempt failed. This revision closes those contracts with direct
in-process handlers, exact zero-locator retirement, precommitted mutation
windows and live provider-side capability binding, guarded legacy INSERT,
ordinary audited readiness samples, a dark M2, and compatible rollback images.
That rejected historical draft also used a three-state downgrade classifier;
it is not a supported contract and is superseded by the exact seven-state,
six-edge classifier above.
The follow-up exact-file review of historical content SHA-256
`3177528e07fe0843657cc09519b0ee7e77e176fcc98a91c169212e5150b1b22f`
returned `RESHAPE`. It found that a window could outlive or escape its
backend/admission capability, HTTP/no-bytes classifications left unused
authority and unbounded responses, fresh HA reconciliation lacked the raw
token needed for fail-stop, absence LISTs did not bind MostRecent pagination,
claimed zero-locator quiescence contradicted the outcome null rule, and shadow
retirement deleted its proof preimage. This revision binds exact
backend/admission descriptors and gap-free watches through the full window;
predeclares and closes every call slot under a checked-in response matrix;
adds a fixed-token-expiry proof constructible from durable state; stores
MostRecent collection/page evidence; makes zero-locator quiescence conditional
on the current claimed attempt; and embeds the sole shadow-attempt preimage.
The next exact-file review of historical content SHA-256
`9b843c49dbfba228b9fc8a463efba55c5aba438b99c6d3cfd31412a6605048f6`
returned `RESHAPE` (`NOT CLEAN`). It found that dynamic admission could strip
the protected locator label or race a name/label rewrite past the observation
proof; the static admission and complete request-execution chain were not
closed; the fixed token was not fully bound into capability evidence; readiness
did not have a representable durable single-use consumption record; and
authority timestamps were caller-proposed rather than database-owned.
Correction preflight also found that release could occur early or strand the
legal definitive-window path, pre-wait evidence could be replayed after the
freeze deadline, a guard could read a lagging PostgreSQL replica, the first
live probe was not guaranteed to reach admission safely, challenge and
worst-case credential deadlines were incomplete, and unbounded Kubernetes
DeleteCollection escaped the request-timeout wait. The same preflight found
that dropping API 005 while a static guard still read its registration ledger
would strand admission fail-closed, and that ledger rows alone could not
inventory a guard installed but never registered. This revision adds
exhaustive immutable named targets and exact-name reads; the closed
request-chain plus disabled/static-freeze branches; a primary-visible
per-backend in-flight barrier with post-drain cache settlement; two staged DB
challenges and an inert dry-run
sentinel probe; the full fixed credential; the durable
registration/consumption/release ledger with SQL-owned clocks and strong Pod
absence; exact definitive/quiesced release branches; and a complete immutable
guard-installation inventory plus durable draining/disabled checkpoint,
guard-free backend rollout pins, retained process-owner quiesce, seven-state
crash resume, and last-artifact removal only after live/postverification gates.
A managed control
plane that cannot expose this strict profile remains shadow-only rather than
weakening the proof.
The 2026-07-31 reconciliation against
`docs/designs/provider-lifecycle-actuation.md` at exact upstream commit
`7aaa99041065a57c6f733ceed04f025520bac871` invalidates every historical
acceptance hash for current bytes. It replaces the provisional Serve-shaped
request array/action lease with API005's reusable generic kernel, normalized
effects and sole PR #1070 request lease; splits Serve state into API006's 1:1
projection; adds the two-stage no-I/O claim-authority seam and
registration-first universal lock order; makes correlated terminalization
atomically enter generic `REDUCING`; and fixes migration order to
`API005 -> Serve032 -> API006` with exceptional reverse removal.
These current exact bytes require a fresh independent adversarial review. Its
verdict and reviewed pre-commit content SHA-256 must be recorded in the carrying
commit/PR metadata, not in this self-hashing file.

Contract-byte and evidence-only changes are different. Any edit outside the
sentinel-delimited ledger below, any edit/deletion/reordering of an existing
ledger entry, or any change to these rules is a contract correction: it requires
a new exact full-file SHA-256 and independent adversarial review. After
`ACCEPT`, an evidence-only descendant commit may only append immediately before
the end sentinel. Review tooling must prove that the append commit's parent
contains the exact accepted full-file bytes identified by the reviewed content
hash, or is an unbroken evidence-only descendant of that commit, and that the
new diff changes no byte outside one append before the end sentinel. Such an
append changes the file hash but not the accepted contract bytes.

An evidence entry records exact commands, counts, subject implementation commit,
image digest, Helm revision, fault-injection result, and cleanup result. The
subject commit must already be an ancestor of the evidence commit. A commit
never records its own SHA: the evidence-append commit SHA is recorded only in a
later descendant entry or in immutable PR/merge metadata. A test name or green
CI link is insufficient unless its assertions cover the stated invariant.

<!-- BEGIN RESOURCE-ACTION EVIDENCE-ONLY APPEND LEDGER -->
<!-- END RESOURCE-ACTION EVIDENCE-ONLY APPEND LEDGER -->

## Closed Gates

- Historical M0 architecture and earlier bounded-transition corrections were
  accepted on 2026-07-30; their hashes are recorded above.

## Open Gates

- Adversarial re-review accepts these exact corrected bytes, and the reviewed
  pre-commit content SHA-256 is recorded in the carrying commit/PR metadata
  before M1 implementation merges.
- M1 implementation is reconciled to the exact v1 schemas, PostgreSQL
  canonicalizer/CHECKs, compositional outcome matrix, mutation-window/
  call-slot/response matrix, complete request-execution-chain and admission
  freeze with primary visibility, in-flight barrier, post-drain cache wait,
  two DB challenges, inert sentinel probe, and guarded release; fixed-token
  expiry, exact MostRecent Pod-UID absence, named-target inventory/GETs,
  durable single-use executor-consumption ledger and readiness activation,
  MostRecent provider observation, settlement capabilities,
  zero-locator proof, capability marker, complete static-guard installation
  inventory, downgrade-only guard-free backend allowlist, durable seven-state
  guard teardown with retained process-owner quiesce, and shared migration
  protocol in this revision, including independent API005 verification without
  any Serve catalog and exact `005 -> Serve032 -> 006` / reverse-order tests.
- Access to the isolated `boltz-test/skypilot-ha/skypilot-ha` deployment target
  and its immutable image publication path is verified.
- The exact Kubernetes build/response-matrix allowlist and provider-side
  settlement, MostRecent, authentication-skew, backend-pinning, complete
  request-chain, static admission-freeze/disabled-engine, central-primary
  visibility, per-backend named/collection-delete barrier, post-drain cache,
  sentinel probe, admission-watch conformance, and, for a static guard, the
  complete out-of-band `StaticPodOwner` install/downgrade inventory evidence
  are populated for
  that target; any
  empty/unverified facet keeps authoritative activation closed.
- Live preflight determines whether the managed `boltz-test` EKS control plane
  exposes an exact eligible branch. Ordinary Helm-installed
  webhook/policy admission is not a static guard and does not qualify; if EKS
  neither disables the dynamic engines nor exposes the allowlisted immutable
  apiserver-native freeze/primary/barrier facility and complete request-chain
  evidence, the
  target remains shadow-only. Shadow evidence on that target does not satisfy
  the M3 authoritative-deployment gate; M3 needs another eligible isolated
  target or an explicitly reviewed contract change.
- M1 additive schema and shadow journal implemented and deployed.
- M2 dark recovery/retry evaluator implemented and deployed with zero action or
  correlated-request authority.
- M3 durable action, retry, and request execution authority implemented and
  deployed.
- M4 legacy process-local ownership deleted and rollback/re-upgrade accepted.
- M5a fleet query proves no authoritative-manager persisted-status reader or
  projection remains; retained named adapters and later M5b stay explicit.
- Full automated regression suite and live fault-injection matrix pass.
- The `boltz-test` shadow release and any separately selected M3 authority
  release are healthy, and each test namespace is clean.
