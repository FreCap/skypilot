# Durable SkyServe Replica Actions

Status: bounded M0 accepted after independent adversarial review; M1a inert
schema implemented and locally verified; M1b typed store pending

Last updated: 2026-07-31

Canonical owner: this file. The provider-side companion is
`docs/designs/provider-lifecycle-actuation.md`.

## Decision

There is a large payoff, but only at a narrow seam.

SkyPilot already has the generic PostgreSQL request queue, claim lease,
heartbeat, and stale-writer fencing introduced by PR #1070. SkyServe still
owns the logical launch/down operation in controller memory: SafeThreads,
`_replica_to_request_id`, `_failed_cleanup_retry_attempts`, and
`_failed_cleanup_retry_at` hold the request association, retry count, and due
time. A controller handoff can therefore preserve the nested API request while
losing the durable identity of the replica operation that caused it.

The high-leverage change is to add one durable resource-action record above
the existing request system. The action owns logical identity, attempts, the
next database-clock retry time, provider-operation identity when available,
typed result, and Serve reduction. Each attempt materializes exactly one
existing API request and queue row. The API request claim remains the only
execution lease. This is not a second generic queue.

The earlier draft coupled that change to central PostgreSQL principal
convergence, superuser bootstrap, reversible cross-schema downgrade,
maintenance readers, external rollout pins, and full-uninstall journaling.
That became a separate platform-hardening program and dominated the size and
risk before the Serve adapter existed. Those concerns are deliberately split
out. They are not prerequisites for proving the resource-action payoff.

The decision gate is therefore:

- build the bounded action kernel and one Serve launch/down adapter;
- deploy it in shadow without changing autoscaling or provider authority;
- promote only eligible services after crash/parity evidence;
- delete the replaced controller-local ownership paths; and
- generalize only after a second domain demonstrates that it can reuse the
  kernel without adding domain-specific queue or lease semantics.

If this bounded program cannot replace the named in-memory ownership paths, or
if it requires the deferred platform work to become authoritative, its claimed
“very high impact / medium effort” payoff is disproven and the rollout stops.

## Goals

- Give every admitted Serve replica launch and committed teardown a stable
  identity across API/controller restarts and leadership handoff.
- Persist the current attempt, deterministic request ID, next retry time,
  provider-operation ID when one exists, typed outcome, and final result.
- Reuse PR #1070's request queue, request claim lease, heartbeat, cancellation,
  and execution-generation fencing.
- Materialize a due action into one request with a short PostgreSQL transaction;
  never hold an action lock or action lease across provider I/O.
- Adopt a launch that completed before a lost acknowledgement when exact
  observation proves it, instead of blindly launching again.
- Treat teardown as successful only after exact absence evidence; uncertainty
  remains visible and retryable without a give-up deadline.
- Keep Serve's current autoscaling, placement, reservation, load-balancer, and
  rollout policy unchanged.
- Roll out per service through `legacy -> shadow -> authoritative`, with only
  the legacy path mutating providers in shadow.
- Delete the replaced launch/down threads, request-ID map, and cleanup retry
  clocks for the eligible authoritative path.

## Non-goals and deferred tracks

The first program does not add:

- another generic request queue, domain worker pool, or action execution lease;
- a public resource-action CLI, SDK, or YAML contract;
- new autoscaling, placement, rolling-update, or load-balancer policy;
- central PostgreSQL role/ownership convergence or new migration/runtime
  credential topology;
- reversible API005/Serve schema downgrade, maintenance-reader fleets, or
  feature-specific full uninstall;
- generic multi-effect workflows, child-action graphs, or a plugin framework;
- managed-job, image-worker, storage, pool-controller, or orphan-cleanup
  migration;
- SQLite support for central resource actions; or
- authoritative execution on provider profiles that cannot furnish the exact
  readback/absence contract in the companion design.

Application rollback keeps the additive schema and deploys a compatible image.
Schema down is outside this design. Full product uninstall uses the product's
database-generation disposal procedure, not a resource-action down migration.

## Public contract

There is no new user-facing interface. `sky serve up`, updates, autoscaling,
and down retain their behavior. Existing service and replica identifiers stay
stable. Users may see individual API request attempts as today; internally,
those requests become children of one logical action.

A handoff may delay reconciliation but must not:

- create two logical replicas for one action identity;
- forget a committed request and create an unrelated replacement;
- report teardown complete from a delete acknowledgement alone; or
- reset attempt/backoff state to process-local defaults.

## Scope and eligibility

The first authoritative cohort is deliberately narrow:

- central PostgreSQL request and Serve state on one physical database so
  replica intent and action admission share one SQL transaction;
- non-pool SkyServe replicas;
- a fully drained old controller cohort before promotion;
- provider lifecycle profile `pod_cluster_v1` from the companion design;
- an immutable replica incarnation and provider locator recorded before the
  first mutation; and
- all controller/API/executor processes on the approved action-aware image,
  at the required database heads, with the existing request-handler inventory
  showing the correlated action handler.

Non-consolidated pools, name-only pre-existing resources, mixed old/new
controller ownership, and providers lacking exact readback remain `legacy` or
`shadow`. Eligibility is explicit; it is never inferred from a version string
or table-exists check.

## Architecture

```text
Serve policy transaction
  replica desired state + capacity reservation
                 |
                 | admits one stable logical action
                 v
api_resource_actions
  READY / QUEUED / BLOCKED / TERMINAL
  current_attempt + next_attempt_at + typed result
                 |
                 | short due/materialization transaction
                 v
api_resource_action_attempts <---- api_requests / api_request_queue
  durable attempt evidence           sole claim lease + heartbeat
  survives request retention                   |
                 +--------- request handler ---+
                                   |
                              provider mutation
                                   |
                           typed result/readback
                                   |
                    one Serve reducer transaction
```

The kernel owns generic mechanics only:

- indexed discovery of due `READY` actions;
- deterministic attempt numbering and request binding;
- database-clock due time;
- correlation to request terminal state;
- a short reducer callback boundary using the caller's SQL connection; and
- retention of action/attempt evidence.

The Serve adapter owns:

- whether a launch/down action should be admitted;
- the immutable replica/provider descriptor;
- retry, observe, quarantine, compensate, or terminal classification;
- capacity and replica state transitions; and
- Serve events and compatibility projections.

The provider facet owns only bounded submission and observation semantics. It
does not choose placement, retry timing, or Serve state.

## Stable logical identity

The canonical identity is:

```text
ResourceActionIdentityV1 = {
  version: 1,
  domain: "serve",
  resource_type: "replica",
  service_hash: Text,
  service_incarnation: UUID,
  replica_id: NonnegativeInteger,
  replica_incarnation: UUID,
  desired_generation: PositiveInteger,
  action_kind: "launch" | "down"
}
```

The normalized JSON object has fixed key order, UTF-8 encoding, no floats, and
bounded strings. `action_id` is UUIDv5 of the fixed SkyPilot resource-action
namespace and those canonical bytes. The human-readable equivalent is close to
`service_hash:replica_id:desired_generation:launch`, but the UUID derivation
also includes both incarnations so deleted/recreated services or replica IDs
cannot alias prior actions.

The database enforces both primary-key uniqueness on `action_id` and uniqueness
on the complete natural identity. Re-admission is idempotent only when the
immutable spec bytes and hash are equal. A same identity with different bytes
is corruption, not an update.

## PostgreSQL schema

API-request migration 005 is additive and PostgreSQL-only. It creates two core
tables and two nullable request-correlation columns. The generic tables do not
contain Serve-specific columns; Serve validates and encodes its identity in the
bounded `resource_identity` and `immutable_spec` fields:

```text
api_resource_actions
  action_id                 UUID primary key
  domain                    TEXT not null              # serve in v1
  resource_type             TEXT not null              # replica in v1
  resource_identity         TEXT not null
  desired_generation        BIGINT not null
  action_type               TEXT not null              # launch | down in v1
  immutable_spec            JSONB not null
  immutable_spec_sha256     TEXT not null
  kernel_state              TEXT not null
  current_attempt           INTEGER not null
  next_attempt_at           TIMESTAMPTZ nullable
  last_result               JSONB nullable
  last_result_sha256        TEXT nullable
  terminal_disposition      TEXT nullable
  revision                  BIGINT not null
  created_at                TIMESTAMPTZ not null
  updated_at                TIMESTAMPTZ not null
  terminal_at               TIMESTAMPTZ nullable

api_resource_action_attempts
  action_id                 UUID not null references actions
  attempt                   INTEGER not null
  request_id                TEXT not null unique
  request_input_sha256      TEXT not null
  provider_operation_id     TEXT nullable
  mutation_boundary         TEXT not null
  typed_outcome             JSONB nullable
  typed_outcome_sha256      TEXT nullable
  request_terminal_state    TEXT nullable
  admitted_at               TIMESTAMPTZ not null
  updated_at                TIMESTAMPTZ not null
  settled_at                TIMESTAMPTZ nullable
  primary key (action_id, attempt)
  unique (action_id, attempt, request_id)
```

`api_requests.resource_action_id` and
`api_requests.resource_action_attempt` are both null for ordinary requests. A
pair-null check plus a normal composite foreign key from
`api_requests.(resource_action_id, resource_action_attempt, request_id)` to
`api_resource_action_attempts.(action_id, attempt, request_id)` validates a
correlated request. The foreign key deliberately points from the short-lived
request to the durable attempt. An attempt stores the deterministic request ID
as `TEXT` but does not reference `api_requests`, so normal terminal-request
garbage collection cannot erase action history or fail on a reverse FK.

Materialization inserts the attempt first and then the request and existing
queue row in one transaction. Before a correlated terminal request becomes
eligible for retention deletion, its terminal state, provider-operation ID,
mutation boundary, and typed outcome must be snapshotted into the attempt.
Request retention skips a correlated terminal request whose attempt is not yet
`SETTLED`.

The durable action states are:

- `READY`: due now or later according to `next_attempt_at`;
- `QUEUED`: the current attempt/request binding exists, including a terminal
  request awaiting reduction;
- `BLOCKED`: an identity conflict or quarantine requires operator-visible
  repair and no mutation request is runnable; or
- `TERMINAL`: result, disposition, and `terminal_at` are complete and
  immutable.

Ordinary PostgreSQL CHECKs enforce only row-local enum, range, null-pair, JSON
shape/size, and terminal-shape rules. Foreign keys and transactional store
methods enforce request existence and terminal-state relationships; a CHECK
constraint cannot inspect another table.

`current_attempt` starts at zero. Attempt `n` uses UUIDv5 of
`(action_id,"attempt",n)` as `request_id`. Attempt numbers are contiguous and
never reused. PostgreSQL owns all timestamps and revision increments.

Python derives UUIDv5 identities and canonical SHA-256 values. PostgreSQL
enforces lowercase SHA-256 shape and conservative JSON byte/type limits; typed
internal code revalidates the canonical preimage. M1 does not add database
crypto functions, recursive JSON walkers, or validation triggers.

## No second queue or lease

Candidate discovery is a nonlocking indexed query over
`kernel_state='READY' AND next_attempt_at <= clock_timestamp()`. Only
authoritative admission creates action rows; shadow rows are stored in the
Serve shadow table and therefore cannot appear in this query. For each
candidate, a short transaction locks the action with `FOR UPDATE SKIP LOCKED`,
revalidates its revision/state/due time, inserts or adopts the deterministic
attempt, creates or adopts the byte-equal API request, inserts its existing
queue delivery, and sets `QUEUED`.

The transaction then ends. The action has no claim token, heartbeat, or lease.
Only the existing API request worker claims execution. If two dispatchers race,
one locks/materializes; the other skips or adopts the exact committed binding.
A lost materialization response is recovered by deterministic request ID and
byte equality.

## Journal-before-I/O and typed outcomes

Before the existing high-level launch/down handler can enter provider I/O, it
must claim-fenced-write `INTENT_COMMITTED` on the correlated attempt and verify
the immutable provider plan/locator already committed by admission. If the
provider returns an operation ID, the handler claim-fenced-writes it as soon as
it is known. The ID is optional because some providers do not expose one; the
immutable requested locator and readback contract are mandatory for
authoritative eligibility.

The v1 typed outcome is:

```text
ServeReplicaActionOutcomeV1 = {
  disposition: "succeeded" | "retryable" | "uncertain" |
               "terminal_error" | "cancelled",
  certainty: "observed" | "provider_acknowledged" | "unknown",
  provider_operation_id: null | Text,
  provider_code: null | Text,
  retry_class: null | "transient" | "capacity" | "rate_limited" |
               "observation_required",
  retry_after_seconds: null | NonnegativeInteger,
  observation: null | ProviderLifecycleObservationV1,
  normalized_message: null | Text
}
```

Provider error strings are diagnostic only. The closed disposition/certainty/
retry fields authorize state transitions. Secrets, credentials, raw tracebacks,
and unbounded provider payloads are redacted before persistence.

## Terminal reduction and retry

An action-correlated `sky.launch` or `sky.down` request keeps its existing
`ReplayPolicy.NEVER` behavior. PR #1070 may complete or fence that one request,
but no terminal request is refreshed as the same action attempt. A lease loss
after the mutation boundary terminalizes the request as ambiguous. Any retry
is action attempt `n+1`, whose handler observes the frozen target before it may
mutate. The controller-action reservation table remains orthogonal: it keeps
fencing non-replayable controller-class requests, while these launch/down
requests use the normal executor class and the action-attempt correlation.

When the correlated request becomes terminal, terminalization snapshots the
claim-fenced attempt evidence. One reducer transaction then locks the current
Serve controller leadership row, action, attempt, and matching Serve
replica/capacity rows in that order. It revalidates request/attempt identity,
action revision, and the replica's current action link/teardown generation,
then does exactly one of:

- commit Serve success projection and action `TERMINAL`;
- commit a retry result, increment no attempt yet, set action `READY`, and set
  `next_attempt_at` from PostgreSQL time plus the domain-classified delay;
- commit `BLOCKED` for an identity conflict or quarantine requiring repair; or
- commit a terminal error/cancellation and the legal Serve failure projection.

The reducer borrows the caller's SQLAlchemy `Connection`; it never opens a
second transaction for Serve state. Same-state replay with byte-equal terminal
evidence is idempotent. A different outcome for a settled attempt rejects. It
does not commit a `REDUCING` state: a crash rolls the transaction back to
`QUEUED`, where another reducer can retry.

Backoff is database-clock based. Jitter, when used, is deterministic from
`(action_id,attempt)` so a process restart cannot move the deadline. Serve—not
the generic kernel—selects retry class, maximum delay, and whether observation
is required.

## Launch semantics

Launch admission freezes the chosen resource descriptor and immutable provider
locator before the action is created. The action does not rerun placement on
retry. Recovery checks, in order:

1. exact provider operation ID, if supported;
2. exact immutable provider locator and replica UUID/incarnation labels; and
3. the companion profile's complete existence/readiness observation.

An exact observed resource is adopted. After any prior attempt reached
`INTENT_COMMITTED`, a point-in-time absence result alone does not authorize a
new launch: the original request may still take effect. Automatic resubmission
requires either a stable provider-side idempotency key that makes overlapping
submissions converge, or authoritative evidence that no prior operation can
still create the target. Otherwise the action stays observation-first and
`READY` with a database-clock deadline, or becomes `BLOCKED` for an identity
conflict/quarantine. It never silently submits a duplicate launch.

## Down semantics

Down is admitted only after Serve policy has durably committed the replica's
teardown generation. Retries target the frozen locator; they do not recompute a
cluster from display name. A provider delete acknowledgement is not success.
The action becomes terminal only after the companion profile proves the exact
resource absent (or proves a different incarnation occupies the name while the
target UID is absent). Recoverable uncertainty returns to observation-first
`READY` with a database-clock deadline and retries indefinitely. `BLOCKED` is
reserved for a conflict/quarantine that requires repair; there is no cleanup
give-up deadline.

A down action may supersede an unsubmitted launch of the same replica
incarnation. Once a launch mutation boundary may have been crossed, down first
observes/reduces that attempt and then targets the resulting exact resource.
This is Serve-specific precedence through the replica's launch/down action
links and teardown generation, not a generic dependency graph. A late launch
reducer cannot project the replica READY after the teardown generation wins.

Reducer guards are action-specific and fail closed. Launch success requires a
matching authoritative `present` observation and the profile's readiness
proof. Down success requires a matching authoritative `absent` observation.
Provider acknowledgement alone terminalizes neither. Cancellation after
`INTENT_COMMITTED` remains ambiguous until observation makes a terminal
projection safe.

## Serve integration

Serve migration 032 is additive. It adds:

- `services.resource_action_mode` with `legacy` default;
- immutable service/replica incarnation fields for eligible rows;
- nullable launch/down action IDs on replicas; and
- a bounded `serve_resource_action_shadow_samples` table.

Admission uses one physical PostgreSQL connection. In authoritative mode the
transaction that changes replica/capacity intent also inserts/adopts the action
and links its ID. If either write fails, neither commits.

In shadow mode the legacy thread remains the sole mutation owner. Before every
eligible legacy launch/down enqueue, the Serve transaction creates a unique
pending shadow row containing the would-be action identity and descriptor. The
legacy request ID and actual result complete that row. Shadow never inserts
`api_resource_actions`, enqueues an action request, suppresses a legacy call, or
calls the provider twice. The evaluator compares the proposed action path with
the actual legacy path and stores only bounded divergence categories.

Shadow is complete for a service, not statistically sampled: promotion blocks
if any eligible decision in the candidate window lacks a row, remains pending,
or diverges. Retention does not delete candidate-window rows before promotion.

## Activation and mixed versions

Service mode is monotonic in the initial program:

```text
legacy -> shadow -> authoritative
```

Promotion requires:

- all old controller-capable processes drained;
- every remaining controller/API/executor running the approved image digest,
  at API005/Serve032, and exposing the registered action handler through the
  existing handler inventory;
- exact provider-profile eligibility for every live candidate;
- no unresolved shadow divergence or unsampled mutation;
- at least 24 hours and a configured minimum sample count of clean live shadow
  operation, including launch and down; and
- successful crash injection at every boundary below.

The promotion transaction rechecks those facts under the service/owner lock.
After promotion, an image that ignores authoritative action rows is forbidden.
Rollback to any pre-action-aware image is unsupported after the first
authoritative promotion: additive schema cannot stop such a binary from
running the legacy mutation path. Application rollback uses a feature-aware
compatible image and keeps the additive heads. Returning a service to legacy
is deferred until a separately reviewed drain protocol exists.

## Migration and stacked implementation

### M0: bounded canonical design

- Accept this exact file and the companion provider contract.
- Record intentional deferral of principal convergence, schema downgrade, and
  full-uninstall hardening.
- Reject scope additions that do not replace a named process-local ownership
  path in the first Serve adapter.

### M1a: inert action schema

- Add API-request 005 tables, row-local constraints, indexes, correlation
  columns, SQLAlchemy metadata, and catalog tests through the current
  PostgreSQL migration path.
- Keep every new column nullable for ordinary requests and create no action
  rows at runtime.
- Use the existing migration runner and principal model; add no triggers,
  custom migration runner, capability plane, or heartbeat-path locks.

M1a verification evidence on 2026-07-31:

- revision 005 upgrades an API004 database with an existing ordinary request,
  preserves its queue row, and leaves both correlation columns null;
- catalog and constraint tests cover the two-table boundary, FK direction,
  partial indexes, JSON shape, natural identity, request binding, schema-down
  refusal, and action/attempt survival after request deletion;
- an ordinary create/claim/finish/delete lifecycle creates no action rows; and
- the full PostgreSQL request test file passes with 47 tests. YAPF, isort,
  mypy across 795 source files, pylint, and dashboard checks also pass.

### M1b: typed store, still dark

- Add typed store methods for idempotent action admission, due discovery,
  deterministic attempt materialization, and terminal reduction.
- Before creating any runtime correlation, make request retention skip a
  correlated terminal request until its attempt is `SETTLED`; test both the
  skip and deletion after the terminal snapshot.
- Do not import Serve modules into the generic store.
- Keep dispatcher and Serve activation disabled.

### M2: Serve shadow journal

- Add Serve032 incarnation/mode/link/sample schema.
- Refactor launch/down decision construction into pure descriptor/classifier
  functions used by both legacy execution and shadow evaluation.
- Persist the legacy request association and retry decision in shadow samples.
- Preserve legacy autoscaling and provider mutation authority.

### M3: dark dispatcher and recovery

- Run due discovery/materialization against synthetic/canary actions only.
- Exercise request correlation, controller eviction, lost admission response,
  retry deadlines, and reducer idempotency.
- Run provider readback on shadow samples without submitting a second mutation.

### M4: per-service authority

- Promote only the isolated eligible cohort after the activation gates.
- Materialize real launch/down requests through PR #1070.
- Keep noneligible services on legacy/shadow without a global flag flip.

### M5: legacy ownership removal

For the eligible authoritative path, delete:

- launch/down SafeThread ownership;
- `_replica_to_request_id` as operation authority;
- `_failed_cleanup_retry_attempts` and `_failed_cleanup_retry_at`;
- monotonic/process-local cleanup scheduling; and
- restart-time inference that substitutes for a durable request/action link.

Compatibility readers may project action state into old status fields, but no
fallback branch may submit or retry an eligible authoritative mutation.

### M6: reuse decision, not automatic generalization

Evaluate one second domain, preferably orphan cleanup or image-worker
distribution. Generalize only if it reuses identity, due/materialization,
request lease, and terminal reduction without adding a domain queue, action
lease, or alternate lock order. Otherwise keep the kernel Serve-scoped.

## Deployment and rollback

The isolated test target is Kubernetes context `boltz-test`, namespace/release
`skypilot-ha/skypilot-ha`, with PostgreSQL and two API/controller replicas.
Production is not a fault-injection target.

Deployment order is:

1. build and push an immutable image digest;
2. `helm upgrade --reuse-values` with the blocking additive migration hook;
3. verify API005 and Serve032 heads/structures;
4. deploy shadow-capable processes and prove image/head/handler inventory;
5. collect parity and crash evidence; and
6. promote only an explicitly selected canary service.

The first additive migration deployment omits `--atomic` unless the selected
automatic rollback image is proven ahead-of-head compatible. Native Helm
rollback is image rollback only; it does not run schema down. A failed
milestone is repaired by deploying a compatible digest against the retained
additive schema. Database principal topology and credentials are unchanged by
this program.

## Fault-injection and verification

Mandatory crash/race points include:

1. before action admission;
2. after replica/action commit but before the controller receives the ID;
3. during two-dispatcher due discovery;
4. after request/queue commit but before materialization acknowledgement;
5. after request claim but before provider mutation boundary;
6. after provider bytes may have been accepted but before operation ID/result;
7. after provider result but before request terminalization;
8. after request terminalization but before Serve reduction;
9. after retry reduction but before the controller observes `next_attempt_at`;
10. during controller/API/executor eviction and controller-leadership change;
    and
11. during compatible image rollback/re-upgrade with nonterminal actions.

Tests must prove:

- deterministic action/request identity and byte-mismatch rejection;
- exactly one queue delivery and one request claim lease per attempt;
- no action lease/heartbeat table or domain due scanner;
- database-clock retry continuity across restart;
- stale owner/request/reducer writes reject;
- observed launch adoption and ambiguous launch blocking;
- down terminalization only from exact absence evidence;
- shadow creates one pending row before every eligible legacy enqueue,
  performs exactly the one legacy mutation, and records the actual result;
- promotion refuses mixed binaries, a missing handler/head, incomplete shadow
  coverage, a pending row, or any unresolved divergence; and
- eligible authority no longer reaches the removed thread/map/clock paths.

Required local test layers are PostgreSQL migration/constraint tests, generic
store concurrency tests, Serve reducer/state-machine tests, provider-profile
contract tests, and an isolated HA fault-injection smoke test. SQLite is not a
central resource-action target.

## Payoff metrics and stop conditions

The restructuring earns its cost only if all of these hold:

- every crash point produces one logical replica action and no false cleanup;
- controller handoff resumes from the action/request rows without rebuilding
  attempt or due state from memory;
- shadow covers representative launch/down success, retry, capacity, lost-ack,
  and absence cases with zero unresolved divergence for the soak window;
- the authoritative Serve path deletes the named in-memory ownership state,
  rather than layering the journal beside it indefinitely;
- no second queue, action lease, or domain worker is introduced; and
- M1-M4 remains independent of the deferred principal/downgrade/uninstall
  program.

Before claiming a generic platform payoff, a second domain must demonstrate
material code removal and reuse of the same state/lock/request semantics. One
Serve adapter alone justifies a durable Serve action layer, not an open-ended
workflow framework.

## Security and operational boundary

This design uses the repository's current PostgreSQL migration and runtime
principal model. Conventional Alembic DDL, startup revision checks, catalog
tests, and typed runtime validation establish the M1 boundary; M1 adds no
trigger or schema-verifier framework and does not claim to defend against a
database owner or superuser executing arbitrary DDL. Central role separation,
credential rotation, managed-PostgreSQL administrator coverage, and hardened
product uninstall require their own design and deployment inventory.

Provider credentials remain in the existing request execution boundary and
are never stored in action JSON. Persisted descriptors/outcomes are bounded and
redacted. Public callers cannot choose action IDs, bind requests to actions, or
write typed outcomes.

## Open gates

- Confirmation that PR #1070's current request schema exposes the transaction
  hooks needed for deterministic action correlation without copying its queue.
- A checked-in inventory of the initial `pod_cluster_v1` eligible cohort.
- Test-cluster SSO and immutable registry-push access for `boltz-test`.
- Measured shadow sample minimums and the first canary service selection.
- A separate decision on whether central principal convergence is worth its
  operational cost; it must not silently re-enter M1-M4.

## Deliberate departures from the superseded draft

- API schema down and feature-specific full uninstall are removed; additive
  heads remain through application rollback.
- No new schema owner/migrator/runtime role topology or superuser bootstrap is
  introduced.
- No maintenance-reader fleet, downgrade fence, rollout-pin authority, or
  external owner-mutation journal is part of resource actions.
- Event-v2 and broad operational-history redesign are deferred.
- The generic kernel has two tables in v1; multi-effect/child-action tables are
  deferred until a real second use case requires them.
- Authoritative provider scope is narrow and explicit instead of claiming
  every existing cloud path at launch.
