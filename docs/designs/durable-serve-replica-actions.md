# Durable SkyServe Replica Actions

Status: bounded M0 and M1b contract accepted after independent adversarial
review; M1a inert schema and dark M1b typed store implemented and locally
verified; M2 schema, cluster identity, immutable provider contracts, and typed
shadow-store foundations implemented and locally verified; runtime shadow
instrumentation pending

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

The `resource_identity` column is the UTF-8 decoding of canonical JSON for this
exact subset:

```text
{
  version: 1,
  service_hash,
  service_incarnation,
  replica_id,
  replica_incarnation
}
```

The full `ResourceActionIdentityV1` object above remains the `action_id`
preimage; `desired_generation` and `action_kind` occupy their separate natural
identity columns rather than being duplicated inside `resource_identity`.
SkyServe already creates `services.hash` with `uuid.uuid4()` before starting
any external child operation. For action-aware rows that one persisted value is
the service incarnation: `service_hash` is its canonical lowercase UUID text
and `service_incarnation` is the same value decoded as a UUID. Serve032 does
not add a second service-incarnation column. A null, non-UUID, or noncanonical
legacy `services.hash` is ineligible rather than backfilled with a fictitious
identity.

`replica_incarnation` is minted when a new action-aware replica row is first
admitted and is never copied when a replica ID is reused. `desired_generation`
starts at one for its launch plan. A retry or controller recovery retains the
generation. A policy decision that changes the frozen provider plan advances
it and admits a new launch identity; committing teardown advances it exactly
once and all down retries retain that value. This is a desired-state version,
not an API-request attempt counter.

`immutable_spec_sha256`, `typed_outcome_sha256`, and `last_result_sha256` are
lowercase SHA-256 hex over the canonical JSON bytes of their respective
NFC-normalized, typed, bounded objects using the exact serializer above. The
normalized object—not an unnormalized equivalent—is stored in JSONB. Typed
code recomputes canonical bytes after reading JSONB; equality means byte
equality of those recomputed bytes plus hash equality, never PostgreSQL JSON
text rendering or semantic JSON equality alone.

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

Attempt `n` uses request ID `str(uuid.uuid5(action_id, f'attempt:{n}'))`.
`request_input_sha256` is SHA-256 of canonical `ResourceActionRequestInputV1`
bytes, not of the mutable request row:

```text
{
  version: 1, action_id, attempt, request_id,
  name, handler_name, payload_type, payload_format, payload_version,
  producer_version, payload_json, execution_class,
  cluster_name, schedule_type, user_id, file_mounts_blob_id,
  ignore_return_value, retryable,
  precondition_type, precondition_payload, precondition_deadline,
  initial_status: "PENDING", should_enqueue: true, queue_priority: 0
}
```

The same canonical JSON rules used for action identity apply. The deadline is
null or UTC RFC 3339 with exactly six fractional digits and `Z`.
Materialization rejects a caller request unless the ID matches, it is a
pristine `PENDING` request with `should_enqueue=true`, and all runtime,
terminal, and claim fields have their initial null/zero/false values.
V1 action requests must use the normal executor, `ignore_return_value=false`,
`retryable=false`, `ReplayPolicy.NEVER`, and no queue precondition or
precondition deadline. Validation is closed over the exact key set above: the
embedded version/action/attempt/request identity and every fixed flag are
checked independently of the caller-supplied hash, so a self-consistent hash
over a malformed or extended object is not accepted. The generic executor
currently requeues `ExecutionRetryableError` and
`ExecutionPausedError` independently of the `retryable` field, so the action
handler/facet must catch both families and return a closed typed
retry/uncertain outcome before either can escape. One request attempt therefore
terminalizes once; only the action reducer can schedule attempt `n+1`.
`created_at`, database timestamps, queue sequence, delivery/claim state,
request result/error/status after creation, and execution generation after
claim are deliberately excluded. Mutable operational-event context is also
excluded because it is audit enrichment rather than execution input.
`producer_version` is intentionally frozen as an internal payload-ABI field;
mixed action-aware images that would produce a different value fail closed.
The action request payload is a minimal immutable action/attempt reference; the
handler loads the frozen action spec from PostgreSQL instead of reserializing
ambient configuration into the request.

On an insert conflict, materialization resolves both possible unique targets:
the `(action_id, attempt)` row and any row that already owns the deterministic
`request_id`. It locks the complete conflicting attempt set in canonical key
order, then its request and queue rows, and never overwrites any of them. A
foreign request-ID owner or more than one resolved binding is corruption and
moves the intended action to `BLOCKED`. For the one expected binding, the store
requires the caller's full canonical input hash to equal the attempt's durable
`request_input_sha256`, then compares the exact correlation and every surviving
immutable request column with the caller's input. A nonterminal adopted request
must still have its original queue row and byte-equal immutable queue inputs. A
terminal request may have no queue row because normal terminalization deletes
it; in that case the attempt hash is the authoritative commitment for
queue-only precondition/priority fields that can no longer be reconstructed,
while every surviving request field is still compared. This is safe because
attempt, request, and initial queue were inserted atomically from that same
hash. A missing request, an
uncorrelated/cross-correlated request, a missing nonterminal delivery, or any
byte mismatch moves the action to `BLOCKED` with a bounded conflict result and
creates no delivery. This is fail-closed adoption after a lost materialization
acknowledgement, not request refresh.

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
one locks/materializes; the other skips and retries the same tuple to adopt the
exact committed binding. A lost materialization response is recovered by the
explicit `QUEUED` branch, deterministic request ID, and byte equality.

The narrow program uses this lock order whenever a transaction touches more
than one class:

1. current controller leadership/owner fence;
2. Serve service parent and replica-incarnation rows, in canonical key order;
3. capacity, placement, handoff, and reservation rows;
4. shadow parent rows in canonical action-ID order;
5. shadow child rows in `(action_id, request_sequence)` order;
6. action row;
7. attempt row;
8. API request row;
9. API request queue row; and
10. global operational-event sequence row.

Transactions may take a suffix or a subset and finish, but may never acquire
an earlier class afterward. Shadow admission/completion uses classes 1-5;
authoritative admission may continue from class 5 to class 6. Materialization
uses 6-9. Existing generic request claim/terminalization uses only 8-10 and
never writes an action or attempt. Authoritative down that adopts shadow launch
evidence locks the parent before admitting/locking its down action.
Provider-I/O code holds none of these locks.

## Journal-before-I/O and typed outcomes

Before the existing high-level launch/down handler can enter provider I/O, it
must claim-fenced-write `INTENT_COMMITTED` on the correlated attempt and verify
the immutable provider plan/locator already committed by admission. These
attempt writes lock action, attempt, and correlated request in that order.
After any lock wait they use a fresh database-clock statement and revalidate
the exact correlation, `RUNNING` state, execution generation, claim token,
worker, current owner fence, and unexpired lease. They update and commit before
provider I/O; no path locks a request and then reaches backward to an action.
If terminalization wins the request lock, the evidence writer wakes and
rejects. If the evidence writer wins, its journal state linearizes before
terminalization. If the provider returns an operation ID, the handler writes
it under the same fence as soon as it is known. The ID is optional because some
providers do not expose one; the immutable requested locator and readback
contract are mandatory for authoritative eligibility. Submission evidence is
write-once: null means that this call learned no new ID and preserves an
existing ID; two different non-null IDs conflict. At settlement, the journaled
ID is injected into a missing/null typed-outcome field before canonical hashing,
while a different non-null typed ID conflicts. The attempt column and persisted
typed outcome therefore cannot disagree.

The v1 typed outcome is:

```text
ServeReplicaActionOutcomeV1 = {
  disposition: "succeeded" | "retryable" | "uncertain" |
               "terminal_error" | "cancelled",
  certainty: "observed" | "provider_acknowledged" | "unknown",
  provider_operation_id: null | Text,
  provider_code: null | Text,
  retry_class: null | "transient" | "capacity" | "quota" | "rate_limited" |
               "observation_required",
  retry_after_seconds: null | NonnegativeInteger,
  observation: null | ProviderLifecycleObservationV1,
  normalized_message: null | Text
}
```

Provider error strings are diagnostic only. The closed disposition/certainty/
retry fields authorize state transitions. Secrets, credentials, raw tracebacks,
and unbounded provider payloads are redacted before persistence.

Shadow JSON fields are bound to named closed types, not merely size/hash
checks. `actual_outcome` and `proposed_outcome` are
`ServeReplicaActionOutcomeV1`; `pre_observation` and `post_observation` are
`ProviderLifecycleObservationV1`; and `invocation` is the companion design's
`ProviderLifecycleInvocationV1`. Parent projections use:

```text
ServeShadowProjectionV1 = {
  version: 1,
  action_kind: "launch" | "down",
  row_disposition: "retained" | "removed",
  replica_status: null | "PENDING" | "PROVISIONING" | "STARTING" |
                  "READY" | "NOT_READY" | "SHUTTING_DOWN" | "FAILED" |
                  "FAILED_INITIAL_DELAY" | "FAILED_PROBING" |
                  "FAILED_PROVISION" | "FAILED_CLEANUP" | "PREEMPTED" |
                  "UNKNOWN",
  capacity_outcome: null | "success" | "capacity_failure" |
                    "quota_failure" | "generic_failure",
  action_disposition: "succeeded" | "retryable" | "uncertain" |
                      "terminal_error" | "cancelled",
  resolved_target: null | ResolvedProviderTargetV1
}

ServeShadowRetryDecisionV1 = {
  version: 1,
  decision: "retry_same_plan" | "replan_new_generation" | "observe" |
            "block" | "terminal",
  retry_class: null | "transient" | "capacity" | "quota" |
               "rate_limited" | "observation_required",
  delay_seconds: null | NonnegativeInteger,
  logical_attempt: PositiveInteger
}
```

`legacy_projection` and `proposed_projection` are
`ServeShadowProjectionV1`; `retry_decision` is
`ServeShadowRetryDecisionV1`. Typed constructors/readers reject unknown or
missing keys, wrong nested versions, and enum combinations before hashing or
parity comparison. The migration CHECKs only the bounded outer storage shape;
typed code owns the complete nested contract.

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
second transaction for Serve state. Same-state replay with the same committed
request-input identity is idempotent. A settled replay does not supply or derive
a second proposed outcome: the canonical typed outcome and result already
committed in the attempt/action rows are the authority and are revalidated
against their stored hashes. Re-running the callback after a commit would both
risk duplicate Serve writes and make recovery depend on code-version drift, and
request GC may already have removed the terminal source row. APIs that do
accept an explicit expected settled-outcome commitment must reject a different
hash, but this v1 replay API intentionally exposes only adoption of the stored
projection. It does not commit a `REDUCING` state: a crash rolls the transaction
back to `QUEUED`, where another reducer can retry.

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

- `services.resource_action_mode` with permanent `legacy` default and
  `resource_action_mode_changed_at` for the promotion window;
- nullable `replica_incarnation`, `desired_generation`,
  `sky_cluster_record_uuid`, current launch/down action IDs, and current
  launch/down shadow-sample IDs on replicas;
  and
- bounded logical-sample and per-legacy-request-attempt shadow tables.

The existing-row additions are exactly:

```text
services
  resource_action_mode              TEXT not null default 'legacy'
  resource_action_mode_changed_at   TIMESTAMPTZ nullable

replicas
  replica_incarnation               UUID nullable
  desired_generation                BIGINT nullable
  sky_cluster_record_uuid           UUID nullable
  launch_action_id                  UUID nullable
  down_action_id                    UUID nullable
  launch_shadow_sample_id           UUID nullable
  down_shadow_sample_id             UUID nullable
```

The mode enum is `legacy | shadow | authoritative`. `legacy` may retain a null
mode timestamp; each explicit transition writes a fresh PostgreSQL clock value.
The initial program permits only `legacy -> shadow -> authoritative`. A typed
transition validates canonical UUID `services.hash`, the current owner, image
inventory, and milestone gates; table defaults or a bare SQL update do not
activate behavior.

The common services/replicas metadata remains usable by local SQLite Serve
databases, whose rows stay `legacy`: revision 032 adds the inert existing-table
columns on both supported Serve dialects so current metadata remains queryable,
but creates neither shadow table on SQLite. The shadow tables and every
action-aware helper are PostgreSQL-only and fail closed on another dialect. A
separate SQLAlchemy metadata owns those tables so revision 001/current-Base
bootstrap cannot accidentally create them in a fresh SQLite database. Existing
rows retain null replica identity/link columns: migration must not mint an identity
that is absent from the live provider resource. New action-aware replicas get
the three identity fields together, with generation one. Row-local checks
require the identity triple to be all null or all nonnull, a positive
generation, and an identity for every nonnull action or shadow link. For each
action kind, the authoritative and shadow link cannot both be nonnull. Partial
unique indexes prevent one action ID, shadow ID, replica incarnation, or
cluster-record UUID from being attached to multiple live rows. There is no
Serve-to-API foreign key because supported deployments may keep the two state
stores separate; consolidated PostgreSQL on one physical connection is checked
at runtime before authority.

The new identity, generation, provider-target, and action/shadow-link columns
are action-owned. Existing generic replica upserts currently replace every
non-primary-key column from `EXCLUDED`; Serve032 changes all ordinary, batch,
paid-capacity, and reserved-fill conflict updates to exclude the action-owned
set. Legacy inserts may still create null action fields, but routine status
persistence can never erase or replace an existing identity/link. Only typed,
owner-fenced transition/admission methods may initialize an identity, advance
a generation, or change its current link.

The logical table is one row per would-be action, keyed by its deterministic
`would_be_action_id`. It stores the complete canonical action identity,
immutable spec and hash, bounded provider plan and hash, service/replica
identity, generation/action kind, profile eligibility, phase, terminal legacy
projection, proposed durable projection, parity class, revision, and
PostgreSQL timestamps. Its closed phases are `PENDING`, `RUNNING`, `COMPLETE`,
`ABANDONED_PRE_SUBMIT`, and `AMBIGUOUS`. Its closed parity classes include
`PENDING`, `MATCH`, `IDENTITY_MISMATCH`, `PLACEMENT_MISMATCH`,
`SUBMISSION_CERTAINTY_MISMATCH`, `OPERATION_ID_MISMATCH`, `RETRY_MISMATCH`,
`OBSERVATION_MISMATCH`, `TERMINAL_MISMATCH`,
`UNSUPPORTED_PROVIDER_PROFILE`, `ABANDONED`, and `AMBIGUOUS`.

The exact parent shape is:

```text
serve_resource_action_shadow_samples
  would_be_action_id        UUID primary key
  service_name              TEXT not null
  service_hash              TEXT not null
  service_incarnation       UUID not null
  replica_id                BIGINT not null
  replica_incarnation       UUID not null
  desired_generation        BIGINT not null
  action_type               TEXT not null
  resource_identity         TEXT not null
  immutable_spec            JSONB not null
  immutable_spec_sha256     TEXT not null
  provider_plan             JSONB not null
  provider_plan_sha256      TEXT not null
  profile_eligibility       TEXT not null       # ELIGIBLE | UNSUPPORTED
  phase                     TEXT not null
  legacy_projection         JSONB nullable
  legacy_projection_sha256  TEXT nullable
  proposed_projection       JSONB nullable
  proposed_projection_sha256 TEXT nullable
  parity_class              TEXT not null
  revision                  BIGINT not null
  created_at                TIMESTAMPTZ not null
  updated_at                TIMESTAMPTZ not null
  completed_at              TIMESTAMPTZ nullable

  unique (service_hash, service_incarnation, replica_id,
          replica_incarnation, desired_generation, action_type)
```

`replica_id >= 0`, `desired_generation > 0`, action/hash/enum shapes, bounded
JSON objects, and pair-null JSON/hash relationships are CHECKed. `PENDING` and
`RUNNING` require `parity_class=PENDING` and no completion timestamp.
`COMPLETE` requires both final projection/hash pairs, a nonpending parity, and
`completed_at`. `ABANDONED_PRE_SUBMIT` and `AMBIGUOUS` require their matching
parity and `completed_at`; their final projections may remain null. Typed reads
recompute both hashes and the deterministic identity before trusting a row.

`serve_resource_action_shadow_attempts` is one row for every legacy high-level
SDK/direct mutation boundary, with primary key
`(would_be_action_id, request_sequence)`. `request_sequence` is contiguous
across the parent; each `logical_attempt` has exactly one primary child and
zero or more cleanup children that belong to that legacy retry. `request_role`
is `PRIMARY_LAUNCH`,
`PRIMARY_DOWN`, or `LAUNCH_CLEANUP_DOWN`. In particular, the cleanup
`sdk.down()` between two legacy launch retries is evidence under the launch
parent, not a separately admitted logical down action. The row also stores
`planned_execution_kind`, a nullable write-once real request ID, a bounded
redacted invocation hash, normalized actual/proposed outcome, retry decision,
pre/post observations, provider correlation evidence, phase, and timestamps.
Its phases are `PRE_SUBMIT`, `REQUEST_BOUND`, `COMPLETE`,
`ABANDONED_PRE_SUBMIT`, and `REQUEST_ASSOCIATION_UNKNOWN`. A partial unique
index prevents a real request ID from belonging to two attempts. A foreign key
with `ON DELETE CASCADE` points to the logical sample because both shadow tables
share one retention boundary; typed retention first proves that the parent is
unreferenced and outside every protected window. Neither table references a
service, replica, API request, or real action, so the evidence survives deletion
and request garbage collection.

The exact child shape is:

```text
serve_resource_action_shadow_attempts
  would_be_action_id        UUID not null references shadow_samples
  request_sequence          INTEGER not null
  logical_attempt           INTEGER not null
  request_role              TEXT not null
  planned_execution_kind    TEXT not null
  phase                     TEXT not null
  legacy_request_id         TEXT nullable
  invocation                JSONB not null
  invocation_sha256         TEXT not null
  provider_operation_id     TEXT nullable
  actual_outcome            JSONB nullable
  actual_outcome_sha256     TEXT nullable
  proposed_outcome          JSONB nullable
  proposed_outcome_sha256   TEXT nullable
  retry_decision            JSONB nullable
  retry_decision_sha256     TEXT nullable
  pre_observation           JSONB nullable
  pre_observation_sha256    TEXT nullable
  post_observation          JSONB nullable
  post_observation_sha256   TEXT nullable
  divergence_class          TEXT nullable
  admitted_at               TIMESTAMPTZ not null
  request_bound_at          TIMESTAMPTZ nullable
  completed_at              TIMESTAMPTZ nullable
  updated_at                TIMESTAMPTZ not null

  primary key (would_be_action_id, request_sequence)
  unique (legacy_request_id) where legacy_request_id is not null
```

Both counters are positive. Every JSON/hash pair is pair-null and canonically
bounded. `REQUEST_BOUND` requires API-request execution, a real ID and bind
timestamp, and no completion timestamp. `COMPLETE` requires a completion
timestamp and, for API-request execution, a real ID/bind timestamp.
`ABANDONED_PRE_SUBMIT` requires no request ID, operation ID, actual outcome, or
post-observation. `REQUEST_ASSOCIATION_UNKNOWN` requires API-request execution,
a null request ID, and a completion timestamp. `legacy_direct_down` may finish
without an ID but is always divergent/promotion-blocking. Contiguous sequence
allocation, write-once request/provider IDs, exact replay, and parent
finalization only after all children are terminal are enforced by typed
transaction methods rather than triggers.

Every shadow JSON object uses the action kernel's exact NFC/sorted canonical
serializer and is at most 65,536 canonical UTF-8 bytes; floats are forbidden.
`resource_identity` is 1..1,024 bytes, `service_name` is 1..256 bytes,
`service_hash` is the 36-byte canonical UUID text required by typed admission,
`legacy_request_id` is 1..128 bytes when present, and
`provider_operation_id` is 1..1,024 bytes when present. SHA fields are exactly
64 lowercase hexadecimal characters. The migration CHECKs PostgreSQL JSONB
object type, stored-rendering size as a conservative outer bound, text/hash
shape, enum, counter, pair-null, and phase shape; typed reads recompute the
canonical-byte bounds and hashes.

The parent has a promotion-window index on
`(service_name, service_hash, created_at, would_be_action_id)`, a partial
blocker index on `(service_name, service_hash, updated_at)` for any noncomplete,
nonmatching, or unsupported row, and a completed-retention index on
`(completed_at, would_be_action_id)`. The child has a partial stale-work index
on `(phase, admitted_at, would_be_action_id, request_sequence)` for
`PRE_SUBMIT` and `REQUEST_BOUND`, in addition to the partial unique request-ID
index. `divergence_class`, when nonnull, is one of the parent divergence enums
other than `PENDING`, `MATCH`, `ABANDONED`, and `AMBIGUOUS`.

Admission uses one physical PostgreSQL connection. In authoritative mode the
transaction that changes replica/capacity intent also inserts/adopts the action
and links its ID. If either write fails, neither commits.

In shadow mode the legacy thread remains the sole mutation owner. Before every
eligible legacy launch/down enqueue, the same transaction that persists the
replica/capacity intent inserts or exactly adopts the logical `PENDING` sample.
Ordinary, paid-capacity, and reserved-fill admission borrow that transaction;
denial commits neither intent nor sample. Teardown admission advances the
generation and inserts the down sample in the transaction that durably commits
the teardown intent.

Paid-capacity and reserved-fill admission preserve the global lock order rather
than appending shadow writes to their current capacity-first transactions. The
combined PostgreSQL helper locks the service, then locks an existing identified
replica or inserts a provisional fully identified replica row, and only then
takes capacity/reservation locks. On denial it removes only the provisional row
before committing any existing waiter/capacity bookkeeping; it creates no
shadow parent or link. On approval it finishes the replica/capacity mutation,
inserts or exactly adopts the parent, and writes the replica link before one
commit. Recovery never turns an older name-only replica into that provisional
form.

Immediately before each `sdk.launch()` or `sdk.down()` call, including an
in-process legacy retry and its cleanup down, the worker commits the next
`PRE_SUBMIT` child. After the SDK returns a request ID it binds that real ID in
a short write-once
transaction, then streams the request and records the normalized result and
retry decision. A superseded decision may become
`ABANDONED_PRE_SUBMIT` only when code proves the SDK/direct mutation function
was never entered. A crash or exception after entering request creation but
before binding its ID becomes `REQUEST_ASSOCIATION_UNKNOWN`, never inferred as
not submitted. A parent left without a completed child is likewise a coverage
failure; recovery adopts its identity and next sequence but never invents a
request ID or silently manufactures a parity result.

Child preparation is serialized under the parent lock. It refuses to append
while any earlier child is nonterminal and never advances past
`REQUEST_ASSOCIATION_UNKNOWN` or `ABANDONED_PRE_SUBMIT`. A second primary for
the same frozen plan additionally requires the preceding primary's committed
retry decision to be `retry_same_plan`; `terminal`, `block`, `observe`, and
`replan_new_generation` do not authorize another primary mutation under that
parent. Cleanup-down retries remain request-sequenced children of the current
logical launch attempt. A first cleanup may follow a completed failed primary;
another cleanup requires the preceding cleanup to be complete with
`retry_same_plan` before another call may begin. Once a cleanup exists, the
next primary additionally requires the latest cleanup to have terminalized
successfully with exact absence/safe-relaunch proof. A cleanup decision of
`retry_same_plan` authorizes only another cleanup; `block`, `observe`, and
`replan_new_generation` authorize no later primary under this parent.

The actual terminal legacy state change and logical sample completion share
one Serve transaction, including paid-capacity outcome/release and replica
removal. Each legacy retry has a distinct child but all attempts for an
unchanged frozen plan share one logical action identity. A policy-selected new
plan advances `desired_generation` and creates a new parent.

`planned_execution_kind` is `api_request` or `legacy_direct_down`. A direct-down
sample may characterize the old path but is always promotion-blocking. During
M2 legacy teardown is routed through `sdk.down()` and
`sdk.stream_and_get()`; every down attempt in a promotion window therefore has
a real request ID. Shadow never inserts `api_resource_actions`, enqueues an
action request, suppresses a legacy call, invents a request ID, or calls the
provider twice. The evaluator compares the proposed action path with the one
actual legacy path and stores only bounded divergence categories.

Shadow is complete for a service, not statistically sampled: promotion blocks
if any eligible decision in the candidate window lacks a parent, any expected
attempt lacks a child or real request association, any row is pending,
abandoned, ambiguous, direct-down, unsupported, or divergent, or either action
kind lacks the configured minimum. Retention does not delete candidate-window
rows before promotion. The transition to `shadow` and its database timestamp
are written under the service/owner lock; the promotion transaction uses that
timestamp for the minimum 24-hour window and rechecks all blockers under the
same lock.

The parent admission helper first locks and revalidates the service name,
incarnation hash, controller owner, nonnull lifecycle epoch, and `shadow` mode;
then it admits/links the parent in the caller's replica-intent transaction.
`created_at` is a fresh PostgreSQL `clock_timestamp()` read after that lock,
never the transaction-start timestamp. Promotion takes the same service lock
before scanning the window, so an admission that waited behind promotion must
revalidate the mode and cannot appear after the scan with a pre-window
timestamp.

`launch_shadow_sample_id` and `down_shadow_sample_id` are incarnation-scoped,
not both constrained to the row's current generation. The launch link retains
the most recent launch parent and therefore may name generation N after
teardown advances the row to N+1; the down link, when present, must match the
current teardown generation. Generation advance never clears launch evidence.
Retention cannot delete a referenced parent or any of its children while the
replica row or a durable cleanup intent exists. If a replica launched in shadow
later enters authoritative down, down admission loads the matching completed
launch child, revalidates its canonical observation and
`ResolvedProviderTargetV1`, and copies that target into the immutable down plan.
Missing, incomplete, unsupported, or mismatched launch evidence keeps that
replica on shadow/legacy teardown; it never falls back to a name-only
authoritative down.

All three in-scope teardown owners—`ReplicaManager._terminate_replica`,
`service._cleanup`, and `serve_utils._terminate_failed_services_locked`—use
the same typed parent/child admission, request-binding, and completion helpers.
Launch-retry cleanup remains `LAUNCH_CLEANUP_DOWN` under its launch parent.
Orphan cleanup is explicitly outside the first adapter and must carry an
exclusion guard rather than creating an uncorrelated shadow sample. A repository
guard test inventories direct `core.down()`, `sdk.down()`, and teardown-helper
call sites so a new in-scope bypass fails CI.

## Activation and mixed versions

Service mode is monotonic in the initial program:

```text
legacy -> shadow -> authoritative
```

Promotion requires:

- all old controller-capable processes drained;
- every remaining controller/API/executor running the approved image digest,
  at API005, Serve032, and global-user-state 028, and exposing the registered
  action handler through the existing handler inventory;
- exact provider-profile eligibility for every live candidate;
- no unresolved shadow divergence or unsampled mutation;
- at least 24 hours and a configured minimum sample count of clean live shadow
  operation, including launch and down; and
- successful crash injection at every boundary below.

`ActivationGateEvidenceV1` is a closed internal value bound to the exact
service name, service-incarnation hash, lifecycle epoch, and (for authority)
the database timestamp that opened the current shadow window. It carries the
three independent schema heads (`API005`, `Serve032`, and global-user-state
`028`), approved image and named inventory fingerprints, and a database-clock
`verified_at`. The transition rejects evidence for another fence/window,
evidence from the database future, or evidence older than five minutes. A
`legacy -> shadow` transition requires a null candidate-window binding; a
`shadow -> authoritative` transition requires exact timestamp equality with
the locked service row. The 24-hour candidate duration is a hard minimum, not
a caller-reducible test parameter.

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

M1b verification evidence on 2026-08-01:

- pure canonical identity/request/reduction tests and real-PostgreSQL store
  tests pass with 22 tests, including both evidence-versus-terminalization race
  directions, lease expiry during a request-row lock wait, two-dispatcher
  materialization, deterministic request-ID collision quarantine, and
  callback-free retry-deadline replay;
- the full PostgreSQL request suite passes with 47 tests, including API005
  migration/catalog coverage and correlated-request retention before and after
  attempt settlement;
- SQLite request-retention coverage passes with 15 tests and the API/GC race
  regression passes with one test; and
- YAPF, isort, mypy across 818 source files, pylint at 10.00/10, dashboard lint
  and formatting, and `git diff --check` pass.

### M2: Serve shadow journal

- Add Serve032 mode/replica-identity/link plus logical-sample/per-attempt
  schema. Refuse schema down while retaining the additive state.
- Add global-user-state revision 028 with a nullable, partial-unique
  `clusters.cluster_record_uuid`; leave historical rows null, omit it from
  ordinary cluster updates, and reserve initialization/adoption for the
  PostgreSQL-only action-aware cluster-row primitive. Refuse downgrade while
  any nonnull commitment remains.
- Reuse the existing UUID-valued `services.hash` as both textual hash and
  service incarnation; do not add or backfill a second service identity.
- Preallocate each new action-aware replica incarnation and SkyPilot cluster
  record UUID in the same transaction as the replica/capacity intent. Keep
  already-live name-only rows ineligible.
- Refactor launch/down decision construction into pure descriptor/classifier
  functions used by both legacy execution and shadow evaluation.
- Persist a child before every legacy SDK call, bind its nullable real request
  association immediately after SDK admission, and complete the child and
  parent transactionally with the legacy Serve projection. Route direct
  teardown through `sdk.down()` before collecting the promotion window.
- Preserve legacy autoscaling and provider mutation authority.

M2 foundation verification evidence on 2026-08-01:

- Serve032 installs the inert mode, replica-identity/link, logical-sample, and
  per-attempt schema while preserving portable inert columns for supported
  local controller databases;
- global-user-state revision 028 installs a nullable portable cluster-record
  UUID and partial unique index, leaves historical rows null, and provides the
  PostgreSQL-only exact insert/adopt/reject primitive without changing ordinary
  cluster updates;
- closed, bounded provider locator, invocation, observation, outcome, shadow
  projection, and retry contracts have canonical byte/hash fixtures and
  action-specific success proof; and
- the PostgreSQL typed shadow store passes its full 21-test suite, including
  exact parent/child replay, retry-chain closure, activation-window fencing,
  action-specific projection proof, retention protection, and lock races. Its
  exact source received independent contract and concurrency acceptance.

Runtime admission/linking, legacy SDK instrumentation, provider identity
propagation/readback, and live shadow evaluation remain M2 gates; no service is
eligible for authority yet.

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
3. verify all independent milestone-specific heads: M1a is API005 with
   unchanged Serve031/global-user-state 027; M2 and later require API005,
   Serve032, and global-user-state 028, with no cross-lineage Alembic
   dependency;
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
- shadow creates one logical row with replica/capacity intent and one child
  before every real legacy SDK attempt, performs exactly that one legacy
  mutation, binds only its real request ID, and records the actual result;
- stale parent, pre-call, call/ID-bind, and completion gaps become explicit
  promotion blockers rather than synthetic results or request IDs;
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

- Serve032 migration, typed shadow store, pure descriptor/classifier, and
  legacy launch/down instrumentation verification.
- A checked-in inventory of the initial `pod_cluster_v1` eligible cohort after
  preallocated cluster UUID propagation, Kubernetes replica-incarnation
  labeling, exact readback, and the redacted invocation builder pass contract
  tests. Until then the profile is shadow-only and promotion-blocking.
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
