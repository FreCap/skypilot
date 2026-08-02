# Durable SkyServe Replica Actions

Status: bounded M0 and M1b contract accepted after independent adversarial
review; M1a inert schema and dark M1b typed store implemented and locally
verified; upstream's shipped Serve032 request-classification migration and the
unshipped resource-action migration lineage have been reconciled as one
guarded Serve033 catalog, implemented and reverified against both the upstream
lineage and adversarial partial/corrupt catalogs; the cluster identity,
immutable provider contracts, typed shadow
store, promotion audit, and generic API006 progress substrate are implemented
and locally verified; the Serve033 preparation-capability commitment schema/typed
store, closed launch-identity wire contracts, shared effective-identity
resolver, capability-fenced no-enqueue endpoint, and bounded authenticated
client are implemented and locally verified, while manager-side CSPRNG
generation/reference creation, client wiring, and raw-capability discard remain
pending; the immutable effect-origin,
closed handler-return,
pre-I/O representability, and reducer-owned quiescence contract is frozen in
design while its Serve validator/reducer, dedicated return codec, and shadow-
outcome parser alignment remain pending; the candidate-only Kubernetes
preparation/admission
handshake and execution-config boundary are in progress; runtime shadow
instrumentation pending

Last updated: 2026-08-02

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

- another generic request queue, domain scheduler, or action execution lease;
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

V1 fixes `RESOURCE_ACTION_MAX_ATTEMPT_V1 = 2147483647`, the PostgreSQL
`INTEGER` maximum used by both `current_attempt` and `attempt`. No action may
materialize, derive a request ID for, or reference attempt max plus one. The
closed exhaustion reduction below handles the boundary before any increment.

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
request-lifecycle boundary, retained provider-I/O watermark, and typed outcome
must be snapshotted into the attempt.
Request retention skips a correlated terminal request whose attempt is not yet
`SETTLED`.

The fixed Kubernetes topology has several externally visible effects inside
one request attempt. Recovery must carry partial UID, handle, and job-submission
commitments into the next effect; a terminal-only outcome is too late. Additive
API-request revision 006 has `down_revision='005'` and therefore replaces 005
as the one API-request lineage head. It adds no queue or table, only this
bounded snapshot plus the nonterminal provider-I/O watermark to
`api_resource_action_attempts`:

```text
  provider_io_boundary       TEXT not null default 'NOT_STARTED'
                               # NOT_STARTED | INTENT_COMMITTED |
                               # SUBMITTED_OR_AMBIGUOUS
  provider_progress          JSONB nullable
  provider_progress_sha256   TEXT nullable
  provider_progress_revision BIGINT not null default 0
```

`mutation_boundary` remains the request-attempt lifecycle field from revision
005 and may advance to `SETTLED`; `provider_io_boundary` is the monotonic
pre-settlement watermark and is never overwritten by settlement. Before
settlement the two fields are equal. Reduction changes only
`mutation_boundary` to `SETTLED`, leaving `provider_io_boundary` as durable
proof of whether provider I/O ever became possible. Revision 006 checks the
closed watermark enum and this equality. Because a revision-005 `SETTLED` row
has already erased the pre-settlement value, the migration first fails closed
if any action-attempt row exists; the feature is dark before 006 and any such
installation requires a separately reviewed evidence backfill rather than an
invented watermark.

The pair is null exactly when revision is zero; otherwise revision is positive,
the value is an object whose stored rendering is at most 65,536 bytes, and the
hash has the normal shape. The Serve handler binds it to the companion's closed
`ProviderLifecycleProgressV1`, recomputes canonical bytes/hash on every read,
and permits only its monotonic phase transitions. Claim-fenced progress writes
lock action -> predecessor attempt when one exists -> current attempt ->
current request, revalidate the fresh request lease after any wait, and commit
before the next provider object or Skylet job effect. Attempt rows are locked
in increasing attempt number, preserving the action -> attempt -> request
class order. Progress
survives request terminalization and is included in the reducer's immutable
attempt snapshot. Ordinary requests and non-provider actions leave it null.
Revision 006 is required before provider-authoritative dispatch; it is not
needed to collect M2 legacy shadow evidence. It is PostgreSQL-only, preserves
ordinary attempts, and refuses schema down once present; application rollback
keeps the additive head.

Additive API-request revision 007 changes no request, queue, action, or attempt
shape. It widens only the named `api_server_instances.role` CHECK from the
API006 set to include `authority-worker`, preserving all existing instance
rows. Downgrade is permitted only when no authority-worker instance row
remains. Ordinary `all`, `executor`, and `controller` queue views exclude the
four closed private Serve handlers by handler name without importing Serve or
referencing Serve033 tables, so an API007 process remains compatible before
Serve033 exists. An `authority-worker` process is PostgreSQL-only and fails
before claiming if its exact four-handler inventory, mounted immutable cohort
routing document, configured active cohort row, or any required Serve033
cohort/reference/coverage table is absent. Active selection gates only creation
of new `PREPARING` references. A worker whose own frozen cohort is `DRAINING`
may continue to claim existing references for that cohort after another cohort
becomes active. Its existing PR #1070 claim query then adds the complete
action/current-attempt/reference/own-cohort or shadow/reference/coverage/
own-cohort predicate; it does not add a queue or lease.

The 65,536-byte column check is not a late runtime escape hatch. Before
authoritative admission creates or binds an action request, a pure companion-
owned representability enumerator renders the exact frozen spec/cohort and each
byte-exact live registered worker identity and claim/attempt-attestation preimage
through every launch phase, terminal handler-return/no-effect-resolution
variant, and reducer-built quiescence variant. The exact case set includes
handler-domain `S` and every legal phase/category `R`/`U`/`B` row with maximal
bounded error code, message, and retry delay; supersession `Q` with every E-only
prefix and every legal `E* + N<i>` prefix; all three direct no-effect bases with
empty, one-link, and maximum-integer-count no-I/O prefixes; request-terminal
fallback `P0`, `O`, `S`, and `X` for each compatible
`SUCCEEDED`/`FAILED`/`CANCELLED` reason; and the shadow basis. It maximizes only
response leaves still unknown at admission and the
reachable five-effect origin schedule. Every
API006 progress envelope, exact
`ServeReplicaActionRequestReturnV1`, and final
`ServeReplicaActionOutcomeV1` must independently fit within 65,536 canonical
UTF-8 bytes. Missing finite leaf bounds or any oversize case rejects authority
before the provider-I/O watermark/intent; no truncation, dropped provenance, or
hash-only replacement is legal. The checked-in fixture manifest and case list
are canonicalized in the provider companion and contain realistic and
`candidate_maximal` goldens with exact byte counts and SHA-256 values. Admission
and the immediate pre-I/O validator run the same versioned enumerator; drift
blocks rather than
discovering an oversized cursor after an effect. The candidate-maximal fixture
does not synthetically maximize arbitrary `Text` inside already-frozen
attestations. If either realistic or candidate-maximal evidence exceeds the
limit, authority stays disabled until the canonical design deduplicates
provenance or tightens an explicit bound; the gate does not assume a passing
measurement.

The initial down-spec implementation activated that gate. A realistic
completed-launch down rendered to 72,567 canonical bytes, while a legal
`HANDLE_COMMITTED` partial-launch down rendered to 183,137 bytes. The latter
contained a 28,716-byte cursor and 27,607-byte reducer quiescence twice through
the prior-basis/plan graph; increasing the generic 65,536-byte bound would only
hide that structural duplication. The accepted correction keeps one full prior
basis in the down invocation and one full cleanup target only in its execution
capsule, while the basis and indexed provider plan store the cleanup target's
recomputed hash and the plan stores the basis hash. For a partial launch, the
basis retains the exact source action/attempt key, progress revision,
cursor/quiescence/spec/cleanup-target hashes, and full
resource/target/workspace preimages, but not a second copy of the cursor,
quiescence, or cleanup target. Down admission locks their retained API006
preimages, re-derives the complete cleanup target, requires it byte-equal to the
sole capsule copy, revalidates the complete typed source outcome and every
hash/projection in the same transaction, and rolls back on absence or mismatch.
V1 retains every API resource action and attempt indefinitely because it has no
generic action/attempt GC; a future GC must first add a typed persisted reverse-
reference relation and migration. The handler executes only the complete
cleanup target in its own immutable capsule. This is verified retained-source
deduplication, not provenance truncation or an unverified hash-only provider
lookup. Authority remains disabled until full realistic and candidate-maximal
specs measure at most 60,000 bytes.

Provider progress is attempt-local storage for one action-wide monotonic
provider cursor. Materializing attempt `n+1` locks the action and settled
attempt `n` before inserting the new attempt. It rejects an unsettled
predecessor, a predecessor cursor already at `SUCCEEDED`, or a typed outcome
that does not authorize retry/observation. If `n` has nonnull progress, the
typed store validates it, byte-copies its
`ProviderLifecycleProgressV1.cursor` into `n+1`, sets the explicitly
attempt-scoped `worker_attestation` member to null, recomputes the canonical
envelope hash, and starts the new attempt at local progress revision one. It
does not claim that the predecessor and successor envelope hashes are equal.
If `n` has null progress,
`n+1` may also start null only when
`n.provider_io_boundary='NOT_STARTED'`; under
the journal-before-I/O invariant that boundary value is itself the durable
proof that no provider call was entered, while the typed outcome independently
authorizes retry/observation. A crossed/ambiguous boundary
with missing or regressed progress is corruption and blocks materialization.
Within an attempt, progress revisions increase from that seed. Across attempts,
every provider identity, partial object UID/allocation, resolved target,
handle, deletion proof, runtime/job commitment, and effect-intent phase must be
byte-equal to or a legal monotonic successor of the settled prior snapshot.
Every inherited launch intent retains its byte-equal original effect claim;
every committed record retains both origin claims and disposition. Origin
ordering is lexicographic by `(attempt, execution_generation)`, because
generation restarts for each deterministic attempt request.
The new request handler consumes only its own seeded row; it never starts from
a caller-supplied or null cursor when prior progress exists.

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
handler/facet must catch both families and return the closed
`ServeReplicaActionRequestReturnV1` carrying a retry/uncertain provider result
before either can escape. One request attempt therefore
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
- `BLOCKED`: an identity conflict, quarantine, or finite attempt-domain
  exhaustion requires operator-visible repair and no mutation request is
  runnable; or
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
Serve shadow table and therefore cannot appear in this query.

The dispatcher calls one retryable operation with
`(action_id, expected_revision, expected_attempt, canonical_request_input)` and
reuses that exact tuple after an unknown commit outcome. A short transaction
locks the action with `FOR UPDATE SKIP LOCKED` and has exactly two successful
branches:

- `READY`: require the exact expected revision and due time, require
  `current_attempt < RESOURCE_ACTION_MAX_ATTEMPT_V1`, and only then require
  `expected_attempt == current_attempt + 1`; insert the attempt, request, and
  existing queue delivery, then set `QUEUED`, advance `current_attempt`, and
  increment revision once.
- `QUEUED` lost-ack adoption: require
  `revision == expected_revision + 1` and
  `current_attempt == expected_attempt`; validate the current attempt/request
  binding and full input commitment as above, return it, and neither increment
  nor enqueue.

`BLOCKED`, `TERMINAL`, a changed attempt/revision, or any binding/input
mismatch rejects. `READY` plus a pre-existing attempt/request is corruption,
because their creation is one transaction. General queued recovery reads the
committed binding directly; only retry of this exact materialization tuple uses
the lost-ack adoption branch.

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
4. worker-cohort registry rows in cohort-ID order;
5. worker-cohort reference rows in decision-ID order;
6. shadow coverage rows in canonical decision-ID order;
7. coverage-only submission rows in `(decision_id, request_sequence)` order;
8. shadow parent rows in canonical action-ID order;
9. shadow child rows in `(action_id, request_sequence)` order;
10. action rows in canonical action-ID order;
11. attempt rows in `(action_id, attempt)` order;
12. API request row;
13. API request queue row; and
14. global operational-event sequence row.

Transactions may take a suffix or a subset and finish, but may never acquire
an earlier class afterward. Shadow admission/completion uses classes 1-9;
authoritative admission may continue from class 9 to class 10. Materialization
uses 10-13. Existing generic request claim/terminalization uses only 12-14 and
never writes an action or attempt. Authoritative down that adopts shadow launch
evidence locks coverage and parent before admitting/locking its down action.
Preparation-reference creation and cohort retirement may take only the cohort/
reference suffix. Shadow or authoritative admission takes owner -> service/
replica -> capacity -> cohort -> reference -> coverage/parent -> action.
Retirement takes cohort then references and performs only nonlocking defensive
reads of later-class state; it never reaches backward to service/capacity rows.
The existing cross-process resources-file lock, when needed for launch-cap
admission, is acquired before the short SQL transaction and released before a
worker is authorized; it is never held during preparation, condition waits, or
provider I/O. Provider-I/O code holds none of these locks.

All deterministic action IDs and candidate immutable rows are constructed
before action-row acquisition begins. A transaction touching multiple action
keys iterates their sorted union. At each key, `SELECT ... FOR UPDATE`,
insertion, or `ON CONFLICT` exact adoption is the acquisition for that key;
insertion is never deferred until after a higher key has been locked. Candidate
bytes derived from an optimistic source read are revalidated after the complete
sorted acquisition, and mismatch rolls back every write.

## Journal-before-I/O and typed outcomes

Before the existing high-level launch/down handler can enter mutating provider
I/O, it dispatches on the current attempt's persisted progress shape. An exact
`provider_io_boundary='NOT_STARTED'`, null progress/hash, revision-zero attempt
uses the fresh-cursor branch—even when it is attempt `n+1` copied from a
proved pre-I/O predecessor—and claim-fenced-writes `INTENT_COMMITTED` to both
boundary fields plus the companion profile's first legal nonnull API006 cursor
and its immutable `ProviderLaunchEffectClaimV1` intent origin in one
transaction after any read-only pre-observation. An exact inherited
retry seed instead has `provider_io_boundary='NOT_STARTED'`, nonnull progress
at local revision one, a cursor byte-equal to the locked settled predecessor,
null worker attestation, and null current-attempt provider operation ID; it
atomically writes `INTENT_COMMITTED` to both boundary fields and binds the
current worker attestation to that validated cursor. Both branches verify the
immutable provider plan/locator already committed by admission. An
authoritative attempt can never have
`provider_io_boundary != NOT_STARTED` with null provider progress. These
attempt writes lock action, predecessor attempt when present, current attempt,
and correlated request in that order.
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
existing ID; two different non-null IDs conflict. At reduction, the journaled
ID is injected only into a null
`provider_result.provider_operation_id` while constructing the final action
outcome, before that final outcome is canonically hashed; a different nonnull
typed ID conflicts. The immutable handler-return hash continues to name the
bytes actually stored on the request. The attempt column and persisted final
typed outcome therefore cannot disagree.

The v1 handler return and reducer-owned outcome are distinct closed types:

```text
ServeReplicaActionProviderResultV1 = {
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

ServeReplicaActionHandlerTerminalResultV1 = {
  version: 1,
  result_kind: "serve_resource_action_handler_terminal_v1",
  action_id: UUID,
  action_kind: "launch" | "down",
  attempt: PositiveInteger,
  request_id: UUID,
  request_execution_generation: PositiveInteger,
  handler_name: "serve_resource_action_launch" |
                "serve_resource_action_down",
  reduction_kind: "domain" | "supersede_to_down",
  request_input_sha256: Sha256,
  final_provider_progress_sha256: null | Sha256,
  worker_attestation: ProviderAuthorityWorkerAttemptAttestationV1,
  worker_attestation_sha256: Sha256,
  provider_result: ServeReplicaActionProviderResultV1,
  normalized_provider_error: null | ProviderErrorV1,
  launch_no_effect_resolution: null | ProviderLaunchNoEffectResolutionV1
}

ServeReplicaActionRequestReturnV1 = {
  version: 1,
  return_type: "serve_replica_action_handler_terminal_result_v1",
  terminal_result: ServeReplicaActionHandlerTerminalResultV1,
  terminal_result_sha256: Sha256
}

ServeLaunchNoIoAttemptProjectionV1 = {
  attempt: PositiveInteger,
  request_id: UUID,
  request_input_sha256: Sha256,
  mutation_boundary: "SETTLED",
  provider_io_boundary: "NOT_STARTED",
  provider_progress_revision: 0,
  provider_progress_sha256: null,
  provider_operation_id: null,
  request_terminal_state: "SUCCEEDED" | "FAILED" | "CANCELLED",
  settled_at: UtcTimestamp
}

ServeLaunchNoIoPrefixV1 = {
  version: 1,
  count: NonnegativeInteger,  # at most PostgreSQL INTEGER max 2147483647
  previous_prefix_sha256: null | Sha256,
  current_attempt: null | ServeLaunchNoIoAttemptProjectionV1,
  prefix_sha256: Sha256
}

ServeReplicaActionDirectNoEffectCancellationV1 = one of:
  {version: 1,
   proof_kind: "unmaterialized",
   action_id: UUID,
   resource_identity: ResourceActionIdentityV1,
   source_action_revision: NonnegativeInteger,
   current_attempt: 0,
   no_io_prefix: ServeLaunchNoIoPrefixV1,
   request_id: null,
   request_terminal_state: null,
   request_row_disposition: "not_applicable",
   request_finished_at: null,
   active_claim: false,
   provider_io_boundary: null,
   provider_progress_revision: null,
   provider_progress_sha256: null,
   provider_operation_id: null,
   current_typed_outcome_sha256: null,
   attempt_settled_at: null,
   cancelled_at: UtcTimestamp}
  {version: 1,
   proof_kind: "terminal_request_unsettled",
   action_id: UUID,
   resource_identity: ResourceActionIdentityV1,
   source_action_revision: NonnegativeInteger,
   current_attempt: PositiveInteger,
   no_io_prefix: ServeLaunchNoIoPrefixV1,
   request_id: UUID,
   request_terminal_state: "SUCCEEDED" | "FAILED" | "CANCELLED",
   request_row_disposition: "retained_terminal",
   request_finished_at: UtcTimestamp,
   active_claim: false,
   provider_io_boundary: "NOT_STARTED",
   provider_progress_revision: 0,
   provider_progress_sha256: null,
   provider_operation_id: null,
   current_typed_outcome_sha256: null,
   attempt_settled_at: UtcTimestamp,
   cancelled_at: UtcTimestamp}
  {version: 1,
   proof_kind: "retained_settled_attempt",
   action_id: UUID,
   resource_identity: ResourceActionIdentityV1,
   source_action_revision: NonnegativeInteger,
   current_attempt: PositiveInteger,
   no_io_prefix: ServeLaunchNoIoPrefixV1,
   request_id: UUID,
   request_terminal_state: "SUCCEEDED" | "FAILED" | "CANCELLED",
   request_row_disposition: "retained_terminal" | "garbage_collected",
   request_finished_at: null | UtcTimestamp,
   active_claim: false,
   provider_io_boundary: "NOT_STARTED",
   provider_progress_revision: 0,
   provider_progress_sha256: null,
   provider_operation_id: null,
   current_typed_outcome_sha256: Sha256,
   attempt_settled_at: UtcTimestamp,
   cancelled_at: UtcTimestamp}

ServeReplicaActionRequestFallbackEvidenceV1 = {
  version: 1,
  request_id: UUID,
  attempt: PositiveInteger,
  fallback_reason: "missing_handler_return" | "invalid_handler_return" |
                   "request_failed" | "request_cancelled",
  request_terminal_state: "SUCCEEDED" | "FAILED" | "CANCELLED",
  request_finished_at: UtcTimestamp,
  active_claim: false,
  journal_class: "not_started_empty" | "valid_nonterminal" |
                 "valid_succeeded" | "invalid",
  provider_io_boundary: "NOT_STARTED" | "INTENT_COMMITTED" |
                        "SUBMITTED_OR_AMBIGUOUS",
  provider_progress_revision: NonnegativeInteger,
  provider_progress_sha256: null | Sha256,
  provider_operation_id: null | Text
}

ServeReplicaActionOutcomeBasisV1 = one of:
  {version: 1,
   basis_kind: "handler_terminal_result",
   request_terminal_state: "SUCCEEDED",
   handler_terminal_result_sha256: Sha256,
   direct_no_effect_cancellation: null,
   request_fallback_evidence: null}
  {version: 1,
   basis_kind: "direct_no_effect_cancellation",
   request_terminal_state: null | "SUCCEEDED" | "FAILED" | "CANCELLED",
   handler_terminal_result_sha256: null,
   direct_no_effect_cancellation:
       ServeReplicaActionDirectNoEffectCancellationV1,
   request_fallback_evidence: null}
  {version: 1,
   basis_kind: "request_terminal_fallback",
   request_terminal_state: "SUCCEEDED" | "FAILED" | "CANCELLED",
   handler_terminal_result_sha256: null,
   direct_no_effect_cancellation: null,
   request_fallback_evidence: ServeReplicaActionRequestFallbackEvidenceV1}
  {version: 1,
   basis_kind: "shadow",
   request_terminal_state: null,
   handler_terminal_result_sha256: null,
   direct_no_effect_cancellation: null,
   request_fallback_evidence: null}

ServeReplicaActionOutcomeV1 = {
  version: 1,
  basis: ServeReplicaActionOutcomeBasisV1,
  provider_result: ServeReplicaActionProviderResultV1,
  supersession_quiescence: null | ProviderLaunchSupersessionQuiescenceV1,
  launch_no_io_prefix: null | ServeLaunchNoIoPrefixV1
}
```

All unions are closed and discriminated before nested parsing. In a direct
basis, the basis terminal state equals the nested proof state; it is null
exactly for `proof_kind="unmaterialized"`. In a fallback basis, the basis and
nested terminal states are byte-equal. A handler basis names the exact nested
terminal-result hash stored in the correlated request. Shadow basis is legal
only in shadow columns. Unknown keys, a mismatched discriminator/null shape, or
a duplicated terminal state/hash that differs from its source rejects.
`terminal_request_unsettled` requires `request_row_disposition="retained_terminal"`,
nonnull request finish and attempt-settlement times, null current-outcome hash,
and equal settlement/cancellation times. `retained_settled_attempt` requires a
nonnull current-outcome hash and attempt-settlement time; request finish is
nonnull exactly for `retained_terminal` and null exactly for
`garbage_collected`, and `cancelled_at >= attempt_settled_at`.

`ServeLaunchNoIoPrefixV1` is a reducer-owned monotonic accumulator, not a
caller assertion. Count zero has null previous/current members and
`prefix_sha256=canonical_sha256([])` and is legal only in an unmaterialized
direct-cancellation proof/outcome. For positive count, `current_attempt` is
nonnull with `current_attempt.attempt=count`; `previous_prefix_sha256` is null
exactly at count one, and `prefix_sha256` is the canonical SHA-256 of
`{"previous_prefix_sha256": previous_prefix_sha256,
"current_attempt": current_attempt.canonical_value}`. A launch attempt gets a
nonnull `launch_no_io_prefix` exactly when its post-settlement journal is
revision-zero `NOT_STARTED` with null progress/hash and operation ID and either
it is attempt one or the locked predecessor has a valid prefix of count
`attempt-1`. The reducer embeds the complete current projection and the prior
immutable hash while holding action, predecessor, current attempt, and request
locks. Down, shadow, any nonnull/inherited cursor, and every crossed/invalid
journal have a null prefix. A direct-cancellation basis embeds a byte-equal
prefix; count equals the locked action's `current_attempt`.
The attempt projection deliberately excludes `typed_outcome` and its hash so
the outcome that contains this prefix is not recursively self-hashed; the
settlement validator independently checks the complete immutable outcome/hash.

All attempt rows remain retained with the action. At direct cancellation the
transaction locks the predecessor when count is greater than one and then the
current attempt in increasing attempt order, revalidates the current full
projection and immediate hash link, and relies on the same invariant already
checked when each immutable prior link was committed. Thus proof construction/
replay is O(1), while an offline audit can traverse every retained preimage. No
unbounded list is serialized or locked and no provider/caller-supplied hash
substitutes for evidence.

Provider error strings are diagnostic only. The closed disposition/certainty/
retry fields authorize state transitions. Secrets, credentials, raw tracebacks,
and unbounded provider payloads are redacted before persistence.
The handler can return only `ServeReplicaActionRequestReturnV1`; it cannot
return `ServeReplicaActionOutcomeV1`, an outcome basis, or
`ProviderLaunchSupersessionQuiescenceV1`. A nonnull
`supersession_quiescence` is legal only with
`basis_kind="handler_terminal_result"` and is equivalent to the reducer handing
that launch cancellation to a real down action. The companion defines its
closed proof, and the Serve transaction constructs it only after byte-comparing
the final cursor, immutable effect origins, exact terminal return, and request
fence. When nonnull, `provider_result.disposition='cancelled'`,
`provider_result.certainty='observed'`, and the provider code, retry
class/deadline, observation, and normalized message are null; it cannot
make the old launch successful. The only other exact
cancelled/observed/null-retry result is the reducer-owned direct no-effect form:
it has null operation ID, provider code, observation, and message, a direct-
cancellation basis, and null quiescence.
No shadow or request-fallback outcome may use either cancellation shape.

The private launch/down handler returns one ordinary Python mapping with the
exact `ServeReplicaActionRequestReturnV1` keys. Dedicated return-value encoders
are registered for only `serve_resource_action_launch` and
`serve_resource_action_down`; they closed-validate and return that JSON object
without default encoding, pickle, compatibility filtering, or omitted nulls.
`terminal_result_sha256` is the canonical SHA-256 of the complete nested
terminal result. The stored PostgreSQL `requests.return_value` must be that
nonnull object, canonicalize to at most 65,536 UTF-8 bytes, and round-trip
byte-equivalently through the closed decoder. Unknown/missing keys, floats,
encoder fallback/drop-to-null, or any other return type are invalid.

The terminal result's action, kind, attempt, deterministic request ID, request-
input hash, private handler name, and execution generation equal the locked
attempt/request row. Its complete worker attestation and hash equal the claim
that returns it. `reduction_kind="supersede_to_down"` is launch-only;
`reduction_kind="domain"` covers down and every launch result that is not the
typed partial-handoff handshake. `final_provider_progress_sha256` is null
exactly for the legal revision-zero pre-I/O shape; otherwise it equals the
current API006 envelope hash.

The handler/result/reducer cross-field table is exact. The tuple symbols below
name the reducer-owned final outcome after operation-ID injection; they do not
rename the immutable handler-return bytes. `OP` is the attempt's journaled
nullable provider-operation ID. The handler's `provider_result` has every
displayed field byte-equal except that its operation-ID field may be null when
`OP` is nonnull; the reducer then injects exactly `OP`. A nonnull handler field
must already equal `OP`, and any different nonnull value rejects. No other
field is injected or rewritten. `SUCCESS_OBSERVATION` is byte-equal to the
launch success observation or down absence observation in the exact terminal
cursor. For a
nonnull `normalized_provider_error`, `C` and `M` are its exact bounded nullable
provider-code and normalized-message leaves. `RC` is exactly `transient`,
`capacity`, `quota`, or `rate_limited` for the same named error category. `D`
is `min(normalized_provider_error.retry_after_seconds if nonnull else 60,
3600)`. These names denote complete provider-result tuples in field order
`(disposition, certainty, provider_operation_id, provider_code, retry_class,
retry_after_seconds, observation, normalized_message)`:

```text
Q = ("cancelled", "observed", OP, null, null, null, null, null)
S = ("succeeded", "observed", OP, null, null, null,
     SUCCESS_OBSERVATION, null)
R = ("retryable", "unknown", OP, C, RC, D, null, M)
U = ("uncertain", "unknown", OP, C, "observation_required", 60,
     null, M)
B = ("terminal_error", "unknown", OP, C, null, null, null, M)
```

| Handler reduction kind and exact final journal | Provider error / no-effect DTO | Legal final outcome provider result | Reducer result |
|---|---|---|---|
| `domain`, launch or down `SUCCEEDED` | error null; no-effect null | `S` | terminal `succeeded`, null quiescence, commit the Serve success projection |
| `supersede_to_down`, launch current-intent phase | error null; exact original-claim `N<i>` | `Q` | terminal `SUPERSEDED_TO_DOWN`, reducer builds the exact `E* + N<i>` quiescence and links one real down |
| `supersede_to_down`, launch nonintent, non-`SUCCEEDED` phase | error null; no-effect null | `Q` | terminal `SUPERSEDED_TO_DOWN`, reducer builds the exact E-only quiescence and links one real down |
| `domain`, revision-zero pre-I/O or a legal nonintent, non-`SUCCEEDED` cursor | nonnull error; no-effect null | exact row from the error-category table below | below max, `R` moves `READY` and `U` moves observation-first `READY`; at max, both take the exhaustion block; `B` moves `BLOCKED`; quiescence null |
| `domain`, launch or down current-intent phase | nonnull error; no-effect null | exact current-intent row from the error-category table below | below max, `U` remains observation-first; at max it takes the exhaustion block; `B` blocks; quiescence null |

| Exact `ProviderErrorV1.category` | Revision-zero or nonintent result | Current-intent result |
|---|---|---|
| `transient` | `R` with `RC="transient"` | `U` |
| `capacity` | `R` with `RC="capacity"` | `U` |
| `quota` | `R` with `RC="quota"` | `U` |
| `rate_limited` | `R` with `RC="rate_limited"` | `U` |
| `unknown` | `U` | `U` |
| `invalid_request` | `B` | `B` |
| `permission` | `B` | `B` |
| `conflict` | `B` | `B` |

When `n < RESOURCE_ACTION_MAX_ATTEMPT_V1`, `R` sets `next_attempt_at` from the
transaction's database time plus `D`; `U` uses exactly 60 seconds. Both retain
the existing replica status, capacity/reservation ownership, and
`ACTION_ACTIVE` cohort reference while scheduling only action attempt `n+1`.
`B` sets action `BLOCKED` with no deadline and also retains those exact Serve
rows/references; it emits the bounded operator event but does not fabricate a
Serve failure/success projection. No v1 handler-domain non-success row directly
terminates an action or replans a generation. The provider code and message are
diagnostic and cannot alter these transitions.
`provider_acknowledged`, a nonnull observation on `R`/`U`/`B`, a retry field on
`B`, a category/result mismatch, or a missing/extra normalized error rejects.

Provider success is legal if and only if the exact final cursor is
`SUCCEEDED`; an earlier cursor never infers success, and a `SUCCEEDED` cursor
accepts no non-success result. The supersession rows accept no successful
cursor. `reduction_kind="supersede_to_down"` additionally requires a nonnull
launch cursor and action-wide provider-I/O-started evidence under the
companion's current-attempt predicate. A revision-zero/null-cursor result can
never use `Q`; owner-fenced teardown uses the direct no-effect route after its
request fence, while an ordinary domain failure uses its exact `R`/`U`/`B`
row. E-only phases reject a no-effect DTO. A current-intent phase requires
exactly one DTO whose effect/role/cursor hash and immutable intent/resolution
origins satisfy the companion; a null, wrong-claim, or extra DTO is corruption.
That DTO's `resolution_origin` has this terminal result's attempt, request ID,
execution generation, and worker. Its `intent_origin` and
`resolution_origin` equal the final intent cursor's immutable original claim,
and its `intent_cursor_sha256` equals the canonical hash of that exact final
cursor. For `call_not_entered`, the terminal worker attestation/hash are also
byte-equal to that claim. For a definitive proof they may differ only by the
same execution's one legal `after` completion. A down result always has null
resolution and cannot use `reduction_kind="supersede_to_down"` or `Q`.

Because domain failures are values in `provider_result`, a valid handler return
terminalizes the generic request as `SUCCEEDED`. A generic/external
`FAILED`/`CANCELLED` terminalization, escaped exception, killed handler, or a
terminal `SUCCEEDED` row with null/invalid/mismatched return value instead uses
`basis_kind="request_terminal_fallback"`. Its `fallback_reason` is
`request_failed` exactly for `FAILED`, `request_cancelled` exactly for
`CANCELLED`, and `missing_handler_return` or `invalid_handler_return` exactly
for terminal `SUCCEEDED`. It can never establish `N<i>`, supersession
quiescence, or partial-down admission.

The fallback mapping is deterministic. V1 fixes
`REQUEST_TERMINAL_FALLBACK_DELAY_SECONDS_V1 = 60`; below the attempt maximum,
the retry result and `next_attempt_at` use that same integer and the
transaction's one PostgreSQL clock read. At the maximum, the result remains
byte-equal but the exhaustion rule sets no deadline. Define the complete
tuples:

```text
P0 = ("retryable", "observed", null, null, "transient", 60, null,
      null)
O  = ("uncertain", "unknown", OP, null, "observation_required", 60,
      null, null)
X  = ("terminal_error", "unknown", OP, null, null, null, null, null)
```

| Exact retained journal class | Provider result | Action/Serve reduction |
|---|---|---|
| `not_started_empty`: `NOT_STARTED`, null progress/hash, revision zero, null operation ID | `P0` | when below the attempt maximum, settle and move `READY` for attempt `n+1` at database time plus 60 seconds; a launch with an owner-fenced teardown request instead takes the direct no-effect row below |
| `valid_nonterminal`: any valid non-`SUCCEEDED` cursor, including an inherited cursor with the current attempt still `NOT_STARTED` | `O` | when below the attempt maximum, settle and move observation-first `READY` at database time plus 60 seconds; retain all capacity/cohort references and admit no down even when teardown is pending |
| `valid_succeeded`: exact fully validated `SUCCEEDED` cursor | `S` | settle and commit terminal `succeeded`; a pending teardown subsequently uses the normal completed-launch basis, never partial handoff |
| `invalid`: malformed/cross-bound cursor, hash/revision mismatch, or impossible watermark/progress combination | `X` | settle to operator-visible `BLOCKED`; retain evidence/references and admit no retry, release, or down |

The `invalid` classifier operates on the locked, outer-schema-bounded raw
attempt row before the domain cursor parser. It copies no malformed progress
object into the fallback outcome; it records only the bounded watermark,
declared hash/revision, operation ID, and literal invalid classification, while
the original row remains retained for repair. A row that violates PostgreSQL
outer CHECKs or whose identity/request binding cannot be decoded is database
corruption outside this typed fallback and remains unreduced under operator
quarantine.

External request failure therefore does not override a claim-fenced durable
success checkpoint: it is success exactly in the `valid_succeeded` row. It also
never makes an earlier cursor successful. The fallback evidence stores the
exact terminal state, finish time, journal classification, watermark,
revision/hash, and operation ID used by this table; the outcome validator
recomputes the classification from the locked attempt rather than trusting the
serialized enum.

After selecting and operation-ID-normalizing the final provider tuple, the
reducer assigns `launch_no_io_prefix` by this exhaustive rule. A launch
settlement with the exact revision-zero `NOT_STARTED`/null-progress/null-
operation journal appends the current projection to the predecessor prefix;
this includes handler-domain pre-I/O `R`/`U`/`B`, fallback `P0`, and the newly
settled direct-cancellation variant. An unmaterialized direct cancellation uses
the count-zero prefix, and a retained-settled direct cancellation copies the
current attempt's already committed nonnull prefix byte-for-byte. Down, shadow,
handler `S`/`Q`, fallback `O`/`S`/`X`, every nonnull or inherited cursor, and
every crossed or invalid journal use null. No other combination is legal.

The attempt-domain exhaustion override is also exact. If a settlement would
otherwise produce handler `R`/`U` or fallback `P0`/`O` while
`n=RESOURCE_ACTION_MAX_ATTEMPT_V1`, the reducer persists that same typed outcome
and any prefix dictated above to the current attempt and action, retains every
Serve/capacity/cohort reference, sets the action to nonterminal `BLOCKED`, sets
`next_attempt_at=null`, and emits the bounded operator event code
`attempt_domain_exhausted`. It does not construct attempt `n+1`, change the
provider tuple to `B`/`X`, terminalize or replan the action, or advance the
desired generation. The owner-fenced direct cancellation route still takes
precedence for an eligible no-I/O launch with teardown requested. Settled replay
re-adopts the same blocked projection without another event. This rule is the
only v1 case where an `R`/`U`/`P0`/`O` provider tuple maps to `BLOCKED`.

For the Serve domain, the transaction that first settles current attempt `n`
writes byte-equal `ServeReplicaActionOutcomeV1` bytes/hashes to that attempt's
`typed_outcome` and the action's `last_result` after the one legal operation-ID
injection. Attempt outcomes are immutable history. A later attempt settlement
or retained-attempt direct cancellation may replace only the action's mutable
latest `last_result`; it never rewrites an earlier settled attempt. While `n`
remains the latest settled attempt and the action has not taken a later direct
transition, the two values are equal. After `current_attempt`/revision advances,
replay of `n` is stale and validates its own retained outcome rather than the
new action result. A direct cancellation before any attempt has no attempt
outcome and stores its closed outcome only in `last_result`.

Shadow JSON fields are bound to named closed types, not merely size/hash
checks. `actual_outcome` and `proposed_outcome` are
`ServeReplicaActionOutcomeV1`; `pre_observation` and `post_observation` are
`ProviderLifecycleObservationV1`; and `invocation` is the companion design's
`ServeShadowAttemptInvocationV1`. Outcome validation receives the bound
`(request_role, parent_spec, invocation)` tuple. The cleanup-only union member
uses down absence semantics against the frozen parent target, while its
observations come from the shadow pre/post observer rather than
`ProviderLifecycleFacet.observe()`.

Shadow actual/proposed outcomes always have
`basis_kind="shadow"`, `supersession_quiescence=null`,
`launch_no_io_prefix=null`, and place their closed provider fields under
`provider_result`; shadow completion never fabricates a handler terminal-return
envelope, request-fallback evidence, direct-cancellation proof, accumulator, or
reducer quiescence. Authoritative outcome validation rejects a shadow basis,
and shadow validation rejects every authoritative basis.

Parent projections use:

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
after the provider-I/O watermark crosses `NOT_STARTED` terminalizes the request
as ambiguous. Any retry
is action attempt `n+1`, whose handler observes the frozen target before it may
mutate. The controller-action reservation table remains orthogonal: it keeps
fencing non-replayable controller-class requests, while these launch/down
requests use the normal executor class and the action-attempt correlation.

Generic request terminalization updates only the request, queue, and existing
operational event; it does not acquire action/attempt/domain locks or snapshot
action evidence. Request terminal states are immutable. A later reducer
transaction takes the lock classes above through the attempt row, then reads
the correlated request without `FOR UPDATE`. Under PostgreSQL `READ COMMITTED`,
an uncommitted terminal transition is seen as nonterminal and reduction simply
retries later. Once terminal, the reducer validates the correlation and
request-input hash. For terminal `SUCCEEDED` with an exact valid return it
closed-decodes and hashes `ServeReplicaActionRequestReturnV1`, copies the
provider result, and constructs a handler-basis outcome. Every other terminal
shape constructs the exact request-fallback basis and applies the literal
journal-class table above. Neither route trusts request status as provider
success or failure: only an exact `SUCCEEDED` cursor yields `S`. The direct
owner-fenced no-I/O cancellation below is a third reducer-owned route and takes
precedence only when teardown is requested and its action-wide proof passes.
The reducer snapshots terminal state/final outcome/provider evidence into the
attempt while updating the action and Serve state atomically. Request GC cannot
remove the source row before this snapshot because both its candidate query and
delete predicate exclude an unsettled correlated attempt.

The reducer transaction locks current Serve controller leadership, matching
service/replica rows, matching capacity/reservation rows, the frozen cohort and
same-ID reference, action, and attempt in that order. It revalidates action revision and the replica's current action
link/teardown generation, then does exactly one of:

- commit Serve success projection and action `TERMINAL`;
- commit `R`/`U` retry only after validating either (a) a nonnull legal
  inheritable provider cursor or (b) the exact pre-I/O shape
  `provider_io_boundary='NOT_STARTED'`, null provider progress/progress hash,
  progress revision zero, and null provider operation ID. For (a), a
  `NOT_STARTED` watermark is legal only for the exact inherited pre-I/O seed:
  local revision one, cursor byte-equal to the locked predecessor, null worker
  attestation, and null current-attempt provider operation ID. Both shapes
  require a typed outcome that independently authorizes retry or observation.
  A crossed provider-I/O watermark with null progress, or any malformed or
  predecessor-mismatched `NOT_STARTED` seed, is corruption and blocks
  reduction. For either legal shape below the attempt maximum, increment no
  attempt yet, set action `READY`, and set `next_attempt_at` from PostgreSQL
  time plus the exact `R`/`U` delay. At the maximum, take the exact exhaustion
  override above instead;
- commit `B`/`X` as `BLOCKED` for an identity conflict, invalid contract, or
  quarantine requiring repair, retaining the current Serve projection and
  references; or
- commit only the exact `Q` supersession or direct no-effect cancellation
  terminal transitions defined above/below. V1 has no other handler-domain
  non-success terminal transition.

A terminal action transition also changes its exact `ACTION_ACTIVE` reference
to `RELEASED` in that transaction after proving every correlated attempt/request
settled. `READY` and `BLOCKED` retain the active reference because later retry or
operator repair can still require the frozen worker cohort.

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

The first retry reduction reads one fresh PostgreSQL clock value after locking,
computes and stores exactly one `next_attempt_at`, snapshots the attempt, and
increments the action revision. A byte-equal replay of that settled attempt
with the action still at the resulting revision/current attempt returns the
stored projection and deadline without invoking the callback or reading a new
clock. Replay after `current_attempt` or revision advances rejects as stale.
Thus a response loss cannot move a retry deadline.

Backoff is database-clock based. V1 handler-domain `R` uses its exact capped
`D`; handler/fallback `U`/`O` and fallback `P0` use the fixed delays above and
add no jitter. A later version that adds jitter must derive it deterministically
from `(action_id, attempt)` so restart cannot move a committed deadline.
Serve—not the generic kernel—selects retry class, maximum delay, and whether
observation is required.

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
resource absent by exact NotFound reads for every frozen object name. A
same-name replacement or different identity is a conflict, never an alternate
absence proof. Recoverable uncertainty returns to observation-first `READY`
with a database-clock deadline while the finite attempt domain remains.
`BLOCKED` is reserved for a conflict/quarantine or the exact attempt-domain
exhaustion that requires repair; there is no time- or failure-count-based
cleanup give-up deadline.

A launch with a complete nonnull no-I/O prefix is not superseded by a down
action. The owner-fenced teardown transaction accepts exactly three direct
proof variants:

- `unmaterialized` requires `current_attempt=0`, no attempt/request row, the
  count-zero prefix, and the exact locked launch identity/action revision;
- `terminal_request_unsettled` requires the current attempt not yet settled,
  an exact terminal correlated request with no active claim, and the exact
  revision-zero `NOT_STARTED`/null-progress/null-operation journal. The
  transaction uses its one database timestamp to settle that attempt, builds
  its `ServeLaunchNoIoAttemptProjectionV1` and next prefix under the predecessor
  locks, and sets proof `attempt_settled_at=cancelled_at`;
- `retained_settled_attempt` requires the current retained attempt already
  `SETTLED`, its immutable typed outcome/hash to parse and contain the exact
  nonnull no-I/O prefix of count `current_attempt`, and the action still to be
  nonterminal at that same current attempt. Its prior action `last_result` must
  equal that current attempt outcome unless an explicitly typed same-attempt
  operator transition is later added; v1 has none. The correlated request may
  be `garbage_collected`. If retained, it must be terminal, unclaimed, and
  byte-equal to the attempt's request ID/state; `request_finished_at` is its
  timestamp. If absent, `request_row_disposition="garbage_collected"` and
  `request_finished_at=null`; the settled attempt's immutable request snapshot
  and `attempt_settled_at` are authority.

For both materialized variants, request ID/state, journal fields, prefix, and
current attempt equal the locked attempt. The retained-settled proof additionally
embeds its existing `typed_outcome_sha256` and existing `settled_at`; its fresh
`cancelled_at` is the new action terminal time and may be later. An inherited
nonnull cursor, a nonzero progress revision, null/invalid prefix, or any crossed
predecessor therefore categorically rejects `CANCELLED_NO_EFFECT`. All variants
bind the exact launch identity and derived action ID;
`source_action_revision` is the locked pre-transition revision, and the
committed terminal action revision is exactly one greater.

On any exact proof the reducer constructs the direct-cancellation-basis
`ServeReplicaActionOutcomeV1` with provider tuple
`("cancelled", "observed", null, null, null, null, null, null)`, null
quiescence, and `launch_no_io_prefix` byte-equal to the proof. Conversely
`terminal_disposition='CANCELLED_NO_EFFECT'` requires that exact outcome and
proof. `unmaterialized` writes only action `last_result`.
`terminal_request_unsettled` writes byte-equal outcome bytes to the newly
settled current attempt and action. `retained_settled_attempt` never overwrites
the historical attempt outcome; it writes the later cancellation only to action
`last_result`. The same transaction releases the counted `PROVISIONING` slot
and exact action-owned capacity/reservation claim and `ACTION_ACTIVE` cohort
reference once, and removes the action-owned provisional replica row under the
owner/incarnation fence. It creates no down action, down link, cleanup target,
prior-launch basis, provider intent origin, or
`ProviderLaunchNoEffectResolutionV1`; no provider intent existed. Lost-response
replay adopts that one terminal action projection and cannot release capacity
or rewrite attempt history twice.

A real down action may supersede a nonterminal launch of the same replica
incarnation only after its provider-I/O watermark crossed. Request fencing alone is
not a quiescence proof: the old launch remains observation-only authority until
every emitted Kubernetes/Skylet effect intent has exact committed effect
evidence, a companion-defined definitive-no-effect completion proving the
entered call cannot later take effect, or authoritative proof that the call was
never entered, and no handler/claim can still emit it. In particular, an
unresolved `CREATE_INTENT` or `JOB_INTENT` cannot hand off; it remains
observation-first until exact evidence advances the cursor or the provider
contract proves the effect cannot still take place. A point-in-time NotFound
never supplies that proof.

Only the original effect claim may return the companion's closed
`ProviderLaunchNoEffectResolutionV1`; a later attempt or execution generation
can exact-adopt committed evidence but cannot assert `call_not_entered` or a
call-specific definitive-no-effect proof for the inherited entrant. The
handler returns the exact `ServeReplicaActionRequestReturnV1`, and generic
request terminalization records only that return plus its own terminal/no-
active-claim facts. It never constructs quiescence.

The Serve reducer requires terminal `SUCCEEDED`, the exact hash-valid handler
DTO, and the final API006 cursor. It embeds/hashes every complete committed-
effect record from that cursor, validates any one current no-effect resolution
against the cursor's immutable intent origin, copies the request envelope's
exact `terminal_result_sha256`, and constructs
`ProviderLaunchSupersessionQuiescenceV1` inside the owner-fenced attempt-
outcome transaction. External terminalization or a missing/invalid DTO is
ineligible. First settlement and lost-ack replay are two branches of the same
transaction and lock program. Both acquire the sorted union of source-launch
and deterministic-down action IDs before locking the named source attempt. In
the first branch that unsettled locked attempt protects its request from GC;
the reducer then nonlocking-reads and validates the terminal request and its
no-active-claim facts, constructs and persists quiescence, terminalizes the
source as `SUPERSEDED_TO_DOWN`, and inserts/adopts and links the down action in
one commit. The replay branch requires the already-settled attempt's retained
request snapshot, outcome, and quiescence to be byte-equal, then exact-adopts
that same down action and link; the original request may already be GCed, and a
surviving row is compared nonlocking but is not required.
No branch may terminalize the source in one transaction and admit the down in a
later transaction. Both freeze the typed partial-launch basis from the exact
API006 cursor and quiescence hashes and require the re-derived cleanup target to
equal the sole copy in the down capsule. The down uses
the normal PR #1070 request/claim path to exact-read all three names, extend
only unknown UID commitments, UID-precondition-delete matching objects, prove
all three absent, and remove any byte-equal same-UUID cluster row. There is no
hidden provider cleanup inside a launch retry and no second scheduler. A
replan launches only after this real down action succeeds. This is
Serve-specific precedence through the replica links and teardown generation,
not a generic dependency graph. A late launch reducer cannot project the
replica READY after the teardown generation wins.

Reducer guards are action-specific and fail closed. Launch success requires a
matching authoritative `present` observation and an API006
`ProviderLifecycleProgressV1.cursor` at `SUCCEEDED` whose exact handle, runtime,
durably running same-key Skylet job, and endpoint evidence satisfy the
companion's launch-readiness proof. Down success requires a cursor at
`SUCCEEDED` reachable only through `TARGET_RESOLVED ->
(DELETE_INTENT(role) -> DELETE_PARTIAL)* -> ABSENCE_EXACT ->
HANDLE_REMOVE_INTENT -> HANDLE_REMOVED -> SUCCEEDED`, retaining the exact cleanup target,
handle-removal proof, and authoritative `absent` observation required by those
transitions.
Provider acknowledgement alone terminalizes neither. Cancellation after
`INTENT_COMMITTED` remains ambiguous until observation makes a terminal
projection safe.

## Serve integration

Serve migrations 032 and 033 are additive. The already-frozen revision 032
adds:

- `services.resource_action_mode` with permanent `legacy` default and
  `resource_action_mode_changed_at` for the promotion window;
- nullable `replica_incarnation`, `desired_generation`,
  `sky_cluster_record_uuid`, current launch/down action IDs, and current
  launch/down represented-sample IDs on replicas; and
- bounded logical-sample and represented per-legacy-request-attempt shadow
  tables.

Revision 033 has `down_revision='032'`. It adds the two nullable replica
coverage-link columns on both supported Serve dialects. On PostgreSQL only, it
creates the worker-cohort registry/reference (including the nonnull preparation-
capability SHA-256 commitment), decision-coverage, and coverage-only submission
tables, their checks and indexes, explicitly adds
`shadow_samples.would_be_action_id -> shadow_coverage.decision_id ON DELETE
RESTRICT`, adds nullable pair-checked `legacy_effect_trace`/hash columns to the
represented-attempt table, and updates the replica checks and partial unique
indexes. It does
not depend on `metadata.create_all(checkfirst=True)` to alter an existing
table. Because no runtime shadow writer has ever been activated on revision
032, the 033 transaction first asserts that both existing shadow tables are
empty. A nonempty installation fails closed for a separately reviewed
backfill; migration never synthesizes coverage for prior samples. Revision 032
is not rewritten and neither revision supports schema down.

Revision 033 remains unshipped while this schema is being authored, so its
fresh-table definition is updated in place. Deployment preflight must prove
that no target database was stamped with an earlier 033 shape. If that premise
is false, rollout stops and uses a new revision 034 that requires an empty
worker-reference table before adding the nonnull column; no default or
synthetic capability commitment is truthful.

The two nonexecuting cohort-retention tables are:

```text
serve_resource_action_worker_cohorts
  cohort_id                 TEXT primary key
  deployment_uid            TEXT not null unique
  cohort_identity           JSONB not null
  cohort_identity_sha256    TEXT not null
  registration_attestations JSONB not null
  registration_attestations_sha256 TEXT not null
  lifecycle_state           TEXT not null
                              # REGISTERING | ACCEPTING | DRAINING |
                              # REMOVAL_AUTHORIZED | RETIRED
  revision                  BIGINT not null
  created_at                TIMESTAMPTZ not null
  state_changed_at          TIMESTAMPTZ not null
  retired_at                TIMESTAMPTZ nullable

serve_resource_action_worker_cohort_refs
  decision_id               UUID primary key
  cohort_id                 TEXT not null references
                              serve_resource_action_worker_cohorts(cohort_id)
                              on delete restrict
  service_hash              TEXT not null
  replica_incarnation       UUID not null
  desired_generation        BIGINT not null
  action_type               TEXT not null
  controller_owner_fence    TEXT not null
  lifecycle_epoch           BIGINT not null
  preparation_capability_sha256 TEXT not null
  reference_state           TEXT not null
                              # PREPARING | SHADOW_ACTIVE |
                              # ACTION_ACTIVE | RELEASED
  revision                  BIGINT not null
  created_at                TIMESTAMPTZ not null
  bound_at                  TIMESTAMPTZ nullable
  released_at               TIMESTAMPTZ nullable
```

`cohort_identity` is the complete bounded
`ProviderAuthorityWorkerCohortV1`; typed insert/adopt/read recomputes its
canonical hash. Each ID permanently names one Deployment UID and identity and
is never reused. `registration_attestations` is the companion's bounded
`ProviderAuthorityWorkerRegistrationSetV1`; typed writes recompute its hash and
permit only distinct, current Pod registrations for the immutable cohort.
At an activation or rollback transition, both each registration's
`registered_at` and its embedded worker identity's `observed_at` must be at or
before the transaction's fresh PostgreSQL `clock_timestamp()` and no more than
five minutes old. The bound is server-owned and not configurable in M2/M3.
Insertion creates `REGISTERING`, never `ACCEPTING`. `REGISTERING -> ACCEPTING`
requires exactly two matching ready-worker attestations and the exact
Deployment's current observed generation with desired/ready/available replicas
all two. Normal retirement is `ACCEPTING -> DRAINING -> REMOVAL_AUTHORIZED ->
RETIRED`. A never-accepted failed cohort may take `REGISTERING ->
REMOVAL_AUTHORIZED`; rollback takes `DRAINING -> ACCEPTING` only in the same
transaction that replaces the registration set with two current matching
attestations while the exact Deployment and ServiceAccount still exist. New
references require the locked cohort to be `ACCEPTING`; existing references
remain executable while it is `DRAINING`.
Reference transitions are `PREPARING -> SHADOW_ACTIVE |
ACTION_ACTIVE | RELEASED`, then either active state to `RELEASED`; no reverse
transition exists. Rows authorize no execution, claim, retry, or due work. No
timeout alone releases a reference, and cohort tombstones remain permanently.
Row-local checks enforce canonical UUID/generation/hash/state/timestamp shapes,
including a lowercase SHA-256 preparation-capability commitment;
typed code enforces the complete transition graph. A partial index on
`(cohort_id, decision_id)` for `reference_state != 'RELEASED'` drives retirement,
and registry state has its own operational index. `REMOVAL_AUTHORIZED` requires
zero active references plus a defensive scan finding no nonterminal action,
private shadow request, or shadow evidence carrying that cohort without a
matching active reference. `RETIRED` additionally requires exact Deployment
and ServiceAccount NotFound after authorized removal. That check is performed
by the still-running API-role retirement verifier, not by the removed cohort.
The current chart retains tombstone-scoped GET permission for the two exact
names through this check and prunes it only after the `RETIRED` commit.

The preparation reference snapshots the exact controller owner fence,
lifecycle epoch, and SHA-256 commitment of a fresh 32-byte random preparation
capability. The raw 64-lowercase-hex capability exists only in the same live
preparation cell, is compared in constant time, and is never stored, logged, or
placed in an action/source/proof. It authorizes only read-only launch-identity
canonicalization while this exact reference remains `PREPARING`; every other
endpoint and state rejects it. Releasing `PREPARING`
requires either the same live cell to close its nonce or an owner-fenced
recovery transaction to prove the stored fence/epoch stale (advancing the epoch
when needed), plus absence of coverage, action, and private request state. Thus
an old cell cannot receive valid authorization after its retention fence is
released.

The retained proof commits the historical `PREPARING` revision-one snapshot
and capability hash. The represented-admission transaction validates that
snapshot against the locked row and atomically stores the proof while changing
the row to `SHADOW_ACTIVE` or `ACTION_ACTIVE`. Later recovery requires the
same-ID row's immutable fields/hash and exact kind-matched legal successor
revision/state; it does not incorrectly require the current row to remain
`PREPARING` and never needs the raw capability.

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
  launch_shadow_coverage_id         UUID nullable
  down_shadow_coverage_id           UUID nullable
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
databases, whose rows stay `legacy`: revisions 032 and 033 add only their inert
existing-table columns on both supported Serve dialects so current metadata
remains queryable, but create no shadow table on SQLite. The shadow tables and every
action-aware helper are PostgreSQL-only and fail closed on another dialect. A
separate SQLAlchemy metadata owns those tables so revision 001/current-Base
bootstrap cannot accidentally create them in a fresh SQLite database. Existing
rows retain null replica identity/link columns: migration must not mint an identity
that is absent from the live provider resource. New action-aware replicas get
the three identity fields together, with generation one. Row-local checks
require the identity triple to be all null or all nonnull, a positive
generation, and an identity for every nonnull action or shadow link. For each
action kind, the authoritative and shadow link cannot both be nonnull. Partial
unique indexes prevent one action ID, coverage ID, sample ID, replica
incarnation, or cluster-record UUID from being attached to multiple live rows.
A shadow sample link requires the matching coverage link, while a durable
`NOT_REPRESENTABLE` decision has only the coverage link. There is no
Serve-to-API foreign key because supported deployments may keep the two state
stores separate; consolidated PostgreSQL on one physical connection is checked
at runtime before authority.

The new identity, generation, provider-target, and action/shadow-link columns
are action-owned. Existing generic replica upserts currently replace every
non-primary-key column from `EXCLUDED`; Serve032 and Serve033 change all ordinary, batch,
paid-capacity, and reserved-fill conflict updates to exclude the action-owned
set. Legacy inserts may still create null action fields, but routine status
persistence can never erase or replace an existing identity/link. Only typed,
owner-fenced transition/admission methods may initialize an identity, advance
a generation, or change its current link.

The coverage table is one immutable row for every capacity-approved launch or
durably admitted teardown decision made while a service is in `shadow`,
including a decision that cannot produce a provider invocation. Its
`decision_id` is exactly the `action_id` UUIDv5 derived from the
provider-independent `ResourceActionIdentityV1` above, not a second identity.
The manager mints the replica incarnation/generation in the in-memory decision
draft before preparation; admission persists or discards that draft atomically.

The exact coverage shape is:

```text
serve_resource_action_shadow_coverage
  decision_id               UUID primary key
  service_name              TEXT not null
  service_hash              TEXT not null
  service_incarnation       UUID not null
  replica_id                BIGINT not null
  replica_incarnation       UUID not null
  desired_generation        BIGINT not null
  action_type               TEXT not null
  normalizer_contract_version SMALLINT not null  # 1
  normalization_outcome     TEXT not null  # REPRESENTABLE | NOT_REPRESENTABLE
  not_representable_reason  TEXT nullable
  worker_cohort_ref_id      UUID nullable references
                              serve_resource_action_worker_cohort_refs(decision_id)
                              on delete restrict
  admitted_at               TIMESTAMPTZ not null

  unique (service_hash, service_incarnation, replica_id,
          replica_incarnation, desired_generation, action_type)
```

`REPRESENTABLE` requires a null reason; `NOT_REPRESENTABLE` requires one closed
reason from the companion provider contract. The row stores no raw request,
YAML, configuration, hash of secret-bearing input, or free-form detail. Typed
insert, read, and replay recompute
`decision_id = UUIDv5(ResourceActionIdentityV1)` and require the same identity,
contract version, outcome, and reason; they never change a reason or convert an
unsupported row into a represented row in place. A changed decision advances
generation. PostgreSQL CHECKs enforce only row-local bounds and enums,
canonical `service_hash = service_incarnation::text`, positive generation, and
the outcome/reason pairing; PostgreSQL does not recompute UUIDv5. Indexes cover
the service promotion window, every `NOT_REPRESENTABLE` row, and unlinked
retention. Typed-boundary tests supply a wrong UUID for otherwise valid fields
and require rejection.

A coverage row routed through `serve_shadow_candidate_launch` or
`serve_shadow_candidate_down` requires `worker_cohort_ref_id=decision_id` and a
locked `SHADOW_ACTIVE` reference. Typed request binding requires its decision
ID, cohort ID, Deployment UID, complete resolved-cohort identity, and internal
request payload to agree. A represented parent carries that same resolved
cohort inside its immutable spec. An authoritative action instead requires the
same-ID reference in `ACTION_ACTIVE`; changing the frozen cohort changes the
immutable provider plan and therefore requires a new desired generation and
decision ID.

A representable decision's shadow parent has that same `decision_id` as
`would_be_action_id` and references coverage with `ON DELETE RESTRICT`; coverage
is inserted first. Its replica coverage and sample links therefore contain the
same UUID. A not-representable decision has only the coverage link and no fake
parent. Coverage is immutable after insert.

A not-representable decision uses a provider-neutral submission ledger rather
than a fake provider parent:

```text
serve_resource_action_shadow_coverage_attempts
  decision_id               UUID not null references
                              serve_resource_action_shadow_coverage(decision_id)
                              on delete cascade
  request_sequence          INTEGER not null
  logical_attempt           INTEGER not null
  request_role              TEXT not null  # PRIMARY_LAUNCH | PRIMARY_DOWN |
                                             LAUNCH_CLEANUP_DOWN
  phase                     TEXT not null  # PRE_SUBMIT | REQUEST_BOUND |
                                             COMPLETE | ABANDONED_PRE_SUBMIT |
                                             REQUEST_ASSOCIATION_UNKNOWN
  legacy_request_id         TEXT nullable
  terminal_request_status   TEXT nullable  # SUCCEEDED | FAILED | CANCELLED
  retry_disposition         TEXT nullable  # RETRY_SAME_DECISION | TERMINAL |
                                             REPLAN_NEW_GENERATION | BLOCK
  admitted_at               TIMESTAMPTZ not null
  request_bound_at          TIMESTAMPTZ nullable
  completed_at              TIMESTAMPTZ nullable
  updated_at                TIMESTAMPTZ not null

  primary key (decision_id, request_sequence)
  unique (legacy_request_id) where legacy_request_id is not null
```

This ledger stores no invocation, provider plan/outcome, raw error, config,
request bytes, or secret-bearing hash. Its phase checks mirror the represented
attempt table. `PRE_SUBMIT` has no ID/completion; `REQUEST_BOUND` has a real
ID/bind time and no completion; `COMPLETE` has ID/bind/completion and closed
terminal/retry fields; `ABANDONED_PRE_SUBMIT` has the proved-no-call shape; and
`REQUEST_ASSOCIATION_UNKNOWN` has no ID and is permanently
promotion-blocking. Counters are positive, and each table has its own partial
unique request-ID index. Typed binding first locks the applicable
already-created `PRE_SUBMIT` evidence row, then locks the exact API request
row. While holding that request row as the cross-table serialization key, it
performs nonlocking reads of both attempt tables, validates that no other row
owns the ID, and writes the applicable row. For a represented launch it also
closed-decodes the persisted `LaunchBody` and compares its effective user
name/hash with the immutable source proof in the same transaction: equality
writes `REQUEST_BOUND` with no identity divergence; mismatch writes
`REQUEST_BOUND` plus write-once `IDENTITY_MISMATCH`. Later completion must
preserve a nonnull binding-time divergence byte-for-byte. Every binder follows
this protocol. Once the correct-kind unowned request is found,
missing/malformed identity takes the mismatch branch and cannot roll binding
back after SDK admission; a missing row, wrong kind/correlation, or ID owned by
another child remains an association conflict. Thus a waiter observes the
winner after acquiring the request lock without taking an earlier-class row
lock. A partial stale index covers `PRE_SUBMIT` and `REQUEST_BOUND`.

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
  would_be_action_id        UUID primary key references
                              serve_resource_action_shadow_coverage(decision_id)
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
with `ON DELETE CASCADE` points to the logical sample because the sample and
attempts share one retention boundary; the sample's restrictive coverage
foreign key is removed only by typed retention after all live replica links and
protected windows are gone. None of the four evidence tables references a service,
replica, API request, or real action, so the evidence survives row deletion and
request garbage collection.

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
  legacy_effect_trace       JSONB nullable
  legacy_effect_trace_sha256 TEXT nullable
  divergence_class          TEXT nullable
  admitted_at               TIMESTAMPTZ not null
  request_bound_at          TIMESTAMPTZ nullable
  completed_at              TIMESTAMPTZ nullable
  updated_at                TIMESTAMPTZ not null

  primary key (would_be_action_id, request_sequence)
  unique (legacy_request_id) where legacy_request_id is not null
```

Both counters are positive. Every JSON/hash pair, including the companion's
closed `LegacyProviderEffectTraceV1`, is pair-null and canonically bounded.
`REQUEST_BOUND` requires API-request execution, a real ID and bind
timestamp, and no completion timestamp. `COMPLETE` requires a completion
timestamp and, for API-request execution, a real ID/bind timestamp.
For a represented launch, null `divergence_class` at `REQUEST_BOUND` attests
that the locked request's effective identity equals the retained canonicalizer
proof; `IDENTITY_MISMATCH` attests the opposite and is write-once through
completion. No other divergence may be assigned at request binding.
`ABANDONED_PRE_SUBMIT` requires no request ID, operation ID, actual outcome, or
post-observation. `REQUEST_ASSOCIATION_UNKNOWN` requires API-request execution,
a null request ID, and a completion timestamp. `legacy_direct_down` may finish
without an ID but is always divergent/promotion-blocking. For a represented
Kubernetes candidate, typed completion requires a nonnull
`LegacyProviderEffectTraceV1`; `MATCH` additionally requires the exact effect
sequence/body/job equality from the companion contract. SQL enforces only the
pair-null shape because profile-specific trace validation belongs to typed
code. Contiguous sequence
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
index. Coverage has a promotion index on
`(service_name, service_hash, admitted_at, decision_id)` and a partial blocker
index for `normalization_outcome='NOT_REPRESENTABLE'`.
`divergence_class`, when nonnull, is one of the parent divergence enums other
than `PENDING`, `MATCH`, `ABANDONED`, and `AMBIGUOUS`.

Admission uses one physical PostgreSQL connection. In authoritative mode the
transaction that changes replica/capacity intent also locks the complete
resolved cohort and same-ID preparation reference, changes `PREPARING ->
ACTION_ACTIVE`, inserts/adopts the action, and links its ID. If any write fails,
none commits.

In shadow mode the legacy thread remains the sole mutation owner. Every
capacity-approved launch and every durably admitted teardown first writes its
coverage row in the same transaction as replica/capacity intent. A represented
decision also inserts or exactly adopts the logical `PENDING` sample and links
both IDs; a not-representable decision links only coverage and stays on the
legacy path. Ordinary, paid-capacity, and reserved-fill admission borrow that
transaction; denial commits no replica intent, coverage, parent, or link.
Teardown admission advances the generation and records coverage, plus a parent
when represented, in the transaction that durably commits teardown intent.

Resource-action launch and down preparation use an explicit one-shot handshake;
creating a preparation worker is not provider enqueue. For launch, the manager first chooses a
provisional placement and preallocates the replica-incarnation and
cluster-record UUIDs in memory, derives the decision ID, and generates one
fresh 32-byte preparation capability. It selects the rendered active manifest, then
in a suffix-only transaction locks the matching complete, attested `ACCEPTING`
cohort row and inserts or exactly adopts the decision's `PREPARING` reference
with the capability hash. Exact adoption requires the same hash and all other
reference bytes; a process-loss recovery creates a new generation/reference,
not a replacement capability under the old row. Only then may the same bounded
preparation worker, separate from the provider-submit semaphore, first call the
private API launch-identity canonicalizer with the raw capability, construct
the retained source/name basis from its proof, then call authority preflight
and produce
either a canonical `PreparedProviderLaunchV1` capsule or the companion's closed
not-representable result. It has no SDK mutation callable and cannot enter
`_launch_thread_pool`. Failure to acquire the reference closes the mutation
gate. No PostgreSQL row lock is held during preflight, policy/config projection,
Kubernetes reads, file work, or a preparation-queue wait.

Down uses the same retention fence before its private preflight. From an
optimistic retained-launch/replica snapshot, the manager constructs the down
identity, frozen target, and prior-basis candidate, selects the rendered active
manifest, and inserts/exactly adopts its `PREPARING` reference under the locked
`ACCEPTING` cohort. Down admission later locks service -> replica -> applicable
capacity/reservation -> cohort -> reference -> coverage/optional parent ->
action, revalidates every optimistic
source byte under the canonical action-row order, and atomically changes the
reference to `SHADOW_ACTIVE` or `ACTION_ACTIVE`. It creates no launch slot or
capacity reservation. Failure or denial releases only with the same proved
pre-call/owner fence as launch.

The manager then runs one short transaction whose locking order is service ->
replica -> capacity -> cohort -> reference -> coverage -> optional parent.
After those locks it changes `PREPARING -> SHADOW_ACTIVE`, writes coverage with
the reference FK, then the same-ID parent when representable, and links. It revalidates
service ownership and lifecycle epoch, the provisional placement, and any
paid/reserved capacity fence. For a represented launch it additionally requires
the source proof's complete reference context and capability hash to be byte-
equal to the locked reference; the raw capability is neither required nor
persisted at admission. An approved launch also persists
`sky_launch_status=RUNNING` and the derived indexed
`replicas.status='PROVISIONING'` in that same commit. That is the exact state
counted by `in_flight_launch_count()`, so the provider slot is durably occupied
before either the cross-process resources-file lock or SQL locks are released.
The manager increments its same-tick local in-flight delta only after this
commit. For action-aware entries, launch-cap evaluation under the cross-process
resources-file lock reads the indexed PostgreSQL `PROVISIONING` rows. The same-
tick local delta is keyed by decision ID and contains only committed IDs absent
from the manager snapshot being counted; snapshot adoption removes them exactly
once, and counts deduplicate by decision ID. Process-local state is never the
cross-process capacity authority. On denial it tells the worker to discard the
preparation and closes its context; the same transaction changes the
`PREPARING` reference to `RELEASED`, and writes no replica, slot, coverage,
parent, or link.

After commit or exact lost-commit readback, the manager sends a one-use
authorization containing the service hash, lifecycle epoch, decision ID, a
process-local unguessable preparation nonce, and, when represented, the exact
stored spec and invocation hashes. Only after receiving that authorization may
the worker enter the existing provider-submit pool. A commit-before-signal
crash intentionally leaves a counted `PROVISIONING` slot. Owner-fenced recovery
either re-prepares and adopts it, or commits proved pre-call abandonment/failure
together with capacity release and slot removal. No elapsed-time-only cleanup
may clear the slot. Thus a preparation task may exist before durable admission,
but no legacy mutation is runnable or queued before coverage, replica intent,
and the counted slot commit.

For a represented decision, durable authority is the full stored canonical
`ServeReplicaActionSpecV1`: its `ProviderLifecyclePlanV1` and
`ProviderLifecycleInvocationV1`, including retained-source,
execution-config/scope, template/inventory references, and their hashes.
`request_payload_sha256` remains exactly the invocation hash. A
`PreparedLaunchRequest`, generic HTTP/request-body bytes, credentials, and
transport ephemera are neither persisted nor hashed and are not durable
authority. The live worker projects its process-local request back to the
stored invocation immediately before `PRE_SUBMIT`. Cross-process recovery
rebuilds only from the retained source and must reproduce the complete stored
spec/invocation byte-for-byte. A mismatch abandons or blocks that generation;
it never submits altered input under the old identity.

Launch's provider-affecting user identity is established before represented
admission by the companion design's private no-enqueue API canonicalizer. It
accepts only the prepared identity slice plus complete logical resource
identity and shares the exact effective-identity resolver used by
`prepare_request_async()`: authenticated requests use `auth_user.name/id`, and
the legacy no-auth case uses the submitted pair. Its closed proof and effective
pair are retained in `ProviderLaunchSourceV1`; the pure projector builds the
launch name basis/capsule from that pair, so recovery can reproduce the full
spec without a current auth lookup. A missing or mismatched proof is
`unfrozen_identity` and cannot enter represented admission or `ACTION_ACTIVE`.

When shadow later submits the original prepared request to legacy `/launch`,
binding the real request ID locks the already-created child and exact API
request row, decodes the effective persisted `LaunchBody`, and atomically
compares its `SKYPILOT_USER`/`SKYPILOT_USER_ID` values with the retained proof.
Equality writes ordinary `REQUEST_BOUND`; drift writes `REQUEST_BOUND` plus
write-once `IDENTITY_MISMATCH`, which completion can never clear or replace.
The mismatch is promotion-blocking but never mutates the already-committed
`REPRESENTABLE` coverage row or stops/replaces the legacy owner. Authoritative
private action handlers carry no legacy `LaunchBody` and read no current
request identity as provider input.

Down execution capsules contain no standalone current/down-request identity or
identity projector. Their locator and cleanup target may retain immutable
launch-derived name hashes, labels, and Pod annotation bytes required to
identify the old objects. Changing the current actor cannot change those exact
cleanup bytes or behavior.

For a not-representable decision, reason equality is only coverage evidence and
grants no cross-process replay authority. The prepared legacy request may be
used only by the same live preparation cell named by the unguessable nonce. If
that cell dies before a coverage-attempt `PRE_SUBMIT` exists, owner-fenced
recovery records proved pre-call abandonment, releases the old counted slot,
advances generation, and prepares anew. If a `PRE_SUBMIT` exists, the
submission is ambiguous and the no-blind-replay rules below apply.

Paid-capacity and reserved-fill admission preserve the global lock order rather
than appending shadow writes to their current capacity-first transactions. The
combined PostgreSQL helper locks the service, then locks an existing identified
replica or inserts a provisional fully identified replica row, and only then
takes capacity/reservation locks. On denial it removes only the provisional row
before committing any existing waiter/capacity bookkeeping; it creates no
coverage, parent, or link. On approval it finishes the replica/capacity
mutation, writes the counted `PROVISIONING` state, inserts or exactly adopts
coverage and, when represented, the parent, and writes the replica links before
one commit. A paid/reserved claim or grant and its launch slot are therefore
atomic. Recovery never turns an older
name-only replica into that provisional form. A capacity change during
preparation simply denies the provisional decision; no network preparation
result reserves capacity.

Handshake recovery is closed. Worker or manager loss before approval leaves no
replica/action/coverage/slot row, but may leave a nonauthorizing `PREPARING`
cohort reference. Owner-fenced recovery releases it only after proving the
preparation cell cannot receive authorization and no coverage, private request,
or action was admitted. An uncertain proof leaks the reference and safely
retains the cohort. A denied or superseded candidate closes its preparation
context and sends no mutation. Loss after approval but before a
represented `PRE_SUBMIT` child leaves a `PENDING` parent and a counted slot;
recovery may reprepare and adopt it only when the complete durable
spec/invocation matches. A proved mismatch before SDK entry marks that parent
`ABANDONED_PRE_SUBMIT`, keeps its coverage as a promotion blocker, releases the
slot/capacity under the owner fence, and replans under a new desired generation.
A coverage-only decision is never replayed from reason equality. Once either
kind of `PRE_SUBMIT` is committed, cancellation or worker loss is handled as
ambiguous and never authorizes an unobserved blind replay.

Immediately before each represented `sdk.launch()` or `sdk.down()` call,
including an in-process legacy retry and its cleanup down, the worker
locks service -> replica -> cohort -> reference -> coverage -> parent -> child,
requires the exact `SHADOW_ACTIVE` reference, revalidates the one-use
authorization, and commits the next `PRE_SUBMIT` child.
Immediately before every coverage-only unsupported SDK call, it instead locks
service -> replica -> cohort -> reference -> coverage -> coverage-attempt,
requires the exact `SHADOW_ACTIVE` reference, revalidates the same
owner/epoch/link/cancellation fences, allocates the contiguous ledger row, and
commits `PRE_SUBMIT`. Only then may either kind enter SDK request creation.
After the SDK returns a request ID, the worker locks its applicable
`PRE_SUBMIT` evidence row first and the exact API request row second. Under the
request-row serialization lock it reads both binding tables without acquiring
another evidence-row lock, verifies that no other row already binds the ID,
and records the ID in the applicable row in a short write-once transaction. It
then streams the request and completes that row; represented rows additionally
store normalized result/parity evidence. A superseded decision may become
`ABANDONED_PRE_SUBMIT` only when code proves the SDK/direct mutation function
was never entered. A crash or exception after entering request creation but
before binding its ID becomes `REQUEST_ASSOCIATION_UNKNOWN`, never inferred as
not submitted. A parent left without a completed child is likewise a coverage
failure; recovery adopts its identity and next sequence but never invents a
request ID or silently manufactures a parity result. A process crash that
leaves a coverage-only `PRE_SUBMIT` is conservatively
`REQUEST_ASSOCIATION_UNKNOWN`; reason equality never proves that no call was
made.

`SHADOW_ACTIVE` changes to `RELEASED` only after every private request and every
represented or coverage-only evidence row is terminal. `ACTION_ACTIVE` changes
to `RELEASED` only after the action is terminal and every correlated attempt/
request is settled. `REQUEST_ASSOCIATION_UNKNOWN`, ambiguous preparation, an
unsettled request, malformed state, or an unreadable store retains the reference
indefinitely.

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

Coverage-only preparation follows the same role/retry graph under the coverage
lock. Recovery distinguishes only durable evidence: no ledger row proves the
SDK gate was not crossed; `REQUEST_BOUND` adopts and streams that exact request;
`COMPLETE` permits a later call only when its committed retry disposition says
`RETRY_SAME_DECISION`; and `PRE_SUBMIT`,
`REQUEST_ASSOCIATION_UNKNOWN`, or `ABANDONED_PRE_SUBMIT` authorizes no later
call. Cancellation takes service -> replica -> coverage -> coverage-attempt
locks and obeys the same boundary. The ledger is a one-use submission fence,
not an alternate provider action, queue, or retry scheduler.

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

Shadow is complete for a service, not statistically sampled. All four launch
owners and all in-scope teardown owners route through the common admission
primitive, enforced by a checked-in call-site guard. Promotion scans coverage
with `admitted_at >=` the locked mode-change timestamp, and separately queries
candidate-window parents without coverage. It blocks if an identified
launch/down link lacks or mismatches coverage, any outcome is
`NOT_REPRESENTABLE`, a `REPRESENTABLE` row lacks exactly one same-ID parent (or
vice versa), any expected attempt lacks a child or real request association,
any row is pending, abandoned, ambiguous, direct-down, unsupported, or
divergent, or either action kind lacks the configured minimum. Only
representable clean `MATCH` graphs count toward those minima. Reason counters
and logs are diagnostic only and never satisfy coverage. Every coverage-only
attempt graph is nevertheless validated and reported; a malformed,
nonterminal, or unknown ledger row is an additional blocker. Retention does not
delete candidate-window coverage, coverage-attempts, parents, or children before promotion. The
transition to `shadow` and its database timestamp are written under the
service/owner lock; the promotion transaction locks service, live replicas,
cleanup intents, coverage by decision ID, coverage-attempts, parents, then
children before rechecking the minimum 24-hour window and all blockers.

The same locked scan computes `ServeShadowCoverageInventoryV1`; it does not
trust the caller's `shadow_coverage_complete` Boolean.  Its canonical JSON
preimage is:

```text
{
  version: 1,
  service_name,
  service_hash,
  candidate_since,  # canonical UTC RFC 3339 with six fractional digits
  decisions: [      # ascending UUID bytes
    {
      decision_id,
      coverage,             # exact CoverageDecisionV1 or null
      cohort_reference,     # null or {reference: exact
                            # WorkerCohortReferenceInputV1, reference_state,
                            # revision, created_at, bound_at, released_at}
      replica_links,        # ascending (replica_id, action_type); each has
                            # replica incarnation/generation, coverage ID,
                            # and represented-sample ID
      represented_parent,   # null or {would_be_action_id,
                            # immutable_spec_sha256, provider_plan_sha256,
                            # phase, parity_class, revision, created_at,
                            # updated_at, completed_at}
      coverage_attempts     # exact CoverageAttemptV1 values in sequence order
    }
  ]
}
```

The decision-ID set is the union of candidate-window coverage, candidate-window
parents, every nonnull live-replica launch/down coverage or represented-sample
link, and every non-`RELEASED` cohort reference for this service hash.  A
coverage-referenced released reference is also projected.  Typed readers first
recompute every embedded row contract and payload hash; the inventory excludes
represented children only because the same locked audit independently validates
their complete parity graph.  The scan fails closed with an explicit promotion
blocker above 10,000 decisions or 100,000 combined coverage-attempt and replica-
link rows.  `coverage_inventory_sha256` is lowercase SHA-256 of this canonical
preimage.  `PromotionBlockerReport` returns the recomputed value, and the
authority transition requires byte equality with the fresh
`ActivationGateEvidenceV1.coverage_inventory_sha256` while retaining the same
locks.  Missing coverage, an unlinked reference, or a malformed row is a
blocker and remains represented in the inventory; it is never omitted to make
the caller's evidence match.

The compound admission helper first locks and revalidates the service name,
incarnation hash, controller owner, nonnull lifecycle epoch, and `shadow` mode;
then it admits/links coverage and the optional parent in the caller's
replica-intent transaction. Coverage `admitted_at` and parent `created_at` use
one fresh PostgreSQL `clock_timestamp()` read after that lock, never the
transaction-start timestamp. Promotion takes the same service lock before
scanning the window, so an admission that waited behind promotion must
revalidate the mode and cannot appear after the scan with a pre-window
timestamp.

The lifecycle epoch is the current fencing token of an API lifecycle
operation, not a stable controller-owner credential: ordinary updates may
advance it without changing the service hash or controller owner. A controller
snapshots the current nonnull epoch from the exact owner row immediately before
action admission, and the admission transaction revalidates that optimistic
snapshot under the service-row lock. A concurrent advance rejects and retries
that admission; it does not permanently cancel the still-current controller.
The controller bootstrap or recovery-script epoch must never be cached as the
expected epoch for later replica actions.

The launch/down coverage links and their optional sample links are
incarnation-scoped, not all constrained to the row's current generation. The
launch links retain the most recent launch decision and therefore may name
generation N after teardown advances the row to N+1; the down links, when
present, must match the current teardown generation. Generation advance never
clears launch evidence. Retention cannot delete referenced coverage, a parent,
or any child while the replica row or a durable cleanup intent exists.
`NOT_REPRESENTABLE` coverage cannot age out while the service remains in its
shadow window. Candidate discovery is nonlocking. For each candidate, typed GC
first locks the exact extant service-incarnation/owner row, then every possible
replica link and cleanup intent in canonical order, then the cohort/reference,
then coverage,
coverage-attempts, the optional parent, and represented children. It re-reads
the mode/window, links, cleanup intents, and terminal shapes; skips active,
referenced, or nonterminal evidence; changes the exact reference to `RELEASED`;
and deletes represented children then parent, or coverage-attempts, and finally
coverage in one transaction. It never
locks coverage and then reaches backward to a replica. If the exact service row
is absent or the name now has another incarnation, M2 GC defers indefinitely;
reclaiming deleted-service evidence requires a separately designed durable
incarnation tombstone. Service/replica deletion never cascades evidence. If a
replica launched in shadow
later enters authoritative down, down admission loads the matching completed
launch child, revalidates its canonical observation and
`ResolvedProviderTargetV1`, exact-reads the same-UUID global-user-state cluster
row and its full provider handle, and copies the typed basis into the immutable
down invocation, the cleanup target into that invocation's execution capsule,
and only their hashes into the indexed provider plan. The admission transaction
then owns that frozen handle; action-aware cluster-row removal is legal only
through the down attempt's expected-UUID, post-absence seam. Missing,
hash-only, incomplete, unsupported, or mismatched launch/handle evidence keeps
that replica on shadow/legacy teardown; it never falls back to a name-only
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
- every remaining controller/API running the approved active digest and every
  versioned authority-worker cohort referenced by a preparation, nonterminal
  private shadow request/evidence row, or nonterminal action running its own
  approved immutable digest, all at API007, Serve033, and
  global-user-state 028; each dedicated attested cohort includes the private
  handlers only for actions frozen to that cohort, while every ordinary
  executor excludes them;
- exact provider-profile eligibility for every live candidate;
- complete decision coverage from the locked window start, zero
  `NOT_REPRESENTABLE` coverage rows, and no unresolved shadow divergence or
  unsampled mutation;
- at least 24 hours and a configured minimum sample count of clean live shadow
  operation, including launch and down; and
- successful crash injection at every boundary below.

`ActivationGateEvidenceV1` is a closed internal value bound to the exact
service name, service-incarnation hash, lifecycle epoch, and (for authority)
the database timestamp that opened the current shadow window. It carries the
three independent schema heads, approved image and named inventory
fingerprints, and a database-clock `verified_at`. `legacy -> shadow` requires
`API005`, `Serve033`, and global-user-state `028`; `shadow -> authoritative`
requires `API007`, `Serve033`, and global-user-state `028`. The transition
rejects evidence for another fence/window,
evidence from the database future, or evidence older than five minutes. A
`legacy -> shadow` transition requires a null candidate-window binding and no
live replica lacking the canonical incarnation/generation/cluster UUID triple
(the first canary is activated while scaled to zero); it never mints identity
onto a name-only provider resource. A
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

- Keep frozen Serve032 mode/replica-identity/sample-link, logical-sample, and
  represented-attempt schema unchanged. Add Serve033 coverage links,
  decision-coverage, and coverage-only attempt schema; assert the dark 032
  tables are empty before adding the explicit parent FK. Refuse schema down
  while retaining the additive state.
- Verify fresh 031 -> 032 -> 033 and already-at-032 -> 033 PostgreSQL upgrades,
  ordinary-row preservation, fail-closed nonempty-shadow detection, explicit
  FK/catalog convergence, and SQLite columns-only behavior.
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
- Split launch/down preparation from mutation permission with the bounded
  prepare/admit/authorize handshake. Add the nonexecuting Serve033 cohort
  registry/reference fence with a committed per-reference preparation-
  capability hash, create `PREPARING` before identity canonicalization/private
  preflight, and
  atomically bind it with coverage/action admission. Persist coverage for every
  approved decision, including a closed not-representable result, before the
  sole legacy mutation can enter its submit pool.
- Persist either a represented child or coverage-only submission row before
  every legacy SDK call, bind its nullable real request association immediately
  after SDK admission, and complete it transactionally with the legacy Serve
  projection. Route direct
  teardown through `sdk.down()` before collecting the promotion window.
- Capture represented Kubernetes and Skylet effects at their actual serialized
  transport boundaries. Route only the narrow candidate through the common
  pure renderer, prebooted runtime, and action-keyed Skylet seam while legacy
  SafeThread/request ownership remains unchanged. Its real request uses a
  private server-owned handler that only the dedicated capability-filtered
  authority-worker cohort may claim; every extra or mismatched call is
  divergent.
- Preserve legacy autoscaling and provider mutation authority.

M2 foundation verification evidence on 2026-08-01:

- The earlier feature-only Serve032/033 migration tests are superseded after
  upstream shipped a different revision 032. Read-only `boltz-test` audit
  proves the target is at upstream request-classification Serve032, has no
  resource-action tables, and was never stamped with either feature-only
  shape. The single guarded Serve033 migration is implemented and its 23-case
  serial PostgreSQL/SQLite matrix passes: unique 031 -> upstream 032 -> 033
  lineage, preservation of upstream classification state, exact reflected
  catalog, rejection before mutation of nonempty evidence or activated action
  state, convergence from every proven-empty partial graph by replacement,
  rejection of malformed defaults/types/checks and feature-only collisions,
  lost-ack replay, and downgrade refusal.
- The still-dark final catalog includes the nonnull, no-default lowercase
  preparation-capability SHA-256 commitment. Typed reference decoding,
  persistence, exact adoption, binding, launch/down fixture coverage, invalid-
  row rejection, and lost-migration-acknowledgement catalog convergence pass
  locally. The closed request/context/proof/response contracts, one-session
  read validator, direct post-auth endpoint, shared queued-request resolver,
  exact status/body/auth behavior, and bounded client transport also pass
  focused contract, PostgreSQL, endpoint, OAuth, executor, and retry tests. An
  adversarial review accepted the corrected full-value user-hash validation
  and fail-closed middleware-state boundary. This evidence does not claim
  manager-side CSPRNG generation, preparation-cell ownership/discard, or use of
  the still-dark client by a manager;
- global-user-state revision 028 installs a nullable portable cluster-record
  UUID and partial unique index, leaves historical rows null, and provides the
  PostgreSQL-only exact insert/adopt/reject primitive without changing ordinary
  cluster updates;
- the initial closed, bounded provider locator, invocation, observation,
  outcome, shadow projection, and retry contracts have canonical byte/hash
  fixtures and action-specific success proof; the companion's stricter frozen
  Kubernetes scope, execution-config boundary, and preparation handshake are
  the next contract slice;
  and
- the PostgreSQL typed shadow store plus Serve033 coverage/cohort and promotion
  suites pass locally, including exact parent/child replay, retry-chain
  closure, reference fencing, coverage-only attempts, bounded canonical
  promotion inventory, outcome-aware replica coverage/sample-link validation,
  activation-window/hash fencing, retention protection, and lock races. The
  pure contract and promotion-audit slices received independent adversarial
  acceptance.

The typed-store foundation intentionally admits represented decisions without
a cohort reference only. Passing a prepared cohort reference to either ordinary
or launch-replica represented admission fails before any service, replica,
coverage, or parent mutation because the current flattened immutable invocation
cannot yet commit
`execution_config.capsule.executor_cohort`. The M2 execution-config closure must
land before this guard is replaced by full byte-equality validation and atomic
`PREPARING -> SHADOW_ACTIVE` binding.

Runtime admission/linking, legacy SDK instrumentation, provider identity
propagation/readback, and live shadow evaluation remain M2 gates; no service is
eligible for authority yet.

### M3: dark dispatcher and recovery

- API-request revision 006 now adds the bounded provider-progress snapshot,
  retained provider-I/O watermark, claim-fenced monotonic write/read methods,
  typed domain hooks, retry-seed derivation, and fail-closed migration; ordinary
  attempts remain null. The generic PostgreSQL substrate and race regressions
  pass locally; the Serve cursor contract and dispatcher integration remain.
- Run due discovery/materialization against synthetic/canary actions only.
- Register the private action handler only in the dedicated
  capability-filtered normal-executor cohort, with `retryable=false` and no
  precondition; ordinary normal executors exclude it. Normalize retry/pause
  exceptions inside the handler so the generic executor cannot requeue the
  same request attempt.
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
Authority-worker Deployments, ServiceAccounts, selector Service, Secrets,
manifest projections, and their NetworkPolicy stay in that release namespace;
only provider workload objects use the separate `skypilot-actions-canary`
namespace. Production is not a fault-injection target.

Deployment order is:

1. build and push an immutable image digest;
2. deploy only the API role with `helm upgrade --reuse-values`, explicitly
   keeping the dark authority-worker role disabled when no cohort exists (or
   pinning every already-deployed cohort to its immutable digest), pinning the
   controller role to its prior immutable digest, and letting the API's blocking
   additive migration hook reach the required heads;
3. verify all independent milestone-specific heads: M1a is API005 with
   unchanged Serve031/global-user-state 027; legacy-controller-only M2 shadow
   requires API005, Serve033, and global-user-state 028; any private-handler
   shadow, M3 provider dispatch, and M4 authority require API007, Serve033, and
   global-user-state 028, with no cross-lineage Alembic dependency;
4. deploy a new immutable versioned authority-worker cohort at the new digest
   while retaining/pinning every cohort with a `PREPARING`, `SHADOW_ACTIVE`, or
   `ACTION_ACTIVE` reference and pinning API/controller roles, then prove its
   exact static manifest, live identity, capability, and handler inventory
   before selecting it for new admissions;
5. deploy controller roles at the new digest while pinning API and
   authority-worker roles, then prove the complete image/head/handler
   inventory;
6. collect parity and crash evidence; and
7. promote only an explicitly selected canary service.

The first ready worker self-attests the projected static manifest and live
Deployment/ServiceAccount UIDs and inserts `REGISTERING`; two distinct ready
Pods must exactly adopt that identity and append current registration evidence
before the typed gate changes it to `ACCEPTING` and permits active selection.
Retirement first commits
`DRAINING`, so no new preparation reference can bind while existing work
remains claimable. After active references release, one transaction locks the
cohort/references, performs fail-closed nonlocking scans of every authoritative
action/attempt/request and private shadow request/evidence carrier, and commits
`REMOVAL_AUTHORIZED`. Unknown or unreadable state counts as a reference. The
current chart may remove the exact Deployment/ServiceAccount only afterward,
while retaining their names in the current chart's tombstone-scoped GET grant.
The surviving API-role verifier—not the removed worker—proves exact NotFound
and commits `RETIRED`; a later chart upgrade then prunes that grant. Thus an active-cohort switch between preflight
and admission and a retirement attempt between reference discovery and
admission are fenced by the same cohort/reference rows.

The first additive migration deployment omits `--atomic` unless the selected
automatic rollback image is proven ahead-of-head compatible. Native Helm
rollback is image rollback only; it does not run schema down. A failed
milestone is repaired with another current-chart `helm upgrade --reuse-values`
that pins every role explicitly and deploys the prior compatible immutable
digest against the retained additive schema; `helm rollback` is not used.
Database principal topology and credentials are unchanged by this program.

## Fault-injection and verification

Mandatory crash/race points include:

1. before and after preparation capability generation, `PREPARING` reference
   commit, identity-canonicalization request/response, and typed-result
   publication;
2. while two managers race for the last slot across ordinary/ordinary,
   ordinary/paid-capacity, and ordinary/reserved-fill admission; while a release
   races new admission; and while owner handoff races recovery release;
3. after counted-slot/coverage/optional-parent commit but before the worker receives
   approval;
4. after approval but before either kind of `PRE_SUBMIT` row;
5. during two-dispatcher due discovery;
6. after request/queue commit but before materialization acknowledgement;
7. after request claim but before the atomic two-boundary/first-cursor or
   two-boundary/inherited-attestation commit, and immediately after either
   combined commit;
8. before and after every later monotonic provider-progress checkpoint, including
   each of the three object effects and the idempotent job submission;
9. before and after the Skylet job/outbox fsync commit, after that commit before
   wakeup, before and after `START_INTENT`, after launcher spawn before its
   durable run-token transaction, before/after `START_COMMITTED`, during failed
   or successful `exec`, before/after the pinned entrypoint's durable `RUNNING`
   handshake, and during watcher or Skylet/container restart;
10. after provider result but before request terminalization;
11. after request terminalization but before Serve reduction;
12. after retry reduction but before the controller observes `next_attempt_at`;
13. while materializing attempt `n+1`, after materialization but before its
    boundary/attestation binding, while carrying a partial cursor, and while
    re-attesting a replacement authority worker;
14. while superseding a partial launch, committing its real down action, and
    exact-reading/deleting each cleanup object and cluster row;
15. during controller/API/executor eviction and controller-leadership change;
    and
16. during compatible image rollback/re-upgrade with nonterminal actions; and
17. while active cohort selection changes between preflight/admission and while
    retirement's zero-reference discovery races admission/private request
    binding.

Tests must prove:

- fresh empty API005 -> API006 migration, both boundary-field constraints, and
  fail-closed rejection of any pre-006 action-attempt row whose provider-I/O
  watermark cannot be reconstructed;
- deterministic action/request identity and byte-mismatch rejection; the
  no-enqueue identity endpoint and `/launch` use the same extracted resolver,
  authenticated and no-auth inputs produce exact golden proofs without an API
  request/queue/action write, raw-body size/content/closed-key parsing and exact
  status mapping reject, the raw preparation capability never persists and its
  constant-time hash check rejects another/missing/released reference, complete
  proof context/resource/prepared-pair drift rejects before represented
  admission, recovery reproduces the invocation from the retained effective
  pair and context with no current-auth lookup, and request binding atomically
  preserves either equality or write-once `IDENTITY_MISMATCH` through terminal
  completion;
- with cap `C`, `N > C` simultaneously prepared successful decisions commit at
  most the available number of new `PROVISIONING` rows; each winner atomically
  has one identified replica, counted slot, applicable capacity claim, coverage,
  optional parent, links, and bound cohort reference, while each loser has none
  of those and may retain only a released preparation reference;
- paid/reserved denial preserves existing waiter/grant bookkeeping without a
  replica intent or coverage row; a commit-before-signal crash remains counted
  across controller death and is adopted or released only with pre-call proof;
  two recovery owners cannot double-release; release racing admission never
  exceeds the cap; restart/local-snapshot lag neither undercounts nor double-
  counts a decision ID;
- capacity race fixtures exercise both lock-acquisition directions and fail on
  deadlock, an external resources-file lock acquired after SQL locks, or any
  condition/preflight/provider wait while either lock is held;
- authoritative `INTENT_COMMITTED` in both boundary fields and the first legal
  API006 cursor are one atomic write, so no crash can leave a crossed provider-
  I/O watermark with null progress;
- monotonic provider-progress replay, stale-write rejection, and partial UID/
  job commitment survival across request-worker eviction;
- every checked-in realistic and candidate-maximal companion fixture for all
  launch phases, both head-Pod edges, terminal no-effect resolutions, request-
  return envelopes, E-only/E+N reducer quiescence, handler-domain `S` and every
  legal phase/category `R`/`U`/`B` tuple, all three direct no-effect bases and
  empty/one/max-count prefix shapes, and request-fallback `P0`/`O`/`S`/`X`
  outcomes remains at most 65,536 canonical UTF-8 bytes with its golden hash;
  additionally, completed-launch down, every legal partial-launch down full-
  spec shape, and capped preflight request/response envelopes are pinned by
  exact byte count/hash and every full action spec is at most 60,000 bytes; an
  oversized/unbounded candidate is rejected by admission and the immediate
  pre-I/O recheck before any intent/watermark;
- effect provenance survives generation reset across attempts: intent origins
  and prior evidence remain byte-equal, evidence-commit origins and
  created/adopted dispositions are exact, later claims may adopt but cannot
  produce a no-effect resolution for an inherited ambiguous intent;
- attempt `n+1` byte-copies the settled predecessor cursor, clears only its
  attempt-scoped worker attestation, recomputes the envelope hash, starts local
  revision one, and cannot materialize from missing/regressed crossed-boundary
  progress;
- an exact pre-I/O retry from `NOT_STARTED` with null progress, progress
  revision zero, and null provider operation ID reduces to `READY` and
  materializes attempt `n+1` with the same null/zero shape, whose handler takes
  the fresh-cursor atomic initialization branch before provider I/O;
- a crash after materializing an inherited revision-one cursor but before
  boundary/attestation binding remains a legal pre-I/O retry shape, reduces to
  `READY`, and can seed the next attempt; an arbitrary, attested, regressed, or
  predecessor-mismatched `NOT_STARTED`/nonnull cursor rejects, and such a
  cursor can never prove `CANCELLED_NO_EFFECT`;
- the private handler's dedicated encoder stores exactly one hash-valid
  `ServeReplicaActionRequestReturnV1`; null/drop/default-encoded/mismatched
  returns and external `FAILED`/`CANCELLED` terminalization cannot produce
  partial-launch quiescence, while the reducer—not the handler—constructs and
  persists the final quiescence from the valid DTO and API006 cursor;
- every request-fallback terminal state and return-failure reason takes the
  literal journal-class row: empty pre-I/O retries at 60 seconds, a valid
  nonterminal cursor remains observation-first, exact `SUCCEEDED` progress
  commits success, and invalid progress blocks; none can synthesize N or
  partial handoff;
- at attempt `RESOURCE_ACTION_MAX_ATTEMPT_V1`, each otherwise-retrying handler
  `R`/`U` and fallback `P0`/`O` outcome settles byte-exactly but blocks with the
  one exhaustion event and no deadline/request/attempt max-plus-one; replay is
  idempotent, while eligible no-I/O teardown still takes the direct route;
- the Skylet durable outbox returns only after the job/outbox fsync commit and
  every enumerated crash/restart state preserves one submission key, one job
  row/job ID, and monotonically increasing run epochs without starting job
  bytes before the run-token transaction; `START_COMMITTED` never satisfies
  `JOB_RUNNING` or launch success;
- exactly one queue delivery and one request claim lease per attempt;
- no action lease/heartbeat table or domain due scanner;
- database-clock retry continuity across restart;
- stale owner/request/reducer writes reject;
- observed launch adoption and ambiguous launch blocking;
- a never-started launch cancels through each of `unmaterialized`, terminal-
  request-unsettled, retained-settled/request-present, and retained-settled/
  request-GC paths using the exact monotonic no-I/O prefix; the unsettled path
  writes attempt plus action, retained paths preserve the old attempt outcome
  byte-for-byte while replacing only action `last_result`, and all paths remove
  the provisional row/count exactly once without a down; prefix-link tampering,
  inherited cursors, and crossed predecessors reject;
- a superseded effectful launch is fenced and terminalized as
  `SUPERSEDED_TO_DOWN` in the same sorted-lock transaction that inserts/adopts
  and links one normally queued down action; the down basis retains the exact
  source key/revision/hashes while the cursor and quiescence remain in their
  locked source rows, and no hidden cleanup or another launch request can run;
- partial-launch cleanup exact-reads all three frozen roles, rejects every
  same-name replacement, uses UID-preconditioned deletes, proves three exact
  NotFound results, and removes or adopts only the exact cluster row before
  terminal success;
- a create that becomes visible after an initial NotFound prevents handoff
  until the old launch commits its exact effect evidence; down cannot admit or
  terminalize from the earlier absence sample;
- after earlier committed effects, a later CoreV1 422, rolled-back cluster-row
  insert, or pre-commit Skylet conflict permits handoff only with its exact
  typed definitive-no-effect proof; timeout, reset, 5xx, lost acknowledgement,
  expired lease, and post-call NotFound do not; source/down insertion races use
  both possible UUID sort orders;
- launch at `ENDPOINT_RESOLVED` and down at `ABSENCE_EXACT` or
  `HANDLE_REMOVED` fail success reduction; only the final claim-fenced
  `SUCCEEDED` cursor can project terminal success;
- down terminalization only from exact absence evidence;
- shadow creates one coverage row with replica/capacity intent for every
  approved decision; a representable row creates the same-ID parent and one
  child before every real legacy SDK attempt, while a not-representable row
  creates no fake parent but does create one coverage-attempt fence before each
  real SDK attempt; neither path blindly repeats an ambiguous mutation;
- stale parent, pre-call, call/ID-bind, and completion gaps become explicit
  promotion blockers rather than synthetic results or request IDs;
- preparation creates a nonauthorizing `PREPARING` cohort reference before
  preflight; admission atomically binds it to `SHADOW_ACTIVE` or
  `ACTION_ACTIVE`; active-cohort switch, stale preparation owner, and the zero-
  reference/admission race cannot strand work; a nonterminal private shadow
  request pins its cohort; missing/unreadable/malformed reference state blocks
  retirement; the registry starts `REGISTERING` and requires two distinct
  matching ready Pods before `ACCEPTING`; down takes the same preflight fence;
  `DRAINING -> ACCEPTING` rollback atomically recollects both attestations; and
  removal occurs only after `REMOVAL_AUTHORIZED`, with the surviving API
  verifier retaining exact tombstone GETs until NotFound commits `RETIRED`;
- promotion refuses mixed binaries, a missing handler/head, a missing or
  not-representable coverage row, an orphan coverage/parent, a pending row, or
  any unresolved divergence; and
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
- no second queue, action lease, or domain-specific scheduler is introduced;
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

- Runtime atomic admission/linking and legacy launch/down instrumentation on top
  of the implemented Serve033 migration, cohort/reference fence, typed
  coverage/attempt store, promotion audit, and retention protocol.
- The Serve-owned cursor validator/reducer and private-handler capability filter
  on top of the implemented generic API006 progress journal, including
  immutable cross-attempt effect origins, per-attempt worker re-attestation,
  the dedicated closed return encoder/decoder, candidate-maximal 65,536-byte
  progress/outcome fixtures, at-most-60,000-byte completed and every-legal-
  partial down full-spec goldens plus capped preflight envelopes,
  reducer-owned quiescence, and
  partial-launch cleanup. Authority remains disabled until realistic and
  candidate-maximal fixture measurements both pass.
- Rendered and live verification of the dedicated versioned authority-worker
  Helm cohort, exact RBAC/admission/NetworkPolicy, purpose token/TLS preflight,
  static-manifest/live-UID qualification, complete-spec submit/observe, worker
  registration/two-Pod attestation, staged API -> worker -> controller rollout,
  namespace-local worker Service/RBAC/projections, surviving-API tombstone
  verification, fail-closed retirement over preparation/action/private-shadow
  references, and current-chart rollback with nonterminal work pinned to its
  cohort.
- A checked-in inventory of the initial `pod_cluster_v1` eligible cohort after
  preallocated cluster UUID propagation, Kubernetes replica-incarnation
  labeling, normalized admitted-object/partial-UID readback, prebooted
  Ray/Skylet and action-keyed job recovery, exact handle/endpoint, dual-LB
  reachability, and the redacted invocation builder pass contract tests. Until
  then the profile is shadow-only and promotion-blocking.
- Build and push the immutable canary image for `boltz-test`. AWS/ECR auth and
  read-only cluster access through the dedicated SSM hop are verified; the
  current rollback baseline is Helm revision 56 with all roles pinned to
  `sha256:6753d5fb3ccac8d6436a436a99870410248e62f03ecad08741ebd4510d1f5e18`.
- Measured shadow sample minimums and the first canary service selection.
- A separate decision on whether central principal convergence is worth its
  operational cost; it must not silently re-enter M1-M4.

## Deliberate departures from the superseded draft

- API schema down and feature-specific full uninstall are removed; additive
  heads remain through application rollback.
- No new schema owner/migrator/runtime role topology or superuser bootstrap is
  introduced.
- No maintenance-reader fleet, downgrade fence, general rollout-pin authority,
  or external owner-mutation journal is part of resource actions. The two
  Serve033 worker-cohort tables are profile-local, nonexecuting retention
  fences for immutable authority-worker cohorts.
- Event-v2 and broad operational-history redesign are deferred.
- The generic kernel has two tables in v1; multi-effect/child-action tables are
  deferred until a real second use case requires them.
- Authoritative provider scope is narrow and explicit instead of claiming
  every existing cloud path at launch.
