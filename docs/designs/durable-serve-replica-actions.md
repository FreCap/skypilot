# Durable SkyServe Replica Actions

Last updated: 2026-08-07

Status: bounded M0 and M1b contracts accepted after independent adversarial
review. The additive API005-007, global-user-state 028, guarded Serve033 action
schema, and Serve034 authority-release ledger; dark generic action store;
typed Serve shadow/coverage/cohort stores;
and cluster-identity fence are implemented and locally PostgreSQL-verified.
The generic API006 kernel now has lineage-safe retry materialization and
claim-fenced progress/reduction validation. The Serve-owned pure progress
validator/reducer, exact launch/down execution configurations, completed and
partial-launch down bases, strict authoritative-handler return codecs, and fail-closed
request-result persistence are implemented and focused pure/PostgreSQL-tested.
The five packaged pinned Kubernetes renderer artifacts, closed renderer
input/seed, exact three-body/capsule cutover, effect-free staged renderer, and
pure request/admitted normalizers are implemented and locally verified.
The four private handlers remain deliberately fail closed before provider I/O;
the shadow-handler strict result codecs required by P3 are not implemented.
The P2a preflight-only transport, exact release/static-manifest contracts,
two-Pod self-attestation/bootstrap, stale-registration retirement fence, API
tombstone verifier, and disabled-by-default Helm topology are implemented and
locally unit/PostgreSQL/Helm verified. Merged-image dark rollout and live cohort
qualification remain pending. The exact representative launch spec is 60,851
bytes in frozen V1 and therefore cannot qualify as the required V2 envelope.
The additive compact V2 capsule/config/invocation/plan graph, V2-only full-spec
parser, typed locked-row V2 cohort resolver, and exact structural full-spec
goldens are
now implemented and locally verified without changing V1. The additive V2
preflight wire/parser, disjoint `/v2/` transport, exact realistic and
candidate-maximal structural envelope goldens, and frozen-V1 isolation are also
implemented and locally verified. The production authority-worker runtime
now installs the mutation-free V2 trust evaluator over an isolated one-
connection PostgreSQL pool. The revision-one `PREPARING` writer/strict reader,
locked service/policy/cohort/handoff/two-lease/reference/current-instance
validator, initial-root-versus-rotated-active-policy split, zero-queue
single-flight transport boundary, and typed dark result are locally unit and
real-PostgreSQL verified. A valid locked request receives only the kind-matched
typed unavailable result; absent, crossed, corrupt, expired, saturated, or
late trust remains the fixed typed V2 503. Manager admission does not yet call
the preparation writer, so no production action can reach the valid branch;
authority remains disabled.
The native V2 launch/down seed and input codecs, launch constructor, sole
cleanup-target rederiver/shared binding validator, down constructor, and exact
V2 binding-schema artifact are now implemented and locally focused-tested.
The down root accepts the typed cleanup-rederivation input and invokes that
sole rederiver itself; no caller-supplied cleanup target is a construction
input. The authoritative direct-no-effect builder and expanded authoritative
handler/direct/fallback outcome parser, raw-invalid journal profile/classifier
integration, shared post-materialization projection, V2 config-access/six-role
artifact/callable inventories, and provisional finite representability
dispatch are now recovered from the continuation checkpoint. The rejected
monolithic draft measured 75,247 bytes, above its 65,536-byte file contract.
Its accepted replacement is now implemented: the stable top-level path is a
715-byte bounded index over two descriptor-safe, content-addressed shards of
37,423 and 37,997 bytes. Their 366 cases remain provisional and only three of
seven boundary families are implemented; the two fixture/result files and CI
golden manifest are not yet present. The rebased repository
baseline is now exact API008 and Serve037: API008 adds generation-bound request
execution-quiescence evidence and API-instance backend/capability attestations;
Serve035 installs the multi-pool reserved-fill protocol, Serve036 adds versioned
controller-configuration snapshots, and Serve037 installs the placement-
normalization/retirement ledger tables, service receipts, and version-retirement
columns. Those current migrations contain no M4 authority state. The forward-
only continuation assigns Serve038 membership/authority
`down_revision='037'`, Serve039 execution history/terminal selectors
`down_revision='038'`, and stacked M5a closure Serve040
`down_revision='039'`. The additive Serve038 membership/authority migration
and Serve039 lineage/terminal-selector migration are now implemented and
locally schema-tested. The exact `008/039/028` policy/candidate/proof codecs are
implemented and locally focused-tested. The claim-start authority barrier,
connection-borrowing API006/terminal seams, and historical settled-replay
validator remain design-only. Existing V1 renderer and full-spec/envelope
goldens do not close those remaining gates.

The current dark intermediate installs the V2-only `PREPARING` reference
transaction and a stateful evaluator fence before complete capsule evaluation.
Only a request with the exact locked `ACCEPTING` cohort, no nonterminal
handoff, both accepted fresh leases, the same revision-one `PREPARING`
reference, and the current unready `authority-worker` API-instance lease may
receive its kind-matched typed
`not_representable: preflight_unavailable_or_invalid` response. Missing,
expired, crossed, or corrupt evidence remains fixed 503. This intermediate
also closes the action-kind context boundary. A launch request must carry the
exact typed `ProviderLaunchIdentityCanonicalizationContextV1` at
`seed.source.identity_canonicalization.context`. Under the locked service and
reference, the store compares its service name, complete resource identity,
decision/cohort/action identity, controller-owner fence, lifecycle epoch,
preparation revision/state, and capability commitment to the locked rows.
A down request must carry no launch canonicalization context; supplying one
rejects, and the down capability commitment remains owned by its locked
reference rather than a caller context. There is no absent-context fallback or
ambient reconstruction for launch. This intermediate does not advance the
reference, construct a capsule/spec, advertise readiness, install a claimant/
executor, or perform provider I/O; complete evaluation and atomic admission
remain required before any authority activation.
Its five-second guarantee is a hard transport/result-publication deadline, not
a claim that an uninterruptible DBAPI call is killed. V2 evaluation has one
nonblocking slot and no queue. It uses a separate size-one pool with 250 ms
checkout, one-second connect, 3.5-second statement, 750 ms lock, four-second
idle-transaction, TCP keepalive/user-timeout, and a cumulative monotonic guard
that forbids starting another statement after four seconds. The latter is not
a hard transaction kill: a statement admitted just before that boundary keeps
its own 3.5-second server timeout.
At most one mutation-free trust transaction may finish late under a database
or network blackhole; its daemon result cell is request-local, permanently
discarded, and cannot send bytes or authorize later work. The transport server
object is one-shot: `stop()` is terminal and recovery constructs a fresh
object/process, so an old daemon slot cannot cross a transport generation.
Saturated requests
fail immediately. The currently implemented, pre-pool dark P2a process therefore
has an explicit persistent ceiling of three synchronous physical PostgreSQL connections: one shared
central-state connection for authority bootstrap, one
`api-requests-control` connection for the API-instance heartbeat, and one
isolated preflight connection. Startup/migration advisory-lock sessions use
`NullPool`, close when their lock ends, and are transient outside that
persistent ceiling. Operationally, the persistent ceiling is
`3 * authority Pod count`, equivalently `6 * concurrently rendered
two-member cohort count`. One live-plus-candidate compatible rotation renders
two such pre-pool cohorts and therefore reserves 12 persistent synchronous
connections. This three-per-Pod statement is not the M4 execution-pool
capacity contract: once the fixed `N`-child pool below is enabled, the manifest-
bound ceiling becomes `3 + 2*N` per Pod. The chart has no PostgreSQL `max_connections` control; proving
that the external database has this persistent capacity plus separate
transient advisory-lock headroom is an authority-activation gate.
The activation contract now admits API005 only for legacy-controller shadow
and requires the exact API008 head for private-handler dispatch readiness and
`shadow -> authoritative`; API006 remains a progress substrate and API007 is
only the historical role/claim-contract predecessor. API008 readiness also
requires the named PostgreSQL request storage/queue backends and
`execution_quiescence_capable=true` API-instance evidence. This is a
fail-closed contract correction, not provider-runtime or authority evidence.
The three server-owned API008 proof builders and their transition/dispatch
writes specified below are not implemented.
Manager/runtime admission, live target observation, dispatcher wiring, live
provider I/O, atomic Serve projection, and runtime shadow instrumentation remain
open.
Authority is disabled, no service has been promoted, and the named legacy
thread/map/retry-clock owners remain in place; the restructuring has not yet
earned its claimed operational payoff.

The frozen Serve034 accepted-V1 ledger bridge and the explicit Helm
`deselect -> tombstone -> none` retirement interface were implemented and
verified against their historical API007/Serve034 baseline. Their rebased
cleanup-only use at exact API008/Serve037/state028, including the chart ceiling
and current tests, is not yet verified. No live retirement rollout has been run;
Serve038 membership activation, Serve039 authority state, and the deprecated-
path removal remain gated on that evidence and the later M4 gates.

The merged P2a foundation is a preflight-only, two-Pod authority-cohort
bootstrap. Its closed transport envelopes, private HTTPS, complete static-
manifest projection, self-attestation, and retirement fences start no request
executor, claim no queue row, admit no manager decision, construct no workload/
action-provider client, and perform no Kubernetes mutation or provider effect.
Its dedicated bootstrap observer only GETs its own Pod, owning ReplicaSet,
exact Deployment, and ServiceAccount. Its initial post-bootstrap response is
intentionally only typed
`not_representable: preflight_unavailable_or_invalid`.

The current remaining M4 tranche first dark-deploys and live-qualifies that
merged P2a artifact, then completes P2b live target observation, P3 private
shadow dispatch/provider execution, and P4 per-service authority on additive
Serve038 membership plus Serve039 historical authority. No P2a evidence is reclassified as runtime shadow or authority
evidence.

Foundation merge commit `93aec0c8a4f2e1a80ed35640c9d424bea3f9e580` was built as
immutable image
`sha256:8bc1295d5cb873861576aaf0806665e89b2d325194da8dd61fa5752f0593d174`
and exercised on `boltz-test` through a staged dark API -> ordinary executor ->
controller rollout. The earlier source artifact at commit
`a836825ef9c219563bb2abc740707c825c26edc5` and digest
`sha256:c5f1306f91c7fe2db151c34131ca4cd39be9beba3d21d170f5757996338f375e`
also completed a current-chart compatible-image rollback with retained
additive schema and staged re-upgrade before the exact merged artifact was
deployed. Every stage kept
`resourceActions.authorityWorker.enabled=false`, converged or retained
global-user-state 028, Serve033, and API007, and left all action, shadow,
coverage, and cohort tables empty. This is binary/schema/mixed-version rollback
evidence only. It does not prove shadow parity, provider I/O, crash recovery of
an action, M4 authority, or the operational payoff.

The follow-up API006-rejection/API007-readiness correction and frozen
renderer-contract merge
commit `4f024b60f2fc71852fa8fb9747390f4d3917b03f` was deployed as immutable
image tag `resource-actions-4f024b60f` and digest
`sha256:06c9e71c5744ea970c41402fb9c4934e6722a7b53271f6715231b4b275525d25`.
Helm revisions 71--73 deployed that exact digest API -> ordinary executor ->
controller with the authority worker explicitly disabled. The final dark
checkpoint retained API007, Serve033, global-user-state 028, and capacity001;
all eight action-family tables and all Serve service/replica/cluster tables
had zero rows. This verifies that API-head readiness and the renderer contract
can be shipped dark. It did not verify the then-proposed server-owned API007
activation proof and is not evidence for the current API008 contract. That
historical image did not contain or exercise the
later pure-renderer implementation; runtime renderer integration, provider
I/O, and authority remain unproved.

The pure-renderer merge commit
`0e894c2a5d7186d15b10d62bbfdb8283201e4e63` was built from a clean detached
checkout, published as immutable tag `resource-actions-0e894c2a5` and digest
`sha256:b21f0e7cc39f62a21bc5887406f941d0b298d8fc277f0b5abb8b1f170c88b198`,
and deployed dark through Helm revisions 74--76. The final checkpoint had all
six ordinary-role Pods ready on that exact image ID with zero restarts and the
expected embedded commit. The packaged renderer inventory was byte-verified,
authority remained disabled and absent, the four schema heads were unchanged,
and every action/correlation/Serve graph count remained zero. This is
deployability evidence for the pure renderer, not live renderer invocation,
private dispatch, provider I/O, shadow parity, action recovery, or M4
authority evidence.

Canonical owner: this file. The provider-side companion is
`docs/designs/skyserve-resource-action-provider-facet.md`. The broader
`docs/designs/provider-lifecycle-actuation.md` remains authoritative for its
separate provider ownership and placement program.

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
- Roll out per service through `legacy -> shadow -> authoritative`. Before the
  represented private-shadow gate opens, only the characterized legacy path
  mutates providers. After that gate opens, each represented candidate uses one
  private request whose handler is the sole mutation owner; the legacy call is
  suppressed for that candidate and the two modes never mutate in parallel.
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

```text
ServeActionCapacityProfileV1 = {
  version: 1,
  profile: "ordinary_ondemand_physical_width1_v1",
  pool: false,
  replica_unit: "physical_backend",
  planned_capacity: 1,
  node_count: 1,
  use_spot: false,
  accelerator: null,
  spot_placer: null,
  reserved_capacity_fill: false,
  cost_rebalance: false,
  dynamic_ondemand_fallback: false,
  base_ondemand_fallback_replicas: 0
}
```

Absent fallback inputs normalize to false/zero. Every live or admitted replica
must also have `type(ReplicaInfo.is_spot) is bool`,
`ReplicaInfo.is_spot == false`, `planned_capacity == 1`,
`reserved_fill == false`,
`is_zero_cost == false`, `paid_capacity_pool_key == null`,
`cost_rebalance_for_replica_id == null`, and
`unknown_capacity_replacement == false`. A nonnull `spot_placer` is ineligible
even when its current placement happens to be on-demand or width one.
`legacy ->` private `shadow` and `shadow -> authoritative` both validate the
closed profile, elected `ServeServiceVersionSpecIdentityV1`, and every live
replica's creating-version identity under
the service/owner lock. A service update that would leave this profile while
the service is in private `shadow` or `authoritative` is rejected before its
version/spec commit; it never silently demotes the service. Admission repeats
the same check immediately before commit. The service lock serializes an
update/admission race into either one eligible old-version action or one
unsupported update/admission rejection, never mixed mutation ownership.

Non-consolidated pools, name-only pre-existing resources, mixed old/new
controller ownership, and providers lacking exact readback remain `legacy` or
`shadow`. Eligibility is explicit; it is never inferred from a version string
or table-exists check.

The prohibition on paid/reserved DML applies only to M4 proof, private
admission, action admission, and action reduction. An excluded service still
uses `LegacyServeReplicaMutationAdapter`, including its existing paid-capacity
and reserved-fill DML and lock protocol. The eligibility transaction must
release every service, version, and replica SQL lock before invoking that
adapter; the adapter is never called from a locked ineligible branch.

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

Every text field is NFC-normalized before validation. Canonical JSON is emitted
with Python `json.dumps(value, sort_keys=True, separators=(',', ':'),
ensure_ascii=False, allow_nan=False)` and encoded as UTF-8; UUIDs use lowercase
hyphenated text and integers use JSON integers. The fixed resource-action UUID
namespace is `ffa24895-49b7-5f76-9a32-ff22809e4dff`, itself UUIDv5 of
`https://skypilot.co/resource-actions/v1` in `uuid.NAMESPACE_URL`. `action_id`
is `uuid.uuid5(RESOURCE_ACTION_NAMESPACE, canonical_identity_bytes.decode(
'utf-8'))`. The human-readable equivalent is close to
`service_hash:replica_id:desired_generation:launch`, but the UUID derivation
also includes both incarnations so deleted/recreated services or replica IDs
cannot alias prior actions. These bytes and namespace are a versioned storage
contract and require a new identity version, not an in-place change.

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
and `service_incarnation` is the same value decoded as a UUID. The action
schema does not add a second service-incarnation column. A null, non-UUID, or
noncanonical legacy `services.hash` is ineligible rather than backfilled with a
fictitious identity.

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
`SETTLED`. For an M4 authoritative route it additionally calls the frozen
Serve039 retention validator on the same transaction and skips deletion until
the exact immutable terminal selector exists and matches the request. For an M4
private-shadow route the generic deleter first locks its class-15 request, then
the same validator nonlockingly point-reads the earlier-class immutable/terminal
represented child and same-key execution history. It requires child `COMPLETE`,
history `SETTLED`, and exact class-17 shadow terminal and settlement receipts.
Both receipt hashes, the settlement source/projection commitment, copied
request-return pair when a handler return won,
settlement basis, outcome/hash, final progress revision/hash, and request
terminal bytes must all cross-equal. A fallback requires a null return pair; a
handler basis requires the exact retained return pair. Any missing, crossed, or
partially copied relation retains the request.

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

### Serve039 historical execution authority and terminal selector

Current Serve038 cohort membership is mutable operational state. It is valid
for deciding whether a worker may claim now, but it is not historical proof
that an execution generation was authorized after a compatible handoff, lease
revocation, Pod replacement, or cold recovery. A V2 reducer therefore never
reconstructs old authority from the current registry. Additive PostgreSQL-only
Serve revision 039 has `down_revision='038'` and requires only the exact
Serve038 catalog at DDL time. Exact API008 plus the consolidated central
PostgreSQL catalog is a runtime activation/admission gate, not a cross-lineage migration
dependency. Serve039 installs an append-only historical lineage relation, an
append-only per-attempt terminal selector, a distinct append-only shadow-
request terminal history, a distinct append-only shadow-settlement commitment
history, distinct append-only linked-admission-fallback commitment and
fallback-progress histories, a retained process-supersession relation, a mutable
singleton authority-API GC cursor, and the nullable live-lease execution-owner
extension plus durable shadow-parent execution-route extension, before any V2
private dispatch:

```text
serve_resource_action_execution_authority_lineage
  action_id                     UUID not null
  attempt                       INTEGER not null
  request_id                    TEXT not null
  request_input_sha256          TEXT not null
  request_execution_generation BIGINT not null
  authority_worker_instance_id  UUID not null       # stable Pod/lease ID
  worker_instance_id            UUID not null       # process API claim owner
  claim_token_sha256            TEXT not null
  controller_generation         BIGINT null       # always null: NORMAL claim
  service_hash                  TEXT not null
  policy_epoch                  UUID not null
  policy_sha256                 TEXT not null
  authority_binding_sha256      TEXT not null
  policy_admission_state        TEXT not null       # OPEN | DRAINING
  policy_admission_revision     BIGINT not null
  cohort_id                     TEXT not null
  cohort_revision               BIGINT not null
  registration_set_revision     BIGINT not null
  worker_lease_revision         BIGINT not null
  reference_revision            BIGINT not null
  api_instance_started_at       TIMESTAMPTZ not null
  api_instance_heartbeat_at     TIMESTAMPTZ not null
  dispatch_membership           JSONB not null
  dispatch_membership_sha256    TEXT not null
  execution_authority           JSONB not null
  execution_authority_sha256    TEXT not null
  authorized_at                 TIMESTAMPTZ not null
  primary key (action_id, attempt, request_execution_generation)
  unique (request_id, request_execution_generation)
  index (cohort_id, authority_worker_instance_id)
  index (worker_instance_id, cohort_id)
  index (service_hash, policy_epoch, policy_sha256,
         authority_binding_sha256)

serve_resource_action_attempt_terminal_authority
  action_id                     UUID not null
  attempt                       INTEGER not null
  request_id                    TEXT not null
  request_input_sha256          TEXT not null
  request_terminal_state        TEXT not null
  request_execution_generation BIGINT not null
  authority_worker_instance_id  UUID null
  worker_instance_id            UUID null            # process API claim owner
  handler_name                  TEXT not null
  authority_disposition         TEXT not null
      # NO_SUCCESSFUL_CLAIM_START | LINEAGE
  lineage_generation            BIGINT null
  terminal_cause                TEXT not null
      # HANDLER_RETURN | REQUEST_FAILED | REQUEST_CANCELLED |
      # CLAIM_START_NOT_REPRESENTABLE | CLAIM_REAUTHORIZATION_FAILED |
      # TERMINAL_BEFORE_CLAIM_START
  request_finished_at           TIMESTAMPTZ not null
  primary key (action_id, attempt)
  unique (request_id)
  index (authority_worker_instance_id)
    where authority_worker_instance_id is not null
  index (worker_instance_id) where worker_instance_id is not null
  foreign key (action_id, attempt, lineage_generation)
      references serve_resource_action_execution_authority_lineage
                 (action_id, attempt, request_execution_generation)

serve_resource_action_shadow_request_terminal_history
  decision_id                   UUID not null
  request_sequence              INTEGER not null
  request_role                  TEXT not null
  request_id                    TEXT not null
  immutable_payload_sha256      TEXT not null
  request_input_sha256          TEXT not null
  handler_name                  TEXT not null
  request_terminal_state        TEXT not null
  request_execution_generation BIGINT not null
  authority_worker_instance_id  UUID null
  worker_instance_id            UUID null            # process API claim owner
  authority_disposition         TEXT not null
      # NO_SUCCESSFUL_CLAIM_START | SHADOW_EXECUTION
  execution_authority_lineage_sha256 TEXT null
  terminal_cause                TEXT not null
      # HANDLER_RETURN | REQUEST_FAILED | REQUEST_CANCELLED |
      # TERMINAL_BEFORE_CLAIM_START
  terminal_winner               JSONB not null
  terminal_winner_sha256        TEXT not null
  request_return_sha256         TEXT null
  request_finished_at           TIMESTAMPTZ not null
  primary key (decision_id, request_sequence)
  unique (request_id)
  index (authority_worker_instance_id)
    where authority_worker_instance_id is not null
  index (worker_instance_id) where worker_instance_id is not null

serve_resource_action_shadow_admission_fallback_history
  decision_id                   UUID primary key
  operation_id                  UUID not null
  deterministic_request_id      UUID not null
  fallback_commitment           JSONB not null
  fallback_commitment_sha256    TEXT not null
  committed_at                  TIMESTAMPTZ not null
  unique (operation_id)

serve_resource_action_shadow_admission_fallback_progress_history
  decision_id                   UUID primary key
  fallback_operation_id         UUID not null
  progress_operation_id         UUID not null
  fallback_commitment_sha256    TEXT not null
  progress_kind                 TEXT not null
      # LEGACY_PRE_SUBMIT | TERMINAL_NO_CALL_RELEASE
  first_request_sequence        INTEGER null
  progress_commitment           JSONB not null
  progress_commitment_sha256    TEXT not null
  progressed_at                 TIMESTAMPTZ not null
  unique (progress_operation_id)
  check ((progress_kind = 'LEGACY_PRE_SUBMIT' and
          first_request_sequence = 1) or
         (progress_kind = 'TERMINAL_NO_CALL_RELEASE' and
          first_request_sequence is null))

serve_resource_action_shadow_settlement_history
  decision_id                   UUID not null
  request_sequence              INTEGER not null
  request_role                  TEXT not null
      # PRIMARY_LAUNCH | PRIMARY_DOWN only
  operation_id                  UUID not null
  terminal_history_sha256       TEXT not null
  successor_kind                TEXT null
      # retry_same_plan | observe_same_plan | partial_down
  successor_decision_id         UUID null
  successor_request_sequence    INTEGER null
  settlement_commitment         JSONB not null
  settlement_commitment_sha256  TEXT not null
  settled_at                    TIMESTAMPTZ not null
  primary key (decision_id, request_sequence)
  unique (operation_id)
  unique (decision_id)
      where successor_kind = 'partial_down'
  unique (successor_decision_id, successor_request_sequence)
      where successor_kind = 'partial_down'
  index (successor_decision_id, successor_request_sequence)
      where successor_kind = 'partial_down'
  check ((successor_kind is null and successor_decision_id is null and
          successor_request_sequence is null) or
         (successor_kind is not null and successor_decision_id is not null and
          successor_request_sequence is not null))

serve_resource_action_shadow_execution_history
  decision_id                   UUID not null
  request_sequence              INTEGER not null
  request_role                  TEXT not null
      # PRIMARY_LAUNCH | PRIMARY_DOWN only
  request_id                    TEXT not null
  handler_name                  TEXT not null
  immutable_payload_sha256      TEXT not null
  request_input_sha256          TEXT not null
  preflight_request             JSONB not null
  preflight_request_sha256      TEXT not null
  preflight_response            JSONB not null
  preflight_response_sha256     TEXT not null
  phase                         TEXT not null
      # BOUND | AUTHORIZED | SETTLED
  request_execution_generation BIGINT null           # exactly 1 when nonnull
  authority_worker_instance_id UUID null
  worker_instance_id            UUID null
  claim_token_sha256            TEXT null
  dispatch_membership           JSONB null
  dispatch_membership_sha256    TEXT null
  execution_authority           JSONB null
  execution_authority_sha256    TEXT null
  execution_authority_lineage_sha256 TEXT null
  authorized_at                 TIMESTAMPTZ null
  provider_io_boundary          TEXT not null
      # NOT_STARTED | INTENT_COMMITTED | SUBMITTED_OR_AMBIGUOUS
  provider_progress_revision    BIGINT not null
  provider_progress             JSONB null
  provider_progress_sha256      TEXT null
  provider_operation_id         TEXT null
  provider_effect_trace         JSONB not null
  provider_effect_trace_sha256  TEXT not null
  request_return                JSONB null
  request_return_sha256         TEXT null
  terminal_history_sha256       TEXT null
  settlement_basis              TEXT null
      # HANDLER_RETURN | REQUEST_FALLBACK
  reduction_disposition         TEXT null
      # S | R | U | B | Q | P0 | O | X
  partial_down_decision_id      UUID null
  partial_down_request_sequence INTEGER null
  partial_down_basis_sha256     TEXT null
  revision                      BIGINT not null
  created_at                    TIMESTAMPTZ not null
  updated_at                    TIMESTAMPTZ not null
  settled_at                    TIMESTAMPTZ null
  primary key (decision_id, request_sequence)
  unique (request_id)
  foreign key (decision_id, request_sequence)
      references serve_resource_action_shadow_attempts
                 (would_be_action_id, request_sequence) on delete cascade
  index (authority_worker_instance_id)
      where authority_worker_instance_id is not null
  index (worker_instance_id) where worker_instance_id is not null
  unique partial (partial_down_decision_id, partial_down_request_sequence)
      where partial_down_decision_id is not null
  check ((partial_down_decision_id is null and
          partial_down_request_sequence is null and
          partial_down_basis_sha256 is null) or
         (partial_down_decision_id is not null and
          partial_down_request_sequence is not null and
          partial_down_basis_sha256 is not null))

alter serve_resource_action_worker_registration_leases
  add execution_owner JSONB null
  add execution_owner_sha256 TEXT null
  add execution_owner_api_instance_id UUID null
  add unique partial (execution_owner_api_instance_id)
      where execution_owner_api_instance_id is not null

serve_resource_action_worker_process_supersessions
  cohort_id                     TEXT not null references
                                      serve_resource_action_worker_cohorts(cohort_id)
  supersession_id               UUID not null
  authority_worker_instance_id  UUID not null
  operation_id                  UUID not null
  source_lease_generation       BIGINT not null
  source_lease_revision         BIGINT not null
  committed_lease_generation    BIGINT not null
  committed_lease_revision      BIGINT not null
  prior_api_instance_id         UUID not null
  current_api_instance_id       UUID not null
  prior_execution_owner         JSONB not null
  prior_execution_owner_sha256  TEXT not null
  current_execution_owner       JSONB not null
  current_execution_owner_sha256 TEXT not null
  container_supersession_proof  JSONB not null
  container_supersession_proof_sha256 TEXT not null
  request_claims                JSONB not null
  request_claims_sha256         TEXT not null
  completed_at                  TIMESTAMPTZ not null
  primary key (cohort_id, supersession_id)
  unique (cohort_id, operation_id)
  unique (prior_api_instance_id)
  unique (current_api_instance_id)
  check (supersession_id = operation_id)

serve_resource_action_api_instance_gc_cursors
  cursor_name                   TEXT primary key
                                      # exactly "authority-worker-v2"
  sweep_epoch                  BIGINT not null       # nonnegative
  sweep_upper_bound_instance_id UUID null
  after_instance_id            UUID null
  revision                     BIGINT not null       # positive
  last_operation_id            UUID not null
  updated_at                   TIMESTAMPTZ not null
  check (cursor_name = 'authority-worker-v2')
  check (sweep_epoch >= 0 and revision > 0)
  check ((sweep_upper_bound_instance_id is null and
          after_instance_id is null) or
         (sweep_upper_bound_instance_id is not null and
          (after_instance_id is null or
           after_instance_id <= sweep_upper_bound_instance_id)))
```

`dispatch_membership` is the complete typed accepted registration-set/member,
no-handoff/cold-recovery fence, reference, API-instance lease, and request-
claim preimage used by the claim decision. `execution_authority` is the exact
server-built readiness/authority proof consumed before that generation may
commit its first attestation or provider-I/O watermark. Claim tokens are never
stored, only their lowercase SHA-256. Both JSON/hash pairs are independently
object-shaped, canonicalized by their closed readers, and limited to 65,536
stored UTF-8 bytes. The row is not encoded as a third enclosing JSON object, so
the two separately bounded children do not widen the generic stored/wire-object
ceiling.

The process-supersession row intentionally has exactly one foreign key: its
restrictive cohort FK. The writer already holds the class-3 cohort before the
class-4 insert. It has no lease, API-instance, request, queue, lineage, selector,
or shadow-history FK, because their implicit parent locks would jump forward
from class 4 and make the later explicit class-5/14/15/16/17 order impossible.
Those relations are closed-parsed and cross-validated under their explicit
locks instead.

The shadow-admission-fallback history is likewise a permanent compact
acknowledgement receipt with no FK to the deletable parent/reference/coverage/
legacy graph. Its closed
`ProviderShadowLinkedAdmissionFallbackCommitmentV1` object/hash pair must equal
the scalar decision, operation, deterministic request, and commit time. The
fallback transaction inserts it at class 17 in the same commit as the parent/
reference transition. Immediate graph-stored adoption requires the exact
`LEGACY_CONTROLLER/RUNNING` plus `SHADOW_ACTIVE` post-state, complete private-
descendant absence, and fallback-progress-history absence. It returns the
original graph so the decision-keyed, idempotent legacy signal is still issued.

The separate FK-free fallback-progress history is inserted exactly once at the
later class-17 phase, either atomically with the first legacy `PRE_SUBMIT` child
or atomically with proved-no-call `ABANDONED_PRE_SUBMIT`, reference release,
capacity/slot release, and terminal parent transition. Its closed
`ProviderShadowLinkedAdmissionFallbackProgressCommitmentV1` object/hash pair
binds the original fallback operation/commitment, its own operation, the closed
progress kind, the first child identity and transition for `LEGACY_PRE_SUBMIT`
or the no-call release transition, and the one database progress time. It is
immutable and permanent but not a GC root. Receipt-only fallback adoption is
legal only when this progress receipt plus either its exact retained legal
first-transition descendant or the complete typed post-GC absence proof is
present. It compares the caller-retained original admission source/failure/
operation directly with the fallback receipt, returns no graph, and never
signals or recreates a second owner. A different valid commitment is a lost
race. The store, not its caller, selects the arm under the complete lock prefix;
an immediate, missing, partial, or crossed graph cannot bypass signaling or
corruption validation by asking for receipt-only adoption.

The shadow-settlement history is a permanent compact acknowledgement receipt,
not a retention root for the mutable evidence graph. It deliberately has no FK
to coverage, parent, child, execution history, request, queue, replica, or
service. Its closed `ProviderShadowSettlementCommitmentV1` object/hash pair is
independently canonicalized and bounded at 65,536 bytes; the scalar identity,
terminal-history hash, successor kind/key triple, and settle time must
byte-equal the adjacent object. The partial unique source index on
`decision_id` both bounds outgoing-Q discovery and physically permits at most
one Q receipt for a parent; the partial unique target index permanently exposes
the unique incoming-Q source after mutable execution history is collected.
Typed GC and receipt-only replay classify a whole parent component as ordinary,
outgoing Q, or incoming Q from these scalar keys rather than from the
particular receipt being replayed. A second or crossed Q peer is corruption. The
settlement transaction inserts it at class 17 after all class-10 graph writes
and class-15/16 request/queue locks. A same-key conflict exact-adopts only a
byte-equal commitment; a different operation or projection is a lost race and
malformed/crossed evidence is corruption. Typed evidence GC requires this
receipt before deleting a settled graph, but the receipt never prevents that
deletion and never authorizes graph reconstruction or another successor.

The one-to-one shadow-execution row is the action-free private-shadow journal.
It never creates an API006 action/attempt row and exists only beside an existing
represented child whose `planned_execution_kind="private_api_request"`, role is
`PRIMARY_LAUNCH | PRIMARY_DOWN`, request ID and immutable payload match, and
handler is kind-matched. A `LAUNCH_CLEANUP_DOWN` child is permanently the frozen
legacy-shadow-only shape and can never have this row or enter a private handler.
The cascading foreign key shares the child/parent retention boundary; typed GC
locks the child and same-key history in class 10 before deletion. Existing
pre-039 and legacy-controller children need no backfill and remain distinguishable
by the absence of this relation.

Every JSON/hash pair in the history is object-shaped, canonically revalidated,
and independently bounded at 65,536 UTF-8 bytes. The immutable preflight
request/response pairs are always nonnull; all later authority/progress/return
pairs retain the phase-specific nullability below. Its nonnull provider-effect-
trace/hash pair starts as the exact empty `LegacyProviderEffectTraceV1` and is
the private execution's comparable transport journal; the name of the child
column is retained for schema compatibility, not ownership. The row is never
serialized as one enclosing object. Identity/request/input/handler/preflight
fields and `created_at` are immutable. `revision` begins at one and increments exactly once
per successful authority, progress, or settlement CAS; `updated_at` is one
PostgreSQL operation time and never regresses. `BOUND` has a wholly null
authority bundle and no settlement fields. Its journal is either exact empty
`NOT_STARTED`/revision zero/null progress/hash/operation ID, or the sole inherited
retry seed: `NOT_STARTED`, revision one, byte-equal predecessor cursor/hash,
null current worker attestation, and the predecessor operation ID when present.
`AUTHORIZED` has generation one, both distinct stable/process worker IDs, token
hash, both proof/hash pairs, lineage hash, and `authorized_at`; settlement,
reduction, and partial-down fields remain null. `SETTLED` retains either that
complete authority bundle or the exact all-null bundle when terminalization won
before successful claim-start, and has a terminal-history hash, basis,
reduction disposition, and settlement time. `HANDLER_RETURN` requires the
complete distinct shadow return pair; `REQUEST_FALLBACK` requires it null. The
three partial-down fields are all nonnull exactly for a launch
`reduction_disposition=Q` with handler basis, and otherwise all null. They name
the exact normal primary-down child/history and canonical
`ServeShadowPartialLaunchCleanupBasisV1` hash created in that same settlement
transaction. The partial unique/indexed source pointer is the normalized GC
root; no retention decision scans the basis JSON.
Every provider-call checkpoint updates progress and the provider effect trace in
one revision CAS. Before a call it appends the exact bounded request/path/body
entry with null response; afterward it may resolve only that entry's response
and returned identifier. Prior entries are immutable, sequence/kind is fixed by
the invocation, and crash-after-call-before-resolution deliberately leaves a
null ambiguous response. The strict handler return binds the final trace hash.
Settlement requires it byte-equal to history and copies the same full trace/hash
to the completed child's `legacy_effect_trace` columns; neither the return nor
the settlement projector may synthesize it from final progress.
For `BOUND`, `AUTHORIZED`, and non-`X` `SETTLED`, progress revision zero is
equivalent to null progress/hash and a positive revision requires a valid
`ProviderShadowLifecycleProgressV1`; crossing `NOT_STARTED` requires positive
progress and a nonnull current worker attestation. A literal `X` settled row is
read through the bounded raw-history layer: it preserves the original raw
progress bytes and declared hash/revision/watermark/operation ID while every
outer identity, effect-trace, receipt, and settlement field remains strict. The
write-once operation ID is normalized exactly as for action progress. SQL owns
outer enum/counter/pair/phase checks; typed readers own all nested and cross-row
equalities.

The GC cursor has no foreign key. Its UUIDs are ordering markers, not retained
API identities, and may name a deleted or never-again-present row. The one exact
singleton is mutable scheduling metadata and grants no deletion authority;
every deletion still comes from the locked target row plus the complete root
proof below. A missing cursor is initialized on first use with epoch zero, null
markers, revision one, a caller-minted operation UUID, and one PostgreSQL time.
Concurrent initialization accepts any already-valid singleton as a scheduling
successor rather than treating its different operation UUID as corruption.

The lineage uses only Serve-local restrictive foreign keys:
`action_id -> serve_resource_action_worker_cohort_refs.decision_id`,
`(cohort_id, authority_worker_instance_id) ->
serve_resource_action_worker_registration_leases`, and the existing unique
`(service_hash, policy_epoch, policy_sha256, authority_binding_sha256)` policy
binding. It has no foreign key to API action, attempt, request, or queue tables.
Those histories are independently migrated and can be physically separate
outside the consolidated-authority deployment; the eligible consolidated
PostgreSQL transaction instead locks and byte-validates the complete
cross-store relationship, including reference cohort/service, action identity,
request/input, policy admission, and worker equality not expressible by those
three foreign keys. Separate indexes on
`(cohort_id, authority_worker_instance_id)` and
`(worker_instance_id, cohort_id)` make stable-member retention and process/API
retention exact without scans; PostgreSQL does not synthesize the child-side FK
index. Action selectors and shadow terminal histories each have partial indexes
on both nonnull stable and process worker IDs. A second lineage index on the
policy-binding tuple makes restrictive parent operations bounded.
Row checks require positive attempt, `request_execution_generation = 1`, null
`controller_generation`,
and positive revision fields, `registration_set_revision == cohort_revision`,
canonical request UUID
and service UUID text, lowercase hashes,
`policy_admission_state in ('OPEN', 'DRAINING')`, JSON
object/size/hash pairs, distinct stable/process identities, and API heartbeat
not before instance start. The dispatch membership requires the lease execution
owner's stable ID to equal `authority_worker_instance_id` and its process API ID
to equal the generic request's `worker_instance_id`; the API row repeats that
same pair. Typed
readers own every nested and cross-row equality.

The terminal selector is the durable, request-GC-safe answer to which terminal
generation/worker/cause a settled attempt used. It has only the nullable
same-class composite lineage foreign key shown above and no reference or API-
table foreign key. A reference FK would make PostgreSQL acquire a hidden class-
6 parent lock after the class-15/16 terminalization locks. The one typed same-
transaction writer point-loads and byte-validates every named lineage before it
inserts or exact-adopts any selector; the immediate FK therefore reacquires
only already-ordered parent keys and cannot introduce an implicit lineage lock
after a later selector key. Typed reference retention/GC includes all
selectors. Row checks require positive
attempt, generation exactly zero or one, canonical
request UUID/lowercase input hash, one of the two kind-matched private handler
names, and the closed terminal/cause enums. `LINEAGE` requires generation one,
nonnull stable worker and process claim owner, `lineage_generation ==
request_execution_generation`, and the matching lineage row.
`NO_SUCCESSFUL_CLAIM_START` requires null `lineage_generation`; generation zero
requires both IDs null, while generation one requires the exact nonnull stable
worker and process claim owner. `CLAIM_START_NOT_REPRESENTABLE` requires the latter generation-one
shape and an absent lineage before a candidate insert.
`CLAIM_REAUTHORIZATION_FAILED` requires `LINEAGE`, `FAILED`, and the exact
previously committed generation after a stored-lineage adoption replay fails a
current-successor or representability check. `TERMINAL_BEFORE_CLAIM_START`
covers any request terminalization that
wins before successful claim-start, including cancellation/failure after a
generation was assigned. The exact state/disposition/cause table is:

| terminal cause | authority disposition | request terminal state | generation/worker |
|---|---|---|---|
| `HANDLER_RETURN` | `LINEAGE` | `SUCCEEDED` | generation one; both IDs non-null; named lineage |
| `REQUEST_FAILED` | `LINEAGE` | `FAILED` | generation one; both IDs non-null; named lineage |
| `REQUEST_CANCELLED` | `LINEAGE` | `CANCELLED` | generation one; both IDs non-null; named lineage |
| `CLAIM_START_NOT_REPRESENTABLE` | `NO_SUCCESSFUL_CLAIM_START` | `FAILED` | generation one; both IDs non-null; no lineage |
| `CLAIM_REAUTHORIZATION_FAILED` | `LINEAGE` | `FAILED` | generation one; both IDs non-null; named lineage |
| `TERMINAL_BEFORE_CLAIM_START` | `NO_SUCCESSFUL_CLAIM_START` | `FAILED` or `CANCELLED` | generation zero with both IDs null, or generation one with both IDs non-null; no lineage |

No other combination parses. In particular, a request cannot become
`SUCCEEDED` without a successful claim-start and typed handler return.

The shadow terminal-history row is request-lifecycle evidence, not an API006
action lineage. It has no FK to class-9/10 shadow rows or API tables because
central terminalization already holds class 15/16 and may not acquire an earlier
lock implicitly. Its typed same-transaction writer closed-decodes the action-
free primary-shadow route and reconstructs the immutable request input before
queue deletion. Checks require a positive sequence, a `PRIMARY_LAUNCH |
PRIMARY_DOWN` role and kind-matched shadow handler, canonical request/
correlation, lowercase invocation/payload hashes, and exactly this matrix:

| terminal cause | authority disposition | request state | generation / workers / lineage hash |
|---|---|---|---|
| `HANDLER_RETURN` | `SHADOW_EXECUTION` | `SUCCEEDED` | generation one; both IDs and the exact history lineage hash nonnull |
| `REQUEST_FAILED` | `SHADOW_EXECUTION` | `FAILED` | generation one; both IDs and lineage hash nonnull |
| `REQUEST_CANCELLED` | `SHADOW_EXECUTION` | `CANCELLED` | generation one; both IDs and lineage hash nonnull |
| `TERMINAL_BEFORE_CLAIM_START` | `NO_SUCCESSFUL_CLAIM_START` | `FAILED` or `CANCELLED` | generation zero/both IDs null, or generation one/both IDs nonnull; lineage hash null |

The receipt also stores the exact reconstructed `request_input_sha256` and an
independently bounded typed `ProviderShadowTerminalCommitmentV1` JSON/hash
pair. The commitment is derived from the complete trusted terminal-winner
source, excludes database-owned finish time and transient lease deadlines, and
retains the stable/process/token, strict-return or fixed-failure, cancellation-
intent, and typed fence-operation commitments that distinguish callers. A
handler winner has the exact strict nonnull `request_return_sha256`; every
failure, cancellation, and fence winner has it null. The winner kind and the locked
`BOUND` versus `AUTHORIZED` history deterministically derive the matrix row:
callers never supply authority disposition or terminal cause. Typed stale-
owner, cold-recovery, and process-supersession winners additionally hash their
complete typed, time-free fence-commitment projection derived from the
preterminal enclosing operation and its sorted mixed action/shadow members.
That projection contains no receipt, completed
fence claim, terminal event, or hash of those outputs; the terminalizer derives
the complete final operation afterward, so no receipt recursively hashes itself.
Those immutable commitment columns make the receipt sufficient to distinguish
`EXACT_ADOPTED` from a different legal `LOST_RACE` after request GC and later
evidence GC; neither adoption requires a current shadow service mode or
recreates a deleted parent/child/history row.

For `SHADOW_EXECUTION`, the terminalizer nonlockingly reads the already write-
once `AUTHORIZED` history named by the locked request and requires its stable/
process IDs, generation, request/input, and lineage hash byte-equal. For
`NO_SUCCESSFUL_CLAIM_START`, the history must still be `BOUND` with its wholly
null authority bundle. Claim-start and terminalization serialize on the same
class-15 request lock: claim-start locks the class-10 prefix before class 14/15,
then changes `BOUND -> AUTHORIZED` while holding the request; a terminal winner
never reaches backward and writes only the class-17 receipt. Thus the receipt
cannot claim execution authority that was not durably committed. The row is
inserted/exact-adopted in the same request transaction as the terminal API row
and event; unequal conflict is corruption.

A later shadow settlement locks its parent, child, and same-key execution
history in the normal earlier-class order, then nonlockingly reads the immutable
terminal receipt and terminal request if retained. It copies the exact receipt
hash and optional strict return into `SETTLED` before request GC may proceed.
The terminal-history row itself has no M4 update, TTL, or deletion path and
remains the GC-safe receipt for stale-handoff, process-supersession, and cold-
recovery fences.

The request-terminal transaction inserts/exact-adopts this selector while it
still owns the request token/generation fence. Because claim-start locks the
same request before lineage, the winner is unambiguous: a successful claim-start
commits lineage first and terminalization records `LINEAGE`; a terminalization
that wins first records `NO_SUCCESSFUL_CLAIM_START`, after which claim-start
fails the terminal request check and cannot insert lineage. Conflict with any
unequal selector byte is corruption. The selector contains no return body and
does not duplicate the immutable outcome; it retains only the authority lookup
key needed after the request row is deleted.

One immutable row exists for the sole generation-one claim of each request that
actually consumes authoritative readiness or commits a worker attestation.
First insert uses one PostgreSQL `database_now` for `authorized_at` and every
same-transaction checked-at field. Lost-ack replay does not mint a fresh
timestamped candidate: it reads the immutable row, validates its hashes and
stable action/request/generation/token key, replays the stored proof at its
stored `authorized_at`, and first requires retained membership/lease/policy/
request rows to be legal historical descendants. Legal historical descendants include lease renewal
or terminal revocation, cohort handoff/revision advance, reference release /
revision advance, and policy close/supersession/revision advance; they preserve
the stable worker/action/reference lineage but do not renew its authority. This
predicate validates immutable history only. A separate current-execution
predicate must still prove the same live generation/token, current process
owner, fresh accepted membership/lease/API instance, an `ACTIVE` policy whose
admission state is `OPEN | DRAINING`, the exact bound `ACTION_ACTIVE` reference,
and absence of a blocking handoff before handler invocation or any new
checkpoint/I/O. `DRAINING` is legal only for an action/reference already bound
to that exact policy before its admission-state CAS; creation of a new
reference/action still requires `OPEN`. A terminal/closed/released historical descendant can validate
the old row but necessarily fails current execution authorization. It never
overwrites history. A conflict on the
same key with unequal stable authority is permanent corruption and blocks
before provider I/O. The same generation may
commit multiple progress/effect checkpoints, but its authority preimages do
not change; every V1-shaped progress attestation names that immutable lineage
key. `ReplayPolicy.NEVER` forbids a second claim generation for the same
request. Same-owner post-future acknowledgement and typed process/Pod-stale or
full-cold-recovery quiescence fences terminalize the old request; generic lease
expiry alone never does. The reducer
then creates attempt `n+1` with a new deterministic request and the carried
journal. Thus history grows with actual action attempts rather than overwriting
a current member or fabricating a fixed action-wide slot model.
Lineage proves only that one generation consumed authority at `authorized_at`.
It is not evidence that provider I/O occurred, cannot disqualify a no-I/O
outcome by itself, and is never a bearer credential for a later effect.

Successful claim-start is the lineage linearization point. Its lineage insert
is one transaction on one server-created consolidated PostgreSQL connection;
when the current journal already has or initializes a claim-bound attestation,
that API006 write is part of the same transaction, otherwise the immutable
lineage row alone records consumption of readiness before handler invocation.
The
transaction locks owner/service/policy/version/replica, cohort, nonterminal
handoff, selected lease plus exact execution owner, reference,
action, predecessor/current attempts,
API-instance, request, and queue in the global order, reads one database time,
revalidates current
membership and the fresh claim after every wait, then inserts/exact-adopts the
lineage and any initial API006 journal write. It never opens a second checkout.
Every later checkpoint revalidates the same immutable lineage key. Handoff or
revocation may prevent a new checkpoint, but cannot invalidate already-
committed historical authority and can never let that history authorize new
I/O.

Reduction first loads the exact attempt terminal selector, then extracts the exact origin keys named by the bounded current /
predecessor progress and terminal evidence, deduplicates and sorts them by the
lineage primary key, and point-loads only those rows. It never scans lineage.
It validates the loaded rows against the immutable action spec, reference,
policy, and worker attestations. It does not require those workers or leases to
remain current or fresh. A `NOT_STARTED`, null-progress attempt has no lineage
row only when its selector proves `NO_SUCCESSFUL_CLAIM_START`: either
terminalization won the request race before the gate or the bounded claim-start
representability gate rejected after assigning generation one. Both
reduce only through the exact no-I/O fallback/direct contract and persist the
fixed cause. If a raw invalid journal cannot reveal an attestation, the
immutable selector's request execution generation selects the lineage row.
Absence is legal only for that exact no-I/O selector shape and otherwise yields
blocking corruption. Request GC remains excluded until the attempt is settled
and its selector is present; afterward the attempt/outcome plus append-only
selector and lineage retain the required request/generation/worker/input
authority.

V2 contexts are deliberately disjoint. Linked admission carries the exact two
currently accepted members. Retry materialization carries the immutable V2
spec/resolved cohort and predecessor historical lineage, but no selected
current member. One claimed execution carries exactly one current membership
and one immutable lineage key. Reduction carries the stored V2 spec/resolved
cohort and only the historical lineage rows named by its bounded progress /
outcome slice. `_ActionContext.from_record()` remains V1-only; explicit V2
wrappers accept these contexts and never perform ambient cohort lookup or
substitute current membership for history. Private shadow has no API005/006
action row. Its separate class-10 execution history, progress/origin types,
reduction matrix, and representability gate are specified below; it remains
disabled until that complete contract is implemented and passes the gate.

Serve039 owns explicit `SERVE039_METADATA` for all nine wholly new relations,
plus separate exact factories for the lease owner/hash/indexed-process-scalar
extension, shadow-parent execution-route/fallback columns and CHECK, and
replacement shadow-child execution-kind CHECK, rather than
appending tables to the dynamically enumerated `SERVE038_METADATA`. Its
migration does not backfill or reinterpret
old action rows and refuses downgrade; application rollback retains all
lineage, selectors, shadow execution/terminal/settlement histories, both
admission-fallback histories, process
supersessions, and lease-owner bytes. `serve_target_version()` honors the chart-owned exact `037`
migration ceiling on PostgreSQL only for the documented cleanup-only phase;
M4 and the initial M5a phase target exact `039`, while the M5a-only server-
authorized rollback-closure gate may target exact `040`. The still-
supported local/controller SQLite path targets exact `037`, and every other
dialect rejects; SQLite's target is never derived from the global PostgreSQL
head.

Serve039 migration is one PostgreSQL transaction with an explicit DDL lock
program. Before DDL it exact-reflects either head 038 plus all expected 038
objects or an exact complete-039 catalog at the old stamp; any incompatible
partial-039 shape fails before mutation. From exact 038 it takes `ACCESS
EXCLUSIVE` in application order on every existing writer-prefix table:
authority policy epochs (class 2), worker cohorts (class 3), registration
handoffs (class 4), registration leases (class 5), cohort refs (class 6), and
shadow parents (class 9), then shadow-attempt children (class 10). It cannot lock a not-yet-created 039
relation. Exact-complete-039 old-stamp adoption instead uses one merged global
schedule, never the 038 prefix followed by an appended suffix: policy epochs
(class 2); cohorts (class 3); handoffs and process-supersession history in
canonical relation order (class 4); leases (class 5); refs (class 6); shadow
parents (class 9); shadow children and shadow execution history in canonical
relation order (class 10);
the API-GC cursor (class 13); then lineage, action selectors, shadow terminal
history, shadow admission-fallback history, shadow admission-fallback-progress
history, and shadow settlement history in canonical relation order (class 17).
Thus complete-catalog adoption
never acquires class 4 or 10 after a later class.
It then re-reflects/re-audits under those locks and holds them through DDL or
empty-catalog adoption, postcondition reflection, and Alembic stamp. The
advisory migration lock alone is not writer exclusion.

Under that retained schedule it adds the lease owner/hash/scalar columns,
replaces the named lease CHECK, adds its partial unique index, and ALTERs the
existing shadow-parent relation in class 9. The parent ALTER first adds nullable
`execution_route`, `private_fallback_reason`, `private_fallback_evidence`, and
`private_fallback_evidence_sha256`; installs the temporary server default
`execution_route='LEGACY_CONTROLLER'`; deterministically updates every pre-039
row to `LEGACY_CONTROLLER` with the complete fallback triple null while writers
are excluded; installs the closed route/fallback/evidence CHECK; and only then
makes `execution_route` NOT NULL. The default remains through the complete M4
application-rollback window, including the all-M5a -> exact-M4 -> all-M5a
deployment matrix, so a pre-039 legacy-shadow writer can still insert a
truthful ordinary-legacy row during the earlier dark pre-owner rollback phase
and both M4/M5a binaries see one exact Serve039 catalog. It cannot mint private
or fallback state, and every M4 and M5a typed writer must supply the route
explicitly. The separately gated second deployment phase of stacked M5a PR
#1240 drops this deprecated default under Serve040 only after the exact-039
M5a rollback/re-upgrade matrix passes and M4 rollback is explicitly closed; it
never changes the column, CHECK, route values, ownership, or durable history.
There is no inference from child rows and no private backfill: every pre-039
parent is legacy-controller history by construction. It then replaces the shadow-child
execution-kind CHECK to admit only the exact primary private-request shape,
creates process supersession with only its cohort FK, creates the FK-free GC
cursor, then lineage, action selector, shadow execution history, shadow
terminal history, FK-free shadow admission-fallback history, FK-free shadow
admission-fallback-progress history, and FK-free shadow settlement history with the declared
Serve-local FKs plus the outgoing-parent and incoming-target partial uniqueness
indexes. It performs no API-lineage DDL:
API008 remains an independent runtime activation dependency. The four-handler
pre-activation/old-image-rollback audit is instead an exact unbounded `EXISTS`
scan inside the policy/cohort activation gate with a fixed 60-second statement
timeout; timeout or read failure blocks activation/rollback. Later process-owner
recovery uses the existing process-first active-claim index and closed
validation. Only
the complete exact catalog passes post-reflection and is stamped 039. Lock
timeout rolls back every DDL/index/stamp byte. An exact already-complete 039
catalog at an old stamp is adoptable only when all nine new relations are empty
and every lease owner/hash/scalar is null. Its shadow-parent columns/CHECK must
already be exact and every existing row must be
`LEGACY_CONTROLLER` with the complete fallback triple null; a pending/private/
fallback route at an old stamp
rejects adoption. Any nonempty or unequal partial object
fails before mutation. The independent runtime activation gate, not migration or
old-stamp adoption, owns the controlled full-table four-handler API inventory
audit.
Bidirectional tests hold each 038 writer prefix against migration and each DDL
prefix against a writer, proving wait/timeout without reverse acquisition,
partial catalog, or false stamp. A complete-039 old-stamp fixture additionally
asserts the literal merged relation order, exact parent backfill/reflection, and
races class-4 process history
against class-10 shadow history in both directions, so adoption cannot regress
to prefix-then-suffix locking. Compatibility tests hold a pre-039 legacy writer
across migration, then insert a parent while omitting every new column and
require the stored route/default plus null fallback triple; new typed-writer
tests fail if any M4 or M5a call omits an explicit route. Exact Serve039 M4 and
M5a catalog fixtures require the compatibility default to remain present; the
separate Serve040 migration/catalog fixture requires it absent.

Serve040 has `down_revision='039'` and is a distinct forward schema revision,
never a second shape under the 039 stamp. The one M5a PR #1240 image ships its reviewed but dormant
`040_drop_shadow_execution_route_default` revision and an exact target resolver
that targets 039 from 038/039, but preserves and accepts an already-current
exact 040 database rather than attempting a downgrade. Its server-owned target
authorization changes to 040 only after the same image has completed the
exact-039 M5a -> M4 -> M5a matrix, persisted the rollback-closure evidence,
closed admission, and drained bound work to zero; a free chart value cannot
bypass that gate. The gated Serve target call supplies one typed, one-shot
Serve040 `on_version_apply` registration to `safe_alembic_upgrade()`. The
private Alembic config attribute binds its server-minted migration operation
ID, exact `serve_db` section, current/target `039/040`, and callback; `env.py`
passes that callback and the same registration through only the online
`context.configure()`. The 040 revision refuses to mutate unless that exact
registration is present in its `MigrationContext`, and an offline 040 render
is forbidden. Neither a chart/CLI value nor a direct ordinary
`alembic upgrade 040` can synthesize it. No other database section, target, or
revision receives the callback.

Migration 040 then takes the migration advisory lock,
nonlockingly discovers the complete service set, locks/revalidates every
controller-owner/service fence in canonical class-1/2 key order, takes
`ACCESS EXCLUSIVE` on the class-2 authority-policy relation, locks every active
closure-policy row and freezes every successor key, and only then takes
`ACCESS EXCLUSIVE` on the class-9 shadow-parent relation. Under those locks it
revalidates every active policy/zero-work/no-shadow/M5a-attestation predicate,
exact-reflects the complete default-bearing Serve039 catalog and stamp, executes
only `ALTER TABLE serve_resource_action_shadow_samples ALTER COLUMN
execution_route DROP DEFAULT`, exact-reflects the otherwise byte-identical
default-free catalog, and stores one closed in-memory
`Serve040MigrationHandoffV1` on the current Alembic `MigrationContext`. That
handoff binds the exact 039 -> 040 step, database/schema identity, canonical
locked service/closure-predecessor inventory, closure-evidence hashes, and the
independently reflected pre/post catalog hashes. It is neither durable evidence
nor a caller-supplied trust source. The revision does not stamp manually or
release a lock when its function returns.

Alembic's `HeadMaintainer` then advances the Serve version row from 039 to 040
on the same connection and transaction and invokes the configured callback
before the outer transaction can commit. The callback accepts only a real
upgrade step with source `(039)`, destination `(040)`, callback `heads={040}`,
and exactly one unconsumed handoff. It re-reads actual `008/040/028`, the
database/schema identity, the complete default-free catalog, locked service
set, and every closure predecessor; recomputes the post-catalog hash against
the handoff; and captures one database verification time. For each service in
canonical order it constructs a distinct `Serve040CatalogProofV1` from those
revalidated bytes, updates that already-locked predecessor to
`SUPERSEDED/CLOSED` revision two, and only then inserts/exact-adopts its one-set
head-040 `SCHEMA_HEAD_ADVANCE` successor as `ACTIVE/CLOSED`. It consumes the
handoff and revalidates the complete successor set before returning, without
committing, rolling back, or checking out another connection. Only the outer
Alembic transaction may commit. Immediately after `context.run_migrations()`
and still inside `context.begin_transaction()`, `env.py` asserts that the exact
registration ran once and its handoff was consumed; this is a no-op when no
registration exists. A missing, duplicate, wrong-step, or unconsumed callback/
handoff, callback exception, or injected failure before or after the version-
row update rolls back the DDL, 040 version row, and every policy write together.
The policy relation/table lock
and every predecessor row were acquired before class 9, so those writes acquire
no backward lock. Any
partial, unexpected, wrong-stamp, or already-default-free 039 shape fails
before mutation; lock timeout rolls back the DDL and version update. A startup
that observes an acknowledgement-lost 040 commit exact-adopts only after the
same post-catalog and complete per-service successor audit passes; 040 with a
missing/extra/partial successor set is corruption. Downgrade is
refused and never re-adds the default. The exact same M5a compatibility binary
therefore remains usable after 040 without schema mutation, but M4 and pre-M4
images are no longer eligible rollback artifacts.
Serve040 changes no service/replica/action/route value, ownership field,
history, or runtime route; only the prelocked policy lineage advances. Its
contention tests race the class-9 lock against each surviving M5a
parent insert/update path in both directions, and its postcondition fixture
proves that every surviving writer still supplies an explicit route.

The Serve membership identity and generic request claim owner are intentionally
different. `authority_worker_instance_id` is the stable Pod UID and
registration-lease key. `worker_instance_id` in the existing API request/queue
claim is a fresh random API-instance UUID per Python/container start. The
authority chart does not downward-inject that UUID; the supervisor mints it
before child spawn and children inherit only that boot's value. The live lease
V2 execution owner binds both IDs plus exact Pod/container incarnation bytes;
its process UUID must differ from its stable authority-worker/Pod UUID, and
normal renewal preserves the owner/hash/normalized-process-scalar triple only
after locking the exact current owner API row and proving the caller is that
process; bootstrap and prior owners cannot renew.
Claim-start, every progress/pre-I/O CAS,
terminal receipts, and recovery require both IDs. A late process from the same
Pod therefore cannot write under a newer owner.

Authority-worker API-instance registration is insert-only with exact adoption,
not the generic upsert. The instance UUID, role, Pod name/UID/IP, version,
supported-handler set, supported-payload map, and bootstrap health shape are
caller-owned immutable bytes for that boot. PostgreSQL owns `started_at`; an
insert retry compares all caller-owned bytes, adopts the stored database time,
and uses only that value as the execution owner's `api_instance_started_at`.
Any unequal collision fails. Health detail carries immutable `boot_nonce` plus
stable ID and a closed `pool_generation`: bootstrap uses zero, bind/supersede
sets one, ready-to-rewarming increments exactly one, and every other legal edge
preserves it. Heartbeat may advance only `heartbeat_at` while repeating the
current closed authority-health bytes; only typed bind/supersede, completed
initial warm, pool failure/recovery, and permanent withdrawal transitions may
change ready, draining, phase, generation, or owner hash. The immutable boot
nonce distinguishes a retry of this container start from a forced UUID collision.
Its exact phases are bootstrap (unready, null drain/owner hash), bound (unready,
null drain, nonnull owner hash), ready, rewarming (unready, null drain, nonnull
owner hash), and draining. Legal edges are insert-to-bootstrap, bootstrap-to-
bound, bound-to-ready, ready-to-rewarming-to-ready, and owner-bound-to-draining;
draining has no recovery edge. Freshness is the fixed 20-second API heartbeat
window. Every validation of a lease's current API owner requires the exact phase
matrix below and, for bound/ready/rewarming/draining,
`health_detail.execution_owner_sha256 == lease.execution_owner_sha256` in
addition to scalar/JSON/API process and stable-Pod/start equality.
Once an execution-owner triple names the row, a missing row is a
retention violation and the process fails stop: heartbeat never recreates it.
This rule, the historical nonreuse indexes, and API-instance GC locking make a
process UUID a permanent identity rather than a reusable liveness key.

The current-owner phase matrix is exact. Initial activation reads bound rows.
Normal same-owner RENEW accepts bound, ready, rewarming, or draining. Claim,
claim renewal, claim-start, progress/pre-I/O, provider effect, and handler return
require ready. Handoff OPEN uses a ready survivor plus bootstrap candidate;
survivor acknowledgement requires ready; completion uses that same ready
survivor plus bound candidate. Cold recovery accepts historical owner-bound old
rows and changes candidates bootstrap to bound. Candidate warm is bound to ready;
same-process pool recovery alone is ready to rewarming to ready. Same-owner and
typed UID/process terminal closure may use the exact historical bound, ready,
rewarming, or draining owner, and revocation/removal accepts those owner-bound
phases. GC considers any unready phase only through its independent stale /
rootless program. Bootstrap cannot renew, claim, acknowledge, terminalize a
generation-one request as its owner, or revoke an owned lease; a typed
supersession/cold-recovery writer may lock a bootstrap candidate while closing
the exact prior owners' requests before binding that candidate.

For a locked active private request `r`,
`private_request_terminal_lower_bound(r)` is exactly
`GREATEST(r.created_at, r.updated_at,
COALESCE(r.heartbeat_at, '-infinity'::timestamptz),
COALESCE(r.cancel_requested_at, '-infinity'::timestamptz))`. A multi-request
operation takes the greatest of those values, with no request term for an empty
inventory. Discovery snapshots every named column and the locked suffix requires
byte equality before using it. Every private finish/cancel-ack/receipt timestamp
is at least this lower bound, including after a backward database-clock step.

An initial post-039 bind is legal only with zero private rows and advances the
stable lease once with `BIND_EXECUTION_OWNER`. Bind is an owner-changing
renewal: it may consume an expired but still `ACTIVE` source, uses one
`GREATEST(clock_timestamp(), source.renewed_at, owner.container_started_at,
owner.observed_at, api_instance.started_at,
new_renewal_registration.worker.observed_at)` operation time, writes a fresh
self-read V2 renewal registration at that time, sets expiry exactly 60 seconds
later, installs the owner/hash/scalar triple, and changes the API health phase
from bootstrap to bound in the same commit. Immediately before commit, the
owner observation must remain within the fixed five-minute identity bound, the
API row inside its 20-second heartbeat window, and PostgreSQL time before the
new lease expiry; otherwise it rolls back and rereads. A post-039 generation-one
lease INSERT uses the same bootstrap-row, operation-time, phase-transition, and
final freshness contract. Only that
fresh committed lease may precede warming or readiness. A same-Pod restart stays
bootstrap-only and proves a new named Kubernetes container ID with a strictly
larger restart count for the same Pod; the fixed `tini -- python` command makes
same-container Python replacement unsupported and it exits to force a
container restart. The container proof and current owner are the same self-read:
container ID/restart count/start, Pod/resourceVersion, observation, and stable
ID are byte-equal; container start is no later than observation, and the current
API stored start equals the owner's API start. The proposed operation time
includes container start, observation, and API start. Immediately before commit,
fresh PostgreSQL time must be at or after the observation, no more than the
fixed 300 seconds after it, inside the current API's 20-second heartbeat window,
and before the new stable-lease expiry; otherwise the transaction rolls back
and rereads. Process discovery and its locked requery select every active
request owned by the prior authority process, with no handler/route/correlation,
generation, status, or queue filter. They both use
`LIMIT 17`, reject rather than truncate the 17th row, preserve the 24,576-byte
request-list ceiling, and reject a complete supersession above 65,536 canonical
bytes before any write. Every selected row must then closed-validate as the
legal generation-one `PENDING | RUNNING` claimed request/queue shape for exactly
one of the four private routes; any ordinary-looking/unmarked row, generation
two, `WAITING`, missing/crossed queue, or partial private marker is blocking
corruption, never an invisible filter miss. One retained process-supersession transaction locks cohort
and resolves the exact nonterminal membership protocol, inserts complete evidence, locks the
stable lease, old/new API rows, all old-owner requests, then all queues, and
uses the common class-17/event batch to terminalize at most 16 claims
`CANCELLED`. The prior API row may be stale or unready but must equal the prior
owner's process/stable-Pod/start identity. The current row must be a fresh,
unready, nondraining, bootstrap-only `authority-worker` with the exact same Pod
name/UID/IP, the new process UUID and `started_at`, the fixed private handler and
payload inventory, and zero claims. Both rows and the container proof cross-
equal the old/new owner objects. With `SUPERSEDE_EXECUTION_OWNER` it advances
the lease generation/revision, refreshes registration/renewal/expiry, and
changes the owner/hash/scalar triple; cohort membership and set revision do not
change. Supersession, like bind, is an owner-changing renewal and may consume an
expired but still `ACTIVE` source. Its one operation time is the greatest of
PostgreSQL time, source `renewed_at`, proof/current-owner container start and
observation, current API `started_at`, new renewal-registration worker
`observed_at`, and every affected request terminal lower
bound; it refreshes the self-read renewal registration, `renewed_at`, and
60-second expiry while installing the owner triple, finishing the requests,
acknowledging any prior cancellation intents, and writing every receipt and the
process row at that same time. Locked revalidation repeats every source/API/
request byte and lower bound before commit and changes the new API row from
bootstrap to bound. Unique
prior/current API IDs, exact operation-ID adoption, receipt hashes, and the
container proof forbid a branch or boot reuse. The locked writer rejects a
candidate process UUID already present in either supersession process-ID
column, except exact adoption of this same operation; a once-retired boot can
never become current again. A `SUPERSEDE_EXECUTION_OWNER` lease operation ID
must resolve directly by `(cohort_id, operation_id)` to the one process row,
whose committed counters, stable worker, current owner/hash/scalar, and time
equal the lease. Later renewals need no history walk: the lease's exact current
owner scalar has at most one indexed `current_api_instance_id` process tip; if
present it validates that tip, and if absent it is the insert/bind owner. The
unique prior/current indexes and frozen writer induction reject branches, gaps,
and reuse in O(1), so no unbounded supersession-chain scan is legal. Only after commit may the new
process warm children, claim, or publish readiness. A live owner that loses its
claim lease instead stops claiming, signals/joins its children, and closes each
original token through `OWNER_QUIESCED_LEASE_LOSS`; inability to commit causes
container exit, never third-party expiry terminalization.

Unknown INSERT/BIND/SUPERSEDE outcomes include the API phase in adoption. A
still-bootstrap row plus unchanged source proves no owner commit and permits a
full retry. A committed operation requires the same API UUID and immutable boot
bytes, exact bound phase/stable ID/execution-owner hash, unready/nondraining
shape, a legal fresh heartbeat advance only, and the exact lease operation (plus
process row and every request/receipt/event for supersession). It may also adopt
the documented same-owner legal lease descendant without rewriting the API row.
Unequal phase/hash/identity or partial joined evidence blocks; a retry never
blindly changes bound back to bootstrap or repeats terminal mutation.

The supersession batch uses only `PROCESS_SUPERSESSION_FENCE`. For an action
claim with a committed lineage it writes
`REQUEST_CANCELLED/LINEAGE/CANCELLED`; for an action claim that was assigned
generation one but lost before claim-start it writes
`TERMINAL_BEFORE_CLAIM_START/NO_SUCCESSFUL_CLAIM_START/CANCELLED`. It never
creates lineage. A shadow claim with `AUTHORIZED` history writes exact
generation-one `REQUEST_CANCELLED/SHADOW_EXECUTION/CANCELLED` history with the
stored lineage hash; one still `BOUND` writes
`TERMINAL_BEFORE_CLAIM_START/NO_SUCCESSFUL_CLAIM_START/CANCELLED` with a null
hash. Every receipt captures
the prior process API owner plus the stable authority-worker ID and uses the
supersession row's one `completed_at` value.
If the locked request already has cancellation intent, that timestamp is
preserved and `cancel_acknowledged_at` is set to the same `completed_at` only
after the container proof establishes quiescence. Without prior intent, both
cancellation fields remain null. The handoff/cold-recovery batches apply the
same rule using their one `fenced_at`; a terminal private request can never
retain an unacknowledged cancellation intent.
API-instance retention is correspondingly widened. The physical lease has a
nullable indexed `execution_owner_api_instance_id` scalar that must equal the
closed execution-owner JSON's process ID; it is preserved with the owner on
renew/revoke and changed only by bind/supersession. No API row may be deleted
while that scalar on any ACTIVE or REVOKED retained lease, any active API
request, action lineage or selector, either worker-ID column of a retained
`AUTHORIZED | SETTLED` shadow execution history, shadow terminal history, or
either scalar process-supersession ID names it. `BOUND` shadow history has null
worker IDs and therefore adds no root. Handoff/cold JSON entries are covered by their
mandatory normalized selector/shadow receipts and are revalidated against them,
never reverse-scanned. Generic API-instance GC excludes role
`authority-worker`; a Serve-owned typed job is the sole deleter for that role,
so the terminal store keeps exactly its three methods and the generic layer
does not import or reflect Serve039. The job is serialized by the class-13
singleton cursor and processes at most 128 target rows per pass, with at most
one epoch-start and one epoch-completion cursor-only transaction. At epoch
start it captures the first scalar returned by
`SELECT instance_id FROM api_server_instances WHERE role =
'authority-worker' ORDER BY instance_id DESC LIMIT 1` as the immutable upper
bound, increments `sweep_epoch`, leaves `after_instance_id` null, and commits.
PostgreSQL 14 has no `MAX(uuid)` aggregate, so that spelling is forbidden. An
empty role leaves the upper bound null and ends the pass.
Within the epoch every candidate query is ordered by instance UUID, is strictly
after the cursor and no later than the upper bound, and requires `ready=false`
and `heartbeat_at <= clock_timestamp() - interval '5 minutes'`. Indexed
`NOT EXISTS` checks for every declared root are a nonauthoritative discovery
prefilter so permanently rooted rows do not consume a page. No remaining
candidate atomically clears both markers and completes the epoch; the next pass
captures a new high-water mark. Rows inserted, newly stale, newly rootless, or
temporarily locked after the cursor are therefore deferred for at most one
finite epoch rather than starved.

One candidate uses one short transaction: lock the cursor, select and lock the
next target at class 14 (skipping a concurrently locked target is legal), repeat
the exact role/unready/five-minute predicate, and point-query every declared
indexed root. A changed, malformed, newly rooted, or otherwise blocked target
is retained; a rootless exact target is deleted. In either ordinary outcome the
cursor advances to that inspected UUID, and cursor advance plus optional delete
commit atomically with one caller-minted operation ID, one revision increment,
and one PostgreSQL `updated_at`. A validation failure is a typed blocked result
and advances; a database statement/commit failure fails closed. An unknown
commit exact-adopts the matching operation ID, while a later valid cursor
successor or missing target permits safe continuation without repeating an
authoritative delete decision. Repeated successful passes finish every bounded
high-water epoch and wrap, even when the first 128 or more discovered rows race
to rooted/blocked state. The cursor has no FK, is not itself a retention root,
and may safely point past a deleted row.

Every bind/supersession and other root-creating writer locks the same API row
and revalidates its existence after acquisition; deletion therefore either
follows five minutes of stale rootless state or makes the concurrent writer
roll back. Read-only root probes after the target lock do not acquire an
earlier-class row lock. Lease expiry or `ready=false` alone is never deletion
authority, and a later heartbeat never recreates a deleted row.
The request point-query uses the existing process-first active-claim index. An
authority-worker-owned active request that does not closed-validate as one of
the four private shapes is both a retention root and blocking corruption; it is
never filtered away as ordinary or partial state.

The process/membership compatibility matrix is closed. With no nonterminal
handoff, the stable lease must be an ACTIVE current `REGISTERING | ACCEPTING |
DRAINING` member. During `OPEN | READY`, supersession is legal only for the
ACTIVE survivor or the exact ACTIVE candidate lease; the immutable handoff
anchors remain stable-Pod evidence, while acknowledgement/completion must use
the lease's new exact-current owner. It is forbidden for the revoked stale
member, an unrelated Pod, or any terminal candidate. Cohort locking serializes
full cold recovery: if supersession wins, recovery's old snapshot/owner drifts
and it must reread the new exact owner or reject against its UID-absence proof;
if cold recovery wins and revokes the old lease, supersession rejects terminal
state. A candidate with no lease before the cold-recovery commit cannot use
supersession; its restarted bootstrap process supplies a fresh API row to a
newly constructed recovery candidate. No branch merges owners or reuses stale
evidence.

Serve039 also adds `AuthoritySchemaHeadsV2`, fixed to API requests 008 and
global-user-state 028 and closed over Serve `039 | 040`. M4 and the first M5a
deployment phase may mint only 039; 040 becomes legal only through the later
server-gated head-advance protocol below. Existing `AuthoritySchemaHeadsV1`
continues to parse the frozen historical 007/035/028 predecessor but cannot mint a V2
candidate, activation proof, dispatch proof, or promotion. The V2 candidate
binding and all three server-built authority proofs require the V2 head. This
is a forward-only schema/ownership transition: application rollback freezes
new admission and retains lineage, references, action ownership, every terminal
history/process supersession, and lease-owner history. It never reclassifies
an authoritative service as legacy or erases
durable state. Future authority-history deletion requires typed proof that the action,
all attempts, Serve projections, and references are terminal; hash-only scans
are insufficient, and no such GC/TTL is part of M4.

The live head and trust contracts are closed additive types; none mutates the
already-shipped V1 codecs:

```text
AuthoritySchemaHeadsV2 = {
  api_requests_head: "008",
  serve_head: "039" | "040",
  global_user_state_head: "028"
}

ApprovedAuthorityDeploymentSetV1 = {
  version: 1,
  role_images: [
    {role: "api" | "ordinary-executor" | "controller",
     oci_manifest_digest: "sha256:" + 64LowerHex,
     source_commit: 40LowerHex,
     artifact_inventory_sha256: Sha256}
  ],  # exactly API, ordinary-executor, controller in role order
  approved_cohorts: [ApprovedAuthorityCohortArtifactV1]
      # 1..16, strictly ascending cohort_id
}

ApprovedAuthorityDeploymentSetBindingV1 = {
  deployment_set: ApprovedAuthorityDeploymentSetV1,
  deployment_set_sha256: Sha256
}

ApprovedAuthorityDeploymentSelectionV1 = {
  api_deployment_set_sha256: Sha256,
  ordinary_executor_deployment_set_sha256: Sha256,
  controller_deployment_set_sha256: Sha256,
  authority_cohort_deployment_set_sha256: Sha256
}

ResourceActionDeploymentCompatibilityInventoryV1 = {
  version: 1,
  selections: [ApprovedAuthorityDeploymentSelectionV1]
      # exact one for a one-set policy; exact 16-value Cartesian product for a
      # two-set policy, lexicographically ordered by the four hashes
}

ResourceActionQualificationPolicyV2 = {
  version: 2,
  api_requests_head: "008",
  serve_head: "039" | "040",
  global_user_state_head: "028",
  candidate_minimum_seconds: 86400,
  minimum_clean_launches: 100,
  minimum_clean_downs: 100,
  approved_deployment_sets: [ApprovedAuthorityDeploymentSetBindingV1],
      # 1..2, strictly ascending distinct deployment_set_sha256
  elected_deployment_set_sha256: Sha256,
  rollback_deployment_set_sha256: Sha256,
  deployment_compatibility_inventory:
      ResourceActionDeploymentCompatibilityInventoryV1,
  deployment_compatibility_inventory_sha256: Sha256,
  crash_canary_inventory_contract:
      "resource_action_crash_canary_inventory_v1",
  required_crash_canary_inventory_sha256: Sha256
}

ResourceActionCandidateBindingV2 = {
  version: 2,
  qualification_policy_sha256: Sha256,
  schema_heads: AuthoritySchemaHeadsV2,
  deployment_inventory: ResourceActionDeploymentInventoryV1,
  deployment_inventory_sha256: Sha256,
  deployment_selection: ApprovedAuthorityDeploymentSelectionV1,
  deployment_selection_sha256: Sha256,
  selected_cohort: ApprovedAuthorityCohortArtifactV1,
  selected_cohort_sha256: Sha256,
  capacity_profile: ServeActionCapacityProfileV1,
  capacity_profile_sha256: Sha256,
  elected_version_identity: ServeServiceVersionSpecIdentityV1,
  elected_version_identity_sha256: Sha256,
  live_replica_identity_inventory: HashedCanonicalObjectV1,
  required_crash_canary_inventory:
      ResourceActionRequiredCrashCanaryInventoryV1,
  required_crash_canary_inventory_sha256: Sha256
}

PrivateShadowActivationProofV2 = {
  version: 2,
  service_fence: AuthorityServiceFenceV1,
  candidate_since_before: null,
  selected_cohort_id: Text,
  approved_cohort: ApprovedAuthorityCohortArtifactV1,
  registration_set: ProviderAuthorityWorkerRegistrationSetV2,
  registration_set_sha256: Sha256,
  deployment_selection: ApprovedAuthorityDeploymentSelectionV1,
  deployment_selection_sha256: Sha256,
  capacity_profile: HashedCanonicalObjectV1,
  elected_version_identity: HashedCanonicalObjectV1,
  schema_heads: AuthoritySchemaHeadsV2,
  verified_at: UtcTimestamp
}

AuthoritativeDispatchBindingV2 = {
  policy_epoch: UUID,
  policy_sha256: Sha256,
  authority_binding_sha256: Sha256,
  policy_admission_state: "OPEN" | "DRAINING",
  policy_admission_revision: PositiveInteger,
  action_id: UUID,
  attempt: PositiveInteger,
  immutable_input_sha256: Sha256,
  progress_revision: NonnegativeInteger,
  progress_sha256: null | Sha256,
  service_version_identity_sha256: Sha256,
  capacity_profile_sha256: Sha256,
  execution_authority: ProviderExecutionAuthorityProofV2
}

ShadowCandidateDispatchBindingV1 = {
  decision_id: UUID,
  request_sequence: PositiveInteger,
  logical_attempt: PositiveInteger,
  request_role: "PRIMARY_LAUNCH" | "PRIMARY_DOWN",
  action_kind: "launch" | "down",
  immutable_spec_sha256: Sha256,
  invocation_sha256: Sha256,
  request_input_sha256: Sha256,
  shadow_execution_history_revision: PositiveInteger,
  provider_progress_revision: NonnegativeInteger,
  provider_progress_sha256: null | Sha256,
  execution_authority: ProviderShadowExecutionAuthorityProofV2
}

PrivateDispatchReadinessProofV2 = {
  version: 2,
  dispatch_kind: "shadow_candidate" | "authoritative_action",
  service_fence: AuthorityServiceFenceV1,
  service_mode: "shadow" | "authoritative",
  candidate_since: UtcTimestamp,
  decision_id: UUID,
  reference_revision: PositiveInteger,
  cohort_id: Text,
  dispatch_membership: ProviderShadowCandidateDispatchMembershipV2 |
                       ProviderResourceActionDispatchMembershipV2,
  proof_inventory: HashedCanonicalObjectV1,
  schema_heads: AuthoritySchemaHeadsV2,
  binding: ShadowCandidateDispatchBindingV1 |
           AuthoritativeDispatchBindingV2,
  verified_at: UtcTimestamp
}

AuthoritativePromotionProofV2 = {
  version: 2,
  service_fence: AuthorityServiceFenceV1,
  candidate_epoch: UUID,
  candidate_since: UtcTimestamp,
  verified_at: UtcTimestamp,
  candidate_duration_seconds: PositiveInteger,  # >= 86400
  qualification_policy_sha256: Sha256,
  qualification_binding_sha256: Sha256,
  coverage_inventory_sha256: Sha256,
  clean_launches: PositiveInteger,  # >= 100
  clean_downs: PositiveInteger,     # >= 100
  blocker_count: 0,
  crash_canary_inventory: HashedCanonicalObjectV1,
  referenced_cohort_inventory: HashedCanonicalObjectV1,
  deployment_inventory: HashedCanonicalObjectV1,
  elected_version_identity: HashedCanonicalObjectV1,
  live_replica_identity_inventory: HashedCanonicalObjectV1,
  schema_heads: AuthoritySchemaHeadsV2
}

ServeAuthorityPolicyRotationProofCommonV2 = {
  version: 2,
  service_fence: AuthorityServiceFenceV1,
  predecessor_policy_epoch: UUID,
  predecessor_policy_sha256: Sha256,
  actual_schema_heads: AuthoritySchemaHeadsV2,
  successor_policy: ResourceActionQualificationPolicyV2,
  successor_policy_sha256: Sha256,
  successor_authority_binding_sha256: Sha256,
  service_version_inventory: HashedCanonicalObjectV1,
  cohort_inventory: HashedCanonicalObjectV1,
  nonterminal_inventory: AuthorityNonterminalInventoryV1,  # exactly empty
  started_at: UtcTimestamp,
  completed_at: UtcTimestamp
}

ServeM5aQualificationWindowV1 = {
  version: 1,
  serve_head: "039" | "040",
  exact_m5a_deployment_set_sha256: Sha256,
  started_at: UtcTimestamp,
  completed_at: UtcTimestamp,
  duration_seconds: PositiveInteger,  # >= 86,400
  clean_launches: PositiveInteger,    # >= 100
  clean_downs: PositiveInteger,       # >= 100
  crash_ha_inventory_sha256: Sha256,
  zero_eligible_legacy_routing: true,
  zero_unresolved_crash_intents: true,
  zero_stale_claims: true,
  zero_duplicate_effects: true,
  zero_divergences: true,
  zero_blockers: true
}

ServeM5aRollbackClosureEvidenceV1 = {
  version: 1,
  exact_m5a_deployment_set_sha256: Sha256,
  exact_m4_rollback_deployment_set_sha256: Sha256,
  mixed_selection_inventory_sha256: Sha256,
  all_m5a_to_m4_to_m5a_completed_at: UtcTimestamp,
  owner_recovery_inventory_sha256: Sha256,
  zero_eligible_legacy_routing: true,
  serve039_qualification_window: ServeM5aQualificationWindowV1,
      # exact head 039 and the same exact M5a deployment set
  m4_rollback_closed_at: UtcTimestamp
}

Serve040MigrationHandoffV1 = {
  version: 1,
  callback_protocol: "serve040_post_head_apply_v1",
  migration_operation_id: UUID,
  source_schema_heads:
      {api_requests_head: "008", serve_head: "039",
       global_user_state_head: "028"},
  destination_schema_heads:
      {api_requests_head: "008", serve_head: "040",
       global_user_state_head: "028"},
  database_name: Text,
  schema_name: Text,
  pre_migration_catalog_sha256: Sha256,
  post_migration_catalog_sha256: Sha256,
  closure_predecessors: [
    {service_fence: AuthorityServiceFenceV1,
     policy_epoch: UUID,
     policy_sha256: Sha256,
     authority_binding_sha256: Sha256,
     rollback_closure_evidence_sha256: Sha256}
  ],  # exact set, strictly ordered by the canonical service-fence key
  prepared_at: UtcTimestamp
}

Serve040CatalogProofV1 = {
  version: 1,
  serve_head: "040",
  pre_migration_catalog_sha256: Sha256,
  post_migration_catalog_sha256: Sha256,
  execution_route_default_absent: true,
  rollback_closure_policy_epoch: UUID,
  rollback_closure_policy_sha256: Sha256,
  verified_at: UtcTimestamp
}

ServeAuthorityPolicyRotationProofV2 = one of:
  {common: ServeAuthorityPolicyRotationProofCommonV2,
   reason: "COMPATIBLE_IMAGE_ROTATION",
   staged_artifact_inventory: HashedCanonicalObjectV1,
   rollback_artifact_inventory: HashedCanonicalObjectV1,
   mixed_deployment_compatibility_inventory:
       ResourceActionDeploymentCompatibilityInventoryV1,
   mixed_deployment_compatibility_inventory_sha256: Sha256,
   rollback_closure_evidence: null,
   serve040_catalog_proof: null}
  {common: ServeAuthorityPolicyRotationProofCommonV2,
   reason: "ROLLBACK_EVIDENCE_CLOSURE",
   staged_artifact_inventory: HashedCanonicalObjectV1,
       # the exact already-deployed M5a one-set only
   rollback_artifact_inventory: null,
   mixed_deployment_compatibility_inventory:
       ResourceActionDeploymentCompatibilityInventoryV1,
       # exactly the one all-M5a selection
   mixed_deployment_compatibility_inventory_sha256: Sha256,
   rollback_closure_evidence: ServeM5aRollbackClosureEvidenceV1,
   serve040_catalog_proof: null}
  {common: ServeAuthorityPolicyRotationProofCommonV2,
   reason: "SCHEMA_HEAD_ADVANCE",
   staged_artifact_inventory: HashedCanonicalObjectV1,
       # the same exact 039/040-aware M5a one-set
   rollback_artifact_inventory: null,
   mixed_deployment_compatibility_inventory:
       ResourceActionDeploymentCompatibilityInventoryV1,
       # exactly the one all-M5a selection
   mixed_deployment_compatibility_inventory_sha256: Sha256,
   rollback_closure_evidence: ServeM5aRollbackClosureEvidenceV1,
   serve040_catalog_proof: Serve040CatalogProofV1}
```

Each deployment set preserves the V1 role/cohort validators and bounds, and the
policy preserves the V1 crash validators. Every qualification-window exact-zero
field is independently derived from and cross-validated against its retained
typed stage inventory; no combined field can hide an unresolved crash intent,
stale claim, duplicate effect, divergence, or blocker. Every nested hash is
recomputed. A
compatible-image proof requires one byte-equal actual/predecessor/successor
head—039 before the advance or 040 afterward—and the exact two-set matrix. A
rollback-closure proof remains specifically at 039, reduces to
the exact M5a one-set, cross-validates its matrix/recovery/closure times, and
can create only an `ACTIVE/CLOSED` policy. A schema-head proof requires actual
040, an immediately preceding closed 039 closure policy, the same exact M5a
one-set, the byte-equal closure evidence, and the post-DDL catalog proof; it can
create only an `ACTIVE/CLOSED` 040 policy. No arm can substitute its inventories,
head transition, nullability, or reason for another. A
one-set initial policy requires both elected and rollback hashes to name that
one set and its compatibility inventory is the one all-elected selection. A
two-set rotation policy requires distinct elected and rollback hashes naming
its exact two sets and contains the complete 16-selection Cartesian product.
No third set, omitted combination, duplicate/unsorted value, leaf outside its
named set, tag-only or PR-head-only artifact, or caller-chosen compatibility
claim is legal. `deployment_selection` resolves each actual role image and the
selected cohort through one exact approved set, is itself present in the policy
inventory, and makes repeated artifacts across sets unambiguous. Runtime may
operate in any attested compatible selection, but qualification/promotion and a
stable soak count only the all-elected selection. The dispatch union is exact:
the shadow branch has `service_mode="shadow"`,
`ShadowCandidateDispatchBindingV1`, and
`ProviderShadowCandidateDispatchMembershipV2`; the authority branch has
`service_mode="authoritative"`, `AuthoritativeDispatchBindingV2`, and
`ProviderResourceActionDispatchMembershipV2`. The two membership/proof shapes
cross-reject and neither is a nullable specialization of the other. The
authority branch's execution proof is byte-equal to
the `execution_authority` child stored in the candidate-or-adopted Serve039
lineage; the shadow branch's membership and execution proof are byte-equal to
the candidate-or-adopted class-10 shadow execution history. In either branch,
`verified_at == authorized_at` for a first authorization. Claim-start constructs
and consumes the proof before lineage/history authorization and before handler
invocation; it is not deferred to the first progress watermark. V1 policy,
candidate, activation, dispatch, promotion, and rotation values remain readable
only as dark/pre-039 history and can never authorize a V2 action. Upgrading to
039 re-qualifies and re-mints the candidate/binding/proofs; the later 040
advance re-attests and re-mints under its fresh policy without rewriting 039
history. No V1 hash or window is grandfathered.

The 65,536-byte column check is not a late runtime escape hatch or a fixture
count. `65,536` is only the maximum canonical UTF-8 byte length of one value.
Before authoritative admission creates or binds an action request, the
companion-owned
`provider_authority_v2/representability_case_inventory.json` index, its two
content-addressed explicit shards, and the pure enumerator
`sky.serve.resource_action_representability.enumerate_provider_resource_action_representability_v2()`
render the exact frozen V2 action/preflight/cohort graph and each byte-exact
live registered worker identity and claim/attempt-attestation preimage through
every launch and down phase, complete/not-representable preflight envelope,
terminal handler-return/no-effect-resolution variant, reducer-built
quiescence/outcome variant, native renderer input and each rendered body,
cleanup target, resolved cohort, worker identity/attestation, and enclosing
capsule/config/invocation/plan/spec.
The exact case set includes
handler-domain `S` and every legal phase/category `R`/`U`/`B` row with maximal
bounded error code, message, and retry delay; supersession `Q` with every E-only
prefix and every legal `E* + N<i>` prefix; request-terminal
fallback `P0`, `O`, `S`, and `X` for each compatible
`SUCCEEDED`/`FAILED`/`CANCELLED` reason; and, at the separate owner-fenced
transition boundary, the only seven legal direct no-effect basis/prefix rows:
count-zero `unmaterialized`, one-link/maximum-count
`terminal_request_unsettled`, and retained-settled request-present/request-GC.
Private-shadow outcomes are deliberately not API006 action-history cases. They
use the same inventory and artifact hash through their distinct represented
parent/child, one-to-one Serve039 execution history, progress, return, fallback,
and outcome cases specified below. The authoritative enumerator maximizes only
response leaves still unknown at admission and the reachable five-effect origin
schedule; the shadow enumerator applies the corresponding closed shadow-origin
substitution. Every API006 action progress envelope, action return/outcome,
shadow progress envelope, shadow return/outcome, terminal receipt, and fallback
object must independently fit within 65,536 canonical UTF-8 bytes. No aggregate
SQL row is serialized to evade or widen those per-child bounds. Missing finite
leaf bounds or any oversize case rejects the applicable route before the private
request is created at admission, or before its first intent on live drift; no
truncation, dropped provenance, or hash-only replacement is legal.

The sixth role of the top V2 artifact inventory continues to point at the
existing path
`provider_authority_v2/representability_case_inventory.json`, but that file is
now a small closed index rather than a monolithic `cases` array. It contains
exactly two ordered descriptors for
`provider_authority_v2/representability_case_inventory/000.json` and
`provider_authority_v2/representability_case_inventory/001.json`. A descriptor
contains only its literal package-relative path, zero-based shard ordinal,
first and last global case sequence, case count, canonical byte count, and
SHA-256. For the current provisional 366-row implementation set the descriptors
are exactly `0..182` and `183..365`, 183 rows each. That count and split are
implementation status, not qualification evidence: only three of the seven
boundary families are implemented, so authority remains disabled and a final
generated-byte audit may require the design and both descriptors to be revised
before either hash is frozen.

Each shard is one independently canonical object plus exactly one LF and is at
most 65,536 bytes including that LF. Its nonempty `cases` array contains only
fully expanded
`{sequence, case_id, dispatch_kind, action_kind, boundary, payload_kind}` rows.
`dispatch_kind` is exactly `authoritative_action | shadow_candidate` and
`boundary` is exactly `complete_preflight`, `linked_admission`,
`claimed_execution`, `pre_io`, `terminalization`, `settlement`, or
`owner_fenced_transition`; the last two shadow boundaries and the owner-fenced
action boundary have the closed applicability defined by the companion. Payload
kinds include the canonical request input and each separately bounded Serve039
`dispatch_membership` and `execution_authority` JSON child, action terminal
selector, completed authority-fence operation, and the independently bounded
shadow progress, return, terminal-history, terminal commitment, fallback,
outcome, retry-decision, observation, effect-trace, partial-down-basis, and
`shadow_projection` leaves. Every completed parent contributes separate actual
and proposed `shadow_projection` rows. Ranges, regexes, implicit Cartesian
products, and "all enum values" placeholders are invalid.

The descriptor-safe loader opens the fixed package root once, rejects absolute
paths, `..`, symlinks, non-regular files, duplicate descriptors, and any path
other than those two literals, reads each descriptor target through that root
without a name-based reopen, and verifies byte count, hash, one-LF canonical
bytes, and unchanged file identity from the same descriptor. Concatenating
shards in index order must yield global sequences `0..len(cases)-1`, unique
case IDs, exact descriptor ranges/counts, and canonical equality with the
production enumerator's complete ordered code tuple. The top artifact inventory
content-addresses the index; the index content-addresses both shards. All three
files are bounded packaged artifacts, and neither shard can point elsewhere.

The separate small CI golden manifest remains
`provider_authority_v2/representability_goldens.json`. It content-addresses the
already-final artifact inventory and case index and has exactly two ordered
fixture entries, `realistic` then `candidate_maximal`. Each entry points to its
existing input file and to a distinct bounded packaged result file,
`provider_authority_v2/representability/realistic.results.json` or
`provider_authority_v2/representability/candidate_maximal.results.json`.
Each result file binds its fixture name/mode and final case-index hash and has
exactly one `{case_sequence, canonical_byte_count, sha256}` result for every
globally concatenated case in order. Every index, shard, fixture, result, and
manifest file is at most 65,536 bytes including its required LF and is loaded
descriptor-safely. The realistic fixture is evaluated only in `current` mode
and the candidate-maximal fixture only in `candidate_maximal` mode; live
boundaries still evaluate both modes. Neither the cohort static manifest, V2
artifact/callable inventory, case index/shards, capsule, nor preflight request
references the golden manifest, fixture, or results. The mandatory DAG is
artifact inventory -> case index -> shards; the later CI golden manifest ->
final artifact inventory/case index plus the two fixture/result pairs. No
cohort-bound artifact points back to CI evidence.

The live root is the closed
`ProviderResourceActionRepresentabilityInputV2` union, discriminated by
`dispatch_kind`, action kind, and seven boundary literals:
`complete_preflight`, `linked_admission`, `claimed_execution`, `pre_io`, shadow-
only `terminalization`, shadow-only `settlement`, and action-only
`owner_fenced_transition`. The provider companion owns every exact field and
cross-validator. In summary:

- authoritative roots retain the complete locked action/spec, exact two aligned
  memberships and API snapshots plus PostgreSQL time at admission, deterministic
  attempt/request/input, bounded reducer history, one actual claim/member at
  claim-start and pre-I/O, immutable Serve039 lineage, and the sole direct no-
  effect builder input/output at the owner-fenced boundary;
- shadow linked admission carries the represented parent, projected primary
  `private_api_request` child, deterministic request/input, candidate `BOUND`
  one-to-one history, the exact two aligned memberships/API snapshots, and one
  database time as source; insertion projection is disjoint. Initial insertion,
  retry insertion, and every legal stored descendant are disjoint and the sole
  projector must reproduce every real child/history/request/queue/correlation
  byte. Permanent rejection uses the separately projected durable legacy-route
  fallback after proving no private descendant;
- shadow claimed execution carries independent candidate-service/reference /
  retained-preflight rows, parent/child, `BOUND` or stored `AUTHORIZED` source
  history, full registration set/handoff fence, actual claim/member/API snapshot,
  exact prior-request historical origins, current progress/attestation, and one
  database time. Membership/authority proof, lineage hash, and authorized
  history are output only. Its builder alone constructs and validates the
  `BOUND -> AUTHORIZED` CAS before handler invocation;
- shadow pre-I/O repeats those independent current authority/preflight/origin
  sources and loads proof/lineage only from authorized history. A sealed
  code-owned scenario tuple builds legal next progress or the now-known strict
  handler return; it cannot invent a later receipt, fallback, reducer outcome,
  or successor;
- shadow terminalization carries the full preterminal request/queue and
  historical-owner source, raw committed history/origins, trusted winner, and
  database time; terminal request, typed permanent commitment/receipt, and any
  completed mixed fence operation are output only. Receipt-only adoption remains
  valid after request/evidence GC. Shadow settlement separately carries the
  immutable terminal receipt, retained request when present, locked parent/
  child/raw history/origins, and an independent source-only optional successor
  construction root. Its outputs are
  strict-return or fallback literal `S/R/U/B/Q/P0/O/X`, projections, retry
  decision, raw-preserving `SETTLED` history, insertion projection, and the
  separate permanent settlement commitment. Graph-stored and receipt-only
  adoption are disjoint, so request/evidence GC cannot erase acknowledgement
  recovery.
  Each validator
  reruns its sole builder byte-for-byte; no fixture/caller supplies future
  output authority.

The launch construction member is exactly the native V2 renderer input plus
its resulting launch capsule. The down member is exactly the native down input,
completed, API006-partial, or shadow-partial cleanup-rederivation input,
rederived cleanup target, and resulting down capsule. Each CI fixture set
contains twenty primary roots: four authoritative boundaries and six shadow
boundaries per action kind. It also contains four closed ordered banks: every
completed/API006-partial/shadow-partial cleanup root; every authoritative
history applicability class; every shadow progress/terminal/settlement and
successor applicability class; and the seven owner-fenced direct-transition
roots. Every bank member is a complete typed production input. Parent/child/
history row projections are scenario inputs, never an enclosing payload case;
only their actual independently stored JSON children are bounded and goldened.
There is no cleanup-target override, history rewrite, proof-kind/scenario
selector, payload mapping, or hand-authored reducer output. The complete
evaluator and the actual admission, claim-start, pre-I/O/return,
terminalization, settlement, and owner-fenced transactions construct their live
roots. No transport value, fixture, artifact, prior boundary root, or caller
mapping can fill a live field.

The cleanup-rederivation input is a transient aggregate of separately bounded
typed children, not an enumerated payload/output subject to the stored/wire
ceiling and not a single database, transport, capsule, or provider-request
value. Its candidate-maximal partial encoding may therefore exceed the generic
65,536-byte stored/wire-object ceiling without widening that ceiling. Its exact
closed children, three-plan cardinality, rederived target, capsule, preflight
envelope, and 60,000-byte qualification budget keep their existing bounds;
this exemption applies only to the temporary rederivation join.

`ProviderResourceActionReducerHistoryProjectionV2` contains exactly the action
ID/kind/revision/current-attempt, typed action last result/hash, null or exact immediate
predecessor and current-attempt snapshots, launch no-I/O prefix, and
supersession quiescence. Each attempt snapshot contains its attempt/request
identity and exact `request_input_sha256`, null or exact terminal request
state/time/return/hash plus request execution generation, cleared null process
worker, and kind-derived private handler name; its separate terminal selector
retains both the pre-update stable authority-worker and process claim-owner
IDs. It also contains provider-I/O
and mutation boundaries, progress/revision/hash, operation ID, immutable typed
outcome/hash, settlement time, and the ordered unique bounded Serve039 lineage
rows named by that progress/outcome/terminal slice. Admission for attempt one
requires current-attempt zero
and null predecessor/current/result/prefix/quiescence history. Admission for a
retry requires the exact settled `next_attempt-1` predecessor and a null
current snapshot. Pre-I/O requires an exact current snapshot, a null
predecessor exactly at attempt one, and otherwise the exact settled immediate
predecessor whose values materialized the current attempt. Adjacent hashes and
all prefix/quiescence copies recompute and compare byte-for-byte. This is the
bounded dependency slice already read by the lock program, not an unbounded
scan: unrelated older outcomes and execution generations are not loaded; every
referenced lineage JSON child was gated at claim-start and committed immutable.

The lineage lists are closed and finite. One attempt can name at most 13 unique
authority keys: five provider effects times the intent-origin and committed-
evidence-origin keys, one progress-envelope attestation key, one terminal-
selector key, and one typed-outcome key. The complete current-plus-
predecessor reduction context permits at most 28: two 13-key attempt bounds,
one action-level last-result key, and one raw-invalid terminal-selector
selector. These are the exact constants
`RESOURCE_ACTION_ATTEMPT_AUTHORITY_KEYS_MAX_V2=13` and
`RESOURCE_ACTION_REDUCTION_AUTHORITY_KEYS_MAX_V2=28`; increasing the five-
effect schedule requires an explicit versioned change to both.
`extract_provider_resource_action_authority_keys_v2()` walks only those named
typed slots. Every `LINEAGE` terminal selector contributes its named key even
when the request row is absent or progress cannot parse; a
`NO_SUCCESSFUL_CLAIM_START` selector contributes no key. The extractor
deduplicates by `(action_id, attempt, request_execution_generation)` and sorts
by action UUID bytes, attempt, then generation. Each attempt snapshot's
`historical_authority` must be exactly its extractor result, and the outer V2
authority context must be exactly the sorted union for the complete bounded
history. Missing, extra, duplicate, unsorted, over-bound, crossed-request, or
hash-unequal lineage rejects. An empty union is legal for the exact pristine
nonterminal shape with null selector and no terminal/effect/attestation
evidence. Once terminal, empty is legal only when the immutable selector proves
`NO_SUCCESSFUL_CLAIM_START`, whether generation zero or generation one
assigned before terminalization/claim-gate rejection; every other progress,
outcome, or terminal key requires its exact row.

Each case ID maps to one fixed-signature projector and one code-owned
applicability predicate in the enumerator's sealed dispatch table. They receive
only the exact union member selected by `dispatch_kind`, `action_kind`, and
`boundary`, plus
the enumerator-owned mode `current` or `candidate_maximal`; there is no
artifact-supplied argument, payload preimage, selector, callable, or expression.
Repository AST inventory requires exact equality among dispatch keys,
applicability keys, projected ordered case rows, and the globally concatenated
rows from both cohort-bound shards. Thus `len(cases)` is the sole finite global
case cardinality and the two result files jointly contain exactly twice that
many results; the small golden manifest contains only their references and no
design invents a second numeric case count.

Applicability is derived only from the typed live root. Common rows apply at
their named boundary. Handler and fallback rows apply only when the exact
kind-specific history and production journal classifier can reach them; a fresh first
attempt, a later-attempt predecessor, inherited-effect adoption into that new
attempt's sole generation-one request, maximum-attempt
exhaustion, and raw-invalid fallback therefore remain distinct applicability
classes. Direct rows apply only at `owner_fenced_transition` and exactly one of
the seven legal builder-input shapes. A down complete-preflight root applies exactly one
cleanup-target row: `completed_launch`, or the sole action- or shadow-partial
case returned by the corresponding closed
`classify_provider_kubernetes_partial_cleanup_rederivation_input_v2()` or
shadow-input classifier. The selected
classifier invokes the sole rederiver, counts committed objects from the
rederived target's nonnull object UIDs, derives Pod allocation from its retained
server allocations, and requires exactly one match in the literal legal-shape
manifest. Mutually exclusive retained launch histories are not fabricated from
one live down root and a nonapplicable row is neither a failure nor evidence for
that action. Across the twenty primary roots plus all four closed CI banks,
however, every global case must have at least one applicable production input.
Each history/direct classifier derives its literal scenario from
its typed bytes and must match exactly one code-owned scenario manifest row.
Fallback `X` remains a linked-admission case: from the full exact admission
root, its projector first consumes the same exact post-materialization
projection and then exact-simulates only the reachable terminal-request/raw-
journal mutation for every sealed code-owned bounded invalid profile. The
resulting raw reducer history has the projected incremented `QUEUED` action,
its exact predecessor, and a sole raw current attempt whose attempt equals the
action's current attempt. For each accepted worker, the hypothetical enclosing
raw fallback reduction input also constructs the exact candidate Serve039
lineage through the production builder. A real reduction instead carries the
immutable lineage selected by the locked request generation; its stored V2
spec and resolved action cohort cross-bind to that historical dispatch /
execution proof, not to current membership.
The history-only classifier runs first, then the real fallback reducer runs
through that same explicit V2 authority-context wrapper; neither resolves
ambient state. Raw invalidity is classified before relying on domain progress /
attestation: some sealed profiles fail parsing, while others parse and then
fail exactly one hash, revision, action-context, operation-ID, or watermark /
progress invariant. The wrapper still validates the compact cohort reference
and, when lineage exists, binds the terminal selector generation/worker to its
immutable historical membership, but does not demand an impossible progress-
attestation projection after classification as `X`. Every valid journal class
retains the normal attestation-to-historical-lineage comparison. The profiles
cover every production invalid-classifier branch and
are an
AST-inventoried production-test tuple, not fixture/live roots or artifact-
supplied selectors; the projector cannot name or hand-author `X`.
For each
case and mode the aggregator evaluates every applicable root, rejects if any
result is oversized, and emits the deterministic maximum result by canonical
byte length and then SHA-256. A missing case fails qualification; distinct
completed/partial roots are not required to render byte-equal results.

For an authoritative action, the mandatory complete-preflight, admission, claim-start,
immediate pre-I/O, and owner-fenced direct-transition calls
evaluate every row reachable from that exact action and reject an empty or
unknown boundary slice. In particular, linked admission evaluates both its own
rows and every future `pre_io` response/progress/outcome row before inserting
the attempt/request. It first calls the same pure production
`project_provider_resource_action_post_materialization_v2()` used by the V2
materializer. Its closed result contains the exact deterministic post-insert
action snapshot, nonterminal attempt snapshot, canonical request input/hash,
and derived reducer history. From the exact locked pre-insert admission root,
that
projector exact-simulates the sole successful insert transition: action
revision plus one, `QUEUED`, `action_current_attempt=next_attempt`, cleared
next-attempt time, and a nonterminal current attempt with the deterministic
request ID/input hash, null operation/outcome/settlement, both boundaries at
`NOT_STARTED`, and either the production-derived inherited retry seed at
revision one or a fresh null cursor at revision zero. It preserves the exact
settled immediate predecessor, rejects any different history, and supplies the
actual deterministic insert values. The request input must hash to the declared
hash and bind the exact action/attempt/request/private handler and pristine
queue state. The materializer byte-compares all deterministic committed action,
attempt, request, and queue columns to the projection before returning success;
database-owned admitted/updated timestamps retain normal transaction semantics
and are not projection fields. The pre-write projection is sizing evidence
only, never durable claim or execution authority.

Admission derives one closed hypothetical claim/pre-I/O root for each of the two
accepted workers from that single projected post-materialization history, the
exact locked action spec, deterministic request identity and input hash,
resolved cohort, the literal generation-one request profile, and code-owned
renewal-successor attestation and response profiles. Each
case evaluates both workers and retains the deterministic largest canonical
result (byte length, then hash) while rejecting if either is oversized.
The sole generation-one claim of every authoritative request then runs claim-start before
handler invocation and before lineage/progress/return/result persistence,
including for a worker introduced by handoff or cold recovery before that
request is claimed. A rejected claim-start emits only
the fixed bounded no-I/O terminal cause. The immediate pre-I/O call reruns the
same `pre_io`
rows using the actual claimed member, execution generation, cursor, operation
ID, attestation, and immutable lineage. Thus no handler-produced request can
terminalize before its actual generation's complete future value domain was
size-gated. `current` preserves known live bytes;
`candidate_maximal` changes only finite response leaves still unknown there.
Drift, an unbounded leaf, or either oversize rendering blocks before provider
I/O. The candidate-maximal fixture does not rewrite arbitrary `Text` inside an
already-frozen live attestation; code-owned renewal-successor profiles maximize
only the explicitly bounded mutable identity leaves. If either fixture exceeds the limit,
authority stays disabled until the design deduplicates provenance or tightens
an explicit leaf bound; passing is not assumed.

Private-shadow linked admission performs the analogous two-member early pass
before inserting its child/history/request graph. Claim-start reruns it for the
actual selected member and may authorize only through the atomic history CAS;
pre-I/O/return sizes only the actual next progress or strict return. The later
generic terminalizer independently builds and sizes the database-time receipt,
and the reducer independently builds and sizes actual/proposed outcomes,
fallback, retry/projections, complete child, post-settlement parent,
`SETTLED` history, and any fully represented retry/observation/Q successor.
Terminalization cannot be sized from a still-live request timestamp, and
fallback/settlement cannot be fabricated at pre-I/O. A new successor reruns the
complete linked-admission root under its sorted locks before either source or
target writes. Exact receipt/settlement adoption accepts only legal terminal
descendants and remains valid after request GC. Each independently stored JSON
child is bounded; no parent/child/history SQL-row aggregate becomes a payload.

The enumerator may emit only values accepted and constructible by the live
production contracts. The progress module therefore owns the closed
`ServeReplicaActionDirectNoEffectCancellationV1`, direct-cancellation outcome,
and one outer authoritative handler/direct/fallback outcome parser. The
specified P3 shadow codec uses its distinct parser and one-to-one execution-
history persistence surface; implementation remains gated. The retained
pre-038 provider-result-only shadow codec remains readable history and is not
silently reinterpreted. Direct cancellation is built only by the reducer-owned
proof builder, and direct-teardown precedence at the attempt maximum is a
transition test over those same outcome bytes rather than another payload
case. V2 progress/reduction uses an explicit resolved-cohort/historical-lineage
authority context while frozen V1 parsing remains byte-stable; no V2 action may
fall through the V1 spec parser or resolve its compact cohort reference from
ambient state. Representability projectors share those production builders and
reducers and must round-trip the production parser.

The seven direct outcomes are measured only from full typed
`owner_fenced_transition` roots and are checked before the atomic outcome /
capacity / release writes. They are never linked-admission or pre-I/O worker
cases. The no-effect domain has five call-not-entered, three
CoreV1 422, two indivisible cluster-row, and four Skylet semantic rows. The
unreviewed 375-row intermediate is not a valid inventory: after removing its
five impossible direct Cartesian rows, three split/crossed no-effect rows, and
one duplicate direct-precedence payload, 366 is only a provisional pre-audit
count. That intermediate also fabricates mutually exclusive reducer histories
from one root, maps direct teardown onto worker admission/pre-I/O, and
hand-authors fallback `X` without the production raw-invalid journal
classifier. The final count remains solely `len(cases)` after generated-byte
distinctness and production-renderability review.

The current rebased PR head has exact full-spec and preflight-envelope goldens
but does not yet have the final two-shard V2 case inventory, acyclic CI golden
manifest/result files, two content-addressed fixture inputs, or complete
production enumerator. The preserved continuation checkpoint's 366 cases are
provisional and only three of seven boundaries are implemented. Those existing
goldens and partial rows are source inputs, not a claim that the complete
representability gate has passed. The native V2
config-access reference and V2 static-manifest inventory necessarily change
capsule/preflight bytes, so final post-cutover full-spec and envelope byte/hash
goldens must be regenerated and must still pass the unchanged 60,000/65,536
budgets; the existing hashes cannot be relabeled as that evidence.

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
lookup. The size gate requires full realistic and candidate-maximal specs to
measure at most 60,000 bytes. The corrected implementation now freezes the
completed-launch case and all 20 legal partial-launch cases with exact
realistic/candidate-maximal byte/hash goldens and enforces that limit in tests.

The live V2 path centralizes this derivation in
`sky.serve.resource_action_cleanup_v2`. Its sole public constructor,
`rederive_provider_kubernetes_cleanup_target_v2()`, never accepts a supplied
cleanup target and never reads a database, clock, Kubernetes client, or ambient
config. It accepts only the companion's closed
`ProviderKubernetesCompletedCleanupRederivationInputV2` or
`ProviderKubernetesPartialCleanupRederivationInputV2`. The completed-source input is the typed retained basis, immutable
source object plans, complete resolved target and handle, exact same-UUID
cluster-row observation, and the preparation-frozen `observed_at`. The partial-
source input substitutes the exact API006 progress/revision and reducer-owned
quiescence for the completed resolved target while retaining the same immutable
plans, cluster-row observation, and frozen timestamp. Transaction adapters own
the locks and typed reads. The optimistic manager preparation, complete
preflight evaluation, locked down admission, and immediate pre-I/O
reauthorization all call this root; the last three require its result to be
byte-equal to the sole seed/capsule copy and recompute every retained hash.
`validate_provider_kubernetes_cleanup_target_binding_v2()` is the one shared
pure basis/target leaf used by the seed decoder, capsule constructor, response
validator, and rederiver; no V2 wire/action module keeps a duplicate V2
binding implementation. The frozen V1 graph retains its V1-local validator for
Serve034 history/cleanup and is never called as live V2 construction
authority. Static fixtures may parse a target, but no other production
path constructs one.

P2a's complete Helm-derived static cohort manifest subsequently increased the
representative launch spec to an exact 60,851 bytes. That measurement is pinned
as an explicit failed activation gate, not accepted by increasing the budget;
it does not block a dark deployment in which no represented action can be
admitted.

Before P2b linked represented admission, the capsule replaces its 5,241-byte
complete cohort with a closed compact durable reference containing only
`version`, `cohort_id`, and `cohort_identity_sha256`. The complete canonical
cohort remains permanently retained in
`serve_resource_action_worker_cohorts`; the admission transaction locks that
row, recomputes its identity hash, and requires exact equality to the compact
reference before creating or dispatching a request. No unlocked or external
hash lookup is permitted. The measured 231-byte reference projects the same
fixture to approximately 55,841 bytes, restoring about 9,695 bytes below the
absolute parser ceiling. Exact post-refactor realistic and candidate-maximal
goldens, rather than this estimate, must pass the unchanged 60,000-byte gate.
This closes only the immutable down-spec graph measurement; it does not supply
the still-missing runtime integration and live qualification of the
implemented renderer/normalizers or the complete live progress, outcome,
worker-attestation, and preflight-envelope representability pass, so authority
remains disabled.

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

The private route's embedded `immutable_payload_sha256` is deliberately not
this enclosing `request_input_sha256`; equating them would require a SHA-256
fixed point because `payload_json` contains the route. It instead binds an
already-committed, branch-specific provider payload: an authoritative action
route equals the locked action's `immutable_spec_sha256`, while a shadow route
equals the locked represented child's `invocation_sha256`. Those existing
columns make the claim predicate an indexed byte equality and require no
PostgreSQL reimplementation of Python canonical JSON. Only after inserting
that noncircular value does action materialization construct and hash the final
complete `ResourceActionRequestInputV1` above for the attempt. Claim SQL,
executor entry, central terminal classification, recovery fencing, and GC
validate the branch-specific route hash; action selector/lineage paths also
reconstruct and validate the independent enclosing request-input hash. A
shadow terminal history carries `immutable_payload_sha256`, not an invented
action-request-input hash. No zero placeholder, iterative hash, caller-supplied
alternative, or route-self-hash is legal. End-to-end action and shadow tests
prove the actual durable joins and a mutation of either preimage fail closed.

The same canonical JSON rules used for action identity apply. The deadline is
null or UTC RFC 3339 with exactly six fractional digits and `Z`.
Materialization rejects a caller request unless the ID matches, it is a
pristine `PENDING` request with `should_enqueue=true`, and all runtime,
terminal, and claim fields have their initial null/zero/false values.
V1 private action requests must use the normal execution class and literal
`schedule_type='long'`, `ignore_return_value=false`,
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

`current_attempt` starts at zero. Attempt numbers are contiguous and never
reused. PostgreSQL owns all timestamps and revision increments.

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
2. Serve service parent and all required authority-policy rows in policy-epoch
   order, immutable version rows in ascending version order, and replica-
   incarnation rows in canonical key order; authoritative down that consumes a
   completed shadow launch also locks the same-UUID global-user-state cluster
   record and its complete provider handle in canonical cluster-UUID order in
   this class;
3. worker-cohort registry rows in cohort-ID order;
4. nonterminal worker-registration handoff rows in handoff-ID order, then
   process-supersession evidence rows in supersession-ID order;
5. worker-registration lease rows in `(cohort_id, worker_instance_id)` order;
6. worker-cohort reference rows in decision-ID order;
7. shadow coverage rows in canonical decision-ID order;
8. coverage-only submission rows in `(decision_id, request_sequence)` order;
9. shadow parent rows in canonical action-ID order;
10. shadow child rows in `(action_id, request_sequence)` order, followed by
    same-key shadow execution-history rows;
11. action rows in canonical action-ID order;
12. attempt rows in `(action_id, attempt)` order;
13. Serve action-domain event rows in `(action_id, event_code)` order, then the
    exact `authority-worker-v2` API-GC cursor singleton;
14. API server-instance rows in instance-ID order;
15. API request rows in request-ID order;
16. API request queue rows in request-ID order; and
17. append-only Serve039 execution-authority lineage rows in
    `(action_id, attempt, request_execution_generation)` order, then action
    terminal-authority selectors in `(action_id, attempt)` order, then shadow
    request terminal-history rows in `(decision_id, request_sequence)` order,
    then shadow admission-fallback-history rows in `decision_id` order, then
    shadow admission-fallback-progress-history rows in `decision_id` order,
    then shadow settlement-history rows in `(decision_id, request_sequence)` order;
    and
18. global API operational-event sequence row.

Transactions may take a suffix or a subset and finish, but may never acquire
an earlier class afterward. Ordinary/pre-039 legacy-controller shadow evidence
admission and completion stop within classes 1-10. The selected-private
fallback's first `PRE_SUBMIT` or terminal proved-no-call release is the sole
legacy exception: after fixing its complete class-1-through-10 transition it
continues on the same borrowed connection to the fallback-progress phase of
class 17, inserts/exact-adopts the permanent receipt, and then commits both or
neither. Unknown commit adopts only the byte-equal child/release transition and
receipt. Private shadow linked admission, claim,
and progress continue through API/request/queue classes 14-16; its terminalizer
uses 14-18, and successor settlement may use the full 1-16 union. Authoritative
admission may continue from class 10 to class 11. Frozen V1
materialization uses 11-12 and 15-16. V2 linked materialization instead locks
the owner/service/policy/version/replica, cohort, nonterminal handoff, both
accepted leases, and reference prefix in classes 1-6 before action/attempt and
request/queue classes 11-12 and 15-16; it constructs the representability root
only from those locked rows and never reaches backward. Existing generic
ordinary request claim/terminalization uses only
15-16 and 18 and never writes an action, attempt, or lineage row. After
nonlocking classification discovery, generation-one V2 private terminalization
first locks its claimed process API-instance row at class 14, then request and
queue, and appends its class-17 receipt through the borrowed consolidated
connection; generation-zero private terminalization has no owner and starts at
class 15. It still never writes an action/attempt or opens another transaction.
The V2
authority-worker claim branch is narrower: after nonlocking discovery it locks
owner, service/policy/version/replica, its exact cohort and any nonterminal
handoff, its registration lease plus exact execution owner and reference,
action/predecessor/current
attempts, server instance, request, queue, and finally its exact lineage key in
this order. This serializes membership with `OPEN` handoff
insertion; a predicate
evaluated from an earlier SQL snapshot cannot claim after the fence. Other
execution classes do not take those additional locks. Authoritative down that
adopts shadow launch evidence locks its same-UUID global cluster record/full
provider handle in class 2, then coverage and parent in classes 7 and 9, before
admitting/locking its down action; it never reaches back from shadow evidence
to the cluster table. Any down admission that consumes an existing source
action nonlockingly discovers all pre-existing natural-key conflicts before
taking class 11, rejects a natural key already bound to a different action
UUID, and then walks the canonical UUID-sorted union of the source IDs and
deterministic new down ID. At each key it locks and validates an existing
source row or inserts/exact-adopts the allowed new row at that exact position.
It acquires every action row before any attempt row and never locks a higher
action UUID before inserting a lower one. Nonlocking discovery is not conflict
authority: if a natural-key row wins the unique-index race after discovery, the
transaction inspects it at the deterministic new action's sorted position and
adopts only the same deterministic UUID with byte-exact content. A different
UUID or any content drift aborts before attempt locks or links. Cohort
retirement may take only the
cohort/handoff/registration-lease/reference
suffix. Every
preparation-reference creation instead takes owner -> service -> active policy
when authoritative -> cohort -> nonterminal handoff -> both accepted
registration leases -> reference and requires
policy admission state `OPEN` and no `OPEN | READY` handoff. Shadow or
authoritative admission takes owner -> service/policy/version/replica -> cohort
-> handoff -> registration leases -> reference -> coverage/parent -> action.
Retirement takes cohort, nonterminal handoffs, registration leases, then
references and performs
only nonlocking defensive
reads of later-class state; it never reaches backward to service/replica rows.
The existing cross-process resources-file lock is acquired before the short
SQL admission transaction and released before a worker is authorized; it is
never held during preparation, condition waits, or provider I/O. Provider-I/O
code holds none of these locks. Cold-recovery rows are immutable evidence
inserted only while the exact cohort row is locked; they are never independently
row-locked and add no late lock class after request/queue rows. Their
`(cohort_id, source_cohort_revision)` unique key plus the cohort CAS serializes
insert and exact-read adoption.
Handoff opening, process supersession, and cold recovery nonlocking-discover
their bounded later-class
claim inventory, then under the cohort lock insert the complete optimistic
class-4 handoff/process-supersession or class-3 cold-recovery evidence row and
all class-5 lease
rows before acquiring API, request,
or queue locks. They revalidate the entire discovered inventory under that
suffix and roll back the transaction on any drift. No protocol inserts or first
locks an earlier-class row after reaching a later class.

The Serve039 lineage, action-selector, shadow-terminal-history, shadow-
admission-fallback-history, shadow-admission-fallback-progress-history, and
shadow-settlement-history relations are class 17. The first three derive their complete
immutable bytes only after the class-14 instance and class-15/16 request/queue
claims are freshly locked. A settlement commitment instead derives after its
complete earlier-class graph prefix and immutable terminal receipt; a no-
successor or request-GCed settlement need not reacquire an unrelated class-14/
15/16 row. The fallback receipt derives after the fallback's complete
class-1-through-16 absence/transition program. Its progress receipt derives
only after the first legacy child or terminal no-call release is fixed by the
earlier-class transition. Within class 17 a transaction has six
strict phases: insert/exact-adopt a claim-start's own new lineage or key-share-
lock every existing named lineage sorted by `(action_id, attempt,
request_execution_generation)`; insert/exact-adopt every applicable action
selector sorted by `(action_id, attempt)`; then insert/exact-adopt every shadow
request terminal-history row sorted by `(decision_id, request_sequence)`; then
insert/exact-adopt every shadow admission-fallback-history row sorted by
`decision_id`; then insert/exact-adopt every shadow admission-fallback-progress-
history row sorted by `decision_id`; then insert/exact-adopt every shadow settlement-history row
sorted by `(decision_id, request_sequence)`.
A transaction begins at the earliest phase it touches, skips untouched later
phases, and may never go backward or alternate phases. A terminalizer never
creates a missing lineage.
The selector's immediate nullable FK only key-share-locks a
lineage already visited in the first phase. Claim-start touches only its new
lineage key; terminalization is already serialized with claim-start by the
class-15 request row. A one-request terminalizer is exactly the batch program
with a singleton input. First lineage insertion, exact lost-ack adoption,
API006 journal write, and any earlier-class updates share one transaction and
one connection; the transaction never reaches backward after class 17.
Reduction nonlockingly discovers immutable selector/history keys after its
action/attempt and optional request prefix, loads the bounded deduplicated
lineage set first, then exact-validates the selectors/histories; append-only rows have no
M4 update or deletion race. Handoff and retention take all earlier locks first
and use the indexed class-17 suffix; no authority-history transaction acquires
a cohort, lease, action, or request lock after class 17. Class-18 operational
events are allocated only after every applicable lineage, selector, terminal-
history, fallback-history, fallback-progress-history, and settlement-history
write succeeds.

The generic API006 store therefore exposes explicit connection-borrowing V2
seams. `materialize_in_transaction()`,
`load_claimed_execution_in_transaction()`,
`commit_intent_with_progress_in_transaction()`,
`write_provider_progress_in_transaction()`,
and `record_submission_in_transaction()` accept the caller-owned consolidated
PostgreSQL connection and never begin, commit, roll back, or check out another
connection. The current public methods remain V1-only convenience wrappers
that open one transaction and delegate to the same cores. V2 code is
repository-inventoried to call only the borrowing variants.

The generic request layer does not import or reflect Serve039 tables. It owns a
single typed `ResourceActionTerminalAuthorityStoreV2` dependency slot whose
registration is installed once in each process before that process accepts
request work and is frozen thereafter. An `install_or_verify_same()` call is
idempotent only for the object-identical store and database namespace so a
single-process `all` role can cross two composition roots; a different second
registration fails. Installation/probe is explicit at every real consumer:

- the controller/`all` main supervisor before `recover_db_and_logs()`, request
  GC, hard-pressure cancellation, controller initialization, or any background
  task;
- the API/`all` main supervisor before authority retirement, handoff/cold-
  recovery verification, or other background work;
- every Uvicorn worker in FastAPI `server.lifespan` before yielding any route;
- the authority dispatcher/supervisor before constructing its request claimant
  or publishing role readiness; and
- every spawn-started executor child, ordinary or authority, in
  `executor_initializer` before `_request_execution_wrapper`, because API-
  cancel and cross-request cluster cancellation may terminalize a V2 request
  from an otherwise ordinary child.

Every spawned child receives its role-appropriate connection configuration as
initargs. An authority-pool child's first actions install the immutable
authority-child
sync-engine allowlist `['api-requests-control', 'shared']` and call
`db_utils.set_max_connections(1)`, before metrics, plugins, any database engine,
or the store probe. `shared` is the normalized name for a null engine namespace.
The process-local guard in `db_utils.get_engine()` rejects a third synchronous
namespace, any async engine, a changed limit, or policy installation after an
engine cache entry exists. The child then installs/probes the terminal store.
It then installs/probes the sync-only terminal store and constructs/probes the
process-local qualified private provider executor. An ordinary executor child
retains its existing separately configured pool policy and installs/probes only
the terminal store; it never installs the authority namespace allowlist or
private executor. Spawned children cannot inherit either parent global.

The authority role uses one fixed, no-burst process pool of `N` workers. The
V2 manifest and Pod-template release inputs carry byte-equal hashed
`ProviderAuthorityWorkerRuntimeCapacityV1` objects with `1 <= N <= 16`, exact
supervisor namespaces `['api-requests-control', 'authority-preflight',
'shared']`, exact child namespaces `['api-requests-control', 'shared']`, a fixed
one-connection QueuePool limit for each namespace, derived supervisor budget 3,
derived per-child budget 2, and exact Pod ceiling `3 + 2*N`; six closed
canonical-decimal environment bindings repeat the limits and derived values.
No default, CPU-derived size, or unbound override exists. Its authority-specific start path constructs
exactly one `RequestWorker` for `ScheduleType.LONG` and that one pool; it never
calls the ordinary two-schedule `executor.start()`. All four private
constructors, durable inputs, candidate/locked predicates, and recovery readers
require literal `schedule_type='long'`; a short private row is corruption, not a
second pool. Repository inventory freezes both start paths and every private
schedule producer/consumer. Before readiness or claiming, the supervisor
submits one initializer barrier per child and proves `N` distinct child PIDs,
the exact budget/store/private-executor order, and successful probes. A
`BrokenProcessPool` or child-init failure withdraws readiness and stops new
claims. Failure during initial bound warming leaves the row bound and claimless
while the supervisor tears down and retries the whole pool. Failure after ready
first takes a short cohort -> handoff -> current lease -> API-row transaction;
it requires the exact ready current owner and changes only that row to rewarming
while incrementing `pool_generation` one. Because every claim/effect takes the
same prefix and requires ready, that commit fences all later work. The supervisor
then kills/joins every child, closes each already-committed current-owner claim
through its exact post-claim-failure or pending-intent owner-ack terminal path,
and rebuilds/eagerly warms the entire pool. A rewarming-to-ready CAS takes the
same prefix, requires the exact generation/owner/lease, an exact-zero inventory
of every active request owned by that process, and the fresh `N`-distinct-child
initializer proof; an unknown result adopts only exact ready/generation bytes.
Failure to quiesce or commit exits the container so supersession fences it;
repeated build failure stays bound or rewarming and never restores readiness.
Draining is permanent and cannot use this recovery. Disposable/unwarmed
private children are forbidden. Before any engine is constructed, the
supervisor installs its immutable exact-three-namespace guard and
`set_max_connections(1)`; the preflight namespace's dedicated factory must also
retain `pool_size=1, max_overflow=0`. Each child installs the exact-two-
namespace guard and the same per-namespace limit. Thus the supervisor can retain
at most three synchronous connections and each child at most two, rather than
mistaking `set_max_connections(2)` for a process-wide budget across two pools.
One authority Pod's frozen persistent budget is `3 + 2*N`. Startup recomputes
the capacity object/hash and all six environment values before process-pool or
engine construction, introspects every created QueuePool as size one with zero
overflow, and rejects any unexpected sync or async engine cache key. The
activation capacity proof includes both Pods and every
other deployed role at its manifest-bound concurrency; an unbudgeted namespace
or computed/burst concurrency rejects readiness. Unit and real-PostgreSQL
tests force simultaneous checkouts in every allowed supervisor/child namespace,
prove the `(3 + 2*N)` high-water ceiling for `N=1` and `N=16`, and prove a third
child namespace, second connection in one pool, async engine, or any hidden
engine cannot exceed it. Transient startup/migration advisory-lock `NullPool`
sessions remain separately bounded operational headroom and are never counted
as persistent pool capacity.

Each installation requires exact API008 plus the active policy-bound Serve039
or Serve040 probe through the central
`DatabaseManager` namespace. Uvicorn, ordinary executor, API, and controller
composition roots register the sync engine and, where their existing paths use
it, the async engine/run-sync facade as allowed connection sources for that one
PostgreSQL database. Authority supervisors and authority-pool children instead
register only their allowed synchronous engine/borrowing facade; construction
or registration of an async engine in those processes fails readiness under the
capacity guard above. Every process role that
can invoke a V2 terminal path is readiness-gated on that installation. A
Uvicorn lifespan failure publishes no route, a main-supervisor failure starts
no recovery/background work, an authority-supervisor failure publishes no role
readiness or queue consumer, and a child-initializer failure prevents that child
accepting a task. Unequal duplicate registration, post-start mutation, a connection from
another manager namespace/database, or a qualifying authoritative correlation
with no store fails closed. An
ordinary request bypasses the slot. A private-shadow request uses the same slot
to create its distinct Serve039 terminal-history receipt but never an action
selector, so
Serve038 and lower generic request operation remains additive-compatible. The
store exposes exactly three borrowed methods:
`terminalize_in_transaction(connection, terminal_context)`,
`terminalize_batch_in_transaction(connection, terminal_contexts)`, and
`validate_request_retention_in_transaction(connection, terminal_snapshot)`.
The singleton method delegates to the batch method and returns its sole typed
receipt. The batch input has 0..32 unique request IDs and is canonically sorted.
A nonempty batch with more than one element is legal only when every context has
one homogeneous trusted mode: `STALE_OWNER_FENCE` at size at most 16,
`PROCESS_SUPERSESSION_FENCE` at size at most 16, or `COLD_RECOVERY_FENCE` at
size at most 32. Every member binds the exact same enclosing operation ID and
operation time. In stale-owner and process-supersession batches every member
also binds the same exact one owner/fence proof. In cold recovery, each member
instead binds exactly one of the recovery row's two sorted source-worker/fence
proofs, at most 16 bind either proof, and the inputs partition by their frozen
request ownership; a third,
crossed, or wrong-worker proof rejects. Every other mode is singleton-only.
Mixed modes, operation IDs, times, or illegal owner/fence proofs reject before
mutation. An empty batch verifies the installed heads/connection and
returns an empty receipt map without a class-17 or event write. It
key-share-locks all existing named action lineage keys, then inserts/exact-
adopts all action selectors and shadow histories in their separate declared
orders, and returns a map with exactly one action-selector or shadow-terminal
receipt for every input request ID and no extra ID; it does not allocate a
class-18 event. The generic layer owns that
subsequent event phase. Each returned receipt is tagged
`NEWLY_TERMINALIZED` or `EXACT_ADOPTED`; an existing receipt adopts only when
the request ID, immutable route/input hash, correlation, handler, terminal
state, generation, stable/process owner pair, cause/lineage disposition, and
persisted finish time equal the trusted terminal context. The batch may not
mix an exact committed operation with newly mutable siblings: a recovery retry
either adopts the complete immutable handoff/cold/process operation plus every
terminal request/receipt/event, or retries an entirely uncommitted operation.
Partial prior commit is blocking corruption. Each method accepts only the caller-owned connection and typed context and verifies
the exact heads through that connection before mutation. Sync and
async SQLAlchemy engine objects are intentionally distinct; object identity
across operations grants no authority. Atomic consolidation means every one
operation uses one caller-owned physical PostgreSQL transaction for both API
and Serve state. The store
may not check out, commit, roll back, discover a table by ambient reflection, or
perform a lazy import. Repository inventory freezes the interface, every
installation root (including the spawn initializer), and every
consumption site.

V2 request deletion invokes
`validate_request_retention_in_transaction()` rather than widening generic SQL
with a Serve import. For an action route the validator point-loads and closed-
parses its immutable selector, requires exact request/action/attempt/input/
status/finish/generation/handler equality (with the terminal request worker
correctly cleared), and requires the attempt already settled. For a shadow
route it requires the exact immutable shadow terminal-history row, a `COMPLETE`
represented child, and same-key `SETTLED` execution history. Their request/
correlation/status, receipt/hash, settlement basis, final progress, outcome/
hash, and copied return/hash (nonnull exactly for a handler basis) must cross-
equal; fallback requires the return pair null. These are nonlocking point reads
of earlier-class immutable/terminal state, not backward row locks. Absence or
drift blocks deletion. Ordinary and historical non-V2
requests retain the existing predicate-only behavior.

GC uses that validator twice. Its keyset-paginated candidate reader orders by
request ID, validates V2 candidates on one central connection before returning
them, records explicit blocked reasons, and continues beyond a full ineligible
page. `delete_requests()` then deduplicates/sorts IDs, handles one request per
short transaction, locks that request and queue if present, repeats the same-
connection validation for TOCTOU, and returns a typed
`RequestDeletionResultV2` containing sorted `deleted_ids` and
`blocked[{request_id, reason}]`; it never silently skips. The cleanup caller
counts only `deleted_ids` and unlinks current, legacy, debug, and lock files only
after the matching database delete commits. A crash may leave an orphan file,
which a new bounded per-request orphan sweep removes; it can never leave a retained
request whose diagnostics were preemptively erased. File-unlink failure is
reported and retried as orphan cleanup without reconstructing the deleted DB
row. The sweep keyset-walks all four current/legacy/debug/lock filename
families, parses only canonical request IDs, requires the file older than the
fixed GC grace interval, point-checks that no database request row exists, and
then unlinks; request IDs are never reused. Unknown names and a raced/failed DB
read are retained. Normal retention and hard-pressure cleanup share this program. Checked-in
call/AST inventory covers candidate selection, final deletion, result handling,
and every cleanup/orphan-sweep entrypoint; batch-size-one blocked-first-page
tests prove no spin, starvation, false count, retained-row log loss, or
permanent crash orphan.

`ResourceActionTerminalAuthorityModeV2` is a trusted internal discriminant with
exactly `PRIVATE_HANDLER_RETURN`, `PRIVATE_POST_CLAIM_FAILURE`,
`CLAIM_START_NOT_REPRESENTABLE`, `CLAIM_REAUTHORIZATION_FAILED`,
`OWNER_ACK_CANCEL`, `OWNER_QUIESCED_LEASE_LOSS`, `STALE_OWNER_FENCE`,
`COLD_RECOVERY_FENCE`, `PROCESS_SUPERSESSION_FENCE`, and
`TERMINAL_BEFORE_CLAIM_START`. It is not accepted from a request body, handler,
plugin, or generic `EventCause`. Private constructors are callable only from
the strict return/failure wrapper, the two claim-start rejection branches, the
same-owner cancellation/lease-loss acknowledgement, the process-supersession
batch, the two UID-qualified recovery batch
programs, and enumerated pre-claim terminal callers respectively. Repository
inventory freezes those constructor sites. The store maps the trusted mode,
terminal state, generation/worker, and lineage presence to the closed selector
or shadow-history cause table and rejects every crossed combination; generic
callers cannot name a special claim-start or recovery cause. `EventCause` is
derived afterward solely for the class-18 audit event.

The two same-process quiescence modes are closed and singleton-only.
`OWNER_ACK_CANCEL` requires a nonnull prior cancellation intent;
`OWNER_QUIESCED_LEASE_LOSS` permits either null or nonnull prior intent. Both
require generation one and the same-fence locked historical API owner, and both
write request state `CANCELLED`. For an action with committed lineage the exact
receipt is `REQUEST_CANCELLED/LINEAGE`; before successful claim-start it is
`TERMINAL_BEFORE_CLAIM_START/NO_SUCCESSFUL_CLAIM_START`, and lineage is never
fabricated. A shadow request uses the corresponding generation-one
`REQUEST_CANCELLED` or `TERMINAL_BEFORE_CLAIM_START` history. If intent was
present, its original timestamp is preserved and `cancel_acknowledged_at` equals
the one finish time after quiescence; without intent, as permitted only for lease
loss, both cancellation fields remain null. Terminal `updated_at`, `finished_at`,
receipt finish, and any acknowledgement use the same
`GREATEST(clock_timestamp(), private_request_terminal_lower_bound(request))`
scalar and exact replay validates them.
For no-intent lease loss, action reduction uses only the retained journal class:
`not_started_empty -> P0`, valid nonterminal -> `O`, valid succeeded -> `S`, and
invalid -> `X`; a retry can materialize only action attempt `n+1`, never refresh
the same request. Prior intent does not make request status provider evidence:
the same table applies unless the separately proven action-wide direct no-effect
teardown route takes precedence. Shadow completion likewise uses its separate
progress/history contract and never infers an effect outcome from `CANCELLED`.

Terminal-history creation belongs to the central request terminalizer, not a
private handler callback. Its typed outer result is exactly
`NEWLY_TERMINALIZED | EXACT_ADOPTED | LOST_RACE`; the first two carry one exact
receipt and `LOST_RACE` carries none. Crossed or missing durable evidence raises
the distinct blocking `PRIVATE_TERMINAL_CORRUPTION` result and never masquerades
as a race.

The entry point first performs nonauthorizing request plus selector/shadow-
history discovery by their unique request-ID indexes. If the request is absent
but one permanent receipt exists, it takes the receipt-only immutable-adoption
branch and acquires no API/request/event/current-Serve locks. For a shadow route
it strict-validates the typed terminal commitment/hash and derives the caller's
commitment from the original trusted winner: equality returns `EXACT_ADOPTED`,
a different closed legal commitment returns `LOST_RACE`, and a crossed or
unparseable receipt is `PRIVATE_TERMINAL_CORRUPTION`. For an action route the
immutable selector plus settled attempt's copied return/outcome provide the
equivalent comparison. This branch appends nothing, remints no time, requires no
current shadow mode or historical API/event row, and never recreates the
deleted request. Receipt absence or ambiguity remains corruption, not proof of
an uncommitted terminal transaction.

The following full-evidence branch applies when the terminal private request is
retained. It must already have exactly one matching receipt and no queue. For a
generation-one receipt it locks that retained process API-instance row at class
14 and then the request at class 15; generation zero starts at class 15. It revalidates the
terminal request's immutable route/input hash against the caller, then first
closed-validates the persisted terminal request, historical API owner, receipt,
lineage/selector or shadow history, cancellation fields, and terminal event as
one internally consistent legal winner under the same trusted-mode/cause table.
It uses the persisted `updated_at`/`finished_at` and appends nothing. Missing,
crossed, or internally inconsistent evidence—including a missing retained API
row, surviving queue, illegal cause/lineage disposition, unequal timestamp, or
unequal terminal event—is blocking corruption. If that legal winner is byte-
equal to the caller's original trusted mode, claim/owner snapshot, and typed
return/failure, the same borrowed singleton exact-adopts it and the outer result
is `EXACT_ADOPTED`. If it is a different closed legal winner for the same
immutable request, the outer result is `LOST_RACE` with no receipt and no write;
valid cancellation, UID handoff/cold fencing, process supersession, or owner-
lease-loss closure is never mislabeled corruption. If the request is still
active but no longer equals the
caller's token/generation/owner snapshot, it returns `LOST_RACE` without write.
If it still equals that claim but has a new nonnull `cancel_requested_at` and
null acknowledgement, a handler return/failure or ordinary claim transition
also returns typed `LOST_RACE`; cancellation intent owns closure through the
quiesced owner-ack path. It is not corruption and cannot be overwritten by
success/failure. Tests serialize both intent-before-handler and handler-before-
intent lock orders, plus handler-versus-owner-ack and handler-versus-UID/process-
fence winners when the losing handler retries after either an acknowledged or
unknown commit result.

For a nonterminal generation-one request, nonauthorizing discovery resolves its
process API row whenever either a private marker is present or that row's role is
`authority-worker`. Either condition makes the mutating transaction lock that
API-instance row at class 14 before it locks and revalidates the request at class
15. A missing/invalid owner, a private marker owned by any other role, or an
authority-worker-owned row that does not closed-classify as one of the four
private routes blocks without mutation. A marker-free request owned by an
ordinary role retains the existing path and never takes this extra lock. The
generation-zero path must still recheck generation/owner under its class-15
lock. If a concurrent claim changed it to generation one after discovery, the
transaction writes nothing, releases, rediscovers the process owner, and
restarts at class 14; it never reaches backward or constructs a null-owner
generation-one receipt. The opposite ordering either terminalizes generation
zero first or makes the claimant fail its locked pristine-request predicate.
The
borrowed core of `_terminalize_locked_request` then uses one closed three-way
classifier without acquiring an earlier row lock.
`authoritative_action` requires both nonnull action/attempt correlation columns,
one of the two action handlers, and a sole alias-only payload that closed-
decodes as the byte-equal action-shaped route. `shadow_candidate` requires null
action/attempt correlation, one of the two shadow handlers, and the exact
decision/sequence/role private correlation plus byte-equal shadow-shaped route.
The remaining `ordinary` arm is legal only when no private handler, route, or
correlation marker is present. Any partial/crossed private shape, invalid route,
or action correlation on a shadow handler is blocking corruption and never
falls through. Each private arm additionally requires
`execution_class=NORMAL`, null `controller_generation`, `ReplayPolicy.NEVER`,
literal `schedule_type='long'`, `retryable=false`,
`ignore_return_value=false`, strict registered return codec,
the exact payload type/format/version, and queue priority zero. Other domains'
correlated handlers remain outside this dependency. Before first V2
activation, an exact full-table `EXISTS` audit under the locked policy/cohort
gate requires zero pre-039 rows with any of the four private handlers,
regardless of request, claim, delivery, or terminal state. It uses a fixed
60-second statement timeout; timeout or read failure blocks rather than
assuming zero. Serve039 is
installed before a private handler can then be admitted, so there is no
historical private shape to grandfather. For a qualifying row the core locks and loads its
single class-16 queue row after the already-locked class-15 request, rejects a
missing or duplicate delivery row, and reconstructs the closed canonical
action or shadow request input from only the immutable request/correlation and
queue-input columns before deleting the delivery. It never locks the earlier
action/attempt or shadow rows; the terminal receipt is later required to equal
their frozen input/correlation during reduction/completion and GC. After mutating the class-15
request and class-16 queue, but before allocating any class-18 operational
event, it calls
the registered store's `terminalize_in_transaction()` on the same connection.
The core evaluates exactly
`GREATEST(clock_timestamp(), private_request_terminal_lower_bound(request))`
once and uses that one scalar value for terminal `api_requests.updated_at`,
`api_requests.finished_at`, and its action selector or shadow-history
`request_finished_at`; exact adoption compares all three. For generation one, before
the request update it captures the process claim-owner ID from the request and
requires it to equal the locked API row's `instance_id`; the API row's canonical
Pod UID yields the stable authority-worker ID, and any retained authority
health-detail identity must equal it even if readiness/draining fields have
since changed. The route supplies no worker identity. For
generation zero no API row or worker ID exists. Every
terminal commit then clears the
complete API007-defined claim triple under API008 (`claim_token`, `worker_instance_id`,
`lease_expires_at`) plus `controller_generation` and `heartbeat_at`, satisfying
`ck_api_requests_claim`. Generation one requires both captured IDs nonnull in
the receipt; generation zero requires both null; any other private
generation is corruption. The terminal request row
therefore has a null process owner in both cases. For handler/post-claim modes,
the named lineage/typed return must repeat the captured pair. Claim-start
rejection uses the already-held claim/lease/API context; owner acknowledgement
and owner-quiesced lease loss use the same-fence locked API mapping; stale,
cold, and process-supersession modes use their already-held exact old-owner
lease/API context. Every source must equal the API-derived pair, and no caller-
invented stable ID, route-only inference, or current-member lookup is legal.
The process ID is deliberately not compared to the cleared request column.
Generic callers may not replace either captured ID or supply an independently
evaluated finish time.
For an action it `SELECT ... FOR KEY SHARE`s the one named lineage before its
selector insert. The trusted mode plus stored lineage yields only the exact
state-matched table above; a missing lineage rejects a mode that requires one,
and terminalization never creates lineage. Success additionally requires the
strict typed handler return and lineage. For shadow it writes only the closed
shadow-history row. Handler completion, strict-encoding failure, cancellation
acknowledgement, precondition/startup failure, UID-qualified recovery,
container-qualified process supersession, and every
other private terminal caller pass through this core. Generic lease expiry
does not terminalize a claimed V2 private request. A checked-in AST/call inventory
covers every terminalization expression and rejects a bypass. If a V2 correlation cannot
construct or exact-adopt its terminal receipt, the whole terminal transaction
rolls back; it may never leave a terminal V2 request without one.
Unknown-result tests cut the connection immediately before commit, immediately
after commit but before acknowledgement, and after acknowledgement for handler
success, strict-return failure, owner cancellation, claim-start rejection, and
one-request recovery fencing. They require exactly one request finish, receipt,
and terminal event, preserve the first database time, and distinguish exact
adoption from a real lost race.

`ProviderResourceActionCancellationIntentSnapshotV2` is the closed preterminal
control shape: the exact private route/correlation, active status, generation
one, worker/token/lease, null controller generation, null-or-positive PID,
claimed queue/generation one/priority zero, one nonnull database-clock
`cancel_requested_at`, and null `cancel_acknowledged_at`. It is neither a
terminal reducer snapshot nor a provider representability payload.

Explicit cancellation of a generation-one active V2 claim is deliberately not a
terminal transition. `kill_requests()` first records one database-clock
`cancel_requested_at` and a fixed bounded status message while preserving the
active status, claimed queue delivery, PID, and complete claim triple; it
writes no terminal receipt or event. A repeated kill exact-adopts the original
timestamp/message and never remints either. Private claim, `try_mark_running`,
heartbeat/renewal, claim-start, every lineage/progress/intent/pre-I/O CAS, and
ordinary handler success/failure terminalization all require both cancellation
timestamps null. Once intent wins, those paths cannot overwrite it with
`SUCCEEDED` or `FAILED`; only same-owner quiesced `OWNER_ACK_CANCEL` /
`OWNER_QUIESCED_LEASE_LOSS`, a UID-qualified stale-owner/cold-recovery fence, or
a container-qualified process-supersession fence may close it. If ordinary
handler terminalization commits first, a later
kill observes terminal state and is a no-op.

Every cancellation observer uses the intent, not only terminal status.
`_request_is_gone_or_cancelled()` treats a nonnull `cancel_requested_at` as
cancelled so retry/pause monitoring cannot reschedule it. If a child returns
normally because intent made `try_mark_running()` fail, `handle_task_result`
re-reads the exact generation/token/worker fence, recognizes the pending
intent, and executes the same no-PID future-done acknowledgement branch rather
than treating the task as successful. Uvicorn shutdown, API-cancel, cross-
request cluster cancellation, hard-pressure cancellation, heartbeat signal,
and child completion are all in the frozen cancellation-writer/observer
inventory.

The exact owning supervisor matches generation/token/worker, signals the
spawned handler when a PID exists, and waits until the future and child/process
cleanup are observably complete. If intent wins after claim but before child
submission/PID publication, the supervisor cancels or drains the queued future;
the later `try_mark_running` rejects on the intent and performs zero provider
I/O, and the same future-done branch still acknowledges the null-PID claim.
Only after that quiescence does the owner call
the new same-fence `acknowledge_and_terminalize_cancelled_claim()` core. In one
historical API-instance -> request -> queue -> class-17-receipt transaction for
a generation-one claim, it first locks the immutable owner API row at class 14
and then uses one database timestamp for
terminal `updated_at`, `cancel_acknowledged_at`, `finished_at`, and receipt finish
time; exact replay validates all four. That timestamp
is exactly `GREATEST(clock_timestamp(),
private_request_terminal_lower_bound(locked_request))`; it can never precede
the retained cancellation intent after a backward clock step. The transaction clears PID,
the complete claim triple, controller generation, lease heartbeat, and queue;
sets `CANCELLED`; and inserts/exact-adopts the receipt and terminal event. An
unclaimed generation-zero cancellation has no remote process/API-owner lock and
starts at request class 15, using that terminal core immediately with requested,
acknowledged, terminal `updated_at`,
finished, and receipt-finish timestamps all equal to
`GREATEST(clock_timestamp(), created_at, updated_at)`; exact replay validates
that equality.

No reducer, direct transition, retry materializer, or request GC accepts a
nonterminal cancellation intent. If the owning supervisor dies, its
parent-death watchdog first kills the spawned handler; only the typed
stale-owner/cold-recovery or container-qualified process-supersession fence may
then acknowledge and terminalize the expired claim. Lease expiry by itself does
not assert that a still-observed old Pod is
quiescent. The generic expired-claim reaper excludes every V2 private claimed
row, whether or not cancellation is pending. A lost signal or acknowledgement
is retried against the same claim fence; a stale owner cannot acknowledge a
different request or token. Thus there is no terminal-active-claim DTO shape:
the cancellation-intent DTO is control state, while representability and
reduction consume only pristine active-claim or post-quiescence terminal
snapshots/receipts.

The generic expired-claim reaper is also one-request-at-a-time. It performs a
nonlocking, request-ID-ordered candidate discovery, then opens one short
transaction per ID, locks request before queue, reruns expiry/replay/private
classification, and either applies the ordinary existing replay/terminal rule
or skips a V2 private claim. It allocates any event before committing that one
request and never reaches from an earlier request's class-18 event back to a
later request row. Missing/raced rows are benign per-ID outcomes; no unordered
joined `LIMIT` batch or scalar terminalizer loop is legal. The stale-owner,
cold-recovery, and process-supersession V2 batches are the only multi-request
private terminalizers and each follows the separate all-requests/all-queues/
class-17/all-events program.

PostgreSQL batch cancellation is not one transaction spanning unrelated rows.
It discovers candidate IDs without locks, sorts canonical request IDs, and
handles each in its own short transaction: request first, then its queue and
class-17 receipt only if the operation terminalizes. It revalidates the user/filter and
active state after locking. This prevents the old pattern of reaching a later
request's queue after inserting an earlier request's class-17 selector and
gives every V2 receipt its canonical per-request lock order; batch cancellation
does not promise all-or-nothing atomicity.

Private generation-one claiming uses three deliberately separate predicates;
the shared stable route/cohort/reference qualifier is never overloaded with
mutable delivery state. The fresh-candidate and locked-claim predicates require
request `status=PENDING`, immutable `should_enqueue=true`, generation zero,
null token/worker/lease/controller/heartbeat and both cancellation timestamps,
plus exactly one queue row at `queued`, null
`claim_generation`, and priority zero. While holding the worker's class-14 API-
instance row, after proving the selected stable lease execution owner names that
exact process UUID and Pod member, the claimant uses an indexed `LIMIT 17`
inventory of every active request owned by that authority process, with no
handler/marker/generation/status/queue filter. It first requires every row to be
exactly one of the four private routes and satisfy the closed current-claim
predicate; an ordinary-looking/unmarked or crossed row is corruption and blocks
claiming. The count of legal
rows must be below
`RESOURCE_ACTION_WORKER_FENCE_MAX_REQUEST_CLAIMS_V2 == 16`; it then changes the
request and queue exactly once to generation one. It never applies generic
`current_generation + 1` to an already-used private request. A queued private
row with nonzero generation or any residual claim field is corruption and is
not a candidate. The current-claim/recovery predicate instead accepts only
generation-one, claimed, same-worker/token queue state with request status
`PENDING` before `try_mark_running()` or `RUNNING` afterward, and cancellation
fields exactly `(null, null)` or `(nonnull, null)` while snapshotting the
nonnull intent. Null-request/non-null-ack and every nonnull acknowledgement on
an active row are corruption. `WAITING` and
every other status are corruption for a claimed private request, so
terminalization and UID-qualified fencing cannot make a legal live claim
invisible or revive a paused one.

The API-instance lock serializes the 16th/17th claim race with handoff/cold
recovery, which locks the same instance before its exhaustive request suffix.
A partial index covers private active claims by worker. `qsize` uses the fresh-
candidate predicate; `put`, generic requeue, retry/pause rescheduling, and
delivery recreation hard-reject a private `ReplayPolicy.NEVER` row. A checked-
in call inventory freezes every queue `put`/requeue/candidate/current-claim
consumer and tests the 15/16 boundary plus a concurrent 17th claimant. Once a
private request terminalizes its delivery is deleted permanently; only the
action reducer may create the new deterministic request for attempt `n+1`.

The current monolithic claimed-attempt helper is split into an action/attempt
prefix lock and an API-instance/request/queue suffix validator. The caller
supplies the exact expected claim snapshot from nonlocking discovery, locks all
earlier Serve classes, then the prefix, then the suffix, then the class-17
lineage key;
the suffix rereads PostgreSQL clock and revalidates token hash, generation,
worker, the NORMAL-class null controller generation, lease, handler,
correlation, null cancellation timestamps, and the exact claimed queue/
generation/priority after
every wait. No V2 path calls a storage helper that performs a hidden checkout
or locks request state before the action/attempt prefix. The existing borrowed
`reduce_in_transaction()` gains the explicit V2 terminal-selector and
historical-authority context
but retains its caller-owned transaction contract. Its existing settled-replay
fast path returns before the reducer callback, so V2 adds a separate borrowed-
connection settled-authority validator (or invokes that validator before the
fast return); replay may never bypass selector/lineage validation.

M4 preserves one shared mixed-mode provider-work budget; it does not create an
action-only launch allowance. Let `P` be the number of all legacy and action
replica rows whose derived status is `PROVISIONING`, let `D` be the number of
all legacy and action rows with `sky_down_status='RUNNING'`, and let `C` be the
existing non-pool request parallelism. The exact integer form of the existing
predicate is `2 * P + D < 2 * C` before admitting either kind. A launch then
adds two units and a down adds one, matching
`P + D / SERVE_LAUNCH_RATIO` with `SERVE_LAUNCH_RATIO == 2.0`, including its
existing edge behavior; a row satisfying both predicates contributes both
weights exactly as today. Down admission additionally requires fewer than
`_MAX_CONCURRENT_DOWNS_PER_SERVICE == 64` already-`RUNNING` downs for that
service. These constants and the launch-before-down order within a manager
refresh are compatibility behavior, not new policy.

Under the one resources-file lock, a mixed legacy/action refresh reads `P` and
`D` once from PostgreSQL, merges committed same-tick deltas by exact decision
ID, evaluates all launch candidates before all down candidates, and updates the
local integer occupancy only after exact committed readback. A winning launch
persists `sky_launch_status='RUNNING'` and derived
`replicas.status='PROVISIONING'`; a winning down persists
`sky_down_status='RUNNING'`. The same transaction also writes the appropriate
legacy shadow fence or durable action/reference/link. Only after that commit or
byte-exact lost-ack adoption may the legacy thread start, the authoritative
action request become dispatchable, or selected private admission proceed to
its one linked request transaction. A denied candidate writes no occupancy or
action artifact.

Retries, observation, and `BLOCKED` retain their original occupancy. The one
owner-fenced terminal/no-I/O reducer transaction releases it exactly once;
replay exact-adopts that release. Recovery of a committed `RUNNING` admission
does not reacquire or increment the budget before redrive. Thus action versus
legacy, launch versus down, controller crash, and ambiguous admission response
all serialize through the same durable rows and cannot undercount or release
twice.

Serve038 adds partial indexes for global `status='PROVISIONING'`, global
`sky_down_status='RUNNING'`, and per-service running downs; the replica columns,
not an index or new reservation table, remain authority. Dispatcher batch size
64 and request materialization grant no capacity. Materialization revalidates
the exact counted replica projection and matching action link before enqueue.
An AST/source inventory covers every transition into either counted state.

This intentionally changes consolidated-PostgreSQL failed-service purge and
whole-service cleanup where they currently launch parallel direct downs without
the shared durable admission boundary: each exact eligible incarnation now
uses the same routed down admission and budget. Excluded SQLite,
non-consolidated, pool, and orphan-cleanup branches retain their explicit legacy
behavior; the design does not describe the in-scope purge as unchanged.

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

ServeShadowCandidateHandlerTerminalResultV1 = {
  version: 1,
  result_kind: "serve_shadow_candidate_handler_terminal_v1",
  decision_id: UUID,
  request_sequence: PositiveInteger,
  logical_attempt: PositiveInteger,
  request_role: "PRIMARY_LAUNCH" | "PRIMARY_DOWN",
  action_kind: "launch" | "down",
  request_id: UUID,
  request_execution_generation: 1,
  handler_name: "serve_shadow_candidate_launch" |
                "serve_shadow_candidate_down",
  reduction_kind: "domain" | "supersede_to_down",
  immutable_payload_sha256: Sha256,
  request_input_sha256: Sha256,
  immutable_spec_sha256: Sha256,
  invocation_sha256: Sha256,
  final_provider_io_boundary: "NOT_STARTED" | "INTENT_COMMITTED" |
                              "SUBMITTED_OR_AMBIGUOUS",
  final_provider_progress_revision: NonnegativeInteger,
  final_provider_progress_sha256: null | Sha256,
  final_provider_effect_trace_sha256: Sha256,
  worker_attestation: ProviderAuthorityWorkerAttemptAttestationV1,
  worker_attestation_sha256: Sha256,
  provider_result: ServeReplicaActionProviderResultV1,
  normalized_provider_error: null | ProviderErrorV1,
  launch_no_effect_resolution: null | ProviderShadowLaunchNoEffectResolutionV1
}

ServeShadowCandidateRequestReturnV1 = {
  version: 1,
  return_type: "serve_shadow_candidate_handler_terminal_result_v1",
  terminal_result: ServeShadowCandidateHandlerTerminalResultV1,
  terminal_result_sha256: Sha256
}

ServeShadowCandidateRequestFallbackEvidenceV1 = {
  version: 1,
  decision_id: UUID,
  request_sequence: PositiveInteger,
  request_id: UUID,
  request_terminal_state: "SUCCEEDED" | "FAILED" | "CANCELLED",
  fallback_reason: "missing_handler_return" | "request_failed" |
                   "request_cancelled",
  terminal_history_sha256: Sha256,
  journal_class: "not_started_empty" | "valid_nonterminal" |
                 "valid_succeeded" | "invalid",
  provider_io_boundary: "NOT_STARTED" | "INTENT_COMMITTED" |
                        "SUBMITTED_OR_AMBIGUOUS",
  provider_progress_revision: NonnegativeInteger,
  provider_progress_sha256: null | Sha256,
  provider_operation_id: null | Text
}

ServeShadowCandidateReductionEvidenceV1 = one of:
  {version: 1,
   basis_kind: "handler_terminal_result",
   request_terminal_state: "SUCCEEDED",
   request_return_sha256: Sha256,
   terminal_history_sha256: Sha256,
   fallback_evidence: null}
  {version: 1,
   basis_kind: "request_terminal_fallback",
   request_terminal_state: "SUCCEEDED" | "FAILED" | "CANCELLED",
   request_return_sha256: null,
   terminal_history_sha256: Sha256,
   fallback_evidence: ServeShadowCandidateRequestFallbackEvidenceV1}

ServeShadowCandidateOutcomeV1 = {
  version: 1,
  basis: ServeShadowCandidateReductionEvidenceV1,
  provider_result: ServeReplicaActionProviderResultV1,
  supersession_quiescence:
      null | ProviderShadowLaunchSupersessionQuiescenceV1
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
  fallback_reason: "missing_handler_return" | "request_failed" |
                   "request_cancelled",
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
An authoritative action handler can return only
`ServeReplicaActionRequestReturnV1`; it cannot
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
No shadow or request-fallback outcome may use either *action* cancellation
proof/quiescence shape. The disjoint shadow outcome may use only its exact
launch `Q` cancelled/observed tuple with
`ProviderShadowLaunchSupersessionQuiescenceV1`; all shadow fallback and shadow
down outcomes reject it.

Each authoritative private launch/down handler returns one ordinary Python mapping with the
exact `ServeReplicaActionRequestReturnV1` keys. Dedicated return-value encoders
are registered for only `serve_resource_action_launch` and
`serve_resource_action_down`; they closed-validate and return that JSON object
without default encoding, pickle, compatibility filtering, or omitted nulls.
`terminal_result_sha256` is the canonical SHA-256 of the complete nested
terminal result. The stored PostgreSQL `requests.return_value` must be that
nonnull object, canonicalize to at most 65,536 UTF-8 bytes, and round-trip
byte-equivalently through the closed decoder. Unknown/missing keys, floats,
encoder fallback/drop-to-null, or any other return type are invalid.

Each shadow-candidate handler instead returns the disjoint
`ServeShadowCandidateRequestReturnV1`. Its two dedicated encoders/decoders are
registered only for `serve_shadow_candidate_launch/down`; action handlers and
shadow handlers cross-reject the other return type before persistence. The
shadow DTO has no action ID/attempt or API006 claim leaf. Its decision,
sequence, logical attempt, primary role, kind, request/input/payload/spec/
invocation hashes, final boundary/revision/progress hash, and worker attestation
must byte-equal the locked class-10 execution history and current claim.
`reduction_kind="supersede_to_down"` is launch-only and requires the exact
shadow no-effect/quiescence/atomic primary-down handoff below; down and every
ordinary shadow result use `domain`. Both shadow return objects are independently
bounded at 65,536 bytes and use strict canonical JSON with all nulls present.

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
terminal `SUCCEEDED` row with a null return value instead uses
`basis_kind="request_terminal_fallback"`. Its `fallback_reason` is
`request_failed` exactly for `FAILED`, `request_cancelled` exactly for
`CANCELLED`, and `missing_handler_return` exactly for terminal `SUCCEEDED`. It
can never establish `N<i>`, supersession
quiescence, or partial-down admission.

All four private handlers strict-decode their complete return before the
terminal transaction. A malformed, cross-handler, or hash-mismatched nonnull
return is converted to the route's fixed bounded `FAILED` error with a null
persisted return; a persisted terminal `SUCCEEDED` row containing any nonnull
value that is not its strict route return is quarantined corruption, not a
fallback case. `missing_handler_return` remains the defensive case for a legal
terminal `SUCCEEDED` row with null return. Activation's zero-private-row audit
means no older private invalid-return shape needs compatibility support.

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

Shadow JSON fields are bound to named closed types, not merely size/hash checks.
For retained legacy-controller children, `actual_outcome` and
`proposed_outcome` remain the old `ServeReplicaActionOutcomeV1` shadow-basis
shape with null quiescence/prefix; their invocation may include the legacy-only
cleanup union member. For a `private_api_request` primary child, both outcome
columns instead contain `ServeShadowCandidateOutcomeV1`, its invocation is a
primary `ProviderLifecycleInvocationV2`, and its one-to-one execution-history
row must exist. The two codecs cross-reject. `pre_observation` and
`post_observation` remain `ProviderLifecycleObservationV1`. Retained pre-039
legacy evidence is never converted, and no private row fabricates an action
outcome, action/attempt ID, API006 fallback, or action quiescence.

Private linked admission is one transaction. It locks owner -> service/
candidate/version/replica -> cohort -> handoff -> both leases -> reference ->
coverage -> parent -> child/history, then both accepted API rows at class 14 and
the deterministic request/queue at classes 15-16. It validates the represented primary graph,
and inserts or exact-adopts the child, its `BOUND` execution history, the
deterministic generation-zero PR #1070 request/queue and private correlation,
changes the initial parent `PENDING_SELECTION/PENDING ->
PRIVATE_API_REQUEST/RUNNING`, and performs the sole initial
`PREPARING -> SHADOW_ACTIVE` transition. A successor starts from `RUNNING` /
`SHADOW_ACTIVE` and performs neither transition. `planned_execution_kind` is exactly
`private_api_request`; request role is primary and kind-matched. Same-ID lost-
ack adoption requires the entire graph and original database times byte-equal.
A partial graph/collision is quarantined and never claimable. No API006 action,
attempt, selector, or queue beyond that one real private request is created.

Linked-admission failure is exhaustive. Retryable lock, membership, freshness,
or artifact drift writes nothing and retains
`PENDING_SELECTION/PENDING` plus `PREPARING` for a bounded owner-fenced retry.
A complete deterministic unbounded/oversized/unsupported result takes the
availability fallback in one exact-adoptable transaction under the same full
1-16 prefix. It proves zero child/history/private-correlation/deterministic-
request/queue descendants, then changes parent only to
`LEGACY_CONTROLLER/RUNNING` with
`private_fallback_reason=linked_admission_not_representable`, the bounded
hash-valid `ProviderShadowLinkedAdmissionFallbackCommitmentV1` evidence/hash
pair, and reference only to `SHADOW_ACTIVE`. That commitment stores the
caller-minted operation ID, decision/request IDs, exact initial-source hash,
complete deterministic production failure/hash, and original database commit
time. The same commit inserts the byte-equal permanent FK-free class-17
fallback-history receipt. It inserts no private graph and does not release the counted
slot. Only after commit may the one decision owner signal the same-cell legacy
worker; recovery after commit-before-signal uses the stored full invocation and
proved absence of a legacy `PRE_SUBMIT` row. Exact replay adopts both original
state transitions/time only when the retained caller source/failure rehash to
that exact commitment and permanent receipt. Graph adoption is legal only
while the graph remains at the exact immediate fallback post-state: the parent
is `LEGACY_CONTROLLER/RUNNING`, the reference is `SHADOW_ACTIVE`, and every
child/history/private-correlation/deterministic-request/queue descendant and
the fallback-progress receipt are absent. It returns those original activation
bytes/time/revisions. The first legacy `PRE_SUBMIT` or terminal no-call release
atomically inserts the permanent progress receipt. Once that receipt exists,
receipt-only adoption is legal only with either its exact retained first-
transition descendant or the full typed-GC absence proof. The store discovers,
locks, and selects this arm; the caller cannot choose it. Receipt-only adoption compares the
caller-retained original source/failure to the permanent commitment, returns no
parent/reference, and never reconstructs, mutates, resurrects, or signals the
graph. A different legal commitment is a lost race; malformed, partial, or
crossed evidence is corruption. The receipt-only acknowledgement cannot hide
graph corruption from the independent typed readers, promotion checks, or GC
validators. A private descendant makes fallback permanently illegal. Thus
permanent rejection cannot strand capacity or authorize both owners.

Claim-start is a second short transaction. Nonauthorizing discovery chooses the
candidate graph and process owner; the writer locks the full class 1-10 prefix,
then the current process API row, request, and queue at classes 14-16. It
revalidates the exact candidate epoch, shadow service, parent/child/history,
`SHADOW_ACTIVE` reference, current accepted membership, ready process/stable
owner, lease/token/generation, request input, and complete finite
representability root. It constructs
`ProviderShadowCandidateDispatchMembershipV2` and
`ProviderShadowExecutionAuthorityProofV2`, then CASes only the history
`BOUND -> AUTHORIZED`, recording both proof pairs, the canonical lineage hash,
both worker IDs, token hash, generation one, and one PostgreSQL time. Only after
that commit may the handler run. Exact lost-ack replay adopts the original
proof/time after validating legal historical descendants; unequal evidence or a
terminal request blocks. Terminalization racing first wins the same request
lock and leaves the history `BOUND`, so authority cannot be backfilled.

Every progress, immediate pre-I/O, and post-effect checkpoint repeats the same
class 1-10 -> 14-16 order. Before locking it extracts the bounded canonical set
of every historical shadow origin reachable from the current/next cursor or
return; the class-9/10 union includes each retained completed predecessor child
and strict settled history, and the immutable class-17 receipts/request-if-
present are revalidated nonlockingly afterward. No missing, extra, duplicate,
raw-`X`, or crossed origin is legal. The checkpoint requires the exact current ready owner and immutable
lineage hash, and CASes the history revision before the next effect. It uses
only `ProviderShadowLifecycleProgressV1`; action-shaped origins reject. The
first child starts empty, while a later deterministic retry may carry only the
exact predecessor cursor as the documented revision-one `NOT_STARTED` seed and
clears the current worker attestation. Committed effect origins remain
immutable. A later claim may exact-adopt committed readback but cannot resolve
an older intent as no-effect. Every call boundary commits before I/O, and every
returned operation ID is write-once. Owner/process/handoff/cold loss can prevent
the next checkpoint but cannot invalidate retained history or authorize another
effect under it.

The handler-return path writes no class-10 row after releasing its progress
transaction. It returns the strict shadow DTO to the central request
terminalizer, which owns classes 14-18 and writes the terminal request, copied
return value, event, and exact shadow terminal-history receipt atomically. If it
dies or loses a race, the winner is instead one of the closed failure/cancel/
pre-claim receipts and no handler return is invented. Request GC retains the
request while the history is not `SETTLED`, while the child is not `COMPLETE`,
or while either terminal-history/hash relationship or the permanent settlement-
commitment/hash relationship is missing. The immutable class-17 receipts and
settled class-10 history make later replay request-GC-safe.

Settlement first performs nonlocking terminal receipt/outcome classification
and successor-key discovery. A no-successor terminal row, including attempt
exhaustion, may use the short owner/service/replica -> cohort -> reference ->
coverage -> parent -> child -> execution-history prefix and then revalidate the
immutable receipt/request without acquiring an earlier lock. A below-maximum
`R`/`U`/`P0`/`O` successor must instead freeze the complete source+successor key
union before its first lock and visit owner/service/version/replica -> cohort ->
handoff -> both accepted registration leases -> reference -> coverage -> parent
-> children/execution histories, each class canonically sorted. It then locks
and revalidates both accepted API-instance rows at class 14, the deterministic
target request at class 15, and target queue at class 16. It reruns the terminal
outcome, successor eligibility, full registration set/handoff fence, accepted
membership/API alignment, source-only successor construction root, and combined
representability projector after every wait. The source root contains the
current pre-settlement child/history and only absent successor keys; it never
contains the completed predecessor or successor graph that this transaction is
about to derive. Drift rolls back; the writer never reaches backward from class
10 for a handoff, lease, or reference.

For non-`Q` settlement, a strict handler
basis requires `HANDLER_RETURN/SHADOW_EXECUTION/SUCCEEDED`, exact return and
terminal hashes, and byte-equal final boundary/revision/cursor/attestation. It
uses the authoritative S/R/U/B provider-result table with only the documented
shadow-origin substitution. `S` completes success; below the maximum `R`
schedules one same-plan primary retry and `U` schedules observation-first
continuation; at the maximum each retains its literal outcome/disposition but
blocks without a successor. `B` blocks and retains all evidence. A launch `Q`
is legal only for the strict
`supersede_to_down` return and exact shadow no-effect/quiescence contract. Q
first constructs the deterministic target identity/spec and complete down
preflight from a nonauthorizing snapshot. Its one retryable transaction then
walks the canonical sorted union of source and target keys at every applicable
class: owner/service/version/replica, cohorts/handoffs/leases/references,
coverage, parents, and children plus execution histories. Only after the full
class-10 union is locked does it revalidate the source, immutable terminal
receipt/request, every historical shadow origin reachable from the source
cursor/quiescence, target preflight, and cleanup basis; it then writes source Q
settlement and derives the absent target's coverage, PENDING_SELECTION parent,
final PRIVATE_API_REQUEST parent, child/`BOUND` history, deterministic request/
queue/private correlation, reference transition, and replica generation/status/
reciprocal links before one commit. The intermediate PENDING_SELECTION parent
is a projector-local construction, never input evidence or an externally
visible committed state. The source declares the exact absences and retains the
byte-equal physical capacity allocation; Q neither releases nor reallocates it.
Both accepted target API-instance rows are
locked and revalidated at class 14; request/queue are acquired only afterward
at classes 15-16. A lost acknowledgement adopts that whole byte-equal union. It
never locks one source child and then reaches backward for a target reference.
No private `LAUNCH_CLEANUP_DOWN` exists.

Every other legal terminal winner uses `REQUEST_FALLBACK`. The reducer applies
the same closed raw-journal classifier as the action path: empty `NOT_STARTED`
is `P0` and retries, valid nonterminal is `O` and remains observation-first,
valid `SUCCEEDED` is `S` and commits observed success, and invalid is `X` and
blocks. Fallback never creates a no-effect resolution, quiescence, partial-down
basis, or promotion-clean graph. Settlement copies the exact return when
applicable, terminal-history hash, basis, normalized provider result, actual/
proposed outcomes, retry decision, observations/effect trace, child
`COMPLETE`, parent projection, and history `SETTLED` in one commit. A same-plan
retry atomically inserts/exact-adopts its new contiguous child, `BOUND` history,
deterministic request and queue, and private correlation as one complete graph;
it never requeues the old `ReplayPolicy.NEVER` request. The
existing maximum logical attempt applies; exhaustion blocks without inventing
another child. Every unknown commit exact-adopts all original bytes and times;
an unequal partial settlement is corruption. The combined settlement projector
first derives the completed predecessor and settled history locally from the
locked pre-settlement source, then invokes linked admission with that local
value. Neither the new-write DTO nor its successor arm may source those outputs.
Unknown-commit recovery instead uses the disjoint stored-adoption arm containing
the caller-retained original new-write root, complete already-settled current
graph, and complete already-inserted
successor. For Q that source is the legal current target descendant—not the
initial-insert output—and includes immutable coverage, a current replica/
capacity snapshot or exact post-removal incarnation/link/cleanup-intent absence
proof, reciprocal links, plus an admission descendant that may already be
authorized, settled, released, or request-GCed. Retained immutable times and
the source cleanup basis reconstruct the insertion. It remains adoptable after
the source service promotes because stored replay uses the coverage-derived
historical candidate binding rather than current `resource_action_mode`; it
never repairs or fills a partial graph.

That same transaction inserts one permanent class-17
`ProviderShadowSettlementCommitmentV1` receipt under a caller-minted operation
ID. The commitment hashes the complete original new-write source and the
separate complete settlement/successor projection, records the immutable
current/optional-successor identities and original settle time, and contains no
mutable graph or provider-cursor bytes. Graph adoption validates it in addition
to every retained row. After typed evidence GC, a store-owned whole-component
proof classifies the parent as ordinary, outgoing Q, or incoming Q from the
permanent scalar successor indexes and proves every required source/peer
absence before permitting receipt-only adoption. A caller retaining its original
source/projection may then exact-adopt from this receipt; any unequal legal
commitment is a lost race and no receipt-only path recreates a graph. Evidence
GC is illegal until the receipt exists and cross-validates the settled graph.

History stores the literal exhaustive disposition: handler `S/R/U/B/Q` and
fallback `P0/O/S/X`. It never normalizes `P0` to `R`, `O` to `U`, or `X` to
`B`. Below the maximum, `R/P0` create a `retry_same_plan` successor and `U/O`
create an `observe_same_plan` successor; at the maximum all four retain their
exact outcome/disposition, use retry decision `block`, complete the parent, and
create no successor. `S` completes terminal success, `B/X` complete blocked,
and launch `Q` completes the source and creates exactly one normal partial-down
successor. The companion's exhaustive mapping table fixes every retry class,
delay, parent phase, and nullability; proposed comparison output never controls
this program.

The private shadow terminalizer never persists an invalid nonnull handler
return. Its strict codec validates the complete
`ServeShadowCandidateRequestReturnV1` before the terminal request transaction;
a malformed, crossed-kind, or hash-mismatched value is converted to the fixed
bounded `private_handler_failed` terminal failure and persisted with a null
return, so settlement classifies it as `request_failed`. A terminal
`SUCCEEDED` row may therefore have either one strict return or null and the
latter is the defensive `missing_handler_return` fallback. A persisted
`SUCCEEDED` row with any nonnull value that does not strict-decode is database
corruption and remains quarantined; `invalid_handler_return` is intentionally
not a shadow fallback enum. The separate generic action private handler uses
the same strict-terminalization rule; neither private route persists a malformed
nonnull return.

Only a strict handler basis with a valid return, exact effect trace/progress,
`MATCH`, no fallback, and every private history settled is promotion-clean.
Legacy/private mixing in one candidate window, missing history, cleanup on the
private path, fallback, pending/invalid evidence, or any representability drift
blocks promotion and contributes zero clean launches/downs. Private dispatch
remains disabled until this complete contract and its same-inventory shadow
representability cases are implemented and verified; that is an implementation
gate, not an unresolved behavior contract.

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

Ordinary generic request terminalization updates only the request, queue, and
existing operational event. An M4 authoritative route additionally appends its
Serve039 selector through the frozen same-connection store, but neither path
acquires action/attempt/domain locks or snapshots action evidence. Request
terminal states are immutable. A later reducer
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
remove the source row before this snapshot and an exact selector because its
candidate query, delete predicate, and registered V2 retention validator all
must pass in the deletion transaction.

The reducer transaction locks current Serve controller leadership, matching
service/replica rows, the frozen cohort and same-ID reference, action, and
attempt in that order. It takes no paid/reserved/logical-capacity lock. It
revalidates the closed capacity profile, action-bound version identity/hash, action
revision, and replica's current action link/teardown generation, then does
exactly one of:

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
source as `SUPERSEDED_TO_DOWN`, revalidates the down action already inserted or
exact-adopted during sorted-union acquisition, and links it in one commit. The
replay branch requires the already-settled attempt's retained request snapshot,
outcome, and quiescence to be byte-equal, then revalidates that already-acquired
down action and exact-adopts its link; the original request may already be GCed,
and a surviving row is compared nonlocking but is not required.
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

Upstream revision 032 is the shipped request-rejection-classification
migration. It adds no resource-action state. An earlier feature-only draft also
used revision ID 032 for the first half of the resource-action catalog; that
lineage was never shipped or stamped on `boltz-test`, but retaining both files
would give Alembic two different revisions named 032. The resource-action
lineage therefore has one additive revision 033 with `down_revision='032'`.

Revision 033 adds all of the resource-action state in one transaction:

- `services.resource_action_mode` with permanent `legacy` default and
  `resource_action_mode_changed_at` for the promotion window;
- nullable `replica_incarnation`, `desired_generation`,
  `sky_cluster_record_uuid`, current launch/down action IDs, and current
  launch/down represented-sample IDs on replicas; and
- bounded logical-sample and represented per-legacy-request-attempt shadow
  tables; and
- the two nullable replica coverage-link columns.

On PostgreSQL only, the same revision creates the worker-cohort
registry/reference (including the nonnull preparation-capability SHA-256
commitment), decision-coverage, and coverage-only submission tables, their
checks and indexes, explicitly adds
`shadow_samples.would_be_action_id -> shadow_coverage.decision_id ON DELETE
RESTRICT`, adds nullable pair-checked `legacy_effect_trace`/hash columns to the
represented-attempt table, and updates the replica checks and partial unique
indexes. It does not depend on `metadata.create_all(checkfirst=True)` to alter
an existing table.

The PostgreSQL revision performs one read-only audit before its first catalog
mutation. It requires the exact upstream-032 request-classification columns and
constraints; an installation stamped by the abandoned feature-only 032 shape
therefore fails instead of silently advancing while missing upstream state. It
then inspects every possible resource-action evidence table: logical samples,
represented attempts, worker cohorts, cohort references, decision coverage,
and coverage-only attempts. Tables may be absent, which is the expected shipped
032 shape, or present and empty, which permits interrupted/hybrid catalog
convergence. Any row fails closed for a separately reviewed backfill. If the
old resource-action columns already exist, every service must still have
`resource_action_mode='legacy'` with a null change timestamp, and every replica
identity/action/sample/coverage link must be null. Any action-owned row state
also fails closed. Migration never synthesizes evidence or intent.

After that audit, revision 033 validates every already-present portable action
column against its exact type, nullability, and default. Because all six action
tables are proven empty, it drops any present subset in dependency-safe order
and creates the complete six-table head graph from one metadata catalog. This
avoids retaining a weak same-name column/constraint and avoids creating a
dependent foreign key before an adopted parent has its key. A reflected
postcondition verifies the complete columns, keys, checks, foreign keys, and
indexes before Alembic may stamp the revision. On non-PostgreSQL Serve
controller databases it adds only the portable service and replica columns at
revision 033. Upstream revision 032 is not rewritten, and resource revision 033
does not support schema down.

Revision 033 is shipped and its action/reference shape is immutable. Revision
034 adds only the PostgreSQL authority-release ledger and permanent exact
cohort-manifest bindings used by the blocking Helm release fence; it does not
repair or rewrite revision 033. If deployment ever discovers an incompatible
historical 033 shape, rollout stops. Any repair must use the next free revision
(038 or later), require an empty worker-reference table before adding a
nonnull commitment, and must not invent a default or synthetic capability.
The paragraph above records revision 033's exact historical contract; no M4
code rewrites, restamps, or conditionally replaces it. The remaining M4
authority state uses additive Serve038, whose guarded preflight validates both
shipped Serve033 and the immutable Serve034 release ledger before adding any
new shape.

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
canonical hash. The Helm value is a version suffix, not the database key. Each
authority installation also has one immutable operator-provisioned UUID `I`
that is unique among clusters/releases sharing the central database. For
release namespace `N`, rendered Helm full name `F`, and suffix `S`, the
database key is
`"ra:" + I + ":" + sha256(N + "\\n" + F + "\\n" + S).hexdigest() + ":" + S`.
`N`, `F`, and `S` are their exact UTF-8 bytes and each displayed
`"\\n"` is one byte `0x0A`, not a two-character backslash escape.
`S` is a lowercase DNS-label of at most 42 characters, and the rendered
Deployment and ServiceAccount names are derived from `F` and `S`; the full
derived key is stored in every manifest, proof, reference, and registry row.
The chart rejects a missing/changed/noncanonical installation UUID or any key /
resource-name overflow. The table primary key plus exact-identity adoption
turns a duplicated installation UUID or cryptographic collision into a
fail-closed rollout conflict rather than cross-release adoption. Cross-release,
cross-namespace, cross-installation, and forced-collision tests cover this
database-wide identity boundary.

Serve revision 034 adds the stable Helm release fence which makes that boundary
durable before any cohort Pod can register:

```text
serve_resource_action_authority_releases
  (namespace, helm_release_name) primary key
  installation_id            UUID not null unique
  helm_full_name              TEXT not null
  enabled                     BOOLEAN not null
  live_manifests              JSONB array not null
  live_inventory_sha256       TEXT not null
  tombstone_suffixes          JSONB array not null
  tombstone_inventory_sha256  TEXT not null
  revision                    BIGINT not null
  created_at                  TIMESTAMPTZ not null
  updated_at                  TIMESTAMPTZ not null

serve_resource_action_authority_release_cohorts
  (namespace, helm_release_name, cohort_suffix) primary key
  cohort_id                   TEXT not null unique
  manifest                    JSONB object not null
  manifest_sha256             TEXT not null
  bound_at                    TIMESTAMPTZ not null
  foreign key (namespace, helm_release_name) references
    serve_resource_action_authority_releases on delete restrict
```

The stable lookup key is the exact namespace plus `.Release.Name`, not the
mutable rendered full name. The first enabled preflight binds the globally
unique installation UUID and rendered full name permanently. Every live suffix
then binds its complete canonical manifest permanently; neither a suffix nor a
cohort ID can later name different bytes. A disabled proposal carries an empty
installation ID and empty inventories. It is a no-op when no release/history
exists, but when the stable release row exists it locks and reuses that anchor,
so clearing all authority values cannot evade the old inventory. A changed
rendered full name, changed nonempty installation UUID, cross-release UUID
reuse, live/retired resurrection, tombstone without an exact bound
`REMOVAL_AUTHORIZED|RETIRED` row, or omission of a nonretired cohort fails the
upgrade. A `REMOVAL_AUTHORIZED` cohort may remain in the live inventory for the
first removal upgrade or move to the tombstone inventory in the next; only
absence from both is unsafe.

The Helm PostgreSQL migration hook receives a closed canonical proposal with
`version`, namespace, Helm release name, rendered full name, installation ID,
enabled flag, sorted live `{cohort_suffix,path}` entries, and sorted tombstone
suffixes. Each path is fixed below
`/etc/skypilot/resource-action-authority/release-preflight/`; a weight `-20`
immutable hook ConfigMap contains the exact same canonical `$manifestJson`
bytes used by the eventual worker ConfigMap, and the weight `-10` migration Job
descriptor-reads its read-only file before calling the ledger transaction. The
hook runs for every PostgreSQL deployment using the required pre-existing
database Secret even when HA and authority are both disabled. Authority cannot
be enabled with the chart-managed database-secret path; changing database
credential topology is a separate post-retirement operation, not part of an
authority removal upgrade.

Runtime activation is never inferred from the older installation/inventory or
preflight-token environment names, which remain available to disabled
deployments for compatibility. The chart exclusively owns and always reserves
`SKYPILOT_RESOURCE_ACTION_AUTHORITY_ENABLED`; only exact text `true` activates
the runtime. The API role receives it for every enabled inventory, including a
tombstone-only retirement release. The controller receives it only while a
live cohort and its preflight credentials/manifest are mounted. Absence is
authoritative disabled even if compatibility environment names are present.

One additional release-fence gate remains before the first enabled cohort. The
current hook is selected from the *proposed* PostgreSQL backend and pre-existing
Secret values. A later ordinary upgrade that clears or changes both cannot yet
discover historical ledger state and could omit the hook while Helm removes
authority objects. Before enablement, a follow-up must install a stable,
retained release anchor without a first-enable crash window, pin the original
database Secret reference, and require every subsequent ordinary upgrade to
resolve that anchor and run the ledger preflight even when proposed backend or
credential values change. Missing/tampered anchors or Secrets must block before
workload deletion. `--no-hooks`, raw Kubernetes deletion, and uninstall remain
explicit administrator bypasses and require a documented break-glass/finalizer
protocol; they are not supported rollout mechanisms. This gap does not block a
dark deployment that has never enabled authority and therefore has no release
row or cohort history.

Both preflight and first registration lock the release row before a cohort row.
Registration resolves the installation UUID from the manifest's full cohort
ID, requires the release to remain enabled, and compares the complete manifest
bytes with both the current live inventory and permanent suffix binding before
inserting `REGISTERING`. Thus a preflight/registration race either registers
under the old locked inventory before preflight evaluates all worker history,
or observes the new inventory and fails closed; it cannot create an unbound
cohort between validation and Helm apply.

Each full ID permanently names one Deployment UID and identity and is never
reused. `registration_attestations` is the companion's bounded
`ProviderAuthorityWorkerRegistrationSetV1`; typed writes recompute its hash and
permit only distinct, current Pod registrations for the immutable cohort.
At an activation or rollback transition, both each registration's
`registered_at` and its embedded worker identity's `observed_at` must be at or
before the transaction's fresh PostgreSQL `clock_timestamp()` and no more than
five minutes old. The bound is server-owned and not configurable in M2/M3.
Insertion creates `REGISTERING`, never `ACCEPTING`. Both Pods must already be
Kubernetes Ready on `/bootstrapz`; the first process that observes the exact
Deployment at its current generation/resourceVersion with desired, updated,
status-total, ready, and available replicas all equal to two and unavailable
replicas zero inserts one registration for
its own Pod/owner chain. The peer reads that row after insert conflict, observes
the same Deployment generation/resourceVersion and `2/2` counters, and
compare-and-swap appends only its own distinct registration; neither process
GETs or invents its peer Pod. The stored two-entry set is then canonically
sorted by Pod UID. `REGISTERING -> ACCEPTING` requires exactly those two
matching ready-worker attestations and a final same-snapshot Deployment read.
Lost insert/append/promotion acknowledgements are resolved by exact row read
and revision adoption, never by blind replay against remembered state.
Normal retirement is `ACCEPTING -> DRAINING -> REMOVAL_AUTHORIZED -> RETIRED`.

A never-accepted cohort cannot be left permanently wedged by a crash. The
API-role abort transaction may change `REGISTERING -> REMOVAL_AUTHORIZED` only
when PostgreSQL time proves every registration timestamp older than five
minutes, the identity was never `ACCEPTING`, and locked scans find zero cohort
references, private requests/evidence, action specs/attempts, or activation /
promotion proofs. It binds the exact Deployment and ServiceAccount UIDs and
rejects any concurrent append, promotion, or reference. The current chart then
removes only those exact objects; API-role NotFound verification commits
`RETIRED`. Recovery deploys a new suffix/full cohort ID; an aborted ID is never
reopened. Crash-before-insert is a no-op, crash-after-insert is recovered by
this abort path, crash-after-second-append can still promote while fresh, and
a lost promotion acknowledgement is adopted by exact read.

Each process that adopts an `ACCEPTING` or `DRAINING` cohort holds only a
process-local evidence lease ending five minutes after the oldest
registration/observation timestamp. A watchdog continuously rereads PostgreSQL time and the exact live
Pod/ReplicaSet/Deployment/ServiceAccount and projected-file snapshots. Before
the lease expires, a same-cohort, same-state `ACCEPTING -> ACCEPTING` or
`DRAINING -> DRAINING` compare-and-swap may
replace only the caller's own Pod-UID entry, preserving the peer entry's
canonical bytes exactly. The sorted UID set, Deployment UID/generation,
Deployment resourceVersion, ServiceAccount UID, immutable identity, and
non-temporal proof fields cannot change; only that caller's observation /
registration timestamps and Pod/ReplicaSet resourceVersions may advance. A CAS loser exact-reads the winner and reapplies
only its own entry. A UID outside the accepted pair fails immediately, and a
survivor clears acceptance no later than peer freshness expiry. A Pod
replacement, Deployment resourceVersion or owner UID/generation change,
artifact drift, failed renewal, clock
failure, or future/stale observation clears local acceptance and makes the
endpoint unavailable. P2a replacement uses a new cohort ID. DRAINING renewal
creates no reference or claim; once P3 exists it only keeps already-frozen work
eligible for its per-dispatch proof. P3 must add its separately reviewed
rolling-replacement protocol before queue claims.

Rollback takes `DRAINING -> ACCEPTING` only in the same transaction that
replaces the registration set with two current matching attestations while the
exact Deployment and ServiceAccount still exist. New references require the
locked cohort to be the currently selected `ACCEPTING` cohort; existing
references retain their frozen cohort and remain claimable while it is
`DRAINING`.

### Serve038 M4 membership and runtime extension

The shipped P2a/Serve034 path above remains the exact preflight-only baseline.
The remaining M4 runtime retains that bootstrap and release fence, then moves
runtime membership to the following additive Serve038 contract before any
private claim or provider effect.

`registration_attestations` is the companion's bounded
`ProviderAuthorityWorkerRegistrationSetV2`; typed writes recompute its hash and
permit only distinct, current Pod registrations for the immutable cohort. V1
is an already-shipped closed contract whose per-Pod identity contains a
Deployment resourceVersion and whose set has no independent Deployment
snapshot. Its bytes and parser remain frozen for retained Serve033 history and
retirement only. Serve038 never creates, renews, selects, activates, rolls back
to, or claims through V1 *registration* evidence. The already-shipped
`ProviderAuthorityWorkerAttemptAttestationV1` inside progress V1 remains
byte-frozen execution-local before/after provenance: its two identities belong
to one process, so its Deployment resourceVersion is not a cross-Pod readiness
claim. Every M4 dispatch additionally projects its common fields and requires
byte equality to that caller's current V2 membership; the extra V1 field grants
no membership or readiness. Removing the per-Pod field and adding the set-level
snapshot changes the canonical registration bytes, so M4 uses version 2 rather
than silently redefining V1.

That version boundary also freezes the projected workload contract. The
shipped P2a static manifest is version 1, carries claim contract
`frozen_action_cohort_join_v1`, and requires an exact `Recreate` Deployment;
its manifest, Deployment-snapshot, registration, and claim parsers remain
readable only for the Serve034 cleanup and retained-retirement program. M4
uses a distinct version-2 static manifest, claim contract
`frozen_action_cohort_join_v2`,
`ProviderAuthorityWorkerDeploymentSnapshotV2` parser, and registration parser.
Its Deployment strategy is exactly `RollingUpdate` with integer
`maxSurge=0` and integer `maxUnavailable=1`. Neither parser accepts the other
version, and no apparently matching V1 manifest, `Recreate` Deployment, or V1
registration can select a cohort, mint a private-dispatch proof, or advertise
a claimant after Serve038. This is a compatibility boundary, not an in-place
reinterpretation of the shipped P2a bytes.

In V2 the snapshot is state-dependent nullable:
`REGISTERING` has one or two registrations and a null snapshot, while
`ACCEPTING | DRAINING` has exactly two registrations and one nonnull final
snapshot. `REMOVAL_AUTHORIZED | RETIRED` preserves whichever of those two
shapes the legal retirement source had; it cannot manufacture evidence. The
cohort has null `removal_authorized_at` before `REMOVAL_AUTHORIZED`, sets it
exactly once from that transition's PostgreSQL clock, and preserves it through
`RETIRED`; `state_changed_at` and `retired_at` still advance truthfully on the
later retirement edge. The
initial one-member V2 `REGISTERING` cohort and embedded set both start at
revision one. Every later legal V2 cohort write—registration append, activation,
handoff or cold-recovery completion, rollback, or lifecycle-only transition—
advances both revisions by exactly one and keeps them equal. Lease renewal and
handoff-table-only transitions advance neither. Unknown commit outcome adopts
only the exact expected before/after revisions, state, canonical bytes, and
hashes. At
initial activation, handoff or cold-recovery completion, or rollback re-
attestation, each
installed registration's
`registered_at` and its embedded worker identity's `observed_at` must be at or
before the transaction's fresh PostgreSQL `clock_timestamp()` and no more than
five minutes old. The one set-level Deployment snapshot has its own equally
bounded `observed_at`; its resourceVersion is not copied into either Pod
identity. The bound is server-owned and not configurable.

Bootstrap remains a permanent phase of the M4 runtime. M4 extends the already
merged preflight-only P2a image rather than deleting or bypassing its bootstrap
boundary:

```text
bind /livez + /bootstrapz + authenticated preflight
  -> verify the projected manifest and this Pod's owner chain
  -> both Pods become Kubernetes-ready on /bootstrapz
  -> register/adopt two distinct current Pod attestations and leases
  -> read/adopt one final set-level Deployment snapshot
  -> cohort REGISTERING -> ACCEPTING
  -> complete static cohort/transport/RBAC readiness and publish /readyz
  -> start/advertise the existing request claimant
```

Insertion creates `REGISTERING`, never `ACCEPTING`. Each Pod GETs only its own
Pod and owner chain. The first insert and peer append each atomically write that
Pod's anchor registration plus its generation/revision-one ACTIVE lease. On an
exact active-policy-bound Serve039 or Serve040 fresh anchor, that post-039
INSERT also locks the process-unique
bootstrap API row after the lease, installs the owner/hash/normalized-process
triple, and changes the API row to exact bound phase with that owner hash in the
same commit. Lost acknowledgement adopts the immutable anchor, its same-stable-
identity lease lineage, and that exact bound API phase/boot identity. The sole
owner-null exception is a retained pre-039 lease; it must complete the typed
`BIND_EXECUTION_OWNER` transaction before activation, and no new post-039 INSERT
may create an owner-null lease. At generation one the lease must retain the exact insert
operation ID and registration bytes; afterward it must be an `ACTIVE`,
generation/revision-equal descendant reached only by legal `RENEW`, initial
`BIND_EXECUTION_OWNER`, or retained `SUPERSEDE_EXECUTION_OWNER` transitions,
whose renewal registration has the same stable projection as the anchor. Any
owner change must resolve through the exact process-supersession chain. The atomic
anchor/lease writer and typed successor rules therefore make an orphan anchor
or lease unrepresentable. `REGISTERING ->
ACCEPTING` requires exactly two sorted
matching ready-Pod anchor registrations. Because bootstrap may take longer than
their freshness window, the activation transaction locks both current ACTIVE
registration leases and then both rows named by those leases' normalized current
execution-owner scalars in canonical process order. Each owner JSON/scalar and
API row must cross-equal, and each API row must bind `pod_uid` to the same
stable authority-worker/lease UUID, remain in exact bound phase with the lease's
owner hash, and have a fresh heartbeat through commit; `ready=true`
is neither required nor legal yet because acceptance precedes published
readiness. The transaction constructs the installed two registrations from their
exact renewal-registration bytes after Pod/stable-projection equality to the
anchors. Before that short transaction, the API verifier reads and hashes the
same Deployment UID/generation/template into a candidate set-level snapshot,
with desired, updated, total, ready, and available replicas all two and
unavailable replicas zero. The transaction performs no Kubernetes I/O; it only
validates the bounded fresh proof and installs it atomically. Immediately before
commit, fresh PostgreSQL time must still precede both registration-lease
and API-instance expiries and keep both registrations plus the snapshot inside the fixed bound;
otherwise activation rolls back and rereads.
The two Pod attestations prove their owner chain and immutable generation but
do not need to equal that final Deployment resourceVersion. `/bootstrapz` is the
Kubernetes readiness probe; `/readyz` is false until the accepted cohort and
static manifest, transport, principal, claim-filter, and RBAC checks make the
worker safe to receive eligible work. Target- and action-kind-specific
preflight is impossible at startup: it runs later for one manager decision
while its exact reference is `PREPARING`, and its result is bound into
admission and the one-request dispatch proof. Only after static readiness may
the existing executor start, resolve its cohort-bound claim config, mark its
server-instance lease ready, and advertise a claimant. Startup never creates
an action, reference, request, queue row, or provider effect by itself.

The bounded implementation as of 2026-08-03 includes initial V2 membership
activation plus the dark, mutation-free `PREPARING` trust fence. Both the
installed static loader and the actual Helm
migration-hook release preflight dispatch only exact numeric manifest versions
1 and 2. Numeric V1 continues through the frozen Serve034 release store;
numeric V2 uses the additive typed Serve038 ledger writer, requires the old V1
live/tombstone inventory to be clear, permanently binds each fresh suffix, and
does not permit V2 removal before its retirement protocol exists. Every
manifest is descriptor-read and JSON-parsed exactly once; numeric dispatch and
the typed value sent to the durable writer derive from those same bytes, so
there is no dispatch/load TOCTOU. Every retained row rejects either raw list,
or their combined length, above 256 before hashing, iterating, or decoding it.
Every V2 registration first locks and fully decodes the retained release row:
canonical typed uniform-version manifests, both recomputed hashes,
sorted/unique/bounded and disjoint inventories, immutable identities, positive
revision, and ordered timezone-aware timestamps, followed by the complete
permanent binding row.

The runtime selects the frozen V1 or additive V2 coordinator by exact parsed
type. The V2 coordinator performs four bounded-time self/owner-chain GETs,
exact `RollingUpdate` snapshot projection, one-member insert/adoption,
second-member append, own-lease renewal, a fresh final Deployment read, and the
existing locked two-member `REGISTERING -> ACCEPTING` transaction. Every
registration/append/renew/activation and local acceptance publication crosses
one process-local stop gate. `stop()` sets the stop event and crosses that same
gate before returning; an already admitted mutation completes before return,
while a Kubernetes or database read that unblocks after the bounded join cannot
write or publish. Every one of the four production bootstrap mutation
transactions first sets
`statement_timeout=5000ms` and `lock_timeout=3000ms` locally before its first
lock. The ordinary engine also bounds pool checkout and connection
establishment at 15 seconds each. Those are the graceful path. `stop()` gives
the complete mutation gate 30 seconds; if both pool and connection budgets are
consumed, it errs toward fail-stop rather than borrowing from termination.
This leaves 20 seconds beyond the ten-second tail join inside the 60-second Pod
grace for the preflight server, health listener, signal loop, and process
teardown. If a DBAPI response/network
blackhole defeats the graceful limits, the dedicated authority-worker invokes
nonreturning `os._exit(70)`: process death closes the PostgreSQL connection and
rolls back uncommitted work, rather than returning into a possible late commit
or adoption. Tests inject a raising `NoReturn` substitute to prove this whole-
gate deadline and immediate acceptance clearing without killing the test
runner. The runtime now installs the disjoint V2 evaluator, but still does not
start an executor, mark the API instance ready, advertise a claim, create an
action/request, or perform provider I/O. The V1 evaluator continues to exact-
type filter V2 membership. Manager admission does not yet invoke the available
`PREPARING` writer, so a production request still receives fixed typed 503;
tests can reach the valid locked branch and receive only typed unavailable.
Accepted membership and that diagnostic response authorize no action, claim,
effect, or provider call. Atomic preparation/admission, claim gates, runtime
handoff, cold recovery, retirement, and rollback orchestration remain later
phases.

`REGISTERING` has no in-place member replacement. Exact UID-qualified absence
of either one-member or two-member anchor before activation makes the cohort
permanently ineligible. A surviving API verifier locks cohort -> handoff slot ->
all registration leases -> references, proves the cohort was never accepted and
has zero handoff/reference/private/action/effect evidence, terminally revokes
every ACTIVE lease with `COHORT_REMOVAL`, and advances directly to
`REMOVAL_AUTHORIZED` using one operation time equal to
`GREATEST(clock_timestamp(), locked_prior.state_changed_at, every affected
locked lease.renewed_at)`. Every such lease has null
`revocation_owner_id`, preserves its nullable or nonnull execution-owner/hash/
normalized-process-scalar triple exactly, records `last_operation_kind=REVOKE`,
and has `revoked_at ==
cohort.removal_authorized_at`. Lease expiry, unready state, deletion timestamp, or
name-only evidence is insufficient. Chart removal plus exact Deployment and
ServiceAccount NotFound commits `RETIRED`; recovery creates a new suffix and
cohort ID rather than deleting or replacing an anchor in place.

Each process renews only its separate Serve038 registration-lease row every 20
seconds by writing a fresh self-read V2 identity/hash under compare-and-swap.
The transaction uses PostgreSQL time and sets expiry to exactly 60 seconds
after `renewed_at`; both constants are server-owned and not configurable. The accepted
membership registrations and sole set-level Deployment snapshot remain frozen
until an explicit handoff, full-set cold recovery, rollback re-attestation, or
retirement transition;
heartbeat churn cannot rewrite their hash. Own-identity drift, failed renewal,
or database-clock failure clears that process's `/readyz` and stops its claims.
Lease insertion or renewal locks cohort -> relevant nonterminal handoff ->
lease; a renewal then locks the lease's normalized current execution-owner API
row at class 14. Renewal is authorized only when the caller's process UUID
equals both the owner JSON and scalar, the API row repeats the same stable Pod,
stored start identity, role and immutable inventories and remains fresh through
commit, and the stable lease is an exact current V2 `REGISTERING | ACCEPTING |
DRAINING` member or the exact candidate of the unique `OPEN | READY` handoff.
The bootstrap process before bind/supersession and every prior process reject;
normal renewal preserves the complete owner triple.
The only additional insertion authorization is for the exact two locked cold-
recovery candidates inside the same atomic membership CAS; they have no lease
before it and cannot renew unless that commit makes them accepted members.
Peer expiry blocks new preparation references; a still-fresh accepted survivor
may finish or recover already-bound work while replacement is in progress.
The Deployment uses `maxSurge=0,maxUnavailable=1`, so an ordinary replacement
removes at most one worker before admitting its successor and never creates an
unattested third worker.

Ordinary one-at-a-time Pod rescheduling under the same immutable Deployment
does not mint a new cohort. It uses the durable `OPEN -> READY -> COMPLETED`
handoff below; there is no one-transaction shortcut that invents a survivor
acknowledgement. The candidate serves `/bootstrapz` but keeps `/readyz=false`,
does not advertise a claimant, cannot receive a request, and has no registration
lease before `OPEN`. It supplies only its fresh self-read V2 identity. Before
opening the handoff, the API verifier obtains a fresh UID-qualified Kubernetes
observation:
GET of the stale Pod name must return exact NotFound or an object with a
different UID. `deletionTimestamp`, `Terminating`, lease expiry, or an
unqualified list result is insufficient. Pod UIDs cannot be recreated, so this
absence fact remains monotonic after SQL lock acquisition.

Before the mutating transaction, nonlocking discovery reads the bounded stale
request-then-queue inventory using that same unfiltered all-active owner query
and closed-validates every row as one of the four private shapes rather than
filtering malformed or ordinary-looking state,
inventory and complete current lease snapshots needed to construct an
optimistic closed fence and proposed operation time
`GREATEST(clock_timestamp(), stale_lease.renewed_at,
survivor_lease.renewed_at,
candidate_registration.worker.observed_at,
candidate_execution_owner.container_started_at,
candidate_execution_owner.observed_at, candidate_api_instance.started_at,
<every discovered private_request_terminal_lower_bound>)`; the final term is
omitted for an empty inventory. The opening transaction
locks the cohort/source V2 set and requires
membership exactly stale plus survivor. For an adopted chain it exact-reads and
validates the immutable terminal predecessor/root before any later-class lock.
Using that proposed operation time, it first inserts the complete `OPEN`
handoff at class
4, including the immutable source, UID-absence proof, optimistic fence, and
candidate registration. It then visits stale, survivor, and absent candidate
registration-lease keys in canonical instance order at class 5: both accepted
rows are locked and must exactly match every discovered byte, generation, and
revision used by the proposed time/fence or the whole transaction rolls back.
The survivor must be ACTIVE and fresh, the candidate key is
rechecked absent immediately before inserting its generation/revision-one
lease, and the stale row must be ACTIVE for `NEWLY_REVOKED` or the exact
retained terminal `STALE_HANDOFF` row for adoption. The new branch revokes the
stale lease optimistically; adoption preserves it.

Only after every class-4/5 insert/update is staged does the transaction lock the
stale/survivor execution-owner and candidate bootstrap API server-instance rows
in process-instance order,
requiring the survivor row and candidate bootstrap row fresh. It revalidates
the candidate's immutable boot/stable identity and atomically stages
bootstrap-to-bound with the newly inserted lease's exact owner hash; no commit
can expose the candidate lease while its API row remains bootstrap/null-hash.
Only then does it lock each
discovered request row in request-ID order and only then every corresponding
queue row in request-ID order. It reruns the bounded stale-claim
inventory under those locks, requires exact equality and no unlisted claim, and
reconstructs every V2 terminal-fence entry. It then invokes the borrowed batch
core once: terminalizes all requests `CANCELLED`, deletes all queues, key-share-
locks all named action lineages, inserts/adopts all action selectors, inserts/
adopts all shadow terminal histories, and only then allocates all events. It
uses only `STALE_OWNER_FENCE`: an action arm with lineage maps to
`REQUEST_CANCELLED/LINEAGE`, an action arm without lineage maps to
`TERMINAL_BEFORE_CLAIM_START/NO_SUCCESSFUL_CLAIM_START`, and a shadow arm writes
exact `REQUEST_CANCELLED/SHADOW_EXECUTION` for `AUTHORIZED` or
`TERMINAL_BEFORE_CLAIM_START/NO_SUCCESSFUL_CLAIM_START` for `BOUND`, with the
corresponding nonnull/null lineage hash. It never creates missing lineage. If
a locked request carries a nonnull cancellation-intent timestamp, the batch
preserves it and writes `cancel_acknowledged_at` equal to this fence time;
otherwise both cancellation fields remain null.
No scalar terminalizer or same-request requeue is legal. Any lease, API, receipt,
or inventory drift
rolls back the entire transaction, including the earlier uncommitted handoff,
lease insert, and revocation. Generic request terminalization need not hold the
cohort lock, so any pre-read/requery mismatch likewise rolls back and retries.
Immediately before commit, a fresh `clock_timestamp()` must still precede the
survivor/candidate registration-lease and API-instance expiries and every
registration/Kubernetes proof must remain inside its fixed freshness bound; a
lock wait past TTL rolls back and retries from new evidence.
No Kubernetes/provider I/O occurs under SQL
locks. Before the atomic commit there is no visible candidate lease or handoff;
after it, only the exact handoff candidate may renew. Unknown-result adoption
requires the exact handoff/lease evidence and the same candidate API row's bound
phase, immutable boot/stable identity, and owner hash; bootstrap plus unchanged
source is uncommitted and permits a full retry, while any partial or unequal
joined shape blocks. While a handoff is `OPEN`
or `READY`, new `PREPARING` references for that cohort reject. Claim SQL—not
only `/readyz`—requires the caller's stable authority-worker ID to be a member
of the currently accepted V2 set, the stable lease's execution owner to equal
the caller's process API-instance ID, both leases to be fresh, and the stable
member not to be the stale or candidate identity of a nonterminal handoff. The
accepted survivor may continue already-
bound work; the fenced stale process and unaccepted candidate cannot claim.

After exact-reading this handoff's `(handoff_id, chain_sequence, revision=1,
state=OPEN)`, the survivor rereads its own Pod -> ReplicaSet -> Deployment
chain. One transaction locks cohort -> handoff -> survivor registration lease
-> that lease's normalized current execution-owner API row. It requires the
acknowledging caller process UUID to equal the owner JSON/scalar, revalidates
the API row's stable Pod/start identity and freshness, renews that lease from
the fresh V2 registration using one acknowledgement time equal to
`GREATEST(clock_timestamp(), source_lease.renewed_at,
fresh_registration.worker.observed_at)` while preserving the owner triple,
stores the exact same
registration/hash as the survivor acknowledgement, and changes `OPEN -> READY`
by compare-and-swap. It does not attest the candidate and does not copy a
Deployment resourceVersion into its identity. The candidate then reads the
final two-ready, two-available Deployment status and constructs the sole
set-level snapshot. One final transaction re-locks the same source revision,
handoff, both registration leases, both rows named by their normalized current
execution owners, and arm-specific
terminal receipts in the
global order; requires each current ACTIVE lease's renewal registration to be
fresh, ID-equal, and stable-projection-equal to its immutable candidate or
post-fence survivor anchor; constructs the final two registrations from those
exact lease bytes; revalidates the snapshot; atomically replaces stale
membership with candidate membership;
writes the complete V2 set and hashes; and changes `READY -> COMPLETED` with
one consecutive cohort revision. It exact-validates every immutable fence
receipt and requires no new stale claim, then immediately
before commit uses fresh PostgreSQL time to require both registration/API leases
unexpired and every Pod/Deployment/absence proof still inside its fixed bound;
drift or a wait past TTL rolls back and retries. Only exact readback of that terminal row and
after-set hash adopts a lost acknowledgement. The candidate then eagerly warms
all fixed children. Its bound-to-ready CAS locks cohort -> any nonterminal
handoff -> its accepted stable lease -> the row named by that lease's normalized
current execution owner; requires the caller process UUID to equal owner JSON/
scalar, the API row to remain exact bound phase with
`health_detail.execution_owner_sha256 == lease.execution_owner_sha256`, stable
Pod/start identity and freshness, and the candidate stable member to remain in
the accepted set. Only that CAS (or exact adoption by the same process) marks
the API row ready and permits `/readyz`/claim advertisement. Supersession during
warming makes the old process CAS fail.

Recovery may resume an `OPEN` or `READY` row from its immutable evidence. It may
change it to `ABANDONED` only after proving exact candidate-Pod UID absence and
zero candidate request claim, journal-before-I/O watermark, provider progress,
or effect; abandonment never changes membership and allows a new candidate to
open a chained handoff under the retained-fence protocol below. Loss of both
registered workers requires exact survivor- and candidate-UID absence proofs
before retaining an abandoned handoff, then the same immutable cohort cold-
recovers both members. Any unprovable fence blocks recovery. A template/image/
Deployment UID change may create a new cohort for new work but cannot replace a
cohort with bound work; that old cohort remains blocked and retained until its
exact immutable recovery surface is restored or zero-work retirement is proved. A
stale never-accepted row may
advance to `REMOVAL_AUTHORIZED` only
after a locked scan proves it was never accepted and has no reference, private
request/evidence, action, or promotion proof; lost acknowledgement is resolved
by exact row read rather than blind replay.

For either abandonment reason, the API verifier obtains the candidate UID-
absence proof, and the survivor proof when required, before SQL; those
UID-qualified Kubernetes facts are monotonic. The no-I/O transaction then
locks cohort -> nonterminal handoff -> candidate registration lease, followed
by the row named by that lease's normalized current execution owner, with JSON/
scalar/API stable-Pod and start equality, then all candidate request rows in request-ID
order, and then all corresponding queue rows in request-ID order if present.
Under that prefix, every legal candidate claim, attempt, progress, provider-
operation, and effect writer is excluded; fail-closed scans of those later
classes construct the zero-effect proof. The one operation time is
`GREATEST(clock_timestamp(), candidate_lease.renewed_at)` and is stored as
`candidate_zero_effect_proof.observed_at == handoff.terminal_at ==
candidate_lease.revoked_at`, with `last_operation_kind=REVOKE`, while the same
CAS writes the immutable absence hashes and terminal `ABANDONED` state. An
unknown outcome exact-reads that joined terminal evidence; no earlier zero-
effect snapshot can be replayed.

Normal retirement is `ACCEPTING -> DRAINING -> REMOVAL_AUTHORIZED -> RETIRED`.
Every lifecycle edge computes one logical database transition time as
`GREATEST(clock_timestamp(), locked_prior.state_changed_at, every affected
locked lease.renewed_at)` and writes it to `state_changed_at`; removal edges
also use that exact value for
`removal_authorized_at` and all `COHORT_REMOVAL` revocations. This preserves the
shipped `state_changed_at >= created_at` invariant across wall-clock regression.
Every lifecycle transition locks cohort -> nonterminal handoff -> both accepted
registration leases and rejects a nonterminal handoff. The
claimable-membership transitions `ACCEPTING -> DRAINING` and
`DRAINING -> ACCEPTING` additionally reject while any currently accepted
member has a terminal `STALE_HANDOFF`-revoked registration lease: that retained
source membership must stay byte-identical until its chained handoff completes.
An explicit exceptional `ACCEPTING -> REMOVAL_AUTHORIZED` edge is legal only for
that unresolved terminal-stale membership when the same transaction continues
through references in global order and proves the complete locked zero-
non-`RELEASED`-reference/zero-work inventory plus fail-closed defensive scans required for
removal. It terminally revokes the survivor, sets `removal_authorized_at`, and
permits no rollback, cold recovery, or future handoff. No unmarked retirement-
only `DRAINING` state exists. The ordinary `DRAINING -> REMOVAL_AUTHORIZED` edge
remains legal under its same exact zero-non-`RELEASED`-reference proof. The
transaction entering `REMOVAL_AUTHORIZED` locks all registration leases before
references, proves the closed zero-non-`RELEASED`-reference inventory, retains
every `RELEASED` row byte-for-byte, terminally revokes all
remaining ACTIVE leases, and advances cohort/set revisions together using one
operation time computed by that same `GREATEST` expression. Every
`COHORT_REMOVAL` lease has null `revocation_owner_id`, preserves its execution-
owner/hash/normalized-process-scalar triple exactly, records
`last_operation_kind=REVOKE`, and has `revoked_at ==
cohort.removal_authorized_at`; that write-once time survives `RETIRED` while
`state_changed_at` advances. Renewal thereafter rejects under the cohort lock.
Before rollback, the API verifier reads and hashes the exact Deployment,
ServiceAccount, both Pod owner chains, and one final snapshot. Rollback takes
`DRAINING -> ACCEPTING` only in a no-I/O transaction that locks cohort ->
handoff -> both registration leases -> both rows named by their normalized
current execution owners, rechecks each owner JSON/scalar/API stable-Pod/start
identity, and requires
both registration and API server-instance leases fresh at precommit, replaces the set with the exact current renewal-
registration bytes after stable equality to its two membership anchors, proves
no nonterminal handoff, and validates every pre-read Kubernetes proof within the
same five-minute database-clock bound. Immediately before commit, fresh
PostgreSQL time rechecks both registration/API expiries and all proof bounds;
drift or a wait past TTL rolls back. New references require the
locked cohort to be the currently selected `ACCEPTING` cohort; existing frozen
references remain executable while it is `DRAINING`.
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
That verifier first obtains and hashes both fresh NotFound proofs outside SQL.
A short no-I/O transaction then locks the stable release row before cohort ->
handoff -> all leases -> references. It revalidates that the exact suffix
remains in the current tombstone inventory and absent from live inventory,
`REMOVAL_AUTHORIZED`, the exact tombstone names, zero non-`RELEASED`
references, byte-equal retained `RELEASED` history, and the proofs, then
commits `RETIRED` by one CAS. It sets `retired_at == state_changed_at ==
GREATEST(clock_timestamp(), removal_authorized_at)`, so a backward wall-clock
adjustment cannot wedge the monotonic timestamp CHECK;
`removal_authorized_at` already dominates every affected locked lease's
`renewed_at`. A concurrent
tombstone-to-live rollback therefore either wins first and blocks the stale
NotFound result, or observes `RETIRED` and rejects recreation.
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

`service-version/spec hash` is the following closed durable primitive, never a
hash of a pickle or an unlocked reconstruction:

```text
ServeServiceVersionSpecIdentityV1 = {
  version: 1,
  service_name: Text,
  service_incarnation: UUID,
  service_version: PositiveInteger,
  effective_service_config_sha256: Sha256,
  effective_task_config_sha256: Sha256,
  capacity_profile: ServeActionCapacityProfileV1,
  provider_profile: "pod_cluster_v1"
}
```

The first live action contract that may carry this identity is a new closed
envelope; the already-shipped action V1 is not extended:

```text
ShadowCandidateActionBindingV1 = {
  version: 1,
  binding_kind: "shadow_candidate",
  candidate_epoch: UUID,
  qualification_policy_sha256: Sha256,
  qualification_binding_sha256: Sha256
}

AuthoritativeActionPolicyBindingV1 = {
  version: 1,
  binding_kind: "authoritative_action",
  policy_epoch: UUID,
  policy_sha256: Sha256,
  authority_binding_sha256: Sha256
}

ServeReplicaActionAdmissionBindingV1 =
  ShadowCandidateActionBindingV1 | AuthoritativeActionPolicyBindingV1

ServeReplicaActionSpecV2 = {
  version: 2,
  service_version_spec_identity: ServeServiceVersionSpecIdentityV1,
  service_version_spec_identity_sha256: Sha256,
  admission_binding: ServeReplicaActionAdmissionBindingV1,
  provider_plan: ProviderLifecyclePlanV2,
  invocation: ProviderLifecycleInvocationV2
}
```

The V2 nested provider graph is additive rather than an in-place change to the
deployed V1 graph:

```text
ProviderAuthorityWorkerCohortReferenceV1 = {
  version: 1,
  cohort_id: Text,
  cohort_identity_sha256: Sha256
}

ProviderKubernetesExecutionCapsuleV2 =
  ProviderKubernetesExecutionCapsuleV1 with {
    version: 2,
    executor_cohort: ProviderAuthorityWorkerCohortReferenceV1
  }

ProviderKubernetesDownExecutionCapsuleV2 =
  ProviderKubernetesDownExecutionCapsuleV1 with {
    version: 2,
    executor_cohort: ProviderAuthorityWorkerCohortReferenceV1
  }

ProviderKubernetesExecutionConfigV2 =
  ProviderKubernetesExecutionConfigV1 with {
    version: 2,
    capsule: ProviderKubernetesExecutionCapsuleV2
  }

ProviderKubernetesDownExecutionConfigV2 =
  ProviderKubernetesDownExecutionConfigV1 with {
    version: 2,
    capsule: ProviderKubernetesDownExecutionCapsuleV2
  }

ProviderLifecycleInvocationV2 = the closed launch/down union whose outer
version is 2 and whose selected launch/down member carries the corresponding
V2 execution config. ProviderLifecyclePlanV2 has the same closed commitments
as V1, uses version 2, and binds only ProviderLifecycleInvocationV2.

ProviderAuthorityPreflightRequestV2 = {
  version: 2,
  contract: "provider_kubernetes_preflight_v2",
  action_kind: "launch" | "down",
  nonce: UUID,
  seed: ProviderLaunchPreflightSeedV2 | ProviderDownPreflightSeedV2,
  expected_cohort_manifest: ProviderAuthorityWorkerCohortManifestV2,
  request_sha256: Sha256
}

ProviderLaunchAuthorityPreflightResponseV2 = {
  version: 2,
  contract: "provider_kubernetes_preflight_v2",
  action_kind: "launch",
  nonce: UUID,
  request_sha256: Sha256,
  disposition: "complete" | "not_representable",
  reason: null | ProviderLaunchNotRepresentableReasonV1,
  resolved_cohort: null | ProviderAuthorityWorkerCohortV2,
  execution_capsule: null | ProviderKubernetesExecutionCapsuleV2,
  executor_policy_proof: null | ProviderPolicyBoundaryProofV1,
  worker_identity: null | ProviderAuthorityWorkerIdentityV2
}

ProviderDownAuthorityPreflightResponseV2 = the exact kind-disjoint peer with
ProviderDownNotRepresentableReasonV1 and
ProviderKubernetesDownExecutionCapsuleV2.
```

The exact V2 seed member lists and kind-disjoint response invariants are
normative in the provider-facet design. Each V2 capsule/config/invocation/plan
above is a new closed object; naming its byte-frozen V1 leaf types does not
embed or parse a V1 capsule/config/invocation/plan node.

The live launch construction boundary is likewise additive and native:

```text
ProviderKubernetesExecutionCapsuleSeedV2 = exactly the closed
  ProviderKubernetesExecutionCapsuleV2 key set minus `objects`, with
  version=2 and the compact ProviderAuthorityWorkerCohortReferenceV1.

ProviderKubernetesRendererInputV2 = {
  version: 2,
  contract: "validated_launch_spec_v2",
  resource_identity: ProviderResourceIdentityV1,
  sky_cluster_name: Text,
  sky_cluster_record_uuid: UUID,
  name_basis: ProviderWorkloadNameBasisV1,
  seed: ProviderKubernetesExecutionCapsuleSeedV2,
  retained_source: ProviderLaunchContentSourceV1
}

ProviderKubernetesDownExecutionCapsuleInputV2 = exactly the closed
  ProviderKubernetesDownExecutionCapsuleV2 key set minus `cleanup_target` and
  `cleanup_target_sha256`, with version=2 and the compact cohort reference.
```

The seed contains exactly `version`, `implementation_contract`,
`executor_cohort`, `config_projection`, `config_projection_sha256`, `scope`,
`principals`, `prerequisites`, `request_identity`, `resources`, `renderer`,
`post_provision`, `endpoint`, `scheduling`, `storage`, `metadata`, `security`,
`topology`, and `mutation_contract`. The input has exactly the eight displayed
keys. The down input has exactly nine keys and receives its target/hash only
from the sole rederiver below. The complete
`ProviderAuthorityWorkerCohortV2` is a transient second
context argument, never another persisted seed member. Native construction in
the new `sky/serve/resource_action_renderer_v2.py` validates the compact
reference against that complete cohort, repeats every V1-equivalent
seed/capsule contextual comparison over the V2 graph, renders and validates
the three object plans, appends them to the unchanged seed, and contextually
revalidates the resulting `ProviderKubernetesExecutionCapsuleV2`. The direct
V2 down root
`construct_provider_kubernetes_down_execution_capsule_v2(down_input,
resolved_cohort, cleanup_rederivation_input)` has no renderer input. It invokes
`rederive_provider_kubernetes_cleanup_target_v2()` internally, places only
that output and hash in `ProviderKubernetesDownExecutionCapsuleV2`, and
performs complete context validation against the same output. A cleanup target
is never a direct argument to the construction root. Neither root may
instantiate, parse, convert, or call a constructor for a V1 seed, renderer
input, capsule, config, invocation, plan, or spec.

Only the complete V2 evaluator creates either input. For launch it derives the
compact reference from the resolved cohort's canonical identity, copies the
outer identity/name/source fields from the validated V2 launch preflight seed,
and fills the capsule seed from kind-specific live preflight results. For down
it fills the nine-key input from those results, constructs the exact typed
completed/partial cleanup-rederivation input from locked retained preimages,
and passes that input to the down root. Manager,
wire, fixture, and persisted-capsule callers cannot supply replacement fields;
recovery must reproduce the same canonical projection.

V2 inventory ownership is closed. The V2 cohort's
`provider_authority_v2/artifact_inventory.json` has exactly six ordered roles:
the five renderer roles plus `representability_case_inventory`; its
`config_access_inventory` role points to the native
`kubernetes_renderer_v2/config_access_inventory.json`, never the sealed V1
call graph. Byte-identical V1 outer-template, node-fragment, and admitted-
normalization leaf artifacts may be referenced because their leaf schemas do
not change. `binding_schema` instead points to
`kubernetes_renderer_v2/binding_schema.json`, which retains the exact 17
bindings but changes the hardcoded input contract from
`validated_launch_spec_v1` to `validated_launch_spec_v2`; the V1 binding schema
is not V2 evidence. The V2 cohort's separate
`provider_authority_v2/callable_inventory.json` owns the exact four handler /
strict-codec rows and four root pure entrypoints: native launch construction,
native down construction, cleanup-target rederivation, and representability
enumeration. The config-access inventory owns the complete internal call /
typed-input-access graph reachable from those four roots, including renderer
leaves, cleanup helpers, and representability dispatch; the top-level callable
inventory does not duplicate it.
Static-manifest validation, worker self-attestation, preflight, and immediate
pre-I/O validation require these V2 inventories and reject the V1
renderer/callable inventories as live M4 evidence.

No V1 capsule, config, invocation, plan, wrapper, preflight protocol, or parser
changes shape or accepted input. The complete cohort remains in the permanent
locked cohort row and may appear transiently only in the additive V2 preflight
response defined by the provider-facet design; it is never copied into a
persisted V2 invocation. The V1 preflight request/response remains
Serve034-retirement-only and cannot prepare live M4 work. Constructing or
parsing a compact reference grants no authority. Every admission,
materialization, claim, provider-context, immediate pre-I/O, recovery, and
reduction boundary locks and parses the named permanent row as
`ProviderAuthorityWorkerCohortV2`, then calls the authority module's sole typed
`validate_locked_action_spec_cohort_v2()` resolver. That resolver recomputes
the complete cohort's canonical identity hash and requires exact ID/hash
equality. The structural action module exposes no caller-asserted lock/version
scalar API and accepts no V1 cohort as live evidence. A typed V2 value that has
not passed that resolver is only a structural value and cannot authorize
persistence, a request, or provider I/O. The additive V2 preflight wire/parser
and bounded envelope goldens are implemented structural inputs. Native V2
construction, the V2 inventories and representability case inventory/CI
goldens, and every
locked runtime use described above remain activation gates, so M4 authority
stays disabled.

Every V2 payload member is immutable and covered by the complete action-spec
hash. The identity hash must recompute from the embedded identity, and the
provider plan and invocation must retain their existing byte-equality,
action-ID, target, and action-kind invariants. A represented private-shadow
parent uses only `ShadowCandidateActionBindingV1`; its tuple equals the locked
service candidate and coverage tuple while the cohort reference remains
`SHADOW_ACTIVE` with a null authority-policy triple. An authoritative action
  uses only `AuthoritativeActionPolicyBindingV1`; at action admission its tuple
  equals the locked `ACTIVE/OPEN` policy row and the nonnull `ACTION_ACTIVE`
  reference tuple. Later materialization, claim, provider-context, pre-I/O, and
  recovery reads require that same frozen tuple under locked
  `ACTIVE/(OPEN | DRAINING)` current execution. `CLOSED | SUPERSEDED` is legal
  only for historical validation and reduction, never current execution. A
  binding-kind mismatch is corruption, not a route fallback.

`ServeReplicaActionSpecV1` and its exact
`ServeReplicaActionSpecV1.from_value()` parser remain byte-frozen for
pre-Serve038 Serve033 history and exact-034 cleanup-only tooling. The sole live
M4 parser is named `serve_replica_action_spec_from_value_v2()`. Manager admission,
private request materialization, claim resolution, provider-context loading,
pre-I/O reauthorization, recovery, and reduction each invoke that V2-only
parser and reject version 1 before creating or advancing a reference, request,
attempt, watermark, or provider effect. A version-dispatching inspection
reader, if one is needed, grants no execution authority. Repository inventory
tests fail if a live boundary calls the V1 or inspection parser. Serve038's
empty-action/shadow migration precondition means no live V1 action is upgraded,
rehydrated as V2, or accepted through a compatibility fallback.

One pure projector reads the immutable nonnull `version_specs.yaml_content`,
rejects duplicate YAML keys, parses it through the same `SkyServiceSpec` and
`Task` constructors used by Serve, materializes defaults, and hashes canonical
JSON from `SkyServiceSpec.to_yaml_config()` and
`Task.to_yaml_config(use_user_specified_yaml=False)`. That Task method retains
the constructor-only `_user_specified_yaml` provenance key even on its
effective projection. The projector requires that value to equal the exact
immutable YAML input, removes exactly that one key before canonical hashing,
and rejects `_metadata` or any other internal/provenance key at schema-owned
top-level and resource-option positions. Underscore-prefixed keys inside
user-owned maps such as environment or labels remain effective input and are
never silently removed. It then constructs the closed identity above. The pure
projection is not persistence authority: the service-locked identity writer
must first hold the complete M4 source/secret/representability proof described
by the provider facet. Declared secret carriers are rejected by the projector
itself, and an arbitrary command, environment, or other source that has not
passed that complete proof cannot write even a hash to the identity columns.
Thus only hashes, never YAML, secret values, or hashes of unapproved
secret-bearing input, enter durable identity. Omitted and explicitly spelled
defaults therefore have equal effective subhashes. The envelope intentionally
remains distinct across service incarnation or version. The exact retained
YAML bytes are bound separately by
`ProviderLaunchContentSourceV1.yaml_content_sha256`; every launch action binds
both objects and requires their service/incarnation/version tuple to agree.

Serve038 adds pair-null `version_specs.resource_action_spec_identity JSONB` and
`resource_action_spec_identity_sha256 TEXT`, plus nullable
`replicas.resource_action_spec_identity_sha256 TEXT`. New M4-eligible version
commits write the identity pair in the same service-locked transaction as the
immutable YAML and elected `services.current_version`. Private activation may
initialize a null pair for an existing immutable version only by recomputing it
from that row and exact-adopting byte equality after a lost acknowledgement;
neither field may later change. Every live V2 action immutable spec embeds the
complete identity and hash, and its replica row stores that same hash. A down action and
its prior-launch basis retain the launch version identity even when a newer
version is elected.

Activation and pre-promotion reset lock the service, the elected version, every
version named by a live replica, and those replica rows in the global order.
Admission locks the service, its elected version, and the target replica; it
either binds the old elected identity or observes the completed update and
binds the new one. Live replicas from several immutable versions are legal only
when every one independently projects the same closed capacity/provider
profile. In private shadow, any elected identity change closes admission and
requires a new candidate epoch. After authority, a same-profile update uses a
server-minted `AuthoritativeServiceVersionAdmissionProofV1` and does not change
the binary authority policy; an update that changes the profile is rejected
before the version/election commit. This makes an update/admission race one of
two complete version bindings, never an unlocked current-spec assertion.

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
  resource_action_spec_identity_sha256 TEXT nullable

version_specs
  resource_action_spec_identity        JSON nullable
  resource_action_spec_identity_sha256 TEXT nullable

serve_resource_action_worker_cohorts  # additive Serve038 ALTER; not Serve033/034
  removal_authorized_at                TIMESTAMPTZ nullable
  replace shipped lifecycle timestamp CHECK with:
    check (state_changed_at >= created_at and
           ((lifecycle_state in ('REGISTERING', 'ACCEPTING', 'DRAINING') and
            (jsonb_typeof(registration_attestations) = 'object') is true and
            (((registration_attestations -> 'version')::text = '2') IS TRUE) and
           removal_authorized_at is null and retired_at is null) or
           (lifecycle_state = 'REMOVAL_AUTHORIZED' and
            (jsonb_typeof(registration_attestations) = 'object') is true and
            (((registration_attestations -> 'version')::text = '2') IS TRUE) and
           removal_authorized_at is not null and
            state_changed_at = removal_authorized_at and retired_at is null) or
           (lifecycle_state = 'RETIRED' and
            (jsonb_typeof(registration_attestations) = 'object') is true and
            (((registration_attestations -> 'version')::text = '2') IS TRUE) and
            removal_authorized_at is not null and retired_at is not null and
            state_changed_at = retired_at and
            retired_at >= removal_authorized_at) or
           (lifecycle_state = 'RETIRED' and
            (jsonb_typeof(registration_attestations) = 'object') is true and
            (((registration_attestations -> 'version')::text = '1') IS TRUE) and
            removal_authorized_at is null and retired_at is not null and
           state_changed_at = retired_at)))
```

The sole null-removal-authorization V1 grandfather clause accepts only the exact
JSON numeric token `1`, requires nonnull `retired_at == state_changed_at`, and
keeps `removal_authorized_at` null.
The text-cast plus `IS TRUE` is intentional: missing, JSON null, string
`"1"`, and numeric `1.0` all fail the CHECK instead of being normalized by
JSONB numeric equality or escaping through SQL NULL.
After Serve038, every nonterminal cohort shape similarly requires the exact
numeric V2 token `2`; missing/null/string/`2.0` and every V1 nonterminal write
fail physically. V1 is legal only as exact already-`RETIRED` history with null
removal authorization and truthful nonnull retirement time;
all V1 retirement transitions complete against the frozen Serve034 action
contract at the exact current Serve037 head before migration.
The nonnull-time terminal branches admit only exact numeric V2; version `1` or
`3`, missing/null/string forms, and numeric `1.0`/`2.0` fail every such shape.

The mode enum is `legacy | shadow | authoritative`. `legacy` may retain a null
mode timestamp; each explicit transition writes a fresh PostgreSQL clock value.
The initial program permits only `legacy -> shadow -> authoritative`. A typed
transition validates canonical UUID `services.hash`, the current owner, image
inventory, and milestone gates; table defaults or a bare SQL update do not
activate behavior.

M4 adds PostgreSQL Serve revision 038 after the exact current Serve037 head.
Serve035's multi-pool reserved-fill state, Serve036's version-controller
configuration columns, and Serve037's placement-normalization/retirement ledger
tables, service receipts, version-retirement columns, PostgreSQL checks, and
foreign keys are unrelated predecessors and must be preserved byte-for-byte;
the shipped Serve033 action relations and Serve034 release ledger likewise must
not be rewritten. Revision 038 adds the durable qualification-window
binding, crash intent, and monotonic authority-policy history that the dark
foundation lacks. Hash columns use `TEXT` plus lowercase-hex checks, matching
the shipped API005--006 and Serve033 conventions:

```text
services
  resource_action_candidate_epoch          UUID nullable
  resource_action_candidate_policy_sha256  TEXT nullable
  resource_action_candidate_binding_sha256 TEXT nullable

serve_resource_action_shadow_coverage
  candidate_epoch                           UUID not null
  qualification_policy_sha256               TEXT not null
  qualification_binding_sha256              TEXT not null

serve_resource_action_worker_cohort_refs
  authority_policy_epoch                    UUID nullable
  authority_policy_sha256                   TEXT nullable
  authority_binding_sha256                  TEXT nullable
  foreign key (service_hash, authority_policy_epoch,
               authority_policy_sha256, authority_binding_sha256) references
    serve_resource_action_authority_policy_epochs(
      service_hash, policy_epoch, policy_sha256, authority_binding_sha256)
    on delete restrict
  check (((reference_state in ('PREPARING', 'SHADOW_ACTIVE')) and
          authority_policy_epoch is null and
          authority_policy_sha256 is null and
          authority_binding_sha256 is null) or
         (reference_state = 'ACTION_ACTIVE' and
          authority_policy_epoch is not null and
          authority_policy_sha256 is not null and
          authority_binding_sha256 is not null) or
         (reference_state = 'RELEASED' and
          ((authority_policy_epoch is null and
            authority_policy_sha256 is null and
            authority_binding_sha256 is null) or
           (authority_policy_epoch is not null and
            authority_policy_sha256 is not null and
            authority_binding_sha256 is not null))))

serve_resource_action_authority_policy_epochs
  service_hash                              TEXT not null
  policy_epoch                              UUID not null
  predecessor_policy_epoch                  UUID nullable
  policy                                    JSONB not null
  policy_sha256                             TEXT not null
  authority_binding_sha256                  TEXT not null
  rotation_proof                            JSONB not null
  rotation_proof_sha256                     TEXT not null
  nonterminal_inventory                     JSONB not null
  nonterminal_inventory_sha256              TEXT not null
  reason                                    TEXT not null
                                              # INITIAL_PROMOTION |
                                              # COMPATIBLE_IMAGE_ROTATION |
                                              # ROLLBACK_EVIDENCE_CLOSURE |
                                              # SCHEMA_HEAD_ADVANCE
  policy_state                              TEXT not null
                                              # ACTIVE | SUPERSEDED
  admission_state                           TEXT not null
                                              # CLOSED | DRAINING | OPEN
  admission_revision                        BIGINT not null
  last_operation_id                         UUID not null
  last_operation_kind                       TEXT not null
                                              # ACTIVATE | ACTIVATE_CLOSED |
                                              # DRAIN | CLOSE | REOPEN |
                                              # SUPERSEDE
  created_at                                TIMESTAMPTZ not null
  admission_changed_at                      TIMESTAMPTZ not null
  activated_at                              TIMESTAMPTZ nullable
  superseded_at                             TIMESTAMPTZ nullable
  primary key (service_hash, policy_epoch)
  unique (service_hash, policy_epoch, policy_sha256,
          authority_binding_sha256)
  foreign key (service_hash, predecessor_policy_epoch) references
    serve_resource_action_authority_policy_epochs(service_hash, policy_epoch)
  unique partial (service_hash, predecessor_policy_epoch)
    where predecessor_policy_epoch is not null
  unique partial (service_hash) where predecessor_policy_epoch is null
  unique partial (service_hash) where policy_state = 'ACTIVE'
  check (predecessor_policy_epoch is null or
         predecessor_policy_epoch <> policy_epoch)
  check ((reason = 'INITIAL_PROMOTION' and
          predecessor_policy_epoch is null) or
         (reason in ('COMPATIBLE_IMAGE_ROTATION',
                     'ROLLBACK_EVIDENCE_CLOSURE',
                     'SCHEMA_HEAD_ADVANCE') and
          predecessor_policy_epoch is not null))
  check ((reason = 'ROLLBACK_EVIDENCE_CLOSURE' and
          policy->>'serve_head' = '039' and
          admission_state = 'CLOSED' and
          ((policy_state = 'ACTIVE' and admission_revision = 1 and
            last_operation_kind = 'ACTIVATE_CLOSED') or
           (policy_state = 'SUPERSEDED' and admission_revision = 2 and
            last_operation_kind = 'SUPERSEDE'))) or
         (reason <> 'ROLLBACK_EVIDENCE_CLOSURE'))
  check ((reason = 'SCHEMA_HEAD_ADVANCE' and
          policy->>'serve_head' = '040' and
          (admission_revision > 1 or
           (policy_state = 'ACTIVE' and admission_state = 'CLOSED' and
            admission_revision = 1 and
            last_operation_kind = 'ACTIVATE_CLOSED'))) or
         (reason = 'ROLLBACK_EVIDENCE_CLOSURE' and
          policy->>'serve_head' = '039') or
         (reason in ('INITIAL_PROMOTION', 'COMPATIBLE_IMAGE_ROTATION') and
          last_operation_kind <> 'ACTIVATE_CLOSED'))
  check (admission_revision > 0 and
         ((policy_state = 'ACTIVE' and admission_state = 'OPEN' and
           ((admission_revision = 1 and
             last_operation_kind = 'ACTIVATE') or
            (admission_revision > 1 and
             last_operation_kind = 'REOPEN'))) or
          (policy_state = 'ACTIVE' and admission_state = 'DRAINING' and
           admission_revision > 1 and last_operation_kind = 'DRAIN') or
          (policy_state = 'ACTIVE' and admission_state = 'CLOSED' and
           ((admission_revision = 1 and
             last_operation_kind = 'ACTIVATE_CLOSED') or
            (admission_revision > 1 and
             last_operation_kind = 'CLOSE'))) or
          (policy_state = 'SUPERSEDED' and admission_state = 'CLOSED' and
           admission_revision > 1 and last_operation_kind = 'SUPERSEDE')))
  check ((policy_state = 'ACTIVE' and
          admission_state in ('OPEN', 'DRAINING', 'CLOSED') and
          activated_at is not null and activated_at = created_at and
          admission_changed_at >= activated_at and superseded_at is null) or
         (policy_state = 'SUPERSEDED' and admission_state = 'CLOSED' and
          activated_at is not null and activated_at = created_at and
          admission_changed_at >= activated_at and
          superseded_at is not null and
          superseded_at >= admission_changed_at))

serve_resource_action_worker_registration_leases
  cohort_id                                TEXT not null references
                                              serve_resource_action_worker_cohorts(cohort_id)
  worker_instance_id                      UUID not null
  pod_uid                                 UUID not null
  generation                              BIGINT not null
  state                                   TEXT not null
                                              # ACTIVE | REVOKED
  renewal_registration                    JSONB not null
  renewal_registration_sha256             TEXT not null
  execution_owner                         JSONB nullable
  execution_owner_sha256                  TEXT nullable
  execution_owner_api_instance_id         UUID nullable
  renewed_at                              TIMESTAMPTZ not null
  expires_at                              TIMESTAMPTZ not null
  revoked_at                              TIMESTAMPTZ nullable
  revocation_reason                       TEXT nullable
                                              # STALE_HANDOFF |
                                              # CANDIDATE_ABANDONED |
                                              # COHORT_COLD_RECOVERY |
                                              # COHORT_REMOVAL
  revocation_owner_id                     UUID nullable
  last_operation_id                       UUID not null
  last_operation_kind                     TEXT not null
                                              # INSERT | RENEW | REVOKE |
                                              # BIND_EXECUTION_OWNER |
                                              # SUPERSEDE_EXECUTION_OWNER
  revision                                BIGINT not null
  primary key (cohort_id, worker_instance_id)
  unique (cohort_id, pod_uid)
  check (worker_instance_id = pod_uid and generation > 0 and revision > 0 and
         expires_at = renewed_at + interval '60 seconds' and
         ((execution_owner is null and execution_owner_sha256 is null and
           execution_owner_api_instance_id is null) or
          (execution_owner is not null and
           execution_owner_sha256 is not null and
           execution_owner_api_instance_id is not null and
           case when jsonb_typeof(execution_owner) = 'object' then
             execution_owner_api_instance_id::text =
                 execution_owner ->> 'api_instance_id' and
             worker_instance_id::text =
                 execution_owner ->> 'authority_worker_instance_id' and
             pod_uid::text = execution_owner ->> 'pod_uid' and
             execution_owner_api_instance_id <> worker_instance_id
           else false end is true)) and
         (last_operation_kind not in
              ('BIND_EXECUTION_OWNER', 'SUPERSEDE_EXECUTION_OWNER') or
          execution_owner is not null) and
         ((state = 'ACTIVE' and revision = generation and revoked_at is null and
          revocation_reason is null and revocation_owner_id is null and
          ((generation = 1 and last_operation_kind = 'INSERT') or
           (generation > 1 and last_operation_kind in
               ('RENEW', 'BIND_EXECUTION_OWNER',
                'SUPERSEDE_EXECUTION_OWNER')))) or
         (state = 'REVOKED' and revision = generation + 1 and
          revoked_at >= renewed_at and
          revoked_at is not null and revocation_reason is not null and
          last_operation_kind = 'REVOKE' and
          ((revocation_reason = 'COHORT_REMOVAL' and
            revocation_owner_id is null) or
           (revocation_reason in ('STALE_HANDOFF', 'CANDIDATE_ABANDONED',
                                  'COHORT_COLD_RECOVERY') and
            revocation_owner_id is not null)))))
  index (cohort_id, expires_at)
    where state = 'ACTIVE'
  unique partial (execution_owner_api_instance_id)
    where execution_owner_api_instance_id is not null

serve_resource_action_worker_registration_handoffs
  cohort_id                                TEXT not null references
                                              serve_resource_action_worker_cohorts(cohort_id)
  handoff_id                               UUID not null
  predecessor_handoff_id                   UUID nullable
  chain_sequence                           BIGINT not null
  stale_fence_disposition                  TEXT not null
                                              # NEWLY_REVOKED |
                                              # ADOPTED_ABANDONED_PREDECESSOR
  source_cohort_revision                   BIGINT not null
  source_cohort_state                      TEXT not null
                                              # ACCEPTING | DRAINING
  source_registration_set_revision         BIGINT not null
  source_registration_set                  JSONB not null
  source_registration_set_sha256           TEXT not null
  stale_worker_instance_id                 UUID not null
  stale_pod_name                           TEXT not null
  stale_pod_uid                            UUID not null
  survivor_worker_instance_id              UUID not null
  survivor_pod_uid                         UUID not null
  candidate_worker_instance_id             UUID not null
  candidate_pod_name                       TEXT not null
  candidate_pod_uid                        UUID not null
  stale_authority_fence                    JSONB not null
  stale_authority_fence_sha256             TEXT not null
  stale_uid_absence_proof                  JSONB not null
  stale_uid_absence_proof_sha256           TEXT not null
  candidate_registration                   JSONB not null
  candidate_registration_sha256            TEXT not null
  survivor_registration                    JSONB nullable
  survivor_registration_sha256             TEXT nullable
  handoff_state                            TEXT not null
                                              # OPEN | READY | COMPLETED |
                                              # ABANDONED
  final_registration_set                   JSONB nullable
  final_registration_set_sha256            TEXT nullable
  final_registration_set_revision          BIGINT nullable
  final_deployment_snapshot                JSONB nullable
  final_deployment_snapshot_sha256         TEXT nullable
  committed_cohort_revision                BIGINT nullable
  candidate_absence_proof                  JSONB nullable
  candidate_absence_proof_sha256           TEXT nullable
  survivor_absence_proof                   JSONB nullable
  survivor_absence_proof_sha256             TEXT nullable
  candidate_zero_effect_proof              JSONB nullable
  candidate_zero_effect_proof_sha256       TEXT nullable
  abandonment_reason                       TEXT nullable
  revision                                 BIGINT not null
  opened_at                                TIMESTAMPTZ not null
  fenced_at                                TIMESTAMPTZ not null
  survivor_acknowledged_at                 TIMESTAMPTZ nullable
  terminal_at                              TIMESTAMPTZ nullable
  primary key (cohort_id, handoff_id)
  foreign key (cohort_id, predecessor_handoff_id) references
    serve_resource_action_worker_registration_handoffs(cohort_id, handoff_id)
  check (predecessor_handoff_id is null or
         predecessor_handoff_id != handoff_id)
  check ((stale_fence_disposition = 'NEWLY_REVOKED' and
          predecessor_handoff_id is null and chain_sequence = 1) or
         (stale_fence_disposition = 'ADOPTED_ABANDONED_PREDECESSOR' and
          predecessor_handoff_id is not null and chain_sequence > 1))
  unique partial (cohort_id, predecessor_handoff_id)
    where predecessor_handoff_id is not null
  unique (cohort_id, source_cohort_revision, chain_sequence)
  unique partial (cohort_id)
    where handoff_state in ('OPEN', 'READY')
  unique (cohort_id, candidate_pod_uid)

serve_resource_action_worker_registration_cold_recoveries
  cohort_id                                TEXT not null references
                                              serve_resource_action_worker_cohorts(cohort_id)
  recovery_id                             UUID not null
  source_cohort_revision                  BIGINT not null
  source_cohort_state                     TEXT not null
                                              # ACCEPTING | DRAINING
  source_registration_set_revision        BIGINT not null
  source_registration_set                 JSONB not null
  source_registration_set_sha256          TEXT not null
  old_uid_absence_proofs                  JSONB not null
  old_uid_absence_proofs_sha256           TEXT not null
  old_authority_fences                    JSONB not null
  old_authority_fences_sha256             TEXT not null
  final_registration_set                  JSONB not null
  final_registration_set_sha256           TEXT not null
  final_registration_set_revision         BIGINT not null
  final_deployment_snapshot               JSONB not null
  final_deployment_snapshot_sha256        TEXT not null
  committed_cohort_revision               BIGINT not null
  completed_at                            TIMESTAMPTZ not null
  primary key (cohort_id, recovery_id)
  unique (cohort_id, source_cohort_revision)

serve_resource_action_worker_process_supersessions
  cohort_id                                TEXT not null references
                                              serve_resource_action_worker_cohorts(cohort_id)
  supersession_id                         UUID not null
  authority_worker_instance_id            UUID not null
  operation_id                            UUID not null
  source_lease_generation                 BIGINT not null
  source_lease_revision                   BIGINT not null
  committed_lease_generation              BIGINT not null
  committed_lease_revision                BIGINT not null
  prior_api_instance_id                   UUID not null
  current_api_instance_id                 UUID not null
  prior_execution_owner                   JSONB not null
  prior_execution_owner_sha256            TEXT not null
  current_execution_owner                 JSONB not null
  current_execution_owner_sha256          TEXT not null
  container_supersession_proof            JSONB not null
  container_supersession_proof_sha256     TEXT not null
  request_claims                          JSONB not null
  request_claims_sha256                   TEXT not null
  completed_at                            TIMESTAMPTZ not null
  primary key (cohort_id, supersession_id)
  unique (cohort_id, operation_id)
  unique (prior_api_instance_id)
  unique (current_api_instance_id)
  check (supersession_id = operation_id and
         source_lease_generation > 0 and source_lease_revision > 0 and
         source_lease_generation = source_lease_revision and
         committed_lease_generation = source_lease_generation + 1 and
         committed_lease_revision = source_lease_revision + 1 and
         prior_api_instance_id <> current_api_instance_id and
         prior_api_instance_id <> authority_worker_instance_id and
         current_api_instance_id <> authority_worker_instance_id and
         jsonb_typeof(prior_execution_owner) = 'object' and
         jsonb_typeof(current_execution_owner) = 'object' and
         jsonb_typeof(container_supersession_proof) = 'object' and
         jsonb_typeof(request_claims) = 'array' and
         jsonb_array_length(request_claims) <= 16 and
         case when jsonb_typeof(prior_execution_owner) = 'object' and
                        jsonb_typeof(current_execution_owner) = 'object' and
                        jsonb_typeof(container_supersession_proof) = 'object'
              then prior_api_instance_id::text =
                       prior_execution_owner ->> 'api_instance_id' and
                   current_api_instance_id::text =
                       current_execution_owner ->> 'api_instance_id' and
                   authority_worker_instance_id::text =
                       prior_execution_owner ->> 'authority_worker_instance_id' and
                   authority_worker_instance_id::text =
                       current_execution_owner ->> 'authority_worker_instance_id' and
                   authority_worker_instance_id::text =
                       prior_execution_owner ->> 'pod_uid' and
                   authority_worker_instance_id::text =
                       current_execution_owner ->> 'pod_uid' and
                   authority_worker_instance_id::text =
                       container_supersession_proof ->> 'authority_worker_instance_id' and
                   prior_api_instance_id::text =
                       container_supersession_proof ->> 'prior_api_instance_id' and
                   current_api_instance_id::text =
                       container_supersession_proof ->> 'current_api_instance_id'
              else false end is true)
  index (cohort_id, authority_worker_instance_id, completed_at)

serve_resource_action_crash_canary_runs
  service_name                              TEXT not null
  service_hash                              TEXT not null
  service_incarnation                       UUID not null
  candidate_epoch                           UUID not null
  boundary_id                               TEXT not null
  run_id                                    UUID not null
  subject_kind                              TEXT not null
                                              # service | action | request
  action_kind                               TEXT not null
  action_id                                 UUID nullable
  attempt                                   INTEGER nullable
  request_id                                TEXT nullable
  qualification_policy_sha256               TEXT not null
  qualification_binding_sha256              TEXT not null
  injection_nonce_sha256                    TEXT not null
  run_state                                 TEXT not null
                                              # STARTED | COMPLETED
  injection_receipt                         JSONB nullable
  injection_receipt_sha256                  TEXT nullable
  verification_evidence                     JSONB nullable
  verification_evidence_sha256              TEXT nullable
  outcome                                   TEXT nullable
                                              # PASS | FAIL | ABANDONED
  revision                                  BIGINT not null
  started_at                                TIMESTAMPTZ not null
  completed_at                              TIMESTAMPTZ nullable
  primary key (service_hash, candidate_epoch, boundary_id, run_id)
  unique (run_id)
  unique partial (service_hash, candidate_epoch, boundary_id)
    where run_state = 'STARTED'

serve_resource_action_attempt_exhaustions
  action_id                                 UUID not null
  event_code                                TEXT not null
  attempt                                   INTEGER not null
  request_id                                TEXT not null
  service_name                              TEXT not null
  service_hash                              TEXT not null
  service_incarnation                       UUID not null
  replica_id                                BIGINT not null
  replica_incarnation                       UUID not null
  desired_generation                        BIGINT not null
  action_type                               TEXT not null
  reduction_basis                           TEXT not null
  request_input_sha256                      TEXT not null
  typed_outcome_sha256                      TEXT not null
  result_sha256                             TEXT not null
  settled_action_revision                   BIGINT not null
  occurred_at                               TIMESTAMPTZ not null
  primary key (action_id, event_code)
  unique (request_id, event_code)
```

The lease rendering above is the final Serve039 catalog shape. At Serve038 the
three execution-owner columns and their partial unique index do not exist;
`last_operation_kind` is only `INSERT | RENEW | REVOKE`, and the named
`serve038_worker_lease_closed_shape_ck` contains the remaining state/counter/
registration/TTL/revocation clauses with no owner reference. Serve039 adds the
columns/index and replaces that one named CHECK atomically with the full
displayed owner-aware expression. No Serve038 metadata factory may mention a
Serve039 column or operation.

Authority policy epochs are opaque UUID identities end to end. Python closed
values use `uuid.UUID`; canonical JSON renders their lowercase hyphenated UUID
text; PostgreSQL binds native `UUID`; and action bindings, references, claim
predicates, dispatch proofs, promotion, rotation, and exact-adoption reads all
compare that same UUID. An integer, numeric string, sequence-derived value, or
noncanonical UUID spelling is invalid. `admission_revision` orders state
changes inside one policy row and `predecessor_policy_epoch` orders the
immutable policy chain; neither may be substituted for the UUID epoch. The
initial root policy UUID equals the qualified candidate UUID, while every
compatible rotation mints a fresh successor UUID.

`serve038_worker_state_check_constraints()` is the single SQLAlchemy owner of
the Serve038 worker-table CHECKs; migration and fresh 038 metadata call the same
factory. The separate
`serve039_worker_lease_execution_owner_check_constraint()` owns the one final-
head replacement lease expression and is used by Serve039 migration, fresh-039
bootstrap metadata, and its post-migration inspector. Every other Serve038
worker CHECK remains byte-stable. Both inspectors compare every named,
normalized expression. Parse-tree normalization resolves each table-local PostgreSQL
`Var` attribute number to its exact column name before comparison, because a
genuine sequential historical catalog and a temporary expected table may have
different physical column order. It preserves the rest of the parsed tree,
including operators, casts, types, collation, constants, boolean structure,
constraint validation state, and constraint name; column-order independence
is not permission for a semantically different CHECK. It emits:

- `serve038_worker_lease_closed_shape_ck`, the pre-owner two-state/counter /
  `INSERT | RENEW | REVOKE`/TTL/revocation-time expression plus renewal-
  registration JSON-object, at-most-65,536-byte stored rendering, and lowercase-
  SHA-256 checks; Serve039 drops and replaces only this name with
  `serve039_worker_lease_execution_owner_ck`, which preserves every old clause
  and adds the triple-null/non-null owner shape plus BIND/SUPERSEDE operations;
- `serve038_worker_handoff_scalar_lineage_ck`, covering closed enums, positive
  revisions, worker/Pod-ID equalities and distinctness, equal source cohort/set
  revisions, `opened_at=fenced_at`, and the disposition/predecessor/sequence
  shape;
- `serve038_worker_handoff_pairing_state_ck`, covering every optional JSON/hash
  pair, JSON root/array type, DTO-specific stored-size bound, lowercase hash,
  and the exact `OPEN` revision-one, `READY` revision-two, `COMPLETED`
  revision-three, and two legal `ABANDONED` nullable-field shapes;
- `serve038_worker_handoff_terminal_revision_ck`, covering completed final/
  embedded/committed revision = source + 1, final snapshot pairing, conditional
  survivor-absence evidence, and terminal/ack timestamp nullability; and
- `serve038_worker_cold_required_json_ck` and
  `serve038_worker_cold_revision_shape_ck`, covering closed source state,
  positive/equal source revisions, required JSON-object/array and lowercase-
  hash shapes, exact-two absence/fence/final-worker arrays, final/embedded/
  committed revision = source + 1, and stored-rendering envelope bounds.

Every JSON predicate is a two-valued `CASE`/`IS TRUE` expression so a missing,
wrong-type, or JSON-null field fails rather than satisfying a PostgreSQL CHECK
through SQL NULL. SQL enforces all row-local shape, pairing, enum, counter,
timestamp, and direct revision relations; typed codecs additionally recompute
canonical hashes, maximal-byte bounds, worker/proof alignment, and cross-row
lease/handoff/cohort equality under locks. Neither layer substitutes for the
other.

The registration lease is liveness only, never a second execution lease or
work queue. Its `worker_instance_id` and `pod_uid` are byte-equal canonical
UUIDs and form the stable Pod membership identity. The distinct V2 execution
owner binds a fresh process API-instance UUID plus exact container incarnation;
that process UUID, not the Pod UUID, owns generic request claims. Insert is exactly
generation/revision one. `ACTIVE` has null revocation fields, a hash-valid fresh
self-read V2 registration whose
`registered_at == renewed_at` and whose worker has
`observed_at <= renewed_at`, with
`expires_at == renewed_at + 60 seconds`. A normal renewal's one operation time
is `GREATEST(clock_timestamp(), source.renewed_at,
new_renewal_registration.worker.observed_at)`; renewal runs every 20 seconds and
advances generation and revision by exactly one while replacing only times and
registration/hash and preserving the execution-owner/hash/normalized-process-
scalar triple, using exact compare-and-swap against one fresh PostgreSQL
clock. Both intervals are server-owned and not configurable. Revocation
preserves generation, renewal registration/hash/times, advances revision by
exactly one, writes database `revoked_at >= renewed_at`, and fills one closed
reason. `STALE_HANDOFF | CANDIDATE_ABANDONED` stores the exact handoff ID as
`revocation_owner_id`, `COHORT_COLD_RECOVERY` stores the recovery ID, and
`COHORT_REMOVAL` requires a null `revocation_owner_id`; every revocation
preserves the separate execution-owner/hash/normalized-process-scalar triple
byte-for-byte. `REVOKED` is terminal for that cohort/
instance. The closed SQL and typed row invariant is: `ACTIVE` means
`revision == generation`, all revocation fields are null, and operation kind is
`INSERT` exactly at generation one or one of `RENEW |
BIND_EXECUTION_OWNER | SUPERSEDE_EXECUTION_OWNER` thereafter; only the two
retained owner protocols may use their named operations. `REVOKED` means
`revision == generation + 1`, operation kind `REVOKE`, nonnull revoke time and
reason, and the reason-specific owner shape above. Reads reject every malformed
combination rather than normalizing it. For `STALE_HANDOFF |
CANDIDATE_ABANDONED`, the typed read resolves `(cohort_id,
revocation_owner_id)` to the exact immutable handoff; for
`COHORT_COLD_RECOVERY` it resolves the same pair to the exact immutable cold-
recovery row. Neither owner row may be deleted while a lease names it. An unknown insert, renewal, or revocation result adopts only exact
expected `last_operation_id/kind`, state, generation, revision, registration/
hash, execution-owner/hash/normalized-process-scalar triple, revocation fields,
and server-timestamp
relationships. The caller mints
the operation UUID before the first attempt and reuses it; it never guesses the
database clock. A later valid renewal may supersede an unknown renewal but is
reported as such rather than pretending to adopt the old operation; insert is
then proved by ancestry, while revocation is terminal. Renewal rechecks one of the exact
membership/handoff authorizers above. Existence or freshness without byte-equal
accepted V2 membership, exact-current lease execution owner, and a fresh
matching process API server-instance lease grants no claim,
preparation, preflight, or provider effect.

Every joined lease insertion/revocation operation uses one normative operation
time: the `GREATEST` of PostgreSQL time, every affected prior lease
`renewed_at`, every inserted or renewed registration worker `observed_at`, the
locked prior cohort `state_changed_at` when cohort state changes, every affected
`private_request_terminal_lower_bound` when requests terminalize, and the
current owner/proof/API start-observation terms when ownership changes. That one value is reused for all inserted
registration/renewal times, fence or recovery times, revocation times, and the
cohort transition time in the operation. For `OPEN` handoff and full-set cold
recovery, global lock order requires class-4 handoff or class-3 cold-recovery
evidence before class-5 leases, so
the proposed value is derived from the complete nonlocking lease snapshots;
after class-5 acquisition every contributing lease and proposed registration/
request byte, generation, revision, and timestamp term must match exactly or the
transaction rolls back the earlier evidence.
This is the only permitted exception to computing the value after lease locks,
and it preserves `revoked_at >= renewed_at` even when the wall clock moves
backward.

Frozen V1 queued fences are audit-only pre-039 history: they retain their exact
64-claim, 24,576-list-byte, 30,720-fence-byte, 65,536-cold-array-byte bounds and
`LIMIT 65` reader negatives, but cannot authorize a live mutation. Every
Serve039 handoff/cold recovery uses the V2 terminal-fence union and exact
constants: at most 16 action/shadow claims, the same three byte ceilings, and
max-plus-one `LIMIT 17`. Generated maximal action-only, shadow-only, and mixed
fixtures prove all enclosures; typed readers reject any overflow. Discovery
constructs the complete arm-specific receipt candidate, and the locked requery
reconstructs every byte before the all-requests/all-queues/lineage/action-
selector/shadow-history/event batch. No truncation, pagination, hash-only
substitute, scalar terminalizer loop, or same-request requeue is legal. The
API-instance-serialized 16-claim admission cap makes overflow unreachable from
valid live state; an observed live overflow is corruption and blocks without
discarding a claim.

The handoff row is a retained state machine, not an overwriteable audit blob.
All JSON/hash pairs use canonical bounded V2 constructors and lowercase
SHA-256 checks. Source, identities, fence, stale-UID absence, and candidate-
registration fields are immutable after insert; candidate/survivor terminal
absence fields are immutable after their one write. `OPEN` is revision one with the survivor,
final, abandonment, and terminal fields null. `OPEN -> READY` is the sole
revision-two transition and write-once fills the survivor pair and
`survivor_acknowledged_at`. The submitted acknowledgement names the exact
handoff ID/sequence and is accepted only by that later `OPEN` CAS after a fresh
live read; its timestamp fields are bounded against the READY transaction clock
but are not compared to an earlier wall-clock value. `READY -> COMPLETED`
is revision three and write-once
fills the final V2 set/snapshot pairs, consecutive committed cohort revision,
and terminal time. `OPEN -> ABANDONED` is exactly revision two and `READY ->
ABANDONED` exactly revision three; each instead write-once fills the
candidate-absence and zero-effect pairs, bounded reason, and terminal time; all
final-set fields stay null. Pairing, timestamps, transition source revision,
membership, and state shapes have SQL CHECKs plus typed transition validation.
Terminal rows are immutable. The nonterminal partial unique index prevents two
replacement protocols from racing within a cohort, while a later replacement
may retain a distinct terminal row. An unknown result at any transition reads
by cohort/handoff and candidate UID and adopts only the exact expected
revision, state, canonical fields, and hashes; otherwise it fails closed.
The source-set revision equals its embedded revision and the locked cohort
revision; `source_cohort_state` is the locked `ACCEPTING | DRAINING` state and
cannot change while the handoff is nonterminal. Its two workers are exactly the
named stale and survivor. Each named
worker instance equals its Pod UID. The stale fence stores the preserved lease
generation, prior ACTIVE revision, exact post-revoke revision equal to prior +
one, and every request/queue claim fenced under its database timestamp. The fence
stores `origin_revoking_handoff_id`. For `NEWLY_REVOKED`, the lease has
`revocation_reason=STALE_HANDOFF`, `revocation_owner_id` and origin both equal
the current handoff, and `revoked_at=fenced_at`. An adopting handoff copies the
fence byte-for-byte while the immutable lease retains
`revocation_owner_id == origin_revoking_handoff_id`, the resolved root handoff.
Its `predecessor_handoff_id` names the immediate chain tip and equals that root
only at sequence two. Typed validation is O(1): it exact-reads the immediate
predecessor and directly reads the same-cohort root named by
`origin_revoking_handoff_id`, compares their source/fence/sequence invariants,
and relies on the unique predecessor/sequence induction; it never walks an
unbounded ancestor chain. For `NEWLY_REVOKED`, row `fenced_at` equals the embedded fence
time and lease `revoked_at`. For `ADOPTED_ABANDONED_PREDECESSOR`, the embedded
origin time remains unchanged while the new row's `opened_at=fenced_at` is one
fresh database time used only as evidence metadata. Raw wall-clock values may
repeat or regress and never order the chain; `chain_sequence` and row-state
CASes define causality. `COMPLETED` preserves `source_cohort_state`, stores a
final-set embedded revision equal to `final_registration_set_revision ==
committed_cohort_revision == source_registration_set_revision + 1 ==
source_cohort_revision + 1`, and stores a separately byte-equal final Deployment
snapshot. `ABANDONED` additionally proves candidate UID absence plus exact zero accepted
membership, live claim, attempt attestation, progress, provider operation, and
provider effect for that candidate. Reason
`both_members_lost_cold_recovery_required` also requires a separately hashed exact
survivor-UID absence proof; lease expiry, unready state, deletion timestamp, or
name-only evidence is insufficient.
At `OPEN`, candidate registration/hash are byte-equal to the initial candidate
lease renewal-registration/hash; registration `registered_at`, lease
`renewed_at`, and row `opened_at=fenced_at` use the same database time, and the
lease is generation/revision one with `last_operation_kind=INSERT`. Candidate
abandonment revokes that lease with `CANDIDATE_ABANDONED`, current handoff ID,
`last_operation_kind=REVOKE`, and `lease.revoked_at == handoff.terminal_at`.
Lost-ack adoption verifies every immutable handoff equality plus the candidate
API row's exact bound phase, immutable boot/stable identity, and owner hash. It
accepts the candidate lease either at that exact generation/revision-one insert
(including the caller's operation ID) or as a valid same-stable-identity ACTIVE
descendant through only legal renewal/process-supersession transitions from the
recorded candidate registration. Bootstrap plus unchanged source proves an
uncommitted attempt; partial bound/lease/handoff evidence blocks. A terminally
superseded handoff is reported as such rather than falsely adopted as `OPEN`.

The first handoff for one stale member has
`stale_fence_disposition=NEWLY_REVOKED`, null predecessor, and
`chain_sequence=1`. If its candidate
is proved absent with zero effect and the handoff becomes `ABANDONED`, another
candidate may open a chained `ADOPTED_ABANDONED_PREDECESSOR` handoff. Its
same-cohort predecessor ID must resolve to the immediately prior terminal
`NEWLY_REVOKED | ADOPTED_ABANDONED_PREDECESSOR` row with reason
`candidate_absent_zero_effect`, identical source set,
stale identity, stale-UID absence proof, and stale-authority fence; the stale
lease must remain at the exact terminal generation/revision and a locked scan
must find zero current stale claims. The accepted survivor and both its
registration and API server-instance leases must still be fresh. The new
candidate must be distinct and gets a new atomic lease/handoff insert with
`chain_sequence == predecessor.chain_sequence + 1`. Under the cohort lock,
typed validation requires the predecessor to be the greatest-sequence,
unadopted terminal tip for that retained source/fence. The partial unique
predecessor index independently rejects a second adopter or a branch back to an
older row, while the `(cohort_id, source_cohort_revision, chain_sequence)`
unique key rejects duplicate roots or sequence positions. A
predecessor with survivor absence, nonzero or unknown candidate effect, changed
membership, or any unequal fence cannot be adopted. Chaining may repeat while
the same survivor remains fresh; the immediate self-FK, immutable insert,
unique predecessor, and consecutive positive sequence prohibit dangling,
branching, cross-cohort, or cyclic provenance without depending on wall-clock
ordering. Full-set cold recovery is reserved for exact absence of both accepted
members.

Handoff history cannot be garbage-collected while a registration-lease owner,
successor self-FK, cold-recovery preserved owner, action, attempt, request,
cohort reference, policy epoch, or rollout-evidence row can name the handoff,
its cohort, or its worker instances. Typed reads require every such owner ID to
resolve to the exact same-cohort handoff and retain the origin row; a JSON cold-
fence owner cannot bypass this rule.

Full-set cold recovery preserves the cohort because every bound action and
request is frozen to it. It is legal only for an `ACCEPTING | DRAINING` V2
cohort with exactly two members, no `OPEN | READY` handoff, and the same live
Deployment UID/generation/template/image/ServiceAccount. Any interrupted
single-member handoff first reaches exact `COMPLETED` or `ABANDONED`. Two
replacement Pods remain bootstrap-only and submit distinct fresh V2 identities
without registration leases or claims. The surviving API verifier independently proves
UID-qualified absence of both accepted Pods, both candidate owner chains, and
one fresh final two-ready/two-available Deployment snapshot; both candidate API
server-instance bootstrap leases remain fresh through commit.

Before the mutating transaction, nonlocking discovery reads both complete old
lease snapshots and
the bounded old request-then-queue claim inventories using the same unfiltered
all-active owner query and exact-four-private-shape validation. It derives the proposed
operation time as
`GREATEST(clock_timestamp(), every affected old lease.renewed_at,
candidate_1_registration.worker.observed_at,
candidate_2_registration.worker.observed_at,
<both candidate execution-owner container start/observation and API start terms>,
<every discovered private_request_terminal_lower_bound>)`, omitting the request
term for an empty inventory. One
transaction locks the cohort and empty nonterminal-
handoff slot. Using that proposed operation time, the immutable
source/proofs, optimistic claim fences, two candidate registrations, final set/
snapshot, and terminal lease revisions are fully constructed, and the complete
cold-recovery evidence row is inserted under that class-3 serialization before
any later lock. The transaction then visits all old and absent candidate lease
keys in canonical instance order at class 5. It locks each old row, requires
every discovered byte, generation, and revision to be exact or rolls back the
earlier evidence insert, and rechecks both candidate keys absent immediately
before inserting their generation/
revision-one ACTIVE leases, and stages each legal old-lease transition. An
ACTIVE old lease is revoked at revision + 1 with `COHORT_COLD_RECOVERY`; an
already-REVOKED old member is allowed only when its exact retained
`STALE_HANDOFF` origin proves that member, in which case its bytes remain
unchanged. Any other prior state rejects.
For an ACTIVE old lease, the cold fence has null preserved-revocation fields,
records prior and terminal=prior+1 revisions, exhaustively lists request and
queue claim mutations, and has `fenced_at == lease.revoked_at ==
recovery.completed_at`; the terminal lease records `COHORT_COLD_RECOVERY`, the
recovery ID, and `last_operation_kind=REVOKE`. For an already-REVOKED old lease,
terminal revision equals prior, preserved reason/owner equal its exact
`STALE_HANDOFF` origin, all lease bytes/timestamps remain unchanged, the claim
list is empty, and `fenced_at == recovery.completed_at` is a new audit time
rather than a second revocation.

Only after the complete evidence and every class-5 change are staged does the
transaction lock the two old execution-owner and two candidate bootstrap API
server-instance rows in process-instance order, require both candidate
bootstrap rows fresh, revalidate each immutable boot/stable identity, and stage
each bootstrap-to-bound transition with its newly inserted lease's exact owner
hash. No commit can expose either candidate lease with a bootstrap/null-hash API
row. It then locks every discovered
old request row in request-ID order and only then every corresponding queue row
in request-ID order. It reruns both bounded inventories under those
locks and reconstructs every V2 terminal-fence entry byte-for-byte. It then
invokes the one borrowed batch core: terminalize all locked requests and delete
all locked queues, key-share-lock named action lineages, insert/adopt all action
selectors, insert/adopt all shadow terminal histories, and only then allocate
all operational events. It uses only `COLD_RECOVERY_FENCE`: action arms
with/without lineage map to `REQUEST_CANCELLED/LINEAGE` or
`TERMINAL_BEFORE_CLAIM_START/NO_SUCCESSFUL_CLAIM_START`, while shadow arms write
exact `REQUEST_CANCELLED/SHADOW_EXECUTION` for `AUTHORIZED` or
`TERMINAL_BEFORE_CLAIM_START/NO_SUCCESSFUL_CLAIM_START` for `BOUND`, with the
corresponding nonnull/null lineage hash; missing lineage is never created. Every
request becomes `CANCELLED` with its arm-specific
receipt. A nonnull prior cancellation intent is preserved and acknowledged at
the fence time; without prior intent both cancellation fields remain null. No
terminal request retains an unacknowledged intent; no same-request requeue or
scalar terminalizer loop exists. Any drift
rolls back the earlier uncommitted recovery row, lease
inserts, and revocations; this includes drift from generic request
terminalization, which need not hold the cohort lock. Immediately before commit,
a fresh `clock_timestamp()` must precede both candidate registration/API lease
expiries and all candidate/absence/snapshot proofs must remain inside their fixed
freshness bounds; a wait past TTL rolls back. With that suffix validated, it updates the already-
locked cohort/final V2 set by exactly one revision and commits membership,
claim fences, leases, and evidence atomically; no earlier-class row is inserted
after API/request/queue locks.
Each candidate registration `registered_at`, lease `renewed_at`, and recovery
`completed_at` is that same timestamp, and each new lease records
`last_operation_kind=INSERT`.

The recovery row's source revisions are equal and its two source workers equal
the two sorted absence/fence proofs. The final revisions satisfy
`final_registration_set_revision == committed_cohort_revision ==
source_cohort_revision + 1`; the final set contains exactly the two distinct
candidate lease registrations and its snapshot is byte-equal to the separately
hashed snapshot. Existing action specs, attempt identity/payload/progress,
request payloads, and cohort references do not change. Each old request
preserves generation one while clearing token/worker/lease/PID/heartbeat,
records `CANCELLED` at the fence time, deletes its queue delivery, and persists
the arm-specific terminal receipt exactly as recorded by the fence. Unknown
commit outcome adopts only the exact recovery
row, cohort/final-set evidence, each candidate API row's exact bound phase/boot /
stable identity/owner hash, and each current candidate lease either at the
recorded generation/revision-one insert or as a valid same-stable-identity
ACTIVE descendant through only legal renewal/process-supersession transitions.
Bootstrap rows plus unchanged source prove an uncommitted attempt and permit a
full retry; partial or unequal bound/lease/recovery evidence blocks.
A later membership transition is
reported as supersession, never repaired by replay. Candidates become `/readyz` and claimable
only after that read. Candidate loss before commit writes nothing; loss after
commit is a new replacement. If the immutable Deployment is gone or changed,
recovery blocks; the chart must retain it while bound work exists, and only a
zero-bound-work retirement can create a new cohort.
Cold-recovery rows are permanent membership history and cannot be deleted while
any registration-lease owner, action, attempt, request, reference, policy,
rollout evidence, or retained registration history can name the recovery,
cohort, or an old/new worker instance.

The candidate binding is the canonical hash of the policy hash, exact deployed
API/controller/ordinary-executor image IDs and inventories, selected cohort ID
and static seed, handler/claim inventory, exact
`ServeActionCapacityProfileV1`, the elected
`ServeServiceVersionSpecIdentityV1`, every live replica's bound version
identity and capacity projection, and required crash inventory. A
`legacy -> shadow` transaction mints the epoch, stores policy/binding hashes,
and writes `resource_action_mode_changed_at` from the same PostgreSQL clock.
Every coverage graph and crash run copies all three values under the service
lock. Promotion scans only exact-equal rows and requires the locked service
values to match the fresh `AuthoritativePromotionProofV2`.

Any pre-promotion change to an ordinary-role image/inventory, selected cohort
ID or seed, handler/claim inventory, qualification-policy bytes, crash
inventory, elected version identity, live-replica version/capacity identity, or
the closed capacity profile invalidates the binding. New private admissions
stop. After all old private requests and references settle, an owner-fenced
shadow-only reset transaction locks the service, relevant version and live
replica rows, cohort/references, coverage/actions, and crash rows in the global
order. It requires zero nonterminal private work and zero `STARTED` crash runs,
mints a new epoch and database timestamp, and recomputes the complete
policy/binding. Old graphs remain retained but cannot count. No 24-hour,
100+100, or crash evidence spans a reset. Authoritative mode has no candidate
reset or demotion path.

The initial qualification policy approves the exact M4 merge artifacts only;
it cannot name a future M5a merge SHA or digest. Initial promotion inserts one
`ACTIVE/OPEN` policy-epoch row whose `policy_epoch` equals the qualified
candidate epoch and whose rotation proof is the exact promotion proof. The
same owner/service-locked transaction changes the service from `shadow` to
`authoritative`, binds its exact candidate epoch/policy/binding, and inserts
that root at admission revision one with its `ACTIVATE` operation ID; no
authoritative service is visible without the root and no root is visible for a
legacy/shadow service. An acknowledgement-lost commit exact-reads and adopts
only that paired service/root state. Its
authority binding covers the policy, approved role/cohort inventories, selected
cohort, handler/claim contracts, and schema heads; per-action version identity
remains separately immutable. Every `ACTION_ACTIVE` cohort reference and
authoritative `ServeReplicaActionSpecV2` binds the same active
`(policy_epoch, policy_sha256, authority_binding_sha256)` tuple. The spec's
`AuthoritativeActionPolicyBindingV1.policy_epoch` is copied byte-exactly to the
reference's `authority_policy_epoch`; both are the native/canonical rendering
of one UUID. The reference has all three fields null before action binding and
all three nonnull afterward. The reference's physical triple-state CHECK
and composite `ON DELETE RESTRICT` FK reject partial or dangling bindings:
`PREPARING | SHADOW_ACTIVE` is all-null, `ACTION_ACTIVE` is all-nonnull, and
`RELEASED` preserves either the prior all-null shadow/preparation lineage or the
prior byte-equal action-policy triple. Release never clears or changes a bound
policy. A later compatible artifact enters authority only through the
monotonic policy-rotation protocol below. No tag, PR head, guessed merge SHA,
or unreviewed image may be added by editing environment or service state.

The three service candidate columns are the immutable initial-promotion root,
not an alias for the current authority policy. Compatible rotation never
rewrites them. Every later preparation/admission reader locks and validates the
singular initial root against those columns, separately locks the singular
`ACTIVE/OPEN` policy, and uses only that current policy's epoch/policy/binding
tuple for new authority. On the initial epoch both records are the same row;
after rotation they are distinct. Requiring the active successor epoch to equal
the service candidate epoch would incorrectly disable every forward rotation
and is forbidden.

Crash evidence is not caller assertion. An API-role qualification transaction
creates one run ID and nonce commitment bound to the locked shadow service
epoch, exact action/request when applicable, and required boundary. It commits
revision-one `STARTED` with all receipt/evidence/outcome/completion fields null
*before* the injector is authorized. A dedicated test-cluster fault-injector
with purpose-specific mTLS/signing material may perform only the named canary
Pod disruption and returns a canonical signed injection receipt. The surviving
API verifier validates that receipt, independently reads the durable
action/attempt/request/Serve/cohort postconditions, and performs one
revision-one-to-two compare-and-swap to `COMPLETED` with `PASS`, `FAIL`, or
`ABANDONED`; the harness cannot write the table or choose policy/binding bytes.
A lost completion acknowledgement exact-adopts only byte equality. A second or
different completion is corruption.

`STARTED` has revision one, a unique partial key per
`(service_hash, candidate_epoch, boundary_id)`, and no result fields.
`COMPLETED` has revision two, nonnull verification evidence/hash and database
completion time; `PASS` also requires a signed receipt/hash, while `FAIL` or
`ABANDONED` may pair-null that receipt when injection certainty could not be
reconstructed. Subject-shape checks require service runs to have no action or
request, action runs to have an action only, and request runs to have an action,
positive attempt, and canonical request UUID text. Payloads are canonical JSON
objects of at most 65,536 bytes, hashes are lowercase SHA-256, service hash
equals incarnation text, and timestamps come from PostgreSQL.

An unresolved `STARTED` blocks promotion and reset until recovery either
completes it from durable evidence or terminalizes it `FAIL`/`ABANDONED`; no
timeout deletes it or manufactures success. Any `FAIL` or `ABANDONED` taints
that candidate epoch permanently, so a later `PASS` cannot hide it. Promotion
locks the service and requires zero `STARTED`, zero non-`PASS` completed rows,
at least one valid `PASS` for every exact policy boundary, exact
policy/binding/epoch equality for every counted row, and a canonical inventory
hash equal to the required policy. If promotion wins first, a later canary
start rejects non-shadow mode; if `STARTED` wins first, promotion observes it
and rejects. Public APIs, controllers, service YAML, logs, and operator
assertions cannot insert or override these rows.

The immutable attempt-exhaustion row is Serve's bounded operator event; it is
not an API004 request-terminal event and has no delivery/acknowledgement state.
Its checks require `event_code='attempt_domain_exhausted'`, attempt
`2147483647`, action kind `launch | down`, positive generation/revision,
`replica_id >= 0`, a 1..256-byte service name, canonical UUID-text service and
request identities, `service_hash == str(service_incarnation)`, three lowercase
SHA-256 values, and exactly one reduction basis from
`handler_retryable | handler_uncertain | request_not_started |
request_observation_required`. Handler `B`, fallback `X`, quarantine, and every
other blocked cause cannot create it. `(occurred_at, action_id)` is its sole
diagnostic index. There is no foreign key to API action/attempt/request tables:
the API and Serve histories remain independently migrated, the evidence must
survive service-row lifecycle changes, and typed reduction validates the full
cross-table relationship.

Before Serve038, the exact M4 image runs once in a cleanup-only phase pinned to
actual API008/Serve037/state028. Its migration ceiling is 037; it cannot start
an API/controller/request executor, create a V2 row, or enable private shadow /
authority. This phase adds the missing typed accepted-V1 retirement bridge in
application code, with no DDL: after the chart deselects a P2a V1 cohort, the
bridge locks cohort -> references, proves P2a's exact zero action/reference /
private-effect inventory, advances `ACCEPTING -> DRAINING`, and then advances
`DRAINING -> REMOVAL_AUTHORIZED` with monotonic PostgreSQL state time. The
current-chart tombstone upgrade then removes only the exact Deployment and
ServiceAccount, so every old authority-worker Pod is gone before the surviving
Serve037 API verifier proves both exact NotFound results, exact-reflects the
frozen Serve034 release-ledger subcatalog, and commits `RETIRED`.
The bridge exact-adopts every lost CAS result.

The chart exposes only
`databaseMigration.authorityV1RetirementPhase = none | deselect | tombstone`,
defaulting to `none`. Both non-default values are upgrade-only, require the
guarded external PostgreSQL Secret and authority topology, forbid bootstrap,
require `activeCohort=""`, and pin every central PostgreSQL client to the exact
Serve037 head through the chart-owned server environment. The pin rejects both
older and newer observed Serve heads; it is not Alembic's normal additive
minimum-version check. `deselect` keeps `enabled=true`, leaves the complete old
`cohorts` and tombstones unchanged, renders the old immutable worker objects,
changes the preflight Service selector to match no cohort, removes private
authority from API/controller, and runs the cleanup-only Job as a
weight-10 `post-upgrade` hook. `tombstone` keeps `enabled=true`, moves every
authorized old suffix from `cohorts` into `retirementTombstones`, and runs the
existing frozen-Serve034 release-ledger preflight at exact Serve037 as the
weight `-10` `pre-upgrade`
hook before Helm removes the exact worker Deployment and ServiceAccount. The
API runs only the retirement verifier in both phases. After every row is
durably `RETIRED`, a separate upgrade sets phase `none`, disables and clears
the old authority inventory, and may apply Serve038.

The cohort values discriminator is phase-exact. Every normal Serve038 live
cohort requires the literal string
`manifestContract: provider_authority_worker_cohort_v2`; only that string may
render a numeric V2 manifest and `frozen_action_cohort_join_v2`. A numeric
values-level selector is forbidden because Helm normalizes integral YAML/JSON
numbers before templates evaluate them and therefore cannot reliably reject
lexical `2.0` while accepting lexical `2`. Missing, numeric, alternate, and the
obsolete `manifestVersion` spelling fail closed in the normal phase. Conversely,
Serve034 `deselect` accepts only the previously shipped cohort values shape with
no `manifestContract` key and renders the byte-frozen numeric V1 manifest,
`frozen_action_cohort_join_v1`, and `Recreate` strategy; any discriminator in
that phase fails. `tombstone` renders no live cohort Deployment. Persisted and
wire manifest versions remain strict numeric integers; this string exists only
at the chart values/render boundary.

This retirement is deliberately one-way once the deselect hook commits an
edge. A failure before that commit may restore the previously selected release;
after any cohort reaches `DRAINING`, `REMOVAL_AUTHORIZED`, or `RETIRED`, an old
selected P2a release is not a supported rollback target. Operators keep the
Serve037 pin and roll forward through tombstone/NotFound retirement; no chart
value, timeout, or rollback manufactures acceptance or retirement evidence.
Local verification covers the strict cleanup environment and lost-CAS paths,
PostgreSQL append/renew/register/promotion/rollback and carrier races, exact
retirement timestamps, all 312 rendered Helm unit tests, the authority render
guard, and chart lint. Deployment evidence remains an open gate.

The cleanup-only gate retains every V1 history row and requires all V1 cohorts
`RETIRED`, zero stale or fresh authority-worker server instances, and no remaining
P2a authority Pod before normal M4 migration. A V1 append/renew/registration
or shipped `REGISTERING -> ACCEPTING` / `DRAINING -> ACCEPTING` transaction
racing either bridge edge linearizes on the cohort lock: it commits
before and is included in the next exact CAS, or observes the later lifecycle
and changes zero rows. Any nonzero or ambiguous carrier blocks the phase rather
than being deleted or inferred away. The 038 migration then
installs the V2-only nonterminal CHECK above, so any stale old-binary V1 insert,
append, or renewal fails at the database even if a Pod is accidentally revived.
Every post-038 chart and binary preflight rejects an authority-worker artifact
that lacks exact-038 capability; rollback to a pre-038 P2a writer is unsupported.
M4 registers only a fresh V2 suffix after the migration. No V1 bytes are
rewritten or deleted.

Serve038 has `down_revision='037'`. Before any cohort alteration or new-table
DDL, the PostgreSQL migration proves the exact current Serve037 catalog,
including Serve035's multi-pool reserved-fill relations/columns, Serve036's
four version-controller configuration columns, and Serve037's two placement-
normalization ledger tables, service/version columns, PostgreSQL retirement
CHECK, and run foreign keys. It also proves the required
Serve033 action catalog and full immutable Serve034 ledger subcatalog: both
release-ledger tables and every column/type/default/nullability/check/key /
foreign key/index. It preserves all of those predecessor objects byte-for-byte
and never adopts a partial or lookalike 037 catalog. Existing placement-
normalization run/row receipts and retired-version evidence may be nonempty;
Serve038 neither reinterprets nor requires them empty and changes none of their
columns, constraints, foreign keys, or rows. Only then does it take `ACCESS EXCLUSIVE` on
every altered existing relation in global class order: class-2 `services`,
`version_specs`, and `replicas`; class-3
`serve_resource_action_worker_cohorts`; class-6
`serve_resource_action_worker_cohort_refs`; then class-7
`serve_resource_action_shadow_coverage`. This keeps every old mode, version,
replica, reference, and coverage writer in the same direction and prevents DDL
from reaching backward from coverage to a reference or class-2 table. It uses
no Alembic `autocommit_block`, reruns the complete activation inventory after
all six locks, and holds them through DDL, postcondition reflection, and head
stamping. The Alembic advisory lock alone is insufficient because Serve033 application
writers do not take it. After lock acquisition it requires zero
shadow/authoritative services, all preexisting service/version/replica/
reference candidate fields null, zero coverage rows, every preexisting cohort
to be exact numeric-V1 `RETIRED` history with null removal authorization and
nonnull `retired_at == state_changed_at`, zero stale or fresh
`authority-worker` API server-instance rows, and every exact
preexisting 038 table to be empty. An incompatible
partial object fails before DDL; an exact empty object is adopted after an
unknown acknowledgement. Both race directions with an old mode or coverage
writer—or any other writer of an altered existing relation—therefore either
commit wholly before the locked audit and make 038 fail, or wait and then fail
the new constraint, never stamping an invented epoch.

Under those locks, migration exact-validates and replaces the shipped cohort
lifecycle timestamp CHECK with the Serve038 shape above. A retained exact-V1
`RETIRED` row has no recoverable prior removal timestamp and remains the
explicitly grandfathered, immutable null-history shape. Every other V1 state,
malformed or non-V1 version token, and nonnull V1 removal timestamp fails before
DDL or stamping. The exact-037 cleanup gate over the frozen Serve034 action
contract has already removed every old
authority writer, so there is no post-038 mixed-old-controller exception. V2
and every later transition reject a null removal time. Migration rewrites no
JSON/hash, lifecycle timestamp, or authority evidence.

The PostgreSQL service CHECK is one closed mode shape: `legacy` requires all
three candidate fields null; `shadow | authoritative` requires a canonical
nonnull epoch and two lowercase SHA-256 hashes. Coverage hashes, policy epoch
states, crash subject/result shapes, canonical UUIDs, bounded JSON, payload/hash
pairing, and database-clock order have exact checks and the documented indexes.
The post-migration inspector proves every column, check, key, partial unique
index, and operational index—including the exact nullable
`worker_cohorts.removal_authorized_at` timestamp and replacement lifecycle
CHECK—before stamping Serve038.

In `sky/serve/resource_action_m4_state_schema.py`, fresh
`service_candidate_columns()` and `coverage_candidate_columns()` factories own
only portable service columns and PostgreSQL coverage columns respectively; a
fresh `cohort_candidate_columns()` factory owns only the additive nullable
PostgreSQL `removal_authorized_at` column.
`SERVE038_METADATA` owns exactly the six wholly new PostgreSQL policy-history,
worker-registration-lease, worker-registration-handoff, worker-registration-
cold-recovery, crash-run, and attempt-exhaustion tables; complete runtime
reflections of the three altered Serve033 evidence relations live in separate
non-enumerated metadata. Migration
`sky/schemas/db/serve_state/038_serve_resource_action_authority.py` explicitly
ALTERs existing relations from those factories, exact-adopts only the matching
nullable cohort timestamp after an unknown DDL acknowledgement, and never invokes `create_all()`
over metadata containing `services`, coverage, either Serve034 ledger table, or
a cloned Serve033 dependency graph. Nothing is appended to the frozen
`resource_action_state_schema.RESOURCE_ACTION_STATE_METADATA`: revision 033
dynamically enumerates its shipped six-table catalog, so mutating it would
silently rewrite fresh 033 databases.

The Serve038 cohort codec dispatches on the registration-set `version`.
Existing V1 bytes remain exactly readable only as terminal retirement history;
no migration rewrites or rehashes them. V1's sole live retirement program runs
at exact Serve037 through the frozen Serve034 action contract: shipped
stale-`REGISTERING` authorization plus the M4
cleanup-only accepted bridge above. Those transactions write only the shipped
lifecycle/revision/`state_changed_at` fields, preserve the registration bytes /
hash and every `RELEASED` or terminal carrier byte-for-byte, and use the chart
tombstone plus surviving API NotFound verifier to reach `RETIRED`. The action
catalog through Serve037 has no `removal_authorized_at` column, lease, handoff,
or cold-recovery row.

Serve038 accepts only exact numeric-V1 `RETIRED` history with null removal
authorization and truthful nonnull retirement time from that
program. Every V1 `REMOVAL_AUTHORIZED -> RETIRED` NotFound edge completes at
exact Serve037 before migration. Every selection, renewal, registration,
rollback, handoff, cold recovery, private activation, dispatch-readiness, and
claim path requires V2, and the physical CHECK forbids every post-038 V1 state
except the grandfathered retired shape. The
first post-038 M4 chart registers a fresh V2 suffix. Version coexistence is
therefore terminal history plus V2 authority, never dual live authority.

The PostgreSQL revision-001 bootstrap path invokes the Serve038 candidate,
version-identity, and replica-identity column factories so a fresh PostgreSQL
database has the same shape that upgraded PostgreSQL receives. Upgraded
PostgreSQL receives those missing columns, the cohort removal timestamp,
coverage/reference alterations, and all six new tables. Serve038 is
PostgreSQL-only: it has no SQLite migration, stamp, compatibility branch, or
fresh-schema target. Local/non-consolidated SQLite remains stamped at the
current Serve037 head but keeps the frozen Serve033/034 resource-action column
projection and legacy adapter; every M4 helper rejects that dialect before
reading or writing Serve038 state.
PostgreSQL downgrade refuses while retaining the additive evidence.

Separate PostgreSQL-only SQLAlchemy metadata owns the Serve038 existing-column
extensions and new tables so revision-001 bootstrap cannot accidentally create
them in a fresh SQLite database. Existing PostgreSQL rows retain null
replica identity/link/version-identity columns: migration must not mint an
identity that is absent from the live provider resource. New action-aware
replicas get the three provider identity fields together, with generation one,
and the exact immutable version-identity hash. Row-local checks
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
non-primary-key column from `EXCLUDED`; the Serve033 integration changes all
ordinary, batch, paid-capacity, and reserved-fill conflict updates to exclude
the action-owned set. Legacy inserts may still create null action fields, but
routine status persistence can never erase or replace an existing identity/link. Only typed,
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
  execution_route           TEXT not null default 'LEGACY_CONTROLLER'
                                                # compatibility default only;
                                                # PENDING_SELECTION |
                                                # LEGACY_CONTROLLER |
                                                # PRIVATE_API_REQUEST
  private_fallback_reason   TEXT nullable       # linked_admission_not_representable
  private_fallback_evidence JSONB nullable
  private_fallback_evidence_sha256 TEXT nullable
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
`PENDING_SELECTION` is legal only with phase `PENDING`, all three fallback
members null, and no child/history/private request. It is written only for a selected private
decision whose capacity/replica/coverage intent has committed but whose linked
admission has not. `PRIVATE_API_REQUEST` requires phase `RUNNING | COMPLETE`, a
null fallback triple, primary `private_api_request` children, and their one-to-
one histories. `LEGACY_CONTROLLER` requires only legacy children; its fallback
reason is nonnull exactly when the evidence/hash pair is nonnull, hash-valid,
and a prior `PENDING_SELECTION` decision atomically fell back after permanent
linked-admission non-representability. Ordinary legacy shadow keeps the complete
fallback triple null. The retained server default is a deprecated dark-rollout
compatibility seam for pre-039 legacy writers, which omit this additive column;
all M4/M5a writers set the route explicitly, and stacked PR #1240 removes the
default only in its gated Serve040 phase after the full M5a -> M4 -> M5a
matrix passes and M4 rollback closes. These checks make route selection durable and
prevent recovery from signaling both owners.

For the legacy-controller arm, including a selected-private decision that
durably took the permanent pre-write fallback, `serve_resource_action_shadow_attempts`
has one row for every legacy high-level SDK/direct mutation boundary. For a
successfully activated Serve039 `PRIVATE_API_REQUEST` arm, the same table
instead has one primary child for the private request boundary and its mandatory
one-to-one execution history; no legacy boundary runs while that route remains
private. The table's primary key is
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

Both counters are positive. The replacement Serve039 execution-kind CHECK
admits exactly `api_request | legacy_direct_down | private_api_request`;
`private_api_request` requires a primary role, `REQUEST_BOUND | COMPLETE`, and
a nonnull request ID/bind time, while `LAUNCH_CLEANUP_DOWN` rejects it. The
one-to-one history FK plus typed writer/reader requires exactly one history for
every private child and none for either legacy execution kind; SQL cannot express
that reverse existence rule in a row CHECK. Every JSON/hash pair, including the
companion's closed `LegacyProviderEffectTraceV1`, is pair-null and canonically bounded.
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
transaction that changes replica intent and its counted ordinary slot takes
owner -> service/policy/version/replica -> cohort -> nonterminal handoff -> both
accepted fresh registration leases -> same-ID preparation reference, changes
`PREPARING -> ACTION_ACTIVE`, inserts/adopts the action, and links its ID. If
any write fails, none commits.

In legacy-controller shadow the legacy thread remains the sole mutation owner.
For the later represented private-handler candidate, that thread remains the
sole decision/admission/request owner and waits for the one private request,
while the attested handler is the sole provider-effect owner; there is still
only one mutation path. A not-representable decision always remains same-cell
legacy SDK work and is never materialized for a private claimant. Every
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
in the suffix of a transaction already holding owner -> service -> active policy
locks, locks the matching complete attested `ACCEPTING` cohort, rejects a
nonterminal handoff, locks both accepted fresh registration leases, and inserts
or exactly adopts the decision's `PREPARING` reference
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
`ACCEPTING` cohort using the same owner/service/policy -> cohort -> handoff ->
accepted-leases -> reference order. Down admission later locks owner -> service/
policy/version/replica and, for a completed shadow source, the same-UUID global
cluster record/full handle in that same class-2 prefix -> cohort -> handoff ->
accepted leases -> reference ->
coverage/optional parent -> action and revalidates every optimistic source byte
under the canonical action-row order. Authoritative admission changes the
reference to `ACTION_ACTIVE`; legacy-controller shadow changes it to
`SHADOW_ACTIVE`; selected private shadow deliberately leaves it `PREPARING` for
the linked-admission transaction below. It creates no launch slot or
capacity reservation. Failure or denial releases only with the same proved
pre-call/owner fence as launch.

The manager then runs one short transaction whose locking order is owner ->
service/policy/version/replica -> cohort -> nonterminal handoff -> both accepted
fresh registration leases -> reference -> coverage -> optional parent.
After those locks the legacy-controller arm changes `PREPARING ->
SHADOW_ACTIVE`, writes coverage with the reference FK, then the same-ID parent
as `LEGACY_CONTROLLER/PENDING` when representable, and links. The selected private arm writes the same
coverage, replica/capacity intent, links, and exact represented
`PENDING_SELECTION/PENDING` parent
but preserves the locked reference as `PREPARING`; it creates no child, history,
request, queue, or provider-submit signal in this transaction. Both arms revalidate
service ownership and lifecycle epoch, the exact elected version identity/hash,
the provisional placement, and the complete capacity profile. For a
represented launch it additionally requires
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

After a legacy-controller commit or exact lost-commit readback, the manager sends a one-use
authorization containing the service hash, lifecycle epoch, decision ID, a
process-local unguessable preparation nonce, and, when represented, the exact
stored spec and invocation hashes. Only after receiving that authorization may
the legacy worker enter the existing provider-submit pool. While a selected
decision remains `PENDING_SELECTION` or becomes `PRIVATE_API_REQUEST`, it never
receives that signal and its committed queue is the sole dispatch path. Only
the atomic `LEGACY_CONTROLLER/linked_admission_not_representable` fallback may
send the legacy signal after commit. The manager first invokes the durable
linked-admission materializer below. A commit-before-signal
crash intentionally leaves a counted `PROVISIONING` slot. Owner-fenced recovery
either re-prepares and adopts it, or commits proved pre-call abandonment/failure
together with capacity release and slot removal. No elapsed-time-only cleanup
may clear the slot. Thus a preparation task may exist before durable admission,
but no legacy mutation is runnable or queued before coverage, replica intent,
and the counted slot commit.

For a represented decision, durable authority is the full stored canonical
`ServeReplicaActionSpecV2`: its embedded service-version identity/hash,
closed shadow-candidate or authoritative-policy binding, and its
`ProviderLifecyclePlanV1` and `ProviderLifecycleInvocationV1`, including
retained-source, execution-config/scope, template/inventory references, and
their hashes.
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
context and sends no mutation. Loss after approval but before a legacy
represented `PRE_SUBMIT` child or the private linked graph leaves a `PENDING`
parent and a counted slot;
recovery may reprepare and adopt it only when the complete durable
spec/invocation matches. A proved mismatch before SDK entry marks that parent
`ABANDONED_PRE_SUBMIT`, keeps its coverage as a promotion blocker, releases the
slot/capacity under the owner fence, and replans under a new desired generation.
A coverage-only decision is never replayed from reason equality. Once either
kind of `PRE_SUBMIT` is committed, cancellation or worker loss is handled as
ambiguous and never authorizes an unobserved blind replay.

For legacy-controller shadow only, immediately before each represented
`sdk.launch()` or `sdk.down()` call, including an in-process legacy retry and
its cleanup down, the worker
locks service -> replica -> cohort -> reference -> coverage -> parent -> child,
requires the exact `SHADOW_ACTIVE` reference, revalidates the one-use
authorization, and commits the next `PRE_SUBMIT` child.
Immediately before every coverage-only unsupported SDK call, it instead locks
service -> replica -> cohort -> reference -> coverage -> coverage-attempt,
requires the exact `SHADOW_ACTIVE` reference, revalidates the same
owner/epoch/link/cancellation fences, allocates the contiguous ledger row, and
commits `PRE_SUBMIT`. Only then may either kind enter SDK request creation.
After the legacy SDK returns a request ID, the worker locks its applicable
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

Private-handler shadow has a different and stricter boundary. For decision UUID
`D`, one-based `request_sequence` `Q`, and canonical uppercase `request_role`
`R`, its sole request ID is exactly
`str(uuid.uuid5(D, f'private-shadow-v1:{Q}:{R}'))`. Initial linked admission is
the sole private activation transaction. It freezes all keys before locking and
visits owner -> service/version/replica -> cohort -> handoff -> both accepted
leases -> `PREPARING` reference -> coverage -> `PENDING` parent -> absent child/
history keys, then both accepted API-instance rows at class 14, the deterministic
request at class 15, and queue at class 16. It revalidates the exact candidate
epoch, owner/lifecycle/cohort fence, complete preflight retained from preparation,
and linked-admission representability source. It atomically inserts or exact-
adopts the directly `REQUEST_BOUND` represented child, its preflight-bearing
`BOUND` execution history with empty effect trace, deterministic PR #1070
request/queue, and write-once private correlation; changes parent `PENDING ->
RUNNING`; and performs the one `PREPARING -> SHADOW_ACTIVE` transition. No prior
transaction performs that CAS. A retry/observation linked admission uses the
same full prefix with an already `SHADOW_ACTIVE` reference and `RUNNING` parent,
inserts the next contiguous child/history/request graph, and performs no second
reference transition. No committed private child ever has the legacy-SDK
`PRE_SUBMIT`/null-ID shape. The queue row cannot become visible
without its exact `(D,Q)` REQUEST_BOUND child and active reference. Same-ID replay must
be byte-equal in every field; a request-ID collision, represented kind
mismatch, partial graph, or pre-existing generic request is quarantined and no
queue row is claimable. The legacy post-SDK binder never accepts these private
handler names.

Quarantine never mutates or deletes the conflicting request/queue row, never
changes `PREPARING` to `SHADOW_ACTIVE`, and never inserts a second delivery.
It commits or retains the represented parent as a promotion blocker and the
reference as a retirement blocker, returns one closed collision/partial-graph
code, and emits bounded audit evidence. Recovery may release only after proving
the conflicting row was never claimable or started for this graph; otherwise it
retains indefinitely. Exact full-graph lost-ack is the sole adoption case.

This materializer is represented-only. `NOT_REPRESENTABLE` coverage has no
durable invocation that a remote handler could truthfully reconstruct, so its
same-cell owner commits the existing coverage-attempt ledger and enters the one
legacy SDK call under the process-local nonce. It cannot create or be claimed
as a private request and is always promotion-blocking.

The closed private body type is `ResourceActionPrivateRequestBodyV1`, derived
from `RequestBody` with `extra='forbid'`. A raw-input validator accepts exactly
one key: serialization alias `_skypilot_resource_action_authority_v1`; the
public spelling and public-plus-alias duplicate both reject. A dedicated
nonambient constructor supplies the inherited in-memory executor fields as
`env_vars={}`, `entrypoint=''`, `entrypoint_command=''`,
`using_remote_api_server=false`, `override_skypilot_config={}`,
`override_skypilot_config_path=null`, `file_mounts_blob_id=null`, and
`client_api_version=7`, without running `RequestBody`'s environment-capturing
defaults. The model sets `serialize_by_alias=true`; each inherited ambient
field is overridden frozen and `exclude=true`. If Pydantic cannot enforce
those properties on the existing base, implementation must first add a
dedicated registered-body base/codec rather than weakening this contract. Every
persistence, request display, retry, and debug-dump path either uses the private
codec or redacts the body; no generic default-name dump is allowed. Canonical
bytes are exactly `canonical_json_bytes(model_dump(mode='json', by_alias=True,
exclude=ambient_fields))`, containing only the underscore key—never raw
`model_dump_json()`, and none of those ambient fields are persisted.
Its `to_kwargs()` returns exactly the public keyword
`resource_action_authority_v1: ResourceActionPrivateRouteV1`; the PostgreSQL
claim predicate checks the literal durable alias. The route closes
`version=1`, handler kind, decision/action UUID, request sequence or action
attempt, deterministic request ID, cohort ID, Deployment UID, reference
revision, and the immutable route-projection hash defined above. Serialization and decode round trips
must preserve the underscore alias and reject every extra or ambient override.
The common authority claim predicate additionally requires
`payload_type='sky.server.requests.payloads:ResourceActionPrivateRequestBodyV1'`,
`payload_format='pydantic-json'`, `payload_version=1`,
`jsonb_typeof(payload_json)='object'`, `jsonb_object_length(payload_json)=1`,
and the underscore key present as an object. Wrong type/format/version, null /
scalar/array JSON, a public-spelling key, or any sibling key is never claimable
even if a nested JSON path happens to match.
The shadow claim SQL joins that exact represented parent, child, and same-key
execution history; requires `child.legacy_request_id =
api_requests.request_id`, `child.phase='REQUEST_BOUND'`,
`history.phase='BOUND'`, `child.planned_execution_kind='private_api_request'`,
the route's matching `D/Q`, the private correlation, and `SHADOW_ACTIVE`. A
coverage-only row, unbound `PRE_SUBMIT` child, missing/crossed history or parent,
non-primary role, or crossed sequence cannot be claimed.

Each registered handler has the exact keyword-only public-field signature, for
example `serve_shadow_candidate_launch(*,
resource_action_authority_v1: ResourceActionPrivateRouteV1)`. It obtains
`request_storage.active_execution_claim()` from the executor context. The two
action handlers call exactly
`execute_resource_action_private_action_claim_v1(*,
expected_handler: ResourceActionPrivateActionHandlerNameV1,
claim: ExecutionClaim, route: ResourceActionPrivateRouteV1) ->
ServeReplicaActionRequestReturnV1`; the two shadow handlers call exactly
`execute_resource_action_private_shadow_claim_v1(*,
expected_handler: ResourceActionPrivateShadowHandlerNameV1,
claim: ExecutionClaim, route: ResourceActionPrivateRouteV1) ->
ServeShadowCandidateRequestReturnV1`. Both live in
`sky.server.requests.resource_action_handlers` and may share only a private
claim-resolution helper whose result is a closed handler-discriminated union;
neither public seam can return or decode the other arm. Each seam requires the active
claim's request ID to equal the route and locked durable request; requires its
execution generation and claim token to equal the locked request/queue lease;
requires that claim still RUNNING/unexpired;
reloads the immutable correlation/reference/attempt under those fences; and
then calls the server-local prepared provider adapter. It accepts no endpoint,
client body, caller connection, or user-selected request ID. It never calls `sdk.launch()`,
`sdk.down()`, `requests.create()`, or any SDK/public request entrypoint, never
creates a nested request, and never uses the legacy post-SDK binder. Repository
guards and monkeypatched integration tests make each forbidden entrypoint a
hard failure.

`SHADOW_ACTIVE` for a private represented reference changes to `RELEASED`
only after its private request and every represented evidence row are terminal.
A same-cell coverage-only reference has no private request and releases only
after its coverage-attempt ledger is terminal. `ACTION_ACTIVE` changes
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

For retained legacy-controller children, `planned_execution_kind` remains
exactly `api_request | legacy_direct_down`; their primary and cleanup roles keep
the frozen legacy codecs. A represented `PRIMARY_LAUNCH | PRIMARY_DOWN` child
may instead use exactly `private_api_request` only with its one-to-one Serve039
execution history and kind-matched private handler. `LAUNCH_CLEANUP_DOWN` can
never use that value. A direct-down sample may characterize the old path but is
always promotion-blocking. During M2 legacy teardown is routed through `sdk.down()` and
`sdk.stream_and_get()`; every legacy-controller down attempt in a promotion
window therefore has a real request ID. Shadow never inserts
`api_resource_actions`, enqueues an action request, invents a request ID, or
calls the provider twice. A legacy-controller row executes exactly one legacy
call and compares it with the proposed path. A selected represented-private row
suppresses that candidate's legacy call and executes exactly one attested
private-handler call, comparing its durable shadow outcome with the same frozen
projection contract. Mixing both execution kinds in one candidate graph is
promotion-blocking; neither mode can claim parity from double mutation.

Shadow is complete for a service, not statistically sampled. All four launch
owners and all in-scope teardown owners route through the common admission
primitive, enforced by a checked-in call-site guard. Promotion scans coverage
with `admitted_at >=` the locked mode-change timestamp, and separately queries
candidate-window parents without coverage. It blocks if an identified
launch/down link lacks or mismatches coverage, any outcome is
`NOT_REPRESENTABLE`, a `REPRESENTABLE` row lacks exactly one same-ID parent (or
vice versa), any expected attempt lacks a child or real request association,
any row is pending, abandoned, ambiguous, direct-down, unsupported, or
divergent, or either action kind lacks the fixed policy minimum of 100 clean
graphs. Only
representable clean `MATCH` graphs count toward those minima. Reason counters
and logs are diagnostic only and never satisfy coverage. Every coverage-only
attempt graph is nevertheless validated and reported; a malformed,
nonterminal, or unknown ledger row is an additional blocker. Retention does not
delete candidate-window coverage, coverage-attempts, parents, children, or
execution histories before promotion. The
transition to `shadow` and its database timestamp are written under the
service/owner lock; the promotion transaction locks service, live replicas,
cleanup intents, coverage by decision ID, coverage-attempts, parents, then
children and same-key execution histories at class 10 before rechecking the
minimum 24-hour window and all blockers. Every private history must be `SETTLED`
with exact child/outcome/return/terminal-receipt hashes, no fallback, and no
legacy/private mix; the scan then nonlockingly point-reads and cross-validates
both immutable class-17 terminal and settlement receipts. A missing, mutable,
or crossed history/receipt contributes zero clean evidence.

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
`AuthoritativePromotionProofV2.coverage_inventory_sha256` while retaining the same
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
shadow window. Candidate discovery is nonlocking. Before acquiring any row
lock, typed GC follows the indexed outgoing `Q` pointer and the reverse-indexed
incoming pointer and closes the candidate's deletion component. The component
is exactly one ordinary graph or one launch-source/primary-down-target pair; a
missing reciprocal edge, a second incoming edge, a chain/cycle, or any crossed
decision/sequence/basis hash is blocking corruption. It constructs the complete
component key inventory before locking, then acquires the canonical sorted union
at every class: all extant service-incarnation/owner rows, replica links and
cleanup intents, cohorts/references, coverage/coverage-attempts, parents,
children, and same-key execution histories. It never reaches from one locked
parent to discover or lock another parent at the same or an earlier class.

After the complete class-10 union is held, typed GC locks every distinct
nonnull private `request_id` in the component at class 15 in sorted UUID order.
Any surviving request retains its child and execution history until generic
request GC has validated and removed the request; typed evidence GC never
cascades either row out from under it. It re-reads the mode/window, every link
and cleanup intent, request retention, terminal shape, and each component
member's permanent class-17 settlement commitment exactly when that member is a
`PRIVATE_API_REQUEST` parent with its mandatory private execution history. A
missing, hash-invalid, or crossed required commitment blocks deletion. Legacy-
controller parents whose fallback reason/evidence pair is nonnull instead
require both the byte-equal permanent admission-fallback receipt and its exact
permanent fallback-progress receipt before deletion. The progress receipt must
name either the first legacy `PRE_SUBMIT` descendant or the same-transaction
terminal no-call release that made the graph eligible; a graph at the immediate
post-fallback/no-progress state cannot be collected.
Ordinary legacy-controller, pre-039, and coverage-only components retain their
existing typed completion/ledger predicates and neither require nor fabricate a
receipt. A
`Q` component is
eligible only when the linked primary down is settled and both source and target
are independently GC-eligible, with no request, replica/cleanup link, protected
window, reference root, or other retention root on either side. The one
transaction changes both exact references to `RELEASED` and deletes both
complete evidence graphs, or changes/deletes neither; deleting either side
alone and leaving a dangling source pointer or partial-down basis is forbidden.
An ordinary component follows the same single-graph predicate. Eligible
execution histories and represented children are deleted before their parents,
then coverage-attempts and coverage. Terminal, admission-fallback,
admission-fallback-progress, and settlement receipts are permanent under their
separately declared policy and are not implicitly deleted by this cascade.
They have no FK back to the graph and therefore do not block it. It never
locks coverage and then reaches backward to a replica. If the exact service row
is absent or the name now has another incarnation, M2 GC defers indefinitely;
reclaiming deleted-service evidence requires a separately designed durable
incarnation tombstone. Service/replica deletion never cascades evidence. If a
replica launched in shadow
later enters authoritative down, down admission loads the matching completed
launch child, revalidates its canonical observation and
`ResolvedProviderTargetV1`, locks the same-UUID global-user-state cluster row
and its full provider handle in class 2 before cohort or shadow-evidence locks,
and copies the typed basis into the immutable
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
  approved immutable digest, all at API008, the exact active policy-bound
  Serve039 or Serve040 head, and
  global-user-state 028; each dedicated attested cohort includes the private
  handlers only for actions frozen to that cohort, while every ordinary
  executor excludes them;
- exact `pod_cluster_v1` provider-profile and
  `ordinary_ondemand_physical_width1_v1` Serve-capacity-profile eligibility for
  the locked elected `ServeServiceVersionSpecIdentityV1` and every live
  candidate/replica's independently bound version identity, including
  `ReplicaInfo.is_spot == false`;
- complete decision coverage from the locked window start, zero
  `NOT_REPRESENTABLE` coverage rows, and no unresolved shadow divergence or
  unsampled mutation;
- at least 24 hours, 100 clean launch graphs, and 100 clean down graphs in the
  current candidate window; and
- successful crash injection at every boundary below.

`ActivationGateEvidenceV1` remains the closed legacy-controller-only value. It
is bound to the exact service name, service-incarnation hash, lifecycle epoch,
and database-clock `verified_at`; it carries the three schema-head strings and
approved image/inventory fingerprints. That compatibility branch permits only
`legacy -> shadow` at actual `API005`, Serve034, and global-user-state 028. It
cannot create a cohort reference or private request and cannot authorize
private-handler dispatch. Caller-supplied revision strings never prove the
actual database revision.

API008 with exact policy-bound Serve039 or Serve040 has three distinct closed
server-minted V2 proof values; they are never a
single caller-populated evidence bag:

- `PrivateShadowActivationProofV2` authorizes only `legacy -> shadow`. It binds
  the exact service name/hash/owner/lifecycle epoch, a null candidate-window
  start before the transition, the selected cohort, its approved immutable
  image manifest/config digests and complete manifest/artifact/callable /
  Pod-template/handler/claim fingerprints, state `ACCEPTING`, exactly two
  fresh distinct sorted matching V2 registrations and their final set-level
  Deployment snapshot, the closed capacity profile, and the complete elected
  version identity/hash. The commit writes the
  PostgreSQL-clock candidate timestamp; the caller cannot propose one.
- `PrivateDispatchReadinessProofV2` is a closed union discriminated by
  `dispatch_kind="shadow_candidate" | "authoritative_action"`, minted for one
  exact private request and execution claim immediately before dispatch. Both
  variants bind service/hash/owner/epoch, locked service mode and candidate
  timestamp, decision/reference/request IDs, the kind-specific attempt identity,
  claim generation and claim token, current unexpired queue lease,
  frozen cohort and proof inventories, and actual schema heads. The shadow
  variant requires mode `shadow`, reference `SHADOW_ACTIVE`, and the exact
  represented shadow parent/child; coverage-only attempts are ineligible. The
  authority variant requires mode `authoritative`, reference `ACTION_ACTIVE`,
  and the exact `api_resource_action_attempts(action_id, attempt)`, immutable
  input hash, API006 progress revision/hash, action-bound version identity/hash,
  the closed capacity profile, and the exact action/reference-bound active
  policy epoch/policy/binding tuple. The authority claim also requires the
  caller's current V2 membership, fresh Serve038 registration and API server-
  instance leases, and absence from the stale/candidate side of an
  `OPEN | READY` handoff. A new authoritative reference/action root requires the
  active policy to be `OPEN`. A private-shadow decision has no authority-policy
  tuple: its new `PREPARING`/`SHADOW_ACTIVE` root instead requires the locked
  service to remain in `shadow`, the exact current candidate epoch/binding, and
  the accepted cohort/activation proof. An already-bound exact-
  policy action may materialize its deterministic current-attempt request as
  continuation history, including attempt one after admission committed before
  any request existed, then claim, claim-start, checkpoint,
  perform I/O, and return
  while that policy is `OPEN | DRAINING`; `DRAINING` requires the action and
  `ACTION_ACTIVE` reference to predate and remain byte-bound across the exact
  admission-state CAS. That continuation preserves the action/reference/policy
  binding and cannot create a new authoritative reference/action or independent
  request root. `CLOSED | SUPERSEDED` permits no current authoritative execution. New
  admission also requires the currently selected cohort to be `ACCEPTING`. Existing
  work frozen to that cohort may dispatch while the cohort is `ACCEPTING` or
  `DRAINING`; changing active selection does not invalidate it. The server
  re-mints and revalidates this proof on every dispatch/retry, so an activation
  proof or remembered readiness result is not reusable. For an authoritative
  action, claim-start consumes it and inserts/exact-adopts the Serve039 lineage
  in the same claim-fenced transaction before handler invocation; a later first
  journal watermark only validates that immutable key. No proof is returned to
  unlocked code as a bearer capability. Private shadow consumes its disjoint
  membership/authority proof through the specified same-transaction
  `BOUND -> AUTHORIZED` history CAS and remains disabled until that contract and
  all same-inventory representability boundaries are implemented and verified.

  The authoritative variant projects the following closed, nonsecret provider
  context value while consuming that readiness proof:

  ```text
  ProviderExecutionAuthorityProofV2 = {
    version: 2,
    schema_heads: AuthoritySchemaHeadsV2,
    service_hash: UUID,
    policy_epoch: UUID,
    policy_sha256: Sha256,
    authority_binding_sha256: Sha256,
    policy_admission_state: "OPEN" | "DRAINING",
    policy_admission_revision: PositiveInteger,
    action_id: UUID,
    action_kind: "launch" | "down",
    immutable_spec_sha256: Sha256,
    resolved_cohort: ProviderAuthorityWorkerCohortV2,
    registration_set_sha256: Sha256,
  cohort_id: Text,
  deployment_uid: Text,
  reference_revision: PositiveInteger,
  api_instance_started_at: UtcTimestamp,
  api_instance_heartbeat_at: UtcTimestamp,
    preflight_request_sha256: Sha256,
    preflight_response_sha256: Sha256,
    representability_case_inventory_sha256: Sha256
  }
  ```

  Its policy tuple is byte-equal to the V2 action binding, `ACTION_ACTIVE`
  reference, and locked `ACTIVE/(OPEN | DRAINING)` row; `policy_epoch` is the same UUID at
  every boundary and is never an integer. Its registration/cohort/Deployment
  fields are derived from the caller's current accepted V2 membership, and its
  preflight hashes bind the fresh purpose-authenticated action-kind request and
  response. The value is an immutable context projection, not a lease or
  bearer capability: it is usable only with the exact claim/action/attempt
  whose `PrivateDispatchReadinessProofV2` was atomically consumed at
  claim-start and frozen in Serve039 lineage, and every later external effect revalidates that durable
  context. Private shadow instead carries the shadow-candidate branch of the
  readiness proof and `ShadowCandidateActionBindingV1`; it has no authority-
  policy field and never invents a null, zero, or
  synthetic authority-policy epoch.
- `AuthoritativePromotionProofV2` authorizes only `shadow -> authoritative`.
  It binds exact equality to the locked candidate timestamp, a candidate
  duration of at least 24 hours, the locked coverage inventory hash and counts
  of at least 100 clean launch plus 100 clean down graphs, zero divergence /
  ambiguity/blockers, the exact successful crash-canary inventory, every
  referenced cohort identity/state needed for nonterminal work, and the
  approved deployment inventory. It also binds the locked elected-version
  identity/hash and proves each live replica's own immutable version identity
  and closed profile. Its proof inventory includes the exact frozen
  representability-case inventory hash; every counted private-shadow graph must
  have been admitted, claimed, terminalized, and settled under that same hash,
  and a changed/mixed/missing hash contributes zero clean evidence. Neither
  minima nor duration is caller configurable.

The frozen pre-039 trust file has the following exact closed shape. It remains
readable as dark history only; the live server-owned trust file is the
`ResourceActionQualificationPolicyV2` defined above and requires the exact
active policy-bound Serve039 or Serve040 head:

```text
ResourceActionQualificationPolicyV1 = {
  version: 1,
  api_requests_head: "007",
  serve_head: "035",
  global_user_state_head: "028",
  candidate_minimum_seconds: 86400,
  minimum_clean_launches: 100,
  minimum_clean_downs: 100,
  approved_role_images: [  # exactly API, ordinary-executor, controller; role order
    {role: "api" | "ordinary-executor" | "controller",
     oci_manifest_digest: "sha256:" + 64LowerHex,
     source_commit: 40LowerHex,
     artifact_inventory_sha256: Sha256}
  ],
  approved_cohorts: [      # 1..16, ascending cohort_id
    {cohort_id: Text,
     oci_manifest_digest: "sha256:" + 64LowerHex,
     oci_config_digest: "sha256:" + 64LowerHex,
     manifest_sha256: Sha256,
     qualification_artifact_sha256: Sha256,
     pod_template_contract_sha256: Sha256,
     pod_template_binding_sha256: Sha256,
     artifact_inventory_sha256: Sha256,
     callable_inventory_sha256: Sha256,
     handler_allowlist_sha256: Sha256,
     claim_contract: "frozen_action_cohort_join_v2"}
  ],
  crash_canary_inventory_contract:
      "resource_action_crash_canary_inventory_v1",
  required_crash_canary_inventory_sha256: Sha256
}

ResourceActionQualificationPolicyRefV1 = {
  path: "/etc/skypilot/resource-actions/qualification-policy.json",
  byte_size: PositiveInteger,  # <= 65536
  sha256: Sha256
}

QualifiedResourceActionRolePodTemplateV1 = {
  version: 1,
  contract: "qualified_resource_action_role_pod_template_v1",
  template_json: Text
      # exact compact UTF-8 canonical JSON for the normalized raw
      # apps/v1 Deployment.spec.template value
}

QualifiedResourceActionRoleDeploymentV1 = {
  version: 1,
  role: "api" | "ordinary-executor" | "controller",
  namespace: Text,
  deployment_name: Text,
  deployment_uid: Text,
  generation: PositiveInteger,
  observed_generation: PositiveInteger,  # exactly generation
  desired_replicas: PositiveInteger,
  updated_replicas: PositiveInteger,      # exactly desired_replicas
  ready_replicas: PositiveInteger,        # exactly desired_replicas
  available_replicas: PositiveInteger,    # exactly desired_replicas
  unavailable_replicas: 0,
  pod_template: QualifiedResourceActionRolePodTemplateV1,
  pod_template_sha256: Sha256,
  oci_manifest_digest: "sha256:" + 64LowerHex,
  source_commit: 40LowerHex,
  artifact_inventory_sha256: Sha256
}

ResourceActionDeploymentInventoryV1 = {
  version: 1,
  contract: "resource_action_deployment_inventory_v1",
  deployments: [QualifiedResourceActionRoleDeploymentV1]
      # exactly API, ordinary-executor, controller in role order
}

ResourceActionCrashCanaryBoundaryV1 =
  "preparation_identity_publication" |
  "weighted_capacity_admission" |
  "admission_commit_before_approval" |
  "approval_before_pre_submit" |
  "dual_dispatcher_due_discovery" |
  "request_commit_before_materialization_ack" |
  "claim_before_initial_progress" |
  "provider_progress_checkpoints" |
  "skylet_job_outbox_runtime" |
  "provider_result_before_terminalization" |
  "terminalization_before_serve_reduction" |
  "retry_reduction_before_due_observation" |
  "retry_materialization_worker_handoff" |
  "partial_launch_supersession_cleanup" |
  "role_eviction_leadership_change" |
  "compatible_image_rollback_reupgrade" |
  "cohort_selection_retirement" |
  "crash_canary_lifecycle" |
  "policy_rotation_mixed_roles" |
  "mixed_path_last_capacity_unit"

ResourceActionCrashCanaryRequirementV1 = {
  sequence: PositiveInteger,  # exactly 1..20
  boundary_id: ResourceActionCrashCanaryBoundaryV1
}

ResourceActionRequiredCrashCanaryInventoryV1 = {
  version: 1,
  contract: "resource_action_crash_canary_inventory_v1",
  requirements: [ResourceActionCrashCanaryRequirementV1]
      # exactly the 20 enum values above in that order
}

# Frozen pre-039 candidate reader. Live qualification uses
# ResourceActionCandidateBindingV2 above and never promotes this value.
ResourceActionCandidateBindingV1 = {
  version: 1,
  qualification_policy_sha256: Sha256,
  schema_heads: AuthoritySchemaHeadsV1,
  deployment_inventory: ResourceActionDeploymentInventoryV1,
  deployment_inventory_sha256: Sha256,
  selected_cohort: ApprovedAuthorityCohortArtifactV1,
  selected_cohort_sha256: Sha256,
  capacity_profile: ServeActionCapacityProfileV1,
  capacity_profile_sha256: Sha256,
  elected_version_identity: ServeServiceVersionSpecIdentityV1,
  elected_version_identity_sha256: Sha256,
  live_replica_identity_inventory: HashedCanonicalObjectV1,
  required_crash_canary_inventory:
      ResourceActionRequiredCrashCanaryInventoryV1,
  required_crash_canary_inventory_sha256: Sha256
}
```

`QualifiedResourceActionRolePodTemplateV1` has one exact preimage. The builder
reads the raw JSON value at `/spec/template` from the live `apps/v1` Deployment;
it does not use YAML, a typed-client `to_dict()` result, a ReplicaSet template,
or a Pod projection. The source object has exactly `metadata` and `spec`.
Metadata has labels, optional annotations, and only an optional null
`creationTimestamp`; the latter is removed and absent/null annotations
normalize to the empty object. Labels are nonempty, labels/annotations are
sorted exact-text maps, and the controller-owned `pod-template-hash` label is
forbidden. Spec is the complete nonempty raw JSON object and has a nonempty
container list with globally unique exact-text names across normal, init, and
ephemeral containers. The complete tree permits only exact JSON scalar/container
types, NFC text without NUL, signed-int64 integers, at most 32 container levels
and 8,192 aggregate members, and no aliases/cycles; floats, duplicate keys,
subclasses, and noncanonical text reject. Existing resource-action canonical
JSON—sorted keys, compact separators, NFC, UTF-8—encodes the normalized object
into `template_json`. The wrapper's canonical SHA-256, including its version
and contract, is `pod_template_sha256`; both the wrapper and digest are embedded
so every reader recomputes it. The complete candidate retains the existing
65,536-byte outer bound.

The deployment inventory is stable across an ordinary same-template Pod
replacement: the builder proves every current ready Pod uses the exact role
artifact, but Pod names and UIDs are not trust identity and are not bound.
Deployment UID, generation, template hash, status counts, and role artifact are
bound, so an unrecorded rollout, scale drift, partial availability, recreated
Deployment, or artifact change rejects. Each deployment role artifact is byte-
equal to the entry in the set named by its selection slot. The selected cohort
is byte-equal to one approved cohort in the set named by the cohort slot; its
manifest and handler hashes bind the static seed and handler inventory, and its
claim contract binds claim behavior. The complete four-slot selection must be
one exact policy compatibility-inventory member. The elected identity's
capacity profile is byte-equal to the separately bound closed profile and its
provider profile is `pod_cluster_v1`. Every redundant digest above is
recomputed. The canonical SHA-256 of the complete candidate object is
`qualification_binding_sha256`.

Every delegated `HashedCanonicalObjectV1.value` uses the same bounded recursive
JSON rules: exact built-in scalar/container types, signed-int64 integers, NFC
text, the 32-level/8,192-member bounds, and no shared containers or cycles.
Candidate construction canonical-round-trips the live-replica inventory and
retains its immutable canonical bytes as a private snapshot. Both candidate
serialization and `validate_for_policy()` revalidate the current delegated
object and compare it with that snapshot. A nested mutation therefore rejects
even if a caller also recomputes the delegated object's inner digest.

The crash inventory is the separate canonical checked-in
`sky/serve/resource_action_artifacts/provider_authority_v1/crash_canary_inventory.json`
artifact. Each ordered boundary corresponds one-for-one to numbered fault
category 1--20 below, and PASS evidence for a boundary covers every subpoint in
that numbered category rather than one representative crash. Promotion
requires the successful live result inventory to contain every required ID and
recompute to the policy hash. Consistent with the existing checked-in JSON
artifact convention, the repository file is the canonical payload plus exactly
one LF; parsing removes only that required LF, while the policy and candidate
bind the typed payload's canonical SHA-256. Both files and every binding contract reject
unknown keys, duplicates, wrong order, wrong counts, noncanonical bytes,
floats, or scalar subclasses. The chart packages the policy as an immutable
ConfigMap key mounted by read-only `subPath`; the exact ConfigMap name, byte
size, and SHA-256 annotations are
`skypilot.co/resource-action-qualification-policy-config-map`,
`skypilot.co/resource-action-qualification-policy-byte-size`, and
`skypilot.co/resource-action-qualification-policy-sha256` in every API Pod
template. The fixed volume name is
`skypilot-resource-action-qualification-policy`, the fixed key is
`qualification-policy.json`, and the fixed mount path is the policy-ref path
above. Those annotation names and the volume name are chart-reserved even when
unconfigured; configurable mount paths are POSIX-cleaned before component-wise
comparison, so exact, ancestor, descendant, trailing-slash, repeated-slash,
dot-segment, and parent-segment aliases of the fixed path all reject. Live
Deployment/ReplicaSet/Pod projection is rechecked before use.
Changing policy bytes therefore rolls the API role and invalidates remembered
proof; request bodies and database rows cannot amend it.

For an upgrade using old stored `--reuse-values`, absence of the entire
`resourceActions` object or only its `qualificationPolicy` key is the sole
backward-compatible exception: Helm resolves either absence to the exact
disabled `{repoPath: "", byteSize: 0, sha256: ""}` triple and projects nothing.
An explicitly present null or non-object policy, a partial object, an extra key,
or any nonempty size/hash/path drift still fails template rendering. This
compatibility rule supplies no policy bytes and cannot activate authority.

`ApprovedOCIArtifactV1` discriminates OCI manifest and config digests and binds
the executable/source inventory. `ApprovedAuthorityCohortArtifactV1` additionally
binds the derived cohort ID, full static-seed hash, renderer/artifact/callable
inventory, Pod-template/RBAC/admission/network contract, handler inventory, and
claim contract. A deployment set is one coherent role/cohort release. The
initial policy contains exactly one set: the exact M4 merge artifacts and
cohort, with elected and rollback both naming that set. There is deliberately no
invented pre-M4 post-owner rollback binary: until the first compatible rotation,
an M4 authority incident closes admission and uses the same exact M4 artifact or
a forward fix. The first head-039 `COMPATIBLE_IMAGE_ROTATION` successor contains
exactly the already-qualified M4 rollback set and the exact already-merged M5a elected set.
It admits their complete attested 16-way role/cohort compatibility matrix. The
later `ROLLBACK_EVIDENCE_CLOSURE` and `SCHEMA_HEAD_ADVANCE` successors instead
contain exactly the validated one-set M5a selection declared above. No arm
admits a tag-only, PR-head-only, guessed, mutable, or third artifact set.
It is a closed, canonical, at-most-65,536-byte read-only projected file whose
hash is bound into the API Pod template. Missing, malformed, duplicate-key,
unordered, drifted, or unapproved bytes make all three API008 proofs
unavailable. Request bodies, headers, environment strings, controller-written
rows, and caller-supplied revision/digest strings are not trust sources.

The repository cannot contain the initial policy payload until the final M4
merge commit, immutable OCI manifest digests, installed-artifact inventories,
and qualified cohort artifacts exist. Until qualification freezes those exact
values, chart values keep the policy reference empty, render no ConfigMap or
mount, and the fixed-path loader fails closed. Qualification adds the canonical
policy file and its exact repo path/size/hash values in a reviewed release
change; projection alone does not enable authority, promotion, or dispatch.
This prerequisite tranche implements the typed deployment/crash/candidate
contracts, checked-in crash requirement golden, descriptor-safe exact policy
loader, and empty-by-default chart projection boundary. It does not implement
the API008 proof builders, live Kubernetes projection verifier, transition
writes, promotion, runtime authority, or dispatch.

All three API008 proof transactions run only through a server-created,
nonpooled physical connection from the configured consolidated PostgreSQL
engine and a session-affine database endpoint. PgBouncer transaction/statement
pooling or any proxy that can change backend sessions is forbidden for both
these locks and the migration runner; direct or session-pooling mode is
required. No caller may supply a connection, URI, search path, schema-head
string, policy digest, or approved inventory. Before beginning the proof
transaction, that connection acquires session-level
`pg_advisory_lock_shared(hashtext(...))` for
`skypilot:alembic:api_requests_db`, `skypilot:alembic:serve_db`, and
`skypilot:alembic:state_db` in that fixed order. These are the migration
runner's exact exclusive-lock keys, so a migration and proof cannot overlap
while independent proof readers may proceed. Acquiring them before `BEGIN`
prevents a wait from fixing a pre-migration MVCC snapshot. The server then
begins `REPEATABLE READ`, fixes the server-owned search path, proves the
configured `current_database()` /
`current_schema()`, and requires exactly one row—not merely a maximum or
caller string—in `alembic_version_api_requests_db`,
`alembic_version_serve_state_db`, and `alembic_version_state_db`, equal to
`008`, the exact locked active policy's `039 | 040`, and `028`. It also proves
that every counted current writer-process API-instance row carries the exact
built-in PostgreSQL request-storage and request-queue backend identifiers and
`execution_quiescence_capable=true`; API008's migration defaults
`unknown`/false are never promotable evidence. Every ordinary
activation/dispatch/promotion proof requires that exact policy/head equality;
the sole mismatch reader is the forward-only `SCHEMA_HEAD_ADVANCE` builder in
the explicitly closed physical-040/policy-039 transition. They then take the documented service/owner/cohort /
reference/coverage locks, perform every proof read inside that one repeatable
snapshot, and revalidate the service and cohort immediately before commit. A
concurrent migration cannot pass between head proof and state transition. It
commits or rolls back, then releases the three session locks in reverse order
and closes the physical connection; every error path does the same cleanup.
The pre-transaction lock calls run in DBAPI autocommit so SQLAlchemy cannot
implicitly begin a transaction. Acquisition has a server-owned bounded deadline
shorter than the request claim lease; timeout yields a closed unavailable proof
and never dispatches or transitions state.

Approved fingerprints and minima come only from a closed
`ResourceActionQualificationPolicyV2` file projected into the API role by the
current chart and hash-bound in its Pod template. Startup byte-validates that
file and exposes only the immutable parsed value to the proof builder. Request
bodies, headers, environment strings, database rows written by controllers,
and caller-supplied revision or digest strings are not trust sources. Missing,
malformed, multiply projected, or changed policy bytes make every API008 proof
unavailable.

Freshness is computed from a database-clock timestamp captured inside the
proof transaction and is fixed at five minutes. The builders reject one,
duplicate, stale, future, mixed-identity, or drifted registration; an unknown,
API005, API006, or API007 actual API-request head; a wrong fence/window; or evidence
older than five minutes. Shadow activation retains the scaled-to-zero /
canonical-incarnation eligibility check and never mints identity onto a
name-only provider resource. API006 remains only the progress substrate and is
not activation evidence. The strong API008 proof builders and transition
writes described here are not implemented by the current dark foundation; an
API008 schema head alone proves only readiness to land them.
After promotion, an image that ignores authoritative action rows is forbidden.
Rollback to any pre-action-aware image is unsupported after the first
authoritative promotion: additive schema cannot stop such a binary from
running the legacy mutation path. Application rollback uses a feature-aware
compatible image and keeps the additive heads. Returning a service to legacy
is deferred until a separately reviewed drain protocol exists.

Post-promotion binary change is a monotonic authority-policy rotation, not a
new qualification epoch, in-place policy edit, or ownership rollback. M4 ships
this protocol before authority is enabled. Its closed
`ServeAuthorityPolicyRotationProofV2`'s `COMPATIBLE_IMAGE_ROTATION` arm binds:

- service/incarnation, owner and lifecycle fences, current authoritative mode,
  predecessor policy epoch/hash, and actual schema heads;
- canonical successor `ResourceActionQualificationPolicyV2` bytes/hash;
- the exact normal elected-successor merge commit, OCI manifest/config
  digests, embedded commit, executable/source inventories, and authenticated claim-disabled
  staging attestations for API, controller, ordinary executor, and the new
  immutable authority cohort;
- the still-approved exact predecessor/rollback artifacts and feature-aware
  cohort used for application rollback;
- the complete canonical 16-selection cross-product proving every API,
  ordinary-executor, controller, and selected-cohort rollback/elected mixture
  accepted by the successor policy;
- the current/elected version identity, every live replica version/capacity
  identity, selected/staged cohort inventories, handler/claim contracts, and
  the exact empty nonterminal-work inventory; and
- PostgreSQL start/completion times and the operator-reviewed rotation reason
  `COMPATIBLE_IMAGE_ROTATION`. For the first head-039 cleanup rotation these
  generic slots are exactly M5a elected and M4 rollback; after head 040 they
  may name a later normally merged elected set and its still-qualified 040
  rollback set without changing the validator.

The policy table is a physically closed lineage with no committed staging
state. `ACTIVE` permits only `OPEN | DRAINING | CLOSED`, has
`created_at == activated_at`, and has no supersession time; `SUPERSEDED` is
exactly `CLOSED` with nonnull activation and supersession times. The
same-service predecessor self-FK, one-root partial unique, one-
successor partial unique, and one-ACTIVE partial unique prohibit cross-service
ancestry, duplicate initial roots, forks, and two active policies.
`INITIAL_PROMOTION` has no predecessor; every other reason requires exactly
one. `COMPATIBLE_IMAGE_ROTATION` is same-head and installs the exact two-set
rollback/elected policy named by its proof (M4/M5a for the first 039 use).
`ROLLBACK_EVIDENCE_CLOSURE` is same-head 039, requires the
completed M5a -> M4 -> M5a matrix, installs the exact one-set M5a policy
directly as `ACTIVE/CLOSED` with `ACTIVATE_CLOSED`, and durably removes M4 from
the compatibility inventory. `SCHEMA_HEAD_ADVANCE` requires that closed
rollback-closure predecessor, actual exact Serve040, the same one-set M5a
artifacts, and a successor policy differing in head only from 039 to 040; it is
also inserted `ACTIVE/CLOSED` before a separately fenced reopen. Immutable
policy/proof/inventory bytes are never rewritten; only the
active row's admission state/time and its final supersession state/time may
advance. Initial activation is revision one with a caller-minted
`ACTIVATE` operation UUID. Every later admission/supersession edge mints one
operation UUID, compare-and-swaps the exact prior revision, advances revision
by exactly one, and stores the closed operation kind. Unknown results adopt
only exact state/revision/operation ID; a later legal edge is reported as
supersession, never mistaken for an earlier acknowledgement. This closes
`OPEN -> DRAINING -> CLOSED -> OPEN` ABA even when logical timestamps repeat.
Every admission edge sets `admission_changed_at = GREATEST(clock_timestamp(),
prior.admission_changed_at, activated_at)`. Compatible activation uses one
logical time equal to `GREATEST(clock_timestamp(), predecessor.created_at,
predecessor.activated_at, predecessor.admission_changed_at,
rotation_proof.completed_at)` for the successor's equal
`created_at`/`activated_at`/`admission_changed_at` and the predecessor's
`superseded_at`. Initial promotion likewise inserts its root directly as
`ACTIVE/OPEN`, revision one/`ACTIVATE`, at one logical time no earlier than its
qualification proof.
Thus a backward clock step cannot invert lineage or state times. The service
lock plus the physical FK/unique/CHECK set, not typed-code discipline alone,
linearizes activation and rejects a second successor.

The #1240 head cutover is a closed global forward-recovery protocol. First, at
exact Serve039, the all-M5a deployment completes every mixed selection, rolls
to exact M4, re-upgrades to exact M5a, and records the complete closure
evidence. Under the service/policy locks each authoritative service then drains
to zero bound/nonterminal work and rotates to its one-set
`ROLLBACK_EVIDENCE_CLOSURE` successor in `ACTIVE/CLOSED`. The global migration
entrypoint additionally requires zero services in `shadow`, zero private shadow
requests/parents in a nonterminal phase, zero nonterminal actions/requests/
references, every active authoritative policy to have exactly that closure
reason/head/set, and every running API/controller/ordinary-executor/authority
cohort to attest the same 039/040-aware M5a artifact. Legacy services are inert
and do not block. These database predicates are revalidated while holding the
class-2 policy table before the class-9 DDL lock, so selecting target 040 in
deployment configuration cannot bypass the server-owned gate.

The revision body then drops the default, post-reflects the default-free
catalog, installs the exact one-shot handoff in the current migration context,
and returns with every lock held. Alembic advances its version row to 040, and
the registered post-head-apply callback on that same connection/transaction
re-reads actual 040 plus the catalog and locked predecessors, constructs one
predecessor-bound catalog proof per service, supersedes each closure row before
inserting its one-set head-040 `SCHEMA_HEAD_ADVANCE` successor in
`ACTIVE/CLOSED`, and final-validates the set. The outer Alembic transaction
alone then commits atomically. No committed physical-head/policy-head mismatch
exists. A callback failure rolls back DDL, version row, and policies; a crash
before commit retains exact 039 plus the closed closure policies; an
acknowledgement-lost commit exact-reads 040 plus the complete closed successor
set and adopts it. Any partial catalog/version/policy set is corruption and
startup remains closed.
All API/controller/executor processes and the selected cohort then restart or
re-attest under actual head 040. Only after their fresh registrations, process
owners, deployment selection, empty nonterminal inventory, and policy/head
equality validate may the new policy CAS `CLOSED -> OPEN`. Retained head-039
policies, lineages, receipts, and projections remain readable historical bytes
but cannot authorize new 040 work. No branch reinstates M4, re-adds the
default, stamps 039, demotes a service, or reopens a head-039 policy.

The exact M5a merge digest cannot exist until M5a is merged. Before that merge,
the promoted exact-M4 route must independently complete a new fixed 24-hour,
100-launch, 100-down window with zero eligible legacy routing across manager
scale, update replacement, whole-service cleanup, failed-service purge,
launch-cancel, and recovery sources. PR-head M5a images may run source and
claim-disabled staging tests, but never satisfy merge-artifact or deployment
evidence. After those M4 gates and exact-head M5a CI pass, M5a may merge and its
exact merge image is then built and staged.

Staging uses separate non-serving, claim-disabled role Pods and a new immutable
authority cohort. The API verifier independently reads their Kubernetes owner
chains, `/bootstrapz` artifact inventories, OCI digests, and embedded commit;
the staged Pods cannot receive an API request, become selected, or perform
provider I/O. A tag, Helm value, operator-supplied hash, or registry claim
without these observations grants no approval.

Rotation then executes this fail-closed sequence:

1. under owner -> service -> active-policy locks, change only the predecessor
   `ACTIVE` row from `OPEN` to `DRAINING`; every new preparation, action/
   cleanup admission, and service-version update rechecks that row and stops,
   while already-bound predecessor-epoch actions alone may claim, retry,
   checkpoint, and reduce;
2. require
   zero leased private requests, zero nonterminal actions, zero active cohort
   references, zero private shadow work, and no ambiguous/unreduced effect;
   blocked work blocks rotation rather than being forgotten; then change the
   predecessor to `CLOSED`, at which point every new claim and fresh pre-I/O
   intent also stops;
3. keep the candidate policy/proof only in claim-disabled staging while it is
   validated. One service-locked transaction then revalidates all attestations
   and the still-empty inventory, changes the predecessor from
   `ACTIVE/CLOSED` to `SUPERSEDED/CLOSED`, and inserts the successor directly
   as `ACTIVE/OPEN` with the one logical activation time above. A lost commit
   result exact-reads and adopts only that paired predecessor/successor state;
   no durable `STAGED` row can expire and wedge the unique successor slot; and
4. resume admission and roll exact-digest API -> ordinary executor -> controller
   -> selected authority cohort. Every mixed-version proof resolves all four
   slots through an exact compatibility-inventory selection in the active
   successor policy; leaf membership alone is insufficient.

Failure before the step-3 commit leaves no successor row and may reopen the unchanged predecessor only after a locked
inventory proves either (a) empty work or (b) every remaining bound action is
still byte-exactly recoverable under that predecessor; a committed but
acknowledgement-lost step 3 exact-adopts the active successor, and any later
failure keeps that successor policy active
and uses a forward fix or the exact M4 compatible image that successor policy
still approves. No step changes `authoritative`, rewrites a policy row, lowers
schema heads, or invokes the legacy adapter. All new `ACTION_ACTIVE` references
bind the successor epoch; the zero-nonterminal precondition means there is no
old action to reinterpret. Policy rows are immutable history after
`SUPERSEDED`.

The M5a deployment gate exercises the all-elected M5a selection, every one of
the 16 approved mixed selections, application rollback to the all-rollback M4
selection, and re-upgrade to all-elected M5a while the same successor policy
remains active and the service remains authoritative. It rechecks every
launch/down/cleanup/recovery source,
the mixed weighted budget, crash/HA inventory, and zero eligible legacy
routing. The exact-M4 rollback artifact necessarily still contains the four
temporary wrappers; under the same successor policy they must remain
unreachable and produce zero eligible legacy admission/mutation. Source and
image symbol absence is asserted after the first M5a deployment, again after
the M5a re-upgrade, and throughout the final M5a soak. After the re-upgrade, the exact M5a
merge must remain deployed for a new uninterrupted 86,400-second window with
at least 100 clean launch graphs, 100 clean down graphs, the complete crash/HA
inventory, and zero eligible legacy routing; no M4 or PR-head interval counts.
Only the later evidence-closure PR may atomically mark the bundle `removed`
with that shared soak plus one M5a source-removal SHA, exact-head CI, normal
merge, and deployment tuple.

## Migration and stacked implementation

The implementation stack began as:

- [PR #1190](https://github.com/boltz-bio/skypilot/pull/1190), the dark durable
  action foundation described through M3 below; and
- [PR #1191](https://github.com/boltz-bio/skypilot/pull/1191), a preparatory
  change that only isolates the process-local launch/down ownership behind a
  named legacy runtime so the eventual M5 deletion has one reviewable boundary.

[PR #1232](https://github.com/boltz-bio/skypilot/pull/1232) subsequently
shipped the P2a preflight-only authority cohort at merge commit
`4c91d3345ccb5f19538c9f8376c5e7403f5644cc`. Its runtime commit is
`3e6d2c92c7995bf41d25cfcc31e58107860aabfe` and its closed-contract commit is
`4232b9aac166d1202dd036eba8e752ab6f234640`. Those shipped bytes establish the
Serve034 release-ledger/P2a baseline; they are not P3 provider-runtime or M4
authority evidence.

PR #1191 merged early as behavior-preserving commit
`a4169fc8daab9583fdd60498eb788a20fcc3634c` and was deployed dark in Helm
revisions 77--79 as image
`sha256:c4477cbd7b51d755bcff17d58d142135d3642bb4a71e8930c7baf5e47c5da79e`.
All three ordinary roles reached two ready replicas with zero restarts. This
was not M5: the commit retains every legacy/shadow mutation path and only moves
its process-local fields behind `_LegacyReplicaMutationRuntime`. Eligible-path
M5a removal therefore requires a new blocked stacked removal PR. That PR cannot
merge until
M4 integrates and live-qualifies the implemented renderer, dispatcher,
provider I/O, reducer projection, and recovery; at
least 24 hours and 100 launches plus 100 downs establish clean live evidence
with zero unresolved divergence; every crash-boundary canary passes; the
compatible M5a/M4 rollback protocol and finite mixed-selection suite pass; and all applicable `PLA-M5-*` source, test,
telemetry, and release-window gates pass. It must delete the four eligible-path
mixed-mode wrappers, authoritative eligible-path fallback-to-legacy branches,
and process-local-state reads, and add final-steady-state tests proving every
surviving writer supplies an explicit route. It explicitly retains private-
shadow linked-admission nonrepresentability fallback, both permanent fallback
histories/codecs/stores, their GC/adoption validation, and excluded/unpromoted
legacy adapters. The same #1240 image carries the dormant Serve040 migration;
only its second gated deployment phase drops the deprecated
`execution_route='LEGACY_CONTROLLER'` server default after M4 rollback closes. Global deletion
of the isolated legacy runtime and `PLA-GAP-005` remain
M5b gates. The early merge of the isolation seam does not satisfy or waive any
removal gate.

Building on the merged foundation, isolation seam, and P2a preflight tranche,
the remaining bounded program has exactly two stacked PRs; the first contains
several internal implementation phases:

- [PR #1239](https://github.com/boltz-bio/skypilot/pull/1239) is the draft M4
  feature parent; and
- [PR #1240](https://github.com/boltz-bio/skypilot/pull/1240) is its draft,
  merge-blocked M5a removal child stacked directly on #1239.

1. one M4 feature PR implements the complete central-PostgreSQL, consolidated,
   non-pool, Kubernetes `pod_cluster_v1` vertical slice; it is deployed dark,
   then private-shadow, then promoted for one canary service only after the
   fixed gates below; and
2. one immediately created draft M5a stack child removes eligible-path
   authoritative legacy ownership/fallback branches while retaining the
   private-shadow linked-admission fallback. Its exact merge gate is at least 86,400 seconds,
   100 clean represented launch graphs, 100 clean represented down graphs,
   zero unresolved divergence/blockers, the complete crash/HA inventory, and
   closure of its applicable source/test/telemetry/rollback gates.

M5a does not depend on `PLA-GAP-005`, which is pooled-worker actuation outside
the bounded non-pool cohort. M5b is the later global retirement of
`_LegacyReplicaMutationRuntime`, its proxies, and characterization test after
pool and orphan cleanup, non-consolidated and SQLite stores, and every other
supported profile migrate or retire. Exact-incarnation non-pool failed-service
purge remains M4/M5a. Global symbol absence is not
an M5a merge gate. Both new PR URLs are added here immediately after creation;
exact merge/deployment evidence is added only after it exists. The cleanup PR
remains draft until its pre-merge gate is objectively satisfied.

The mandatory child exists before M4 has a normal merge SHA. On that child
only, all four bundle rows therefore use the same `blocked` value from
`present`, with `blocker.draft_source_removal: true`, the exact draft PR URL,
and byte-equivalent gate evidence. This is an out-of-band review state, not a
rollout-state advance: `introduced_by` remains the exact M4 source commit,
`required_feature_merge` remains null, and the checker requires all four
locators absent together. The M4 parent retains ordinary `present` rows and
present locators. Once the exact normal M4 merge and live gate exist, the child
resumes all four rows atomically through `present -> gating ->
ready_to_remove -> removal_in_progress`; only then may it record the feature
merge and become mergeable. No PR-head, draft blocker, or source absence
supplies qualification or deployment evidence.

The operational order is exact:

1. commit and adversarially review this canonical design;
2. open the M4 PR and its draft M5a child with `gh-stack`;
3. merge M4 only after CI and review, then build and deploy the exact merge SHA
   dark with authority disabled;
4. enable the attested cohort and private shadow for the one canary, collect
   the fixed window/graph/crash/HA evidence under one candidate epoch, and
   promote and verify that canary;
5. while the exact M4 merge remains authoritative, collect the separate fixed
   24-hour/100+100 zero-legacy-route window across every launch/down/cleanup/
   recovery source, rebase/update M5a with that evidence, and pass exact-head
   steady-state CI;
6. merge M5a, build its exact merge image, attest it in claim-disabled staging,
   and perform the admission-closed monotonic authority-policy rotation;
7. deploy the exact M5a merge, verify zero eligible legacy routing, exercise the
   approved M4 application rollback and M5a re-upgrade, leave excluded adapters
   and linked-admission fallback intact, and complete the new exact-M5a/
   Serve039 24-hour/100+100 crash/HA soak; then persist rollback closure, drain
   all work, run the same image's atomic Serve040 policy/schema advance, and
   complete a second exact-M5a/Serve040 24-hour/100+100 crash/HA soak; and
8. use a normal evidence-closure PR after that final soak to move the four-row atomic removal bundle
   to `removed` with its one exact removal/CI/merge/deployment tuple.

PR-head images may be used for pre-merge smoke testing, but they never satisfy
an exact-merge deployment or authority-policy gate. The initial frozen
qualification policy contains one exact M4 deployment set, named by both its
elected and rollback fields. Exact M5a artifacts enter a two-set successor
policy only after their normal merge, server-attested claim-disabled staging,
and the complete finite mixed-selection compatibility suite.

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
- Extract a connection-borrowing request/queue insert helper from the existing
  PostgreSQL request creator. Correlated materialization and adoption use that
  helper inside the action transaction; ordinary request creation keeps its
  existing behavior.
- Implement the literal namespace, canonical preimages, pristine-request
  validation, and mismatch-to-`BLOCKED` behavior above.
- Before creating any runtime correlation, make request retention skip a
  correlated terminal request until its attempt is `SETTLED` in both candidate
  selection (before log unlink) and the final delete predicate; test both the
  skip and deletion after the terminal snapshot.
- Keep generic request terminalization action-unaware. Snapshot terminal
  evidence only in the separate reducer transaction and add inverse-concurrency
  tests for the declared lock order.
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

- Preserve upstream's shipped request-classification Serve032. Install all
  unshipped resource-action service/replica columns, logical evidence,
  cohort/reference retention, decision coverage, and coverage-only attempts in
  one guarded Serve033 transaction. Refuse schema down while retaining the
  additive state.
- Verify unique Alembic lineage; fresh 031 -> upstream 032 -> combined 033 and
  already-at-upstream-032 -> 033 PostgreSQL upgrades; upstream telemetry and
  ordinary-row preservation; rejection of the abandoned feature-032 stamp,
  nonempty evidence, or nonlegacy/nonnull action state before mutation; empty
  hybrid/lost-ack convergence by transactional replacement of the proven-empty
  action graph; malformed portable-column rejection; exact reflected
  postconditions; SQLite columns only at 033; and downgrade refusal.
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

M2 foundation verification evidence through 2026-08-02:

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
- the closed, bounded provider locator, invocation, observation, outcome,
  shadow projection, and retry contracts have canonical byte/hash fixtures and
  action-specific success proof. The stricter frozen Kubernetes scope, exact
  launch and down execution configurations, completed-launch basis, all 20
  legal partial-launch cleanup shapes, and their full-spec byte/hash goldens
  are now implemented and locally verified. The five exact packaged artifacts,
  pure staged renderer, exact body/capsule cutover, and request/admitted
  normalizers now consume those launch values in an effect-free construction
  path. No manager/runtime mutation path consumes the result yet; and
- the PostgreSQL typed shadow store plus Serve033 coverage/cohort and promotion
  suites pass locally, including exact parent/child replay, retry-chain
  closure, reference fencing, coverage-only attempts, bounded canonical
  promotion inventory, outcome-aware replica coverage/sample-link validation,
  activation-window/hash fencing, retention protection, and lock races. The
  pure contract and promotion-audit slices received independent adversarial
  acceptance.

The typed store now validates a prepared cohort reference against the exact
immutable execution configuration. Its legacy-controller primitive performs
store-level `PREPARING -> SHADOW_ACTIVE` binding with the represented
coverage/parent graph; the selected-private contract above deliberately splits
capacity/PENDING selection from its sole linked-admission activation. Exact
replay adopts the applicable complete graph;
partial, crossed, stale-owner, or byte-unequal graphs fail closed. Focused
PostgreSQL state-store tests cover those transitions. This is not runtime
admission: no manager currently creates and discards the preparation
capability, calls the canonicalization/preflight client, or routes a real
legacy or action request through this primitive.

Manager/runtime admission, legacy SDK instrumentation, runtime integration and
live qualification of provider rendering/normalization, provider identity
propagation/readback, atomic action-to-Serve projection, and live shadow
evaluation remain M2 gates. No service is eligible for authority.

### M2a: preflight-only cohort bootstrap

- Implement the companion design's six closed preflight envelopes and strict
  private HTTPS server/client. The first evaluator is deliberately incapable
  of producing a complete launch or down response; after cohort acceptance it
  returns only
  `not_representable: preflight_unavailable_or_invalid`.
- Split bootstrap readiness from claim readiness. Bind health and preflight
  first; use `/livez` for liveness and `/bootstrapz` for both the Kubernetes
  startup and readiness probes while `/readyz` remains false; return 503 from
  preflight until the local worker has adopted the accepted cohort. The
  authority role must not call
  `executor.start()`, resolve a claim configuration, or claim the PR #1070
  queue in this tranche. Its `ServerInstanceLease` stays `ready=false` with
  `health_detail={"phase":"preflight-only"}`; no P2a path calls
  `set_ready(true)`.
- In a coordinator outside the read-only endpoint, byte-verify the complete
  projected manifest; observe the live Pod -> ReplicaSet -> Deployment and
  ServiceAccount identity using a timestamp obtained from the PostgreSQL clock
  before the reads; and register/merge/promote exactly two sorted, distinct,
  current ready-Pod attestations through the existing Serve033 compare-and-swap
  store. Both Pods must first be `/bootstrapz`-Ready; the first inserts its own
  registration only after Deployment `2/2`, and the peer reads/compare-and-swap
  appends its own registration only against the same Deployment generation /
  resourceVersion. The resulting two entries are sorted by Pod UID. Unequal
  identity or stale-version retry discards every observation, takes a new
  database timestamp, repeats, and fails closed. A same-Pod acceptance lease
  and watchdog refresh only the caller's timestamps and Pod/ReplicaSet
  resourceVersions while freezing Deployment resourceVersion and the identical
  Pod UID set; replacement or drift clears acceptance. A stale never-accepted
  `REGISTERING` row takes the separately fenced abort/tombstone path and its ID
  is never reused.
- Expand Helm inputs and the projected manifest to contain the qualified OCI
  manifest/config digests and exact qualification, Pod-template, artifact, and
  callable inventory references. The three installed-package refs must be
  relative, normalized, descriptor-opened without symlink traversal, regular,
  singly listed, and byte/hash/size verified. The canonical static manifest and
  external qualification are instead fixed absolute read-only `subPath` files
  from immutable ConfigMaps, with exact regular-file/canonical-byte/size/hash
  checks; neither is claimed as image-installed.
  The manifest also carries the complete release-specific Pod-template binding:
  every container, command/args, env name/value source, Secret/ConfigMap
  name/key/path, port, probe, volume/mount, database reference, ServiceAccount,
  selector, label, annotation, image, pull policy, security context, and
  scheduling/termination field. The installed artifact is the closed pure
  builder/projector, not an expected generic template. A single
  `$MANIFEST_SHA256` annotation placeholder makes the expected-template hash
  and subsequent static-manifest hash acyclic; live attestation verifies and
  substitutes only that path. The authority ServiceAccount keeps explicit token
  automount true, while its PodSpec sets automount false and uses one fixed
  `kube-api-access` projection (3,607-second token, root CA, and namespace) at
  the standard in-cluster credential path. This prevents admission from adding
  a dynamically named volume. The expected template also makes the
  `serviceAccount` alias, scheduler/container/probe defaults, zero priority,
  `PreemptLowerPriority`, and the two ordered 300-second NoExecute tolerations
  explicit; a configured toleration using either reserved key with an
  all-effects or NoExecute effect rejects. Deployment/ReplicaSet template
  `metadata.creationTimestamp` must
  be exactly null and is the sole verified-and-normalized API-storage default;
  the live Pod may additionally remove only its scheduler-assigned `nodeName`.
  The preflight Service is exactly
  `<full-name>-authority-preflight.<namespace>.svc`; P2a renders only the
  authority ingress policy, not a new isolating controller egress policy.
- Install Serve034's stable release ledger before any authority object apply.
  The blocking pre-install/pre-upgrade hook uses namespace plus Helm release
  name as its immutable anchor, descriptor-reads each exact chart-rendered live
  manifest, and commits the desired live/tombstone inventory. First worker
  registration locks and revalidates that ledger row, closing the preflight to
  registration race. Fully cleared and HA-off proposals still execute the
  hook; an empty disabled proposal cannot erase nonretired durable history.
- Merge/build the dark runtime first, inspect the immutable image's real
  manifest and config digests, then land a separate checked-in qualification
  artifact/values change for that already-built image. This qualification is a
  distinct immutable chart-packaged ConfigMap-projected file, not an artifact
  claimed to exist in the previously built image. Never fabricate
  self-referential qualification. Keep
  `resourceActions.authorityWorker.enabled=false` until that evidence is
  reviewed.
- No manager preflight call, preparation reference, shadow/action/request/queue
  row, workload/action-provider client, Kubernetes mutation, provider effect,
  or rolling replacement recovery is in M2a. The bootstrap's dedicated
  read-only Kubernetes observer may GET only its own Pod, owning ReplicaSet,
  exact versioned Deployment, and ServiceAccount. Drift after acceptance makes
  preflight unavailable; rolling refresh semantics are required before M3
  claim routing.

M2a is a transport and bootstrap tranche, not shadow evidence. Its tests and
dark deployment may prove that an immutable cohort can self-attest and reach
`ACCEPTING` without a readiness cycle, but cannot start an M3/M4 qualification
window.

### M3: dark dispatcher foundation; P3 canary completion

- API-request revision 006 adds the bounded provider-progress snapshot,
  retained provider-I/O watermark, claim-fenced monotonic write/read methods,
  typed domain hooks, retry-seed derivation, and fail-closed migration; ordinary
  attempts remain null.
- The generic store materializes a retry only after locking and validating the
  exact immediate predecessor and current attempt in ascending order. It
  byte-validates the complete inherited seed, rejects settled-lineage or
  lost-ack tampering, retains the null-progress fresh-cursor branch, and never
  adds an action lease or second queue.
- The Serve-owned pure API006 progress contract and reducer implement exact
  launch/down cursor validation, monotonic transitions, cross-attempt origin
  retention, request-terminal fallback, retry scheduling, supersession
  quiescence, and maximum-attempt blocking. The exact launch/down execution
  configurations and completed/partial down bases are frozen inputs to that
  contract.
- Before any private shadow dispatch, add
  `PrivateDispatchReadinessProofV2` and its specified atomic consumption with
  the
  exact Alembic/cohort/registration/claim reads above. The earlier
  `legacy -> shadow` transition separately requires
  `PrivateShadowActivationProofV2`; neither caller-provided evidence nor an
  activation proof can authorize dispatch.
- Add the narrow represented-only atomic private-request materializer. It
  creates and binds the only PR #1070 request/queue row in the same transaction
  that creates the represented child and authorizes `SHADOW_ACTIVE`, and
  persists the underscore routing alias exactly. Coverage-only decisions cannot
  enter it. Keep the existing legacy SDK binder restricted to its current public
  request names.
- The two action handler names have dedicated strict
  `ServeReplicaActionRequestReturnV1` encoders/decoders, and the two shadow
  handler names have dedicated strict `ServeShadowCandidateRequestReturnV1`
  encoders/decoders. The disjoint codecs cross-reject. A successful
  request with a null, malformed, unknown-key, wrong-kind, or default-encoded
  return is terminalized as `FAILED` with a persisted error instead of
  `SUCCEEDED` with an unusable value. The strict rule applies to those exact
  private names in both persistence backends; ordinary request names retain
  their existing codecs and behavior.
- Current shipped M3-foundation status: the four private handlers and their
  capability-filtered registrations exist, but intentionally raise before
  reading provider credentials or crossing a provider-I/O boundary. Dispatcher
  invocation of the pure renderer/normalizers, pre-I/O admission, provider
  checkpoint writes, request-to-reducer wiring, and atomic Serve projection
  remain dark. No synthetic/canary action has yet executed provider I/O through
  this shipped foundation. These are current-state statements, not the M3/P3
  completion contract.
- M3 completes only with the companion P3 tranche, after P2b observation,
  same-Pod/DRAINING renewal, rolling-registration gates, and both API008
  shadow-activation/dispatch proofs are implemented. P3 replaces the
  fail-closed implementation only for an explicitly selected represented
  synthetic/canary shadow request. Its handler invokes the extracted in-server
  provider seam under the active execution claim, performs the specified live
  CoreV1/Skylet I/O as the sole mutation path, and never enters an SDK
  submission path or creates a nested request. Authoritative action-handler
  canaries remain separately explicit and bounded. No ordinary service gains
  provider authority, and no `shadow -> authoritative` transition is legal
  until M4's 24-hour/100+100/zero-divergence/crash gates pass.

M3 foundation verification evidence on 2026-08-02:

- `tests/unit_tests/test_resource_actions_pg.py` passes against real PostgreSQL
  and covers lineage-safe retry materialization, exact lost-ack adoption,
  attempt-two tamper rejection,
  predecessor-before-current lock order, null-progress retry, operation-ID
  binding, claim expiry, and both terminalization/progress race directions;
- `tests/unit_tests/test_serve_resource_action_progress.py` passes and covers
  the closed cursor phase tables, effect origins, inherited retry seeds,
  request-terminal `P0/O/S/X` fallback,
  handler `S/R/U/B/Q` reduction, supersession quiescence, maximum-attempt
  blocking, and malformed/crossed/hash-invalid evidence;
- `tests/unit_tests/test_serve_resource_action_launch_execution_config.py` and
  `tests/unit_tests/test_serve_resource_action_down_execution_config.py` pass
  and freeze exact prepared-reference/cohort binding, completed-launch down,
  all 20 legal partial-launch cleanup shapes,
  40 full-spec byte/hash goldens, and the declared size bounds. Focused
  cases in `tests/unit_tests/test_serve_resource_action_state_pg.py` pass
  against PostgreSQL and cover linked admission and exact replay;
- `tests/unit_tests/test_serve_resource_action_return_codec.py` plus the
  focused generic request PostgreSQL/SQLite cases pass and prove strict round
  trip, exact request-name binding, and fail-closed persistence for null or
  malformed private returns; and
- the 60-case focused renderer/artifact/normalizer matrix and complete
  1,543-case `test_serve_resource_action*.py` matrix pass, the latter with the
  real PostgreSQL test URL. They cover the five packaged artifact preimages,
  descriptor-bound nonblocking regular-file resolution, exact staged
  call/input inventory, atomic
  exact-body/full-spec golden cutover, pure request/admitted normalization, and
  completed-capsule revalidation. The built-wheel test embeds all five
  artifacts byte-for-byte. Runtime executable and source-AST seals pass on
  CPython 3.10.20, 3.11.15, 3.12.13, 3.13.14, 3.14.3, and the CI image's
  3.14.6 after making nested code-object traversal identity-unique and using
  `Instruction.positions.lineno` with an exact-integer `starts_line` fallback;
  the exact one-fingerprint-per-minor allowlist remains fail closed; and
- these results verify pure contracts and dark persistence only. They provide
  no runtime integration or live qualification of the pure renderer and
  normalizers, live preflight, provider mutation/readback, runtime dispatcher,
  atomic Serve projection, shadow-parity, or crash-canary evidence. The
  reducer's maximum-attempt disposition is implemented, but emission of the
  named `attempt_domain_exhausted` operational event remains open.

M4 dark trust-fence PR-head evidence on 2026-08-03:

- `tests/unit_tests/test_serve_resource_action_reference_v2_pg.py` passes
  serially against real PostgreSQL. It covers the exact preparation/preflight
  lock order, strict revision-one/null-policy decoding, insert lost-ack
  adoption, immutable-row equality before/after every successful trust read,
  initial-root versus compatible-rotation successor policy selection, service /
  policy/cohort/handoff/two-lease/reference/current-instance drift, future and
  stale clocks, real row-lock contention, transaction-local settings, and the
  cumulative no-late-statement guard. It also proves that an exact launch
  canonicalization context succeeds without mutation, crossed capability,
  owner, lifecycle, or service-name context rejects without mutation, launch
  rejects an absent context, and down succeeds only with explicit no-context;
- the V2 transport/evaluator/runtime tests pass with the real TLS server. Eight
  simultaneous valid requests invoke exactly one blocked validator, create no
  queue, return the canonical V2 503 inside the original absolute deadline,
  discard its late typed result, and recover only for a new request after that
  read finishes. A slow-dripped body and evaluator share that same deadline;
  shutdown remains bounded around a never-returning evaluator and the stopped
  server object rejects restart so no daemon slot can cross generations;
- the database utility tests prove the shared and `authority-preflight`
  QueuePools are distinct, size one, and zero-overflow, freeze their short
  connect/checkout/server/TCP settings, and normalize the internal preflight
  namespace to the existing bounded `other` metrics label. A real-PostgreSQL
  plus real-TLS test holds the sole isolated checkout, proves eight concurrent
  requests invoke one evaluator and all return fixed V2 503 without opening a
  second physical connection, then releases the checkout and proves typed-200
  recovery on that same connection. Runtime wiring requests only the isolated
  engine and records the explicit three-connection persistent physical ceiling
  per authority Pod; and
- this is local PR-head evidence only. No manager invokes preparation in the
  deployed system, no enabled cohort or live valid preflight has been observed,
  and no capsule, action, request, claim, readiness, provider I/O, or authority
  evidence follows from these tests.

### M4: per-service authority

- One feature PR implements the remaining bounded vertical slice on top of the
  already merged P2a preflight/bootstrap tranche. Its internal order is the
  Serve038 V2 membership and retained release-anchor completion, additive
  Serve039 lineage/head/V2 proof builders, transport, manager admission and
  private shadow, action dispatch/reduction, then operational promotion.
- Before a complete preflight result or represented admission, land the native
  V2 seed/input and launch/down constructors, the six-role V2 artifact
  inventory and V2 callable inventory, the sole cleanup-target rederiver, and
  the fully expanded representability case inventory/enumerator and CI-only
  post-inventory goldens. Their repository
  inventories must prove that no V1 construction root or duplicated cleanup
  builder is reachable. Run the complete finite case tuple against the two
  content-addressed fixtures before enabling its live admission/pre-I/O calls.
- `sky/serve/replica_mutation_router.py` owns
  `ServeReplicaMutationRouter` and `LegacyServeReplicaMutationAdapter` as the
  one manager boundary. The module accepts closed manager callbacks/projections
  and never imports `replica_managers.py`, preventing a circular owner. At every launch,
  launch retry/cleanup, down, service cleanup, and failed-service purge
  admission it acquires the existing service/owner lock and rereads the live
  mode, lifecycle epoch, identity, eligibility, and cohort. The mode captured
  when a manager object was constructed is only a hint. Pools, orphan cleanup,
  non-consolidated/SQLite state, unsupported provider profiles, every capacity
  profile except `ordinary_ondemand_physical_width1_v1`, and incomplete
  identity route to `LegacyServeReplicaMutationAdapter` and create no partial
  action graph. The eligibility transaction releases all SQL locks before that
  call, so excluded paid/reserved behavior retains its existing adapter-owned
  DML and lock order.
- Serve038 implements the serialized migration, version-spec identities,
  pre-injection crash intents, attempt-exhaustion evidence, and immutable
  UUID authority-policy epochs above. Its manager admission, private request,
  claim, provider-context, recovery, and reducer boundaries accept only
  `ServeReplicaActionSpecV2` through
  `serve_replica_action_spec_from_value_v2()`; retained V1 readers are never on
  a live authorization path. The M4 PR also upgrades the removal checker to
  manifest schema 2, rejects nonnull provenance for planned absent artifacts,
  and enforces the four-row M5a atomic bundle before any runtime symbol is
  introduced.
- Serve039 then installs, under separate metadata, the lease owner/hash/process-
  scalar extension, process-supersession table, append-only execution-authority
  lineage, action selectors, shadow terminal histories, and permanent shadow
  admission-fallback, fallback-progress, and settlement commitments. Before any claimant
  starts, M4 composes the three-method same-engine terminal store into every
  process root, narrows private claims/terminalization to generation zero or one,
  implements cancellation intent plus quiesced owner acknowledgement, and uses
  only the homogeneous stale-owner/process-supersession/cold-recovery batch
  programs and their exact 16/16/32 bounds.
- The same pre-dispatch phase implements insert/exact-adopt authority API
  bootstrap identity and boot nonce, the closed bootstrap/bound/ready/
  rewarming/draining
  health machine, Serve-owned historical API-row GC, the fixed eagerly warmed
  no-burst LONG process pool, post-039 INSERT/BIND and current-owner-only RENEW /
  survivor acknowledgement, `SUPERSEDE_EXECUTION_OWNER`, and stable-Pod/process-
  chain attestation at claim-start, progress, pre-I/O, effect, and return. It also
  installs the V2 heads/policy/candidate/proof family, claim-start linearization,
  connection-borrowing API006 seams, settled-replay lineage validator, and
  PostgreSQL-versus-SQLite target-version routing. No live V2 admission or
  dispatch is legal at Serve038 or partial Serve039 runtime completion.
- A checked-in AST inventory covers all three current `_launch_replica()` call
  expressions, every `_terminate_replica()` source, `service._cleanup`, and
  `serve_utils._terminate_failed_services_locked`. Exact-incarnation non-pool
  whole-service and failed-service cleanup are in scope. Pool branches and
  `_terminate_orphaned_service_children_impl` carry explicit exclusion guards;
  any new direct launch/down/cancel call outside the router fails CI.
- Legacy-controller shadow keeps the existing sole mutation. Private shadow is
  a later represented-only phase: the manager still owns policy/admission, but
  it atomically creates the represented evidence and deterministic PR #1070
  request, then the private handler is the sole provider-effect owner. It uses
  the same frozen renderer/session that authority will use and captures the
  exact CoreV1/Skylet effect trace. It never also invokes the high-level legacy
  provider mutation. Qualification-window teardown uses request-associated
  `sdk.down()` rather than untracked direct `core.down()`; unsupported teardown
  remains explicit legacy and promotion-blocking.
- Every API-role replica hosts `ResourceActionDispatcherDaemon` and
  `ServeResourceActionReducerDaemon`; there is no singleton leader whose loss
  can strand work. The dispatcher reads at most 64 due candidates ordered by
  `(next_attempt_at, action_id)`. Discovery is nonlocking; each materialization
  is its existing short `FOR UPDATE SKIP LOCKED`/exact-adoption transaction.
  The reducer reads at most 64 terminal-request candidates ordered by
  `(action.updated_at, action_id)`, then takes the existing service/owner lock
  before action/attempt/request locks and uses replay-safe reduction. Competing
  API replicas either skip, observe a stale revision, or exact-adopt the same
  result; none creates a second attempt or projection.
- Both loops use PostgreSQL `clock_timestamp()`, a one-second active poll and
  bounded exponential idle poll up to five seconds. In-process wake events on
  action admission and request terminalization reduce latency but are never
  required for correctness. Per-candidate invariant failures durably block or
  quarantine that graph and do not stop later candidates; database loss backs
  off the whole loop without advancing state. The reducer revalidates the
  current service owner/lifecycle epoch inside the projection transaction; a
  handoff loser writes nothing and the new owner/API pass resumes the graph.
- Materialization, request correlation, journal-before-I/O, lost-ack exact
  adoption, and attempt retry retain the existing deterministic IDs and
  predecessor-before-current lock order. The sole execution lease remains the
  existing API request claim. There is no second queue, action lease, provider
  worker loop, or nested `sdk.launch()` / `sdk.down()` call inside a private
  handler.
- The provider runtime uses purpose-authenticated TLS preflight, the accepted
  two-Pod cohort, the pinned `pod_cluster_v1` renderer, one CoreV1 object
  session, and the action-keyed Skylet outbox/run token. Every external effect
  has an intent/progress commit before I/O and an exact readback after it. UID
  fences protect same-name successors; down succeeds only after exact absence.
- Bounded `list_reducible()` discovery locks action/attempt/request evidence in
  canonical order, invokes the pure reducer, and atomically projects the exact
  Serve replica/ordinary-slot/action-domain-event change, retry schedule, and
  reference release in one owner-fenced transaction. A crash before commit
  leaves the old Serve projection and the same reducible graph; replay is byte-
  idempotent.
- Action and legacy launch/down admission share the existing integer-equivalent
  `2P + D < 2C` budget and per-service down cap. Both kinds persist their
  `PROVISIONING` or `sky_down_status='RUNNING'` occupancy and owner-specific
  graph before dispatch, and terminal/no-I/O reduction releases it once.
- Promote only the isolated eligible service after
  `AuthoritativePromotionProofV2` passes. Noneligible services remain on their
  explicit legacy/shadow adapter without a global flag flip.
- Ship and test the admission-closed compatible policy-rotation protocol before
  promotion. M4's initial policy names M4 only; future exact merge artifacts
  cannot be guessed or preapproved.

M4 is not complete and no M4 authority evidence exists. These V2 contract
corrections are design-only: schema, parser, daemon, provider-executor, or
router scaffolding without the complete live admission-to-reduction path does
not satisfy the milestone. Authority remains disabled in code and deployment;
no service may be described as action-authoritative on the basis of the M1-M3
foundation tests or this corrected contract.

### M5: legacy ownership removal

For the eligible authoritative path, delete:

- launch/down SafeThread ownership;
- `_replica_to_request_id` as operation authority;
- `_failed_cleanup_retry_attempts` and `_failed_cleanup_retry_at`;
- monotonic/process-local cleanup scheduling; and
- restart-time inference that substitutes for a durable request/action link.

Compatibility readers may project action state into old status fields, but no
fallback branch may submit or retry an eligible authoritative mutation.

M5 has not started. The named fields and call sites carry deprecation markers,
but launch/down SafeThreads, `_replica_to_request_id`, cleanup retry maps and
clocks, and restart-time legacy inference still own live behavior. A dormant
journal beside those owners is not a completed restructuring.

Rows `PLA-M5-022`--`025` form one declared removal bundle. They have null
`introduced_by` while their symbols are absent/planned. The M4 feature branch
first lands all four symbols in one ordinary source commit; a subsequent
manifest-only commit records that already-resolvable parent SHA on every member
and the checker proves the symbols are present in that exact Git tree. This is
never a self-referential guessed commit. The PR is rebased before that
provenance commit and is merged with a normal merge that preserves it; squash
or a later history rewrite requires regenerating and rechecking the provenance.
Their lifecycle state advances atomically. The M5a source commit removes all
four, exact-head CI tests the final steady state, and all four later receive one
identical removal-SHA/normal-merge/deployment tuple after the exact M5a merge
rollout. The bundle's `required_feature_merge` stays null while planned and is
filled only after the normal M4 merge exists; from the first removal-gating
state onward the checker proves that merge is an ancestor of the M5a branch,
and at closure proves it precedes the retained M5a removal merge on first-parent
history. Each rollout stage records ordered exact merge SHAs and OCI digests,
the active authority-policy hash, UTC start/completion timestamps, clean graph
counts, exact-zero eligible legacy routes/unresolved crash intents/stale
claims/duplicate effects/divergences/blockers, and retained evidence locators.
The checker derives duration, enforces
the stage floors, resolves every commit, and proves each bundled symbol absent
in the introducing commit's parent and present in its tree.
Per-row source/test gates remain exact, but one member cannot become
`ready_to_remove`, `removal_in_progress`, or `removed` ahead of another.

Those M4 symbols are mixed-mode wrappers at call sites shared by legacy,
unpromoted, and authoritative services; they are not permission to fall back
after authority. Each rereads the locked service row. `authoritative` selects
the durable owner, and any admission/recovery failure blocks that graph without
calling the legacy adapter. `legacy` or an explicitly excluded profile selects
`LegacyServeReplicaMutationAdapter` before any action artifact exists. M5a
rewires those two permanent destinations directly and deletes the wrappers plus
their action-path reads of process-local state. Thus an unpromoted service can
remain legacy after the canary while the qualified authoritative route has no
compatibility seam.

M5a's merge gate is collected after M4 authority: a fixed 24-hour, 100 clean
launch, 100 clean down window covers manager scale/replacement, launch cancel,
whole-service cleanup, failed-service purge, and recovery with zero eligible
legacy calls. After merge, its exact digest enters only through the monotonic
policy rotation; deployment must include exact-M4 compatible rollback and
exact-M5a re-upgrade without changing authority mode, followed by a distinct
exact-M5a 24-hour/100+100 crash/HA soak with zero eligible legacy routing. The
bundle reaches `removed` only in the subsequent evidence-closure PR.

M5a has not started. The named process-local owner still controls all live
behavior, so the existing journal remains a dark foundation rather than a
completed restructuring.

### M5b: global legacy-runtime retirement

Delete `_LegacyReplicaMutationRuntime`, all compatibility proxies, their
fields/maps/clocks, and their characterization test only after pools, orphan
teardown, SQLite and non-consolidated state, and every other supported provider
or capacity profile—including paid capacity, reserved fill, spot/fallback,
cost rebalance, and logical width other than one—has migrated or been retired.
Exact-incarnation non-pool whole-service cleanup and failed-service purge for
the eligible profile are part of M4/M5a, not deferred here. `PLA-GAP-005` and the
global `PLA-M5-016`/`PLA-M5-019`--`PLA-M5-021` gates belong here. M5b is not
part of the bounded M4/M5a PR stack and cannot be inferred from one successful
non-pool canary.

### M6: reuse decision, not automatic generalization

Evaluate one second domain, preferably orphan cleanup or image-worker
distribution. Generalize only if it reuses identity, due/materialization,
request lease, and terminal reduction without adding a domain queue, action
lease, or alternate lock order. Otherwise keep the kernel Serve-scoped.

## Deployment and rollback

The isolated test target is Kubernetes context `boltz-test`, namespace/release
`skypilot-ha/skypilot-ha`, with PostgreSQL and two API, ordinary-executor, and
controller replicas.
Authority-worker Deployments, ServiceAccounts, selector Service, Secrets,
manifest projections, and their NetworkPolicy stay in that release namespace;
only provider workload objects use the separate `skypilot-actions-canary`
namespace. Production is not a fault-injection target.

The dark compatibility rollout, with authority workers disabled and absent, is
API -> ordinary executor -> controller:

1. build and push an immutable image digest;
2. deploy only the API role with `helm upgrade --reuse-values`, explicitly
   keeping the authority-worker role disabled, pinning ordinary executors and
   controllers to the prior immutable digest, and letting the API's blocking
   additive migration hook reach the required heads;
3. verify all independent milestone-specific heads: M1a is API005 with
   unchanged Serve031/global-user-state 027; legacy-controller-only M2 shadow
   requires API005, Serve034, and global-user-state 028; any private-handler
   shadow transition requires `PrivateShadowActivationProofV2`, each M3
   provider dispatch requires `PrivateDispatchReadinessProofV2`, and M4
   promotion requires `AuthoritativePromotionProofV2`; all live V2 proofs require actual
   API008, Serve039, and global-user-state 028, with no
   cross-lineage Alembic dependency; also verify preserved requests/inventory,
   zero action-family rows, and zero authority-worker resources;
4. deploy ordinary executors at the new digest while pinning API to the new
   digest and controllers to the prior digest;
5. deploy controllers last with API and ordinary executors pinned to the new
   digest;
6. run compatible-image rollback with the current chart and prior digest for
   all three ordinary roles while retaining additive heads; and
7. repeat API -> ordinary executor -> controller to restore the new digest.

Step 6 is dark, pre-owner compatibility evidence only. A pre-039 image is legal
only when the locked inventory, including the timeout-bounded controlled full-
table private-handler scan, proves: zero nonnull lease owner/hash/
normalized-process-scalar triples; zero process-supersession, lineage, action-
selector, shadow-terminal-history, shadow-admission-fallback-history, shadow-
admission-fallback-progress-history, or shadow-settlement-history rows; zero rows
in every state for all four
private handlers; and no V2 candidate window, dispatch, action request, or
activation/admission evidence. The first post-039 lease INSERT/BIND that stores
an owner triple closes this window even when no action exists. Thereafter every
rollback image must itself be Serve039- and process-owner-aware, be named by the
active V2 policy, preserve the owner triple, process rows, selectors/lineage and
authoritative mode, and implement the same cancellation, same-Pod supersession,
handoff, and cold-recovery terminal batches. It closes new admission while its
exact work inventory drains/resumes and never derives or remints a process API
UUID from the stable Pod UID. The chart/startup gate refuses a pre-039 image;
rollback never changes an
authoritative service to shadow/legacy and never deletes or rewrites durable
state.

Before the first compatible rotation, the one-set initial policy has no
distinct older post-owner binary: rollback is an idempotent redeploy of exact
M4, or admission remains closed for a forward fix. The rollback qualification
test therefore runs after the two-set M5a successor activates. It deploys every
approved mixed selection, reaches all-M5a, rolls to the policy's all-M4 rollback
selection, performs a same-Pod restart over mixed action/shadow/pending-
cancellation inventories at 0, 16, and blocking 17, executes both handoff and
full cold recovery, and re-upgrades to all-M5a. It requires byte-stable owner/
process/lineage/selector/shadow history, one terminalization per request, and
rejection of every late prior process throughout.

A versioned authority-worker cohort is a separately gated M2a/M3 deployment.
It is not part of this ordinary-role dark rollout and must not be described as
exercised here. M2a may render and self-attest the preflight-only cohort after
the post-build qualification artifact is checked in, but it starts no executor
and creates no reference/request/action. M3 later enables claim readiness only
after refresh semantics and the API008 private-request boundary are complete.
Every cohort with a `PREPARING`, `SHADOW_ACTIVE`, or `ACTION_ACTIVE` reference
is retained; parity/crash evidence and promotion remain limited to an
explicitly selected canary service.

The values-level `activeCohort` is consumed only by the Service/manager
selection templates. It is absent from every cohort Deployment Pod template,
environment, projected manifest, and annotation, so switching it cannot roll
or mutate an old cohort. Render-diff tests require byte-identical cohort
Deployments, ServiceAccounts, per-cohort manifest/qualification ConfigMaps,
RoleBinding subjects, and Pod templates across an active-selection-only change.
Each live cohort independently carries the exact values-level
`manifestContract: provider_authority_worker_cohort_v2` discriminator described
above; `activeCohort` never supplies, defaults, or upgrades that contract.

Both workers first become bootstrap-ready. The first process that subsequently
observes the exact Deployment with spec/status-total/updated/ready/available
replicas all two, unavailable zero, and one generation/resourceVersion inserts
its own registration. The peer reads that
row and compare-and-swap appends its own registration only after observing that
same Deployment snapshot; the stored pair is sorted by Pod UID. Neither needs
permission to read the peer Pod. The typed gate rereads the same Deployment and
changes the row to `ACCEPTING`, permitting active selection.
P2a Pod readiness is `/bootstrapz`, independent of database lifecycle;
preflight is 503 until acceptance and queue `/readyz` remains false, which
breaks the otherwise circular dependency between two ready Pods and
`ACCEPTING`.
The never-accepted-row abort locks its `REGISTERING` cohort first and performs
only nonlocking reads of the later reference/evidence/action/request classes;
every writer of those classes must acquire the cohort/reference prefix first,
so append, promotion, and admission races serialize without a backward lock.
Same-key/cross-identity carrier bytes retain the row. The P2a audit recursively
locates the complete cohort ID in every current action, request, shadow-parent,
and shadow-child JSON carrier; target-located terminal, released,
unknown-handler, malformed, or hash-inconsistent rows all block. A malformed
row with no recognizable target locator is not a global P2a blocker for every
cohort: P3 must add normalized locators and the complete typed/hash/terminal
graph audit for normal `DRAINING` retirement. P2a has no separately persisted
cohort-bearing activation/promotion-proof carrier. Final `RETIRED` does not
repeat an all-history scan: it trusts the durable `REMOVAL_AUTHORIZED` fence and
requires both exact Kubernetes names to return NotFound.
Retirement first commits
`DRAINING`, so no new preparation reference can bind while existing work
remains claimable. After active references release, one transaction locks the
cohort -> nonterminal handoffs -> all registration leases -> references,
performs fail-closed nonlocking scans of every authoritative action/attempt/
request, private shadow request/evidence carrier, and cold-recovery history,
terminally revokes all ACTIVE leases, and commits `REMOVAL_AUTHORIZED`. Unknown
or unreadable state counts as a reference. The
current chart may remove the exact Deployment/ServiceAccount only afterward,
while retaining their names in the current chart's tombstone-scoped GET grant.
The surviving API-role verifier—not the removed worker—proves exact NotFound
outside SQL, then a short transaction locks the current release tombstone
fence before cohort -> handoff -> leases -> references, validates those proofs,
and commits `RETIRED`; a later chart upgrade then prunes that grant. Thus an
active-cohort switch between preflight
and admission and a retirement attempt between reference discovery and
admission are fenced by the same cohort/reference rows.

The chart projects the surviving API role's exact retirement scope as the
immutable installation UUID and canonical sorted-unique JSON arrays of live and
tombstone suffixes. A PostgreSQL advisory-lock singleton is further keyed by
namespace and rendered release name. It aborts stale live `REGISTERING` rows
without Kubernetes I/O, waits on live `REMOVAL_AUTHORIZED` rows, and performs
only exact-name Deployment/ServiceAccount GETs for inventoried tombstones. Only
404 counts as absence; authorization, transport, and identity errors fail the
individual record closed while the bounded pass continues. Tombstone-only
rendering retains the API environment, ServiceAccount token, and exact-name GET
Role/Binding but renders none of the authority-worker Pods, ServiceAccounts,
ConfigMaps, Service, or NetworkPolicy. Two exact 404s plus the durable
`REMOVAL_AUTHORIZED` fence commit `RETIRED`; no list/watch/delete or repeat scan
of terminal history is permitted.

The first additive migration deployment omits `--atomic` unless the selected
automatic rollback image is proven ahead-of-head compatible. Native Helm
rollback is image rollback only; it does not run schema down. A failed
milestone is repaired with another current-chart `helm upgrade --reuse-values`
that pins every role explicitly and deploys the prior compatible immutable
digest against the retained additive schema; `helm rollback` is not used.
Database principal topology and credentials are unchanged by this program.

### `boltz-test` dark rollout evidence (2026-08-02)

The first tested artifact used ECR immutable tag `resource-actions-a836825ef`, source
commit `a836825ef9c219563bb2abc740707c825c26edc5`, and digest
`sha256:c5f1306f91c7fe2db151c34131ca4cd39be9beba3d21d170f5757996338f375e`
(`new`). The compatible baseline digest was
`sha256:d05257c3018c570861104c6c0a509c92d29af93df2d167a58e50d6748a1590a1`
(`old`). After PR #1190 merged, immutable tag `resource-actions-93aec0c8a`,
exact merge commit `93aec0c8a4f2e1a80ed35640c9d424bea3f9e580`, and digest
`sha256:8bc1295d5cb873861576aaf0806665e89b2d325194da8dd61fa5752f0593d174`
(`merged`) were built from a clean checkout and deployed separately.
After PR #1202 merged, immutable tag `resource-actions-4f024b60f`, exact merge
commit `4f024b60f2fc71852fa8fb9747390f4d3917b03f`, and digest
`sha256:06c9e71c5744ea970c41402fb9c4934e6722a7b53271f6715231b4b275525d25`
(`renderer-contract`) were deployed separately. In the rows below, `prior`
means the compatible digest running that ordinary role immediately before its
staged replacement.

The pure-renderer artifact used immutable tag `resource-actions-0e894c2a5`,
exact merge commit `0e894c2a5d7186d15b10d62bbfdb8283201e4e63`, and digest
`sha256:b21f0e7cc39f62a21bc5887406f941d0b298d8fc277f0b5abb8b1f170c88b198`
(`renderer`).

| Helm revision | Started (UTC) | Purpose | API / executor / controller | Migration job (UTC) | Heads after checkpoint |
|---|---|---|---|---|---|
| 57 | 03:05:54 | Observed baseline | old / old / old | prior rollout | 027 / Serve032 / API004 |
| 58 | 10:05:42 | Dark stage 1: API and migrations | new / old / old | 10:05:51–10:06:56, succeeded | 028 / Serve033 / API007 |
| 59 | 10:13:24 | Dark stage 2: ordinary executor | new / new / old | 10:13:33–10:13:45, succeeded | 028 / Serve033 / API007 |
| 60 | 10:20:53 | Dark stage 3: controller | new / new / new | 10:21:02–10:21:14, succeeded | 028 / Serve033 / API007 |
| 61 | 10:26:42 | Current-chart compatible-image rollback | old / old / old | 10:26:51–10:27:02, succeeded on old | retained 028 / Serve033 / API007 |
| 62 | 10:36:06 | Re-upgrade stage 1: API | new / old / old | 10:36:15–10:36:26, succeeded | retained 028 / Serve033 / API007 |
| 63 | 10:42:39 | Re-upgrade stage 2: ordinary executor | new / new / old | 10:42:48–10:43:00, succeeded | retained 028 / Serve033 / API007 |
| 64 | 10:46:23 | Re-upgrade stage 3: controller; final | new / new / new | 10:46:32–10:47:04, succeeded | retained 028 / Serve033 / API007 |
| 65 | 13:26:30 | Exact-merge stage 1: API | merged / new / new | 13:26:40–13:27:49, succeeded | retained 028 / Serve033 / API007 |
| 66 | 13:34:59 | Exact-merge stage 2: ordinary executor | merged / merged / new | 13:35:09–13:35:21, succeeded | retained 028 / Serve033 / API007 |
| 67 | 13:40:08 | Exact-merge stage 3: controller; final | merged / merged / merged | 13:40:18–13:40:28, succeeded | retained 028 / Serve033 / API007 |
| 71 | 16:14:34 | Renderer-contract stage 1: API and migrations | renderer-contract / prior / prior | 16:14:43–16:15:51, succeeded | retained 028 / Serve033 / API007 / capacity001 |
| 72 | 16:23:19 | Renderer-contract stage 2: ordinary executor | renderer-contract / renderer-contract / prior | 16:23:29–16:23:40, succeeded | retained 028 / Serve033 / API007 / capacity001 |
| 73 | 16:27:59 | Renderer-contract stage 3: controller; final | renderer-contract / renderer-contract / renderer-contract | 16:28:09–16:28:57, succeeded | retained 028 / Serve033 / API007 / capacity001 |
| 74 | 20:13:46 | Pure-renderer stage 1: API and migrations | renderer / renderer-contract / renderer-contract | 20:13:56–20:14:33, succeeded | retained 028 / Serve033 / API007 / capacity001 |
| 75 | 20:20:53 | Pure-renderer stage 2: ordinary executor | renderer / renderer / renderer-contract | 20:21:03–20:21:13, succeeded | retained 028 / Serve033 / API007 / capacity001 |
| 76 | 20:26:01 | Pure-renderer stage 3: controller; final | renderer / renderer / renderer | 20:26:10–20:26:21, succeeded | retained 028 / Serve033 / API007 / capacity001 |

Each revision 57--67 checkpoint was held until every changed role converged to
exactly 2/2 ready replicas at the intended digest with zero container restarts.
The final revision had two ready API endpoints, zero services, replicas, and
clusters, zero ungranted PostgreSQL locks, and no schema, migration,
private-handler, or resource-action error in the role-log scan. The 18
pre-existing API requests were preserved; normal executor processing reduced
nonterminal requests from 9 to 6 during the exercise.

At every revision 57--67 checkpoint, all eight action-family tables remained
empty:
`api_resource_actions`, `api_resource_action_attempts`,
`serve_resource_action_shadow_samples`,
`serve_resource_action_shadow_attempts`,
`serve_resource_action_worker_cohorts`,
`serve_resource_action_worker_cohort_refs`,
`serve_resource_action_shadow_coverage`, and
`serve_resource_action_shadow_coverage_attempts`. No authority-worker
Deployment, ServiceAccount, Service, Secret, NetworkPolicy, Role, or RoleBinding
was present.

At the revision-67 stable checkpoint, all six ordinary-role Pods reported the
exact `merged` image ID, 2/2 replicas were ready and available for each role,
and every current container had zero restarts. Services, Serve replicas, and
clusters remained zero. All 18 API requests were retained (11 `SUCCEEDED`, six
`RUNNING`, and one `FAILED` from ordinary processing), and the post-deployment
role-log scan contained no traceback, exception, critical, or error match. The
API007, Serve033, and global-user-state/capacity 028/001 heads were unchanged,
all eight action-family tables remained empty, and authority-worker resources
remained absent.

The exact-merge rollout also encountered Karpenter churn: the 13:26–13:44 UTC
event window recorded 40 aggregated `FailedScheduling` events, six evictions,
three taint-manager evictions, and startup/drain readiness failures while
replacement nodes and Pods converged. Each stage retained an old ready replica
until its replacement became ready; the final checkpoint recovered every role
to 2/2 with zero restarts. This remains ordinary Deployment recovery evidence,
not action crash-canary, shadow-parity, or M4 soak evidence. No M4 candidate
window starts from this dark rollout.

At the revision-73 stable checkpoint, all six active ordinary-role Pods
reported the exact `renderer-contract` image ID, were ready, and had zero
container restarts. The post-rollout role-log scan from the revision-71 start
found zero traceback,
exception, critical, fatal, unhandled, or error matches. PostgreSQL reported
API007, Serve033, global-user-state 028, and capacity001; all eight
action-family tables, services, replicas, and clusters had zero rows.
`resourceActions.authorityWorker.enabled=false`, and no authority-worker
workload resource existed. Karpenter churn occurred during the three stages,
but no mixed-version Pod remained at the checkpoint. This is
dark binary/schema/contract evidence only and starts no M4 candidate window.

At the revision-76 stable checkpoint at 20:30:53 UTC, all three ordinary
Deployments reported 2/2 ready, updated, and available replicas. All six
active Pods had zero restarts, no deletion timestamp, the exact `renderer`
image ID, and embedded commit
`0e894c2a5d7186d15b10d62bbfdb8283201e4e63`. Both API replicas byte-checked
the packaged 23,710-byte config-access inventory at SHA256
`19901e8e0491a4e9f957f7ff2a1244fc1baff132c37015c9e8e726af2d538f13`.
PostgreSQL retained API007, Serve033, global-user-state 028, and capacity001.
The eight action-family tables, correlated API requests, services, replicas,
and clusters all had zero rows. The authority-worker value remained
explicitly false, no authority workload existed, and a filtered 20-minute
role-log scan found no unexpected traceback, exception, error, fatal, crash,
or failed match.

Karpenter capacity provisioning and consolidation caused transient pending,
surge, and terminating Pods during revisions 74--76. Every stage retained
ready old-role capacity until replacements became ready, then converged to
exactly two Pods at the intended digest. This remains ordinary rolling-update
recovery evidence. No resource action, shadow comparison, provider session,
provider I/O, or authority worker existed, so revision 76 starts no M3/M4
qualification window and does not satisfy the M4/M5 payoff gates.

The original revisions 58--64 rollout encountered ordinary Kubernetes
infrastructure churn. Across the 10:05–10:49 UTC event window the selected
role/migration objects accumulated
134 `FailedScheduling` events while Karpenter supplied capacity, 10
`Underutilized` evictions, two transient AWS-CNI `FailedCreatePodSandBox`
events, and 167 startup/readiness `Unhealthy` events. Every affected Deployment
recovered to 2/2 at the intended digest with zero restarts. Because no resource
action existed and provider I/O was disabled, this is ordinary Deployment
recovery evidence, not an M3/M4 action crash-canary result.

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
4. after approval but before a legacy `PRE_SUBMIT` row or private linked graph;
   before/after permanent linked-admission fallback commit and before its legacy
   signal;
5. during two-dispatcher due discovery;
6. after request/queue commit but before materialization acknowledgement;
7. after request claim but before claim-start; before/after the atomic Serve039
   lineage insert/exact adoption; while terminalization races claim-start;
   before/after the no-lineage terminal-selector commit; and before/after the
   later atomic two-boundary/first-cursor or inherited-attestation commit;
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
    boundary/attestation binding, while carrying a partial cursor, and before/
    after each replacement-worker `OPEN`, stale-authority fence, survivor
    acknowledgement, `READY`, final-set commit, `COMPLETED`, or `ABANDONED`
    boundary;
14. while superseding a partial launch, committing its real down action, and
    exact-reading/deleting each cleanup object and cluster row;
15. during controller/API/executor eviction and controller-leadership change;
    and
16. during compatible image rollback/re-upgrade with nonterminal actions;
17. while active cohort selection changes between preflight/admission and while
    retirement's zero-reference discovery races admission/private request
    binding;
18. after crash-canary `STARTED` commit before injection, after injection before
    receipt/completion, and while start races promotion or candidate reset;
19. while one M4 and one M5a role coexist, while policy admission is closed,
    immediately before/after the atomic policy-epoch switch, and during exact-M4
    rollback and exact-M5a re-upgrade; and
20. while legacy launch, legacy down, action launch, and action down candidates
    race for the same last weighted provider-work unit and per-service down cap;
21. after cancellation intent before child submission/PID publication, after
    signal before child/future quiescence, and after quiescence before the
    same-fence acknowledgement commit; and
22. while two bootstrap supervisors race to supersede one same-Pod owner,
    before/after process-supersession evidence and lease-owner commit, and while
    handoff or cold recovery races that process transition.

Tests must prove:

- fresh empty API005 -> API006 migration, both boundary-field constraints, and
  fail-closed rejection of any pre-006 action-attempt row whose provider-I/O
  watermark cannot be reconstructed;
- fresh/upgraded PostgreSQL Serve038 catalogs have exactly the documented
  shape and preserve the exact Serve037 placement-normalization/retirement
  tables, service/version columns, checks, foreign keys, and arbitrary valid
  nonempty ledger/retirement rows byte-for-byte; service/mode, version-spec,
  replica, the Serve037 placement normalizer, cohort-
  reference, and coverage writers each race the PostgreSQL `ACCESS EXCLUSIVE`
  migration in both directions and prove the six-relation global order cannot
  deadlock or reach backward; lock timeout leaves no partial catalog or stamp;
  exact empty partial objects adopt; incompatible objects fail before DDL; and
  old mode writes cannot create a shadow row with null candidate identity after
  038;
- fresh/upgraded PostgreSQL Serve039 catalogs use separate `SERVE039_METADATA`
  and contain exactly nine wholly new relations: lineage, action terminal-selector, one-to-one shadow
  execution-history, shadow request-terminal-history, shadow admission-fallback-
  history, shadow admission-fallback-progress-history, shadow settlement-history,
  process-supersession, and authority-API
  GC-cursor relations
  plus lease owner/hash/
  indexed-process-scalar columns and shadow-parent execution-route/fallback
  evidence columns plus compatibility default,
  checks,
  restrictive Serve-local foreign keys, and indexes above, including exact
  reflection of the settlement-history partial unique source-parent and
  reverse-target indexes; migration 038's
  dynamic metadata enumeration remains byte-stable, downgrade refuses, and the
  target-version router keeps local/controller SQLite at its supported pre-
  authority head. Runtime activation accepts the central manager's registered
  sync and async/run-sync connection sources, rejects another namespace or
  database, and requires one caller-owned PostgreSQL connection per operation.
  Store registration is once-before-work and immutable; missing, duplicate, or
  late registration and private-route corruption fail without mutation;
  migration tests cover nonempty pre-039 shadow parents, deterministic
  `LEGACY_CONTROLLER`/null backfill under the class-9 lock, exact NOT NULL/CHECK
  reflection, complete-catalog old-stamp adoption, every partial column/CHECK,
  and rejection of a pending/private/fallback route at an old stamp;
- #1240 catalog/policy tests keep its first M5a phase at exact Serve039 with
  the compatibility default present, reject a 040 target before the complete
  M5a -> M4 -> M5a matrix and typed Serve039 qualification window, and require
  every authoritative service to have a one-set permanently CLOSED
  `ROLLBACK_EVIDENCE_CLOSURE` successor plus global zero shadow/bound work.
  Serve040 races owner/service/policy/parent writers in every lock direction;
  rejects a missing/wrong-step/duplicate callback registration and direct or
  offline 040 execution before DDL; proves the revision returns with locks and
  a one-shot handoff still live, Alembic `HeadMaintainer` updates 039 -> 040,
  and only then the callback reads actual 040 on that identical connection and
  transaction. Faults before/after DDL, the version-row update, callback entry,
  each predecessor supersession, and each successor insert atomically roll back
  catalog, version, and policies; pre/post catalog hash handoff drift rejects;
  the callback updates the predecessor before inserting the one ACTIVE
  successor and cannot commit/roll back/check out a connection; and on
  acknowledgement loss accepts only the complete default-free catalog/stamp/
  per-service head-advance set. Physical/parser negatives reject an OPEN
  closure policy, `ACTIVATE_CLOSED` on initial/compatible rotation, a revision-
  one OPEN head-advance, wrong 039/040 reason, crossed predecessor catalog
  proof, missing/extra service, shadow/nonterminal work, stale M4 artifact, or
  partial successor set. Reopen requires fresh actual-040 role/cohort/process
  attestations and the final typed Serve040 window independently proves 86,400
  seconds, 100 clean launches, 100 clean downs, crash/HA inventory, and all
  exact-zero gates;
- claim-start inserts/exact-adopts one immutable lineage row before handler
  invocation; lost acknowledgement replays stored `authorized_at`, hashes, and
  legal membership/lease/reference/policy successors without overwriting.
  Handoff, renewal, policy close/rotation, terminalization, and claim-start races
  linearize in both orders. A winning pre-gate terminalization or fixed
  representability rejection writes exactly one
  `NO_SUCCESSFUL_CLAIM_START` selector and no lineage, while a post-gate terminal
  writes a `LINEAGE` selector naming the exact row. A stored-lineage adoption
  whose current-successor or representability replay fails writes the fixed
  `CLAIM_REAUTHORIZATION_FAILED`/`LINEAGE` selector and reduces from its actual
  journal, while corrupt stored lineage mutates nothing. Unequal replay, duplicate,
  missing, crossed request/worker/generation, and orphan lineage all reject.
  Success, failure, strict-codec failure, owner-acknowledged cancellation,
  owner-quiesced lease loss, and typed UID/process recovery fencing each use one
  database finish timestamp, capture both pre-update stable/process IDs in the
  selector, and clear the full API007-defined claim triple under the API008 head so the physical
  `ck_api_requests_claim` constraint passes. Owner-quiesced lease-loss fixtures
  cross action/shadow, lineage/pre-claim-start, and null/pending cancellation
  intent; require the exact `CANCELLED` cause/disposition and cancellation-field
  mapping; and exercise `P0`/`O`/`S`/`X` reduction, attempt-`n+1` retry, and zero
  same-request replay;
- reduction point-loads only the sorted at-most-28 key union from its exact
  selectors/progress/outcomes, never scans lineage or substitutes current
  membership. Both sync and async bulk-GC call sites lock candidate requests
  in order and invoke the frozen same-connection retention validator; request
  GC is refused before settlement plus selector or on any selector drift. After GC,
  valid/invalid journal, fallback, direct-cancellation, and settled replay use
  the retained selector and historical rows. Shadow request GC separately
  requires child `COMPLETE`, history `SETTLED`, exact terminal receipt, outcome/
  return/fallback/progress hash relationships, and tests handler/fallback plus
  request-present/request-GC replay; either worker-ID column of authorized/
  settled history is an API-GC root. The settled fast path cannot skip this
  validation;
- `ServeServiceVersionSpecIdentityV1` rejects pickle/current-row assertions,
  produces equal effective subhashes for omitted versus explicit defaults,
  binds exact YAML separately, preserves distinct immutable versions for a
  mixed-version service, and serializes update/admission into a complete old or
  new action binding;
- `ServeReplicaActionSpecV1` goldens and parser bytes remain unchanged;
  `ServeReplicaActionSpecV2` rejects unknown/missing keys, a mismatched embedded
  identity hash, crossed action IDs/kinds/targets, and shadow/authority binding
  substitution; every live admission/materialization/claim/context/pre-I/O/
  recovery/reducer boundary inventories only
  `serve_replica_action_spec_from_value_v2()` and rejects V1 before writing an
  artifact or effect; UUID policy-epoch goldens round-trip through Python,
  canonical JSON, and native PostgreSQL while integers, numeric strings,
  noncanonical UUID text, and revision-for-epoch substitution reject;
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
- with cap `C`, mixed action/legacy launch/down candidates use exact occupancy
  `2P + D < 2C`, launch/down weights two/one, launch-before-down ordering, and
  the 64-running-down per-service cap; each winner atomically has its durable
  `PROVISIONING` or down-`RUNNING` row plus its owner-specific graph before
  dispatch, each loser has neither, retries retain occupancy, and terminal/
  no-I/O replay releases it exactly once across crash and lost acknowledgement;
- non-Boolean/missing/true `ReplicaInfo.is_spot`, nonnull `spot_placer` (even
  width one with cost rebalance disabled), reserved fill, paid-claim markers,
  spot/fallback, accelerator/multi-node input, or logical width other than one
  creates no `PREPARING` reference, coverage, action, or private request and
  performs no M4-owned DML in paid/reserved tables; after all eligibility locks
  release, an excluded service's legacy adapter still exercises its existing
  paid/reserved DML; a profile-changing update in private shadow or authority
  rejects before the version/spec commit, and an update/admission race cannot
  mix owners;
- a commit-before-signal crash remains counted across controller death and is
  adopted or released only with pre-call proof; two recovery owners cannot
  double-release; release racing admission never exceeds the cap; restart/
  local-snapshot lag neither undercounts nor double-counts a decision ID;
- ordinary-cap race fixtures exercise both resources-file/SQL acquisition
  directions and fail on deadlock, an external resources-file lock acquired
  after SQL locks, or any condition/preflight/provider wait while either lock
  is held;
- authoritative `INTENT_COMMITTED` in both boundary fields and the first legal
  API006 cursor are one atomic write, so no crash can leave a crossed provider-
  I/O watermark with null progress;
- `ProviderExecutionAuthorityProofV2` accepts exactly its documented closed
  fields, a UUID policy epoch, current V2 membership and reference revision,
  and fresh preflight hashes; it rejects a crossed action/reference/policy,
  integer epoch, stale registration or server-instance lease, handoff member,
  reused readiness proof, and every attempt to use the authority proof as a
  shadow or unlocked bearer capability;
- monotonic provider-progress replay, stale-write rejection, and partial UID/
  job commitment survival across request-worker eviction;
- every checked-in realistic and candidate-maximal companion fixture for all
  launch phases, both head-Pod edges, terminal no-effect resolutions, request-
  return envelopes, E-only/E+N reducer quiescence, handler-domain `S` and every
  legal phase/category `R`/`U`/`B` tuple, the count-zero unmaterialized direct
  no-effect basis and the one/max-count legal materialized basis/prefix pairs,
  and request-fallback `P0`/`O`/`S`/`X`
  outcomes remains at most 65,536 canonical UTF-8 bytes with its golden hash;
  additionally, completed-launch down, every legal partial-launch down full-
  spec shape, and capped preflight request/response envelopes are pinned by
  exact byte count/hash and every full action spec is at most 60,000 bytes; an
  oversized/unbounded candidate is rejected by admission and the immediate
  pre-I/O recheck before any intent/watermark;
- all seven representability boundary literals and twenty primary roots execute:
  eight authoritative plus twelve shadow. Linked admission measures request
  input and both members' hypothetical dispatch/execution children; claim-start
  and pre-I/O measure the independently stored proof/progress/strict-return
  children plus exact prior-request origin sources; shadow terminalization
  measures each exact typed commitment/receipt and mixed fence projection only
  after its database time exists, including receipt-only adoption; and shadow
  settlement measures actual/proposed outcome,
  fallback, retry/observation/effect evidence, complete child/post-parent,
  raw-preserving `SETTLED` history, request-present/evidence-GC replay, and each
  R/U/P0/O/Q successor from an independent source-only construction root. Each
  of those five paths covers fresh commit, precommit crash, unknown-commit
  stored adoption, partial/crossed graph, both source/target UUID sort orders,
  authority drift, and output mutation without source mutation. Q stored
  adoption additionally advances the target through AUTHORIZED/SETTLED,
  releases its reference/GCs its request, removes its replica row, and promotes
  the source service before replay. Receipt-only settlement adoption repeats
  after ordinary, outgoing-Q, and incoming-Q evidence GC, derives the Q peer
  from parent-wide permanent successor indexes rather than the replayed
  receipt, proves bounded absence after a maximum-length non-Q retry history,
  rejects a late second outgoing Q at the physical unique index plus every
  one-sided/second/crossed peer and different source/projection/operation
  commitment, and recreates no graph. Fallback adoption
  tests reject receipt-only selection at the immediate unsignaled post-state,
  atomically bind the first-PRE_SUBMIT and terminal-no-call progress receipts,
  accept retained-descendant and typed-GC receipt paths, and reject missing/
  crossed progress evidence or partial graphs. The
  sealed attestation/scenario profiles, complete
  action and shadow cleanup roots, authoritative and shadow history banks, and
  seven direct roots are AST-inventory-equal to their classifiers. Action/shadow
  codecs cross-reject, SQL-row aggregates never become payload cases, all three
  direct terminal states are legal, and none exceeds the explicit maxima;
- effect provenance survives replacement by a later attempt's new
  sole-generation-one request: intent origins and prior evidence remain
  byte-equal, evidence-commit origins and
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
- each authoritative private handler's dedicated encoder stores exactly one
  hash-valid `ServeReplicaActionRequestReturnV1`, and each shadow handler stores
  exactly one disjoint hash-valid `ServeShadowCandidateRequestReturnV1`;
  action/shadow cross-kind, null/drop/default-encoded/mismatched
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
- retry/pause exceptions produce one terminal typed outcome and never requeue
  or repeat provider mutation within the same request/attempt;
- no action lease/heartbeat table or domain due scanner;
- database-clock retry continuity across restart;
- stale owner/request/reducer writes reject;
- both evidence-versus-terminalization race directions linearize according to
  the action-attempt-request lock order;
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
  preflight; legacy/authoritative admission atomically binds it to
  `SHADOW_ACTIVE` or `ACTION_ACTIVE`, while selected-private capacity admission
  persists `PENDING_SELECTION` and its linked transaction performs the sole
  activation. Permanent enumerator rejection atomically records
  `LEGACY_CONTROLLER/linked_admission_not_representable`, proves zero private
  graph, preserves the counted slot, signals only after commit, and exact-adopts
  commit-before-signal; retryable drift stays pending. Active-cohort switch,
  stale preparation owner, and the zero-
  reference/admission race cannot strand work; a nonterminal private shadow
  request pins its cohort; missing/unreadable/malformed reference state blocks
  retirement; the registry starts `REGISTERING` and requires two distinct
  matching ready V2 Pods plus one final set-level Deployment snapshot before
  `ACCEPTING`; V1 manifest/registration/Deployment-snapshot/claim parsing and
  exact `Recreate` strategy are byte-frozen retirement-only, while M4 accepts
  only the version-2 manifest and exact `RollingUpdate` integer
  `maxSurge=0,maxUnavailable=1` contract; crossed manifest/parser/strategy
  combinations reject before cohort selection; bootstrap proves
  `/bootstrapz -> ACCEPTING -> /readyz`; one- and two-anchor `REGISTERING` loss
  require exact UID absence, reject expiry/unready/deletionTimestamp/name-only
  evidence and append/activation races, revoke leases, retire with zero related
  evidence, exact-adopt lost acknowledgement, and create a fresh suffix; and a
  one-at-a-time replacement proves
  exact stale-UID absence, fences its instance/claims, durably advances
  `OPEN -> READY` only from a post-fence survivor re-attestation, then atomically
  swaps membership and `COMPLETED` with the final snapshot; deletionTimestamp,
  candidate claims before completion, stale-SQL-snapshot claims, independent
  membership writes, unsafe abandonment, and lost-ack replay reject. Repeated
  candidate loss adopts the retained origin fence while the survivor lives;
  double-worker loss tests ACTIVE+ACTIVE and retained-STALE_HANDOFF+ACTIVE lease
  branches, exact request/queue fences, two fresh members, atomic same-cohort
  membership, unchanged bound work, lost-ack adoption, and changed-Deployment
  rejection; down takes the same
  preflight fence;
  `DRAINING -> ACCEPTING` rollback atomically recollects both attestations; and
  removal occurs only after `REMOVAL_AUTHORIZED`, with the surviving API
  verifier retaining exact tombstone GETs until NotFound commits `RETIRED`;
- registration-lease constraint/read tests reject ACTIVE+REVOKE,
  REVOKED+RENEW, unknown reasons, crossed/null owners, nonpositive counters,
  worker/Pod-ID mismatch, overlong or short expiry, revoke-before-renewal, and
  every malformed generation/revision, JSON-root/size, or hash shape;
  insert/renew/revoke unknown results
  exact-adopt by operation ID or the documented same-instance stable successor
  lineage; one-lease abandonment and two-lease handoff/removal/cold-recovery
  fixtures move `clock_timestamp()` behind the greatest locked `renewed_at` and
  prove that the shared `GREATEST` operation time keeps every joined insert,
  fence, revocation, and cohort transition aligned with
  `revoked_at >= renewed_at`;
- handoff lineage tests cover sequence-one roots, sequence-two and sequence-
  three adoption where immediate predecessor differs from immutable root,
  duplicate root/sequence, second adopter, old-predecessor branch, crossed
  source/fence, and O(1) immediate-tip/root validation; raw timestamps may
  repeat or regress without changing causality;
- OPEN and full-set cold-recovery concurrency tests prove that complete
  handoff/evidence and lease rows are staged before API/request/queue locks,
  requests are all locked before queues, generic-terminalization inventory drift
  rolls back every optimistic write, and a lock wait beyond registration/API
  TTL or proof freshness rolls back before commit;
- abandonment tests pre-read exact candidate/survivor UID absence, hold the
  cohort/handoff/lease/API/request/queue prefix across fail-closed zero-effect
  scans, require zero-proof observed time = handoff terminal time = candidate
  revoke time, and reject replay of an earlier count or any later-class race;
- fence-bound tests keep 0/64/65 only for the dark V1 historical reader. Live
  V2 tests cover 0/16/17 action-only, shadow-only, and mixed claims; nested-
  list, complete-fence, and exact-two cold-array byte boundaries with maximal-
  width scalar goldens; over-limit retained-row rejection; overflow-before-
  write; no generic private expiry/requeue; and a 16-to-17 crossing between
  discovery and locked requery;
- initial activation locks both registration leases before both API instance
  rows, binds exact worker/Pod IDs, rejects API update/expiry races and
  proves `ready=true` is not a prerequisite, and rolls back if either lease or the final
  snapshot ages out before commit;
- initial Serve039 owner binding proves zero rows for all four private handlers
  in every request/delivery state, advances each retained stable lease exactly
  once, and never rewrites cohort membership. Fresh post-039 anchor, handoff-
  candidate, and cold-candidate INSERT tests prove lease owner/hash/scalar and
  API bootstrap-to-bound/owner-hash commit or exact-adopt atomically; retained
  pre-039 owner-null plus typed BIND is the sole exception. Physical-constraint
  tests reject every null/crossed/malformed owner JSON, hash, scalar, stable-ID,
  process-ID, start-time, and process-row proof combination;
- authority API-instance tests cover insert lost acknowledgement with adoption
  of the database-owned start time, equal retry, forced UUID collision with an
  unequal boot nonce or immutable inventory, deletion before retry, every legal
  and illegal bootstrap/bound/ready/rewarming/draining edge, heartbeat-only CAS,
  owner-hash mismatch, missing-row no-recreation, and bound-to-ready racing
  supersession. They run `ready -> rewarming` against both claim-start and
  immediate pre-I/O in both lock orders; prove the generation increments
  exactly once, an acknowledgement-lost increment exact-adopts, and a stale
  generation cannot ABA-adopt; cover initial warm failure, repeated rewarm
  failure, and the absence of every draining-to-recovery edge; and prove an
  unmarked ordinary-looking active owner row blocks rewarming rather than being
  hidden by private-shape filtering;
  current-owner RENEW and survivor acknowledgement pass while bootstrap, prior,
  crossed-stable-ID, crossed-process-ID, noncanonical textual UUID, stale, and
  wrong-phase callers reject;
- same-Pod restart tests cover simultaneous supervisors, same-container
  rejection, a strictly larger restart count and different current container ID,
  restart-count jumps, an expired but ACTIVE source lease, lost acknowledgement,
  reuse of a historical process UUID, future proof time, the exact 300-second
  proof-age boundary and one tick beyond it, lock-wait expiry, and backward
  database clocks. They reject late old heartbeat/claim-start/progress/pre-I/O /
  effect/return writes and exercise mixed action/shadow/pending-cancellation
  inventories at 0/16/17. The process-supersession batch proves both action
  selector branches, exact shadow histories, one completion/terminal-updated
  timestamp, no missing-lineage insert, owner/lease/API-phase plus receipt/event
  atomicity, and both handoff and cold-recovery race directions before the new
  process can publish readiness. An unmarked ordinary-looking active owner row
  independently blocks supersession, handoff, cold recovery, claim-cap
  admission, and terminalization;
- batch-terminalization tests reject mixed modes, operation IDs, operation
  times, owners, or fences; enforce singleton-only ordinary modes, 16 stale-
  owner, 16 process, and two partitions of at most 16 for cold recovery; and
  prove exact whole-operation adoption or whole-transaction rollback with no
  partially terminal sibling. Terminal replay validates request `updated_at ==
  finished_at == receipt.request_finished_at` and, where present, the same
  cancellation-acknowledgement time;
- cancellation tests prove intent-first idempotence, claim/heartbeat/progress/
  success/failure rejection after intent, claim-before-submit and null-PID
  future draining, PID signal plus child/future quiescence before terminal
  acknowledgement, owner crash followed only by typed UID/process fencing, and
  no generic expiry terminalization or same-request retry/requeue. Handler-
  versus-owner-ack and handler-versus-UID/process-fence races cover both lock
  orders and losing-handler retry after acknowledged or unknown winner commit:
  a different internally valid winner is `LOST_RACE`, while only inconsistent
  durable evidence is corruption;
- composition-root tests install the terminal store before every controller,
  API, Uvicorn, authority supervisor, and ordinary/authority spawned-child
  consumer; reject unequal or cross-database registration; prove child DB
  budgets are installed before engine/plugin construction; and prove the
  authority role starts one eagerly warmed no-burst LONG pool with distinct
  child PIDs and withdraws readiness through full-pool rebuild. Exact manifest
  widths `N=1` and `N=16` prove the physical `3 + 2*N` PostgreSQL high-water;
  widths zero and 17, environment/manifest-hash drift, a hidden or duplicate
  engine namespace, and any namespace with a wrong QueuePool size or overflow
  reject before readiness;
- GC tests keyset-walk past a fully blocked page, revalidate action selector or
  shadow history plus represented terminal state under the final per-request
  lock, report typed deleted/blocked results, unlink files only after commit,
  and remove only aged canonical-ID orphan files whose request row is still
  absent while retaining unknown names and failed/raced reads;
- Serve-owned authority-API GC tests distinguish fresh and more-than-five-minute
  stale rootless bootstrap rows; retain each indexed root family independently—
  ACTIVE or REVOKED lease scalar, any active request including malformed/crossed
  private state, lineage, selector, shadow history, and prior or current process
  ID—and delete only after the class-14 locked predicate recheck. Cursor tests
  cover exact initialization races, a persisted finite high-water epoch, 128 and
  129 rooted/blocked leading rows, insertion and eligibility changes before and
  after the cursor, temporarily locked targets, wrap, restart, lost commit
  acknowledgement, target deletion between passes, and a cursor UUID that names
  no row; repeated passes reach a rootless tail and later revisit a newly
  rootless prefix, while the cursor itself retains nothing. Heartbeat-
  versus-delete and BIND/SUPERSEDE-versus-delete run in both lock orders, the
  writer revalidates existence after waiting, generic API GC never selects the
  authority role, and heartbeat never recreates a deleted row;
- lifecycle tests reject bound-work `ACCEPTING -> DRAINING` and
  `DRAINING -> ACCEPTING` with unresolved terminal-stale membership, while the
  exact-zero-work direct `ACCEPTING -> REMOVAL_AUTHORIZED` edge revokes the
  survivor and cannot be rolled back or cold-recovered; ordinary zero-work
  DRAINING removal remains legal;
- V1 tests require zero lease/handoff/cold-recovery and non-`RELEASED` reference
  or nonterminal/ambiguous effect state, preserve released/terminal history and
  registration bytes/hash exactly through the exact-034 stale/accepted bridge,
  require every `REMOVAL_AUTHORIZED -> RETIRED` edge before Serve038 and cover
  grandfathered `RETIRED` null-removal-authorization/non-null-retirement
  history, explicitly reject missing, JSON-null, string `"1"`,
  and numeric `1.0` registration-set versions while accepting only numeric
  `1` in the retired V1 clause, reject version `1`, `3`, missing/null/string,
  and numeric `1.0`/`2.0` in every nonnull-time terminal branch, prove exact-034
  deselection/retirement closes every V1 cohort before
  038, race an old V1 append/renew/registration plus shipped
  `REGISTERING -> ACCEPTING` and `DRAINING -> ACCEPTING` CASes across the gate and require them
  to affect zero post-038 rows, reject every nonterminal V1 write under the
  new physical CHECK, and prove V1 can never select, claim, roll back, hand
  off, or cold-recover; V2 removal revokes and
  `removal_authorized_at` share one database time, which survives the monotonic
  NotFound-to-RETIRED edge;
- promotion refuses mixed binaries, a missing handler/head, a missing or
  not-representable coverage row, an orphan coverage/parent, a pending row, or
  any unresolved divergence;
- crash canary start commits one unique `STARTED` intent before injection;
  completion CAS/lost-ack adoption is exact; a crash before/after injection,
  concurrent start, start-versus-promotion/reset, `FAIL` then `PASS`, and
  `ABANDONED` all block the original epoch; only a fully drained shadow reset
  can mint a new one, and no timeout/log/operator value manufactures `PASS`;
- exact-M5a policy rotation proves claim-disabled role/cohort attestations,
  closes new authority roots, permits only already-bound actions to materialize
  and claim their deterministic attempts while `DRAINING`, reaches exact-zero
  nonterminal inventory, then proves `CLOSED` blocks every current execution,
  exact-adopts an acknowledgement-lost atomic predecessor-supersession /
  successor-activation commit, rejects any persisted `STAGED` shape and a
  second root or successor fork, exercises `OPEN -> DRAINING -> CLOSED -> OPEN`
  with revision/operation-ID CAS and rejects stale-ABA acknowledgement,
  survives backward-clock activation/supersession with monotonic logical
  times, atomically advances the policy epoch, permits mixed M4/M5a compatible roles,
  then passes exact-M5a rollout, exact-M4 rollback, and exact-M5a re-upgrade
  without demotion or legacy routing. Physical/parser negatives reject zero or
  three deployment sets; omitted, duplicate, or outside-set selections; and
  elected/rollback hashes crossed between sets. Only the all-elected selection
  may accrue soak qualification; every mixed or all-rollback interval is
  excluded; and
- the lifecycle checker rejects nonnull `introduced_by` for a planned absent
  artifact and prevents any member of the four-row M5a bundle from advancing or
  recording terminal evidence independently; and
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

As of 2026-08-04 these payoff conditions are not met. The implementation has
proved a bounded kernel and frozen/tested Serve contracts, but live ownership
still belongs to the legacy threads, maps, and clocks. The payoff remains a
hypothesis until an eligible service runs through the durable path, the shadow
and crash gates pass, and M5 removes those owners without introducing a second
scheduler or fallback mutation path.

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

- Implement Serve039 with separate metadata; lease owner/hash/process-scalar and
  process-supersession schema; immutable lineage, terminal-selector, and shadow-
  history stores; exact authority API boot/health lifecycle and Serve-owned GC;
  the fixed warmed supervisor/process pool; post-039 INSERT/BIND, current-owner
  RENEW/acknowledgement, and process supersession; generation-only cancellation /
  terminal batches; and stable/process attestation at every execution boundary.
  Complete the exact `AuthoritySchemaHeadsV2`/policy/candidate/activation /
  dispatch/promotion/rotation codecs, PostgreSQL-versus-SQLite target routing,
  borrowed same-engine API006/terminalization seams, claim-start barrier, lost-
  ACK adoption, bounded historical reducer context, and settled-replay
  validation. The full process/API/GC/concurrency suite and exact-M4 idempotent
  redeploy gate are M4 merge gates; any new forward-fix binary first requires a
  qualified successor two-set policy. The distinct all-M5a -> all-M4
  -> all-M5a binary rollback matrix is the later two-set M5a deployment gate.
  Serve038 or a partial Serve039 runtime cannot enable dispatch.
- Build the exact already-merged PR #1232 artifact, dark-deploy it, and live-
  qualify the implemented M2a/P2a slice:
  six closed preflight envelopes, strict TLS/purpose-token transport,
  `/bootstrapz` versus queue `/readyz`, full static-manifest projection,
  Serve034 release fence, live self-identity observer, PostgreSQL-clock two-Pod
  registration, and post-build OCI qualification. The role starts no request
  executor, the manager never calls it, and its only accepted response is typed
  not-representable. P2a's same-Pod registration refresh is stable-membership
  liveness only; it is not Serve039 current-process owner RENEW. Replacement-
  registration and process-owner protocols remain required before claim routing.
- Before rendering the first enabled cohort, land and live-test the retained
  release/database connection anchor described above. Prove first-enable crash
  recovery, fully cleared values, backend/Secret changes, missing Secret,
  rollback, and ordinary uninstall refusal. Until then the merged P2a artifact
  may be dark-deployed only with
  `resourceActions.authorityWorker.enabled=false`.
- The additive compact V2 action graph and typed authority resolver are now
  implemented without changing the frozen V1 graph. Exact structural
  full-spec goldens
  are 56,994 bytes for realistic launch, 56,977 for the alternate admitted
  launch binding, 45,045 for completed down, and 48,560 for the selected
  candidate-maximal partial down; the latter keeps frozen inputs byte-exact
  and maximizes only declared runtime-derived evidence. Their respective
  SHA-256 values are
  `7d680f846c37326330903064bc210fb73a67e6b7625b1614b17ce9df6feea733`,
  `7392f6792ec560ce4a99884b9bc2dd6ac83a4a5925a936ace27de8fcf458891e`,
  `f638480d05f9283a52c7b1075ab2df9a1a3a8280890f9e01fa10053d3277c82d`,
  and
  `b66dabb27ec6f8cb7fff670bf8a1975228741ea80b8aea2c87cd822dd901c796`,
  as pinned in the checked-in golden test. A separately valid
  nested graph with the generic 1,024-byte workspace maximum renders 62,047
  bytes and proves the unchanged 60,000-byte outer gate rejects oversize
  combinations. The native V2 seed/input, launch-and-down constructors, and
  sole cleanup rederiver are implemented. Before linked admission, finish the
  final V2 artifact/callable inventories and fully expanded representability
  case inventory/enumerator plus CI-only post-inventory goldens; then
  prove every store/runtime boundary invokes both the typed locked-row
  resolver and the applicable contextual validator. The additive V2
  preflight wire/transport is already structural input, not construction or
  qualification evidence. Regenerate the exact full-spec and envelope goldens
  after binding the V2 inventories; the baseline byte counts and hashes above
  cannot qualify that changed graph. The exact 60,851-byte V1 launch remains frozen
  history, not a V2 qualification result; neither budget may be raised.
- Manager-side preparation-capability generation/discard, canonicalization and
  preflight client wiring, runtime use of the store-level atomic admission
  primitive, legacy launch/down instrumentation, and the owner-fenced atomic
  action/replica/capacity/event projection on top of the implemented Serve033
  stores and promotion/retention protocol.
- Manager preparation/client integration with the installed dark preflight
  reader, atomic admission binding, candidate-maximal measurement, and canary /
  live qualification of the provider renderer and normalizers; exact live
  valid preflight; real private-handler
  submit/observe/readback/checkpoint implementation; dispatcher and reducer
  invocation; partial-launch cleanup execution; and
  `attempt_domain_exhausted` event emission. The pure renderer/normalizers,
  cursor/reducer, lineage-safe generic store, strict codecs, execution
  configurations, quiescence construction, and realistic/candidate-maximal
  full-spec goldens are implemented; capped live rendered bodies and preflight
  envelopes still require measurement. Runtime private-handler shadow and
  provider dispatch must consume the API008-only
  `private_handler_dispatch_ready` fence immediately before dispatch.
  Implement that fence as the server-owned same-transaction schema/cohort /
  registration proof, atomically materialize the sole private request/queue
  row, persist the exact underscore routing alias, and invoke the direct
  in-server execution seam without nested SDK submission.
  Authority remains disabled until the runtime and live gates pass.
- The dark API -> ordinary executor -> controller rollout and current-chart
  compatible-image rollback are verified above. Still open are rendered/live
  activation of the dedicated versioned authority-worker Helm cohort, exact
  RBAC/admission/NetworkPolicy, purpose token/TLS preflight,
  static-manifest/live-UID qualification, complete-spec submit/observe, worker
  registration/two-Pod attestation, claim routing/retirement, surviving-API
  tombstone verification, and rollback while nonterminal action/private-shadow
  references pin a cohort.
- A checked-in inventory of the initial `pod_cluster_v1` eligible cohort after
  preallocated cluster UUID propagation, Kubernetes replica-incarnation
  labeling, normalized admitted-object/partial-UID readback, prebooted
  Ray/Skylet and action-keyed job recovery, exact handle/endpoint, dual-LB
  reachability, and the redacted invocation builder pass contract tests. Until
  then the profile is shadow-only and promotion-blocking.
- Measured attainment of the fixed, non-operator-configurable 100 clean launch
  and 100 clean down minima, plus the first canary service selection.
- Authoritative canary evidence followed by deletion of the launch/down
  SafeThread owners, `_replica_to_request_id`, cleanup retry maps/clocks, and
  restart-time legacy inference. Deprecation comments alone do not close this
  gate or earn the payoff.
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
