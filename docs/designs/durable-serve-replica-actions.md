# Durable SkyServe Replica Actions

Status: M0 design accepted; implementation, deployment, and removal gates are
open

Last updated: 2026-07-30

Canonical owner: this file. External plans, pull request descriptions, and
rollout notes must link here instead of restating a divergent contract.

## Summary

SkyPilot's PostgreSQL API request store durably delivers and leases individual
requests, but a SkyServe replica launch or teardown is not yet a stable
database resource across request replacement and controller handoff. The
replica manager currently owns launch and down threads, the replica-to-request
mapping, and cleanup retry clocks in process memory. Recovery reconstructs
intent from persisted replica state and provider observations.

This design adds a PostgreSQL resource-action journal for SkyServe replica
launch and down. A resource action is stable domain intent. Every API request
linked to it is an immutable execution attempt. The existing API request claim
is the only execution lease; this design does not add another worker queue or
a competing lease.

The migration is complete only after the durable action is authoritative,
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
- Reuse the PostgreSQL request queue, claim lease, heartbeat, cancellation, and
  stale-write fencing from PR #1070.
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
- Preserve existing autoscaling, placement, rolling-update, reserved-capacity,
  and load-balancer safety policy.
- Keep every stacked implementation milestone independently testable,
  deployable, and revertible.
- Finish by deleting all legacy operation-ownership code listed in the
  Removal Map.

## Non-Goals

- A second generic queue, worker pool, or action lease.
- Porting dstack's rolling-update algorithm or cleanup give-up deadline.
- Replacing SkyPilot's provider-specific placement and capacity policy.
- A public fleet or resource-action API.
- Migrating managed jobs, image workers, pool-level scheduling/capacity
  ownership, or storage actions in the first implementation. Pool replicas use
  this same journal for their per-replica launch/down side effects because they
  share the replica manager; pools still have no inference endpoint or load
  balancer drain.
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
authenticated, current controller generation. It must not let a public client
claim or replace another action's request.

## Core Model

The two durable objects have deliberately different lifetimes:

```text
Serve desired state
        |
        v
api_resource_actions             api_requests / api_request_queue
one stable logical action  1:N   one immutable execution attempt
domain state and retry clock ---> existing claim token and lease
        |                               |
        |                               v
        +-------------------------- provider mutation
        |
        v
Serve replica observed-state projection
```

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
- `action_spec` is immutable, bounded, redacted JSON containing its schema
  version, cluster name, workspace, durable service/version-spec reference,
  placement and resource overrides, resource scope, logical target/retirement
  facts, whether this is a pool replica, drain deadline for non-pool down, and
  the provider mutation inputs needed to reconstruct an identical request
  after controller loss.

`spec_hash` is SHA-256 over canonical UTF-8 JSON with sorted object keys,
compact separators, no non-finite numbers, and an explicit schema version.
Credentials, environment secret values, and opaque user payloads are excluded;
the spec stores durable references to separately controlled source data. A
change to immutable launch inputs creates a new replica incarnation; it is
never silently treated as another retry. A down action references the same
incarnation and may only be superseded by a durable logical-target or terminal
service transition.

### PostgreSQL schema and migration ordering

Two additive migrations are required. Serve schema 032 runs first and adds:

```text
replicas.replica_incarnation_id UUID nullable during backfill
services.resource_action_mode   TEXT NOT NULL DEFAULT 'legacy'
```

The mode constraint admits only `legacy`, `shadow`, or `authoritative`.
Backfill completion makes the incarnation non-null for every retained replica;
application writes require it from the first 032-capable image. The column is
not dropped on ordinary application rollback.

API-request schema 005 then creates `api_resource_actions`:

```text
action_id                    UUID primary key
resource_kind                TEXT
service_name                 TEXT
service_hash                 TEXT
replica_id                   BIGINT
replica_incarnation_id       UUID
action_type                  TEXT
action_spec                  JSONB
spec_hash                    TEXT
origin                       TEXT
state                        TEXT
current_attempt              BIGINT
current_request_id           TEXT nullable
next_reconcile_at            TIMESTAMPTZ nullable
last_outcome                 JSONB nullable
planner_controller_generation BIGINT nullable
row_revision                 BIGINT
created_at                   TIMESTAMPTZ
updated_at                   TIMESTAMPTZ
completed_at                 TIMESTAMPTZ nullable
```

Required constraints and indexes:

- unique structured logical identity over resource kind, service hash, replica
  ID, replica incarnation, and action type;
- unique non-null `current_request_id`;
- `origin` is immutable and constrained to `native` or `legacy_backfill`;
- `row_revision >= 0`, incremented by every action mutation;
- `current_attempt >= 0`;
- terminal states require `completed_at`;
- `PLANNED`, `RETRY_WAIT`, and `VERIFYING` require
  `next_reconcile_at`;
- `QUEUED` and `RUNNING` require a current request and no due timestamp;
- an index on `(state, next_reconcile_at)` for bounded due-action scans; and
- an index on `(service_hash, replica_id, replica_incarnation_id)` for one
  replica's history.

`api_requests` receives nullable `resource_action_id` and
`resource_action_attempt`, `resource_action_payload_hash`, and
`provider_operation_refs` columns with:

- a foreign key to `api_resource_actions`;
- uniqueness on `(resource_action_id, resource_action_attempt)` when non-null;
- an all-or-none constraint for action ID, positive attempt, and payload hash;
- a bounded JSON array of provider operation/idempotency references stored on
  the immutable request attempt, because one SkyPilot request may perform
  bounded internal placement/failover operations; and
- an index on `resource_action_id`.

Requests that predate this design or are unrelated to resource actions keep
the correlation fields null.

The action's `current_request_id` is an atomically maintained projection, not
a reverse foreign key; adding one would create a cyclic action/request foreign
key. `api_requests.resource_action_id` uses `ON DELETE RESTRICT`, and retention
deletes action/request history as one explicitly ordered operation.

Serve and API-request logical database managers use distinct SQLAlchemy engine
objects but the same physical PostgreSQL database. Cross-table atomicity is
allowed only when the action/request store is passed one explicit
`Connection` and performs all writes through it. It must never call a Serve
manager and request manager that each open or nest their own transaction.
Every action-linked transaction locks in this universal order:

```text
api_resource_actions -> api_requests -> api_request_queue -> event rows
```

Unlinked requests retain their existing fast path. Claim code discovers an
action-linked candidate without retaining a conflicting lock, then acquires
the rows in the universal order and revalidates delivery eligibility.

### Why the action has no independent lease

The request row already persists execution generation, claim token, worker
instance, lease expiry, and heartbeat. Copying those fields into the action
would create two clocks and two ownership authorities. The authoritative
action view joins its `current_request_id` to the request claim.

The action update is fenced by:

- expected action state and row revision;
- expected `current_attempt` and `current_request_id`;
- the request backend's current claim predicates when the worker publishes an
  outcome; and
- the service hash, replica incarnation, logical target, and planning
  lifecycle fence when the controller projects an outcome into replica state.

This is an intentional refinement of the research note's suggestion to
persist a second lease on the action row.

`current_attempt` starts at zero with no request. The first admitted request is
attempt one; every later request is exactly the prior attempt plus one. An
attempt number is never reused. `current_request_id` continues to point to the
latest immutable request after it becomes terminal so reconciliation can
inspect its result and provider operation ID.

### Relationship to `api_controller_action_reservations`

`api_controller_action_reservations` remains a per-request fence for
non-replayable controller-class handlers. Its current logical action ID is the
request ID and its owner is a controller generation. Those semantics are not
changed or overloaded.

`api_resource_actions` is the cross-generation domain journal. The two tables
may reference the same request during execution, but neither replaces the
other.

## State Machine

States are:

```text
PLANNED
QUEUED
RUNNING
VERIFYING
RETRY_WAIT
SUCCEEDED
TERMINAL_FAILED
SUPERSEDED
```

Allowed transitions are closed in code and constrained in tests.

State invariants are:

- `PLANNED` has attempt zero, no request, and a database
  `next_reconcile_at` (immediately due for launch or the durable eligibility
  time for down);
- `QUEUED` and `RUNNING` have attempt at least one and a current request;
- `VERIFYING` has a database `next_reconcile_at`. Native actions have an
  ambiguous terminal/expired current request; `legacy_backfill` may instead be
  attempt zero with no request and a bounded imported-state outcome marker;
- `RETRY_WAIT` has a classified terminal request and a database
  `next_reconcile_at`;
- `SUCCEEDED` and `TERMINAL_FAILED` have `completed_at` and no due timestamp.
  They normally have a terminal current request; observation-proven
  `legacy_backfill` success may be attempt zero with no request;
- `SUPERSEDED` may have no request or a terminal request, but never a live
  request; and
- every transition compares and increments `row_revision`.

Identity, action type, and action spec are immutable. All due times and retry
decisions use PostgreSQL `clock_timestamp()`, never a controller monotonic or
wall clock. `SUPERSEDED` is legal only before any provider mutation could have
started or after provider absence is proven; possible residue advances to
`VERIFYING`. A launch is `TERMINAL_FAILED` only when residue is impossible or
absence has been proven. Down is `SUCCEEDED` only on authoritative absence.
The attempt-zero exception is encoded in database constraints and is not
available to native action admission.

### Launch

1. Under the existing service and policy fences, the controller chooses an
   immutable replica identity and placement.
2. The replica intent, any reserved-capacity fill or paid-capacity claim, and
   `PLANNED` launch action are committed through one caller-owned PostgreSQL
   connection. Every existing persistence variant must accept that connection;
   there is no committed replica-without-action gap. Repeating the transaction
   returns the same action.
3. Admission creates attempt `N` and its API request transactionally, then
   links `current_request_id` and advances the action to `QUEUED`.
4. The API request executor claims the request with the existing request
   lease. The claim transaction locks the action first and advances
   `QUEUED` to `RUNNING` atomically with the request claim.
5. Immediately before the provider mutation, the one-attempt launch handler
   validates the claim token, action/request/attempt correlation, service
   action mode, service hash, replica incarnation, current logical target,
   cluster name, and spec hash. Serve's lifecycle epoch advances on unrelated
   up/update/down/purge lock acquisitions, so it is used only to fence the
   planning or supersession transaction and is not cross-generation action
   identity. This replaces the current PID/IP launch precondition with a
   durable resource fence.
6. Where supported, the action ID and attempt are passed as the provider
   idempotency token. Bounded provider operation references are appended to the
   request with the active claim fence as soon as they become available.
7. Request success and action `SUCCEEDED` are committed together; the
   controller then idempotently projects the replica into the existing
   provisioning/probing lifecycle.
8. A classified retryable failure advances to `RETRY_WAIT` with a database
   timestamp. A later attempt gets a new immutable request row.
9. A configuration, authorization, or other proven terminal failure advances
   to `TERMINAL_FAILED` and enters the existing replica failure policy.

### Down

1. Existing routing, target-generation, idle, and replacement-safety policy
   chooses a victim.
2. The controller persists the existing replica route-removal and drain
   fields plus a `PLANNED` down action through one caller-owned connection
   before any provider mutation. The action references those fields; it does
   not create a second drain-wave state machine.
3. The action remains `PLANNED` while draining. No generic executor is occupied
   by a drain sleep.
4. A fresh load-balancer idle proof or the durable drain deadline makes the
   action eligible for request admission.
5. The down request is executed by a one-attempt correlated request handler
   under the normal API request lease. Immediately before provider mutation it
   applies the same action/service/incarnation/logical-target/claim fence as
   launch.
6. `SUCCEEDED` requires provider absence or an equivalent authoritative
   deletion proof. Only then may the replica row and usage interval be closed.
7. Provider presence schedules another attempt. Provider uncertainty advances
   to `VERIFYING` with `next_reconcile_at` and never deletes the replica row.
8. Cleanup retries have no give-up deadline.

For a pool replica, the pool's existing policy chooses the victim and commits
the down action, but steps involving routing, load-balancer idle proof, and
drain waiting are skipped. The action is immediately due. Provider absence and
all other action/request fences are identical to a non-pool replica.

### Ambiguous owner loss

The request reaper must not replay an externally mutating attempt merely
because its lease expired. For an action-linked mutator it atomically
terminalizes the expired request, deletes its queue delivery, records the
existing operational event, and advances the action to `VERIFYING` with a due
database timestamp. It never enqueues that request again.

For launch:

- matching scoped cluster/provider resource exists: adopt and continue;
- provider proves absence: schedule the next attempt;
- provider is unavailable or uncertain: remain `VERIFYING` and retry the
  observation later.

For down:

- provider proves absence: complete cleanup;
- resource exists: schedule another idempotent down attempt;
- provider is unavailable or uncertain: retain the action and replica and
  retry observation later.

A stale request worker cannot publish after its claim expires. A successor
does not issue a new provider mutation until the ambiguous attempt has been
reconciled.

### Atomic request/action completion

The PostgreSQL request store's `_terminalize_locked_request()` transaction is
the sole completion boundary. For an action-linked attempt it also validates
and updates the locked action row. In one commit it:

1. writes the terminal request status, result or error, and final bounded
   provider operation references;
2. deletes the request's durable delivery;
3. advances the action to `SUCCEEDED`, `TERMINAL_FAILED`, `RETRY_WAIT`, or
   `VERIFYING` with its normalized outcome and next due timestamp; and
4. emits the existing PR #1074 operational event.

There is no supported state in which an action result is committed before its
request terminalization. A crash can occur after this joint commit and before
the Serve replica projection; the promoted controller must reapply that
projection idempotently from the action/request rows.

## Idempotent Request Admission

The controller includes the action ID, proposed next attempt, expected action
revision, spec hash, canonical request payload hash, and controller generation
in a private action-admission context. The context is accepted only from an
authenticated controller-class caller after validating the current durable
controller generation; a public client cannot self-assign action correlation.
The API performs one transaction:

1. lock the action row;
2. validate the expected state/revision, exact next attempt, spec hash,
   canonical payload hash, activation mode, and controller generation;
3. insert or retrieve the unique request for `(action_id, attempt)`;
4. insert durable queue delivery if this is the winning insert;
5. set `current_attempt`, `current_request_id`, and `QUEUED`; and
6. commit.

The request payload hash uses the same versioned canonical-JSON algorithm as
the action spec and covers the full provider-mutating request after server-side
normalization. The full body remains in the request row under its existing
controls; the digest is used only for equality.

If the HTTP response is lost, retrying the same proposed attempt with the same
hash returns the existing request ID without modifying the action. The route
sets a trusted-only `request.state.response_request_id` to that canonical
stored ID after admission, and `RequestIDMiddleware` prefers that field after
the route returns when setting `X-Skypilot-Request-ID`. Ordinary routes cannot
set the override. It must not return the random exchange ID initially generated
by middleware.

The action dispatcher capability-checks API-request schema 005 and the private
admission contract before use. An old server accepting the underlying launch
or down as an uncorrelated ordinary request is a hard error, not a fallback.

The API refuses:

- a different payload or spec hash for an existing action attempt;
- a proposed attempt other than the existing idempotent attempt or exactly one
  higher than the durable attempt;
- admission from a stale controller generation;
- admission for a terminal or superseded action; and
- reuse of one request by two actions.

## Reconciliation and Scheduling

The active controller remains the policy owner. It performs bounded indexed
queries for:

- `PLANNED` actions eligible for admission;
- `RETRY_WAIT` actions whose `next_reconcile_at` is due;
- active actions whose request became terminal; and
- `VERIFYING` actions whose `next_reconcile_at` is due.

The action table is not a worker queue. It records desired state and a unified
reconciliation time for drain eligibility, ambiguity observation, and retry.
Only `api_request_queue` assigns execution work and leases it to a worker.

There is no resource action for an autoscaling decision, rolling-update wave,
load-balancer cutover, or drain wave. Their existing Serve replica/service
fields remain authoritative. The journal covers only per-replica external
launch and down side effects.

The controller batches request-status reads and action transitions. It does
not create one polling thread per replica. A controller handoff simply resumes
the same indexed reconciliation queries.

Launch and down execute as dedicated one-attempt Serve action handlers (or an
equivalently fenced internal mode of the existing endpoints). They contain no
controller-local retry loop. Backend-internal bounded placement/failover may
remain within one request attempt and is reflected in that attempt's provider
operation reference list. A failed launch does not admit another attempt until
cleanup/absence is proven; uncertainty always enters `VERIFYING`.

Existing global and per-service provisioning/down concurrency rules remain in
force. During migration, admission counts both legacy threads and active
actions. In steady state, durable active-action counts replace thread-map
counts; no controller-process file lock is an ownership primitive.

## Typed Outcomes

`last_outcome` is a versioned JSON object:

```text
version
kind: no_capacity | quota | auth | invalid_config |
      transient | interrupted | unknown
scope: resource | zone | region | cloud | account | unknown
retry_after: optional timestamp
cleanup_certainty: not_started | uncertain | present | deleted
failover_safe: boolean
provider_details: bounded, redacted object
```

Existing exception classification remains a compatibility producer during the
first migration. Provider adapters should emit the normalized outcome at the
source as later stacked commits land. Raw serialized errors remain on the API
request; the action stores only the bounded decision record.

## Migration Plan and Stacked Commits

### M0: Canonical design

- Commit this design before implementation.
- Run adversarial review against this exact file.
- Resolve every blocking review finding in place and re-review.

### M1: Additive schema and shadow journal

- Add Serve schema 032 and PostgreSQL-only API-request schema 005 in that
  order.
- Assign and backfill stable replica incarnation UUIDs and add the per-service
  activation mode.
- Add typed action models and a fenced store.
- Shadow-create actions for new Serve launch/down decisions.
- Persist action/request correlation and cleanup reconciliation timestamps
  while the
  existing thread implementation remains authoritative.
- Add schema, state-machine, idempotent-admission, and PostgreSQL concurrency
  tests.
- Deploy to the isolated `skypilot-ha` test release and prove migrations,
  shadow parity, rollback, and cleanup.

Rollback: stop shadow writers, retain both additive schemas, and deploy the
prior application image only while every service remains `legacy` or
`shadow`. Schema downgrade is an exceptional operator action allowed only when
there are no action rows and every service is `legacy`; ordinary app rollback
does not drop incarnation identity or action history. If explicitly approved,
schema downgrade runs API-request 005 down before Serve 032 down.

### M2: Durable recovery and retry authority

- Recover current request IDs and request results from actions.
- Move launch/down attempt and cleanup retry scheduling to PostgreSQL wall-clock
  timestamps.
- Reconcile ambiguous attempts before replacement requests.
- Continue using the legacy thread only as an execution adapter; it no longer
  owns logical identity or retry policy.
- Add controller-handoff and crash-boundary tests.
- Deploy, kill controllers at every boundary, and verify adoption and cleanup.

Rollback: pause new action admission, wait for or reconcile active attempts,
project each nonterminal action into the legacy replica fields, and deploy the
M2 compatibility image. Do not lower a service's activation mode and do not
start a pre-032 controller. The design must record exact live rollback evidence
before M3.

### M3: Durable request execution authority

- Submit launch/down provider work as correlated API request attempts.
- Move drain waiting into the durable action phase.
- Replace per-replica thread polling with batched action/request
  reconciliation.
- Count durable active actions for admission.
- Advance each service from `shadow` to `authoritative` only after every
  controller-capable old binary has drained, all live replicas have incarnation
  IDs, both schema revisions are verified, and the target image advertises the
  action capability.
- Deploy and run the full fault-injection matrix.

Rollback: activation is one-way (`legacy -> shadow -> authoritative`) for a
service. Fleet rollback keeps the schema and marker, pauses admission, uses the
M2 projection protocol, and deploys only the compatibility image that
understands `authoritative`. A pre-capability controller must never run
concurrently with or after an authoritative service unless every such service
has first been fully removed.

### M4: Remove legacy ownership

- Delete every steady-state legacy path in the Removal Map.
- Delete compatibility tests that only instantiate removed maps or threads and
  replace them with durable action invariants.
- Keep only explicitly fleet-gated readers needed for old persisted replica
  rows.
- Deploy the removal image, roll back to the last M3-compatible image, and
  re-upgrade.
- Record exact test and live evidence here.

### M5: Fleet-gated persisted-format cleanup

- After every active service has a durable action-capable replica format and
  the rollback window excludes old readers, remove legacy serialized
  `sky_launch_status` / `sky_down_status` fields and the redundant replica
  column.
- Record the fleet query and date that closed the gate.

## Existing-Service Backfill

M1 does not manufacture completed action history for every healthy replica.
It creates actions on the next relevant transition.

At M2/M3 activation, nonterminal replicas are classified conservatively:

- pending/provisioning with a correlated active request: attach to it;
- pending/provisioning without correlation: create `VERIFYING`, observe the
  scoped cluster, and never immediately relaunch;
- shutting down or failed cleanup: create `VERIFYING` down and prove provider
  state;
- ready replicas: no active action is required;
- terminal failed replicas without possible provider residue: no active action
  is required; and
- malformed/legacy identity: retain the existing fail-closed recovery path and
  emit an operator-visible migration error.

Backfill is idempotent and fenced by service hash, replica row lock,
incarnation ID, and lifecycle epoch. It never guesses identity from cluster
name alone.

Backfilled actions use `origin=legacy_backfill` and start in `VERIFYING` at
attempt zero with no synthetic API request. Observation then follows an exact
contract:

- matching launch resource: adopt and enter observation-proven `SUCCEEDED` at
  attempt zero;
- absent launch resource: enter immediately due `PLANNED`, then admit attempt
  one;
- absent down resource: enter observation-proven `SUCCEEDED` at attempt zero;
- present down resource: enter immediately due `PLANNED`, then admit attempt
  one; and
- uncertain observation: remain attempt-zero `VERIFYING` with a new database
  due time.

The outcome records that success was observation-proven and the provider
certainty used. This preserves the no-manufactured-history rule while keeping
native action/request invariants strict.

## Activation and Mixed-Version Protocol

`services.resource_action_mode` is durable and monotonic:

- `legacy`: existing threads and fields are authoritative; no action is
  required;
- `shadow`: existing execution remains authoritative and every eligible
  launch/down transition is journaled and parity-checked; and
- `authoritative`: action/request state owns admission, retry, ambiguity, and
  completion, while legacy fields are compatibility projections only.

Newly created services use the deployment's fleet-gated default; adding the
column does not silently change existing services. A current-generation
controller performs each transition under a service-row lock and records an
event. No code path decrements the mode.

Before the first authoritative transition, the rollout gate proves that all
controller-capable pods and controller subprocess launchers advertise Serve
032/API 005 support and that old consumers are drained. During an application
rollback, database migrations normally remain applied. The compatible M2
image can read authoritative rows, stop new admission, reconcile or project
every active action, and preserve safety while the fault is repaired. Rolling
back to an image that ignores the marker is forbidden.

## Deployment and Rollback Protocol

Every milestone deployment uses the guarded HA release described in
`docs/designs/multi-replica-api-server.md`:

- Kubernetes context `boltz-test`;
- namespace and Helm release `skypilot-ha`;
- PostgreSQL request backend;
- blocking migration hook before target-image pods; and
- `--reuse-values` on upgrades.

The canonical isolated target remains `boltz-test/skypilot-ha/skypilot-ha`.
Discovery on 2026-07-30 found only an expired SSO, private production path and
no usable local credentials or image builder for that target. Production
`gitops-hub-rainier/skypilot/skypilot` is explicitly out of scope for iterative
fault injection. No milestone may substitute production for the missing test
target; test-cluster access and an immutable-image publication path are an open
deployment prerequisite.

For every image:

1. preserve the current Helm values and immutable image digest;
2. build and push the exact commit;
3. run the migration hook;
4. wait for two Ready API, executor, and controller replicas;
5. verify the schema revision and action activation phase;
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
5. after the provider may have accepted/applied the mutation but before the
   caller receives a response or can persist an operation reference;
6. after the provider handler returns but before the single request/action
   terminal transaction;
7. after atomic request/action terminalization but before replica projection;
8. after lease expiry, with no reassignment until provider observation;
9. during controller generation handoff;
10. during API and executor pod eviction; and
11. during rollback and re-upgrade.

For every point, assert:

- one logical action and at most one active request attempt;
- no duplicate logical replica;
- a stale worker cannot commit;
- ambiguous launch is observed/adopted before retry;
- down never succeeds without deletion proof;
- retry attempt and `next_reconcile_at` survive every restart;
- no old-generation resurrection;
- old serving capacity remains until existing replacement gates permit
  retirement; and
- final request, action, replica, usage, and provider state agree.

Required automated coverage:

- migration upgrade/downgrade and schema constraints on real PostgreSQL;
- concurrent action upsert and concurrent idempotent request admission;
- request-lease expiry and stale claim-token rejection;
- atomic request terminal status, queue deletion, action outcome, and
  operational-event commit/rollback;
- controller leadership handoff;
- launch/down state-machine unit tests;
- retry timing with the database clock;
- existing Serve autoscaling, rolling, logical-replica, reserved-capacity,
  load-balancer, and cleanup regression suites;
- Helm migration ordering and mixed-version rendering; and
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

These paths are retained only for the stated migration phase. They must be
deleted, not merely bypassed, when their gate closes.

### Process-local launch ownership — remove in M4

From `sky/serve/replica_managers.py`:

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

The provider launch implementation, task construction, security-group scoping,
placement policy, capacity classification, and service-owner precondition
remain. `ServiceReplicaLaunchPrecondition`'s PID/IP ownership form is removed;
its service hash/status checks move into the correlated action fence together
with replica incarnation and current attempt.

### Process-local down ownership — remove in M4

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

The fail-closed provider deletion check, load-balancer idle proof, durable drain
start, lifecycle fences, and provider cleanup implementation remain.

### Full-service and ownerless teardown bypasses — remove in M4

The journal must own committed per-replica downs even outside the steady-state
replica manager. Remove or route through durable down actions:

- the `SafeThread` replica teardown pool and status polling in
  `sky/serve/service.py`'s full-service cleanup path;
- the direct failed-service and orphan-service `terminate_cluster()` parallel
  loops in `sky/serve/serve_utils.py`;
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

### Process-local cleanup scheduling — remove in M2/M4

From `sky/serve/replica_managers.py`:

- `_failed_cleanup_retry_attempts`;
- `_failed_cleanup_retry_at`;
- `_failed_cleanup_retry_state()`;
- `_clear_failed_cleanup_retry()`;
- `_schedule_failed_cleanup_retry()`; and
- monotonic-clock eligibility in `_reconcile_failed_cleanup()`.

The replacement reads `attempt`, `next_reconcile_at`, and outcome from the
durable down action. Infinite fail-closed retry remains steady-state behavior.

### Replica persisted compatibility state — remove in M5

After the fleet gate:

- `ReplicaStatusProperty.sky_launch_status` compatibility reads/writes;
- `ReplicaStatusProperty.sky_down_status` compatibility reads/writes;
- JSON serialization keys used only by those fields;
- the redundant `replicas.sky_down_status` projection column and its migration
  compatibility code; and
- recovery branches whose only input is legacy `SCHEDULED` / `RUNNING`
  process status rather than a durable action.

Replica lifecycle status, logical-retirement commitments, drain timestamps,
placement attribution, planned capacity, and failure evidence remain.

### Process-local resource admission lock — conditional M4 removal

Remove `controller_utils.get_resources_lock_path()` use around replica
launch/down thread admission once durable active-action accounting is the only
cross-service admission source. Retain the lock if a separately documented
non-action consumer still requires it; document that consumer and narrow the
lock instead of leaving action ownership coupled to it.

### Tests — remove or rewrite with their production paths

Delete tests whose only contract is manual construction or cleanup of:

- `_launch_thread_pool`;
- `_down_thread_pool`;
- `_replica_to_request_id`;
- `_replica_to_launch_cancelled`;
- `_failed_cleanup_retry_attempts`; or
- `_failed_cleanup_retry_at`.

Replace them with tests over durable action identity, request correlation,
database retry time, ambiguous reconciliation, and stale-write rejection.
Preserve tests of placement, capacity, rolling safety, drain correctness,
provider deletion proof, and service lifecycle fencing.

### Steady-state mechanisms that must not be removed

- `api_request_queue`, request claim leases, heartbeats, and claim-token
  fencing;
- `api_controller_leadership` and controller generations;
- `api_controller_action_reservations` for non-replayable controller requests;
- service hash, lifecycle epoch, and launch preconditions;
- ownerless cluster reconciliation from PR #1071;
- PR #1074's atomic terminal request event and audit history;
- fail-closed cleanup and provider absence proof;
- version quarantine and rolling replacement safety;
- load-balancer routing, idle, drain, cutover, and rollback fences; and
- managed-job, pool-level scheduling/capacity, and image-worker ownership
  systems outside this scope. Pool replica launch/down side effects are in
  scope.

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

Rejected because logical ownership, retry timing, and completion would remain
split between database rows and controller-process objects. It is permitted
only through M2.

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
No implementation or live acceptance evidence has been recorded yet.

Each stacked commit must add exact commands, counts, commit SHA, image digest,
Helm revision, fault-injection result, and final cleanup evidence here. A test
name or green CI link is insufficient unless its assertions cover the stated
invariant.

## Closed Gates

- M0 adversarial review accepted on 2026-07-30.

## Open Gates

- Access to the isolated `boltz-test/skypilot-ha/skypilot-ha` deployment target
  and its immutable image publication path is verified.
- M1 additive schema and shadow journal implemented and deployed.
- M2 durable recovery and retry authority implemented and deployed.
- M3 durable request execution authority implemented and deployed.
- M4 legacy process-local ownership deleted and rollback/re-upgrade accepted.
- M5 fleet query proves no old persisted status readers remain.
- Full automated regression suite and live fault-injection matrix pass.
- Final `skypilot-ha` release is healthy and the test namespace is clean.
