# SkyServe Non-Pool Launch Request Boundary

Last updated: 2026-08-15

Status: the dedicated SkyServe resource-action provider facet is retired before
activation. This companion defines the one existing API-request boundary that
all central-PostgreSQL non-pool launch profiles will reuse. The ordinary-only
R1--R3 work is a transition foundation; the 2026-08-15 mixed-version incident
requires a generalized G1 transition and simultaneously authored blocked G2
cleanup. Neither is merged or deployed.

## Decision

SkyServe continues to use the existing `sky.launch` implementation and API
request executor. It will not introduce a native Kubernetes
renderer, dedicated authority worker, private transport, execution-worker
cohort, provider-specific action client, second credential plane, or second
provider outcome hierarchy for the controller-restart issue.

The prior V1/V2 capsules, representability inventories, authority policies,
Serve038/039 state, and private handlers never formed a production
admission-to-effect path. Their structural tests and dark packaging runs prove
only that inert code could ship. They do not prove provider authority, crash
recovery, operational payoff, or safe removal of current Serve owners.

On 2026-08-11, the operator explicitly directed completion of the remaining
accepted work. That satisfies the parent design's localized correctness gate
without supplying evidence for the rejected physical-capacity or authority
architectures. The first stacked change is PostgreSQL Serve041 handoff
telemetry; the focused request binding remains the next change and must preserve
the contract below.

On 2026-08-15, incident evidence corrected the premise that surviving
generation-zero request fields imply no execution. An old mixed-version
executor claimed requests and caused real provider effects without the current
receipt protocol. Later cancellation/storage showed generation zero, missing
process identity, and `execution_quiescence_required = false`. Those values are
not absence proof, and no migration or operator repair may fabricate a
quiescence receipt. The long-term boundary must therefore cover every non-pool
profile with an exact capability cohort and typed reconciliation.

## Goals

- Bind every non-pool Serve launch to the exact existing API request before that
  request is eligible to execute.
- Adopt or inspect that request after controller restart instead of submitting
  another request for the same replica record and launch generation.
- Keep the API request claim and execution generation as the sole
  execution lease.
- Preserve current provider selection, credentials, request logs, cluster
  locking, launch retries, and cleanup behavior.

## Non-goals

- A new provider mutation API or provider-specific outcome hierarchy. The
  small closed reconciliation/provider-evidence vocabulary below is Serve
  recovery state, not another provider implementation.
- Direct construction of Kubernetes objects for Serve replicas.
- A cross-provider exact-locator or provider-absence protocol.
- Bypassing `sky.launch` or recursively invoking the public SDK from a new
  worker.
- Generalizing the solution to every cloud mutation or lifecycle domain.
- Replacing the current down or cleanup path.
- Replacing reserved-fill, paid-capacity, system-OOM, replacement, rebalance,
  or placement planners. Their exact authorization is input to this boundary,
  not authority recreated by it.
- Generalizing into a capacity scanner, observation cache, occupancy ledger,
  provider renderer, scheduler, or cross-domain mutation worker.

## Request contract

The existing API request remains the execution record. The generalized
internal non-pool launch seam performs the following sequence:

1. Accept one controller-generated stable submission UUID in the dedicated
   endpoint body; never derive binding identity from the fresh ID assigned to
   each HTTP attempt by middleware. Deterministically derive the association
   and exact API request IDs from that submission plus authenticated tenant
   scope.
2. Accept one closed immutable profile: `ORDINARY_PAID`,
   `ORDINARY_ZERO_COST`, `RESERVED_FILL`,
   `UNKNOWN_CAPACITY_REPLACEMENT`, `COST_REBALANCE`, or
   `SYSTEM_OOM_RECOVERY`. Canonicalize its kind/version, domain-authorization
   references, and digest server-side.
3. Verify the exact built-in PostgreSQL request/queue backend, API011 and
   Serve047 heads, promoted per-service binding/cohort epochs, and the complete
   API/request-backend/executor/GC/controller/profile-participant capability
   and lease-drain barrier.
4. Construct the distinct generic bound request with the normal request
   serializer. In one PostgreSQL transaction and connection, lock the
   lifecycle fence, service, replica, current association, profile authority,
   request, queue, and retention pin; insert the association, complete
   correlated request, request-retention pin, and queue row, and set the
   replica association pointer. Reserved-fill row acceptance and binding are
   one commit; its `FillCommitResult` names the exact association/request IDs.
5. Commit the complete row set together. Queue visibility begins only at
   commit, after the binding is durable. There is no committed activation gap
   and no sweep creates a missing queue row.
6. On a lost acknowledgement, the same submission/request identity, profile,
   cohort, and exact canonical server-computed digest return the committed
   request ID in the endpoint response body; any mismatch fails closed.

An old API server does not understand this sequence and must not receive a
binding-enabled launch. Unknown request context is not a safe fallback because
an old `/launch` endpoint would execute it unbound. API009's hard-coded
ordinary handler and boolean capability are not a generalized capability.
API011 adds one versioned generic handler/profile constraint and exact
capability-profile digest. The controller uses the legacy path only during G1
and only until every recent API target, request backend, queue executor, GC
participant, possible controller owner, and profile participant advertises the
same immutable digest and old/recent leases pass the complete stale/quiescence
window. Queue candidacy and locked claim require the generic handler plus exact
profile version to be locally supported, so a stale old executor leaves the
row queued. Per-service controller capability and cohort epoch are persisted
with the fresh controller-incarnation UUID and monotonic service owner epoch;
they are not inferred from a supervisor lease. The service, association, and
request carry the same epoch.

The exact built-in PostgreSQL request backend owns this cross-table
transaction, and both schema lineages must resolve through the same physical
database/connection. Serve never reserializes a private copy of the request or
coordinates two engines. SQLite, plugin backends, and schema mismatch fail
closed. An active correlated request without a queue row is invariant
corruption. Generation zero, no process identity, or
`execution_quiescence_required = false` is not effect evidence by itself. Only
an exact current-handler/current-cohort request with no claim, effect phase
`NOT_STARTED`, and the current receipt contract can become
`PRE_EFFECT_TERMINAL`. A legacy or mixed-version request with those same stored
fields is `LEGACY_EFFECT_AMBIGUOUS`; an exact current request that may have
crossed an effect boundary is `POST_EFFECT_AMBIGUOUS`. Startup never
synthesizes execution, quiescence, association identity, or provider absence.

The existing Serve042 association is a separate central-PostgreSQL record, and
the replica row points to its current association. It does not reuse
`ReplicaInfo.launch_request_id`, whose historical validation is specific to
system-OOM recovery, and it does not reinterpret nullable Serve033 action
columns. Serve047 extends the association in place with immutable profile kind,
version, digest, domain-authorization references, and cohort epoch plus typed
reconciliation/provider evidence. It creates no second table or dual write.
Services default to durable `legacy` mode; promotion to `bound` is an
explicit per-service transaction that requires no nonterminal legacy non-pool
request or unbound `PENDING`/`PROVISIONING` replica. An explicit rollback
demotion is allowed only after every bound association is terminal, quiescent,
copied, projected and unpinned and no launch generation is active; it increments the
binding epoch before an incapable controller may own the service.

Immutable association identity includes the durable service workspace as well
as name/hash/version. The active-only generic request retention pin has a
request FK with `ON DELETE RESTRICT`/`NO ACTION`; both GC selection and final
delete require it to be absent. Projection explicitly deletes the pin and
records release time on the association rather than cascading it away.

Reconciliation has closed execution outcomes `ACTIVE_ADOPT`,
`RESULT_RECORDED`, `PROJECTED`, `PRE_EFFECT_TERMINAL`,
`POST_EFFECT_AMBIGUOUS`, and `LEGACY_EFFECT_AMBIGUOUS`. Provider evidence is a
separate closed value: `NOT_QUERIED`, `PRESENT`, `ABSENT`, `UNKNOWN`, or
`REPLACED`. Timeout, partial enumeration, malformed identity, RBAC denial, and
same-name/new-UID mismatch are never absence. Exact provider/result evidence
may settle one ambiguous row; terminal request fields alone may not.

Startup schedules bounded reconciliation per association. It starts the
prober, coordinator, autoscaling, and route publication without waiting for a
fleet-wide repair. Provider calls, polling, backoff sleeps, and network waits
hold no manager/global recovery lock. One poisoned association quarantines
only its replica and profile authority (for reserved fill, the exact pool/card/
grant); sibling rows and pools continue. A legacy cluster-name scan is
transition-only evidence collection for already-durable unbound rows. It never
creates a binding or receipt.

## Executor fence

Before the existing executor invokes launch, a small internal check confirms:

- queue candidacy and the locked claim both support the distinct generic bound
  handler and exact profile version;
- the request claim/token/worker lease and execution generation are current;
- the association points to this exact request;
- the exact service and replica record still exist;
- the row still wants the associated non-pool launch generation;
- the normalized input and profile digests are unchanged;
- the capability-cohort epoch is current;
- the profile planner's exact authorization still holds; and
- the association owner/revision matches the current durable
  service-controller owner epoch/revision for this service.

A failed check terminates before launch. Under the existing shared service
launch-authority guard, the association advances from `NOT_STARTED` to
`PROVIDER_IO` immediately before provider work. The service-job boundary
advances to `SERVICE_JOB_IO` before its effect and records
`SERVICE_JOB_RECORDED` plus the returned ID afterward; a crash between those
writes is durable may-have-submitted ambiguity. No new worker, provider client,
or renderer is introduced.

This fence records only the existing launch and service-job boundaries.
It does not claim cross-provider effect absence: a terminal/quiescent request
that crossed `PROVIDER_IO` without an exact projectable outcome remains
ambiguous and cannot authorize a successor.

## Adoption, result, and cancellation

On restart, the controller loads the association by exact
`replica_record_id` through the replica pointer. The service-owner CAS installs
a fresh controller-incarnation UUID, advances a durable monotonic owner epoch,
and transfers every unresolved association in the same transaction under the
exclusive service launch-authority guard. While
the bound request is queued, running, succeeded but not yet reduced, or not
proven pre-effect terminal, the new owner adopts/inspects it and must not submit
another request. The guard makes takeover wait for an opaque provider call
already in flight; it never replays that call.

Current Serve launch payloads embed the old controller PID/IP and are rejected
after owner replacement. Bound launches instead carry the association ID; the
executor validates the current association owner. Owner replacement alone is
not cancellation and bypasses the legacy unconditional owner-loss cancel.
Cancellation targets the exact bound request only after the current owner has
atomically committed supersession or teardown using server-side predicates on
service owner epoch, association revision, request, and replica record. A
controller-side read followed by generic cancel is forbidden. A controller
that lost the epoch/revision cannot publish, cancel, or project it.

Generic request terminal and quiescence transactions remain request-only. The
retention pin preserves their immutable evidence. A Serve reducer takes the
canonical service/replica/association order, reads exact request status/cause,
generation, result/service-job ID, and quiescence without reversing that lock
order, then commits the replica update, association projection, and pin release
together. It may create a successor only from a `NOT_STARTED` pre-effect
terminal predecessor whose same replica record still wants a retry and whose
exact current handler/cohort receipt proves no effect. A legacy request with
generation zero does not meet that predicate. Unclear or post-effect state is
operator-visible typed ambiguity and blocks automatic replacement.

`PRE_EFFECT_TERMINAL` is settled only when the same transaction copies exact
terminal/quiescence, projects the retryable failure, clears the replica
pointer, and deletes the active pin. The partial unique constraint treats
active, cancel-requested, `RESULT_RECORDED`,
`POST_EFFECT_AMBIGUOUS`, and `LEGACY_EFFECT_AMBIGUOUS` rows as unsettled.
A successor requires `PRE_EFFECT_TERMINAL`, a clear pointer, no pin, and an
unchanged service binding/cohort epoch plus fresh profile authorization;
terminal/quiescent evidence alone is insufficient.

A later same-name replica has a different `replica_record_id` and cannot adopt,
cancel, project, or delete its predecessor's request.

Teardown and any other pointer/record/generation invalidation serialize with
the service launch-authority guard: commit fenced cancel intent, cancel the
exact request, prove its execution quiescence, then delete/replace the replica
and retain the association tombstone. A normal ORM row delete may not race the
pre-I/O or service-job fence.

## Compatibility and rollback

Rollout is capability-gated and mixed-version safe:

- G1 owns forward-only API011 and Serve047; new schema and tolerant readers ship
  before generic binding writes;
- every service initially persists `legacy` mode;
- old controllers continue the legacy path and cannot promote a service;
- new controllers bind only after all recent API acceptors, request backends,
  queue executors, GC, possible service controllers, and profile participants
  advertise one exact handler/profile/receipt digest and incapable ready plus
  non-ready-recent leases pass the full stale/quiescence window;
- no single logical launch is eligible through both paths; and
- binding enablement is one-way per service after zero unbound active requests
  and zero unbound `PENDING`/`PROVISIONING` rows.

Existing bound ordinary associations are deterministically backfilled in place
to `ORDINARY_PAID/v1` or `ORDINARY_ZERO_COST/v1` only when immutable request,
association, and replica identity agree. No historical unbound request receives
a synthetic association. The seven incident rows are reconciled individually
from exact provider/result evidence and retain `LEGACY_EFFECT_AMBIGUOUS` until
settled.

Rollback first disables new promotion, then waits for every bound request to
become terminal, quiescent, copied, projected, and retention-unpinned, then
performs the fenced durable demotion. Only then may a controller/API/executor
image return to a version that lacks the binding capability. An incapable
controller can never own a service still in `bound` mode. Rollback never clears
or rewrites an association and never downgrades PostgreSQL schema. Reserved-
fill activation remains generation-fenced and one way: once active, rollback is
only to a capability-compatible artifact after bound work drains, never to the
unbound fill path.

API012/Serve048 belong to the intervening demand-feed and provider-free route
convergence. G2 is authored with G1 and that change but remains blocked. It
owns API013 and Serve049; any earlier API011 combined-role cleanup or Serve047
reserved-fill final cleanup is renumbered. After the rollback and monitoring
gates, G2 removes unbound
non-pool admission/recovery, old handler/profile aliases, global startup
recovery locking/backoff, cluster-name quiescence authority, process-map
authority, demotion/promotion transition surfaces, and transition telemetry.
Fresh central-PostgreSQL non-pool work is always bound. Historical unbound
`READY` rows remain readable but have no active recovery path. Pools remain
separate.

G2 requires zero legacy-capable participants, zero active/unsettled old-handler
requests, zero unbound non-pool rows requiring recovery, one controller restart
and ordinary service update, complete readiness/+10/+30 monitoring, one
180-second Serve authority horizon plus the stale-writer/quiescence horizon,
bounded manager-lock and fresh-route evidence, broker conservation/no paid
spill, and a rollback rehearsal. After G2, rollback is fix-forward only.

Projected association tombstones are retained for at least 60 days by the
database clock. Bounded GC may remove one only after exact quiescence,
projection, pin release, and proof that no replica pointer or retained request
still references it. Unresolved or ambiguous associations have no age-based
deletion. This keeps history bounded without allowing a numeric replica-ID
successor to inherit stale work.

## Verification

PostgreSQL and fault-injection tests cover timeout before atomic commit, lost
response after commit, exact and conflicting stable-key retries, startup
corruption, claim, immediately before provider and service-job effects,
terminal/quiescence copying, Kubernetes Pod creation, request GC, teardown,
provider-free route publication, and Serve projection. They
prove:

- atomic association/request/pin/queue commit and no valid active request
  without a queue row;
- startup marks a correlated missing-queue row ambiguous and never
  manufactures execution;
- exact retry returns the committed request and digest conflict inserts
  nothing;
- an old executor cannot claim a locally unsupported bound handler;
- the exact 2026-08-15 mixed-version case performs a real provider effect yet
  remains `LEGACY_EFFECT_AMBIGUOUS` when terminal storage says generation zero,
  missing process identity, and quiescence-required false; no receipt is
  fabricated and no successor is admitted;
- a distinct current-protocol generation-zero/no-claim/`NOT_STARTED` case is
  `PRE_EFFECT_TERMINAL` only with exact handler/cohort/receipt proof;
- compare-and-swap handoff and exact adoption after controller restart while
  queued, claimed, and inside the existing launch/provider call;
- at most one API request eligible per replica record and launch generation;
- at most one service-job submission for that logical launch;
- a crash after service-job I/O starts but before its ID is recorded remains
  ambiguous and never creates a successor;
- no cancellation on a valid owner handoff, and exact cancellation on
  supersession, teardown, or failed handoff;
- an active retention pin blocks both request selection and deletion, and exact
  projection releases it;
- terminal/quiescence publication racing fenced cancel/projection has no
  request-association lock inversion and preserves one exact outcome;
- teardown proves exact-request quiescence before deleting/replacing a replica;
- no predecessor result/cancel/delete applied to a successor record; and
- provider `PRESENT`, exact `ABSENT`, `UNKNOWN`, and `REPLACED` identity
  handling, with timeout/partial/RBAC failure remaining unknown;
- one poisoned association among hundreds does not block probes, routes,
  autoscaling, or sibling reserved pools/cards, and no provider/network wait
  occurs under a manager/global recovery lock;
- exact route generation and `(replica_id, replica_record_id)` identity prevent
  stale demand/result/URL evidence from affecting a successor; and
- safe mixed old/new API, controller, executor, GC, and profile-participant
  behavior.

G2 source-absence tests reject any remaining unbound non-pool admission or
recovery, old handler/profile alias, cluster-name effect authority, global
startup recovery lock/backoff, process-map authority, or transition-only
surface. The material generalized correction resets the prior three
ordinary/Serve047 review rounds; three consecutive reviews are rerun on the
exact frozen G1/G2 and reserved-fill heads.

An approved live canary uses the existing provider implementation and compares
the durable association, API request history, service-job history, and Serve
projection. It does not claim provider-level exactly-once or absence semantics.
Any canary creating capacity requires the exact cost/capacity plan and explicit
approval in the parent design.

## Historical evidence

PR #1232 proved that a disabled preflight topology and additive schema could be
packaged. Later dark runs kept authority false and relevant tables empty. PRs
#1332, #1333, #1335, and #1342 added only uncalled contracts, forward schema,
preflight, and renderer/representability evidence; closed PRs #1336, #1338, and
#1343 never completed admission, claiming, execution, terminalization, or
legacy-owner removal. The merged dark islands are retired together, and none
of that evidence is reclassified as non-pool launch-binding qualification.
