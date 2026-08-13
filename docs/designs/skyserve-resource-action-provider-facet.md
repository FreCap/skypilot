# SkyServe Ordinary-Launch Request Boundary

Last updated: 2026-08-11

Status: the dedicated SkyServe resource-action provider facet is retired before
activation. This companion defines only the existing API-request boundary that
the now-correctness-mandated ordinary-launch binding will reuse. R1 is
diagnostic only; it does not alter this request boundary.

## Decision

SkyServe continues to use the existing `sky.launch` implementation and
ordinary API request executor. It will not introduce a native Kubernetes
renderer, dedicated authority worker, private transport, execution cohort,
provider-specific action client, second credential plane, or provider outcome
model for the controller-restart issue.

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

## Goals

- Bind an ordinary Serve launch to the exact existing API request before that
  request is eligible to execute.
- Adopt or inspect that request after controller restart instead of submitting
  another request for the same replica record and launch generation.
- Keep the ordinary API request claim and execution generation as the sole
  execution lease.
- Preserve current provider selection, credentials, request logs, cluster
  locking, launch retries, and cleanup behavior.

## Non-goals

- A new provider mutation API or outcome taxonomy.
- Direct construction of Kubernetes objects for Serve replicas.
- A cross-provider exact-locator or provider-absence protocol.
- Bypassing `sky.launch` or recursively invoking the public SDK from a new
  worker.
- Generalizing the solution to every cloud mutation or lifecycle domain.
- Replacing the current down or cleanup path.

## Request contract

The existing API request remains the execution record. If the bounded feature
is authorized, a new internal Serve launch endpoint or submission seam
performs the following sequence:

1. Accept one controller-generated stable submission UUID in the dedicated
   endpoint body; never derive binding identity from the fresh ID assigned to
   each HTTP attempt by middleware. Deterministically derive the association
   and exact API request IDs from that submission plus authenticated tenant
   scope.
2. Verify the exact built-in PostgreSQL request/queue backend, required API and
   Serve schema heads, the promoted per-service binding mode, and the complete
   API/executor/GC/controller participant capability and lease-drain barrier.
3. Construct the distinct ordinary-bound registered request with the normal
   request serializer. In one PostgreSQL transaction and connection, lock the
   lifecycle fence, service, replica, current association, request, then queue
   and retention pin; insert the association, complete correlated request,
   request-retention pin, and queue row, and set the replica association
   pointer.
4. Commit all four rows together. Queue visibility begins only at commit, after
   the binding is durable. There is no committed activation gap and no sweep
   creates a missing queue row.
5. On a lost acknowledgement, the same submission/request identity and exact
   canonical server-computed digest return the committed request ID in the
   endpoint response body; any mismatch fails closed.

An old API server does not understand this sequence and must not receive a
binding-enabled launch. Unknown request context is not a safe fallback because
an old `/launch` endpoint would execute it unbound. The controller uses the
legacy launch path until every recent API target, ordinary executor, and
possible service-controller owner advertises the capability and old leases
pass the quiescence window. Queue candidacy and locked claim require the
distinct bound handler to be locally supported, so a stale old executor leaves
the row queued. Per-service controller capability is persisted with the
fresh controller-incarnation UUID and monotonic service owner epoch; it is not
inferred from a supervisor lease. A new subprocess installs a new incarnation
and advances the epoch even when PID/IP are reused.

The exact built-in PostgreSQL request backend owns this cross-table
transaction, and both schema lineages must resolve through the same physical
database/connection. Serve never reserializes a private copy of the request or
coordinates two engines. SQLite, plugin backends, and schema mismatch fail
closed. An active correlated request without a queue row is invariant
corruption. With execution generation zero, no claim evidence, and effect phase
`NOT_STARTED`, startup cancels/quiesces generation zero and records
`PRE_EFFECT_TERMINAL`. Any nonzero generation, claim/lease evidence, or advanced
effect phase is `AMBIGUOUS`; startup terminalizes the request and requires the
exact owner acknowledgement or lease-expiry quiescence proof. It never
synthesizes execution.

The association is a separate central-PostgreSQL record, and the replica row
points to its current association. It does not reuse
`ReplicaInfo.launch_request_id`, whose current validation is specific to
system-OOM recovery, and it does not reinterpret nullable Serve033 action
columns. Services default to durable `legacy` mode; promotion to `bound` is an
explicit per-service transaction that requires no nonterminal legacy ordinary
request or PENDING/PROVISIONING replica. An explicit rollback demotion is
allowed only after every bound association is terminal, quiescent, copied,
projected and unpinned and no launch generation is active; it increments the
binding epoch before an incapable controller may own the service.

Immutable association identity includes the durable service workspace as well
as name/hash/version. The active-only generic request retention pin has a
request FK with `ON DELETE RESTRICT`/`NO ACTION`; both GC selection and final
delete require it to be absent. Projection explicitly deletes the pin and
records release time on the association rather than cascading it away.

## Executor fence

Before the existing executor invokes launch, a small internal check confirms:

- queue candidacy and the locked claim both support the distinct bound handler;
- the request claim/token/worker lease and execution generation are current;
- the association points to this exact request;
- the exact service and replica record still exist;
- the row still wants the associated ordinary-launch generation; and
- the normalized input digest is unchanged; and
- the association owner/revision matches the current durable
  service-controller owner epoch/revision for this service.

A failed check terminates before launch. Under the existing shared service
launch-authority guard, the association advances from `NOT_STARTED` to
`PROVIDER_IO` immediately before provider work. The service-job boundary
advances to `SERVICE_JOB_IO` before its effect and records
`SERVICE_JOB_RECORDED` plus the returned ID afterward; a crash between those
writes is durable may-have-submitted ambiguity. No new worker, provider client,
or renderer is introduced.

This fence records only the existing ordinary launch and service-job boundaries.
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
terminal predecessor whose same replica record still wants a retry. Unclear or
post-effect state is operator-visible ambiguity and blocks automatic
replacement.

`PRE_EFFECT_TERMINAL` is settled only when the same transaction copies exact
terminal/quiescence, projects the retryable failure, clears the replica
pointer, and deletes the active pin. The partial unique constraint treats
`BOUND`, `CANCEL_REQUESTED`, `RESULT_RECORDED`, and `AMBIGUOUS` as unsettled.
A successor requires `PRE_EFFECT_TERMINAL`, a clear pointer, no pin, and an
unchanged service binding epoch; terminal/quiescent evidence alone is
insufficient.

A later same-name replica has a different `replica_record_id` and cannot adopt,
cancel, project, or delete its predecessor's request.

Teardown and any other pointer/record/generation invalidation serialize with
the service launch-authority guard: commit fenced cancel intent, cancel the
exact request, prove its execution quiescence, then delete/replace the replica
and retain the association tombstone. A normal ORM row delete may not race the
pre-I/O or service-job fence.

## Compatibility and rollback

Rollout is capability-gated and mixed-version safe:

- new schema and readers ship before binding writes;
- every service initially persists `legacy` mode;
- old controllers continue the legacy path and cannot promote a service;
- new controllers bind only after all recent API, ordinary-executor, GC, and
  possible service-controller participants advertise the exact capability and
  incapable leases pass the quiescence window;
- no single logical launch is eligible through both paths; and
- binding enablement starts with one approved service after validation-only
  evidence.

Rollback first disables new promotion, then waits for every bound request to
become terminal, quiescent, copied, projected, and retention-unpinned, then
performs the fenced durable demotion. Only then may a controller/API/executor
image return to a version that lacks the binding capability. An incapable
controller can never own a service still in `bound` mode. Rollback never clears
or rewrites an association and never downgrades PostgreSQL schema.

The stacked R3 change merges only after every eligible existing non-pool
service is promoted and monitored. It makes new eligible services explicitly
start in `bound` mode and removes legacy submission/restart inference only for
rows whose durable service mode is `bound`; pools and excluded profiles retain
`legacy` and their existing contracts.

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
terminal/quiescence copying, request GC, teardown, and Serve projection. They
prove:

- atomic association/request/pin/queue commit and no valid active request
  without a queue row;
- startup marks a correlated missing-queue row ambiguous and never
  manufactures execution;
- exact retry returns the committed request and digest conflict inserts
  nothing;
- an old executor cannot claim a locally unsupported bound handler;
- compare-and-swap handoff and exact adoption after controller restart while
  queued, claimed, and inside the existing launch/provider call;
- at most one API request eligible per replica record and launch generation;
- at most one service-job submission for that logical launch;
- a crash after service-job I/O starts but before its ID is recorded remains
  ambiguous and never creates a successor;
- no cancellation on a valid ordinary owner handoff, and exact cancellation on
  supersession, teardown, or failed handoff;
- an active retention pin blocks both request selection and deletion, and exact
  projection releases it;
- terminal/quiescence publication racing fenced cancel/projection has no
  request-association lock inversion and preserves one exact outcome;
- teardown proves exact-request quiescence before deleting/replacing a replica;
- no predecessor result/cancel/delete applied to a successor record; and
- safe mixed old/new API and controller behavior.

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
of that evidence is reclassified as ordinary-launch request qualification.
