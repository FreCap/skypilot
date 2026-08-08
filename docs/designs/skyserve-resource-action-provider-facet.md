# SkyServe Ordinary-Launch Request Boundary

Last updated: 2026-08-08

Status: the dedicated SkyServe resource-action provider facet is retired before
activation. This companion defines only the existing API-request boundary that
a future evidence-gated ordinary-launch binding may reuse.

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

1. Verify that the receiving API instance advertises the exact ordinary-launch
   binding capability.
2. Reserve the API request ID without making it eligible to execute.
3. In one PostgreSQL transaction, insert both the complete API request row and
   an immutable association keyed by service and `replica_record_id`, with a
   neutral ordinary-launch generation, normalized input digest, current
   controller owner/revision, and request ID. Do not insert the queue row in
   this transaction.
4. Commit the request and association together before either can execute.
5. Idempotently activate only that request in the ordinary queue. A recovery
   sweep repeats activation for a committed nonterminal request that lacks a
   queue row.

An old API server does not understand this sequence and must not receive a
binding-enabled launch. Unknown request context is not a safe fallback because
an old `/launch` endpoint would execute it unbound. The controller uses the
legacy launch path until every eligible API target advertises the capability.

The association is a separate central-PostgreSQL record. It does not reuse
`ReplicaInfo.launch_request_id`, whose current validation is specific to
system-OOM recovery, and it does not reinterpret nullable Serve033 action
columns. The exact table and migration are specified in the feature PR only if
the telemetry/product gate authorizes implementation.

## Executor fence

Before the existing executor invokes launch, a small internal check confirms:

- the request claim and execution generation are current;
- the association points to this exact request;
- the exact service and replica record still exist;
- the row still wants the associated ordinary-launch generation; and
- the normalized input digest is unchanged; and
- the association owner/revision matches the current durable
  service-controller owner/revision for this service.

A failed check terminates before launch. No new worker, provider client, or
renderer is introduced.

This fence does not claim to identify whether an arbitrary provider call
crossed its mutation boundary. That broader instrumentation is absent today
and is not required to fix duplicate API request/service-job submission after
controller restart.

## Adoption, result, and cancellation

On restart, the controller loads the association by exact
`replica_record_id`. It compare-and-swaps the association owner only if service
version, replica record, launch generation, request ID, and input digest still
match. While the bound request is queued, running, succeeded but not yet
reduced, or not proven terminal, the new owner adopts/inspects it and must not
submit another request.

Current Serve launch payloads embed the old controller PID/IP and are rejected
after owner replacement. Bound launches instead carry the association ID; the
executor validates the current association owner. Owner replacement alone is
not cancellation. Cancellation targets the exact bound request only when the
desired launch is superseded, teardown is committed, or the handoff cannot be
validated. A controller that lost the association-owner revision cannot
publish, cancel, or project it.

The controller projects the existing request result using current Serve logic.
It may create a successor request only after the predecessor has exact
terminal/quiescence evidence and the same replica record still wants a retry.
Unclear request state is operator-visible and blocks automatic replacement;
this design does not invent provider-level absence from that ambiguity.

A later same-name replica has a different `replica_record_id` and cannot adopt,
cancel, project, or delete its predecessor's request.

## Compatibility and rollback

Rollout is capability-gated and mixed-version safe:

- new schema and readers ship before binding writes;
- old controllers continue the legacy path;
- new controllers bind only through API instances advertising the exact
  capability;
- no single logical launch is eligible through both paths; and
- binding enablement starts with one approved service after validation-only
  evidence.

Rollback first disables new bound admission, then waits for every bound request
to become terminal and projected. Only then may the controller/API image return
to a version that lacks the binding capability. Rollback never clears or
rewrites an association and never downgrades PostgreSQL schema.

## Verification

PostgreSQL and fault-injection tests crash before the atomic request/association
commit, after commit but before queue activation, during repeated activation,
at claim, immediately before launch, at request result, and at Serve
projection. They prove:

- atomic request-and-association commit before idempotent queue activation;
- recovery activates a committed nonterminal request with no queue row, while
  an absent or rolled-back request can never be activated;
- compare-and-swap handoff and exact adoption after controller restart while
  queued, claimed, and inside the existing launch/provider call;
- at most one API request eligible per replica record and launch generation;
- at most one service-job submission for that logical launch;
- no cancellation on a valid ordinary owner handoff, and exact cancellation on
  supersession, teardown, or failed handoff;
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
