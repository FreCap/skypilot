# Durable SkyServe Replica Actions

Last updated: 2026-08-11

Status: the dedicated resource-action authority proposal is retired before
activation. PRs #1112, #1239, #1240, #1336, #1338, and #1343 are closed. PR
#1335 merged, but its dark V2 preflight and qualification layer is retired by
this cleanup. PR #1342 also merged after the retirement review began; its dark,
uncalled renderer and representability evidence was removed atomically rather
than left import-broken by the retirement of its authority dependencies. PR
#1333's forward-only Serve038/039 migrations are retained inert while its
uncalled runtime state layer is removed. The unexercised V2 authority contracts
merged by PR #1332 and the disabled PR #1232 activation surface were also
removed. PR #1340 merged the compatibility cleanup. PR #1346 merged as
`0b77ca77ae8b099c2de07566670743651744bbe2` and deletes the temporary disabled
Helm value, private-handler quarantine, private result codecs, and authority
routing. Its `boltz-test` deployment gates passed on 2026-08-08: the exact
compatibility artifact passed readiness, +10, and +30, and the sole `skypilot`
chart release in that cluster had its stored value scrubbed. The exact
final-removal artifact then passed `boltz-test` readiness, +10, and +30 at
04:04:08, 04:14:25, and 04:35:25 UTC, with every private-handler request absent
across all statuses.
Production reached compatibility-artifact readiness at 03:38:22 UTC and passed
its +10-minute gate at 03:49:03 UTC and +30-minute gate at 04:08:49 UTC. The
released plugin `claim_scope` API remains as an inert `GENERAL`-only
compatibility shim; its retired authority value is rejected and does not affect
queue selection.
No service was promoted through the proposed authority path, no authority
worker claimed a request, and no provider effect ran through that path. Source
cleanup is merged and deployed, but operational closeout remains open: the
first final-removal artifact preserved every safety and retired-state invariant
while failing its exact +60 heartbeat-latency comparator. PRs #1355, #1360,
and #1362 merged bounded latency corrections; production revisions 370, 371,
and 372 each preserved safety but failed the stricter zero-new-timeout
qualification. PRs #1367, #1368, #1369, and #1370 complete the coalesced
snapshot, read-collapse, and full-owner-attestation fix-forward. Their combined
exact artifact passed the fresh `boltz-test` readiness/+10/+30 gate, but
production revision 374 failed +30 when an unbounded shared Kubernetes owner
read amplified one stall across both slots. The bounded task/transport deadline
below is now stacked on PR #1373's merged dedicated-executor isolation. The
combined exact artifact is the implementation gate. This canonical follow-up
remains the sole R0 closeout gate after that fix passes production
qualification.

The combined HA latency fix through PRs #1367, #1369, and #1370 shipped as
release `1.1.1176` and passed the exact `boltz-test` readiness/+10/+30 window.
One later-clock production +10 sample failed with one client timeout on each
`boltz-l4-fleet` slot while the service controller was under provisioning
pressure, and the binding +30 trace identified an unbounded provider read.
Safety held, but completion is now blocked on the combined executor-isolation
and snapshot-deadline artifact plus a fresh production
readiness/+10/+30/+60 window defined below.

The review found one smaller, independent correctness gap in the existing
Serve controller: an ordinary replica launch records its exact API request ID
only in process memory. A controller restart can therefore lose the association
and submit another launch request. The system-OOM recovery path already has a
bounded durable binding and adoption mechanism, but ordinary launches do not.
That gap does not justify a dedicated authority deployment, a native provider
renderer, a second execution topology, or a universal physical-capacity
kernel. A bounded fix may proceed only under the contract and evidence gates
in this document. Issue #1352 owns that telemetry-first follow-up; it is not an
R0 authority-retirement blocker.

## Decision record

The original 2026-07-30 request was an evaluation of whether a unified
physical-capacity convergence kernel had enough long-term payoff to justify
its migration risk. The evaluation concluded that the payoff was conditional
and required 30--60 days of production evidence across at least two domains.
The subsequent disabled deployment found no Serve services or replicas, no
independent provider-call audit source, empty capacity tables, and zero
projector database connections. The large payoff therefore remained a
hypothesis.

Subsequent user directions explicitly authorized implementation, testing,
deployment, removal of the old path, and later phases. The defect was not a
lack of authorization: implementation outran the original 30--60-day,
two-domain evidence gate and expanded into a dedicated authority stack before
it proved a complete admission-to-effect path. The stack had already reached
roughly 37,000 changed lines across 113 files at the original review, and later
dark merges expanded it further. It introduced separate authority workers,
cohort and lease protocols, private transport, native rendering, V2
representability inventories, and Serve038/039 state while the named legacy
mutation owners remained authoritative. The deployment evidence does not
justify that architecture, so it is rejected.

The accepted decision is:

1. retain the already shared and independently useful API request/action
   substrate and forward-only additive schemas;
2. remove uncalled V2 authority contracts and the authority-worker deployment,
   preflight, transport, packaging, and claim paths;
3. retain Serve038/039 as forward-only empty schema, but never activate or
   write its authority state and do not add another claimant/executor topology;
4. treat exact ordinary-launch request binding as a localized Serve recovery
   issue; and
5. require measured demand or an explicit correctness decision before that
   localized issue becomes an implementation project.

### 2026-08-11 localized correctness mandate

The operator's current request to identify and complete the remaining accepted
physical-layer work is the explicit correctness decision required by item 5.
It authorizes the localized ordinary-launch request-binding stack regardless of
eligible production volume. It does not reverse the capacity-scanner no-go,
revive the universal kernel or retired authority topology, waive the R1-first
sequence, or authorize a capacity-creating canary.

The stack therefore starts with the diagnostic-only R1 change below. R2 may be
implemented in the next stacked change, but binding activation and legacy-path
removal remain subject to their compatibility, crash-matrix, and rollout gates.

## Goals

- Preserve the exact request identity for an ordinary Serve launch across a
  controller restart.
- Adopt the same request after restart rather than submit an untracked second
  request.
- Keep launch intent, request binding, service identity, replica incarnation,
  and terminal projection mutually consistent.
- Preserve current launch/down ordering, provider-work limits, retry behavior,
  and public Serve semantics.
- Reuse the ordinary API request executor and existing internal launch path.
- Make ambiguity explicit and operator-visible instead of guessing success or
  absence.

## Non-goals

- A universal resource or physical-capacity state machine.
- A dedicated resource-action authority worker, private HTTPS control plane,
  cohort, policy rotation, or special execution lease.
- Reimplementing SkyPilot launch/down through a native Kubernetes renderer.
- Moving provider credentials or provider clients into a new component.
- Replacing the existing API request queue or ordinary executor.
- Changing pools, managed jobs, paid capacity, reserved fill, spot fallback,
  cost rebalance, placement failover, or logical replica accounting.
- Removing process-local fields that remain useful as caches before their
  durable replacements are proven in production.

## Current behavior and bounded gap

The current controller already persists replica-record identity and cleanup
intent. `replica_record_id` fences the ordinary replica row and prevents a stale
controller from deleting or routing a successor record. The Serve033
action-owned incarnation and generation columns remain null on the ordinary
legacy write path; they are not a current recovery fence and this design does
not pretend otherwise. Failed cleanup remains durable and is redriven after
restart; the process-local cleanup retry maps only preserve exponential-backoff
timing, so a restart may retry sooner but does not forget the cleanup.

Launch behavior is asymmetric:

- system-OOM recovery persists `launch_request_id` and `service_job_id`, then
  adopts that exact request after restart;
- ordinary launches publish the request ID only in
  `_LegacyReplicaMutationRuntime.replica_to_request_id`; and
- restart reconstruction of an ordinary `PENDING` or `PROVISIONING` replica
  can call `_launch_replica()` again without proving what happened to the old
  request.

Cluster-name idempotency may prevent a second provider resource in common
cases, but it is not a durable proof that the old request was adopted, canceled,
or terminal. The controller must not rely on that incidental behavior as its
recovery contract.

## Public contract

There is no new CLI, SDK, configuration, or provider interface. Existing
Serve behavior remains backward compatible.

If the bounded fix is authorized, an ordinary non-pool replica launch will have
the following internal contract:

1. A replica row has a stable record identity. The bounded implementation adds
   a neutral ordinary-launch generation or association identity; it does not
   silently reinterpret nullable Serve033 action columns.
2. Before execution can escape the ordinary API request boundary, the row is
   durably bound to one exact API request ID for that generation.
3. A restarted controller with the same row identity and generation adopts
   that request ID.
4. A controller may create a successor only after the predecessor is exact
   terminal/quiescent and its durable effect phase proves neither provider nor
   service-job I/O began. Terminal/quiescent post-effect ambiguity blocks.
5. Ordinary controller replacement transfers the association to the new
   controller by owner-epoch compare-and-swap and adopts the exact request.
   Cancellation targets that request only for committed supersession/teardown;
   losing an in-memory cache is never permission to cancel or replace it.
6. A same-name replica created later has a different record identity and cannot
   inherit the predecessor's request, result, absence proof, or cancellation.
7. Unclear request state becomes a durable operator condition and blocks
   another launch request until reconciled.
8. The controller supplies one stable submission key for an admission attempt.
   A lost HTTP acknowledgement followed by an identical retry returns the
   already-bound request ID; reuse of that key with a different canonical
   launch digest fails closed.

The API request remains the execution record. A second generic action DAG is
not introduced merely to wrap it.

## Architecture and invariants

### Ownership

- `ReplicaManager` owns the desired replica transition and stable replica
  record identity.
- A separate central PostgreSQL association record owns the neutral ordinary-
  launch identity and exact request binding.
- The existing API request queue and ordinary executor own request claim,
  execution generation, cancellation, and terminal result.
- Existing SkyPilot launch/down internals own provider selection and effects.
- The Serve reducer validates the row/request association before projecting a
  result.

The exact built-in PostgreSQL request backend owns the admission transaction.
It reaches the Serve association and replica tables through the same physical
database and the same SQLAlchemy connection; Serve does not duplicate request
serialization or attempt a transaction across two engines. Admission fails
closed on SQLite, plugin request or queue backends, or either schema lineage
being behind its required head. Every effect-authorizing cross-table path takes
the service launch-authority guard, then locks lifecycle, service, replica,
association, request, queue, and retention-pin rows in that order, omitting only
unused suffixes and never inverting it. Generic API terminal/quiescence writes
remain request-only. Queue claim locks only request/queue rows and performs a
non-locking association validity read; the authoritative association lock and
revalidation is the later pre-I/O fence, so claim cannot create a lock cycle or
grant effect authority by itself. No component may own both an unfenced stale
replica snapshot and permission to start provider I/O.

### Commit-before-effect

The bounded implementation adds one internal atomic bind-and-enqueue seam. A
dedicated `/internal/serve/ordinary-launch` endpoint accepts one controller-
generated stable submission UUID. It does not fall back to `/launch`, whose old
implementation would ignore unknown binding context and execute unbound. The
controller reuses that UUID for every transport retry. The server
deterministically derives the association and exact API request IDs from the
submission UUID plus authenticated tenant scope, independent of the fresh ID
assigned to each HTTP attempt by `RequestIDMiddleware`, and returns the exact
bound request ID in the response body.

In one transaction the server locks the lifecycle fence, service row, exact
replica row, and current association, constructs the complete ordinary-bound
request with a distinct registered handler on the normal executor topology,
and inserts the association, `api_requests` row, generic request-retention pin,
and `api_request_queue` row. It also sets the replica row's exact association
pointer. Queue visibility occurs only at transaction commit,
after every fence and binding is durable. There is no committed
PENDING-without-queue activation state and no second worker or recovery sweep.
Timeout before commit leaves none of those rows. A lost response after commit
is resolved by retrying the same submission UUID: exact identity and digest
return the existing request; any mismatch fails closed.

The canonical binding digest is computed server-side from the exact prepared
`LaunchBody` bytes after removing binding-only and mutable owner fields. The
association records that digest, but does not store another copy of the task or
provider payload. The seam does not call the public SDK recursively, create a
new execution topology, or render provider-native objects.

Central API revision 009 adds an `ordinary_launch_binding_capable` instance
advertisement, request-to-association correlation, and generic retention-pin
table. Serve revision 042 adds the neutral association table, replica pointer,
monotonic service-controller owner epoch, per-service controller capability,
and durable binding mode/epoch. Bound admission requires every recent API
acceptor, ordinary executor, GC participant, and possible service-controller
owner to advertise the exact built-in PostgreSQL protocol; old ready and non-
ready-but-recent leases must pass the documented quiescence window. The
dedicated endpoint makes an old API target fail with no effect. Queue candidate
selection and the locked claim require the distinct handler to be in the local
supported-handler set, so an old executor leaves the row queued instead of
claiming it. The service owner CAS persists the subprocess capability beside
its owner epoch rather than inferring it from an API supervisor lease.

An active correlated bound request without its queue row is invariant
corruption, not an activation state. Startup locks the correlated evidence. If
execution generation is zero, no claim/lease exists, and effect phase is
`NOT_STARTED`, it cancels/quiesces generation zero and records
`PRE_EFFECT_TERMINAL`. Any nonzero generation, claim evidence, or advanced
effect phase becomes `AMBIGUOUS`; startup terminalizes the request and requires
the exact owner acknowledgement or lease-expiry quiescence proof. It never
synthesizes execution or infers a successor.

The transaction compares at least:

- service name and service version;
- replica ID and immutable replica record ID;
- the new ordinary-launch association identity and server-selected generation;
- a server-recomputed digest of the canonical prepared `LaunchBody`, excluding
  binding-only and mutable owner fields and never using a diagnostic raw-YAML
  or `repr` fallback;
- initial controller owner and association-owner revision; and
- expected absence of a conflicting nonterminal binding.

### Durable association and per-service cutover

The association contains immutable association/submission UUIDs, service
name/hash/workspace, lifecycle and binding epochs, service version, replica ID and
`replica_record_id`, server-selected launch generation, cluster name, exact API
request ID, and canonical digest/version. Uniqueness covers submission UUID,
association UUID, request ID, and
`(service_name, replica_record_id, launch_generation)`, with at most one
unsettled association for a replica record. The replica row has a nullable
`ordinary_launch_association_id`; generation allocation and pointer update occur
under its lock. No existing system-recovery, Serve033 action, or `ReplicaInfo`
request/job field is reinterpreted.

Mutable fields are current controller-owner incarnation/epoch,
association-owner revision,
effect phase, request terminal status/cause and quiesced generation, optional
exact service-job ID, ambiguity code, projection state, and database-clock
timestamps. Effect phases are `NOT_STARTED`, `PROVIDER_IO`, `SERVICE_JOB_IO`,
and `SERVICE_JOB_RECORDED`; resolution states are `BOUND`,
`CANCEL_REQUESTED`, `RESULT_RECORDED`, `PROJECTED`,
`PRE_EFFECT_TERMINAL`, and `AMBIGUOUS`.
Identity/digest fields never change, and unresolved history cannot be deleted.

| Resolution state | Required evidence | Unsettled / pinned | Exit |
|---|---|---|---|
| `BOUND` | Request active or terminal evidence not yet reduced; effect phase is authoritative | yes | result reduction, fenced cancel, or ambiguity |
| `CANCEL_REQUESTED` | Current owner committed exact supersede/teardown intent | yes | exact terminal + quiescence reduction |
| `RESULT_RECORDED` | Exact terminal/quiescence and service-job ID copied after `SERVICE_JOB_RECORDED` | yes | atomic replica projection |
| `PRE_EFFECT_TERMINAL` | Exact terminal/quiescence copied while effect remained `NOT_STARTED`; failure projected, pointer cleared, pin deleted | no | successor generation may be admitted |
| `PROJECTED` | Exact result/tombstone projected, pointer cleared, pin deleted | no | 60-day tombstone retention |
| `AMBIGUOUS` | Effect/claim/result cannot prove a safe terminal disposition | yes | explicit operator reconciliation only |

The partial unique constraint treats `BOUND`, `CANCEL_REQUESTED`,
`RESULT_RECORDED`, and `AMBIGUOUS` as unsettled. A successor transaction
requires the predecessor to be `PRE_EFFECT_TERMINAL`, the replica pointer to be
clear, the retention pin absent, and the service binding epoch unchanged.

The service row has a non-null controller-incarnation UUID, monotonic
`controller_owner_epoch`, capability bound to that exact incarnation,
`ordinary_launch_binding_mode` (`legacy` or `bound`), and monotonic binding
epoch. Every controller subprocess startup supplies a fresh incarnation UUID;
the owner CAS changes it and increments the epoch even when PID/IP are reused.
Existing services default to `legacy`.
Promotion to `bound` is an explicit transaction requiring a non-pool service,
the full participant/quiescence barrier, and zero legacy nonterminal ordinary
requests or PENDING/PROVISIONING replica rows. A fenced rollback demotion to
`legacy` is permitted only after every bound association is terminal,
quiescent, copied, projected and unpinned and no launch generation is active;
it increments the binding epoch. An incapable controller can never claim a
service while its mode is `bound`.

Atomic admission inserts a generic active-only retention pin separate from the
request correlation. Its request FK uses `ON DELETE RESTRICT`/`NO ACTION`, not
cascade. Both GC candidate selection and final deletion require `NOT EXISTS`
for that pin. The exact projection transaction deletes the pin only after
copied evidence, replica result, and association projection are durable; the
association records the release timestamp for audit. This preserves evidence across a
controller outage longer than the default 24-hour retention.

Projected association tombstones remain for at least 60 days by the database
clock. Bounded GC deletes one only after exact quiescence, projection, pin
release, and proof that no replica pointer or retained request references it.
Unresolved or ambiguous associations have no age-based deletion.

Every successful service-owner CAS installs a fresh controller-incarnation UUID,
increments `controller_owner_epoch`, and transfers all unresolved associations
to that incarnation/epoch in the same transaction under the exclusive service
launch-authority guard; publishing a port for the already-current incarnation
does not advance it. PID/IP remain routing metadata and cannot act as an ABA-
safe authority token. The executor resolves immutable association identity,
including workspace, and validates current owner incarnation/epoch/revision. Publish,
cancel, supersede, and teardown are server-side transactions whose predicates
include owner epoch/revision and exact request/record identity; a controller-
side read followed by generic cancel is forbidden. Normal replacement detaches
the old waiter and adopts the association without legacy owner-loss
cancellation. Cancellation is reserved for a committed supersession/teardown
intent owned by the current epoch/revision.

### Pre-I/O fence

Immediately before provider I/O, the ordinary executor or internal handler
must revalidate:

- its live request claim and execution generation;
- the locked claim still resolves the distinct locally supported bound handler;
- the service and exact replica row still exist;
- the replica pointer and association still name this record, generation, and
  request ID;
- the submitted input digest matches the durable binding; and
- the association owner/revision matches the current durable service-controller
  owner/revision.

A failed check terminates without provider I/O. Under the existing shared
service launch-authority guard, the backend repeats validation and atomically
advances `NOT_STARTED` to `PROVIDER_IO` immediately before provider work. The
service-job boundary revalidates the same tuple, advances to `SERVICE_JOB_IO`
before its call, then records `SERVICE_JOB_RECORDED` plus the exact returned job
ID. A crash in that interval is conservative may-have-submitted ambiguity. A
lost claim never becomes permission for another effect. Controller takeover
takes the exclusive guard, waits for opaque provider work already in progress,
and adopts the same request; it never replays that call.

### Result and retry

Success is projected only from the exact request result associated with the
same row and generation. A restarted controller loads the association through
the replica pointer after atomic owner-epoch handoff and adopts the exact
request result. The process-local request map is only a cache.
Generic request terminal and quiescence transactions update request state only;
the retention pin prevents collection. Using canonical service/replica/
association lock order, the Serve completion reducer reads immutable request
terminal status/cause, result/service-job ID, execution generation, and exact
quiescence without locking request after association. It copies that evidence,
updates replica status, marks the association `PROJECTED`, and releases the pin
in one transaction on both ordinary and paid-capacity completion paths.

Failure and retry policy use the database clock. Terminal plus quiescent does
not prove effect absence. A successor generation is allowed only from
`PRE_EFFECT_TERMINAL` with effect phase `NOT_STARTED`, proving neither provider
nor service-job I/O began. Any terminal result after `PROVIDER_IO` without an
exact projectable service-job outcome, any `SERVICE_JOB_IO` crash, unclear
request/result, fence rejection, or cancellation race is `AMBIGUOUS` and blocks
automatic resubmission. This bounded design does not claim cross-provider
effect absence.

### Cleanup

The existing durable cleanup intent remains authoritative. This project does
not need a second cleanup action graph. Teardown or supersession finds the
association by exact record identity and serializes with the service launch-
authority guard. It first commits an owner-epoch/revision-fenced cancel intent,
cancels only that request, and proves execution quiescence before deleting or
replacing the replica row and projecting a tombstone. Ordinary
`remove_replica(s)` cannot race a bound pre-I/O check with a direct ORM delete.
Association history is retained so a same-number successor record cannot
inherit, cancel, or project predecessor work.

Any later change to persist retry deadlines must preserve current immediate
restart redrive and be independently justified by observed retry storms or
provider throttling.

## Implementation phases

### R0: retire the unproven authority stack

- Revert PR #1332's V2 authority-only code and generated artifacts.
- Retain PR #1333's forward-only Serve038/039 migrations and schema catalog,
  but remove its uncalled authority/identity runtime state modules and tests.
- Remove design and removal-ledger claims for nonexistent transitional router
  symbols.
- Keep API005--008, global-user-state 028, Serve033, and other generic or
  forward-only foundations that have independent consumers.
- Remove PR #1232's dormant authority Deployment, bootstrap, claimant,
  preflight, network, native-renderer, provider-artifact, packaging, and Helm
  activation surfaces. Remove PR #1335's V2 preflight and qualification-policy
  additions and PR #1342's dark V2 renderer/representability island atomically
  with them. Do not reverse forward-only migrations.
- In the compatibility artifact, retain the four private handler names as a
  fail-closed quarantine. Ordinary executor and compatibility `all` queues must
  neither advertise nor claim them. This stacked final-removal change deletes
  the handlers, authority routing, queue exclusion, and codecs only after the
  all-status zero-request gate below is recorded. Retain the released plugin
  `claim_scope` parameter and enum as a `GENERAL`-only inert shim; explicitly
  reject the retired authority scope.
- In the compatibility artifact, retain the legacy
  `resourceActions.authorityWorker` Helm value shape so `--reuse-values`
  upgrades with `enabled: false` remain valid and reject `enabled: true` with a
  clear retired-feature error. Before this stacked change may merge, scrub the
  stored value from every release using the exact compatibility chart and
  image. The final chart omits the key from defaults and schema and keeps a
  narrow tombstone that rejects any stored `resourceActions` value instead of
  silently ignoring it.
- Deploy the exact merged cleanup artifact with existing Helm values and verify
  that no authority workload or provider effect is introduced. Preserve the HA
  rollout strategy; temporary CPU surge capacity follows the explicit capacity
  approval gate below.
- Keep this already-authored stacked final-removal PR in draft and blocked
  until stored Helm values are scrubbed and the compatibility artifact records
  zero matching private requests across all statuses at readiness, +10 minutes,
  and +30 minutes. Then merge and deploy the final artifact and repeat the same
  checkpoints before closing R0.

### R1: evidence gate

First ship a telemetry-only PR. It adds an append-only central-PostgreSQL
`serve_ordinary_launch_handoff_events` table with at least 60 days of retention;
the existing API requests are normally collected after 24 hours and cannot
support this gate alone. Each event uses the database clock and records a
closed event kind, service/version, `replica_record_id`, controller route epoch,
ordinary request ID when known, service job ID when known, and a redacted input
digest. It stores no provider payload or credential.

The closed event kinds cover request publication, controller-start observation
of a nonterminal replica, restart redrive, owner-loss cancellation request, API
terminal result, Serve result projection, service-job observation, and cleanup
retry after a route-epoch change. Queries report:

- total eligible ordinary launches and controller restarts during nonterminal
  launch windows;
- replica records associated with more than one ordinary request ID before
  terminal projection;
- restart redrives whose predecessor status is unknown because no terminal
  observation was retained, or whose predecessor was observed terminal but
  remained unreduced;
- duplicate service-job submissions for one replica record;
- distinct owner-loss cancellation requests, explicitly not terminal
  cancellation proof; and
- cleanup retries whose process-local backoff reset after controller restart.

Observe those queries for 30--60 days of eligible production traffic, or record
an explicit product correctness decision that the restart gap must close
regardless of volume. The telemetry writer is diagnostic only: it cannot delay,
cancel, authorize, retry, or project a launch.

The R1 implementation advances only the central PostgreSQL Serve schema to
revision 041. Its closed event writer uses a bounded process-local queue and a
daemon writer so launch callers never wait for telemetry. For ordinary
controller launches, versioned diagnostic identity travels inside the existing
launch context; the API process publishes `REQUEST_PUBLISHED` only after
request scheduling returns, so a lost HTTP acknowledgement does not hide the
accepted request merely because `sdk.launch()` never returned to the
controller. Publication requires all five durable service-owner fence fields,
cross-checks the nested diagnostic service/version against that outer fence,
and queues only the closed fence. Before inserting the event, the writer
performs a fresh PostgreSQL authorization read; invalid, stale, or unavailable
provenance drops only the evidence. This API-side publication remains
asynchronous and fail-open.

Event timestamps use the database clock, payloads and credentials are never
stored, and updates and truncation are rejected. One PostgreSQL-backed
distributed singleton owned by the central server's controller/all background
runtime runs a five-minute retention cadence. It deletes at most 1,000 rows per
pass, and only rows strictly older than 60 days; event insertion never performs
retention work and additional controller processes do not prune independently.

The summary query labels all event evidence as a lower bound, counts controller
starts as distinct service/route-epoch pairs rather than replica rows, reports
redrives with no observed predecessor publication, and uses an explicit
predecessor-status-unknown bucket when the one-shot terminal observation
retained no terminal evidence. Absence of that evidence is never labeled
active.
It also includes explicitly process-local queue depths, queue drops, writer
failures, backend-unavailable events, provenance rejections/check failures,
retention-prune failures, and terminal-lookup failures since module import. A
low-cardinality multiprocess Prometheus counter exports enqueue, persist, drop,
unavailable, provenance, lookup, and prune outcomes across scraped fleet
processes. These surfaces prevent a lossy diagnostic process from presenting
unexplained zeros as fleet-wide completeness; they do not make asynchronous
telemetry an authority or a complete audit log. Initial instrumentation covers
ordinary request publication, controller-start observation and restart
redrive, owner-loss cancellation request, observed API terminal result with a
closed `SUCCEEDED`/`FAILED`/`CANCELLED` status, service-job observation, and
Serve result projection. The cancellation-request event records local intent
once per ordinary request ID; it never claims that the target became terminal,
and the summary deduplicates by replica-record/request identity. A
system-recovery candidate's bound recovery request is excluded, while any
later retry is instrumented once durable demotion makes it ordinary. Terminal
lookup uses a fixed two-worker daemon pool and one no-retry HTTP attempt with a
five-second connect/read timeout. Thus one unexpectedly hung lookup cannot
starve the queue, while production lookups are bounded and
missing/nonterminal/inexact results remain unclassified. If redacted digest
serialization, fallback `repr()`, or UTF-8 encoding fails, the complete
telemetry envelope for that launch attempt is omitted; both initial launch and
restart redrive continue through the unchanged canonical path. The closed
cleanup-retry kind is retained for the point where a route-epoch change can be
proved rather than inferred.

If there are no eligible launches and no correctness mandate, stop. The design
remains a documented limitation and no runtime is added.

### R2: bounded binding and adoption

If R1 authorizes work:

- generalize the proven request-binding/adoption seam from system-OOM recovery
  to ordinary launches;
- add one neutral central-PostgreSQL association table keyed by service,
  `replica_record_id`, and ordinary-launch generation; do not reuse the system-
  recovery `launch_request_id` or action-only Serve033 columns;
- use one stable controller submission UUID at the dedicated endpoint,
  deterministically derive association/request IDs server-side, allocate
  generation under row locks, and return the exact request ID in the response
  body on first admission and lost-ACK retry;
- atomically insert association, request correlation, retention pin, queue row,
  and replica pointer through the exact built-in PostgreSQL request backend;
- add participant capability, distinct-handler claim filtering, and durable
  service binding mode/epoch; keep legacy admission until API, executor, GC,
  and service-controller capability/quiescence gates pass;
- replace immutable PID/IP validation only for bound launches with the
  association/current-owner-epoch fence and an atomic service-plus-association
  owner handoff;
- add claim, pre-provider-I/O, and service-job-I/O association fences;
- pin request retention until terminal evidence is copied and projection
  releases it atomically;
- persist explicit ambiguity instead of resubmitting;
- retain the in-memory request map only as an optimization; and
- add crash tests at intent commit, request binding, claim, pre-I/O, result,
  and projection boundaries.

System-OOM recovery, pools, reserved-fill launches, and other special launch
profiles remain on their existing contracts and may not enter this ordinary
association path.

The crash matrix includes timeout before transaction commit, response loss
after atomic commit, identical and conflicting submission-key retries, old-
handler claim, claim loss, controller handoff inside the provider guard,
service-job-I/O crash, terminal result before projection, startup corruption,
teardown/delete races, and request GC while pinned. Every case proves no orphan
active request, no valid active request without its queue row, at most one
request/queue entry and service-job submission, and no successor after
ambiguous state.

This phase must be one focused feature PR. If it temporarily preserves an old
fallback, the removal PR is created at the same time as a blocked stacked PR.

### R3: rollout and removal

Deploy dark/read-only validation first, then one eligible non-pool service.
Remove the old resubmission inference only after the exact merged artifact has
completed the monitoring gate. The already-authored removal change makes the
bound endpoint mandatory for eligible ordinary launches, removes the branch in
`_recover_legacy_replica_operations()` that resubmits without first resolving
an exact association, removes the capability-controlled unbound submission
fallback, and deletes transition-only compatibility probes. It retains the
process map and legacy recovery for pools, system-OOM recovery, reserved-fill,
and other excluded profiles; global deletion of those contracts is outside
this design.

## Deployment and rollback

R0 is a code cleanup over retained additive forward-only schemas. Before the
compatibility upgrade, prove no request in any status uses any of the four
private handlers
`serve_shadow_candidate_launch`, `serve_shadow_candidate_down`,
`serve_resource_action_launch`, or `serve_resource_action_down`. Prove all
generic action/attempt, shadow, cohort/reference/coverage, release, and
authority-history relations are empty. The six Serve038 relations
`serve_resource_action_authority_policy_epochs`,
`serve_resource_action_worker_registration_leases`,
`serve_resource_action_worker_registration_handoffs`,
`serve_resource_action_worker_registration_cold_recoveries`,
`serve_resource_action_crash_canary_runs`, and
`serve_resource_action_attempt_exhaustions` must be empty. The nine Serve039
relations `serve_resource_action_execution_authority_lineage`,
`serve_resource_action_attempt_terminal_authority`,
`serve_resource_action_shadow_request_terminal_history`,
`serve_resource_action_shadow_admission_fallback_history`,
`serve_resource_action_shadow_admission_fallback_progress_log`,
`serve_resource_action_shadow_settlement_history`,
`serve_resource_action_shadow_execution_history`,
`serve_resource_action_worker_process_supersessions`, and
`serve_resource_action_api_instance_gc_cursors` must also be empty.

All nullable Serve038 candidate/identity columns on services, version specs,
replicas, worker cohorts, and worker-cohort references must be null. The shadow
coverage table must itself be empty because its Serve038 candidate columns are
non-nullable. Run the same assertions after rollout. This stacked cleanup must
remain draft if any private-handler row exists in any status; deleting its
decoder or queue quarantine is forbidden if any assertion fails.

Deploy the exact merged image as one compatible Helm rollout. The `boltz-test`
release explicitly pins `apiService.image`, `controllerService.image`, and
`executorService.image`, so its `helm upgrade --reuse-values` must set all three
to the same immutable digest; updating only the API value would leave mixed old
controller/executor images. Production stores only `apiService.image`; its
controller and executor image values are null and inherit the API value. Its
ordinary upgrades must use `--reuse-values`, override only the API image, and
require the exact digest in all three client-rendered chart positions. A stored
`resourceActions.authorityWorker.enabled: false` value remains schema-valid and
renders no authority resources. A stored `enabled: true` value fails the
upgrade with `resourceActions.authorityWorker.enabled=true is no longer
supported; the dedicated resource-action authority worker has been retired`.
Verify no authority Deployment, Service, ServiceAccount, ConfigMap, or Pod
exists. Rollback is an application-image rollback of all three roles only. Do
not downgrade PostgreSQL migrations.

After the compatibility artifact passes readiness, +10-minute, and +30-minute
checks, scrub the retired value from every Helm release before merging this
stacked cleanup. Export each release's complete user values as JSON, remove
`.resourceActions.authorityWorker`, remove `.resourceActions` too when it is
then empty, and compare complete client-side renders of the original and
sanitized values. Upgrade the same compatibility chart and image with
`--reset-values` and the complete sanitized values file. Preserve each
release's existing image-inheritance topology, set its explicit image value or
values to the immutable digest, and require that digest in all three rendered
positions. Do not use server-side Helm dry-run, combine `--reset-values` with
`--reuse-values`, use a null override, or use `--atomic`; the migrations remain
forward-only.
Verify `helm get values` contains no retired key.

Only then may this final-removal change merge. PR #1346 instead merged through a
concurrent workspace action at 03:49:52 UTC, after production +10 but before
production +30 and before the two adversarial-review design corrections were
committed. This is a process-contract departure, not evidence that the gate was
waived. No final-removal artifact had been deployed at merge time. A second
concurrent action later deployed it to `boltz-test` before the canonical
corrections landed, as recorded below; at that point, production promotion still
required the corrected design and completed test monitoring.

PR #1350 merged at 04:35:28 UTC, but its exact-capacity-approval gate remained
open and it did not yet contain the zero-capacity exception or the reproducible
60-minute comparison below. A third concurrent action started production
revision 369 at 04:36:18 UTC. The zero-capacity record was authored only at
04:38:37 UTC in an unmerged local follow-up; later commit and rebase timestamps
are not rollout gates. This is a third process-contract departure: the later
empirical zero-capacity proof and monitoring contract do not retroactively
satisfy design-first ordering. Kubernetes had already accepted the rollout
when detected, so no competing rollback or replay was issued; every
post-deployment gate remains binding.

The final chart has no
`resourceActions.authorityWorker` schema or enabled-value guard, and its
request registry intentionally has no private handler, authority claim routing,
codec, or ordinary-queue exclusion. A narrow value tombstone rejects an
unscrubbed release, and the released plugin API retains only an inert
`GENERAL` claim scope. Deploy its exact immutable image to all three roles,
run the retained migrations forward, and repeat readiness, +10-minute, and
+30-minute checks. Production additionally requires the issue-#1349-aware
+60-minute comparison below. A retired-state write, schema/head mismatch,
split-brain, unintended failover, health loss, restart, or source-attributed
regression stops the rollout and restores production revision 368's exact
`1.1.1159` compatibility chart
`sha256:07ed313fc8f7e80ea1aaa82f0a2eb0163b0cc9827e46ed0e5c72cb7d4048d6c6`
and image
`sha256:d4237ec47a2e74d58b93a312157b58cf9066ec134bcce262681ac356087dd4b5`.
Rollback changes application images/chart only and never downgrades the
database. Exceeding only a pre-existing #1349 comparison limit holds R0 open
for attribution; it does not trigger an automatic rollback to an artifact that
already exhibited that signal. An unexplained warning or error also holds R0
open, and requires rollback if investigation connects it to the new artifact.

R2 ships with every service in durable `legacy` mode, so schema and capability
writes are dark. Promotion changes one approved non-pool service to `bound`
only after its controller capability, the full participant barrier, and the
legacy-drain transaction pass. An incapable controller cannot own that service.
Rollback disables further promotion, keeps capable binaries serving existing
bound rows, and waits for every request to become terminal, quiescent, copied,
projected, and unpinned. The fenced demotion transaction then proves no active
generation, sets the service back to `legacy`, and increments its binding epoch
before any incapable image may own it. A rollback must not clear associations
or tombstones, release pins early, change replica record IDs, or race a
predecessor with a successor. R3 makes `bound` the creation/steady-state mode
for eligible non-pool services after all such services are promoted; excluded
profiles retain explicit `legacy` mode.

No canary that creates provider capacity is authorized by this design alone.
Before such a canary, record the logical GPU slots, physical instance shape and
count, region, duration, market/reservation class, and incremental cost, and
obtain explicit management approval.

## Verification and monitoring

R0 completion requires both the compatibility and final-removal deployments:

- focused unit tests for the retained generic action substrate and removal
  checker;
- no registered private handler, private return codec, authority claim routing,
  or queue exclusion in the final artifact; the released `GENERAL`-only plugin
  shim remains inert;
- exact merged SHA and immutable image digest for each rollout;
- staged `boltz-test` rollout with preserved Helm values;
- production promotion only after the corresponding `boltz-test` evidence
  closes;
- all ordinary control-plane Pods ready with zero new crash loops;
- authority worker disabled and absent;
- no unexpected action, policy, cohort, handoff, or authority rows created;
- ordinary API request and Serve reconciliation health unchanged; and
- start, 10-minute, and 30-minute post-readiness checkpoints recorded in the
  relevant PR with identical empty authority state and no new error/restart
  trend.

Because production issue #1349 overlaps the ordinary Serve-health signal, the
final-removal production rollout also requires an exact 60-minute window from
the first instant that all 17 API/load-balancer workloads are Ready on the
final digest. All 16 slots must remain Ready, `STABLE`, synced, non-draining,
and converged, with exactly one ACTIVE and one STANDBY slot per service. The
window permits zero restart, split-brain, unintended-failover, or health-loss
events, and no role-sync failure interval may reach 60 seconds.

For the exact `boltz-l4-fleet` service pair, use the persisted controller logs
for its current service incarnation, including rotated `controller.log*`
segments. Deduplicate identical access lines and restrict them to the exact
window. Sort the completion timestamps of
`POST /controller/load_balancer_role`; the gap numerator is the number of
adjacent completion intervals at least eight seconds, and the rate is that
count divided by window hours. It must not exceed 24.23/hour (therefore at most
24 in the 60-minute window), which is 125% of the revision-366 baseline of 21
gaps in 65 minutes. For `POST /controller/load_balancer_sync`, divide access
lines with status 503 by all access lines for that path. That controller-side
rate must not exceed 3.44%, which is 125% of the revision-366 baseline of five
503s in 182 attempts. Do not add a load-balancer proxy log to either the
controller numerator or denominator. Enumerate it separately; a proxy-side 503
without a controller-side 503 in the same sync cycle is unexplained and blocks
closure, while a correlated line is classified once and disclosed.

This exact method reproduces the revision-366 baseline as 21 gaps in 3,900
seconds with 2,190 role completions, all HTTP 200, and five sync 503s in 182
attempts. The validated final-artifact +10 window had six gaps in 600 seconds,
328 role completions all HTTP 200, and zero sync 503s in 24 attempts. The
interim rate is above the threshold, but the contractual decision is the exact
60-minute numerator and denominator.

At +60, query `/_lb/capacity` on both exact `boltz-l4-fleet` Pods. On each slot,
`ha_observability.role.total_seconds.p99_recent` must be at most 10.32 seconds,
controller `total_seconds.p99_recent` at most 9.75 seconds, lock-wait maximum at
most 8.74 seconds, lock-hold p99 at most 9.64 seconds, pod-authority maximum at
most 9.39 seconds, and Service-routing-read maximum at most 8.75 seconds. These
are 125% of the worst recorded revision-368 values of respectively 8.25, 7.80,
6.99, 7.71, 7.51, and 7.00 seconds. Each `p99_recent` is the last at most 256
observations at the snapshot; each `max` spans the current process lifetime and
therefore includes startup. These process-local measures supplement rather
than replace the exact access-log window. The eight-second gap is also the
configured client deadline.

Enumerate every application WARN/WARNING, ERROR, CRITICAL, traceback, FATAL,
and PANIC line and every Kubernetes Warning event in the exact window. The only
pre-classified application signature is #1349's recovered
`HA role heartbeat failed; retaining role ... TimeoutError`; it is acceptable
for this cleanup-only comparison only when all safety and numeric limits above
pass. Any other signature is unexplained and blocks R0 until attributed.

This cleanup-only attribution gate is intentionally less strict than the
existing real-cluster HA `observe` qualification, which permits zero
`client_timeout` outcomes and caps recovered role-channel failure at 15
seconds. Passing it establishes that the source cleanup did not worsen the
pre-existing signal; it does not satisfy the HA qualification, change its SLO,
or close #1349. That issue remains the owner of eliminating the timeouts and
qualifying the large-fleet topology under the stricter contract.

### `boltz-test` compatibility deployment evidence (2026-08-08)

PR #1340 merged as `66de423064d01b7e0fbeaf552804bd55236d00f6`.
Its exact chart is `1.1.1159` with OCI digest
`sha256:07ed313fc8f7e80ea1aaa82f0a2eb0163b0cc9827e46ed0e5c72cb7d4048d6c6`;
all three roles use image digest
`sha256:900c539a4c70264bd6f978bc463be665a57a08d6029552c70dac5b6ba56beb2f`.
The monitored workload rollout was Helm revision 93; revision 94 later applied
the stored-value scrub without changing the workload.

The attempted `helm upgrade --dry-run=server` was not read-only in this
environment: it persisted a release revision, executed its migration hook,
patched Deployments, and requested surge capacity. Do not use server-side Helm
dry-run for this release. Interrupted revisions were stopped without a schema
downgrade; their pending release records were checksum-backed up before
removal. All forward heads remain API008, Serve039, state028, and capacity001.

Readiness at 03:00:25 UTC, +10 at 03:11:31 UTC, and +30 at 03:31:38 UTC passed
with all six role Pods on the exact commit and image, zero restarts, no
post-readiness Warning events, no authority objects, every private-handler/all
retained authority table count zero, and every candidate nullable-column count
zero. Recovery and convergence launched nine transient CPU-only Spot
instances, eight of which terminated; the remaining one replaced a consolidated
baseline node. No on-demand instance ran, and the NodePool returned to its
captured 10-node / 80-vCPU baseline at 03:02:23 UTC.

The `boltz-test` cluster has one `skypilot` chart release. Its original and
sanitized values rendered byte-identically with the exact compatibility chart.
Revision 94 applied the sanitized complete values at 03:32 UTC with
`--reset-values`, the same three image digests, and no `--reuse-values` or
`--atomic`. Migration job 94 succeeded, all six Pod names and creation
timestamps remained unchanged, all Deployment generations remained observed
and 2/2, the database zero-state remained unchanged, and `helm get values` now
contains no `resourceActions` key.

### Production compatibility deployment evidence (2026-08-08)

Production Helm revision 368 deployed the exact `1.1.1159` compatibility chart
and central image digest
`sha256:d4237ec47a2e74d58b93a312157b58cf9066ec134bcce262681ac356087dd4b5`.
Readiness at 03:38:22 UTC, +10 at 03:49:03 UTC, and +30 at 04:08:49 UTC passed.
The combined-role API Pod remained Ready with zero restarts, all 16 warm-standby
load-balancer Pods were Ready on the same digest with zero total restarts, and
the full 31-minute log scan found no ERROR, CRITICAL, or traceback signature.
The drained old-role heartbeat aged out by +10; exactly one current `all`
heartbeat was ready and authority heartbeats remained zero.

Production stored values contain no `resourceActions` key, no authority object
exists, and all heads remain API008, Serve039, state028, and capacity001. Every
private-handler request across all statuses, gated relation, and gated nullable
column remained zero or null.

The severe-signature scan through +30 was empty, but the interval was not
warning-free. Across the two `boltz-l4-fleet` slots, 22 HA role-heartbeat
attempts logged an asyncio `TimeoutError`: 10 while retaining ACTIVE and 12
while retaining STANDBY. They occurred in clusters at 03:41:15--03:43:15,
03:54:36--03:56:52, 03:59:41--04:01:27, and 04:04:36--04:04:38 UTC. Both slots
remained Ready with zero restarts, retained safe roles, and continued returning
healthy liveness responses; the other 14 load balancers and API logged no
application warning.

The extended audit through 04:25:04 UTC counted 26 matching warnings, 12 while
retaining ACTIVE and 14 while retaining STANDBY. Post-+30 recurrences were
ACTIVE at 04:14:46 and 04:19:30 and STANDBY at 04:16:15 and 04:19:32. The ACTIVE
slot also logged controller-sync HTTP 503 failures at 04:14:20, 04:19:39, and
04:23:39, each with one ERROR and one traceback line. Readiness, roles, and zero
restarts were unchanged. These signatures are disclosed explicitly; the
post-+30 interval was not a zero-severity quiet window.

Independent pre-change evidence closes attribution to this cleanup. The
persisted controller access log on production revision 366 had 21 role-response
gaps of at least eight seconds from 02:25--03:30 UTC, compared with 19 from
03:40--04:25 on revision 368. The corresponding windows had respectively five
controller-sync 503s in 182 attempts and two controller-side 503s in 155
attempts, plus the one post-change proxy-side 503 above. The exact diff from
revision 366 commit `5eb15b544e6fdb5bf43853b5e753d6e24cf4515e` to compatibility
merge `66de423064d01b7e0fbeaf552804bd55236d00f6` is a broad authority
cleanup spanning 122 files; attribution does not depend on characterizing that
whole diff as small. The executable heartbeat-path comparison leaves
`load_balancer.py`, `lb_k8s.py`, `controller_proxy.py`, `lb_ha.py`, and
`lb_ha_observability.py` unchanged. The diff changes 33 other
`sky/serve` files, but inspection of those deltas shows deletion or
disconnection of retired `resource_action*` modules, arguments, state helpers,
and preflight token functions. In the adjacent `constants.py` and
`controller.py`, it deletes retired-authority constants and a startup-only
token-isolation check; it does not change the functions serving role, proxy,
Kubernetes-authority, routing, or sync traffic. Bounded runtime observations
locate the latency in pre-existing serialized Kubernetes reads: pod-authority
and Service-routing reads reached 6.25--7.51 seconds and role-lock wait reached
6.99 seconds against an eight-second client budget. Issue #1349 owns that
separate performance defect. The evidence does not support treating it as a
#1340 or #1346 regression, so it no longer blocks the authority-cleanup
production promotion.

### HA role latency fix-forward contract (2026-08-08)

The first two bounded #1349 fixes preserved safety but did not meet the exact
latency gate. Production revision 370 shared the Pod and Service reads, yet the
two large-fleet slots each added 14 clean-window `client_timeout` outcomes.
Revision 371 parallelized the independent Pod, Service, and first Deployment
reads. After clean T0 at 07:48:28 UTC, both slots timed out again at 07:52:20
UTC. One validated Kubernetes snapshot took 5.53 seconds while its peer waited
7.40 seconds on the per-service role lock; end-to-end role time reached 8.999
seconds. All eight pairs retained one ACTIVE and one STANDBY, stayed synced and
non-draining, and all 17 Pods remained Ready with zero restarts. The source is
therefore serialized read-side head-of-line blocking, not provider capacity or
an authority-fence failure.

The next fix-forward removes that blocking only for a validated STABLE
read-only snapshot:

- Read the controller fence, durable cutover state, and fail-closed Kubernetes
  role snapshot before acquiring the per-service transition lock. Concurrent
  slot heartbeats may overlap these independent reads.
- Enter the existing role lock and re-read both the controller fence and the
  complete frozen cutover state. Use the prefetched snapshot only when both are
  byte-for-byte equal and the phase remains STABLE; otherwise return the
  existing fail-closed `cutover_state_unavailable` outcome and retry from a new
  snapshot.
- Keep every MIGRATING, ROLLING_BACK, PREPARING, DRAINING, planned promotion,
  selector patch, database transition, session-ledger update, and drain-view
  publication under the existing lock. No prefetched observation can cross a
  durable transition.
- For the read-only snapshot, use the Service's exact API Deployment
  ownerReference as the expected identity, then perform one live Deployment UID
  read after the Service and require equality. This preserves the prior final
  replacement linearization point while removing the redundant earlier GET.
  Mutation callers retain their existing two-read owner fence.

Focused tests must prove that two 143-backend STABLE slot heartbeats overlap
their snapshot reads, then serialize only the short exact fence/state
revalidation and decision tail. They must also prove that any fence or state
change rejects the prefetch, that non-STABLE phases never prefetch, that
malformed/replaced owners fail closed, and that transition mutation ordering is
unchanged. Completion still requires one immutable exact-merge artifact, direct
Helm staging and production rollouts on the existing fixed capacity, and a fresh
readiness/+10/+30/+60 production window with zero `client_timeout` delta,
recovered failures at most 15 seconds, no role/controller/phase observation in
the eight-second bucket, and every safety, health, schema, state, event, and log
gate passing.

PR #1362 implemented the overlap and under-lock revalidation portions of that
contract, while retaining the historical two-read Deployment owner helper. It
merged as
`0d6bd802bb32e2c35a3af7469e8968f4d39ea4b0`. Release `1.1.1171` and source
image digest
`sha256:830a2e317fcb9a9b80d39bc74046ca00b79925169dbf611db173999db8390343`
point exactly to the merge. Its fresh `boltz-test` readiness/+10/+30 window
passed with six exact Ready workloads, zero restarts, and no On-Demand node.
Production direct-Helm revision 372 reached a clean exact readiness baseline at
09:09:50 UTC with all 17 workloads Ready, zero restarts, all eight pairs
`STABLE` and converged, and the same three fixed `m6i.8xlarge` nodes.

The production +10 exact-behavior gate nevertheless failed. Each
`boltz-l4-fleet` slot added 21 `client_timeout` outcomes and ended with an
active failure streak; each `boltz-l4-fleet-test` slot added one recovered
timeout. The change did remove the targeted cross-slot contention: large-fleet
role-lock wait p99 fell to 0.39/0.58 seconds and lock-hold p99 to 0.45/1.40
seconds. The remaining path is one slow individual Kubernetes snapshot plus
duplicated SQL and proxy fences. Kubernetes snapshot p99 remained 6.43/6.44
seconds, controller p99 7.51/7.71 seconds, and end-to-end role p99 entered the
8.998-second bucket. Safety, state, health, fixed capacity, and zero-restart
invariants stayed intact, so this is a fix-forward latency failure rather than
a rollback trigger.

PR #1368 adds the complementary bounded read collapse without changing the
eight-second client budget or six-second independent report-freshness gate:

- One PostgreSQL query returns the exact controller owner/incarnation tuple and
  the complete durable cutover state, including drain start. The pre-lock read
  supplies the fence, frozen state, and resource scope used by the Kubernetes
  snapshot, replacing the prior fence query, cutover-state query, and snapshot
  owner query. No authority value is cached.
- Under the existing role lock, repeat that same complete query and require the
  entire owner/fence/state record to equal the pre-lock record before the
  Kubernetes result may affect the session ledger or role decision. Non-STABLE
  and mutation paths use the record read under the lock and retain all existing
  transition serialization.
- On both reads, derive the owner fingerprint from the live service hash,
  controller PID, normalized IP, and controller port and require it to equal
  the immutable fingerprint with which this controller child booted. Matching
  only PID/IP is insufficient: a controller restart may reuse them while
  changing its port, and a service incarnation change must also fence the old
  child.
- After that exact under-lock owner proof, the controller may attach its
  existing owner fingerprint to the role response. The stable API proxy accepts
  the attestation only when it exactly equals the owner fingerprint read before
  routing the request; it then omits its redundant post-response owner query.
  A missing attestation, as during a mixed-version rollout, retains the current
  post-response owner read. A mismatched attestation fails closed. Every
  non-role controller route retains both proxy owner reads.
- The attestation moves the successful role response's last owner
  linearization point from the proxy to the controller's immediately preceding
  complete under-lock row read. It does not extend a TTL, trust a client value,
  weaken controller-request authentication, accept a stale cutover state, or
  permit a transition outside the lock.

The first merged #1368 implementation compared only PID/IP to the controller's
bootstrap owner before returning the old bootstrap fingerprint. Pre-production
adversarial review rejected that implementation: if the database hash or port
changed after the proxy's first read but before controller prefetch while
PID/IP were reused, the old child could attest the stale proxy fingerprint and
incorrectly suppress the final proxy read. Release `1.1.1174` reached only
`boltz-test`; its interrupted qualification is not promotion evidence. The
fix-forward must validate the complete live fingerprint on both controller
database reads and fail closed before any snapshot or attestation when hash,
PID, IP, or port differs.

Focused tests must prove one SQL read before and one after the Kubernetes
snapshot, byte-for-byte owner/state mismatch rejection, no snapshot-side SQL
owner read, pre-prefetch rejection for every bootstrap fingerprint component,
exact controller attestation, mixed-version proxy fallback,
mismatched-attestation rejection, and unchanged two-read behavior for every
other proxy route. The 143-backend overlap, owner-replacement, transition, and
full external-load-balancer suites remain mandatory. The immutable follow-up
must repeat the same direct-Helm staging and production qualification; revisions
372 and the interrupted `1.1.1174` staging window do not satisfy completion.
PR #1367 subsequently addressed the cross-slot provider amplification exposed
by the same production evidence. It merged as
`34822adbbd56d946cd21c70eebf4aa11cb8dc8ac` and release `1.1.1173`, but was not
deployed independently before the complete read-collapse change was ready.

Revision 372's remaining amplification is between the two slot heartbeats.
After #1362 they independently execute the same fail-closed Pod, Service, and
final live Deployment-owner reads for the same immutable PostgreSQL fence and
cutover state. Under provider pressure that doubles identical Kubernetes API
traffic. Moving the final live Deployment UID read earlier is not acceptable:
it would widen the replacement race by making the owner check cease to be the
last Kubernetes snapshot observation.

PR #1367 coalesces only an identical snapshot while it is actively running:

- key the in-flight task by the complete immutable controller owner/fence row
  and frozen cutover state, and share it only for validated STABLE requests
  with the exact same key;
- remove the task immediately when it completes, with an identity-checked
  callback so an older task cannot clear a newer different-key task. There is
  no TTL, completed-result reuse, cache, or stale authority window;
- shield the shared task from individual request cancellation so one timed-out
  slot cannot cancel work already awaited by its peer. Snapshot success,
  bounded failure, and subphase timings are deterministic for every waiter;
- keep the Service-then-live-Deployment ordering and final owner
  linearization inside `get_lb_role_snapshot` unchanged; and
- independently re-read and compare the exact complete owner/fence/cutover row
  under the role lock for every request before its own session-ledger update or
  role decision. Different keys start independent tasks, and non-STABLE states
  keep the existing locked path.

Focused tests must prove that two concurrent exact-key STABLE heartbeats make
one provider snapshot call but retain two independent fenced decisions, that a
different fence or state is never shared, that cancelling one waiter does not
poison its peer, that shared errors retain their deterministic fail-closed
outcome, and that non-STABLE behavior is unchanged. The same immutable staging
and fresh production readiness/+10/+30/+60 gates remain mandatory.

PR #1369 completes the remaining read-only Kubernetes owner collapse. For HA
Pod authority, the already-read Service supplies the exact API Deployment
ownerReference. The helper validates its kind, name, non-empty UID, service
incarnation label, and resource version, then performs one live Deployment UID
read after the Service and requires equality. This retains the prior final
replacement linearization point while removing the redundant pre-read.
Mutation helpers keep both reads because they must construct a new desired
ownerReference. Focused tests require exactly one live Deployment read on the
HA authority path and fail-closed behavior when the Deployment is replaced.
The immutable rollout artifact must include #1369 together with #1367, #1368,
and the full controller-owner attestation correction above.

### HA role executor-isolation and snapshot-deadline correction (2026-08-08)

PR #1369 merged as
`b8ba790278459a5f228e0108e54f8fcfa98a8d7b`. PR #1370 then corrected the
first #1368 implementation by deriving the complete live service hash, PID,
normalized IP, and port on both database reads and comparing all four values to
the controller's immutable bootstrap fingerprint before any Kubernetes snapshot
or attestation. Its focused mismatch matrix proves that each independently
changed component fails closed before invoking the snapshot handler. The exact
integrated external-load-balancer, Kubernetes, proxy, and state suite passed,
its exact head passed 30/30 CI checks, and it merged as
`54184f7c7046d1113077f61232045d5e8fe4d6d7`.

Release `1.1.1176` points exactly to that final merge. Its source image is
`255203429798.dkr.ecr.us-east-1.amazonaws.com/skypilot-nightly-boltz@sha256:68b9869f4fcc7ae8fa752443b98ed779d827c5a6d1e734bc849b58bd49617cbc`;
the chart OCI digest is
`sha256:04288f5d76edaf4658a6d0204667f27cba6f6ba61c3b6a0ef9f526d62600259b`.
The test-account mirror resolves to the same image digest. `boltz-test` Helm
revision 104 deployed that immutable artifact with `--reuse-values`; the
canonical non-image values before and after were byte-equivalent and migration
104 succeeded exactly once. A temporary two-Spot-claim rollout surge returned
to exactly 10 Spot claims, zero On-Demand claims, and zero deleting claims
before clean T0 at 11:21:46 UTC. Readiness, +10 at 11:32:21, and +30 at
11:52:27 passed with six exact-digest workloads Ready, zero restarts, exact
health/version/commit, schema heads API008/Serve039/state028/capacity001, two
fresh healthy heartbeats per role, empty private/gated state, no authority
objects, and no post-T0 application severity or Kubernetes Warning event.

Production preflight rendered digest
`236a230e92356fbf43d312f3e0156430b3279883fcb648380e269968acbdf1ef`.
The fixed node group remained min/max/desired/actual 3/3/3/3 on the same three
`m6i.8xlarge` instances: `i-003a087558f131dc8`,
`i-01d341c152ac226b3`, and `i-084d983ca017ad5d8`. A concurrent operator
deployed the same exact `1.1.1176` artifact as Helm revision 373 at 11:47:18
UTC, five minutes before the staging +30 gate unlocked promotion. This is an
explicit sequencing-contract departure; it does not waive the staging or
production evidence gates. After staging +30 passed, the authorized
`--reuse-values` deployment of the identical chart and image completed as
revision 374 at 11:53:31 UTC without `--atomic` or a capacity change. Canonical
non-image values are unchanged from revision 372, and both forward-only
migration jobs 373 and 374 succeeded once with zero failures.

Production reached a clean exact-artifact T0 at 11:55:42 UTC. All 17 API and
load-balancer workloads were Ready on the exact digest with zero restarts; all
eight service pairs were `STABLE`, converged, synced, non-draining, correctly
routed, and split one ACTIVE/one STANDBY; all 16 capacity endpoints succeeded;
health reported version `1.1.1176` and the exact merge; the four schema heads,
fresh heartbeat, empty retired/private/gated state, and absent authority-object
checks passed. The +10 sample at 12:05:55 also passed with no pod or node
identity change, no non-success outcome delta, no active or recovered failure,
no eight-second role/controller/phase bucket, no Kubernetes Warning-event
delta, and no application severity since T0. The large-fleet pair added 243/242
successful observations; its role p99 was 3.20/2.94 seconds and controller p99
was 2.39/2.30 seconds. The three exact ASG members remained `InService` and
healthy at 3/3/3/3.

The binding +30 sample at 12:25:55 failed the intended-behavior gate without a
safety regression. Between T0 and that sample, each large-fleet slot added five
`client_timeout` outcomes and entered the eight-second role bucket. Both slots
recovered to `last_outcome=success` and the correct ACTIVE/STANDBY split, but
their maximum failure-recovery durations were 29.42/29.93 seconds. The shared
snapshot's final live Deployment-UID validation reached 32.90 seconds. Because
#1367 deliberately shields and shares an identical in-flight snapshot, every
heartbeat arriving during that provider stall joined the same unbounded task;
one slow owner read was therefore amplified into a roughly 30-second role
failure streak. A route-lease 503 and later sync 503 corroborate the interval.
All 17 workloads remained Ready with zero restarts, all eight pairs stayed safe,
the four schema/empty-state gates and Kubernetes Warning-event gate remained
clean, and the fixed ASG remained 3/3/3/3 on the same healthy members. Revision
374 remains deployed because revision 372 has the same latency defect and there
is no safety rollback trigger. The failed monotonic window stops before +60 and
cannot qualify R0.

The next bounded fix-forward gives a shared STABLE snapshot one three-second
task-creation deadline. That remains above the observed 1.61-second snapshot
p99 while leaving headroom inside both the six-second report-freshness limit
and the eight-second client timeout. Every waiter observes the same deadline;
a late joiner cannot extend it. On expiry, the controller returns the existing
fail-closed routing-unavailable outcome, removes that exact task from the
shareable slot, ignores any eventual executor result, and permits the next
heartbeat to start a fresh fenced snapshot. The underlying Pod, Service, and
final Deployment Kubernetes calls also receive the same three-second transport
deadline so abandoned executor work cannot accumulate without bound. No partial
or expired snapshot is cached or consumed; every successful retry still
performs the final live Deployment-UID validation and under-lock exact database
revalidation, and all transition/mutation paths remain unchanged. Focused tests
must prove a hung provider read is shared only until the fixed deadline, both
waiters fail closed below the client budget, a later fresh read can succeed
without a stale completion clearing or replacing it, transport deadlines cover
all three role snapshot reads, cancellation remains isolated, and non-STABLE
behavior is unchanged. Completion again requires an exact immutable artifact
and fresh staging readiness/+10/+30 followed by production
readiness/+10/+30/+60 on the unchanged capacity, with zero `client_timeout`
delta, failure recovery at most 15 seconds, and every safety/state/log/event
gate passing.

A concurrent monitor reset its revision-374 clock at 11:57:52 UTC. Its +10
sample at 12:08:06 caught the first timeout from the same failed window: each
large-fleet slot added one `client_timeout` at 12:06:15/12:06:16 and recovered
in 4.974/4.738 seconds, while unrelated service controllers remained
responsive. The Kubernetes selector returned exactly the two Pods for one
service incarnation, and fleet sync/readiness work was active in the same
controller process. Because cancelled clients did not publish their controller
phase trace, that earlier sample could not distinguish default-executor queue
delay from a PostgreSQL, Kubernetes, GIL, or process-scheduling stall.

PR #1373 therefore made the independently justified scheduling correction. It
gave every initially HA-enabled controller a fixed two-worker role executor for
all blocking work in `_handle_load_balancer_role`, including both PostgreSQL
reads, the shared STABLE snapshot, and serialized transition work. Ordinary
provisioning, autoscaling, sync, and unrelated controller work remain on the
default executor. In that merged implementation the pool was created only when
HA was enabled during construction, was shut down by the controller lifespan,
and exposed queue delay separately from blocking-operation latency. A focused
test saturates the default executor and still requires a real role request to
return the intended role. PR #1373's exact head passed all CI checks and 494
selected tests, and merged as
`5c399d5dfa65b711d5c24010111eeef9054d3a3e`.

Executor isolation is complementary to, not a replacement for, the provider
deadline. The binding +30 trace measured 32.90 seconds inside
`snapshot_ownership_validation`, after the dedicated executor job would have
started. A dedicated worker prevents unrelated default-executor work from
queuing ahead of authority reads, but it cannot bound an already-running
Kubernetes Deployment GET. Deploying #1373 alone therefore cannot satisfy the
observed failure contract and is not a qualification candidate.

The stacked bounded correction retains #1373 and adds these invariants:

- The shared STABLE snapshot has one three-second deadline measured from task
  creation. That is above the observed healthy 1.61-second snapshot p99 and
  leaves retry and proxy/database headroom inside the six-second report-age
  limit and eight-second client timeout. Every exact-key waiter observes the
  same deadline; a late join cannot extend it.
- On expiry, every waiter receives the existing fail-closed
  `routing_unavailable` outcome, the exact task is removed from the shareable
  slot, and its eventual side-effect-free executor result is ignored. A later
  heartbeat starts a fresh fenced read, and an old identity-checked completion
  cannot clear a newer task.
- The STABLE Pod LIST, Service GET, and final live Deployment UID GET each use
  the same three-second Kubernetes transport timeout. This bounds abandoned
  workers as well as asyncio waiters. Non-STABLE transition and mutation paths
  retain their existing transport behavior because timing them out could leave
  an ambiguous write or transition.
- The complete owner/fence/cutover record is still read before the Kubernetes
  snapshot and compared byte-for-byte under the role lock. Every successful
  retry still performs Service-then-live-Deployment validation. Session-ledger,
  state-machine, demand-lock, transition, and mutation fences are unchanged.
  There is no TTL, completed-result cache, fallback authority, stale result, or
  capacity change.
- The dedicated role executor is a constructor-established typed field on
  every controller; its threads remain lazy while HA is disabled. An in-place
  legacy-to-HA transition retains that exact executor. Role handling and shared
  snapshot submission access the field directly, so no missing/optional
  fallback can silently select asyncio's shared default executor.

Focused tests must jointly prove default-executor isolation both at startup and
after an in-place legacy-to-HA transition, fixed shared-task expiry for both
slots, a late waiter's inability to extend the deadline, fresh retry while the
expired synchronous read is still blocked, late-result isolation, all three
STABLE transport timeouts, cancellation isolation, and no timeout keyword on
non-STABLE reads. The complete external-load-balancer,
owner-replacement, exact-key, complete-row mismatch, and transition suites
remain mandatory. Completion requires one immutable merge containing both
corrections, direct-Helm staging readiness/+10/+30, and a fresh direct-Helm
production readiness/+10/+30/+60 window with zero incremental
`client_timeout`, no role/controller/phase observation in the eight-second
bucket, recovery at most 15 seconds, unchanged fixed capacity, and every
safety, state, schema, event, log, health, and restart gate passing. The
qualification release combines placement PR #1380 with both HA corrections
#1373 and #1374, including PostgreSQL Serve revision 040. Its reader-first
upgrade must start from the exact live Serve039 head with
`serve.controllerHold=false`, leave revision 040 open and terminal retirement
state empty, and validate every prior protocol-1/2/3 manifest. Staging must
validate and converge its exact complete local receipt inventory; production
must preserve all eight requested/loaded receipts unchanged. Bounded
default-mode (`mode=None`) placement inventory/ledger dry-runs are required and
permitted during qualification because they have `run_id=None` and create no
protocol-4 identity. Enabling the hold, approving writer freeze evidence,
invoking an apply-supported or terminal writer, and requesting a protocol-4
receipt remain forbidden during both HA windows. Only a passing production +60
sample authorizes a separate hold revision on the identical immutable pin tuple
(combined merge SHA, image digest, chart version, and chart digest), followed by
the placement design's final held preflight, full snapshot/restore drill, and
writer gates. After terminal apply, the combined artifact—not standalone
v1.1.1182—is the production-qualified operational rollback floor for the live
database. Final combined local verification passed after closing the in-place
migration gap: the new legacy-to-HA/default-executor regression passed, the
complete three-file controller/HA/Kubernetes selection passed all 430 tests,
and the exact 21-file controller/external-load-balancer selection passed all
924 tests.
The configured mypy run passed 887 source files, Pylint rated the changed Python
files 10.00/10, dashboard lint/format passed, and `git diff --check` passed.
Exact-head CI and both deployment windows remain open gates.

### Final-removal artifact evidence (2026-08-08)

Although it merged before the production compatibility gate closed, PR #1346's
exact code head `7a5315d577b54c1ba970991d3ca974b5fbee797c` passed all 32 CI
checks, and the adversarial code and migration review found no implementation
blocker. Merge `0b77ca77ae8b099c2de07566670743651744bbe2` published release
`1.1.1161`. The source image digest is
`sha256:310effb333ad0808b4289f05ee46ac89ea21b156b6e54e5df5e47bbe8198e002`;
the chart OCI digest is
`sha256:4bc611db6048419dfd296bf4d82d9542f9a0bb599e54febb5520bcc79b2bf799`.
The chart metadata records the exact merge and version.

The same image was mirrored into the existing `boltz-test` registry as
`sha256:b780e6b7c7fcc2606baed83ce06dc2f12a6913db13e01d615d2fcdce48d15eb6`.
Registry normalization changed compressed-layer and manifest digests, while the
image configuration digest remained exactly
`sha256:b31d9b0414aa61fa7b0183d58e5155ddc079838e312c701215e59a059c94543f`.
The local image identity is linux/amd64, version `1.1.1161`, and exact merge
`0b77ca77ae8b099c2de07566670743651744bbe2`. Publishing and mirroring created no
compute capacity. The exact chart rendered client-side with the complete
sanitized `boltz-test` values and all three roles pinned to the mirrored digest
with SHA-256
`d6d4af9e2c32db8c4603cdacf9660e4b1d4d5f015da929e5c45735bbde81982a`;
the render contained no retired authority reference. A seeded disabled legacy
value failed with the exact chart tombstone.

Production's complete stored values also rendered client-side with the exact
`1.1.1161` chart and final source digest, with SHA-256
`557bc456a226cd8959a05aa945eaea1389a90687b0e7fb6d72a27c5505814c6c`.
The final digest appeared in all three chart-owned image positions, the prior
digest appeared nowhere, and the render contained no retired-authority
reference. The three existing fixed nodes are `m6i.8xlarge`, each with about
31.85 allocatable vCPU and 120.9 GiB. The two non-API nodes had respectively
1.49 vCPU / 5.1 GiB and 2.24 vCPU / 6.7 GiB requested. One API surge requests
16 vCPU / 96 GiB; even the conservative simultaneous 16-load-balancer surge
adds only 1.6 vCPU / 8 GiB. The combined 17.6-vCPU / 104-GiB transient request
plus the existing load is at most 19.84 vCPU / 110.7 GiB, so it fits either
non-API node without provider capacity. The sole EKS managed node group and its
Auto Scaling group are fail-closed at min=max=desired three; the cluster exposes
no Karpenter/NodeClaim API and runs no Karpenter or cluster-autoscaler workload.
All 17 new Pods actually scheduled across the same three preflight nodes,
provider instances `i-003a087558f131dc8`, `i-01d341c152ac226b3`, and
`i-084d983ca017ad5d8`. At readiness and +10 they retained their June 27 node
UIDs, remained Healthy/InService, and the Auto Scaling group had no activity
after 04:30 UTC. The expected and observed incremental node/GPU/cost delta is
therefore zero; actual placement also proves the workload scheduling
constraints were eligible on those nodes. If scheduler or scaling state ever
invalidates that bound or any new provider-capacity request appears, stop
rather than relying on unapproved expansion.

### `boltz-test` final-removal deployment evidence (2026-08-08)

A concurrent workspace process started Helm revision 95 at 03:58:59 UTC despite
the recorded deployment hold, before this design correction landed, and without
the required named capacity approval. This is a second process-contract
departure. Kubernetes had already accepted the release when it was detected,
so no competing rollback or retry was issued.

Migration job 95 succeeded once on the exact digest. Readiness at 04:04:08 UTC,
+10 at 04:14:25 UTC, and +30 at 04:35:25 UTC passed with API, controller, and
executor each 2/2 Ready: six Pods on exact mirror digest
`sha256:b780e6b7c7fcc2606baed83ce06dc2f12a6913db13e01d615d2fcdce48d15eb6`,
zero restarts, healthy and ready API endpoints at merge `0b77ca77`, and no
targeted error signature. Stored values contain no `resourceActions` key, no
authority object exists, heads remain API008, Serve039, state028, and
capacity001, and every private-handler, gated-relation, and gated-nullable check
remains empty or null. By +10 and again at +30, fresh heartbeats were exactly
two ready rows per role, drained rows had aged out, and every current role
application container had zero warning or severe log signature from readiness.
One Kubernetes Warning event is recorded explicitly: at 04:04:14 UTC, the
already-draining old executor Pod returned a readiness-probe 503. That Pod is no
longer present; it produced no current restart or readiness degradation.

The rollout created exactly two temporary 8-vCPU nodes at 03:59:21 UTC, both
Spot; on-demand exposure was zero. Karpenter returned accounted capacity from
96 to the captured 80-vCPU baseline by 04:04:41 UTC and began terminating the
two surplus claims. The cluster returned physically to 10 nodes / 80 vCPU at
04:11:35 UTC, with all 10 claims Spot and none deleting. The required +30
checkpoint reconfirmed that exact state with zero on-demand exposure.

### Production final-removal deployment evidence (2026-08-08)

Production Helm revision 369 started deploying the exact final-removal chart
and source image at 04:36:18 UTC. Migration job 369 succeeded once. All 17 API
and load-balancer workloads first converged on the exact digest with Ready Pods
and zero restarts at 04:40:17 UTC. The release is deployed on chart/app
`1.1.1161`, OCI digest
`sha256:4bc611db6048419dfd296bf4d82d9542f9a0bb599e54febb5520bcc79b2bf799`,
and central image digest
`sha256:310effb333ad0808b4289f05ee46ac89ea21b156b6e54e5df5e47bbe8198e002`.
The API health and readiness endpoints returned 200 at exact merge
`0b77ca77ae8b099c2de07566670743651744bbe2` and version `1.1.1161`.

Revision 369 used `--reset-values` with the complete sanitized current
user-values stream and only the `apiService.image` override. The exact
client-side render had SHA-256
`557bc456a226cd8959a05aa945eaea1389a90687b0e7fb6d72a27c5505814c6c`,
preserved the database and credential configuration, and placed the exact image
in all three chart positions. No `--atomic`, server-side dry-run, schema
rollback, or platform-level change was used. Because production already had no
retired stored value, using reset instead of the required `--reuse-values` was
a process-contract departure and is not precedent for later upgrades.

The readiness invariant audit passed. Stored values contain no
`resourceActions` key, no authority Kubernetes object exists, and database
heads remain API008, Serve039, state028, and capacity001. Every private-handler
request, gated relation, and gated nullable value remains empty or null. All
eight HA services are `STABLE` with no pending or draining transition, and each
Service selector and generation matches the durable slot state. The +10-,
+30-, and issue-#1349-aware +60-minute production monitoring results are
recorded below. The original +60 comparison failed, so a fresh window on the
bounded fix remains required.

The readiness window was not timeout-free. Counters retained from rollout show
two and three recovered `client_timeout` outcomes on the `boltz-l4-fleet`
ACTIVE/STANDBY slots and one on each `boltz-l4-fleet-test` slot. Their maximum
role durations were 8.464, 8.668, 8.389, and 8.058 seconds; maximum recovered
failure durations were 5.976, 6.311, 3.421, and 2.995 seconds. All four ended
with `last_outcome=success`, inactive failure streaks, correct roles, and
converged durable state; the other 12 slots had success-only counters. These
values are the explicit T0 process baseline. Post-readiness deltas and the
comparable controller access-log window determine the +60 result under the
scoped cleanup gate above.

The +10 audit sampled from 04:50:44 through 04:54:44 UTC and passed. All 17
workloads remained Ready on the exact digest with zero restarts; all eight
service pairs remained `STABLE`, synced, non-draining, and converged with one
ACTIVE and one STANDBY slot. Health still reported exact merge `0b77ca77` and
version `1.1.1161`; the four schema heads, 30 empty gated relations, four
private-handler populations, nullable candidate fields, and absence of
authority objects were unchanged. The same three fixed provider instances
hosted every Pod and the fixed three-node Auto Scaling group recorded no
scaling activity.

The only application signature from 04:40:17 through the final +10 sample was
14 instances of #1349's pre-classified timeout on `boltz-l4-fleet`: nine while
retaining ACTIVE and five while retaining STANDBY. Both slots ended with
`last_outcome=success`, inactive failure streaks, and safe roles; the API and
other 14 slots had zero warning or severe signature. Kubernetes recorded the
expected startup/readiness probe warnings while old and new Pods overlapped,
ending at 04:40:15 UTC, two seconds before the exact monitoring window; it
recorded no Warning event inside the window through +10. The access-log rate
and +60 latency thresholds remain open and are not inferred from these raw
client-timeout counters.

The exact +30 window ended at 05:10:17 UTC. Safety and cleanup-state checks
passed: all 17 workloads remained Ready on the final digest with zero restarts;
all eight HA pairs remained `STABLE`, correctly routed, synced, non-draining,
and converged; health, schema heads, empty gated/private state, authority-object
absence, and the fixed three-node capacity bound were unchanged. The exact
access-log result was 21 gaps in 1,800 seconds, 971 role completions all HTTP
200, and one controller sync 503 in 82 attempts (1.2195%). That 04:59:07 sync
failure had matching proxy/controller/LB records and recovered on the next
cycle. The window contained 17 ACTIVE-retaining and 14 STANDBY-retaining
#1349 warnings, no other application warning or severe signature, and no
Kubernetes Warning event.

The +30 process metrics remained within every +60 ceiling: role p99 was
8.998/8.990 seconds, controller p99 7.254/7.598, lock-wait maximum 6.500/7.669,
lock-hold p99 6.849/6.459, pod-authority maximum 7.732/6.703, and routing-read
maximum 6.043/7.579 for ACTIVE/STANDBY. Both slots ended in success with no
active failure streak; maximum recovered-failure duration was 21.477/14.778
seconds, below the cleanup gate's 60-second safety limit but not the stricter
#1349 qualification. After +30, a second correlated sync 503 recovered at
05:14:00 UTC. At 05:15:03.842 UTC the exact role-gap numerator reached 25 in
2,086.842 seconds (43.127/hour), irreversibly exceeding the at-most-24 +60
limit. Revision 369 therefore cannot close R0 even though its safety and
retired-state invariants remain intact. The bounded #1349 fix must pass a fresh
window before the retirement can be declared production-complete.

The exact +60 window ended at 05:40:17 UTC and confirmed that failure without
finding a safety regression. Exact-line-deduplicated current-incarnation logs
contained 1,843 role completions, all HTTP 200, but 64 adjacent completion gaps
of at least eight seconds (64/hour, versus the at-most-24 limit). The maximum
gap was 44.946 seconds and the head/tail coverage gaps were 0.566/0.500 seconds.
Controller sync passed at three 503s in 186 attempts (1.6129%, versus the 3.44%
limit); the failures at 04:59:07.635, 05:14:00.478, and 05:31:15.174 UTC each
had a same-cycle LB error and traceback and then recovered. There was no
proxy-only 503.

Both exact `boltz-l4-fleet` slots ended successfully in generation 160, synced
and non-draining, with no active failure streak. ACTIVE/STANDBY role p99 was
8.9988/8.9982 seconds, controller p99 7.8955/8.5815, lock-wait maximum
6.5713/7.6686, lock-hold p99 7.7653/8.2385, pod-authority maximum
7.7317/6.7360, and routing-read maximum 7.3040/8.1605. Maximum recovered
failure duration was 40.372/39.773 seconds: below this cleanup comparison's
60-second safety ceiling, but above issue #1349's 15-second qualification.
The exact window contained 105 pre-classified recovered heartbeat warnings
(53 retaining ACTIVE and 52 retaining STANDBY), the six lines belonging to the
three correlated sync failures, no unexpected application severity, and no
Kubernetes Warning event.

The final safety audit kept all 17 workloads Ready and available on the exact
digest with zero restarts. Health and version, the four schema heads, empty
private and gated state, nullable retirement fields, all eight `STABLE` HA
rows, Service selectors/generations, fresh-heartbeat rows, authority-object
absence, and the fixed three-node capacity bound remained correct. Local
observer access was interrupted from 05:20:38-05:26:42 UTC by expired SSO and
from 05:34:32.011-05:36:59 UTC by an SSM/WebSocket loss. Persisted controller
logs, retained Kubernetes logs/events, unchanged Pod UIDs, and zero restarts
backfilled both intervals; neither interruption is attributed to production.
The evidence manifest SHA-256 is
`f9e9bcd638000aed2deb5c170605f259d0909cd7e63239bee5409fdea680f162`.

PR #1355 owns that bounded fix. It replaces the duplicated steady-role Pod and
Service authority reads with one fail-closed snapshot under the existing role
lock. The snapshot keeps the PostgreSQL owner/hash/lifecycle/state fence,
incarnation-scoped Pod UIDs and slots, exact Service ownership and
resourceVersion, runtime revision, and both live API Deployment UID reads. It
reduces the successful steady heartbeat from seven sequential Kubernetes
requests to four without caching authority or adding another execution path.
Its 203 focused tests and exact-head CI passed, and PR #1355 merged as
`606b4b29703dd2a6e69f57e49db685e85a3c6468`. Release `1.1.1166` points exactly
to that merge; its source image digest is
`sha256:ad1fe699b9b940d669f6161cafcd1d719a5d8e4742572854adc9a7b5bf0c2013`
and chart digest is
`sha256:520ffca476dfcdeb8b10a90ce3403a956e9035dc4aeeac3f261951695a7c84e4`.
The zero-incremental-capacity rollout and a fresh 60-minute production
qualification remain open.

Production Helm revision 370 deployed that exact artifact at 06:10:11 UTC,
and all 17 API/load-balancer workloads first became Ready on its exact digest
with zero restarts at 06:14:31 UTC.  The stricter issue-#1349 qualification
failed immediately without a safety regression: both `boltz-l4-fleet` slots
recorded new `client_timeout` outcomes after readiness while retaining the
correct generation-161 ACTIVE/STANDBY roles.  By 06:18:28 UTC the exact
post-readiness logs contained six ACTIVE-retaining and seven
STANDBY-retaining timeout warnings.  Both slots remained synced,
non-draining, `STABLE`, and converged; all 17 Pods remained Ready on the exact
digest with zero restarts, the retired-state gate stayed empty, and Kubernetes
recorded no Warning event after readiness.  The zero-timeout gate is
monotonic, so revision 370 cannot qualify and its +60 timer was stopped rather
than misrepresented as recoverable evidence.

The new phase telemetry first narrowed the remaining work inside each shared
snapshot. On revision 370's first post-readiness sample, the ACTIVE/STANDBY
`kubernetes_role_snapshot` recent p99 was 5.065/3.475 seconds. Its Pod LIST
reached 4.111/2.067 seconds, the first live Deployment-identity read reached
3.572/1.133 seconds, and the second live Deployment-ownership validation
reached 1.177/1.862 seconds. PR #1360 therefore issued only the independent
Pod LIST, Service GET, and first Deployment GET concurrently, joined all
three before parsing or making a role decision, and retained the second live
Deployment UID read after the join. It changed no cache, TTL, authority
contract, mutation path, or provider capacity. Its 204 focused tests and exact
30/30 CI passed, and it merged as
`701ae52216254b5f25e485f42adf8d307062e37a`. Release `1.1.1168` points exactly
to that merge; its source image digest is
`sha256:1f25b3b44e01c6420284cd79862245ed717e11dbbfe7a6f54ce0b2ece5b1d2df`
and chart digest is
`sha256:20283f5c4fe469f2c8ac2b2424062e426f05ee6d930318e42a0ee524443e6fee`.

Test Helm revision 98 deployed that exact artifact on the bounded CPU-only
Spot cluster. A first observation window was invalidated, rather than counted,
when Karpenter terminated an older Spot node at its first +10 sample. The
subsequent fresh window passed at T0 07:07:09, +10 07:17:58, and +30 07:37:43
UTC: all six exact workloads remained Ready with zero restarts, both slots
remained correctly routed, stable, synced, and non-draining, schema and retired
state stayed exact and empty, the 10-claim baseline did not grow, and no
On-Demand node appeared.

Production Helm revision 371 then deployed the same exact chart and image with
the complete preserved values and no incremental provider capacity. The
migration succeeded once, all 17 workloads converged to the exact digest with
zero restarts, all eight pairs were stable, correctly routed, synced,
non-draining, and converged, and the fixed three-node Auto Scaling group and
all cleanup-state gates remained unchanged. That artifact nevertheless failed
the monotonic zero-new-timeout gate immediately. At T0 07:51:29 UTC the
`boltz-l4-fleet` ACTIVE/STANDBY counters were 24/20 `client_timeout` and 85/62
success; by 07:51:55 they were 25/22 timeouts. The +60 timer was stopped.
Successful owner reads remained below 0.3 seconds, while the serialized
controller-forward path reached roughly 8.1--8.65 seconds and controller
role-lock wait reached 7.38 seconds. The evidence proves that concurrent reads
inside one snapshot are insufficient while the two slot snapshots themselves
remain serialized across the per-service role lock.

PR #1362 is the parent correction. Only an already validated `STABLE`,
read-only Kubernetes role snapshot may execute before the transition lock, so
the two warm slots may overlap their slow provider reads. The handler then
acquires the lock and exactly revalidates the PostgreSQL owner fence and the
complete frozen durable state before the snapshot can affect the session
ledger or a role response. Any mismatch fails closed. Every non-STABLE
snapshot, planned promotion, selector/database mutation, ledger update, and
drain publication retains the existing serialized path. The read-only snapshot
uses the Service ownerReference as its expected API Deployment identity and
retains a final live Deployment UID validation after the Service read; mutation
callers retain their two-read owner fence. There is still no authority cache,
TTL, stale-role acceptance, new execution path, or provider-capacity
dependency. The 216 focused HA/Kubernetes/qualification tests and complete
462-test external-load-balancer unit surface pass locally. Exact-head CI and a
fresh exact readiness/+10/+30 test qualification followed by
readiness/+10/+30/+60 production qualification remain mandatory.

### R0 manual test plan

Before the compatibility deployment, run the focused Python tests for the
retained generic action substrate and private-handler quarantine, Helm unit
tests, chart lint, and the image-worker template guard. Against a disposable
release seeded with the legacy value object, verify both compatibility
branches:

1. Render the complete persisted disabled-value fixture with `helm template`.
   It must succeed and contain none of the retired authority workload,
   preflight, token, volume, or environment surfaces. Do not use
   `helm upgrade --dry-run=server`; it executed hooks and mutated the live
   release during this rollout.
2. The same client render with `enabled=true` fails with the exact
   retired-feature message above.

For the `boltz-test` rollout, record
`helm get values skypilot -n skypilot -o yaml`
and the current revision first. Upgrade with `--reuse-values` while setting the
API, controller, and executor images to the same immutable digest. This cleanup
requires zero GPUs. That guarded HA rollout can request at most two temporary
8-vCPU nodes before freed slots are reused; Spot is preferred, and any
on-demand fallback requires the recorded management approval, price ceiling,
and hard time window. Production instead uses the exact zero-incremental-node
bound above and must stop if scheduler state invalidates it. At readiness, +10
minutes, and +30 minutes:

- confirm all ordinary control-plane Pods are ready with no new restarts;
- confirm the namespace has no authority Deployment, Service,
  ServiceAccount, ConfigMap, or Pod;
- query the four private API handler names and every relation/nullable column
  listed in the R0 preflight above, and require the same empty/null result;
- confirm ordinary API request, Serve reconciliation, and database error rates
  have no adverse trend; and
- record the exact image digest, chart revision, queries, and observations in
  the PR before declaring the cleanup done.

After those three checkpoints, export and sanitize every release's complete
stored values using:

```bash
jq 'del(.resourceActions.authorityWorker) |
    if .resourceActions == {} then del(.resourceActions) else . end' \
  complete-user-values.json > complete-sanitized-values.json
```

Render the complete original and sanitized value sets client-side with the same
compatibility chart and exact role images and require byte-identical output.
Then upgrade with `--reset-values -f complete-sanitized-values.json` plus all
three exact role image references and prove `helm get values` has no retired
key. This step must not use server-side Helm dry-run, `--reuse-values`, a null
override, or `--atomic`.

For this final-removal source, regenerate `values.schema.json`, run focused
generic request/action tests, Helm unit tests and lint, and verify that neither
the defaults nor generated schema defines `resourceActions.authorityWorker`.
Verify the request registry has none of the four private names, the default
encoder/decoder is used for their old request-name strings, and ordinary queue
SQL has no special handler exclusion. Verify the final chart rejects both
disabled and enabled persisted authority values, and the released plugin API
accepts only the inert `GENERAL` claim scope. Merge only after the compatibility
evidence and values scrub are recorded. Deploy the exact final digest to API,
controller, and executor, then repeat every readiness, +10-minute, and
+30-minute query and health check above. In production, also pass the comparable
+60-minute issue-#1349 gate before declaring R0 complete.

R2 rollout first deploys revisions API009 and Serve042 with every controller on
durable mode `legacy`. Mixed fleets advertise false until every recent API
acceptor, ordinary executor, GC process, and possible service-controller owner
supports atomic bind-and-enqueue, local bound-handler claim filtering,
retention pins, and owner/effect fences, and incapable leases pass the
quiescence window. Promotion transactionally changes one eligible non-pool
service to `bound`. Rollback drains and projects its rows with capable binaries,
then performs the fenced demotion before any old image can become ready. The
additive PostgreSQL schemas are not downgraded. An old API cannot serve the
private endpoint; an old executor does not advertise the distinct handler and
leaves its queue row unclaimed. The R3 removal remains draft until this mixed-
version and rollback sequence,
the exact crash matrix, and the monitoring window below have passed.

R2 completion, if authorized, requires all of the following in tests and the
approved canary:

- zero duplicate API requests or service-job submissions for a promoted
  replica record and launch generation;
- identical submission-key retry after a lost acknowledgement returns the
  exact committed request, while a digest conflict returns no new request;
- an incapable executor leaves the distinct bound handler queued and never
  claims or terminalizes it;
- zero launch-handler invocation after a stale claim or failed association
  fence;
- zero cancellation or deletion caused by ordinary controller replacement and
  zero cancellation or deletion of a successor replica record;
- zero eligible launches using restart inference after promotion;
- exact handoff and adoption after controller restart while queued, claimed,
  and inside the existing launch/provider call;
- no successor after `PROVIDER_IO`, or after `SERVICE_JOB_IO` without an exact
  recorded/projectable outcome, regardless of terminal/quiescence state;
- no request collection while its retention pin is active, and normal
  collection after exact projection releases it;
- teardown commits cancel intent and proves exact-request quiescence before
  replica deletion or replacement;
- terminal/quiescence publication racing fenced cancel or projection completes
  without request/association lock inversion and preserves one outcome;
- terminal projection within two controller polls after the API result;
- no ambiguous binding older than two configured retry intervals without an
  alert; and
- bounded p99 request dispatch and reconciliation latency relative to the
  pre-change baseline.

## Open gates

- [x] Complete and merge the compatibility R0 cleanup.
- [x] Deploy and monitor the compatibility cleanup on `boltz-test`; the
  all-status zero private-handler gate passed at readiness, +10 minutes, and
  +30 minutes.
- [x] Scrub the sole `skypilot` chart release in `boltz-test` of its stored
  legacy Helm value and record the sanitized stored values.
- [x] Complete the production compatibility-artifact monitoring: readiness at
  03:38:22 UTC, +10 at 03:49:03 UTC, and +30 at 04:08:49 UTC passed on
  2026-08-08.
- [x] Diagnose the production `boltz-l4-fleet` HA role-heartbeat timeout trend.
  Persisted revision-366 access logs prove the same role stalls and sync 503s
  predated #1340; exact code comparison excludes the cleanup paths. Issue #1349
  tracks the separate serialized-Kubernetes-read latency defect.
- [x] Require exact-head CI and merge PR #1346. The exact head passed 32/32, but
  the merge occurred before the preceding production gate and is recorded as a
  process-contract departure above.
- [x] Merge the canonical-design follow-up that records the corrected retired
  quarantine contract, scoped deployment evidence, and sequencing departure;
  PR #1350 merged as `0407c5a7daf65a375c55275b5ff4224f4dfc5154`
  before production promotion.
- [x] Complete PR #1346's `boltz-test` monitoring. Readiness, +10, and +30
  passed, and physical capacity returned to the 10-node / 80-vCPU baseline.
- [ ] Merge this canonical follow-up after it records the complete production
  monitor and the third deployment-sequencing departure.
- [x] Deploy PR #1355's published immutable artifact with zero incremental
  provider capacity. Production revision 370 preserved every safety and state
  invariant but failed the monotonic zero-new-timeout gate immediately.
- [x] Merge PR #1360, qualify its exact immutable artifact on `boltz-test`, and
  deploy it to production with zero incremental provider capacity. Test
  revision 98 passed its fresh readiness/+10/+30 window; production revision
  371 preserved every safety and state invariant but failed the monotonic
  zero-new-timeout gate by its first post-T0 sample.
- [x] Merge PR #1362 and qualify its exact immutable artifact on `boltz-test`.
  Production revision 372 preserved every safety and state invariant but failed
  its +10 intended-behavior gate with 21 new timeouts per large-fleet slot.
- [x] Merge PRs #1367, #1368, #1369, and #1370; qualify their combined exact
  `1.1.1176` artifact on `boltz-test`; and deploy it to production on unchanged
  capacity. Revision 374 passed readiness/+10 but failed +30 with five new
  timeouts per slot and a 32.90-second shared owner-validation read.
- [ ] Pass exact-head CI and merge the bounded shared-snapshot deadline fix on
  top of merged #1380 and #1373. Qualify the resulting exact
  #1380+#1373+#1374 immutable pin tuple on `boltz-test` through fresh
  readiness/+10/+30 without On-Demand capacity, then deploy the same artifact
  to production with `--reuse-values` on the fixed three-node capacity and pass
  fresh readiness/+10/+30/+60 gates, including zero `client_timeout` delta,
  recovery at most 15 seconds, and the exact issue-#1349 comparator.
- [x] Complete PR #1346's production monitoring. Revision 369 deployed the
  exact artifact and passed its readiness, +10, +30 safety/state, sync-rate,
  and 60-second recovery-safety gates. Its exact +60 comparison failed at 64
  role gaps versus the allowed 24, so the completed result holds R0 open and
  requires PR #1355's fresh passing window.
- [x] Record R1 ownership and its telemetry-first disposition: issue #1352 owns
  an existing-executor durable binding; it is independent of R0 and must not
  revive the authority stack.
- [x] Record the 2026-08-11 explicit localized correctness mandate. R2
  engineering no longer depends on observing a minimum traffic volume.
- [ ] Merge and deploy the revision-041 R1 telemetry change, then verify its
  closed event kinds, database-clock timestamps, 60-day retention, diagnostic
  failure isolation, and summary counters before enabling any R2 write.
- [ ] Keep every service in durable `legacy` mode until atomic bind-and-
  enqueue, stable retry, adoption, owner-epoch/provider/service-job fences,
  retention pin, local-handler claim, mixed-version/demotion, teardown-order,
  and crash-matrix tests pass.
- [ ] Keep the stacked legacy-fallback removal blocked until the promoted R2
  artifact passes its exact canary and monitoring gates.
- [x] Record the final production rollout's exact zero-incremental-capacity
  bound; the worst-case API plus 16-LB surge fits either existing non-API node.
- [ ] R2 only: obtain named capacity approval before any positive launch/down
  crash canary or any rollout that invalidates the zero-capacity bound.

Until the bounded shared-snapshot deadline artifact passes its production
monitor and this stacked canonical follow-up merges, the dedicated
authority-stack retirement is not production-complete. The bounded
request-binding follow-up remains independently incomplete until issue #1352
satisfies the R1/R2 evidence above.
