# Durable SkyServe Replica Actions

Last updated: 2026-08-16

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

The localized follow-up is now authored as a stack through R3. R1 adds only
diagnostic evidence, R2 adds the bounded binding/adoption machinery while
leaving all services in `legacy`, and this R3 change makes that machinery
mandatory before the first controller child is spawned for a fresh eligible
central-PostgreSQL non-pool service. R3 does not change schema defaults,
migrate an existing service, promote a recovery, or change pool/local/pre-042
behavior. R4, the already-planned legacy-fallback removal, remains blocked on
the rollout and migration gates below. None of R1--R3 is treated as deployed
merely because its stacked source is authored.

The 2026-08-15 mixed-version incident materially changes the accepted steady
state. An old executor claimed launch requests, ran real provider effects, and
later left terminal request state without the newer execution-quiescence
receipt. Consequently, `execution_generation = 0`, a missing PID or process
entrypoint, and `execution_quiescence_required = false` are not proof that no
effect occurred. The earlier ordinary-only scope and R4 cleanup are superseded
by the generalized non-pool launch-binding stack below. The existing R1--R3
work remains useful transition machinery, but it is not the long-term boundary
and must not be deployed as though excluded launch profiles can remain
permanently unbound.

As of 2026-08-16, G1 is merged as PR #1498. It was present in the v1.1.1296
revision-401 artifact, but a concurrent Terragrunt/Terraform apply created Helm
revision 402 and regressed the runtime to v1.1.1287 while PostgreSQL correctly
remained forward at
API-request 011/Serve047. The current runtime is therefore not capable of the
G1 legacy-reconciliation operation; no legacy evidence or service authority
write may be attempted until one exact G1-capable cohort is restored. EKS
audit records attribute revision 402 to Terraform's Helm provider; the
checked-in platform runtime pin, not a direct Helm mutation, is the durable
deployment authority. G1 adds
the generalized non-pool association/admission path, exact legacy-evidence
ledger, bounded current-protocol provider-evidence reconciler, pointerless
pre-admission retirement, failure-isolated startup recovery, and exact
reserved-fill provider-absence projection. The absence path requires a fresh
physical-UID read after exact request quiescence, revalidates the same request,
profile, provider payload/digest, service owner, and replica record under row
locks, then atomically projects the failed replica, clears its association
pointer, settles the association, releases its request pin, and verifies that
no paid-capacity claim exists. Local evidence currently includes the focused
source contracts, all 53 real-PostgreSQL Serve047 migration/binding/reducer
tests, real-PostgreSQL API011 atomic admission/quiescence and rollback/projection
tests, `git diff --check`, Python compilation, repository-wide mypy over 937
source files, changed-file pylint, and dashboard lint/format. CI and merge are
complete; additive rollout, exact retained-row reconciliation, and stacked
cleanup gates remain open.

The fresh pre-migration inventory supersedes the historical seven-row recovery
scope. Replica IDs 52032--52038 are absent and must not be recreated or treated
as quiescent. The current service has 64 nonterminal current-version rows: 46
`READY`, 15 recent zero-cost `PENDING` intents, and three zero-cost
`PROVISIONING` rows. Replica 52689 is the current global legacy-recovery
blocker: its A100 provider cluster is present, its request is cancelled after
lease expiry, and it lacks exact execution quiescence. The exact old API Pod
UID is absent, which can support a reviewed executor-termination attestation
but cannot be written as a synthetic request receipt. G1 must seal only this
retained identity, record `LEGACY_EFFECT_AMBIGUOUS`, reconcile the exact
provider effect, and project only after a new physical-UID-fenced `ABSENT`
observation starts after the termination evidence. Replica 52688 has a present
provider cluster and a succeeded/quiesced request. Replica 52690 has a failed,
quiesced request and a PHX Kubernetes admission failure caused by a missing
server-owned Kueue queue label. Those rows remain ordinary typed-recovery work;
the label defect is owned by the workspace/provider admission configuration,
not by this action protocol.

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

At that time, the stack started with the diagnostic-only R1 change below,
followed by R2's bounded machinery and R3's mandatory fresh-service adoption.
Binding activation in production and R4 legacy-path removal remain subject to their
historical compatibility, crash-matrix, migration, and rollout gates. The next
section supersedes R4.

### 2026-08-15 generalized non-pool correction

The canonical steady state is one typed execution binding for every central-
PostgreSQL, non-pool SkyServe launch. It extends the existing Serve042
association in place; it does not add a second association table, request
queue, executor, provider renderer, capacity scanner, or mutation worker.
`ORDINARY_PAID`, `ORDINARY_ZERO_COST`, `RESERVED_FILL`,
`UNKNOWN_CAPACITY_REPLACEMENT`, `COST_REBALANCE`, and
`SYSTEM_OOM_RECOVERY` are closed versioned profiles on that one binding.
Pools remain outside it because they have a different lifecycle and no
inference endpoint.

Profile planners retain all domain authority. In particular, reserved-fill
broker generations, zero-cost admission sequencing, exact pool/card grants,
worker-projection digests, and reclaim-policy tickets remain mandatory and
fail closed. The generalized binding owns only the common effect envelope:
exact request identity, planner-owned intent commit followed by atomic
association/request/queue/pin binding,
execution generation and effect phase, owner transfer, adoption, cancellation,
terminal result, quiescence, and typed reconciliation. Existing `sky.launch`
and provider paths remain the only effect implementation.

The incident correction is evidence preserving:

- the old mixed-version executor did perform real effects; it did not merely
  fabricate a terminal row;
- no migration, reducer, repair script, or operator may fabricate a
  quiescence receipt, backfill a fake claim, or mutate terminal state to claim
  the old execution was safe;
- a legacy or mixed-version row without exact current-protocol proof is
  `LEGACY_EFFECT_AMBIGUOUS` until exact request result plus provider readback
  proves its disposition; and
- only a request admitted and observed entirely by the exact current handler,
  capability cohort, and receipt protocol may use generation zero plus no
  claim and `NOT_STARTED` as `PRE_EFFECT_TERMINAL` evidence.

The immediate cancel-then-rediscover correction is a bounded mitigation. It
must preserve the may-have-effect classification and cannot become a second
long-term recovery path.

### 2026-08-16 executor-retirement correction

The retained-row investigation exposed a second, independent lifecycle risk.
In the HA chart, Kubernetes starts the Pod termination-grace countdown before
running `preStop`, while `preStop` spends `readinessDrainSeconds` only touching
the readiness marker and sleeping. The application still receives
`SKYPILOT_GRACE_PERIOD_SECONDS` equal to the complete Pod grace period. It can
therefore budget work past the real SIGKILL deadline.

EKS audit and PostgreSQL evidence prove that this timing defect caused replica
52689's missing receipt. Request `e8522a85-33da-4c7c-b5bb-f9dfce503d68` was
created at 13:51:14.086 UTC. The ReplicaSet controller deleted exact Pod
`skypilot-api-server-57d7dd9584-mqglt`, UID
`c74d8735-f5f9-4e9a-8bd1-19e69f8b68ea`, at 13:51:15.759 UTC (audit ID
`877cb2bc-db4b-405d-b9ff-23121728c40b`). The rendered Pod had a 60-second
termination grace and a 20-second readiness `preStop`, while the application
was incorrectly given the full 60 seconds. Kubernetes recorded the API
container terminated with exit 137 at 13:52:16 UTC (audit ID
`50a76fdf-882f-4941-9667-e9c83f8ff8ec`), and the kubelet finalized the exact
Pod with grace zero at 13:52:16.990 UTC. Only afterward did the request lease
expire at 13:52:36.525 UTC and the request become `CANCELLED` at 13:53:01.138
UTC without an execution-quiescence receipt. This is a rollout retirement
failure, not provider scarcity.

The steady-state correction makes retirement an application-visible protocol,
not an incidental signal sequence:

1. the existing pod-local drain marker is the single early retirement input;
   each API, executor, and controller runtime watches it, durably marks its
   exact instance lease draining, stops new claims, and starts child/receipt
   convergence before the readiness-propagation sleep;
2. Helm exposes and validates disjoint readiness-propagation, execution-drain,
   and final-commit margins. The application receives only its real remaining
   budget, and chart rendering fails unless their sum fits inside
   `terminationGracePeriodSeconds`;
3. a graceful owner exits only after every exact claim and invocation warden it
   owns has either published its real terminal/quiescence receipt or remains
   durably classified as effect-ambiguous. A timeout never fabricates
   quiescence;
4. abnormal death uses a separate typed `EXECUTOR_TERMINATED` certificate. It
   is not a request receipt and only proves that the named execution sandbox
   can no longer perform effects. Automatic issuance requires authoritative
   infrastructure evidence for the exact Kubernetes context, namespace, Pod
   UID, and container termination outcome. Lease expiry, Pod `NotFound`, a
   replacement Pod, or a missing process-map entry alone remain insufficient;
   those cases require reviewed attestation and stay fail closed; and
5. after executor termination, any request that may have crossed the effect
   boundary remains `LEGACY_EFFECT_AMBIGUOUS` or its current-protocol typed
   equivalent. Cleanup still requires a fresh physical-UID provider read that
   starts after the certificate and is revalidated under the canonical row
   locks. No successor is authorized from executor death alone.

This extends the existing request-worker/guardian and association protocol. It
does not add a second queue, executor topology, provider renderer, or recovery
authority. Per-action isolation in G2 ensures an ambiguous action cannot block
unrelated cards or services.

## Goals

- Preserve the exact request identity for every non-pool Serve launch across a
  controller restart.
- Adopt the same request after restart rather than submit an untracked second
  request.
- Keep the typed launch profile, its domain authorization, request binding,
  service identity, replica incarnation, and terminal projection mutually
  consistent.
- Preserve current launch/down ordering, provider-work limits, retry behavior,
  and public Serve semantics.
- Reuse the API request executor and existing internal launch path.
- Make ambiguity explicit and operator-visible instead of guessing success or
  absence.
- Let one poisoned or ambiguous association quarantine only its exact replica,
  reserved pool/card grant, and successor decision while unrelated probes,
  routes, autoscaling, and sibling pools continue.

## Non-goals

- A universal resource or physical-capacity state machine.
- A dedicated resource-action authority worker, private HTTPS control plane,
  execution-worker cohort, policy rotation, or special execution lease. The
  deployment capability cohort is only a mixed-version admission proof.
- Reimplementing SkyPilot launch/down through a native Kubernetes renderer.
- Moving provider credentials or provider clients into a new component.
- Replacing the existing API request queue or ordinary executor.
- Replacing pool, managed-job, paid-capacity, reserved-fill, cost-rebalance,
  system-OOM, placement, or logical-replica domain policy. Those planners feed
  the common binding but retain their own authorization and accounting.
- A shared capacity scanner, observation cache, occupancy ledger, provider
  renderer, action worker, or scheduler across domains.
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

The 2026-08-15 incident also disproved a narrower inference in the first
ordinary-binding design. An old executor can claim and execute a request
without publishing the API008/010-era process and quiescence evidence that a
new reducer expects. Cancellation can subsequently leave
`execution_generation = 0`, no PID/entrypoint, and
`execution_quiescence_required = false` even though provider effects already
ran. Those fields describe the surviving database record, not historical
effect absence. Exact provider readback, immutable result evidence, or a
current-protocol pre-effect receipt is required before retry.

## Public contract

There is no new CLI, SDK, configuration, or provider interface. Existing
Serve behavior remains backward compatible.

Every central-PostgreSQL non-pool replica launch has the following internal
contract after generalized activation:

1. A replica row has a stable record identity. The bounded implementation adds
   one neutral launch generation and association identity; it does not
   silently reinterpret nullable Serve033 action columns.
2. Before execution can escape the API request boundary, the row is
   durably bound to one exact API request ID, typed profile kind/version/digest,
   and capability-cohort epoch for that generation.
3. A restarted controller with the same row identity and generation adopts
   that request ID.
4. A controller may create a successor only after the predecessor is exact
   terminal/quiescent and its durable effect phase proves neither provider nor
   service-job I/O began. Terminal/quiescent post-effect ambiguity blocks.
5. Controller replacement transfers the association to the new
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
9. A profile-specific planner supplies immutable authorization references, and
   both admission and the terminal pre-I/O fence revalidate them. A generic
   binding never converts missing reserved-fill, paid-capacity, system-OOM, or
   replacement authority into permission.
10. Recovery classifies exact durable evidence. It never treats terminal
    status, generation zero, missing process identity, or a false quiescence-
    required bit as effect-absence proof for a legacy/mixed-version request.
11. Each profile has one adapter at the common binding boundary. The adapter
    validates any profile-owned execution envelope before queue visibility,
    revalidates it against the locked planner intent before provider I/O, and
    projects profile-owned result fields in the reducer transaction. Merely
    labeling an ordinary launch with a special profile is forbidden.

The API request remains the execution record. A second generic action DAG is
not introduced merely to wrap it.

## Architecture and invariants

### Ownership

- `ReplicaManager` owns the desired replica transition and stable replica
  record identity.
- The existing central PostgreSQL Serve042 association record owns the neutral
  non-pool launch identity, immutable typed profile, domain-authorization
  references, capability-cohort epoch, and exact request binding. Serve047
  extends this table in place; no parallel association or dual write exists.
- The existing API request queue and executor own request claim,
  execution generation, cancellation, and terminal result.
- Existing SkyPilot launch/down internals own provider selection and effects.
- Each domain planner owns its admission policy and authorization payload. The
  Serve reducer validates that payload and the row/request association before
  projecting a result.

The exact built-in PostgreSQL request backend owns the admission transaction.
It reaches the Serve association and replica tables through the same physical
database and the same SQLAlchemy connection; Serve does not duplicate request
serialization or attempt a transaction across two engines. Admission fails
closed on SQLite, plugin request or queue backends, or either schema lineage
being behind its required head. Every provider-effect-authorizing cross-table
path takes the shared service launch-authority guard, then locks lifecycle,
service, replica, association, request, queue, and retention-pin rows in that
order, omitting only unused suffixes and never inverting it. Owner transfer and
binding-mode changes take the exclusive side. Cancellation and reduction grant
no provider authority and deliberately do not wait for either advisory side;
they take the same canonical row sequence and revalidate owner epoch/revision.
Generic API terminal/quiescence writes remain request-only. Queue claim locks
only request/queue rows and performs a non-locking association validity read;
the authoritative association lock and revalidation is the later pre-I/O
fence, so claim cannot create a lock cycle or grant effect authority by itself.
Non-authorizing adoption/cancel-target snapshots also take no advisory guard
and can only feed a later canonical transaction. No component may own both an
unfenced stale replica snapshot and permission to start provider I/O.

### Commit-before-effect

The generalized implementation has one internal atomic bind-and-enqueue seam
after the existing planner-owned replica-intent commit.
A dedicated versioned `/internal/serve/non-pool-launch` endpoint accepts one
controller-generated stable submission UUID and one closed typed profile. It
does not fall back to `/launch` or the ordinary-only endpoint: either would let
an old server ignore unknown binding context and execute unbound. The
controller reuses that UUID for every transport retry. The server
deterministically derives the association and exact API request IDs from the
submission UUID plus authenticated tenant scope, independent of the fresh ID
assigned to each HTTP attempt by `RequestIDMiddleware`, and returns the exact
bound request ID in the response body.

The planner first commits its replica intent and exact domain authority using
its existing transaction: paid claim, zero-cost sequence, reserved allocation,
replacement observation, rebalance decision, or recovery intent. That row is
not effect authority. In a second transaction the server locks the lifecycle
fence, service row, exact replica row, and current association; revalidates the
profile planner's exact authorization; constructs the complete bound request
with one distinct generic handler on the normal executor topology; and inserts
the association, `api_requests` row, generic request-retention pin, and
`api_request_queue` row. The transaction also sets the replica row's exact
association pointer. Queue visibility occurs only at transaction commit, after
every effect fence and binding is durable.

Serve047 adds one nullable `replicas.non_pool_launch_authorization` JSONB
scalar for planner evidence that was previously process-local. It is populated
only by the typed initial replica-intent insert, omitted by generic
ReplicaInfo/status writers, and immutable after that insert. Unknown-capacity
replacement records the exact predecessor record/version, stable service
hash/lifecycle/binding identity, authoritative reconcile generation, `UNKNOWN`
classification, and complete logical target/card shapes. Cost rebalance
records the exact predecessor record/version and the same stable service
identity; its target decision remains in the existing fenced service-level
stabilization state. The envelope deliberately excludes controller
incarnation/owner epoch: those are transferable association ownership, and an
immutable planner intent must remain adoptable across a legitimate controller
handoff. A service recreation or binding transition still invalidates the
stable identity.
Admission and pre-I/O require the exact predecessor still to exist and
recompute the profile from both rows. This scalar is planner intent evidence,
not an association, receipt, provider fact, or retry authority.

That transaction also runs the selected profile adapter. Most profiles have no
additional execution payload. `SYSTEM_OOM_RECOVERY/v1` is different: the
adapter validates the complete unbound recovery envelope against the locked
recovery intent, replaces its one-use nonce with the server-derived request
ID, and stores that bound envelope only in the normal immutable `LaunchBody`.
The association stores the canonical profile/authorization digests, not a
second payload copy. At the pre-I/O boundary the adapter reconstructs the
expected bound envelope from the still-locked recovery intent and exact
request ID and requires exact field equality. Thus the generic path cannot
silently execute a recovery candidate as an ordinary launch.

This deliberate two-commit protocol keeps the request executor out of all six
planner transactions without weakening commit-before-effect. A crash after the
intent commit but before binding leaves a pointerless `PENDING` intent from
which no request or provider effect could have escaped. Per-service promotion
requires zero `PENDING`/`PROVISIONING` rows, so a pointerless pending row under
an active generic capability is provably post-cutover rather than historical;
startup locks the lifecycle, service, replica, and association history and
atomically retires that pre-admission planner intent (including its paid claim,
if any). The current demand, fill, rebalance, replacement, or recovery planner
then makes a fresh decision. Startup must not reconstruct a retained profile
from a subset of fields or run provider cleanup. If concurrent admission won
the row lock first, startup instead finds and adopts the exact association. A
crash after binding likewise finds the exact association and adopts that
request. A lost response retries the same submission UUID. The API first
derives the deterministic association identity and, when it already exists,
uses its immutable stored profile rather than re-resolving mutable planner
observations. The admission transaction still locks and exact-matches the
complete association/request bytes; only a first admission recomputes live
planner authority. The shared pre-provider guard independently recomputes live
authority before any external effect. Thus an exact identity and digest return
the existing request after a lost acknowledgement, while any mismatch fails
closed. There is no second executor, fallback launch, or provider-discovery
inference.

The same one-intent/one-action rule applies after an exact current-protocol
`PRE_EFFECT_TERMINAL` result. The reducer clears the pointer and releases the
request pin and paid claim; the controller atomically retires the now-proven
effect-free planner row. The owning planner then creates a fresh record and
fresh authorization decision. It does not reuse the old record, reconstruct a
special profile from selected fields, or allocate association generation + 1.
Transport retries before terminal settlement continue to reuse the original
stable submission UUID and exact request.

The canonical binding digest is computed server-side from the exact prepared
`LaunchBody` bytes after removing binding-only and mutable owner fields. The
association records that digest, but does not store another copy of the task or
provider payload. The seam does not call the public SDK recursively, create a
new execution topology, or render provider-native objects.

Central API revision 009 added an ordinary-only capability, hard-coded handler
constraint, request-to-association correlation, and generic retention-pin
table. It is not a safe generalized capability: an old executor can advertise
or understand only that ordinary profile. Forward-only API revision 011 adds
the generic handler, closed profile kind/version/digest, request capability-
cohort epoch, and exact per-profile supported-set digest; it replaces neither
historical 009 nor revision 010. Serve revision 042 already owns the neutral
association table, replica pointer, monotonic service-controller owner epoch,
and binding mode/epoch. Forward-only Serve revision 047 extends that existing
table and service state in place with the generic profile, authorization,
reconciliation, provider-evidence, and capability-cohort fields.

Bound admission requires one exact immutable cohort digest and epoch across
all API acceptors, request-backend writers, queue executors, request GC,
possible service controllers, and every profile-specific admission/executor
participant. Old ready leases and non-ready-but-recent leases must drain past
the maximum stale/quiescence window. The service, association, and request all
persist that cohort epoch. The dedicated endpoint makes an old API target fail
with no effect. Database handler/profile constraints, queue candidate
selection, and the locked claim require the generic handler and exact local
profile version; an old ordinary handler cannot claim it and a stale executor
leaves it queued. The service owner CAS persists the subprocess capability
beside its owner epoch rather than inferring it from an API supervisor lease.

The forward schema contract is:

- API011 retains API009's physical association/correlation and retention-pin
  columns. It adds generic binding protocol version, profile kind/version/
  SHA-256, and capability-cohort epoch to `api_requests`, plus an instance/lease
  capability protocol version and profile-set SHA-256. Its closed constraint
  permits the historical ordinary handler only for historical protocol-v1
  correlations and requires the new generic handler plus complete v2 fields for
  every new generic correlation.
- Serve047 retains the Serve042 association table and replica pointer. It adds
  the same protocol/profile/cohort tuple, a typed authority kind/reference/
  generation plus canonical authority digest, typed reconciliation and
  provider-evidence fields, service cohort epoch/digest, and the immutable
  initial-insert-only replica planner-authorization scalar used by replacement
  profiles.
- Existing protocol-v1 associations are never rewritten or given protocol-v2
  receipts. They must settle, project, unpin, and drain before per-service v2
  promotion. Historical unbound rows are not inserted into the association
  table. Every new field is closed by
  completeness, value, and digest-length constraints; nullable transition
  shapes are readable but have no generic effect authority.
- API012/Serve048 add the controller-independent demand feed, ordered
  zero-cost-before-paid admission, and provider-free route projections owned by
  `skyserve-demand-capacity-convergence.md`.
- API013/Serve049 are the blocked cleanup heads. They remove protocol-v1/new-
  admission compatibility and transition columns/constraints only after G2's
  gates; they preserve immutable tombstones, typed profiles, current cohort,
  route history needed by live clients, and permanent reserved authorization.

An active correlated bound request without its queue row is invariant
corruption, not an activation state. Startup locks and types the correlated
evidence. Only when the request was admitted by the exact current protocol and
cohort may execution generation zero, no claim/lease, and `NOT_STARTED` become
`PRE_EFFECT_TERMINAL`. A legacy or mixed-version row with the same surviving
field values is `LEGACY_EFFECT_AMBIGUOUS`; it is never retroactively given a
receipt. A claimed `PENDING` or `WAITING` row between queue
handoff and `RUNNING` publication remains active when its exact token, worker,
generation, queue delivery, and live lease agree. Correlated bound rows are
excluded from the generic queue lease reaper: only the association-aware
reducer may interpret their expiry. An exact expired owner generation may be
terminalized and marked quiescent atomically only while the association still
proves `NOT_STARTED`, because the expired claim can no longer acquire the
provider fence. This also closes an already-terminal, queue-deleted exact
generation while retaining its token/worker evidence for a late idempotent
owner acknowledgement. The same expiry at `PROVIDER_IO` or later becomes
durably `AMBIGUOUS`. Lease expiry alone never proves that post-effect executor
code stopped, and no generic timeout sweep synthesizes that proof. Startup
never synthesizes execution or infers a successor.

The transaction compares at least:

- service name and service version;
- replica ID and immutable replica record ID;
- the non-pool launch association identity and server-selected generation;
- immutable profile kind, profile version, canonical profile digest, domain-
  authorization references, and capability-cohort epoch;
- a server-recomputed digest of the canonical prepared `LaunchBody`, excluding
  binding-only and mutable owner fields and never using a diagnostic raw-YAML
  or `repr` fallback;
- initial controller owner and association-owner revision; and
- expected absence of a conflicting nonterminal binding.

### Durable association, typed reconciliation, and per-service cutover

The association contains immutable association/submission UUIDs, service
name/hash/workspace, lifecycle and binding epochs, service version, replica ID and
`replica_record_id`, server-selected launch generation, cluster name, exact API
request ID, profile kind/version/digest, exact domain-authorization references,
capability-cohort epoch, and canonical launch digest/version. The physical
Serve042 table and replica pointer may retain their historical `ordinary`
names for forward compatibility, but their only live meaning after Serve047 is
the generalized non-pool contract. Uniqueness covers submission UUID,
association UUID, request ID, and
`(service_name, replica_record_id, launch_generation)`, with at most one
unsettled association for a replica record. The replica row has a nullable
`ordinary_launch_association_id`; generation allocation and pointer update occur
under its lock. No existing system-recovery, Serve033 action, or `ReplicaInfo`
request/job field is reinterpreted.

The profile envelope is closed and minimal. It stores typed references and a
canonical digest, not a duplicate planner payload:

| Profile | Immutable authorization references |
|---|---|
| `ORDINARY_PAID/v1` | Exact paid-capacity pool/claim identity and generation plus selected placement digest. |
| `ORDINARY_ZERO_COST/v1` | Exact zero-cost placement identity and database-assigned admission sequence. |
| `RESERVED_FILL/v1` | Gate, allocation, claim, and observation generations; exact pool/physical-UID/card; intent key; zero-cost admission sequence; committed service version; worker-projection digest; reclaim-policy identity and ticket reference. |
| `UNKNOWN_CAPACITY_REPLACEMENT/v1` | Immutable replica planner authorization: exact predecessor record/version, stable service hash/lifecycle/binding identity, reconcile generation, `UNKNOWN` classification and complete logical target; plus new-placement digest and current paid/zero-cost authority. |
| `COST_REBALANCE/v1` | Immutable replica planner authorization: exact predecessor record/version and stable service hash/lifecycle/binding identity; plus the current service-level stabilization decision/target-placement digest and current paid/zero-cost authority. |
| `SYSTEM_OOM_RECOVERY/v1` | Exact recovery-intent launch generation and nonce, authorization/runtime/task/image/envelope identities, selected placement/funding, and candidate disposition. Mutable recovery-row revision and request/result fields are excluded. |

The authoritative domain fields remain in the locked service, replica,
allocation, claim, projection, and recovery rows. Admission and pre-I/O resolve
these references, recompute the profile digest, and fail closed if a referenced
row is missing, stale, partial, or inconsistent.

Mutable fields are current controller-owner incarnation/epoch,
association-owner revision, effect phase, request terminal status/cause and
quiesced generation, optional exact service-job ID, typed reconciliation and
provider-evidence outcomes, canonical evidence payload/digest, projection
state, and database-clock timestamps.
Effect phases are `NOT_STARTED`, `PROVIDER_IO`, `SERVICE_JOB_IO`, and
`SERVICE_JOB_RECORDED`. Reconciliation outcomes are `ACTIVE_ADOPT`,
`RESULT_RECORDED`, `PROJECTED`, `PRE_EFFECT_TERMINAL`,
`POST_EFFECT_AMBIGUOUS`, and `LEGACY_EFFECT_AMBIGUOUS`. Provider evidence is
independently closed as `NOT_QUERIED`, `PRESENT`, `ABSENT`, `UNKNOWN`, or
`REPLACED`; timeout, partial enumeration, RBAC denial, malformed identity, or
an unrecognized same-name resource is `UNKNOWN`, never `ABSENT`.
Identity/digest fields never change, and unresolved history cannot be deleted.

| Reconciliation outcome | Required evidence | Unsettled / pinned | Exit |
|---|---|---|---|
| `ACTIVE_ADOPT` | Exact current-protocol request, claim/generation/lease, profile, cohort, and association agree | yes | adoption, result reduction, fenced cancel, or ambiguity |
| cancel requested | Current owner committed exact supersede/teardown intent; cancellation is not absence evidence | yes | typed terminal/quiescence/provider reconciliation |
| `RESULT_RECORDED` | Exact terminal/quiescence and service-job ID copied after `SERVICE_JOB_RECORDED` | yes | atomic replica projection |
| `PRE_EFFECT_TERMINAL` | Exact current-protocol cohort proves no claim/effect and terminal/quiescence is copied while effect remained `NOT_STARTED` | no | retire the effect-free planner row and let its owning planner create a fresh record/authorization |
| `PROJECTED` | Exact result/tombstone projected, pointer cleared, pin deleted | no | 60-day tombstone retention |
| `POST_EFFECT_AMBIGUOUS` | Current protocol crossed or may have crossed an effect boundary without an exact projectable result | yes | exact provider/result evidence and profile-specific operator reconciliation |
| `LEGACY_EFFECT_AMBIGUOUS` | Legacy/mixed-version execution may have caused effects but lacks the current receipt contract | yes | exact provider/result evidence only; never synthesized quiescence |

The partial unique constraint treats active, cancel-requested,
`RESULT_RECORDED`, `POST_EFFECT_AMBIGUOUS`, and
`LEGACY_EFFECT_AMBIGUOUS` rows as unsettled. A successor transaction uses a
fresh replica-record identity after the effect-free predecessor planner row is
retired. Association generation remains an immutable historical field; the
generalized controller never uses same-record generation + 1 as a second retry
path.

Startup and ordinary reconciliation schedule one bounded task per association;
they do not hold a global recovery lock, manager lock, or actuation lock across
provider I/O, network waits, sleeps, or polling. One ambiguous row quarantines
only that exact row and, for reserved fill, its exact pool/card/grant. Probers,
route publication, the reconciliation coordinator, autoscaling, and sibling
rows start immediately and continue. The legacy cluster-name scan is retained
during transition only for already-durable unbound rows and may gather typed
provider evidence; it never synthesizes a binding or authorizes a successor.
Provider absence used for legacy cleanup always comes from a fresh physical-
cluster read started after the reviewed executor-termination attestation; the
normal teardown snapshot cache is explicitly bypassed for this evidence.
Current-protocol provider reconciliation is likewise two-sided: the exact
request generation must be terminal and quiescent before the fresh provider
read starts, and the evidence commit re-locks the association/request and
requires that same quiescence and current controller owner after the read.
This prevents an active executor from creating a resource after an `ABSENT`
observation but before it publishes quiescence.

Serve047 also adds one transition-only append-only legacy-reconciliation
evidence table. It is not an association, request-admission path, queue, or
source of launch authority. A row names the exact service, replica record,
cluster name, request, Kubernetes context, and physical-cluster UID; preserves
the observed request facts; records an explicit old-executor termination
attestation and its digest; and then records a provider observation made after
that attestation. Only the monotonic
`LEGACY_EFFECT_AMBIGUOUS -> CLEANUP_AUTHORIZED -> PROJECTED` reduction is
allowed. `CLEANUP_AUTHORIZED` requires exact provider `ABSENT` observed after
the attested executor termination. It permits only exact UID-fenced cleanup of
that legacy replica and never supplies a receipt, association, retry, or
successor-launch proof. G2 removes the writer and active transition surface
after the zero-legacy gate while retaining its audit tombstones.

The service row has a non-null controller-incarnation UUID, monotonic
`controller_owner_epoch`, ordinary capability, a complete generic protocol /
profile-set / cohort / receipt capability tuple bound to that exact
incarnation by a separately constrained capable-controller-incarnation UUID,
`ordinary_launch_binding_mode` (`legacy` or `bound`), and a
monotonic binding epoch. Every controller subprocess startup supplies a fresh
incarnation UUID;
the owner CAS changes it and increments the epoch even when PID/IP are reused.
Serve042 migration rows and existing services default to `legacy`. Fresh R3
services also insert as `legacy`; after claiming a fresh capable controller
incarnation, but before spawning its child, an eligible central-PostgreSQL
non-pool service must complete the existing explicit promotion transaction and
refresh the exact committed authority. That transaction requires the full
participant/quiescence barrier and zero legacy nonterminal ordinary requests or
PENDING/PROVISIONING replica rows. Any promotion or exact mode/epoch refresh
failure aborts startup before child creation. Recovery preserves the persisted
mode, and pools plus stores without a capable Serve042 authority remain outside
automatic promotion. A fenced rollback demotion to `legacy` is permitted only
after every bound association is terminal,
quiescent, copied, projected and unpinned and no launch generation is active;
it increments the binding epoch. An incapable controller can never claim a
service while its mode is `bound`.

Serve047 extends, rather than bypasses, the Serve042 database guard for that
single epoch. In addition to Serve042's `legacy <-> bound` transitions, the
guard permits exactly one `bound -> bound` epoch advance when the same
controller incarnation atomically changes the complete generic non-pool
capability tuple. This is the per-service generic promotion/demotion fence;
an epoch-only update, a partial capability tuple, a non-adjacent advance, or a
combined controller handoff and capability-epoch transition fails in the
database. No second capability epoch is introduced as a competing ownership
clock.

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

Immediately before provider I/O, the generic non-pool executor handler
must revalidate:

- its live request claim and execution generation;
- the locked claim still resolves the distinct locally supported bound handler;
- its exact profile kind/version/digest and capability-cohort epoch;
- the service and exact replica row still exist;
- the replica pointer and association still name this record, generation, and
  request ID;
- the submitted input digest matches the durable binding;
- the profile planner's exact paid, zero-cost, reserved-fill, replacement,
  rebalance, or system-OOM authorization remains current; and
- the association owner/revision matches the current durable service-controller
  owner/revision.

A failed check terminates without provider I/O. Under the existing shared
service launch-authority guard, the backend repeats validation and atomically
advances `NOT_STARTED` to `PROVIDER_IO` immediately before provider work. The
fenced provider tail includes every `Storage.construct()` call, since bucket
creation/checks and source synchronization are externally effectful. No bound
storage construction occurs in the pre-effect policy/optimization prefix. The
service-job boundary revalidates the same tuple, advances to `SERVICE_JOB_IO`
before its call, then records `SERVICE_JOB_RECORDED` plus the exact returned job
ID. A crash in either interval is conservative may-have-submitted ambiguity. A
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
quiescence by locking the request, queue, and retention pin after the
association. It copies that evidence,
updates replica status, marks the association `PROJECTED`, and releases the pin
in one transaction on both ordinary and paid-capacity completion paths. The
reducer relies on those canonical row locks rather than the exclusive provider
advisory guard. This is safe because its only expiry writes either settle an
exact `NOT_STARTED` generation whose expired lease can no longer enter the
provider guard or mark an advanced phase `POST_EFFECT_AMBIGUOUS`; projection additionally
requires the executor's exact-generation quiescence receipt, which is emitted
only after its provider guard has exited. The same rows serialize controller
owner transfer and effect-phase advance.

The reducer invokes the same closed profile adapter before clearing the
association pointer. For `SYSTEM_OOM_RECOVERY/v1`, an exact recorded service
job atomically projects the association request ID and service-job ID into the
existing recovery fields and advances their revision once; a pre-effect
terminal result projects neither. Any profile/envelope/intent mismatch rolls
back status, paid feedback, pointer clearing, pin release, and recovery-field
projection together.

Failure and retry policy use the database clock. Terminal plus quiescent does
not prove effect absence, and a false `execution_quiescence_required` value is
not itself quiescence. Automatic re-planning is allowed only after
`PRE_EFFECT_TERMINAL` with effect phase `NOT_STARTED` and exact current-protocol
cohort evidence proving neither provider nor service-job I/O began, followed by
atomic retirement of that effect-free planner row. Any
terminal result after `PROVIDER_IO` without an exact projectable service-job
outcome, any `SERVICE_JOB_IO` crash, unclear request/result, fence rejection,
or cancellation race is `POST_EFFECT_AMBIGUOUS`. A legacy/mixed-version row
without the current receipt is `LEGACY_EFFECT_AMBIGUOUS` even when its stored
generation is zero. Both block automatic resubmission. Exact provider presence,
absence, replacement-incarnation, or result evidence may settle a typed row;
the design does not claim cross-provider absence from generic request fields.

### Cleanup

The existing durable cleanup intent remains authoritative. This project does
not need a second cleanup action graph. Teardown or supersession finds every
association through a non-authorizing exact record snapshot. It first commits
owner-epoch/revision-fenced cancel intent for all targets in one complete pass,
without an advisory guard, so one stuck provider cannot make cancellation of a
peer unreachable. A second pass drives each canonical row-lock reducer to
projection, safe pre-effect settlement, or durable ambiguity, then the
transition-only generic barrier covers pre-existing unbound requests. Only
after those proofs may teardown
take exclusive owner authority and delete or replace replica rows. Ordinary
`remove_replica(s)` cannot race a bound pre-I/O check with a direct ORM delete.
Association history is retained so a same-number successor record cannot
inherit, cancel, or project predecessor work. Fresh non-pool requests never
enter that legacy barrier after generalized activation.

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

### R2: bounded ordinary binding and adoption (historical foundation)

R2 describes the already-authored ordinary-only foundation. G1 below
supersedes its profile exclusion and reuses its association, transaction,
retention, handoff, and fencing mechanics in place.

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

The R2 exclusion of system-OOM, reserved fill, zero-cost placement, unknown-
capacity replacement, and cost rebalance was a transition boundary, not a
valid steady state. G1 converts each to a closed profile on the same
association. Each profile names the exact replica ID, canonical
`replica_record_id`, generation, service version, and planner-owned authority.
Admission and every provider, service-job, cancellation, and projection
boundary decode and revalidate the complete profile. Unknown, partial, stale,
mismatched, or caller-authored profile claims fail closed. Pools retain their
separate authority and do not use the non-pool association.

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

The R2 implementation keeps every service in `legacy` mode by default and
exposes no public SDK or CLI switch. The only transition surface is a hidden,
administrator-authenticated API operation carrying the exact service hash and
source binding epoch. It forwards to the exact owner-protected controller
endpoint, which holds the
controller actuation lock and the manager admission lock while one PostgreSQL
transaction advances the binding epoch. Promotion pairs that transaction with
the service's existing launch-authority advisory lock; legacy request
admission takes the shared side before request/queue insertion. This closes
the admission phantom without global request-table locks or a queue-claim lock
upgrade cycle. Promotion and demotion retries accept only the immediately
adjacent epoch under the same controller incarnation. A controller that
already installed that exact target returns the committed epoch without
rerunning barriers, so a lost response is idempotent but an epoch ABA fails
closed.

The exact reducer validates the complete successful `(service_job_id,
CloudVmRayResourceHandle)` result and cluster identity before projection. A
non-cancelled `PRE_EFFECT_TERMINAL` keeps the replica pending and retains any
exact paid-capacity claim for generation `N+1`; teardown or supersession
cancellation releases the claim and cannot retry. An exact association pointer
keeps the paid-capacity claim live through the replica's transient
`INTERRUPTED` teardown state until projection releases it, so another service
sharing the pool cannot consume that capacity early. Service teardown first
publishes `SHUTTING_DOWN` under canonical lifecycle/service row locks and
delivers exact cancellation to every target while a provider retry may still
hold shared authority. It then reduces/projects under the old exact owner and
runs the transition-only legacy-unbound quiescence barrier before taking
exclusive authority and claiming a fresh restricted teardown incarnation. Dead-child
respawn and ordinary HA recovery try that exclusive ownership nonblockingly, so
they cannot occupy the only process that can observe a later teardown.
`FAILED_CLEANUP` recovery uses the post-fence `SHUTTING_DOWN` status in that
ownership transfer. This also
covers teardown recovery after the serving controller subprocess has
disappeared. Request-owned GC selects and deletes only settled, aged, unpinned,
unreferenced association tombstones in one transaction; unresolved or
ambiguous evidence remains durable.

Cancellation is absorbing. A first transaction durably records the association
cancel intent and immutable reason; idempotent redelivery then terminalizes the
request and removes its queue row in a separate request transaction. Once the
association has durably entered
`CANCEL_REQUESTED`, its reason and timestamp are immutable, a cancelled
`PRE_EFFECT_TERMINAL` predecessor cannot allocate generation `N+1`, and generic
request cancellation skips every request carrying an ordinary association.
The association cancel-intent transaction locks the canonical lifecycle,
service, replica, and association rows directly; it does not wait behind the
shared provider-authority advisory guard. The request cancellation transaction
then locks the exact request and queue rows. A crash between them is recovered
by redelivering the durable reason, never by inventing a replacement reason.
The row-lock reducer similarly never waits for that advisory guard: it cannot
authorize cleanup from an unquiesced post-effect request, but it can durably
expose ambiguity instead of deadlocking behind the opaque call.

A live controller retries an unresolved transport admission with the same
stable submission UUID while its worker still owns the retained intent. After a
process restart, only an association is an adoptable action identity; a
pointerless pre-admission intent is transactionally retired and replanned. A
persisted `SHUTTING_DOWN` race re-enters exact settlement and teardown with its
saved scale-down, purge, and drain-cap fields. Restart adoption freezes the
replica's persisted service version, retires superseded rows, and refuses a
newly elected version at both admission and effect boundaries. Finally, the
parent marks a started bound worker `RUNNING` only through a locked compare-and-
update that requires the exact active association and still-`SCHEDULED` row; a
faster child projection always wins.

### R3: mandatory fresh-service adoption

Keep `add_service()` and the Serve042 migration default at `legacy`; directly
creating a bound row would bypass both the request-side participant/drain
barriers and the exact capable controller authority required by promotion.
For a fresh service only, `_start()` first claims a capable controller
incarnation. If the service is non-pool and the claim returned a Serve042
central-PostgreSQL authority, it then transactionally promotes that exact
authority, refreshes it, and requires mode `bound` at exactly the returned
adjacent binding epoch before calling the child-spawn boundary. Promotion,
barrier, epoch, or refresh failure propagates and therefore fails closed before
any child can admit a launch.

Recovery, including recovery of an existing `legacy` row, never automatically
promotes. Pools and an absent authority (the local, SQLite, and pre-042
compatibility result) also preserve their prior behavior. This phase performs
no bulk migration and exposes no new public switch. The explicit R2 transition
and fenced demotion surfaces remain available for controlled migration and
rollback of existing services.

### R4: ordinary-only fallback removal (superseded)

Deploy R1 and R2 dark/read-only validation first, then R3 and one newly created
eligible non-pool service. Remove the old resubmission inference only after the
exact merged artifact has completed the monitoring gate and every existing
eligible legacy service has been explicitly promoted or retired. The stacked
removal change makes the bound endpoint mandatory for eligible ordinary
launches, removes the branch in `_recover_legacy_replica_operations()` that
resubmits without first resolving an exact association, removes the
capability-controlled unbound submission fallback, and deletes
transition-only compatibility probes. Its proposed retention of separate
system-OOM, reserved-fill, and other non-pool launch paths is rejected by the
2026-08-15 incident correction. R4 must not merge; G2 is its replacement.

### G1: generalized non-pool transition feature

G1 is one focused transition PR, stacked above the immediate incident hotfix
and below a simultaneously authored blocked G2 cleanup PR. It:

- adds forward-only API-request revision 011 and Serve revision 047; historical
  API009/010 and Serve042--046 migrations are never edited or renumbered;
- extends the existing Serve042 association in place with the closed profile,
  authorization, cohort, reconciliation, and provider-evidence contract;
- replaces the ordinary-only endpoint/handler for new admissions with one
  generic non-pool handler whose exact version and supported-profile digest are
  constrained in PostgreSQL and in queue claim;
- retains each domain planner's authoritative replica-intent transaction, then
  atomically commits the association, request, queue row, retention pin, and
  revalidated profile authority before provider I/O;
- persists one immutable cohort digest/epoch across service, association, and
  request, and promotes a service only after every API acceptor, request
  backend, queue executor, GC participant, possible controller, and profile
  participant is capable and every older/recent lease has drained;
- schedules per-row typed reconciliation without holding manager/global locks
  across provider reads, network waits, polling, or sleeps;
- settles only an exact `RESERVED_FILL` `ABSENT` observation, recorded after
  terminal executor quiescence against the immutable Kubernetes context and
  physical cluster UID. The current owner atomically marks the replica for
  failed cleanup, clears the pointer, settles the association, releases the
  exact request pin, and proves the action has no paid-capacity claim. Every
  other profile or provider classification remains quarantined;
- retains legacy cluster-name discovery only for historical unbound rows. It
  never backfills an association or receipt for those rows.

Protocol-v1 associations are never rewritten as v2 and never receive a v2
receipt. They drain before promotion. G1 never synthesizes an association for
a historical unbound request. Such a request is settled by the typed legacy
reconciler and remains a conservative capacity debit while its effect
disposition is unknown.

### G2: blocked steady-state cleanup

G2 is authored with G1 and the demand-convergence P2 change and remains
draft/blocked until every gate below is recorded. It owns forward-only
API-request revision 013 and Serve revision 049.
Any earlier draft that assigned API011 to combined-role cleanup or Serve047 to
reserved-fill final cleanup is renumbered to API013/Serve049; API012/Serve048
belong to the intervening demand/route convergence. Migration numbers must be
globally unique and already-published revisions are immutable.

G2 removes every unbound non-pool admission and recovery branch, the ordinary-
only handler/profile alias, cluster-name quiescence as active authority, global
startup recovery lock/backoff, process-map authority, legacy promotion and
demotion surfaces after the rollback window, and transition-only telemetry.
Fresh central-PostgreSQL non-pool work is always bound. Historical unbound
`READY` rows may remain readable for status/cleanup, but no active launch or
automatic recovery path consumes them. Pools retain their separate lifecycle.
The final topology has one non-pool handler, one association, one request queue,
one executor topology, and the existing provider path.

### G1S: additive executor-retirement hardening

G1S is an additive PR above the deployed G1/P2 baseline and below the G2
cleanup. It makes the existing drain marker active in API, executor, and
controller runtimes; records drain-start and completion against the exact
`api_server_instances` identity; splits and validates the Helm shutdown
budgets; and adds the typed executor-termination evidence source without
changing request terminal or quiescence fields. It retains the reviewed manual
attestation path for historical or incomplete infrastructure evidence. A
simultaneously authored stacked cleanup removes the old sleep-only hook and
full-grace application environment once mixed chart versions have drained.

The crash matrix includes drain-marker-before-SIGTERM, SIGTERM during provider
I/O, SIGKILL before receipt commit, node loss, force deletion, database outage,
old/new chart overlap, and launch during a rolling update. Every case asserts
that no second effect starts without the canonical binding and that unknown
termination evidence remains quarantined rather than synthesized.

## Deployment and rollback

### Generalized G1/G2 rollout

Ship API011 and Serve047 schemas plus tolerant readers before any generic
writer or claim is enabled. Then converge every split role on one immutable
image and exact capability-profile digest. The cohort inventory includes API
acceptors, request-backend writers, queue executors, GC, possible service
controllers, and each profile-specific admission/executor participant; role
health alone is insufficient. Drain every old ready lease and every non-ready-
but-recent lease through the maximum stale-writer and quiescence horizon before
creating the next cohort epoch.

Before the first per-service cutover:

1. Reconcile the seven incident rows individually. Preserve the fact that the
   old executor caused real provider effects. Record exact request result and
   provider evidence and classify each row; never fabricate a receipt or edit
   it into `PRE_EFFECT_TERMINAL`.
2. Require zero unbound active requests and zero unbound
   `PENDING`/`PROVISIONING` replica rows for that service. A historical
   unbound row with uncertain effects is quarantined and blocks only its exact
   successor/profile authority.
3. Prove the exact service owner and all participants advertise the same
   generic handler version, profile set/digest, receipt protocol, and cohort
   epoch. Prove an old handler cannot claim the new queue row.
4. Publish one complete manager-owned route projection for the new owner and
   prove LB/API reads use PostgreSQL only. Startup launches the prober,
   coordinator, and route publication immediately; row reconciliation runs in
   bounded independent tasks.
5. Advance the service binding epoch and capability-cohort epoch once under the
   canonical locks. New non-pool admission is thereafter generic-only for that
   service.

Reserved-fill activation remains one way and fail closed. It may enter
`SEQUENCED_ACTIVE` only after its typed profile participates in the same G1
cohort and row acceptance returns exact binding IDs. After activation, rollback
is limited to a capability-compatible artifact after bound work is drained;
the database and authorization generation are never downgraded and legacy fill
is never reopened. Before reserved activation, a G1 application rollback may
disable new promotions, retain the forward schemas, drain/project all bound
work, and use the fenced demotion only within the documented rollback window.
Once G2 removes demotion and legacy admission, recovery is fix-forward.

G2 cannot merge on elapsed time alone. It requires zero legacy-capable
participants, zero active or unsettled old-handler requests, zero unbound
non-pool rows requiring recovery, a controller restart and ordinary service
update on the generic path, complete readiness/+10/+30 monitoring, at least one
full 180-second Serve authority horizon plus the longer stale-writer/
quiescence horizon, bounded manager-lock hold time, fresh route projection,
broker conservation/no paid spill, and a successful rollback rehearsal before
the point of no return. The cleanup PR then removes the rollback path and later
defects are fixed forward.

The generalized contract is a material design change. All prior adversarial
rounds over an ordinary-only R2/R3 or a Serve047 reserved-fill cleanup are
historical and the required consecutive review sequence restarts at zero on
the exact G1/G2 heads.

G1S rolls out dark with the existing binding modes unchanged. Qualification
must observe one planned retirement for each runtime role, prove drain-start
precedes readiness sleep and SIGTERM, prove every owned claim settles or is
explicitly quarantined before exit, and inject one hard-kill case that leaves
an ambiguous action isolated. Rollback may restore the prior binary only while
the additive evidence schema remains unread-but-tolerated; it must not restore
the sleep-only chart hook after the first service relies on early retirement.
The stacked cleanup merges only after all old chart/runtime cohorts age beyond
the stale-writer and quiescence horizons.

### Historical R0 deployment record

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

Historically, R2 was designed to ship with every service in durable `legacy`
mode, so schema and capability writes are dark. Promotion changes one approved
non-pool service to `bound` only after its controller capability, the full
participant barrier, and the
legacy-drain transaction pass. R3 retains that same transition as the safe
bootstrap for a fresh eligible service: the row is inserted in `legacy`, its
fresh capable incarnation is claimed, promotion commits, and the exact bound
epoch is refreshed before child spawn. A failed barrier or refresh leaves no
controller child running. Existing services and recovery are not implicitly
migrated. The participant barrier includes every API,
queue-executor, and service-controller role that must preserve and revalidate
the closed excluded-profile discriminator; a queued special request may cross
promotion only because all of those roles understand its exact persisted
identity. An incapable controller cannot own that service.
Rollback disables further promotion, keeps capable binaries serving existing
bound rows, and waits for every request to become terminal, quiescent, copied,
projected, and unpinned. The fenced demotion transaction then proves no active
generation, sets the service back to `legacy`, and increments its binding epoch
before any incapable image may own it. A rollback must not clear associations
or tombstones, release pins early, change replica record IDs, or race a
predecessor with a successor. R3 makes `bound` mandatory before the first child
spawn for each newly created eligible non-pool service. After all existing
eligible services are explicitly promoted or retired and rollout evidence
passes, the superseded R4 would have removed only the ordinary fallback.

G1 replaces that ordinary-only rollout boundary. Eligibility is exactly every
central-PostgreSQL non-pool launch, including reserved fill, zero-cost/
reservation, system-OOM recovery, unknown-capacity replacement, and cost
rebalance. These profiles enter one typed association without surrendering
their planner-owned authority. A fresh paid launch whose exact current-protocol
request terminates before either effect boundary may reuse its paid claim in a
successor generation; a legacy/mixed-version terminal row may not.

No canary that creates provider capacity is authorized by this design alone.
Before such a canary, record the logical GPU slots, physical instance shape and
count, region, duration, market/reservation class, and incremental cost, and
obtain explicit management approval.

## Verification and monitoring

### Generalized binding verification

G1 fault injection covers crashes before and after atomic commit, queue claim,
each provider-I/O boundary, Kubernetes Pod create/adopt, service-job submission,
terminal result, and replica projection. Lost-ACK retries
must return the exact request; a stable-key or profile-digest conflict must
write nothing. Old/new API, controller, executor, GC, and profile-participant
permutations prove an old handler cannot claim a generic request and a stale
lease cannot satisfy the cohort gate.

The incident regression is mandatory: an old mixed-version handler performs a
real provider effect and later leaves generation zero, no process identity, and
`execution_quiescence_required = false`. Recovery must classify
`LEGACY_EFFECT_AMBIGUOUS`, rediscover the provider state, and never fabricate a
receipt or create a successor. A separate modern-protocol test proves the
narrow inverse: exact cohort admission, no claim, `NOT_STARTED`, and exact
terminal/quiescence evidence classify `PRE_EFFECT_TERMINAL` and permit a
fenced retry.

Typed provider tests cover `PRESENT`, exact `ABSENT`, `UNKNOWN`, and
`REPLACED` for a same-name/new-UID resource. Timeout, partial enumeration,
malformed identity, and RBAC denial are `UNKNOWN`. Exact result and provider
evidence must agree before projection; contradictory evidence remains
quarantined. The exact `ABSENT` integration test forces the ReplicaInfo
projector to fail once and proves the replica state, association transition,
and request-pin deletion all roll back; its successful retry proves
`AMBIGUOUS` to `PROJECTED`, failed-cleanup status, pointer clearing, immutable
terminal/provider evidence, and pin release commit together.

A scale test injects one poisoned association among hundreds of replicas and
requires probes, provider-free route publication, LB sync, autoscaling, the
reconciliation coordinator, and sibling reserved pools/cards to continue. No
provider/network wait, sleep, or poll may occur under the manager/global
recovery locks; lock-order instrumentation enforces the canonical database and
in-process order. Reserved-fill tests prove broker conservation, exact
`FillCommitResult` association/request IDs, no oversubscription or paid spill,
and stale profile authority rejected at both admission and provider I/O.

Route tests bind every record to exact `(replica_id, replica_record_id)` and
generation identity. A predecessor URL/demand/result cannot affect a successor
record; cold owner replacement remains fail closed until a complete fresh
projection. Instrumented LB/API reads make zero provider, Kubernetes, cluster-
state, or replica-list calls.

G2 final-state tests are source-absence tests. They fail if any unbound
non-pool admission/recovery, old handler/profile alias, global startup recovery
lock/backoff, cluster-name authority, process-map authority, demotion surface,
or transition-only telemetry remains callable. They retain readable historical
tombstones and the separate pool lifecycle.

### Historical R0 verification

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
leaves its queue row unclaimed. R3 may then be deployed to require the same
transactional promotion for newly created eligible services. Verify one fresh
service becomes exactly bound before its controller child starts, and verify a
forced participant-barrier failure creates no child. The R4 removal remains
draft until this mixed-version and rollback sequence,
the exact crash matrix, and the monitoring window below have passed.

The operational promotion/demotion request is intentionally absent from the
public Serve API contract. An operator must first read the current service hash
and binding epoch and submit both with the requested mode; a replaced service,
stale owner,
non-adjacent epoch, active local launch worker, incapable participant, legacy
request that is not terminal and exactly quiescent, queued legacy request, or
unsettled bound association returns a conflict without changing mode.

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
- every caller-authored exclusion marker and every persisted special-profile
  marker is rejected by bound admission and by the final effect fence;
- zero eligible launches using restart inference after promotion;
- exact handoff and adoption after controller restart while queued, claimed,
  and inside the existing launch/provider call;
- a claimed pre-`RUNNING` handoff remains active, the generic expiry reaper
  leaves all correlated bound claim evidence untouched in either lock order,
  and exact active or already-terminal expiry settles only at `NOT_STARTED`;
- all bound storage construction occurs after `PROVIDER_IO` publication under
  the exact claim/service guards, and interruption there blocks a successor;
- no successor after `PROVIDER_IO`, or after `SERVICE_JOB_IO` without an exact
  recorded/projectable outcome, regardless of terminal/quiescence state;
- malformed durable request errors or successful service-job result payloads
  become explicit operator-visible ambiguity rather than an in-memory retry
  loop, while valid decoded capacity/quota errors retain exact paid-pool
  feedback;
- expired active or terminal owner evidence at `PROVIDER_IO` or later becomes
  durable `AMBIGUOUS`, with no timeout-only execution-quiescence synthesis;
- no request collection while its retention pin is active, and normal
  collection after exact projection releases it;
- teardown commits cancel intent and proves exact-request quiescence before
  replica deletion or replacement;
- cancel intent commits while a provider retry holds its shared authority
  guard, generic cancellation cannot bypass the Serve transaction, and
  cancellation cannot be cleared, rewritten, or followed by a successor;
- terminal/quiescence publication racing fenced cancel or projection completes
  without request/association lock inversion and preserves one outcome;
- terminal projection within two controller polls after the API result;
- no ambiguous binding older than two configured retry intervals without an
  alert; and
- bounded p99 request dispatch and reconciliation latency relative to the
  pre-change baseline.

## Open gates

### Generalized non-pool binding gates (current)

- [x] Author, review, and merge G1 (API011/Serve047); author demand convergence
  (API012/Serve048) and the blocked stacked G2 cleanup (API013/Serve049)
  together; link the PRs and state G2's exact merge gate.
- [ ] Restack any draft API011 combined-role cleanup to API013 and any draft
  Serve047 reserved-fill permanent cleanup to Serve049. Prove each schema
  lineage has one forward-only head and no historical migration changed.
- [x] Inventory the historical seven incident rows and prove that IDs
  52032--52038 are absent. Do not recreate them or misstate absence as
  quiescence.
- [ ] Reconcile the current retained rows from exact request, provider, and
  cluster-incarnation evidence. In particular, reconcile replica 52689 through
  the exact G1 legacy ledger after restoring a capable runtime. Preserve real
  old-executor effects; record no fabricated quiescence receipt or synthetic
  association.
- [ ] Converge every API acceptor, request backend, queue executor, GC
  participant, possible controller, and profile participant on one immutable
  generic handler/profile/capability digest; drain old and recent leases through
  the complete stale/quiescence horizon.
- [ ] Prove zero unbound active request and zero unbound
  `PENDING`/`PROVISIONING` row before each per-service cutover. Quarantine a
  legacy ambiguous row locally rather than taking a global recovery lock.
- [ ] Pass the complete G1 crash/mixed-version matrix, including real legacy
  effects with misleading generation-zero fields, modern pre-effect generation
  zero, provider present/absent/unknown/replaced, and lost-ACK identity replay.
- [ ] Add and review the exact current-protocol provider-absence projection
  transition. Evidence collection is already two-sided by request quiescence,
  owner-fenced, and per-row isolated; activation remains closed until exact
  `ABSENT` can atomically project the replica, association, request pin, and
  capacity claim without weakening the Serve042 transition invariants.
- [ ] Pass reserved-fill conservation, no-paid-spill, exact binding receipt,
  provider-free route projection, bounded manager-lock, and poisoned-row
  progress tests.
- [ ] Deploy G1 through readiness, +10, and +30, then observe at least one full
  180-second Serve authority horizon and the longer stale-writer/quiescence
  horizon. Complete one controller restart, one ordinary service update, and a
  rollback rehearsal before G2 becomes eligible.
- [ ] Run three new consecutive pragmatic adversarial reviews against the exact
  frozen G1/G2 and reserved-fill heads. The 2026-08-15 material correction
  resets prior ordinary-only and Serve047-cleanup review counts to zero.
- [ ] Author G1S and its stacked sleep-only cleanup. Validate the three-part
  Helm shutdown budget, exact drain-marker ownership transition, real receipt
  completion, and typed executor-termination evidence across the complete
  rollout/crash matrix. Do not treat Pod absence or lease expiry as automatic
  execution proof.
- [ ] Deploy G1S dark and pass one planned retirement per API, executor, and
  controller role plus one injected hard-kill quarantine test before enabling
  its automatic infrastructure certificate issuer or merging its cleanup.
- [ ] Merge G2 only after zero legacy-capable participants, zero old-handler
  active/unsettled requests, and zero unbound non-pool rows requiring recovery.
  After G2, roll forward only.

### Historical retirement and ordinary-only gates

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
- [x] Author R2's atomic bind-and-enqueue, stable retry, adoption,
  owner-epoch/provider/service-job fences, retention pin, local-handler claim,
  mixed-version/demotion, teardown-order, and crash-matrix implementation in
  the stack. It remains operationally dark until the preceding R1 gate passes.
- [x] Author R3's fresh-service adoption: claim first; transactionally promote
  only a fresh capable central-PostgreSQL non-pool authority; verify the exact
  adjacent bound epoch; and fail before spawn on any transition error. Preserve
  recovery, existing rows, pools, and absent-authority stores.
- [ ] Merge and deploy R1, R2, and R3 in stack order. On the exact R3 artifact,
  verify successful promotion precedes first child spawn and an injected
  participant/drain-barrier failure produces no child or launch request.
- [ ] Inventory every existing eligible `legacy` service and explicitly
  promote or retire it; R3 intentionally does not perform that migration.
- [x] Retire the ordinary-only R4 cleanup without merge. G2 supersedes it and
  removes every unbound non-pool path only after the current gates above.
- [x] Record the final production rollout's exact zero-incremental-capacity
  bound; the worst-case API plus 16-LB surge fits either existing non-API node.
- [ ] R2 only: obtain named capacity approval before any positive launch/down
  crash canary or any rollout that invalidates the zero-capacity bound.

Until the bounded shared-snapshot deadline artifact passes its production
monitor and this stacked canonical follow-up merges, the dedicated
authority-stack retirement is not production-complete. The bounded
request-binding follow-up remains independently incomplete until the
generalized G1/G2 gates above close; satisfying the historical R1--R3 evidence
alone cannot complete it.
