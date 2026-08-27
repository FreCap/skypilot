# SkyServe multi-pool reserved-capacity admission

Last updated: 2026-08-27

Status: **in progress**. The single PostgreSQL-authoritative reserved-capacity
path has proved full East occupancy, reclaim, and synchronized post-fix
East/PHX convergence. Full idle occupancy is no longer a steady-state goal:
reserved capacity is now demand-driven and returns through the existing
utilization gate when idle. The independent paid provider-lifecycle gate is complete
on release `1.1.1513`, PR #1744, merge
`329f6f5a33bab85401fef59b023714b47fb1d5eb`. One atomic PostgreSQL wave
committed 120 exact Spot debits; provider-native GCP observations then reached
100 concurrently `RUNNING` one-L4 `g2-standard-4` Spot VMs 3 minutes 41.9
seconds after that commit and peaked at 117. On-demand remained zero. Normal
`sky serve down` reached provider `RUNNING=0` 4 minutes 56.4 seconds after the
down request completed and exact service, PostgreSQL, VM, and disk zero after
5 minutes 35.2 seconds. Two later guard samples, for three exact-zero samples
total, and independent PostgreSQL
and GCP censuses agreed. The run's 10,000 stable IDs at concurrency 256 were
bounded provider-lifecycle stimulus, not model-serving or terminal-ledger
evidence. The compact immutable record is in the
[Spot lifecycle evidence bundle](evidence/skyserve-gcp-spot-lifecycle-2026-08-26/README.md).
The accepted provider-lifecycle gate is specifically at least 100 physical paid
Spot L4 VMs concurrently provider-`RUNNING`, followed by normal provider and
PostgreSQL exact-zero teardown; reserved request execution is neither stimulus
nor evidence for that gate. The final mixed reserved-plus-paid load campaign,
terminal request reconciliation, UI proof, and takeover proof remain open under
the broader heterogeneous objective. Lifecycle 116 used only PHX's existing
externally owned Kueue lane
without changing scheduler policy, and is now intentionally absent after
cleanup. Its first production fill exposed a claim-heartbeat ordering defect.
PR #1746 moved pruning and overlap reads before the five-second reclaim
authorization, but lifecycle 117 proved that the remaining PostgreSQL
protocol/lifecycle lock convoy could still consume 7--15 seconds after the
ticket was minted. The steady-state order is therefore ``broker fence ->
prune/overlap and immutable preparation -> begin PostgreSQL transaction ->
protocol-first, owner, version/projection, and complete current-write-set locks
-> reconstruct exact scope -> read proactively renewed PostgreSQL receipts ->
emit proof observability -> mint exact authorization -> validate/write/commit``.
Provider proof renewal and all provider I/O remain outside the transaction. No
nonessential work or contended lock acquisition may run after the short-lived
authorization is minted. PR #1750 merged this correction as
`f22c459d53749e0d3a707d45621b633f6528073e`; release `1.1.1519` deployed it as
Helm revision 639. Eight consecutive observed post-rollout claim/publish rounds
succeeded without a rejected claim-set heartbeat.

The first lifecycle-117 terminal-ledger campaign exposed one additional
ordering defect. With more than 300 reserved replicas, the optimistic
reserved-supply projection can take longer than the load-balancer heartbeat
interval. The controller captured demand first, projected supply second, and
only then entered the publication transaction. Sustained traffic therefore
advanced queue/in-flight semantics on every projection and correctly fenced
every paid plan, creating deterministic starvation rather than a transient
retry. The steady-state order is now ``prepare/project supply -> capture one
fresh demand snapshot -> compute the decision -> begin publication -> lock
demand/route first -> revalidate the same supply graph -> commit``. No
blocking optimistic supply projection may occur after the demand snapshot.
Changed demand still fails closed; this correction removes the avoidable race
instead of treating different telemetry as equivalent.

Release ``1.1.1521`` deployed that ordering in Helm revision 640 without
recreating lifecycle 117. Resuming the same ledger then exposed a narrower
copy of the same defect: the reconcile projected one immutable broker
allocation before demand, but redundantly read the allocation again afterward.
A normal broker heartbeat could change the second allocation identity, causing
the controller to discard the already-projected graph before publication. One
reconcile now captures one sequenced allocation/projection pair and reuses it
as the publication input. A later bounded identity check may reject and retry
the plan, but it cannot replace that input with a different allocation. The
PostgreSQL publication
transaction remains the authority: it locks and reconstructs the captured
allocation and supply graph with demand and rejects either if stale. This
removes an optimistic identity race; it does not accept stale supply.

Release ``1.1.1522`` deployed that allocation reuse in Helm revision 641.
The same immutable 10,000-request run then reached 5,962 accepted identities
and exposed the final ordering gap: after computing the supply-aware target,
the controller still published local target/retirement state and attempted a
zero-cost demand launch before beginning the paid-plan transaction. Those
operations can wait on the large-fleet replica-manager lock long enough for a
new demand receipt or allocation heartbeat to arrive. PostgreSQL correctly
rejected the old inputs, but repeated rejection left paid residual starved.

The canonical promoted, allocation-bound order is therefore stricter:
``prepare/project supply -> capture demand -> compute target -> publish the
atomic PostgreSQL paid plan -> publish local target/retirement state ->
actuate``. The sequenced allocation has already committed every current and
pending reserved holding plus its unmaterialized tail before demand is read,
so a second demand-owned zero-cost probe is not a prerequisite for the paid
residual. It may still materialize reserved supply after publication; if it
does, no paid provider effect occurs in that branch and the controller replans.
Any allocation, supply-graph, route, demand, cap, version, or owner change at
publication still rejects atomically, and every later paid claim revalidates
the current plan and graph. This is an ordering correction, not permission to
accept changed demand or stale reserved capacity.

Release ``1.1.1523`` deployed that order in Helm revision 642. The preserved
run resumed at 8,080 accepted identities and proved that local actuation was no
longer ahead of publication, but also exposed that shape/Kueue decision-input
preparation itself remained on the demand side of the boundary. That
preparation resolves durable replica handles and lane capacity and may outlive
an LB report interval; rising queue/in-flight telemetry therefore still
advanced before the now-immediate publication. The canonical boundary includes
all immutable decision preparation: ``project supply -> load replica/runtime
and shape/Kueue inputs -> capture demand -> compute target -> publish``. A
durable planning fingerprint captured with the prepared replica snapshot is
rechecked after demand, while the publication transaction independently
reconstructs allocation, economic supply, route, lifecycle, and demand. No
demand comparator is relaxed. The first reconcile after controller birth may
prepare no inputs and fail closed; the captured demand then makes the next
reconcile use the canonical short section.

Release ``1.1.1524`` deployed that preparation order in Helm revision 643,
using one immutable image digest across two API, two controller, and three
executor Pods without recreating lifecycle 117. Run
``final10k-1524-20260827-0246`` then accepted 2,125 stable identities, retained
29 transport-ambiguous identities for exact reconciliation, and left 7,846
unsubmitted identities when its verifier stopped. Live reports contained 128
in-flight requests and queue depths of 1,758--1,770; the autoscaler computed
targets of 414 and 494. Nevertheless three consecutive positive-plan rounds
failed with ``Demand feed advanced with changed or unavailable semantics
before plan publication``. PostgreSQL, AWS, and GCP remained at zero paid and
zero on-demand capacity throughout. This is decisive evidence that shortening
an optimistic post-demand section is not a correctness-complete design: two HA
reporters may legitimately stagger changed demand often enough that no safe
publication gap exists.

The final steady-state boundary therefore does not compare a plan made from an
earlier demand snapshot with a later one. Before the transaction, the
controller prepares only immutable replica/runtime/shape/Kueue inputs and
samples the durable planning fingerprint on both sides of that preparation.
It then acquires the controller's short in-process routing epoch before any
PostgreSQL row. This preserves the routing-epoch -> PostgreSQL order used by
service-version transitions and prevents an update from deadlocking with
planning. While that epoch remains held, one PostgreSQL transaction takes the
existing deterministic protocol-first database lock order: protocol,
service/owner/version, current demand generation and reporter rows, route,
allocation, capacity/Kueue graph, and plan head. The transaction recomputes
the planning fingerprint from the exact locked service and replica rows and
rejects unless it equals the prepared fingerprint. Because every demand
reporter locks the service row before changing
its report or generation, the transaction reconstructs one exact current
normalized demand snapshot while report writers briefly wait. It reconstructs
the exact current economic reserved-supply projection from the already locked
capacity graph, then invokes one bounded in-memory planning callback. The
callback performs no PostgreSQL, provider, Kubernetes, HTTP, replica-manager
lock, or filesystem I/O. It runs under the already-owned routing epoch and
returns the supply-aware exact-card target and local
actuation candidate; the same transaction validates the cap, persists the plan
and head, and commits. Only the returned committed authority may publish local
target state or initiate a provider effect.

This collapses the optimistic supply projection plus later byte-comparison into
one canonical locked input. It does not weaken freshness or treat changed
demand as equivalent: demand that commits before the service lock is included
in the plan, and demand that arrives after it commits in the next generation
after the capacity transaction. Unknown, incomplete, stale, or route-mismatched
demand still fails closed. The callback runs only after all conflict-prone rows
are locked, so no expected concurrency rejection remains after it mutates the
candidate autoscaler state; a database failure still publishes neither local
state nor provider authority and the next current reconcile replaces that
candidate state.

The promoted controller has no second publisher or compatibility branch. The
former controller-local ``_publish_ordered_paid_authority`` helper is removed;
all promoted positive and zero residuals use the same
``plan_and_publish_current`` transaction. The repository's lower-level
``publish`` operation remains an internal primitive and claim-path validation
surface, not an alternate promoted reconciliation path.

PR #1756 merged this boundary as ``355cdc9168bb398fd8d73c886fa24e0abdf9e852``
and release ``1.1.1525`` deployed it as Helm revision 644. Dark verification
found a narrower liveness defect before traffic resumed: the prepared-state
fingerprint used PostgreSQL ``xmin`` for every replica row. Replica-manager
writers may persist an identical normalized ``ReplicaInfo`` document, which
advances ``xmin`` without changing any input consumed by the autoscaler. With
345 reserved replicas, those physical no-op rewrites repeatedly rejected the
otherwise current locked graph as ``Prepared planning state changed``.

The fingerprint contract is semantic, not physical. It covers the service
runtime fields and a database-side SHA-256 of each canonical JSONB replica
document consumed by planning, including row identity and state version, but
excludes PostgreSQL tuple revisions and timestamps that are not part of those
documents. This keeps the fingerprint compact while a byte-equivalent/no-op
rewrite cannot starve publication. Any replica addition/removal or normalized
state change still changes the fingerprint and fails closed before the
callback. The transaction continues to lock and plan from the complete current
rows; this correction changes neither demand, supply, Kueue, cap, nor provider
authority.

PR #1757 merged the semantic fingerprint as
``fa97e7673719cb6721c73051e2185fe3086da31b``. Release ``1.1.1526`` deployed it
as Helm revision 645 on one immutable image digest across two API, two
controller, and three executor Pods. Lifecycle 117 remained version 1 and
``READY`` with 345/345 zero-cost replicas. After startup, three samples over
42 seconds held prepared-state conflicts at exactly zero; the only eight
planning conflicts were conservative demand-unavailable results while the
load-balancer slots started. PostgreSQL and provider-native guards remained at
zero paid Spot and zero on-demand capacity.

The accepted objective changed after that proof. The service no longer keeps
every scheduler-free research GPU occupied merely because it is free. Its
ordinary target is the minimum compatible logical capacity needed to satisfy
the configured queue-wait objectives, subject to utilization headroom and
bounded launch-to-ready estimates. Reserved capacity, already-running paid
Spot, and already-committed paid Spot satisfy that target before a new paid
residual is authorized. Opportunistic fill is utilization-gated with a zero
floor, so idle fill returns without changing East scheduling or Simone's PHX
Kueue policy.

The existing implementation has one calculation defect relative to this
contract. Aggregate queued work applies priority timeout weights, while the
exact-card compatibility allocator consumes raw queued profile counts. A
complete compatibility report can therefore raise the aggregate target back
to one logical slot per queued request and erase the timeout weighting. The
steady state has one queue-work representation: every queued compatibility
profile is converted to work with the same priority timeout, expected request
duration, launch lead, and utilization policy used by the aggregate target.
That exact work is then allocated once across compatible cards. Raw counts
remain only the conservative mixed-version fallback when the priority or
compatibility report is incomplete; they are not a second current happy path.

A fresh positive snapshot can follow a committed zero-demand retirement while
the pre-transaction replica snapshot still marks paid capacity as scaling
down. Every nonterminal cleanup-unproven paid row remains part of the locked
paid baseline even after ``is_scale_down`` becomes true, so the transaction
cannot authorize a replacement for capacity that may still exist or bill.
Cancellation remains its own exact service/demand-generation transaction and
is not hidden in the planning callback. After the conservative plan commits,
the controller cancels only retirements fenced by that exact positive demand
generation; if any row changes, it publishes no local candidate, refreshes
replica/runtime/shape inputs and their fingerprint, and retries. No manager or
provider operation runs under the capacity transaction.

Zero-residual revocation is deliberately unbound from a reserved allocation.
It therefore discards any earlier economic supply projection before building
the plan; carrying an allocation graph digest on an unbound zero plan is both
meaningless and rejected by the repository.

The normal lifecycle-116 teardown then exposed two independent reducer defects.
First, exact reserved-fill absence reached the common replica projection, but
terminal request status and cause were read only in the paid-provider branch.
Reserved cleanup therefore raised ``UnboundLocalError`` before its atomic
PostgreSQL projection. Release ``1.1.1516`` scopes explicit-cancel
classification to paid-provider cleanup and carries only a boolean into the
common terminal reducer. It recovered the same teardown and retired 194 of 196
rows without manual state mutation.

That recovery exposed the second defect: the two remaining exact-ABSENT rows
were atomically settled with failed launch and failed down diagnostics, while
the post-ABSENT retirement predicate recognized only the pre-provider
``INTERRUPTED`` immediate-cleanup marker. Both remaining rows had exact
current-record association history, execution quiescence, canonical
post-quiescence Kubernetes ``ABSENT`` evidence, a null association pointer,
zero retention pins, and no paid claim. Current writers therefore normalize
reserved ``ABSENT`` projection to the one existing immediate-removal marker.
N-1 recovery recognizes only the exact ``1.1.1516`` ``FAILED/FAILED`` default
shape as a compatibility candidate. That candidate is not authority: the
existing PostgreSQL transaction must first re-lock and validate the exact
lifecycle, record UUID, association, request generation and quiescence,
evidence, pointer, pin, queue, and claim state. The removal path then
independently revalidates its owner, record, and Kueue fences. Neither path can
authorize provider I/O or weaken the pre-provider ``PRESENT`` cleanup marker.
Both the live replica manager and whole-service ``FAILED_CLEANUP`` recovery
consume this DB-only candidate before considering provider cleanup. The
service-level path also requires the exact SkyPilot cluster record to be
absent, then removes the replica with the provider-free pre-job Kueue retirement
scope. Before cleanup, both retained replica rows' SkyPilot cluster records
were absent; no inferred provider deletion or manual row edit was required.

PR #1748 merged as ``9a965c4fe83648bd7c32bb16b3f385861c5217fc`` and
release ``1.1.1517`` deployed as Helm revision 638. The retained service no
longer had an HA recovery script, so the daemon correctly refused to invent a
recovery launch. The supported ``sky serve down --purge`` orphan path then
claimed a fresh fenced owner and consumed the same PostgreSQL, cluster-record,
and Kueue retirement authorities. Request
``c8e86f2e-ef90-4cee-b6d7-aae7ea0c1dd8`` completed successfully at
2026-08-26 21:18:44 UTC. The current service, replica, active association,
claim, waiter, intent, Kueue admission, route, cluster-record, East/PHX worker,
GCP VM, and GCP disk censuses are all zero. Historical evidence rows remain by
design. This final reserved-row cleanup required no provider call or manual
database mutation.

This file is the canonical living design. It describes the current contract,
the latest production evidence, and only the gates that remain. Historical
incident chronology is intentionally left to Git.

## Decision

Run one heterogeneous SkyServe service with one capacity authority:

1. Convert current in-flight, queued, rejected, and sustained-arrival demand
   into one priority/deadline-weighted logical work target.
2. Allocate that work once across its exact compatible accelerator sets.
3. Observe and commit compatible zero-cost Kubernetes capacity without
   mutating the scheduler.
4. Satisfy the target with ready, committed, and newly admitted reserved
   capacity, then with already-committed paid Spot.
5. Admit only bounded L4 Spot for the remaining exact-card residual.
6. Make every provider effect conditional on the exact committed PostgreSQL
   graph that authorized it.
7. Release opportunistic zero-cost fill through the utilization gate when it
   no longer backs demonstrated work.

When recreated for the broader heterogeneous objective, the canonical service
uses East A100 and A100-80GB as zero-cost reserved capacity and AWS/GCP L4 Spot
as demand-only residual. The target service additionally uses PHX H200 only
through the existing
`boltz-research/be -> research-be` Kueue lane. SkyPilot submits at workload
priority `be-lt` and Pod priority -1000, treats Kueue admission as the final
spendable-capacity authority, and does not create or modify scheduler policy.

The control plane is PostgreSQL-only. EFS, a PVC, KubeRay, Terraform,
Terragrunt, and `boltz-platform` application state are not part of this design.

## Current state

The distinction between source, deployment, activation, and proof is
intentional. A source-complete behavior is not production-proven merely because
it has merged or been deployed.

| Layer | Current state |
|---|---|
| Source base | `origin/improvements` at merge `fa97e7673`, release `1.1.1526`, including PR #1757's semantic prepared-state fingerprint, PR #1756's PostgreSQL-linearized planner, and the earlier ordering, claim, retained-row, and reducer corrections. PR #1758 contains the unified priority/deadline-weighted exact-card target described here; it is not yet deployed or proven. |
| Deployed control plane | SkyPilot `1.1.1526`, Helm revision 645. Two API, two controller, and three executor Pods are Ready and all use immutable image `255203429798.dkr.ecr.us-east-1.amazonaws.com/skypilot-nightly-boltz@sha256:34e1ef5560dafd04612c5b415a395d4d0fedd1df86e7470511eb40ad12e9c59b`. Helm storage remains disabled and the namespace has no PVC. |
| Writer protocol | Public API 93, worker projection 10, non-pool capability cohort 12, and async request-ledger protocol 1. |
| Storage | PostgreSQL is the sole central correctness store; Helm `storage.enabled=false`; no SkyPilot EFS or PVC. |
| Active service | Lifecycle 117, incarnation `3bc2c88b-2c28-40fa-a9d5-482880767b3e`, is a clean current-schema PostgreSQL-only service at committed/applied version 1 with paid residual cap 100. At the 2026-08-27 baseline it is `READY` with 345/345 zero-cost replicas and no paid row. The submitted version still carries the superseded `utilization_gate: false`; changing that field to true is an explicit rollout gate below. The service is test-only and not yet production-qualified. |
| Reserved occupancy | At 2026-08-26 23:09--23:13 UTC, East had 328 healthy compatible GPUs on 41 nodes: research requested 45 and 283 `boltz-l4-fleet` Pods requested the exact remainder; all 283 were Running and Ready, with zero free compatible GPU and zero pending research or fleet GPU Pod. PHX had 512 healthy H200 GPUs: research held 482 and the unchanged Kueue policy admitted 30/30 fleet Workloads; all 30 Pods were Running/Ready and PostgreSQL `READY`, with zero pending research GPU Workload. PostgreSQL independently reported exactly 63 A100, 220 A100-80GB, and 30 H200 reserved replicas `READY`, with zero durable intent pending. Thus the same lifecycle occupied East 328/328 and PHX 512/512 without changing scheduler policy. |
| Reserved readiness projection | For the final PHX replica, PostgreSQL committed the intent at 22:43:32, the Pod appeared at 22:43:55, Kueue admitted it at 22:43:56, and the Pod became Ready at 22:44:32. PostgreSQL projected it `READY` only between 22:52:25 and 22:52:40, exposing a separate roughly eight-minute status-freshness lag rather than a capacity/admission failure. The post-Helm 23:13 UTC census retained the exact 30/30 admission and readiness with no churn. |
| Claim-heartbeat convergence defect | Resolved in source, deployed, and dark-verified in production. Lifecycle 117 had logged successful exact reclaim-policy proofs followed 7--15 seconds later by rejected claim-set heartbeats because the broker minted the five-second ticket before entering the PostgreSQL replacement and its protocol/lifecycle locks. PR #1750 passes an authorization callback into the state transaction, locks protocol, owner, immutable version/projection, claim-set/edge rows and the legacy projection, reconstructs exact scope, and only then reads already-renewed PostgreSQL proof receipts. Proof logging completes before the ticket timestamp; the ticket is then immediately validated and written. Ordinary drained boundary failures remain fail-closed and boundary ambiguity remains controller-terminal. The correction changes neither Kueue nor TTL/batch/quantum limits. Real-PostgreSQL tests cover waits beyond the ticket lifetime on the affected lock paths. Release `1.1.1519` then produced eight consecutive observed successful claim/publish rounds after controller takeover with no rejected heartbeat. |
| Reserved teardown projection | Complete. PR #1747 projected all formerly blocked associations and retired 194 rows. PR #1748 normalized current writers to the existing immediate-removal marker and accepted only the exact `1.1.1516` `FAILED/FAILED` shape as an N-1 DB-retirement candidate. Release `1.1.1517` plus the supported orphan purge retired the final two rows through exact PostgreSQL authority and independent owner, record, cluster, and Kueue fences. No provider, Kueue, schema, migration, or manual-cleanup behavior changed. Exact service/control-plane/Kubernetes/GCP zero is production-proven. |
| PHX access | The controller identity can exact-read the required namespace/queue and manage only worker Pod/Service lifecycle; it cannot list or patch ClusterQueues. The worker ServiceAccount is tokenless and cannot read Pods, queues, or secrets. A historical audit-only group still has an unused broad Kueue LIST grant from platform PR #8800; it is read-only, has no scheduling effect, and is not used or expanded by this rollout. |
| Paid state at idle | The 23:11--23:12 UTC post-fix census found PostgreSQL paid claims 0, waiters 0, Spot replicas 0, paid-attributed replicas 0, and native provider inventories at zero: AWS across 18 regions had no service instance, open/active Spot request, or tagged volume; GCP had no service instance or disk. No scan errors occurred. Earlier lifecycle-gate exact-zero samples remain in the linked evidence bundle. |
| Routing and queue | Lifecycle 117 is `READY` with 345 reserved replicas and no paid rows. The lifecycle-115 run attempted all 10,000 stable synthetic IDs at concurrency 256, but used them only as bounded provider-scale stimulus; it is not the separate 10,000-terminal-request ledger proof. Run `final10k-1524-20260827-0246` proved current nonzero queue/in-flight demand but stopped after the publication race described above. A fresh nonzero queued/processing/in-flight/completed UI proof remains part of the final heterogeneous load run. |
| Partial mixed proof | Provider/DB censuses at 2026-08-25 19:45:47.538 and 19:45:56.281 UTC bracketed a 72-request completion wave and both had 44 reserved plus 28 paid replicas all `READY`, the same 28 AWS Spot instances—27 `g6.2xlarge` and one `g6.4xlarge`—and zero on-demand. The wave completed from 19:45:48.956 through 19:45:51.187; every request performed 9.533–12.451 seconds of concurrency-one GPU work, so at least 28 necessarily executed on Spot beside the 44 reserved workers. The Spot instances later fully drained at the provider. |
| GCP Spot lifecycle proof | Complete on `1.1.1513`. The fixed-120 update completed at 18:23:39.277 UTC. After five fail-closed prospective conflicts while the traffic writer changed telemetry, the sixth attempt atomically committed all 120 debits at 18:25:12.183. Provider-native observations first reached 100 `RUNNING` at 18:28:54.100, then 107, 110, 114, and 117. Every object was GCP Spot `g2-standard-4` with exactly one NVIDIA L4; zero on-demand or non-Spot capacity appeared. The peak 117 VMs were in `asia-northeast3` and `asia-south1`. Normal teardown reached native `RUNNING=0` at 18:35:00.512 and exact all-state zero at 18:35:39.315. |
| Final load proof | Not complete. Run `final10k-1524-20260827-0246` retains one immutable 10,000-ID manifest. It accepted 2,125 identities, classified 29 exact transport outcomes as ambiguous for durable reconciliation, and retained 7,846 pending identities before stopping on the publication defect. Live telemetry proved nonzero queued/in-flight demand and computed targets of 414 and 494. The exact 503 body `No replica has confirmed free async capacity. Use "sky serve status [SERVICE_NAME]" to check the replica status.` is a definitive retryable pre-dispatch rejection only when the PostgreSQL request receipt is exactly `REJECTED_PRE_DISPATCH`; the harness may classify that exact pair without relaxing any other 5xx outcome. The run identity must be resumed, not replaced. |
| Demand/publication ordering | PR #1756's PostgreSQL-linearized current-demand/current-supply transaction and PR #1757's semantic prepared-state fingerprint are merged and deployed on `1.1.1526`; the superseded promoted publisher is removed. Dark verification preserved lifecycle 117 and all 345 ready reserved replicas and observed zero prepared-state conflicts after startup. Final deadline-weighted load proof remains pending. |

The completed paid-gate post-rollout census was green after Helm revision 635:
the service, replicas, claims, waiters, request associations, queue rows,
retention pins, and cluster bookkeeping were all zero, as were provider-native
GCP VMs and attributable disks. The later lifecycle-116 reserved qualification
and its final two-row cleanup are also complete under release `1.1.1517`, as
described above. Dashboard rows such as
`SHUTTING_DOWN` are controller lifecycle records, not provider billing proof.
Cost closure always requires the provider-native census plus the PostgreSQL
paid-authority census.

## Goals and acceptance criteria

### Reserved capacity

- Admit as much healthy, exact-card-compatible reserved capacity as the
  current priority/deadline-weighted demand target needs. Do not retain idle
  capacity merely to maximize occupancy.
- Count every GPU on a multi-GPU machine once. One logical asynchronous worker
  owns one GPU; an eight-GPU node can therefore host eight workers.
- Treat available supply as dynamic. When research releases a compatible slot,
  the next fresh observation makes it eligible for demand placement; when
  research or Kueue reclaims a slot, SkyPilot yields it and stops counting it
  as spendable. SkyPilot does not claim that Kueue will choose a victim outside
  the policy it actually implements.
- With no demonstrated demand, a zero-floor utilization-gated fill claim
  converges to zero. Blind telemetry freezes briefly and then resumes bounded
  release; it never restores a static full-pool reservation.
- Never infer free capacity from nominal hardware totals, stale rows, or a
  pending workload that the scheduler has not admitted.

### Deadline-aware target

- Interpret `load_balancer.request_queue.timeout_seconds` and
  `timeout_seconds_by_priority` as dispatch/wait objectives. They are not an
  end-to-end scientific completion guarantee.
- Seed request duration and launch lead from service configuration, then use
  fresh empirical observations when the existing minimum-sample and freshness
  gates are satisfied.
- For a queued profile at priority ``p``, convert one request to
  ``min(1, duration / max(duration, timeout(p) - launch_lead))`` units of
  concurrent work. Apply the same conversion before both aggregate sizing and
  exact-card allocation.
- Existing in-flight work remains one occupied slot. Sustained arrival-rate
  work remains an independent floor because a finite backlog deadline does not
  authorize falling behind a continuing arrival stream. Rejected demand stays
  bounded by the existing short and retained windows.
- A complete current report groups queue work by `(priority, exact compatible
  accelerator set)`. Allocate the highest priorities first and preserve the
  request's compatibility set; within one priority, protect the profile with
  the worst alternative before assigning flexible work.
- Incomplete priority or compatibility telemetry fails conservatively to raw
  queue work and cannot authorize a guessed exact-card provider effect. This is
  the bounded N-1/N-2 transition behavior, not a parallel current policy.
- The target counts logical GPU slots. Provider placement may coalesce those
  slots onto a compatible multi-GPU machine, but every device must expose and
  complete one independent worker slot.

### Paid residual

- Commit reserved holdings, pending reserved admission, and allocation tail
  before computing the residual for paid capacity.
- Allow paid capacity only for authenticated demand not covered by compatible
  reserved capacity.
- Use L4 Spot only. Ordinary on-demand is forbidden.
- Enforce `max_live_paid_gpu_units` in logical GPU units across ready,
  provisioning, shutting-down, and cleanup-unproven paid rows.
- Select the cheapest currently eligible normalized Spot pool first, then use
  exact provider feedback to move to another eligible Spot pool. A static
  region order or stale UI price is not cost authority.

### Request execution and observability

- Execute a fresh campaign of 10,000 stable logical requests through the
  service-owned asynchronous queue.
- Recover an ambiguous HTTP outcome by looking up the same PostgreSQL ledger
  identity before replaying; never create a second live execution for the same
  intent.
- Count HTTP admission separately from processing, terminal completion, and
  scientific artifact verification.
- Show fresh queued, processing, in-flight, rejected, completed, and unknown
  counts in the service UI, with explicit completeness and freshness.

### Convergence and cleanup

- Admit large launch waves without serial provider, probe, drain, or teardown
  I/O holding the fleet manager lock.
- Reach at least 100 provider-running one-GPU Spot instances from sustained
  excess demand without a fixed ten-replica launch prefix. Controller
  admission time and provider boot time are measured separately. The completed
  120-debit qualification reached 100 in 3 minutes 41.9 seconds and peaked at
  117; this is a provider lifecycle gate, not a reserved-serving gate.
- Let the positive paid cap remain in place while demand falls to zero. Natural
  retirement, rather than lowering the cap, must remove paid capacity.
- Prove zero paid PostgreSQL authority and zero provider instances, Spot
  requests, and residual disks immediately, at +10 minutes, at +30 minutes,
  and through one complete stale/quiescence interval.

## Non-goals and ownership boundaries

- Do not create or modify Kueue queues, cohorts, flavors, quotas, fairness,
  priorities, or preemption. PHX uses the existing externally owned
  `boltz-research/be -> research-be` lane; SkyPilot creates no workaround lane
  and does not enlarge the capacity Kueue admits.
- Do not add Terraform, Terragrunt, KubeRay, HPTO, EFS/PV/PVC, or a
  `boltz-platform` runtime pin.
- Do not change `boltz-platform` application code. The request ledger,
  dispatch, and capacity transactions are internal SkyPilot control-plane
  behavior. The service definition may be operated from the allowed
  `ml_models/providers/skypilot/` boundary.
- Do not grant SkyPilot application-admin or cluster-admin permissions. The
  controller uses bounded server-owned namespace lifecycle permissions; an
  audit identity is read-only; workers cannot mutate Kubernetes policy.
- Do not add ordinary on-demand fallback or split services by accelerator,
  cloud, region, campaign, or revision. `use_spot: false` is valid here only
  for an exact fail-closed zero-cost Kubernetes pool.
- Do not make status or the dashboard call providers or become launch authority.
- Do not hide a lock convoy with a longer TTL, a larger timeout, an unbounded
  retry, a feature flag, or a second scheduler.
- Do not repair ambiguous state by manually deleting rows, Pods, Workloads, or
  provider resources without exact ownership and quiescence evidence.

## Production service contract

The existing public service-policy fields are sufficient. The live
qualification shape is equivalent to:

```yaml
resources:
  accelerators: {L4: 1}
  use_spot: true
  any_of:
    - infra: k8s/prod_research_cluster_eks
      accelerators: {A100-80GB: 1}
      use_spot: false
    - infra: k8s/prod_research_cluster_eks
      accelerators: {A100: 1}
      use_spot: false
    # Reviewed AWS and GCP L4 locations follow; all inherit use_spot: true.

service:
  replica_policy:
    min_replicas: 0
    max_live_paid_gpu_units: 100
    max_replicas: 1000
    reserved_capacity_fill:
      floor_replicas: 0
      weight: 100
      utilization_gate: true
```

`min_replicas: 0` and the paid cap make paid capacity demand-only and
scale-to-zero. `utilization_gate: true` makes opportunistic reserved fill
activity-backed as well: after the bounded idle dwell, the broker releases
surplus in drain-safe steps until its zero floor is reached. It never creates
paid demand and never changes scheduler policy.

`floor_replicas: 0` is not a target of zero. It means there is no unconditional
minimum. Fresh demonstrated work may raise the utilization cap and the ordinary
demand path may still commit scheduler-authorized reserved capacity immediately.

Every reserved intent is pinned to the exact zero-cost Kubernetes location and
accelerator that authorized it. It cannot fall through to Spot. Paid Spot is a
separate residual path, so “no reserved spill” must not be misread as “the
service can never use Spot.”

The service advertises exact accelerator compatibility. A request keeps one
scientifically valid compatibility set independent of current availability;
the load balancer routes it only to a compatible ready worker.

## State and storage boundaries

### PostgreSQL control-plane authority

PostgreSQL owns current SkyServe correctness state:

- lifecycle, service version, owner incarnation, and capability epochs;
- physical-pool observations and authenticated reserved allocations;
- reserved intents, replicas, associations, API requests, queue rows, and
  retention pins;
- durable demand, route projections, paid plans, claims, and global paid
  debits;
- request-ledger admission, attempt, processing, and terminal status; and
- cleanup, quiescence, provider evidence, and retirement authority.

No local controller file, EFS object, cache, in-memory map, dashboard row, or
provider name inferred from a log can override this state.

### Object and worker storage

Object storage remains appropriate for immutable model bundles, request
payloads, result artifacts, and scientific completion markers. Those objects
do not replace the PostgreSQL capacity or request ledger. Signed request
capabilities are ephemeral and must not be written into durable control-plane
state or logs.

Worker caches and scratch are disposable, bounded, server-owned local storage.
They are not shared control-plane state. A worker restart may lose cache, but
must not lose a committed request or create a second dispatch.

## Capacity model

### Physical identity and units

The atomic reserved-pool identity is:

```text
(physical Kubernetes cluster UID, exact accelerator card)
```

The Kubernetes access context and worker width are carried separately. Context
aliases that prove the same physical UID/card cannot multiply capacity. A
context or card collision, inconsistent width, missing physical UID, or
unrecognized card fails closed.

Provider observations store raw exact-card GPU counts. The broker converts raw
GPUs to worker slots exactly once using the authenticated worker width. The
service ceiling and paid cap use logical GPU units for this fleet.

### Dynamic denominator

The reserved denominator is not the number of installed GPUs. It is the fresh
intersection of:

```text
healthy physical GPUs
∩ exact-card compatibility
∩ server-owned worker projection
∩ capacity currently available under the existing scheduler
```

East has no Kueue dependency. Its server-owned projection uses the existing
`gpu-binpack-scheduler` and
`rescluster-k8s-prod-east1-preemptible-inference-low` PriorityClass at numeric
priority -1000 with `preemptionPolicy: Never`. `Never` prevents a fill Pod from
preempting another Pod; higher-priority research can still evict the fill Pod.

PHX is outside the current live denominator only because the service YAML omits
it. Its target projection is server-owned and exact: Kubernetes context
`phx_research_cluster_eks`, namespace `boltz-research`, LocalQueue `be`,
ClusterQueue `research-be`, WorkloadPriorityClass `be-lt` at value 11,
ServiceAccount `skypilot-pool-sa`, Pod priority -1000 with
`preemptionPolicy: Never`, and one H200 per worker. The LocalQueue and
ClusterQueue are existing externally owned objects. SkyPilot validates them
read-only and never creates or mutates them.

A PHX physical-free observation can authorize an intent, but a waiting Kueue
Pod is not spendable capacity and cannot suppress paid residual. Only the exact
Pod UID whose Workload is admitted to `research-be` debits PHX capacity. Kueue
may queue or preempt that workload under its unchanged policy; SkyPilot then
refills or yields without interpreting nominal quota as an entitlement. The
2026-08-25 snapshot had 158 physically and quota-free H200s, but 158 is an
observation, not a fixed target or a new quota.

That unchanged policy has deliberate limits. MA and WA can nominate a
lower-priority `be-lt` fill victim while reclaiming nominal quota; HA has no
nominal GPU guarantee and relies on the configured fair-sharing decision; a
research BE workload at the same `be-lt` WorkloadPriorityClass cannot
Kueue-preempt another `be-lt` workload. Pod priority -1000 lets an already
admitted higher-priority research Pod win at kube-scheduler, but cannot itself
unblock Kueue admission. SkyPilot therefore consumes only exact admission,
revokes capacity on eviction, preemption, or lost admission, and launches
nothing on observer uncertainty. If the unchanged policy fails to reclaim in
a real research incident, the safe operation is to disable PHX fill, not add a
SkyPilot scheduler workaround.

### Observation freshness

Read-only pool observations run independently with bounded deadlines. A slow
or unavailable pool cannot serialize a healthy sibling. A successful result is
timestamped at observation start and expires after the conservative authority
horizon, currently 180 seconds. A newer blackout prevents fallback to an older
success.

Starting an observation consumes no capacity. Admission and first successful
materialization advance separate PostgreSQL sequences. These commit-order
sequences, not wall clocks or readiness guesses, prevent an observation from
double-spending a Pod created while the read was in flight.

## Reconciliation architecture

The one-way flow is:

```text
read-only scheduler/provider observation
    -> authenticated PostgreSQL pool snapshot
    -> broker allocation and reserved debit
    -> reserved replica/intent/request admission
    -> exact zero-cost provider effect

durable LB demand + immutable route projection
    -> target - committed/pending reserved - existing paid
    -> bounded paid residual plan
    -> paid claim/debit and launch binding
    -> exact Spot provider effect
```

There is no provider side effect on an arrow before the corresponding durable
commit.

### Reserved admission

Reserved fill uses one PostgreSQL transaction to:

1. lock the current protocol, service, allocation, and intent authority;
2. revalidate service ceiling, exact-card feed, projection digest, and owner;
3. allocate the global zero-cost admission sequence;
4. insert the replica and consume the durable intent/capacity debit; and
5. bind the generic non-pool association, API request, queue row, and retention
   pin.

The transaction returns exact replica, record, association, request, and launch
generation identities. Only then may the request executor adopt the launch.
A lost acknowledgement is reconciled from that receipt; it does not create a
second row or provider action.

Reserved launch resources contain one exact Kubernetes candidate. Retry
optimization and cloud failover are disabled for that execution capsule. If
the candidate or physical-UID fence is unavailable, the intent waits or fails
closed; it never becomes a paid launch.

### Paid residual and admission

The paid plan is computed from one locked, fresh graph:

```text
paid residual = max(
    0,
    authenticated demand target
    - committed reserved capacity
    - pending/allocation-reserved capacity
    - existing paid capacity)
```

The result is clipped by the elected service cap and all cleanup-unproven paid
rows. The plan carries exact demand, route, service, capacity-graph, reserved
allocation, accelerator, and pool identity.

Only immutable computation inputs are prepared before the correctness
boundary: replica/runtime handles, autoscaler decision inputs, and a durable
semantic planning fingerprint sampled before and after preparation. The
fingerprint hashes the complete normalized replica state consumed by planning;
it does not hash PostgreSQL tuple revisions, so persisting an identical row is
not a planning-state change. The controller does
not project economic supply or capture demand optimistically. It acquires its
short routing epoch first; one PostgreSQL transaction then locks the service,
current demand/report/route authority, allocation and current capacity/Kueue
graph in deterministic database order. The transaction recomputes and matches
the fingerprint from those locked service/replica rows before planning. It
reconstructs both the current normalized demand snapshot and the exact current
reserved-supply projection from those locked rows, then invokes one bounded
in-memory planner. Reporter writers already take the service lock first, so a
writer either commits before this snapshot and is included or waits and becomes
the next generation. The resulting target, supply accounting, paid residual,
plan and head therefore describe one linearized state, with no heartbeat-sized
optimistic publication window and no duplicate supply projection to compare.

The planning callback may inspect only its immutable prepared inputs and the
locked demand/supply values passed by the repository. It performs no database,
provider, Kubernetes, HTTP, filesystem, or replica-manager operation. All
conflict-prone rows are locked before it runs; after it returns, only canonical
validation and plan/head writes remain. Local target, logical reconcile state,
retirement state, and provider effects are published only after commit.
Fresh-positive retirement cancellation is a distinct exact-generation
transaction after an aborted candidate and before a fully refreshed retry; it
never runs inside the callback.

A prospective Phase-A debit may cross newer demand receipt generations only
when the semantic plan itself is unchanged. In the same PostgreSQL transaction
that inserts the wave, the controller locks the current service, route, demand
reports, capacity graph, and plan head; requires every current reporter to be
fresh, complete, elected, and bound to the plan's exact route; reconstructs the
normalized demand snapshot from those locked reports; and compares it with the
plan. A monotonically newer generation or receipt watermark is not by itself a
semantic change. New, increased, or redistributed queue, in-flight, rejected,
arrival, priority, or accelerator demand; fresh aggregate zero; a reporter or
route mismatch; unavailable evidence; or any supply, cap, ownership, or plan
change aborts the whole transaction before the first row is inserted. Natural
rolling-window arrival expiry remains the only accepted demand-semantic
contraction. This lets a large provider-free candidate preparation wave survive
HA heartbeat churn without weakening immediate zero-demand revocation or the
commit-before-provider boundary.

The semantic-equivalence comparator above remains a defensive contract for a
previously committed plan used by later claim admission. The controller's
canonical plan publisher no longer depends on it to race a current reporter:
its plan is built from the exact demand generation already locked in the same
transaction.

One bounded paid wave uses one PostgreSQL transaction to:

1. acquire the protocol-observation, lifecycle, service-owner,
   service-envelope, accelerator-frontier, priority, exact-pool, capacity-plan,
   route, demand, and reserved-allocation authority in deterministic lock
   order;
2. validate the immutable plan once per distinct debit card for the sum of its
   accepted candidates, within the same transaction;
3. insert the ordered policy-valid subset as `SCHEDULED` replica rows with paid
   claims and global/pool debits; and
4. commit before any worker is registered or started.

Each intended member has one exact cheapest-first location and is never
reassigned in-transaction. Exact-pool saturation defers that member while later
already-prepared distinct slots may continue; higher-priority or frontier
deferral stops that accelerator card, and service/paid-cap saturation stops the
wave. The next fresh tick may choose another pool only for a never-committed
slot. The batch is clipped at the existing service, card-frontier, exact-pool,
priority, paid-GPU, and cost limits. Process headroom only bounds preparation
advisorially; the later P transaction is authoritative and may leave excess
committed rows `SCHEDULED`. It does not weaken any limit to reach a requested
size. A normal singleton admission is the same operation with one candidate.
There is no durable batch table or historical manifest.

After commit, the existing launch-reservation transaction charges P before
starting manager workers. Each worker then uses the unchanged generic non-pool
binding transaction to create its association, immutable API request, queue
row, and retention pin. Queue visibility at that later commit is intentional
executor activation. The provider guard still exact-matches the complete
replica, claim, binding, request, and execution authority before its first
effect. Fresh complete current load-balancer reports authorize the prospective
paid debit, not a second post-commit lease: report expiry, ingestion blackout,
or HA-role heartbeat advancement cannot revoke the exact committed graph's one
provider effect. The guard still requires an unexpired current route head and
matching lifecycle, source epochs, owner, plan integrity, and bounded debit. A
crash or lost commit acknowledgement between the two phases leaves inert state,
never provider authority. In-process recovery exact-matches only the ambiguous
frozen identities after releasing the manager lock; startup recovery enumerates
them after process death. Both atomically retire and replan association-less
replica+claim pairs, adopt an exact association if binding won the race, and
infer no historical batch manifest.

The current `1.1.1510` deployment implements the atomic batch and accepts
semantic-equivalent HA heartbeat advancement. Its first repeat exposed a later
fence collision: the process-local logical authority used one deadline bounded
both by fresh demand/report validity and by the oldest selected backend
occupancy sample. A busy or slow selected backend could therefore expire
queued additive launches even though the exact-card target, route, and demand
remained fresh. The source candidate carries a separate additive deadline from
the same PostgreSQL read. It keeps the existing occupancy-bounded deadline and
database-clock `valid_until` for retirement, drain, teardown, and every other
destructive commit. Unknown occupancy remains unknown and its associated
supply remains conservatively protected; it cannot be treated as absent or
released for paid residual or scale-down. The existing bounded
`UNKNOWN_CAPACITY_REPLACEMENT` path may independently admit one demand-fenced
replacement after its degradation timeout while the predecessor remains
counted and protected; that path cannot recurse.

Every AWS paid association uses a stable provider idempotency token bound to
the immutable association and exact instance parameters. A lost provider
response is replayed with the same token. Only a typed provider rejection with
complete negative evidence may authorize absence and release the debit; a
timeout, unknown response, partial create, or lost acknowledgement remains
ambiguous and conserves the claim.

Every GCP paid association derives cleanup identity only from the retained
PostgreSQL launch graph: the paid-pool key freezes workspace, region, zone,
shape, Spot market, and node count; the immutable request snapshot freezes the
workspace's GCP project; and the association freezes the generated provider
cluster name. The observer never consults today's ambient workspace. After
exact executor quiescence, it reads that project and zone outside database row
locks and treats the allocation as absent only when the exact generated-name VM
set and SkyPilot-managed boot-disk set are empty, no attributable GCE insert
operation is non-`DONE`, and a second uncached census agrees. Cohort 12 stops
deleting timed-out Compute Operation metadata, because that API call never
cancels the underlying create and destroys reconciliation evidence. A complete
set of exact `DONE` child insert operations can settle immediately; otherwise a
300-second post-quiescence propagation horizon is also required. The new
cohort-12 controller may use this conservative contract to settle retained
cohort-11 rows, while old binaries cannot authorize it. The finite horizon is a
conservative mitigation for a missing terminal child-operation record, not the
formal request-ID/operation receipt proof required by the final steady state.
Presence grants only immediate fenced cleanup; it does not settle or release
the paid debit. Cleanup
idempotently deletes the exact resources, waits for VM disappearance and disk
detach/delete, and then requires fresh VM, disk, and create-operation absence
before PostgreSQL releases the claim and retention pin. A missing retained
request, mismatched project/workspace, unsupported disk identity, provider read
failure, stale controller authority, in-flight create, or incomplete
quiescence remains `UNKNOWN` and fails closed.

### Price and pool feedback

Placement compares current eligible Spot offerings in normalized cost units.
It first chooses the cheapest exact compatible pool that passes account,
region, zone, subnet, image, disk, and provider fences. A typed capacity or
quota rejection closes only that exact pool for a bounded period and permits
the next eligible Spot pool. It does not authorize on-demand capacity.

The dashboard price is diagnostic. The committed paid-pool key, decision-time
catalog evidence, and provider market establish what was selected and billed.

## Probe, launch, and cleanup concurrency

The fleet manager lock protects short in-memory reductions. It must never be a
lease for slow or blocking work.

The required probe/reconcile shape is:

1. capture an immutable opening lifecycle and replica snapshot;
2. perform provider-fenced reads outside the manager lock;
3. perform HTTP, URL resolution, Kubernetes, provider, SSH, join, cancel, and
   readiness waits outside the manager lock;
4. exact-reread current records and apply one short reducer/CAS under the lock;
5. enqueue durable cleanup claims under the lock but execute cleanup outside;
6. carry exact replica record, owner, and worker identity in every completion;
7. discard a stale completion with zero side effects, even if its numeric
   replica ID has been reused; and
8. admit queued launches before or independently of slow drain URL resolution.

Launch and teardown admission use separate bounded counters behind one stable
PostgreSQL transaction-scoped advisory key shared by every API/controller pod.
One short transaction acquires the key on its own connection, reads the global
durable counters once, exact-locks an ordered candidate batch, persists every
admitted row as running, and commits before any worker or provider effect can
start. Connection loss therefore rolls the reservations back and releases the
lock atomically; there is no second-session liveness window and a node-local
file lock is not production authority. Protocol-v2 reserved admission joins
this same transaction so its replica, association, executable request, queue,
retention pin, capacity debit, and counted launch reservation become visible
together. Launches retain the full configured launch parallelism. External
teardowns retain the historical maximum of two lightweight `core.down` workers
per launch slot, subject to the per-service teardown cap. Saturating either
direction therefore cannot starve the other; normal scale-down, whole-service
cleanup, failed-service purge, and orphan purge all use this gate rather than
independent per-row loops.

An active launch worker may call `core.down` inline to clean a partial provider
attempt before retrying. That cleanup remains charged to its launch reservation
for the entire call and does not recursively acquire the teardown budget. Thus
external teardown is bounded by `D = 2P`, inline launch cleanup by `P`, and the
absolute simultaneous `core.down` bound is `D + P = 3P`; the launch reservation
cannot be released until the inline cleanup returns.

The current deployed release does not yet satisfy this completely. The source
candidate in `fix/serve-probe-scale-convoy-v3` moves the remaining probe,
preemption, thread refresh, URL resolution, and completion paths across this
boundary. It remains unmerged and unproven in production.

Launch and down workers use per-worker immutable identity and cancellation
tokens, not a shared numeric-ID flag. Completion must exact-match the current
record and owner before changing a row, paid feedback, placement state, route,
or cleanup authority.

The disposable outer guardian is the PID recorded by the durable request
claim. After it closes the raw controller-capability descriptor and before it
publishes `READY`, it must set the `SkyPilot:executor:guardian:<pid>` process
title. Exact cancellation continues to require the matching PID birth ticks,
the executor title, and direct parentage to the owning server process before a
pidfd signal is sent. The title identifies the already-authorized guardian; it
does not weaken process ownership or expose the capability.

## Recovery and retirement

- Controller restart reconstructs demand, route, allocation, intent, claim,
  request, and retention ownership from PostgreSQL.
- A replacement controller uses a new controller incarnation and owner epoch;
  the service incarnation remains unchanged unless the service itself is
  recreated. Stale predecessor reports and worker completions cannot mutate
  the successor.
- A reserved Pod reclaimed by research is removed from spendable capacity on
  the next fresh observation and follows normal exact-identity cleanup.
- A paid replica becomes retirement-eligible from fresh durable idle demand,
  not from a controller-local empty queue.
- Graceful drain first removes routing, then waits for exact asynchronous
  occupancy within its bounded cap, then tears down the provider resource.
- Once that bounded functional drain settles, provider termination runs in a
  fresh logless context. Opening, writing, copying, or synchronizing a local or
  remote diagnostic log is never a prerequisite for the teardown effect.
- A controller `SHUTTING_DOWN` row remains cleanup-conservative until durable
  provider/quiescence evidence settles it. It is not proof that a provider
  instance still exists or is billable.
- No replica row lacking quiescence or provider evidence is manually deleted.
  Provider-free pre-effect cancellation may retire only with its exact
  `NOT_STARTED`/`NOT_QUERIED` proof.
- Provider-present cleanup uses the immutable association and provider identity;
  absence is never inferred from a missing SkyPilot cluster row.

PR #1726 added exact cancelled pre-effect retirement. PR #1727 added exact
completed guardian-family reaping. Both are merged and deployed in `1.1.1496`;
the former lifecycle-102 PHX cleanup rows are no longer live blockers.

## Routing, request ledger, and UI

### Route projection

The controller publishes a complete immutable PostgreSQL route generation
after a successful probe round. It binds service lifecycle/version,
incarnation, replica record identity, URL, exact accelerator/count, occupancy
mode, and economic provenance. A partial or ambiguous round publishes nothing.

Load balancers apply one coherent route generation. A new controller starts
fail-closed until it publishes current owner-bound routes; a warm load balancer
may retain only its last already-coherent snapshot during that bounded gap.
Route and request reads make no provider call.

### Exact asynchronous request ledger

A protocol-covered request supplies the advertised ledger protocol, exact
service incarnation, stable execution request ID and semantic intent digest,
and stable job/demand identity.

PostgreSQL allocates the server-owned attempt and revision. Retrying after a
transport ambiguity uses the same execution identity and intent. A different
intent under the same identity fails closed. A read-only receipt lookup cannot
create a row.

HTTP 202/200 admission is not completion. Terminal success requires the exact
ledger state plus the request's durable completion/artifact evidence. The UI
must identify whether its counts are exact or partial.

### UI fields

With a source timestamp and freshness, the service UI exposes accepted/recent,
queued, processing, in-flight asynchronous, rejected, terminal completed and
failed, and unknown/protocol-uncovered requests.

Replica presentation has independent economic (reserved fill, other zero-cost,
paid Spot, paid non-Spot, unknown) and lifecycle (ready, provisioning, shutting
down, cleanup-uncertain, historical) axes.

This prevents a historical `SHUTTING_DOWN` row from being presented as a live
billable Spot instance and prevents an idle zero count from being mistaken for
missing telemetry.

## Core invariants

1. **Conservation:** one physical UID/card GPU can be spent at most once.
2. **Reserved before paid:** committed and pending reserved supply is debited
   before paid residual is published.
3. **Commit before effect:** no Kubernetes or cloud create may precede its
   durable admission graph and exact provider guard.
4. **No reserved spill:** a reserved intent can effect only its exact zero-cost
   Kubernetes candidate.
5. **No on-demand:** paid residual can effect only Spot candidates.
6. **Bounded cost:** every cleanup-unproven paid GPU unit counts against the
   elected global cap.
7. **Scheduler ownership:** SkyPilot consumes, queues, or yields under external
   scheduler policy and never changes that policy.
8. **Exact identity:** lifecycle, incarnation, replica record, association,
   request generation, worker, and provider token fence every side effect.
9. **Fresh authority:** stale, incomplete, malformed, unknown-version, or
   owner-mismatched evidence authorizes no effect that depends on that evidence
   dimension. Fresh load-balancer reports are required to create a prospective
   paid debit; after that atomic commit they are no longer an evidence dimension
   for its one provider effect. Stale occupancy authorizes no retirement, drain,
   teardown, or capacity release, but does not revoke additive work backed by
   independently fresh demand and route evidence while unknown supply remains
   protected. A newer heartbeat receipt is fresh only when the current locked
   reporter set and generation are self-consistent and its normalized demand is
   semantic-equivalent to the immutable plan; byte identity with the older
   receipt watermark is neither required nor sufficient.
10. **No slow lock:** blocking provider, HTTP, URL-resolution, Kubernetes, SSH,
    join, or cancel I/O never runs while holding the fleet manager mutex or a
    PostgreSQL admission lock. Short PostgreSQL rereads and CAS operations are
    intentionally allowed under reducer/admission locks.
11. **Single happy path:** reserved and paid launches share the generic non-pool
    request/executor/recovery path; reserved admission adds no second executor.
12. **PostgreSQL central truth:** EFS, local files, caches, and process memory
    are not control-plane recovery authority.
13. **Multi-GPU completeness:** every visible compatible GPU has its own worker
    slot and must complete fresh work in qualification.
14. **Telemetry is not authority:** UI and status are read-only projections and
    cannot launch, retire, or prove provider billing state.
15. **Teardown is log-independent:** after bounded functional drain, provider
    termination uses a fresh context without an inherited file-log sink and is
    never gated by diagnostic log I/O.
16. **Mutation directions cannot starve:** launch and teardown have independent
    bounded admission budgets, so a full provisioning wave cannot retain a
    billable `SHUTTING_DOWN` resource and a cleanup wave cannot convoy fill.

## Compatibility contract

Compatibility applies to durable state, not permission for mixed binaries to
start fresh provider effects.

| Runtime/cohort | Read and status | Recovery/settlement | Fresh admission/provider effect |
|---|---|---|---|
| N (current) | Yes | Yes | Yes, after homogeneous-writer and current-authority checks. |
| N-1 | Yes for declared additive shapes | Broad retained cleanup/recovery for the exact adjacent cohort | No while the writer cohort is mixed or current authority does not exact-match. |
| N-2 | Exact bounded decode/status | Only already-proven terminal, quiesced, pin-released, provider-`ABSENT`/`PROJECTED` row retirement | Never. |
| Older, newer, malformed, or partial | Fail closed | No guessed repair | Never. |

Current code writes worker projection protocol 10 and non-pool capability
cohort 12. Older readers are retained only for the bounded contracts above.
Decodability alone never grants provider authority.

New durable shapes are additive. A removal waits for a current-row census, no
old writers, and one complete stale/quiescence horizon. New effects require a
homogeneous current writer digest even when N-1 can safely read current state.

SkyPilot is currently test-only. For `boltz-l4-fleet`, old service-state
migration is not an operational requirement: normal evidence-backed `serve
down` followed by fresh `serve up` on the current schema is preferred when it
is simpler. This does not weaken N/N-1/N-2 cleanup rules for any retained row,
and it does not permit manual SQL deletion.

## Deployment and fix-forward operation

### Preconditions

- PostgreSQL is healthy and the central schema is at the current image head.
- Provider and cost guards report zero unexpected paid/on-demand resources.
- A control-plane release may be deployed while the current service still
  excludes PHX. Before PHX activation, the reviewed service definition adds
  only one non-Spot `k8s/phx_research_cluster_eks` H200 candidate to the East
  A100/A100-80GB and reviewed AWS/GCP L4 Spot candidates.
- Read back the exact server-owned PHX projection and existing external lane.
  Do not copy those settings into task-owned Kubernetes overrides and do not
  apply a Kueue or Terraform change.
- Do not rerun the historical Terraform config-seed Job. Its retained
  ConfigMap omits the PHX namespace and replaces `workspaces` wholesale; the
  PostgreSQL server-config row is the current authority. Helm does not run that
  Job.
- `min_replicas: 0`, fill floor 0, `utilization_gate: true`, and the explicit
  paid cap read back from the submitted service version.
- No task-owned namespace, service account, scheduler, priority, Kueue, raw
  Pod config, hostPath, or PVC override is present.
- Helm storage remains disabled.

### Control-plane deployment

Build and publish one immutable image. Upgrade the existing release with
`helm upgrade --reuse-values`, explicitly selecting that same image for API,
controller, and executor roles. Do not update a `boltz-platform` runtime pin.

Before provider effects resume, prove all two API, two controller, and three
executor Pods are Ready on the exact same image digest. If the reclaim-policy
or writer identity changes, invoke the existing PostgreSQL transition command
to authorize one fix-forward generation; an exact retry is idempotent.

The Helm migration inserts PostgreSQL server configuration only when the row
is absent; it cannot overwrite the retained revision. Read back the exact
server-config identity and PHX projection before and after Helm. Service
recreation does not recreate or reseed that central configuration.

### Service deployment

The local reviewed service YAML is deployment authority for this test service.
A clean `serve down`/`serve up` is allowed and preferred over compatibility
migration when a current lifecycle is easier to reason about. Teardown still
must finish through supported evidence-backed cleanup, and callers must refresh
the endpoint after recreation.

### Failure and rollback

The capacity-authority transition is one-way. After activation or an additive
schema commit, repair by deploying a newer homogeneous image and reauthorizing
forward. Do not demote authority, restore old database rows, or roll back to a
binary that cannot decode the current head.

The safe failure mode is underfill: stale or unavailable authority suppresses
new reserved admissions and prospective paid debits while already healthy
coherent routes continue serving. An exact paid debit that already committed
may perform only its one graph-fenced provider effect while its independently
required current route head remains fresh. It is never duplicate capacity or
on-demand spill.

## Verification plan

### Source qualification

- Run formatter/type/lint checks on every changed source file.
- With complete current telemetry and a zero launch-lead seed, prove 1,000
  queued requests of ten seconds each produce the same
  priority/deadline-weighted work in the aggregate and exact-card maps. At a
  600-second timeout and 95% utilization the queue needs 18 logical slots;
  fifty compatible ready slots authorize no additional launch. At a 60-second
  timeout it needs 176 logical slots before other demand terms and hard caps
  are applied.
- Mix constrained A100-only demand with L4-or-A100 demand. Prove the constrained
  profile retains A100 authority while flexible residual chooses L4 when both
  can meet the same priority objective; higher numeric priority remains the
  explicit first ordering key. Prove no flexible request is counted in two
  card targets.
- Prove an eight-GPU compatible backend supplies eight logical slots and that
  ready, provisioning, reserved, and already-paid supply suppress the exact
  corresponding cold residual.
- Repeat with missing, partial, and N-1 priority/compatibility gauges. Missing
  current semantics must use raw conservative queue work or hold prior
  exact-card authority; it must never publish a discounted guessed-card paid
  plan.
- Run focused probe batching, replica-manager, paid-capacity, reserved
  admission, route, request-ledger, recovery, and teardown tests.
- Run real-PostgreSQL tests for transaction atomicity, lost acknowledgements,
  stale identity, owner takeover, provider rejection/ambiguity, and concurrent
  admission.
- Advance one and both HA reporter generations while a 100-member paid wave is
  frozen and prove one atomic commit when normalized demand is unchanged.
  Repeat with fresh-zero, queue, rejection, in-flight, compatibility, reporter,
  and route changes and prove the complete batch rolls back with zero rows,
  claims, waiters, or pool debits.
- Hold the routing epoch, then hold the linearized planning callback after the transaction has locked the
  current service, demand/report/route, allocation, capacity/Kueue, and plan
  rows. Start both HA report writers and prove they wait at the service row;
  prove the callback sees the exact last committed generation, one plan/head
  commits, and both writers advance normally after commit. Repeat with changed
  demand already committed before the lock and prove the new semantics are
  planned rather than rejected as stale.
- Race a service-version transition against that held callback and prove it
  waits for the routing epoch without a routing/PostgreSQL lock inversion.
  Mutate a replica's normalized state between immutable input preparation and
  the transaction; prove the locked semantic fingerprint rejects the stale
  candidate without invoking the callback or publishing local/provider state.
  Repeat with a PostgreSQL no-op update that advances ``xmin`` while preserving
  the exact normalized document; prove the current plan commits.
- Instrument every PostgreSQL/provider/Kubernetes/HTTP/filesystem and
  replica-manager boundary reachable from the callback to fail if invoked;
  prove current-demand/current-supply planning completes with no I/O. Inject a
  callback exception and a final plan-write failure and prove no plan/head,
  claim, local target, or provider authority becomes visible.
- Begin from a committed zero-demand retirement, advance to exact positive
  demand, and give the planner a prepared snapshot containing that active paid
  retirement. Prove the locked paid baseline still counts the row and commits
  no replacement residual; prove the separate exact-generation cancellation
  commits after that boundary, local publication is skipped, refreshed
  preparation removes the retiring classification, and the next transaction
  publishes exactly one correct residual without double-counting
  cleanup-unproven paid capacity.
- After an exact paid claim commits, expire or remove every demand report and
  prove its bound request may enter provider I/O once while a prospective claim
  still fails closed. Repeat with an ACTIVE-slot/cutover-generation mismatch.
  In both cases an expired or owner-mismatched current route head must still
  reject the committed request before provider I/O.
- Prove no blocking provider/HTTP/URL/Kubernetes/SSH/join/cancel call occurs
  under the manager lock with deterministic race tests; allow only the short
  PostgreSQL reread/CAS critical sections the reducer requires.
- Advance monotonic time by more than one complete claim-authorization
  lifetime during broker-lock admission, prune/overlap work, a real PostgreSQL
  protocol-row wait, a legacy-projection-row wait, and proof logging. Prove the
  callback is not invoked until the transaction owns its complete current
  write set and has reconstructed exact scope; prove the callback can read an
  exact provider-proof receipt on an independent READ COMMITTED connection
  without self-deadlocking on the parent gate lock; then prove the ticket is
  timestamped after logging and immediately accepted without weakening its
  five-second bound. Provider renewal/I/O must never run under these locks.
- Prove a stale same-numeric-ID completion causes zero row, placement, paid,
  route, or cleanup side effects.
- Let selected occupancy expire while demand and route reports remain fresh;
  prove additive row persistence and final cloud admission remain authorized,
  while retirement, drain, teardown, and unknown-capacity release remain
  blocked.
- Exercise the real disposable guardian topology and prove it passes exact
  ownership attestation, drains its complete process family on cancellation,
  and releases its executor lane only after the receipt is acknowledged.
- Do not add SQLite migration coverage for this central PostgreSQL path.

### Post-Helm qualification

- Read back the exact image and chart digest and the 2/2/3 homogeneous role
  inventory.
- Verify PostgreSQL health, storage disabled, no PVC/EFS, bounded controller
  memory/thread count, and continuously fresh proof renewal.
- Verify the active service lifecycle, version, incarnation, authority modes,
  route generation, and both load-balancer slots.
- Verify no Kueue, Terraform, Terragrunt, KubeRay, IAM, or application object
  changed as part of the rollout.

### Reserved-capacity proof

- Reconcile physical East and PHX capacity, research occupancy, SkyPilot Pods,
  Kueue admission in PHX, PostgreSQL intents/replicas, routes, and ready workers
  at one synchronized observation.
- Under positive demand, require every logical target unit to be backed by a
  compatible ready, committed, or scheduler-admitted reserved unit before a
  paid residual appears. Waiting PHX Pods remain visible as waiting and do not
  count as spendable or admitted capacity.
- Under sustained zero demand, require the utilization-gated fill claim and
  its fill-origin replicas to decrease according to the bounded dwell/step
  contract and converge to zero without a paid launch. It is correct for
  compatible research GPUs to remain free in this state.
- Continuously inspect pending GPU research Workloads during qualification. If
  one remains quota-blocked while PHX fill holds admissions that unchanged
  Kueue does not promptly reclaim, disable PHX fill and record the scheduler
  limitation; do not mutate Kueue or synthesize victims.
- Prove all eight logical workers can pack on one healthy eight-GPU node and
  each device completes fresh accelerator-attested work.
- Delete no Pod or row merely to make totals align.

### Paid Spot provider-lifecycle proof

Complete on 2026-08-26 with release `1.1.1513`. The disposable GCP-only service
used a fixed floor and ceiling of 120, `max_live_paid_gpu_units: 120`, a hard
guard cap of 120, and only Spot `g2-standard-4` with exactly one L4. The fixed
update completed at 18:23:39.277 UTC. Five prospective transactions rolled
back safely because the concurrent traffic writer changed demand semantics;
the sixth committed all 120 replica and paid-claim debits atomically at
18:25:12.183. Provider-native GCP observations reached 100 concurrently
`RUNNING` at 18:28:54.100, 3 minutes 41.9 seconds after the debit commit and 5
minutes 14.8 seconds after the update. Later fresh samples observed 107, 110,
114, and a peak of 117 at 18:29:25.311. Every provider object remained the
approved one-L4 Spot shape and on-demand remained zero.

The normal `sky serve down` request completed at 18:30:04.135 UTC. Native
`RUNNING` reached zero at 18:35:00.512, and the service, PostgreSQL paid rows,
claims, waiters, request associations, queue rows, pins, cluster bookkeeping,
GCP VMs, and managed disks all reached zero at 18:35:39.315. Guard samples at
18:35:39.315, 18:36:19.373, and 18:36:57.577 stayed exact zero, and separate
PostgreSQL and GCP all-state censuses agreed. Teardown used no direct provider
delete, database-row delete, or executor restart.

The run attempted 10,000 stable synthetic IDs at concurrency 256 only to
provide bounded lifecycle stimulus. It did not run the production model and is
not terminal-ledger or reserved-serving evidence. No Kueue, Terraform,
Terragrunt, KubeRay, IAM, `boltz-platform`, EFS, or PVC object changed. The
service remains absent and the isolated launch window and long success TTL were
restored after qualification.

For a repeat, start the provider/PostgreSQL guard before raising the floor,
retain the 120 hard cap and exact Spot shape, and require at least 100 native
VMs concurrently `RUNNING` followed by normal exact-zero teardown. For a
fixed-floor lifecycle qualification, commit the fixed target before starting a
volatile traffic writer. Longer term, investigate a decision-output
fingerprint that includes the effective target, exact-card allocation,
compatibility, launch priority, and waiter-fairness result. A telemetry change
may avoid invalidating a fixed-floor wave only when every one of those outputs
is unchanged; any priority or fairness change must reject. Fresh complete
reporter, route, supply, cap, and ownership evidence, and every change that can
alter any decision output, remain fail-closed. This is not permission to weaken
demand-driven zero-demand revocation.

### Broader mixed 10,000-request proof

This is an open terminal-ledger and mixed-capacity campaign, not a rerun or a
condition of the already-complete paid Spot provider-lifecycle gate.

1. Start from the synchronized East-plus-Kueue-admitted-PHX reserved census,
   zero paid claims/waiters, and empty AWS/GCP provider inventories.
2. Arm a fresh bounded run with a positive Spot-only paid cap and 10,000 new
   stable logical request IDs.
3. Verify both load balancers advertise the same ledger protocol and service
   incarnation before submission.
4. Fill and refill the authenticated reported queue capacity. Honor
   `Retry-After`; retry definitive pre-admission rejections with bounded
   backoff, and reconcile ambiguous outcomes by exact receipt lookup.
5. Require paid claims and provider Spot units only for demand remaining after
   every current compatible reserved worker is committed first.
   The convergence target is the configured paid cap or the lower
   provider-available limit, not an instantaneous exact count of 100. Require
   bounded overshoot/undershoot to reconcile within a few minutes and record
   time-to-limit from the atomic paid-wave commit.
6. Require zero ordinary on-demand instance, zero paid non-Spot row, and no
   provider action outside the armed service/run envelope.
7. Record selection-time pool, instance shape, normalized price, provider
   request, and actual accelerator evidence. Verify cheaper eligible pools are
   attempted before more expensive ones unless typed availability feedback
   closed them.
8. Finish only when all 10,000 logical IDs have exact terminal ledger receipts
   and durable completion/artifact evidence, with no ambiguous or unsettled
   tail.

### Natural-drain proof

- Stop submitting demand without lowering the positive paid cap.
- Require queue, processing, in-flight, claims, and waiters to converge to zero.
- Require every paid route to drain and every provider resource to terminate.
- Check PostgreSQL, AWS, and GCP immediately, at +10, at +30, and through one
  complete stale/quiescence horizon.
- For AWS, inspect instances, open Spot requests, and every tracked EBS volume;
  for GCP, inspect instances and disks.
- Require the UI to separate historical/shutting-down rows from current
  billable provider resources.

### Restart and takeover proof

- Restart one service-controller child and then replace one controller Pod.
- Require a new controller incarnation/owner epoch but the same service
  incarnation, lifecycle, version, and routes; also require no duplicate
  provider effect, renewed proof receipts, and continued request telemetry.
  Only explicit service recreation may change the service incarnation.
- A stale predecessor worker completion must be ignored.

## Evidence to retain

Each final run retains one compact immutable evidence bundle: source,
image/chart and service-YAML digests; Helm revision and exact role inventory;
lifecycle/version/incarnation/authority generation; synchronized reserved and
worker/device proof; request manifest and ledger/artifact census; paid
claims/pool decisions and provider census; and all drain-horizon receipts.

Do not paste raw logs, credentials, signed URLs, request payloads, or repeated
minute-by-minute chronology into this design. Link the durable evidence bundle
and keep only the latest result in the current-state table.

## Remaining gates

The current lifecycle has completed the synchronized reserved-capacity gate:
East is 328/328 occupied and PHX is 512/512 occupied, with the latter limited
to the 30 fleet GPUs admitted by unchanged Kueue policy. The independent paid
Spot lifecycle gate is also complete. Neither gate requires reserved workers to
execute the Spot lifecycle stimulus. Re-run the paid lifecycle gate only if a
later source change affects paid launch admission or teardown behavior.

1. Reconcile exactly 10,000 terminal logical requests with no ambiguous,
   missing, failed, or unsettled tail.
2. Capture a nonzero service-UI sample for queued, processing, in-flight, and
   completed requests, then a fresh idle-zero sample after drain.
3. Restart/take over the service controller and one controller Pod during the
   final load campaign; prove the same service incarnation, no duplicate
   provider effect, renewed receipts, and uninterrupted telemetry.
4. Re-audit transitional cleanup PRs against current source. PRs #1619 and
   #1633 are still stacked on historical feature branches; PRs #1660 and #1556
   are currently dirty against `improvements`. Rebase or replace only the
   still-required deletion changes after the exact retained-row and
   stale-writer census; do not merge the old branches as-is. In the same
   cleanup scope, evaluate replacing volatile prospective telemetry equality
   with the complete decision-output fingerprint investigation described
   above.

The clarified paid provider-lifecycle objective and synchronized reserved-fill
objective are complete. The broader heterogeneous production objective remains
open until gates 1–3 pass. Gate 4 is separately required before declaring
transition-code and architecture cleanup complete; it does not authorize
expanding the Kueue or infrastructure scope.

## Rejected alternatives

- Mutate Simone's Kueue topology, add a SkyPilot-owned lane, infer entitlement
  from nominal quota, or treat Pod priority alone as a reclaim contract.
- Restore EFS/PVC state, platform dispatch logic, infrastructure expansion, or
  per-provider/accelerator services.
- Let reserved spill to paid, Spot spill to on-demand, or retries mask a lock
  convoy.
- Treat UI lifecycle as billing truth or delete state without exact evidence.

## Historical record

The pre-compaction 9,311-line implementation and incident record is available
with `git show a700ef02:docs/designs/serve-multi-pool-reserved-capacity-fill.md`.

That revision is historical evidence, not a second design, rollout runbook, or
source of current production state.
