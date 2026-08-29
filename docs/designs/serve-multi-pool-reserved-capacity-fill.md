# SkyServe multi-pool reserved-capacity admission

Last updated: 2026-08-29

Status: **in progress**. The single PostgreSQL-authoritative reserved-capacity
path has proved full East occupancy, reclaim, synchronized post-fix East/PHX
convergence, and a complete paid Spot lifecycle. PR #1792 merged the stable-v3
demand witness at `0c2dfa3c6a780073635e30abe4bcbff83eae1143`. Release
`1.1.1561` is deployed homogeneously at Helm revision 675 and public API 93,
with image digest
`sha256:12179be463ceed190898a19c0ece886b242a6970ec003ab17cfb53deb8f24b5a`
and chart digest
`sha256:6c7df416d90254906a82a6c6deeb5af1e088cc739eaef6c062b6ca43fff8833b`.
The hard schema-3 cutover occurred with no Serve rows, Helm storage disabled,
and no SkyPilot PVC or EFS. Full idle occupancy is no longer a steady-state
goal:

The 2026-08-28 amendment makes multi-node physical paid accounting explicit.
Capacity-planning envelope schema 3 carries one task-authoritative
``backend_num_nodes`` scalar beside the exact per-node accelerator shape;
total provider debit is derived as their product. It also advances the demand
witness domain to v2. Schema-3 decoding is strict-current: schema-1/2 plans are
not a mixed-rollout compatibility path and no database migration rewrites them.
The source merge and homogeneous empty-state cutover are complete. Live
multi-node provider-effect qualification remains a separate evidence gate.

Lifecycle 138 then exposed a utilization-gate witness livelock. One exact A100
request remained queued while the broker repeatedly published settled A100 and
A100-80GB grants, but the planner created no intent. The v2 witness had hashed
the load balancer's five-second ``remaining_seconds`` countdown and mutable
empirical work values. Each equivalent heartbeat therefore invalidated the
allocation that had just settled. The v3 witness is a typed
decision-equivalent projection: lifecycle/fill-policy scope, physical capacity
shape, normalized priority/compatibility request classes, aggregate target,
and exact-card attribution. Countdown, FIFO sequence, and empirical work are
excluded because their capacity consequence is already represented by the
target and attribution. Deadline counts are summed by priority and compatible
card set, so bucket repartitioning is also identity-preserving. A changed
priority, compatibility class, deadline-class count, target, card attribution,
service lifecycle, or fill policy still changes the witness. Ordinary
empirical count/work changes remain equivalent only when the reduced target and
exact attribution are unchanged.

The no-effect broker read also rebinds a fresh committed acquisition plan
across heartbeat-only demand-generation advances after reconstructing and
comparing normalized demand and exact route context in one PostgreSQL
snapshot. Fresh zero, changed normalized demand, a route change, expiry, or an
ownership/version change still returns no witness. Provider authority is not
relaxed: only a later current plan bound to the matching settled allocation can
commit an intent or paid claim. Source and PostgreSQL regression tests preceded
deployment. Lifecycle 139 then proved that the stable witness reaches the
broker, but exposed a separate card-budget defect: H200 and A100-80GB consumed
the aggregate budget's two discovery units while an exact A100 request received
an A100 edge cap of zero. No provider launch, intent, or paid claim was created.
The exact-card pool-budget correction advances that stable semantic domain to
v4 by binding the typed reservation-acquisition projection itself. Production
qualification remains open.

V2, v3, and v4 witnesses intentionally have no mixed-binary compatibility path.
Lifecycle 138 reached exact zero before API, controller, and executor roles
deployed the v3 image homogeneously and lifecycle 139 was recreated. The v4
successor uses the same exact-zero, homogeneous-image rollout contract. A
mixed cohort fails closed as underfill, but is not an accepted rollout state.
The allocation reader still recognizes the two retained pre-v4 JSON field
sets for teardown and diagnosis. That is read-only compatibility: neither
shape contains the exact acquisition projection, so it cannot settle a v4
demand gate or authorize a new reserved or paid provider effect. A current v4
writer replaces it on the next homogeneous reconciliation.

Immediately before that cutover, release `1.1.1554` lifecycle 137 reached
exactly 100 provider-`RUNNING` GCP Spot one-L4 workers with zero ordinary
on-demand and zero wrong-shape capacity. All 10,000 authenticated warm requests
returned first-attempt HTTP 200, and normal down converged service, replica,
claim, waiter, VM, and disk state to exact zero. The cold run took roughly
9.5 minutes because durable recent-failure/cooldown evidence limited many
pools; clean pools did use the configured base window of four. Count,
no-spill, warm-request, and teardown evidence are therefore complete for that
writer, while the 3--5 minute cold-frontier objective remains open and is not a
reason to weaken the durable cooldown fence.

reserved capacity is now demand-driven and returns through the existing
utilization gate when idle. The historical paid provider-lifecycle gate was
completed on release `1.1.1513`, PR #1744, merge
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
nor evidence for that gate. The current writer requalified scale-out on release
`1.1.1540`: one clean GCP-only lifecycle reached 105 concurrently `RUNNING`
one-L4 `g2-standard-4` Spot VMs and later peaked at 109, while provider-native
censuses remained zero for on-demand and wrong-shape instances. A fresh
256-request wave moved the native census from 71 to 105 in roughly two minutes.
Normal `sky serve down` then reduced 109 running VMs to one in 4 minutes 43
seconds, but the last exact retained association exposed a missing
`SERVICE_JOB_IO` provider-reconciliation authority. PR #1772 added Serve064 and
release `1.1.1541` deployed it on Helm revision 659. The supported recovery
path issued the exact GCP VM delete at 16:32:18 UTC, GCP completed it at
16:32:40, PostgreSQL projected canonical `ABSENT` and released the retention
pin at 16:32:49, and the firewall was deleted next. Service, replica, claim,
waiter, pin, cluster record, VM, disk, and unfinished-operation counts remained
zero through the five-minute horizon. The terminal request and projected
association remain inert 60-day audit tombstones, not capacity or billing
authority. The current writer's scale-out, no-on-demand, and exact-zero
teardown are production-proven. The final mixed reserved-plus-paid load
campaign, terminal request reconciliation, UI proof, and takeover proof remain
open under the broader heterogeneous objective. Lifecycle 116 used only PHX's
existing externally owned Kueue lane
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

A clean lifecycle-124 calibration on release ``1.1.1541`` exposed one final
reserved-before-paid causality gap in that demand-driven path. Two hundred
fresh requests arrived while the last authenticated allocation still described
the preceding idle utilization sample: all three compatible physical pools
reported 193 spendable GPUs, but the service-wide utilization ceiling and all
grants were still zero. The locked paid planner treated that internally
consistent allocation as current supply authority and committed two L4 Spot
replicas. The next broker cycle observed the demand, raised the raw
entitlement, and began A100/H200 reserved admission. This was neither provider
scarcity nor Kueue rejection; it was a missing causal dimension in allocation
schema 5.

Allocation schema 6 closes that gap without adding another scheduler or
capacity path. Each authenticated service-wide map also binds the exact
utilization-gate sample that produced its ceiling (non-blind demonstrated need
and boot state) and a service-wide ``upward_grants_settled`` bit derived from
every locked pool round's raw and damped grants. For positive compatible paid
authority, the current locked demand callback recomputes the same demonstrated
need. The map is usable only when its non-blind sample dominates that need and
every raw upward entitlement has reached the conservative fill grant. An idle,
blind, or first-upward-round map therefore publishes no paid residual; the
existing poller advances the one canonical broker path and reconciliation
retries. Once settled, the existing exact-card allocation tail is debited
before paid capacity, so there is no speculative raw-capacity projection and
no duplicate fairness implementation. Static fill keeps the same allocator;
downward damping may conservatively suppress paid but never authorizes it.

The allocation JSON is the only durable shape changed. There is no new table,
column, EFS state, provider call, Kueue object, or platform dependency. A
schema-6 writer treats the immediately preceding schema-5 map as stale and
replaces it under the existing allocation-generation CAS; it never uses that
map for a new provider effect. A schema-5 writer rejects a schema-6 map and
therefore also fails closed during a rolling deployment.

Release ``1.1.1542`` deployed schema 6 on Helm revision 660. A fresh
200-request idle-to-burst calibration proved the stale-map guard itself: while
the locked queue was 200, the old idle and first-upward allocations repeatedly
suppressed paid admission and PostgreSQL retained zero paid claims. The same
run then exposed a narrower semantic mismatch. The SLA planner selected 28
logical replicas, but ``FillDemandSample.demonstrated_need()`` represented only
2.85 concurrency-work units and authenticated a utilization ceiling of four.
After that smaller ceiling settled, the paid planner correctly treated only
four reserved grants as committed and attempted the remainder on Spot even
though the same allocation observed 185 compatible spendable GPUs. This is
not Kueue or provider scarcity; the utilization gate and paid planner were
using different demand units.

The canonical demand witness is therefore the maximum of current concurrency
work, busy/pre-ready fill holdings, and the current SLA-selected logical
replica target. The target is already computed from the same locked durable
demand snapshot inside the paid-plan callback; it is not a second planner or
speculative capacity. A compatible positive paid plan additionally requires
the authenticated utilization ceiling to cover that current target. The
poller then publishes the same target through the existing claim, the existing
one-sided release governor raises immediately, and the existing damped broker
rounds settle real grants. If compatible physical supply is smaller than the
target, settled grants debit that supply and only the genuine remainder may
become Spot. No allocation schema, database migration, Kueue object, or
provider contract changes for this correction.

Release ``1.1.1543`` deployed that shared witness on Helm revision 661. Its
fresh 200-request calibration reached a schema-6 allocation with demonstrated
need 28, ceiling 35, settled grants 35, and zero paid claims while the same
round observed 240 compatible spendable GPUs. The transaction then selected
the correct compatibility-aware economic target: eight existing A100 replicas,
eleven additional A100 slots, and nine A100-80GB slots covered all 28 logical
replicas with a zero paid residual. Local logical actuation nevertheless kept
the pre-economic request-class target of eight A100 plus twenty L4. It launched
and served on the eight A100s, but could neither launch the remaining exact-L4
target on reserved A100-80GB/H200 nor spend paid authority, because PostgreSQL
correctly authorized no Spot residual. This is the remaining utilization gap:
the transaction commits one economic target while the replica manager actuates
a different pre-transaction target.

The committed economic target is therefore also the sole local actuation and
retirement target for that demand generation. After PostgreSQL returns the
exact capacity-plan authority, the controller carries the transaction-selected
card counts and exact paid residual into the same-generation logical scale-up,
published logical target, and scale-down fences. Configured accelerator shape
names are preserved; compatible reserved cards receive the priority selected
from the same locked request snapshot. The pre-economic target remains only
diagnostic input in ``normalized_demand``. No second compatibility allocator,
table, migration, provider path, scheduler policy, or service-version path is
introduced.

That target is one frozen ``LogicalCapacityTarget`` domain object everywhere
after autoscaler planning. There is no three-field/five-field tuple union,
length-based dispatch, magic positional indexing, or controller-local legacy
variant. The object validates exact accelerator counts and shapes at
construction, so an invalid state cannot enter the manager. The controller's
seven-field reconciliation result is likewise one frozen named plan rather
than an ``Any``-typed positional tuple. Compatibility decoding, when an actual
external or persisted old representation must be supported, belongs at that
boundary and must produce the same canonical object before core logic runs;
this test-only deployment has no retained logical-target representation that
requires such a decoder.

Release ``1.1.1544`` deployed that typed target and the economic-target
actuation correction on Helm revision 663. A fresh 200-request exact-ledger
calibration committed an 18-unit A100 economic target with zero paid residual,
and the controller launched additional reserved A100 replicas instead of the
pre-economic L4 request-class target. All 200 identities were admitted; the
deliberately failing application payloads drained in 234.9 seconds, so this was
an actuation-boundary calibration rather than the final terminal model proof.

The natural drain then exposed one remaining conflation. PostgreSQL correctly
committed fresh aggregate zero while eleven A100 replicas were ready. The
autoscaler separately retained a temporary five-unit local target under the
configured 300-second downscale hysteresis and 50-percent retirement-wave
limit. Requiring the zero demand target and the retained actuation target to
have the same aggregate suppressed the entire local publication and retirement
wave. It also left the prior positive logical launch fence installed, allowing
already-scheduled replacement launches to appear after demand had drained.

Committed demand and bounded retained actuation are distinct domain concepts,
not schema versions or competing authorities. Under positive demand they are
identical after economic placement. Under authenticated fresh aggregate zero,
the PostgreSQL demand target and paid residual remain exactly zero, every
scale-up option is removed, and local retained actuation is clamped to the
smaller of the autoscaler's hysteresis target and already-committed logical
capacity. Its exact-card map may select only that existing committed supply;
unmaterialized reserved grants and every paid location are excluded. Publishing
the new retained target revokes every prior positive launch fence immediately,
while same-generation scale-down decisions may retire the excess down to that
target. Each later bounded wave recomputes from the smaller committed fleet
until both retained actuation and physical capacity reach zero. A target
divergence is valid only for this typed fresh-zero retention state; any positive
committed demand mismatch remains fail-closed.

Release ``1.1.1545`` deployed the fresh-zero correction on Helm revision 664
using immutable image digest
``sha256:2f9b4cc46c0914e919f8ffb08e56b24b075f6bf87594f2e602f8a3d8e2820c8a``.
The live fleet converged through bounded retained-capacity waves from eleven
ready A100 slots to five and then two without creating a new replica row or
provider request after the fresh zero snapshot.  A subsequent exact encrypted
model request reached a durable ``SUCCEEDED`` receipt.  The interrupted mixed
qualification retained 165 accepted identities; all 30 identities whose local
harness acknowledgement was interrupted were reconciled by exact request and
intent identity to durable ``SUCCEEDED`` receipts, leaving zero ambiguous
accepted work.

That qualification also reproduced a planning-topology defect independently
of provider or scheduler availability.  One fresh snapshot produced 12 units
of compatibility-attributed traffic demand and three units of zero-cost-only
local padding; a later snapshot produced 25 traffic units and ten padding
units.  The aggregate/local targets were therefore 15 and 35 while the exact
demand maps correctly summed to 12 and 25.  Code that required every projection
to have the same aggregate treated these valid distinctions as incomplete and
suppressed actuation.  The same tick could additionally recompute economic
placement after temporarily replacing Kueue state and restoring warm-retention
state.  This is a controller modeling defect, not Kueue rejection, GPU
scarcity, or a reason to let padding create paid demand.

The steady-state correction is one deterministic capacity-planning pipeline.
An impure adapter snapshots live controller state exactly once into a deeply
immutable, keyword-only ``CapacityPlanningSnapshot``. One pure
``plan_capacity`` call returns one typed ``CapacityPlanCandidate`` with
distinct named projections; its closed ``CapacityPlanningEnvelope`` is
persisted, and only the exact committed candidate becomes a
``CommittedCapacityPlan``:

- traffic/economic demand and its compatibility attribution;
- compatible reserved capacity committed to that demand;
- zero-cost-only padding;
- desired and wave-limited local actuation;
- warm retention and retirement targets;
- paid residual and cold-launch authority; and
- attribution completeness, infeasible work, generation, and fingerprint.

For a central logical Concurrency service, ``DURABLE_FEED`` is the only
supported demand owner and this pipeline is the only planner. Missing or
explicit legacy demand authority fails closed and requires recreate/promotion;
it never falls back to the mutable controller calculation. The older private
calculation remains temporarily only for physical Concurrency and other
service classes that have not been ported. After production proof, its
logical-only branches and helpers are deleted while the physical/QPS behavior
is retained. This is a scoped transition boundary, not two valid logical
happy paths.

The 2026-08-28 post-merge audit at
`3088071b0c64000893cbd228d9ca56a3ad987a76` found this contract already
source-complete. The durable adapter calls the canonical planner exactly once,
does not install candidate policy state before commit, consumes locked Kueue
classes through immutable decision inputs rather than borrowing the
process-local per-tick fields, and does not temporarily clear or restore warm
retention. Reserved supply and both ``utilization_gate`` modes enter the same
``ReservationPlanningInput`` and the same planner. The audit therefore does
not introduce a duplicate allocator, alternate plan record, provider path, or
database migration. The remaining work is qualification and eventual removal
of the explicitly scoped legacy logical helpers after its documented gate, not
a second implementation of the durable planner.

The durable logical service is also single-version for this initiative. If
the locked replica/intent graph contains any nonterminal prior service version,
planning fails closed with an explicit recreate-required result: it grants no
launch, retirement, or provider authority. Operators use ``sky serve down``
through exact-zero and then create the new service lifecycle. This is the
accepted contract while SkyPilot is test-only and short interruption is
acceptable. It avoids embedding a second N/N-1/N-2, cross-card, multi-GPU
replacement state machine inside capacity planning. The existing generic
physical/QPS rollout behavior is not changed. If zero-downtime rolling update
for durable logical services becomes a requirement, it needs a separate
design and proof rather than compatibility branches in this planner.

Reserved supply and ``utilization_gate`` (the usage gate) are orthogonal inputs
to this same plan. The snapshot carries an explicit typed gate policy,
authenticated compatible reservation capacity by card, and exact-card capacity
currently eligible under the gate. With the gate enabled, only demonstrated
compatible demand may turn reservation headroom into actuation; an aggregate
L4 witness cannot admit unrelated A100 or H200 fill.

A clean gated service needs an explicit two-phase result from that one planner.
Without it, the controller has a causal cycle: a normal positive plan requires
a settled allocation, while the broker needs the plan's current SLA target to
create that allocation. When compatible or flexible demand is positive and the
locked allocation is absent, blind, smaller, or unsettled, the planner returns
the tagged ``GATE_ACQUISITION`` variant. It preserves the immutable demand
attribution and target as a witness, but its provider capacity target,
reservation commitment, reserved launch target, paid residual, paid launch
target, local actuation, retirement, and policy-state installation authorities
are all zero. The repository commits that non-actuating result in the existing
capacity-plan/head tables. No schema or second allocator is introduced.

The reserved-capacity poller may consume only the current committed acquisition
witness (or the current committed normal plan's equivalent target) after
validating its service hash, lifecycle, elected version, controller ownership,
and freshness. The witness has a typed semantic fingerprint over immutable
service/worker policy, normalized compatibility/priority/SLA demand, and the
reduced exact-card target. It deliberately excludes sampled clocks, receipt
sequence churn, allocation evidence, mutable supply/inventory, provider
discovery, and the dynamic route membership/content digest. Equivalent demand
heartbeats and replica-ready/retire route changes may refresh a plan row
without changing this entitlement identity; changed demand semantics, static
worker/card policy, lifecycle, or version changes it. The active plan still
requires the exact current fresh route head/content and locked supply graph
before every effect. Fresh aggregate zero replaces/revokes the witness
immediately.

Acquisition uses a distinct no-effect freshness horizon long enough to cover a
bounded broker poll and upward-settlement cycle; the ordinary 15-second paid
plan TTL is insufficient beside the 60-second reserved poll interval. This
longer horizon never extends provider authority because an acquisition plan
has none. The poller publishes the target and semantic fingerprint to the
existing utilization-gate claim. A missing, expired, malformed, or wrong-owner
witness is armed-but-blind and fails closed. The allocation authenticates the
same witness identity. Once the broker has published a non-blind allocation
whose demonstrated need and utilization ceiling cover the same target and
whose upward grants are settled, the next reconciliation revalidates that
identity under the plan transaction, invokes the same planner again, and
commits the normal reservation-first plan. Only that second plan may create
reserved intents or a genuine compatible Spot residual. Controller restart or
lost acknowledgement replays the acquisition witness from PostgreSQL; mutable
load-balancer or autoscaler state is not a correctness mirror.

The canonical durable broker budget is not the legacy overlay expression
``max_replicas - autoscaler_target``. That expression assumes fill sits above
ordinary demand; here reserved workers are supply *for* the immutable demand
target. Using it would let acquisition grant ``N`` slots, then collapse the
grant after the active plan installs target ``N`` (to zero when
``N == max_replicas``), causing an acquire/active oscillation. In
``DURABLE_FEED``/``DURABLE_INTENT`` mode the service-wide reservation ceiling
is the immutable service maximum. Gate-on tightens it with the current durable
SLA witness and the broker's release headroom; gate-off leaves it available for
static prefill. The pure plan and atomic intent/replica inventory debit bound
actual materialization. Only the retained legacy/direct overlay keeps the
``max - mutable target`` formula.

When the complete positive target is exact-card and proven disjoint from the
immutable reserved-worker projection, it retains the statically-incompatible
exception and does not wait for an unrelated gate allocation. A mixed snapshot
containing any flexible or reservation-compatible demand conservatively runs
the acquisition phase for the whole generation; this may delay an independently
disjoint Spot card by one bounded acquisition cycle, but avoids a second
per-card authority state machine. The subsequent settled plan still reserves
compatible A100/H200 supply before launching the genuine L4 residual. With the
gate disabled, the same planner may consume
the full authenticated reservation envelope under its normal fill policy. The
service setting is the immutable configured policy and the sole gate-mode
authority. There is no process-local override: it cannot be made consistent
across HA writers or distinguished from an older writer's missing activity
fields. In both modes, committed compatible reservation capacity is debited
before Spot residual, padding remains zero-cost-only, and ordinary on-demand
remains forbidden. This is a policy field and a tagged result in one immutable
planner, not a second allocator or controller path.

The required reservation-policy-by-``utilization_gate`` acceptance matrix is:

| Reservation policy | ``utilization_gate`` | Fresh zero demand | Positive demand |
| --- | --- | --- | --- |
| Not configured | N/A | No reservation claim or idle fill | Reuse compatible running capacity, then launch only the compatible Spot residual |
| Configured | ``true`` | Revoke the witness and converge reservation authority to zero | Commit a non-actuating PostgreSQL acquisition witness, wait for the matching settled allocation, then commit compatible reserved capacity before any genuine Spot residual |
| Configured | ``false`` | Keep authenticated zero-cost static fill warm within the configured floor/envelope | Use the same planner to commit compatible reserved capacity before any genuine Spot residual |

Missing, stale, blind, unsettled, or wrong-owner reservation evidence grants no
compatible/flexible provider effect in any row. Only a complete exact-card
target proven statically disjoint from every reservation may take the bounded
Spot exception without waiting for an unrelated allocation.

Gate-off static prefill at fresh traffic zero is an explicit zero-cost fill
projection/plan variant; it is not smuggled through traffic demand, ordinary
padding, or the fresh-zero retention variant. A stale, blind, disappearing, or
unsettled gated entitlement contributes zero eligible new reservation supply.
It does not erase real demand, but it suppresses a new compatible/flexible Spot
effect until a causally covering allocation arrives; only the immutable
statically-incompatible exception is independent of that evidence. Already
committed compatible reservation capacity remains debited from demand under
either gate mode.

Reservation demand debit and reservation actuation are separate typed
projections. A one-slot demand may be covered by one slot of an eligible
eight-GPU worker for economic accounting, but the provider-free launch target
is the whole eight-slot machine. The plan records the one-slot demand debit,
the width-quantized eight-slot reservation launch target, and seven slots of
zero-cost-only packing padding. Paid residual consumes only the demand debit;
post-commit reserved-fill actuation consumes only the whole-machine launch
target. A partial logical debit must never livelock a physical fill planner or
silently suppress Spot when no complete compatible machine can be admitted.
Static prefill and retained existing capacity are orthogonal projections and
may coexist during an idle drain.

Paid demand debit and paid actuation are likewise distinct typed projections.
``paid_residual`` is the logical compatible work left after committed reserved,
pending reserved, and existing/committed paid capacity.
``cold_launch_authority`` is the deterministic whole-backend projection
required to spend that residual. For one logical unit on an eight-GPU paid
backend it grants exactly one eight-slot launch and records seven slots of paid
packing padding. The complete eight slots count against the paid cap and
same-generation pending/retention accounting, so the logical residual cannot
livelock a physical launch or mint a second machine. Paid packing padding is
never demand, is never attributed to a request, and cannot independently
retain a machine after the covered demand drains.

The planner has one authoritative per-node physical GPU width for each
accelerator card and one task-authoritative ``backend_num_nodes`` for the
service version. Logical replicas require ``backend_num_nodes=1``. Physical
backend service units remain one per backend, while the hard paid-cap debit is
``per_node_width * backend_num_nodes``. A CPU-only paid pool has an empty
accelerator shape and a typed zero-GPU debit; its legacy non-planner Phase-A
path remains subject to ordinary claim, pool, service, and provider policy.
The GPU-only immutable planner does not synthesize an aggregate accelerator to
represent CPU. The adapter loads width and node count from the same exact task
and must never obtain either by last-write-wins
iteration over ``task.resources``. A logical service version that offers the
same card at conflicting widths is rejected during
preflight until the public contract is extended with a typed backend-shape
dimension; silently choosing the first, last, minimum, or maximum width would
make reserved and paid debits describe different provider effects. This
service-level invariant is independent of provider inventory observation,
which may still coalesce several queries for the same physical pool.

The immutable snapshot also carries the elected
``max_live_paid_gpu_units``. Existing, pending, and cleanup-unproven paid
capacity across every retained service version is charged before planning a
new launch. Current-version paid capacity is a separate projection: it may
cover current demand, whereas an old-version VM remains a cost debit but must
not suppress a required compatible replacement. The logical
``paid_residual`` remains the observable unmet economic demand, while
``cold_launch_authority`` is the deterministic whole-backend prefix that fits
the remaining paid cap. It may therefore be smaller than the logical residual,
including zero when fewer than one complete backend fits. A later generation
can authorize the next whole backend after committed inventory is re-read; no
generation may persist authority that the paid-cap transaction is guaranteed
to reject.

Reserved retirement is another projection of this same plan, not a
controller-side recomputation. The impure adapter derives one immutable
exact-card shelter from the PostgreSQL-locked allocation and materialized
holdings before invoking the planner. The snapshot carries that shelter and
the candidate carries the composed retirement floor. The controller and
replica manager consume that persisted floor; they do not rerun shelter
composition after planning. Missing or inconsistent shelter evidence makes
the candidate incomplete. It must never fall back to a synthesized zero
shelter, because that converts observation failure into destructive
authority.

Prospective paid cards are closed evidence, not a default. For a service with a
paid placer, the adapter includes only currently discovered locations whose
immutable placement explicitly says Spot. Catalog failure, an empty result, or
an on-demand-only result remains an exact empty tuple through snapshot,
fingerprint, plan, publication, and claim; it never expands to every configured
card. Generic services without a paid placer keep their existing ordinary
Serve policy, while the promoted fleet's final provider guards continue to
forbid on-demand.

Reservation tail packing is accelerator-scoped and whole-backend exact. Before
planning, each allocation tail is clipped to complete physical workers that fit
the remaining service headroom in deterministic policy order. A reservation
worker wider than the remaining headroom contributes zero prospective supply;
it is never partially admitted. Such a non-fitting H200 tail cannot abort an
otherwise valid exact L4 Spot plan, while shared identity, allocation, or
capacity-graph corruption still aborts the whole transaction.

The allocation used by this plan is read only after the PostgreSQL protocol
and service rows are locked. No pre-transaction allocation identity is an
input or publication precondition: broker heartbeats may advance before the
transaction, and the callback simply plans from the current locked map. The
committed plan still binds that exact locked allocation and every later claim
revalidates it. This removes an optimistic starvation race without accepting
stale reservation evidence.

This initiative does not reinterpret Kueue admission. Rows positively owned
by the existing Kueue projection retain its admitted/waiting/unknown classes;
rows outside that projection retain the prior no-override behavior. Planner
immutability must not turn an absent unrelated Kueue row into new scheduler
policy.

Positive demand, fresh-zero retention, and incomplete input are explicit plan
variants rather than boolean-mode combinations.  A candidate plan cannot
authorize a launch; only the same plan committed under the PostgreSQL
generation/fingerprint fence becomes ``CommittedCapacityPlan``.  Policy state
updates are returned as immutable next-state data and applied only after that
commit. Identical snapshots produce byte-equivalent plans. Map-like inputs are
order-independent, while configured-policy, cold-cost, priority, and FIFO
orders remain explicit immutable inputs. Traffic demand is conserved across
its attribution; local
padding is conserved separately and is always excluded from paid residual.

``_allocate_compatibility_target`` remains the only production allocation
kernel. The mutable economic wrapper, temporary Kueue assignment, temporary
warm-retention restoration, and repeated within-generation economic planning
are absent from the activated durable logical path. External or persisted
legacy representations, when supported, are decoded once at their boundary
into the canonical types. Core Serve code must not dispatch semantic variants
by tuple length, use magic positional indexing, or place mutable dictionaries
inside a frozen planning object. A source-architecture test enforces those
constraints
for the capacity-planning/controller/manager boundary.

Adversarial implementation review on 2026-08-27 confirmed that extracting the
pure kernel alone is not the completion boundary. A real logical reconcile
still invoked the nominal planner five times before economic admission, with
five independently sampled planning times. The PostgreSQL callback still
mutated live demand, hysteresis, adoption, generation, and target state before
the transaction committed. A wave-limited cross-card transition also proved
that the pre-wave desired map is not the actuation plan: with 12 A100-only
demand units, three padding units, 15 retained L4 units, and a 50% migration
wave, the valid actuation map was 11 A100 plus four L4, not the controller's
reconstructed 12 A100 plus three L4. The final plan must therefore carry both
desired and wave-limited actuation, including transition retention, and one
immutable ``next_policy_state``. The callback installs that next state only
after commit. These are activation blockers, not follow-up cleanup.

The first deadline-planner production campaign on lifecycle 121 exposed a
remaining violation of that demand-driven contract. Two hundred explicitly
L4-only, priority-zero queue entries correctly produced an L4-only deadline
target and 59 Spot L4 commitments, but the sequenced reserved-fill pre-demand
phase also admitted 17 A100/H200 Kubernetes intents. The utilization governor
had reduced only one aggregate fill budget; its typed fill planner still spent
that budget in physical-pool order before reading the request compatibility
classes. The campaign was stopped and the service was taken down immediately.
No scheduler policy was changed.

Lifecycle 139 reproduced the remaining implementation form after the v3
witness itself stabilized. The sequenced protocol already atomizes every pool
by `(physical cluster UID, exact accelerator card)`, but its service-global
budget still ignored the witness's card semantics. With an exact A100 target
of one and aggregate ceiling two, stable pool order assigned caps to H200 and
A100-80GB and left A100 at zero.

The bounded correction carries one typed immutable exact-reservation
projection inside the committed v4 witness and makes budget authority
explicit:

- `LEGACY` is available only to an explicit non-durable/transition caller and
  preserves historical all-pool water-fill.
- `HOLDINGS_ONLY` retains bounded existing fill but cannot acquire a new pool.
  Missing, stale, flexible, mixed, configured-minimum, floor/fixed-inflated,
  or otherwise incomplete durable evidence always selects this mode.
- `EXACT_SINGLETON` is permitted only when the adopted positive target is no
  greater than raw demand and is explained by singleton request classes
  already bound by the v4 digest. A normal rate-limited upscale wave may use a
  smaller adopted prefix of that raw target; downscale retention above current
  raw demand is not acquisition authority. Each pool must prove one exact
  card. Holdings and new cap on a card are bounded by that card's proven
  target, incompatible holdings receive cap zero, and unused
  aggregate/headroom budget is not transferred to a sibling card.

The bounded exact path currently requires one physical pool per positive
target card. Two pools for the same target card, or a composite physical pool,
remain `HOLDINGS_ONLY`/unsettled until the joint service-by-pool matcher is the
canonical writer. Logical-GPU targets additionally require physical worker
width one; physical-backend targets already count whole workers and may use a
multi-GPU worker. These are explicit fail-closed scope guards, not silent
underfill heuristics.

Demand attribution for a flexible class remains a cheapest-compatible
explanation, not exact-card acquisition authority. General flexible reserved
acquisition therefore remains fail-closed until a single planner result carries
class cardinality into an atomic exact-card matching/grant protocol. A card-set
union or pool-order fallback is not an accepted substitute. An unmatchable
flexible witness publishes neither demonstrated need nor a causal digest, so a
settled zero grant cannot unlock paid residual. For a proven exact-card
witness, partial or zero reserved grants may settle the aggregate/digest gate
only after fresh locked pool observations prove that the per-card discovery
cap covers the target or all spendable supply. Grant settlement alone is not
enough: at final capacity admission, every granted unit must be represented in
the same PostgreSQL lock by usable zero-cost replicas, live pending zero-cost
intents, or currently feedable allocation tail. Only the remainder after that
reserved commitment is genuine paid residual. This final-row fence prevents a
stale-high claim heartbeat from turning peer holdings that are still being
reclaimed into premature Spot authority.

The canonical correction is deliberately smaller than adding a second
per-accelerator entitlement protocol. A service with
``utilization_gate: true`` admits only the exact-card reservation commitment
selected by the ordinary demand planner. A static
``utilization_gate: false`` service uses that same planner and commit boundary;
its candidate may additionally carry a typed static-prefill projection. The
controller has no pre-demand fill phase in either mode. It materializes only
the reservation target returned by the committed candidate, then replans the
paid residual from the new durable holdings. Existing gated fill holdings
remain retirement-sheltered while they drain. Missing, incomplete, or
adjacent-version compatibility telemetry grants no fresh durable-logical
provider authority; it never re-enables cross-card prefill.

The same aborted lifecycle exposed an independent paid-teardown defect. An
ambiguous AWS Spot association with a quiesced failed handler and no exact
zero-effect rejection receipt was sent to the GCP provider observer. The GCP
observer correctly refused an AWS pool with
``missing-immutable-gcp-provider-identity``, but that fail-closed result made
the service teardown retry forever. Paid reconciliation must dispatch on the
immutable paid-pool cloud, never on the profile kind alone. AWS associations
without a negative acknowledgement use their association-derived EC2
``ClientToken``, frozen workspace credential profile, account, region, zone,
instance type, Spot market, and cluster tag for an exact native census. A
quiesced empty census is accepted only after the propagation horizon and two
uncached reads. A matching live instance authorizes exact instance-ID
termination followed by a fresh empty census; mismatched credentials or
allocation fields remain ``UNKNOWN``. GCP keeps its existing VM, disk, and
retained-operation observer. This closes teardown without treating a missing
SkyPilot cluster row as provider absence and without manual row deletion.

The first AWS census correctly returned an exact empty allocation, but the
live PostgreSQL ``serve059_paid_receipt_scope_ck`` still admitted only the
older AWS negative-ack ``receipt`` envelope. The durable census reducer and
the table constraint must recognize the same closed authority. Serve062
therefore widens only that existing constraint to accept
``aws-client-token-instance-presence-v1``. The application reducer validates
the complete association-derived identity; the table constraint independently
rechecks its account, workspace, region, zone, instance type, node count and
Spot flag against the immutable paid pool, plus the canonical client-token and
cluster-name shapes. The legacy negative-ack arm and non-AWS behavior are
retained. No row is rewritten and no evidence is manufactured by the
migration.

Lifecycle 122 on release ``1.1.1537`` exposed two remaining violations of the
same contracts. First, an ``UNKNOWN_CAPACITY_REPLACEMENT`` AWS association
retained the exact Spot pool, request snapshot, account, workspace, placement,
cluster name, and terminal executor-quiescence receipt, but the AWS identity
extractor and EC2 client-token helper admitted only ``ORDINARY_PAID``. The
provider observer consequently returned
``missing-immutable-aws-provider-identity`` even though the replacement uses
the same paid association identity and the native service-tag census was
empty. AWS and GCP now share one capability rule: either paid reconciliation
profile may reconstruct provider identity when its provider-specific immutable
contract is complete. The association UUID remains the EC2 ``ClientToken``
domain, so retry and recovery generations cannot create a second allocation.
This is an additive Serve063 schema capability; it manufactures no receipt and
does not permit an AWS pool lacking version-2 account and exact Spot placement.

Second, the controller treated one stale reserved-fill allocation as global
authority for every accelerator. Exact L4 demand was therefore prevented from
publishing an L4 Spot residual whenever unrelated A100/H200 observer authority
was stale, even though the immutable current worker projection proved that no
reserved L4 pool existed. Reserved-before-paid is accelerator-scoped, not a
global liveness dependency. Under the locked current service version, the
repository derives the closed set of configured reserved accelerator cards
from the immutable worker projection. A positive paid target that intersects
that set still requires and binds the complete fresh broker allocation. A
positive target disjoint from that set binds an immutable
``STATICALLY_INCOMPATIBLE`` authority naming the exact positive target cards
and reserved-card projection digest. It may subtract committed/pending
reserved rows already present for those cards, but it never guesses that stale
compatible supply is absent. Flexible demand or any target with an unknown or
compatible reserved card continues to fail closed. The prospective claim
transaction re-derives and exact-matches the same current projection before it
can authorize a provider effect. This removes cross-accelerator coupling; it
is not a freshness timeout, fallback, or second planner.

Service teardown is likewise failure-isolated per exact association. A closed
``UNKNOWN`` provider observation retains that association, claim, and pin and
marks only its replica as cleanup-failed. It does not prevent known-present or
ordinary cleanup work for independent replicas from running. The service
remains ``FAILED_CLEANUP`` and recovery retries the retained row until exact
``PRESENT`` or ``ABSENT`` evidence resolves it. Identity, lifecycle, controller,
or database authority loss still aborts the whole attempt; only a typed exact
provider-observation uncertainty is isolated. This preserves fail-closed cost
accounting while preventing one ambiguous row from convoying every billable
teardown.

The release-``1.1.1540`` requalification proved that paid provider ambiguity is
not limited to the narrow ``PROVIDER_IO`` phase. Replica 101's GCP VM existed,
the executor was durably quiesced, and the association retained its immutable
request, paid pool, workspace, project, region, zone, machine type, Spot flag,
node count, cluster name, association UUID, and replica-record UUID. The launch
failed only after entering ``SERVICE_JOB_IO`` and before recording a
``service_job_id``. Restricting the exact GCP census to ``PROVIDER_IO`` therefore
returned ``missing-immutable-gcp-provider-identity`` and retained one billable
VM even though its complete provider identity remained reconstructable.

The canonical paid-provider reconciliation phases are consequently exactly
``PROVIDER_IO`` and ``SERVICE_JOB_IO`` when ``service_job_id`` remains null.
Both phases mean a provider allocation may exist but no service-job result was
durably recorded. They use the same immutable AWS or GCP identity, terminal
executor-quiescence requirement, uncached provider census, canonical evidence
payload, and PostgreSQL settlement transaction. No earlier phase is admitted;
no negative provider acknowledgement is inferred from ``SERVICE_JOB_IO``; and
no phase with a recorded service job is widened. ``PRESENT`` still authorizes
only exact provider cleanup, while ``ABSENT`` still requires post-quiescence
propagation and repeat observations before projection and retirement. Serve064
writes no evidence and rewrites no association; it only aligns the PostgreSQL
constraint and trigger guards with this application invariant. The same source
audit removed an accidental AWS-only liveness restriction: the AWS observer
already produced canonical ``PRESENT`` evidence for both paid reconciliation
profiles, but the common cleanup authorizer still admitted only GCP and the AWS
terminator still admitted only ``ORDINARY_PAID``. Both now consume the same
closed profile/pool/phase predicate; account, ClientToken, placement, Spot,
terminal, and quiescence checks remain unchanged.

PR #1772 merged Serve064 as ``4cfccc3e889e44cf4517ae7182adefe152c34070``.
Release ``1.1.1541`` deployed that merge at Helm revision 659 on immutable
digest ``sha256:29f4d700253a79fa92ffbdabba8c0243203eba5f524d8b7ff730fbfcf9be52c3``
across two API, two controller, and three executor Pods. PostgreSQL revision
064 reconstructed the retained replica-101 GCP identity and authorized the
exact deletion through the existing provisioner service account. GCP recorded
the matching VM delete from 16:32:18 through 16:32:40 UTC; PostgreSQL then
committed ``PROJECTED``/``ABSENT``, released the request pin, and removed the
service, replica, claim, waiter, and cluster record. The exact request and
association are intentionally retained as terminal audit history until their
60-day tombstone horizon. Repeated provider-native censuses through 16:38:09
reported zero matching VM, disk, or unfinished operation, with storage still
disabled and no direct provider or database deletion.

Before PR #1758, aggregate queued work applied priority timeout weights while
the exact-card compatibility allocator consumed raw queued profile counts. A
complete compatibility report could therefore raise the aggregate target back
to one logical slot per queued request and erase the timeout weighting. The
deployed implementation now has one queue-work representation: every queued
compatibility profile is converted to work with the same priority timeout,
expected request duration, launch lead, and utilization policy used by the
aggregate target. That exact work is then allocated once across compatible
cards. Generic non-durable service classes may retain a conservative raw-count
decoder, but the recreated durable logical fleet requires the complete current
report and grants no provider authority otherwise. The separate capacity-time
refinement below addresses ready versus cold supply; it does not reopen this
fixed aggregate/exact-card split.

A fresh positive snapshot can follow a committed zero-demand retirement while
the pre-transaction replica snapshot already labels paid capacity terminal or
scaling down. Every cleanup-unproven paid row remains charged, regardless of
controller lifecycle label or service version, until durable
``sky_down_status=SUCCEEDED`` evidence exists. A current-version row also
remains in the usable paid baseline while cleanup is unproven, so the
transaction cannot authorize a replacement for capacity that may still exist
or bill.
Cancellation remains its own service/demand transaction and is not hidden in
the planning callback. After the conservative plan commits, the controller
takes the service lock once, reconstructs the current fresh durable demand,
and atomically cancels the still-active retirement subset when that current
generation is at least the positive generation observed by the planner and is
independently still positive. A newer zero, stale, or incomplete generation
fails closed. This one-transaction wave prevents normal load-balancer
heartbeats from racing one exact-generation transaction per replica. If any
row changes, the controller publishes no local candidate, refreshes
replica/runtime/shape inputs and their fingerprint, and retries. No manager or
provider operation runs under the capacity transaction.

A zero-demand retirement wave freezes one fresh authoritative load-balancer
drain report before admitting its first replica. Every tracker in that wave is
seeded from the same pre-removal snapshot and from the freshness timestamp at
which the wave captured it. Admission persists each replica off-route in its
own exact transaction; re-reading a mutable process-local report after that
commit can observe the route already absent and permanently lose the required
``seen -> clean`` edge. The frozen report is only the ``seen`` half of the
proof. Provider teardown still requires a later fresh zero-occupancy report
from the same LB/HA generation, so a cold replacement load balancer cannot
turn an empty local cache into drain authority. PR #1760 implements and tests
this invariant after lifecycle 119 retired 76 of a 100-replica wave but left
the 24 tail trackers waiting indefinitely because an LB sync landed during
the serial admission loop.

The frozen report is not sufficient unless it acknowledged the exact route
that admission will revoke. Lifecycle 119 later cancelled and restored 16
retirements under fresh positive demand; a new fresh-zero wave could begin
before either load balancer had acknowledged every restored route. A tracker
seeded from that report again lacked the required ``seen`` edge. The canonical
admission now reads the exact current PostgreSQL route leases once, selects
only URLs present in the fresh frozen report, and passes each selected URL into
the retirement transaction. That transaction locks the route lease and
requires its URL to remain byte-identical before it revokes the route and
persists the off-route replica. An absent, stale, not-yet-acknowledged, or
concurrently changed route therefore remains serving and retries on a later
wave. It can neither enter an unprovable drain nor authorize provider teardown.
PR #1761 implements this exact acknowledgement fence.

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
| Source base | `origin/improvements` at PR #1793 merge `65268391a7a6a726cac0288d696ea48f5074f837` (release `1.1.1562`). It includes PR #1765's capacity-time SLA planner, PR #1777's immutable planner, PR #1778's complete sparse target, PR #1779's locked-capacity ordering, PR #1781's retained-route demand correction, PR #1783's processing/in-flight telemetry, PR #1784's plan-authoritative paid cohort, PR #1786's schema-3 multi-node physical debit, PR #1792's stable v3 acquisition witness, and PR #1793's Zurich opt-in S3 endpoint fallback. The source-under-test correction binds the exact acquisition projection in witness v4, budgets exact cards rather than pool order, proves discovery completeness, and rechecks grant realization from final locked service rows. It is not yet merged or deployed. |
| Immutable planner correction | **Merged and deployed homogeneously.** One keyword-only frozen snapshot feeds one pure durable logical planner invocation. Its typed candidate separately records cold demand attribution, supply-aware actuation, warm/transition retention, reservation commitments and whole-backend padding, genuine paid residual and cap-bounded cold-launch authority, completeness/infeasibility, source generation, and snapshot/candidate fingerprints. Policy state installs only after the PostgreSQL commit through a generation-and-fingerprint compare-and-swap. PR #1786 extends the same closed envelope to exact per-node width times task-authoritative node count for physical backends. |
| Deployed control plane | SkyPilot `1.1.1561`, Helm revision 675, public API 93, homogeneous image `sha256:12179be463ceed190898a19c0ece886b242a6970ec003ab17cfb53deb8f24b5a`, chart `sha256:6c7df416d90254906a82a6c6deeb5af1e088cc739eaef6c062b6ca43fff8833b`. All two API, two controller, and three executor replicas were Ready on that exact digest; `/api/health` returned HTTP 200. PostgreSQL remains the sole central store; Helm storage is disabled; no SkyPilot PVC or EFS is present. |
| Schema-3 activation | **Complete from an empty Serve state.** The cutover inventory contained no Serve rows, so no schema-1/2 capacity plan, claim, or provider effect crossed the strict-current decoder boundary. API, controller, and executor roles first moved to homogeneous `1.1.1555` and are now homogeneous on `1.1.1561`. There was no row rewrite, compatibility decoder, storage migration, or infrastructure change. |
| Lifecycle-137 evidence | Release `1.1.1554` reached exactly 100 provider-`RUNNING` GCP Spot one-L4 workers with zero ordinary on-demand and zero wrong-shape capacity. All 10,000 authenticated warm requests returned first-attempt HTTP 200. Normal down converged service, replica, claim, waiter, VM, and disk state to exact zero before the schema-3 cutover. |
| Lifecycle-136 evidence | Run `9462207b-e026-4c5e-b610-acaba61e9b0a` on `1.1.1550` reached exactly 100 provider-`RUNNING` GCP Spot L4 VMs, with zero on-demand and zero non-L4 VMs. It accepted the 10,000-ID continuation and subsequent 5,000-ID extension. Normal teardown reached provider zero in about 3 minutes 16 seconds and full PostgreSQL/provider/disk zero in about 3 minutes 45 seconds. The immutable bundle records SHA-256 `audit.jsonl` `51807331f170d1352e9001324bd2e66f169a8a04867b7ca9bf94d8c4b953a8d7`, `arm.json` `92542d925ad50f0916cd8dcdc3977d27aa7f6a5e27b269445e03b70eadc36e70`, and `guard.json` `54a503e1f83eaa4899bce38bcc254591f885587ba87e81241fe3332a4188a649`. |
| Cold-scale timing | **Count is proven; the 3--5 minute performance objective remains open.** Lifecycle 137 took roughly 9.5 minutes to reach 100 because durable recent-failure/cooldown evidence limited many pools. Clean pools did use the configured base launch window of four. The follow-up must distinguish a clean-frontier benchmark from correctly fenced recently failed pools; it must not erase durable cooldown evidence to improve the number. |
| Telemetry | PR #1783 is deployed in the current source lineage. Lifecycle 137 proves 10,000/10,000 authenticated warm HTTP successes, but no separate current-schema nonzero queued/processing/in-flight/completed UI proof is recorded here. |
| Writer protocol | Public API 93, worker projection 10, deployed and source non-pool capability cohort 13, and async request-ledger protocol 1. |
| Storage | PostgreSQL is the sole central correctness store; Helm `storage.enabled=false`; no SkyPilot EFS or PVC. The schema-3 cutover added no database migration. |
| Service activation | **Active for qualification, not production-proven.** Lifecycle 139, hash `8a3412b2-206a-41d0-8095-12e1135aab75`, was recreated from local provider-only commit `988931e25`: two catalog-driven paid templates (`aws` and `gcp`), the three reviewed reserved cards, `min_replicas: 0`, fill floor 0, `utilization_gate: true`, and paid cap 100 physical GPUs. Its first exact-A100 qualification proved the v3 witness stable but exposed pool-order budget assignment: H200 and A100-80GB received caps while A100 received zero. The attempt left no provider launch, intent, replica, or paid claim. Qualification resumes after the exact-card correction deploys. |
| Paid-location catalog | The two regionless paid templates expand into exact immutable cloud/region/zone/shape pools and remain Spot-only. Zurich (`eu-central-2`) is the only missing commercial AWS G6 region that currently passes the strict publication gate: live L4 Spot offers and prices, a public curated CUDA13 AMI, a successful real Spot boot/workdir/CUDA smoke, and exact teardown are proven. Upstream source PR #10587 and catalog PR #191 carry the surgical source support and 1,127-row v8 slice; publication remains blocked on the hidden catalog-publisher account attesting Zurich opt-in, source release preceding the shared catalog, and identical GitHub/S3 hashes. The Boltz storage-refactor prerequisite is complete in PR #1793/release `1.1.1562`; activation still waits for the canonical public catalog rather than adding a private catalog override. Sao Paulo has live G6 Spot but no supported curated GPU image; Hyderabad has a hosted image but no available opted-in account; Malaysia has neither opted-in-account nor image/launch proof. PRs #10589, #10590, and #10594 therefore remain draft. Apparent local exclusion of `eu-south-2` and `me-central-1` was an unrelated account-specific AZ-map artifact; both already have hosted VM and image rows and must not be duplicated. |
| Reserved occupancy | At 2026-08-26 23:09--23:13 UTC, East had 328 healthy compatible GPUs on 41 nodes: research requested 45 and 283 `boltz-l4-fleet` Pods requested the exact remainder; all 283 were Running and Ready, with zero free compatible GPU and zero pending research or fleet GPU Pod. PHX had 512 healthy H200 GPUs: research held 482 and the unchanged Kueue policy admitted 30/30 fleet Workloads; all 30 Pods were Running/Ready and PostgreSQL `READY`, with zero pending research GPU Workload. PostgreSQL independently reported exactly 63 A100, 220 A100-80GB, and 30 H200 reserved replicas `READY`, with zero durable intent pending. Thus the same lifecycle occupied East 328/328 and PHX 512/512 without changing scheduler policy. |
| Reserved readiness projection | For the final PHX replica, PostgreSQL committed the intent at 22:43:32, the Pod appeared at 22:43:55, Kueue admitted it at 22:43:56, and the Pod became Ready at 22:44:32. PostgreSQL projected it `READY` only between 22:52:25 and 22:52:40, exposing a separate roughly eight-minute status-freshness lag rather than a capacity/admission failure. The post-Helm 23:13 UTC census retained the exact 30/30 admission and readiness with no churn. |
| Claim-heartbeat convergence defect | Resolved in source, deployed, and dark-verified in production. Lifecycle 117 had logged successful exact reclaim-policy proofs followed 7--15 seconds later by rejected claim-set heartbeats because the broker minted the five-second ticket before entering the PostgreSQL replacement and its protocol/lifecycle locks. PR #1750 passes an authorization callback into the state transaction, locks protocol, owner, immutable version/projection, claim-set/edge rows and the legacy projection, reconstructs exact scope, and only then reads already-renewed PostgreSQL proof receipts. Proof logging completes before the ticket timestamp; the ticket is then immediately validated and written. Ordinary drained boundary failures remain fail-closed and boundary ambiguity remains controller-terminal. The correction changes neither Kueue nor TTL/batch/quantum limits. Real-PostgreSQL tests cover waits beyond the ticket lifetime on the affected lock paths. Release `1.1.1519` then produced eight consecutive observed successful claim/publish rounds after controller takeover with no rejected heartbeat. |
| Reserved teardown projection | Complete. PR #1747 projected all formerly blocked associations and retired 194 rows. PR #1748 normalized current writers to the existing immediate-removal marker and accepted only the exact `1.1.1516` `FAILED/FAILED` shape as an N-1 DB-retirement candidate. Release `1.1.1517` plus the supported orphan purge retired the final two rows through exact PostgreSQL authority and independent owner, record, cluster, and Kueue fences. No provider, Kueue, schema, migration, or manual-cleanup behavior changed. Exact service/control-plane/Kubernetes/GCP zero is production-proven. |
| PHX access | The controller identity can exact-read the required namespace/queue and manage only worker Pod/Service lifecycle; it cannot list or patch ClusterQueues. The worker ServiceAccount is tokenless and cannot read Pods, queues, or secrets. A historical audit-only group still has an unused broad Kueue LIST grant from platform PR #8800; it is read-only, has no scheduling effect, and is not used or expanded by this rollout. |
| Paid state at idle | **Production-proven through lifecycle 137.** Normal down reached exact service, replica, claim, waiter, VM, and disk zero, with no on-demand or wrong-shape capacity observed during the lifecycle. This does not turn the roughly 9.5-minute cold scale-out into an accepted performance proof. |
| Routing and queue | Lifecycle 119's low-priority run produced a small deadline-weighted target; the high-priority run increased the target through 49, 64, 128, and 178 before the paid cap clipped it at 100. The bounded stimulus recorded 2,248 submission starts, 289 accepted requests, 252 completion markers, and definitive queue-full rejections/retries; it is not the separate 10,000-terminal-request ledger proof. PR #1765's deployed capacity-time planner uses deadline buckets, exact compatibility, per-card service-time estimates, finite supply availability, and paid cold lead. A fresh current-schema nonzero queued/processing/in-flight/completed UI and heterogeneous capacity-time proof remains open. |
| Partial mixed proof | Provider/DB censuses at 2026-08-25 19:45:47.538 and 19:45:56.281 UTC bracketed a 72-request completion wave and both had 44 reserved plus 28 paid replicas all `READY`, the same 28 AWS Spot instances—27 `g6.2xlarge` and one `g6.4xlarge`—and zero on-demand. The wave completed from 19:45:48.956 through 19:45:51.187; every request performed 9.533–12.451 seconds of concurrency-one GPU work, so at least 28 necessarily executed on Spot beside the 44 reserved workers. The Spot instances later fully drained at the provider. |
| GCP Spot lifecycle proof | **Count, no-spill, warm-request, and teardown evidence exists; cold-scale performance remains open.** Lifecycle 137 on `1.1.1554` reached exactly 100 concurrently provider-`RUNNING` one-L4 GCP Spot VMs with zero ordinary on-demand/wrong-shape capacity, served 10,000/10,000 authenticated warm requests with first-attempt HTTP 200, and completed normal exact-zero teardown. Its roughly 9.5-minute cold run does not meet the 3--5 minute objective. |
| Final load proof | **Warm paid transport is complete; the broader mixed reservation-plus-paid terminal-ledger proof is not.** Lifecycle 137 completed 10,000/10,000 authenticated warm requests at first-attempt HTTP 200. It does not by itself prove the final utilization-gated reserved-first matrix, mixed reserved/Spot execution, current-schema UI telemetry, terminal artifact receipts, or HA takeover. Historical run `final10k-1524-20260827-0246` remains immutable incident evidence and is not rebound to the new schema or service lifecycle. |
| Demand/publication ordering | PRs #1777, #1778, #1779, #1781, and #1784 are merged; lifecycle 137 proves the `1.1.1554` plan-authoritative writer can create an exact 100-L4 Spot cohort, serve the complete warm request set, and tear down without on-demand spill. PR #1786 is merged and carried by the strict schema-3 `1.1.1557` cohort; its live provider-effect qualification remains open. |
| Utilization/allocation causality | **V3 is deployed; the v4 exact-card correction is under test.** Lifecycle 138 reproduced the countdown witness livelock; PR #1792/release `1.1.1561` stabilized and rebound decision-equivalent witnesses. Lifecycle 139 then exposed the independent pool-order budget defect with a stable exact-A100 target. The correction uses explicit legacy/holdings-only/exact-singleton modes, refuses flexible cold attribution or aggregate headroom as card authority, requires complete per-card discovery, and keeps paid closed until the locked service inventory can realize every broker grant. Remaining proof is exact A100 reservation admission, the corrected reservation/gate matrix, current-schema nonzero telemetry, mixed reserved-plus-Spot execution, terminal receipts, multi-node provider accounting, and HA takeover. |

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
- For ``utilization_gate: true``, admit new reserved capacity only through the
  ordinary exact-card demand plan. Do not let the aggregate activity governor
  authorize pre-demand fill on a card absent from the request compatibility
  classes. ``utilization_gate: false`` remains the explicit static-prefill
  contract.
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
- Incomplete priority or compatibility telemetry fails closed for a durable
  logical provider effect. The test-only fleet is recreated on one homogeneous
  current version; it has no N-1/N-2 queue-report operating mode.
- The target counts logical GPU slots. Provider placement may coalesce those
  slots onto a compatible multi-GPU machine, but every device must expose and
  complete one independent worker slot.

### Capacity-time SLA planning (source complete; live qualification open)

The earlier uniform deadline weighting was a safe first-order backlog target,
but it treated already-ready compatible slots as if they also had to launch.
PR #1765 replaced that production path with one discrete capacity-time plan.
The current planner accounts for each finite slot's availability, uses all
timely compatible finite supply before prospective paid capacity, and debits
GPU-time once across strict-priority deadline buckets. It is source-complete
and deployed in `1.1.1557`; current-schema live heterogeneous qualification is
still required.

For one newly arrived batch with ``N`` requests, service time ``s``, dispatch
deadline ``D``, utilization ``u``, and ``R`` already-ready compatible logical
slots, the useful first approximation is capacity-time rather than a uniform
per-request discount:

``demand_seconds = N * s``

``ready_budget = R * D * u``

``committed_budget = sum(max(0, D - eta_i) * u)`` for each already-committed
slot with a bounded ready-time estimate ``eta_i``

``new_slot_budget = max(0, D - cold_lead) * u``

``new_slots = ceil(max(0, demand_seconds - ready_budget - committed_budget) /
new_slot_budget)``

The implementation uses bounded discrete start capacity at deadline
boundaries; the equations above state the policy rather than its exact loop.
With 1,000 ten-second requests, 50 ready slots, and 95% utilization,
the ready budget is already 28,500 GPU-seconds for a 600-second objective, so
the paid residual is zero. For a 60-second objective it covers 2,850 of 10,000
GPU-seconds; with zero additional lead the residual is 126 slots. If cold lead
is at least the remaining deadline, new capacity cannot rescue that batch.
The controller must publish an infeasible-SLA signal and size only for later
deadline buckets, continuing arrivals, and bounded backlog recovery instead of
claiming that a one-slot-per-request launch meets the expired objective.

The exact steady-state planner keeps one physical request queue and enriches
its existing typed profiles; it does not create one queue per accelerator. It:

- reports remaining-deadline buckets by ``(priority, compatible cards)`` so
  queue age is not mistaken for a newly arrived batch;
- learns a conservative service-time estimate per service version and exact
  accelerator card, seeded from configuration, rather than using one
  fleet-wide mean to promise an SLA;
- builds a per-card capacity availability curve from ready and committed
  zero-cost slots, then compatible free reservation, then ready and committed
  paid Spot, and only then prospective paid Spot candidates;
- satisfies cumulative strict-priority deadline constraints with the existing
  scarcity-aware compatibility allocator, protecting A100-only work before
  assigning L4-or-A100 work; and
- minimizes total paid Spot after compatible reserved capacity, including
  retargeting flexible work so surplus paid capacity drains when free
  reservation appears, while retaining the no-on-demand and paid-cap fences.

The queue report carries bounded remaining-deadline buckets rather than a
second scheduling queue.  Each bucket contains only priority, the exact
compatible-card set, a conservative remaining-seconds lower bound, and a
count.  Counts must cover the complete queue exactly under the same routing
version and accelerator catalog as the existing compatibility gauges.  The
load balancer derives the deadline from the request's actual queue timeout and
floors remaining time to a fixed bucket; PostgreSQL receipt time subtracts
transport/report age before the autoscaler consumes it. Missing, partial,
saturated, mixed-catalog, or adjacent-version deadline telemetry cannot
authorize the capacity-time planner or a provider effect for the durable
logical fleet. A current complete report is required after service recreation;
there is no adjacent-version raw-target path for this lifecycle.

For each decision tick the planner consumes one immutable supply curve:

1. ready zero-cost slots, with busy slots delayed by their observed in-flight
   work;
2. committed zero-cost slots with a launch-age-adjusted ready estimate;
3. free reserved slots with the conservative cold-start estimate;
4. already-running and committed paid Spot slots; and
5. prospective paid Spot slots in the placer's current cost order.

The first four tiers are finite.  The fifth remains bounded by
``max_replicas`` and ``max_live_paid_gpu_units`` and is still subject to the
PostgreSQL paid-admission and provider-policy fences.  Within a priority the
planner satisfies earlier deadlines first and protects the compatibility
profile with the worst alternative before flexible profiles.  A slot's useful
budget is continuous capacity-time, ``max(0, D - eta) * utilization``; the
published target is an integer count of slots and every partial final slot is
rounded up.  Capacity assigned to a higher-priority or earlier-deadline bucket
is debited from later buckets, so the same GPU-second cannot satisfy two
requests.

Successful service time is read from the existing PostgreSQL asynchronous
request ledger, grouped by selected worker service version and exact projected
accelerator.  A bounded conservative quantile replaces the configured seed
only after the existing minimum sample and freshness gates.  No new database,
filesystem, EFS volume, or per-request mutable state is introduced.  Legacy or
non-ledger completions continue to inform the aggregate estimator; a card with
insufficient exact evidence uses the configured service-duration seed.  A
committed replica's ETA is the measured conservative launch-to-ready lead minus
its durable launch age, floored at zero.

The planner publishes, in the existing autoscaler projection, the chosen
target by card, demand seconds by deadline/priority, estimator source and
freshness, and infeasible request counts.  ``infeasible`` means no capacity
that can become available before that bucket's remaining deadline can rescue
it; it never means the SLA was met.  Infeasibility does not suppress scaling:
the residual is assigned best-effort through the same economic order, up to
the existing raw-concurrency target, ``max_replicas``, paid-admission, and
provider-policy ceilings.  This preserves recovery for a cold burst whose SLA
is shorter than its measured provisioning lead while making the inevitable
miss visible instead of representing that launch as SLA-compliant.

This replaces the uniform cold-lead queue calculation whenever the elected
load balancers provide a complete current deadline gauge; it is not an
operator-selectable second scaler. The durable logical fleet accepts only a
complete current-version report and produces exactly one capacity-time result.
Request-age buckets, exact-card PostgreSQL service-time reads, finite-supply
ready-time observations, planner integration, and autoscaler projection are
implemented and unit-tested. The remaining gate is live current-schema proof
that the projected target, reserved commitment, paid residual, UI fields, and
provider effects all agree under bounded heterogeneous load.

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
    - infra: k8s/phx_research_cluster_eks
      accelerators: {H200: 1}
      use_spot: false
    # Reviewed AWS and GCP L4 locations follow; all inherit use_spot: true.

service:
  replica_policy:
    min_replicas: 0
    max_live_paid_gpu_units: 120
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

Here ``existing paid capacity`` means compatible current-version demand
supply. Independently, every old- or current-version paid row without durable
cleanup success consumes the paid GPU cap. Keeping these two projections
separate avoids both false suppression of an upgrade and duplicate billable
capacity.

The relational ``sky_down_status`` scalar is cleanup authority and its
ReplicaInfo JSON copy must match. A matched ``SUCCEEDED`` proof removes the row
from both locked planning and Phase-A billing before either seam inspects stale
historical pool, zero-cost, or shape copies; durable cleanup therefore cannot
leave phantom paid capacity. If cleanup is not proven, the relational
``paid_capacity_pool_key`` is authoritative and the JSON pool key and zero-cost
classification must agree exactly, including both missing-copy directions.
Malformed or contradictory cleanup-unproven rows fail closed identically in
locked planning and atomic Phase A.

That logical result is clipped by the elected service target. The pure planner
then projects it to the smallest whole paid backend count per exact card. The
whole-backend ``cold_launch_authority`` (not the raw logical residual) is clipped
by and charged to the elected paid cap together with all running, pending, and
cleanup-unproven paid rows. The plan carries both projections plus exact demand,
route, service, capacity-graph, reserved allocation, accelerator, backend width,
prospective Spot cards, and pool identity.

Reserved allocation authority is required only for accelerator cards that can
consume configured reserved supply. The current version's immutable worker
projection is locked with the service and normalized to a reserved-card set.
For a positive target:

- if any positive target card is flexible, unknown, or intersects that set,
  the plan must bind the complete fresh reserved allocation;
- if every positive target card is exact and the complete set is disjoint,
  the plan binds a ``STATICALLY_INCOMPATIBLE`` authority containing the sorted
  target cards and the current worker-projection digest; and
- a zero target retains the existing unbound revocation authority, because it
  creates no paid provider effect.

The locked inventory still counts every committed or cleanup-unproven reserved
and paid row. Static incompatibility permits no negative inference about a
compatible card and no retirement or reserved cleanup. Claim admission locks
the current version and exact-matches the authority again, so a service update
that introduces compatible reserved supply invalidates the old plan before a
provider effect. The controller has no independent global shelter check; the
PostgreSQL repository is the sole authority for whether the exact positive
target needs a broker allocation.

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
Fresh-positive retirement cancellation is a distinct current-generation batch
transaction after an aborted candidate and before a fully refreshed retry. It
accepts a generation newer than the caller's observation only after
reconstructing fresh positive demand under the service lock; it never runs
inside the callback.

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

For a planner-bound paid call, the immutable plan's
``paid_launch_target_by_accelerator`` is the sole aggregate Phase-A purchase
authority. ``PaidLaunchAuthority`` carries the capacity unit, exact per-node
physical GPU width, and ``backend_num_nodes`` from that same committed planner
candidate; the controller does not reconstruct those values from mutable task
overrides. One physical backend therefore debits one service-plan unit for
``PHYSICAL_BACKEND`` plans but consumes the full
per-node-width-times-node-count GPU cap. A ``LOGICAL_GPU`` backend still has one
node and debits its exact logical GPU width. Candidate preparation first
subtracts durable
claims already debited to the same service incarnation, plan generation, and
content digest. It then converts only the uncommitted plan units into whole
backend claims.

The adaptive exact-pool limit remains independent failure containment, not a
second aggregate demand authority. The cohort opens the minimum canonical
price-ordered Spot pool frontier whose current per-pool backend-claim headroom
can hold the plan-authorized cohort, while preserving the configured minimum
exploration diversity. A closed cheapest pool contributes zero headroom and
the next cheapest exact-shape pools are included only as needed. On-demand and
wrong-width locations contribute none. The legacy evidence-aware service
window remains only for true non-planner callers. A missing or malformed
candidate set defers only that accelerator card; it cannot suppress an
independent card with complete exact-shape Spot candidates. For planner-bound
admission,
the canonical pool card, per-node width, and node count must all match the
immutable authority; equality of total GPU count alone is insufficient.
the transactional service-claim bound is the existing unresolved claim count
plus this exact backend cohort; the per-service paid-GPU cap, global execution
cap, priority, stale-generation, exact-pool, and provider-policy fences remain
unchanged. After Phase A commits the claims, the existing global launch
reservation may pace provider execution, but no fixed pre-commit service
window serializes a larger immutable plan into repeated cohorts.

One bounded paid wave uses one PostgreSQL transaction to:

1. acquire the protocol-observation, lifecycle, service-owner,
   plan-derived service cohort, accelerator-frontier, priority, exact-pool,
   capacity-plan, route, demand, and reserved-allocation authority in
   deterministic lock order;
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
slot. The batch is clipped at the plan-derived service cohort, card-frontier,
exact-pool, priority, paid-GPU, and cost limits; a non-planner caller retains
the legacy evidence window. Process headroom only bounds post-commit execution;
the Phase-A transaction is authoritative and may leave excess committed rows
`SCHEDULED`. It does not weaken any exact-pool, priority, paid-GPU, or provider
limit to reach a requested size. A normal singleton admission is the same
operation with one candidate. There is no durable batch table or historical
manifest.

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

Release `1.1.1510` implemented the atomic batch and accepted
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

Cohort 13 extends this exact identity to paid
``UNKNOWN_CAPACITY_REPLACEMENT`` associations. The immutable authorization
reference freezes the server-observed twelve-digit AWS account, while the
association UUID continues to derive the ClientToken. A cohort-12 replacement
can be decoded and reconciled, but cannot begin or replay a provider effect;
it predates the account-bearing replacement reference. This is a deliberate
recovery-only N-1 boundary, not a tokenless AWS fallback.

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
   elected global cap. The relational ``paid_capacity_pool_key`` and
   ``sky_down_status`` columns are canonical; their ReplicaInfo JSON copies are
   cross-checks. Missing or contradictory copies fail closed. Pool card,
   per-node width, and node count match immutable planner authority before a
   prospective or restarted provider effect.
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
12. **PostgreSQL central truth:** the control plane has no EFS/PVC storage.
    Local files, caches, and process memory are never recovery authority; all
    durable request, capacity, replica, and recovery state is PostgreSQL-only.
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
17. **Accelerator-scoped dependency:** unavailable reserved authority for one
    card cannot block a statically incompatible exact-card paid residual, but
    flexible, unknown, or compatible demand remains allocation-bound.
18. **Failure-isolated cleanup:** exact provider uncertainty retains and
    retries only its own association; independent cleanup proceeds, while any
    shared identity or controller-authority failure remains globally
    fail-closed.
19. **Demand-causal allocation:** a utilization-gated positive compatible paid
    plan may use only an authenticated allocation whose non-blind demonstrated
    need and utilization ceiling cover the locked current SLA-selected target,
    and whose raw upward grants have all reached their damped grants. An older
    idle, concurrency-only, or upward-in-flight allocation is retryable
    unavailable authority, never paid residual evidence.
20. **One compatibility target:** the exact-card economic target committed in
    the PostgreSQL capacity-plan transaction is the target actuated and used by
    same-generation retirement fences. The pre-economic request-class target
    is diagnostic input only; it cannot independently select provider shape.
21. **Closed Spot discovery:** an empty, failed, or on-demand-only prospective
    discovery remains empty and authorizes no paid launch; only an explicitly
    discovered Spot placement may enter fleet cold-launch authority.
22. **Whole-backend paid actuation:** logical paid residual and physical paid
    cold-launch authority are separate same-plan projections. Every authority
    is width-quantized, its padding is accounted, and its complete width is
    debited before a second launch.
23. **Scoped reservation packing:** a non-fitting whole reservation worker is
    omitted from prospective supply for that card; it cannot partially launch
    or globally suppress an independently valid exact-card Spot residual.
24. **Gate acquisition has no provider authority:** the first plan for a clean
    gated positive target may durably witness demand, but it grants zero
    reserved launch, paid launch, local actuation, retirement, or policy-state
    installation. Only a later plan bound to the causally covering settled
    allocation may actuate. The witness is read from the current PostgreSQL
    plan head with exact service/version/semantic-fingerprint/TTL ownership;
    its no-effect horizon covers the slower reserved poll and settlement cycle,
    while fresh-zero revokes it immediately. There is no mutable demand mirror
    and no policy or hysteresis counter is installed or consumed.
25. **Durable reservation capacity is not overlay headroom:** the sequenced
    broker never subtracts the installed autoscaler target from the service
    maximum. Its claim is bounded by immutable service policy and the durable
    gate witness; same-plan inventory debit and intent admission prevent
    duplicate materialization. A target equal to the service maximum must stay
    causally settled across repeated broker rounds rather than oscillating
    back to acquisition.
26. **One durable logical version:** any nonterminal prior-version row or
    intent makes a central durable logical reconciliation recreate-required and
    zero-authority. Version replacement is never inferred from compatible
    cards or funded by Spot. Exact-zero down followed by a new lifecycle is the
    sole supported update path for this service.

## Single-version service contract

The durable logical fleet has one supported runtime version: the current
elected version on one homogeneous writer image. A prior-version nonterminal
replica, intent, request association, or provider operation makes planning
recreate-required and grants zero fresh launch, retirement, or provider
authority. Compatible accelerator cards never imply version compatibility.

`boltz-l4-fleet` is test-only, so this initiative does not migrate an old
service lifecycle or maintain an N/N-1/N-2 operating matrix. Operators complete
normal evidence-backed `serve down` to exact PostgreSQL/provider zero, then
create one fresh lifecycle on the current schema and image. Historical terminal
rows remain readable audit evidence only; they cannot authorize a provider
effect, and they are never removed by guessed repair or manual SQL deletion.
Generic compatibility decoders retained elsewhere in SkyServe are outside this
service contract and are not a second happy path.

## Deployment and fix-forward operation

### Preconditions

- PostgreSQL is healthy and the central schema is at the current image head.
- Provider and cost guards report zero unexpected paid/on-demand resources.
- A control-plane release may be deployed while the current service still
  excludes PHX. Before PHX activation, the reviewed service definition adds
  only one non-Spot `k8s/phx_research_cluster_eks` H200 candidate to the East
  A100/A100-80GB and the catalog-qualified regionless AWS/GCP L4 Spot
  templates.
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

Build and publish one immutable `repository:tag@sha256:digest` image. Upgrade
the existing release with `helm upgrade --reuse-values`, explicitly selecting
that literal image for API, controller, executor, and the gcp login init
container. Do not update a `boltz-platform` runtime pin.

Before provider effects resume, prove all two API, two controller, and three
executor Pods are Ready on that exact tag-and-digest reference and their
runtime image IDs resolve to that digest. If the reclaim-policy or writer
identity changes, invoke the existing PostgreSQL transition command to
authorize one fix-forward generation; an exact retry is idempotent.

The Helm migration inserts PostgreSQL server configuration only when the row
is absent; it cannot overwrite the retained revision. Read back the exact
server-config identity and PHX projection before and after Helm. Service
recreation does not recreate or reseed that central configuration.

### Service deployment

The local reviewed service YAML is deployment authority for this test service.
A clean `serve down`/`serve up` is the sole supported configuration/version
change for this durable logical fleet; no compatibility migration is run.
Teardown still must finish through supported evidence-backed cleanup, and
callers must refresh the endpoint after recreation.

Envelope schema 3 required a hard homogeneous cutover, not a rolling mixed
deployment. That cutover is complete: release `1.1.1554` lifecycle 137 first
completed its exact Spot-100/no-spill/warm-request proof and supported exact
teardown; the activation inventory then contained no Serve rows; and every API,
controller, and executor role moved to the same release `1.1.1555` image at
Helm revision 673 and then to the same `1.1.1557` image at revision 674.
Schema-1 and schema-2 envelopes remain strict decode
failures; there is no N-2 decoder or row rewrite. Once any schema-3 plan or
claim exists, rollback to schema-1/2 binaries is unsafe and recovery is
fix-forward on a newer homogeneous image.

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
- Repeat with missing, partial, and adjacent-version priority/compatibility
  gauges. Every such durable-logical input must grant zero fresh provider
  authority; it must never publish a raw fallback or discounted guessed-card
  paid plan.
- Seed conflicting process-local per-tick Kueue fields and warm-retention
  state, then invoke the durable adapter with a different immutable decision
  snapshot. Prove the canonical planner is called exactly once, observes the
  process-local fields unchanged, and returns without mutating or restoring
  either field; only post-commit policy installation may change retention.
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
- Prove a cold one-GPU target of 100 opens exactly the minimum 25-pool frontier
  under an unchanged four-claim exact-pool bound in its first Phase A, rather
  than serializing through the legacy service window. Repeat with four-GPU
  logical backends and prove 100 GPU units become 25 claims over seven pools.
  Repeat with eight-GPU ``PHYSICAL_BACKEND`` locations and prove 100 plan units
  remain 100 backend claims, not twelve or thirteen. Close the cheapest pool
  and prove it contributes no capacity while only the required later
  cost-ordered pools open. Include prior same-generation debits and prove they
  are subtracted before candidate preparation.
- For an eight-GPU, two-node ``PHYSICAL_BACKEND``, prove cap 16 grants exactly
  one backend, cap 32 grants two, and one existing backend leaves exactly one
  new backend at cap 32. Prove advisory headroom 15 rejects, headroom 16 admits
  and debits to zero, and concurrent PostgreSQL Phase-A transactions at cap 16
  commit exactly one claim. Reject wrong per-node width, wrong node count,
  malformed huge integers and products, relational/JSON pool contradictions,
  and cleanup scalar contradictions. Prove a matched durable cleanup proof
  ignores stale historical pool/zero-cost copies at both billing seams, while
  the same contradiction without cleanup proof fails closed. Prove an
  empty-accelerator CPU pool commits through the legacy non-planner Phase-A
  path with zero GPU debit when no GPU cap is configured, without changing
  zero-cost routing.
- Encode a schema-3 plan and prove strict rejection of schema-1 and schema-2
  envelopes. Before qualification, cleanly down/recreate the test service on a
  homogeneous schema-3 cohort; there is no in-place plan migration or mixed
  decoder rollout.
- Hold the routing epoch, then hold the linearized planning callback after the transaction has locked the
  current service, demand/report/route, allocation, capacity/Kueue, and plan
  rows. Start both HA report writers and prove they wait at the service row;
  prove the callback sees the exact last committed generation, one plan/head
  commits, and both writers advance normally after commit. Repeat with changed
  demand already committed before the lock and prove the new semantics are
  planned rather than rejected as stale.
- Race a rejected service-version transition against that held callback and
  prove it waits for the routing epoch, grants zero provider authority, and
  reports recreate-required without a routing/PostgreSQL lock inversion.
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
- Begin from an active zero-demand retirement wave, advance to exact positive
  demand, and give the planner a prepared snapshot containing that active paid
  retirement. Prove the locked paid baseline still counts the row and commits
  no replacement residual; advance the positive demand generation again and
  prove the separate current-positive batch cancellation commits every
  still-active row atomically after that boundary. Prove local publication is
  skipped, refreshed preparation removes the retiring classification, and the
  next transaction publishes exactly one correct residual without
  double-counting cleanup-unproven paid capacity.
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

- Read back the exact image and chart digest, the 2/2/3 homogeneous role
  inventory, every gcp login init image, and, after service recreation, both
  fleet load-balancer slots. Every Pod spec must retain the literal
  `repository:tag@sha256:digest`; every runtime image ID must resolve to that
  digest.
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

Historical baseline, complete on 2026-08-26 with release `1.1.1513`. The disposable GCP-only service
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

The latest pre-schema-3 provider evidence is lifecycle 137 on release
`1.1.1554`. It reached exactly 100 provider-`RUNNING` GCP Spot one-L4 workers
with zero ordinary on-demand and zero wrong-shape capacity, served all 10,000
authenticated warm requests with first-attempt HTTP 200, and returned service,
replica, claim, waiter, VM, and disk state to exact zero through normal down.
The roughly 9.5-minute cold run does not close the 3--5 minute performance
objective. Durable recent-failure/cooldown state limited many pools, while
clean pools used the configured base window of four. A follow-up benchmark
must record those cohorts separately and preserve the cooldown fence. Release
`1.1.1555` completed the homogeneous schema-3 cutover from no Serve rows, and
`1.1.1557` carried it forward homogeneously at Helm revision 674;
its own live provider-effect and multi-node accounting proof remains open.

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
   From an authenticated idle map, additionally require the first positive
   paid claim to postdate a schema-6 allocation with a non-blind covering
   utilization sample, a utilization ceiling covering the current locked SLA
   target, and ``upward_grants_settled=true``. Observe at least one deliberately
   stale idle-to-burst pass and one smaller concurrency-only allocation
   committing neither a paid claim nor a provider request, followed by
   reserved admission and only then the genuine Spot residual.
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

Historical lifecycles completed synchronized reserved-capacity evidence and a
provider-native at-least-100 Spot lifecycle without changing Kueue policy.
Lifecycle 137 closes the `1.1.1554` count, no-spill, 10,000-request warm
transport, and exact teardown gates. PR #1786 is merged, and release `1.1.1557`
is the active homogeneous schema-3 cohort after the empty-state cutover. The
remaining gates are:

1. Qualify schema-3 provider effects on release `1.1.1557`: require exact
   planner/claim generation and fingerprint agreement, zero ordinary
   on-demand or wrong-shape capacity, normal exact PostgreSQL/provider/disk
   teardown, and a live multi-node physical-backend case whose paid debit is
   exactly per-node GPU width times task-authoritative node count.
2. Re-run the cold-frontier benchmark with clean and durable-cooldown pools
   reported separately. Clean eligible pools must open the configured base
   window of four and reach the documented 3--5 minute objective without
   bypassing recent-failure/cooldown evidence. The lifecycle-137 roughly
   9.5-minute result remains valid fail-closed evidence, not permission to
   erase that state.
3. Capture live nonzero queued, processing, in-flight, and completed values
   from the current schema in the service UI, prove they are internally
   coherent, and return to a fresh idle-zero sample after drain.
4. Complete the utilization-gated reservation matrix and a bounded
   current-schema mixed terminal-ledger proof with production-valid encrypted
   envelopes and signed result/marker URLs. Positive compatible demand must
   commit the matching reservation witness and exact-card reserved capacity
   before a genuine Spot residual; fresh zero must revoke gated fill;
   statically incompatible reserved cards must never suppress eligible Spot
   demand. Lifecycle 137 already supplies the separate 10,000-request paid
   transport and Spot scale/teardown evidence; raw synthetic payloads must not
   be replayed against the canonical Boltz service.
5. Complete controller restart and HA takeover evidence with no duplicate
   provider effect and with exact generation/fingerprint fencing preserved.
6. Re-audit the recreated min-zero heterogeneous service and control plane for
   no EFS/PVC and no Kueue policy, Terraform, Terragrunt, KubeRay, IAM, or
   `boltz-platform` application change. Re-audit only still-required
   transitional cleanup against current source; no historical cleanup branch
   may be merged as-is.

The historical proofs remain useful evidence but do not substitute for gates
1--6 on the schema-3 heterogeneous writer. None of these gates authorizes
expanding the Kueue, EFS/PVC, infrastructure, or application scope.

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
