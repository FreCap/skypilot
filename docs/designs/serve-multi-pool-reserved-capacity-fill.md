# SkyServe multi-pool reserved-capacity admission

Last updated: 2026-09-02

Status: **the reservation-aware planner, exact-card compatibility, bounded
Spot-only paid admission, historical at-least-100 Spot scale, 10,000-request
transport, and exact provider teardown are production-qualified. The paid
commit-to-worker-publication race and the singleton-admission fanout are
source-corrected per admitted wave but not yet deployed or live-qualified.
Merged PR #1857 now enforces one 100-member atomic database-wave bound
independently from the service target, paid cap, provider window, and launch
concurrency. Real PostgreSQL proves a stale successor cannot run the planner or
change the first committed graph and then proves fresh generations converge as
100/100/100/100/20 with exact 420-member cardinality. The unchanged 15-second
authority lease remains fail-closed. The canonical implementation extends the
existing fused PostgreSQL capacity-admission transaction through
executable-request binding and makes the durable request queue, not a
process-local handoff, the recovery source. The principal unpaid PostgreSQL
and adversarial source gates pass. Canonical request reconstruction now runs
before correctness locks against one advisory source read; the transaction
accepts it only when an exact immutable service/version fingerprint still
matches. Request-log directory creation is post-commit best effort, so no
filesystem or EFS path can roll back paid admission. One invalid GCP project
entry is omitted without suppressing healthy AWS or GCP siblings. Qualification
source now proves through the production durable planner adapter that the exact
cold-start scale contract authorizes 800 logical L4 units. The rendered task's
exact one-L4 backend shape therefore authorizes 800 physical backends and
exceeds the 100-worker provider gate. Qualification runner merge, deployment,
the final
current-writer AWS/GCP
scale/traffic/drain receipt, and clean `boltz-l4-fleet` recreation remain
open.** One
PostgreSQL-authoritative planner
is the canonical source path for reservation-aware actuation and paid Spot
residual. Historical production runs proved complete East occupancy,
Kueue-bounded PHX occupancy under the unchanged research policy, reclaim,
mixed reserved-plus-Spot execution, at-least-100 Spot scale-out, 10,000
authenticated warm requests, and exact provider teardown. Full idle research
occupancy is no longer a steady-state goal: `utilization_gate: true` permits
only demand-backed fill and returns it to the unchanged scheduler when idle.

The current production control plane is release ``1.1.1612`` at Helm revision
736 on 2026-09-01. PostgreSQL is the central store and Helm storage is disabled.
The fused paid-wave correction described here is merged as PR #1857 and is not
part of that live release.
The cold ``spot-e2e-0901k`` campaign reached 113
concurrent provider-``RUNNING`` GCP Spot L4 VMs and returned its complete
provider/PostgreSQL graph to exact zero; it first crossed 100 at 343.5 seconds.
Earlier clean-frontier evidence crossed 100 in 221.9 seconds and peaked at 117.

The full AWS/GCP campaign ``spot-e2e-0901ac`` exposed one deterministic
controller race before any provider call. PostgreSQL repeatedly committed
420-member paid replica/claim waves. Worker construction then rebuilt all 420
launch objects outside the manager lock; before publication, the concurrent
refresher mistook those exact rows for abandoned provider-free Phase-A work and
retired each row and claim. The materializer subsequently published stale
workers, which the next refresh discarded because their durable rows no longer
existed. Twenty-seven synchronized samples, direct AWS census across all 14
frozen regions, direct GCP instance/disk/operation census, and PostgreSQL all
showed zero associations, launch requests, provider operations, instances, and
disks. A separate two-member probe reached a real AWS ``g6.2xlarge`` L4 Spot
replica and then completed normal teardown, proving the provider path itself.

The same 420-member wave exposed an independent admission-topology defect after
the materialization handoff was corrected. Every acknowledged receipt member
became one launch thread, one synchronous
``/internal/serve/non-pool-launch`` request, and one PostgreSQL transaction.
An async API worker dispatched each synchronous database phase through
``asyncio.to_thread`` even though its ordinary request-control ``QueuePool`` is
deliberately bounded to one connection with no overflow and a 15-second
checkout deadline. Each distinct-replica transaction also takes the same
exclusive lifecycle and service-row locks. The resulting queue therefore
serialized twice--at the process pool and at PostgreSQL--until tail requests
failed with ``QueuePool limit size 1 overflow 0``. This is not a leaked
connection and is not repaired by increasing pool size, overflow, checkout
time, HTTP retries, or controller thread count; those changes only move the
same 420-member queue and consume more central-database capacity.

The existing ``CapacityAdmissionRepository.plan_and_admit_current()``
transaction is the sole paid request-materialization unit. Before it begins,
the controller constructs an immutable, canonically ordered candidate tuple
containing every ``PaidLaunchSpec`` and its exact server-local prepared launch
bytes. The repository then performs one non-authoritative PostgreSQL read,
closes that checkout, fully reconstructs and validates those bytes, and freezes
the complete binding authority, resource scope, replica port, and a canonical
fingerprint of the service spec, launch YAML, placement catalog and contract,
and controller configuration. Manager-side candidate construction remains
provider-, HTTP-, filesystem-, and database-free; advisory validation may use
local validation and console paths but holds no correctness lock. The
repository revalidates the complete preflight fingerprint under its locks and
takes the protocol/lifecycle/service prefix
once, arbitrates the sparse accepted subset, writes the plan, capacity debit,
replica, and claim, derives every retry-stable submission UUID, resolves the
newly durable paid profile, and invokes the sole
``non_pool_admission.build`` / ``bind_in_transaction`` implementation for each
accepted member before commit. Plan/head, debit, replica, claim, association,
request, retention pin, queue row, and replica pointer are therefore one atomic
graph: either all accepted members become executable or none does.
Handler registration, request-backend capability discovery, and nested lazy
module resolution are process preflight, never correctness-transaction work.
The preflight emits one frozen ``NonPoolLaunchBindingRuntime`` token; the
transaction accepts that token only alongside its locked PostgreSQL fleet,
cohort, authority, and service/version checks. A fleet-capability read given a
caller-owned connection uses only that connection and cannot initialize or
check out a second database handle. The token therefore removes cold-process
imports and backend construction from the lock interval without replacing any
durable authorization fact.
The receipt retains only durable request identity. Its derived log path is
created and touched after commit as best effort, so filesystem availability is
not part of the graph's commit condition.

Queue visibility at commit is the durable handoff. The generic executor may
claim work immediately; controller workers published afterward are optional
observers/adopters of the already-durable request IDs and never submit the
singleton endpoint. A lost controller acknowledgement or process death needs
no batch hydration protocol and cannot expose an unbound paid replica: queued
work and the association pointer already committed together, and ordinary
recovery adopts that graph. The singleton endpoint remains temporarily for
rolling compatibility and non-paid generic profiles, but ordinary-paid use is
explicitly instrumented. There is no batch-to-singleton fallback for a new paid wave.
The compatibility branch is removed after one homogeneous-capability stale
window plus the unpaid bounded-wave 420-target convergence and provider-native
qualification gates. This
correction adds no table or service-data rewrite. Its forward-only Serve067
migration aligns the existing paid-pool constraints and guard functions with
the exact project-scoped GCP-v2 contract. It adds no provider call, retry
policy, Kueue object, Helm value, or storage dependency, and it does not change
the executor's exact pre-I/O fence.

The mandatory unpaid gate runs the production fused capacity repository against
ephemeral PostgreSQL with the synchronous process limit fixed at one and
provider calls installed as fail traps. For every admitted bounded wave it
proves in the same transaction the exact ordered plan/debit/replica/claim/
association/request/queue/pin/pointer cardinality. A changed executable member
must roll back that complete wave. A real post-COMMIT acknowledgement loss must
leave every committed request executable and exactly adoptable, with no
singleton HTTP call and no second correctness checkout for materialization.
Paid admission uses two sequential, never concurrent checkouts: one advisory
source read that closes before canonical reconstruction and one atomic
correctness checkout (whose optional history projection may use a later
transaction on that same connection).
Provider adapters remain fail traps throughout, so this gate costs no cloud
resources. The correctness graph commits atomically first; the existing
minute-history projection may then commit or roll back best-effort on the same
checkout without affecting that graph. The later billable qualification
remains necessary only for provider placement, runtime, traffic, and teardown
evidence. No process-local handoff, provider, scheduler, placement-mode, or
timeout change is introduced. Serve067 is additive control-plane DDL only and
does not rewrite service rows.

The first project-wide follow-up, ``spot-e2e-0901v``, loaded 44 eligible GCP
Spot L4 pools and froze exact project/workspace/version authority without a
single-region restriction. Its cheapest two zones returned genuine provider
capacity failures. Thirteen exact Spot VMs materialized transiently, and normal
failure cleanup then proved three consecutive samples with zero claims,
debits, VMs, disks, operations, and waiters. The qualifier stopped on a real
controller invariant violation before a scale result: six paid Phase-A rows
had no retained launch association, yet their claims survived terminal row
projection until a future paid-admission sweep.

The canonical correction does not weaken claim/debit guards or add a retry
path. A finished ordinary-paid worker carrying one exact committed Phase-A
receipt and no durable bound association is transferred to the existing exact
pre-admission retirement lane. That PostgreSQL transaction locks and
revalidates the service, replica record, association absence, request graph,
and claim before deleting the planner row and claim together. If association
admission won the race, retirement returns ``ASSOCIATED`` and normal bound
adoption remains the only path. No provider ``down`` is authorized from
pre-admission absence. A separate cleanup correction accepts a later
``sky_down=SUCCEEDED`` marker only for the already exact provider-``ABSENT``,
quiesced, claim-free and pin-free paid projection; every ambiguous or failed
down marker remains rejected.

The next billable qualification is bounded at 800 logical L4 slots and requires
at least 100 physical provider-``RUNNING`` workers. The rendered task uses an
exact one-L4 backend shape without pinning an instance type, so its 800-slot
target authorizes 800 physical backends and comfortably exceeds the provider
gate. The campaign first submits 800 immutable async
identities and proves that exact held prefix is resident before it submits the
remaining 9,200 zero-duration identities. This removes client/network ordering
from the load balancer's strict FIFO contract: the first 800 remain active for
340 seconds while the tail stays queued behind them. Within the original
60-second offered-arrival window, the qualifier joins a fresh PostgreSQL
request reduction showing exactly 10,000 queued plus in-flight identities to
the routed ACTIVE load balancer's exact 10,000 unique-job arrival counters.
Queued identities do not yet have ledger rows; dispatched in-flight identities
must exactly equal active ledger rows, and the terminal 10,000-row delta later
proves that the whole queue was processed. The worker accepts at most 360
seconds of synthetic work and the queue expires at 600 seconds, so demand
survives the five-minute scale SLO without depending on timeout. The controller
Helm throttle remains 420 prepared physical launches; neither that shared
throttle nor `boltz-l4-fleet`'s paid cap changes. The real-cloud gate remains at
least 100 provider-running workers within five minutes, exact request-ledger
completion, fresh attributed demand telemetry, and natural exact-zero drain.
The hermetic qualification gate enters through the production durable planner
adapter with this exact cold-start contract: 10,000 priority-50 L4 requests,
the 600-second default queue deadline, a ten-second configured service time,
the 600-second automatic cold-lead seed, an 800-logical-slot service/paid
ceiling, and the rendered task's exact one-L4 backend shape. The canonical
planner returns an 800-slot deadline, raw, supply-aware, wave-limited,
paid-residual, and paid-launch target, which authorizes 800 physical backends
and therefore exceeds the 100-worker provider gate. It also publishes all
10,000 requests as infeasible against the cold-start SLA; the 800 target is
therefore explicitly bounded backlog recovery and not a claim that capacity
arriving after the deadline can satisfy that deadline.
The 100-member atomic PostgreSQL wave bound is independent from the logical
service target, physical-launch throttle, per-location window, and provider
concurrency. Successive fresh generations must converge to the target while
each transaction stays inside the unchanged authority lease. The existing
optional best-effort history projection may use a second transaction on the
same checkout. These are qualification inputs, not a scheduler-policy change
or authorization to raise the long-lived production paid cap.

Provider availability and runtime/service readiness are separate facts. The
write-once provider-allocation marker commits normal pool-success feedback
after the in-tree provisioner returns a single-node full-fresh allocation
observed as provider-``RUNNING`` and before runtime setup. It is allowed only
for the exact active protocol-v2 ``ORDINARY_PAID`` association, request
execution claim, replica record, Spot pool, built-in provisioner, zero resumed
nodes, and matching provider account/project, region, zone, instance type,
instance ID, and cluster identity. Custom provisioners, multi-node replicas,
partial creates, resumes, replacements, Kubernetes, and reserved fill retain
terminal feedback and do not use this checkpoint.

The paid claim stores a PostgreSQL timestamp and receipt SHA-256 as an
all-or-none pair; the closed receipt contract name is covered by that digest.
An exact replay is a no-op; a different or partial receipt fails closed. The
provider-evidence fields on the launch association remain solely
post-terminal cleanup/recovery evidence and are not overloaded. Once the claim
contains the marker, every later terminal request outcome is economically
neutral for that pool: success cannot double-ramp it, and a runtime/setup
failure cannot incorrectly bench a provider pool that already returned the
full running allocation. Replica result persistence, claim release, provider
cleanup, and request quiescence retain their existing paths.

Non-pool capability cohort 14 is the first cohort allowed to begin an
ordinary-paid provider effect that can publish this provider-allocation
checkpoint. Cohort 13 remains readable for exact settlement and cleanup, but
cannot begin or replay provider I/O and cannot write the new marker. Activation
therefore requires one homogeneous cohort-14 API/controller/executor tuple;
there is no mixed-version fallback for a new paid effect.

The source successor advances the capability boundary again for exact GCP
project ownership. A fresh GCP pool/profile is protocol v2 and freezes the
project selected from the locked workspace-and-region controller snapshot; the
ordinary request body receives only the normal sanitized locked configuration,
not a second full-config copy. GCP protocol-v1 rows remain readable only for
settlement and cleanup. Cohort 15 is the first cohort allowed to begin either
an ordinary-paid or ``UNKNOWN_CAPACITY_REPLACEMENT`` effect against a v2 GCP
pool. Cohort 14 cannot begin or replay that provider I/O, so successor
activation requires one homogeneous cohort-15 API/controller/executor fleet
before planning, admission, or any provider call.
Project resolution is fault-isolated per catalog location: a missing or invalid
exact project omits that GCP location and emits an operator warning, while
malformed global workspace/config input still fails closed. The omission never
removes a healthy AWS pool or a separately valid GCP project from the same
bounded wave.

The provider-allocation checkpoint was a forward-only additive PostgreSQL
schema change. Its activation requires
an exact-zero service and a homogeneous writer deployment; old binaries may
ignore the nullable columns before activation. Required source gates are:
typed AWS/GCP receipt validation; first-write/replay/conflict/concurrency and
rollback tests; stale owner/request/effect rejection; terminal
success/capacity/quota/other-failure neutralization; and an integration test
that blocks runtime setup after provider return while proving the next paid
wave is admitted from the marker within one controller reconciliation. The
production gate remains cold at-least-100 provider-``RUNNING`` Spot VMs within
five minutes, 10,000 exact async ``SUCCEEDED`` ledger completions with fresh
positive queued/in-flight/processing telemetry, and natural exact-zero drain.
The current atomic request-binding and GCP-v2 correction requires the
forward-only Serve067 additive control-plane migration. It replaces the exact
existing paid-pool constraints and guard-function fragments; it creates no
table and rewrites no service data. The qualification service is absent and is
recreated from exact zero under those current constraints; legacy GCP v1
remains a cleanup-only decoder, not a fresh-admission branch.

The deployed bounded-candidate correction removed the earlier representation
error without forming a target-times-catalog cross product. An optimistic
PostgreSQL pool
budget selects at most the configured per-card backend ceilings in canonical
cost order; the locked repository scans that bounded cohort and charges only
accepted plan units. Prospective alternatives never spend the global physical
GPU cap, which remains solely locked PostgreSQL authority. Selection previews
do not consume retry probes or numeric replica IDs; only an acknowledged
receipt reserves and persists the exact retry location before worker
publication. Fused PostgreSQL coverage proves that a 60/1/39 three-pool wave
fills 100 and that cheaper L4 alternatives cannot starve an A100-only target
under a four-GPU global cap. Deployment and natural exact-zero teardown are
proved by campaign ``spot-e2e-0901k``; the five-minute cold-scale threshold,
exact 10,000-completion async ledger, fleet recreation, and nonzero dashboard
ledger capture remain open.

Within one identical typed balancing tier -- normalized cost, purchase market
(``use_spot``), and exact physical backend shape (accelerator, width, and node
count) -- a globally managed launch wave balances by the largest remaining
fraction of each pool's immutable wave-start PostgreSQL headroom. This is the
sole exception to stable catalog-rank tie-breaking: it cannot select a more
expensive location, cross purchase markets or backend shapes, alter a
local/legacy budget, or infer missing headroom. Thus three 60-slot equal-cost
pools divide a 120-member wave as 40/40/40, while headroom 60/60/1 divides the
same capped wave as 60/59/1. The locked validator permits rank interleaving
only inside one identical typed tier. Distinct tiers form contiguous blocks;
same-cost tier blocks may appear in either order, but a closed tier cannot
resume. Normalized cost remains nondecreasing and every rank-local occurrence
remains exact. Expensive-to-cheaper traversal fails before any replica, claim,
or plan write.

Release `1.1.1578`, source merge
`c52a4dde95bc80036801d2d8bfb96d5bd8d43473`, deployed PR #1809 homogeneously
at Helm revision 697. All two API, two controller, three executor, and GCP login
init roles run immutable image digest
`sha256:a582ad0f9a8f2437ac5e7bc5d62103f382c44fba190e3bde7ddcbd0bbe4f29bf`.
The fixed-wave policy and one-transaction
plan/head/policy/replica/claim admission path passed the focused PostgreSQL
suite and a 100-replica atomic admission wave. The final-deletion census now
makes future teardown conditional on the complete same-name authority graph.

Clean recreation then produced lifecycle 150 and failed closed before provider
admission for two independent read-side reasons. First, exactly two of 8,994
detached old-incarnation associations retained canonical post-quiescence
`UNKNOWN` observations even though their durable effect boundary was
`PRE_EFFECT_TERMINAL`/`NOT_STARTED`. The shared final-deletion/genesis
classifier treated every non-`NOT_QUERIED` observation as provider-possible.
The deployed correction accepts only the exact neutral historical
shape described below; it does not infer provider absence or weaken any
provider-effect path. Second, the reclaim-policy renewal treated East's exact
zero matching GPU nodes as topology nonconformance, aborting otherwise healthy
PHX receipt renewal. The deployed correction records typed zero for an empty
context, while concrete launch authority still requires positive capacity for
the target flavor. Generation 41 then renewed simultaneous provider proofs:
PHX reported 64 non-deleting eight-H200 nodes and East reported zero matching
A100 and A100-80GB nodes.

That production proof exposed another cross-layer mismatch. The broker
still classified East's confirmed successful zero-card observation as a
legacy blackout and stored a SQL `NULL` exact-card feed. Whole-map allocation
correctly rejected that incomplete pool, which also withheld healthy PHX and
format-6 genesis. The canonical correction preserves the existing transient
phantom debounce, but after confirmation publishes an authenticated exact-card
zero envelope (`observed={card: 0}`, `spendable={card: 0}`, service feed empty,
and the exact slot width). SQL `NULL` remains reserved for provider failure,
malformed evidence, or a not-yet-confirmed transient observation. Neither
correction adds a schema, migration, Kueue object, infrastructure object, EFS
path, or alternate planner. Release `1.1.1579` deployed that correction at Helm
revision 698. The broker now publishes a complete three-pool allocation with
authoritative East zero and PHX-positive observations.

The first live format-6 replay then exposed two service/planner composition
errors that component tests did not cover. The recreated service retained the
legacy process-local `adaptive_scale_up` block even though the canonical durable
planner intentionally supports only the fixed PostgreSQL policy. Removing that
block reveals a case-only identity mismatch: genesis history is stored in the
lowercase accounting-card domain while the YAML catalog retains display names
such as `A100` and `H200`. Accelerator identity is case-insensitive throughout
the public contract. The snapshot adapter therefore reprojects a prior
candidate's exact-card capacity fields into the current configured display
domain after proving the folded name and width sets are identical; unknown,
added, removed, duplicate, or width-changing cards still fail closed. The clean
fleet definition removes `adaptive_scale_up` and uses the sole supported fixed
policy of 100 percent, minimum 50, per 60 seconds. Format-6 genesis and the
current mixed/Spot campaigns remain open until that source and service update
are deployed and proved.

A composed PostgreSQL regression now invokes the production controller
`_plan_and_admit_current_capacity()` path for two consecutive fresh-zero
generations. It loads a persisted fixed-policy service spec, combines lowercase
repository genesis with display-case configured cards, consumes East exact-zero
and PHX-positive allocation evidence, commits the first candidate, then proves
the committed display-case head remains valid on the next reconciliation.
Together with the fleet repository's exact service-definition validator, this
would have caught both the service-policy drift and the case-only induction
failure before deployment. Positive reserved-first/paid-residual behavior and
provider effects remain live campaign gates rather than claims from this
fresh-zero regression.

PR #1811 then deployed the history-domain correction as release `1.1.1580` at
Helm revision 699. The old service was already at exact zero; the single-version
logical protocol correctly rejected an in-place update, so it was deleted
without purge and recreated from the validated fixed-policy definition. The new
incarnation `64aeb479-0d57-4316-9ffb-991a0d247244` has `min_replicas: 0`, zero
fill floor, `utilization_gate: true`, paid cap 100, no task-owned Kubernetes
override, and no adaptive policy. Both HA load balancers became ready on the new
endpoint; PostgreSQL remains authoritative and Helm storage remains disabled.

That replay passed the history adapters and exposed the next case-only boundary.
Provider-free paid candidates deliberately use lowercase accounting-card keys,
while the now-canonical planner candidate carries configured display spelling.
`_clip_prepared_paid_admission()` compared those maps case-sensitively before it
could discard the unused candidates at fresh zero, so every round failed closed
with `Prepared paid launch contradicts the planned backend shape.` The correction
folds only the lookup identity for physical widths, remaining paid units, and
priority; immutable candidate evidence, exact widths, node count, catalog order,
and every downstream authority check remain unchanged. The composed PostgreSQL
regression now includes a real prepared lowercase Spot candidate so this seam is
exercised even under a zero paid target. A positive PostgreSQL regression also
requires display-case `L4` planning plus a lowercase provider candidate to
commit exactly one replica, paid claim, and receipt with the planned priority
and debit width; planner identity remains display-canonical through finalization
while the provider pool and persisted claim remain lowercase.

The subsequent retained-paid cleanup qualification proved a different
restart invariant violation. Nine live ordinary-paid associations retained
profiles frozen with paid-claim priority 20, while their current claim rows had
priority 0. Byte-for-byte reconstruction showed that priority was the sole
changed claim or placement field: restoring only 20 reproduced both the frozen
authorization digest and the complete profile digest for every row. The writer
chain was exact: controller recovery called
``paid_capacity.adopt_existing_claims(...,
priority=LB_REQUEST_PRIORITY_MIN)``, and
``adopt_paid_capacity_claims()`` used ``ON CONFLICT DO UPDATE`` to overwrite
the priority of a claim that already existed. Existing-claim arbitration no
longer uses claim priority; mutable contender priority belongs to the waiter
row. A paid claim is therefore immutable admission evidence from insertion
until deletion, not mutable restart bookkeeping.

Prior tests did not merely omit this case. One isolated paid-capacity
redrive test admitted priority 20, retried at 21, and asserted that the stored
claim changed to 21 while ``claimed_at`` remained unchanged. Separate binding
tests correctly treated the digested claim/profile as immutable. Historical
live scale, serving, and drain evidence did not compose those contradictory
contracts by restarting a controller after profile freeze and before terminal
cleanup. The missing release gate was therefore the production composition:
admit and bind a paid claim, restart/re-adopt it, then reconcile both
provider-present and provider-absent cleanup against the same frozen profile.

The steady-state source correction makes an existing claim replay or restart
adoption validation-only: it exact-checks record and pool identity and writes
neither the claim nor profile-covered replica state. Only a genuinely missing
legacy claim may take the insert branch. For the nine rows already corrupted
by the old writer, cleanup first runs the strict validator, then permits one
temporary cleanup-only matcher only when the *current* priority is exactly 0
and varying the historical priority is sufficient to reproduce the complete
frozen profile. Any different current priority or any plan, pool, claim,
placement, record, service, reference, or digest drift remains fail-closed.
This compatibility grants no launch authority. It must be removed after one
homogeneous fixed-writer deployment has settled every old-writer row and the
full stale/quiescence/provider-clean horizon proves that no affected row
remains; the final path is strict profile equality only.

The stacked removal branch ``cleanup/remove-paid-priority-repair`` is restacked
on the current source head, including exact ``COMMITTED`` retirement-receipt
consumption. Its three removal gates passed on 2026-09-01: release ``1.1.1584``
was homogeneous across all API, controller, and executor roles; the exact
historical-priority affected-row census reached zero; and provider/cleanup
health stayed exact zero for 372 continuous seconds. The removal deletes the
temporary matcher and its transition-only tests; it retains the permanent
replay/adoption immutability and retirement-consumption code and tests and
restores both ordinary-paid cleanup paths to strict frozen-profile equality.

PR #1813, ``[Serve] Add provider-native paid Spot E2E``, merged at
``a64bb69163e074dfe2a415ca388ed23c06d8c1cd``. It adds one executable small and
scale qualifier with exact PostgreSQL binding and provider-native GCP
instance/disk/operation guards, authenticated request identity, scale timing,
and normal drain/cleanup receipts. Its focused tests and smoke-test collection
qualify the runner, not a billable provider effect. No live PR-#1813 billable
receipt has been captured yet, so provider-native end-to-end qualification
remains an open production gate. PR #1854 supersedes that GCP-only runner with
the canonical provider-neutral AWS/GCP economic qualifier and its narrowly
authorized missing-provider canary; the historical #1813 result remains source
evidence only.

Lifecycle 148 on the preceding `1.1.1575` release accepted a sustained
exact-L4 pressure test: 270,000 attempts over 184.8 seconds produced a raw logical target of
1,000 and only Spot L4 placement. It exposed two remaining violations of the
single-authority contract. First, the durable demand-feed controller returns
before the legacy mutable pressure reducer. The durable planner therefore sees
the configured base 20-percent/minimum-10 wave forever; the separate
100-percent/minimum-50 adaptive mode is unreachable on that path. Second, the controller commits a capacity plan, then
prepares a provider-free paid cohort, then inserts the replica/claim wave in a
second transaction. A deadline heartbeat can commit between those transactions
and correctly revoke the old prospective lease, causing deterministic
admission churn despite continuous positive demand.

The accepted correction removes the unused adaptive mode instead of making its
second state machine durable. Durable logical services use one configured fast
paid-wave policy; `boltz-l4-fleet` uses 100 percent/minimum 50 per 60 seconds,
bounded by its hard paid-GPU cap and pool feedback. Capacity-planning envelope
format 6 replaces format 4 at an exact-zero homogeneous cutover because
portable DB epochs cannot be decoded as process-monotonic format-4 clocks and
the census receipt must be distinguishable from intermediate local format-5
heads. Its minimal `CapacityPolicyState` retains only irreducible
hysteresis/adoption counters plus the paid-window DB start and accepted
per-card ceiling. Prior
targets and every effect projection are read from the prior committed candidate,
not duplicated in policy state. The one pure reducer consumes those values plus
the current locked demand and returns a policy transition and proposed wave. Provider-free
candidate templates may be prepared before the correctness boundary, but one
PostgreSQL transaction locks current demand, route, supply, service, plan and
paid-pool state; invokes the production planner once; clips the proposed paid
wave against those locked pools; withholds paid admission whenever the planner
first needs compatible reserved intents while leaving a wholly statically
disjoint Spot target eligible;
samples the DB clock; finalizes policy state from the accepted subset; writes
the new plan/head and exact replica/claim wave; then resamples the DB clock and
revalidates every TTL before committing them together.
Only after commit may workers be constructed or a provider mutation/launch
effect begin. Bounded read-only identity, catalog, and ranking preflight may run
before the transaction, but it freezes only scalar inputs and grants no launch
authority. The pure planner and transaction consume only those frozen values;
before exact-token ``RunInstances`` the postcommit path rechecks that the live
AWS account equals the committed account identity. A report committed before
the service-row lock is included in planning; a report arriving after the lock
is causally after the committed wave. Fresh zero therefore prevents every
uncommitted provider effect without racing a separately published plan.

Format 6 is an envelope-format cutover, not a relational migration or a new
authority protocol: it adds no
table, column, row rewrite, dual decoder, EFS, Kueue object, infrastructure
component, or alternate allocator. The test-only service is first brought to
supported PostgreSQL and provider exact zero, all API/controller/executor roles
then move to one format-6-capable image, and only then is the service recreated.
A format-4 writer and format-6 writer are never an accepted mixed cohort.

The deployed source contains one canonical compatibility matcher. It
replaces the greedy finite-supply path that could leave compatible reservation
supply unused: for equal-priority classes ``{A,B}``, ``{A,B}``, and ``{A,C}``
with one free slot on each card, the predecessor selected ``A:2,B:1`` instead
of the complete ``A:1,B:1,C:1`` matching. That predecessor could therefore
produce a false paid residual. Feeding raw pool observations into it would
also have moved unowned, cross-service observations across the broker's
ownership boundary. Source and PostgreSQL qualification are complete and the
matcher remains deployed homogeneously through `1.1.1575`; its bounded
flexible/mixed-card production proof remains open.

A later release `1.1.1572` 10,000-request qualification exposed a distinct
second-wave starvation bug below that matcher. The first paid wave changed the
projected route head as workers became selectable. The load balancer still
reported the same positive demand against its immutable applied route, but the
controller required the retained and current selectable-route sets to be
identical. It therefore discarded valid demand until the finite window expired
instead of planning the next wave.

Release `1.1.1573` deployed the correction that replaces whole-context
equality with one typed
route relation. A retained report is `ADDITIVE_COMPATIBLE` only when its
routing policy, service version, and queue-attribution mode are unchanged and
every selectable retained URL has the same immutable route identity and wire
contract in the current head. The current head may add selectable routes;
those routes are new supply, not a change to demand already accepted by the
load balancer. Route contraction, URL rebinding, policy drift, malformed
identity, or queue-mode drift remains `INCOMPATIBLE`. `STANDBY` and `ARMED`
reports do not participate in demand authority. Additive compatibility may
authorize reserved and paid scale-up or revoke new spend on fresh zero, but it
cannot authorize retirement. Destructive authority requires an exact route
relation and a durably `STABLE` LB cutover phase; final logical retirement
rechecks both under the existing PostgreSQL locks. Service promotion likewise
requires an exact current route relation and a durably `STABLE` cutover. This
correction is source-, PostgreSQL-test-, and homogeneous-deployment complete.
Helm revision 690 then reached the route relation in production: lifecycle 147
selected an `EXACT` route at its saturated post-failure frontier after the
first paid target of 10 had committed and eight AWS Spot VMs had run. The two
remaining launches failed availability. The eight worker jobs failed only
because all four task secret values had been omitted and were empty, not
because of routing, admission, or model health; an independent Spot worker
model-health request returned HTTP 200. Lifecycle 148 was recreated with
nonempty secrets persisted in the task. This was deployment and constituent
evidence, not the later successor multi-wave proof completed in the source
gate below.

The first `1.1.1573` recreated lifecycle exposed a separate post-failure
cold-frontier demand admission defect. Lifecycle 147 first committed target 10,
launched eight AWS Spot replicas (11--18), and recorded two failed-provision
replicas (19--20). Its eight user jobs failed independently because all four
task secrets were empty, and those replicas were torn down. Once active
capacity returned to zero, the ACTIVE load balancer published an exact current
route, a complete demand-window classification, and an exact L4
rejected-demand profile, but its bounded offered-arrival tracker had reached
the 100,000-entry cap. Durable ingestion incorrectly folded that
magnitude-overflow bit into exact-card compatibility completeness. The
controller therefore rejected the entire snapshot and could not commit the
successor capacity plan that would launch the next wave.

Offered-arrival saturation is a bounded conservative magnitude signal, not a
missing exact-card gauge. A current report remains compatibility-complete when
its demand-window, queue, rejection, priority, version, and accelerator-catalog
profiles are otherwise complete; it preserves
`offered_arrival_tracking_saturated: true` unchanged. The canonical pure
planner must not extrapolate that aggregate bound through a retained partial
sample: one classified L4 arrival cannot turn the other 99,999 unknown arrivals
into L4 authority, and mixed retained samples cannot choose a card mix for the
unknown remainder. Under saturation, exact classified arrivals, queued demand,
and rejected demand authorize only their own attributable work; the
unattributed offered-arrival gap authorizes neither paid capacity nor
retirement. Fixed work is split before any arrival shaping: observed in-flight
work bounded by materialized current supply becomes typed exact-card
`fixed_work`, `explicit_fixed_work`, and `paid_fixed_work`, while work on
retiring supply, unknown-capacity work, and overflow without materialized
exact-card proof remain unattributed. Under saturation only exact attributable
outstanding pressure and measured classified arrivals enter paid attribution.
The existing outstanding-pressure projection (queue, rejection, and in-flight
work) is one observation; retained arrivals are another observation of the
same request stream, not additive demand. A request's priority and compatible
accelerator tuple are immutable across its arrival, queue, and rejection
projections for every accepted exact/additive route relation. Consequently,
only an identical typed class can overlap between those classified
projections; different tuples remain distinct instead of being coupled by an
arbitrary maximum-flow tie-break. Exact fixed in-flight work has lost request
priority and compatibility. It therefore cannot be paired with one of several
arrival classes without inventing request identity and potentially selecting
the wrong paid card. A residual arrival class that intersects lossy fixed work
therefore grants no provider authority until current queue or rejection
telemetry establishes authoritative outstanding work. It may shelter only
committed cards compatible with that class, never an unrelated accelerator
fleet. Classes disjoint from every fixed card remain immediately incremental.
This explicit information boundary avoids recreating a second subset-rank
allocator merely to infer request identity from aggregate observations.
Fixed-only capacity carries the minimum request priority unless typed work is
copacked into its slot, so an unrelated high-priority arrival cannot upgrade
its reservation entitlement. This reconciliation is linear in classified
profiles and the at-most-eight-card catalog; it expands neither requests nor
profile pairs. The aggregate offered-arrival counter remains an alternative
magnitude floor, never a second work stream.
Unattributed fixed work never enters aggregate demand, reservation acquisition,
actuation scale-up, or paid authority; it can only preserve a retirement shelter/floor
bounded by already committed positive capacity, or make the tick fail closed
when no exact shelter identity exists. It cannot borrow a partial arrival
sample's card.

The adapter preserves an `unattributed_saturated_work` boundary when request
duration is unknown, the arrival gap remaining after classified fixed-overlap
work is positive, fixed work lacks an exact card, or classified arrival
identity still overlaps lossy fixed work.
The first three are global attribution failures and may retain committed
positive capacity on every exact card. Fixed-overlap ambiguity carries an
explicit compatible-card set and shelters only committed capacity on those
cards. A fully attributed saturated report therefore follows normal downscale
policy instead of sheltering unrelated idle supply. The same boundary removes
prior demand targets, ownership, actuation maps, and cold authority from the
planning projection so stale hysteresis cannot resurrect unattributed paid or
provider authority. The live prior fingerprint and generation remain the
post-commit CAS precondition, and provider wave timestamps/ceilings remain as
launch-pacing evidence. Missing or partial compatibility profiles remain
fail-closed, and a saturated report can never prove fresh aggregate zero or
authorize a zero-demand retirement.

Offered-arrival counters and their saturation bit are stable planner inputs and
therefore belong to the immutable normalized demand semantics used by
PostgreSQL admission. Raw queue-deadline buckets remain available only in the
locked planner input. Their `remaining_seconds` countdown is deliberately
absent from the outer normalized identity because hashing the raw heartbeat
countdown caused the reservation broker and controller to revoke each other's
otherwise stable work. This does not make two countdowns decision-equivalent.
The persisted production-plan semantic digest remains the only causal lease
identity. A newer heartbeat may retain an older plan only as an explicitly
older, free-capacity lower bound when its complete deadline multiset is a
monotonic tightening of the persisted planner input. The older plan keeps its
original demand generation; it is never relabeled current. Prospective paid
claim validation requires the exact current demand generation and receipt
watermark for every deadline-bearing plan. A deadline extension, tightening,
class/count change, incomplete gauge, or any fresh paid/provider effect
therefore requires a new invocation of the one production planner under the
PostgreSQL lock. Thus a 585-to-580-second countdown may retain target 18 only as
a free/reserved-capacity lower-bound witness while the locked planner publishes
fresh target 19; the retained 18 can neither suppress that replan nor authorize
paid launch. The deadline-completeness bit remains normalized so a missing
gauge still fails closed. PR #1806 deployed the saturated-demand correction in
`1.1.1575`: lifecycle 148 reached raw target 1,000 and 49 Spot-L4 commitments,
then exposed the unreachable secondary pacing reducer and two-transaction
plan/admission defects addressed by capacity-envelope format 6.

The steady-state implementation is one deterministic counted subset-rank
matcher, shared by capacity planning and the reserved-pool broker.  The pure planner
reduces fractional work into typed owned capacity classes after policy and
deadline reduction.  A class records its priority, compatible cards, and
integer capacity cardinality; co-packed fractional work intersects the
contributing compatibility sets.  Deadline-selected capacity is exact-pinned
unless the deadline planner itself proves that relocation preserves its
capacity-time schedule.  Exact-card demand is simply a singleton class, not a
second mode or allocator.

The matcher uses the already-enforced Serve limit of eight interacting
accelerator cards as its bounded dimension.  Two exact subset-rank calculations
first derive the lexicographically maximal matched count for each priority and
then expose the physical supply units that can realize those fixed counts.
Counted matroid greedy consumes preferred units before nonpreferred units and
then applies the stable supply rank.  It does not expand logical GPU units,
materialize demand-by-pool edges, or run a second residual/min-cost allocator.
Demand-only and supply-only cards that cannot interact do not enlarge the
subset universe; more than eight interacting cards fail closed at the canonical
matcher boundary.  A 2,040-class, eight-card, 512-pool regression completes in
well under the ten-second bound, and a checked-in exhaustive small-graph oracle
compares the full priority, preference, stable-rank, and global-cap objective.

The broker supplies typed physical pool atoms containing the immutable pool
identity, exact card, retained holdings, and fresh bounded capacity hint.  The
same matcher lexicographically preserves higher-priority demand, maximizes
compatible cardinality, consumes caller-typed preferred capacity, and applies
a stable pool/card tie-break.  Capacity planning marks every zero-cost unit as
preferred over paid capacity; brokering marks authenticated holdings as
preferred over new acquisition.  One global assignment bound lets this same
kernel select a priority-preserving partial scale-up wave, so a 10-slot wave of
a 100-slot target does not require a second truncation allocator.  Its per-pool
result becomes the existing edge cap.  Publication reconstructs current
classes and locked pool observations and reruns the same matcher before
accepting those caps.  The copied witness digest and canonical class list carry
one self-consistency digest in the claim JSON, so a changed class list cannot
settle under the old witness.  No assignment matrix, raw observation, or
alternate target is persisted.  A missing class reduction, stale observation,
changed witness, binding mismatch, or mismatched result remains
``HOLDINGS_ONLY`` and grants no new reserved or paid effect.

This homogeneous current-version cutover advanced the planning envelope from
schema 3 to schema 4 and the demand-witness semantic domain from v4 to v5.
There was no relational migration, dual decoder, EFS state, Kueue, or
infrastructure change. The service was recreated only after the homogeneous
Helm rollout, so older envelopes and claim state failed closed instead of
gaining a compatibility path.

PRs #1794, #1795, and #1796 closed the defects exposed by the exact-card
production matrix: exact-card budgets are no longer assigned by pool iteration
order, current admission maps canonicalize configured card casing, and statically
disjoint paid demand is not fenced by unrelated reservation-allocation
generations. Release `1.1.1565` first qualified those edges. PRs #1798 through
#1803 then carried the schema-4 matcher and paid-wave boundaries into release
`1.1.1572`, deployed homogeneously at Helm revision 688 and public API 93 with
image digest
`sha256:d07db658aff6ef40fe6b78b055eaba38b5ebba5e7cf3f3209787be02f8648a8a`.
Both hard cutovers occurred with no retained Serve rows. PostgreSQL remains the
only central correctness store, Helm storage is disabled, and there is no
SkyPilot PVC or EFS.

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
PR #1794 advanced that stable semantic domain to v4 by binding the typed
reservation-acquisition projection itself and budgeting the exact requested
card. Its production qualification admitted one exact A100 worker on
`prod_research_cluster_eks` with `ZERO_COST_ADMISSION`, no paid pool key, and
no paid claim. The worker reached `READY`, served the classified request, and
was removed through normal zero-demand teardown.

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

The following exact L4 campaign then proved the complementary statically
disjoint edge. Lifecycle 141, service hash
`b519fa0f-37d9-4fee-9fd8-b575495ad88c`, committed one schema-3 paid claim for
AWS Spot `g6.2xlarge` in `eu-south-2b`; the locked reservation projection
contained only A100, A100-80GB, and H200. No incompatible reserved unit reduced
the L4 residual. Instance `i-0a8d91515fc10f2ae` launched at 03:40:09 UTC,
reached SkyServe `READY` in about three and a half minutes, and completed the
authenticated marker/result qualification. The request used Spot only; no
ordinary on-demand capacity appeared. Zero demand then removed the claim and
replica. Controller down began at 03:46:33, AWS recorded user-initiated
termination at 03:46:57, EC2 reached `terminated` at 03:52:37, Spot request
`sir-7nvfhtrk` closed, and delete-on-termination root volume
`vol-05c66f5f69916044a` no longer existed. PostgreSQL ended with zero replicas
and zero paid claims. The launch-time market price was $0.1402/hour; the larger
request price was only the Spot ceiling.

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
Provider proof renewal and bounded read-only provider observation remain
outside the transaction. They freeze identity and proof scalars only; no worker
construction, provider mutation, or launch effect may begin before the fused
commit. No nonessential work or contended lock acquisition may run after the
short-lived authorization is minted. PR #1750 merged this correction as
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

The deployed linearized-planner predecessor therefore stopped comparing a plan
made from one demand snapshot with a later one. It prepared only immutable
replica/runtime/shape/Kueue inputs, acquired the short in-process routing epoch
before SQL, and rebuilt demand and reserved supply under PostgreSQL locks before
invoking the pure planner. That removed the heartbeat-sized publication race,
but it still published the plan/head before the separate paid-claim Phase A.
Release ``1.1.1575`` proved that this remaining split can race the next demand
generation. It is superseded by the combined plan-and-accepted-wave transaction
defined below.

In the steady state, the routing epoch is still acquired before SQL and no
manager or actuation lock is held while entering SQL. One repository-wide total
order then covers protocol, service/version, demand/report, route, reserved
allocation, capacity/Kueue, plan, every candidate or retained paid pool, and
their dependent claim/waiter/replica rows. After that complete union is locked,
the transaction reconstructs one exact current snapshot, invokes the sole pure
planner once, clips provider-free templates, finalizes the durable policy
transition, and writes the plan/head plus the accepted replica/claim wave. It
acquires no later authority/read-set lock; PostgreSQL may take ordinary DML
locks for the already-predeclared writes. Only the exact committed members may publish local
target state or initiate their graph-fenced provider effect.

This collapses both the optimistic supply comparison and the later paid-claim
lease into one canonical locked transition. It does not weaken freshness or
treat changed demand as equivalent: a report committed before the service lock
is a current planner input, while a report that obtains that lock afterward is
the next causal generation. Unknown, incomplete, stale, or route-mismatched
demand grants no provider authority. The planner mutates neither autoscaler nor
repository state; the durable policy state is written inside the transaction,
and only its process-local cache is refreshed after commit. A database failure
therefore exposes no successor plan, paid row, local target, or provider
authority.

The promoted controller has no second publisher or compatibility branch. The
former controller-local ``_publish_ordered_paid_authority`` helper is removed;
all promoted positive and zero residuals use the same in-place
``plan_and_admit_current`` transaction, which replaces
``plan_and_publish_current`` and ends by calling the connection-local paid
pool/row insertion core. The repository's lower-level
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
`3088071b0c64000893cbd228d9ca56a3ad987a76` found the pure-planner extraction
source-complete. The durable adapter calls the canonical planner exactly once,
consumes locked Kueue classes through immutable decision inputs, and does not
temporarily clear or restore warm retention. Reserved supply and both
``utilization_gate`` modes enter the same ``ReservationPlanningInput`` and the
same planner. Production later proved that the surrounding policy state and
paid admission were still split across process memory and two transactions.
Format 6 and the combined transition in this design are therefore required
source work, not qualification-only cleanup. They add neither a duplicate
allocator nor a relational migration.

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
target, local actuation, and retirement authorities are all zero. Every
  format-6 head carries non-null policy state: this no-effect result copies the
  prior minimal hysteresis, adoption, and paid-window values without reducing
  the demand generation or consuming paid-window authority. It may advance
  envelope source identity only. The repository commits the
result in the existing capacity-plan/head tables. No second allocator or
relational schema is introduced.

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

Positive demand, fresh-zero retention, gate acquisition, retryable incomplete
input, and recreate-required state are explicit typed dispositions rather than
boolean-mode combinations. An incomplete candidate does not advance the head.
A candidate plan cannot
authorize a launch; only the same plan committed under the PostgreSQL
generation/fingerprint fence becomes ``CommittedCapacityPlan``. Policy state
updates are returned as immutable next-state data and written atomically with
that committed head; only the process-local cache is refreshed after commit.
Identical snapshots produce byte-equivalent plans. Map-like inputs are
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
immutable ``next_policy_state``. The repository writes that state inside the
same commit as the plan and accepted wave; a postcommit compare-and-swap updates
only the disposable process cache. These are activation blockers, not follow-up
cleanup.

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

#### Production-qualified schema-3 predecessor

The bounded predecessor carried one typed immutable exact-reservation
projection inside the committed v4 witness and made budget authority explicit:

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

The bounded exact path required one physical pool per positive target card.
Two pools for the same target card, or a composite physical pool, remained
`HOLDINGS_ONLY`/unsettled in that writer. Logical-GPU targets additionally
required physical worker width one; physical-backend targets already counted
whole workers and could use a multi-GPU worker. These were explicit fail-closed
scope guards, not silent underfill heuristics.

Demand attribution for a flexible class remained a cheapest-compatible
explanation, not exact-card acquisition authority. General flexible reserved
acquisition therefore remained fail-closed until one planner result carried
class cardinality into an atomic exact-card matching/grant protocol. A card-set
union or pool-order fallback was not an accepted substitute. An unmatchable
flexible witness published neither demonstrated need nor a causal digest, so a
settled zero grant could not unlock paid residual. For a proven exact-card
witness, partial or zero reserved grants could settle the aggregate/digest gate
only after fresh locked pool observations proved that the per-card discovery
cap covered the target or all spendable supply. Grant settlement alone was not
enough: at final capacity admission, every granted unit had to be represented
in the same PostgreSQL lock by usable zero-cost replicas, live pending zero-cost
intents, or currently feedable allocation tail. Only the remainder after that
reserved commitment was genuine paid residual. This final-row fence prevented
a stale-high claim heartbeat from turning peer holdings that were still being
reclaimed into premature Spot authority.

Schema 4 supersedes that predecessor. `MATCHED` accepts the planner's canonical
typed capacity classes, exact demand is a singleton class rather than a second
mode, duplicate exact-card pools are ordinary supply atoms, and one global
assignment bound selects partial launch waves. The publisher reconstructs the
same classes and exact locked pool supply and reruns the same subset-rank
matcher before settlement. The implementation uses the bounded accelerator
card domain directly and has no residual-flow, min-cost, or alternate fallback
allocator. `EXACT_SINGLETON` is removed from current source; the bullets above
remain only as the production history of the schema-3 writer.

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
6. Atomically commit the exact capacity plan, plan head, paid replica/claim
   wave, and cap debit before any worker construction or provider effect.
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
| Source base | Merged PR #1857 extends each accepted capacity-admission wave through its complete executable request graph. It prepares canonical request bytes provider-free, then commits plan/head/policy, debit, replica, claim, association, request, retention pin, queue row, and replica pointer all-or-none in one checkout and one atomic correctness commit. The existing optional minute-history projection then commits or rolls back best-effort on the same checkout. The durable queue is the recovery source; postcommit controller workers are optional adopters, not a correctness handoff. The controller and repository share one 100-member atomic-wave bound while the service target, paid cap, provider window, and launch concurrency remain independent. Paid-wave fairness is computed only across active, positively priced, exact-shape AWS/GCP Spot catalog cards, so configured reserved-only A100/A100-80GB/H200 cards consume no L4 transaction slots. Real PostgreSQL converges a 420-member target as 100/100/100/100/20 fresh generations and rejects a deliberately stale successor without changing the first graph. Qualification-runner merge, deployment, and live qualification remain open. It changes no scheduler object, infrastructure, EFS/PVC, or provider placement policy. Serve067 is additive control-plane DDL that aligns existing constraints and guards without a table or service-data rewrite. |
| Immutable planner correction | **The existing plan/replica/claim fusion is deployed; its source-only extension through request binding is not deployed.** One keyword-only frozen snapshot feeds one pure durable logical planner invocation. Its typed candidate separately records cold demand attribution, supply-aware actuation, warm/transition retention, reservation commitments and whole-backend padding, genuine paid residual and cap-bounded cold-launch authority, completeness/infeasibility, source generation, and snapshot/candidate fingerprints. The source successor locks the elected version, exact server-owned service YAML, semantic controller configuration, catalog ordering, controller incarnation/owner epoch, demand, route, allocation, capacity/Kueue, prior plan, pools and dependent effects; invokes the planner once; and commits the exact accepted wave plus its complete generic request graph before releasing the service-row lock. Provider launch materialization consumes the exact committed spec/config/catalog/project evidence. Only disposable observations update postcommit. PR #1786 already carries exact per-node width times task-authoritative node count for physical backends. Lifecycle 152 emitted multiple schema-6 successor heads and paid waves, but its recovery failures prevented a complete scale receipt; its full cleanup graph is now exact zero. |
| Deployed control plane | A fresh direct `sky api info` query verifies healthy SkyPilot release `1.1.1612` from source commit `1614a36119e46fab5beb86854db34f27c33ba16b`. A fresh Helm query through the private EKS SSM tunnel verifies revision 736, image `1.1.1612@sha256:b8fbcba50f591e46ac95d2406506c4e0ea3152af121fd1f7d602fd58378fb0ce`, two API pods, two controller pods, and seven executor pods, all `Running`. PostgreSQL remains the sole central store and Helm storage is disabled. The atomic paid-wave request-binding correction described here is not deployed. |
| Fixed paid pacing | **Deployed since `1.1.1578`; final fast-scale qualification remains.** Durable logical services use one configured fixed wave and PostgreSQL owns the accepted paid-window cursor across takeover. Campaign `spot-e2e-0901k` proved that the bounded 120-unit service window no longer truncates the target; its delayed second wave identified terminal-only provider feedback as the remaining latency source. |
| Atomic plan and paid admission | **Plan/head/policy/replica/claim admission has been deployed since `1.1.1578`; atomic executable-request binding is source-only.** The successor invokes the one planner and inserts the accepted wave plus association/request/queue/pin/pointer before releasing the service-row lock. No freshness comparator or TTL is relaxed. There is no second request-admission transaction, singleton fanout, batch-to-singleton fallback, or process-local recovery handoff. Release `1.1.1583` separately closed restart-adoption mutation by making existing-claim replay validation-only. |
| Paid restart replay and frozen cleanup | **The replay correction, historical repair, and final row settlement are production-complete; the temporary repair is ready for removal.** Recovery supplied priority 0 to an adoption UPSERT that rewrote nine existing priority-20 claims after their profiles were frozen. Release `1.1.1583` made existing claim replay/adoption validation-only and deployed a cleanup-only transition accepting solely a current priority of 0 whose historical-priority reconstruction exactly matches the frozen profile. Production released all nine claims. Release `1.1.1584` then consumed each exact same-record irreversible `COMMITTED` receipt atomically with its replica; supported cleanup also settled the two older `ACTIVE` rows. The affected PostgreSQL graph reached exact zero and stayed provider/cleanup-clean for 372 seconds, satisfying the strict-removal gate. |
| Provider-native paid E2E | **Count and cleanup pass on the historical writer; the five-minute and exact-ledger gates remain open.** `spot-e2e-0901k` reached 113 concurrent provider-`RUNNING` GCP Spot L4 VMs and first crossed 100 at 343.5 seconds, with zero ordinary on-demand/wrong-shape capacity. Normal cleanup returned claims, debits, VMs, disks, operations, and the service to three exact-zero samples. A homogeneous cohort-15 successor must repeat the run within five minutes, complete 10,000 authenticated async requests, and capture fresh positive queued/in-flight/processing plus exact terminal-ledger evidence before natural exact-zero drain. |
| Format-4 activation | **Superseded cleanly.** No older capacity plan, claim, or provider effect crossed the strict-current decoder boundary before format 6 activation. There was no row rewrite, compatibility decoder, storage migration, or infrastructure change. |
| Format-6 activation | **Complete from an exact-zero service recreation and current through `1.1.1598`.** Current writers strictly reject formats 1--5; lifecycle 152 and later campaigns committed schema-6 heads and paid waves. The service is now absent, so the next clean fleet creation requires no retained service-version migration. |
| Lifecycle-137 evidence | Release `1.1.1554` reached exactly 100 provider-`RUNNING` GCP Spot one-L4 workers with zero ordinary on-demand and zero wrong-shape capacity. All 10,000 authenticated warm requests returned first-attempt HTTP 200. Normal down converged service, replica, claim, waiter, VM, and disk state to exact zero before the schema-3 cutover. |
| Lifecycle-136 evidence | Run `9462207b-e026-4c5e-b610-acaba61e9b0a` on `1.1.1550` reached exactly 100 provider-`RUNNING` GCP Spot L4 VMs, with zero on-demand and zero non-L4 VMs. It accepted the 10,000-ID continuation and subsequent 5,000-ID extension. Normal teardown reached provider zero in about 3 minutes 16 seconds and full PostgreSQL/provider/disk zero in about 3 minutes 45 seconds. The immutable bundle records SHA-256 `audit.jsonl` `51807331f170d1352e9001324bd2e66f169a8a04867b7ca9bf94d8c4b953a8d7`, `arm.json` `92542d925ad50f0916cd8dcdc3977d27aa7f6a5e27b269445e03b70eadc36e70`, and `guard.json` `54a503e1f83eaa4899bce38bcc254591f885587ba87e81241fe3332a4188a649`. |
| Cold-scale timing | **Count is proven; the five-minute current-writer objective remains open by 43.5 seconds.** Campaign `spot-e2e-0901k` first reached 100 at 343.5 seconds and peaked at 113. Provider census showed the first 60 VMs ready well before the terminal launch receipts opened the second wave. Serve066 moves that one economic feedback edge to exact provider-`RUNNING`; it does not erase cooldown evidence or weaken capacity failures. |
| Telemetry | PR #1783 is deployed in the current source lineage. The current demand endpoint is controller-independent and, after lifecycle-141 drain, reported two fresh complete HA reporters with exact queued, async-processing, HTTP-in-flight, and total-in-flight values all zero. A source-only successor now projects each finalized `CommittedCapacityPlan` into the existing minute autoscaler history immediately after its authoritative commit. The existing history read requires the projection generation, digest, and validity horizon to match the current plan head and service version/hash, returns the PostgreSQL clock, and lets the dashboard reject expiry against that clock; no new endpoint or table is added. It is not deployed or production-proven yet. Request history retained the classified successful request. The qualification client did not create protocol-covered async-ledger rows, so a current-schema nonzero exact terminal-ledger/UI capture remains a full-design acceptance gate. Telemetry is not provider billing or launch authority. |
| Writer protocol | Public API 93, worker projection 10, source non-pool capability cohort 15, and async request-ledger protocol 1. The deployed capability cohort must be freshly queried before activation. Fresh ordinary-paid and `UNKNOWN_CAPACITY_REPLACEMENT` GCP effects require a homogeneous cohort-15 API/controller/executor fleet; older cohorts are settlement/cleanup only. |
| Storage | PostgreSQL is the sole central correctness store; Helm `storage.enabled=false`; no SkyPilot EFS or PVC. The historical schema-3 cutover rewrote no schema. The source successor now includes forward-only Serve067 additive constraint/guard DDL, with no table or service-data rewrite. |
| Service activation | **The qualification service and `boltz-l4-fleet` are absent while the paid correction is qualified and deployed.** Activation requires Serve067 at the control-plane schema head, but the exact-zero test lifecycle needs no service-row rewrite or retained service-version compatibility path. The scale qualification uses an unpinned-instance AWS/GCP Spot-only exact-one-L4 task, an 800-logical-slot service ceiling, and the unchanged 420-prepared-physical-launch Helm throttle. Its 800 physical launch candidates exceed the at-least-100 provider gate; those temporary qualification bounds do not raise the long-lived production cap. The later clean `boltz-l4-fleet` recreation retains `min_replicas: 0`, zero fill floor, `utilization_gate: true`, East A100/A100-80GB and PHX H200 through the existing server-owned contexts, and AWS/GCP L4 Spot only for genuine residual demand. |
| Paid-location catalog | The two regionless paid templates expand into exact immutable cloud/region/zone/shape pools and remain Spot-only. Of the four missing commercial AWS G6/L4 regions, Zurich (`eu-central-2`) is the only qualified candidate: it has a ready source patch, a compatible curated image, and a successful real Spot launch/driver/workdir/teardown proof. Upstream source PR #10587 remains approval-blocked even though all checks pass, so source support is not yet merged or released. Draft catalog PR #191 was refreshed onto catalog master `69166fce3ece5b9dffe639d3e9ceca2ee1f89fa1`; its diff remains exactly 1,127 Zurich rows and no deletions, producing v8 VM hash `2e0ca474d692a484ba60e39af45d62babd5492376394bb732ea7e9a5d2b5614b` from current base/non-Zurich hash `f242f8b176755ab0f53ec7a8f112ba49c32be746dfd2df4c8879558f3136793a`. It must remain draft until #10587 merges, the publisher identity attests Zurich opt-in, source support is released before the shared catalog, and the authorized publisher makes GitHub and S3 byte-identical. Sao Paulo lacks a compatible curated image and launch proof; Hyderabad has images but no available opted-in account or launch proof; Malaysia has neither images nor opt-in/launch proof. No other missing commercial G6/L4 location passes all three gates, so none is added speculatively. GovCloud is outside this commercial catalog scope and also lacks the required source/image/credential/proof chain. `eu-south-2` and `me-central-1` already have hosted VM and image rows and must not be duplicated. |
| Reserved occupancy | At 2026-08-26 23:09--23:13 UTC, East had 328 healthy compatible GPUs on 41 nodes: research requested 45 and 283 `boltz-l4-fleet` Pods requested the exact remainder; all 283 were Running and Ready, with zero free compatible GPU and zero pending research or fleet GPU Pod. PHX had 512 healthy H200 GPUs: research held 482 and the unchanged Kueue policy admitted 30/30 fleet Workloads; all 30 Pods were Running/Ready and PostgreSQL `READY`, with zero pending research GPU Workload. PostgreSQL independently reported exactly 63 A100, 220 A100-80GB, and 30 H200 reserved replicas `READY`, with zero durable intent pending. Thus the same lifecycle occupied East 328/328 and PHX 512/512 without changing scheduler policy. |
| Reserved readiness projection | For the final PHX replica, PostgreSQL committed the intent at 22:43:32, the Pod appeared at 22:43:55, Kueue admitted it at 22:43:56, and the Pod became Ready at 22:44:32. PostgreSQL projected it `READY` only between 22:52:25 and 22:52:40, exposing a separate roughly eight-minute status-freshness lag rather than a capacity/admission failure. The post-Helm 23:13 UTC census retained the exact 30/30 admission and readiness with no churn. |
| Claim-heartbeat convergence defect | Resolved in source, deployed, and dark-verified in production. Lifecycle 117 had logged successful exact reclaim-policy proofs followed 7--15 seconds later by rejected claim-set heartbeats because the broker minted the five-second ticket before entering the PostgreSQL replacement and its protocol/lifecycle locks. PR #1750 passes an authorization callback into the state transaction, locks protocol, owner, immutable version/projection, claim-set/edge rows and the legacy projection, reconstructs exact scope, and only then reads already-renewed PostgreSQL proof receipts. Proof logging completes before the ticket timestamp; the ticket is then immediately validated and written. Ordinary drained boundary failures remain fail-closed and boundary ambiguity remains controller-terminal. The correction changes neither Kueue nor TTL/batch/quantum limits. Real-PostgreSQL tests cover waits beyond the ticket lifetime on the affected lock paths. Release `1.1.1519` then produced eight consecutive observed successful claim/publish rounds after controller takeover with no rejected heartbeat. |
| Reserved teardown projection | Complete. PR #1747 projected all formerly blocked associations and retired 194 rows. PR #1748 normalized current writers to the existing immediate-removal marker and accepted only the exact `1.1.1516` `FAILED/FAILED` shape as an N-1 DB-retirement candidate. Release `1.1.1517` plus the supported orphan purge retired the final two rows through exact PostgreSQL authority and independent owner, record, cluster, and Kueue fences. No provider, Kueue, schema, migration, or manual-cleanup behavior changed. Exact service/control-plane/Kubernetes/GCP zero is production-proven. |
| PHX access | The controller identity can exact-read the required namespace/queue and manage only worker Pod/Service lifecycle; it cannot list or patch ClusterQueues. The worker ServiceAccount is tokenless and cannot read Pods, queues, or secrets. A historical audit-only group still has an unused broad Kueue LIST grant from platform PR #8800; it is read-only, has no scheduling effect, and is not used or expanded by this rollout. |
| Paid state at idle | **Production-proven on the final writer through +30 and the configured stale/quiescence horizon.** Lifecycle 141 normal demand-driven down removed the paid claim and replica, terminated the exact AWS Spot instance, closed its one-time Spot request, deleted its root disk, and returned PostgreSQL and the provider to exact zero. At 04:24:37 UTC, more than 30 minutes after the Spot request closed and more than 22 minutes after the retained +10 exact-zero sample, PostgreSQL still had zero replicas, paid claims, paid waiters, and zero-cost intents; the fresh plan targeted zero on every card, both authoritative HA reporters were complete/fresh/idle, the instance remained terminated, the Spot request closed, and the root volume absent. That interval exceeds the configured 20-second instance-stale, 70-second controller-quiescence, and 180-second reserved-observation horizons. No on-demand or wrong-shape capacity appeared. |
| Routing and queue | Lifecycle 119's low-priority run produced a small deadline-weighted target; the high-priority run increased the target through 49, 64, 128, and 178 before the paid cap clipped it at 100. The bounded stimulus recorded 2,248 submission starts, 289 accepted requests, 252 completion markers, and definitive queue-full rejections/retries; it is not the separate 10,000-terminal-request ledger proof. PR #1765's deployed capacity-time planner uses deadline buckets, exact compatibility, per-card service-time estimates, finite supply availability, and paid cold lead. A fresh current-schema nonzero queued/processing/in-flight/completed UI and heterogeneous capacity-time proof remains open. |
| Partial mixed proof | Provider/DB censuses at 2026-08-25 19:45:47.538 and 19:45:56.281 UTC bracketed a 72-request completion wave and both had 44 reserved plus 28 paid replicas all `READY`, the same 28 AWS Spot instances—27 `g6.2xlarge` and one `g6.4xlarge`—and zero on-demand. The wave completed from 19:45:48.956 through 19:45:51.187; every request performed 9.533–12.451 seconds of concurrency-one GPU work, so at least 28 necessarily executed on Spot beside the 44 reserved workers. The Spot instances later fully drained at the provider. |
| GCP Spot lifecycle proof | **Count, no-spill, warm-request, and teardown are complete.** Lifecycle 137 on `1.1.1554` reached exactly 100 concurrently provider-`RUNNING` one-L4 GCP Spot VMs with zero ordinary on-demand/wrong-shape capacity, served 10,000/10,000 authenticated warm requests with first-attempt HTTP 200, and completed normal exact-zero teardown. Earlier clean-frontier evidence reached 100 in 3 minutes 41.9 seconds and peaked at 117; lifecycle 137's roughly 9.5-minute run correctly retained recent-failure cooldowns. |
| Final load proof | **The constituent exact-card placement and warm-transport proofs are complete; final-writer mixed convergence remains open.** Lifecycle 137 completed 10,000/10,000 authenticated warm requests, the historical mixed campaign proved 44 reserved plus 28 Spot workers serving concurrently with zero on-demand, lifecycle 139 proved exact compatible reservation admission, and lifecycle 141 proved a final-writer statically disjoint Spot request and teardown. Lifecycle 152 peaked at 87 real provider-`RUNNING` Spot VMs and then froze on restart recovery/cleanup; release `1.1.1583` safely returned its real AWS provider resources to zero but does not supersede lifecycle 137's scale receipt. A current-schema PR-#1854 AWS/GCP multi-wave campaign with exact async-ledger coverage, HA takeover, and provider-native teardown remains required. |
| Demand/publication ordering | The deployed plan/head/policy/replica/claim writer leaves request binding postcommit, and `spot-e2e-0901ac` exposed both that publication race and its 420-singleton database fanout. Merged PR #1857 removes the gap per accepted wave: every accepted member's complete generic request graph is in the same capacity transaction, and queue visibility at commit is durable recovery authority. Optional local workers adopt exact committed request IDs; construction failure, lost acknowledgement, process death, and HA takeover all recover from PostgreSQL without singleton resubmission. The same-checkout best-effort-history outcomes pass. A maximum 100-member transaction survives a real post-COMMIT acknowledgement loss with its queue claimable; a 420 target converges across five fresh bounded generations with exact debit/graph cardinality; stale authority between waves fails before planner entry and resumes without duplicates after a fresh report. Provider-free GCP project/cohort and executable-tamper gates pass. Deployment and live scale/teardown receipts remain open. |
| Utilization/allocation causality | **Production-qualified for compatible and statically disjoint exact-card demand.** PR #1792 stabilized the decision-equivalent acquisition witness; PR #1794 bound budgets to exact cards; PRs #1795 and #1796 made statically disjoint L4 admission canonical and independent of unrelated A100/H200 allocation churn. Lifecycle 139 selected one zero-cost A100 and no paid claim. Lifecycle 141 selected one L4 Spot while committing zero incompatible reserved capacity, then returned to exact zero under `utilization_gate: true`. Flexible mixed-card acquisition, nonzero exact-ledger UI capture, multi-node paid accounting, and HA takeover remain full-design acceptance gates. |

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

### Capacity-time SLA planning (deployed; combined live stress remains)

The earlier uniform deadline weighting was a safe first-order backlog target,
but it treated already-ready compatible slots as if they also had to launch.
PR #1765 replaced that production path with one discrete capacity-time plan.
The current planner accounts for each finite slot's availability, uses all
timely compatible finite supply before prospective paid capacity, and debits
GPU-time once across strict-priority deadline buckets. It remains deployed in
`1.1.1572`. The exact A100 and L4 campaigns exercised its compatible and
statically disjoint placement edges. A combined multi-priority, multi-card
deadline stress remains a full-design acceptance case.

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

For `DURABLE_FEED`, observability consumes that same finalized result rather
than recomputing mutable autoscaler state. Plan/head/policy/admission commit
first. Before provider effects, a bounded best-effort transaction projects the
`CommittedCapacityPlan` into the existing `serve_autoscaler_history` minute
bucket using the plan decision time. The plan connection is retained across
commit and reused for this bounded second transaction, so no pool checkout can
consume the authority lease. A telemetry failure cannot roll back or suppress
authority; a later reconciliation retries it. The old controller-sync
writer checks and locks explicit `LEGACY_CONTROLLER` ownership in the same
transaction as its upsert; unknown ownership fails closed. No new history
table, migration, allocator, endpoint, or provider read is introduced.

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
- durable demand, route projections, paid plans, claims, service paid-GPU
  debits, and provider/account/pool admission debits;
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

An exact empty or deletion-only Kubernetes NodeList is successful typed
topology evidence with zero matching nodes; it is not topology nonconformance.
It contributes zero capacity and may participate in whole-fleet proof renewal
without suppressing a healthy sibling context. A launch authorization still
requires at least one positive matching node for the target accelerator's
reviewed flavor. A returned node that violates the exact selector, product,
resource name or per-node GPU width remains hard nonconformance rather than
being silently skipped.

The broker must preserve the same distinction. Before the configured phantom
debounce completes, a successful empty result shelters existing authority as a
bounded transient observation and publishes no exact-card launch evidence.
After confirmation, it is authoritative zero capacity rather than a blackout:
the round keeps the normalized claim edge, withdraws grants and feeds, and
publishes the requested accelerator with count zero. A true observation
failure or malformed split continues to publish no exact-card envelope. This
lets a complete multi-pool allocation authenticate a zero sibling without
letting that sibling suppress or poison healthy capacity.

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

locked demand + route + reserved allocation/inventory + policy head + paid pools
    -> one pure reserved-first/paid-residual plan
    -> atomic format-6 plan/head/policy/replica/claim/debit commit
    -> postcommit binding and exact provider guard
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

Here ``existing paid capacity`` means compatible current-version ready,
provisioning, or cleanup-unproven purchased supply. Independently, every old-
or current-version paid row without durable cleanup success consumes the
service's configured ``max_live_paid_gpu_units``. Keeping these two projections
separate avoids both false suppression of an upgrade and duplicate billable
capacity.

The relational ``sky_down_status`` scalar is cleanup authority and its
ReplicaInfo JSON copy must match. A matched ``SUCCEEDED`` proof removes the row
from the one normalized locked row view consumed by both planning and the
connection-local insertion core before either interpretation can inspect stale
historical pool, zero-cost, or shape copies; durable cleanup therefore cannot
leave phantom paid capacity. If cleanup is not proven, the relational
``paid_capacity_pool_key`` is authoritative and the JSON pool key and zero-cost
classification must agree exactly, including both missing-copy directions.
Malformed or contradictory cleanup-unproven rows fail the fused transaction
closed.

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

Only provider-free computation inputs are prepared before the correctness
boundary. The controller acquires its short routing epoch before SQL and holds
no manager or actuation lock while entering the repository. A prepared planning
fingerprint covers the immutable service/runtime inputs; PostgreSQL tuple
revisions are excluded, so a byte-equivalent replica rewrite cannot create
false drift. Demand, reservation allocation, inventory, pool headroom, policy
state, and paid accounting are reconstructed only from locked PostgreSQL rows.

#### Provider-free paid templates

A staged ``PaidLaunchSpec`` is a deeply immutable, keyword-only value. It contains no
mutable ``ReplicaInfo``, ``Location``, mapping, list, callback, closure, worker,
provider reservation, or object whose later mutation could change admission.
It contains only the provider-free facts that are stable independently of the
planning transaction:

- service name/hash, lifecycle, elected version and immutable spec/config/
  capability identities;
- a stable replica-record UUID (the member nonce), ordering ordinal and
  candidate numeric replica ID that the transaction must validate against the
  locked identity namespace;
- the deterministic cluster-name seed and frozen worker-construction bytes;
- exact Spot provider, provider account, cloud, workspace, region, zone,
  instance shape, pool and frontier identities;
- exact accelerator card, per-node width, node count and resource override;
  and
- the canonical catalog/price fingerprint used to prepare that exact
  provider-free location.

A launch spec deliberately contains no plan generation/content, claim, plan-unit
or physical-GPU debit, demand priority, demand/report/route epoch, TTL,
timestamp, association, request, queue, pin, spend lease, policy transition,
provider operation, or provider effect. It also has no proposal or template
digest that could become a second durable identity. Those values either come
from current locked state or are derived and cross-checked by the one transaction. Numeric
replica IDs are reusable and never stand alone as a fence; the same candidate
ID with a different locked UUID is a conflict.

The template also carries no serialized initial ``ReplicaInfo``. PostgreSQL
derives the ingress port from the locked YAML plus elected immutable service
spec, invokes the one pristine paid-row constructor, and stamps ``created_at``
from the paid-pool transaction's database clock. That constructor owns every
initial readiness, retirement, cleanup, recovery and attribution default. The
producer exercises the same constructor with a null timestamp but cannot
provide row bytes or lifecycle state. Transactional readback and postcommit
materialization compare the complete committed storage document against the
same constructor, preventing a wrong port or pre-stamped terminal state from
becoming provider authority.

No new independently idempotent member key is invented. Preparation stages one
bounded, state-aware, canonically cost-ordered set of launch specs. Its size is
the sum of the configured per-card backend ceilings, not target multiplied by
catalog width. The optimistic budget may become stale, so the transaction locks
and revalidates every selected pool, scans later candidates after a rejection,
and accepts only the subset allowed by the current plan, physical cap, pool,
frontier, fairness, and pacing authority. A prospective location that cannot be
prepared simply contributes no spec; there is no complete-cohort requirement,
proposal identity, or durable underfill state. SQL never fabricates or
reassigns a location. Preparation uses a pure location preview and consumes no
retry reservation. Only exact receipt members reserve their retry location,
and that dirty state is persisted before worker publication or provider effect.
Preparation performs no worker construction, database insertion, local target
publication, or provider call.

#### One repository-wide lock order

Every path acquiring any two of these lock classes uses the same total order,
not merely paths that happen to touch both plan and pool state:

0. acquire the in-process routing epoch before SQL; never hold a manager or
   actuation lock while entering SQL;
1. shared protocol/observation singleton;
2. service-lifecycle fence;
3. service-owner row;
4. immutable elected-version/spec row;
5. demand generation, then elected reports in stable reporter/session order;
6. route head, then its referenced route snapshots in stable identity order;
7. reserved allocation pool rounds, claim sets, allocation edges, projection
   rows and the required post-edge allocation reread, each in canonical
   physical identity order;
8. zero-cost intents, then replicas, then Kueue projection rows, each in stable
   physical identity order;
9. capacity-plan head, then its referenced current plan;
10. every paid-pool row in sorted ``pool_key`` order for the union of retained
    claims and all staged candidates; and
11. existing paid claims, waiters, candidate replica identities and every
    dependent row in stable identity order.

Missing paid-pool rows are inserted with conflict-safe semantics in sorted
order, then selected in that same order. Provider discovery and prepared
template order are hints only: every authority-bearing fact is revalidated
from the locked read set. Once planner evaluation begins, the transaction may
perform only writes whose identities were predeclared by that read set; it may
not discover a new authority row, change read-set identity, or acquire a new
authority/read-set lock. PostgreSQL may acquire the ordinary DML locks needed
to insert or update those predeclared identities. Frontier and priority are derived from the locked
pools/claims/waiters, not separate lock classes. A path touching only a subset
uses an order-preserving subsequence. ``pool -> lifecycle/service`` and
``pool -> demand/route/allocation/capacity/head`` are all forbidden.

Before activation, this order is audited and, where necessary, refactored in
``plan_and_admit_current``, its connection-local insertion core, outcome
recording, waiter reconciliation, association adoption, cleanup, recovery,
reserved allocation, retirement, and teardown. The standalone Phase-A wrapper
remains only for legacy/non-promoted service classes; it may not lock pools and
then invoke a validator that locks demand, route, allocation, or plan state.

#### Format-6 policy state and dispositions

The current head supplies the prior committed candidate and its minimal reducer
memory; it is not a lease a later transaction races to consume. Format 6
strictly replaces formats 1--5, whose process-monotonic clocks or missing
census receipt are never reinterpreted. ``CapacityPolicyState`` contains only:

- the last demand generation actually reduced and the upscale-observation
  counter;
- ``downscale_started_db_epoch`` and the downscale-veto counter;
- the existing snap/adoption and pending-capacity counters needed by the next
  pure hysteresis reduction; and
- ``paid_window_started_db_epoch`` plus the typed accepted per-card absolute
  ceiling in the plan's ``CapacityUnit``.

Prior target, exact-card attribution, warm/transition retention, reservation
commitment, paid residual, cold authority, padding, actuation and retirement
come from the prior committed ``CapacityPlanCandidate``. They are never copied
into policy state. The separate durable-logical ``adaptive_scale_up`` mode,
pressure baseline/streak/hold, and process-monotonic paid/downscale clocks are
deleted. Current pressure remains a planner input and observability signal, not
a second pacing state machine. The autoscaler stores only a disposable
postcommit projection of the committed candidate/state.

Every committed format-6 head, including ``GATE_ACQUISITION`` and
``FRESH_ZERO``, contains non-null policy state. Stale or incomplete evidence is
``ABORT_RETRY`` and publishes no successor head. With no prior head, a clean
genesis may be synthesized only when the locked service has no retained
provider-effect authority: no prior plan/head authority, provider-possible
replica, unsettled or still-referenced association, live zero-cost intent,
cross-incarnation reserved claim/debit, paid claim/waiter, active or ambiguous
API request, queue/pin, retention pin, unresolved provider operation, live
VM/disk, or other format-bearing effect. A current, fully validated allocation
observation may seed reserved-first planning because it is supply evidence
rather than a provider effect. Detached old-incarnation association tombstones
and their immutable API request roots are ignored only after PostgreSQL proves
an exact terminal status and canonical cause, request completion, matching
execution-generation quiescence, no resource-action linkage, and the absence
of replica, queue, pin and Kueue references. The association must independently
retain its settled terminal/quiescence proof. Genesis binds the
immutable service incarnation, elected version, ``CapacityUnit`` and maximum;
its counters, clocks and ceilings start at zero/``None``. The same transaction
may then plan current positive demand. A missing head beside any live graph is
``RECREATE_REQUIRED`` and never permission to synthesize state.

Normal teardown must make that genesis condition inductive. Before deleting
the exact retiring service row, its final PostgreSQL transaction locks and
validates the complete same-name retained authority graph:
every remaining nonterminal zero-cost intent belongs to the exact retiring
lifecycle and has a bijective committed edge to a locked replica;
every remaining association is canonically settled and execution-quiescent;
every retained request either carries the same terminal/quiescence receipt or
has already been collected after copying it; no request queue, retention pin,
resource-action link, paid claim, waiter, replica/Kueue pointer, or unresolved
provider effect remains. A normal `SERVICE_JOB_RECORDED` tombstone may retain
`NOT_QUERIED`; a provider-effect path without a recorded service job must carry
the existing exact, post-quiescence typed `ABSENT` evidence. A failed barrier
rolls the transaction back and leaves the service in retryable cleanup state.
Audit tombstones remain for ordinary retention and become naturally inert to a
successor; deletion does not relabel, rewrite, or erase them.

A materialized Kueue graph may instead carry the existing same-transaction
retirement proof produced by its exact Pod/admission cleanup. The census
accepts only that proof's exact service lifecycle, replica record, association,
and association revision in the transaction that issued it; it never weakens
the generic provider-absence validator. Likewise, a pre-effect terminal graph
is accepted only when its inert receipt matches the complete locked service and
replica snapshot, not merely its own association fields.

``PRE_EFFECT_TERMINAL`` with effect phase ``NOT_STARTED`` is itself the
provider-free authority boundary: the supported executor must durably compare
and swap that phase to ``PROVIDER_IO`` before entering provider I/O. The normal
closed evidence is therefore ``NOT_QUERIED``. Historical generic-profile
writers could additionally record ``UNKNOWN`` after quiescence when that
profile had no durable provider UID. That observation is neutral, not provider
absence, only when its exact six-field
``immutable-provider-identity-v1``/``profile-has-no-durable-provider-uid``
payload is bound to the association, cluster, profile and replica record, its
digest is canonical, and it postdates exact executor quiescence. Any other
``UNKNOWN`` shape, ``PRESENT``/``REPLACED`` evidence, provider-effect phase,
malformed envelope or live reference remains blocking. Final deletion and
headless genesis consume this same classifier; neither rewrites the retained
audit row.

The barrier is one exhaustive same-name relationship census, not
independent best-effort probes. It locks requests reached by either the
association's immutable request ID or a request's association pointer, and it
locks every retention pin reached by either request ID or association ID. A
divergent reverse pointer, any pin kind, or any queue row fails closed. The
census applies to every PostgreSQL non-pool service and cannot be bypassed by
changing its mutable ordinary-launch binding-mode field. A missing lifecycle
fence or noncanonical pool discriminator fails closed. The barrier uses the
same terminal request-root classifier as clean genesis so teardown and
recreation cannot disagree about whether retained history is inert.

Teardown first locks the complete same-name intent domain once in immutable
intent-key order. It terminalizes only exact-current, provider-free pending
intents and returns the remaining nonterminal rows as one typed census for the
replica bijection check. It does not acquire an exact-incarnation subset and
then a broader subset: that would invert row order when an old low key and a
current high key coexist. Paid-claim readers likewise use one shared
pool/service/hash/replica lock order, matching paid admission even when replica
IDs and pool keys sort oppositely.

Legacy controller launch rows with an exact, valid different service name are
outside this service's census. An unscoped or malformed legacy launch remains
globally ambiguous and blocks deletion even when its request executor is
terminal and quiesced: executor death does not prove that an already-started
provider effect is absent. Such a row requires independent evidence-backed
provider adjudication; it is never reclassified as safe from age or request
status alone.

That exhaustive cross-incarnation census occurs until a strict-valid format-6
head exists. Each strict-valid current head is an inductive durable receipt:
genesis performed the census, and every supported successor writer validated
its predecessor under the same service/head locks before replacing it. The
current receipt is trusted only after the strict service/hash/lifecycle/version/
envelope decoder succeeds. Format 5 is not such a receipt: intermediate local
images emitted it before exhaustive genesis was required. A format-5, missing,
or non-decodable-schema head therefore selects the exhaustive authority scope
and can never unlock the bounded query. A payload that advertises format 6 but
fails its content digest, service identity, or strict envelope decode instead
fails closed immediately before either association scope; scanning audit
history adds no safety to an already-invalid claimed receipt. There is no
format-5 decoder or in-place promotion. An otherwise clean format-5 head
subsequently fails the strict format-6 policy decode and requires the authorized
exact-zero test-service recreation; the activation gate deletes the old
service-scoped head/history only after both PostgreSQL and provider authority
are proven zero, then lets headless format-6 genesis perform its census.
Supported association creation, owner transfer, effect transition, cancellation,
terminal reduction,
reconciliation and projection writers serialize on that service lock; the
request root is created in the same transaction before that lock is released.
Later request execution updates may lock only the request suffix, but can only
reduce an active root to terminal/quiesced state: the supported request
repository rejects terminal
reopening, while association resolution, terminal evidence and identity are
database-guarded as monotonic/immutable. Settled detached rows therefore cannot
reacquire effect authority. A steady-state reconciliation therefore
locks only the indexed unresolved association set, exact locked replica and
Kueue attachment identities, and exact prepared replica-record collision
identities; lifecycle-local numeric replica collision checks are restricted to
the current service hash. Request-root lookup uses the request primary key and
the existing API009 unique partial association index. Every UUID comparison is
bound as PostgreSQL's native UUID type. Any old-hash row in that
live/attached/collision scope remains a fail-closed conflict. Settled detached
60-day tombstones and their closed request roots do not enter the steady
transaction, so the association/request portion of admission latency and lock
volume is independent of retained association campaign history.

The zero-cost-intent census likewise locks only ``GRANTED``, ``ACTUATING``,
``COMMITTED`` and ``RETRYABLE`` rows through the existing
``ix_serve052_zero_cost_intent_service(service_name, state)`` index in both
atomic admission and the read-only autoscaler snapshot. The
database-constrained ``TERMINAL`` shape has no lease, replica ID, replica-record
ID or commit pointer, and supported writers never reactivate it. Pointerless
terminal rows are therefore inert for both headless genesis and cross-version/
cross-incarnation fencing. A retained Kueue row that still names an intent
absent from the live lock remains visible in the separately locked Kueue census
as ``UNKNOWN``. Its immutable admission copy may provide only a conservative
capacity debit after its unit exactly matches the current service
``FillCapacityUnit``; it grants neither scheduler ownership nor demand supply,
and a unit mismatch fails closed. Every nonterminal old-hash intent remains a
fail-closed conflict. Retained terminal campaign history therefore does not
enter either steady transaction. The replica census intentionally remains
complete and unbounded in this stack: a provider-possible replica is live
authority until exact cleanup evidence proves otherwise, not inert audit
history. No new table or index is required for either history bound.

The repository returns one explicit transaction disposition:

- ``COMMIT_PLAN`` writes one complete positive plan/head, finalized policy
  state, and the exact accepted effect rows. Its paid subset may be empty after
  reserved-first deferral or provider-free rejection, but the accepted
  demand/policy transition is still complete;
- ``GATE_ACQUISITION`` commits only the acquisition witness and non-null copied
  policy state, with no capacity-effect row or consumed demand generation/paid
  window;
- ``FRESH_ZERO`` commits an authenticated complete zero head, resets
  hysteresis and the paid window, inserts no paid rows, applies the defined
  zero/downscale transition, and revokes every uncommitted positive launch
  fence;
- ``ABORT_RETRY`` covers stale/incomplete/malformed evidence, route mismatch,
  prepared fingerprint/launch-spec identity or shape, owner/policy drift, and
  transient SQL conflict. It rolls back every successor write and preserves the
  prior head; and
- ``RECREATE_REQUIRED`` covers missing/undecodable prior authority beside a
  live graph, format collision, or unsupported version graph and installs no
  successor state or effect.

Valid supply, cap, queue, rejection, class, count, or deadline changes committed
before the service lock are current planner inputs and may produce a positive
wave. Stale or incomplete evidence is never fresh zero. Route contraction or
rebinding is ``ABORT_RETRY`` rather than a newly committed zero/no-effect head.
Same-generation retries and supply-only replans do not advance the upscale
counter or paid window. ``GATE_ACQUISITION`` copies policy state without
changing ``last_reduced_demand_generation``; the later settled plan may reduce
that generation once. Takeover decodes the prior candidate and this minimal
state from the locked head.

#### Pure planning, paid pacing, and accounting

After the complete lock union, the repository samples PostgreSQL
``clock_timestamp()`` as ``planning_db_epoch``, reconstructs the exact current
demand/route/allocation/inventory/pool state, invokes the canonical planner once,
and reserves the successor generation only in memory. The planner performs no
I/O and mutates no autoscaler, manager, input, or repository state. After
arbitration, the repository samples ``clock_timestamp()`` as
``decision_db_epoch`` and revalidates every selected report, route, deadline
horizon, pool observation, owner lease and other time-dependent authority. It
derives ``valid_until`` as the minimum of ``decision_db_epoch + plan_ttl`` and
every locked source expiry, derives every committed downscale/paid-window
timestamp from ``decision_db_epoch``, and runs the pure finalizer. It then
performs only the predeclared writes. Immediately after the last write and
before commit, it samples ``clock_timestamp()`` again as
``postwrite_db_epoch`` and repeats the time-dependent validation, including
``postwrite_db_epoch < valid_until``. Expiry rolls the whole transaction back;
neither validity nor pacing is backdated to the first sample, and time spent
writing can never produce an already-expired committed authority.
Raw deadline buckets from the exact locked generation are always replanned; an
older tightening-compatible plan may be a free/reserved-capacity lower-bound
witness only and never authorizes a paid template.

Before that lock union, a bounded read-only preflight may resolve provider
identity, catalog offerings, and deterministic ranking into immutable scalar
templates. Neither the planner nor the transaction constructs a provider
worker or performs provider I/O. After commit, materialization revalidates the
committed provider identity; the AWS path specifically rechecks account
equality before the exact idempotency-token ``RunInstances`` call.

For durable logical services, the adapter converts the existing public
``max_scale_up_rate_percentage``, ``scale_up_rate_min_replicas``,
and ``scale_up_rate_period_seconds`` fields into one explicit typed
``PAID_RESIDUAL`` pacing policy. A non-null ``adaptive_scale_up`` is rejected
for this path rather than creating a second reducer; other service classes keep
their existing semantics. The recreated fleet uses 100 percent, minimum 50,
and 60 seconds. Reserved admission is independently bounded by the
exact settled allocation and durable per-pool intent window; provider actuation
uses the existing parallel one-lane-per-physical-pool, quantum-four conveyor.
Reserved intents consume neither the paid cursor nor paid-GPU headroom. The
configured wave width may use total committed service plan units as its rate
base, but the absolute paid ceiling is based on locked current-incarnation
nonterminal *paid* plan units; reserved capacity can size but never spend that
cursor.

The planner proposes a per-card paid wave but does not mark it spent. The pure
post-arbitration finalizer receives both accepted plan-capacity units by card
and accepted physical GPU units. The former advances pacing; the latter charges
the elected service's configured ``max_live_paid_gpu_units``. For a new paid period over locked baseline
``B`` with authorized wave ``W``, the absolute per-card ceiling is ``B + W`` in
the plan's ``CapacityUnit``. Zero accepted plan units preserve the prior wave
time and ceiling, so an all-rejected cohort cannot create a cooldown. The first
positive acceptance after the prior period expires starts the new period and
fixes the full ceiling. Positive acceptance inside an existing period preserves
its original start and ceiling. Later transactions may consume only its unused
per-card remainder; they cannot mint a new wave or substitute another
accelerator. Fresh zero clears the period. The
finalizer is not a second planner.

Paid accounting has deliberately distinct scopes:

1. the elected service's ``max_live_paid_gpu_units`` charges every
   cleanup-unproven paid row in that service lifecycle across retained versions
   and plan generations;
2. current paid residual subtracts compatible current-version ready,
   provisioning, and cleanup-unproven purchased supply from current demand;
3. remaining authority inside one plan subtracts only claims bound to that
   exact plan generation and content identity; and
4. provider/account/frontier pool headroom and fairness are independent
   admission limits over the exact locked paid-pool rows.

Advancing the head never frees an older cleanup-unproven claim. Fused admission
plan/GPU/pool debits are also distinct from the later executor-concurrency
launch reservation ``P``; ``P`` paces postcommit execution and cannot authorize
or erase capacity.

``PaidLaunchAuthority`` carries the plan ``CapacityUnit``, exact per-node width
and node count. One ``PHYSICAL_BACKEND`` claim consumes one plan unit but the
full width-times-node-count GPU cap; ``LOGICAL_GPU`` consumes its exact logical
GPU width. Pool/card/width/node-count equality is exact; equal aggregate GPU
count is insufficient. On-demand and wrong-shape pools contribute no headroom.

#### Atomic commit and exact recovery

A committed paid-claim row is an immutable debit and admission receipt for its
full lifetime. Its pool, priority, ``claimed_at``, capacity-plan identity,
demand identity, accelerator, units, service incarnation, replica identity,
and record association cannot be refreshed by a retry or restart. An exact
redrive may validate and reuse that receipt, but it writes neither the claim
nor the profile-covered replica projection. A changed priority requires
release of the old claim under its normal exact settlement authority followed
by admission of a new claim; it is never an in-place scheduling update.
Waiter priority remains mutable because waiters, unlike acquired claims, still
participate in arbitration.

Restart adoption has two disjoint dispositions. If the exact claim already
exists, the transaction locks the service, the sorted pool union, the exact
claim, and the replica; validates the same record, paid-only shape, and exact
pool; and performs no claim or replica write. If and only if an unresolved
legacy row has no claim, adoption may stamp its exact pool and insert the
legacy claim. A conflict update is forbidden in both the fused paid-admission
path and recovery adoption. This keeps replay idempotent without turning a
stale in-memory ``ReplicaInfo`` into durable placement authority.

The temporary historical cleanup matcher is deliberately downstream of all
terminal, execution-quiescence, association, request/pin, and provider-evidence
checks. Strict profile equality runs first. The fallback requires the current
claim priority to equal ``LB_REQUEST_PRIORITY_MIN`` (0), reconstructs the
closed historical priority domain while holding every other current claim and
placement byte fixed, and accepts exactly one match to the frozen profile.
It cannot authorize provider start, claim admission, capacity debit, or a new
request. After the fixed writer is homogeneous, the affected-row census is
zero, and that state remains provider/cleanup-clean for 300 continuous seconds,
the fallback and its transition-only tests are deleted and cleanup returns to
strict equality only. All three receipts were attached after the homogeneous
``1.1.1584`` deployment and a 372-second exact-zero provider/cleanup horizon;
the stacked removal PR is therefore unblocked.

Final provider-absence row removal and paid-retirement receipt consumption
compose in one terminal transaction after the separately committed projection
releases the claim and pin and clears the association pointer. A same-service,
same-record retirement in `COMMITTED` state is irreversible teardown authority,
not a live dependency. After the existing cleanup authority has independently
proven canonical post-quiescence provider absence, the final transaction locks
the retirement row and accepts only exact service hash, replica record,
lifecycle, original service version, non-null `committed_at`, and null
`cancelled_at`. It then deletes that receipt and the exact replica row
atomically. A missing receipt remains valid for provider-free launch rejection;
`ACTIVE`, `CANCELLED`, mismatched, active-route, or Kueue state fails closed.
Current controller identity and mutable plan generations are intentionally not
reconsulted, because a committed receipt must survive controller takeover. No
provider operation can be authorized by this cleanup transaction.

Reserved-first uses the planner's existing typed projections and introduces no
component-state abstraction. If the candidate proposes any new compatible
reserved launch, its exact allocation-bound intents are published through the
existing zero-cost transaction and the combined commit accepts no paid member
or pacing debit. The next tick replans from durable inventory. A candidate
whose complete positive target is already proven ``STATICALLY_DISJOINT`` from
configured reserved supply remains eligible for paid Spot immediately. This
may defer an unrelated paid subtarget by one reconciliation when the same plan
also needs reserved intents; that bounded delay is preferred to another durable
component protocol. A
deferred, retryable, or terminal reserved intent neither charges the paid
cursor nor by itself authorizes Spot fallback; a later locked allocation/Kueue
observation must prove the compatible reservation is no longer spendable.

Each prepared paid template receives exactly one member disposition:
``ACCEPTED``, ``RETRYABLE_DEFERRED`` or ``TERMINAL_REJECTED``. Only
``ACCEPTED`` creates a replica/claim row and spends plan/GPU units. Deferred or
terminal provider-free templates create no replica, claim, failed-history row,
or paid cursor debit. A complete ``COMMIT_PLAN`` may advance its policy transition
with an empty paid subset, but its paid cursor remains unchanged.

One bounded operation:

1. acquires the routing epoch and complete SQL lock union above;
2. decodes or constructs the permitted genesis, consumes current evidence, and
   invokes the sole planner once;
3. if new compatible reserved launch is proposed, accepts zero paid members;
   otherwise clips ordered provider-free Spot launch specs under exact plan,
   pool/frontier/priority, plan-unit and physical-GPU limits;
4. computes every accepted row/debit value that does not depend on decision
   time in memory;
5. samples ``decision_db_epoch``, revalidates every time-dependent authority,
   derives plan creation/validity and every downscale/paid-window timestamp, and
   finalizes policy state from the exact accepted subset in both units;
6. performs only the predeclared plan/head, replica, claim, waiter, paid-pool or
   service-cap writes and, for every accepted member, the shared generic
   association/request/retention-pin/queue/pointer binding writes;
7. samples ``postwrite_db_epoch`` immediately after the final write and repeats
   every TTL/source/owner validation, rolling the entire transaction back if
   any authority has expired; and
8. commits the complete graph together before any worker is built, registered,
   or started. Queue visibility at this commit is executable authority.

Each launch spec keeps one exact cheapest-first location. Pool saturation may
underfill its card but never causes an in-transaction reassignment. Process
headroom only bounds later execution; it does not shrink the transaction's
accepted ``PaidLaunchSpec`` subset. A singleton is the same operation with one
launch spec. There is no batch table or
historical manifest.

The repository returns one durable sparse capacity receipt and one binding
receipt from the same commit. Together they contain service name/hash,
lifecycle/version, finalized plan generation/content and capacity unit, each
member's ``(replica_id, replica_record_id UUID, claim, pool, accelerator, plan
units, physical GPU units, accepted outcome)`` identity, and the exact
submission, association, request, launch-generation and binding-context
identity. Every field is read back from the committed graph, not trusted from
mutable preparation. An acknowledged commit lets the manager construct only
optional adopters for those exact request IDs; it performs no request admission
and cannot submit the singleton endpoint. A rejected member never builds a
worker, and unused templates are released.

Commit ambiguity is reconciled from the normal request graph, never by
replaying a historical batch or constructing from a precommit template. A
successful transaction exposes the replica, claim, association, request,
retention pin, queue row and pointer together; a rollback exposes none. A
partial current-writer graph is corruption and permits no provider effect.
The generic executor claims the durable queue by exact request and association
identity. An unavailable, cancelled, timed-out, or transport-partial controller
acknowledgement changes no authority: queued work remains executable and
ordinary recovery adopts it. Read failure is never equivalent to row absence.

A postcommit local policy-cache or optional-worker publication failure cannot
discard committed members. Process death after commit, lost acknowledgement,
partial optional-worker construction, and HA takeover all use the same durable
queue and graph. Startup does not enumerate a remembered batch, hydrate a
manifest, or retire a newly admitted association-less Phase-A row: the fused
writer cannot create that shape. Legacy association-less rows retain their
existing exact cleanup-only path. This makes recovery sound without a batch
table or process-local handoff.

Release `1.1.1510` implemented the atomic batch and accepted
semantic-equivalent HA heartbeat advancement. Its first repeat exposed a later
fence collision: the process-local logical authority used one deadline bounded
both by fresh demand/report validity and by the oldest selected backend
occupancy sample. A busy or slow selected backend could therefore expire
queued additive launches even though the exact-card target, route, and demand
remained fresh. The steady state carries separate scale-up and destructive
deadlines from the same PostgreSQL read. Fresh report/route TTL authorizes
scale-up for `EXACT` and `ADDITIVE_COMPATIBLE` route relations. The destructive
deadline is nonzero only for `EXACT`, a durably `STABLE` LB cutover, and fresh
selected occupancy; it continues to bound retirement, drain, teardown, and
every other destructive commit. Additive fresh zero may publish a revoking
zero plan and reject new spend, but it cannot retire or drain capacity until
the reporter catches up and the cutover is `STABLE`. Unknown occupancy remains
unknown and its associated supply remains conservatively protected; it cannot
be treated as absent or released for paid residual or scale-down. A promoted
``DURABLE_FEED`` service does not admit an
``UNKNOWN_CAPACITY_REPLACEMENT`` overlap. Retaining that older exception would
create a second prospective paid-admission path and require the replica-manager
lock to enter PostgreSQL. The predecessor remains protected until a fresh probe
or exact cleanup evidence resolves it. Legacy replacement rows remain readable
and cleanable, but the format-6 happy path creates no new replacement.

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

Every fresh GCP paid association derives provider identity only from the locked
service graph. Before candidate preparation, the controller applies normal
workspace-and-region precedence to the sanitized, elected controller snapshot
and resolves one exact project for every GCP catalog location. A location with
no exact locked project is omitted from the authoritative paid budget; it
cannot become a candidate. The protocol-v2 paid-pool key and profile both
freeze that project together with workspace, region, zone, shape, Spot market,
and node count, while the association freezes the generated provider cluster
name. Canonical request rebuilding requires the prepared bytes, catalog
resources, pool, profile, and locked project to agree exactly. The ordinary
launch body continues to carry the normal sanitized locked configuration; no
second full-config or ambient-project snapshot is introduced.

The executor rejects a missing or different v2 GCP project before the bulk
provision call, for both ordinary-paid and
``UNKNOWN_CAPACITY_REPLACEMENT`` effects. A protocol-v1 GCP row is never valid
for a fresh effect under cohort 15, but remains readable for retained cleanup.
That cleanup never consults today's ambient workspace: after exact executor
quiescence, it reads the retained project and zone outside database row locks
and treats the allocation as absent only when the exact generated-name VM set
and SkyPilot-managed boot-disk set are empty, no attributable GCE insert
operation is non-`DONE`, and a second uncached census agrees. Timed-out Compute
Operation metadata is not deleted because that API call never cancels the
underlying create and would destroy reconciliation evidence. A complete set of
exact `DONE` child insert operations can settle immediately; otherwise the
existing 300-second post-quiescence propagation horizon remains required. The
finite horizon is conservative mitigation for a missing terminal child-
operation record, not a request-ID/operation receipt proof. Presence grants
only immediate fenced cleanup; it does not settle or release the paid debit.
Cleanup idempotently deletes the exact resources, waits for VM disappearance
and disk detach/delete, and then requires fresh VM, disk, and create-operation
absence before PostgreSQL releases the claim and retention pin. A missing
retained request, mismatched project/workspace, unsupported disk identity,
provider read failure, stale controller authority, in-flight create, or
incomplete quiescence remains `UNKNOWN` and fails closed.

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
  not from a controller-local empty queue. The retained demand route must be
  exact with the current selectable route set and the durable LB cutover phase
  must be `STABLE` both when authority is minted and under the final retirement
  locks.
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

Only current-generation `ACTIVE` and `DRAINING` reports participate in demand
authority, and the selected `ACTIVE` report is mandatory. `STANDBY` and `ARMED`
reports remain operational HA telemetry but are excluded from demand
aggregation, receipt watermarks, route validation, plan/claim authority,
promotion, retirement, and the public request summary. For selected reports,
unadvertised identities and recovery-fenced entries do not enter the selectable
route set. Equal retained/current selectable sets are `EXACT`; an
identity-exact retained subset under unchanged policy and queue mode is
`ADDITIVE_COMPATIBLE`; every contraction, rebinding, selected-route mutation,
or policy/mode change is `INCOMPATIBLE`.

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

The existing autoscaler section of the controller-independent history read is
also the current capacity-plan projection. One PostgreSQL statement joins the
service, current head, and exact plan generation. A minute sample retains plan
provenance only when generation, digest, validity horizon, service hash, and
service version match that current authority. The PostgreSQL clock defines the
read window; the dashboard rejects a returned horizon that has expired against
that clock. The existing history refresh runs every ten seconds and uses its
demand target and exact-card breakdown even when controller/provider route or
status enrichment is unavailable. `UNAVAILABLE`, `STALE`, and `MALFORMED`
carry null planned values and an exact reason; none is rendered as zero. An
old server or explicit `LEGACY_CONTROLLER` owner retains the prior controller
projection boundary. No API-version bump or second allocation path exists.

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
   elected service's configured ``max_live_paid_gpu_units``. The relational
   ``paid_capacity_pool_key`` and
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
   protected. `ADDITIVE_COMPATIBLE` route growth may create or continue
   capacity and may revoke prospective spend on fresh zero; destructive work
   and demand-source promotion require `EXACT` and a durably `STABLE` LB
   cutover. `INCOMPATIBLE` route
   evidence fails closed. A newer heartbeat receipt is fresh only when the current locked
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
    reserved launch, paid launch, local actuation, or retirement. It installs
    one non-null format-6 policy state that copies the minimal hysteresis,
    adoption, and paid-window memory and advances source identity while
    reducing no demand generation and consuming no paid window. Only a later
    plan bound to the causally covering settled
    allocation may actuate. The witness is read from the current PostgreSQL
    plan head with exact service/version/semantic-fingerprint/TTL ownership;
    its no-effect horizon covers the slower reserved poll and settlement cycle,
    while fresh-zero revokes it immediately. There is no mutable demand mirror.
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
27. **One plan, including observability:** durable history and the current UI
    target are projections of the exact finalized committed plan. They never
    rerun allocation, observe a different mutable generation, or translate
    missing/stale/malformed authority into a zero target.

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
runtime image IDs resolve to that digest. During the format-6 transition, keep
Serve writes and service traffic stopped from the pre-rollout zero census until
that homogeneous readback succeeds. If the reclaim-policy or writer identity
changes, invoke the existing PostgreSQL transition command to authorize one
fix-forward generation; an exact retry is idempotent.

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

Schema 4 used the same current-only cutover discipline. It activated from
supported exact zero and remains the homogeneous current format through
release `1.1.1575`; no old envelope or claim was rewritten, and a mixed
schema-3/schema-4 cohort was never accepted.

Format 6 is another hard current-only envelope cutover, not a relational
migration. Before Helm, normally down lifecycle 148 and prove service-scoped
plans/heads, replicas, associations, zero-cost intents, reserved claim edges,
paid claims/waiters, retention pins, request/provider operations, VMs, disks,
and every other schema-bearing authority are exact zero. Keep Serve writes
stopped, deploy the exact same format-6-capable image to every API, controller, and
executor, and verify every runtime digest before recreating the service. The
format-6 decoder strictly rejects formats 1--5; there is no row rewrite, dual
decoder, EFS/PVC state, or mixed-writer interval. Rollback is permitted only
before any format-6 head exists. After the first format-6 head, recovery is
fix-forward on a newer homogeneous format-6-capable image.

### Failure and rollback

The capacity-authority transition is one-way. After activation or the first
format-6 head/authority commit, repair by deploying a newer homogeneous image and reauthorizing
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
- Commit a durable plan and require its exact minute-history projection before
  provider effects. Inject a history-write failure and prove plan/head/admission
  remain committed and usable; require a later reconciliation to fill the
  projection gap. In a mixed-writer minute, require the committed-plan
  projection to win latest state while preserving pressure peaks; require the
  controller writer's ownership check and upsert to share one transaction under
  explicit `LEGACY_CONTROLLER` ownership.
- Read the current projection through the existing autoscaler-history section.
  Expose provenance only for an exact current service hash/version, head
  generation, content digest, and matching validity horizon. Return the
  PostgreSQL clock and require the dashboard to reject expiry against it.
  Independently corrupt each fence and require planned values to be
  unavailable rather than zero. Prove the dashboard displays the PostgreSQL
  target while controller/provider enrichment is unavailable and retains a
  last-good projection only until its DB-relative lease expires.
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
- Saturate the bounded offered-arrival tracker while preserving a complete
  exact-card rejection profile. Prove ingestion preserves the saturation bit,
  the durable snapshot remains available, and paid authority covers only the
  exact attributable rejection work. With one retained L4 arrival and with
  mixed retained L4/A100 arrivals, prove the unknown remainder of the 100,000
  saturation bound is never extrapolated onto those cards. Prove identical
  classified pressure/arrival tuples overlap by magnitude, different immutable
  tuples remain distinct, arrivals intersecting compatible lossy fixed-card
  work remain shelter-only, and disjoint typed arrivals retain their original
  paid authority. Prove a subsequent exact queue or rejection class authorizes
  missing scale-up without borrowing identity from the arrival sample, and the
  ambiguity shelter excludes unrelated committed cards. Prove disjoint
  high-priority arrivals do not upgrade fixed-only reservation classes.
  Exercise all 255 compatibility subsets across multiple priorities without a
  pairwise graph. With both current
  A100 work plus idle L4 supply and old/retiring A100 work plus one L4 arrival,
  prove fixed work does not inherit the L4 sample's paid authority. Seed the
  latter case with a stale inflated L4 target and prove the planning projection
  clears its demand/provider authority while the live prior identity still
  guards the successful CAS. Cover unknown-capacity work and in-flight work
  beyond materialized slots: neither may enter reserved, paid, or actuation
  authority, while only positive committed capacity is sheltered. Prove a
  subset-card fleet constructs a positive-entry-only shelter, while a fully
  attributed saturated report does not shelter unrelated idle supply. Repeat
  without compatible attribution and with partial queue/rejection/priority profiles;
  the unknown gap and incomplete evidence must publish no provider authority.
  Prove offered counters and saturation participate in normalized demand.
  Exercise the production planner with 1,000 ten-second L4 requests: a
  585-second deadline commits target 18 and a 580-second heartbeat committed
  before the service lock is replanned from that exact generation to target 19
  under a new semantic digest. An older tightening-compatible plan may remain a
  free/reserved-capacity lower-bound witness but must authorize zero paid
  templates. Prove a deadline extension rejects retention. A saturated report
  must not prove fresh aggregate zero or authorize retirement.
- Seed conflicting process-local per-tick Kueue fields and warm-retention
  state, then invoke the durable adapter with a different immutable decision
  snapshot. Prove the canonical planner is called exactly once, observes the
  process-local fields unchanged, and returns without mutating or restoring
  either field. The durable policy transition is written with the plan/wave;
  only the disposable process cache may change after commit.
- Configure the durable logical planner with the fleet's sole fixed pacing
  policy: 100 percent, minimum 50, period 60 seconds. Prove the first accepted
  subset starts one DB-epoch window with its fixed per-card ceiling; a partial
  subset may consume only that window's unused per-card remainder; and an
  all-rejected subset starts no window. Same-generation retries, supply-only
  replans, and a committed ``GATE_ACQUISITION`` consume neither a new window nor
  demand generation. ``ABORT_RETRY`` leaves the head unchanged, authenticated
  ``FRESH_ZERO`` clears the window, and takeover from every transition produces
  the same next result from PostgreSQL. Reject a non-null
  ``adaptive_scale_up`` on this durable logical path and prove no pressure
  baseline, streak, or hold state is encoded.
- Run focused probe batching, replica-manager, paid-capacity, reserved
  admission, route, request-ledger, recovery, and teardown tests.
- Run real-PostgreSQL tests for transaction atomicity, lost acknowledgements,
  stale identity, owner takeover, provider rejection/ambiguity, and concurrent
  admission.
- Compose the exact paid restart lifecycle in one real-PostgreSQL regression:
  admit and bind a priority-20 ordinary-paid claim, invoke the production
  restart adoption entry point with priority 0, and prove the complete claim
  and replica rows remain byte-identical. Preserve a separate missing-legacy-
  claim case that still inserts at the supplied default. Then reproduce only
  the historical 20-to-0 corruption and prove provider-present and
  provider-absent terminal cleanup both succeed. Mutating any other claim,
  plan, placement, association, record, reference, or profile field, or using
  any other current in-range priority, must fail closed. This composition gate
  replaces the former isolated redrive expectation that incorrectly treated
  acquired-claim priority as mutable.
- Compose provider-absent cleanup with the retirement state in the same real
  PostgreSQL graph: stage the historical priority corruption, insert an exact
  `COMMITTED` receipt under the predecessor controller, project canonical
  provider absence and claim/pin release, then invoke the production final-row
  transaction. Require both idle-proof receipt shapes to delete receipt and
  replica atomically. Independently mutate state to `ACTIVE` or `CANCELLED`,
  or mutate service hash, record, lifecycle, or original version, and require
  both rows to remain. A later replica-delete CAS failure must roll back the
  receipt delete.
- Freeze a bounded provider-free exact-L4 launch-spec set large enough for the
  configured hard cap while one or both HA reporters
  continuously advance deadline-bearing demand. If a heartbeat commits before
  the service-row lock, prove the sole planner consumes that exact generation
  and atomically commits its plan/head plus the exact accepted plan-clipped
  replica/claim subset. If authenticated fresh zero commits first, prove the same
  transaction writes a zero head, resets paid pacing, and inserts no
  paid rows. If queue/rejection/class/count/deadline or valid supply/cap changes
  commit first, prove they are planned as current inputs and may authorize a
  positive wave. Route contraction/rebinding, stale/incomplete evidence, or
  prepared identity/shape, owner, or policy drift returns ``ABORT_RETRY`` without a
  successor head; unsupported version or a live-graph/genesis conflict is
  ``RECREATE_REQUIRED``. If a report waits behind the service-row lock, prove
  the already-current wave commits first and the report becomes the next causal
  generation. Inject a SQL failure at every write boundary and prove plan head,
  policy state, replicas, claims, waiters and pool debits all roll back
  together. Delay the maximum cohort beyond each report, route, deadline, pool,
  and owner horizon; the post-write DB-clock resample must roll everything back.
  Bounded read-only identity/catalog/ranking preflight may precede the
  transaction, but no worker construction, provider mutation, or launch effect
  may occur before commit. Prove the postcommit AWS account-equality recheck
  precedes exact-token ``RunInstances``.
- Repeat with fewer preparable specs than the authorized wave. Commit only the
  accepted subset, charge only that subset, and prove there is no complete-cohort
  rejection, proposal identity, underfill state, or fabricated replacement
  location.
- Publish a current head that adds an identity-valid selectable route while a
  positive reporter retains its prior immutable head. Prove demand remains
  available, the plan and paid claim bind the current head, and scale-up
  continues. Prove an unadvertised identity or recovery-fenced entry remains
  `EXACT`; a contraction, rebound identity, selected-route mutation, or policy
  change is `INCOMPATIBLE`.
- Prove additive fresh zero revokes prospective spend but grants no scale-down,
  fresh-zero retirement, paid retirement, or logical retirement. Prove
  `PREPARING` and `ROLLING_BACK` likewise grant no destructive authority, and
  promotion waits for `EXACT` plus `STABLE`. Prove `STANDBY` and `ARMED` cannot
  alter demand aggregation, the request summary, receipt watermark, route
  validation, plan/claim authority, promotion, or retirement.
- Prove a cold one-GPU target of 100 opens exactly the minimum 25-pool frontier
  under an unchanged four-claim exact-pool bound in its first
  ``plan_and_admit_current`` commit, rather
  than serializing through the legacy service window. Repeat with four-GPU
  logical backends and prove 100 GPU units become 25 claims over seven pools.
  Repeat with eight-GPU ``PHYSICAL_BACKEND`` locations and prove 100 plan units
  remain 100 backend claims, not twelve or thirteen. Close the cheapest pool
  and prove it contributes no capacity while only the required later
  cost-ordered pools open. Include prior exact-plan debits and prove they are
  subtracted under the combined locked transaction, never during provider-free
  template preparation. Prove advancing the head cannot free a cleanup-unproven
  claim from an older generation.
- For an eight-GPU, two-node ``PHYSICAL_BACKEND``, prove cap 16 grants exactly
  one backend, cap 32 grants two, and one existing backend leaves exactly one
  new backend at cap 32. Prove advisory headroom 15 rejects, headroom 16 admits
  and debits to zero, and concurrent PostgreSQL ``plan_and_admit_current``
  transactions at cap 16
  commit exactly one claim. Reject wrong per-node width, wrong node count,
  malformed huge integers and products, relational/JSON pool contradictions,
  and cleanup scalar contradictions. Prove a matched durable cleanup proof
  ignores stale historical pool/zero-cost copies at both billing seams, while
  the same contradiction without cleanup proof fails closed. Prove an
  empty-accelerator CPU pool commits through the legacy non-planner Phase-A
  path with zero GPU debit when no GPU cap is configured, without changing
  zero-cost routing.
- In both ``LOGICAL_GPU`` and ``PHYSICAL_BACKEND`` units, exercise zero,
  partial, and full accepted subsets. Prove an eight-GPU two-node physical
  backend advances the paid pacing cursor by one plan unit while charging 16
  physical GPU units, and that per-card ceilings cannot be spent on another
  accelerator. Prove the service GPU cap, current residual, exact-plan remainder, pool
  debit, and executor reservation ``P`` remain distinct accounting scopes.
- Produce a positive reserved launch target and prove the fused transaction
  admits no paid row and consumes no paid cursor, ceiling, or cooldown. Let the
  existing allocation-bound zero-cost transaction commit the reserved intents,
  then replan from their durable inventory. Separately, prove a complete
  positive target already classified ``STATICALLY_DISJOINT`` may admit Spot
  without reserved authority. In a mixed target that still needs a new reserved
  intent, prove an unrelated paid subtarget waits one reconciliation rather than
  introducing compatibility-component state. Exercise deferred/rejected
  intents and takeover. Prove
  one parallel quantum-four lane per physical pool refills from durable intents
  while unchanged Kueue remains the PHX admission authority; only a successor
  plan from durable inventory may admit genuine Spot residual.
- Cover crash after commit before return, lost acknowledgement, partial
  optional-worker construction, local policy-cache publication failure, and HA
  takeover. Prove every successful member already has its association,
  request, pin, queue row and replica pointer, and the generic executor can
  claim all work without a controller callback or singleton submission. Inject
  the final binding failure and prove plan, head, claim, replica, association,
  request, pin, queue and pointer all remain zero. A partial graph,
  same-numeric-ID/different-UUID, or claim mismatch fails closed. Never build
  from a precommit template after ambiguous acknowledgement, and never replay
  a historical batch.
- Encode a format-6 plan with DB-epoch policy fields and prove formats 1--5 are
  strict failures. Before qualification, down to the complete service/provider
  zero inventory, hold Serve writes, deploy every role homogeneously, and only
  then recreate. Prove rollback is allowed before the first format-6 head and
  fix-forward is required afterward; there is no row rewrite or mixed decoder.
- Hold the routing epoch, then hold the pure planner after the transaction has
  locked protocol, lifecycle/owner/version, demand/reports, route, reserved
  allocation, replicas/intents/capacity/Kueue, plan head/current plan, the full
  sorted paid-pool union, and claims/waiters/target identities. Start both HA
  report writers and prove they wait at the service row;
  prove the callback sees the exact last committed generation, one plan/head
  commits, and both writers advance normally after commit. Repeat with changed
  demand already committed before the lock and prove the new semantics are
  planned rather than rejected as stale.
- Run deadlock permutations across ``plan_and_admit_current``, its
  connection-local insertion core, outcomes, waiters, adoption, cleanup,
  recovery, reserved allocation,
  retirement, and teardown. Assert every path uses an order-preserving
  subsequence of the total order, missing pools are created/locked in sorted
  order, and no ``pool -> lifecycle/service`` or ``pool -> demand/route/
  allocation/capacity/head`` edge exists. No manager/actuation lock may enter
  SQL. After the pure planner begins, permit only ordinary DML locks for
  predeclared writes; forbid every new authority/read-set lock and every lock
  for a non-predeclared identity.
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
  planner exception, final-clock expiry, and a failure at every graph write;
  prove no plan/head, policy state, replica, claim, waiter, pool debit, local
  target, or provider authority becomes visible.
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

The current qualification contract separates economic placement from provider
reachability. One generic AWS/GCP economic service lets the production
cheapest-first selector choose either cloud; its scale gate is at least 100
physical provider-``RUNNING`` VMs in aggregate within five minutes, never a
synthetic requirement that both clouds win. Exactly 10,000 stable async-ledger
identities are the sole demand stimulus for that run. A joined exact-zero
request/provider baseline immediately precedes traffic. The staged campaign
then proves exactly 10,000 resident and deduplicated offered arrivals within 60
seconds; every provider scale sample must itself retain positive PostgreSQL and
load-balancer demand, and its dispatched in-flight gauge must equal active
exact-ledger rows. Its exact one-L4 task shape, 800 logical-slot cap, and 800
held identities authorize 800 physical backends and therefore permit the
at-least-100 physical gate. If its
completed receipt has no positive ``RUNNING`` evidence for one provider, that
receipt may authorize exactly one provider-pinned canary rendered from the same
source YAML. Rendering fails before ``sky serve up`` unless the requested cloud
is exactly the provider absent from the economic result. The canary offers one
exact async request and permits one exact one-L4 physical backend
(``max_replicas: 1``, paid cap one). It is provider reachability evidence, not
a second economic-placement policy. No standalone or unnecessary canary is
billable.

The renderer removes only its typed profile/provider fields to derive one
canonical source-task digest, then binds each allowed projection to that digest.
The terminal aggregate gate requires every canary to carry the economic source
digest, its exact canary projection digest, and the SHA-256 of the exact
economic receipt that authorized it. It also joins every
qualification receipt to its matching immutable service identity and cleanup
receipt. It accepts only real provider scale samples, the exact AWS/GCP
positive-provider union, positive attributed economic telemetry, the 10,000
request terminal-ledger delta, and strict baseline-before-campaign-before-
provider-scale-before-terminal-ledger-before-drain timing. Provider shapes and
totals are reduced from complete raw samples, not trusted receipt scalars. The
gate also requires the physical scale SLO, three distinct
increasing pre-down natural-drain samples, and three distinct increasing
post-down cleanup samples with canonical AWS and GCP zero projections. Runtime
placement policy remains unchanged.

PR #1854 is the canonical executable provider-native qualifier for the next
run and supersedes PR #1813's GCP-only runner. Its merged code is source
evidence only. The gate closes only when the
qualifier emits one immutable live receipt from the intended deployed image and
service version, proves every bound provider effect is the approved GCP Spot L4
or AWS Spot L4 shape, reaches the selected small or scale threshold within its
SLO, serves its authenticated stable request identities, naturally drains, and
holds exact PostgreSQL/VM/disk/operation zero for the configured interval. A
collected smoke test, mocked provider response, unit receipt, or historical
pre-#1854 run does not satisfy this gate.

The production qualifier reads instances, disks, and operations through
the Google Compute v1 API with application-default credentials; it has no
``gcloud`` executable dependency. Every PostgreSQL sample records the exact
controller PID, IP, owner epoch, and incarnation. Every live paid claim must
retain request priority 50 and its immutable one-L4 capacity-plan link. Those
fields make a controller restart or takeover auditable from one receipt rather
than from mutable logs. The test source is not installed in the API image; the
operator copies the single qualifier file into a temporary API-pod path for the
run. The installed SkyPilot package supplies its runtime imports.

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

The latest at-least-100 provider evidence is lifecycle 137 on release
`1.1.1554`. It reached exactly 100 provider-`RUNNING` GCP Spot one-L4 workers
with zero ordinary on-demand and zero wrong-shape capacity, served all 10,000
authenticated warm requests with first-attempt HTTP 200, and returned service,
replica, claim, waiter, VM, and disk state to exact zero through normal down.
The roughly 9.5-minute cold run does not close the 3--5 minute performance
objective. Durable recent-failure/cooldown state limited many pools, while
clean pools used the configured base window of four. A follow-up benchmark
must record those cohorts separately and preserve the cooldown fence. Release
`1.1.1555` completed the homogeneous schema-3 cutover from no Serve rows, and
`1.1.1565` carries it forward homogeneously at Helm revision 681. Lifecycle 141
closes the current-writer single-node provider-effect proof; only a dedicated
multi-node physical-debit case remains open.

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
- Freeze at least one ordinary-paid claim/profile before restart. Require the
  complete claim and profile-covered replica projection to remain unchanged
  through child restart and Pod takeover. The current executable qualification
  sends and requires exact priority 50. If separately qualifying a historical
  cohort, require its frozen historical priority and cleanup to match exactly
  that one value with no other drift. Capture the affected-row census before
  and after cleanup.
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

## Remaining convergence gates

Releases through `1.1.1576` close the compatible exact-A100 reserved edge,
statically disjoint exact-L4 Spot edge, joint matcher, additive-route relation,
saturated-demand attribution, immutable fixed-wave planning, and atomic
plan/head/policy/replica/claim admission. Historical runs separately prove
at-least-100 Spot scale-out, 10,000-request warm transport, mixed
reserved-plus-Spot execution, and exact provider drain. They do not replace
these remaining current-writer acceptance gates:

1. **Complete:** PR #1809 deployed the exact pre-effect-neutral and
   zero-capacity-context corrections homogeneously as release `1.1.1578` at
   Helm revision 697. PostgreSQL remains the only central store; Kueue,
   platform and infrastructure configuration were unchanged.
2. **Complete:** reclaim-policy generation 41 has fresh simultaneous
   East-zero and PHX-positive revision-`1.1.1578` receipts. East grants no
   concrete launch authority; PHX attests the reviewed positive H200 flavor.
3. **Complete:** release `1.1.1579` deployed the confirmed typed-zero broker
   correction homogeneously at Helm revision 698. The three-pool allocation is
   complete: both East cards are authoritative zero and PHX H200 is positive;
   SQL `NULL` remains blackout-only.
4. **Complete:** PR #1811 deployed the case-only prior-history correction as
   release `1.1.1580` at Helm revision 699. The exact-zero old lifecycle was
   deleted without purge and the service was recreated with fixed pacing (100
   percent, minimum 50, period 60; no `adaptive_scale_up`). It retains
   `min_replicas: 0`, fill floor 0, `utilization_gate: true`, Spot-only paid
   candidates, no task-owned Kubernetes override, and PostgreSQL-only state.
5. **Complete:** PR #1812 deployed the case-only provider-free paid-candidate
   clipping correction as release `1.1.1581` at Helm revision 700. Lifecycle
   152 crossed the former clipping boundary and recorded repeated schema-6
   successor heads and accepted paid waves; its later restart-cleanup failure
   is tracked separately below.
6. **Complete in source:** run the provider-free source gate against PostgreSQL
   with the synchronous request pool fixed at one and provider adapters
   installed as fail traps. The implemented 100-member atomic-wave bound is
   independent from service, pool, and provider limits; fresh successors
   account for every preceding committed replica and claim. The real-PostgreSQL
   gate converges 420 as 100/100/100/100/20 and preserves exact cap/debit
   accounting across five generations. For each fused correctness wave require
   one database checkout, one atomic correctness commit, and exact cardinality
   across the nine graph components: plan, head, claim, replica, association,
   request, queue, pin, and replica pointer. Permit only the existing optional
   best-effort history transaction on that same checkout. Failure after the
   final bind rolls back the whole graph; a lost post-COMMIT acknowledgement
   leaves every durable queue row claimable. Stale source, old executor cohort,
   missing provider identity, and canonical-body mutations reject before
   planner entry, graph writes, or provider I/O. PR #1857 merged this source as
   merge commit ``6696c24da``.
7. Deploy the atomic paid-wave correction on one immutable image. Require all
   API, controller, executor, and GCP-login-init roles to resolve to that digest
   and the API/controller/executor fleet to advertise cohort 15 homogeneously
   before creating the qualification service. Apply the forward-only Serve067
   additive control-plane migration before activation; it aligns existing
   paid-pool constraints and guard functions without a table or service-data
   rewrite. This adds no scheduler, infrastructure, or provider configuration.
8. Run the provider-native scale profile against that exact image. Drive the
   800-logical-slot bounded Spot target to at least 100 provider-``RUNNING`` L4
   Spot VMs in aggregate within five minutes; do not require both providers to
   win the economic selection. Keep the shared 420-physical-launch Helm
   throttle unchanged and record physical VMs and logical L4 slots separately.
   Use exactly 10,000 authenticated async identities as the only scale
   stimulus. Submit and observe the 800 held identities first, then submit the
   9,200 zero-duration tail; within 60 seconds prove all 10,000 are resident
   and appear in the ACTIVE load balancer's exact deduplicated offered-arrival
   counters. Hold the prefix for 340 seconds, capture positive queued,
   processing, in-flight, exact attribution, and exact terminal-ledger/UI
   evidence, and require zero ordinary on-demand or wrong-shape capacity. Only
   after the economic receipt proves a missing provider, run its one
   provider-pinned, one-request canary projected from the same immutable source
   task. Then stop demand, retain three natural exact-zero PostgreSQL, VM, disk,
   and provider-operation samples for every service, and require the aggregate
   receipt to join all identities and prove the exact AWS/GCP provider union.
9. Restore the conservative Helm executor sizing, then recreate
   `boltz-l4-fleet` from the checked, current-only service definition.
   Under positive compatible demand, prove reserved capacity commits before
   paid residual, East consumes every healthy compatible physical GPU that the
   utilization gate needs, and PHX consumes every slot actually admitted by
   Simone's unchanged Kueue policy. Separately prove genuine L4 residual can
   still launch Spot. Record Kueue admission, Pod readiness, PostgreSQL
   inventory, physical GPU children, paid claims, and request telemetry; idle
   research capacity need not be occupied with the usage gate enabled.
10. **Complete:** release `1.1.1583` deployed validation-only claim
   replay/adoption plus the cleanup-only exact-priority compatibility and
   released all nine corrupted claims. Release `1.1.1584` deployed atomic
   exact-`COMMITTED` receipt consumption and supported cleanup settled every
   remaining row. The fixed writer is homogeneous; affected service, replica,
   retirement, claim, waiter, and cluster rows are exact zero; and three
   provider-native exact-zero samples across 20 AWS regions plus GCP span 372
   seconds. The strict-removal branch is unblocked and restores both cleanup
   paths to frozen-profile equality with no priority enumeration.
11. Complete a controller-child restart and controller-Pod HA takeover while a
   wave is live. Require restart-safe fixed pacing, byte-identical retained
   claim and profile-covered replica state, no duplicate provider effect,
   exact ambiguous-commit recovery, and preserved generation/fingerprint
   fences.
12. Stop demand without lowering the paid cap and prove natural drain to exact
   PostgreSQL, VM, Spot-request and disk zero immediately, at +10, +30, and
   through the full stale/quiescence horizon.
13. Retain the live multi-node paid physical-backend case as a full-design gate:
   charged paid units must equal per-node GPU width times task-authoritative node
   count while the pacing cursor advances in plan units.

Zurich catalog activation is an independent capacity follow-up, not a blocker
for fleet convergence. It remains gated on upstream source release, account
opt-in attestation, authorized publication, and byte-identical GitHub/S3
catalogs.

Cold-frontier performance remains an operational metric, not a correctness
exception. Report clean pools separately from pools carrying durable failure
cooldowns; never erase or bypass cooldown evidence to improve timing. None of
these gates authorizes changing Kueue policy, adding EFS/PVC, KubeRay,
Terraform/Terragrunt or IAM scope, or modifying `boltz-platform` application
code.

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
