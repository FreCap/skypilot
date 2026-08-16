# SkyServe demand, capacity, and telemetry convergence

Status: P1 is merged in PR #1498; P2a is implemented in PR #1499, P2b1 in
PR #1503, and P2b2 in PR #1504. The additive stack is in exact-head CI and
has not been deployed. P2b2 includes the adversarial-review
correction that separates cheapest-compatible demand attribution from
supply-aware exact-card capacity accounting. The production compatibility
precondition is satisfied, but production remains on the legacy
controller-coupled demand and route paths.

Last updated: 2026-08-16

Canonical owner: this file for request telemetry ingestion, paid-capacity
admission, and the user-visible demand/capacity contract. Durable non-pool
action ownership remains owned by `durable-serve-replica-actions.md`, and
reserved zero-cost allocation remains owned by
`serve-multi-pool-reserved-capacity-fill.md`.

## Summary

SkyServe already measures request arrivals, completions, prediction time,
in-flight work, queue depth, rejection pressure, and accelerator compatibility
inside each load balancer. It also already has PostgreSQL request-history
tables, status projections, dashboard request cards, and a zero-cost reserved
capacity broker. The missing boundary is between measurement and control:
load balancers currently send the whole report through the per-service
controller. A controller delayed by provider reconciliation therefore makes
both autoscaling demand and the dashboard stale, even though the load balancer
continues to observe traffic.

The steady state has three independent, durable publications:

```text
authenticated load-balancer reporters
  -> central PostgreSQL demand feed
       -> autoscaler demand snapshot
       -> dashboard live/history snapshot

bounded controller readiness probes
  -> central PostgreSQL route projection
       -> provider-free load-balancer route reads

fresh demand + current route/capacity + current zero-cost allocation
  -> one placement plan
       -> commit zero-cost rows first
       -> admit paid rows only for the residual compatible deficit
       -> generic durable non-pool action executor
```

Provider reads, action recovery, and route publication run independently per
association or bounded batch. One ambiguous launch may quarantine its exact
replica and capacity claim, but it cannot block demand ingestion, healthy
routes, autoscaling, sibling pools, or the service dashboard.

## Goals

- Make request counts, in-flight work, queue depth, rejection pressure, and
  freshness visible without waiting for the service controller.
- Use one authenticated durable demand feed for both autoscaling and UI.
- Account compatible ready and committed zero-cost capacity before any paid
  Spot or On-Demand launch is authorized.
- Require fresh authenticated unmet demand at both ordinary demand-driven paid
  admission and the provider-I/O fence.
- Publish complete routes from already-collected controller observations so
  load-balancer and API reads perform no provider queries.
- Keep a poisoned launch local to its exact association and capacity claim.
- Preserve scale-to-zero: the Boltz service has `min_replicas: 0`, no positive
  accelerator floor, reserved-fill floor zero, and utilization gating enabled.
- End with one placement planner, one non-pool launch binding, one demand feed,
  and one route projection.

## Non-goals

- This does not create another deployment controller, artifact registry,
  provider abstraction, request queue, or autoscaler.
- This does not make telemetry a launch receipt or provider evidence.
- This does not infer executor quiescence, provider absence, or cleanup safety
  from a stale report, cancelled request, elapsed time, missing PID, or missing
  cluster-database row.
- This does not let reserved-fill fall through to paid capacity. Reserved fill
  remains zero-cost-only.
- This does not promise that all ready accelerators are interchangeable. Every
  demand and capacity value is reduced through the service's explicit
  accelerator compatibility and throughput model.
- This does not retain the controller-coupled telemetry path after migration.

## Existing foundation

The implementation must reuse these checked-in mechanisms:

- `LoadBalancer` request aggregation, compatibility profiles, in-flight and
  queued gauges, rejection classification, prediction-time histograms, and
  stable reporter-session identity;
- the stable authenticated API proxy under `/api/internal/serve/{service}` and
  its exact service-hash fence;
- `serve_request_activity_history`, daily rollups, response/prediction history,
  autoscaler history, and their current retention logic;
- dashboard request history, demand-pressure, recent-rate, in-flight, queued,
  rejected, and freshness rendering;
- the protocol-v2 reserved-capacity observation/allocation ledger and
  database-assigned zero-cost admission sequence;
- paid-capacity pool claims, placement projections, controller ownership, and
  the already-merged ordinary durable launch binding; and
- the generic non-pool binding and per-association reconciliation specified by
  `durable-serve-replica-actions.md`.

The P2a draft PR #1499 now contains, but has not deployed or promoted:

- a PostgreSQL-clock-fenced latest-report table and stable authenticated
  ingestion endpoint;
- a non-destructive five-second-bucket load-balancer demand window plus
  existing cumulative minute history; and
- a provider/controller-free direct read endpoint polled independently by the
  dashboard, plus a status overlay for CLI/legacy consumers. Both prefer fresh
  durable telemetry and report stale/unavailable explicitly.

P2b1 now adds the complete provider-free route projection, and the P2b2
working branch now adds the promoted autoscaler reader, zero-cost-first
replanning boundary, immutable capacity plan/head, and planner-bound paid
claim revalidated immediately before provider I/O. These changes remain dark
and unpromoted. The final dashboard placement explanation and P3 removal of
the legacy demand/route paths are not yet implemented.

The 2026-08-16 production read-only audit found Serve revision 046 on release
v1.1.1291, no remaining replica rows 52032--52038, and no generalized
non-pool association/action rows for `boltz-l4-fleet`. Absence of those replica
rows is not by itself quiescence evidence, so the historical-scope evidence
gate remains open. The same audit reproduced the live economic defect: one
fresh interval had target 65, 28 ready zero-cost slots, 201 observed free
reserved slots, and 11 paid L4 cold-launch authorizations. Broker grants were
fresh and large enough (48 A100, 40 A100-80GB, and 144 H200), proving that the
blocker was controller ordering/accounting rather than reserved scarcity.

## Public contract

### Demand report

Each load-balancer process sends a cumulative, idempotent report to the stable
central API endpoint. The display-compatible version 1 contract contains:

- service name and exact service-incarnation hash;
- load-balancer slot, Pod UID as the durable LB session ID, and a
  process-lifetime request-history/reporter session ID;
- report protocol version and monotonically increasing reporter sequence;
- reporter observation time for diagnostics; PostgreSQL computes `received_at`
  and `valid_until` from its own clock;
- a non-destructive cumulative five-second arrival window for live scaling,
  plus cumulative per-minute arrivals, completions by outcome, rejections, and
  prediction-time histograms for operational history;
- current in-flight, queued, and rejected-window gauges;
- the closed accelerator-compatibility demand map and the routing-spec version
  under which it was measured; and
- saturation/partial-observation flags already emitted by the load balancer.

Version 2 is the only capacity-authoritative contract. It additionally binds
the exact applied route generation/digest/source epoch and reports the complete
set of URLs for which occupancy was sampled plus total slots by URL. Version 1
remains readable for the P2a dashboard transition but is always incomplete for
autoscaling and paid admission. Its sampled-URL, occupancy, freshness, and
total-slot key sets must be identical, and every routed URL must be either in
that set or explicitly occupancy-unknown.

The outer internal-auth middleware authenticates the existing purpose-scoped
LB sync credential. The endpoint locks the service row and requires the exact
service hash before accepting a report; this preserves the current sync trust
domain without misrepresenting the shared token ring as a per-service
principal. PostgreSQL assigns `received_at` and `valid_until`; a caller
timestamp cannot extend freshness. Duplicate reporter sequence or
cumulative minute data is idempotent. A lower sequence, changed immutable
reporter identity, malformed compatibility map, future protocol, or wrong
service hash fails closed.

Reporter wall-clock skew is bounded to 30 seconds. Live bucket ages are
rebased onto PostgreSQL receipt time before aggregation, so even tolerated
clock skew cannot extend the rolling demand window. Historical event buckets
retain wall-clock timestamps for charts but never authorize capacity in P2a.
Compatibility priorities are bounded to the public 0--100 range. A report with
complete compatibility totals but contradictory per-priority profiles and
gauges is rejected rather than becoming future placement authority.

Minute history remains additive across distinct reporter sessions and
greatest-value idempotent within one reporter minute. Live gauges remain one
row per reporter incarnation. The aggregate includes every non-stale reporter:
during an HA handoff the old draining reporter's in-flight work and the new
active reporter's queue are both real demand. A reporter row expires; it is
never converted to a zero observation. Capacity authority additionally
requires a fresh `ACTIVE` report for the service row's exact selected LB slot
and cutover generation; every fresh `ACTIVE`/`DRAINING` stream owner must name
that current generation. A still-fresh pre-cutover report remains display
history only and cannot promote demand, publish a plan, or preserve a paid
claim.

### Freshness states

Every consumer exposes one of three states:

- `fresh`: at least one valid reporter whose applied HA role is `ACTIVE`; a
  fresh standby cannot conceal an expired or partitioned traffic owner. A
  separate
  `compatibility_complete` bit determines whether exact-card demand may become
  scaling or launch authority. A current report with occupancy-unknown routes
  keeps arrival/queue telemetry fresh but exposes the processing count as
  unavailable rather than zero;
- `stale`: the last report exists but its database-clock validity expired; or
- `unavailable`: no valid report can be read or the protocol is unsupported.

`0` is a value only in `fresh`. Stale and unavailable are never displayed or
used as zero. They authorize no new paid launch. Existing capacity is not
destroyed solely because telemetry is unavailable; ordinary downscale
hysteresis and drain proof still apply.

### Capacity accounting

For each demand compatibility class, one immutable planner snapshot contains:

- demand-feed generation and fresh reporter receipt watermark;
- route-projection generation/digest/source epoch;
- the complete supply-aware exact-card capacity target selected after
  compatibility allocation, including zero entries for every configured card;
- nonterminal compatible zero-cost and paid committed capacity, derived from
  locked replica rows rather than supplied by the controller; and
- service incarnation, lifecycle, version, and demand-source epoch.

The raw demand-feed generation is a telemetry receipt generation: it advances
when any non-duplicate reporter heartbeat lands, including a heartbeat whose
effective demand is unchanged. P2b therefore materializes a separate,
content-addressed planner snapshot generation. Its digest changes only when
the normalized authoritative demand inputs or fences change. Paid actions bind
to that stable planner generation and its source demand receipt watermark;
they do not bind directly to the continuously advancing heartbeat generation.
This preserves pre-I/O freshness checks without livelocking launches behind a
five-second reporting cadence.

Every promoted reconcile follows one ordered path:

```text
fresh protocol-v2 demand + exact fresh route
  -> autoscaler target
  -> supply-aware exact-card capacity target
  -> attempt ordinary/reserved zero-cost-only admission
  -> if any zero-cost row commits: stop and replan
  -> PostgreSQL locks service, reports, route, and all replica rows
  -> paid_residual = max(0, target - committed_zero_cost - committed_paid)
  -> publish/refresh plan head
  -> admit paid claim only within that residual
```

The zero-cost phase uses the existing manager and reserved broker with paid
selection hard-disabled. `Accepted` means a durable replica row was committed,
not that an in-memory choice was attempted. Deferred or rejected fill is not
counted. Any accepted row ends the reconcile so paid residual is never inferred
from its pre-commit snapshot.

The public `demand_target_by_accelerator` remains an explanation of where
flexible work would cold-start at current paid preference; it is not a durable
capacity accounting class. For example, 65 headerless compatible requests may
be displayed as L4 demand while already-materialized zero-cost A100 and H200
slots satisfy that work. Paid residual is therefore computed against the
autoscaler's supply-aware `capacity_target_by_accelerator`, never by
subtracting exact-card inventory from the cheapest-compatible demand map.
Using the display map would either over-authorize L4 Spot or fail every mixed-
card plan closed. A missing, partial, or sum-inconsistent supply-aware target
suppresses paid admission.

Plan publication and claim admission share the service-row mutex and lock the
fresh demand receipts, route head/snapshot, complete current-version replica
inventory, plan head, and relevant claims. The repository derives exact-card
or aggregate committed capacity from those rows. Promotion and planning
require normalized ReplicaInfo v18 rows with explicit zero-cost and logical
width attribution. An exact-card plan fails closed if any committed
current-version row cannot be classified into its accounting set; ambiguity
cannot be converted into a paid deficit. A semantic change treats all
current rows as the new baseline and mints a new plan generation. For an
unchanged heartbeat, it subtracts current-plan claim units from the full paid
inventory to reconstruct the immutable baseline, refreshes the same semantic
generation's head with the newest receipt generation/watermark, and preserves
bounded queued work. Every fresh promoted reconcile publishes, including a
zero target; a demand drop therefore mints a zero-residual generation that
revokes the prior head.

A paid claim persists plan generation/digest, source demand receipt generation,
demand-source epoch, canonical accelerator accounting class, and positive
capacity units. Its existing generic association/request authorization copies
that immutable claim tuple. Admission and the generic executor revalidate the
current plan head, receipt watermark, recomputed locked inventory, and the
content-addressed route payload immediately before provider I/O. Expired,
satisfied, corrupt, or owner-mismatched authority terminates the request at
`NOT_STARTED`; it never reaches a cloud API. Once provider I/O may have
started, the durable action reconciliation contract owns the result and no
telemetry change can pretend the effect did not happen.

Spot is a paid market for this contract. It may be preferred over On-Demand by
the existing placement policy, but neither market is authorized without a
positive residual. Reserved-fill admission can never select either market.
Cost rebalance, unknown-capacity replacement, and system recovery remain
separate generic profiles with their existing exact predecessor/recovery
authority; they cannot consume an ordinary demand-plan residual or silently
become ordinary scale-up.

### Route projection

The controller publishes a generation only after bounded readiness probes and
the complete service/replica snapshot finish. Immutable snapshot metadata
contains the exact service hash/lifecycle, applied service version,
controller-owner tuple, protocol, generation, and digest. Its full response
contains the routing spec, normalized routable endpoints, accelerator
material, and capacity hint; its private identity payload binds current and
bounded alias URLs to exact replica records. The freshness head owns
`refreshed_at` and `valid_until`.

Serve049 stores immutable, full route snapshots plus one freshness-bearing
head per service. A semantic change inserts one snapshot and advances the head;
an identical bounded probe round refreshes only the head. The snapshot owns the
existing LB response document and a private normalized URL -> exact
replica/record identity map. It does not add a second full/delta application
protocol. Old snapshots are retained for longer than the demand-report TTL and
are pruned by a fixed upper bound.

Every projected response adds the snapshot generation, digest, and route-source
epoch. The LB records those fields only after it atomically applies that same
routing spec and ready set, then echoes them in its durable demand report.
Future demand authority can therefore translate URL-keyed occupancy through
the exact immutable snapshot the reporter observed; a current URL is never
guessed to represent an older report.

Publication locks the service row; reads use one PostgreSQL transaction and
exact-match the service hash, lifecycle epoch, controller
incarnation/owner epoch, PID/IP, applied version, and non-pool discriminator.
The service row has one explicit
`LEGACY_PROXY` or `DURABLE_PROJECTED` route mode and monotonic mode epoch.
Promotion requires a fresh complete head published by the current capable
controller incarnation. After promotion, a missing, corrupt, stale, or
owner-mismatched projection fails closed; it never falls back to the controller
proxy. The legacy proxy remains the only response owner before promotion.

An ambiguous replica is omitted or marked unroutable without suppressing
healthy siblings. Provider-phase contention with no observation makes the
whole publication round incomplete and leaves the prior head untouched; an
identity failure for one exact row is positive ambiguity evidence and only
withholds that row. The stable API and load balancer read the projection only;
they perform no provider, Kubernetes, Ray, or cluster-database query. A stale
read returns unavailable, so a warm LB retains its already-applied routes under
the existing sync-outage behavior while a cold LB cannot become ready from
stale evidence. A stale generation is visible as stale telemetry and cannot
contribute ready capacity to a new paid-admission decision.

### Dashboard

The service details page always renders a `Requests now` state:

- fresh in-flight and queued counts;
- accepted arrivals and rejected pressure over the current window;
- report age and reporter count; or
- an explicit stale/unavailable explanation.

The page polls the hash-fenced stable API demand endpoint independently of the
controller-backed status projection. A controller timeout therefore cannot
delay fresh request counters. During the dark-write transition, an older API
server or non-consolidated installation falls back to the existing status
response; a new consolidated server never silently converts a failed direct
read to zero.

History continues to use the existing minute tables and charts. Empty history
renders `0 requests observed` only when the selected interval is completely
covered by fresh reports; otherwise it renders the coverage gap. Placement
shows demand target, ready compatible capacity, zero-cost committed/granted,
paid committed, residual deficit, and the exact observation generations. This
makes a paid Spot launch explainable from the UI.

## Architecture and invariants

### Ownership

- Load balancers own measurement and cumulative retry buffers.
- The central API owns authentication, validation, and durable ingestion.
- PostgreSQL owns freshness, idempotency, aggregation, and planner fencing.
- The service controller owns autoscaler policy, readiness probing, and route
  publication, but not telemetry durability.
- The reserved broker owns zero-cost allocation; the paid-capacity ledger owns
  paid claims; neither silently substitutes for the other.
- The generic non-pool action executor owns provider effects and exact
  quiescence/result reconciliation.

### Isolation

Telemetry ingestion takes only the service-incarnation and reporter keys. It
does not join provider phases, manager locks, launch associations, or route
publication. Route publication does not wait for legacy action reconciliation.
Per-association reconciliation holds no manager/global lock across I/O or
sleep. These rules are tested with a permanently blocked provider read and an
ambiguous legacy row while traffic reports, healthy route generations, and
dashboard reads continue.

### No global artifact control plane

Mixed-version safety uses the existing server-instance capability leases,
versioned endpoint/payload, PostgreSQL handler/profile constraints, and
per-service binding/cohort promotion described by the durable-action design.
This initiative deliberately rejects a second global artifact rollout gate,
runtime artifact participant registry, autonomous-effect catalog, config-seed
receipt system, or Helm rollout orchestrator. Those mechanisms broaden the
fault domain without adding evidence about an individual provider effect.
Immutable images and reviewed Helm rollout remain deployment requirements, not
runtime launch authority.

## Implementation plan and PR stack

### P0: compatibility deployment

This precondition is satisfied by the later v1.1.1291 production deployment.
The 2026-08-16 baseline uses the reviewed Helm release, the existing
single-`all` `Recreate` topology, Serve schema 046, API schema 010, and
`LEGACY_ACTIVE`. It has not activated the new generalized action, demand, or
placement authorities. Subsequent rollouts must continue to use
`--reuse-values`; they must not redeploy the older v1.1.1284 artifact merely to
reproduce the originally proposed sequence.

The same inspection found no surviving replica, request, coverage, shadow, or
association records for historical replica IDs 52032--52038. Their earlier
orphaned state remains incident evidence, but database absence is not executor
quiescence and must not be converted into a synthetic receipt. Migration input
is the exact retained inventory observed at migration time.

### P1: generalized durable non-pool actions

Implement API011/Serve047 from `durable-serve-replica-actions.md`: one generic
handler, six typed profiles, planner-intent commit followed by atomic
association/request/queue/pin binding,
pre-I/O revalidation, typed result/provider reconciliation, and per-association
quarantine. Add reusable append-only legacy reconciliation scopes and register
only exact retained legacy rows found by the rollout inventory. Reconcile any
such rows only from old-executor termination plus fresh exact provider
evidence. The historical IDs 52032--52038 must not be recreated, backfilled,
or registered as a scope when no source row remains. No synthetic quiescence
or association backfill is permitted.

Actual draft size: approximately 6,300 source/test lines across 39 files,
larger than estimated because it closes all six profiles, legacy evidence,
lock-yielding recovery, process capability fencing, and PostgreSQL transition
tests. It does not include a deployment control plane.

### P2a: durable demand telemetry and UI

API version 82/Serve048 add the demand-report/live-gauge tables, stable API
ingestion, non-destructive reporter window, request-history acknowledgement,
direct current-demand read, status projection, and dashboard freshness
contract. During transition the LB sends both old controller sync data and the
new durable report. The durable feed is authoritative for display only when
fresh; it has no scaling or launch authority in P2a.

Current reviewed size: 2,814 additions and 85 deletions across 38 files,
mostly reusing existing aggregators, history tables, proxy
authentication, and components. The additional direct-read hook, strict
bounded report validation, and PostgreSQL migration matrix account for the
increase over the 1,000--1,800 estimate.

Local review evidence on 2026-08-16 includes all 9 real-PostgreSQL Serve048
tests, 13 validation tests, 121 focused dashboard tests, repository-wide mypy
over 940 source files, changed-file pylint at 10/10, dashboard ESLint and
Prettier, and the exact HA cases where replica-global async occupancy must not
be double-counted and a fresh standby must not turn an expired active report
into a false zero. The first exact-head CI run also established four permanent
integration gates: current-head forward-only tests derive the Serve revision
instead of pinning the predecessor; all five load-balancer background loops,
including demand publication, are owned and cancelled; the dashboard demand
GET is viewer-readable while the internal reporter POST is explicitly denied;
and controller-capacity tests stub durable demand so one authority cannot
silently contaminate the other's fixture. The four exact regressions and the
real-PostgreSQL case pass locally; the corrected exact-head CI and live rollout
evidence remain open.

### P2b1: provider-free route projection

API version 83/Serve049 add bounded immutable full route generations, one
freshness-bearing head, and an explicit per-service route mode/epoch. The
replica readiness owner publishes the complete route/routing-spec response
from the bounded probe round's already-resolved endpoints and exact replica
records. The stable API can answer the existing LB sync wire shape from
PostgreSQL without a provider, Kubernetes, Ray, cluster-record, or
service-controller read.

The projection is dark until one exact API/LB/controller cohort is capable and
a fresh complete generation exists. Promotion changes only the response owner;
it does not create a second LB application format. Legacy controller proxying
is the temporary transition path and is removed by P3. A stale projection may
retain an already-applied route only under the existing bounded LB outage
contract; it is never ready capacity for planning.

Current implementation size: 2,146 additions and 59 deletions across 29 files,
including about 600 lines of pure and real-PostgreSQL tests.
The increase over the 900--1,500 estimate comes from closed persisted-payload
validation, immutable URL/record alias evidence, and integration tests at the
probe, proxy, LB-apply, and demand-report boundaries. The earlier experimental
route branch is not a source of truth: it added roughly 5,500 lines, was not
connected to runtime publication or reads, and duplicated full and delta
application machinery that the existing LB wire contract does not need.

Local P2b1 review evidence on 2026-08-16 includes the complete existing Serve
replica-manager module, new route/proxy/LB/demand unit tests, all new Serve049
real-PostgreSQL tests, the earlier Serve schema migration matrix, repository
mypy, changed-source pylint, YAPF/isort, and whitespace checks. Adversarial
review found and fixed a poisoned-row isolation bug: a malformed retained
record identity is now withheld from both public routing and private identity
translation without suppressing healthy siblings. CI, PR review, and live dark
publication evidence remain open.

### P2b2: one demand authority and ordered capacity admission

API version 84/API012/Serve050 add API-fleet capability identity, explicit
per-service demand promotion, the autoscaler durable reader,
content-addressed planner-generation fields, the source demand receipt
watermark, and the paid-authority tuple.
Serve050 owns the demand promotion mode and epoch; Serve048 deliberately does
not add authority fields. Promotion disables controller-sync demand mutation
for that service epoch. Zero-cost admission is committed before paid residual
planning, and both are revalidated before provider I/O. A heartbeat with
unchanged normalized demand does not mint a new planner generation or
invalidate already-admitted work.

The exact Serve050 shape is one `LEGACY_CONTROLLER` / `DURABLE_FEED` source
mode and monotonic epoch on `services`; immutable `serve_capacity_plans` rows;
and one freshness-bearing `serve_capacity_plan_heads` row per service. A plan
binds the service incarnation/lifecycle/version, demand-source epoch, complete
fresh reporter receipt watermark, exact route generation/digest/epoch,
normalized demand, demand target, PostgreSQL-derived zero-cost and paid
baseline capacity, the distinct supply-aware exact-card capacity target, and
paid residual by accelerator. The semantic payload is
content-addressed. The head carries the latest demand generation and
receipt-watermark digest; an identical reconcile reconstructs its baseline by
subtracting same-plan claims from locked paid inventory and refreshes those
receipt fields and the expiry without minting a new semantic generation.

The existing `paid_capacity_claims` row gains one all-or-none tuple containing
plan generation, plan digest, demand-feed generation, demand-source epoch, and
the canonical accelerator compatibility class debited by that claim.
The tuple also stores the positive planner units consumed, so a multi-GPU
logical backend cannot spend a one-backend claim as if it represented one
unit.
Planner-bound claims also have a database foreign key to the immutable
`(service_name, plan_generation)` row, with service/plan deletion cascading;
legacy claims keep the nullable transition shape.
The existing generic non-pool profile includes that tuple in its paid-claim
authorization. Admission and the shared pre-provider-I/O guard lock the claim,
plan/head, service, route head, and demand generation and exact-match the
tuple. They require an unexpired plan head and demand/route receipts, but do
not require that their wall-clock freshness timestamps equal those observed at
admission. This keeps freshness fail-closed while allowing an unchanged
heartbeat to extend authority without changing content identity. A newer
semantic plan invalidates old claims at the pre-I/O fence; their durable rows
remain action/reconciliation evidence and their committed replicas become
baseline capacity in the new plan.
Capacity plans are operational fences rather than history. Plan publication
removes superseded generations that are neither the current head nor
referenced by a planner-bound claim; the claim foreign key retains every
generation that can still authorize or explain unsettled work without allowing
the table to grow with every semantic reconcile.

API012 advertises one exact ordered-admission protocol capability on every
live `all|api|executor|controller` participant. Per-service promotion locks the
service, proves that fleet capability, a fresh complete durable demand report,
a fresh matching projected route, current controller ownership, and no legacy
demand mutation in flight, then advances the source epoch. After promotion the
controller-sync endpoint may still accept routing/drain reports during the
transition, but it cannot call `collect_request_information`; only the durable
reader may advance autoscaler demand state.

Reviewed P2b2 size: 40 files, 3,973 additions and 194 deletions.
This is large and above the original 1,200--2,000-line estimate because it
includes sequential API/Serve migrations, real-PostgreSQL inventory/claim
races, controller ordering tests, strict route/content/LB-generation
validation, bounded plan retention, and mixed-fleet capability tests.
The 2026-08-16 post-review correction adds a cross-card PostgreSQL regression:
L4-attributed compatible demand with an A100 actuation target debits the A100
zero-cost row and authorizes only the remaining A100 residual. It also makes a
sequenced reserved-fill commit return explicit progress to the ordered
controller: any accepted row ends the promoted reconcile and forces a fresh
plan, just like an ordinary zero-cost commit.

Local P2b2 correction evidence on 2026-08-16 includes the complete Serve
controller, concurrency/QPS autoscaler, compatibility-contract,
decision-contract, and pure admission suites, plus all ten admission tests on
a real local PostgreSQL 14 server, the complete PostgreSQL API-request suite,
and 101 focused dashboard tests. Formatting, mypy over 947 source files,
pylint, and dashboard lint all pass. Remote CI, injected provider failure, and
production promotion evidence remain open.

### P3: blocked steady-state cleanup

Author API013/Serve051 with P1/P2 and keep it stacked and blocked. After the
documented rollout gates it removes controller-coupled telemetry ingestion,
unbound non-pool admission/recovery, the ordinary-only handler alias, global
startup recovery waiting, cluster-name/process-map authority, legacy incident
writers, dual-feed selection, and transition-only metrics/tests. Historical
audit tombstones and minute history remain.

Expected size: net-negative. The final topology must have fewer callable paths
than the current system.

## Deployment and rollback

All source branches target `boltz-bio/skypilot:improvements`. Production
deployment authority is the reviewed live Helm release, not a platform pin.
The release tuple is `skypilot` in namespace `skypilot`; `improvements` is the
source branch and must never be substituted as the release name.
Every upgrade captures live values/manifest, reviews the rendered diff, uses
`--reuse-values`, pins the immutable image for every explicitly overridden
role, and records the live Helm revision, image digest, schema heads, rollout,
and post-deploy observations.

P1 and P2 are additive and dark before per-service promotion. Promotion
requires one exact capable cohort, no unsettled unbound work for that service,
fresh demand/route publications, and a successful injected-failure rehearsal.
Rollback before P3 means disable new promotion, drain/project bound work, and
return the service to the previous cohort through its fenced transition. Schema
downgrade is forbidden. After P3, rollback of the removed paths is forbidden;
fix forward with a new immutable image and cohort.

The Boltz service configuration is updated to `min_replicas: 0`, with no
positive per-accelerator floor. Reserved fill stays floor zero with utilization
gating enabled. The configuration change occurs only after telemetry freshness
and scale-from-zero are verified, so stale telemetry cannot strand demand.

## Verification

Automated tests must cover:

- report authentication, exact service hash, protocol/version rejection,
  cumulative idempotency, sequence rollback, reporter restart, HA overlap,
  database-clock expiry, and stale-is-not-zero behavior;
- controller/provider deadlock while telemetry writes and dashboard reads
  continue within their latency budget;
- compatible demand accounting across L4, A100, A100-80GB, and H200 throughput
  weights;
- zero-cost acceptance before paid planning, commit races, deferred fill,
  stale broker observation, demand expiry before provider I/O, and strict
  reserved-fill no-paid-spill;
- no paid Spot or On-Demand request at zero/stale/unavailable demand;
- provider-free route reads, atomic generation replacement, one poisoned row,
  stale projection, controller restart, and service update;
- dashboard fresh-zero, live processing, queued, rejected, stale, unavailable,
  history-gap, and placement-explanation states; and
- transition and P3 source-absence tests proving one feed, planner, route path,
  and generic non-pool handler remain.

Manual production verification records:

1. the live v1.1.1291 compatibility baseline, with unchanged single-`all`
   topology, Serve046/API010 schema heads, and `LEGACY_ACTIVE`;
2. a fresh pre-migration inventory of retained legacy rows and unsettled
   requests; reconcile only rows that actually remain. Record the historical
   absence of IDs 52032--52038 without treating it as quiescence, recreating
   rows, or backfilling associations;
3. provider-phase latency and proof that one quarantined row does not block
   routes, demand, or sibling reconciliation;
4. a fresh zero-traffic interval with target zero and no new paid request;
5. traffic served first by compatible ready/reserved capacity, followed by a
   paid Spot launch only after a recorded positive residual;
6. scale from zero, scale back to zero, controller restart, and service update;
7. dashboard in-flight/queued/completed counts and freshness matching the
   durable feed; and
8. readiness, +10, +30, and one complete stale/quiescence horizon before P3.

## Adversarial review decision

The reviewed experimental branch added roughly 7,800 lines across 68 files and
four API migrations before implementing demand ordering or UI telemetry. Its
commit-before-effect and exact legacy-evidence ideas are retained. Its global
artifact gate, participant/effect-epoch registry, autonomous-effect catalog,
config-seed authority, runtime verifier, and Helm/Terraform rollout controller
are rejected for this initiative. They duplicate deployment and request/action
authority, enlarge the blast radius of a single ambiguous Serve row, and do not
prove provider quiescence.

The accepted plan extends existing request, capability-lease, history,
reserved-broker, paid-claim, projection, and ordinary-binding abstractions. It
adds two durable operational concepts only: a live demand report and a route
generation. The final cleanup removes their predecessor paths.

P2b1 adversarial review also rejected a mutable current-route row and a new
full/delta LB protocol. The accepted implementation keeps immutable full
snapshots with one freshness head, uses the existing full sync response, binds
each response to an exact private URL/record map, and retains only bounded
fixed-lifetime aliases for already-live records. It fails closed when route
source ownership cannot be proven and isolates exact malformed or colliding
rows instead of converting ambiguity into a fleet-wide publication barrier.

## Open gates

- [x] Verify the equivalent later v1.1.1291 production compatibility baseline
  without generalized-action, demand-authority, or placement promotion
  (2026-08-16: single-`all` `Recreate`, Serve046/API010, `LEGACY_ACTIVE`).
- [x] Merge P1 as PR #1498 and publish P2a as PR #1499, P2b as PRs #1503/#1504,
  and the blocked P3 removals as draft PRs #1506/#1510.
- [ ] Pass the complete P1 crash/mixed-version/provider-evidence matrix.
- [ ] Immediately before migration, inventory exact retained legacy rows and
  unsettled requests, then reconcile only present rows without fabricated
  quiescence or manual deletion. The historical seven-row scope is absent and
  must not be reconstructed.
- [ ] Pass demand conservation, no-paid-spill, provider-free route, controller
  stall isolation, and dashboard tests.
- [ ] Promote the service on one immutable capable cohort and set
  `min_replicas: 0` after scale-from-zero preflight.
- [ ] Complete production readiness/+10/+30/stale-horizon monitoring and a
  rollback rehearsal.
- [ ] Merge P3 only after zero legacy-capable participants, zero unsettled
  unbound non-pool rows, and zero old telemetry/route-path usage.
