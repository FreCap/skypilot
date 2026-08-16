# SkyServe demand, capacity, and telemetry convergence

Status: P1 is implemented in draft PR #1498; the rebased and locally reviewed
P2a implementation is in draft PR #1499; production remains on the legacy
controller-coupled demand path

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
- Require fresh authenticated unmet demand at both paid admission and the
  provider-I/O fence.
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

The following is not yet present:

- an autoscaler reader that consumes that table as its sole demand source;
- one atomic planner snapshot that orders zero-cost admission before paid
  admission for the same demand generation;
- a paid-launch authority tuple that includes the demand and zero-cost
  allocation generations and is revalidated immediately before provider I/O;
- a complete provider-free route projection used by the load balancer; and
- the final dashboard placement explanation that binds paid decisions to their
  demand and zero-cost generations.

## Public contract

### Demand report

Each load-balancer process sends a cumulative, idempotent report to the stable
central API endpoint. Version 1 contains:

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
never converted to a zero observation.

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

- demand-feed generation and freshness deadline;
- route-projection generation and ready compatible capacity;
- nonterminal compatible zero-cost and paid committed capacity;
- reserved broker allocation/observation generation and spendable zero-cost
  slots; and
- service lifecycle, version, controller-owner, placement-policy, and binding
  epochs.

The raw demand-feed generation is a telemetry receipt generation: it advances
when any non-duplicate reporter heartbeat lands, including a heartbeat whose
effective demand is unchanged. P2b therefore materializes a separate,
content-addressed planner snapshot generation. Its digest changes only when
the normalized authoritative demand inputs or fences change. Paid actions bind
to that stable planner generation and its source demand receipt watermark;
they do not bind directly to the continuously advancing heartbeat generation.
This preserves pre-I/O freshness checks without livelocking launches behind a
five-second reporting cadence.

The planner computes:

```text
residual_before_zero_cost =
  max(0, demand_target - ready_compatible - committed_compatible)

zero_cost_to_admit =
  min(residual_before_zero_cost, authenticated_spendable_zero_cost)

paid_to_admit =
  max(0, residual_before_zero_cost - accepted_zero_cost)
```

`accepted_zero_cost` means rows committed by the database admission ledger in
this round, not in-memory intent. Deferred or rejected fill is not counted.
The final paid transaction locks the same service/capacity ordering, rereads
all compatible nonterminal rows, exact-matches the demand and allocation
generations, recomputes the residual, and spends a paid claim. Concurrent
controllers therefore cannot both consume the same deficit.

A paid association persists demand generation, demand expiry, compatibility
class, placement-decision digest, zero-cost allocation generation, and paid
claim identity. The generic executor revalidates all of them at its
commit-before-provider-I/O fence. Expired or satisfied demand terminates the
request at `NOT_STARTED`; it never reaches a cloud API. Once provider I/O may
have started, the durable action reconciliation contract owns the result and
no telemetry change can pretend the effect did not happen.

Spot is a paid market for this contract. It may be preferred over On-Demand by
the existing placement policy, but neither market is authorized without a
positive residual. Reserved-fill admission can never select either market.

### Route projection

The controller publishes a generation only after bounded readiness probes and
the complete service/replica snapshot finish. The projection contains exact
service hash, version, controller-owner epoch, route generation, routing-spec
version, normalized endpoint, replica/record identity, accelerator/capacity,
readiness, drain eligibility, and `valid_until`.

Publication replaces one service generation transactionally. An ambiguous
replica is omitted or marked unroutable without suppressing healthy siblings.
The stable API and load balancer read the projection only; they perform no
provider, Kubernetes, Ray, or cluster-database query. A stale projection keeps
the last known route only under the existing bounded outage contract and is
shown as stale. It cannot contribute ready capacity to a new paid-admission
decision.

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

Deploy v1.1.1284 directly through the reviewed Helm workflow with
`--reuse-values`, the existing single-`all` `Recreate` topology, and
`LEGACY_ACTIVE`. Do not normalize or activate reserved fill. This restores
bounded reads of the exact retained ReplicaInfo shapes and enables subsequent
source work to inspect the fleet safely.

### P1: generalized durable non-pool actions

Implement API011/Serve047 from `durable-serve-replica-actions.md`: one generic
handler, six typed profiles, planner-intent commit followed by atomic
association/request/queue/pin binding,
pre-I/O revalidation, typed result/provider reconciliation, and per-association
quarantine. Add reusable append-only legacy reconciliation scopes and register
the seven production rows as one exact reviewed scope. Reconcile those rows
only from old-executor termination plus fresh exact provider
evidence. No synthetic quiescence or association backfill is permitted.

Actual draft size: approximately 6,300 source/test lines across 39 files,
larger than estimated because it closes all six profiles, legacy evidence,
lock-yielding recovery, process capability fencing, and PostgreSQL transition
tests. It does not include a deployment control plane.

### P2a: durable demand telemetry and UI

API version 80/Serve048 add the demand-report/live-gauge tables, stable API
ingestion, non-destructive reporter window, request-history acknowledgement,
direct current-demand read, status projection, and dashboard freshness
contract. During transition the LB sends both old controller sync data and the
new durable report. The durable feed is authoritative for display only when
fresh; it has no scaling or launch authority in P2a.

Current reviewed size: approximately 2,650 source/test/UI lines across 33
files, mostly reusing existing aggregators, history tables, proxy
authentication, and components. The additional direct-read hook, strict
bounded report validation, and PostgreSQL migration matrix account for the
increase over the 1,000--1,800 estimate.

Local review evidence on 2026-08-16 includes all 9 real-PostgreSQL Serve048
tests, 13 validation tests, 121 focused dashboard tests, repository-wide mypy
over 940 source files, changed-file pylint at 10/10, dashboard ESLint and
Prettier, and the exact HA cases where replica-global async occupancy must not
be double-counted and a fresh standby must not turn an expired active report
into a false zero. CI and live rollout evidence remain open.

### P2b: one demand authority, routes, and ordered capacity admission

API012/Serve049 add API-fleet capability identity, explicit per-service demand
promotion, the autoscaler durable reader, route projection,
content-addressed planner-generation fields, the source demand receipt
watermark, and the paid-authority tuple. Serve049 also owns the demand
promotion mode and epoch; Serve048 deliberately does not add authority fields.
Promotion disables controller-sync demand mutation for that service epoch.
Zero-cost admission is committed before paid residual planning, and both are
revalidated before provider I/O. A heartbeat with unchanged normalized demand
does not mint a new planner generation or invalidate already-admitted work.

Expected size: medium/large, approximately 1,500--2,500 source/test/UI lines.

### P3: blocked steady-state cleanup

Author API013/Serve050 with P1/P2 and keep it stacked and blocked. After the
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

1. live v1.1.1284 compatibility rollout with unchanged capacity and
   `LEGACY_ACTIVE`;
2. settlement of replica IDs 52032--52038 from exact evidence and independent
   retirement of the two shutting-down Spot rows;
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

## Open gates

- [ ] Deploy and verify v1.1.1284 in production without normalization or
  reserved-fill activation.
- [x] Publish the P1 draft as PR #1498.
- [ ] Publish the blocked P3 removal after P2b supplies the final replacement
  path and keep it stacked until the removal gates pass.
- [ ] Pass the complete P1 crash/mixed-version/provider-evidence matrix.
- [ ] Reconcile the exact seven-row production scope without fabricated
  quiescence or manual row deletion.
- [x] Publish the P2a durable-demand/UI draft as PR #1499.
- [ ] Publish P2b and update P3 for every transition-only demand/route path.
- [ ] Pass demand conservation, no-paid-spill, provider-free route, controller
  stall isolation, and dashboard tests.
- [ ] Promote the service on one immutable capable cohort and set
  `min_replicas: 0` after scale-from-zero preflight.
- [ ] Complete production readiness/+10/+30/stale-horizon monitoring and a
  rollback rehearsal.
- [ ] Merge P3 only after zero legacy-capable participants, zero unsettled
  unbound non-pool rows, and zero old telemetry/route-path usage.
