# SkyServe demand, capacity, and telemetry convergence

Status: P1, P2a, P2b1, and P2b2 are merged in PRs #1498, #1499, #1503, and
#1504. The complete additive stack is deployed in production; its per-service
demand, route, and ordered-capacity authorities remain dark. PR #1521's
partial-coverage in-flight observability is merged and deployed as direct Helm
revision 406 / release `1.1.1305` at image digest
`sha256:b493c8a03d32f62307af9c4093ad94cbe20cf80fde4915f907548d8149954173`
and chart digest
`sha256:170056bb3654f35ba52d6a42421d4feacf31233a9e028407ccb796a2fdfe7e62`.
The API reports version 86 and all 14 warm-standby LB Deployments converged on
the image. Live incomplete occupancy now exposes the confirmed in-flight lower
bound and unknown-backend count rather than suppressing processing activity.
This corrects observability only; it does not promote authority.
P2b2 includes the adversarial-review correction that
separates cheapest-compatible demand attribution from supply-aware exact-card
capacity accounting. A production observation at Serve048 exposed that the
closed revision-040 placement-normalization authority registry stopped at
Serve047; P2b1 now recognizes the reviewed additive Serve048 and Serve049 heads
and P2b2 recognizes its additive Serve050 head before either path can be
promoted. Production remains on the legacy controller-coupled demand and route
paths pending the documented test-service promotion gates.

The exact post-#1503 P2b2 review scope was 42 files with 4,298 insertions and
241 deletions relative to `improvements`. The blocked draft removals #1506 and
#1510 remain unmerged until the promotion and observation gates pass.

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
- Preserve useful request observability under partial occupancy coverage:
  publish a confirmed in-flight lower bound and the exact number of unknown
  replica URLs while keeping the exact in-flight total nullable.
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

Merged P2a PR #1499 is present in the current production binary but has not
been promoted:

- a PostgreSQL-clock-fenced latest-report table and stable authenticated
  ingestion endpoint;
- a non-destructive five-second-bucket load-balancer demand window plus
  existing cumulative minute history; and
- a provider/controller-free direct read endpoint polled independently by the
  dashboard, plus a status overlay for CLI/legacy consumers. Both prefer fresh
  durable telemetry and report stale/unavailable explicitly.

P2b1 adds the complete provider-free route projection, and P2b2 adds the
promoted autoscaler reader, zero-cost-first replanning boundary, immutable
capacity plan/head, and planner-bound paid claim revalidated immediately
before provider I/O. Both are deployed but remain dark and unpromoted. The
final dashboard placement explanation and P3 removal of the legacy
demand/route paths are not yet implemented.

The 2026-08-16 production read-only audit first found Helm revision 401 on
v1.1.1296, commit `036c7a2627b34050e00b335b41c8cd7e329cdc2a`, API 81,
API-request revision 011, and Serve revision 047. The API deployment and both
`boltz-l4-fleet` load balancers used immutable digest
`sha256:cb383b53e4723903d62c4115e961c3869b51b5a91e3e7bddec1460703ec54756`.
A concurrent Terragrunt/Terraform apply then created Helm revision 402 and
regressed the runtime to v1.1.1287, commit
`88e6ea7dbd28c85d048fc2608c6c48ab33e1e3e1`, digest
`sha256:04567b501cc4a35d93aca2ba95701fe9ad56bb39fdc89731997af6b2a84035b3`.
The PostgreSQL heads correctly remained forward at API-request 011 and Serve
047. EKS audit records attribute the mutation to Terraform's Helm provider
under the operator session for `simone-boltz.bio`; Argo CD is installed on the
hub but has no Application for the SkyPilot release. The audit initially
mistook a checked-in `boltz-platform` pin for production deployment authority.
That conclusion was wrong: SkyPilot production intentionally fixes forward
from merged `boltz-bio/skypilot:improvements` artifacts with a direct,
reviewed Helm upgrade. No `boltz-platform` pin update is required or desired.
Schema is rolled forward only; revision 401 is not replayed.

While the additive stack was under review, a second direct Helm mutation
created revision 403 with v1.1.1299, exact commit
`8326c5f0490e745d8bd0fea61eb4fe2b16fafbc8`, API 82, and image digest
`sha256:d0d53742eab3b613e2318def9fd1e55750f86b07da29c01f59df175df327e401`.
EKS audit attributes that Helm 3.16.4 update to operator identity
`francesco@boltz.bio`. It is an ordinary direct-Helm fix-forward deployment,
not platform-pin drift.

While P2b2 exact-head CI was running, another independent direct Helm update
created revision 404 with v1.1.1301, exact commit
`b8c017dd6451e4373040d185634ffed5f7c2bf55`, API 84, and image digest
`sha256:14f7c252bef9c5a1c3c9ec5a22df253a6e66102e9ca09091c93a04cb5a3adeb3`.
The rollout completed with no restart and advanced the forward PostgreSQL
heads to Serve049/API-request 011. Its active requests after readiness were
read-only status, inventory, and managed-jobs queue reads; it created no
launch/down provider mutation.

The reviewed P2b2 artifact was then deployed directly with Helm as revision
405: v1.1.1302, commit `895223b618ce0a3c013a90145395761eb7f29270`, API 85,
image digest
`sha256:3d88395de8ee87834f8a87af0ecdc98b1a08a64b287003a140231b9ce254b689`,
and chart digest
`sha256:d28233613d64207c0b9d873393536f510f36feae53f6109e94baf2e4d18ef4f3`.
The migration job completed, the API and both provider init containers ran the
exact image digest with zero restarts, PostgreSQL reached API-request 012 and
Serve050, and both health endpoints returned 200. All new authorities remain
dark for `boltz-l4-fleet`, which is still in legacy ordinary binding, legacy
route, and legacy controller-demand modes.

A subsequent zero-traffic observation found 53/53 physical replicas ready:
34 A100, 13 A100-80GB, and six paid Spot L4s. Durable request telemetry was
fresh and complete with zero arrivals, zero queue depth, and zero in-flight
work, while the legacy controller still reported a supply-insensitive L4
target of three (`raw_target_num_replicas=2`). The 47 compatible ready
zero-cost backends did not debit that L4 target. This is direct production
evidence for the ordered-capacity promotion: additive deployment alone cannot
correct overlaunch while `LEGACY_CONTROLLER` remains authoritative.

The service remains `resource_action_mode=legacy`,
`ordinary_launch_binding_mode=legacy`, and non-pool capability false. It has no
generalized non-pool association, associated request, legacy
scope/reconciliation, or paid claim. Historical replica IDs 52032--52038
remain absent. Absence of those replica rows is not by itself quiescence
evidence, so they must not be recreated or registered as a historical scope.

The exact pre-migration inventory found 64 nonterminal current-version rows:
46 `READY`, 15 `PENDING`, and three `PROVISIONING`. The 15 pending zero-cost
rows had no provider-cluster record at observation time but are recent planner
intents, so they are neither manually deleted nor used as absence proof. The
three provisioning rows require typed reconciliation:

- replica 52688 has a present A100-80GB provider cluster and a succeeded,
  exactly quiesced launch request;
- replica 52689 has a present A100 provider cluster and a cancelled request
  whose execution lease expired without an execution-quiescence receipt. The
  exact launch began at 13:51:14.086 UTC, its ReplicaSet deleted owner Pod UID
  `c74d8735-f5f9-4e9a-8bd1-19e69f8b68ea` at 13:51:15.759 UTC, and Kubernetes
  recorded the API container killed with exit 137 at the 60-second Pod grace
  deadline at 13:52:16 UTC. The lease expired only afterward. This proves a
  rollout retirement failure, including the chart's overlapping 20-second
  readiness sleep and full-grace application budget, caused the missing
  receipt. The exact failed-container audit record is executor-termination
  evidence, not a fabricated request receipt. This one row caused the legacy
  recovery pass to time out every 30 seconds; and
- replica 52690 has an `INIT` H200 provider record and an exactly quiesced
  failed request. Its PHX Kubernetes admission failed because the Pod omitted
  the server-owned `kueue.x-k8s.io/queue-name` label. This is a separate
  workspace/provider admission defect, not demand or reserved scarcity.

Revision 403 restored P1's sanctioned legacy-reconciliation ledger. The
operator sealed replica 52689's exact retained identity, recorded the reviewed
termination evidence, observed the physical-UID-fenced provider effect as
`PRESENT`, performed the exact fenced teardown, and only then recorded a new
provider observation as `ABSENT`. Event
`9f747a67-28f1-5883-a8d6-0b64903a5ef0` projected the row. The replica and
cluster records are absent; the original request remains truthfully
`CANCELLED` without a fabricated quiescence receipt, and no paid claim exists.

The recovery loop subsequently cleared all 15 prior intents from nonterminal
`PENDING` state and moved 52688 and 52690 into ordinary typed cleanup, leaving
46 ready plus those two provisioning rows in the 2026-08-16 post-recovery
snapshot. It then exposed a
separate release-lineage defect: the closed revision-040 authority registry
accepted only heads through Serve047, so the live Serve048 database is rejected
before placement-normalization acknowledgement and cleanup. P2b1 fixes the
registry through Serve049; P2b2 extends it through Serve050. The remaining two
rows must converge after that reviewed runtime deploy, and their outcomes must
be verified rather than assumed.

The same audit reproduced the live economic defect before revision 400: one
fresh interval had target 65, 28 ready zero-cost slots, 201 observed free
reserved slots, and 11 paid L4 cold-launch authorizations. Broker grants were
fresh and large enough (48 A100, 40 A100-80GB, and 144 H200), proving that the
blocker was controller ordering/accounting rather than reserved scarcity. A
later revision-400 sample with zero arrivals had 35 ready slots, 8 provisioning
rows, 7 Spot rows shutting down, target 5/raw target 1, no paid cold-launch
authority, 196 reserved-fill target slots, and 172 observed free reserved
slots. The lack of paid authority in that later sample does not invalidate the
earlier race; it confirms that the defect is reconcile-order dependent rather
than permanent Spot scarcity.

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
  keeps arrival/queue telemetry fresh. It exposes `in_flight_requests` as null,
  `confirmed_in_flight_requests` as the proven lower bound, and
  `unknown_in_flight_replica_count` as the exact coverage gap; it never turns
  partial coverage into an exact zero;
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
- when occupancy coverage is partial, the confirmed processing lower bound
  and number of backends with unknown occupancy rather than a request-rate
  substitute;
- accepted arrivals and rejected pressure over the current window;
- report age and reporter count; or
- an explicit stale/unavailable explanation.

The page polls the hash-fenced stable API demand endpoint independently of the
controller-backed status projection. A controller timeout therefore cannot
delay fresh request counters. During the dark-write transition, an older API
server or non-consolidated installation falls back to the existing status
response; a new consolidated server never silently converts a failed direct
read to zero.

The processing display and destructive idleness proof share authenticated
report inputs but have different projections. Retirement and paid-capacity
reconciliation continue to require a current-round proof for every relevant
URL. Operator observability may carry the latest generation-valid per-URL
sample through its bounded freshness TTL and reports coverage explicitly.
The load balancer must eventually publish those two projections separately so
one transient probe miss does not erase every confirmed processing count while
never weakening the downscale fence.

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

This precondition was satisfied by the later v1.1.1291 and v1.1.1296
production deployments. The live database remains compatible and forward at
Serve047/API-request 011 after revision 402 regressed only the binary to
v1.1.1287. It has not activated the new generalized action, demand, or
placement authorities. Subsequent rollouts must continue to use
`--reuse-values`; they must capture a fresh snapshot of the current live
revision immediately before each upgrade and must not redeploy an older
artifact merely to reproduce the originally proposed sequence.

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

The status projection keeps the durable request-report age and the
controller's ready-capacity observation age as separate fields and freshness
clocks. Overlaying a fresh or unavailable durable request report therefore
cannot revive stale logical ready capacity or invalidate a fresh controller
capacity observation.

Current reviewed size: 2,851 additions and 90 deletions across 38 files,
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
and controller-ready and durable-request observations retain independent
freshness clocks. The last gate exposed and fixed a product bug, rather than
only fixture contamination: fresh demand can no longer revive stale logical
capacity, and unavailable demand can no longer erase fresh logical capacity.
The exact regressions and the real-PostgreSQL case pass locally; corrected
exact-head CI and live rollout evidence remain open.

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

API version 85/API012/Serve050 add API-fleet capability identity, explicit
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

Serve050's authoritative constraints remain PostgreSQL-only, while the shared
controller metadata omits their PostgreSQL-specific expressions when a local
controller creates its still-supported SQLite database. The migration and
runtime authority stay on PostgreSQL; this prevents the central database
contract from accidentally breaking the separate local-controller backend.

API012 advertises one exact ordered-admission protocol capability on every
live `all|api|executor|controller` participant. Per-service promotion locks the
service, proves that fleet capability, a fresh complete durable demand report,
a fresh matching projected route, current controller ownership, and no legacy
demand mutation in flight, then advances the source epoch. After promotion the
controller-sync endpoint may still accept routing/drain reports during the
transition, but it cannot call `collect_request_information`; only the durable
reader may advance autoscaler demand state.

Reviewed P2b2 size after the API84 base collision was resolved: 46 files,
4,316 additions and 246 deletions.
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

Restack the first cleanup onto API014/Serve051 and keep it stacked and blocked;
API013 is owned by G1Sb executor-termination evidence. Restack the second
cleanup immediately above it onto API015/Serve052. After the documented rollout
gates, the two cleanup PRs remove controller-coupled telemetry ingestion,
unbound non-pool admission/recovery, the ordinary-only handler alias, global
startup recovery waiting, cluster-name/process-map authority, legacy incident
writers, dual-feed selection, and transition-only metrics/tests. Historical
audit tombstones and minute history remain.

Expected size: net-negative. The final topology must have fewer callable paths
than the current system.

## Deployment and rollback

All SkyPilot source branches target `boltz-bio/skypilot:improvements`. The
production deployment path is a direct Helm fix-forward from an immutable
artifact produced after the reviewed PR merges. The release tuple is
`skypilot` in namespace `skypilot`; `improvements` is the source branch and
must never be substituted as the release name. There is no required
`boltz-platform` runtime pin and no Terragrunt apply in this path.

Before every upgrade, capture `helm history`, live user and all-values,
manifest, current image IDs, and their hashes as rollback evidence. Pull the
exact OCI chart and verify its digest. Render a client-side dry run from that
chart with `--reuse-values`; a server-side dry run is forbidden because hooks
may mutate the live release. Any list-valued override must restate the complete
list element rather than replacing it with a partial `--set` value. Confirm the
diff contains only the intended immutable image/chart and configuration changes
and gate the rollout on zero active mutating requests. Upgrade with
`helm upgrade skypilot <exact-chart> -n skypilot --reuse-values --atomic
--wait --wait-for-jobs`, plus a reviewed complete image override when needed.
Afterward, record the new Helm revision and verify the migration job, exact
image IDs for API and init/sidecar containers, PostgreSQL schema heads,
readiness, health, and service behavior. Database migrations remain
forward-only. Roll back the Helm revision only when the old binary is proven
compatible with the current schema; otherwise merge, publish, and deploy a new
fix-forward artifact.

P1 and P2 are additive and dark before per-service promotion. Revision 405 is
one exact P1/P2-capable cohort; deployment alone intentionally did not change
any per-service authority mode. Promotion
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

1. the live deployment tuple at rollout time, including direct Helm revision
   405's exact v1.1.1302 artifacts, unchanged single-`all` topology, and the
   forward Serve050/API-request-012 database heads;
2. the completed pre-migration inventory of retained legacy rows and unsettled
   requests; reconcile only rows that actually remain. Record the historical
   absence of IDs 52032--52038 without treating it as quiescence, recreating
   rows, or backfilling associations. Preserve exact typed evidence for 52688,
   52689, and 52690; retain 52689's completed P1 ledger and original unmodified
   request, and let 52688/52690 use ordinary typed cleanup after the registry
   fix;
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

- [x] Verify the v1.1.1296 production compatibility baseline and the later
  revision-402 binary-only regression without generalized-action,
  demand-authority, or placement promotion (2026-08-16: single-`all`
  `Recreate`, forward Serve047/API-request-011 heads, service resource-action
  and binding modes legacy, non-pool capability false).
- [x] Merge P1, P2a, and P2b1 as PRs #1498, #1499, and #1503; publish P2b2 as
  PR #1504 and the blocked P3 removals as draft PRs #1506/#1510.
- [ ] Restack draft PR #1506 on the G1S cleanup lineage as API014/Serve051 and
  draft PR #1510 immediately above it as API015/Serve052; adversarially
  re-review both exact diffs before either is eligible to merge.
- [ ] Pass the complete P1 crash/mixed-version/provider-evidence matrix.
- [x] Inventory exact retained legacy rows and unsettled requests immediately
  before migration. The historical seven-row scope is absent and must not be
  reconstructed; retained rows 52688--52690 are recorded above.
- [x] Reconcile replica 52689 through reviewed executor-termination evidence,
  deliberate exact provider teardown, and a subsequent fresh exact absence
  observation through the P1 legacy ledger. Preserve its unmodified cancelled
  request without fabricating quiescence.
- [x] Deploy the reviewed Serve049/050 registry lineage as direct Helm
  revision 405.
- [ ] Verify ordinary typed cleanup converges replicas 52688 and 52690 without
  manual deletion.
- [ ] Repair the PHX server-owned Kubernetes admission configuration so an
  H200 reserved-fill Pod carries the required Kueue queue label; prove the
  admitted Pod still has the approved low preemptible priority class.
- [ ] Pass demand conservation, no-paid-spill, provider-free route, controller
  stall isolation, and dashboard tests.
- [ ] Promote the service on one immutable capable cohort and set
  `min_replicas: 0` after scale-from-zero preflight.
- [ ] Complete production readiness/+10/+30/stale-horizon monitoring and a
  rollback rehearsal.
- [ ] Merge P3 only after zero legacy-capable participants, zero unsettled
  unbound non-pool rows, and zero old telemetry/route-path usage.
