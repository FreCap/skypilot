# SkyServe demand, capacity, and telemetry convergence

Status: P1, P2a, P2b1, and P2b2 are merged in PRs #1498, #1499, #1503, and
#1504. PR #1521's partial-coverage in-flight observability, the complete G1S
executor-termination precursor stack through PR #1528, and PR #1529's exact
reserved-fill deployment-policy bundle are also merged. The newest exact
artifact is deployed directly with Helm as production revision 408 / release
`1.1.1312`, commit `966f74369d0722b253c7d47dad12248711928e70`, image
digest
`sha256:004478e3f12e2d217beea95acd6ddc79629cc064fdb87f002e8f6017d843dcc7`,
and chart digest
`sha256:18125d491ea2fd416f70b0f1f6c902747420c157fe018ca887d3ca48cb122825`.
The migration Job completed at API-request revision 014 and Serve revision 050;
the API reports protocol version 87. The API, active `boltz-l4-fleet` LB slot,
and the other 13 warm-standby LB deployments are ready with zero restarts. The
inactive `boltz-l4-fleet` standby deliberately remains on revision 407 until a
future cutover; it is not an active traffic owner.

P2c API88/Serve051 is implemented and locally reviewed in PR #1531 from branch
`fix/serve-route-replica-leases` as four implementation milestones plus
review/CI fix-forward commits above the revision-408 source. Its remote
PostgreSQL CI, PR merge, immutable image build, dark Helm deployment, and
production provider-stall qualification remain open. No P2c behavior has been
promoted on `boltz-l4-fleet`.

The `boltz-l4-fleet` authority modes remain deliberately unpromoted:
`LEGACY_CONTROLLER` demand, `LEGACY_PROXY` routes, legacy ordinary binding, and
no generic non-pool capability. Revision 407 nevertheless made the dark P2b1
route publisher and protocol-v2 demand reporter observable at production fleet
size. That qualification failed P3a's provider-stall merge gate. A sampled
route head had a 60-second TTL, but generation 515 was already 30 seconds stale
before generation 516 appeared; generation 516 then reached 208 seconds of age,
or 148 seconds past expiry, without refresh. Demand ingestion remained fresh.
The cause is architectural: the stable route read is provider-free, but the
only publisher still waits for one complete provider-fenced, manager-locked
fleet probe before refreshing the head.

The same observation separated the other live symptoms. The direct demand
projection had two fresh reporters, zero requests in the latest 60 seconds,
zero queue, zero rejected requests, and 25 accepted requests in the preceding
hour. Exact processing was correctly nullable because 27 routed backends had
unknown occupancy; the safe lower bound was zero. The dashboard can render
`0 confirmed processing` plus that coverage gap, but it must not claim exact
zero processing. Capacity was 63 ready out of 172 current-or-uncertain rows:
39 non-Spot on-premises backends and 24 paid Spot backends, with 109 Spot rows
already shutting down. Paid claims and waiters were both zero. Recent H200
reserved-fill attempts 53925--53933 converged to `FAILED_PROVISION` rather than
remaining phantom capacity, but they also proved that pool-phase contention
can still materialize speculative replica rows before the actuation lane is
owned. Their exact API errors show that Pods were rendered in
`rescluster-k8s-prod-east1-preemptible-inference` without a
`kueue.x-k8s.io/queue-name` label and were rejected by the namespace's
fail-closed admission policy. The live PHX contract is LocalQueue `be` ->
ClusterQueue `skypilot-be`; the `mt_hybrid` service workspace selects
WorkloadPriorityClass `be-ls`, while the lower-throughput partition workspace
uses `be-lt`. The selected Pod identity remains `skypilot-pool-sa` with Pod
PriorityClass `rescluster-k8s-prod-east1-preemptible-inference-low` (-1000,
`Never`). Revision 408 replaces that stale embedded policy with schema-v3
exact context contracts: east is explicitly unmanaged by Kueue and cannot
claim fill, while PHX names LocalQueue `be`, ClusterQueue `skypilot-be`,
WorkloadPriorityClass `be-ls`, and service account `skypilot-pool-sa`. The
policy remains correctly fail-closed. Its first live attestation blocker is
now explicit: the API-server IAM principal receives Kubernetes 403 when it
reads ServiceAccount `skypilot-pool-sa` in the PHX namespace. Once that
read-only RBAC is granted, the missing PHX `gpu-binpack-scheduler` Deployment
is the next blocker. Version 58 also retains task-owned Kubernetes
`pod_config`, `remote_identity`, and `provision_timeout` overrides, so the
strict worker-projection builder correctly persisted null rather than blessing
a competing Pod contract.

Revision 408 made no authority promotion or service/config mutation. A
post-rollout query found zero paid Spot rows with `created_at` at or after the
09:10:10 UTC Helm upgrade, including after fresh request telemetry resumed.
The latest sampled report had six confirmed processing requests, two
occupancy-unknown backends, zero queue, and fresh but incomplete compatibility
coverage. This proves the release stayed economically dark while also proving
why the UI must display confirmed processing and unknown coverage separately.

The exact post-#1503 P2b2 review scope was 42 files with 4,298 insertions and
241 deletions relative to `improvements`. Draft removals #1506 and #1510 are
implemented and tested, but production disproved one of their prerequisites.
They remain unmerged and undeployed. P2c and P2d below are now required first;
after their two additive Serve revisions, #1506 must be restacked as
API015/Serve053 and #1510 immediately above it as API016/Serve054.

Last updated: 2026-08-17

Canonical owner: this file for request telemetry ingestion, paid-capacity
admission, and the user-visible demand/capacity contract. Durable non-pool
action ownership remains owned by `durable-serve-replica-actions.md`, and
reserved zero-cost allocation remains owned by
`serve-multi-pool-reserved-capacity-fill.md`.

## Summary

SkyServe already measures request arrivals, completions, prediction time,
in-flight work, queue depth, rejection pressure, and accelerator compatibility
inside each load balancer. It also has PostgreSQL request-history tables,
status projections, dashboard request cards, and a zero-cost reserved-capacity
broker. P2a removed the controller from durable measurement and dashboard
reads, but the legacy autoscaler is not yet promoted to that feed. P2b1 removed
providers from route reads without removing them from route freshness: one
slow provider-fenced fleet probe can still starve the only projection head.
P2b2 added ordered zero-cost-before-paid planning, but it is still dark and the
fill launcher still creates a replica before it knows it owns the physical-pool
actuation lane. P2c and P2d close those remaining ownership boundaries.

The steady state has three independent, durable publications:

```text
authenticated load-balancer reporters
  -> central PostgreSQL demand feed
       -> autoscaler demand snapshot
       -> dashboard live/history snapshot

provider-fenced endpoint resolution
  -> exact per-replica PostgreSQL route material
       -> provider-free parallel readiness leases
            -> immutable route projection + freshness head
                 -> provider-free load-balancer route reads

fresh demand + current route/capacity + current zero-cost allocation
  -> one placement plan
       -> commit zero-cost actuation intents first
       -> admit paid rows only for the residual compatible deficit
       -> one per-pool actuation owner materializes a replica/action
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
- Resolve exact route material once under the provider identity fence, then
  renew or revoke each replica's readiness lease without provider queries,
  manager locks, or an all-fleet completion barrier.
- Let fresh aggregate zero demand revoke paid scale-up authority immediately;
  drain each paid replica and wait for its own exact zero-occupancy proof before
  destructive teardown. Incomplete exact-card attribution may block scale-up,
  never preserve a stale paid target by itself.
- Persist a zero-cost fill actuation intent before a replica row. Create the
  replica/action only after the single per-physical-pool executor owns its
  provider phase and physical identity fence.
- Treat the embedded deployment-policy bundle and effective server/workspace
  worker projection as one release contract. A version cannot become
  fill-capable unless both match the same live namespace, queue, service
  account, Pod priority, scheduler, accelerator, and physical-cluster
  attestation.
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
- This does not widen a route TTL, retry the whole fleet more aggressively, or
  treat load-balancer health checks as provider identity. Those are mitigations,
  not the route-publication ownership fix.
- This does not infer zero occupancy from zero arrivals or a zero queue. A paid
  replica is destroyed only after it is off-route and its own current work is
  proved zero under the existing drain contract.

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
  -> attempt ordinary/reserved zero-cost-only intent admission
  -> if any unexpired zero-cost intent commits: stop and replan
  -> PostgreSQL locks service, reports, route, and all replica rows
  -> paid_residual = max(0, target - committed/pending_zero_cost
                                  - committed_paid)
  -> publish/refresh plan head
  -> admit paid claim only within that residual
```

The P2b transition uses the existing manager and reserved broker with paid
selection hard-disabled. There, `Accepted` means a durable replica row was
committed, not that an in-memory choice was attempted. P2d moves the acceptance
boundary earlier but keeps it durable: `Accepted` means PostgreSQL committed an
unexpired actuation intent for an exact broker grant and physical pool. Such an
intent temporarily debits the zero-cost residual before a replica exists. A
deferred, rejected, expired, or superseded intent is not counted. Any accepted
intent ends the reconcile so paid residual is never inferred from its
pre-commit snapshot.

Fresh zero has a narrower destructive contract than positive demand. A fresh
current ACTIVE protocol-v2 report with zero arrivals in the complete demand
window and zero queue revokes all ordinary paid *scale-up* authority and
publishes an aggregate paid target of zero even if exact-card compatibility is
incomplete. It may also place paid endpoints into the normal draining route
state so compatible zero-cost endpoints receive new work. Zero arrivals and
zero queue do not prove zero processing: each paid replica remains alive until
every current ACTIVE/DRAINING reporter proves that exact URL has zero in-flight
work and the existing drain fence commits. An unknown URL therefore delays only
that replica's teardown, not route draining, zero plan publication, unrelated
paid replicas, or reserved fill. Exact-card incompleteness continues to block
positive paid scale-up and cross-card replacement; it cannot by itself preserve
a stale positive paid target.

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

### Zero-cost actuation intent

Serve052 adds one PostgreSQL `serve_zero_cost_actuation_intents` relation. It
is the only boundary between a broker allocation and creation of a
`ReplicaInfo`/generic non-pool action. An immutable intent binds service hash,
lifecycle/version, allocation generation/digest, exact pool key, physical
cluster UID, Kubernetes context, accelerator/count, reclaim-policy identity,
worker projection digest, and the existing broker idempotency key. The unique
idempotency key makes repeated broker rounds one intent. Its states are:

- `GRANTED`: accepted zero-cost capacity, with no replica or API request;
- `ACTUATING`: one executor owns the exact pool-lane lease and provider-phase
  admission; still no provider effect is inferred;
- `COMMITTED`: the executor atomically created the exact replica, generic
  association/request, and intent-to-replica receipt while retaining the lane;
- `RETRYABLE`: the lane was unavailable or the executor lost ownership before
  row creation, so no replica exists and the same intent may be leased again;
- `TERMINAL`: the grant expired, was superseded, or failed validation before
  materialization.

The per-pool executor leases one intent with PostgreSQL fencing, then acquires
the process provider phase and physical-cluster identity fence before it may
materialize a replica row. If either lane is busy, it returns the intent to
`RETRYABLE`; it must not allocate a replica ID or API request. Once row/action
creation commits, the generic non-pool executor and its termination evidence
own every ambiguous outcome. A crash before that commit leaves only a lease
that can expire; a crash after it leaves an exact association that existing
recovery can reconcile. Executors for other physical pools proceed
independently.

An unexpired `GRANTED` or `ACTUATING` intent is pending zero-cost capacity in
the paid-residual transaction, bounded by the broker allocation TTL. `COMMITTED`
is counted through the replica row, never both representations. This preserves
no-paid-spill without allowing a dead intent to suppress paid capacity forever.
The dashboard exposes intent counts separately from queued replicas, provider
setup, and cleanup uncertainty.

### Route projection

Serve049's immutable full snapshots, exact private URL/record identities,
bounded aliases, and freshness-bearing head remain the public projection
format. P2c changes its producer, not its load-balancer wire document. A
semantic change inserts one snapshot and advances the head; an identical
provider-free compose pass refreshes only the head. Old snapshots remain
retained longer than the demand-report TTL and are pruned by the existing fixed
upper bound.

Serve051 adds one `serve_route_replica_leases` row per exact
`(service_name, service_hash, replica_id, replica_record_id)`. It contains
normalized URL and accelerator material, the service/controller/version owner
tuple under which that material was resolved, a content digest, readiness
result/generation, PostgreSQL-clock `observed_at`/`valid_until`, and explicit
revocation metadata. Route material enters this relation only after the
existing provider-fenced resolver proves the physical identity and resolves
the endpoint. That resolver performs no fleet publication.

A dedicated supervised route worker has two provider-free stages:

1. The readiness stage reads only durable candidate rows and immutable version
   probe settings, probes URLs concurrently with the existing bounded HTTP
   timeout, and commits each exact lease independently. One timeout or corrupt
   identity expires/withholds only that row.
2. The compose stage reads fresh leases plus current replica/service/version
   rows in one PostgreSQL transaction, excludes individually stale, revoked,
   non-ready, draining, wrong-version, or owner-mismatched entries, rebuilds the
   capacity hint from durable replica attribution, and publishes or refreshes
   the immutable full snapshot/head.

The worker never acquires the replica-manager lock, provider phase, physical
cluster fence, Kubernetes client, Ray handle, or cluster database. Its cadence
and head TTL are fixed operational bounds; a worker stall still expires the
head, but a provider stall cannot. There is no all-fleet `complete` bit. A new
endpoint simply remains absent until its own material and readiness lease are
valid, while every healthy sibling continues to renew.

Every central replica mutation that makes a route ineligible--record-identity
replacement, READY exit, drain admission, version retirement, service update,
controller-owner change, quarantine, or teardown--revokes the exact lease in
the same PostgreSQL transaction. A later HTTP success cannot revive it because
the lease update exact-matches the record ID, owner tuple, and revocation
generation. Endpoint re-resolution creates new material and a new readiness
generation; bounded aliases remain snapshot history only.

Every projected response adds the snapshot generation, digest, and route-source
epoch. The LB records those fields only after it atomically applies that same
routing spec and ready set, then echoes them in its durable demand report.
Future demand authority can therefore translate URL-keyed occupancy through
the exact immutable snapshot the reporter observed; a current URL is never
guessed to represent an older report.

Composition locks the service row; reads use one PostgreSQL transaction and
exact-match the service hash, lifecycle epoch, controller
incarnation/owner epoch, PID/IP, applied version, and non-pool discriminator.
The service row has one explicit
`LEGACY_PROXY` or `DURABLE_PROJECTED` route mode and monotonic mode epoch.
Promotion requires protocol-2 route capability, fresh per-replica lease
coverage for every currently advertised URL, and a fresh head composed for the
current capable controller incarnation. After promotion, a missing, corrupt,
stale, or owner-mismatched projection fails closed; it never falls back to the
controller proxy. The legacy proxy remains the only response owner before
promotion.

An ambiguous replica is omitted or marked unroutable without suppressing
healthy siblings. Provider-phase contention delays only new or changed route
material; it cannot prevent existing exact leases or the projection head from
refreshing. The stable API and load balancer read the projection only; they
perform no provider, Kubernetes, Ray, or cluster-database query. A stale read
returns unavailable, so a warm LB retains its already-applied routes under the
existing sync-outage behavior while a cold LB cannot become ready from stale
evidence. A stale generation is visible as stale telemetry and cannot
contribute ready capacity to a new paid-admission decision.

During rollout, an exact API/controller cohort first advances the existing
per-service route projection protocol to 2 while route source remains
`LEGACY_PROXY`. Protocol 1 publication stops for that service; mixed writers
never share one head. After a full dark qualification horizon, ordinary route
promotion selects the already-existing `DURABLE_PROJECTED` read path. P3a then
removes the protocol-1 all-fleet publisher together with the legacy proxy. No
second LB application protocol or long-lived dual-writer path is introduced.

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
- The service controller owns autoscaler policy and provider-fenced endpoint
  resolution. The dedicated route worker owns HTTP readiness leases and route
  composition; neither owns telemetry durability.
- The reserved broker owns zero-cost allocation; the paid-capacity ledger owns
  paid claims; neither silently substitutes for the other.
- The per-physical-pool actuation executor owns the transition from a broker
  intent to one replica/action. The broker never creates speculative replicas.
- The generic non-pool action executor owns provider effects and exact
  quiescence/result reconciliation.

### Isolation

Telemetry ingestion takes only the service-incarnation and reporter keys. It
does not join provider phases, manager locks, launch associations, or route
publication. Route readiness/composition does not wait for provider I/O,
legacy action reconciliation, or a sibling URL. Pool actuation serializes only
the exact physical pool; a busy pool creates no replica and cannot block
another pool. Per-association reconciliation holds no manager/global lock
across I/O or sleep. These rules are tested with a permanently blocked
provider read, a busy pool lane, and an ambiguous legacy row while traffic
reports, healthy route generations, unrelated fill, and dashboard reads
continue.

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

### P2c: incremental route leases and safe paid retirement

API version 88/Serve051 add `serve_route_replica_leases`, route projection
protocol 2, the provider-free readiness/composition worker, transactional
revocation hooks, and the fresh-zero paid-retirement rule. The existing LB
sync response, snapshot tables, aliases, and route read endpoint remain the
only public route protocol. Protocol 1 stays available only for unconverted
services during the mixed cohort and is removed by the already-authored P3a
cleanup after the protocol-2 production gate.

Implementation is split into four reviewable code commits in one additive PR,
preceded by the canonical-design update:

1. PostgreSQL schema/repository and pure validation for exact route material,
   readiness leases, owner fencing, revocation, and bounded retention;
2. provider-fenced resolver writes with no publication side effect;
3. supervised provider-free HTTP readiness and snapshot composition, including
   controller restart/adoption and one poisoned-row isolation;
4. service/LB capability, dark protocol-2 selection, promotion gates, fresh
   aggregate-zero autoscaler semantics, and per-replica exact-idle paid
   retirement.

The reviewed branch currently changes 36 files with roughly 4,400 additions
and 200 deletions relative to revision 408, including the design and one Serve
migration. This is larger than the original 1,600--2,600-line estimate because
the final implementation includes immutable material digests, protocol-1/2
collision fencing, transactional route revocation at every READY-exit path,
restart recovery, exact paid-retirement authority, and real-PostgreSQL race
tests. The exact-versus-confirmed processing UI was already merged in PR #1521;
P2c consumes and preserves that contract rather than adding a second dashboard
path. It is intentionally not a TTL adjustment: the required
stress test holds provider I/O forever while at least ten consecutive route
head renewals stay within the configured cadence, one URL independently
expires, a replacement URL becomes ready, and demand/dashboard writes remain
fresh. At fresh aggregate zero, tests prove zero paid claims/cold-launch
authority immediately, off-route draining for paid endpoints, no destructive
teardown for an occupancy-unknown URL, and teardown of each independently
proved-idle paid replica.

The paid-retirement state is durable and explicitly exact-idle-only. It is not
the existing bounded graceful-drain fallback: elapsed time, a missing route,
or controller restart never converts unknown occupancy into teardown
authority. Admission revokes the exact route in the same transaction and may
proceed for every paid replica; the teardown executor advances only the subset
whose current active/draining reporters prove zero occupancy. A later positive
demand plan may cancel an uncommitted retirement only through the normal
generation fence, never by silently republishing the revoked lease.

The route-revocation hook first checks for the additive Serve051 lease table
and caches that answer only for the current state transaction. It is an
idempotent no-op on pre-Serve051 schemas and for historical non-routable
replica-ID-zero sentinels because neither can have a valid lease. Once Serve051
exists, all valid route identities take the single transactional revocation
path. Paid-retirement authority does not share this compatibility behavior:
missing retirement state remains a fail-closed error on destructive paths.

The implemented paid-retirement transaction binds the exact service owner,
replica record, demand generation, fresh route head, and zero-residual capacity
plan. Fresh aggregate zero immediately clears cold paid-launch authority and
publishes a nonempty all-zero capacity target. Every paid replica is then made
off-route atomically with an `ACTIVE` retirement intent. A never-ready replica
may commit immediately; a previously routable replica has no deadline-based
escape and becomes `COMMITTED` only after its retained exact route URL reports
idle. Positive demand can cancel only an `ACTIVE` intent under a strictly newer
demand generation. `COMMITTED` is irreversible and recovery re-drives it with
a zero-second teardown cap.

Local evidence on 2026-08-17 includes the complete Serve controller, replica
manager, Serve state, concurrency autoscaler, demand, capacity-admission,
route-projection, incremental-worker, request-aggregator, and migration utility
suites. The three changed PostgreSQL suites collect successfully but skip
locally because no Docker daemon is available; remote real-PostgreSQL execution
is therefore a merge gate. `format.sh` passed mypy over 953 source files,
changed-file pylint at 10.00/10, dashboard ESLint, and dashboard formatting.

The additive PR must link to draft #1506 as its stacked removal. #1506 is
expanded and restacked to delete the protocol-1 all-fleet publisher, old route
mode, transition capability/metrics, and transition tests only after one
complete production stale horizon with protocol 2 as the sole selected writer.

### P2d: grant-before-row zero-cost actuation

Serve052 adds the actuation-intent relation and per-physical-pool executor
defined above. The existing broker remains the sole allocation planner, and
the generic non-pool executor remains the sole owner after a replica/action
commit. The old broker-to-manager direct row materialization remains only for
services outside the protocol-2 cohort during rollout.

Implementation is split into four reviewable commits in one PR stacked on
P2c:

1. intent schema/repository, idempotency, TTL, state reducer, and paid-residual
   debit;
2. broker publication of exact intents without replica allocation;
3. per-pool leasing, provider/physical-fence admission, and atomic
   intent-to-replica/generic-action commit; and
4. restart recovery, observability/UI status, migration tooling, and removal
   manifest integration.

Estimated size is 1,800--3,200 source/test additions across 22--35 files and
one Serve migration. Required tests hold one pool's provider phase indefinitely
and assert repeated broker rounds create one intent and zero replicas/requests,
while another pool materializes normally. Crash injection covers every state
boundary. PostgreSQL tests prove a pending intent debits paid residual exactly
once, expiry releases it, `COMMITTED` transfers accounting to the replica, and
reserved fill can never select Spot or On-Demand.

#1506 is also the stacked removal for P2d: after every non-pool service uses the
intent executor and the direct-path usage counter remains zero for the
documented horizon, it removes direct broker-to-replica materialization and
the transition branch/tests. The feature and removal PR descriptions must name
the same exact production evidence gate.

### P3: blocked steady-state cleanup

Restack the first cleanup (#1506) onto P2d as API015/Serve053 and keep it draft
and blocked; API013/API014 remain owned by G1Sb/G1Se executor-termination
evidence. Restack the second cleanup (#1510) immediately above it as
API016/Serve054. After the documented rollout gates, the two cleanup PRs remove
controller-coupled telemetry ingestion, the protocol-1 all-fleet route
publisher, direct broker-to-replica fill, unbound non-pool admission/recovery,
the ordinary-only handler alias, global startup recovery waiting,
cluster-name/process-map authority, legacy incident writers, dual-feed
selection, and transition-only metrics/tests. Historical audit tombstones and
minute history remain.

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

P1, P2, P2c, and P2d are additive and dark before per-service promotion.
Revision 408 is the current exact G1S/P2/policy-capable cohort; deployment
alone did not change any `boltz-l4-fleet` authority mode. P2c and P2d each
deploy first as
an additive direct-Helm revision with `--reuse-values --atomic --wait
--wait-for-jobs`, then activate only on one exact capable cohort after their
dark gates. Promotion requires no unsettled unbound work for that service,
fresh protocol-2 demand/route publications, zero direct fill-path usage, and a
successful injected-failure rehearsal. Rollback before P3 means disable new
promotion, drain/project bound work, and return the service to the previous
cohort through its fenced transition. Schema downgrade is forbidden.

P3a/P3b are forward-only offline cutovers, not additive rollouts. Their Helm
upgrade explicitly omits `--atomic` so Helm cannot restore an old binary after
the migration transaction commits. All old central participants remain stopped
for the documented horizon; any post-commit failure is repaired with a new
schema-compatible image and another direct Helm fix-forward. After P3,
automatic or manual restoration of removed paths is forbidden.

Revision 408 used the intended exact chart/image and `--reuse-values` path,
but its preflight also executed a server-side Helm dry run and the final
command omitted `--atomic --wait-for-jobs`. The server dry run completed
without mutation and the actual migration/rollout completed successfully, but
both are recorded deviations rather than new precedent. Subsequent additive
rollouts use the canonical client-side render and atomic wait-for-jobs command
above.

The PHX fill admission correction is an operational prerequisite rather than a
SkyPilot image pin. The exact live read on 2026-08-17 resolved the release
contract for `boltz-l4-fleet`'s `mt_hybrid` workspace as:

- namespace `rescluster-k8s-prod-east1-preemptible-inference` and service
  account `skypilot-pool-sa`;
- LocalQueue `be`, ClusterQueue `skypilot-be`, and WorkloadPriorityClass
  `be-ls` (value 12, latency-sensitive best effort);
- Pod PriorityClass
  `rescluster-k8s-prod-east1-preemptible-inference-low` (value -1000,
  `Never`), scheduler `gpu-binpack-scheduler`, H200 ResourceFlavor
  `ml.p5e.48xlarge`, and physical cluster UID
  `ba2dcdca-2a0d-447f-ad8a-31849a63c1d5`.

Production uses PostgreSQL-backed API-server configuration; the mounted
`skypilot-config` ConfigMap is intentionally `{}`. Snapshot and hash the full
current config, then use the audited workspace-config update path to add the
server-owned `mt_hybrid` PHX `kueue.local_queue_name: be` and
`serve_worker_kueue_workload_priority_class_name: be-ls`, together with the
complete server-owned priority, service-account, scheduler, accelerator,
cache, and scratch projection inputs. Do not seed or patch the database out of
band. Revision 408 already corrected the embedded Boltz policy bundle and
deployed it dark. The next operational prerequisite is a narrowly scoped
read-only RBAC grant that lets the API-server principal attest ServiceAccount
`skypilot-pool-sa`; the current request is denied with Kubernetes 403. The
following prerequisite is deployment of the exact
`gpu-binpack-scheduler` named by both the live worker contract and policy. Only
after both attestations pass may the audited config transaction compile/elect
a new service version with task-owned Kubernetes overrides removed and
`min_replicas: 0`. Its immutable worker placement projection must contain the
exact pair. A successful H200 Pod must show the `be` queue label,
`skypilot-be` Kueue admission, `be-ls` workload priority, and the approved
reclaimable Pod priority. Guessing `default`, relying on admission mutation to
invent a queue, or adding a task-owned label is forbidden.

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
- provider-free route reads and publication, per-replica lease expiry/revocation,
  atomic generation replacement, one poisoned row, provider stall, controller
  restart, and service update;
- zero-demand paid drain with incomplete exact-card attribution, exact
  per-replica occupancy gating, and no false destructive zero;
- grant-before-row pool isolation, intent expiry/accounting transfer, every
  crash boundary, and zero replica/request creation while a pool lane is busy;
- dashboard fresh-zero, live processing, queued, rejected, stale, unavailable,
  history-gap, and placement-explanation states; and
- transition and P3 source-absence tests proving one feed, planner, route path,
  and generic non-pool handler remain.

Manual production verification records:

1. the live deployment tuple at rollout time, including direct Helm revision
   408's exact v1.1.1312 artifacts, unchanged single-`all` topology, and the
   forward Serve050/API-request-014 database heads;
2. the completed pre-migration inventory of retained legacy rows and unsettled
   requests; reconcile only rows that actually remain. Record the historical
   absence of IDs 52032--52038 without treating it as quiescence, recreating
   rows, or backfilling associations. Preserve exact typed evidence for 52688,
   52689, and 52690; retain 52689's completed P1 ledger and original unmodified
   request, and let 52688/52690 use ordinary typed cleanup after the registry
   fix;
3. the failed P2b1 dark-route gate: 60-second TTL, generation 515 stale before
   516, and generation 516 observed 148 seconds past expiry while durable
   demand stayed fresh. P2c must replace this evidence with at least ten
   on-cadence protocol-2 renewals during an injected provider stall;
4. a fresh zero-traffic interval with target zero, zero paid claims/waiters,
   paid endpoints draining, and no new paid request. Record exact occupancy
   coverage and do not call a lower bound an exact zero;
5. traffic served first by compatible ready/reserved capacity, followed by a
   paid Spot launch only after a recorded positive residual;
6. scale from zero, scale back to zero, controller restart, and service update;
7. dashboard in-flight/queued/completed counts and freshness matching the
   durable feed. Revision 408 has additionally shown six confirmed processing,
   two occupancy-unknown backends, and zero queue after rollout; neither
   unknown coverage nor an arrival rate may be presented as exact processing;
8. PHX H200 intent-to-Pod admission with exact server-owned Kueue and
   reclaimable-priority evidence, plus zero speculative rows under a held pool
   lane; and
9. readiness, +10, +30, and one complete stale/quiescence horizon before P3.

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
reserved-broker, paid-claim, projection, and ordinary-binding abstractions. P2a
and P2b added a live demand report and route generation. Production
qualification proved two additional durable boundaries are necessary: an exact
per-replica route lease between provider resolution and projection, and a
zero-cost actuation intent between broker allocation and replica creation. They
replace process-local/all-fleet coordination rather than duplicating it; P3a
removes both predecessor paths.

P2b1 adversarial review also rejected a mutable current-route row and a new
full/delta LB protocol. The accepted implementation keeps immutable full
snapshots with one freshness head, uses the existing full sync response, binds
each response to an exact private URL/record map, and retains only bounded
fixed-lifetime aliases for already-live records. It fails closed when route
source ownership cannot be proven and isolates exact malformed or colliding
rows instead of converting ambiguity into a fleet-wide publication barrier.

The revision-407 gate rejects three tempting mitigations. Increasing the route
TTL merely hides an unbounded provider dependency. Republishing the last full
snapshot without current per-replica leases can revive a replaced or draining
endpoint. Creating a replica first and retrying cleanup when the pool lane is
busy preserves the phantom-capacity failure mode. P2c renews exact leases and
composes only their fresh subset; P2d queues the grant and materializes no row
until the lane is owned.

The 2026-08-17 PHX adversarial review rejected three further partial fixes.
Patching the mounted ConfigMap cannot work because production configuration is
PostgreSQL-backed and the ConfigMap is `{}`. Adding only the PHX queue to the
server config cannot create a trusted version because version 58 still carries
task-owned Pod config, remote identity, scratch/cache mounts, priority, and
provision timeout; strict projection correctly rejects those competing inputs.
Finally, editing only the PHX strings in the policy's schema-v2 JSON would
leave the same false mandatory-Kueue assumption for east. The accepted path is
one optional per-context Kueue contract, one complete audited server-config
transaction, and one service version with all worker Pod identity/configuration
owned by the server projection.

The 2026-08-17 exact P2c diff review rejected four unsafe shortcuts. A legacy
unbounded-looking drain row cannot identify a paid retirement because its JSON
shape collides with older cleanup states; only the PostgreSQL retirement intent
is the discriminator, and failure to read that table blocks teardown. An empty
accelerator map cannot vacuously prove an all-zero target. A protocol-1 route
owner cannot assert the protocol-2 aggregate-zero exception. Finally, an exact
route generation is insufficient after its head expires, so retirement
admission and commit also lock and validate the still-fresh route head. These
corrections are implemented and covered by focused regressions.

## Open gates

- [x] Verify the v1.1.1296 production compatibility baseline and the later
  revision-402 binary-only regression without generalized-action,
  demand-authority, or placement promotion (2026-08-16: single-`all`
  `Recreate`, forward Serve047/API-request-011 heads, service resource-action
  and binding modes legacy, non-pool capability false).
- [x] Merge P1, P2a, and P2b1 as PRs #1498, #1499, and #1503; publish P2b2 as
  PR #1504 and the blocked P3 removals as draft PRs #1506/#1510.
- [x] Complete the first G1S restack of draft #1506 as API015/Serve051 and run
  its full remote CI; complete the local API016/Serve052 #1510 restack and its
  real-PostgreSQL API-request suite. Both remain draft and undeployed.
- [ ] After P2c/P2d, restack #1506 as API015/Serve053 and #1510 immediately
  above it as API016/Serve054, expand #1506 with both transition removals, and
  adversarially re-review the exact diffs before either is eligible to merge.
- [x] Pass the complete G1S precursor crash/mixed-version/provider-evidence
  qualification and record it in merged PR #1528.
- [x] Inventory exact retained legacy rows and unsettled requests immediately
  before migration. The historical seven-row scope is absent and must not be
  reconstructed; retained rows 52688--52690 are recorded above.
- [x] Reconcile replica 52689 through reviewed executor-termination evidence,
  deliberate exact provider teardown, and a subsequent fresh exact absence
  observation through the P1 legacy ledger. Preserve its unmodified cancelled
  request without fabricating quiescence.
- [x] Deploy the reviewed Serve049/050 registry lineage and G1S precursor as
  direct Helm revision 407 / v1.1.1310; verify API014/Serve050, exact images,
  migration completion, all API/LB readiness, and zero restarts.
- [x] Merge PR #1529 and deploy its exact schema-v3 policy bundle as direct
  Helm revision 408 / v1.1.1312; verify API014/Serve050, active fleet LB
  cutover, health, zero restarts, and zero paid rows created after the upgrade.
- [ ] Verify ordinary typed cleanup converges replicas 52688 and 52690 without
  manual deletion. Both rows are now absent; audit their exact terminal
  receipts before closing this gate because absence alone is not evidence.
- [x] Run P3a's provider-stall route gate on revision 407. It failed: the
  60-second dark route head remained stale for at least 148 seconds while
  demand stayed fresh. Keep #1506 draft and undeployed.
- [ ] Merge P2c API88/Serve051 after remote real-PostgreSQL CI and final exact
  diff review, update its simultaneously maintained #1506 removal diff, then
  deploy dark and prove the ten-renewal provider-stall gate. Local
  implementation and focused review are complete on
  `fix/serve-route-replica-leases`.
- [ ] Implement, adversarially review, and merge P2d Serve052 with its
  simultaneously maintained #1506 removal diff; deploy dark and prove busy
  pool, crash, no-paid-spill, and accounting-transfer gates.
- [x] Replace the stale revision-407 PHX deployment-policy identities with
  LocalQueue `be`, ClusterQueue `skypilot-be`, WorkloadPriorityClass `be-ls`,
  and service account `skypilot-pool-sa`; deploy the correction dark in
  revision 408.
- [ ] Grant the API-server principal exact read-only attestation access to PHX
  ServiceAccount `skypilot-pool-sa`, deploy the exact
  `gpu-binpack-scheduler`, then prove the policy preflight passes without
  weakening any check.
- [ ] Apply the complete audited server-owned worker config, compile/elect a
  clean service version, and prove an H200 reserved-fill Pod is admitted with
  the exact queue, workload priority, and low preemptible Pod priority.
- [ ] Pass demand conservation, no-paid-spill, provider-free route publication,
  fresh-zero paid drain, grant-before-row pool isolation, controller stall
  isolation, and dashboard tests.
- [ ] Promote the service on one immutable capable cohort and set
  `min_replicas: 0` after scale-from-zero preflight.
- [ ] Complete production readiness/+10/+30/stale-horizon monitoring and a
  rollback rehearsal.
- [ ] Merge P3 only after zero legacy-capable participants, zero unsettled
  unbound non-pool rows, and zero old telemetry/route-path usage.
