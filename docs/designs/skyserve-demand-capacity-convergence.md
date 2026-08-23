# SkyServe demand, capacity, and telemetry convergence

Current status (2026-08-23 07:48 UTC): `boltz-l4-fleet` lifecycle 94/version 1
uses PostgreSQL-authoritative `DURABLE_INTENT` actuation and
`SEQUENCED_ACTIVE` generation 36. PR #1679 is deployed as release `1.1.1449` /
Helm revision 573 and completed the supported lifecycle-93 purge plus clean
lifecycle-94 recreation. PR #1680 is deployed homogeneously as release
`1.1.1450` / Helm revision 575 at merge
`b311dd2775c150895918121cbf2b16c0ba21f5dd`; its generation-35-to-36
active-to-active CAS is complete and must not be repeated. The next permissible
authorization is one generation 36-to-37 CAS only after the reviewed historical-
cleanup change is merged and deployed homogeneously and the exact preflight in
`serve-multi-pool-reserved-capacity-fill.md` passes. That document is
authoritative for live rollout state and evidence. The older phase account below
is historical chronology and is not an executable runbook.

Status: P1, P2a, P2b1, and P2b2 are merged in PRs #1498, #1499, #1503, and
#1504. PR #1521's partial-coverage in-flight observability, the complete G1S
executor-termination precursor stack through PR #1528, PR #1529's exact
reserved-fill deployment-policy bundle, P2d PR #1537, and PR #1540's closure
of the untyped protocol-v2 fill batch path are also merged. PR #1542's
supply-aware paid-residual bound is merged and qualified. The newest exact
artifact is deployed directly with Helm as production revision 418 / release
`1.1.1325`, merge commit
`f5cf1c74cfe2c417a3551f70951f0191e762bad4`, image digest
`sha256:e330fb8d0cdff9e153291173c513eb6aca34b0556d51ce435906ce82cce7fc49`,
and chart digest
`sha256:586ee857f199713522432431291363a55ee7dcb930a13616d4229533df87d512`.
The Serve052/API-request-015 migration Job completed, the API reports protocol
version 89, and the API plus both `boltz-l4-fleet` load-balancer slots are ready
on the exact image with zero restarts. No service version, authority mode, or
`boltz-platform` pin changed in this direct Helm rollout.

P2d Serve052/API-request-015 is merged and deployed dark. It adds the durable
intent ledger, API89's exact protocol-2 paid-admission
fleet barrier, broker publication, per-physical-pool executor, atomic
intent-to-replica accounting transfer, one-way promotion endpoint, and dark
capability advertisement. The direct demand endpoint and service UI also expose
the actuation mode, epoch, pending-before-row count, and state counts separately
from replica/provider state. The focused local verification is green: 13
real-PostgreSQL admission/migration tests, 81 manager/broker/binding tests, and
129 dashboard tests. The complete affected admission surfaces are also green:
145 API-request PostgreSQL tests and 31 capacity-admission/refill tests.
Production remains `DIRECT_REPLICA`; the initial dark-rollout verification
found zero intents and zero replicas or `sky.launch` requests created at or
after the upgrade. Later samples found legacy direct-controller H200 fill
attempts, described below; the P2d intent path itself remains dark with no
authority promotion.

P2c API88/Serve051 is merged in PR #1531 and deployed dark. Its complete remote
PostgreSQL run passed 17,471 tests (plus 199 subtests), and the final affected
local PostgreSQL run passed 293 tests. The first production qualification found
a bootstrap integration defect before provider-stall injection: the controller
selected its route producer from `get_service_from_name()`, whose deliberate
Serve037 compatibility projection omits `route_source_mode` and
`route_projection_protocol_version`. The deployed controller therefore kept
publishing protocol 1, wrote zero exact route leases, and could not exercise the
P2c renewal contract. PR #1532 changed that bootstrap read to the route
repository's exact current-owner projection and revision 410 deployed it.
Production then selected protocol 2 and wrote 149 exact material rows (16 A100,
15 A100-80GB, and 118 paid L4), but generation 660 expired after its first
publication and none of the readiness leases was observed. A nonblocking thread
dump showed both the incremental worker and LB sync waiting on
`_routing_state_lock` while the autoscaler held it for fleet-wide cost planning.
The worker later recovered to generation 673 with 125 fresh-ready leases only
after that lock was released; the intervening head expiry still fails the
cadence contract.
PR #1533's fix-forward publishes version plus routing policy through a separate
immutable route-contract snapshot and proves composition completes while the
autoscaler epoch lock is held indefinitely. It is merged and deployed in
revision 411. Production proves that boundary works: a nonblocking controller
dump showed the autoscaler doing fleet-wide cost planning while the incremental
worker proceeded independently. It also exposed the next bounded ownership
defect. Every completed HTTP probe synchronously opened and committed its own
PostgreSQL transaction on the worker's asyncio event loop. At 94 current
targets, connection checkout and receipt serialization delayed a first refresh
by roughly 50 seconds and stretched later nominal five-second refreshes to
roughly 10--13 seconds.

PR #1534 moved receipt persistence to one dedicated bounded writer and is live
in revision 412. HTTP tasks now return immutable exact-generation results; the
event loop coalesces at most the newest result per exact target; and the writer
persists one bounded batch with one bulk PostgreSQL update in one transaction.
A 12-second production thread profile proved the receipt thread executing the
bulk update while the incremental event-loop thread independently listed,
probed, composed, and renewed. Sixty-four current material rows converged to
64 observed and 64 fresh rows, and the minimum sampled head TTL was 57.426
seconds.

That qualification exposed the final database-shape defect rather than closing
the cadence gate. The provider resolver still holds the service-owner row and
all selected replica/lease rows while executing per-replica identity reads,
material reads, generation queries, upserts, history queries, and deletes. The
probe-target reader likewise executes one replica lookup per lease. During a
natural provider material refresh, one ten-renewal sample included a 16.987
second interval instead of the nominal five-second cadence even though all 64
leases stayed fresh. The current fix-forward replaces those fleet-proportional
round trips with a fixed number of bounded bulk statements while retaining the
same owner, replica-record, version, revocation, material-generation, and
history invariants. No P2c behavior has been promoted on `boltz-l4-fleet`.

PR #1535 merged that fixed-statement writer and revision 413 deployed it dark.
The exact image is healthy with zero restarts, its migration completed, and all
121 current exact-owner materials converged to observed, fresh-ready leases.
Production nevertheless rejected promotion again. One takeover window
contained a 100.433-second head interval, and steady-state stale-trigger
captures found repeated 12.675- and 17.406-second intervals. PostgreSQL showed
the incremental composer connection `idle in transaction` after its
`SELECT ... FROM replicas ... FOR UPDATE`; the controller stack showed that
same thread deserializing `ReplicaInfo` and waiting on a lazy Kubernetes module
load. Material and receipt writers were blocked behind its service-owner lock.
The remaining defect is therefore not the material batch: composition still
runs arbitrary Python decoders and the capacity callback while holding the
service, every replica, and every lease row lock.

PR #1536 makes composition a two-phase optimistic publication.
The prepare phase reads owner, replica, and lease inputs without row locks,
closes its transaction, and performs all deserialization, lazy imports,
capacity aggregation, and response construction with no database transaction
open. The publish phase locks the service, replicas, and leases in canonical
order, revalidates the exact owner plus a compact replica-state fingerprint and
the route-safety fields of every lease, and uses a fresh database clock to
verify that route eligibility did not change while preparing. It publishes the
prebuilt result only if the prepared input is still current.
Successful readiness refreshes may monotonically extend an otherwise identical
lease without forcing a retry; revocation, readiness loss, material change,
record replacement, status/version change, or replica-state change rejects the
prepared result. No decoder, callback, cloud module, provider operation, or
fleet-sized JSON object construction runs while publication locks are held.
Both phases select only the bounded actionable replica set (`PENDING`,
`PROVISIONING`, `STARTING`, `READY`, and `NOT_READY`). Terminal and superseded
failure rows remain available through service status/history, but they are not
routing or admission capacity and cannot make route cadence proportional to
months of retained diagnostics. A transition into or out of that actionable
set changes the final locked fingerprint and rejects the prepared publication.

Revision 414 deployed PR #1536 dark and closed the composition cadence gate.
After controller takeover, approximately 40 consecutive refreshed route heads
landed 4.329--5.827 seconds apart while the controller independently reported
provider-health read timeouts. Exact-owner material briefly advanced from 57
to 130 rows; readiness caught up to 130/130 without delaying head renewal.
PostgreSQL sampling found no fleet-sized idle transaction: the observed
composer owner read held its transaction for roughly 6.7 milliseconds rather
than the 6--100 second revision-413 critical sections. No replica row was
created after the Helm upgrade, the fresh demand projection remained zero,
and no paid Spot launch was admitted. Route authority remains deliberately
`LEGACY_PROXY`; this evidence qualifies the implementation but does not skip
P2d or the documented promotion gates.

Revision 415 deployed P2d dark and exposed a separate controller-startup
complexity defect. The service retained 5,536 replica rows, including 38
logical-retirement rows and 160 current ready rows. Each retirement re-ran
`get_replica_infos()` and decoded the complete retained history twice during
its final safety check. A nonblocking production thread profile captured that
exact call chain in `_refresh_wait_for_idle`; the child remained near 106%
CPU, `/autoscaler/info` timed out, and one new load-balancer standby needed
roughly eight minutes to synchronize while its old peer continued serving.
The fix-forward uses the scalar `READY` projection as a conservative SQL
prefilter, decodes one shared ready-capacity snapshot per refresh pass, and
retains every existing decoded-state and exact occupancy check. Terminal
history therefore remains available for audit and status without participating
in destructive replacement-capacity proof. This changes database transfer and
deserialization from O(retirements x retained history) to one O(current ready
capacity) snapshot per pass; the small decoded snapshot is still checked for
each retirement. It introduces no timeout, cache, alternate authority path,
schema change, or behavior change.

The revision-415 durable demand sample was fresh but compatibility-incomplete:
31 accepted arrivals in 60 seconds (0.5167 requests/second), 36 confirmed
in-flight requests, zero queue, and 61 routed backends with unknown occupancy.
The UI can therefore display `36 confirmed processing` plus the coverage gap,
but exact processing correctly remains null until every routed backend has a
current occupancy proof. This also invalidates the older point-in-time claim
that production demand is currently zero; it does not authorize a paid launch
because the service still uses the legacy demand/controller path and the exact
zero-cost-first promotion gates remain open.

Later revision-415 samples also corrected the initial zero-launch observation.
While the service remained `DIRECT_REPLICA`, the legacy controller repeatedly
materialized H200 fill replicas and submitted `sky.launch` requests before
owning the P2d per-pool intent lane. Sampled replicas 54468, 54471, and 54473
requested `H200:1` on `phx_research_cluster_eks` with `use_spot=false`, the
exact protocol-2 H200 reserved-fill pool identity, and no paid claim. The new
durable fence rejected each request with `ReservedFillLaunchFenceError` and
the message that durable reserved-fill reconciliation owns exact cleanup. This
is fail-closed and proves no paid spill, but it is not the steady state: the
legacy path still creates speculative replica/request churn before rejection.
P2d promotion moves that acceptance boundary to the durable zero-cost intent;
the stacked P3 removal then deletes the direct broker-to-replica path.

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
policy remains correctly fail-closed. The exact ServiceAccount RBAC read is
now present, but the per-spoke audit IAM roles and EKS access entries are not.
The 2026-08-18 audit also invalidated the old scheduler premise: platform PRs
#8524/#8526/#8527 deliberately replaced PHX `gpu-binpack-scheduler` with Kueue
v0.19 topology-aware scheduling plus `default-scheduler`. Reinstalling the
custom scheduler would create a second placement authority. Policy schema v4
therefore binds the exact TAS feature gates and H200 ResourceFlavor
`topologyName: hyperpod` while retaining east's custom scheduler. Version 58
also retains task-owned Kubernetes
`pod_config`, `remote_identity`, and `provision_timeout` overrides, so the
strict worker-projection builder correctly persisted null rather than blessing
a competing Pod contract.

Revision 416 / v1.1.1321 deployed PR #1538's bounded logical-retirement scan.
The exact API image is
`sha256:19e9859eb32838287273d1cd39fd40075887304ffbafdd63fd9d4856ff41be61`;
controller health recovered from a five-second timeout to 0.159 seconds and
`/autoscaler/info` completed in 0.789--2.057 seconds. Both replacement load
balancer slots reached readiness on that image, in 130 and 24 seconds rather
than the roughly eight-minute pre-fix delay. This closes the retained-history
scan gate, but qualification exposed a distinct remaining authority bypass.

The unpromoted service is still `DIRECT_REPLICA`, and its committed version 58
has a null worker-placement projection. The legacy autoscaler emitted one
95-slot PHX H200 wave through `scale_up_batch()` using only the base
protocol-v2 physical fence. Requests 54491--54498 show the consequence: each
zero-cost launch reached Kubernetes without the projection-owned
`kueue.x-k8s.io/queue-name` label and failed the namespace admission policy
with HTTP 422. This was not paid L4 Spot activity. The singular `scale_up()`
entrypoint already rejects protocol-v2 dictionaries in favor of typed
`accept_reserved_fill(FillPlan)`, but the batch entrypoint retained a second
dictionary-shaped admission path. That inconsistency, rather than provider
scarcity, allowed null-projection service state to materialize rows, threads,
and API requests.

PR #1540 removed that second admission path and revision 417 deployed it dark.
Repeated legacy H200 broker decisions after the rollout created no replica
row, launch thread, or `sky.launch` request. The service remained on elected
version 58 and on legacy route/demand/ordinary-action authority plus
`DIRECT_REPLICA` fill; neither the service nor `boltz-platform` was changed.
The API, migration Job, and both load-balancer slots are healthy on the exact
revision-417 image with zero restarts.

The same qualification exposed a separate paid-authority defect in the legacy
controller. A 300-second aggregate downscale hold correctly retained a
40-slot safety floor, but its adopted exact-card state also retained stale L4
paid ownership. With only seven units of fresh compatible work and 112 ready
zero-cost A100/A100-80GB slots, the supply-aware actuator selected a 40-slot
target backed by existing supply. Its conservative reconciliation map
nevertheless retained 33 L4 slots while moving seven slots to A100. Two
retiring L4 rows then appeared as same-card shortages, and stale adopted paid
ownership authorized candidate replicas 54535 and 54536. The placer selected
one AWS and one GCP L4 Spot location. Both candidates were superseded and their
rows deleted before any `sky.launch` API request or provider launch, so this
was paid planning churn rather than two cloud instances. It still violates the
steady-state contract: a reconciliation-only exact-card hold cannot mint paid
authority after compatible supply has eliminated the economic residual.

The accepted fix keeps the aggregate and exact-card reconciliation fences but
bounds their combined current/adopted paid ownership by the global shortage in
the fresh supply-aware allocation. The bound applies to uncommitted shortages,
not absolute card targets. A genuinely under-capacity held target therefore
retains same-card retry authority, while surplus compatible zero-cost or paid
supply reduces paid authority to zero even if an older exact-card map remains
temporarily pinned for non-preemptive reconciliation. This is one additional
guard on the canonical supply-aware path; it introduces no second allocator,
fallback, timeout, or service-specific rule.

PR #1542 merged that guard after all 31 required checks passed, including the
complete unit-test shard. Revision 418 then deployed the exact merge artifact
and completed its migration successfully. The API and both `boltz-l4-fleet`
load-balancer slots converged to the exact image with zero restarts. Across
repeated post-upgrade autoscaler cycles, the public conservative card map still
held `L4: 6`, while observed scale-up decisions were protocol-2 zero-cost-only
H200/A100-80GB grants. With fresh demand at 14 recent requests, 0.2333
requests/second, four confirmed in-flight requests, and zero queue, the
replica watermark remained 54530: no row at or above 54531 and no
`sky.launch` request at or after the original 21:20 UTC audit boundary. The
adjacent under-capacity regression also proves that removing both compatible
materialized supply and its redundant paid source restores exactly one
same-card cold-launch authority rather than suppressing a legitimate retry.

Revision 408 made no authority promotion or service/config mutation. A
post-rollout query found zero paid Spot rows with `created_at` at or after the
09:10:10 UTC Helm upgrade, including after fresh request telemetry resumed.
The latest sampled report had six confirmed processing requests, two
occupancy-unknown backends, zero queue, and fresh but incomplete compatibility
coverage. This proves the release stayed economically dark while also proving
why the UI must display confirmed processing and unknown coverage separately.

The exact post-#1503 P2b2 review scope was 42 files with 4,298 insertions and
241 deletions relative to `improvements`. Production disproved a prerequisite
of the historical cleanup drafts #1506 and #1510; both are now closed and
superseded. They reserve no API or Serve heads and must not be restacked. Any
later deletion-only cleanup is re-derived from current old-path-use evidence
after P2c/P2d and receives a migration number only if its concrete diff needs
one. Serve054 belongs solely to the reserved-fill provider-proof receipt in
`serve-multi-pool-reserved-capacity-fill.md`.

Last updated: 2026-08-23

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

For a `DURABLE_INTENT` service, every positive plan is
`ALLOCATION_BOUND`: its content-hashed payload contains the canonical full
`ReservedFillAllocationIdentity` that was current for that reconcile. An
all-zero plan is instead an explicit `UNBOUND_ZERO_REVOCATION`, so revoking a
stale paid head never depends on retaining an allocation. Non-durable services
and elected immutable service versions with reserved fill disabled use the
explicit `NOT_APPLICABLE` mode. Publication and claim validation lock and
decode the server-owned current `version_specs.spec`; they never infer feature
enablement from the always-durable mechanism column or reparse task YAML.
These modes are exact: a positive fill-enabled plan without an allocation
binding, including a plan written before this field existed, fails closed after
durable-intent promotion; zero revocations cannot authorize a paid claim.

Positive plan publication, initial paid-claim admission, and every paid
provider-start validation linearize with allocation replacement under the
canonical lock order: protocol observation `SHARE`, lifecycle when applicable,
service `FOR UPDATE`, service-local rows, exact current-allocation validation,
then capacity and plan-head rows. This includes retained protocol-v1 effects
and replacement or recovery profiles whose locked funding is a paid claim;
direct-mode legacy claims retain their existing compatibility only until the
durable selectors activate. Validation uses
`read_current_in_connection()` in that transaction and requires the bound
identity to equal the complete current identity, including generation and
digest. If allocation replacement owns the protocol lock first, the old plan
or claim is rejected after the successor commits. If provider-start validation
owns it first, that already-validated effect may start and the writer follows.
The validator carries the minimum plan-head, route-head, demand-report, and
allocation-snapshot freshness horizon through request/queue and retention-pin
locking, then samples the PostgreSQL clock once more at the literal provider
boundary; time spent waiting on any downstream lock cannot outlive authority.
The controller publishes an unbound zero revocation and returns before
optimistic planning whenever a sequenced durable observation has no current
allocation, preventing a planning early-return from preserving an obsolete
positive paid head.

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
- `COMMITTED`: the executor atomically created the exact replica profile and
  intent-to-replica receipt while retaining the lane; generic non-pool request
  admission follows from that durable profile through the existing bounded
  association protocol;
- `RETRYABLE`: the lane was unavailable or the executor lost ownership before
  row creation, so no replica exists and the same intent may be leased again;
- `TERMINAL`: the grant expired, was superseded, or failed validation before
  materialization.

The same revision adds a per-service `DIRECT_REPLICA` /
`DURABLE_INTENT` actuation mode and monotonic epoch, plus a capability tuple
bound to the current controller incarnation and protocol 1. New binaries
advertise capability without changing the mode. Promotion is an explicit
PostgreSQL transaction that requires the current generic non-pool binding,
current protocol-2 reserved-fill authority, no direct admission in flight, and
the exact controller capability. It also requires every live API request
participant to advertise API-request-015 ordered-capacity protocol 2, proving
that paid admission debits pending intents during a rolling deployment. After
promotion, a missing or unavailable
intent repository fails closed; it never falls back to direct replica
materialization. This is the single transition boundary a later, re-derived
deletion-only cleanup must remove after current production evidence permits.

The per-pool executor leases one intent with PostgreSQL fencing, then acquires
the process provider phase and physical-cluster identity fence before it may
materialize a replica row. If either lane is busy, it returns the intent to
`RETRYABLE`; it must not allocate a replica ID or API request. Once row/action
creation commits, the existing generic non-pool admission and executor own the
request and all provider effects. A crash before the atomic row commit leaves
only a lease that can expire. A crash after the row commit but before request
admission leaves an exact durable generic profile that existing pre-admission
retirement can prove safe or a later association can adopt; a crash after
request admission has the existing exact association and termination evidence.
Executors for other physical pools proceed independently.

An unexpired `GRANTED`, `ACTUATING`, or `RETRYABLE` intent is pending zero-cost
capacity in the paid-residual transaction, bounded by the broker allocation
TTL. `RETRYABLE` is still an accepted grant and therefore cannot open a paid
deficit while waiting for its next lane lease. `COMMITTED` is counted through
the replica row, never both representations. This preserves no-paid-spill
without allowing a dead intent to suppress paid capacity forever. The
dashboard exposes intent counts separately from queued replicas, provider
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
   probe settings and probes URLs concurrently with the existing bounded HTTP
   timeout. Completed immutable exact-generation results enter one bounded,
   latest-result-per-target queue. One dedicated writer persists at most one
   bounded batch at a time in one PostgreSQL transaction; each stale or
   no-longer-eligible member is rejected independently without poisoning
   healthy siblings.
2. The compose stage first reads fresh leases plus current
   replica/service/version rows without row locks and closes that prepare
   transaction. It deserializes replicas and rebuilds the capacity hint and
   route payload outside every database transaction. A short publish
   transaction then locks service, replica, and lease rows in canonical order,
   exact-matches the prepared replica fingerprints and lease safety fields,
   and verifies at the current database clock that time-dependent route
   eligibility is unchanged before publishing or refreshing the prebuilt
   immutable full snapshot/head. Any changed safety input rejects that tick for
   a fresh retry.

The composition event loop never invokes or awaits readiness HTTP or receipt
persistence. Receipt persistence never creates one transaction or connection
per probe: it has one writer, one in-flight bulk update transaction, and a
bounded coalescing backlog. Exact-row conflicts retain PostgreSQL's normal
transactional serialization, but there is no application-level join or fleet
barrier. If PostgreSQL receipt persistence is slow, exact leases may expire but
the last valid head continues to refresh from independently readable state
until its own TTL; the worker does not assert readiness it failed to persist.

Provider material persistence also has one canonical bounded-batch path. It
locks the exact service owner once, reads and locks all matching current READY
replica rows in one statement, and reads their bounded lease histories in one
statement. It then revokes replaced record identities in one bulk update,
upserts every accepted material row in one bulk insert/update, and prunes
history with one ranked delete. Python may validate and construct the bounded
values documents between those statements, but it performs no database call
per replica. Lock ordering remains service, replica, then lease, matching route
composition and central replica mutations; therefore owner replacement,
replica-record replacement, and lease generation cannot race publication.
Stale or non-READY siblings are omitted independently, and a revoked row may
be recreated only through the existing explicit READY/route-allowed rule.

The readiness target reader performs one owner check and one lease-to-current-
replica join. It neither deserializes ReplicaInfo nor runs an identity query per
lease. A target that becomes stale after that read remains harmless because
the receipt bulk update repeats the exact owner, current READY replica,
material, readiness, and revocation predicates before changing the lease.
Composition retains one short provider-free owner/replica/lease publish
transaction, but no longer performs decoding or capacity callbacks inside it;
the fixed-statement material writer and optimistic composer together remove
both fleet-proportional critical sections that previously delayed renewal.

The worker never acquires the replica-manager lock, provider phase, physical
cluster fence, Kubernetes client, Ray handle, cluster database, or the shared
autoscaler/demand routing-epoch lock. Version and routing policy are published
to it as one immutable snapshot behind a dedicated constant-time lock; reads
copy that snapshot only after releasing the lock. Its cadence and head TTL are
fixed operational bounds; a worker stall still expires the head, but a provider
or cost-planning stall cannot. There is no all-fleet `complete` bit. A new
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
routing spec and ready set, then echoes them in its HA role and durable demand
reports. Standby promotion reconstructs its expected route/occupancy contract
from that exact durable snapshot and rechecks the same fresh head in the
STABLE-to-PREPARING transaction; controller restart never substitutes an empty
process-local cache for this contract. Occupancy probes capture the LB's
applied-route observation epoch at dispatch. Any change to service version,
projection generation/digest, or route-source epoch advances that observation
epoch and clears exported proof before the new route fence becomes visible;
only a probe that starts and finishes under the new epoch can repopulate it.
Thus a successor replica that reuses a URL cannot inherit either local capacity
or controller-facing idle proof from the prior immutable projection. An
identical head renewal does not advance the epoch or discard a current sample.
Future demand authority can therefore translate URL-keyed occupancy through
the exact immutable snapshot the reporter observed; a current URL is never
guessed to represent an older report.

The promotion reader joins the service-owner shared-lock queue for at most one
second, then copies the database clock, current head, and exact immutable
snapshot with one indexed join before releasing the lock and decoding. This
bounded wait prevents repeated short capacity-admission writers from starving
every role heartbeat; timeout remains fail closed and the next heartbeat
retries without disturbing the selected slot. STABLE-to-PREPARING
revalidates the same head under the exclusive service lock. The one-way route
promotion uses that lock too and, for an HA service, rejects every non-STABLE
phase. Therefore either route promotion wins while HA is STABLE and the next
legacy cutover CAS fails its mode check, or cutover wins and route promotion
waits then fails closed; PREPARING and MIGRATING cannot act on stale legacy
evidence.

Normal old-generation cleanup uses the same fail-closed principle without
making the load balancer or provider part of adjudication. A pre-service-job
reserved-fill failure may be declared already absent only when its exact
protocol-v2 intent, replica, association, any retained terminal request, and
`INTENT_PENDING` or `POLICY_ADMITTED` Kueue receipt remain mutually identical;
the executor is durably quiesced; canonical provider `ABSENT` was observed no
earlier than quiescence; no paid claim, queue row, or retention pin remains;
the replica is off-route and unmaterialized; and the current protocol-2
`SEQUENCED_ACTIVE` gate is strictly newer than its frozen generation. The
decision performs no provider read and no row deletion; existing replica
terminalization records cleanup success. Same-generation or incomplete graphs
stay unknown. Whole-service teardown retains its separate stronger batch and
deletion fences.

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

Revision 414 production qualification also separates the remaining request-UI
work from telemetry collection. The direct demand endpoint is fresh with two
reporters and currently returns zero recent arrivals, zero queue, zero
rejections, a confirmed in-flight lower bound of zero, and an exact in-flight
value of null because two routed backend URLs lack current occupancy evidence.
The request-history projection is available and contains 42 accepted arrivals
in the latest hour across 11 nonempty minute buckets. The existing `Requests
now` card is wired to both projections, but it must make `0 confirmed
processing; 2 backends unknown` visually explicit instead of allowing the
nullable exact count to resemble missing telemetry. Closing those two
occupancy gaps is required before the card may display exact zero processing.
Accepted arrivals/history are not a completed-request counter; if operators
need cumulative completions, the load-balancer report and PostgreSQL minute
history must carry an explicit idempotent outcome counter. The UI must never
derive completions from arrivals, request rate, or an in-flight delta.

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
The one exception is the replica-row cleanup DELETE itself, which is also an
idempotent no-op before its Serve051 table exists because there is no intent to
remove and it grants no teardown authority.

Local fix-forward verification on 2026-08-17 passed all 293 affected tests on
PostgreSQL 14, including the pre-Serve051 schema hooks, the full route and paid
retirement suites, reserved-fill broker races, system-recovery persistence,
resource-action state, launch authority, and capacity observations. The final
remote Python 3.14/PostgreSQL run passed 17,471 tests, one expected failure, 199
subtests, and no failures before PR #1531 merged.

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
route-projection, incremental-worker, request-aggregator, migration utility,
and real-PostgreSQL suites. `format.sh` passed mypy over 953 source files,
changed-file pylint at 10.00/10, dashboard ESLint, and dashboard formatting.

The historical additive stack linked draft #1506 as its removal. That draft is
now closed and superseded and reserves no schema head. After one complete
production stale horizon with protocol 2 as the sole selected writer, re-derive
the smallest deletion-only cleanup from current source and old-path-use
evidence.

### P2d: grant-before-row zero-cost actuation

Serve052 adds the actuation-intent relation and per-physical-pool executor
defined above. The existing broker remains the sole allocation planner, and
the generic non-pool executor remains the sole owner after a replica/action
commit. The old broker-to-manager direct row materialization remains only for
services outside the protocol-2 cohort during rollout.

Implementation is split into two reviewable commits in one PR stacked on P2c:

1. intent schema/repository, idempotency, TTL, state reducer, and paid-residual
   debit; and
2. API-request-015's fleet barrier, broker publication without replica
   allocation, per-pool leasing, provider/physical-fence admission, atomic
   intent-to-replica commit, restart recovery, and observability/UI status.

The earlier four-commit estimate was collapsed because broker publication,
the API015 barrier, and execution are one fail-closed authority change: no
intermediate commit should advertise or admit a durable grant without both its
paid-capacity debit and its executor. The schema/repository remains an
independently reviewable first commit; the second is the complete dark path.

Estimated size is 1,800--3,400 source/test additions across 22--38 files and
one Serve plus one API-request migration. Required tests hold one pool's
provider phase indefinitely
and assert repeated broker rounds create one intent and zero replicas/requests,
while another pool materializes normally. Crash injection covers every state
boundary. PostgreSQL tests prove a pending intent debits paid residual exactly
once, expiry releases it, `COMMITTED` transfers accounting to the replica, and
reserved fill can never select Spot or On-Demand.

The implemented diff is approximately 3,000 additions across 30 files over the
two commits, including tests and the canonical design. The focused P2d surface
passes locally: 13 real-PostgreSQL ledger/API-fleet tests, 81
manager/broker/binding tests, and 129 dashboard tests. The PostgreSQL suite
includes lost-response promotion idempotency and atomic intent-to-replica
accounting transfer; the manager suite includes a held east-pool lane while a
west-pool intent materializes independently. All 145 API-request PostgreSQL
tests and all 31 capacity-admission/refill tests pass against the new schema
heads as well.

After every non-pool service uses the intent executor and the direct-path usage
counter remains zero for the documented horizon, a new deletion-only cleanup
must remove direct broker-to-replica materialization and the transition
branch/tests. It must be derived from then-current source and name the same
exact production evidence gate; closed #1506 is not revived.

The post-revision-416 fix-forward closes the remaining untyped protocol-v2
batch bypass. `accept_reserved_fill(FillPlan)` is the only public manager
admission for protocol-v2 reserved fill in both transition modes;
`scale_up()` and `scale_up_batch()` reject dictionary-shaped v2 fill entries
before provider admission, replica-row allocation, launch-thread creation, or
API submission. Ordinary demand, cost rebalance, and protocol-1 compatibility
entries retain their existing batch behavior. This is a path deletion aligned
with P3, not a new fallback: a service without a complete immutable worker
projection receives no v2 fill until sequenced allocation can construct a
typed plan. Mixed batches fail the v2 entries closed without suppressing
unrelated ordinary entries, and tests prove the rejection is side-effect free.
The implemented change deletes the context/UID conflict shim and the
provider-busy batch exception that existed only for the bypass. Local Python
3.14 verification passes the complete reserved-capacity-fill suite, the
durable manager-receipt suite, and the replica-manager scale-up-batch/protocol
v2 selection. The repository formatter passes YAPF, isort, mypy over 957 source
files, pylint at 10.00/10, dashboard ESLint, and dashboard formatting.

### P3: blocked steady-state cleanup

Closed PRs #1506/#1510 are superseded and reserve no schema heads;
API013/API014 remain owned by G1Sb/G1Se executor-termination evidence. After
the documented rollout gates, re-derive and adversarially review one smallest
current-source deletion stack to remove
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
  `Never`), Kubernetes `default-scheduler`, Kueue v0.19 topology-aware
  scheduling with H200 ResourceFlavor `ml.p5e.48xlarge` and
  `topologyName: hyperpod`, and physical cluster UID
  `ba2dcdca-2a0d-447f-ad8a-31849a63c1d5`.

Production uses PostgreSQL-backed API-server configuration; the mounted
`skypilot-config` ConfigMap is intentionally `{}`. Snapshot and hash the full
current config, then use the audited workspace-config update path to add the
server-owned `mt_hybrid` PHX `kueue.local_queue_name: be` and
`serve_worker_kueue_workload_priority_class_name: be-ls`, together with the
complete server-owned priority, service-account, scheduler, accelerator,
cache, and scratch projection inputs. Do not seed or patch the database out of
band. Revision 408 already corrected the embedded Boltz policy bundle and
deployed it dark. The exact ServiceAccount read is now present. The remaining
identity prerequisite is creation of the two narrowly scoped cross-account
audit roles, their EKS access/RBAC, and the hub controller's exact
AssumeRole/TagSession permission. Policy schema v4 must then attest PHX's
existing Kueue TAS/default-scheduler contract; no custom scheduler is
installed. Only after both attestations pass may the audited config transaction compile/elect
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
- allocation-bound positive plan publication, explicit unbound zero
  revocation, pre-binding durable-plan rejection, successor-allocation races,
  and provider-start protocol-lock linearization;
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
9. readiness, +10, +30, and one complete stale/quiescence horizon before P3;
10. revision 415's exact API89/Serve052 artifact tuple, migration completion,
    two ready fleet LB slots, zero restarts, unchanged `DIRECT_REPLICA` mode,
    an initially empty post-upgrade intent/replica/request sample, and later
    fail-closed legacy H200 direct-launch churn with no paid claim; and
11. after the ready-snapshot fix-forward, bounded controller startup and
    `/autoscaler/info` latency with the retained 5,536-row history and 38
    simultaneous logical retirements, without deleting audit rows.

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

The post-revision-411 receipt-path review rejected three further partial
fixes. Sending every synchronous receipt to the default executor preserves one
queued task, connection checkout, and transaction per replica and merely moves
the burst into the shared executor pool. Batching the existing Python loop on
the asyncio thread still makes head cadence depend on receipt I/O. Moving that
loop to one thread removes the event-loop stall but holds owner and lease locks
across hundreds of round trips, recreating a composition barrier in
PostgreSQL. The accepted path has one dedicated writer, one coalescing
exact-target backlog, and one `UPDATE ... FROM (VALUES ...)` statement whose
owner, current replica, material, and revocation predicates reject stale
siblings independently. The single writer bounds connection use, the bulk
statement bounds row-lock time, and composition has no application-level
dependency on receipt completion.

The post-revision-412 material-path review rejected three more partial fixes.
Removing only the probe-target N+1 query leaves the provider writer holding the
service owner across fleet-proportional SQL. Committing one material row at a
time shortens each lock hold but permits partial provider generations and
multiplies connection/transaction pressure. Dropping the owner and current-
replica locks in favor of a pre-write existence check permits an owner or
replica replacement transaction to revoke the old set and then lose a race to
a stale insert. The accepted fixed-statement batch retains one short ordered
transaction and exact current-row locks, while eliminating all per-replica
database round trips. This is the same canonical material path for a single
entry and a fleet batch; no compatibility branch or timeout adjustment is
introduced.

The post-revision-413 composition review rejected three final shortcuts.
Preloading Kubernetes modules merely hides the observed import once and leaves
all future decoder or capacity callback work inside the transaction. Removing
`FOR UPDATE` without revalidation permits a draining or replaced replica to be
published after its revocation commits. Moving the same locked callback into a
new thread or process preserves the PostgreSQL critical section even if it
isolates the controller GIL. The accepted two-phase path has one lock-free
prepare snapshot and one short ordered publish transaction. Replica-state
fingerprints reject any changed durable capacity input, while lease validation
allows only a monotonic TTL extension of otherwise identical readiness and
material evidence. The final locked validation uses a new database timestamp
and rejects any route whose temporal eligibility changed, so a lease cannot
cross expiry during preparation and then be republished as fresh. All route
JSON construction remains outside the locked transaction.

Local verification for the receipt-writer fix-forward passes the incremental
worker, route-projection, controller, controller-event-loop,
controller-respawn, and route-lease suites, plus the exact formatter, mypy,
pylint, and dashboard checks. The receipt bulk statement was also executed
against the production PostgreSQL dialect and schema inside an explicit outer
transaction: one stale sibling was rejected, one current sibling was accepted,
and the transaction was rolled back.

The material-batch candidate passes the focused route-projection and
incremental-worker suites, mypy, and pylint. Its exact module was loaded beside
the deployed module and executed against all 74 current production material
rows in an explicit outer transaction with five-second lock and 15-second
statement ceilings. It accepted 74 duplicate materials, used six statements
for the whole material batch and two for all probe targets, completed in 3.198
seconds while sharing locks with the legacy per-row live writer, and was rolled
back. The real-PostgreSQL tests additionally cover fixed statement count,
stale/revoked sibling isolation, and a replica-replacement lock race. Full
remote PostgreSQL CI remains a merge gate.

The merged two-phase composer passes the complete real-PostgreSQL route
projection suite plus exact formatting, mypy, and pylint. Focused concurrency
regressions acquire the service and replica row locks with `NOWAIT` while the
capacity callback is deliberately suspended, reject a replica-state mutation
between prepare and publish, accept only a monotonic successful readiness
refresh whose temporal eligibility stays unchanged, reject an expired route
that becomes eligible during preparation, and prove retained terminal replica
history is never decoded or counted as admission capacity. PR #1536 passed all
31 required GitHub checks, including the complete unit-test shard.
The revision-414 production cadence qualification described above closes its
dark deployment gate.

## Open gates

- [x] Verify the v1.1.1296 production compatibility baseline and the later
  revision-402 binary-only regression without generalized-action,
  demand-authority, or placement promotion (2026-08-16: single-`all`
  `Recreate`, forward Serve047/API-request-011 heads, service resource-action
  and binding modes legacy, non-pool capability false).
- [x] Historical rollout: merge P1, P2a, and P2b1 as PRs #1498, #1499, and
  #1503; publish P2b2 as PR #1504; and prepare then-draft cleanup PRs
  #1506/#1510. Those cleanup drafts were later closed and superseded.
- [x] Historical rollout: complete the then-current G1S cleanup diff and its
  remote CI plus the paired local real-PostgreSQL suite. The old #1506/#1510
  drafts are closed, reserve no heads, and are not deployment candidates.
- [ ] After P2d and the production horizon, re-derive and adversarially review
  the smallest deletion-only cleanup from current old-path-use evidence. Do
  not revive closed/superseded #1506/#1510 or reserve schema heads
  speculatively.
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
  demand stayed fresh. The historical #1506 draft remained undeployed and was
  later closed/superseded.
- [x] Merge P2c API88/Serve051 after remote real-PostgreSQL CI and final exact
  diff review, update the then-maintained historical cleanup diff, and deploy
  it dark as direct Helm revision 409 / v1.1.1313.
- [x] Merge and deploy PR #1532's exact-owner route-producer bootstrap
  fix-forward as direct Helm revision 410 / v1.1.1314. The owner selected
  protocol 2 and wrote 149 exact material rows, but the first head expired and
  all readiness observations remained null because the worker waited on the
  autoscaler routing-epoch lock.
- [x] Merge PR #1533's independent immutable route-contract fix-forward and
  deploy it as direct Helm revision 411 / v1.1.1315. It removed the routing-lock
  dependency, but production exposed synchronous per-probe PostgreSQL receipt
  work on the composition event loop; revision 411 remains unpromoted.
- [x] Merge and deploy PR #1534's bounded batch receipt writer as direct Helm
  revision 412 / v1.1.1316. Production proves receipt persistence is off the
  event loop and all 64 current rows become fresh, but a provider material
  refresh still produced a 16.987-second head interval.
- [x] Merge and deploy PR #1535's fixed-statement
  material-batch/probe-target-join fix-forward as direct Helm revision 413 /
  v1.1.1317. All 121 materials converged, but production found the composer
  idle inside its locked transaction while decoding replica state; sampled
  head intervals reached 100.433, 12.675, and 17.406 seconds.
- [x] Merge PR #1536 and deploy the two-phase optimistic composer as direct
  Helm revision 414 / v1.1.1318. Controller takeover plus provider-health
  timeout stress retained approximately 40 consecutive 4.329--5.827-second
  renewals, converged 130/130 current material/readiness rows, and admitted no
  post-upgrade replica or paid launch. Route authority remains dark pending
  P2d and the combined promotion gates.
- [x] Merge P2d Serve052/API-request-015 as PR #1537 and deploy it dark as
  direct Helm revision 415 / v1.1.1320. API89/Serve052 are current; both fleet
  LB slots and the API use the exact image with zero restarts; the service
  remains `DIRECT_REPLICA`; the initial post-upgrade query found zero intents
  and zero new replicas or `sky.launch` requests; and later legacy H200
  direct-launch attempts were rejected by the new durable fence without a paid
  claim. Promotion and the stacked direct-path removal remain open.
- [x] Merge and deploy PR #1538's logical-retirement ready-snapshot
  fix-forward as revision 416 / v1.1.1321. With 38 simultaneous retirements
  and 5,536 retained rows, controller health recovered to 0.159 seconds,
  `/autoscaler/info` to 0.789--2.057 seconds, and the replacement LB slots to
  130 and 24 seconds. The service remained unpromoted on `DIRECT_REPLICA`.
- [x] Close the batch-only untyped protocol-v2 fill bypass in PR #1540, deploy
  it dark as revision 417 / v1.1.1323, and prove repeated legacy H200
  decisions create zero new replica rows, launch threads, or `sky.launch`
  requests while ordinary batch entries remain unaffected.
- [x] Merge and deploy the supply-aware paid-residual bound. Reproduce the
  live 40-slot hold with 31 paid L4 plus 112 compatible zero-cost A100/
  A100-80GB slots; prove the actuation map may retain its conservative card
  fence while cold paid authority and post-rollout `sky.launch` requests stay
  zero. Also prove a genuinely under-capacity held same-card target still
  retries within its existing wave budget.
- [x] Replace the stale revision-407 PHX deployment-policy identities with
  LocalQueue `be`, ClusterQueue `skypilot-be`, WorkloadPriorityClass `be-ls`,
  and service account `skypilot-pool-sa`; deploy the correction dark in
  revision 408.
- [ ] Create the exact east and PHX cross-account audit roles, EKS access/RBAC,
  and hub source permission; deploy policy schema v4 and prove east's custom
  scheduler plus PHX's Kueue TAS/default-scheduler topology contract without
  weakening any check. The direct ServiceAccount read is already proven.
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
