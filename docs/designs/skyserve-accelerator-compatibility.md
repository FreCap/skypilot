# SkyServe exact-accelerator compatibility, priority, and per-card capacity plan

_Created: 2026-07-19. Updated: 2026-08-07._

## Decision summary

Add one compatibility-aware SkyServe queue. Each request may carry a subset of the exact accelerator identifiers configured by the service. The subset constrains where the request may run; it is not a preemption instruction and it is not a hard preference for the first card in the list.

Scheduling and scaling follow these rules:

1. Never preempt or migrate an admitted request.
2. Numeric request priority remains the primary queue order (`high = 50`, `low = 20` in boltz-platform).
3. At equal numeric priority, make a supply-aware assignment that maximizes immediate admissions and protects the request with the worst realistic fallback; FIFO breaks a true fallback tie. Raw compatibility-set size is not a sufficient ordering rule.
4. A request uses already-ready compatible capacity before causing a scale-up,
   even if that ready capacity is a larger card. Among otherwise-valid ready
   assignments, prefer reserved/zero-cost replicas before paid replicas. This
   routing choice does not reattribute flexible traffic demand to the serving
   card: the per-card demand target remains on the cheapest compatible card.
5. Demand attribution and launch suppression are separate calculations. Assign
   each flexible demand unit to the cheapest compatible card first. Then clamp
   its cold-launch authority with all compatible healthy ready, provisioning,
   and free reserved supply, regardless of which card currently supplies it.
   This counts the request once, keeps a ready A100 eligible to serve it, and
   avoids both an A100 demand target and a duplicate L4 launch. A retained
   per-card target is not authority to replace disappeared warm capacity on
   that same card: every cold launch must still be justified by the current
   compatibility profiles, floors, pinned work, and supply snapshot.
   The logical reconciliation target and paid cold-launch authority are also
   separate controller-to-manager fields. The former describes the capacity
   mix to converge toward; only the latter permits a paid launch.
6. A missing compatibility field means every exact accelerator configured for the active SkyServe service version is compatible.
7. Global demand target, hard per-card serving-replica floors, and optional reserved-fill targets remain separate control-plane signals. With reserved fill enabled, every fresh broker-granted slot is launched independently of demand while total live and planned capacity remains below the hard `max_replicas` ceiling. The UI shows all three signals.
8. `A100` and `A100-80GB` are distinct identifiers in validation, queue indexes, metrics, APIs, placement, tests, and UI. Matching may be case-insensitive, but it must never use family, prefix, regex, or memory-suffix normalization.
9. Controller startup must not perform provider feasibility or pricing lookups
   while configuring exact-card order, seeding reserved-capacity identities,
   or adopting recovery claims.
   Kubernetes locations are already classified as zero-cost when the placer is
   constructed, and any paid prices already known to the placer may be read
   directly. If the paid price map is incomplete, startup preserves the
   explicit service resource order. Candidate-set growth therefore cannot
   prevent the controller from binding its health endpoint.
10. Load-balancer sync reads the cached zero-cost identity set and the cached
    reserved-capacity observations, with all PostgreSQL reads dispatched off
    the FastAPI event loop. Autoscaler decisions taken under the logical-state
    lock also read only cached paid costs. Neither path may warm paid-provider
    costs or make the controller health endpoint wait behind a catalog-wide
    price scan or a blocking database read.

The priority rule deliberately means that a flexible priority-50 request remains ahead of a constrained priority-20 request. Within the same numeric priority, however, an `A100`-only request has no fallback and therefore gets the next A100 slot ahead of older flexible `L4/A100/H100` work. This preserves the existing strict-priority contract while protecting scarce-card access among peers.

## Baseline and scope

- SkyPilot merge baseline: `boltz-bio/skypilot` `origin/improvements` at `33074d9e0995028104e711119a5e4d152762a769`.
- The implementation was synchronized again on 2026-07-21 with
  `origin/improvements` at `ae8388ccdb49fc133b3345feb63ca8584d4d63a7`.
  This includes the production-calibrated simulation runbook from PR #740,
  the bounded consecutive downscale-veto fix from PR #744, and logical versus
  reserved-fill history from PR #748. It also includes demand-independent
  reserved fill, consumed bench-retry admission, one-snapshot pool scheduling,
  and off-event-loop autoscaler status serialization.
- PR #748 owns Serve database revision `020`. Exact-accelerator autoscaler
  history therefore uses revision `021`, with `020` as its predecessor. This
  preserves a linear PostgreSQL upgrade path for installations that already
  ran the capacity-mode migration.
- The aggregate one-minute PostgreSQL contract in
  `docs/designs/serve-autoscaler-history.md` remains authoritative. This
  design extends each aggregate sample with exact-card maps; it does not add
  another history writer or dashboard time range.
- boltz-platform integration baseline: `boltz-bio/my-full-stack` PR branch `feat/skyserve-request-priority-header` at `3d6df7a48d68f90cc603f585b9bb1537c8a17fa3`.
- Existing SkyServe request priority, process-local admission queue, instance-aware least-load policy, exact `replica_info.gpu_type`, targeted resource override support, and reserved-capacity broker are extended rather than replaced.
- Existing HA behavior remains: one active load-balancer authority owns queue/admission state; clients retry across an authority change. Queue durability across an LB failover is not introduced by this project.
- This plan covers SkyServe and the boltz-platform request path. It does not add preemption, priority aging, a persistent distributed queue, or per-card maximums.

## Implementation and production changelog

This table is the canonical chronological index of landmark accelerator
changes. A change to the contract must update this table in the same PR. Keep
merge, release, and deployment state distinct; a merge or image publication is
not evidence that the behavior is active in production.

| Release | Change | Result | Production state |
|---|---|---|---|
| `1.1.623` | PR #783, demand-independent reserved fill | Fresh broker-granted reserved slots became zero-cost-only launch intent, independently of traffic demand and below the hard maximum. | Included in deployed `1.1.704`. |
| `1.1.635` | PR #628, exact accelerator compatibility | Added the compatibility header, one compatibility-aware priority queue, exact-card validation and routing, per-card autoscaling, history, and dashboard surfaces. | Included in deployed `1.1.704`. |
| `1.1.641` | PR #800, restart reconciliation | Rebuilt exact-card targets safely after controller restart instead of falling back to an aggregate-only launch signal. | Included in deployed `1.1.704`. |
| `1.1.648` | PR #807, legacy zero-cost history | Conservatively attributed pre-marker reserved rows so history and restart accounting did not invent paid demand. | Included in deployed `1.1.704`. |
| `1.1.652` | PR #813, exact-card fill shelter | Kept zero-cost fill and retirement accounting exact for `A100`, `A100-80GB`, and every other configured identifier. | Included in deployed `1.1.704`. |
| `1.1.656` | PR #814, demand-only paid backfill | Removed reserved-fill rows from traffic backfill and paid replacement authority while preserving their ability to serve compatible work. | Included in deployed `1.1.704`. |
| `1.1.667` | PR #827, compatible warm-capacity safety | Prevented warm-only compatible cards and restart hints from creating unsupported cold launches; hardened retirement and failover behavior. | Included in deployed `1.1.704`. |
| `1.1.686` | PR #844, exact-card arrival floor | Preserved offered-arrival pressure in exact-card scaling when queue and in-flight gauges changed between samples. | Included in deployed `1.1.704`. |
| `1.1.688` | PR #846, flexible in-flight overflow | Reassigned flexible overflow to compatible supply and stopped multiplying one in-flight request across card targets. | Included in deployed `1.1.704`. |
| `1.1.691` | PR #850, card-mix downscale hysteresis | Prevented harmless compatible-card reshuffles from resetting the aggregate downscale delay. | Included in deployed `1.1.704`. |
| `1.1.698` | PR #858, simulation runbook | Defined the production-data replay boundary and kept modeled placement separate from live provider and billing truth. | Documentation remains current. |
| `1.1.700` | PR #860, cold-launch authority visibility | Exposed the exact per-card signal that can create cold capacity, separately from demand, warm retention, and reserved fill. | Included in deployed `1.1.704`; the field remains the launch audit surface. |
| `1.1.702` | PR #862, separate demand from actuation | Attributed flexible unmet demand to the cheapest compatible cold card, then independently adopted ready, provisioning, and free reserved compatible supply for actuation. | Included in deployed `1.1.704`; this remains the active allocation contract. |
| `1.1.703` | PR #863, response-time history | Added full HTTP completion history without changing placement. | Included in deployed `1.1.704`; later superseded by prediction-time history. |
| `1.1.704` | PR #864, bounded paid placement cohorts | Limited unresolved fresh paid launches to four per exact paid location by default, spilled later probes to the next-cheapest eligible location, and kept zero-cost fill outside the paid cohort. The detailed subdesign is `docs/designs/serve-paid-placement-cohort.md`. | Deployed 2026-07-22 as Helm revision 191. Initial post-deploy samples through 15:21 America/New_York found no active A100-class placement outside the fixed reserved research cluster; every pending A100-class launch was reserved, zero-cost Kubernetes fill, L4-compatible demand remained assigned only to L4, and A100-class cold-launch authority remained zero. An automated five-minute watch remains active through 03:00 America/New_York. |
| `1.1.721` | PR #877, reserved rollout no-paid-spill | Prevents broker-reported but unmaterialized free A100-family slots from moving L4 demand into A100-family rollout actuation. Mixed-version rollouts preserve the adopted compatibility-owned card map; reserved fill remains independently zero-cost-only. | Included in deployed `1.1.726`. Production then exposed a separate catalog-ordering edge case when a zero-cost-only A100 preceded paid L4. |
| Unreleased | PR #1303, generation-aware vanished-card release, plus tri-state provenance hardening | Splits an adopted card with live old-version backing from one whose capacity is absent from every generation. A complete provenance snapshot permits only the latter to move toward explicitly owned compatible placement before the mixed-version rollout guard; an unproven vanished unit receives no paid same-card authority. The follow-up makes unknown provenance preserve the adopted map instead of treating it as known empty. | PR #1303 merged 2026-08-06; the hardening is included in this follow-up. Neither change is deployed; both await the next control-plane release. |
| Unreleased | PR #1304, mixed-version compatible replacement authority | On a fresh, complete, non-downscale logical tick with explicit cross-card compatibility proof, recomputes the supply-aware latest-version target even when adopted units are still backed by old-version rows. Old rows continue fencing nonpreemptive retirement, but do not select the paid replacement card. The manager consumes explicit paid authority using typed launch funding provenance. | Merged 2026-08-06 and covered by unit and cluster-free incident reproduction tests; production deployment remains pending. |
| Unreleased | Owned Serve interface normalization | Materializes every supported legacy `ReplicaInfo`, `ReplicaStatusProperty`, and `SkyServiceSpec` default at its persistence boundary, and gives autoscalers, managers, controllers, and load balancers complete runtime state at construction. Policy, admission, recovery, and accounting code use those declared fields and methods directly. A malformed current-version object fails loudly instead of being silently reinterpreted through a local `getattr()` default. | Implemented in this follow-up; production deployment remains pending. |
| Unreleased | Explicit placement contract | Resolves engine, replica unit, catalog expansion, price unit, reserved-fill mode, and workload kind once at the service-spec boundary, then runs physical and per-GPU policies through one dynamic engine. `dynamic_fallback_per_gpu` is the primary new GPU-concurrency policy; the public physical preset remains for pools. The canonical subdesign is `docs/designs/serve-explicit-placement-contract.md`. | Transition and blocked steady-state cleanup are implemented locally. Cleanup removes fieldless and historical physical/per-GPU contracts only after measured inventory/version gates pass. Control-plane rollout requires reviewed release artifacts and a Platform pin; the separate service-policy update remains blocked until its canonical scale-to-zero spec is corrected, applied, converged, and drained. |
| Unreleased | Preserve exact cards during downscale-held retries | Keeps the held part of an adopted exact-card target on its prior cards while the request queue is briefly empty. Fresh remaining demand may still change its own card assignment, but the held portion cannot turn an L40S retry into an L4 cold launch. | Required after the 2026-07-27 `clin-structure-eval-6f51471-l40s-v8` acceptance run selected L4 while reporting an exact `{"L40S": 1}` target. |
| Unreleased | Reserved-only card paid fallback | Excludes cards whose every successfully priced location is zero-cost from flexible cold-paid ordering. Paid-capable and unpriced cards keep the all-or-nothing service-order fallback, while exact demand can still target a reserved-only card. | Required before the next `opendde-10c200s-v4` rollout so default-all demand selects paid L4 instead of waiting on the reserved-only A100 location. |
| Unreleased | Centralized placement catalog | Materializes every exact location and nominal cost once per immutable service version, persists the complete catalog in PostgreSQL, backfills legacy versions before controller-child spawn, and removes the old partial-cache accessors and fallback feasibility resolver. | Supersedes the bounded partial-cache fix for the July 23 `boltz-l4-fleet` and `boltz-l4-fleet-test` controller startup failures. The canonical subdesign is `docs/designs/serve-central-placement-catalog.md`. |

### Controller startup liveness

Reserved fill needs the zero-cost location identities before the first
autoscaler tick so a recovered controller cannot mistake live fill capacity
for surplus and terminate it. This startup seed is identity-only: it grants no
free slots and records no fresh capacity snapshot.

The service parent constructs and commits a complete versioned placement
catalog before it spawns the controller child. Legacy versions are backfilled
with a compare-and-set write before child spawn. The child loads that
PostgreSQL record and must never enumerate providers, resolve feasibility, or
price a location because a catalog entry is absent. A missing catalog is a
startup invariant failure, not permission to reconstruct one in the child.

Controller startup, claim recovery, load-balancer sync, reserved-capacity
polling, placement, cold-paid card ordering, and cost rebalancing all consume
the same complete in-memory catalog. The poller's free-capacity observations
remain transient runtime data and do not alter the immutable location/cost
catalog. A location whose nominal price is unavailable carries infinity in
memory and JSON `null` durably; the all-or-nothing ordering rule preserves
explicit service order for any affected paid-capable card.

Regression coverage must include a large candidate set and prove that child
construction, pre-bind exact-card configuration, startup seeding, claim
adoption, load-balancer sync, autoscaler ordering, and cost rebalance never
call resource feasibility or pricing code. Recovery must reuse the persisted
bytes; one legacy parent backfill may construct the catalog once. Production
rollout verification requires the exact deployed commit, migration 028,
non-null catalog data, healthy controller endpoints for the one-replica canary
and production fleet, successful load-balancer syncs, and continuity of
pre-existing ready replicas.

The dashboard's provisioning count is not itself a paid-capacity signal. For a
launch audit, combine `cold_launch_authority_by_accelerator` with the durable
replica location, `reserved_fill`, and `is_zero_cost` provenance. Provider
inventory is the final billing check. A nonzero A100 or A100-80GB provisioning
count is expected while the reserved research cluster has granted empty slots;
it is not evidence of a paid cloud launch.

The manager must not use a subsequently appended `ReplicaInfo` row as the
source of launch-budget accounting. Its placement/persistence seam returns an
explicit typed launch result containing planned capacity and funding
provenance. A `PAID` result debits paid cold-launch authority by that capacity;
a `ZERO_COST` result does not. Persisted `ReplicaInfo.is_zero_cost` remains the
durable operational provenance used for routing, history, and audits, not an
implicit success-channel side effect. Policy code reads that typed field
directly. Pre-v11 pickle migration and JSON decoding materialize the boolean at
the record boundary; downstream admission, allocation, recovery, and ordering
must not carry independent `getattr(..., False)` fallback paths.

The same boundary owns every persisted replica field used by Serve policy.
`ReplicaInfo.__setstate__()` and `ReplicaInfo.from_storage_dict()` materialize
the complete current `ReplicaInfo` and nested `ReplicaStatusProperty`
interfaces, including conservative defaults for pre-v8 logical width, pre-v9
unknown-capacity replacement, pre-v10 bridge verification, additive fill and
paid-capacity provenance, and retirement state. Missing
`logical_retirement_committed` remains the one deliberate tri-state migration:
it decodes as `None`, while a newly constructed record uses `False`. Outside
those decoding/migration seams, consumers access declared fields directly.
Tests and mocks must implement the real interface; they must not cause
production code to grow attribute-existence fallbacks. A current-version object
with a deleted required field is malformed and must raise rather than acquire a
policy default.

The same rule applies to non-record Serve objects. `SkyServiceSpec.__setstate__`
owns persisted-spec compatibility; normal properties never probe whether their
backing fields exist. Autoscaler, replica-manager, controller, spot-placement,
and load-balancer constructors initialize their complete shared runtime
interfaces, including neutral values for capabilities that only some concrete
implementations populate. Shared code calls declared methods and reads declared
fields directly instead of using reflection as a capability test. Reflection
remains appropriate only at explicitly dynamic integration boundaries, such as
Kubernetes client models, HTTP request/client/response metadata, SQLAlchemy
column namespaces, and deliberate schema-field iteration.

### Production operating point

The initial `boltz-l4-fleet` configuration remains:

```yaml
request_queue:
  size_per_replica: 10
  max_size: 10000
  timeout_seconds: 20
  timeout_seconds_by_priority:
    - min_priority: 0
      timeout_seconds: 600
    - min_priority: 50
      timeout_seconds: 60
replica_policy:
  target_concurrency_per_replica: 1
  target_utilization_percentage: 90
  expected_request_duration_seconds: 30
  max_scale_up_rate_percentage: 20
  scale_up_rate_min_replicas: 10
  scale_up_rate_period_seconds: 60
  adaptive_scale_up:
    max_scale_up_rate_percentage: 100
    scale_up_rate_min_replicas: 50
    pressure_observations: 2
    hold_seconds: 120
  downscale_delay_seconds: 300
  max_scale_down_rate_percentage: 50
```

A post-version-36 production window contained 23,022 SkyServe attempts, one
SkyServe rejection, and no platform spill from SkyPilot. During the final
burst, the adopted target rose to 256 and then drained to 128 while raw demand
was 15, with the next bounded step continuing toward ready reserved capacity.
The temporary high target was the expected two-veto, five-minute hysteresis
tail, not an indefinitely pinned fleet.

Gauge replay rejects increasing `expected_request_duration_seconds` to 60 or
90 because it materially increases paid target pressure without an observed
service-quality deficit. A minute-level replay cannot reproduce sub-minute
pressure latches, priority buckets, request compatibility, or provider
placement, so its candidate cost rows are advisory under
`serve-autoscaling-simulation.md`. No queue, duration, utilization, or
downscale configuration change is justified until the exact implementation is
deployed and a held-out trace includes priority and compatibility dimensions.
The 30-second duration remains a pressure-conversion horizon; it is not an
estimate of end-to-end request runtime because live in-flight work is measured
directly.

### Demand-independent reserved-fill increment

The 2026-07-21 base increment corrects the aggregate reserved-fill overlay and
is part of this exact-card design:

- A fresh spendable reserved slot is launch intent, not merely a target to
  compare with traffic demand. The autoscaler emits one zero-cost-only launch
  for every broker-granted slot that fits under `max_replicas`, even when paid
  replicas already satisfy a larger demand target.
- The hard-ceiling budget counts every nonterminal old-version replica plus the
  greater of latest-version nonterminal capacity and the latest demand target.
  This reserves room for ordinary demand launches and prevents rolling-update
  rows from hiding physical occupancy.
- At `max_replicas`, fill waits for normal autoscaling or lifecycle transitions
  to create headroom. It does not overlap replicas or evict paid capacity.
  Generic `cost_rebalance` remains a separate paid-to-paid policy. It never
  selects zero-cost candidates or incumbents while fill is enabled; the broker
  exclusively owns paid-to-free convergence.
- Freshness damping, pending-row occupancy debit, broker grants, grant epochs,
  active-location checks, and zero-cost-only launch pinning remain mandatory.
  The aggregate demand target and capacity hint remain demand-only.

### Demand-only paid backfill and retirement accounting

Reserved fill is opportunistic supply, not traffic intent. A replica launched
by the reserved-fill overlay (`reserved_fill=true`) must never raise or retain
the traffic target and must never cause a paid replacement when its reserved
slot is reclaimed. The broker may replace that row only through the existing
epoch-fenced, zero-cost-only fill launch path. Replicas launched for traffic
remain demand-owned even when placement happens to put them on a zero-cost
location; losing one may therefore require an ordinary compatible replacement.

For logical concurrency scaling, use demand-owned latest-version capacity for
all three traffic-retirement state transitions:

- the one-shot aggregate target reconstruction after controller restart;
- the configurable percentage limit applied after a completed downscale
  hysteresis window; and
- the frozen pending-capacity retention budget for that downscale episode.

The rebuilt-blind capacity hint sent to the load balancer before the first
fresh autoscaler decision must use the same demand-owned latest-version
cohort. It may continue reporting total ready and provisioning capacity in
their dedicated fields, because fill capacity can serve traffic while it
exists, but fill-origin rows must not raise the advertised traffic target or
delay platform spill/backfill as if they were paid demand intent.

Apply the pending budget only to demand-owned pending rows. Fill-origin pending
rows neither enlarge the cancellation allowance nor consume the protected
demand cohort. Rows persisted by versions that predate `reserved_fill` default
to demand-owned, which is the conservative compatibility direction.

Do not replace the existing total-committed accounting used for scale-up wave
budgets, duplicate-launch suppression, aggregate hard ceilings, readiness
coverage, or status/history capacity. Already committed fill capacity can
satisfy compatible traffic and must prevent a duplicate paid launch while it
exists. The separation controls which capacity can retain demand intent, not
whether compatible reserved capacity can serve requests.

During a controller/LB mixed-version interval, the existing active-Pod,
HA-slot, lifecycle-generation, and routing-version fences remain authoritative.
An incomplete exact-card report cannot authorize scaling. It also must not
convert the committed fill fleet into traffic demand: the held traffic baseline
is reconstructed from demand-owned rows, while the independent fill overlay
continues protecting observed zero-cost capacity.

## Behavioral contract

### Request compatibility wire contract

Add the data-plane header:

```text
X-SkyServe-Compatible-Accelerators: L4,A100,A100-80GB,H100
```

- One header field is allowed. Reject repeated fields, an empty value, empty tokens, duplicate exact cards, unknown cards, excessive token count, or a value longer than a small fixed limit (for example 512 bytes) with HTTP 400 before queue admission.
- Trim optional HTTP whitespace around comma-separated tokens.
- Resolve each token case-insensitively against the active service version's configured exact accelerator IDs, then retain the service's canonical display spelling.
- Never apply `HardwareGroup` regex matching or accelerator-family canonicalization. `A100` does not match `A100-80GB` in either direction.
- Treat the supplied sequence as a compatibility set for queueing and demand aggregation. Preserve its order for observability and deterministic equal-cost tie-breaking, but do not let list order force a cold paid launch when a cheaper compatible card exists.
- When the header is missing, synthesize the full set of exact accelerators configured by the active service version. This behavior lives in SkyServe, so older and non-platform clients automatically get the safe default.
- Strip the SkyServe-only header before proxying to user replicas.
- Snapshot the active service version and canonical compatibility set at admission. On an in-place service update, intersect queued waiters with the new exact set: re-index surviving waiters without changing priority/sequence; fail a waiter whose set becomes empty with a retryable 503 rather than silently widening it.

The capacity endpoint advertises a versioned capability, for example:

```json
{
  "request_accelerator_compatibility_version": 1,
  "configured_accelerators": ["L4", "A100", "A100-80GB", "H100"]
}
```

SkyServe advertises version 1 only when it has complete exact-card telemetry and at most `MAX_COMPATIBILITY_ACCELERATORS = 8` configured cards. Services outside that bound keep their legacy behavior and do not advertise the feature, so this addition does not invalidate an existing larger `resources.any_of` service. boltz-platform sends the header only after seeing version 1. An omitted platform field may safely remain omitted against an old LB because both sides mean all configured cards. An explicit subset must either use another exact-compatible provider candidate or fail closed with an unsupported-capability/no-capacity result when version 1 is absent; it must never be silently widened by omitting the header. This provides a safe SkyPilot-first rollout and prevents a new platform binary from assuming filtering on an old load balancer.

### Queue and dispatch order

Keep one authoritative waiter object and one ownership/state transition path. Per-card indexes contain references to those waiters; they are not separate queues.

Process numeric-priority tiers from highest to lowest. Within one tier, group authoritative FIFO waiters by compatibility bitmap and solve a bounded profile-to-ready-card assignment (at most 255 profiles by 8 exact card types) with these lexicographic objectives:

1. maximize the number of requests admitted immediately;
2. give a scarce ready slot to the compatibility profile whose best non-selected fallback is worse;
3. preserve FIFO sequence when fallback quality is equal.

The synchronous load-balancer matcher does no work when the global dispatch
budget is zero. Otherwise it stops after filling the current dispatch budget
or the exact-card slot snapshot, whichever is smaller. Once one waiter from a
compatibility profile cannot augment an unchanged matching, later waiters from
that identical profile are not retried in the same pass. Runtime therefore
scales with the bounded profile graph and immediately grantable slots, not with
repeated recursive walks over the full backlog.

Define fallback quality from the same exact-card supply snapshot as an ordered tuple, not from compatibility count:

```text
ready reserved/zero-cost alternative
  < ready paid alternative
  < healthy provisioning alternative within startup SLA
  < free reserved-capacity alternative
  < paid cold alternative (cheapest/fastest first)
  < unavailable alternative
```

A ready card is an admission edge only when it is in the waiter's exact compatibility set. Provisioning/reserved/paid alternatives influence which waiter most needs a scarce ready slot, but they do not become routable until ready. A provisioning attempt that exceeds its startup SLA or enters failure stops counting as a healthy fallback and triggers replanning.

Reserved-first is a replica-assignment tie-break after numeric priority, maximum immediate admission, and scarce-card protection. It must not let a flexible request take the only ready reserved A100 from an equal-priority A100-only request when a paid L4 can serve the flexible request. Within an equivalent exact-card assignment, route to a healthy reserved replica with a free concurrency slot before a paid replica; never overload reserved capacity merely to preserve the cost preference.

Reserved preference is consumed per free zero-cost slot while constructing a
batch. A card with one free reserved slot is preferred for at most one planned
assignment; subsequent assignments compare its paid slots normally.

Example for equal numeric priority:

- Request A is compatible with `{L4, A100}`; request B with `{A100, H100}`.
- If L4 is unavailable and A100 plus H100 are ready, maximize admissions by assigning A to A100 and B to H100.
- If only A100 is ready and neither request has a viable alternative, both fallbacks are equally unavailable and FIFO decides.
- If only A100 is ready but A's alternative is a cheap paid L4 while B's alternative is a more expensive paid H100, B gets A100 and the scale planner targets L4 for A.

This matching rule subsumes the simple A100-only-versus-flexible case without making the incorrect assumption that all two-card sets are equally flexible.

The scheduler atomically grants a waiter a card-specific eligible replica set while retaining one process-level admission owner. It removes all secondary references in the same lock, so a flexible request cannot be granted once from L4 and again from A100. New requests always enter the authoritative waiter registry before dispatch; they cannot bypass already-eligible waiters.

If a granted replica fails during proxying before the upstream outcome becomes ambiguous, the already-admitted request may retry on another compatible ready card. It retains the same process-level admission ownership throughout the bounded retry loop, so it cannot leak or double-consume a slot. If no compatible replica remains, it fails with a retryable 503 and releases admission; it is not silently widened to an incompatible card.

Strict numeric priority and fallback-aware ordering can starve lower-priority requests or requests with consistently better alternatives. This is intentional and matches the accepted priority policy; existing queue timeout/cancellation remains the bound.

### Demand allocation and scale-up choice

Compatibility demand is counted once. It must never be copied into every compatible card's backlog.

The active LB reports a bounded histogram keyed by `(numeric priority, compatibility bitmap)` over the service's exact configured cards. List order is ignored for the histogram, so equivalent sets coalesce. `MAX_COMPATIBILITY_ACCELERATORS = 8` bounds compatibility masks at 255; numeric priority is already bounded to 0..100, and the sparse payload is additionally bounded by the configured request-queue size. Priority partitions demand for ordering but never duplicates a request or multiplies desired capacity. In-flight work is attributed to the exact card actually holding the slot.

The autoscaler allocates aggregate demand by numeric priority descending, with
stable assignments and existing up/down hysteresis to prevent oscillation.
Below `max_replicas`, all priority partitions still contribute demand; when
the cap forces a choice, the per-card target allocation mirrors queue
precedence instead of reserving scarce capacity for work that cannot yet be
admitted. For each compatibility profile:

1. consume any unused hard-floor capacity on a compatible card;
2. attribute the remaining demand to the cheapest cold paid compatible card,
   using request/service order only as a deterministic equal-cost tie-break;
3. independently recompute cold-launch authority by consuming compatible ready
   reserved, ready paid, healthy provisioning, and free reserved supply before
   leaving any exact-card launch shortage.

Existing supply therefore changes whether a launch is necessary, not which
card owns flexible demand in the dashboard or retained demand target. Cold
placement does not fall through to a more expensive card merely because the
cheapest card is temporarily unavailable.

For placer-backed services, cheapest means the lowest cataloged nominal hourly
cost across every enumerated paid location, including a location currently
benched after a capacity failure. Availability controls whether an exact-card
launch can proceed, but it never changes the card target. If the cheapest card
is unavailable, the request remains queued and SkyServe retries that exact card
under the placer's normal bounded retry policy; it does not cold-start a larger
compatible card. Already-ready or healthy-provisioning larger cards remain
valid warm supply and can avoid that cold launch. Without a placement policy, a
multi-card service must use an ordered accelerator resource list before
advertising compatibility. An unordered `resources.any_of` service keeps
legacy aggregate behavior rather than turning transient availability or hash
iteration into a cold-card policy.

#### Downscale-held exact-card retries

Aggregate downscale delay retains capacity while a request temporarily leaves
the live queue, including the gap between a timed-out HTTP connection and a
deduplicated client retry. That retained capacity keeps its adopted exact-card
assignment. Recomputing the held portion as default-all demand would allow the
actuator to replace a constrained L40S target with the cheaper L4 even though
no new compatibility evidence authorized that change.

Fresh demand remains independently reassignable. If an adopted three-slot L40S
target is held while one new L4-only request is visible, the actuation target
may become one L4 plus two held L40S slots. It must not reinterpret all three
slots as L4. Generic overprovision continues to follow the fresh desired map
because it is outside the adopted traffic target. Already-running compatible
GPU supply may still replace a held slot; only a new cold launch is forbidden
from silently changing the held slot's card.

This downscale hold is deliberately different from an ordinary mixed-version
scale-up tick. While the hold is active, its adopted paid-owned portion remains
the exact-card retry authority: a failed L40S launch is retried as L40S, not
reinterpreted as L4. Synthesized padding in the same held reconciliation map
does not thereby acquire paid authority. Mixed-version reassignment is enabled
only when the tick is not holding a downscale.

The live failure was observed on 2026-07-27 with service
`clin-structure-eval-6f51471-l40s-v8`. One queued request produced aggregate
target 1 and exact target `{"L40S": 1}`. After two L40S Spot locations failed
for capacity (`g6e.xlarge` in `ap-south-1a`, then `us-east-2a`), the queue was
briefly empty while the 900-second downscale delay held target 1. The next
replica selected L4 (`gr6.4xlarge` in `eu-north-1a`) despite the controller
still reporting `{"L40S": 1}`. The dispatcher and service were stopped
immediately; the Kubernetes Job, pod, service, and replica cluster were
confirmed absent.

Regression coverage must reproduce the two reports without cloud resources:
first one L40S-constrained queued request, then a complete empty report before
downscale delay expires. Both scale-up decisions must remain exactly one L40S.
Coverage must also show that a smaller fresh target can change only its own
slots while the remainder preserves the adopted exact-card mix, that a
rate-limited downscale preserves the cards of the remaining slots, and that
already-running compatible supply remains reusable.

Local verification on 2026-07-27 ran all 241 concurrency-autoscaler unit tests
after rebasing onto current `improvements`.
The production-shape reproduction changed from
`retry_actuation={"L4": 1}` before the fix to
`retry_actuation={"L40S": 1}` after it, with raw target 0 and held target 1.
The touched Python files pass pinned YAPF 0.43.0 and Pylint 4.0.4 at 10/10.

Nominal cost ordering is all-or-nothing across cards that may have paid
capacity. If any such card has an unavailable catalog price or no cataloged
location, the controller and autoscaler preserve the explicit service order
among those cards. An unavailable price must never promote a larger priced
card ahead of an unpriced cheaper card. A card whose every matched location is
cataloged as zero-cost is not a cold-paid candidate, so it is ordered after all
paid-capable and unpriced cards. This does not remove the reserved-only card
from compatibility or from reserved fill: exact demand may still target it,
and free broker capacity may still materialize it through the independent
zero-cost-only fill path.

The controller recomputes after each supply transition. It may launch reserved and paid capacity in the same control cycle when demand exceeds already-ready, provisioning, and reserved capacity; the list above is allocation accounting, not a requirement to wait serially for one tier to finish.

The adopted demand map and the cold-launch map have different safety roles.
The adopted map always records the cheapest compatible placement of the
already-adopted aggregate target. It must not retain the current physical card
mix during compatibility hysteresis, a logical-card migration, or controller
restart recovery. If aggregate hysteresis temporarily holds 47 traffic slots
while fresh all-compatible demand has fallen below 47, the complete demand map
is still `{L4: 47}` when L4 is cheapest. Existing A100 and A100-80GB supply is
reported through warm retention and actuation, never by rewriting that demand
map.

Before emitting any exact-card scale-up or card-specific retirement, a fresh
and complete control tick recomputes a supply-aware actuation placement at the
already-adopted aggregate target from the current compatibility profiles, hard
floors, non-retiring pinned work, and latest-version supply. That placement is
the capacity mix reconciliation should converge toward. Only its positive
shortage that also appears in the separately published paid authority is
allowed to create paid capacity.

The allocation result carries three independently computed per-card maps plus
an attribution-completeness bit:

- the full reconciliation map includes all demand and padding;
- the explicit-compatibility map includes request compatibility evidence,
  exact-card floors, and fixed exact-card work, and bounds mixed-version
  cross-card movement;
- the paid-ownership map includes demand that may buy capacity in an ordinary
  latest-only service. In addition to explicit demand, this includes the
  aggregate minimum and headerless queued/rejected demand, so cold
  scale-from-zero remains possible. It excludes inferred in-flight overflow
  and generic overprovision padding.

All three maps are intersected with the same full allocation; ownership cannot
be transferred to a card selected only because unrelated unproven work changed
the allocator's marginal placement. The explicit and paid-owned maps are
adopted alongside the demand target so exact retries survive a transiently
empty histogram. A later target adoption replaces those carried maps.

On a non-downscale mixed-version tick, only the explicit-compatibility subset
may move an adopted unit, including one still backed by an old version.
Aggregate queue/rejection gaps, running work whose accepted compatibility
history has aged out, unattributed arrival work, and aggregate
minimum/overprovision padding never enter that explicit subset. On a
latest-only tick, the paid-ownership subset may also move to its freshly
allocated card; this lets a headerless queue reprice vanished supply without
letting inferred in-flight overflow guess a purchase.

For explicitly owned units, an old row protects active work and prevents its
own retirement until compatible latest-version READY coverage is available;
it never dictates the paid replacement card. Thus an old A100 that had served
explicit default-all work may remain nonpreemptively alive while the latest
version launches the allocator-owned L4 replacement. Exact A100 demand or an
A100 hard floor still selects A100. A backed adopted unit outside the explicit
subset may authorize only its same-card rollout replacement. A vanished
unproven unit and synthesized padding authorize no paid placement; they may
retain a reconciliation/zero-cost probe without becoming an A100 or L4
purchase. Same-card rollout ownership is an absolute latest-version ceiling:
`min(reconciliation target, adopted demand, latest committed + live old
backing)`. The decision layer subtracts latest committed capacity from that
ceiling, so a partially completed `latest=1, old=1, target=2` rollout receives
exactly one more launch rather than deadlocking at zero.

During a mixed-version rollout, stale or otherwise incomplete telemetry keeps
the conservative adopted reconciliation target and publishes an explicit empty
paid-authority map. A stale single-version target may still be recomputed from
retained valid gauges, but it likewise authorizes no paid mutation and cannot
retire capacity. Existing zero-cost placement remains eligible under its own
fence. Logical-card wave limits and busy/unknown-work protections apply in all
cases.

Rows already marked for scale-down or preempted are excluded from
ready/provisioning supply in that cold-launch recomputation and from
latest-version coverage used to authorize an old-version rollout drain.
Logical supply uses the same committed-capacity function as duplicate-launch
suppression: healthy READY width is bounded by observed slots, pending width is
planned capacity, and a persistently zero/unknown READY row contributes zero
after the bounded replacement timeout. A row explicitly marked as that
bounded replacement remains committed so telemetry loss cannot recursively
launch replacements. Work still draining on an excluded row remains in the
aggregate outstanding-work safety total, but it does not pin replacement
capacity to the retiring row's card. The replacement portion is allocated by
the current request compatibility sets. Consequently:

- losing an idle or retiring reserved A100 that had served default-all or
  `L4/A100` work shifts any unbacked replacement shortfall to L4 when L4 is the
  cheapest compatible paid card, even after the A100 row disappears and even
  while every L4 location is temporarily benched;
- an A100-only hard floor, running request on a non-retiring A100, or current
  A100-only queued demand can still authorize an A100 cold launch;
- an already-ready compatible A100 remains eligible for routing and avoids an
  unnecessary L4 launch, including when it is reserved capacity;
- stale or incomplete compatibility telemetry authorizes neither a paid card
  migration nor a paid cold launch. In particular, a mixed-version rollout
  preserves its adopted card map; a stale single-version tick may recompute a
  reconciliation target from retained valid gauges but cannot spend or retire.
- a timed-out degraded A100 cannot bias a flexible allocator toward A100 or
  preserve A100 backing while shortage accounting treats it as zero; the
  allocator, actuation revalidator, and decision layer share one committed
  capacity value.

During a mixed-version rollout, generation provenance is tri-state. An
explicit old-version supply map is a complete snapshot, and an omitted card in
that map has known zero old-version supply. An empty explicit map therefore
proves that every adopted card absent from latest-version supply is gone from
the whole fleet. If old-version provenance is unavailable, it is represented
as unknown rather than as an empty map; the actuator fails closed and preserves
the adopted exact-card map. With complete provenance, adopted units backed by
neither latest nor old non-retiring supply may move only as far as the
per-card explicit ownership subset requires before conservative mixed-version
handling. Any unproven remainder stays outside paid authority. When the rest of
the tick is also fresh, complete, and not downscale-held, ordinary rollout
reconciliation may likewise move units that remain old-version-backed toward
that explicit subset. Preempted and scale-down rows never count as backing on
either generation.

The logical replica-manager reconciliation target and paid cold-launch
authority are deliberately distinct. The target drives convergence and
zero-cost placement eligibility; the paid-authority map is an incremental,
per-card spending budget. While they differ from the adopted retirement map,
the autoscaler suppresses unsafe retirement. After normal hysteresis adopts
the new card assignment, scale-down again uses the adopted map and the existing
idle/graceful-drain proofs.

The fence is checked again immediately before each queued demand launch makes
its first cloud mutation. Persisting a replica row or placing it in the local
launch pool is not launch authority: a large wave may wait behind bounded
launch concurrency while a newer compatibility report changes the exact-card
target. For each card, the pre-launch check counts current non-retiring
READY/STARTING/PROVISIONING capacity first, then authorizes only the oldest
zero-cost demand-owned PENDING rows and finally the oldest paid demand-owned
PENDING rows that fit the remaining target. Rows launched for reserved fill
remain governed by the independent broker-grant fence and are not charged to
this demand budget.

Placement returns a typed launch result with `funding` (`PAID` or
`ZERO_COST`) and `planned_capacity`. Batch reconciliation debits authority only
for `PAID` results, using the returned planned capacity. It never recovers that
decision by inspecting a newly appended `ReplicaInfo`, and it never charges a
zero-cost demand placement against paid authority. If a card has no paid
authority left, the manager may still attempt an eligible zero-cost location;
it must not fall through to paid placement.

An ordinary unpinned launch has no per-replica resources override before that
first mutation. When the complete configured catalog contains exactly one
card, that card is nevertheless authoritative for every such row and is used
for the budget check. An unpinned row remains unclassifiable and fails closed
when the catalog contains multiple cards; the controller must not guess which
optimizer alternative a future launch will select.

A reconciliation generation is an observation stamp, not by itself a semantic
target change. A queued launch may remain authorized across a newer generation
only when the fresh current target has the same service version, aggregate
capacity, exact-card capacities, and accelerator shapes as its stored fence,
and a new fleet read still includes that replica in the current launch budget.
Any version, aggregate target, exact-card target, or shape change revokes the
stored authority. Newly READY or PROVISIONING capacity can also remove the
candidate even when the target is otherwise unchanged.

Every final-cloud rejection records one stable reason code plus a bounded,
secret-free summary of the stored/current target, target freshness, card
budget, and candidate classification. A rejected launch is otherwise
indistinguishable from a legitimate supersession because both stop before
`sky.launch`; preserving the exact fail-closed reason is required to prove the
fence against a live service without weakening it for diagnosis.

This check is restart-safe. A recovered logical controller must not treat all
durable PENDING demand rows as fresh launch orders. It reconstructs their
authorization from the first fresh, complete exact-card target it receives;
until then, queued demand launches fail closed. A row excluded by the current
budget is removed through normal replica cleanup and cannot call `sky.launch`.
Recovery also revalidates an interrupted PROVISIONING row before re-driving
`sky.launch`: the controller cannot prove that its pre-restart asynchronous
request still owns live cloud work because the request ID is not durable.
Cleanup may therefore cancel never-ready infrastructure, but it cannot preempt
a routed user request. READY and STARTING capacity remains governed by the
existing graceful downscale path. Consequently, a controller restart cannot
turn an old `A100: 165` wave into new paid A100 cloud mutations after the
current target has become `A100: 93` and 93 or more compatible A100 slots are
already materialized.

Reserved fill is reconciled against shaped demand launches in the same tick.
Each exact-card demand launch first claims at most one freshly reported
physical reserved slot of that card; logical targets convert their slot
shortfall to physical backend claims using the configured GPU width. The fill
overlay subtracts those claims before emitting zero-cost-only launches, so a
hard floor or flexible demand target and reserved fill cannot create two rows
for one free physical slot. A later poll restores any conservatively withheld
fill after the demand row has committed or fallen back to paid supply. Any
remaining same-tick fill intents carry exact-card overrides for the unclaimed
free slots, so fill cannot collide with a demand claim on another reserved
card.

Preempted or already-scaling-down replacements are not committed capacity for
logical reconciliation and cannot complete a recovered cost-rebalance pair.
The incumbent remains serving until a healthy non-retiring replacement is
ready.

This is why an already-ready reserved A100 may serve flexible L4/A100/H100 work, while an empty fleet normally cold-starts the cheaper L4. When an A100-only request later arrives, no running flexible request is interrupted. At equal priority it owns the next A100 admission opportunity, and its demand increases the A100 target if capacity is otherwise occupied.

### Global target, per-card target, and floors

Add an exact-card floor map to `replica_policy`:

```yaml
service:
  replica_policy:
    min_replicas: 0
    max_replicas: 100
    min_replicas_by_accelerator:
      L4: 0
      A100: 0
      A100-80GB: 0
      H100: 0
```

- Keys must resolve to distinct exact accelerators present in the service task resources. Unknown/family/regex keys are invalid.
- Missing keys have floor zero. The whole map may be omitted for backward compatibility.
- Values are non-negative serving-replica counts. Reject `sum(per-card floors) > max_replicas`.
- Existing `min_replicas` remains an independent aggregate floor. If it
  exceeds the sum of card floors, attribute the remainder to the cheapest
  configured card. Compatible existing supply may suppress the resulting cold
  launch, but does not change the floor's demand attribution.
- `demand_target_by_accelerator` remains the backward-compatible per-card
  demand target. It includes the hard per-card floor and its entries sum to
  the existing aggregate `target_num_replicas`. Flexible work is attributed to
  its cheapest compatible card even when a larger ready card serves it. Work
  whose compatibility is unknown or known to be exact-card constrained remains
  pinned conservatively. The demand target is not, by itself, authority to
  cold launch any card.
- `warm_retention_target_by_accelerator` reports running or unknown work that
  must remain on its current exact card. It is explanatory and non-additive,
  but is not required to be a subset of the cheapest-card demand map. For
  example, demand can be `{L4: 6}` while warm retention is `{A100: 2}` and cold
  launch authority is empty.
- `cold_launch_authority_by_accelerator` reports only the positive incremental
  exact-card shortage that can emit a scale-up decision in the current
  reconciliation. It is zero for an expensive card whose demand target is
  satisfied by already-materialized capacity. For a fully compatible cold
  demand wave it may be nonzero only on the cheapest compatible card selected
  by the allocator. Exact-card-constrained demand may authorize its required
  card.
- Logical scale decisions carry the reconciliation target and paid authority
  independently. An explicit empty authority means zero paid launches even if
  the retained reconciliation target is nonzero. A missing authority field is
  reserved only for backward compatibility with legacy aggregate decisions;
  new exact-card decisions always publish the field explicitly.
- Broker-reported free reserved slots are not materialized capacity and cannot
  back a supply-aware demand reassignment. They are consumed only by the
  reserved-fill overlay, whose launch carries the zero-cost-only fence. This
  remains true during rolling updates: losing or failing a research-pool
  A100-family slot cannot create a paid A100-family replacement launch for
  otherwise L4-compatible demand.
- In logical concurrency mode, running work remains visible in
  `warm_retention_target_by_accelerator` and blocks its replica from draining,
  but the serving card does not pin the private desired-card actuation map.
  Compatibility demand ownership selects the cold card. While old and latest
  versions coexist, a fresh, complete, non-downscale tick may move only the
  per-card explicitly owned subset toward the supply-aware compatible
  latest-version placement, even when old-version rows still back the prior
  card. Those rows fence nonpreemptive retirement but do not choose the paid
  replacement card. With complete generation provenance, units absent from
  every generation move only toward explicit ownership and receive no
  same-card paid fallback; with unknown provenance, the full map is preserved
  and paid authority is explicitly empty. A downscale hold keeps its adopted
  paid-owned exact-card retry authority while unowned padding remains
  zero-cost-only. After rollout, materialized latest-version supply may satisfy
  compatible demand without changing ownership. This prevents both a warm and
  a reclaimed research A100 from becoming paid A100 replacement authority for
  L4-compatible work.
- The aggregate demand target is `max(calculated demand, min_replicas, sum(per-card floors))`, capped by `max_replicas`. When demand exceeds the cap, requests remain queued; compatibility is never widened.
- Scale-up decisions carry an exact accelerator resource override. Scale-down selects an exact card whose current serving replicas exceed that card's target and floor, observes the existing graceful/idleness delay, and never terminates active work.
- Economic cost rebalancing may move a replica to a cheaper provider, region,
  or cluster only when the replacement preserves the same exact accelerator ID
  and the service's configured GPU-count shape. Generic services without an
  authoritative exact-card catalog retain legacy cross-card rebalancing. A
  persisted replacement pair that no longer matches the active catalog is
  unwound by retaining the incumbent and gracefully retiring the replacement.
- For this first version, require one GPU-count shape per exact accelerator ID in a multi-card service. Reject ambiguous configurations such as both `A100:1` and `A100:8` under one `A100` floor until the public identity is extended to an exact card-plus-count shape.
- Exact-card compatibility and per-card floors support either dictionary
  `target_qps_per_replica` or scalar per-GPU
  `target_concurrency_per_replica`, and require
  `instance_aware_least_load`. Other autoscalers retain their legacy aggregate
  behavior and do not advertise the compatibility capability, preventing
  constrained demand from triggering generic scale-ups onto the wrong card.
  An in-place policy downgrade to `least_load` withdraws the capability and
  clears the autoscaler's prior exact-card catalog, targets, gauges, and
  reserved-card supply atomically with the autoscaler version transition,
  before aggregate scaling resumes or the new replica-manager fence can accept
  a decision.
  A nonempty card or GPU-count catalog change also invalidates replaceable
  compatibility gauges and holds exact-card scaling until a complete LB report
  arrives under the new routing version. Old A100 demand is never reinterpreted
  as H100 demand merely because the task catalog changed.
- Per-card values use the service's public replica unit. Physical-backend
  services count machines. Logical services count GPU slots, matching the
  existing concurrency target, min, max, and capacity fields. A logical
  `A100:8` backend therefore contributes eight A100 units to ready, target,
  and floor accounting, while it remains one physical machine in the
  separately reported physical counts.
- Concurrency sizing preserves the existing aggregate outstanding-work
  contract and its utilization, rejection-duration, hysteresis, wave-limit,
  and stale-report rules. Running and unknown work remains physically pinned
  for warm retention and actuation, so a priority change never preempts active
  work. In logical-slot mode, the demand map uses the complete bounded
  accepted-arrival compatibility histogram for running work and the protocol's
  all-configured-cards default after that evidence ages out. Flexible queued,
  recently rejected, and attributable accepted work is assigned priority-first
  to the cheapest compatible card. Physical-backend mode keeps its historical
  current-card attribution because heterogeneous backend counts are not
  logical capacity units. A separate supply-aware pass pins active work, then
  consumes ready-reserved, ready-paid, provisioning, and free-reserved
  capacity to derive cold-launch authority.
- Exact-card rounding happens independently. If fractional duration-normalized
  work spans several disjoint compatibility profiles, the compatibility-safe
  target may be slightly larger than `ceil(total_work / per_slot_capacity)`;
  the published global target remains exactly the sum of the per-card targets.
- An empty per-card map is a valid complete target when aggregate demand and
  floors are zero. It gracefully retires idle paid replicas on every card and
  never falls back to the incomplete-target fence.
- `num_overprovision` remains separate from the published demand target. At
  actuation time, extend the complete per-card demand map to the final target
  with the same ready, reserved, provisioning, then cold ordering, so every
  overprovisioned launch still carries an exact card override.
- A logical same-total card migration is a scale-up event even though its
  aggregate demand target is unchanged. Publish the complete cheapest-compatible
  demand map immediately, then limit only the actuation map's positive cold
  shortages by the configured slot wave. Retain corresponding old-card supply
  as a transition placeholder and retire it only after the authorized
  replacement target is ready. Apply a hard floor larger than one wave over
  successive waves without weakening the eventual floor or mislabeling the
  placeholder as demand.

The control loop exposes three related but different values:

```text
global demand target = sum(demand target per exact card)
effective desired per card = max(demand target, reserved-fill target)
actual replicas = ready + provisioning + other live states
reserved-fill launch budget = min(fresh granted slots,
                                  max_replicas - live/planned capacity)
```

The effective desired value governs steady-state retention. The launch budget is deliberately independent: a mixed paid/reserved fleet may launch all free reserved slots immediately while paid scale-down is still draining, provided the hard aggregate ceiling has headroom. This keeps global and per-card targets independently understandable without allowing them to contradict each other.

### Reserved capacity

Reserved capacity is supply, not accelerator identity and not hidden demand.

- Observe, claim, and report reserved slots by exact `(cluster/context, accelerator_id)` pool. Lowercasing for case-insensitive equality is allowed; collapsing `A100` with `A100-80GB` is forbidden.
- Split any current multi-accelerator broker/fill round into exact-card grants before applying per-card targets. A claim for A100 cannot satisfy an A100-80GB decision.
- A free compatible reserved slot has zero incremental infrastructure cost and therefore wins before a paid cold start, including when it is a larger card.
- A healthy ready replica already running on reserved infrastructure wins before an otherwise-equivalent ready paid replica. This makes paid replicas idle sooner so the normal graceful scale-down can remove them; request priority, compatibility matching, and concurrency safety still take precedence.
- Keep `reserved_capacity_fill` as an optional overlay, reported as `fill_target_by_accelerator` and `free_reserved_slots_by_accelerator`, not folded into demand target.
- With fill enabled, launch every fresh broker-granted reserved slot that fits under the aggregate `max_replicas` ceiling. Do not suppress launches merely because the demand target is greater than the fill target or because paid replicas currently satisfy demand.
- With fill enabled, zero-cost serving replicas may intentionally remain above demand/floors; the UI labels them as fill capacity. With fill disabled, idle serving replicas gracefully drain to demand/floors while the underlying reserved physical machines may remain up and appear as free reserved supply. This is expected extra capacity, not a failed scale-down.
- When demand and fill both want the same exact-card replica, count it once via `max(demand_target, fill_target)`, not by adding both targets.
- Scale-down sheltering applies that overlap per exact card. Allocate the
  aggregate broker fill target to existing zero-cost holdings in configured
  card order before projecting any remainder onto free supply, then subtract
  only demand for the same exact card. Demand assigned to L4, for example,
  cannot reduce the shelter for A100 or A100-80GB holdings. If a rolling-update
  row has no safely attributable exact card, or the per-card demand map is
  incomplete or does not account for the full final demand target, retain the
  legacy aggregate shelter for that tick rather than guessing or widening
  accelerator identity.
- `max_replicas` is a hard aggregate fill ceiling. Count all old-version nonterminal capacity and reserve the latest-version demand plan before emitting fill launches. At the ceiling, retain the observed free-slot intent for a later control cycle instead of launching overlap.

## Architecture flow

```text
boltz-platform user request
  compatibility?: [exact Hardware values]   priority: high|low
                 |                                  |
                 +-------------+--------------------+
                               v
                 one SkyServe service request
                 compatibility header + 50/20 priority
                               |
                               v
        one authoritative LB waiter registry / admission scheduler
          exact-card secondary indexes; no per-card duplicate queues
                               |
             ready exact card? +---- yes ----> grant and proxy
                               |
                               no
                               v
        compatibility bitmap demand histogram to active controller
                               |
          priority-first, supply-aware per-card demand allocation
          floor map + reserved observations + cost
                               |
       exact-card scale-up/down decisions and UI/status breakdown
```

## Implementation milestones

### Milestone 1 - Exact accelerator identity and control-plane schema

SkyPilot changes:

- In `sky/serve/service_spec.py` and `sky/utils/service_schema.py`, add `min_replicas_by_accelerator`, canonical exact-card validation, the one-GPU-count-shape-per-card guard, and serialization/backward-compatible defaults.
- Add a shared SkyServe exact accelerator registry derived from the active task resources. It maps case-insensitive wire tokens to canonical display IDs but exposes no family/prefix matcher.
- Persist explicit zero-cost/reserved-supply provenance on every replica placed on a reserved zero-cost location, whether it was launched for ordinary demand or proactive fill. Do not infer this from `reserved_fill`, which describes why the replica was launched rather than where it runs.
- Extend `sky/serve/constants.py` with the compatibility header name, version, and size/count bounds.
- Extend autoscaler/controller status types in `sky/serve/autoscalers.py`, `sky/serve/controller.py`, `sky/serve/serve_utils.py`, and API schemas with additive per-card maps while preserving existing aggregate fields for old clients.
- Add unit tests proving `A100`, `A100-80GB`, and differently cased spellings have the intended equality boundaries; test invalid maps, floor/max conflicts, serialization, and old service YAML.

Acceptance gate:

- Existing single-card services produce identical behavior and status.
- A configuration containing both A100 variants preserves two exact registry entries, two floor keys, and two status rows.

### Milestone 2 - Compatibility-aware admission and routing

SkyPilot changes:

- In `sky/serve/load_balancer.py`, parse/default/validate the header at admission, store canonical exact-card compatibility on `_RequestQueueWaiter`, and strip it before forwarding.
- Replace the single global-head grant loop with one authoritative waiter registry plus exact-card secondary indexes. Run the bounded priority-tier/profile-to-card matcher from a consistent supply snapshot and keep atomic cross-index removal/cancellation.
- Use existing `LoadBalancingPolicy.select_replica(..., eligible=...)` support to restrict dispatch to URLs whose `replica_info.gpu_type` is an exact compatible card.
- In `sky/serve/load_balancing_policies.py`, centralize exact-card URL lookup while retaining instance-aware least-load selection among eligible replicas.
- Include the persisted zero-cost provenance in controller-to-LB `replica_info` and use it only as the final ready-replica cost preference after the compatibility matcher has protected constrained demand.
- Preserve one admitted owner across compatible proxy retries and prove cancellation/failure cleanup releases it exactly once.
- Add bounded compatibility-set queue metrics and the capability/configured-card fields to the LB capacity endpoint.

Tests:

- Extend `tests/unit_tests/test_serve_request_queue.py` for default-all, invalid headers, exact A100 separation, no incompatible dispatch, no new-arrival bypass, cancellation cleanup, and one-grant-only behavior for flexible waiters.
- Add deterministic concurrency cases: 1000 same-priority flexible L4/A100/H100 waiters, then an A100-only waiter; the constrained waiter gets the next A100 slot, L4 continues serving flexible work, and no running request is interrupted.
- Test numeric dominance separately: priority-50 flexible remains ahead of priority-20 A100-only for an A100 slot.
- Test the crossed two-card case: `{L4,A100}` and `{A100,H100}` use A100/H100 when L4 is unavailable and H100 is ready; with only A100 and equally unavailable alternatives FIFO wins; with paid L4 versus paid H100 fallback, assign A100 to the request avoiding the worse fallback and target the cheaper cold card for the other.
- Test that a zero dispatch budget bypasses matching, that one reserved slot
  influences only one assignment in a batch, and that 500 waiters spanning all
  255 nonempty profiles over eight cards fill 128 slots within a bounded
  load-balancer event-loop budget.
- Test compatible replica retry preserves one admission owner and never leaks occupancy; exhaustion fails retryably without widening compatibility.

Acceptance gate:

- Every proxied request reaches only an exact compatible replica.
- The queue remains one ownership domain, and flexible requests are neither duplicated nor lost under concurrent card releases.

### Milestone 3 - Per-card autoscaling and reserved-capacity allocation

SkyPilot changes:

- Extend LB-to-controller demand reports with the bounded `(numeric priority,
  compatibility bitmap)` arrival, queued, and recent-rejection histograms.
  Attribute the authoritative per-URL in-flight gauge to exact cards in the
  controller from the same replica catalog used for routing. Queue, rejection,
  and in-flight values are replaceable gauges. Keep the payload sparse and
  make the active authority the sole reporter.
- In `sky/serve/autoscalers.py`, add a deterministic, sticky priority-first
  supply-aware allocator shared by request-rate and concurrency autoscalers.
  Request-rate mode converts profile counts with each exact card's QPS target.
  Concurrency mode uses the existing per-GPU concurrency knob, pins running
  and unknown work to its actual exact card, and allocates queued/rejected
  work with the same marginal fallback ranking as admission.
- Update autoscaler decisions to carry exact accelerator resource overrides on ordinary demand scale-up, not only reserved-fill scale-up.
- Make scale-down exact-card-aware and enforce both aggregate and per-card hard floors under the existing graceful delays.
- Carry the per-card logical target through `LogicalScaleTarget` and the
  controller publication, `LogicalScaleDownTarget`, and replica-manager
  reconciliation fence. Logical placement may pack several slots into one
  compatible physical backend, but it must not satisfy one card's shortfall
  with another card. Logical retirement, including controller-restart and HA
  recovery, must prove both the aggregate target and every exact-card target
  remain covered by ready capacity. Durably committed provisioning capacity
  suppresses duplicate launches, but does not keep a healthy uncommitted drain
  off route when its exact card has a ready-capacity shortfall. When an exact-
  card catalog exists but the compatibility report is incomplete, the
  controller explicitly revokes retirement authority, so an aggregate target
  or an older exact target cannot authorize adoption or teardown. During a
  catalog-changing rollout, an old-version card removed from the new catalog
  may retire once aggregate capacity and every new exact-card target remain
  covered; it does not have to masquerade as a current compatible card.
- In `sky/serve/reserved_capacity.py`, `sky/serve/reserved_capacity_broker.py`, and `sky/serve/replica_managers.py`, expose exact-card free supply, prefer zero-incremental-cost compatible supply, and keep fill targets separate from demand targets. Broker entitlement remains aggregate for a service's zero-cost location group: it prevents cross-service overcommit, while exact demand placement consumes the per-card free-supply map. `fill_target_by_accelerator` is an observed projection of that aggregate surplus, not a second per-card actuator.
- In the existing aggregate fill overlay, make free-slot launch emission independent of the demand/fill target ordering while preserving the hard aggregate ceiling across rolling-update versions.
- Mark both demand-launched and fill-launched replicas as zero-cost when their selected exact location is in the current reserved-capacity set, persist that marker across controller restarts, and clear/recompute it only from authoritative placement metadata—not a stale fill reason.
- Preserve sticky private actuation assignments to ready/provisioning cards
  across control loops. Published demand attribution remains
  cheapest-compatible and is recomputed independently of that stickiness.
  Existing aggregate hysteresis may hold the total demand target, but it must
  not reconstruct the per-card demand map from the physical cards carrying
  that held capacity. Exact-card wave limits govern only cold-launch actuation
  and non-preemptive retirement.

Tests:

- Extend `tests/unit_tests/test_instance_aware_autoscaler.py` and
  `tests/unit_tests/test_reserved_capacity_fill.py` with empty-fleet cheapest
  selection, ready larger-card supply suppressing a launch without changing
  cheapest-card demand attribution, healthy-provisioning capacity preventing a
  duplicate launch, timed-out provisioning triggering replanning,
  free-reserved-before-paid residual scale-out, constrained demand
  reserving/scaling its exact card, crossed-set fallback-cost allocation, and
  no double-count of flexible demand.
- Cover global min greater than floor sum, floor sum greater than calculated demand, max-replica saturation, per-card graceful scale-down, optional fill enabled/disabled, and reserved physical machines remaining after serving replicas drain.
- Cover a paid fleet satisfying a demand target larger than the reserved
  target, all fresh granted slots launching into available headroom, planned
  demand consuming ceiling headroom, and old-version replicas counting against
  the hard ceiling. Exercise the interaction in both exact-card QPS and
  concurrency modes.
- Preserve and expand the existing A100/A100-80GB reserved-pool separation tests.
- Test that demand and fill replicas on reserved infrastructure both advertise zero-cost provenance to the LB, while a paid replica and a replica with unknown/stale provenance do not receive reserved-first preference.
- Add controller/LB synchronization tests for service-version changes and stale reserved observations.
- Add concurrency tests for physical and logical replica units, exact-card
  in-flight attribution, queued and rejected compatibility gauges, stale
  mixed-version reports, HA handoff, controller restart, bounded scale-up
  waves, same-total card migration, floors larger than one wave, demand-to-zero
  retirement, `num_overprovision`, multi-GPU logical packing, and card-safe
  non-preemptive retirement.

Acceptance gate:

- `A100 min=0` can still use an already-ready or free-reserved A100 without forcing a paid L4 launch.
- When compatible demand disappears, paid serving replicas drain toward per-card floors; any remaining fill replicas/physical reserved capacity are explicitly attributable to the fill overlay.

### Milestone 4 - boltz-platform exact compatibility propagation

boltz-platform changes:

- In `packages/common-backend/src/compute/compute.types.ts`, add `a100-80gb` as its own closed `Hardware` leaf and update cost/latency/VRAM catalogs and `MODEL_HARDWARE_LADDER` entries where that card is actually offered. Never express compatibility through `HardwareGroup` regexes.
- Add optional `compatibleHardware?: readonly Hardware[]` to the user-owned Boltz prediction/compute submit contract and thread it through `Boltz2SubmitInput`, workflow/retry state, `ComputeJobOptions`, and provider dispatch without replacing the existing concrete deployment identity.
- Validate the user-owned value globally as a non-empty, duplicate-free list of exact closed `Hardware` leaves. Omission stays omission so each selected backend applies its established default.
- Add the exact allowlist to platform placement resolution. Before priority-affinity/cost/load ordering, `resolveDispatchCandidates` removes every concrete non-SkyPilot deployment whose declared `hardware` is not in the set. For each SkyPilot fleet candidate, intersect the global set with that service's advertised exact cards; remove the candidate only when the intersection is empty, and attach the non-empty candidate-local intersection to that dispatch attempt. Thus global `[A100,H100]` can use an A100-only fleet without sending it an unknown H100 token. Capacity-shed retries reuse the validated global allowlist and recompute the candidate-local intersection; they may change provider but never escape to a disallowed card.
- If an existing explicit `capability?: ComputePool` selects a truly concrete provider/card outside `compatibleHardware`, reject the request as contradictory. If the capability's cell is only a legacy alias for one multi-card SkyPilot service endpoint, treat it as selecting that service, not as pinning the cell's card; require a non-empty advertised intersection and let the compatibility allowlist govern exact replicas behind the endpoint.
- In `packages/common-backend/src/compute/providers/skypilot.provider.ts`, map the selected candidate's exact intersection to `X-SkyServe-Compatible-Accelerators`, preserve the global set across idempotent retries/capacity shedding, and send a freshly resolved candidate-local set only when the capacity capability version is 1. If the array is explicit and capability version 1 is absent, fail closed as unsupported/no-capacity for that fleet instead of retrying without the constraint. Continue mapping high/low to 50/20.
- Treat a multi-card SkyServe service as one provider deployment and one DAFQ/admission resource. Do not manufacture several platform pools pointing at the same LB URL; card choice belongs to the compatibility-aware SkyServe scheduler, which can see ready and reserved supply.
- Keep upstream platform admission aggregate and priority-aware only; it must not create a second per-card queue or make card-specific grants for the SkyPilot fleet. SkyServe is the sole card-level compatibility queue. For a SkyPilot multi-card deployment, the resolved platform pool selects the service, while `compatibleHardware` constrains exact replicas inside that service. Other providers retain concrete-card placement but are eligible only when that exact card is allowed.

Tests:

- Update `compute.types`, placement/catalog, deployment-validation, router, and SkyPilot provider tests for the distinct `a100` and `a100-80gb` leaves.
- Verify header omission, exact serialization, validation, priority plus compatibility together, retry preservation, global-to-candidate intersection, default-all behavior against an old LB, exact-compatible spill to another provider, explicit-subset fail-closed behavior when no capable candidate remains, true concrete-capability conflicts, multi-card SkyPilot cell aliases, and no duplicate per-card platform admission queues.
- Add integration-shaped tests whose capacity payload advertises L4/A100/A100-80GB/H100 and proves each in-service subset is passed unchanged and exact, plus a partial-overlap case that sends only the exact candidate-local intersection.

Acceptance gate:

- A platform caller can constrain one request to A100/H100 and another to L4/A100/H100 while both use the same SkyServe service and priority system; no provider retry may leave those exact sets.
- An old SkyServe LB never receives a compatibility header it cannot enforce, and an explicit subset is never widened to default-all.

### Milestone 5 - Status, metrics, and dashboard visualization

SkyPilot changes:

- Extend `/autoscaler/info`, LB capacity hints, service status, and Prometheus metrics additively with:
  - `demand_target_by_accelerator`
  - `warm_retention_target_by_accelerator`
  - `cold_launch_authority_by_accelerator`
  - `min_replicas_by_accelerator`
  - ready/provisioning/live counts by accelerator
  - `fill_target_by_accelerator`
  - `free_reserved_slots_by_accelerator`
  - queued/in-flight counts by exact compatibility set (bounded labels; do not emit unbounded raw header values)
- Keep existing aggregate `target_num_replicas`, current/max/in-flight capacity, and old dashboard fields intact.
- In `sky/dashboard/src/data/connectors/services.jsx` and `sky/dashboard/src/pages/services/[service].js`, retain the global `ready / total (target: N)` summary and add an exact-card table:

| Card | Ready | Provisioning | Demand target | Warm retention | Cold-launch authority | Hard floor | Fill target | Free reserved slots |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| L4 | ... | ... | ... | ... | ... | ... | ... | ... |
| A100 | ... | ... | ... | ... | ... | ... | ... | ... |
| A100-80GB | ... | ... | ... | ... | ... | ... | ... | ... |
| H100 | ... | ... | ... | ... | ... | ... | ... | ... |

- Add tooltips explaining that demand target is cheapest-compatible demand
  attribution, warm retention is the independent current-card floor for work
  that cannot be interrupted, cold-launch authority is the current incremental
  shortage that can request new capacity after compatible supply is consumed,
  floor is a hard serving minimum, fill is optional zero-cost extra serving
  capacity, and free reserved slots are physical supply not yet represented by
  a serving replica.

Persist the same exact-card values in the existing one-minute
`serve_autoscaler_history` row as bounded PostgreSQL JSON objects. Store:

- demand targets, warm-retention targets, and cold-launch authority by exact
  accelerator;
- ready, provisioning, and non-failed tracked capacity by exact accelerator;
- hard floor, reserved-fill target, zero-cost ready capacity, and free
  reserved slots by exact accelerator.

Classify zero-cost capacity with the same active reserved-location matcher
used by the autoscaler's fill overlay. The persisted `is_zero_cost` field
remains a valid positive provenance signal, but it cannot be the sole source:
replicas written before that field existed deserialize as false even when
their concrete placement is still an active zero-cost location. Location
matching may add that legacy capacity to the exact-card observation, but a
missing or unknown location must remain unattributed rather than being guessed.

Each object has at most `MAX_COMPATIBILITY_ACCELERATORS` entries. Keys must be
non-empty exact configured identifiers and values must be nonnegative integers.
Within a minute, the newest controller observation replaces every map together
with the aggregate target/capacity fields. Old rows and mixed-version writers
use empty maps, which means exact-card history unavailable, not zero capacity.
No migration backfill invents a historical card assignment.

Each non-empty map envelope also stores the aggregate controller session,
version, replica unit, traffic/capacity targets, and capacity counts that the
exact values explain. Readers require both matching observation timestamps and
an exact aggregate-envelope match before returning the card maps. This second
fence covers an old HA writer winning an aggregate-only upsert at the same
PostgreSQL timestamp as a new writer; pressure peaks remain independent.

The history UI keeps one synchronized range and presents three operational
views:

1. **Traffic and capacity.** The default aggregate view keeps the existing
   traffic target (with hysteresis), traffic-or-reservation target, ready,
   provisioning, and non-failed tracked capacity lines. An `Accelerators`
   view renders small multiples for demand target, independent warm retention,
   cold-launch authority, ready, provisioning, and non-failed tracked capacity
   by exact card. It must never stack overlapping target concepts.
   In particular, warm retention is not added to demand target and cold-launch
   authority is an incremental scale-up signal, not desired total capacity.
   Reserved fill remains its own line in the reserved view, rather than
   inventing an exact-card version of the aggregate maximum.
2. **Reserved capacity.** Aggregate and exact-card views show reserved-fill
   target, zero-cost ready capacity, and free reserved slots. These are
   separate from traffic demand so an already-provisioned reserved cluster is
   visible as supply rather than unexplained demand.
3. **Demand pressure.** Keep request arrivals, peak in-flight, peak queued,
   and rejections aggregate. Flexible compatibility sets cannot be truthfully
   labeled as queue depth "on A100" before the allocator chooses a target.
   The exact-card demand-target history is the allocation result. The
   cold-launch-authority history is the card-specific scale-up graph.

Regression coverage must include a logical-slot all-compatible wave in which
the demand target is attributed only to L4 even while A100 and
A100-80GB serve or retain already-running work. That warm work appears only in
`warm_retention_target_by_accelerator`, and compatible warm supply may leave
`cold_launch_authority_by_accelerator` empty. When more capacity is actually
required, cold authority contains only L4. A constrained A100-only wave must
instead target and authorize A100.

Mixed-version regression coverage must use the production-shaped multi-card
case where old A100 rows back part of an adopted target but current compatible
demand belongs on L4. A fresh, complete, explicitly-proven non-downscale tick
must move the latest-version reconciliation target and paid authority to L4,
then a second tick after that L4 capacity commits must allow the old A100 rows
to retire without creating a paid A100 replacement. Companion cases must prove
that 40 old L4-only units remain L4, an exact A100 floor or demand retains A100
authority, a downscale-held exact-card retry stays on its adopted card, and
stale or incomplete telemetry publishes explicit zero paid authority. With a
fresh report but no accepted compatibility history for running old A100 work,
the allocator must publish an empty explicit-ownership subset, preserve A100
only for the backed same-card replacement, and never follow its synthesized
default-all L4 placement. A partial explicit profile may authorize only its own
L4 units plus the actually backed A100 replacement; vanished or aggregate
padding must receive no paid authority. An explicit flexible profile covering
the complete target is the positive companion that permits full L4 movement.

Replica-manager tests must distinguish paid from zero-cost demand placement by
the typed launch result. Paid planned capacity consumes the matching authority;
zero-cost planned capacity does not. The same tests must prove that exhausting
paid authority cannot fall through to a paid location and that no
`ReplicaInfo` lookup participates in the debit.

Allocator regressions must separately cover latest-only cold scale-from-zero
for an aggregate minimum and for headerless/default-compatible queued demand;
both receive paid authority on the selected cheapest card. Inferred in-flight
overflow remains outside paid ownership. A timed-out degraded READY A100 with
zero observed slots must contribute zero to both allocation and actuation,
select L4 for a flexible explicit request, and emit exactly one bounded
replacement ID. The companion row marked as the bounded unknown-capacity
replacement remains committed and cannot recursively launch another wave.

The all-compatible case must also be replayed from the production recovery
shape: the aggregate target is held above fresh raw demand, the process-local
per-card map starts empty, and committed L4, A100, and A100-80GB supply already
exists. Recovery must publish the entire held demand target on L4, keep any
physical A100 retention separate, and authorize no expensive-card cold launch.
This case specifically guards against reconstructing demand from committed
supply after a controller rollout.

The aggregate/card switch changes only presentation. Aggregate values remain
stored directly as the backward-compatible control-plane contract. Per-card
demand reconciles to the autoscaler's traffic target before generic
`num_overprovision`; the aggregate history keeps the final target after that
overlay. The UI labels those values separately instead of implying their sums
must always match. `A100` and `A100-80GB` are always separate series and legend
entries. Physical-machine lifecycle history remains a separate chart from
logical serving slots; neither is relabeled as the other.

Tests:

- Add status/API schema tests and dashboard connector/component tests for missing additive fields, totals, fill overlays, exact-card history, aggregate/card switching, and separate A100 rows.
- Assert when `num_overprovision` is absent that the displayed global demand
  target equals the per-card demand-target sum. With overprovisioning, show the
  aggregate overlay separately rather than assigning it to an invented card.
- Add PostgreSQL migration/upsert/serialization tests proving last-observation
  map semantics, empty-map mixed-version compatibility, incarnation fencing,
  retention, and exact `A100` versus `A100-80GB` keys.
- Add controller coverage proving a legacy replica without persisted
  `is_zero_cost` provenance is attributed from the autoscaler's exact active
  reserved-location matcher, while an unknown placement remains
  unattributed.

Acceptance gate:

- An operator can explain every replica above the demand target as either a hard floor, provisioning lag, or reserved fill, both now and across the retained history; A100 variants are never visually combined.

### Milestone 6 - Rollout and production validation

The controller/LB demand protocol is explicitly mixed-version safe. A new
controller advertises support for the replaceable compatibility-queue gauge in
each successful sync response. A new LB continues publishing the legacy
pre-admission arrival event until that acknowledgement is observed, and treats
a later missing acknowledgement as a controller rollback. On rollback it
backfills every already-waiting request into the legacy event feed exactly once.
An old LB ignores the additive response field; an old controller ignores the
additive queue gauge. Services without an advertised exact-card catalog always
retain legacy aggregate arrival scaling because they cannot publish bounded
card profiles.

Every new LB demand sync echoes the service version of the routing spec it has
already applied. The controller returns its applied service version in every
sync response and accepts compatibility gauges as complete only when those
versions match. A first-sync, old-LB, or delayed old-catalog report may still
refresh aggregate demand, but it cannot re-arm exact-card scaling after a
catalog update. The LB advances its echoed version only after it atomically
applies the response's routing spec, exact-card catalog, and route set. A
spurious empty endpoint response retains both the prior snapshot and its prior
version. Within the controller, autoscaler catalog/version publication,
routing-spec publication, the applied-version fence, compatibility validation,
and compatibility-gauge ingestion share one routing-state lock. A report can
therefore be interpreted wholly before an update (and then cleared by it) or
wholly after it (and rejected if stale), never across the transition.
If the applied version changes while a controller sync is resolving its
replica snapshot, the response intentionally withholds the routing spec; the LB
acknowledges the demand batch but retains its last coherent routing epoch until
the next complete sync.

On controller recovery, a logical autoscaler reconstructs its aggregate safety
target from current committed capacity before it considers a lower fresh demand
target. Its published exact-card demand map attributes that held aggregate from
fresh compatibility profiles and marginal-cost ordering, not from the
committed exact-card inventory. Thus default-all recovery publishes the entire
held demand target on the cheapest compatible card even while more expensive
cards remain physically committed. Committed exact-card inventory seeds a
separate process-local actuation map, where bounded replacement waves and
non-preemptive retirement preserve serving coverage. The empty recovered
demand map is not itself a card increase and must not reset the aggregate
downscale window. The held aggregate demand map and private actuation map
remain independently reconcilable through the normal graceful downscale delay.

Recovery of queued launch rows is deliberately stricter than recovery of the
retirement target. Durable demand-owned PENDING rows and interrupted
PROVISIONING re-drives wait for a fresh complete exact-card target and are
revalidated at cloud-mutation time. Missing or stale compatibility telemetry
can therefore retain ready capacity conservatively, but it cannot authorize a
new paid cold start. This does not change request-priority semantics: no READY
replica is preempted, and normal scale-down keeps its graceful drain contract.

Production validation exposed a second, independent cold-launch authority
edge. Running and occupancy-unknown work is pinned to its current exact card so
that a card migration cannot preempt it. That pin is a retention floor, not a
claim that every unit of work above the card's already-materialized serving
capacity needs a new replica on the same card. Otherwise a brief concurrency
burst or occupancy-unknown wave can turn 126 units already executing on 121
ready reserved A100 slots into an apparent five-slot A100 shortfall, even when
the aggregate fleet is short only one slot and L4 is the cheapest compatible
paid cold start.

For concurrency scaling, cap fixed in-flight and unknown work on each exact
card at the latest non-retiring materialized work capacity of that card. Keep
that capped portion exact so every serving replica carrying work remains
protected. Reintroduce only the non-negative overflow as one all-configured-card
compatibility profile at the lowest numeric priority before allocation. The
overflow represents additional pressure from work that is already being
served; it does not move or preempt those requests. New queued and rejected
profiles retain their original priority and compatibility sets, so explicit
A100-only demand can still justify an A100 cold start while flexible or
headerless overflow uses ready compatible supply and then the cheapest
compatible paid card. Preserve total work exactly: capped fixed work plus its
flexible overflow must equal the original fixed work in both logical-slot and
physical-replica modes.

In logical mode, count that flexible overflow as allocator-attributed work
before shaping any offered-arrival floor gap. This prevents the same accepted
work from reappearing through the retained arrival window. The remaining
arrival gap still keeps its recorded priority and compatibility, so an
A100-only arrival profile is allocated before the lowest-priority flexible
overflow. Per-card rounding is then applied to those distinct profiles without
turning oversubscribed in-flight work back into same-card cold-launch authority.

HA cutover snapshots carry both accepted compatibility arrivals and the live
compatibility queue, recent-rejection, and exact-card in-flight gauges. Arrival
events transfer to the promoted controller state exactly once; replaceable
gauges remain conservative per-profile or per-card maxima for the bounded
handoff interval. Repeated active heartbeats must not replay the same arrival
batch, while old snapshot JSON that lacks these additive fields continues to
deserialize as empty compatibility demand. A report is complete for
compatibility-aware concurrency only when all aggregate and compatibility
gauges are present and valid; a mixed-version report therefore keeps the prior
handoff floor and cannot authorize a card-specific downscale. Each durable HA
demand snapshot also records its routing version. Aggregate timestamps,
queueing, rejection, and in-flight safety floors may cross a version boundary,
but exact-card arrivals and queue/rejection profiles are merged only when the
snapshot and promoted LB report have the same non-legacy routing version.

An in-process autoscaler replacement during `sky serve update` transfers the
windowed compatibility arrivals and current queue/rejection gauges together
with aggregate timestamps and in-flight state. The replacement must never
retain total demand while forgetting its exact-card constraints. Dynamic state
also carries the source exact-card catalog and compatibility-completeness bit.
When a concurrency/QPS mode switch changes that catalog, the replacement drops
the old exact-card profiles but keeps aggregate demand. Until a version-matched
LB report establishes the new card constraints, it represents that aggregate
demand as one flexible profile over every newly configured exact card, combines
it with hard per-card floors, and allocates it with the normal supply-aware
ordering. This preserves legacy scaling without reinterpreting an old A100-only
profile as H100-only demand. The same aggregate fallback replaces, rather than
supplements, any stale exact profiles after an incomplete report, so all
aggregate arrivals are counted exactly once. An incomplete delayed report
cannot re-arm the cleared profiles.
Because completeness describes only the current report and its replaceable
gauges, a later complete report does not retroactively attribute older
aggregate-only arrivals still inside the QPS window. The allocator subtracts
only windowed exact arrival counts from aggregate arrivals and adds the
non-negative remainder as one all-configured-card profile. It never subtracts
the queued compatibility gauge, which is additive outstanding demand rather
than a duplicate arrival feed.
Both retained concurrency and instance-aware QPS autoscalers serialize version,
performance-profile, exact-card catalog, demand ingestion, reserved-supply
updates, dynamic-state snapshots, and scaling decisions under their own state
locks. The controller uses each autoscaler's atomic
`update_version_and_accelerator_shapes` transition, so the decision thread can
never launch from a new version using the previous version's card catalog.
For an autoscaler type replacement, the controller also holds the routing-state
lock from the old autoscaler's locked dynamic-state dump through restore,
version/catalog initialization, and publication of the replacement. This both
prevents torn dumps and prevents an authoritative LB report from landing on the
old object after the snapshot and being lost during the swap.
Fleet-size-dependent cluster-handle resolution must not run as one database
query per replica while either autoscaler state lock is held. Before entering a
decision critical section, batch-read the cluster records needed for uncached
shapes or costs and expose that immutable tick snapshot to every shape/cost
pass. A
not-yet-completed exact-card launch may use its current accelerator override as
its planned shape without a cluster read; it is not memoized across ticks, so a
later failover or completed launch is still re-resolved. The LB sync handler
may then acquire the state lock and refresh the 60-second exact-card authority
even while the next fleet snapshot is being fetched. Keep the existing
fail-closed freshness fence rather than lengthening it to hide controller
starvation.
Concurrency retains the windowed exact-card arrival profiles solely for this
handoff, while continuing to size outstanding work from live gauges and using
only aggregate arrivals as its stale-signal floor. The profile key is emitted
even when empty, so a QPS replacement can distinguish a complete new-format
snapshot from an older dump that could retain aggregate arrivals while losing
their card constraints. The latter uses the same all-configured-card aggregate
fallback until a fresh, version-matched LB report.

1. Land and deploy SkyPilot schema/status changes with behavior disabled by absence of the new header/map.
2. Land compatibility queue and per-card autoscaling behind a service flag; deploy to a test multi-card fleet.
3. Verify the capacity endpoint advertises compatibility version 1 and distinct configured cards.
4. Enable per-card floors and reserved integration in test, then run cold, warm, reserved, saturation, retry, and service-update scenarios.
5. Deploy the SkyPilot release to production and verify its exact release/health/capability response.
6. Land/deploy boltz-platform propagation only after production advertises version 1.
7. Enable platform caller/UI selection gradually, starting with omission/default-all and then explicit subsets.

Production checks:

- Cold fleet + flexible request starts the cheapest compatible paid card.
- Ready reserved A100 + flexible L4/A100/H100 request uses A100 without launching L4.
- Ready reserved A100 + ready paid L4 + flexible L4/A100 request uses A100 when no constrained peer needs it, allowing L4 to become idle; with an equal-priority A100-only peer, match that peer to A100 and the flexible request to L4.
- Large flexible backlog + same-priority A100-only request gives the next A100 slot to the constrained request with no preemption.
- High flexible versus low constrained demonstrates numeric priority dominance.
- `A100` traffic never reaches `A100-80GB`, and vice versa, unless both are explicitly in the compatibility set.
- Card-specific queue depth, target, floor, fill, free reserved slots, and actual replica counts reconcile in API metrics and UI.
- Removing demand causes graceful per-card scale-down; reserved physical capacity or configured fill remains clearly reported as extra supply.
- Retire one ready reserved A100 while default-all or `L4/A100` demand remains.
  Confirm the cold-launch target moves the missing slot to L4 immediately and
  no paid A100 replacement is created. Delete the retired row before target-map
  hysteresis adopts L4 and confirm the fence remains on L4. Bench every L4
  location and confirm flexible demand waits for L4 rather than cold-starting
  A100. Repeat with A100-only demand and confirm that A100 remains a valid cold
  target.
- Queue a logical wave with `A100: 165`, start only part of it, then publish a
  fresh complete `A100: 93` target while at least 93 A100 slots are already
  READY or PROVISIONING. Confirm that not-yet-started paid A100 rows make zero
  cloud launch calls. Repeat across a controller restart and confirm the same
  result from recovered PENDING and interrupted PROVISIONING rows. Confirm
  excluded never-ready rows are cleaned up without a new `sky.launch`, READY
  replicas retain graceful downscale, and reserved-fill rows remain untouched.
- During a rolling update, mark latest-version physical and logical replicas
  preempted while their derived status is still READY. Confirm they do not
  authorize retirement of healthy old-version serving coverage.
- During a mixed-version update, retain old A100 replicas serving
  L4-compatible work and publish a fresh complete compatibility snapshot whose
  supply-aware latest-version placement is L4. Confirm the latest version
  launches only the authorized L4 replacement wave, old A100 work is not
  preempted, a subsequent fresh tick retires idle old A100 only after compatible
  latest READY coverage exists, and no paid A100 launch occurs. Repeat with a
  stale or incomplete snapshot and confirm paid launch authority is explicitly
  empty.
- Replay representative traffic with measured per-request service times and
  per-card startup distributions. Low-priority requests must complete queue
  admission before their 600-second timeout, default-all/L4-compatible traffic
  must create zero paid A100-or-larger cold launches, and paid-card busy time
  should converge toward 80 percent over stable-demand windows rather than
  leaving a large idle paid tail.

## Failure handling and invariants

- Fail closed on unknown or empty explicit compatibility. Never silently widen an invalid subset to all cards.
- Missing is the only path that means all configured cards.
- A waiter has exactly one lifecycle state and at most one granted card reservation.
- Sum of compatibility-group demand is total demand; it is not multiplied by compatible-card count.
- Sum of demand targets by card equals the aggregate demand target. Fill is an overlapping overlay, not added demand.
- Fresh broker-granted reserved slots are consumed independently of demand-target ordering, but a fill decision never takes total nonterminal plus planned demand capacity above `max_replicas`.
- Per-card floors are hard for serving replicas; reserved physical machines are supply and do not satisfy a serving floor until a replica is launched.
- All comparisons use exact canonical IDs. No code path may use `startswith`, hardware regex groups, generic `A100*`, or suffix stripping for compatibility.
- If exact-card telemetry is missing or stale, do not route or scale on a guessed family. Mark the replica/card unknown, exclude it from compatibility grants, and surface degraded status.
- If no compatible resource can be provisioned before queue timeout, return the existing retryable no-capacity result with the requested exact set in structured diagnostics.

## Alternatives rejected

1. **One queue/service per card.** This duplicates flexible requests, creates cross-queue races, and prevents the scheduler from using live/reserved supply coherently.
2. **Platform chooses one card before SkyServe.** The platform cannot see replica occupancy or exact reserved slots at dispatch time, so it would cold-start cheaper hardware while compatible larger hardware is already paid for and ready.
3. **Always choose the first compatible card.** List order is a weak proxy for marginal cost and ignores already-ready/reserved supply.
4. **Always choose cheapest, including warm capacity.** This causes unnecessary cold launches and latency while compatible capacity is idle.
5. **Card-family matching.** This violates the explicit A100/A100-80GB isolation requirement and makes floors, capacity, and billing irreconcilable.
6. **Treat reserved physical machines as hard serving floors.** A physical slot without a ready SkyServe replica cannot immediately serve a request; conflating the layers hides provisioning work and scale-down behavior.
7. **Preempt flexible work when constrained work arrives.** Explicitly out of scope; priority affects the next grant and future scaling only.

## Manual test plan

Use test SkyServe services configured with exact L4, A100, A100-80GB, and H100
resources, one with distinct per-card QPS targets and one with
`target_concurrency_per_replica` in logical GPU-slot mode.

1. Start with zero serving replicas and no free reserved capacity. Submit a default/missing-field request and confirm the cheapest compatible paid card is targeted.
   Repeat with the first configured card available only at a zero-cost
   reserved location and the second card available at a positive paid price.
   Confirm default-all demand targets the paid card, while exact demand for the
   reserved-only card keeps that exact target and waits for reserved fill.
2. Expose a free reserved A100 slot (and no ready replica), submit the same request, and confirm the exact A100 resource override is selected before paid L4.
3. Keep a reserved A100 replica and a paid L4 replica ready with spare concurrency. Submit only a flexible L4/A100 request and confirm reserved A100 dispatch. Then add an equal-priority A100-only request and confirm the matcher assigns A100-only to reserved A100 and flexible to paid L4.
4. Fill all A100 slots with flexible requests, queue an older same-priority flexible request and then an A100-only request, release one A100 slot, and confirm A100-only runs next. Confirm existing work was not interrupted.
5. Repeat with flexible priority 50 and A100-only priority 20; confirm the priority-50 request runs first.
6. Queue equal-priority `{L4,A100}` and `{A100,H100}` requests. With L4 unavailable and A100/H100 ready, confirm the maximum matching uses A100/H100. With only A100 and no viable alternatives, confirm FIFO. Then expose paid L4 versus paid H100 fallbacks and confirm the ready A100 avoids the worse fallback while L4 is the cold target.
7. Submit explicit `A100` and `A100-80GB` requests and confirm exact isolation. Submit both together and confirm either exact type is allowed.
8. Start a compatible replica provisioning and confirm it prevents a duplicate launch without receiving traffic; exceed/fail its startup SLA and confirm reserved/paid residual capacity is replanned.
9. Set A100 floor zero. Remove A100 demand and confirm paid replicas drain after grace; then repeat with reserved fill enabled and confirm any retained replica is shown as fill, while physical reserved capacity remains separately visible.
10. Keep paid replicas above the reserved fill target, expose several fresh broker-granted reserved slots, and confirm every slot launches zero-cost-only without changing the demand target. Repeat near `max_replicas` and confirm only hard-ceiling headroom launches; include old-version draining rows in the ceiling check.
11. Set distinct floors for all cards, drive mixed demand, and reconcile global target, per-card targets, actual states, fill targets, and free reserved slots through API, metrics, and dashboard.
12. Change the active service version while requests are queued; confirm compatible waiters retain sequence and an emptied intersection receives retryable 503.
13. Point the platform test client at a pre-capability LB: confirm an omitted field keeps default-all behavior, an explicit subset may spill only to an exact-compatible concrete provider, and otherwise fails closed. Point it at version 1 and confirm each selected fleet receives only its exact non-empty intersection plus the numeric priority header on every retry.

## Completion criteria

- All milestone acceptance gates and the manual test plan pass.
- SkyPilot unit suites for queueing, instance-aware autoscaling, reserved fill, service schema/status, and dashboard pass after `bash format.sh` on changed files.
- boltz-platform typecheck/lint/unit suites for compute types, routing, placement, SkyPilot provider, and retries pass using the repository's `mise` environment.
- The exact deployed SkyPilot release advertises capability version 1 before platform explicit subsets are enabled.
- Production observability can distinguish demand, hard floors, active reserved fill, and unconsumed reserved supply per exact accelerator.
- A held or recovering per-card target cannot cold-replace lost warm A100-or-
  larger capacity for traffic that is also compatible with a cheaper paid
  card. Production has no GCP A100 spot replicas for the current default-all
  workload after the bounded graceful drain.
- Backtesting reports queue-timeout rate, per-priority wait distributions,
  paid utilization by exact card, cold launches by exact card, and cost. For
  the current workload it shows no low-priority timeout at 600 seconds, no
  paid A100-or-larger cold launch, and stable-window paid utilization centered
  near the 80 percent operating objective.
