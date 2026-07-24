# Restart-safe SkyServe cost rebalancing

_Status: implemented; PostgreSQL CI and production canary pending_
_Created: 2026-07-24_
_Last updated: 2026-07-24_

## Goals

SkyServe services with a placement catalog should converge from expensive
fallback capacity back to materially cheaper capacity without requiring a
service update. The policy is enabled by default for services with
`spot_placer`, remains explicitly disableable, and is safe to use with
`reserved_capacity_fill`.

The complete placement feedback and replacement protocol must survive a
controller or API-server restart:

- only structured provider capacity and quota failures bench a location;
- the reason and original wall-clock expiry of a bench are durable;
- paid replacement launches participate in the existing atomic exact-pool and
  per-service admission envelopes;
- continuous cheaper-candidate stabilization survives restart;
- the replacement-to-incumbent pair, readiness gate, off-route transition,
  zero-occupancy proof, and teardown intent remain durable.

The dashboard must identify the exact instance type and distinguish controller
eligibility from provider inventory. It must expose why and until when a
location is suppressed.

## Non-goals

- Predicting live Spot inventory without a provider launch.
- Replacing an incumbent before a healthy replacement is serving.
- Moving a replica to a different accelerator contract or to less serving
  capacity.
- Rebalancing during an ordinary scale-up, scale-down, rollout, or recovery
  wave.
- Replacing the reserved-capacity broker's authority over zero-cost capacity.
- Increasing the configured demand `max_replicas`; temporary overlap remains
  bounded by the replacement policy.

## Existing behavior and gaps

The current opt-in `cost_rebalance` implementation already persists a
replacement row with its incumbent ID, waits for the replacement to become
ready, persists the incumbent off-route, and waits for explicit load-balancer
zero occupancy before teardown. Those phases recover from restart.

Four gaps prevent enabling it by default:

1. Every failed placer-backed launch currently enters the process-local
   `failed_spot_locations` set, including security-group creation errors,
   request throttling, and other control-plane failures. The shared paid
   authority separately recognizes typed capacity errors, so the local and
   global views can disagree.
2. Process-local benches disappear on controller restart. Paid launches are
   still protected by the PostgreSQL paid-pool cooldown, but zero-cost and
   local placement state can be retried immediately and dashboard state loses
   its reason.
3. The cheaper-candidate stabilization clock uses `time.monotonic()` in the
   autoscaler and restarts from zero after every process restart.
4. A cost-rebalance replacement is a fresh pinned launch but bypasses paid
   claim admission. Multiple services can therefore probe the same cheap pool
   outside the global exact-pool bound.

`cost_rebalance` and `reserved_capacity_fill` are also rejected at spec
validation even though their responsibilities can be made disjoint.

## Public contract

For a service with `spot_placer`, an absent or `null`
`replica_policy.cost_rebalance` enables the policy with these conservative
defaults:

```yaml
service:
  replica_policy:
    cost_rebalance:
      min_savings_fraction: 0.3
      max_parallel_replacements: 1
      stabilization_seconds: 300
```

`cost_rebalance: false` explicitly disables new replacements. `true` is
accepted as shorthand for the defaults. An object continues to configure the
existing fields. A service without `spot_placer` has no candidate catalog, so
the implicit default is inert; explicitly configuring `true` or an object
without a placer remains an error.

Existing persisted specs whose `_cost_rebalance` field is absent or `None`
adopt the new default when they have a placer. Persisted `False` is the durable
opt-out.

The policy launches only when all of the following remain true:

- no ordinary autoscaling decision is pending;
- no old service version is nonterminal;
- the incumbent is latest-version, ready, routed, and not already paired;
- the candidate preserves exact accelerator and logical-capacity policy;
- its cached unit price is at least 30% lower by default;
- eligibility has been continuous for the stabilization interval;
- fewer than the configured number of replacement pairs exist; and
- paid-capacity admission accepts the exact candidate when its cost is
  non-zero.

The selected replacement is pinned to the exact catalog location. The
incumbent is retained until the replacement is ready. It is then persisted
off-route and teardown starts only after a fresh matching load-balancer report
proves zero occupancy. Disabling the policy while a pair exists keeps the
incumbent and retires the replacement.

## Failure classification

`ResourcesUnavailableError` alone is not capacity evidence. The launch path
classifies every terminal provider failure from structured provider error
codes carried by its explicit exception/failover chain:

- `capacity`: known physical-capacity codes only;
- `quota`: known regional/global quota codes only;
- `other`: an unknown code, a mixed capacity/non-capacity batch, or a failure
  without a structured provider code.

Quota dominates a batch containing only recognized capacity and quota codes.
Any unrecognized code makes the batch `other`.

Capacity and quota failures:

- fail fast at the selected exact location;
- bench that location and invalidate older queued siblings;
- persist the durable bench reason and observation time;
- close the matching paid-capacity pool through the existing durable negative
  epoch; and
- remain separately labeled for history and diagnostics.

Capacity benches only the selected exact location. Quota additionally benches
the service's catalog candidates with the same cloud, region, purchase mode,
and accelerator shape, because retrying sibling zones or instance types cannot
repair a regional accelerator/Spot quota denial. The provider provisioning
path also retains its process-shared quota cooldown. A later exact success
reactivates that exact location; the remaining regional scope expires normally
rather than being cleared by a success that may have consumed the final quota
slot.

Other failures retain their bounded in-place retry. If those retries exhaust,
the replica fails, but its location stays eligible and no provider-capacity
negative evidence is created. Security-group assertions and
`RequestLimitExceeded` are therefore never availability evidence.

## Durable state

Migration 029 adds two nullable JSONB columns to the central `services` row:

- `spot_placement_state`: versioned exact-location benches containing the
  location, reason, wall-clock `observed_at`, and an optional separate
  `retry_reserved_at` for the one permitted expired-bench probe;
- `cost_rebalance_state`: versioned stabilized candidates containing service
  version, incumbent replica ID, exact candidate location, and wall-clock
  `first_seen_at`.

The columns are intentionally separate. The replica-manager refresher owns the
bench column and the autoscaler loop owns the candidate column, so concurrent
updates cannot overwrite unrelated state. Every write validates service
incarnation and controller owner. Service deletion removes both with the
parent row.

The replica manager restores exact matching benches before it re-drives
durable PENDING or PROVISIONING rows. Unknown and no-longer-cataloged locations
are ignored. A future timestamp is clamped to the controller wall clock; an
old timestamp retains its original expiry and is immediately probe-eligible
instead of restarting the cooldown.

Selecting that eligible probe persists `retry_reserved_at` without rewriting
the provider observation. This prevents a restart or launch burst from issuing
multiple probes in one window. Success clears the bench, a typed failure
replaces the observation and reason, and a generic/control-plane failure clears
only the probe reservation so it cannot silently turn into a new availability
window.

The autoscaler restores candidate state before its first decision. Entries are
accepted only for the current service version and a syntactically valid exact
location. The first decision reconciles restored entries against live
incumbents and the current catalog before any actuation. The state is bounded
to the most expensive eligible incumbents needed for the configured
replacement concurrency plus a fixed look-ahead:
`max(16, 4 * max_parallel_replacements)`, capped at 256. Invalid and duplicate
entries are dropped. A future `first_seen_at` is replaced with the current wall
clock; elapsed time is always clamped at zero.

Candidate state is persisted under the current owner fence before any
cost-rebalance scale-up decision is actuated. A failed or rejected persistence
write removes cost-rebalance scale-ups from that tick. Ordinary autoscaling is
not blocked. Candidate timestamps use wall clock only for persistence; elapsed
time is clamped at zero for backward clock movement and future timestamps.

Existing durable mechanisms remain authoritative for later phases:

- `ReplicaInfo.cost_rebalance_for_replica_id` persists the pair;
- paid-capacity claims persist unresolved paid replacements;
- replacement status persists readiness;
- `wait_for_idle_before_termination`, drain start, logical retirement fence,
  and teardown status persist retirement progress.

## Paid-capacity admission

A non-zero-cost cost-rebalance candidate acquires a paid-capacity claim in the
same transaction that persists its replacement replica. It consumes:

- the exact pool's current cold/proven/probe limit; and
- the service's 16-claim unresolved envelope.

It may not spill to a different location after admission denial because the
autoscaler decision is pinned. The next tick recomputes the cheapest candidate.
Recovery of an already-persisted replacement adopts/reuses its existing claim
and never acquires a duplicate.

Success and typed capacity/quota failure update the same durable pool evidence
as ordinary demand launches. Other failure releases the claim without
poisoning the pool.

## Reserved-capacity compatibility

When `reserved_capacity_fill` is enabled, the reserved-capacity broker remains
the only authority allowed to create zero-cost fill. Generic cost rebalancing:

- never selects a zero-cost candidate;
- never selects a zero-cost or `reserved_fill` incumbent; and
- may replace one paid incumbent with a cheaper paid candidate.

The existing fill overlay continues to launch zero-cost capacity within its
grant and hard ceiling. Once that capacity is ready, ordinary demand
autoscaling may retire paid surplus. This provides paid-to-free convergence
without bypassing broker grants or using cost-rebalance overlap for free
capacity.

## Dashboard and observability

Each live placement row includes:

- exact `instance_type`;
- controller eligibility (`ACTIVE`, `BENCHED`, or `PROBE_ELIGIBLE`);
- a statement that eligibility is cached controller state, not live provider
  inventory;
- durable bench reason and observation/next-probe timestamps;
- cached catalog price; and
- paid admission state when available (`open`, `saturated`, `cooldown`, or
  `probe`) with remaining exact-pool and service claims. This is advisory
  display state; atomic claim persistence remains authoritative.

The UI calls selectable locations “Eligible” rather than “Available”. It
shows the instance type on every chip so separate exact pools in one zone are
not visually merged.

Logs aggregate typed capacity and quota failure waves independently. Generic
failures keep their replica traceback but do not emit an availability warning.

## Compatibility, rollout, and rollback

Migration 029 is additive and nullable. Old binaries ignore both columns.
Downgrade retains them so a later forward deployment resumes the same evidence.
New binaries tolerate absent state during a mixed rollout.

Rollout order:

1. run migration 029;
2. deploy new API-server/controller code with existing Helm values reused;
3. canary default rebalancing on one large service while retaining
   `cost_rebalance: false` as the per-service kill switch;
4. verify typed failure ratios, claim bounds, replacement success, strict
   drains, request errors, and hourly cost;
5. allow the default to apply fleet-wide.

Rollback is the previous image or an explicit service update with
`cost_rebalance: false`. Existing pairs complete through the conservative
policy: the incumbent stays and the replacement retires. No rollback path
deletes durable evidence.

## Verification plan

- Spec/schema round trips for absent/default, `true`, object, and explicit
  `false`, including old pickled specs.
- Structured capacity, quota, mixed-code, security-group, and throttling
  classification tests.
- Replica-manager tests proving only typed capacity/quota failures bench,
  persist reason, invalidate siblings, and update shared evidence.
- Restart tests proving a bench retains its original expiry and reason.
- Cost-rebalance state dump/load tests proving stabilization continues across
  restart, discontinuity resets it, future timestamps fail conservatively, and
  persistence rejection suppresses actuation.
- PostgreSQL owner-fence and migration 028 -> 029 -> 028 -> 029 tests.
- Paid replacement claim tests for exact-pool saturation, service saturation,
  success, typed failure, generic failure, and recovery adoption.
- Combined `reserved_capacity_fill` tests proving zero-cost candidates remain
  broker-only while cheaper paid replacements still work.
- Pair recovery and strict idle-drain tests across controller restart.
- Placement API/dashboard tests for exact instance type, eligibility wording,
  reason, expiry, and paid admission.
- Focused Python and dashboard suites, real PostgreSQL lane, `format.sh`,
  mypy, pylint, dashboard lint/build, and `git diff --check`.

## Open gates

- Mandatory real-PostgreSQL CI. The local environment has no Docker daemon, so
  the PostgreSQL 028 -> 029 -> 028 -> 029 preservation test was added but
  skipped locally.
- Production canary and cost/SLA observation before fleet-wide rollout.

## Verification evidence

Completed locally on 2026-07-24:

- adversarial review of this exact design; accepted findings tightened quota
  scope, bounded durable state, kept paid admission authoritative, and made
  the diagnostic admission snapshot advisory;
- `format.sh --files ...`: YAPF, mypy (747 source files), pylint (10/10),
  dashboard ESLint, and Prettier passed; the wrapper's only nonzero condition
  was the expected report that the implementation remains unstaged;
- focused failover classification, Spot placer, paid-capacity, cost-rebalance,
  Serve state, controller, replica-manager, and migration unit suites passed
  sequentially;
- both real-PostgreSQL migration tests were collected and skipped because
  Docker is unavailable; the SQLite migration chain reached revision 029;
- 36 focused dashboard tests passed; and
- the optimized Next.js production dashboard build completed successfully.
