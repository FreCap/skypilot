# SkyServe: adaptive demand estimation

## Problem

The concurrency autoscaler sizes every target from two hand-set numbers:

- `replica_policy.expected_request_duration_seconds` converts queued,
  rejected, and arriving requests into concurrent work. It multiplies into
  every demand estimate.
- `replica_policy.initial_provision_lead_time_seconds` (see
  [SLA-aware scale-up](serve-sla-aware-scaleup.md)) sets how much of a
  request's SLA budget is already spent before new capacity can serve.

Both drift, and neither is verified against reality. The lead is the worse
of the two: it is a property of spot capacity and cloud provisioning latency
that changes hour to hour, and no operator re-tunes it.

Correction (2026-07-23): an earlier revision of this document asserted that
boltz-l4-fleet's configured 30 s duration ran against an observed 45-60 s.
That figure was an assumption carried over from the simulator's default
service time, not a measurement. The first production prediction-time data
says the opposite: across 547 completions, 97% finished within the
(10 s, 30 s] bucket, and the fleet's configured 30 s is a reasonable value.
The case for measuring rests on drift and on the unknown lead, not on that
fleet being misconfigured.

Meanwhile the system already measures both facts and throws them away for
sizing purposes:

- The load balancer reports per-minute prediction-time histograms
  (`prediction_time_history`), which the controller persists for the
  dashboard. It never reaches the autoscaler.
- Every replica row carries `created_at` and
  `status_property.first_ready_time`, and `time_to_ready_seconds` is already
  derived from them for status reporting. The autoscaler receives those rows
  each tick and ignores the timing.

There is precedent for preferring measurement over declaration in this same
autoscaler: probed `observed_slots_by_replica_id` already supersede the
controller's planned per-replica capacity.

## Behavior contract

Adaptive estimation is **on by default**
(`replica_policy.adaptive_demand_estimation`, default `true`; set it to
`false` to pin static configured estimates). A service that never touches
its configuration measures its own workload:

0. **The provisioning lead is a seed, not a setting.**
   `initial_provision_lead_time_seconds` defaults to `auto`: assume
   `AUTOSCALER_DEFAULT_PROVISION_LEAD_SECONDS` (10 minutes) until the
   service has measured its own launch-to-ready time, then use the
   measurement. Provisioning a GPU replica takes minutes on every supported
   cloud, so assuming zero would size a young service's first bursts as if
   capacity were instant. An explicit number pins the seed (still superseded
   by measurement); an explicit `0` opts out of lead accounting entirely.

Then, whenever adaptive estimation is enabled:

1. **Measured request duration supersedes the configured duration.** The
   autoscaler folds newly completed requests from the load balancer's
   prediction-time histograms into an EMA (alpha 0.2). Each bucket
   contributes its geometric midpoint, the unbiased summary of a log-scale
   bucket. Only `succeeded` outcomes count: a fast failure describes an
   error path, not how long serving occupies a slot.
2. **Observed launch-to-ready supersedes the configured lead.** Each replica
   that reaches ready contributes one `first_ready_time - created_at`
   sample, capped at the 50 most recent; the estimate is their p75. Sizing
   against the median would leave the slower half of launches arriving after
   the budget they were sized for.
3. **Configuration is the fallback, never overridden blindly.** A measured
   value supersedes only while it holds enough evidence
   (20 completions, 5 launches) and is fresh (6 h). Otherwise the configured
   value stands, which is also the cold-start behavior.
4. **Estimates survive a controller restart.** Both are persisted in the
   autoscaler's dynamic states. Re-learning from zero would silently revert
   to configuration during warm-up, precisely when a restart under load can
   least afford an undersized target.
5. **Each replica is sampled once**, and the dedup ledgers are bounded (live
   replica ids only; histogram buckets pruned to the freshness window). A
   load balancer re-reports a histogram bucket until the controller durably
   accepts it, so only the positive delta may contribute.

Effective values are exposed in `autoscaler.info()`
(`effective_request_duration_seconds`, `effective_provision_lead_seconds`,
`measured_duration_samples`, `provision_lead_samples`) so an operator can see
what the fleet is actually sizing against.

## Evidence (simulation)

Same simulator as the SLA-aware design, extended so the configured duration
and the true service time can differ, with estimators that mirror the
implementation (EMA of completions, p75 of observed leads). Failure rate is
offered-request-weighted, 3 seeds.

**Configuration correct (config 30 s, truth 30 s): adaptive is neutral.**
Failure rates match the static policy within noise (e.g. slow ramp 0.5% vs
0.4%), at up to +13% replica-hours from tracking reality more closely.

**Configuration wrong (config 30 s, truth 60 s: the fleet's real state):**

| scenario | static config | adaptive | mean wait, static -> adaptive |
|---|---|---|---|
| slow ramp | 0.1% | **0.0%** | 24 s -> 11 s |
| fast ramp | 2.9% | **1.9%** | 52 s -> 23 s |
| mixed priority burst | 7.7% | **6.9%** | 42 s -> 21 s |
| sustained | 5.1% | **4.4%** | 31 s -> 21 s |
| step burst | 9.2% | **8.3%** | 44 s -> 22 s |

Cost is +11-17% replica-hours, which is the fleet being correctly sized for
work that was always there.

**The knob stops needing to be right.** A policy with no configured lead at
all that learns it from scratch reaches 0.0% on the slow ramp and 2.3% on
the fast ramp, against 0.0% and 1.9% for a correctly hand-tuned 540 s.
Operators no longer have to guess a value that only a measurement can know.

**The new default vs today's default**, measured end to end (default-on
adaptive with the 10-minute `auto` seed, against the current shipped
behavior). This is the change every service gets on upgrade:

| scenario | today | new default | mean wait | replica-hours |
|---|---|---|---|---|
| slow ramp | 1.4% | **0.3%** | 60 s -> 7 s | +23% |
| fast ramp | 5.8% | **2.5%** | 79 s -> 14 s | +34% |
| sustained | 5.9% | **5.8%** | 20 s -> 8 s | +21% |
| cold spike | 9.6% | **9.5%** | 23 s -> 11 s | +37% |
| step burst | 9.6% | 9.7% | 32 s -> 13 s | +35% |

With the duration also misconfigured (the fleet's real state) the same
comparison is 2.8% -> 0.0% on the slow ramp and 9.2% -> 1.8% on the fast
ramp, with waits falling from ~150-200 s to ~10-25 s.

The cost is real and is the honest price of the new default: roughly
+20-37% replica-hours during load, because the fleet is now sized for work
that was always there rather than planned to arrive at the deadline. A
service that prefers the old economics sets `adaptive_demand_estimation:
false`, or `initial_provision_lead_time_seconds: 0` to keep measurement
while restoring pure deadline discounting.

**The 10-minute seed earns its place.** Starting from no assumption
(`0`) and learning only from observed launches gives 0.8% on the slow ramp
and 4.8% on the fast ramp, against 0.3% and 2.5% with the seed: the seed
covers the cold-start window before the first launches have been measured,
which for a young or long-idle service is exactly when a burst arrives.

## Alternatives considered

- **Opt-in instead of default-on.** Rejected by product decision: a
  correct default matters more than an inert upgrade, and the whole point
  is that nobody re-tunes these numbers. The cost of the new default is
  quantified below and an explicit `false` (or an explicit lead) restores
  the previous behavior for a service that wants it.
- **Bucket upper bounds instead of midpoints.** Shipped first, then
  reverted against production data: the buckets are log-scale and wide (the
  10 s-30 s bucket spans 3x), and with 97% of real requests inside that one
  bucket the upper bound inflated the estimate 1.70x (29.8 s vs 17.5 s).
  Conservatism in sizing belongs in the knobs an operator can see and tune,
  not hidden in a histogram summary where it compounds with them
  invisibly.
- **Percentile duration (p75) instead of an EMA mean.** The duration feeds
  aggregate work (count x duration), where the mean is the correct
  aggregate; a percentile would systematically over-size. The lead is a
  latency to beat, not an aggregate, so it uses p75.
- **Clamping the measured value to a multiple of configuration.** Rejected
  as re-introducing the stale number the feature exists to escape. Blast
  radius is already bounded by `max_replicas` and the scale-up wave limiter.

## Test plan

Unit tests in `tests/unit_tests/test_concurrency_autoscaler.py`
(`TestAdaptiveDemandEstimation`) and `test_serve_concurrency_spec.py`:

- measured duration supersedes config; below the sample floor it does not;
  disabled feature never supersedes; a stale measurement falls back.
- a re-reported histogram bucket is not double counted; a histogram version
  mismatch is dropped; `failed` outcomes never define service time.
- measured lead supersedes config at p75; never-ready sentinels are ignored;
  each replica is sampled once.
- both estimates survive a dump/load restart cycle.
- an end-to-end sizing case: measured 60 s duration weights a 600 s-timeout
  queue at 1.0 where the configured 30 s would weight it at 0.5.
- spec YAML round-trip and strict boolean validation for the new knob.

Manual: enable on boltz-l4-fleet, then confirm via `autoscaler.info()` that
`effective_request_duration_seconds` converges toward the dashboard's
prediction-time distribution and `effective_provision_lead_seconds` toward
observed launch-to-ready, and that the demand target rises accordingly.

## Rollout

Default-on for every service, including existing ones, at the next
controller restart. Old persisted specs deserialize with the field absent,
which resolves to enabled, and with no configured lead, which resolves to
the 10-minute `auto` seed. Both effects are intentional; the cost table
above is what an operator should expect to see.

No API version bump: the new demand-feed field is additive and an older
controller simply never sends it, leaving the autoscaler on configured
values. Opt-outs (`adaptive_demand_estimation: false`, or an explicit
`initial_provision_lead_time_seconds`) are available per service without a
code change.

TODO(FreCap): once the fleet has run on measured values, revisit whether
`expected_request_duration_seconds` should become optional rather than
required for logical services.
