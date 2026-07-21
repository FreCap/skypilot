# Priority-aware queue patience and stable SkyServe scaling

## Status

Implemented and deployed on 2026-07-20. Exact feature commit
`332a3ed64266708c51bf87378a632755f93ff13d` received a Fable `PURSUE` verdict,
then merged as commit `ac0ade92d558fb5ee1fe421665318785c9b1ed1c` and was
released as SkyPilot `1.1.575`. The production `boltz-l4-fleet` policy was
applied as service version 36 with the 600/60-second priority thresholds,
normal and adaptive scale-up waves, five-minute downscale delay, and
independent 50 percent downscale limits described below.

The numerical production policy is an initial operating point, not a permanent
default recommendation for every service. Future tuning must follow the
[SkyServe autoscaling simulation runbook](serve-autoscaling-simulation.md) and
compare a candidate against the exact live baseline on held-out traffic and
supply traces.

## Problem

SkyServe already supports strict request priority, concurrency-native logical
autoscaling, deduplicated rejected-job pressure, bounded scale-up waves, a
wall-clock downscale delay, and a configurable whole-fleet scale-down limit.
Those controls still leave three gaps for bursty one-request-per-GPU services:

1. One queue timeout applies to every priority. A short timeout spills all
   traffic before slow GPU capacity can start, while a long timeout makes every
   retained waiter count as one immediately required GPU.
2. The normal 20 percent scale-up wave is intentionally conservative, but it
   reacts too slowly when queue and rejection pressure persists. Removing the
   wave entirely would reintroduce the unsafe zero-to-maximum launch jump.
3. A 50 percent whole-fleet downscale can cancel almost the complete
   provisioning cohort when ready capacity is already more than half the
   fleet. A burst one minute later must then relaunch the same capacity.

The production incident on 2026-07-20 demonstrated the third gap. The service
had 124 ready logical slots and 109 provisioning slots. Its adopted target fell
from 236 to 129, and provisioning fell from 109 to 7. The next minute, queue
and rejection pressure returned and the target rose again. The total-fleet
limit was respected, but more than 90 percent of pending capacity was lost.

## Goals

- Let multiple priority thresholds choose independent queue patience.
- Keep high-priority work strict-higher-first and able to spill sooner.
- Convert retained queue depth into work that must drain within its patience
  horizon instead of one immediate GPU per waiter.
- Preserve rejected work as scale-up pressure without multiplying provider
  retries for a stable job ID.
- Use deduplicated offered arrivals as a raise-only load floor.
- Accelerate scale-up under sustained pressure while retaining time pacing.
- Prevent fresh demand from racing a downscale decision.
- Apply the configured downscale percentage independently to committed capacity
  and to the provisioning cohort.
- Preserve scalar-only specs and mixed old/new controller and load-balancer
  deployments.

## Non-goals

- Changing the existing `X-SkyServe-Priority` contract or scheduler ordering.
- Preempting an admitted request for a higher-priority request.
- Guaranteeing that a queued request completes before its patience expires.
- Persisting request identifiers, priorities, or per-priority history in the
  central database.
- Predicting provider capacity or launch success.
- Replacing the existing readiness, occupancy, drain, or logical-actuation
  safety fences.

## Public configuration

The existing scalar remains the fallback. An optional ordered threshold list
selects a timeout using the highest matching `min_priority`:

```yaml
service:
  load_balancer:
    request_queue:
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

Rules:

- Priority remains an integer from 0 through 100. Missing priority is 0.
- Thresholds must use unique, strictly increasing `min_priority` values in the
  inclusive 0 through 100 range.
- Every threshold timeout must be finite and greater than zero.
- The highest threshold with `min_priority <= request priority` wins.
- If no threshold matches, the scalar `timeout_seconds` wins.
- An absent or empty threshold list preserves scalar queue behavior and the
  current unweighted queue contribution.
- A waiter stores its absolute monotonic deadline when admitted. A live update
  affects only later arrivals.
- `adaptive_scale_up` is valid only for logical concurrency services with the
  existing three normal wave fields configured.
- Adaptive percentages use the same 1 through 100 range as the normal wave;
  minimum replicas and observation count are positive integers; hold time is a
  finite positive number.

## Load-balancer demand report

The load balancer keeps the existing aggregate gauges and adds bounded maps:

```text
queue_depth_by_priority: {priority: count}
rejected_in_window_by_priority: {priority: unique_job_count}
rejected_in_recent_window_by_priority: {priority: unique_job_count}
```

The maps contain at most 101 keys. Aggregate fields remain authoritative for
old controllers and status clients. The priority maps are process-local demand
gauges and are not written to request history.

Rejected stable jobs remain keyed by `X-SkyServe-Job-ID`, but each retained
entry also stores the most recently rejected effective priority. Headerless
rejections remain unique per request. A later successful acceptance clears the
stable job from both aggregate and per-priority views.

The load balancer also reports four bounded offered-arrival counts:

```text
unique_job_arrivals_60s
unique_job_arrivals_300s
headerless_arrivals_60s
headerless_arrivals_300s
```

Stable job IDs are hashed before entering process-local tracking and never
leave the load balancer. Retries of the same stable job refresh one entry.
Headerless arrivals use an attempt-based bounded timestamp deque because they
cannot be deduplicated safely. Combined tracking is capped at 100,000 entries,
matching `LB_REQUEST_TIMESTAMP_CAP`, and pruned by the 300-second window. If
the cap is reached, both arrival-window counts saturate at 100,000 for 300
seconds instead of evicting entries and undercounting the heaviest offered
load. Arbitrary IDs therefore cannot grow memory without bound.

HA demand handoff applies a maximum floor to every aggregate count and every
priority bucket. It never adds old and current gauges, which would double-count
one active slot across a cutover. The completeness gate retains its current
definition and does not require any new map or arrival field, so an old load
balancer can still complete a handoff. All new fields are optional everywhere.
Old load balancers omit them; the new controller falls back to aggregate queue,
rejection, and timestamp behavior.

## Demand calculation

For logical concurrency services with a non-empty threshold list:

```text
queue_work = sum(
    queue_depth_by_priority[p]
    * min(1, expected_request_duration_seconds / patience_seconds(p)))

outstanding_work = in_flight
                 + queue_work
                 + normalized_rejected_work
                 + unknown_occupancy_floor
```

If the expected duration, threshold list, or per-priority queue map is absent,
queue work falls back to the existing aggregate queue depth. Rejected work
retains its existing duration normalization and stays priority-neutral. A
rejected low-priority job still represents work another provider must execute.
The same aggregate fallback applies when the priority buckets sum to less than
the aggregate queue gauge. This preserves conservative demand during a
mixed-version HA handoff where an old active contributes only the aggregate
floor and a new active contributes an empty or partial priority map.

The offered-load floor is raise-only:

```text
unique_60 = unique_job_arrivals_60s + headerless_arrivals_60s
unique_300 = unique_job_arrivals_300s + headerless_arrivals_300s

arrival_work = max(
    unique_60 * expected_request_duration_seconds / 60,
    1.15 * unique_300 * expected_request_duration_seconds / 300)

arrival_target = ceil(arrival_work / effective_capacity_per_gpu)
raw_target = max(outstanding_target, arrival_target)
```

If the expected duration or new arrival counts are unavailable, the existing
one-minute timestamp floor remains the fallback. The floor never lowers an
outstanding-work target. The 15 percent five-minute headroom covers burst
variance without treating every retry as new work. Unlike today's timestamp
floor, which is used only while the demand report is stale, the new deduplicated
floor participates in every fresh recompute. A one-shot burst can therefore
hold some capacity for the five-minute window plus downscale hysteresis, about
8 to 10 minutes with the proposed production policy. This is intentional
startup-latency protection. The existing stale-mode timestamp behavior is
unchanged.

Queue weighting uses the patience selected at admission, not remaining
patience. A cohort near its deadline therefore keeps the same duration/patience
weight. Modeling deadline age is deferred because it would require age buckets
in every load-balancer report and HA handoff. If the request times out, its
deduplicated rejection contribution becomes the recovery pressure signal.

## Pressure and scale-up

One fresh authoritative demand report is a pressure observation when any of
these holds relative to the preceding report:

- aggregate queue depth increased;
- recent deduplicated rejection count increased; or
- offered arrivals in the 60-second window increased.

The observation is computed only after a complete demand report. An incomplete
or stale report never activates adaptive mode and never authorizes downscale.
A report carrying an active HA handoff floor is also excluded from pressure
deltas because maximum-merged old gauges are not new offered demand. The
controller already knows whether `DemandHandoff.apply()` is active. It passes a
process-local `pressure_report_is_floored` boolean to the autoscaler beside the
effective gauges; this is not a load-balancer wire field or protocol
requirement. Demand sizing uses the conservative floored gauges, while pressure
streaks and vetoes ignore that report.
Consecutive pressure observations activate adaptive mode for `hold_seconds`.
Another pressure observation refreshes the hold. A quiet report resets the
pre-activation streak but does not end an already active hold early. A stable
nonzero rejection count is not repeated pressure, so one retried doomed job
cannot keep adaptive mode or a downscale veto alive forever. Adaptive-active
state by itself is not a downscale veto; only a new pressure delta is.

Normal mode keeps the existing wave:

```text
step = max(normal_min, ceil(committed * normal_percentage / 100))
```

Adaptive mode substitutes the adaptive minimum and percentage. Both modes use
the same `scale_up_rate_period_seconds` timer. Adaptive mode changes wave size,
not pacing, and can never jump from zero to the complete maximum in one tick.
The adopted target is still clipped to service bounds and all existing logical
actuation fences remain in force.

## Downscale stability

The existing wall-clock downscale delay remains continuous quiet-time proof.
Before accepting a lower target after the delay, the autoscaler vetoes the
decision and restarts the delay if the latest complete, non-handoff-floored
report has a rising queue, recent-rejection count, or offered-arrival count.
It records a bounded reason string for status. A stable or shrinking queue and
a stable nonzero rejection population do not veto forever. Each delta can
restart the delay once; unchanged gauges cannot generate another veto.

Consecutive vetoes are additionally capped at 2 per downscale episode (a run
of recomputes whose raw target stays below the adopted target). The latch is
magnitude-blind, so under trickle traffic a tiny positive delta re-arms it
nearly every quiet window and an unbounded veto would restart the delay
forever, starving downscale indefinitely. After the cap the elapsed delay
accepts the lower target. Genuine rising pressure raises the raw target and
takes the upscale branch, which ends the episode and refreshes the veto
budget; an accepted downscale, an equal target, a stale tick, and a version
update also reset the streak. Worst case the cap adds two full delay windows
of extra hold, preserving the protection at the moment pressure begins.

The pressure baseline is the latest complete, non-floored report current when
the downscale delay starts or the previous veto is consumed. Reports may arrive
faster than decision ticks, so any positive delta is latched until the next
decision consumes or clears it. Consuming a veto clears the latch and advances
the baseline to the latest eligible report. A stale tick clears the latch and
baseline. The first eligible report after construction, restart, or staleness
only establishes a baseline and is never pressure.

When a downscale is accepted, let:

```text
committed = latest-version nonterminal planned logical capacity
provisioning = committed capacity in PENDING, PROVISIONING, or STARTING
total_allowance = ceil(committed * max_scale_down_rate_percentage / 100)
pending_allowance = ceil(provisioning * max_scale_down_rate_percentage / 100)

limited_target = max(raw_target, committed - total_allowance)
pending_retention_floor = max(0, provisioning - pending_allowance)
```

The accepted lower target freezes `pending_retention_floor` for that downscale
episode. The allowance and all spending are measured in logical slots, not
physical replica count. Logical victim selection may remove at most
`pending_allowance` from PENDING, PROVISIONING, and STARTING combined, even
across later reconciliation ticks. A multi-slot pending victim whose committed
width would exceed the remaining slot budget is skipped, so heterogeneous
fleets never overspend the percentage. Once that frozen floor binds, selection
may consider idle READY capacity under the existing ready-capacity fence and
the whole-fleet target. A new lower target and new cohort budget require another
complete continuous downscale delay. An equal target never refreshes the
budget.

This makes the limits independent instead of taking the maximum of two target
floors. A single stuck provisioning replica cannot reduce a 200-ready fleet to
one retirement per five minutes. At the same time, a large pending cohort
cannot be almost completely cancelled by repeated reconciliation ticks after
one accepted target. An adopted target increase, service update, or controller
restart clears the frozen cohort budget. Restart remains
fail-closed until fresh demand and a new quiet interval establish authority.

For the observed 124-ready plus 109-provisioning fleet at 50 percent, the
pending allowance is 55 and the frozen retention floor is 54. Because ready
capacity was below the adopted target, the existing ready-capacity fence would
not retire READY slots. The first downscale episode therefore keeps at least 54
pending slots, so a one-minute rebound does not start from 7.

## State, compatibility, and rollback

- All new demand and pressure state is process-local and bounded.
- In-process service updates retain arrival, pressure, and adaptive hold state,
  but clear the frozen cohort budget and reset downscale hysteresis. Any further
  cancellation therefore requires a fresh complete quiet interval under the
  new policy.
- A controller restart starts with no adaptive authority and follows the
  existing stale-report fail-closed behavior until fresh reports arrive.
- A new load balancer with an old controller sends extra JSON keys that are
  ignored. A new controller with an old load balancer uses aggregate queue,
  rejection, and timestamp fields.
- Removing `timeout_seconds_by_priority` restores scalar timeout and unweighted
  queue work for new arrivals. Removing `adaptive_scale_up` restores normal
  waves. No schema migration or replica rollout is required.
- Rolling back the control plane loses only process-local demand refinements;
  the existing aggregate gauges, whole-fleet cap, and wave controls continue.
- During a mixed load-balancer rollout, an old load balancer ignores
  `timeout_seconds_by_priority` and applies the scalar timeout to every
  priority. This is safe, observable, and temporary.

## Observability

Autoscaler status exposes:

- aggregate and per-priority queue depth;
- aggregate and per-priority retained and recent rejections;
- weighted queue work and rejected work;
- 60-second and 300-second offered-arrival counts and arrival floor;
- raw and adopted targets plus committed and provisioning capacity;
- pressure streak, adaptive-active state, and adaptive hold remaining;
- downscale elapsed time, veto reason, whole-fleet allowance, provisioning
  allowance, frozen provisioning-retention floor, and pending slots spent in
  the current episode.

These values flow through the existing Serve status surface. Minute history
keeps its existing aggregate schema in this change. Persisting priority maps
would create high-cardinality product policy and is deliberately deferred.

## Data-driven tuning requirement

A dashboard screenshot or one burst is evidence for an investigation, not a
sufficient basis for changing a policy. Any later change to queue patience,
utilization, expected duration, minimum capacity, scale-up waves, downscale
hysteresis, or downscale limits must use the simulation runbook linked above.

The comparison must include the currently deployed policy unchanged, use the
same request and capacity traces for every candidate, model launch delay and
failure, and report results by priority. When request priority, duration, or
historical cluster supply is unavailable, the report must label the missing
input and sweep a bounded range instead of substituting one silent guess.

The simulator is not a proof of production behavior. Before a policy is
adopted, its baseline replay must be calibrated against observed targets,
ready and provisioning capacity, queue depth, and rejections. A candidate that
only wins under an uncalibrated or optimistic model is not eligible for
rollout.

## Alternatives considered

- Count every queued request as one GPU. Rejected because long patience then
  turns queue retention into immediate overprovisioning.
- Use only request rate and average duration. Rejected because active occupancy
  and a growing queue are stronger immediate state than an arrival estimate.
- Remove the scale-up wave under rejection. Rejected because it permits the
  unsafe zero-to-maximum launch jump.
- Veto downscale whenever queue depth is nonzero. Rejected because a stable
  long-patience queue could block scale-down indefinitely.
- Apply only the existing whole-fleet 50 percent cap. Rejected by the observed
  provisioning cancellation.
- Persist raw job IDs for exact controller deduplication. Rejected because the
  load balancer can provide bounded aggregate windows without exporting
  identifiers.

## Test plan

- Service schema and `SkyServiceSpec` tests cover threshold ordering, bounds,
  finite timeouts, adaptive field dependencies, serialization, copy, and old
  pickle defaults.
- Queue tests prove highest-threshold resolution, scalar fallback, fixed
  admission deadline across live updates, strict priority ordering, timeout
  rejection attribution, successful stable-job clearing, and map bounds.
- HA tests prove snapshot parsing, map maximum merge, mixed-version omission,
  and offered-arrival floors without addition.
- Concurrency-autoscaler tests cover queue weighting, aggregate fallback,
  60/300-second arrival floors, saturation, retry-deduplicated counts, adaptive
  activation and expiry, HA-floor pressure exclusion, unchanged pacing,
  delta-only downscale veto, and an independent provisioning cancellation
  budget that stays frozen across reconciliation ticks.
- Existing focused request-queue, concurrency-autoscaler, LB sync, HA,
  controller, logical reconciliation, and status tests pass.
- Run formatter, mypy, pylint, Ruff, and the broader Serve unit-test slice.

## Rollout

1. Obtain an independent Fable adversarial review of this exact design and
   resolve every confirmed blocker before implementation.
2. Implement and test on the latest `origin/improvements` head.
3. Obtain an independent Fable review of the exact tested commit. Any code
   change invalidates that implementation approval.
4. Merge only with the full visible GitHub check rollup green.
5. Build and deploy an immutable image from the exact merge commit with Helm
   `--reuse-values`. Verify API health and reported commit identity.
6. Update `boltz-l4-fleet` to priority patience 600/60, normal 20 percent/10,
   adaptive 100 percent/50 after two observations for 120 seconds, five-minute
   downscale, and 50 percent scale-down limits.
7. Verify the new service version, routing continuity, queue and pressure
   fields, paced scale-up, and no unexpected ready-capacity retirement.
8. Update the canonical Boltz Platform service YAML and open its PR so the live
   policy is reproducible. Do not merge that downstream PR as part of this
   rollout unless separately requested.
