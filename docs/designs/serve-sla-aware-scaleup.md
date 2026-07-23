# SkyServe: SLA-aware scale-up sizing and burst admission

## Problem

Two production incidents on 2026-07-22/23 (boltz-l4-fleet) showed the
concurrency autoscaler and the load balancer queue jointly failing the
service's actual objective: requests must neither expire their per-priority
queue timeout (the SLA, e.g. 600 s for low priority) nor be rejected by the
queue size cap. Beyond the launch wedge fixed separately, simulation replay of
the incidents shows the *healthy* algorithm still rejects 4-15% of a burst.
Four structural causes:

1. **Deadline discounting with no delivery plan.** `_queue_work()` weighs a
   queued request by `expected_request_duration / timeout` (a 600 s-timeout
   request counts as 0.05 replicas). This sizes the fleet so requests finish
   *exactly at* their deadline under instant provisioning. Real provisioning
   takes 6-15 minutes, consuming most or all of the 600 s budget, so
   discounted sizing structurally delivers late capacity.
2. **Queue cap tied to READY capacity only.** The queue limit is
   `size_per_replica x ready-units`. During the provisioning window the cap
   does not grow with authorized capacity, so a burst that the autoscaler has
   already decided to absorb is rejected at the door anyway. In simulation
   this cap, not sizing, is the dominant rejection channel.
3. **Wave limiter keyed to latest-version committed capacity.** The scale-up
   budget base and ceiling use latest-version slots only. During a rolling
   update where the serving fleet is still on the old version, the base is 0
   and the adopted target crawls from the wave minimum regardless of fleet
   size (observed live: raw target 1000, adopted target 50 with 156 ready).
4. **Pressure detection requires strictly increasing samples.** A queue
   pinned flat at its cap with steady rejections resets the pressure streak,
   so adaptive scale-up disarms exactly when saturation is worst (observed
   live: queue flat at 1560 for 10 minutes, adaptive not held).

## Behavior contract

1. **Burst admission follows authorized capacity.** The load balancer queue
   limit uses `max(ready_units, min(target_num_replicas, ready_units +
   provisioning_replicas))` as its unit base, from the existing controller
   capacity hint. A burst the autoscaler has authorized capacity for waits in
   queue (bounded by its own timeout) instead of being 503-rejected. When the
   hint fields are absent (older controller), behavior is unchanged.
2. **Queue work is weighted by the *remaining* SLA budget after capacity
   lands.** With the new `replica_policy.provision_lead_time_seconds` knob
   (default 0 = current behavior), the weight becomes
   `min(1, duration / max(duration, timeout - lead))`. A 600 s-timeout
   request with a 540 s lead counts as 0.5 replicas, not 0.05.
3. **The scale-up wave base counts the whole demand-owned fleet.** Budget
   base and ceiling use non-terminal, non-scale-down, demand-owned capacity
   across *all* versions. Rolling updates no longer reset ramp speed; the
   rolling surge/drain machinery still bounds replacement pacing separately.
4. **Saturation is pressure.** A pressure observation also latches when the
   reported queue depth holds at or above its previous value while at or
   above `scale_up_rate_min_replicas` (a plateaued queue). A draining queue
   still resets the streak, and stable rejection populations deliberately
   remain non-latching to preserve the existing bounded downscale-veto
   behavior; cap and timeout rejections always ride on a deep queue, which
   the plateau clause covers.

Downscale behavior, hysteresis, exact-card attribution, reserved fill, and
the wave limiter's existence are unchanged.

## Evidence (simulation)

Discrete-time simulator (`temp` investigation tooling, results reproduced in
the PR) modeling the fleet's exact config: concurrency 1, utilization 95%,
`expected_request_duration` 30 s, waves 20%/60 s min 10, adaptive 100% min 50
after 2 observations, downscale 300 s delay + 50%/60 s, queue cap
10/replica, timeouts 600 s (prio 0) / 60 s (prio >= 50), decision tick 20 s.
Service time lognormal(45 s), provisioning uniform 6-11 min, 3 seeds.

Validation: with launches broken and scale-down frozen (the actual wedge) the
simulator reproduces both incidents: queue pinned at exactly
`10 x ready` (1560 / 2000), fleet frozen, 18-43% rejected.

| scenario (offered) | current | current+cap only | cap+SLA weights (chosen) |
|---|---|---|---|
| step burst 400/min, fleet 156 | 9.9% rej | 2.8% rej | **0.1%** |
| sustained 700/min, fleet 200 | 5.6% | 1.7% | **0.1%** |
| cold spike 200/min, fleet 10 | 9.8% | 3.1% | **0.2%** |
| mixed priority burst | 8.5% | 1.9% | **0.0%** |
| overload > max_replicas throughput | 36.0% | 36.0% | 36.0% |

Cost: the chosen policy spends +8% to +70% replica-hours during burst windows
only (e.g. 630 vs 469 on the step burst) and is identical at steady state.
Cap-only (without weight change) leaves 600 s-timeout expiries because the
fleet stays undersized. Weights-only (without cap change) leaves ~9.6%
rejections because the cap fires during the provisioning window. Both are
needed; each alone is insufficient. Removing the wave limiter entirely
performed the same as keeping it, so it is kept as a safety brake.

Sensitivity (service 60 s, provisioning 8-15 min): chosen policy 1.0-2.3%
failures vs 8.2-14.5% current. Residual failures there are physically
unavoidable: when provisioning latency exceeds the SLA budget, reactive
scaling cannot save a burst's front edge; only warm headroom
(`min_replicas` / overprovision) or shorter provisioning can. Operators
should set `expected_request_duration_seconds` honestly (the fleet's 30 s vs
observed ~45-60 s under-sizes every estimate) and set
`provision_lead_time_seconds` to the observed p75 launch-to-ready time.

## Alternatives considered

- **Full-weight queue work (drain ASAP, ignore deadlines).** Same SLA
  compliance as chosen policy but strictly higher cost (up to +12% more
  replica-hours) and no wait-time benefit. Rejected.
- **Arrival lead-time cover (size for arrivals expected during provisioning).**
  Redundant once weights + cap are in place (identical results); a naive
  version saturates the target at `max_replicas` and quadruples cost.
  Rejected.
- **Removing the wave limiter.** Equal outcomes in simulation, but it is the
  only brake against demand-signal glitches mass-launching spot instances.
  Kept.
- **Dynamic lead estimation (EMA of observed launch-to-ready).** Better
  long-term than a static knob; deferred to keep this change reviewable. The
  knob's plumbing is where the estimator would land.
  TODO(FreCap): follow up with adaptive lead estimation.

## Test plan

Unit tests (all in existing suites):
- `_queue_work` lead weighting: 0 lead preserves current weights; 540 s lead
  yields 0.5 for a 600 s timeout; weight never exceeds 1; missing priority
  map still falls back to raw depth.
- Wave budget under version skew: old-version fleet of N gives budget base N
  (not 0) and ceiling `N + budget`; scale-down base unchanged.
- Pressure plateau: flat non-zero queue depth latches an observation; a
  draining queue does not; recent rejections latch without an increase.
- LB `_request_queue_limits`: cap grows to authorized units with hint
  target/provisioning present; absent fields preserve ready-based cap;
  authorized units never exceed `target_num_replicas`.

Manual: replay a step burst against a kind cluster service with a low
`max_replicas`, verify queue holds without 503s while replicas provision and
that requests dispatch before their priority timeout.

## Rollout

All changes except the weight knob are default-on behavior fixes. The knob
defaults to 0 (no sizing change) so other services are unaffected until they
opt in; boltz-l4-fleet sets `provision_lead_time_seconds: 540` in its next
fleet update. No API version bump: the capacity hint fields consumed by the
LB already exist, and old LB + new controller (or the reverse) degrade to
current behavior.
