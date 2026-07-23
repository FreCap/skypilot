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

1. **Queue work is weighted by the *remaining* SLA budget after capacity
   lands.** With the new `replica_policy.provision_lead_time_seconds` knob
   (default 0 = current behavior), the weight becomes
   `min(1, duration / max(duration, timeout - lead))`. A 600 s-timeout
   request with a 540 s lead counts as 0.5 replicas, not 0.05. This orders
   capacity ahead of saturation, which on gradually rising load keeps the
   queue away from its cap entirely.
2. **The scale-up target ceiling counts the whole fleet.** The aggregate
   ceiling (`committed + budget`) uses non-terminal, non-retiring capacity
   across *all* versions, so a saturated old-version fleet can grow to meet
   demand instead of being pinned below itself while the new version ramps.
   The wave *rate* deliberately stays on the latest-version base: it also
   paces rollout replacement launches, and ramping a new version from its
   own committed capacity is an existing contract that this change
   preserves. Consequence to note: across a version boundary the new
   version's `target_capacity` now reflects real demand rather than the
   ramped value, so a rollout under load reaches its surge sooner. Actual
   launches remain bounded by the unchanged per-wave `launch_budget`.
   The retained-cooldown ceiling introduced by "Preserve unspent rollout
   waves" is derived from the same all-version base at every recording
   site, since a mixed base would leave the ceiling below its own
   subtrahend and silently zero retained authority.
3. **Saturation is pressure.** A pressure observation also latches when the
   reported queue depth holds at or above its previous value while at or
   above `scale_up_rate_min_replicas` (a plateaued queue). A draining queue
   still resets the streak, and stable rejection populations deliberately
   remain non-latching to preserve the existing bounded downscale-veto
   behavior; cap and timeout rejections always ride on a deep queue, which
   the plateau clause covers. Together with (2) this guarantees a saturated
   flat queue keeps the fleet doubling wave over wave until raw demand or
   `max_replicas` is reached.

**Queue admission is deliberately unchanged** (operator decision): the load
balancer queue limit stays `size_per_replica x ready units`. Extending it to
authorized (target + provisioning) capacity was evaluated and quantified but
rejected to keep the bounded-queue contract; the numbers are retained below
for any future revisit.

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

| scenario (offered) | current | chosen (SLA weights, cap unchanged) | with cap extension (rejected) |
|---|---|---|---|
| slow ramp 30->400/min over 40 min | 2.4% rej | **0.3%** | 0.0% |
| fast ramp 30->500/min over 15 min | 8.2% | **2.9%** | 0.0% |
| step burst 400/min, fleet 156 | 9.9% | **9.6%** | 0.1% |
| sustained 700/min, fleet 200 | 5.6% | **5.4%** | 0.1% |
| cold spike 200/min, fleet 10 | 9.8% | **9.6%** | 0.2% |
| mixed priority burst | 8.5% | **8.2%** | 0.0% |
| overload > max_replicas throughput | 36.0% | 36.0% | 36.0% |

The chosen policy's leverage is on rising load, the shape of both production
incidents: capacity is ordered ahead of saturation, so the queue never
reaches its cap (slow ramp peak queue 615 vs 2269 today, mean wait 20 s vs
109 s). Cost is +8-17% replica-hours during ramp windows only and identical
at steady state. On an instantaneous step burst the residual is physics:
ready capacity (and with it the queue cap) cannot grow faster than
provisioning latency, so the front edge of a true step is shed at the cap
regardless of sizing policy; the retained-rejection signal then sizes the
recovery, and the plateau + wave fixes guarantee the ramp completes at the
configured maximum rate. Removing the wave limiter entirely performed the
same as keeping it, so it is kept as a safety brake.

Sensitivity (service 60 s, provisioning 8-15 min): the same ordering holds
with all failure rates roughly doubled. When provisioning latency exceeds
the SLA budget, reactive scaling cannot save a burst's front edge; only warm
headroom (`min_replicas` / overprovision) or shorter provisioning can.
Operators should set `expected_request_duration_seconds` honestly (the
fleet's 30 s vs observed ~45-60 s under-sizes every estimate) and set
`provision_lead_time_seconds` to the observed p75 launch-to-ready time.

## Alternatives considered

- **Extending the queue cap to authorized capacity.** Eliminates residual
  step-burst rejections (table above) but changes the bounded-queue
  admission contract; excluded by operator decision. The evaluation is
  retained should that tradeoff be revisited.
- **Full-weight queue work (drain ASAP, ignore deadlines).** Same SLA
  compliance as chosen policy but strictly higher cost (up to +12% more
  replica-hours) and no wait-time benefit. Rejected.
- **Arrival-trend extrapolation (project the arrival slope over the
  provisioning lead).** Bought only 0.1-0.9 points over the chosen policy
  at equal cost while adding a phantom-demand surface on transient spikes.
  Rejected.
- **Arrival lead-time cover (size for arrivals expected during provisioning).**
  Redundant once the SLA weights are in place; a naive version saturates
  the target at `max_replicas` and quadruples cost. Rejected.
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
- Pressure plateau: a flat queue at or above `scale_up_rate_min_replicas`
  latches an observation; a draining queue resets; a flat trickle queue
  below the floor stays non-latching.
- Incident regression: a flat saturated queue with committing capacity
  progresses adaptive waves 120 -> 200 -> 400 -> 800 -> `max_replicas`.

Manual: replay a rising-load window against a kind cluster service with a
low `max_replicas`, verify the target rises ahead of queue saturation and
that requests dispatch before their priority timeout.

## Rollout

The wave-base and plateau fixes are default-on behavior fixes confined to
the controller's autoscaler. The knob defaults to 0 (no sizing change) so
other services are unaffected until they opt in; boltz-l4-fleet sets
`provision_lead_time_seconds: 540` in its next fleet update. No API version
bump and no load balancer changes.
