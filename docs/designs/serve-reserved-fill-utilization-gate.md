# Utilization-gated reserved capacity

- **Status:** implementing (M1 to M3 landing in this PR; M0 and M4 are operator steps)
- **Last updated:** 2026-07-25
- **Milestones:** M0 config (operator), M1 persistence, M2 signal (log only), M3 gate
  (default off), M4 staged enable (operator)

Reserved-fill capacity is arbitrated by static declared floors and weights. Nothing in the
allocation math knows whether a claimant is doing any work, so an idle service keeps everything
its weight entitles it to, indefinitely. This design adds a utilization gate: a claimant that
demonstrates no work walks its entitlement down to `floor_replicas` in bounded steps, and the
released capacity returns to genuinely free GPUs where any service can take it, including one
that does not declare `reserved_capacity_fill` at all.

All line references are at prod SHA `a0028d62c7be576a97937d8fe7471bfa7c019849` (SkyPilot 1.1.807),
which is an ancestor of the branch this lands on. Read them with
`git show a0028d62c7be576a97937d8fe7471bfa7c019849:<path>`.

## Blocking dependency: drain proof across load balancer restarts

Measured after this design was accepted, and it changes the rollout order.
`protenixv2-hybrid-v1`'s load balancer Deployment rolled **46 times in 41.9
hours** (mean interval 55 minutes), essentially all of them side effects of
control-plane deploys, because the load balancer pod template pins the
controller image digest. Every such roll makes the controller mark every live
replica occupancy-unknown, and the logical retirement gate reads that blind
capacity view as a shortfall and **aborts the whole in-progress drain wave**,
returning its victims to routing with their elapsed drain lost.

So on this cluster the gate's release does not merely run slowly, it is
repeatedly reverted: any wave lasting more than about an hour is expected to
span at least one roll. The trajectory in "Release path and end-to-end latency
budget" below assumes no roll and is therefore a best case, not a typical one.

The gate is still correct to ship (it is default off, and the mechanism is
independent), but do not enable it on `protenixv2-hybrid-v1` before Milestones
1 and 2 of `serve-drain-proof-across-lb-restarts.md` are deployed. Enabling it
earlier will produce churn without release.

## Operator decisions on record

Taken 2026-07-25, and they override the sizing recommendations in "Configuration for the live
services" below, which are retained as the analysis that produced them.

1. **`boltz-l4-fleet` keeps `floor_replicas: 10`.** Not raised to 12.
2. **`protenixv2-hybrid-v1` keeps `floor_replicas: 0`** (the field is absent today). The decay
   target for protenix is therefore zero fill replicas, accepted knowingly. Its practical idle
   residue is about 4 A100: three demand-placed zero-cost rows that no broker lever can reclaim
   (`fill_ceiling = grant + zero_cost_demand_placed`) plus `min_replicas_by_accelerator`.
3. **Rebalancing is done by the gate, not by a static reallocation.** The design's "available
   today, no code change" lever (a) is therefore not the plan of record; it remains documented as
   the interim fallback.
4. **Idle means no in-flight requests and no queued work**, sustained. This is requirement 4 in
   the behavior contract.

Consequence to keep in view: with `protenix floor 0`, a burst after a full release is served by
nothing warm and waits the full `readiness_probe.initial_delay_seconds` of 1800s for its first
useful replica. Risk 1 below is the analysis; the decision stands. Open question 1 (protenix's
real `effective_request_duration_seconds`) is still the gate on M4 for that service, because it
determines whether zero is a defensible floor or merely a cheap one.

## Problem

The reserved-fill broker allocates a shared GPU pool by static weight and a structural headroom cap. Neither term knows whether a service is doing any work, and the headroom cap is *largest* when a service is idle (`effective_cap = max(0, max_replicas - demand_target)`, `reserved_capacity.py:474-475`, consumed at `reserved_capacity_allocation.py:140-145`), so idleness slightly *increases* entitlement today. The live consequence: `protenixv2-hybrid-v1` holds 77 A100s with zero in-flight requests and zero queue depth, while `boltz-l4-fleet` ran a 61-deep queue on 10 replicas three hours earlier. There is no code path by which the second fact can affect the first.

| Observation | Value | Source |
| --- | --- | --- |
| Pool key | `["prod_research_cluster_eks", ["a100", "a100-80gb"]]` | broker round log |
| A100 in cluster | 328 (264x A100-SXM4-80GB, 64x A100-SXM4-40GB) | live k8s, `sagemaker-hyperpod-eks-cluster` |
| Held by research jobs (`hyperpod-ns-research`) | 241 | live k8s |
| Held by SkyPilot (`rescluster-k8s-prod-east1-preemptible-inference`) | 87 | live k8s |
| Measured free | 0 | every broker `feed` is 0 |
| Published grants | protenix 74, boltz-l4-fleet 10, boltz-l4-fleet-test 0 | broker round log |
| Actual pods | protenix 77, boltz-l4-fleet 10 | live k8s |
| protenix at snapshot | `in_flight_total` 0, `queue_depth` 0, arrivals in last 60s 0, demand target 2 | Concurrency report, `autoscalers.py:4436-4445` |
| protenix's only burst that day | 52 requests at 08:06 UTC | LB request history |
| boltz-l4-fleet 05:22-05:24 UTC | `queue_depth` 61, `in_flight_total` 9-11, on 10 replicas | Concurrency report |
| protenix weight | `1000000.0`, exactly `RESERVED_FILL_MAX_WEIGHT` (`constants.py:524`) | service spec |
| protenix share of the 74-slot remainder | `1e6 / (1e6 + 100 + 0.1)` = 99.99% | `water_fill`, `reserved_capacity_allocation.py:66-116` |
| protenix `floor_replicas` | absent, i.e. 0 | service spec |
| protenix readiness `initial_delay_seconds` | 1800 | service spec |
| boltz-l4-fleet readiness `initial_delay_seconds` | 1200 | service spec |

Grep for `idle`, `last_request`, `activity`, `utilization` over `autoscalers.py`, `constants.py`, `service_spec.py` and `reserved_capacity*.py` at this SHA returns only per-replica predicates. There is no per-service idleness notion anywhere in the entitlement math.

## What the model means now

Two knobs, both existing, both redefined. Nothing new is user-facing except an enable flag.

**`floor_replicas` is the guaranteed warm base.** It is the number of fill replicas the service keeps when it is doing nothing at all. It is never gated by utilization, it is refilled by `feed` even while the service is idle, and it is the level an idle service decays *to*. Structurally it is untouched: the gate tightens only the water-fill headroom (`reserved_capacity_allocation.py:140-145`), never `scale_floors` (49-63) and never `attainable_floor()` (25-30). Operationally its role changes completely: under the old model it was a fairness minimum that rarely bound, because idle hoarding kept everyone warm by accident. Under the new model it is the **only** capacity that survives a quiet period, so it is the entire burst-latency contract. A burst arriving after a decay is served by `floor_replicas` alone for the first `readiness_probe.initial_delay_seconds` (1800s for protenix, 1200s for boltz-l4-fleet). If you get one number right in the spec, get this one right.

**`weight` is the contention tiebreaker among services that have simultaneously demonstrated work.** It divides the remainder above the floors, and only among claimants whose utilization cap has not already bound them below their weighted share. It has no effect on an idle service, because an idle service's headroom is 0 before the weighted split runs. Two corollaries. First, extreme weights are now actively wrong: `1e6` no longer means "protenix wins when it is working" (utilization handles that); it means "a busy boltz-l4-fleet gets nothing above its floor whenever protenix also has a single request in flight", which is a much harsher policy than intended. Use ratios in the 2x to 10x range. Second, weight can no longer buy idle hoarding, so raising it is no longer a way to reserve capacity. Raising `floor_replicas` is.

**Everything above the floor is borrowed.** It is held only while the holder can prove work, it is released in bounded steps when the proof stops, and it returns to genuinely free GPUs (measured by `query_pool_group_observation`, `reserved_capacity.py:161-194`), not to a named peer. Free is the only channel that a service which does not declare `reserved_capacity_fill` can reach, because a non-claimant structurally cannot hold fill rows and never appears in `get_reserved_fill_claims` (`serve_state.py:5075-5084`).

## Behavior contract

1. **Idle is `demonstrated_need == 0`.** `demonstrated_need = max(busy_fill_holdings + pre_ready_fill_holdings, ceil(outstanding_work / work_per_replica))`. It is zero only when: `in_flight_total == 0` (requirement 4), `queue_depth == 0` (requirement 4), no retained rejections in the 360s `LB_REJECT_WINDOW_SECONDS` window, no occupancy-unknown replica, no fill replica reporting in-flight work, and no fill replica in PENDING / PROVISIONING / STARTING.
2. **No signal means busy.** A claimant with no fresh LB report (`has_fresh_demand_report()` false, `autoscalers.py:4166-4172`, threshold `3 * LB_CONTROLLER_SYNC_INTERVAL_SECONDS` = 60s), an autoscaler class with no occupancy telemetry, a stale activity stamp, or the gate disabled, is *ungated*: it gets exactly today's entitlement. The gate never fails toward release.
3. **Idleness must persist for 300s before anything is released** (`RESERVED_FILL_IDLE_DWELL_SECONDS`), measured in wall clock, and no release step is taken while any fill replica the service already authorized is still booting.
4. **A gated claimant's entitlement walks down to `floor_replicas` in bounded steps**, at most `max(2, ceil(0.25 * (cap - floor)))` replicas per `RESERVED_FILL_RELEASE_STEP_SECONDS` = 300s, and only after the previous step has been physically actuated (`holdings_fill <= cap`). The target is `floor_replicas`, never zero, and the floor is structurally unreachable from below because the gate caps at `max(floor, ...)` and the floor is applied by `scale_floors` before the gated headroom is computed.
5. **Recovery is one round, release is many.** Any round in which `ceil(need * 1.25) > cap` sets `cap = ceil(need * 1.25)` immediately, with no dwell and no step schedule. Measured asymmetry on this pool: full entitlement restored in <= 120s (one poller cycle plus `damp_grants`' two-round up-move), versus ~72 minutes to walk 77 down to 16.
6. **Lowering a grant never deletes a pod.** It removes scale-down shelter (`autoscalers.py:1129-1141`, `1147-1190`) and the ordinary drain-aware scale-down does the work, gated on `not _replica_is_busy(info)` (`autoscalers.py:4217-4239`, applied at 6632-6635 physical and 6759-6763 logical) and, for a logical replica that has served, on the LB reporting exactly zero in-flight and not-unknown (`replica_managers.py:~6228-6231`). A replica whose occupancy the LB cannot vouch for is never force-evicted; its retirement aborts and it rejoins service (`replica_managers.py:5962-5965`).
7. **Conservation is preserved and becomes strict.** `Sum(entitlements) <= total` holds verbatim: the change is a pure tightening of the `caps` mapping, and `water_fill`'s bound `sum(result) <= amount` is cap-independent (`allocation.py:97-114`: `remaining` is non-increasing, the cap clamp at 102-106 only lowers `give`). When every claimant's gated headroom is 0, `water_fill`'s `active` list (77-81) is empty, the loop never runs, and `Sum(entitlements) = Sum(floors) < total`. The difference is the capacity that drains back to free.
8. **A non-claimant service can expect exactly this:** the pool's steady-state free level rises from 0 to `total - Sum(floors) - (active surplus)`. With the recommended floors and both services idle that is 87 - 28 = 59 A100s genuinely free, takeable by any ordinary cheapest-first optimizer placement, by a SkyServe service that never declares `reserved_capacity_fill`, and by the research namespace. It is *not* a reservation: a busy claimant can re-take a freed slot within one 60s round via `compute_feeds`, and on this cluster the research tenant will usually take it first.
9. **The demand path is never gated by a decayed grant.** `_demand_should_skip_zero_cost` (`replica_managers.py:3633-3675`) reads `max(damped_grant, raw_grant)`, not the ceiling. A bursting claimant's ordinary demand scale-up onto the zero-cost tier is its fastest reacquisition path (immediate, no round, no feed, demand rows exempt from the ceiling) and it stays open.
10. **The gate is off by default and has a process-wide kill switch.** `reserved_capacity_fill.utilization_gate` defaults false; `SKYPILOT_SERVE_RESERVED_FILL_UTILIZATION_GATE=0` disables it for every service in the process without a spec redeploy.

## Mechanism

### Where the policy lives

`reserved_capacity_broker.py:133-146` is a facade that re-exports `ClaimInput`, `_largest_remainder_round`, `scale_floors`, `water_fill`, `compute_entitlements`, `damp_grants` and `compute_feeds` from `reserved_capacity_allocation.py` (290 lines, pure functions, no I/O) and rewrites `__module__` to preserve import and pickle identity. All allocation policy edits land in `reserved_capacity_allocation.py`. The broker assembles inputs and publishes.

The gate is **broker-side** because the state must survive a controller restart (the whole api-server pod is recreated on every deploy, and all controllers are processes inside it), because a shared dwell and step schedule is what makes borrowing symmetric (a per-claimant decay speed re-creates static priority through differential decay), and because conservation is only provable where `total` is known, inside the round.

### Signal (claimant side)

Measured in `_broker_cycle` (`reserved_capacity.py:417-512`), which runs unconditionally every `RESERVED_CAPACITY_POLL_INTERVAL_SECONDS` = 60 (`constants.py:433`) on the poller thread, and holds a live reference to the very `Autoscaler` instance the decision tick uses (`controller.py:3661-3695` passes `lambda: self._autoscaler`). This placement matters: measuring inside `_collect_request_information_locked` would be wrong, because that function early-returns when `in_flight is None` (`autoscalers.py:4278-4284`), is not called at all when the controller is not demand-authoritative (`controller.py:1046-1050`), and sets `_report_received_at` at 4431-4435, which would make any `has_fresh_demand_report()` check evaluated next to it unconditionally true.

New read-only projection, added next to `collect_reserved_capacity` (`autoscalers.py:650-693`):

```python
@dataclasses.dataclass(frozen=True)
class FillDemandSample:
    outstanding_work: float
    busy_fill_holdings: int
    pre_ready_fill_holdings: int
    upscale_pending: bool
    work_per_replica: float
```

`Autoscaler.fill_demand_sample(replica_infos) -> FillDemandSample | None` returns `None` on the base class. `None` means no telemetry, which the gate treats as blind, so `RequestRateAutoscaler` (2024), `InstanceAwareRequestRateAutoscaler` (2675), `QueueLengthAutoscaler` (7354) and `FallbackRequestRateAutoscaler` (7186) are never gated. `ConcurrencyAutoscaler` overrides it, returning `None` when `not self.has_fresh_demand_report()` and otherwise, under `self._logical_state_lock` (`autoscalers.py:3965`):

- `outstanding_work`: from `_outstanding_work()` (`autoscalers.py:5230-5286`). This is the correct quantity and not a new one: it already fuses in-flight, `_queue_work()` (4651-4677), `_rejected_work()` (4921-4933, which retains rejections over `LB_REJECT_WINDOW_SECONDS` = 360, `constants.py:390`) and the unknown-occupancy floor, and it is what the autoscaler itself trusts for its own target. Using raw `queue_depth` instead would be self-destructing: `LB_REQUEST_QUEUE_TIMEOUT_SECONDS` = 120 (`constants.py:338`) converts every queued request into a rejection two minutes after a burst hits a saturated fleet, so a naive queue-depth signal would collapse to zero long before a 1800s cold start could deliver anything. **Required refactor:** `_outstanding_work` writes `self._weighted_queue_work` and `self._rejected_concurrency` at 5281-5282 (read only by `info()` at 6933 and 6940). Extract the body into a pure `_outstanding_work_parts(replica_infos) -> tuple[float, float, float]`; `_outstanding_work` keeps the two assignments, `fill_demand_sample` calls the pure variant so a poller-thread read cannot clobber an observability field the decision tick owns.
- `busy_fill_holdings`: count of this service's nonterminal FILL rows for which `_replica_is_busy(info)` is True (`autoscalers.py:4217-4239`). Fill/demand classification reuses the single existing definition (`count_zero_cost_holdings`, `autoscalers.py:695-718`, via `_fill_row_occupies_free_slot` 766-804 and `_replica_on_zero_cost_location` 806-815). This term is **per replica, not per service**, which is the critical repair: with `graceful_drain_async_occupancy: true` on protenix, an unknown-occupancy replica is busy individually (`4233-4234`), it does not pin the whole 77-replica fleet as busy. Three unknown replicas out of 77 produce `need = 3`, not "the service is busy".
- `pre_ready_fill_holdings`: fill rows in PENDING / PROVISIONING / STARTING. This is the boot protection. `_replica_is_busy` deliberately reports these as *idle* (4237-4239) and `scale_down_decision_order` (`serve_state.py:625-630`) makes them the first scale-down victims, so without this term the gate would order a fleet, hold it for 25 minutes, and cull it mid-boot before it served a request.
- `work_per_replica`: `self.target_concurrency_per_replica`, or `self._effective_logical_capacity_per_gpu()` (`autoscalers.py:4632`) in logical mode. Mirrors `_outstanding_work`'s own denominator at 5245-5248. Both live services run `target_concurrency_per_replica: 1`.

Derived and shipped on the claim:

```python
demonstrated_need = max(busy_fill_holdings + pre_ready_fill_holdings,
                        math.ceil(outstanding_work / work_per_replica))
boot_hold = (pre_ready_fill_holdings > 0) or upscale_pending
```

The lock is taken and released inside `_broker_cycle` **before** `upsert_claim` takes `constants.RESERVED_FILL_BROKER_LOCK_ID` (`reserved_capacity_broker.py:266-268`). Never nested, so no new lock-order surface.

### Claim row change

Three nullable columns on `reserved_fill_claims_table` (`serve_state.py:233-259`), inserted after `launchable` (`:257`) and before `heartbeat_ts` (`:258`), all `server_default=None`:

```python
sqlalchemy.Column('demonstrated_need', sqlalchemy.Integer, server_default=None)
sqlalchemy.Column('boot_hold', sqlalchemy.Integer, server_default=None)
sqlalchemy.Column('activity_ts', sqlalchemy.Float, server_default=None)
```

`activity_ts` is the mandatory anti-skew witness. `upsert_reserved_fill_claim` (`serve_state.py:4946-4999`) builds its `values` dict from the columns *that binary* knows (4962-4972) and the `ON CONFLICT DO UPDATE` `set_` comprehension iterates `values` (4990-4996), so an old binary heartbeating a migrated row advances `heartbeat_ts` while leaving the three new columns frozen indefinitely. If the frozen value were `demonstrated_need = 0`, that service would be judged permanently idle and walked to its floor while actually busy. `_claim_input` therefore treats a claim as **blind** unless `0 <= heartbeat_ts - activity_ts <= RESERVED_FILL_ACTIVITY_MAX_LAG_SECONDS` (60.0, `= 3 * LB_CONTROLLER_SYNC_INTERVAL_SECONDS`, matching `_staleness_threshold_seconds` at `autoscalers.py:4156-4164`). A new binary writes `activity_ts` unconditionally from the single `now` computed at `broker.py:268`, in the same INSERT, so the pairing is exact-float and epsilon-free. An old binary fails the lag check within one poll interval and its claim is ungated, which is today's behavior. This invariant must be pinned by a real-Postgres test, because it silently degrades to "trusts stale data" if `heartbeat_ts` ever stops advancing.

`upsert_claim` (`broker.py:253-301`) gains `activity: dict[str, Any] | None = None` after `effective_cap` (`:262`), defaulted so every existing caller and all 133 existing broker tests stay valid, and expands it into the three columns.

### Round row change

One nullable Text column `utilization_state` on `reserved_fill_rounds_table` (`serve_state.py:265-325`), the exact structural sibling of `feed_state` (287-288). Per-claimant entry:

```json
{"cap": 61, "hot_until": 1753435200.0, "stepped_at": 1753434900.0, "blind_since": null}
```

Putting the durable state on the round row rather than in controller memory is the single most important structural choice. It survives controller restart, api-server pod Recreate and broker writer rotation; it is written under the lease CAS in `publish_reserved_fill_round` (`serve_state.py:5221-5292`) so two writers cannot race it; and skew is self-healing in the safe direction, because an old binary's publish omits `utilization_state` from its `values` dict and the `set_` comprehension therefore leaves it untouched, so one old-binary round computes ungated grants and the next new-binary round resumes from the preserved state. Entries for claimants absent from `claim_rows` are dropped on each rebuild, mirroring the sticky rebuild at `allocation.py:274-289`, so the JSON cannot grow unbounded.

### The release governor (pure, `reserved_capacity_allocation.py`)

```python
def advance_release_target(prev, *, floor, holdings, need, boot_hold, blind,
                           now, dwell, step_seconds, step_fraction, min_step,
                           headroom, blind_grace):
    cap = int(prev['cap']) if prev else max(floor, holdings)
    hot_until = float(prev['hot_until']) if prev else now + dwell
    stepped_at = float(prev['stepped_at']) if prev else now
    blind_since = prev.get('blind_since') if prev else None

    if blind:
        blind_since = now if blind_since is None else float(blind_since)
        if now - blind_since <= blind_grace:
            # FREEZE. Never raise (that would undo a decay in progress),
            # never lower (we cannot see the work), pause the step clock.
            return _entry(cap, max(hot_until, now + dwell), now, blind_since)
        need, boot_hold = 0, False   # wedged past grace: resume the decay
    else:
        blind_since = None

    target = max(floor, math.ceil(need * (1.0 + headroom)))
    if target > cap:                            # RISE: one round, no schedule
        return _entry(target, now + dwell, now, None)
    if boot_hold:                               # authorized fleet still booting
        return _entry(cap, max(hot_until, now + dwell), now, None)
    if now < hot_until:                         # DWELL
        return _entry(cap, hot_until, stepped_at, None)
    if holdings > cap:                          # previous step not yet actuated
        return _entry(cap, hot_until, now, None)
    if now - stepped_at < step_seconds:
        return _entry(cap, hot_until, stepped_at, None)
    anchor = max(floor, min(cap, holdings))
    step = max(min_step, math.ceil(step_fraction * (anchor - floor)))
    return _entry(max(floor, anchor - step), hot_until, now, None)
```

Five properties are load-bearing.

*The blind branch freezes and never raises.* Raising to `max(cap, holdings)` on a blind round would reset a decay in progress every time the api-server pod restarts, and since all controllers restart together, every claimant's decay would reset simultaneously. On this fork's deploy cadence that alone would make the feature never complete a release cycle.

*The step is gated on actuation.* `holdings > cap` means the previous step has not drained yet, so no new step is proposed. This keeps the cap at most one step ahead of physical reality (the "cap runs ahead of pods" problem), and it converts a stuck drain (an occupancy-unknown replica that keeps aborting its retirement at `replica_managers.py:5962-5965`) into a visibly stalled cap rather than a cap sitting at the floor with 77 pods still running. Effective step period in practice is 300s plus the actuation lag, i.e. 350-450s.

*Rounding is `max(floor, anchor - step)` with `min_step = 2`.* A pure geometric decay never terminates in integers. The minimum step guarantees monotone progress and kills the long 1-replica tail.

*The rise carries a 25% headroom* (`RESERVED_FILL_UTILIZATION_HEADROOM = 0.25`), so the gate is never the binding constraint on a service that is actively growing. The ordinary autoscaler target and `effective_cap` stay the binding constraints on the way up, exactly as today.

*`stepped_at` is refreshed on rise, boot hold, blind freeze and unactuated steps*, so no combination of pauses can produce a double step when rounds resume.

**Known limitation -- intermittent blindness stalls the release.** The freeze both restarts the dwell (`hot_until = max(hot_until, now + dwell)`) and pauses the step clock (`stepped_at = now`). A blind round recurring more often than once per dwell (300s, five poll intervals) therefore rewinds the schedule before it can complete, so a claimant that is idle whenever it *is* seen but blinds on a sub-dwell cadence never releases -- and the `blind_grace` escape never fires either, because `blind_since` is cleared on every seen round, so no blind *streak* ever reaches 900s. This is the safe-direction failure (a possibly-busy service keeps its capacity, never over-released), but it is the same "silently inert" pathology this design set out to remove, now keyed on blind *frequency* rather than a flapping replica count. It does not surface in a period of infrequent LB rolls (the motivating incident's ~55min cadence leaves clean dwell windows) but does under a crash-looping LB or a deploy storm that reblinds a pool every few minutes. Fixing it correctly requires accumulating confirmed-idle progress across blind blips without banking a double step, i.e. new state rather than a one-line clamp, and is deferred until the gate is enabled on a service where it bites. It is pinned by `test_intermittent_blindness_within_the_dwell_stalls_the_release` / `..._spaced_past_the_dwell_still_releases` so any future change is a visible diff, and the rollout observation and standing alert below watch for it explicitly.

Constants, in `sky/serve/constants.py` next to the reserved-fill block at 490-524, each with its derivation in a comment:

| Constant | Value | Derivation |
| --- | --- | --- |
| `RESERVED_FILL_IDLE_DWELL_SECONDS` | 300.0 | equals `downscale_delay_seconds` on both live services (gate at `autoscalers.py:5199-5200`), equals `RESERVED_FILL_CLAIM_TTL_SECONDS` (`constants.py:497`), 5 poll intervals, 15 LB syncs, 5x the 60s report-staleness threshold |
| `RESERVED_FILL_RELEASE_STEP_SECONDS` | 300.0 | one step per `downscale_delay` window, so the local scale-down can actuate a step before the next is proposed |
| `RESERVED_FILL_RELEASE_STEP_FRACTION` | 0.25 | release rate proportional to surplus; scale-free across a 10-replica and a 200-replica fleet |
| `RESERVED_FILL_RELEASE_MIN_STEP` | 2 | integer termination |
| `RESERVED_FILL_UTILIZATION_HEADROOM` | 0.25 | growth room so the gate never throttles the autoscaler's own target |
| `RESERVED_FILL_ACTIVITY_MAX_LAG_SECONDS` | `3 * LB_CONTROLLER_SYNC_INTERVAL_SECONDS` = 60.0 | matches `_staleness_threshold_seconds` (`autoscalers.py:4156-4164`); the version-skew discriminator |
| `RESERVED_FILL_BLIND_GRACE_SECONDS` | 900.0 | 15 rounds, 3x the claim TTL; a permanently wedged LB must not pin 74 A100s forever |
| `RESERVED_FILL_UTILIZATION_GATE_ENV_VAR` | `SKYPILOT_SERVE_RESERVED_FILL_UTILIZATION_GATE` | process-wide kill switch, parsed like `claim_ttl_seconds()` (`broker.py:60-70`) |

These are pool-global on purpose. A per-service half-life or dwell would let the slower-decaying service win every contested transient purely by decaying slower, which is the static-priority pathology being removed.

### Entitlement change

One optional field on `ClaimInput` (`reserved_capacity_allocation.py:9-30`), defaulted so every existing construction, pickle and `dataclasses.replace` stays valid:

```python
utilization_cap: int | None = None   # None = ungated, today's exact behavior
```

One replacement of the cap construction in `compute_entitlements`, replacing lines 140-145:

```python
caps: dict[str, int | None] = {}
for name, claim in claims.items():
    bounds = []
    if claim.effective_cap is not None:
        bounds.append(max(0, claim.effective_cap - claim.attainable_floor()))
    if claim.utilization_cap is not None:
        bounds.append(max(0, claim.utilization_cap - claim.attainable_floor()))
    caps[name] = min(bounds) if bounds else None
```

Lines 136-139 (`scale_floors`, `remainder`) and 146-149 (`water_fill`, `floors[name] + shares[name]`) are untouched. `attainable_floor()` (25-30) is untouched, so the floor is structurally immune to the gate and requirement 5's decay target is an invariant, not a check. `water_fill`'s existing cap-redistribution loop (100-112) delivers symmetric borrowing for free: a gated claimant's unused share is redistributed to the ungated ones with no new code.

Conservation proof, unchanged from today's argument because the gate is a pure tightening of a mapping the invariant never depended on:

1. `scale_floors(total, attainable_floors)` gives `sum(floors) <= max(0, total)`. Identity branch (59-60) when `floor_sum <= total`; otherwise `_largest_remainder_round(scaled, total)` (62-63) sums to exactly `total` by construction (33-46).
2. `remainder = max(0, total) - sum(floors) >= 0` (139).
3. `water_fill(remainder, weights, caps)` satisfies `sum(result) <= remainder` for *every* caps mapping: `remaining` initializes to `max(0, amount)` (98), each iteration's `rounded` sums to exactly `remaining`, `give <= rounded[name]` because the cap clamp at 102-106 only lowers it and `give = max(0, room)` is never negative, and `remaining -= give` (108-109) so `remaining` is non-increasing and never negative.
4. Therefore `sum(entitlements) <= sum(floors) + remainder = max(0, total) <= total`, given `total = entitlement_free + conserved_holdings >= 0` (`broker.py:892-907`).
5. The gate makes the inequality strict in the idle case. `water_fill`'s `active` construction at 77-81 is `weights[name] > 0 and (caps.get(name) is None or (caps[name] or 0) > 0)`; when every gated headroom is 0 the active list is empty, the `while` loop never runs, all shares are 0, and `sum(entitlements) = sum(floors)`.
6. Lower bound preserved: `sum(entitlements) >= sum(floors)` always.

`compute_feeds` (197-290) needs **no change**, and it is worth stating why, because it looks like a hole. An idle claimant's feed need is `max(0, min(damped, raw, effective_cap) - holdings_fill)` (237-243), and its grant is capped at its release target, so an idle claimant can only ever be fed up to its own floor deficit. It cannot re-absorb what it released, and it cannot outbid a busy peer for capacity above its floor no matter what its weight is. Floor refill *does* win over a peer's above-floor growth, which is deliberate: that is what "guaranteed" means.

### Round assembly

`_claim_input` (`broker.py:318-346`) reads the three new columns with the tolerant `.get` pattern already used for `effective_cap` at 319, applies the `heartbeat_ts - activity_ts` lag check and the env kill switch, and returns an `ActivityInput` alongside the `ClaimInput`. It does not compute the cap, which needs the round's `now` and the previous state.

In `_run_round_locked`, the advance goes **inside the `if query_ok:` branch** (`broker.py:892-957`), after the live-holdings `dataclasses.replace` at 862-868 (so `holdings` is the row-scan-corrected value, not the possibly-stale claim value) and immediately before `compute_entitlements` at 908. Placing it before the `query_ok` split, as an earlier draft did, would let the governor advance on measurement-blackout rounds where grants are never recomputed: N consecutive blackout rounds would silently walk the cap down and then apply the whole drop in one step when the query recovered, possibly with `holdings_shrank` confirmed (927-941) so the immediate-down bypass (`allocation.py:184-185`) skipped the damping entirely.

The blackout branch (`broker.py:958-992`) needs two changes. Its floor at `claim.holdings_fill` exists so a blackout never strips a live replica's shelter, but as written it also *un-decays* a claimant mid-release, republishing its grant at its holdings and making `Sum(grants) > total` for that round. Change it to floor at `min(claim.holdings_fill, carried_cap)`, and carry `utilization_state` forward with `stepped_at = now` and `hot_until = max(hot_until, now + dwell)` for every claimant so a long blackout cannot produce a double step on recovery.

The single-claimant fast path (`broker.py:817-841`) **must** be gated. Left alone it publishes `grants = {service: None}`, `collect_reserved_capacity` stores that as `_fill_grant = None`, and `autoscalers.py:1129-1133` applies no ceiling at all, so the gate is computed and discarded. That configuration (one fill claimant plus several non-claimants) is precisely the one requirement 3 describes, and it also arrives by accident whenever a peer's claim TTLs out. Changes:

- `grants = {service_name: cap}` when the gate is armed for that claimant, else `None` as today.
- `feeds = {service_name: free}` unchanged, raw measured free. The #108 feed identity is preserved and it is safe, because the launch side is separately clamped by `fill_ceiling_launch` (`autoscalers.py:1191-1210`).
- `raw_grants = {service_name: cap}` instead of `{}`. Without this, the first multi-claimant round after the transition finds `prev_published` carrying an integer but `prev_raw` empty, and `damp_grants` takes the `last_proposed is None` branch (`allocation.py:177-182`) and stalls the up-move for a round.
- `feeds_changed` stays forced False for `len(claims) == 1` (`broker.py:1005-1006`), unchanged. Grant changes do bump the pool epoch (995-1031), but the step-and-settle schedule produces ~10 grant changes per full release instead of ~110 for a per-round decay, and each one is a real allocation change that the fence exists to protect.

### The demand-gate split

`_demand_should_skip_zero_cost` (`replica_managers.py:3633-3675`) returns True when `holdings >= grant`, reading `get_cached_grant` (`broker.py:163-172`). With a decayed grant of 16, protenix's burst would have its DEMAND launches steered to paid capacity for the ~120s that `damp_grants` takes to walk the ceiling back up. That is the exact opposite of requirement 1, and it also closes the fastest reacquisition path available on a saturated pool.

Fix: add `demand_gate_grant = max(damped, raw)` to the `Allocation` dataclass (`broker.py:121-129`) and store *that* in `_GRANT_CACHE` at both write sites (`broker.py:606` in `_allocation_from_round`, which can compute it from the round row's `raw_grants` column, and `broker.py:1071`), while `allocation.grant` keeps the damped value for the ceiling. The two consumers need caution in opposite directions: the ceiling must be conservative on the way up (do not launch what you are about to cull), the demand gate must be permissive on the way up (do not push a burst to paid capacity for two minutes). Because the rise is instantaneous, `raw` jumps in the same round the burst is observed, so the demand gate reopens within one poll interval.

### Release path and end-to-end latency budget

protenix, logical replica unit, `graceful_drain_async_occupancy: true`, `graceful_drain_seconds: 7200`, `downscale_delay_seconds: 300`.

| # | Step | Code | Latency |
| --- | --- | --- | --- |
| 1 | Last request completes, LB reports zero occupancy | `LB_CONTROLLER_SYNC_INTERVAL_SECONDS` = 20 (`constants.py:230`) | 0-20s |
| 2 | Poller samples the autoscaler, upserts the claim | `_broker_cycle`, `reserved_capacity.py:456-485` | 0-60s |
| 3 | Dwell: `need` must stay 0 | `advance_release_target` | 300s |
| 4 | First step, cap 77 -> 61 | round, `broker.py:908` (freshness gate 686-689 may defer one round) | 0-60s |
| 5 | Damping: free-driven down needs two rounds | `allocation.py:186-191`; a monotone descent satisfies it from the second round, so a constant one-round lag, removed entirely once `holdings_shrank` confirms (`broker.py:927-941`) | 0-60s |
| 6 | Grant delivered to the autoscaler | `reserved_capacity.py:504-509` -> `autoscalers.py:691-693` | same cycle |
| 7 | Ceiling applied | `fill_ceiling = _fill_grant + zero_cost_demand_placed`, `autoscalers.py:1129-1141`; decision interval 20s (`constants.py:568`) | <= 20s |
| 8 | Shelter quota shrinks, unsheltered SCALE_DOWNs pass | `autoscalers.py:1147-1190` (quota taken from the tail, i.e. least-preferred victims keep it) | 0 |
| 9 | Victim must be non-busy; `downscale_delay` already elapsed for an idle service | `autoscalers.py:6759-6763`, `5199-5205` | 0-300s, typically 0 |
| 10 | Logical batch: a served victim requires not-unknown and `in_flight == 0` | `replica_managers.py:6074-6285`, gate ~6228-6231 | 0 |
| 11 | Drain proof via `_ReplicaDrainTracker` | `replica_managers.py:5839-5995`, tracker 700-795; 7200s is a cap, not a wait (`_wait_for_drain` short-circuits at 621-656) | 20-60s typical |
| 12 | Pod deleted, GPU released | `_terminate_replica`, `replica_managers.py:4634+` | 10-30s |
| 13 | Slot readable as free | `query_pool_group_observation`, `reserved_capacity.py:161-194` | 0-60s |

**First freed A100: ~11 minutes worst case, ~8 minutes typical.**

Full trajectory, cap 77 with floor 16, `step_fraction` 0.25, `min_step` 2, steps at ~400s effective (300s schedule plus actuation): 77, 61, 49, 40, 34, 29, 25, 22, 20, 18, 16. Ten steps.

- 50% of the 61-replica surplus released by ~25 minutes.
- 80% by ~45 minutes.
- Floor reached by ~72 minutes.

These are grant trajectories and therefore upper bounds on the pod trajectory: `max_scale_down_rate_percentage` (`autoscalers.py:3888-3889`), `_consume_downscale_pressure_veto` (5207-5228) and the actuation gate can each make the physical release slower, never faster.

### Anti-thrash

Seven layers, five of which already exist and are untouched.

1. Dwell: 300s of continuously-zero need before the first step, reset on every rise.
2. Actuation gate: no step until the previous one is physically realized.
3. Step schedule: at most one step per 300s, at most 25% of the surplus.
4. `damp_grants` unchanged (`allocation.py:152-194`): two-round persistence in both directions. A single anomalous sample cannot move the published grant.
5. `compute_feeds`' `min(damped, raw)` clamp (237-243) is already exactly right for a decaying grant: during a down-move's damping window the published grant sits above the raw entitlement, and feeding that gap would launch a replica the grant is about to catch down and cull.
6. Feed stickiness (`RESERVED_FILL_STICKY_FEED_INTERVALS` = 2, `constants.py:508`) unchanged.
7. Local rate limits unchanged. The physical kill rate is bounded independently of the broker.

A pod dies only when the governor, the damper and the local downscaler all independently agree, on three separate clocks, one of which is the LB's own occupancy proof.

### Configuration and migration surface

Schema (`sky/utils/service_schema.py:355-379`, `additionalProperties: False` so it must be declared):

```yaml
replica_policy:
  reserved_capacity_fill:
    floor_replicas: 16
    weight: 4
    utilization_gate: true     # NEW, default false
```

Plumbed through `service_spec.py` validation (187-251), the `reserved_fill_utilization_gate` property (near 1498-1519, default False), `to_yaml_config` round-trip (1230-1240), `__setstate__` default (766) and `override` passthrough (1710-1711); snapshotted on the autoscaler at `autoscalers.py:377-385` and refreshed in `update_version` (553-560).

Migration: new `sky/schemas/db/serve_state/030_reserved_fill_utilization_gate.py`, `down_revision = '029'`, four `db_utils.add_column_to_table_alembic` calls (three on `reserved_fill_claims`, one on `reserved_fill_rounds`) inside `with op.get_context().autocommit_block():`, all `server_default=None`, no-op `downgrade`. Style from `029_restart_safe_placement_policy.py`; single-column-on-a-reserved-fill-table shape from `005_reserved_fill_phantom_streak.py`. Explicitly **not** the `004_reserved_fill_broker.py` precedent (columns folded into a create), because these land on a live table. Bump `SERVE_VERSION` from `'029'` to `'030'` at `sky/utils/db/migration_utils.py:54`.

The serve DB resolves to PostgreSQL in the prod api-server pod (`db_utils.get_engine` returns Postgres when `ENV_VAR_IS_SKYPILOT_SERVER` and `ENV_VAR_DB_CONNECTION_URI` are both set), so the fork's Postgres-only policy applies. Ordering is closed by construction: `charts/skypilot/templates/database-migration-job.yaml` runs to completion before the api-server pod starts, and all controllers live in that pod. The additive `server_default=None` columns also make it safe to run the migration ahead of the binary deploy.

## Rejected alternatives

**Claim-side idle decay (`activity_cap`): the claimant publishes an already-decayed self-cap, allocation math untouched.** The seam is genuinely elegant, and this design borrows it: folding a claimant-derived cap into `effective_cap` does bite `attainable_floor()`, the headroom and the feed need, which are exactly the three places it must. It lost on durability and on measurement scope. The decay state lived in controller memory, and its own fail-closed rule defeated its rehydration: `has_fresh_demand_report()` is false on the first tick after any restart, which disarms the tracker and republishes a NULL cap in the same tick. Since all controllers restart together inside one api-server pod, every routine deploy would simultaneously reset every claimant's decay, restore full weighted entitlements within 120s, and (if any free existed) feed 40 new 1800s-readiness replicas that would be released again 30 minutes later. Its busy predicate was also service-level and all-or-nothing over `unknown_replicas > 0`, so one flapping replica out of 77 would pin protenix busy forever, making the feature silently inert on the one service it was written for. This design keeps the cap-tightening seam, moves the state to the round row in Postgres, and replaces the service-level boolean with a per-replica count.

**Two-tier lease: guaranteed floors plus activity-renewed borrowed slots.** The reframing of `floor_replicas` as the cold-start knob is the best restatement of the three and is adopted here almost verbatim. The mechanism lost on two structural defects. First, the leased tier was served from the remainder *before* the active water-fill, and a single request renewed the lease in full, so a claimant receiving one request every 890s would permanently outrank a peer with `queue_depth` 61 while holding everything above its floor. That reproduces the 05:22 incident exactly, and the design advertised it as its main anti-thrash property. Second, the busy stamp was placed inside `_collect_request_information_locked`, which does not run when the LB is dead, when `in_flight` is None (`autoscalers.py:4278-4284`), or when the controller is not demand-authoritative (`controller.py:1046-1050`), so its own fail-closed terms were dead code and an LB outage would age a busy service into "idle" and dump its fleet. The binary lease also cannot express partial release, which is the common case here. This design keeps the semantics, discards the lease structure, and measures in `_broker_cycle`, which runs unconditionally.

The exponential-envelope form of the broker-side gate (continuous per-round decay) was also rejected in favor of step-and-settle: a grant that moves every round bumps the per-pool fencing epoch every round (`broker.py:995-1031`), which is the exact pathology the in-code comment at 993-1000 warns about, and it lets the cap run arbitrarily far ahead of physical pod count when drains lag.

## Risks and mitigations

**1. Cold start is the real cost and only `floor_replicas` addresses it.** After a full release, a burst is served by the floor alone for 1200s (boltz-l4-fleet) to 1800s (protenix). The user chose this knowingly. The consequence is that `floor_replicas` moves from a rarely-binding fairness minimum to the number that determines burst SLA. Sizing rule: `floor = ceil(B * D / T_ramp)`, where `B` is the burst that must not wait, `D` the mean request duration, `T_ramp` the readiness delay. Derivation: the warm base clears `B` jobs in `B*D/floor` seconds and the elastic fleet becomes useful at `T_ramp`; equating them is the point where the marginal warm replica stops buying latency, because below it the burst waits on the floor and above it you are paying to keep capacity the ramp would have delivered by the same moment. **Recalibrate `D` before enabling on protenix**: read `effective_request_duration_seconds` from `Autoscaler.info()` (surfaced at `autoscalers.py:6923-6929`, estimator state 7001-7005) or from `serve_autoscaler_history` (serve migration 019). If `D` is really ~1800s rather than ~600s, the formula gives `floor ~= 52` and the honest conclusion is that protenix should barely decay at all. This is a gate on Milestone 4, not a caveat.

**2. Release may be one-way on this cluster (highest severity).** SkyPilot's inference pods run in `rescluster-k8s-prod-east1-preemptible-inference` below the research tenant in scheduling priority, and `hyperpod-ns-research` holds 241 of 328 GPUs with standing demand. A released A100 is likely bound to a pending research pod within seconds, well inside the 60s `query_pool_group_observation` interval, so `observed_free` never rises and every `feed` stays 0, which is already today's steady state. A raised grant then authorizes nothing. Three consequences that must be accepted explicitly: (a) `floor_replicas` is a hard capacity contract, not a soft warm base, and should be rounded up, not down; (b) `scale_floors` (`allocation.py:49-63`) scales the floors themselves down proportionally if the pool total falls below `Sum(floors)`, so "guaranteed" is guaranteed relative to what SkyPilot still physically holds, not absolutely; (c) the fastest reacquisition path is the ordinary demand scale-up onto the zero-cost tier, which is why the `max(damped, raw)` demand-gate split is mandatory and not an optimization. Mitigation before Milestone 4: measure reacquisition empirically on `boltz-l4-fleet-test` (floor 0, no production traffic) by releasing and observing whether the freed GPUs are still available to SkyPilot one poll interval later. If they are not, this design should ship with much larger floors and a smaller `step_fraction`, and open question 2 below needs a user decision.

**3. The 7200s `graceful_drain_seconds` on protenix is a cap, not a delay, but it is also the failure mode.** For a genuinely idle replica the drain tracker proves drained in one to two LB syncs (`replica_managers.py:5839-5995`, tracker at 700-795) and `_wait_for_drain` short-circuits (621-656). But with `graceful_drain_async_occupancy: true`, a replica whose occupancy the LB never validity-filters (`controller.py:749-795`) is UNKNOWN, and a current-version logical victim that cannot prove it drained has its retirement **aborted** and rejoins service (5962-5965). Under this design that surfaces as a stalled cap (the actuation gate stops stepping) rather than a runaway release, which is the correct fail-closed behavior but is silent. Mitigation, must ship with the feature: alert when `holdings_fill > cap` persists for more than three consecutive rounds, and when `unknown_replicas > 0` in the Concurrency report for more than an hour.

**4. Version skew.** An old binary heartbeating a migrated claim row freezes the three new columns while advancing `heartbeat_ts`; defeated by the `heartbeat_ts - activity_ts <= 60s` lag check, which fails open within one poll. An old binary driving a round leaves `utilization_state` untouched (its publish `values` dict omits it) and computes ungated grants for one round; self-healing. A new binary against a pre-030 DB raises `ProgrammingError` from `get_reserved_fill_claims` (`serve_state.py:5079` emits an explicit column list from Python metadata), which escapes `_run_round_locked`, passes `run_round_if_stale` (catches only `locks.LockTimeout`, `broker.py:661`) and is swallowed by the poller's broad handler (`reserved_capacity.py:593-595`); `collect_reserved_capacity` is then never called, so `_fill_grant` freezes (it is never staleness-decayed) while `_fill_free_slots` decays to 0 after three intervals. Closed by the pre-deploy migration job ordering, but the handler at 593-595 should also log the exception type and traceback, because today a schema error is indistinguishable from a transient one.

**5. The no-allocation path removes the ceiling entirely.** `collect_reserved_capacity(0, keys, time.time())` at `reserved_capacity.py:496` and `:502` leaves `grant` at its `None` default, and `autoscalers.py:691` assigns it unconditionally, so a round-lock timeout or a rejected claim un-caps the fill fleet for that cycle. It cannot launch anything (feed is 0 and `_fresh_fill_free_slots` decays, `autoscalers.py:750-758`) and the next successful round re-applies the cap within 60s, so this stays as-is: it is the existing fail-open to pre-broker behavior and changing it is out of scope. Worth knowing when reading logs during a decay.

**6. Cross-service handoff is slow and burns readiness time.** A full transfer costs the dwell plus the step schedule plus the release chain plus the acquirer's 1200-1800s readiness, roughly 45 to 90 minutes end to end, during which the transferred GPUs serve nothing on either side. This design is a decongestant, not a fast load balancer between services. If the two services alternate on a sub-two-hour cadence, a large fraction of the transferred GPU-hours is spent booting, and the correct answer is larger floors on both, not a faster gate.

**7. Visible utilization drop that will be reported as a regression.** With both services idle and the recommended floors, SkyPilot occupancy on this pool falls from 87 to 28 of 328 GPUs. That is the intended outcome, but a dashboard tracking SkyPilot GPU-hours will show a step change. Pre-brief: GPU-hours-held stops being the metric; served-work-per-GPU-hour becomes it.

**8. Constants tuned against one day of traffic.** `step_fraction`, `min_step` and the 25% headroom were chosen against a single 52-request protenix burst at 08:06 and one boltz queue episode at 05:22. They are defensible, not validated. Mitigation: replay `serve_history.serve_request_activity_history_table` (`serve_history.py:107-140`, minute-bucket per-service arrival history, already in the same Postgres DB) through `advance_release_target` offline before enabling on protenix. Blast radius of a bad constant is one env-var flip.

## Configuration for the live services

### Available today, no code change

Two levers exist right now. They are genuinely different and should not be conflated.

**(a) Rebalance weights and set protenix a floor.** Immediately fixes the both-busy starvation, costs nothing, and is a strict prerequisite for the gate anyway. It does **not** release idle capacity, because `water_fill` still disposes of the whole remainder every round.

```
sky serve update protenixv2-hybrid-v1 ...   # reserved_capacity_fill: {floor_replicas: 16, weight: 4}
sky serve update boltz-l4-fleet ...         # reserved_capacity_fill: {floor_replicas: 12, weight: 1}
```

With floors 16/12/0 and weights 4/1/0.1 on an 87-slot pool: floors 28, remainder 59 split 46/12/1, giving protenix 62, boltz-l4-fleet 24, test 1. Under the observed 05:22 conditions boltz-l4-fleet would have had 24 replicas for its 61-deep queue instead of 10.

**(b) Lower `max_replicas` on the fill claimants.** This is the only existing lever that can create genuinely free capacity, because `effective_cap = max(0, max_replicas - demand_target)` (`reserved_capacity.py:474-475`) becomes the binding headroom cap. To leave N GPUs free you need `Sum(max_replicas - demand_target) < 87`. It is **traffic-blind and permanent**: it caps the burst response by exactly the same amount it releases, at all times, whether or not anyone needs the capacity. Use it only as a bridge, and prefer (a) alone if you can wait for the code. That `max_replicas` is the only existing lever, and that it cannot distinguish "idle" from "small", is the argument for building the gate.

### Post-change values

| Service | Knob | Current | Recommended | Reasoning |
| --- | --- | --- | --- | --- |
| `protenixv2-hybrid-v1` | `floor_replicas` | absent (0) | **16** | `ceil(B*D/T_ramp)` with `B` = 52 (observed 08:06 burst), `T_ramp` = 1800 (readiness `initial_delay_seconds`), `D` ~= 600s: `52*600/1800` = 17.3. 16 warm replicas clear a 52-job burst in ~3.3 waves, roughly the same wall time as the elastic ramp's cold start, which is the efficient crossover. Recalibrate `D` before enabling the gate; if `D` ~= 1800s the formula gives ~52 and the gate should be enabled only after that conversation. |
| | `weight` | `1000000.0` | **4** | `1e6` is exactly `RESERVED_FILL_MAX_WEIGHT`; it encodes "boltz-l4-fleet gets literally nothing above its floor". 4:1 against boltz reflects the observed ratio of outstanding work (52 vs ~11 at the two recorded episodes) and preserves protenix's precedence without zeroing a peer. |
| | `utilization_gate` | n/a | **true** (last to enable) | 1800s readiness and `graceful_drain_async_occupancy: true` make this the highest-risk service. |
| | `graceful_drain_seconds` | 7200 | **7200** (unchanged) | It is a cap, not a wait, and it is the guard that stops the ceiling from ever evicting a replica the LB cannot vouch for. |
| `boltz-l4-fleet` | `floor_replicas` | 10 | **12** | Same rule with `T_ramp` = 1200, `B` = 72 (queue 61 + in-flight 11 at 05:22), `D` ~= 180s: `72*180/1200` = 10.8. Validates the existing 10 and raises it modestly, justified because 10 replicas measurably produced a 61-deep queue. |
| | `weight` | 100 | **1** | The baseline the others are expressed against. Against protenix's 4 this is the intended 4:1. |
| | `utilization_gate` | n/a | **true** (enable second) | Smaller blast radius than protenix, physical replica unit, strict drain branch (`replica_managers.py:5975-5995`) so it releases more reliably. |
| `boltz-l4-fleet-test` | `floor_replicas` | 0 | **0** (unchanged) | A test service should hold nothing at rest, and under the new model it genuinely does instead of holding whatever it last drifted into. |
| | `weight` | 0.1 | **0.1** (unchanged) | Already encodes "loses every contention", which stays correct. Its practical path is now the free-slack path, not the grant path. |
| | `utilization_gate` | n/a | **true** (enable first) | No production traffic; the correct place to validate the full 13-step release chain and, critically, whether a released A100 is reacquirable at all on this cluster. |

Pool-level constants stay at their defaults. Do not set them per service.

### Resulting allocation, 87 fill-arbitrable GPUs

| Scenario | protenix | boltz-l4-fleet | test | Free | Today |
| --- | --- | --- | --- | --- | --- |
| Both idle | 16 | 12 | 0 | **59** | 74 / 10 / 0, free 0 |
| protenix busy (need 52), boltz idle | 65 | 12 | 0 | 10 | 74 / 10 / 0 |
| boltz busy (need 72), protenix idle | 16 | 71 | 0 | 0 | 74 / 10 / 0, impossible today at any weight |
| Both busy (needs 52 and 72) | 63 | 24 | 0 | 0 | 74 / 10 / 0 |

The third row is requirement 2: symmetric borrowing, and it is the exact inversion of the 05:22 incident.

## Milestones

Each is independently shippable and independently valuable.

**M0. Weights and floors, no code (0.5 day).** Apply the recommended `floor_replicas` and `weight` via `sky serve update` on all three services. Fixes the both-busy starvation immediately. Watch one full traffic cycle. Prerequisite for everything else, because enabling the gate on protenix while its floor is 0 would decay it to zero fill replicas.

**M1. Persistence (1 day).** Three columns on `reserved_fill_claims`, one on `reserved_fill_rounds`, migration `030`, `SERVE_VERSION` bump. No reads, no policy, no behavior change. Ships and deploys alone so the migration is proven against the live table before any code depends on it.

**M2. Signal, log only (1.5 days).** `_outstanding_work_parts` extraction, `FillDemandSample`, `Autoscaler.fill_demand_sample`, the `ConcurrencyAutoscaler` override, the `_broker_cycle` measurement, the claim writes, the `activity_ts` lag check in `_claim_input`, and the extended round and per-service log lines. The gate is not wired to `compute_entitlements`. **Bake for one week** and confirm that measured idle and busy transitions match the known traffic history in `serve_request_activity_history`, and that protenix's `demonstrated_need` actually reaches 0 (if `unknown_replicas` keeps it pinned, the feature would be inert and M3 should not ship as designed).

**M3. The gate (1.5 days).** `advance_release_target`, `ClaimInput.utilization_cap`, the `compute_entitlements` cap tightening, the `utilization_state` round column wiring, the single-claimant fast-path gating (grants, raw_grants, feeds unchanged), the blackout-branch floor and carry, the `demand_gate_grant` split, the schema and spec knob, the env kill switch. Default off. Ships dark.

**M4. Enable, staged (0.5 day plus bake).** Recalibrate `D` for protenix first. Then `boltz-l4-fleet-test` (validate the full release chain and reacquisition), then `boltz-l4-fleet`, then `protenixv2-hybrid-v1`, with at least one full traffic cycle between each.

Total engineering effort ~4.5 days, plus roughly two weeks of staged bake.

## Test plan

Repo philosophy: the minimum tests that establish logic correctness, never assertions on log or error message text.

### `tests/unit_tests/test_reserved_fill_broker.py` (2134 lines, 77 tests today) - pure math

- `advance_release_target`: rise is instantaneous and ignores dwell and step schedule; dwell blocks the first step for exactly `dwell` seconds measured in wall clock (a delayed or skipped round does not shorten it); `boot_hold` blocks a step and extends `hot_until`; `holdings > cap` blocks a step (actuation gate); the step is `max(min_step, ceil(fraction * (anchor - floor)))` against hand-computed values for the 77 -> 16 trajectory; the cap converges to exactly `floor` in finite steps and never below it; blind freezes without raising and without lowering, and pauses the step clock; blind past `blind_grace` resumes the decay.
- `compute_entitlements`: `Sum(entitlements) <= total` under every combination of `effective_cap` present/absent and `utilization_cap` present/absent, including the all-capped case where the strict inequality must hold and `Sum(entitlements) == Sum(floors)`; floors are never gated (property test over random floors, caps and needs asserting `entitlement >= attainable_floor()`); a gated claimant's freed remainder is redistributed to an ungated peer by the existing `water_fill` cap loop.
- `compute_feeds` interaction: a gated claimant at its cap has need exactly 0 (it cannot re-absorb what it released); a gated claimant below its floor is still fed up to its floor.
- `damp_grants` interaction with a monotone stepwise descent: the published grant lags the raw by exactly one round, and the lag disappears once `holdings_shrank` is confirmed.
- `_claim_input`: an unpaired or out-of-lag `activity_ts` yields an ungated `ClaimInput`; the gate combines with `effective_cap` by `min` with None-as-unbounded on both sides; `dataclasses.replace(claim, holdings_fill=...)` at `broker.py:862-868` preserves `utilization_cap`.
- Single-claimant fast path: an armed gate publishes an integer grant and a non-empty `raw_grants`; `feeds` equals raw measured free; `utilization_gate: false` restores the `None` grant and the empty `raw_grants` exactly.
- Blackout branch: a carried grant is floored at `min(holdings_fill, carried_cap)`, so a decay in progress is not undone and `Sum(grants) <= total` still holds; `stepped_at` is pushed forward so recovery cannot double-step.

### `tests/unit_tests/test_reserved_fill_broker_pg.py` (4170 lines, 56 tests today) - real Postgres

- Migration `030` applies to a populated pre-030 `reserved_fill_claims` table; existing rows read as ungated.
- **Skew invariant (mandatory):** write a paired claim with the new writer, then simulate an old writer's upsert (a `values` dict omitting the three columns) and assert the round classifies the claim as blind within one poll interval and computes ungated grants.
- `utilization_state` survives writer rotation, is not clobbered by an old-shaped `publish_reserved_fill_round`, and is dropped for claimants whose claims expired.
- A three-claimant round where one claimant is gated and the others' grants rise, asserting `Sum(entitlements) <= total`.

### `tests/unit_tests/test_reserved_capacity_fill.py` (2159 lines, 114 tests today) - autoscaler side

- `fill_demand_sample` returns `None` when `has_fresh_demand_report()` is false, and on every non-`ConcurrencyAutoscaler` class.
- `demonstrated_need` is 0 only under the full six-term idle condition; each of the six independently forces a non-zero need (in-flight, queue depth, retained rejections, unknown occupancy, a busy fill replica, a booting fill replica).
- An occupancy-unknown replica contributes to `need` **per replica**, not by pinning the service: 3 unknown of 77 gives `need = 3`.
- `_outstanding_work_parts` extraction is behavior-preserving: `_outstanding_work` returns the same value and still assigns `_weighted_queue_work` and `_rejected_concurrency`, while the pure variant assigns nothing.
- A reduced `_fill_grant` lowers `fill_target` and `surplus_covered` and admits previously-suppressed SCALE_DOWNs, and the resulting victims exclude every `_replica_is_busy` replica.
- Revise the pinned #108 single-claimant identity tests for the fast-path change; add coverage that `feed` is still byte-identical and that `utilization_gate: false` restores full identity.

### `tests/unit_tests/test_reserved_capacity_spec.py` (314 lines, 19 tests today)

- `utilization_gate` schema acceptance and rejection of unknown keys under `additionalProperties: False`; `to_yaml_config` round-trip; `__setstate__` default False for specs unpickled from pre-030 rows; `update_version` refresh.

### `tests/unit_tests/test_concurrency_autoscaler.py`

- `work_per_replica` uses `_effective_logical_capacity_per_gpu()` in logical mode and `target_concurrency_per_replica` otherwise, matching `_outstanding_work`'s own denominator.

### Live validation against the prod pool

1. **After M1:** confirm migration `030` applied (`SERVE_VERSION` at startup, and `\d reserved_fill_claims` shows the three columns as NULL on all three live claims). Confirm grants are byte-identical to before.
2. **After M2, one week:** for each service, plot `demonstrated_need` against the known traffic from `serve_request_activity_history`. Required observations before M3 is enabled anywhere: protenix reaches `need == 0` for at least one continuous 300s window per idle period (if `unknown_replicas` prevents it, stop and reconsider); boltz-l4-fleet's `need` tracks its queue episodes; no claim ever reads blind for more than one poll interval outside a deploy, **and no idle claim blinds more than once per dwell window** (the frequency, not just the duration, of blind rounds is what stalls a release; see the intermittent-blindness limitation above).
3. **After M4 step 1 (`boltz-l4-fleet-test`, floor 0):** drive one synthetic burst, let it go idle, and record the full 13-step chain with timestamps. Then measure the decisive question: one poll interval after each pod deletion, is the freed A100 still counted free by `list_accelerators_realtime`, or has the research namespace taken it? This single measurement determines whether release is reversible on this cluster and gates everything after it.
4. **After M4 step 2 (`boltz-l4-fleet`):** confirm the cap walks 22 -> 12 and that a synthetic burst restores full entitlement within 120s and reopens the demand gate (verifiable by observing that new demand replicas land on the zero-cost tier, not on paid capacity).
5. **After M4 step 3 (`protenixv2-hybrid-v1`):** watch the first idle-to-burst transition end to end. Alert at T+30min on `queue_depth > 0` or `rejected_in_recent_window > 0`. If the floor is undersized, raise it before touching any pool-level constant.
6. **Standing alerts, shipped with M3:** `holdings_fill > cap` for more than three consecutive rounds (stalled release, usually unknown occupancy); a claim blind for more than one hour while `serve_request_activity_history` shows zero arrivals for the same service over the same period (wedged LB, feature silently inert); a claim whose `cap` has not stepped down across several dwell windows while `demonstrated_need` reads 0 on every non-blind round over the same span (intermittent-blindness stall, feature silently inert -- distinct from the contiguous-blind alert because no single blind streak is long, the blind rounds are merely frequent); `Sum(grants) > total` in any round (conservation violation, should be impossible).

## Open questions

1. **What is protenix's actual `effective_request_duration_seconds`?** If it is ~600s the recommended floor of 16 stands. If it is ~1800s (which `graceful_drain_seconds: 7200` hints at), `ceil(B*D/T_ramp)` gives ~52 and protenix should barely decay at all, which changes whether M4 should enable the gate there and changes the entire value proposition for the largest holder on the pool. Resolvable from `Autoscaler.info()` or `serve_autoscaler_history` before M3 ships.
2. **Is a released A100 reacquirable on this cluster?** If `hyperpod-ns-research` absorbs every freed slot permanently, release is a one-way donation, `floor_replicas` becomes a hard permanent contract rather than a warm base, and `step_fraction` should be much smaller. Measured directly in live validation step 3. This is the question most likely to change the design.
3. **Given the answer to 2, does the user still want release when no peer needs the capacity?** Requirement 5 says yes and was chosen knowingly, but it was chosen before the one-way-release interaction was quantified. The alternative (release only against a peer's demonstrated need) satisfies requirements 1 and 2 but not requirement 3, because a non-claimant can never demonstrate need to the broker. This is a genuine fork in the design and needs an explicit re-confirmation with the reacquisition data in hand.
4. **Should `utilization_gate` default to true after the bake?** Shipping it opt-in is correct for this rollout, but leaving it opt-in forever means every new fill service starts with the wrong semantics. Proposal: flip the default in the release after M4 completes, with the flip called out in the changelog.
5. **Does `queue_depth` charged at one slot per unit need a concurrency divisor?** `_queue_work()` (`autoscalers.py:4651-4677`) already weights by priority timeout, and both live services run `target_concurrency_per_replica: 1`, so the current form is exact today. A future claimant with concurrency 8 and a queue of 80 would claim 80 slots instead of 10, over-estimating in the retain direction (never dumping a busy fleet). Carrying `target_concurrency_per_replica` on the claim is the v2 fix; it does not change anything for the three live services.