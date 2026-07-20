# Demand-aware logical scaling with bounded waves

## Problem

Logical concurrency autoscaling currently treats
`target_concurrency_per_replica` as both model execution concurrency and a
divisor over all outstanding work. For one-job-per-GPU models, configuring 10
to damp rejected bursts also divides real running occupancy by 10. This can
publish a target below the number of GPUs that are actively executing jobs.

The rejection signal has the opposite problem before saturation is applied.
The load balancer retains each uniquely rejected job for 360 seconds. Adding
that retained population one-for-one to current occupancy interprets every
rejected arrival as six minutes of concurrent GPU work, even when the typical
job takes 15 to 60 seconds.

Finally, once hysteresis accepts a higher raw target, SkyServe can adopt the
entire target in one decision. A short burst can therefore authorize hundreds
of logical GPU launches before any provisioned capacity becomes ready.

The production symptom has two separate parts. On 2026-07-19,
`boltz-l4-fleet` had a demand target of 3 logical slots with no rejected demand,
but 546 ready logical slots remained on 363 physical machines while service
versions 15 and 16 overlapped. Target correctness and physical rollout or
downscale convergence must remain separately observable.

## Behavior contract

### Demand components

For logical concurrency services:

```text
active_work = in_flight + queue_depth + unknown_occupancy_floor

retained_rejected_concurrency =
    rejected_in_retention_window * expected_request_duration_seconds
    / rejected_retention_window_seconds

recent_rejected_concurrency =
    rejected_in_recent_window * expected_request_duration_seconds
    / recent_window_seconds

rejected_concurrency = max(
    retained_rejected_concurrency,
    recent_rejected_concurrency)

effective_capacity_per_gpu =
    target_concurrency_per_replica
    * target_utilization_percentage / 100

raw_target = ceil(
    (active_work + rejected_concurrency)
    / effective_capacity_per_gpu)
```

`target_concurrency_per_replica` again describes simultaneous model execution
slots per GPU. For Boltz it is 1. `target_utilization_percentage` describes
request-slot headroom and is not hardware utilization.

The load balancer reports both the existing six-minute deduplicated rejection
population and the subset refreshed during the autoscaler's one-minute request
window. The recent rate reacts to spikes; the retained rate keeps retried work
as a slower pressure floor. A controller paired with an older load balancer
uses only the retained value.

If `expected_request_duration_seconds` is absent, rejected jobs retain the
existing one-for-one contribution for backward compatibility. The expected
duration conversion applies only to rejected pressure. Real running, queued,
and unknown work remain current state and are never duration-compressed.

The stale-report arrival floor uses the same workload conversion over its
60-second timestamp window:

```text
arrival_concurrency =
    arrivals_in_window * expected_request_duration_seconds
    / arrival_window_seconds
```

It is then divided by effective per-GPU capacity. Stale mode remains raise-only
and continues to prohibit scale-down.

### Scale-up wave

When raw demand exceeds the adopted target, a logical concurrency service may
adopt at most:

```text
step = max(
    scale_up_rate_min_replicas,
    ceil(current_committed_capacity
         * max_scale_up_rate_percentage / 100))

next_target = min(raw_target, current_committed_capacity + step)
```

Current committed capacity is latest-version nonterminal planned logical
capacity, including ready and provisioning backends. This prevents repeated
ticks from authorizing the same missing capacity while launches are pending.
On a version update, the previous version's adopted target is not treated as
already-authorized capacity for the new version. When the wave limiter is
enabled, the new version resets its adopted target to `min_replicas`; the next
fresh or stale recompute may then authorize at most one wave above the new
version's committed capacity. This keeps a rolling update from inheriting an
arbitrarily large target and launching that entire target from zero in one
reconciliation.
Because the adopted target is controller-local, the first fresh recompute
after a controller rebuild first raises its actuation baseline to current
committed capacity. Any lower raw demand then goes through the ordinary
downscale delay and wave limit instead of retiring a recovered fleet at once.

After one upward wave, another increase requires
`scale_up_rate_period_seconds`. Lower raw demand may still cancel an unneeded
target through normal downscale rules. A controller rebuild may allow a fresh
wave, but it still bases the ceiling on current committed capacity and can
never jump from zero to the complete raw target.

The limiter applies to demand-driven logical scale-up, including the stale
arrival floor. It does not throttle failed replica cleanup, explicit service
operations, or old-version retirement. Unknown-capacity replacement remains
bounded by its existing incident controls.

### Logical actuation generation fence

Logical actuation must not require the capacity snapshot generation to remain
bit-for-bit equal to the demand generation that produced the target. On large
fleets, a probe round or manager-lock wait can outlive the load balancer sync
interval, so an exact-generation fence can discard every scale-up or retirement
batch forever. While the exact published
`(version, decision_generation, target)` remains current, actuation may use any
fresh capacity snapshot from the same version whose generation is at least the
decision generation. A newer published target or version still supersedes an
unaccepted batch. Scale-up rechecks the fence and newest committed capacity
before each replica row is persisted. Scale-down recomputes coverage and
verifies each selected victim is still known idle in the newest snapshot before
marking it off-route. Scale-down counts one conservative slot per ready
old-version backend, but backends already off route never count toward
coverage.

A same-version target change does not blanket-cancel retirements that were
already durably accepted and taken off route. Before irreversible teardown,
each accepted retirement uses the newest fresh snapshot and current target to
recompute coverage without that victim. Ready capacity remains the requirement
for irreversible teardown. If ready capacity is temporarily short but
non-retiring, never-ready current-version capacity already committed to
provisioning covers the target, the accepted retirement stays off route and
waits for that capacity to become ready. Previously ready but currently degraded
or unobservable capacity does not qualify. Only a shortfall not covered by
either ready or committed capacity reactivates enough accepted retirements to
cover the gap; later victims then re-evaluate against that restored capacity. A
version change, pending newer version, stale demand snapshot, or unavailable
current target continues to block or abort retirement as appropriate. This
prevents one scale-up decision from both launching replacement capacity and
reactivating a large old fleet while preserving the ready-capacity fence before
destructive cleanup.
Unknown-capacity replacement stays tied to the exact decision generation; a
newer snapshot may narrow or cancel the set but cannot authorize new overlap
from stale evidence.

### Rolling replacement bridge

A logical rolling update must not wait for latest-version ready capacity to
reach the complete adopted target before retiring any old backend. That rule
can deadlock convergence when replacement supply is scarce, and it preserves a
large obsolete fleet even when current demand is small.

Old physical backends predate authoritative logical-width observations. Treat
each READY old backend as a conservative floor of one logical slot. For every
fresh demand report, calculate:

```text
coverage_target = max(raw_target, adopted_target)

required_ready_old_backends = max(
    0,
    coverage_target - latest_version_ready_logical_capacity)

excess_ready_old_backends = max(
    0,
    ready_old_backends - required_ready_old_backends)
```

An old backend that is not READY contributes no serving coverage and may be
retired first. A READY old backend is eligible only when the load balancer
reports it idle. Retire at most 20 eligible old physical backends per
autoscaler tick, taking non-READY backends before excess idle READY backends.
Busy or occupancy-unknown old backends remain protected. The conservative
one-slot floor guarantees the remaining old backend count plus observed latest
logical capacity is never below the larger of raw demand and the adopted
target.

An in-process service update is also a retirement-authority transition. A
newer pending version freezes already selected victims instead of returning
them to routing. Irreversibly committed teardowns continue through the shared
termination pool. Uncommitted, off-route victims retain their original drain
deadlines and are handed to the same bounded recovery path used after a
controller restart; they may be re-fenced only after the new version publishes
a fresh target and capacity snapshot. If that snapshot proves a current-version
capacity shortfall, recovery may reactivate only the capacity needed to cover
the shortfall. Runtime-equivalent uncommitted victims are relabelled to the new
version while remaining off route, so they can satisfy such a shortfall without
a replacement launch; irreversibly committed victims keep their original
version and finish teardown. The existing bounded recovery timeout remains the
availability fallback if an update cannot publish that evidence at all. During
a successful policy-only or runtime-equivalent update, asynchronously draining
backends must therefore stay off route instead of the whole old fleet becoming
READY again.

The 20-backend cap bounds each transition without tying rollout progress to a
wall-clock rate limit. If five new logical slots become ready, up to five
additional READY old backends become excess. If the old fleet was already far
above the coverage target, repeated 20-backend batches remove that proven
excess even before replacement supply reaches the complete target. Stale
demand reports continue to prohibit all rolling retirement. A pending logical
scale-up wave does not block the bridge because the raw-demand side of the
coverage target already protects work that the adopted target has not reached.

### Launch completion and teardown progress

The replica-manager refresher holds the manager lock while reconciling launch
and teardown workers. A large launch wave may finish many workers in one
refresh. Persisting each completed launch in a separate transaction makes the
lock-held pass grow with the wave size and can delay admission of already
selected teardown workers behind repeated PostgreSQL round trips. The rolling
bridge is then visibly bounded but provider cleanup does not keep pace.

The refresher reads all completed-launch replica rows in its existing batch,
applies their launch and placement outcomes in memory, and persists those
completed-launch transitions with one existing multi-row upsert. It removes
the completed workers from local tracking and schedules failed-launch cleanup
only after that batch commit succeeds. If the batch write fails, the workers
remain tracked and the next refresh retries the same durable transition.

This changes only persistence cardinality and retry atomicity for completed
launches. Pending-launch authorization, placement benching, launch and
termination admission order, shared resource limits, demand sizing, retirement
selection, and provider cleanup behavior remain unchanged.

### Load-balancer demand handoff

An HA load-balancer promotion temporarily preserves the previous active slot's
demand gauges so a cold promoted process cannot prove idle capacity and trigger
an early drain. The 60-second handoff countdown starts after the promoted,
authoritative slot reports the complete demand-gauge contract: in-flight work,
queue depth, retained and recent rejections, and explicit unknown-occupancy
URLs. It does not wait for every backend occupancy probe to succeed.

Backends missing a fresh occupancy sample remain represented in the current
report's unknown set and stay individually protected from retirement. Coupling
the whole demand handoff to complete occupancy would instead let one
unreachable backend preserve an obsolete queue or rejection snapshot forever.
Older load balancers that omit any required demand gauge continue to hold the
handoff floor, preserving mixed-version safety.

### Scale-down wave

After raw demand remains lower for `downscale_delay_seconds` of elapsed wall
clock time, ordinary demand-driven logical downscale may reduce the adopted
target by at most:

```text
allowance = max(
    1,
    ceil(current_committed_capacity
         * max_scale_down_rate_percentage / 100))

next_target = max(raw_target,
                  current_committed_capacity - allowance)
```

The elapsed window starts when a fresh recompute first observes raw demand
below the adopted target. For compatibility with the established one-tick
default, that first observation receives at most one nominal decision interval
of evidence; all remaining progress uses monotonic elapsed time. A demand
rebound to or above the adopted target, a stale demand report, a version
update, or an accepted lower wave resets the window. Controller reconstruction
also starts with no prior timer evidence. Another lower wave requires a new
complete elapsed window.

Decision-loop counts are diagnostic only. They cannot implement this delay:
large-fleet probing can make a nominal 20-second decision tick take much
longer, which turned a configured 300-second delay into roughly 9.5 minutes in
production. Busy-replica and stale-signal safety continue to clip actual
victims. Controller reconstruction may restore the committed fleet as the
target baseline only for the first fresh recompute. An adopted lower target
must not rebound to committed capacity on later ticks while asynchronous
retirement is still catching up.

Failed/stopping cleanup, explicit shutdown, cost rebalance, and old-version
retirement remain exempt. These are lifecycle actions rather than ordinary
demand downscale. The dashboard and logs must expose raw demand, adopted
target, and current committed capacity so an exempt rollout drain is not
mistaken for a demand wave.

### Configuration

Add the following fields to logical concurrency replica policies:

| Field | Type and range | Compatibility behavior |
|---|---|---|
| `target_utilization_percentage` | integer, 1 through 100 | 100 when absent |
| `expected_request_duration_seconds` | positive number | absent preserves one-for-one rejected pressure |
| `max_scale_up_rate_percentage` | integer, 1 through 100 | absent disables the scale-up wave limiter |
| `scale_up_rate_min_replicas` | positive integer | required with scale-up percentage |
| `scale_up_rate_period_seconds` | positive integer | required with scale-up percentage |
| `max_scale_down_rate_percentage` | integer, 1 through 100 | 50 for new specs, 100 for old persisted objects missing the field |

Scale-up fields must be configured together. Reject booleans and partial
groups. Duration, utilization, and scale-up fields require
`target_concurrency_per_replica` and logical replica semantics.

The production Boltz policy is:

```yaml
target_concurrency_per_replica: 1
target_utilization_percentage: 90
expected_request_duration_seconds: 30
max_scale_up_rate_percentage: 20
scale_up_rate_min_replicas: 10
scale_up_rate_period_seconds: 60
upscale_delay_seconds: 20
downscale_delay_seconds: 300
max_scale_down_rate_percentage: 50
```

## Implementation milestones

### 1. Policy plumbing

Extend `SkyServiceSpec`, YAML schemas, parsing, serialization, copying,
pickling compatibility, policy descriptions, and autoscaler diagnostics. Keep
the new scale-up group opt-in so unrelated services do not change merely from
a server upgrade.

### 2. Logical target calculation

Split active work from rejected pressure, calculate concurrent-equivalent
rejections and stale arrivals, apply target utilization, and preserve unknown
occupancy as fail-closed active work.

### 3. Bounded actuation

Apply the scale-up wave to fresh and stale target increases. Persist its
timestamp through in-process service updates. Reset a newly committed
version's adopted target to its minimum so an inherited old-version target
cannot bypass the first wave. Gate the 50 percent downscale wave on elapsed
wall-clock hysteresis and reset its timer after each permitted reduction,
without changing legacy count-based hysteresis for other autoscaler modes.
During a logical rolling update, preserve coverage using observed
latest-version logical capacity plus a conservative one-slot floor per READY
old backend, and retire eligible old physical backends in batches of at most 20
per tick.
Start the HA demand-handoff expiry from the first complete authoritative demand
gauge report even when some backend occupancy samples remain unknown.

### 4. Production consumer and rollout

Update the Boltz Platform production spec and inherited validator. Deploy an
exact SkyPilot control-plane version first, then update the service directly.
Keep the public load-balancer endpoint unchanged.

Monitor through 08:00 EST. Success means the applied target follows actual
in-flight work plus duration-normalized rejection pressure, target increases
obey the configured minimum-or-percentage per-minute ceiling, ordinary
downscale obeys the five-minute 50-percent waves, and physical capacity
converges without request or error regression.

## Alternatives considered

### Continue using saturation 10

Rejected. Dividing real running occupancy by 10 can target fewer GPUs than are
actively executing. It also hides a duration assumption inside a concurrency
field.

### Remove rejected pressure

Rejected. Rejected jobs represent demand SkyPilot could serve on later retries
and must continue to incentivize capacity growth.

### Average all outstanding work over a moving window

Rejected for this iteration. Running and queued jobs are current state, not an
arrival rate, and smoothing them can hide a growing real queue. The retained
rejection population already provides a bounded arrival window and is the
signal that needs duration conversion.

### Use raw request rate only

Rejected. Actual occupancy is more reliable for accepted asynchronous jobs,
and pure request rate loses the safety floor for long-running and
unknown-occupancy work.

### Wait for complete latest-version capacity before any retirement

Rejected. It prevents progress when the latest version cannot acquire the full
target and keeps obvious old-version excess online. Coverage can instead be
proven incrementally from latest logical capacity plus a conservative
one-slot-per-old-backend floor.

### Apply the five-minute demand rate limit to rollout retirement

Rejected. A rollout is not an ordinary demand decrease, and waiting five
minutes between old-version batches would prolong duplicate fleets. The
per-tick 20-backend batch is an actuation bound, while busy-backend and fresh
demand coverage checks provide the safety gate.

### Keep using nominal decision-loop counts for downscale

Rejected. The controller's decision interval is a scheduling target, not a
duration guarantee. Fleet probing and reconciliation can stretch a tick, so a
15-tick threshold does not reliably represent 300 seconds. Changing only the
logical concurrency downscale gate avoids broad hysteresis changes for legacy
request-rate and physical-backend policies.

## Rollout and rollback

The user approved direct production deployment without a test-fleet gate.
Focused unit and schema tests, formatting, exact-head review, and control-plane
health are still mandatory before the service update.

Deploy SkyPilot first. Verify the live API commit and version, then update the
Platform service policy. Verify the service's committed and applied version,
endpoint identity, raw and adopted targets, and logical and physical capacity.

Rollback the service policy before rolling back SkyPilot. Remove the new
scale-up fields, restore utilization 100 and concurrency 10, set scale-down
rate to 100, then restore the previous control-plane image if required.

## Test plan

- Validate defaults, explicit values, invalid ranges, partial scale-up groups,
  YAML round trips, copying, and old-pickle fallbacks.
- Verify running and queued work remain one-for-one while rejected work is
  converted from both 30/360 retained and 30/60 recent rates, takes the larger
  value, and is divided by 90 percent effective capacity.
- Verify stale arrivals use duration divided by the 60-second arrival window.
- Verify zero-to-high demand adopts 10 slots, waits 60 seconds, then adopts the
  larger of 10 or 20 percent based on committed logical capacity.
- Verify provisioning capacity is committed and prevents duplicate waves.
- Verify continuously advancing load-balancer snapshots cannot starve a
  still-current logical scale-up or retirement target, while a newer target or
  version still fences an unaccepted batch before persistence. An accepted
  same-version retirement must recompute coverage and idle evidence from the
  newest target and snapshot, keeping covered victims off route and
  reactivating only the capacity required for a shortfall.
- Verify a backend that recovered between the decision snapshot and actuation
  is removed from the unknown-capacity replacement set.
- Verify final manager admission and teardown fences count one slot per ready
  old backend, but never count a backend that is already off route.
- Verify in-process updates preserve the rate timestamp and a rebuild never
  jumps directly to the raw target.
- Verify a demand rebound does not cause scale-down and stale reports cannot
  shrink capacity.
- Verify 50 percent downscale requires 300 elapsed seconds per wave even when
  decision ticks are irregular or slow, resets on a demand rebound or stale
  report, does not rebound while retirement lags, and works for mixed 1, 4,
  and 8-slot backends.
- Verify logical rolling retirement starts before latest capacity reaches the
  complete target, never reduces conservative coverage below raw or adopted
  demand, retires non-READY old backends first, protects busy or unknown old
  backends, and emits no more than 20 victims per tick.
- Verify a newer pending version freezes old-version retirement admission,
  committed teardowns remain irreversible, and an applied in-process update
  re-fences uncommitted victims from fresh new-version evidence without
  advertising the whole draining fleet again.
- Verify an authoritative HA demand report starts the handoff expiry when all
  demand gauges are present, while incomplete or legacy reports retain the
  previous floor. Missing occupancy samples must still protect those replicas
  through the unknown-occupancy set without preserving stale queue or rejection
  gauges.
- Verify failed cleanup and cost-rebalance safety remain unchanged or exempt as
  specified.
- Run focused Serve tests, format changed files, then rerun the focused suite.

## Manual production test

1. Record the live API commit/version, service endpoint, service version,
   target components, and logical/physical capacity.
2. Deploy the exact SkyPilot build and verify health and commit identity.
3. Apply the production service YAML and verify committed version equals
   applied version without endpoint change.
4. Sample every minute. A rising raw demand target may be large, but the
   adopted target must increase by no more than the configured wave per 60
   seconds.
5. During falling demand, verify no ordinary second downscale wave occurs
   before a fresh 300-second window.
6. Track request rate, in-flight, queue, rejected, failed, and endpoint health
   while logical and physical capacity converge.
