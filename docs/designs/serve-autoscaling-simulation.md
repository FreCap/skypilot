# Run data-driven SkyServe autoscaling simulations

## Purpose

This runbook defines the minimum reproducible process for evaluating a
SkyServe queue or autoscaling policy against historical traffic. It is intended
for decisions about:

- request queue size and priority-specific patience;
- target concurrency, utilization headroom, and expected request duration;
- minimum replicas or a fixed queue floor;
- normal and adaptive scale-up waves;
- startup delay, placement failure, and cluster scarcity;
- downscale delay, pressure-veto budgets, and scale-down limits.

The goal is not to find one policy that perfectly explains one day. The goal
is to compare the exact current policy with a small set of candidates under
the same observed workload, quantify the tradeoffs, and reject candidates
that depend on optimistic assumptions.

There is not yet a general-purpose replay CLI checked into this repository.
The simulations that motivated the 2026-07-20 policy were investigation-local
models. Until a canonical harness is added, the exact simulator or notebook
used for a decision must be versioned with the decision artifacts and satisfy
the input, calibration, scenario, and output contract below. Results from an
unversioned temporary script are exploratory and cannot by themselves approve
a production change.

## Decision rule

Do not change a production scaling policy from a dashboard screenshot alone.
For every proposed policy:

1. Export an immutable input bundle before retained history expires.
2. Replay the exact deployed policy as the baseline.
3. Calibrate the baseline against observed production behavior.
4. Change one policy family at a time and replay every candidate on identical
   traces and random seeds.
5. Run sensitivity and scarcity cases for every unobserved input.
6. Choose from a Pareto frontier of service quality and capacity cost, not from
   one composite score whose weights hide the tradeoff.
7. Validate the selected policy in a bounded canary before broad rollout.

A simulation result is advisory if baseline calibration fails. It is not a
rollout gate until the cause of the mismatch is understood.

The simulator must derive every displayed scenario field from the scenario
object actually executed. A second declarative metadata block is not an
acceptable source because it can silently describe a different policy. The
baseline launcher must use the same exact-card choice rules as the deployed
controller and must be able to launch every card represented in the modeled
supply. A hard-coded cheapest-card-only launcher cannot validate whether the
baseline would have requested A100.

Visualizations may render only accelerators whose supply, startup, service
rate, and dispatch behavior are modeled. Do not add empty card panels that
imply H100, H200, or B200 behavior when the replay contains no such model. A
per-card desired total is labeled `Serving target`, not `Traffic target`, when
it can include retention of already-running exact-card work. If the replay
models them, show warm retention and incremental cold-launch authority as
separate, non-stacked series.

For the 2026-07-22 production comparison, the canonical policy family keeps
the deployed 300-second downscale delay and least-load dispatch. Compare the
current 90 percent target utilization with a 95 percent target-utilization
candidate. A 20-second downscale delay or a lookahead/ordered dispatch policy
is a separate experiment and is not part of this rollout candidate.

## Fidelity levels

Use the highest fidelity whose required data is available:

| Level | Inputs | Appropriate decisions |
|---|---|---|
| Aggregate replay | One-minute SkyServe history plus empirical duration and startup distributions | Coarse scale-up/down and minimum-capacity comparisons |
| Request replay | Request arrival time, duration, priority, retry lineage, plus SkyServe history | Queue patience, strict priority, rejection, and latency comparisons |
| Supply-aware replay | Request replay plus time-varying cluster/provider capacity, placement outcomes, and startup timing | Production rollout decisions for mixed research-cluster and cloud capacity |

Do not use aggregate replay to claim exact per-priority rejection or wait-time
results. Do not use successful startup samples alone to claim behavior under
scarcity.

## Reproducible input bundle

Create one directory per evaluation and keep it outside the repository because
request and infrastructure traces may be sensitive:

```text
serve-sim-<service>-<UTC timestamp>/
  provenance.json
  skyserve.json
  duration.json
  requests.csv
  supply.csv
  scenarios.yaml
  simulator/
  results.json
  comparison.csv
  timeline.html
```

`provenance.json` records:

- SkyPilot version and commit;
- service name, hash, elected version, and trace bounds;
- timezone, simulator revision, command line, and random seeds;
- source system and query identifier for each input;
- whether each field is observed, derived, or assumed;
- the capacity unit and incremental-cost class used by every supply series;
- SHA-256 checksums of every input file.

Never store request payloads, authorization headers, secrets, raw stable job
IDs, or a complete service YAML in the bundle. Hash stable identifiers with a
run-specific salt when retry deduplication is required.

### Export the SkyServe history

SkyServe retains aggregate status and request history for at most 72 hours.
Export it early. Recent placement events are retained for at most 24 hours,
but they are not a replacement for the historical supply trace described
below. The following command runs read-only inside the API server and writes
only the policy fields and replica metadata needed by the simulation:

```bash
SERVICE=boltz-l4-fleet
HOURS=72
RUN_DIR="serve-sim-${SERVICE}-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$RUN_DIR"

kubectl -n skypilot exec -i deployment/skypilot-api-server \
  -c skypilot-api -- env SERVICE="$SERVICE" HOURS="$HOURS" python - \
  >"$RUN_DIR/skyserve.json" <<'PY'
import json
import os
import sys
import time

import sky
import yaml
from sky.server import common as server_common
from sky.server.requests import payloads

service = os.environ['SERVICE']
hours = int(os.environ['HOURS'])


def submit(path, body):
    response = server_common.make_authenticated_request(
        'POST',
        path,
        json=json.loads(body.model_dump_json()),
        timeout=(5, None),
    )
    return sky.get(server_common.get_request_id(response))


records = submit(
    '/serve/status',
    payloads.ServeStatusBody(
        service_names=[service],
        summary_only=False,
        history_hours=hours,
    ),
)
if len(records) != 1:
    raise RuntimeError(f'Expected one service, got {len(records)}.')
record = records[0]
service_doc = yaml.safe_load(record.get('service_yaml') or '{}')
service_config = service_doc.get('service') or {}

replica_fields = (
    'replica_id', 'status', 'version', 'planned_capacity', 'is_spot',
    'created_at', 'launched_at', 'ready_at', 'time_to_ready_seconds',
    'cloud', 'region', 'zone', 'instance_type', 'resources_str_full',
)
replicas = [{
    key: (str(replica[key]) if key == 'status' else replica[key])
    for key in replica_fields
    if replica.get(key) is not None
} for replica in record.get('replica_info', [])]

bundle = {
    'captured_at': time.time(),
    'sky_version': sky.__version__,
    'sky_commit': sky.__commit__,
    'service_name': service,
    'service_hash': record.get('hash'),
    'service_status': str(record.get('status')),
    'elected_version': record.get('elected_version'),
    'active_versions': record.get('active_versions'),
    'replica_policy': service_config.get('replica_policy'),
    'request_queue': (service_config.get('load_balancer') or {}).get(
        'request_queue'),
    'history': record.get('replica_status_history'),
    'replicas': replicas,
}
json.dump(bundle, sys.stdout, sort_keys=True, default=str)
sys.stdout.write('\n')
PY
```

Inspect the resulting keys and checksums, not the raw request data, before
sharing the bundle:

```bash
jq '{sky_version, sky_commit, service_name, service_hash, elected_version,
     history_samples: (.history.autoscaler_samples | length),
     request_samples: (.history.request_samples | length),
     startup_samples: ([.replicas[].time_to_ready_seconds] | length)}' \
  "$RUN_DIR/skyserve.json"
shasum -a 256 "$RUN_DIR"/*
```

If an API server uses more than one deployment name or container name, resolve
the exact ready pod first and substitute it in the command. Never export from
a different service incarnation merely because it has the same display name.

### Export the request trace

The SkyServe minute history does not contain duration, priority, queue wait, or
retry lineage. Export a privacy-safe trace from the caller or request metrics
system with this logical schema:

The proposed SkyPilot-owned
[per-card request duration history](serve-per-card-duration-history.md) was
dropped because the model runtime already emits a more accurate completion-path
distribution. Use that aggregate for service time, and retain the caller-side
trace for priority, retry lineage, provider spill, and logical outcomes. Never
join aggregate metric counts to request rows as if they were one-to-one.

```text
arrival_ts,job_id_hash,retry_group_hash,priority,duration_seconds,
terminal_outcome,provider,accelerator,workload_class
```

Requirements:

- `arrival_ts` is the first attempt presented to SkyServe, with sub-minute
  precision when available.
- `job_id_hash` is stable within the run and contains no reversible customer
  identifier.
- `retry_group_hash` joins attempts of the same logical job across providers.
- `priority` is the actual value sent on the wire, not the product label that
  was later mapped to it.
- `duration_seconds` measures model execution, not total workflow duration.
- `accelerator` identifies the card on which the duration was observed.
  Duration from another accelerator may be used only as a labeled sensitivity
  input, never as observed duration for the target service.
- `workload_class` is a privacy-safe request-shape bucket when execution time
  varies materially by model input or method.
- `terminal_outcome` distinguishes served, queue timeout, queue full, provider
  failure, caller timeout, and trace truncation.
- `provider` lets the analysis distinguish a local rejection from a failed
  logical job. A SkyServe rejection that succeeds elsewhere is still offered
  pressure, but it is not a second unique arrival.

Use the empirical joint distribution when duration correlates with accelerator,
priority, time of day, or request type. Report p50, p90, p95, p99, and the mean
for every accelerator and workload class with enough samples. A single average
duration erases the long tail that determines queue delay and capacity
recovery.

If only minute counts exist, generate at least three within-minute traces:

- uniform arrivals across each minute;
- arrivals concentrated in the first 15 seconds;
- one instantaneous burst per minute.

Report all three. Do not call the uniform result observed request timing.

### Export model-handler duration by card

For Boltz model deployments, use the existing Datadog distributions:

```text
boltz.prediction.duration  # milliseconds around handle_request
boltz.prediction.count     # completed handler invocations
```

Filter both queries to the exact `env`, `service`, `boltz.model.name`,
`boltz.workload`, `boltz.machine.type`, `boltz.gpu.type`, and
`boltz.compute.provider` population. Use identical UTC bounds and completion
guard bands for both metrics. Export the raw responses and a normalized
`duration.json` with counts, sums, means, percentiles, query strings, bounds,
release identity, unknown-dimension fractions, and checksums.

Compute each whole-window mean as:

```text
sum(boltz.prediction.duration) / sum(boltz.prediction.count) / 1000
```

Do not average per-replica averages or percentile time-series points. When
Datadog percentile indexing is enabled, query whole-window p50, p90, p95, and
p99 by the same stable card and workload dimensions. Use the mean as the
offered-arrival service-time estimate, and use percentiles only as sensitivity
and tail-risk scenarios.

Use an earlier interval for calibration and a later chronological interval for
evaluation. Freeze every duration coefficient and policy candidate before the
holdout replay. Record runtime releases that intersect either interval because
the query intentionally excludes high-cardinality version and replica tags.

Cross-check `boltz.prediction.count` against SkyServe and caller counts. It
includes handler exceptions, excludes SkyServe rejections, and must never be
added to the SkyServe arrival trace as independent demand.

### Export capacity and placement supply

The current SkyServe placement response includes live capacity hints and recent
placement attempts. It does not reconstruct historical free GPUs for the
entire trace. For a supply-aware replay, create `supply.csv` with:

```text
observed_ts,cluster,provider,region,accelerator,free_gpus,
capacity_cost_class,placement_outcome,startup_seconds,failure_code
```

Include every eligible source, especially prepaid or reserved research-cluster
capacity. Model cloud fallback separately from research-cluster capacity so a
failed or delayed placement is not treated as an immediately available GPU.
Classify each supply row as `incremental_cost` or `no_incremental_cost` from the
decision's perspective. This classification affects cost and utilization
scoring, not dispatch or availability. The existing replica-history
[capacity modes](serve-replica-history-capacity-modes.md) provide durable
logical capacity and free-reserved attribution after their migration, but they
do not reconstruct historical cluster supply before a replica was launched.
Use the Placement tab or `/serve/placement` API as supporting evidence for
recent attempt outcomes, but obtain the time-varying free-GPU series from the
cluster or provider telemetry that observed it at the time.

Startup samples must include unsuccessful attempts and cancellations. Current
ready replicas are success-biased. When historical supply is missing, run a
grid such as 25, 50, 75, and 100 percent launch success plus observed startup
p50, p90, and p99. Label that grid as sensitivity analysis.

## Run sequence

The simulator used for a production decision must be checked into the
evaluation bundle under `simulator/` or referenced by an immutable repository
commit. It must expose commands that perform these four phases separately:

```text
validate inputs -> replay baseline -> calibrate -> replay scenario matrix
```

Record the exact commands in `provenance.json`. A typical invocation is:

```bash
python "$SIMULATOR" validate \
  --skyserve "$RUN_DIR/skyserve.json" \
  --requests "$RUN_DIR/requests.csv" \
  --supply "$RUN_DIR/supply.csv" \
  --scenarios "$RUN_DIR/scenarios.yaml"

python "$SIMULATOR" replay \
  --scenario baseline \
  --output "$RUN_DIR/baseline.json"

python "$SIMULATOR" calibrate \
  --observed "$RUN_DIR/skyserve.json" \
  --simulated "$RUN_DIR/baseline.json" \
  --output "$RUN_DIR/calibration.json"

python "$SIMULATOR" replay-matrix \
  --scenarios "$RUN_DIR/scenarios.yaml" \
  --results "$RUN_DIR/results.json" \
  --comparison "$RUN_DIR/comparison.csv" \
  --timeline "$RUN_DIR/timeline.html"
```

These command names define the minimum workflow contract, not a claim that an
arbitrary investigation script already implements this interface. If the
simulator uses different flags, record its real commands and produce the same
four phase outputs. Do not proceed to the matrix when input validation fails.
Do not use candidate results as rollout evidence when baseline calibration is
outside the predeclared tolerance.

## Simulation model contract

Prefer a discrete-event model with one-second request timing and the real
controller decision cadence. The model must represent these states separately:

```text
request: offered -> queued -> dispatched -> completed
                         \-> queue-timeout or queue-full rejection

capacity: absent -> placement-attempt -> provisioning -> ready-idle
                                                  \-> ready-busy
                                                  \-> failed
          ready/provisioning -> draining -> stopped or cancelled
```

### Request behavior

- Dispatch strict higher priority first and FIFO within one priority.
- Resolve patience with the highest matching `min_priority`; keep the scalar
  timeout as fallback.
- Fix a request's deadline at admission. A policy update does not rewrite an
  existing waiter's deadline.
- Model the caller's HTTP timeout independently from queue patience. The
  effective wait cannot exceed the caller deadline, and a caller disconnect is
  not a successful queue retention. Include provider retry or spill only once
  in the logical job outcome.
- Calculate queue capacity exactly as production does:

  ```text
  queue_capacity = min(
      max_size,
      max(min_size, ready_capacity_units * size_per_replica))
  ```

- In logical async-occupancy mode, use planned logical capacity for queue size
  and observed free slots for dispatch. Do not assume every ready physical
  backend has one slot.
- Retain rejected stable jobs as deduplicated scale-up pressure for the real
  retention windows. Do not count provider retries as new logical jobs.
- End-of-trace queued requests are censored, not silently successful. Report
  them separately and extend the trace with a drain period when possible.

### Autoscaler behavior

Use production autoscaler classes directly where practical. If the replay
reimplements a formula, pin the SkyPilot commit and add golden tests against
production outputs for:

- queue work weighted by expected duration over patience;
- retained and recent rejected-work normalization;
- 60-second and 300-second deduplicated arrival floors;
- target utilization and service bounds;
- upscale and downscale delays;
- normal and adaptive scale-up wave size and cadence;
- pressure activation, hold, and downscale veto consumption;
- maximum consecutive pressure vetoes and every episode-reset condition;
- independent total-fleet and provisioning-cohort scale-down allowances;
- controller restart and stale or incomplete demand behavior.

The simulated target is not simulated capacity. Launches become ready only
after sampled placement and startup events. Provisioning capacity counts as
committed for scale-up pacing but does not serve requests.

Keep these target series distinct throughout the replay and its graphs:

- `raw_demand_target`: the instantaneous result of outstanding work and
  arrival floors before downscale stabilization;
- `adopted_demand_target`: the demand target after delay, pressure vetoes,
  rate limits, and service bounds;
- `capacity_target`: the effective target after any free reserved-capacity
  fill overlay.

The 2026-07-20 incident is the calibration case for this distinction: raw
demand was 3 to 8 while the adopted target remained 144 under trickle traffic.
A simulator that calls both values `target` cannot diagnose or compare the
control-loop behavior.

### Cross-system and saturation comparisons

An algorithm from Self-hosted GPU or another provider is a candidate scenario,
not a separate benchmark. Feed it the same request, duration, initial-state,
and supply traces as the SkyServe baseline. Normalize the output to the same
logical capacity unit and apply the same caller timeout, startup delay, and
placement failures.

Define every borrowed signal mathematically. In particular, `saturation` may
mean busy/ready slots, no free dispatch slots, a growing queue, rejected work,
or a combination. Record the exact numerator, denominator, window, threshold,
and missing-data behavior. Port the concept only if SkyServe observes the
required signal with comparable semantics. A similarly named flag with a
different window or capacity unit is not an equivalent algorithm.

Report at least two utilization measures:

```text
service_utilization = busy_slot_seconds / ready_slot_seconds
incremental_cost_utilization =
    busy_incremental_cost_slot_seconds /
    ready_incremental_cost_slot_seconds
```

The first describes dispatch efficiency. The second is the selection metric
when the goal is at least 80 percent utilization while excluding research
capacity that has no incremental cost. Report no-incremental-cost capacity and
its utilization separately. If busy time cannot be attributed to the two cost
classes, report a bounded range and do not claim the 80 percent gate passed.

### Minimum replicas and minimum queue

Test these as separate policy dimensions:

- `min_replicas` reserves ready or starting capacity before demand. It reduces
  cold-start exposure but consumes GPU time during idle periods.
- request queue `min_size` permits a fixed number of waiters even when ready
  capacity is zero. It can reduce immediate spill, but it does not create
  compute and may only turn rejections into long waits.
- `size_per_replica` scales waiting-room size from ready logical capacity. For
  example, 100 ready capacity units and 10 entries per unit permit up to 1,000
  queued requests unless `max_size` is lower.

Always compare at least the current minimum, a small warm floor, and no warm
floor when scale-to-zero is supported. For queue changes, report both rejection
and wait SLOs so a larger queue cannot appear better merely by hiding work.

### Initial state and warm-up

Prefer the actual target, ready, provisioning, and draining state immediately
before the trace. If that snapshot is unavailable, run the trace twice and
score only the second cycle, or prepend a warm-up window at least as long as
the longest startup, rejection-retention, arrival, and downscale window.

Never initialize each candidate with a different favorable fleet. State the
initial condition in the report.

## Scenario matrix

Keep the exact deployed policy as `baseline`. A useful first matrix is:

| Scenario | Change from baseline | Question answered |
|---|---|---|
| Baseline | None | Does the model resemble production? |
| Normal-ramp alternatives | One wave percentage or minimum | What is the steady response/cost tradeoff? |
| Adaptive alternatives | Pressure observations, fast wave, hold | Does sustained pressure need a temporary faster ramp? |
| Pressure-veto budget | Unbounded, 0, 1, and 2 consecutive vetoes | Does trickle traffic protect a real rebound or starve downscale? |
| Warm floors | Several `min_replicas` values | What cold-start SLO is purchased by idle capacity? |
| Queue floors | Several `min_size` values | Does retaining work help, or only defer spill? |
| Priority patience | Candidate threshold sets | Which lane receives capacity and which spills? |
| Scarcity | Supply and launch-success grid | Does the policy remain safe when capacity is unavailable? |
| Restart/update | Controller restart and service update events | Does state loss create a launch or retirement wave? |
| Self-hosted GPU reference | Exact reference algorithm on normalized inputs | Does another control law improve the same frontier? |

Change one family at a time before testing interactions. A large grid of mixed
changes can find an accidental winner without explaining why it won.

Use multiple deterministic random seeds for sampled duration, startup, and
within-minute timing. Apply the same seed to every policy in one comparison.

## Outputs and graphs

Produce machine-readable `results.json` and `comparison.csv`. At minimum,
report:

### Service quality

- served, queue-full rejected, queue-timeout rejected, and caller-timeout
  counts by priority;
- rejection and spill rate by priority;
- queue wait p50, p95, p99, and maximum by priority;
- end-to-end latency SLO misses when completion timing is available;
- maximum queue depth and oldest waiter age;
- time from sustained pressure to enough ready capacity.

### Capacity and stability

- ready, busy, provisioning, draining, and failed slot-hours;
- average and peak ready, target, and committed capacity;
- raw demand, adopted demand, and effective capacity target separately;
- service and incremental-cost utilization, with reserved capacity separate;
- launch attempts, placement failures, pending cancellations, and replacements;
- number and magnitude of scale-up and scale-down decisions;
- target overshoot, target deficit, and capacity-minutes below target;
- controller restart and service update effects.

Generate one shared-axis timeline containing:

1. offered arrivals and actual or simulated rejections by priority;
2. in-flight, queue depth, and oldest wait;
3. raw target, adopted target, ready, provisioning, and draining capacity;
4. research-cluster free GPUs and cloud placement attempts;
5. controller restart, version update, adaptive activation, pressure veto, and
   scale-down markers.

Also produce a frontier plot with GPU-hours or ready slot-hours on the x-axis
and a service-quality metric on the y-axis. Keep separate frontiers for high
and low priority if their SLOs differ.

## Baseline calibration

Before comparing candidates, replay the production policy and compare it with
the observed history. At minimum, check:

- request totals per bucket are identical;
- target, ready, and provisioning peaks occur in the same periods;
- target and capacity error distributions are reported, not only visualized;
- observed rejection bursts appear within the same windows;
- simulated startup quantiles match observed successful startup quantiles;
- placement failure and capacity scarcity rates match the supply trace;
- controller or version transitions are represented;
- raw and adopted targets diverge in the same windows and for the same reason;
- the old unbounded-veto scenario reproduces a low-raw, high-adopted plateau,
  while the capped scenario releases it within its declared veto budget.

Investigate material divergence before tuning. Common causes include retry
double counting, a success-biased startup sample, missing research-cluster
capacity, assumed within-minute timing, incorrect logical slot width, and using
request duration as total workflow duration.

The simulator does not need to reproduce every individual request, but it must
reproduce the operating regime. Record the accepted error tolerance before
looking at candidate results.

## Holdout and robustness

Use the freshest 24 hours for rapid iteration only. Before choosing a policy:

- replay all available 72-hour SkyServe history;
- split calibration and holdout windows chronologically;
- include at least one burst, one idle-to-burst transition, one scarcity
  period, one trickle-traffic downscale period, and one ordinary low-traffic
  period;
- repeat on another day when older source telemetry is available;
- run optimistic, median, and conservative duration/startup cases;
- run supply failure and controller restart stress cases.

A candidate should not be selected if it wins only on the calibration window,
only with uniform arrivals, or only with 100 percent launch success.

### Production-observable policy check

Before recommending an online policy, rerun it after replacing every
request-level value unavailable to the controller with its deployable proxy.
In particular, SkyServe does not know the sampled duration of a queued request
or the remaining duration of an in-flight request. A paid-capacity gate may
use queue depth, in-flight count, deduplicated offered arrivals, retained
rejections, configured expected duration, ready and provisioning capacity,
zero-cost attribution, and the configured startup estimate. It must not use
future arrivals, exact queued runtimes, or exact residual runtimes.

Treat queueing, recent rejection, or ready-capacity saturation as a fail-open
signal for any speculative paid-launch guard. Recheck the candidate with 0,
50, 80, and 100 percent of observed reserved capacity and with both nominal
and degraded paid-launch success. A policy that saves cost only with the full
research reserve is a service-specific configuration candidate, not a safe
general autoscaler default.

The 2026-07-22 `boltz-l4-fleet` replay is an example of why this check matters.
Its 624-minute overlap contained 47,658 requests, a 166-slot observed research
reserve, a 513-second median L4 request-to-ready startup, and a 199-second
median billed warmup. The replay compared least-load dispatch, paid-first
dispatch, aggregate paid-work gates, upstream queue signals, minimum paid
capacity, and utilization targets. After removing unavailable request-runtime
oracles:

- a separate aggregate paid-work gate was effectively neutral at the existing
  90 percent target once saturation fail-open behavior made it safe;
- moving the service target from 90 to 95 percent produced nearly all of the
  useful savings, about 15 percent fewer paid slot-hours with the full reserve,
  zero rejections, and a 200-second maximum low-priority wait;
- at 80 percent of the observed reserve, the 95 percent target kept maximum
  wait below 274 seconds and spilled at most 110 of 47,658 requests in the
  conservative 66 percent launch-success sensitivity;
- an abrupt 50 percent reserve loss at the observed 640-request/minute peak,
  combined with 558-second p95 or 618-second p99 L4 startup and 66 percent
  launch success, kept low-priority wait below 353 seconds and injected
  high-priority wait below 5 seconds. The queue drained, and no scale-down or
  provisioning cancellation occurred while it was non-empty;
- paid-first dispatch increased the utilization percentage but also increased
  paid slot-hours because the slower paid L4 handled work that faster reserved
  cards could have completed; and
- no least-load policy reached 80 percent billed-startup-inclusive paid
  utilization. The service should optimize paid slot-hours and queue/spill
  bounds instead of gaming that ratio.

The selected service-specific rollout therefore changes only the utilization
target. It keeps the existing 20-second upscale observation, 20 percent
one-minute normal ramp, adaptive pressure ramp, 300-second downscale delay,
50 percent downscale cap, retained rejection pressure, reserved fill, and
least-load dispatch. The Datadog duration distribution remains offline
calibration input, never an online control dependency.

Complete reserve loss is an accepted disaster mode rather than a 95-percent
target regression. With paid startup near or above the 600-second low-priority
patience, both the 90- and 95-percent policies spill thousands of requests,
then drain the queue. Do not add an unbounded rule that forbids provisioning
cancellation whenever any queue or retained rejection exists: after requests
spill, that can ratchet obsolete paid launches indefinitely. A future churn
test should instead detect cancellation followed by a replacement launch
within one startup window. Fix confirmed non-disaster churn with a bounded
cancellation cooldown or remaining-useful-work guard.

## Selection and rollout report

The decision note must include:

```text
Baseline policy and exact service version:
Candidate policies:
Trace bounds and service hash:
Observed inputs:
Assumed inputs and sensitivity ranges:
Baseline calibration error:
Service-quality table by priority:
Capacity/stability table:
Frontier and timeline links:
Selected candidate and why:
Rejected alternatives and why:
Canary scope, success thresholds, and rollback:
```

Prefer the simplest policy on the acceptable frontier. A candidate must have
explicit canary thresholds for rejection, wait, utilization, target deficit,
launch rate, and pending cancellation. Compare the canary with a matched
historical window, and revert when a threshold is exceeded for the agreed
duration.

## Known limitations and follow-ups

- Minute buckets cannot recover the actual ordering of a sub-minute burst.
- Current SkyServe history does not persist priority, completion latency, or
  retry lineage.
- Current placement history is shorter than request/autoscaler history and is
  not a complete historical free-capacity series.
- Current replica startup observations are biased toward attempts that became
  visible and ready.
- A replay cannot predict a new provider outage or scheduler behavior not
  represented in the trace.

These limitations must appear in the report. They are reasons to collect a
better trace or widen sensitivity analysis, not reasons to silently pick a
favorable constant.
