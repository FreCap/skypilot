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
- downscale delay, pressure vetoes, and scale-down limits.

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
  requests.csv
  supply.csv
  scenarios.yaml
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

```text
arrival_ts,job_id_hash,retry_group_hash,priority,duration_seconds,
terminal_outcome,provider
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
- `terminal_outcome` distinguishes served, queue timeout, queue full, provider
  failure, caller timeout, and trace truncation.
- `provider` lets the analysis distinguish a local rejection from a failed
  logical job. A SkyServe rejection that succeeds elsewhere is still offered
  pressure, but it is not a second unique arrival.

Use the empirical joint distribution when duration correlates with priority,
time of day, or request type. A single average duration erases the long tail
that determines queue delay and capacity recovery.

If only minute counts exist, generate at least three within-minute traces:

- uniform arrivals across each minute;
- arrivals concentrated in the first 15 seconds;
- one instantaneous burst per minute.

Report all three. Do not call the uniform result observed request timing.

### Export capacity and placement supply

The current SkyServe placement response includes live capacity hints and recent
placement attempts. It does not reconstruct historical free GPUs for the
entire trace. For a supply-aware replay, create `supply.csv` with:

```text
observed_ts,cluster,provider,region,accelerator,free_gpus,
placement_outcome,startup_seconds,failure_code
```

Include every eligible source, especially prepaid or reserved research-cluster
capacity. Model cloud fallback separately from research-cluster capacity so a
failed or delayed placement is not treated as an immediately available GPU.
Use the Placement tab or `/serve/placement` API as supporting evidence for
recent attempt outcomes, but obtain the time-varying free-GPU series from the
cluster or provider telemetry that observed it at the time.

Startup samples must include unsuccessful attempts and cancellations. Current
ready replicas are success-biased. When historical supply is missing, run a
grid such as 25, 50, 75, and 100 percent launch success plus observed startup
p50, p90, and p99. Label that grid as sensitivity analysis.

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
- independent total-fleet and provisioning-cohort scale-down allowances;
- controller restart and stale or incomplete demand behavior.

The simulated target is not simulated capacity. Launches become ready only
after sampled placement and startup events. Provisioning capacity counts as
committed for scale-up pacing but does not serve requests.

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
- busy/ready utilization, with the metric definition stated;
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
- controller or version transitions are represented.

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
  period, and one ordinary low-traffic period;
- repeat on another day when older source telemetry is available;
- run optimistic, median, and conservative duration/startup cases;
- run supply failure and controller restart stress cases.

A candidate should not be selected if it wins only on the calibration window,
only with uniform arrivals, or only with 100 percent launch success.

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
