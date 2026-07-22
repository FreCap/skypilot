# SkyServe per-card request duration history

## Status

Dropped. Model-runtime telemetry supersedes this design.

## Decision

Do not add a SkyPilot request-duration table, load-balancer histogram, HA
reporting protocol, controller acknowledgement, status response, or online
Datadog feedback loop for autoscaling calibration.

Boltz model runtimes already emit `boltz.prediction.duration` exactly once
around `handle_request`, on success and exception, and emit the matching
sampling-independent `boltz.prediction.count`. The metrics carry stable model,
workload, machine type, GPU type, compute provider, environment, and service
dimensions. They measure the execution boundary needed by the offline replay
more accurately than SkyServe can infer from HTTP lifetime or polled async
occupancy.

SkyPilot history remains authoritative for offered requests, queue depth,
rejections, raw and adopted targets, ready and provisioning capacity, reserved
capacity, and startup behavior. The model-runtime metric is authoritative only
for handler service time. It does not measure SkyServe queue wait, provider
spill latency, or complete workflow latency.

## Offline calibration contract

- Compute mean service time as the whole-window duration sum divided by the
  whole-window completion count. Do not average per-replica averages.
- Use p50, p90, p95, and p99 as sensitivity and tail-risk inputs, not as a
  direct replacement for the mean expected duration.
- Filter to the exact environment, service, model, workload, machine type, GPU
  type, and provider. Record unknown-card and unknown-provider fractions.
- Fit on an earlier training interval, freeze the candidate parameters, and
  evaluate them unchanged on a later chronological holdout.
- Keep the deployed runtime release boundary outside the metric grouping and
  record it in the simulation provenance.
- Preserve `raw_target = max(live_outstanding_target,
  offered_arrival_target)`. Never add completion counts or duration work as a
  second copy of live outstanding work.
- Keep Datadog out of the online autoscaling loop. A coefficient change is a
  reviewed, versioned configuration rollout.

## Why the original proposal was rejected

Async occupancy transitions are busy episodes, not exact request completions.
HA merging would require contributor-scoped cumulative generations, bounded
compatibility buffers, and carefully censored role handoffs. Minute aggregates
with exact sums can also reveal a single request when the count is one. Those
costs and risks duplicate an existing, more accurate completion-path signal.

## Reopen conditions

Reconsider SkyPilot-owned persistence only for a separate product requirement,
such as provider-independent customer-facing runtime history when the model
telemetry backend is unavailable or has insufficient retention. That work must
have a new reviewed design and must not reuse async occupancy as per-request
duration.
