# SkyServe prediction-time history

## Status

Accepted for implementation on 2026-07-22 after explicit product approval to
replace full HTTP response-time history with customer-facing prediction time.

## Problem

The service dashboard currently labels and charts full SkyServe HTTP completion
time. That value includes authentication, load-balancer queueing, replica
selection, retry backoff, proxying, and response delivery. It can therefore
show five minutes for a prediction whose model execution was much shorter. The
product requirement is prediction time, not HTTP latency.

Boltz model runtimes emit the exact `boltz.prediction.duration` metric, but the
SkyPilot API server does not have a provider-independent telemetry query path.
The established asynchronous replica protocol does return
`processing_time_ms` in terminal `async_status` responses. Synchronous replicas
do not return an equivalent duration field, so SkyServe must observe their
replica execution boundary.

This design supersedes the previous full HTTP response-time design. It must not
infer prediction duration from async occupancy, treat a fast async submission
acknowledgement as completion, or put Datadog into the online control path.

## Behavior contract

Prediction history covers successful and failed predictions that reach a
replica. It excludes SkyServe authentication, admission and queue time, retry
backoff between attempts, load-balancer-generated errors, and downstream
delivery to the caller.

### Synchronous requests

For a request outside the known asynchronous protocol, timing starts
immediately before the accepted upstream HTTP dispatch and ends when that
replica returns terminal response headers. A retriable response or transport
error does not produce a sample for that attempt. The final non-retriable
response produces one sample:

- HTTP 2xx is `succeeded`;
- every other terminal upstream status is `failed`.

This boundary is replica service time rather than exact in-process handler
time. It includes the short load-balancer-to-replica network interval and, for
a streaming model, ends when response headers begin rather than when model
streaming finishes. The current Boltz synchronous contract returns prediction
results only after handler completion, so this is the closest available
customer-facing boundary without changing every model image.

### Asynchronous requests

Known asynchronous actions are `async_predict`, `async_status`,
`async_capacity`, and `async_cancel`.

- A nonempty SkyServe stable-job header also identifies a platform-held async
  submission, even when its body omits `action`. Its acceptance response never
  produces a prediction-time sample.
- `async_predict` is only an acceptance acknowledgement and never produces a
  prediction-time sample.
- Non-terminal `async_status` responses do not produce a sample.
- An HTTP 2xx terminal `async_status` response with a nonnegative finite
  `processing_time_ms` and nonempty `request_id` produces one sample. The
  model-reported duration is authoritative and the poll's own HTTP lifetime is
  ignored.
- `SUCCEEDED` is `succeeded`. `FAILED`, `EXPIRED`, `CANCELED`, and `CANCELLED`
  are `failed`. `NOT_FOUND` is not a completion.
- `async_capacity`, `async_cancel`, malformed JSON, missing durations, and
  unknown status values produce no sample.

The active load balancer keeps a bounded least-recently-used set of terminal
request IDs and records each ID once per process. Normal clients stop polling
after the first terminal result, so duplicate terminal polls are already rare.
A response-loss plus load-balancer failover can still duplicate one async
sample across reporter sessions. Avoiding that rare approximation would
require per-request durable identifiers and request-rate-scaled storage, which
is outside this compact history contract.

Samples are assigned to their completion-observation minute. Histograms use
inclusive upper-bound buckets of 100 ms, 250 ms, 500 ms, 1 s, 2.5 s, 5 s,
10 s, 30 s, 60 s, 120 s, 300 s, 600 s, 1,200 s, 1,800 s, and 3,600 s, plus one
overflow bucket. Bucket boundaries are versioned constants shared by
collection, validation, persistence, serialization, and the dashboard.

Prediction history is observability only. Parsing, collection, reporting,
persistence, or rendering failures must never fail authentication, routing,
retries, autoscaling, draining, or controller liveness.

## Data flow

```text
sync upstream dispatch -> terminal upstream headers -> measured duration
async terminal status  -> processing_time_ms       -> reported duration
                                      |
                                      v
                      LB reporter-minute histogram
                                      |
                       cumulative controller sync
                                      v
                     PostgreSQL prediction history
                                      |
                                      v
                      /serve/status history payload
                                      |
                                      v
                    service dashboard prediction card
```

## Load-balancer aggregation

Request JSON action parsing reuses the body already cached for proxying. It
does not retain model input after the request ends. Synchronous timing is scoped
to the final accepted upstream attempt. For async status, the existing raw-body
stream is forwarded unchanged while a bounded copy is retained for terminal
JSON parsing. A body over the fixed parsing cap is forwarded but not parsed or
recorded. A response with non-identity content encoding is also forwarded but
not parsed, because observability must not alter or decompress the proxy's raw
response stream. The established Boltz async-status response is uncompressed.

The load balancer keeps a bounded dictionary keyed by observation-minute epoch.
Each value contains two fixed-length integer arrays, `succeeded` and `failed`.
The current load-balancer Pod UID remains the reporter identity. Pruning uses
the existing one-hour request-history window and runs on minute boundaries.

Prediction history uses its own top-level sync payload and acknowledgement:

```text
prediction_time_history = {
  bucket_seconds: 60,
  histogram_version: 1,
  buckets: [{
    bucket_start: <aligned epoch second>,
    outcome_counts: {
      "succeeded": [16 nonnegative integers],
      "failed": [16 nonnegative integers]
    }
  }]
}
```

Zero-only outcome arrays may be omitted on the wire. Missing outcomes normalize
to zero arrays. A snapshot is acknowledged only when the controller returns
`prediction_time_history_accepted: true`. An old controller ignores the new
payload and omits that field, so a new load balancer retains its bounded
cumulative counters. The drain history endpoint carries and acknowledges the
same payload.

## PostgreSQL schema and idempotency

Migration 023 creates `serve_prediction_time_history`. The central Serve DB
path is PostgreSQL-only for this table.

```text
service_name         text        primary key
service_hash         text        primary key
reporter_session_id  text        primary key
bucket_start         timestamptz primary key
observed_at          timestamptz not null
prediction_count     integer     not null check >= 0
succeeded_counts     integer[]   not null
failed_counts        integer[]   not null
```

Every array has the fixed version-1 length and nonnegative values. The writer
derives `prediction_count` as the sum of all array elements. Reporter-minute
histograms are cumulative, so an upsert replaces stored arrays only when the
incoming count is at least the stored count. Duplicate and out-of-order reports
cannot double-count or decrement one reporter's history.

Lookups use `(service_name, service_hash, bucket_start desc)`. The hourly
retention sweep deletes rows older than the existing 72-hour Serve history
retention. Service-hash predicates prevent same-name recreation leakage.

Migration 022 and `serve_response_time_history` remain intact for rollback.
New load balancers stop emitting its payload and the status API stops reading
or exposing it, so no old HTTP latency appears in the replacement card.

## Controller compatibility

During a mixed rollout, new controllers accept both the legacy
`response_time_history` payload and the new `prediction_time_history` payload.
Legacy persistence remains an observability-only compatibility path, but it is
not exposed by the new API. New load balancers emit only prediction history.

This permits controller-first rollout and rollback:

- old load balancer to new controller keeps its legacy delivery contract;
- new load balancer to old controller retains unacknowledged prediction
  counters until it reaches a new controller;
- rollback leaves migration 023 and unused rows in place.

## API and dashboard

`replica_status_history` remains the compatibility envelope and adds:

```text
prediction_time_histogram_version: 1
prediction_time_bucket_upper_bounds_seconds: [15 finite values]
prediction_time_samples: [{
  timestamp: <minute epoch second>,
  outcome_counts: {
    "succeeded": [16 aggregated counts],
    "failed": [16 aggregated counts]
  }
}]
```

The API aggregates reporter sessions by minute after a bounded,
incarnation-scoped read. Response-time fields are removed from the new payload.

The dashboard replaces the `Response time` card with `Prediction time`. It
defaults to all predictions and offers All, Succeeded, and Failed filters. The
upper chart shows approximate p50, p95, and p99 by minute. The lower chart shows
the fixed-bucket distribution over the selected range. Empty minutes remain
gaps rather than fabricated zero duration. The explanatory copy distinguishes
synchronous replica service time from model-reported asynchronous processing
time.

## Cost model

A synchronous terminal upstream response performs two monotonic-clock reads, a
fixed-bound lookup, and one integer increment. Async terminal parsing copies at
most a small bounded status response and maintains a bounded request-ID set.
No model input, result, exact duration, route, or identifier is persisted.

At steady state, an active load balancer sends one changed minute on each
20-second controller sync. Persistence remains one compact row per active
reporter-minute and scales with active services and the 72-hour window, not
request rate.

## Alternatives considered

Keeping full HTTP latency was rejected because it answers a different product
question and makes queueing look like model execution. Querying Datadog from
the API was rejected because it introduces provider credentials and an
external telemetry dependency into customer-facing history. Per-request
PostgreSQL rows were rejected because storage and privacy exposure scale with
traffic. Async occupancy transitions were rejected because busy episodes are
not prediction completions. Changing every model image to return a new sync
duration header remains a possible future precision improvement, but it would
not make this SkyPilot rollout useful for current images.

## Rollout and rollback

Deploy the API server and migration 023 before or with new controllers. The
schema addition and API fields are backward compatible. New controllers accept
old load balancers, and new load balancers retain prediction counters until a
new controller acknowledges them. PostgreSQL failures suppress only new
samples and produce bounded warnings.

Production rollout is complete only after verifying the exact release and Helm
revision, migration 023, healthy API pods and service controllers, healthy
active and standby load balancers, one synchronous sample, one asynchronous
terminal sample, the prediction fields in PostgreSQL and `/serve/status`, and
the renamed dashboard card. If live traffic cannot safely provide both paths,
the missing path must be verified with a controlled test service before the
rollout is called complete.

Rollback leaves the new table and fields unused. No runtime state or autoscaler
decision depends on this history.

## Test plan

- Unit-test prediction histogram boundaries, outcome partitioning,
  completion-minute assignment, pruning, acknowledgement races, and stale
  snapshots.
- Test synchronous dispatch timing, retriable and transport failures, async
  acknowledgement exclusion, terminal-status parsing, malformed and oversized
  status bodies, outcome mapping, and request-ID deduplication.
- Test normal sync, old-controller missing acknowledgement, legacy payload
  compatibility, persistence errors, and bounded drain flush.
- Execute migration 023 against PostgreSQL and verify array constraints,
  idempotent upserts, multi-reporter aggregation, incarnation fences, retention,
  and API serialization.
- Test dashboard normalization, approximate quantiles, outcome selection,
  selected-range aggregation, synchronized range selection, empty history, and
  absence of response-time copy and fields.
- Run focused Serve and PostgreSQL tests, dashboard tests and production build,
  formatter and type checks, and the complete visible PR CI rollup.
