# SkyServe response-time history

## Status

Accepted for implementation on 2026-07-22 after explicit product approval for
provider-independent, customer-facing full HTTP response history.

## Problem

SkyServe stores one-minute offered-request, rejection, autoscaler, capacity,
and replica history in PostgreSQL. It does not retain the distribution of full
HTTP response times. The existing `boltz.prediction.duration` runtime metric is
more accurate for handler execution, but deliberately excludes SkyServe queue
wait, retry backoff, proxy overhead, and response streaming.

This is a separate requirement from the dropped per-card service-time design
in `serve-per-card-duration-history.md`. The implementation must not infer
request duration from async occupancy transitions and must not add Datadog to
the online control loop.

## Behavior contract

Response time starts when an HTTP inference request enters the load balancer
and ends after the terminal response body is sent successfully. It includes:

- load-balancer authentication and admission;
- request queue time;
- replica selection and retry backoff;
- upstream processing and proxying;
- complete response-body streaming.

The collector includes terminal responses from the catch-all inference route,
including load-balancer-generated 4xx and 5xx responses. It excludes readiness,
liveness, capacity, and other `/_lb/*` operational traffic. It does not record
an incomplete client disconnect as a completed response. A terminal response
is classified by its final HTTP status class, 1xx through 5xx.

Samples are assigned to their completion minute. The histogram uses inclusive
upper-bound buckets of 100 ms, 250 ms, 500 ms, 1 s, 2.5 s, 5 s,
10 s, 30 s, 60 s, 120 s, 300 s, 600 s, 1,200 s, 1,800 s, and 3,600 s, plus one
overflow bucket. Bucket boundaries are versioned constants shared by
collection, validation, serialization, and the dashboard.

This history is observability only. Collection, reporting, persistence, or
rendering failures must never fail authentication, routing, retries,
autoscaling, draining, or controller liveness.

## Data flow

```text
inference request
      |
      v
LB ASGI completion observer
      |  one local integer increment
      v
reporter-minute cumulative arrays
      |  additive snapshot every normal 20 s sync
      v
Serve controller validation and durable acknowledgement
      |  executor, observability-only failure boundary
      v
PostgreSQL reporter-minute row
      |  service-incarnation query and reporter aggregation
      v
/serve/status history payload
      |
      v
service dashboard percentile trend and selected-range histogram
```

## Load-balancer aggregation

The completion observer wraps the ASGI `send` callable. It captures the final
status from `http.response.start` and records only after a terminal
`http.response.body` with `more_body` false has been sent. This makes streaming
completion exact without buffering the response. Unexpected exceptions before
response start are classified as 5xx. Exceptions or disconnects after response
start but before a terminal body are incomplete and are not recorded.

The load balancer keeps a bounded dictionary keyed by completion-minute epoch.
Each value contains five fixed-length integer arrays, one per status class.
The current process session remains the reporter identity. Pruning uses the
existing one-hour request-history window and runs on minute boundaries rather
than every request.

Response history uses its own top-level sync payload and acknowledgement:

```text
response_time_history = {
  bucket_seconds: 60,
  histogram_version: 1,
  buckets: [{
    bucket_start: <aligned epoch second>,
    status_class_counts: {
      "1xx": [16 nonnegative integers],
      "2xx": [16 nonnegative integers],
      "3xx": [16 nonnegative integers],
      "4xx": [16 nonnegative integers],
      "5xx": [16 nonnegative integers]
    }
  }]
}
```

Zero-only status arrays may be omitted on the wire. Missing classes normalize
to zero arrays. A snapshot is acknowledged only when the new controller
returns `response_time_history_accepted: true`. An old controller ignores the
new payload and does not return that field, so a new load balancer retains its
bounded cumulative counters during a controller-first rolling upgrade. The
draining history-only endpoint carries and acknowledges the same payload.

## PostgreSQL schema and idempotency

Migration 024 creates `serve_response_time_history`. The central Serve DB path
is PostgreSQL-only for this table.

```text
service_name         text        primary key
service_hash         text        primary key
reporter_session_id  text        primary key
bucket_start         timestamptz primary key
observed_at          timestamptz not null
response_count       integer     not null check >= 0
status_1xx_counts    integer[]   not null
status_2xx_counts    integer[]   not null
status_3xx_counts    integer[]   not null
status_4xx_counts    integer[]   not null
status_5xx_counts    integer[]   not null
```

Every array has the fixed version-1 length and nonnegative values. The
validated writer derives `response_count` as the sum of every array element.
The process histogram is cumulative within a minute, so an upsert replaces the
stored arrays only when the incoming response count is at least the stored
count. Duplicate and out-of-order reports cannot double-count or decrement
history.

Lookups use `(service_name, service_hash, bucket_start desc)`. The hourly
retention sweep deletes rows older than the existing 72-hour Serve history
retention. Service-hash predicates prevent same-name recreation leakage.

## API and dashboard

`replica_status_history` remains the compatibility envelope. It adds:

```text
response_time_histogram_version: 1
response_time_bucket_upper_bounds_seconds: [15 finite values]
response_time_samples: [{
  timestamp: <minute epoch second>,
  status_class_counts: {
    "1xx": [16 aggregated counts],
    "2xx": [16 aggregated counts],
    "3xx": [16 aggregated counts],
    "4xx": [16 aggregated counts],
    "5xx": [16 aggregated counts]
  }
}]
```

The API aggregates all reporter sessions for each minute in Python after a
bounded incarnation-scoped read. This keeps SQL and migration logic simple and
bounds the maximum read to 72 hours of compact active-reporter rows.

The dashboard adds a `Response time` card to the synchronized Serve history
section. It defaults to all status classes and offers All, 2xx, 3xx, 4xx, and
5xx selection. The upper chart shows approximate p50, p95, and p99 by minute.
The lower chart shows the aggregate fixed-bucket distribution for the selected
time range. Values are labeled approximate because a fixed histogram cannot
recover exact quantiles. Empty minutes remain gaps rather than fabricated zero
latency.

## Cost model

Per completed response, the load balancer performs two monotonic-clock reads,
one wall-clock read for completion-minute assignment, a fixed-bound bucket
lookup, and one integer increment. No request payload, identifier, route,
model input, or exact duration is retained.

At steady state, the active load balancer sends one changed minute in each
20-second controller sync. This is about three idempotent upserts per active
service-minute and one retained row per active reporter-minute. Write traffic
and retained storage therefore scale with active services and the 72-hour
window, not request rate. Five arrays contain only 80 four-byte counters plus
row and index overhead.

## Alternatives considered

Per-request persistence was rejected because writes, storage, and privacy risk
scale with traffic. Datadog-only history was rejected because it is not the
provider-independent customer-facing Serve history requested here. The model
runtime metric remains useful but cannot measure the full HTTP boundary.
Async occupancy transitions remain unsuitable because a busy episode is not a
request completion. A single service-minute row without reporter identity was
rejected because HA retries cannot make additive multi-reporter writes exactly
idempotent without a more complex contributor protocol.

## Rollout and rollback

Deploy API server and migration 024 before or with new controllers. The schema
addition and API fields are backward compatible. New controllers accept old
load balancers with no response payload. New load balancers retain unacknowledged
payloads when paired with old controllers. PostgreSQL failure only suppresses
new samples and produces a bounded warning.

Rollback leaves the table and additive payload fields unused. Old components
ignore them. No runtime state or autoscaling decision depends on this history.

Production rollout is complete only after verifying the release and Helm
revision, migration revision, healthy API pods and service controllers, healthy
active and standby load balancers, a controlled 2xx and 4xx or 5xx request in
PostgreSQL and `/serve/status`, and the dashboard card rendering those buckets.

## Test plan

- Unit-test histogram boundary classification, completion-minute assignment,
  pruning, concurrent completion during acknowledgement, and stale snapshots.
- Exercise ASGI normal, streaming, local-error, unexpected-error, and client
  disconnect paths without buffering bodies or double-recording.
- Test normal sync, old-controller missing acknowledgement, persistence error,
  and bounded drain flush.
- Execute migration 024 against real PostgreSQL and verify array constraints,
  idempotent upserts, multi-reporter aggregation, incarnation fences, retention,
  and API serialization.
- Test dashboard normalization, quantile approximation, status selection,
  selected-range aggregation, synchronized range selection, and empty history.
- Run focused Serve tests, real PostgreSQL tests, dashboard Jest tests and
  production build, formatter and type checks, and the complete visible PR CI
  rollup.
