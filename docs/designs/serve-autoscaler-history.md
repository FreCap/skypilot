# Persist and graph SkyServe demand history

_Created: 2026-07-19_

## Problem

The service page persists request arrivals and physical-machine status in
one-minute buckets, but the autoscaler's decision and demand-pressure signals
remain point-in-time fields. Operators can see a request spike and a later
machine change without seeing the target, ready-capacity gap, queue pressure,
or controller lifecycle transition between them.

The existing `target_num_replicas` field is also insufficient for reserved
capacity. It intentionally remains demand-only while the fill overlay may
retain or launch substantially more zero-cost capacity. A single unlabeled
target line would therefore make a healthy reserved-capacity fleet appear
overprovisioned.

## Goals

- Persist the low-cost signals already present in the controller or load
  balancer in one-minute PostgreSQL buckets with the existing 72-hour
  retention.
- Record quiet minutes without introducing a per-service API-server poll.
- Distinguish demand target from effective capacity target.
- Preserve exact, retry-safe request and rejection counts across HA load
  balancers.
- Graph request arrivals, targets, ready/provisioning capacity, demand
  pressure, and controller/version transitions over one synchronized range.
- Derive target deficit and recovery-duration summaries without storing
  redundant aggregates.
- Keep observability failures isolated from routing and autoscaling.

## Non-goals

- Request latency percentiles, response-code histograms, and completion
  counts. Those require request-completion instrumentation and a separate HA
  aggregation contract.
- A Prometheus dependency or a new background poller.
- Historical reconstruction before this migration is deployed.

## Data contract

### Autoscaler samples

Add a `serve_autoscaler_history` table keyed by service incarnation and minute:

- `service_name`, `service_hash`, `bucket_start`: primary key and incarnation
  fence.
- `observed_at`: newest controller observation retained for the minute.
- `controller_session_id`: opaque random identifier created by each controller
  process. A change between samples is a restart marker.
- `version`: controller-applied service version. A change is an update marker.
- `replica_unit`: `physical_backend` or `logical_slot`.
- `demand_target`: `get_final_target_num_replicas()`, including configured
  overprovisioning.
- `capacity_target`: `max(demand_target, fill_target)`, the effective capacity
  level after reserved-capacity fill.
- `ready_capacity`, `provisioning_capacity`, and `total_capacity`: controller
  counts in the same unit as the targets.
- `peak_in_flight` and `peak_queue_depth`: highest authoritative gauge observed
  in the minute, nullable for autoscalers that do not expose the signal.

Targets and capacities are last-observation-wins within a minute. Pressure
gauges retain the maximum. Missing controller observations remain gaps and are
never forward-filled across an outage.

The authoritative load-balancer sync already runs during quiet traffic and
already has controller ownership, autoscaler state, and replica counts. It
writes the previously applied authoritative demand snapshot before the final
ownership fence. The next frequent sync therefore captures the newly accepted
report without adding an await after the runtime-mutation fence. The write runs
in the executor and failures are logged without failing sync or changing
runtime state.

### Exact rejection counts

Extend the existing cumulative request-history bucket with `rejected_count`.
The load balancer increments it once for each terminal 503 response. Snapshot
and acknowledgement track request and rejection counts independently, so a
request that is acknowledged while still running can report a later rejection
without losing it. A bucket is valid when either count is positive; missing
`rejected_count` from an old load balancer is stored as zero with an unavailable
capability marker.

PostgreSQL keeps one cumulative row per reporter process and minute. Upserts
retain the greatest acknowledged value for each counter, and the status query
sums reporters. A minute's rejection count is exposed only when every reporter
row supports it. This preserves retry idempotency, makes HA reporters additive
without double-counting retries of the same snapshot, and renders mixed-version
minutes as gaps instead of false zeroes. The top-level capability becomes true
when at least one reporter supports the counter, allowing upgraded and mixed
minutes to coexist in the same chart.

## API and dashboard

The existing nested `replica_status_history` response adds:

- `autoscaler_samples`, ordered by minute.
- Nullable `rejected_count` on each `request_sample`; null marks a minute that
  includes an old reporter without the exact counter.

Old clients ignore the new keys. New clients treat missing keys as unsupported
rather than inventing zeroes for an old server.

The Request history card keeps requests on the left axis and adds stepped lines
on a right capacity axis for demand target, effective capacity target, ready
capacity, provisioning capacity, and total capacity. The effective target line
is omitted when it never differs from demand target. Controller restart and
service update markers are point-only datasets at the contemporaneous capacity
level.

A synchronized Demand pressure card graphs peak in-flight, peak queued, and
rejected requests per minute. It is hidden when the server provides none of
those fields.

Derived summaries for the selected range include peak demand target, peak
capacity target, peak target deficit, minutes below effective target, longest
continuous below-target interval, peak in-flight, peak queue depth, and total
rejections. Derivations use only observed autoscaler buckets; missing minutes
do not count as healthy or unhealthy.

## Compatibility and failure behavior

- Central history remains PostgreSQL-only. SQLite returns history unavailable,
  matching the existing contract.
- The migration is idempotent for fresh databases because earlier migrations
  import the current table metadata. It inspects existing columns and
  constraints before adding `rejected_count`.
- Service hashes prevent same-name service reincarnations from sharing data.
- Controller history uses an opaque session identifier and exposes no pod IP,
  PID, endpoint, request identifier, or payload.
- An old load balancer continues to report request history with zero rejected
  counts. The database default also keeps old controllers able to write during
  a rolling upgrade. An old dashboard ignores the additional response fields.
- Cleanup runs with the existing hourly history retention pass.

## Simulation use and known gaps

The history is the SkyServe side of the reproducible input bundle defined by
the [autoscaling simulation runbook](serve-autoscaling-simulation.md). It
provides request and rejection buckets, actual targets, ready and provisioning
capacity, controller/version transitions, and aggregate pressure. Those fields
are sufficient to calibrate an aggregate replay and to detect a simulator that
does not resemble the real controller.

It is not a complete request-level trace. In particular, it does not persist
request duration, priority, queue wait, stable job identity, retry lineage, or
historical free GPU supply. Simulations that evaluate priority patience or
provider spill must join privacy-safe request facts from the caller and
capacity observations from the relevant infrastructure telemetry. If those
inputs are unavailable, the simulation must report sensitivity ranges and
must not present one assumed mix as observed production behavior.

The 72-hour retention also makes the export itself part of incident response.
Capture the immutable bundle before the relevant window expires. Store the
SkyPilot commit, service hash, service version, sanitized policy, trace bounds,
and source provenance with the export so a later replay cannot silently use a
different implementation or service incarnation.

## Alternatives considered

Adding the target to request rows was rejected because idle minutes have no
request row, even though downscaling and controller recovery still matter.
Polling `/autoscaler/info` once per service from the API server was rejected
because it adds N controller calls per minute and fails precisely during the
outages the history should expose. Adding target columns to physical replica
snapshots was rejected because targets use logical capacity units for some
services and are authored by the controller, not the API-server snapshotter.

Latency, response outcomes, and completion throughput were deferred because
they require completion-path hooks, streaming semantics, and histogram merge
rules. They are not comparable in cost to the signals already carried by the
sync channel.

## Changed-path-to-test matrix

| Changed production path or invariant | Test file | Verification |
|---|---|---|
| PostgreSQL autoscaler samples, last/peak upsert semantics, retention, and response serialization | `tests/unit_tests/test_reserved_fill_broker_pg.py` | Focused PostgreSQL test file |
| Request/rejection validation and non-PostgreSQL compatibility | `tests/unit_tests/test_serve_status_history.py` | Focused history tests |
| Independent cumulative rejection acknowledgement | `tests/unit_tests/test_serve_lb_auth.py` | Focused load-balancer history tests |
| Terminal 503 increments rejection history | Existing load-balancer queue/retry test module selected by implementation | Focused test |
| Controller records authoritative target/capacity/pressure without failing sync | `tests/unit_tests/test_serve_controller.py` | Focused controller tests |
| Response normalization and old-server behavior | `sky/dashboard/src/data/connectors/services.test.jsx` | Dashboard connector test |
| Request/capacity overlays, derived deficit stats, lifecycle markers | `sky/dashboard/src/components/serve-request-history.test.jsx` | Component test |
| Pressure series and summaries | New focused demand-pressure component test | Component test |
| Shared range remains synchronized across all history cards | `sky/dashboard/src/components/serve-history.test.jsx` | Section integration test |

## Rollout and verification

Run the focused Python and dashboard suites, format all changed files, run the
broader Serve history/controller tests, and build the dashboard. The migration
must apply before new controller code starts writing. After deployment, verify
that an idle service emits one autoscaler sample per minute, request and reject
counters remain monotonic through an LB restart, target gaps appear during a
controller outage, and the selected time range stays synchronized across all
charts. Require the full visible CI rollup on the exact PR head before merge.
