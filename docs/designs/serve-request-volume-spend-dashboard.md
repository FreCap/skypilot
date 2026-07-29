# Daily service request volume on the spend dashboard

Status: accepted for implementation

## Problems

SkyServe already records idempotent request counts in PostgreSQL at one-minute
resolution, but operational history is retained for only 72 hours. The spend
dashboard supports UTC ranges up to 90 days, so it cannot compare request
volume per service over the same period as estimated compute spend.

The dashboard also needs a precise counting contract. The available durable
counter represents one admitted or capacity-rejected inbound load-balancer
request. Internal retries across replicas reuse the request and do not add
counts. A new client request, including a client retry, is a new count because
SkyPilot has no cross-request idempotency key.

## Goals

Provide durable daily request counts per service for the spend dashboard's
existing UTC date range. Keep the request path unchanged, preserve historical
service data after deletion or same-name recreation, and avoid making dashboard
reads scan raw minute history.

The page should show stacked daily request volume for the busiest services and
a totals table that makes their selected-range volume, share, and estimated
compute cost per inbound request easy to compare.

## Background

`RequestTimestamp` already accumulates exact per-minute request counters in the
external load balancer. The controller persists cumulative reporter-minute rows
in `serve_request_activity_history`, keyed by service incarnation,
load-balancer reporter session, and minute. Upserts take the greatest observed
counter, so delivery retries and out-of-order reports cannot duplicate or
decrement a row.

`serve_history.record_status_snapshot()` runs on the existing history cadence
and prunes raw Serve history after 72 hours. `/estimated_spend` is admin-only,
uses UTC day boundaries, and returns at most 90 days. Its dashboard already
renders top-eight stacked series with an `Other` remainder and top-50 tables.
Serve state and global spend state share the API server's central PostgreSQL
connection.

The prior request-accounting extraction is already merged through PR #1025 at
commit `72e2a38ceb`. Older unpublished extraction branches are obsolete and
will not be resumed.

## Solution

### Durable daily request volume

Add a PostgreSQL-only `serve_request_activity_daily` table in the Serve schema.
One row represents one service incarnation and UTC day:

| Field | Meaning |
| --- | --- |
| `day_start` | UTC midnight for the aggregate day |
| `service_name` | User-visible service identity |
| `service_hash` | Incarnation identity, preserving same-name recreation |
| `first_bucket_start` | Earliest raw minute represented |
| `last_bucket_start` | Latest raw minute represented |
| `request_count` | Sum of current cumulative reporter-minute counts |
| `observed_at` | Latest rollup observation |

The primary key is `(day_start, service_name, service_hash)`. Counts use a
wide integer. The table has indexes for date-range reads and service/date
grouping. It is not created for SQLite.

On the existing Serve history cadence, run an isolated best-effort rollup by
UTC day, service name, and service incarnation. The first run and hourly runs
aggregate all raw rows still present, with the hourly pass occurring before
raw pruning. Intervening runs aggregate the current UTC day; runs during the
first UTC hour also recompute the previous day so late final-minute reports are
included. This bounds routine work without losing initial backfill or day
finalization.

The rollup uses its own transaction and error boundary: a daily rollup failure
must not abort replica status snapshots, raw-history pruning, or the history
loop. Upsert `request_count` with
`GREATEST(existing, recomputed)`, take the least first bucket and greatest last
bucket, and advance `observed_at`. This has three properties:

1. A late or retried report can increase a day.
2. The rolling deletion of raw minute rows can never reduce a finalized day.
3. The first rollout automatically backfills every minute still available,
   currently up to 72 hours, without a separate migration job.

Daily rows remain indefinitely. Their cardinality is one row per active service
incarnation per day, while the serving API remains bounded to the existing
90-day dashboard window.

### API projection

Extend the additive `/estimated_spend` response with:

```text
service_requests:
  available: bool
  definition: admitted_inbound_requests
  coverage_start_utc: epoch seconds or null
  total_request_count: integer
  services:
    - service_name
      request_count
  series:
    - service_name or is_other
      request_count_by_day: integer[]
```

The query groups incarnations by `service_name`, fills missing selected days
with zero, orders the table by request count, limits it to 50 services, and
builds series for the top eight plus `Other`. `coverage_start_utc` comes from
the earliest represented bucket rather than UTC midnight so the UI can identify
a partial first day.

On a non-PostgreSQL server, `service_requests.available` is false and the
remaining data is empty. The endpoint remains admin-only. The API version is
bumped because the response contract changes, but the new field is additive so
old clients continue to work. The dashboard tolerates an absent field for
version skew.

### Dashboard comparison

Reuse the spend page's active UTC date range. Add a section after the spend
breakdown with:

- a stacked bar chart titled `Daily requests by service`;
- a top-services table with `Service`, `Requests`,
  `Est. compute cost / request`, and `Share` columns;
- selected-range total request volume;
- empty, unavailable, partial-coverage, and current-day-partial states.

Copy explains the counting contract: one admitted or capacity-rejected inbound
request counts once, internal replica retries do not add requests, and client
retries are separate requests.

### Estimated compute cost per request

Daily spend rows attribute replica compute to
`workload_type == "service"` and the canonical service name in `workload_id`.
Join them to request history by UTC day and `service_name == workload_id`.
This keeps the cost numerator aligned with the request denominator without
changing request-path telemetry or introducing another rollup.

Extend each `service_requests.services` row with:

```text
estimated_cost: float
estimated_cost_per_request: float or null
ratio_request_count: integer
ratio_coverage_start_utc: epoch seconds or null
priced_machine_seconds: integer
excluded_machine_seconds: integer
cost_coverage: complete, partial, or unavailable
```

Extend each named chart series with aligned arrays:

```text
estimated_cost_by_day: float[]
estimated_cost_per_request_by_day: (float or null)[]
```

The selected-range value is a weighted ratio:

```text
sum(estimated service compute cost on aligned covered UTC days)
/
sum(inbound requests on those UTC days)
```

Never average daily ratios. The first historical UTC day is excluded from the
selected-range ratio when `coverage_start_utc` is after that day's midnight,
because its request numerator is incomplete while the spend row covers the
whole day. The current UTC day remains included as an accrued partial day, with
the existing partial-day notice. `ratio_request_count` makes the aligned
denominator explicit when it differs from the table's request total, and
`ratio_coverage_start_utc` identifies the first included UTC day.

Return a null ratio when the aligned request count is zero, no priced service
cost exists, or any aligned service machine time is excluded from pricing.
Kubernetes and unknown-price time must not silently appear as zero cost. An
available zero-cost ratio is not expected under the catalog pricing basis, so
absence of priced service time is treated as unavailable. The ratio also
remains unavailable until the spend rollup's historical backfill is complete;
otherwise active rows could understate the selected range's cost.

The metric is explicitly labeled estimated compute cost per request. It is not
an invoice or total service cost: Kubernetes, shared API-server and
load-balancer infrastructure, and reservation adjustments are excluded. The
denominator is inbound attempts, including capacity rejections and client
retries, rather than successful or unique model operations.

Keep the chart axis as request count. The tooltip adds the hovered service's
estimated compute cost and cost per request, but the ratio is not plotted or
stacked because ratios are non-additive and use a different scale. Format
positive sub-cent ratios to four decimal places, with values below `$0.0001`
shown as `<$0.0001`; the existing two-decimal spend formatter remains unchanged.

## Data flow

```text
Inbound request
    |
    v
Load balancer RequestTimestamp
    | idempotent cumulative minute report
    v
serve_request_activity_history (72-hour raw PostgreSQL history)
    | existing history cadence, monotonic UTC-day rollup
    v
serve_request_activity_daily (durable low-cardinality history)
    | bounded aggregate query
    v
GET /estimated_spend
    |
    v
Spend page daily service chart and totals table
```

## Alternatives considered

Keeping 90 days of raw minute rows was rejected because the dashboard needs
daily totals and should not pay minute-level storage and aggregation costs.

Computing daily totals directly from Datadog was rejected because the dashboard
already has a durable PostgreSQL source, and product reporting must not depend
on external log retention or ingestion.

Counting prediction completions was rejected because it answers throughput,
not requests received, and would omit admitted requests that later fail or
remain in progress.

Extending `estimated_spend_daily` with request counts was rejected because spend
rows are keyed by compute cluster, while a service request is independent of
which replica or cluster eventually serves it. Joining those ownership models
would duplicate or misattribute traffic.

Requests per dollar was rejected as the primary metric because it is the
reciprocal efficiency measure, not average cost per request. It can be added
later under its own label if operators need it.

Calculating the ratio from the browser's existing workload table was rejected
because those rows can cover days without request telemetry, are limited and
grouped for another UI, and cannot correctly surface unpriced service time.

## Implementation

Expected implementation areas:

- `sky/serve/serve_history.py`
- `sky/schemas/db/serve_state/031_serve_request_activity_daily.py`
- `sky/estimated_spend.py`
- `sky/server/constants.py`
- `sky/dashboard/src/components/estimated-spend.jsx`
- focused Serve history, estimated-spend, connector, and component tests

The cost-per-request extension does not need a new migration or request-path
change. Its response fields are additive within `service_requests`, so it does
not require another API-version bump.

## Rollout

The migration creates an empty PostgreSQL table. An isolated step on the
existing history cadence backfills available raw rows and keeps daily totals
current. Dashboard reads are best effort: a failed rollup leaves the last
durable totals visible and does not affect status snapshots, history pruning,
API readiness, or request serving.

After deployment, verify the new API version and migration, compare a short
range against raw minute history, confirm the earliest coverage timestamp, and
confirm new requests advance the current UTC day. Do not claim history earlier
than the retained raw source.

Rollback removes readers and stops rollup writes. The table can remain safely
for a later retry; the downgrade is non-destructive.

## Test plan

Backend tests cover PostgreSQL-only migration, multi-reporter summation,
same-name multi-incarnation grouping, idempotent reruns, late counter increases,
raw-retention non-decrement, initial 72-hour backfill, UTC midnight boundaries,
zero-filled days, top-eight plus `Other`, top-50 ordering, partial coverage, and
non-PostgreSQL unavailability. A fault-injection test proves a request-rollup
failure cannot prevent status snapshot persistence or raw-history pruning.

API tests preserve admin-only authorization, verify the additive response and
date-range behavior, and prove old response consumers remain valid.

Dashboard tests cover chart and table rendering, shared date controls, totals
and shares, empty and unavailable states, partial-day copy, a missing
`service_requests` field, refresh behavior, and stale-request fencing.

Cost-per-request tests cover canonical service attribution, daily alignment,
weighted selected-range ratios, zero requests, no attributed cost, excluded
machine time, an incomplete first history day, current-day partial values,
sub-cent formatting, unavailable table values, and daily tooltip content.

Run focused Python and Jest tests, `bash format.sh --files` on changed Python
files, dashboard lint and production build, static checks, and
`git diff --check`.

Manual verification:

1. Seed two services with minute rows spanning two UTC days and confirm the API
   and chart totals.
2. Send one request whose proxy path performs an internal replica retry and
   confirm the daily count advances by one.
3. Re-run the rollup and confirm counts do not change.
4. Advance beyond raw-history retention and confirm the durable daily total
   does not decrease.
