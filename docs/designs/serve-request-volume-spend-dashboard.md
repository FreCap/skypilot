# Daily service request volume on the spend dashboard

Status: accepted for implementation; non-rejected amendment re-reviewed with
`PURSUE`

## Problems

SkyServe already records idempotent request counts in PostgreSQL at one-minute
resolution, but operational history is retained for only 72 hours. The spend
dashboard supports UTC ranges up to 90 days, so it cannot compare request
volume per service over the same period as estimated compute spend.

The original durable counter represents one admitted or capacity-rejected
inbound load-balancer attempt. That contract is useful for demand visibility but
is not a valid denominator for estimated compute cost per request: a request
rejected by the load balancer did not receive the service whose unit cost the
ratio is meant to estimate. The raw table stores a separate rejection counter,
but some rejection-only paths were never part of the attempt counter, so blindly
subtracting every rejection can also undercount.

The spend dashboard therefore needs a second, exact contract. A non-rejected
request is one classified inbound attempt that did not terminate through the
load balancer's terminal rejection path. Internal retries across replicas reuse
the request and do not add counts. A new client request, including a client
retry, is a new count because SkyPilot has no cross-request idempotency key.

## Goals

Provide durable daily attempt and non-rejected request counts per service for the
spend dashboard's existing UTC date range. Preserve the attempt fields for API
compatibility, preserve historical service data after deletion or same-name
recreation, and avoid making dashboard reads scan raw minute history.

The page should show stacked daily non-rejected request volume for the busiest
services and a totals table that makes their selected-range volume, share, and
estimated compute cost per non-rejected request easy to compare. Historical
attempt-only rows must not be silently presented as non-rejected requests.

## Background

`RequestTimestamp` already accumulates exact per-minute request counters in the
external load balancer. The controller persists cumulative reporter-minute rows
in `serve_request_activity_history`, keyed by service incarnation,
load-balancer reporter session, and minute. Upserts take the greatest observed
counter, so delivery retries and out-of-order reports cannot duplicate or
decrement a row.

The same payload independently reports terminal rejection counters. Normal
queue-full, queue-timeout, no-ready-replica, and all-at-capacity exits increment
both the attempt and rejection counters, while pre-admission limits can increment
only the rejection counter. The two existing counters therefore cannot be
subtracted safely. A capable load balancer must additionally classify each
eligible request exactly once when its load-balancer outcome is known.

Classification uses a separate cumulative minute history and acknowledgement.
At the terminal classification minute, a non-rejected request increments
`classified_request_count`; a terminal load-balancer rejection increments both
`classified_request_count` and `counted_rejected_count` in the same operation and
minute. Each event therefore preserves
`counted_rejected_count <= classified_request_count`, and their difference is
monotonic. This deliberately buckets the new metric by classification time,
rather than subtracting a completion-minute rejection from an arrival-minute
attempt.

Classification uses dedicated request-local `eligible` and `classified` fences,
not the legacy demand marker. Queue-full and queue-timeout paths mark the request
eligible immediately before recording their terminal rejection. Requests that
pass admission become eligible only after the final drain and role fence.
No-ready-replica and all-at-capacity exits are classified as rejected. A
response returned by a replica, including a replica 4xx or 5xx, is classified as
non-rejected. Eligible 499 client disconnects, cancellations, ambiguous upstream
502s, and internal 500 exits are also non-rejected because the load balancer did
not reject them for capacity; the metric is not a success counter. A final
classification guard covers every eligible return, exception, and cancellation,
while the terminal rejection path classifies rejected outcomes first. The
`classified` fence makes the final guard a no-op for those rejected exits.

Pre-admission body limits and the drain or role-fence exits that precede
eligibility are outside the classified subset. The retry loop re-checks the
drain and role fences on every attempt; a request that trips one of those
per-attempt fences is already eligible and already counted as an attempt, so it
is classified as a terminal load-balancer rejection rather than left to the
final non-rejected guard. Like the pre-admission drain exits, it is not
autoscaling pressure and does not enter the reject-window gauge or the history
rejection counter. Internal replica retries stay inside one eligible request and
never create another classification.

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
| `classified_request_count` | Sum of terminally classified eligible requests emitted by capable load balancers, or null when the day is incomplete |
| `counted_rejected_count` | Terminal rejections that are also present in `classified_request_count`, or null when unavailable |
| `classified_first_bucket_start` | Earliest classified minute represented, or null |
| `classified_last_bucket_start` | Latest classified minute represented, or null |
| `classification_incomplete` | Durable one-way latch set when any positive attempt source lacked classification support |
| `observed_at` | Latest rollup observation |

The primary key is `(day_start, service_name, service_hash)`. Counts use a
wide integer. The table has indexes for date-range reads and service/date
grouping. The global coverage lookup first selects the earliest `day_start`
through the existing day index, then computes `MIN(first_bucket_start)` only
for rows on that day. Since every represented minute belongs to its row's UTC
day, this preserves the exact global minimum while preventing dashboard
refreshes from scanning the indefinitely retained historical table. The table
is not created for SQLite.

On the existing Serve history cadence, run an isolated best-effort rollup by
UTC day, service name, and service incarnation. The first run and hourly runs
aggregate all raw rows still present, with the hourly pass occurring before
raw pruning. Intervening runs aggregate the current UTC day; runs during the
first UTC hour also recompute the previous day so late final-minute reports are
included. This bounds routine work without losing initial backfill or day
finalization.

Migration 032 adds nullable classified and counted-rejection columns to the raw
minute table and the durable daily table. The columns must either both be null,
or both be nonnegative with counted rejections no greater than classified
requests. Null distinguishes legacy reporters from capable reporters; zero
remains a valid capable count. The daily non-rejected count is
`classified_request_count - counted_rejected_count`. Existing attempt columns
and rows are unchanged.

Every new load balancer sends classification protocol version 1 even when its
classification snapshot is empty. An independently acknowledged classification
transaction validates that complete envelope, promotes the accompanying
request-history arrival buckets to a zero pair as support evidence, and upserts
the cumulative terminal counters, which can also create a classification-only
minute row. The generic request-history writer never marks classification
support. The controller commits valid version 1 support before it allows the
generic writer to expose positive arrival rows. A classification database
failure skips the generic write and retains both snapshots for retry, so a
concurrent rollup cannot permanently latch a transient unsupported row. A
malformed version 1 envelope is acknowledged and any independently valid
arrival rows remain null and visibly incomplete rather than falsely complete.
Conversely, a valid version 1 classification remains durable when its optional
arrival snapshot is malformed: the classification transaction omits support
promotion, while the generic writer independently acknowledges and drops the
invalid arrival snapshot.

A distinct `request_classification_history_accepted` response acknowledges only
the supported classification snapshot. For a valid version 1 envelope, the
legacy `request_history_accepted` response is also gated on classification
persistence so a database failure retains both snapshots for an atomic retry.
An absent or unsupported future classification version does not block legacy
request acknowledgement, and an unsupported version is never acknowledged as
classified. A new load balancer talking to an old controller retains and retries
classification history instead of clearing it on the legacy acknowledgement. An
old load balancer talking to a new controller continues to write null pairs.

The rollup uses its own transaction and error boundary: a daily rollup failure
must not abort replica status snapshots, raw-history pruning, or the history
loop. Raw reporter-minute component upserts use `GREATEST`. A recomputed day is
classification-complete only when every positive-attempt raw source row carries
the paired capability fields. This lets a late legacy reporter demote a day that
initially appeared complete.

Daily component counts remain monotonic independently of attempt rows:
classified and counted-rejection totals use null-aware `GREATEST`, and their
first and last boundaries use null-aware `LEAST` and `GREATEST`. A separate
`classification_incomplete` value is initialized true for every pre-migration
daily row with positive attempts and becomes true whenever a recomputation sees
a positive raw attempt row with a null classification pair. Conflict updates OR
this value with the stored latch; it can never become false. This protects
classification-only rows, whose attempt count is zero, when raw retention later
removes them. The attempt count remains the greater value; first and last
attempt boundaries remain the least and greatest values; `observed_at` advances.
This has five properties:

1. A late or retried report can increase a day.
2. The rolling deletion of raw minute rows can never reduce a finalized day.
3. The first rollout automatically backfills every minute still available,
   currently up to 72 hours, but only new classified rows can populate the new
   fields.
4. Legacy attempt-only rows never masquerade as non-rejected request history.
5. Raw pruning can neither reduce a finalized component nor promote an
   incomplete mixed-version day to complete.

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
  non_rejected:
    available: bool
    definition: non_rejected_inbound_requests
    coverage_start_utc: epoch seconds or null
    coverage: complete, partial, or unavailable
    complete_by_day: bool[]
    total_request_count: integer
    services:
      - service_name
        request_count
        coverage: complete, partial, or unavailable
        complete_by_day: bool[]
    series:
      - service_name or is_other
        request_count_by_day: (integer or null)[]
```

The existing top-level request fields retain their inbound-attempt semantics for
older consumers. The nested `non_rejected` projection groups only complete
classified pairs, subtracts only rejections in that paired subset, groups
incarnations by `service_name`, orders the table by exact covered count, limits
it to 50 services, and builds series for the top eight plus `Other`. Its
`coverage_start_utc` comes from the earliest classified bucket rather than UTC
midnight. Global `complete_by_day` is false before that point, for its partial
first UTC day, and whenever any observed service-day has its incomplete latch
set. Each service has its own aligned completeness array, so one legacy service
does not invalidate an otherwise exact service's cost ratio. Counts for an
incomplete service-day are null, never zero. Missing activity after the first
complete coverage day is an exact zero. The projection is unavailable until at
least one complete service-day or an accrued complete current service-day
exists.

`total_request_count` sums exact service-day cells only and `coverage` is
`partial` whenever another observed cell is incomplete. The UI labels that total
as known rather than complete. `Other` is null for a day when any nondisplayed
service on that day is incomplete, and the global daily stack is explicitly
partial when global `complete_by_day` is false. Shares are unavailable for a
partial global total rather than being computed from an understated
denominator.

Cost enrichment uses each service's complete days only. Existing top-level
attempt fields remain unchanged for cached consumers. Under API version 68,
their legacy cost-per-request enrichment is explicitly unavailable: ratio values
and daily ratio arrays are null and `cost_coverage` is `unavailable`, rather than
reinterpreting those fields or falling back to attempts. Corrected cost fields
exist only under the nested non-rejected projection. The new dashboard renders
that projection directly.

On a non-PostgreSQL server, both projections are unavailable. The endpoint
remains admin-only. API version 68 adds the nested projection. The change is
additive, so old clients continue to receive the existing attempt fields. A new
dashboard connected to an older server treats missing non-rejected history as
unavailable and never falls back to the inflated attempt denominator.

### Dashboard comparison

Reuse the spend page's active UTC date range. The section after the spend
breakdown shows:

- a stacked bar chart titled `Daily non-rejected requests by service`;
- a top-services table with `Service`, `Requests`,
  `Est. compute cost / request`, and `Share` columns;
- `N/A` rather than zero for a service whose exact request coverage is
  unavailable, plus each service's own cost-ratio denominator start date;
- selected-range total request volume;
- empty, unavailable, partial-coverage, and current-day-partial states.

Copy explains the counting contract: one classified inbound request counts only
when it is not rejected by the load balancer, internal replica retries do not
add requests, and client retries are separate requests. The coverage notice
states when exact classification begins.

### Estimated compute cost per request

Daily spend rows attribute replica compute to
`workload_type == "service"` and the canonical service name in `workload_id`.
Join them to non-rejected request history by UTC classification day and
`service_name == workload_id`. This keeps the cost numerator and denominator on
the same UTC-day grid without introducing another rollup. It is an explicitly
estimated operational ratio: a request that crosses midnight belongs to the day
its load-balancer outcome becomes classifiable.

Extend each `service_requests.non_rejected.services` row with:

```text
estimated_cost: float
estimated_cost_per_request: float or null
ratio_request_count: integer
ratio_coverage_start_utc: epoch seconds or null
priced_machine_seconds: integer
excluded_machine_seconds: integer
cost_coverage: complete, partial, or unavailable
```

Extend each named non-rejected chart series with aligned arrays:

```text
estimated_cost_by_day: float[]
estimated_cost_per_request_by_day: (float or null)[]
```

The selected-range value is a weighted ratio:

```text
sum(estimated service compute cost on aligned covered UTC days)
/
sum(non-rejected inbound requests on those UTC days)
```

Never average daily ratios. For each service, every day where that service's
`complete_by_day` entry is false is excluded from both the selected-range
numerator and denominator. The current UTC day remains included when that
service's classification is complete so far, with the existing partial-day
notice. `ratio_request_count` makes the aligned denominator explicit when it
differs from the table's request total, and `ratio_coverage_start_utc` identifies
the first included UTC day.

Return a null ratio when the aligned request count is zero, no covered service
machine time exists, or any aligned service machine time has genuinely unknown
pricing. For this service-only ratio, Kubernetes machine time is covered at
zero cost because the Serve placement catalog defines Kubernetes locations as
reserved or already-paid zero-cost capacity. This exception does not change
the global spend estimate, where Kubernetes remains excluded. A service backed
only by covered Kubernetes capacity therefore reports `$0.0000` per request
instead of N/A. The ratio also remains unavailable until the spend rollup's
historical backfill is complete; otherwise active rows could understate the
selected range's cost.

The metric is explicitly labeled estimated compute cost per non-rejected
request. It is not
an invoice or total service cost: reserved Kubernetes service capacity is
valued at zero, while shared API-server and load-balancer infrastructure and
other reservation adjustments are excluded. The denominator excludes every
terminal load-balancer rejection that was part of the classified attempt stream.
It still includes client retries as distinct requests and replica-returned
responses regardless of status; it is not a successful or unique model-operation
count.

Keep the chart axis as request count. The tooltip adds the hovered service's
estimated compute cost and cost per request, but the ratio is not plotted or
stacked because ratios are non-additive and use a different scale. Format every
available ratio with four decimal places, including `$0.0000` for covered
zero-cost capacity, with positive values below `$0.0001` shown as `<$0.0001`;
the existing two-decimal spend formatter remains unchanged.

## Data flow

```text
Inbound request attempt
    |
    v
Load balancer terminal classification + exact counted-rejection subset
    | independently acknowledged cumulative minute report
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
Spend page daily non-rejected service chart and totals table
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

Subtracting the existing `rejected_count` from `request_count` was rejected
because those counters are independent: pre-admission body-budget rejections can
exist without a corresponding request-count unit, and arrival and terminal
rejection can fall in different minute buckets. Recording both new components
together at terminal classification makes their difference exact and monotonic
without changing demand or autoscaling telemetry.

Counting queue admission alone was rejected because a request can pass the
admission fence and still terminate through no-ready-replica or all-at-capacity
load-balancer rejection. That would retain precisely the rejected capacity
traffic this metric must exclude.

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

- `sky/serve/request_aggregator.py`
- `sky/serve/load_balancer.py`
- `sky/serve/serve_history.py`
- `sky/schemas/db/serve_state/032_serve_request_rejection_classification.py`
- `sky/estimated_spend.py`
- `sky/server/constants.py`
- `sky/dashboard/src/components/estimated-spend.jsx`
- focused Serve history, estimated-spend, connector, and component tests

The non-rejected extension requires Serve migration 032 and API version 68. Its
response fields are additive within `service_requests`, while the existing
attempt fields remain unchanged.

## Rollout

The migration adds nullable component columns and initializes the daily
incomplete latch for pre-feature positive-attempt rows. Deploy the controller and
migration before rolling load balancers. Independent acknowledgement prevents a
new load balancer from clearing classified history merely because an old
controller accepted legacy request history. New load balancers then begin
classified reporting. An
isolated step on the existing history cadence backfills only classified raw rows
and keeps daily totals current. Attempt-only history remains available to legacy
API consumers but is never used by the new dashboard denominator. Dashboard
reads are best effort: a failed rollup leaves the last durable totals visible and
does not affect status snapshots, history pruning, API readiness, or request
serving.

After deployment, verify API version 68 and Serve revision 032, confirm every
load-balancer deployment uses the new image digest, compare a short range against
raw `classified_request_count - counted_rejected_count`, confirm the earliest
coverage timestamp, and prove a terminal capacity rejection advances attempts
and counted rejections but not the nested non-rejected count. Do not claim
non-rejected history before classified telemetry begins or for a mixed-version
service-day.

Rollback removes readers and stops classified rollup writes. Old controllers can
still acknowledge and persist legacy attempt rows, so any rollback day with
positive traffic becomes durably incomplete on the next forward deployment. A
new load balancer retains unacknowledged classification for only the bounded
one-hour history window; a longer controller rollback or outage creates a
coverage gap and must never be described as lossless. Migration 032's downgrade
is non-destructive so old binaries ignore the added columns and later forward
deployment preserves data that was already acknowledged.

Draining stops the regular controller sync loop, so classification itself
coalesces a history-only flush whenever it occurs after draining begins. A
classification that arrives during an in-flight flush triggers another pass;
one that arrives after the task clears schedules a new pass. This prevents an
already-admitted request from becoming stranded after the initial drain flush.

## Test plan

Backend tests cover PostgreSQL-only migration, multi-reporter summation,
same-name multi-incarnation grouping, idempotent reruns, late counter increases,
raw-retention non-decrement, initial 72-hour backfill, UTC midnight boundaries,
zero-filled days, top-eight plus `Other`, top-50 ordering, partial coverage, and
non-PostgreSQL unavailability. Query-shape coverage also asserts that the
global coverage lookup is restricted to the earliest represented day. A
fault-injection test proves a request-rollup failure cannot prevent status
snapshot persistence or raw-history pruning.

Classification tests prove that queue-full, queue-timeout, no-ready-replica, and
all-at-capacity exits increment both classified requests and the counted
rejection subset together exactly once; replica responses increment only the
classified component; internal replica retries, pre-admission rejection-only
paths, drain fences, failed delivery, and out-of-order snapshots preserve the
contract. Protocol tests prove old-controller acknowledgements cannot clear the
new history, future versions are never falsely acknowledged, late drain outcomes
flush, classification commits before version 1 arrivals become visible,
classification failure retains the coupled v1 arrival snapshot, and old load
balancers remain nullable. Migration and PostgreSQL
rollup tests prove paired constraints, monotonic component upserts, exact
subtraction, the one-way incomplete latch, classification-only pruning safety,
per-service coverage gaps, and non-destructive downgrade/re-upgrade.

API tests preserve admin-only authorization, verify the additive response and
date-range behavior, and prove old response consumers remain valid.

Dashboard tests cover chart and table rendering, shared date controls, totals
and shares, empty and unavailable states, partial-day copy, a missing
`service_requests` field, refresh behavior, and stale-request fencing.

Cost-per-request tests cover canonical service attribution, daily alignment,
weighted selected-range ratios, zero requests, zero-cost Kubernetes reservation
coverage, genuinely unknown pricing, an incomplete first history day,
current-day partial values, fixed four-decimal formatting, unavailable legacy
history, rejected-request exclusion, and daily tooltip content.

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
5. Submit one capacity-rejected request and one non-rejected request, then
   confirm the attempt total advances by two while the displayed denominator
   advances by one.
