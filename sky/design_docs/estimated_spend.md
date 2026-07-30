# Estimated spend by day

Status: implemented MVP

## Goal

Expose a low-cost, eventually consistent estimate of SkyPilot-created compute
spend, split by UTC day and attributable to a managed job when the machine is
dedicated to that job.

This is an operational estimate, not an invoice. It must never refresh cloud
state, call a cloud billing API, acquire a cluster lock, or delay a launch,
job transition, status request, or teardown.

## Decision

Use the existing cluster-history usage intervals as the accounting source of
truth. A periodic best-effort daemon will split those intervals at UTC day
boundaries, price them with SkyPilot's local catalog, and upsert a small daily
rollup table. The API and dashboard will read only the rollup table.

Managed-job state is used only to label dedicated cluster intervals with a
job and task. It is not a second source of usage, so it cannot double-count
the backing machines.

Cloud billing exports can be added later as a delayed reconciliation series.
They must not replace or gate the local estimate.

## Estimate semantics

The estimate is:

```
sum(overlap_seconds_with_day / 3600
    * catalog_hourly_price
    * launched_node_count)
```

where the overlap comes from `cluster_history.usage_intervals` and an open
interval ends at the estimator's `as_of` timestamp.

The estimate includes:

- dedicated managed-job VMs, including provisioning, setup, recovery, and
  teardown time while the backing cluster interval is open;
- ordinary SkyPilot VM clusters;
- VM-backed shared pools, services, and controller overhead, reported as
  separate workload types rather than falsely attributed to a job; and
- on-demand or spot catalog pricing as encoded by the launched `Resources`.

The estimate explicitly does not include:

- Kubernetes-backed clusters or pods, even when a custom Kubernetes pricing
  catalog is configured;
- reservation, committed-use, savings-plan, negotiated-contract, credit,
  refund, or tax adjustments;
- storage, disks, snapshots, network egress, public IPs, load balancers, or
  other non-compute line items; or
- machines created outside SkyPilot's cluster history.

For AWS/GCP reservations, the displayed value is therefore the
pay-as-you-go-equivalent catalog estimate. We do not claim that a particular
instance consumed a reservation. Kubernetes usage is excluded rather than
displayed as `$0`, so excluded usage cannot be confused with known-free usage.

All UI and API labels should say **Estimated compute cost**, not **Spend** or
**Bill**, and show the basis and `as_of` time.

## Why not use managed-job duration

`spot.job_duration` describes task execution and intentionally excludes
provisioning and recovery time. It also cannot describe multiple recovery
clusters, multiple nodes, or shared pool uptime. It is useful for attribution
and diagnostics, but not for machine accounting.

The invariant is: every `cluster_hash` is charged at most once, to exactly one
workload bucket. Jobs decorate that charge; they never create another one.

## Existing source data

`cluster_history` already persists:

- `cluster_hash` and `name`;
- `num_nodes` and the launched `Resources` object;
- one or more `(start, end)` usage intervals, with `end = NULL` while active;
- user/workspace and cloud/region/zone metadata; and
- `is_managed` for controller-created clusters.

This is also the source used by `sky cost-report`. Its known inaccuracies
(manual cloud-console changes, partially launched multi-node clusters, and
current rather than historical catalog prices) are acceptable for this rough
estimate and must be surfaced in the response metadata.

## Architecture

```
cluster lifecycle writes
        |
        v
cluster_history  ---- managed-job metadata (attribution only)
        |
        | bounded local DB reads; no cloud calls
        v
estimated-spend rollup daemon
        |
        v
estimated_spend_daily + estimated_spend_state
        |
        | aggregate SQL only
        v
GET /estimated_spend --> admin dashboard

optional, later:
provider billing export --> billed_spend_daily (separate reconciliation)
```

### Launch-path metadata

Add nullable scalar attribution columns to `clusters` and `cluster_history`:

- `workload_type`: `managed_job`, `pool`, `service`, `controller`, or
  `cluster`;
- `workload_id`: managed job ID, pool name, service name, or cluster name; and
- `workload_task_id`: task ID for a managed pipeline/job group.

For managed jobs, the controller already places `SKYPILOT_MANAGED_JOB_ID` and
the task ID in the task environment. Pass those existing values through the
backend's launch context into `add_or_update_cluster()`. This adds only scalar
fields to the DB write already performed during launch; it adds no new write,
lookup, lock, or network call.

For old rows, the backfill may reconstruct dedicated managed-job attribution
from the deterministic task-name/job-ID cluster name and the managed-jobs DB.
If it cannot verify a mapping, it uses `managed_unattributed`; it must not
guess.

Shared pools are attributed to `pool:<name>`, not to individual jobs. A later
allocation model could apportion a pool using overlapping running intervals
and requested resources, but that would be a different, explicitly modeled
estimate.

### Rollup tables

Add a global-user-state migration containing:

```
estimated_spend_daily
  day_start_utc          INTEGER
  cluster_hash           TEXT
  cluster_name           TEXT
  workload_type          TEXT
  workload_id            TEXT NULL
  workload_task_id       INTEGER NULL
  user_hash              TEXT NULL
  workspace              TEXT NULL
  cloud                  TEXT NULL
  region                 TEXT NULL
  use_spot               BOOLEAN NULL
  num_nodes              INTEGER NULL
  machine_seconds        INTEGER
  catalog_hourly_rate    FLOAT NULL
  estimated_cost         FLOAT NULL
  exclusion_reason       TEXT NULL
  priced_at              INTEGER NULL
  updated_at             INTEGER
  PRIMARY KEY (day_start_utc, cluster_hash)

estimated_spend_state
  singleton_id           INTEGER PRIMARY KEY
  last_started_at        INTEGER NULL
  last_success_at        INTEGER NULL
  source_watermark       INTEGER NULL
  source_watermark_hash  TEXT NULL
  active_cursor_hash     TEXT NULL
  coverage_start_utc     INTEGER NULL
  last_error             TEXT NULL
```

Add `usage_updated_at` (indexed) to `cluster_history` and update it in the same
transaction whenever usage intervals change. This gives the daemon a reliable
incremental watermark without another lifecycle write.

`estimated_cost` is `NULL`, not zero, when pricing is unknown or the row is
excluded. `exclusion_reason` distinguishes `kubernetes` and `unknown_price`
in the MVP.

The daily table keeps one row per source cluster per day. API queries can then
group by day, workload, workspace, user, or cloud without unpickling handles
or resource objects on a request path.

## Rollup algorithm

The daemon runs every five minutes by default, with the interval configurable.
Each sweep:

1. Acquires a dedicated distributed lock non-blockingly. If another API-server
   replica owns it, this sweep exits immediately.
2. Reads a bounded batch of source rows that are active, have
   `usage_updated_at` beyond the durable watermark, or belong to an unfinished
   initial backfill.
3. Unpickles only the selected rows' usage intervals and launched resources.
4. Excludes Kubernetes before pricing.
5. Calculates one local catalog hourly rate for the cluster and splits every
   interval at UTC midnight.
6. Replaces/upserts the affected `(day_start_utc, cluster_hash)` rows in short
   transactions. Reprocessing a source row produces the same values; it never
   increments an existing total blindly.
7. Advances `last_success_at` and the watermark only after the bounded sweep
   succeeds.

Active rows and the previous two UTC days are recomputed every sweep. Both the
active scan and changed-row scan use durable keyset cursors, so fleets larger
than one batch still make progress without an unbounded query. This closes
open intervals and corrects late status/teardown observations. An initial
90-day backfill runs in small batches and yields between batches; its coverage
is reported while incomplete. Rows older than the serving window are pruned in
bounded batches from the rollup table only.

Safety properties:

- no cloud, Kubernetes, SSH, controller, or billing API calls;
- no cluster or job locks;
- no write to a lifecycle table beyond the scalar metadata/timestamp fields in
  an existing transaction;
- bounded source rows and short rollup-table write transactions per sweep;
- failures are caught and recorded, and the previous completed estimate stays
  readable; and
- the daemon can be disabled without affecting any SkyPilot operation.

Suggested operational metrics (without job-ID labels):

- `sky_estimated_spend_rollup_last_success_timestamp_seconds`;
- `sky_estimated_spend_rollup_lag_seconds`;
- `sky_estimated_spend_rollup_duration_seconds`;
- `sky_estimated_spend_rollup_rows_processed_total`; and
- `sky_estimated_spend_rollup_errors_total`.

## API

Add a new admin-only API instead of changing the existing `cost_report`
response shape:

```
GET /estimated_spend?days=30&group_by=job
GET /estimated_spend?start_date=2026-07-12&end_date=2026-07-12&group_by=job
```

The endpoint bumps `API_VERSION`, defines named minimum-version constants, and
the dashboard's client API version. Older client/server combinations continue
to use the unchanged `cost_report` contract. It accepts either a bounded
1--90 day rolling range or an exact inclusive UTC `start_date` and `end_date`
within the retained 90 days, plus a `job`, `user`, or `purchase_option`
grouping. Exact bounds apply to every returned aggregate. The additive response
includes a cost-ranked table capped at 50 groups and a daily chart capped at
the eight highest-cost groups plus `Other`. Job and user rows include spot and
on-demand subtotals. The route is denied to default users and viewers by RBAC
and also checks the admin role directly before querying.

The paginated owner, workload, task, and physical-attempt hierarchy is
specified in `docs/designs/spend-attribution-hierarchy.md`. It remains
admin-only, applies the same bounded date range, and reads the rollup table
without changing the estimate semantics above. Future workspace or cloud
filters must likewise apply RBAC before querying the rollup table.

Example response:

```
{
  "currency": "USD",
  "basis": "skypilot_catalog_payg_equivalent",
  "as_of": 1783702800,
  "last_successful_refresh_at": 1783702800,
  "stale": false,
  "kubernetes_included": false,
  "reservation_adjustments_applied": false,
  "coverage_start_utc": 1781136000,
  "start_date": "2026-07-10",
  "end_date": "2026-07-10",
  "requested_days": 1,
  "totals": {
    "estimated_cost": 4312.18,
    "priced_machine_seconds": 987654,
    "excluded_machine_seconds": 1234
  },
  "days": [
    {
      "date": "2026-07-10",
      "estimated_cost": 182.41,
      "priced_machine_seconds": 42120,
      "excluded_machine_seconds": 0
    }
  ]
}
```

The endpoint performs aggregate SQL over the rollup only. It never triggers a
refresh. If the daemon is late or failed, it returns the last snapshot with
`stale: true` and the actual timestamp rather than blocking for fresh data.

The UI uses a stacked daily chart and a matching table grouped by job/workload,
user, or purchase option. It offers Today, Yesterday, rolling presets, and an
exact UTC range. Chart tooltips name each nonzero series and omit zero-valued
entries. Managed jobs, pools, services, ordinary clusters, and platform
overhead remain distinct within job/workload grouping. The existing CLI can
expose the same result as `sky cost-report --daily` after the new API version
is negotiated.

## Provider APIs and reconciliation

Provider pricing APIs can refresh catalog rates in a separate background
process, but they do not report current incurred spend. Provider billing data
is also too delayed and inconsistently attributable to be the serving source:

- AWS Cost Explorer normally updates at least daily, and resource-level/hourly
  data is opt-in and limited to a recent window.
- Google Cloud Billing export arrives throughout the day but has no delivery
  or latency guarantee and must be enabled before it can serve as durable
  history.
- Azure Cost Management usage commonly arrives many hours later and recommends
  low-frequency polling.

If invoice reconciliation is later required:

1. Tag provider resources with stable `cluster_hash`, workload, workspace, and
   job identifiers where the provider supports it.
2. Run a separate once-daily importer into `billed_spend_daily`.
3. Show billed and estimated series separately, with their different `as_of`
   times and coverage.
4. Never overwrite the operational estimate or make job/cluster operations
   depend on billing permissions or exporter health.

Reservation and Kubernetes allocation remain explicit non-goals for this
reconciler unless a separate accounting policy is designed.

## Edge cases

- An open interval is clipped to the sweep's single `as_of` timestamp.
- Invalid or negative intervals are skipped; one corrupt row never fails the
  sweep. A future metric can expose the skipped-interval count.
- A multi-node cluster uses the persisted launched node count. Partial launch
  time remains an acknowledged source limitation.
- A terminated and relaunched cluster normally has a new `cluster_hash`; a
  stop/restart contributes multiple intervals to the same hash without double
  counting.
- Recoveries for one managed job may create several cluster hashes. Their
  daily rows aggregate under the same job ID.
- Parallel job-group tasks remain individually attributable by task ID and
  sum to the job total.
- Pool machines are never assigned wholly to the last or longest-running job.
- Catalog lookup failure yields unknown cost, not `$0`.
- A price-catalog update may change a recomputed recent estimate. `priced_at`
  makes that visible; this is acceptable for an estimate.

## Rollout

1. Add attribution fields, `usage_updated_at`, migrations, and the pure daily
   interval-splitting/pricing library.
2. Add the bounded best-effort daemon and JSON API; measure DB latency, sweep
   time, and lag in one API-server deployment.
3. Compare the sum of non-Kubernetes daily rows with `sky cost-report` for the
   same interval and `as_of` time.
4. Add the dashboard chart and job/pool/cluster breakdown.
5. Consider provider billing reconciliation only after the estimate is stable.

## Test plan

Unit tests:

```bash
pytest tests/unit_tests/test_sky/test_estimated_spend.py
pytest tests/unit_tests/test_sky/test_cost_report.py
```

Cover UTC-midnight splits, open intervals, multiple stop/start intervals,
multi-node and spot rates, managed-job recoveries, corrupt rows, unknown
prices, explicit Kubernetes exclusion, idempotent reruns, watermark recovery,
stale-snapshot behavior, mixed spot/on-demand jobs, user attribution, and the
bounded `Other` chart series.

Database/API tests should run with both SQLite and PostgreSQL and verify that:

- concurrent daemon attempts have one non-blocking winner;
- rerunning a sweep does not change totals;
- a failed sweep leaves the previous snapshot readable;
- workspace filters cannot expose inaccessible rows; and
- the API query performs no catalog, provider, cluster-refresh, SSH, or
  Kubernetes calls.

Manual verification:

1. Launch a short non-Kubernetes managed job and record its backing cluster
   start/stop interval.
2. Wait one rollup interval and confirm the job/day row is within rounding
   error of `nodes * catalog_rate * uptime`.
3. Force a recovery and confirm both cluster hashes aggregate once under the
   same job.
4. Run a Kubernetes job and confirm its machine seconds are reported as
   excluded with no contribution to estimated cost.
5. Stop or crash the rollup daemon and confirm launches, job transitions, and
   teardowns continue normally while the API serves a stale snapshot.
6. Run formatting and targeted checks:

```bash
bash format.sh --files <changed-python-files>
pytest tests/unit_tests/test_sky/test_estimated_spend.py
```
