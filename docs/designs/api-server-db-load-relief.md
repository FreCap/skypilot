# API server PostgreSQL load relief

_Created: 2026-08-02_

## Problem

The PostgreSQL-backed API server, managed-job controllers, and SkyServe
controllers perform several high-frequency operations whose database cost is
independent of useful work:

1. Every managed-job controller polls for a `WAITING` job every 10 seconds.
   The candidate query has no supporting index, so every idle poll scans the
   complete `job_info` table and opens a new PostgreSQL session.
2. Each PostgreSQL request queue dispatcher reaps expired claims before every
   queue pickup.
3. An idle PostgreSQL request dispatcher polls every 100 ms with no backoff.
4. Every replica readiness round upserts every trackable replica, even when its
   persisted readiness state did not change.
5. Each forwarded HA load-balancer role heartbeat reads controller ownership
   and cutover state five times across the stable API proxy and service
   controller.
6. Production opens roughly 168 PostgreSQL sessions per second, but the server
   does not expose which process role and engine policy create those physical
   sessions. Pooling changes would therefore be guesswork across paths with
   different event-loop and advisory-lock correctness constraints.

These operations are individually small, but their aggregate rate grows with
API-server roles, services, and replicas. They also compete with user-facing
requests for database connections, CPU, I/O, and lock time.

The changes in this design are independent and ranked by measured production
impact, then rollout risk. Each milestone must be implemented, measured, and
shipped separately. Milestone 0 requires one forward-only managed-jobs schema
migration. No milestone changes a public API or requires an API version bump.

## Goals

- Replace the idle managed-job full-table scan with an ordered empty index probe
  without changing candidate order, row locking, batch serialization, or claim
  ownership.
- Remove expired-claim sweeps from the queue pickup cadence without changing
  claim fencing or replay decisions.
- Reduce empty-queue polling while keeping a hard, configurable pickup-latency
  bound and full throughput while the queue is busy.
- Stop stable readiness probes from rewriting unchanged replica rows.
- Coalesce redundant HA owner and cutover reads without weakening the proxy's
  before/after ownership fence, the controller's early ownership rejection, or
  its in-lock authority check.
- Keep every optimization safe during a mixed `v1.1.1015` and current-version
  rollout within the existing HA ownership and rollout gates.
- Provide an explicit disabled or legacy-equivalent setting for each behavioral
  optimization so production enablement and rollback are configuration-only.
- Attribute successful physical PostgreSQL connections to a bounded process
  role, engine namespace, and sync/async mode before changing any pool policy.

## Non-goals

- Replacing polling with PostgreSQL `LISTEN/NOTIFY`.
- Changing the 30-second request claim lease or 10-second claim heartbeat.
- Changing request ordering, precondition semantics, replay policy, or executor
  parallelism.
- Changing readiness probe transport, interval, teardown thresholds, or the
  serialized `ReplicaInfo` format.
- Removing either stable API proxy owner read.
- Removing the controller's initial `_owns_current_service()` read.
- Changing HA routes, headers, role responses, cutover state, or compare-and-set
  mutations.
- Adding SQLite compatibility to central/API-server paths.
- Changing the managed-job controller's 10-second idle polling interval.
- Changing synchronous or asynchronous pool classes, pool sizes, connection
  lifetimes, advisory-lock ownership, or prepared-statement behavior as part of
  the connection-attribution milestone.
- Adding database URLs, names, users, PIDs, job IDs, service names, request
  names, or any other unbounded value to PostgreSQL connection metrics.

## Baseline and version comparison

The initial scheduler-index implementation was based on commit
`34ac0be6c3d7ca1bdaaf7ec8459a590a07a4df7f`, tagged `v1.1.1043`. The
connection-attribution milestone is based on commit
`3e06b0e88587479dfc58496f2cf9e71d4a8e73b2`, after the scheduler index shipped
in `v1.1.1047` and the retired physical-capacity evidence scan was removed, on
`origin/improvements`.

The changed-only readiness milestone is based on commit
`5b003e81e462ba70382fda5b7483c4bf0b77ad4e` on `origin/improvements`, tagged
`v1.1.1066`. That base
includes typed system-OOM recovery commit
`0127dfc4ce23eb03e4da0df3be4f6ff785f09054`, tagged `v1.1.1061`, which added
durable recovery reductions and route-suspension writes to the readiness probe
loop. Those writes are outside the readiness fingerprint and must remain
unconditional.

The relevant behavior was compared directly with `v1.1.1015`:

| Path | `v1.1.1015` and current behavior | Relevant current-only difference |
|---|---|---|
| `RequestWorker.process_request()` and `run()` | Function ASTs are identical. An empty `queue.get()` sleeps 100 ms; a nonempty queue immediately starts the next pickup. | None. |
| `PostgresQueueBackend.get()` | Every call opens a transaction, checks leadership when applicable, runs `_reap_expired_claims()`, then runs `_candidate()`. | Current code scopes work by execution class and controller leadership. Cadence is unchanged. |
| `PostgresQueueBackend._reap_expired_claims()` | Selects up to 100 expired claimed rows with `FOR UPDATE SKIP LOCKED`, then requeues replayable work or terminalizes ambiguous mutating work. | Current code applies the same execution-class and controller-leadership predicates used for pickup. |
| `SkyPilotReplicaManager._probe_all_replicas()` | Every non-preempted, non-terminal probe result is appended to `pending_writes`, then the whole list is batch-upserted. | Current code also performs typed system-recovery reductions and owner-fenced route suspensions in the probe loop. Changed-only filtering must be limited to ordinary replicas and preserve those recovery writes. |
| `controller_proxy._proxy_controller_sync()` | Function ASTs are identical. It reads owner before forwarding and after receiving the response. | None. |
| `SkyServeController._handle_load_balancer_role()` | Function ASTs are identical. It performs an initial owner read, then an in-lock fence read and a separate cutover-state read. | None. |

The current single-process server starts one long and one short dispatcher. On
an empty PostgreSQL queue, each dispatcher attempts about 10 pickups per second.
Each attempt performs one expired-claim `SELECT` and one candidate `SELECT` in
one short transaction. The nominal empty-queue floor is therefore about 20
transactions and 40 `SELECT`s per second per process that owns both dispatchers.
Executor pool size does not multiply this rate.

The production snapshot checked on 2026-08-02 still uses the SQLite request
backend and has no PostgreSQL API-request tables. The request-queue milestones
therefore remove no current production PostgreSQL load. They are pre-cutover
safeguards; their projected read reductions apply only after a PostgreSQL
request-backend cutover.

A read-only live sample after the `v1.1.1034` rollout identified the immediate
production work:

| Signal | Observed production value | Interpretation |
|---|---:|---|
| Aurora writer CPU | 91 to 98 percent one-minute average, repeatedly reaching 100 percent | The database is already at its configured compute ceiling. |
| Transactions | about 673 per second | High fixed control-plane churn competes with useful work. |
| New PostgreSQL sessions | about 168 per second | Short async operations use `NullPool`; connection setup is material but needs path-by-path ownership proof before pooling changes. |
| `job_info` | 1.22 million rows scanned per minute from a 5,131-row table | The scheduler candidate query performs about 238 full scans per minute even with no `WAITING` jobs. |
| `replicas` | 471 updates per minute in one sample | Stable readiness persistence is about 65 percent of observed row updates. |
| `services` | 3,237 indexed scans per minute | Service reads remain a later consolidation candidate after the measured scan and write amplifiers. |

A fresh production snapshot on 2026-08-03 confirmed which remaining milestone
is useful before a PostgreSQL request-store cutover. The Aurora Serverless v2
writer was at its 8-ACU ceiling, with 100 percent ACU utilization and 94.9
percent average CPU over 20 minutes. A 141.8-second database sample measured
about 165 new sessions and 505 commits per second. The two ready services had
176 trackable replicas, of which 171 were already `READY`.

In an aligned 51.1-second `pg_stat_user_tables` sample, `replicas` accounted for
353 of 586 user-table updates, or 60.2 percent, and only 175 of those replica
updates were HOT. A longer 141.8-second sample observed 875 replica updates, or
about 370 per minute. Removing stable readiness rewrites can therefore remove
roughly 97 percent of readiness-generated replica updates and much of the
observed tuple, index, and WAL churn. It cannot explain or remove the measured
session and commit rates: the readiness path batches a fleet round into about
0.04 transactions per second. CPU improvement must be measured in the canary,
not projected from row counts.

Because production still uses the SQLite request store, the current-production
execution order after connection attribution is Milestone 3, followed by
Milestones 1 and 2 only as PostgreSQL request-store cutover safeguards.
Milestone 4 remains evidence-gated because its HA fencing contract has higher
correctness risk.

The `job_info` snapshot contained 5,008 `DONE`, 114 `ALIVE_BACKOFF`, four
`LAUNCHING`, three `INACTIVE`, two `ALIVE`, and zero `WAITING` rows. There were
no batch rows. Existing indexes covered only the primary key, `pool`, and
`current_cluster_name`. The exact candidate `EXPLAIN` was
`Limit -> LockRows -> Sort -> Seq Scan job_info`, with the busy-batch subplan
using the pool index. This makes the ordered waiting-job index the first current
production fix.

The scheduler poll is not the main session-churn source. Its 10-second interval
and hard 64-controller limit bound it at 6.4 new sessions per second, and the
live scan count bounds the observed contribution closer to four per second.
That is at most about 3.8 percent of the measured 168 new sessions per second.
Milestone 0 removes row visits and sort work but intentionally does not change
async `NullPool` behavior. Architecture-wide connection reuse remains a
separate investigation because a cached async engine may cross event loops.

The exact `v1.1.1047` production canary confirmed that boundary. In the
313.6-second aligned pre-deployment interval, `job_info` accumulated 1,249
sequential scans and 6.41 million sequential tuple reads. In the 304.6-second
aligned post-deployment interval, both deltas were zero, and the live candidate
plan became `Limit -> LockRows -> Index Scan` with no outer sort. New sessions
still measured about 176 per second and Aurora CPU remained between 98 and 100
percent. The index removed the targeted scheduler amplification completely,
but connection attribution is required before changing any pool policy.

For a service with `R` trackable replicas and the default 10-second readiness
interval, the current probe path upserts about `6R` replica rows per minute even
when readiness is stable. At 500 replicas that is about 3,000 row updates per
minute. At 1,000 replicas it is about 6,000.

For an HA service, the stable API proxy performs two owner reads and the
controller performs three more reads for each role heartbeat. With two slots
reporting every two seconds, the service receives about one combined role
heartbeat per second, or about five authority-state `SELECT`s per second.

## Data flow and invariants

### Managed-job scheduler

```text
controller process -> every idle 10 seconds -> get_waiting_job_async()
                   -> owner fence in one transaction
                   -> busy batch-pool subquery
                   -> highest priority WAITING row FOR UPDATE
                   -> compare-and-swap to LAUNCHING -> commit
```

Every controller process polls independently. With about 20 controllers and a
10-second idle interval, the production rate is about two candidate calls per
second. The candidate statement references `job_info` twice, which explains
the observed roughly four sequential scans per second. The selected order,
`priority DESC, spot_job_id ASC`, is a fairness contract. The row lock, owner
fence, and compare-and-swap are concurrency contracts. The first fix changes
only the access path used to find rows satisfying `schedule_state = 'WAITING'`.

### Request queue

```text
dispatcher -> queue.get()
           -> PostgreSQL transaction
              -> optional controller leadership lock
              -> expired-claim sweep
              -> candidate read
           -> claim transaction if a candidate exists
           -> executor submission
```

Expired-claim cleanup is a recovery duty. Candidate lookup is a latency duty.
They share a transaction today, but they do not require the same cadence. The
lease token, execution generation, role predicate, leadership predicate, replay
policy, and `FOR UPDATE SKIP LOCKED` behavior are correctness boundaries and
must remain unchanged.

### Readiness persistence

```text
fleet read -> parallel probes -> mutate readiness bookkeeping in memory
           -> full-fleet batch upsert -> teardown changed replicas
           -> return end-of-round snapshot
```

The probe round currently mutates only these persisted readiness fields for a
non-preempted replica:

- `status_property.service_ready_now`
- `status_property.first_ready_time`
- `first_not_ready_time`
- `first_consecutive_failure_time`

Preemption and teardown have separate persistence paths. The probe bookkeeping
must still be durable before `_terminate_replica()` re-reads a row.

### HA role heartbeat

```text
stable API: owner-before -> controller POST -> owner-after
controller: initial owner -> role lock -> Kubernetes pod authority
            -> owner/cutover fence -> cutover state -> saga actions
```

The stable API reads are a temporal fence around a non-retried POST and cannot
be merged. In the controller, the authoritative database validation must occur
inside the role lock and after Kubernetes pod authority is established. The two
adjacent database reads at that point can be one snapshot.

### Physical PostgreSQL connection attribution

```text
PostgreSQL engine creation -> optional bounded connect listener
                           -> successful physical DBAPI connection
                           -> counter(process role, engine namespace, mode)
```

The counter observes SQLAlchemy's physical `connect` event. It does not count
ORM sessions, pool checkouts, transactions, statements, failed connection
attempts, or PostgreSQL backend connections hidden behind a future proxy. A
`NullPool` operation therefore increments for every successful physical
connection, while repeated checkouts from one live `QueuePool` connection do
not. For an async engine, the listener attaches to `AsyncEngine.sync_engine`,
which is SQLAlchemy's supported event target for the adapted DBAPI connection.

Attribution uses closed labels only. The process role is resolved when the
physical connection opens, not when the engine is created, because cached
engines can be inherited by child processes. The server's existing
`SKYPILOT_API_SERVER_ROLE` supplies `all`, `api`, `executor`, and `controller`.
A validated write-once process-local override identifies
request executor children, consolidated managed-job controllers, and
consolidated Serve controllers before plugins or database state initialize.
An unexpected role maps to `unknown` instead of becoming a label.

Engine namespaces are normalized to `shared`, `api-requests-control`,
`api-requests-liveness`, `advisory-lock`, `reserved-fill-reclaim-proof`, or
`other`. Sync and async are the only mode values. The complete Cartesian bound
is therefore 7 process roles times 6 namespaces times 2 modes, or at most 84
labeled combinations.
The production multiprocess collector exports one `_total` series for each
combination. A non-multiprocess local registry may also expose Prometheus
client's `_created` companion series. Database URLs, users, process IDs, job
IDs, service names, request names, and caller-supplied namespaces never appear
in labels.

The first production attribution canary is deliberately limited to the current
monolithic deployment: `apiService.highAvailability.enabled=false`, one API
pod, and executor and controller children that share the pod's metrics
environment and `/tmp/metrics` directory. With high availability enabled, the
chart runs API, controller, and executor roles in separate pods, while
`apiService.metrics.enabled` currently exposes only the API
deployment's metrics endpoint. The aggregate must not be treated as complete
in that topology until every database-owning role has a scraped endpoint or an
equivalent cross-pod aggregation path. Adding that HA metrics topology is
outside this observability-only PR.

## Ranked solution

### Milestone 0: index the waiting-job scheduler path

Add managed-jobs schema revision `027` with this logical index:

```sql
CREATE INDEX ix_job_info_schedule_priority
    ON job_info (schedule_state, priority DESC, spot_job_id ASC);
```

The leading equality column makes an idle `WAITING` lookup an empty index probe.
The remaining key order preserves the existing fairness order and allows
`LIMIT 1` without a sort. Do not use a partial
`WHERE schedule_state = 'WAITING'` index: SQLAlchemy binds the state value, and
PostgreSQL cannot generally prove at plan time that a parameter implies a
partial-index predicate. The batch-pool eligibility expression remains a
residual filter; do not add a busy-batch index until a workload with batch rows
proves that subplan material.

Declare the same composite index in `sky/jobs/state_schema.py` so a database
bootstrapped through revision 001 has the same shape as an upgraded database.
Revision 027 must:

- create the PostgreSQL index concurrently inside an Alembic autocommit block;
- create the equivalent ordered composite index on SQLite, because local
  managed job/controller databases still officially use SQLite;
- validate an existing same-name index's table, key columns, ordering,
  uniqueness, access method, readiness, and validity;
- repair only invalid or not-ready PostgreSQL residue from an interrupted
  concurrent build, and reject a valid same-name index with the wrong shape;
- remain forward-only on downgrade, matching the managed-jobs migration policy.

No query text, polling interval, lock, transaction, batch predicate, owner
fence, or state transition changes in this milestone. On PostgreSQL, the target
plan is an ordered index scan under `LockRows` with no `Sort` and no sequential
scan of `job_info` when no waiting row exists.

Files:

- `sky/jobs/state_schema.py`
- `sky/schemas/db/spot_jobs/027_add_waiting_job_priority_index.py`
- `sky/utils/db/migration_utils.py`
- `tests/unit_tests/test_batch_recovery.py`
- `docs/designs/api-server-db-load-relief.md`

Focused tests:

- a fresh SQLite schema and an upgrade from revision 026 expose the exact
  ordered composite index;
- a PostgreSQL upgrade requests a concurrent build;
- an interrupted invalid PostgreSQL index is repaired;
- a valid same-name wrong-shape index is rejected without advancing revision;
- fresh bootstrap and upgraded databases converge to the same index shape;
- scheduler behavior tests preserve priority order, batch-pool exclusion,
  `FOR UPDATE`, and the compare-and-swap result;
- a PostgreSQL plan test with thousands of terminal rows and no waiting rows
  uses `ix_job_info_schedule_priority` and contains no `Seq Scan` or `Sort` on
  `job_info`.

### Observability gate: attribute physical PostgreSQL connections

Add this counter to `sky/metrics/utils.py`:

```text
sky_postgres_connections_opened_total{
  process_role,
  engine_namespace,
  mode
}
```

The metric is present only through the existing API-server metrics surface.
`sky/utils/db/db_utils.py` installs listeners only when
`SKY_API_SERVER_METRICS_ENABLED=true`. When disabled, `db_utils` must neither
resolve its lazy `sky.metrics.utils` import nor attach a listener. This makes a
code-only rollout behaviorally inert and avoids adding Prometheus allocation
work to controller processes that do not publish metrics.

Attach exactly one listener immediately after each new PostgreSQL engine is
created in both cache-miss branches:

- the default sync or async engine in `get_engine()`;
- the dedicated advisory-lock engine in `get_postgres_lock_engine()`.

Do not attach on cache hits or SQLite engines. Normalize a missing default
namespace to `shared`, preserve `api-requests-control`,
`api-requests-liveness`, and `reserved-fill-reclaim-proof`, assign
`advisory-lock` to the lock engine, and map every other namespace to `other`.
Attach async events to `AsyncEngine.sync_engine` and label them `async`; all
other paths are `sync`.

Resolve the process role inside the event callback. Add a validated
process-local setter in `db_utils` and call it before plugin or database
initialization in:

- `sky/server/requests/executor.py:executor_initializer()` with `executor`;
- `sky/jobs/controller.py:main()` with `managed-job-controller`; and
- `sky/serve/controller.py:run_controller()` with `serve-controller`.

This explicit override is required because the current consolidated Serve
controller sets the same `IS_SKYPILOT_JOB_CONTROLLER` compatibility marker as
the managed-job controller. The base server role remains the fallback for
ordinary API, executor, and controller processes. Invalid
base roles map to `unknown`; invalid explicit setter values raise before any
engine can be opened. Repeating the same explicit value is idempotent, while an
attempt to change an already-set explicit process role raises. Entrypoint tests
prove that every specialized child sets that role before plugin or database
initialization, so it cannot first emit under the inherited server role.

This is an observability-only PR. Do not change `NullPool`, `QueuePool`, pool
sizes, recycling, timeouts, advisory-lock ownership, or event-loop behavior.
The counter records a successful physical connection after SQLAlchemy obtains
it. A failed connection attempt does not increment it. The listener must fail
open: a listener-registration, import, registry, multiprocess-file, or increment
exception emits at most one warning per process and never prevents engine use
or rejects or closes the database connection. Metrics are not part of database
correctness.

Files:

- `sky/metrics/utils.py`
- `sky/utils/db/db_utils.py`
- `sky/server/requests/executor.py`
- `sky/jobs/controller.py`
- `sky/serve/controller.py`
- `tests/unit_tests/test_sky/utils/test_db_utils.py`
- `tests/unit_tests/test_parent_death_watchdog.py`
- controller entrypoint tests selected by the final diff
- `docs/designs/api-server-db-load-relief.md`

Focused tests:

- the label allowlists and normalization fallbacks are closed and bounded;
- SQLite engines never attach the PostgreSQL listener;
- metrics-disabled PostgreSQL creation does not resolve the lazy metrics import
  or register a listener;
- every PostgreSQL cache-miss branch attaches once and cache hits do not attach
  again;
- two successful `NullPool` physical connections increment twice, while two
  checkouts reusing one `QueuePool` physical connection increment once;
- an async engine attaches through `sync_engine` and reports `async`;
- failed physical connection attempts do not increment;
- callbacks resolve the current process role at connection time;
- a metric-recording exception warns at most once and does not make connection
  establishment fail;
- executor, managed-job controller, and Serve controller entrypoints install
  their explicit role before plugins or database initialization;
- the seven-label role allowlist stays bounded, repeated same-role
  initialization is allowed, and cross-role reassignment fails; and
- Prometheus multiprocess collection exposes the counter without a PID label.

### Milestone 1: throttle expired-claim reaping independently

This is the first pre-cutover request-store candidate. It is isolated to queue
recovery cadence, preserves pickup cadence, and does not change the SQL or state
transitions of a sweep.

Add this internal environment variable in
`sky/server/requests/postgres.py`:

```text
SKYPILOT_SERVER_API_REQUEST_EXPIRED_CLAIM_REAP_INTERVAL_SECONDS
```

Contract:

- Parse it once when each `PostgresQueueBackend` is constructed.
- Unset, invalid, non-finite, or negative values use 0.1 seconds. An API-server
  process emits at most one warning when any backend sees an invalid explicit
  value, avoiding duplicate startup warnings from the long and short backends.
  The 0.1-second default matches the current nominal empty-queue cadence.
- An explicit value of zero selects exact legacy cadence: sweep on every
  `get()`. This is the configuration-only emergency rollback for a recovery
  regression.
- A newly constructed backend is immediately due for a sweep.
- Use `time.monotonic()` for the process-local deadline.
- Decide whether a sweep is due before opening the existing `get()` transaction.
- When due, run the existing `_reap_expired_claims()` before `_candidate()` in
  the same transaction.
- Make `_reap_expired_claims()` report how many rows its existing capped query
  selected. Advance the next deadline only after that transaction commits
  successfully and the selected row count is below
  `_MAX_EXPIRED_CLAIMS_PER_SWEEP`.
- If a sweep selects the full 100-row batch, leave it immediately due. The next
  pickup performs another sweep, so a mass owner loss drains at the current
  dispatcher cadence instead of at only 100 rows per configured interval. One
  final under-cap sweep advances the deadline.
- A rollback, database exception, or failed controller leadership check also
  leaves the sweep due so the next pickup retries it.
- A successful under-cap sweep sets the next deadline from a fresh monotonic
  timestamp, avoiding catch-up bursts after a pause.
- Keep deadlines independent per queue backend. Long, short, and controller
  consumers must not suppress each other's recovery duty.
- Preserve all current schedule, execution-class, controller-leadership, lease,
  replay, and row-lock predicates byte-for-byte. The temporary private-handler
  quarantine belonged to the retired authority stack and is removed by the
  final R0 cleanup; it is not part of this milestone's steady-state contract.

Each `PostgresQueueBackend` must remain owned by exactly one dispatcher thread,
as it is in the current architecture. The cadence state is process-local and
single-consumer. Sharing one backend instance across dispatchers is unsupported:
with `SKIP LOCKED`, a concurrent under-cap sweep could advance the shared
deadline while another sweep holds a full batch, delaying the remaining
backlog. Separate backend instances may race safely because row locking and
terminal transitions remain the source of correctness. The throttle does not
add a new correctness lock.

Production enablement is a separate deployment configuration change. Start at
1.0 second. With two idle dispatchers, that changes the queue read floor from
about 40 to about 22 `SELECT`s per second: about 20 candidate reads plus two
sweeps. Recovery of a claim after its 30-second lease expires can be delayed by
at most the configured interval plus normal scheduling tolerance when fewer
than 100 claims are due for one backend. Larger bursts trigger immediate
successive sweeps until the backlog is below the batch cap.

Files:

- `sky/server/requests/postgres.py`
- `tests/unit_tests/test_api_requests_pg.py`
- `docs/designs/api-server-db-load-relief.md`

Focused tests:

- default and invalid configuration use 0.1 seconds, while zero and valid
  finite values are preserved;
- the server-owned environment-variable prefix keeps the knob out of durable
  request payload environments;
- multiple backends in one process emit only one invalid-value warning;
- zero performs a sweep on every `get()`;
- the first `get()` sweeps immediately;
- a second `get()` before the deadline performs candidate lookup without a
  sweep;
- a due `get()` sweeps again;
- long and short backend instances have independent deadlines;
- a successful under-cap transaction advances the deadline;
- rollback, database failure, and leadership loss do not advance it;
- a full 100-row sweep remains immediately due and drains the next batch;
- `test_expired_mutating_claim_is_not_replayed` retains its terminal outcome;
- `test_expired_read_only_claim_replays_with_new_generation` retains its new
  generation and token outcome;
- one throttled current backend and one legacy-style backend can race without a
  duplicate claim or invalid terminal transition.

### Milestone 2: bounded exponential idle queue backoff

Add this internal environment variable in
`sky/server/requests/executor.py`:

```text
SKYPILOT_API_REQUEST_QUEUE_IDLE_BACKOFF_MAX_SECONDS
```

Contract:

- The initial idle delay is 0.1 seconds, the multiplier is 2, and the configured
  value is a hard cap.
- Unset, invalid, non-finite, zero, or negative values use a 0.1-second cap.
  The default is therefore behavior-equivalent to the current idle loop.
- Do not add jitter. Jitter after capping can exceed the promised wake bound.
- Change `RequestWorker.process_request()` to return whether it obtained a queue
  item. An item that is later found stale, missing, or cancelled still counts as
  activity and resets backoff, allowing a nonempty queue to drain at full speed.
- Set the activity result immediately after a non-`None` dequeue and preserve it
  through every early return and exception path. A failure after dequeue must
  not be mistaken for an empty queue.
- A `queue.get()` exception occurs before activity and participates in the idle
  backoff. This replaces the current tight error retry with the same hard-bounded
  delay while preserving error logging.
- Move idle waiting to `RequestWorker.run()`.
- On an empty result, wait with `self._cancel_event.wait(delay)` instead of
  `time.sleep(delay)`, then double the delay up to the cap.
- On any obtained item, reset the next idle delay to 0.1 seconds and do not
  sleep.
- Preserve all retry, pause, and continue-condition sleeps. They are request
  semantics, not queue-idle polling.

Enable a 0.5-second cap on one server role first, then 1.0 second if short
request queue-wait p99 remains within its service objective. With Milestone 1 at
1.0 second and both dispatchers at the 1.0-second idle cap, the empty-queue
floor approaches four `SELECT`s per second: two candidate reads and two due
sweeps. This is about 90 percent below the current 40 `SELECT`s per second.

The tradeoff is explicit: after a sufficiently long idle period, a new request
can wait up to the cap before pickup. There is no reliable cross-process wake
signal today. A local event set by enqueue would not wake another pod, so it is
not part of this milestone.

Files:

- `sky/server/requests/executor.py`
- `tests/unit_tests/test_sky/server/requests/test_executor.py`
- `docs/designs/api-server-db-load-relief.md`

Focused tests:

- delay progression is exactly 0.1, 0.2, 0.4, then the configured cap;
- the default cap remains 0.1;
- the delay resets after any dequeued item;
- an exception before dequeue backs off, while an exception after dequeue resets
  the delay;
- a continuously nonempty queue never sleeps;
- cancellation interrupts an idle wait immediately;
- invalid configuration falls back to 0.1 and logs once;
- `process_request()` callers and broken-pool retry tests retain their outcomes;
- retry and pause paths still use their existing waits and are not capped by the
  idle backoff.

### Milestone 3: persist readiness rows only when readiness changes

Add this internal kill switch, defaulting to false for its first release:

```text
SKYPILOT_SERVE_CHANGED_ONLY_READINESS_PERSISTENCE
```

Parse the flag once when the replica manager is constructed. Only the
case-insensitive value `true` enables it. Unset or `false` disables it; any
other explicit value logs a warning and fails closed to disabled.

In `SkyPilotReplicaManager._probe_all_replicas()`, capture a compact immutable
readiness persistence fingerprint immediately before reducing each probe
result. Compare it with the same fingerprint after applying the result. A row
may be omitted from `pending_writes` only when the feature is enabled, the row
is changed-only eligible both before and after the reduction, and the
fingerprint did not change.

The fingerprint contains exactly:

```text
(
  status_property.service_ready_now,
  status_property.first_ready_time,
  first_not_ready_time,
  first_consecutive_failure_time,
)
```

This comparison deliberately does not call `to_storage_dict()` twice per
replica. Full serialization includes unrelated JSON and pickle state and would
replace database write amplification with controller CPU and allocation cost.
The helper must be named for its narrow contract, kept adjacent to the probe
mutations, and covered by transition tests for every field above. A code change
that adds another probe-mutated persisted field must update the fingerprint and
tests in the same change.

Changed-only eligibility is deliberately narrower than "no readiness change."
A row is eligible only when both its before and after state have
`system_recovery_disposition == ORDINARY`, `system_recovery is None`, and no
`system_recovery_quarantine`. Candidate, capable, quarantined, or transitioning
recovery rows are always appended even when the four readiness fields are
stable. This fail-closed boundary preserves the typed system-OOM recovery
subdocument without serializing or comparing it. Historical launch intent on
an otherwise validated ordinary row does not by itself make the row
ineligible, because the ordinary probe path does not mutate that intent.

Additional contract:

- Feature disabled: append and upsert every non-preempted probe result exactly
  as today, including the existing harmless `_persist_replicas([])` call when
  every result took the separate preemption path.
- Feature enabled: stable true-to-true and false-to-false ordinary results
  perform no replica upsert.
- Candidate, capable, quarantined, or recovery-transitioning rows remain
  unconditional writes. Preserve every system-recovery reduction, revision,
  route lease, and route-suspension outcome from the `v1.1.1061` base.
- Persist readiness transitions and timer initialization, clearing, and
  sentinel changes before any teardown re-read.
- Keep `_handle_preemption()` on its existing persistence path.
- When the feature is enabled and `pending_writes` is empty, do not call
  `_persist_replicas()` unless route suspensions must be committed. A nonempty
  route-suspension batch always calls the owner-fenced persistence path even
  if its replica-write batch is empty. The empty-batch variant must open a
  transaction and validate a nonempty service hash plus the exact live
  controller PID/IP owner before route holds are committed; partial, missing,
  malformed, or stale owner identity fails closed.
- Do not change the one full fleet read, probe concurrency, service-status
  update, returned end-of-round snapshot, owner fences, or batch-upsert format.
- Do not add `ON CONFLICT ... WHERE` as the primary filter. Sending and locking
  every row would retain much of the database cost, and a generic full-row
  equality predicate could interfere with compatibility repair of serialized
  state.

At stable readiness, row updates fall from about `6R` per minute to near zero.
Transitions still write once. Database reads and HTTP probes are unchanged.

Files:

- `sky/serve/replica_managers.py`
- `sky/serve/serve_state.py`
- `tests/unit_tests/test_probe_round_batching.py`
- `tests/unit_tests/test_serve_replica_managers.py`
- `tests/unit_tests/test_serve_state.py`
- `docs/designs/api-server-db-load-relief.md`

Focused tests:

- disabled mode preserves the existing full batch;
- stable ready and stable not-ready rows are omitted in enabled mode;
- candidate, capable, quarantined, and before/after recovery-transition rows
  remain in the batch even with stable readiness;
- a route-suspension-only round still calls the owner-fenced persistence path;
- an empty route-suspension batch validates the current controller identity
  and rejects missing or stale identity;
- false-to-true and true-to-false transitions persist;
- first readiness, first non-readiness, consecutive-failure start, and
  consecutive-failure recovery each persist;
- crossing the initial-delay threshold persists `first_ready_time = -1.0`
  before teardown;
- crossing a consecutive-failure threshold still tears down from already
  durable failure evidence;
- a mixed round persists exactly the changed replica IDs;
- preempted rows remain on their separate path;
- keep the existing
  `test_failed_research_probe_enters_interruption_prefilter` expectation in
  default-disabled mode and add an enabled-mode variant that performs no empty
  persistence call;
- a structural test guards the ordinary-path readiness assignments and the
  changed-only eligibility boundary against an undeclared persisted mutation;
- the typed system-OOM recovery probe and route-lease suites retain their
  outcomes unchanged.

### Milestone 4: combine the in-lock HA fence and cutover state

This milestone does not remove the stable API proxy's owner-before or
owner-after query, or the controller's initial ownership query.

Add an immutable `LbRoleAuthoritySnapshot` and
`get_lb_role_authority_snapshot(service_name)` in
`sky/serve/lb_cutover_state.py`, exposed through the historical
`sky.serve.serve_state` facade. One `SELECT` from `services` must return:

- service hash;
- controller PID and IP;
- lifecycle epoch;
- HA enabled flag;
- active and pending slots;
- cutover generation and phase; and
- drain start time.

Factor the existing cutover-row validation into one private parser used by both
`get_lb_cutover_state()` and the new combined reader. The parser must preserve
the existing PostgreSQL enforcement, malformed-state exceptions, slot and phase
parsing, lifecycle epoch, and `LbCutoverState` values.

Add a controller helper that compares the snapshot's service hash and owner
tuple with `self._service_hash` and `self._controller_owner`. Inside
`_handle_load_balancer_role()`, after Kubernetes pod authority succeeds and
while holding `_lb_role_lock`, replace `_lb_cutover_fence()` plus
`get_lb_cutover_state()` with that one helper call. If the snapshot is absent,
disabled, owner-mismatched, incarnation-mismatched, epochless, or has no active
slot, return the existing `CUTOVER_STATE_UNAVAILABLE` outcome. Preserve the
existing malformed-state exception and outcome behavior exactly rather than
normalizing it into a new response category.

The initial `_owns_current_service()` read remains the source of the existing
early `CONTROLLER_NOT_OWNER` outcome. The combined in-lock read replaces only
the two adjacent calls that currently produce the fence and cutover state.
Query count falls from five to four per forwarded role heartbeat, about 20
percent.

Do not implement this milestone merely because the query can be removed. After
Milestones 0 through 3, first verify that HA role authority reads remain a
material share of database load. If they do not, defer Milestone 4 and avoid
adding a new snapshot contract to the cutover repository.

Files:

- `sky/serve/lb_cutover_state.py`
- `sky/serve/serve_state.py`
- `sky/serve/controller.py`
- `tests/unit_tests/test_serve_lb_ha.py`
- `tests/unit_tests/test_serve_state.py`
- `tests/unit_tests/test_serve_lb_cutover_state_contract.py`
- `tests/unit_tests/test_serve_controller_proxy.py` as an unchanged-fence
  regression suite
- `docs/designs/api-server-db-load-relief.md`

Focused tests:

- the combined reader executes exactly one `SELECT` and returns the same
  `LbCutoverState` as the legacy reader for every valid phase;
- both readers reject every currently malformed state identically;
- missing service, disabled HA, missing active slot, owner mismatch, service
  replacement, and lifecycle mismatch fail closed with existing outcomes;
- the combined database read remains after Kubernetes authority and inside the
  role lock;
- all role saga success, recovery, cancellation, and compare-and-set rejection
  tests retain their responses and mutation order;
- the proxy still performs exactly two owner reads and rejects an owner change
  after the controller response;
- update the structural digest for `get_lb_cutover_state()` only if factoring
  the shared parser changes its AST, and prove the parser preserves behavior.

## Alternatives considered

### Dedicated expired-claim reaper thread

A dedicated thread would fully remove sweeps from pickup, but adds lifecycle,
leadership, shutdown, and role-routing coordination. A per-backend monotonic
deadline gives the useful reduction with a smaller failure surface.

### Reap only when the queue is empty

A continuously busy queue could then starve expired claims indefinitely. The
independent deadline preserves bounded recovery under both idle and busy load.

### Make idle backoff unbounded or use the generic jittered backoff helper

Unbounded polling delay has no pickup-latency contract. The generic helper adds
jitter after its cap and can exceed the configured bound. A small deterministic
state machine is more appropriate.

### Wake local workers when enqueueing

The request may be enqueued by a different process or pod. A process-local event
would improve only some cases and make latency depend on topology. PostgreSQL
notifications could solve cross-process wakeup, but require reconnect and missed
notification semantics outside this design.

### Compare full serialized replica rows

Serializing JSON and pickle before and after every probe doubles steady
controller serialization work. The probe mutation surface is narrow and can be
guarded explicitly.

### Add a conditional PostgreSQL upsert only

A conditional upsert can avoid some physical updates but still transfers every
row, parses every value, and takes conflict-path locks. It also makes serialized
compatibility repair part of the equality contract. Filtering before the batch
has greater benefit and a clearer behavior boundary.

### Merge the proxy owner reads

The before and after values prove that the non-retried POST stayed on one owner
incarnation. Removing either would accept a response across an ownership change.
They are intentionally retained.

### Remove the initial controller owner read

This would reduce the path to three reads, but it cannot preserve the exact
current response contract. Today an owner mismatch before the handler returns
`CONTROLLER_NOT_OWNER`, while an ownership change after that check but before
the in-lock fence returns `CUTOVER_STATE_UNAVAILABLE`. One later snapshot cannot
distinguish those timings. It would also make a stale controller perform a
Kubernetes authority read before rejection. Defer this until an explicit
response-contract change is accepted.

## Backward compatibility

- Revision 027 adds one backward-compatible managed-jobs index and no column or
  serialized-state change. The remaining milestones add no database migration.
  No milestone changes REST routes, headers, response fields, or public API
  versioning.
- Connection attribution is installed only when the existing
  `SKY_API_SERVER_METRICS_ENABLED=true` contract is active. With metrics
  disabled, there is no listener and `db_utils` does not resolve its metrics
  import. Enabling it adds one bounded counter to the existing metrics endpoint
  without changing pool behavior or requiring an API version bump.
- For handlers understood by both versions, old and new queue consumers can run
  together. Old consumers sweep more often; row locks, lease tokens, execution
  generations, and terminal transitions remain authoritative.
- The dedicated authority-worker routing, private-handler quarantine, and old
  mixed-version gate are retired after the all-status database gate proved no
  matching request exists. The released plugin `claim_scope` argument remains
  as a `GENERAL`-only compatibility shim; the retired authority scope is
  rejected and does not affect queue selection.
- Old and new idle workers can run together. The faster poller may claim first,
  but the PostgreSQL claim transaction prevents duplicate execution.
- During a Serve owner handoff, only the fenced current controller may persist
  probe state. Changed-only persistence changes write frequency, not ownership.
- Old proxy and new controller, or new proxy and old controller, continue using
  the same owner header and role endpoint. The combined reader is internal to
  the new controller process.
- Default queue settings retain the nominal 100 ms behavior. The readiness
  optimization is disabled by default in its first release.

## Rollout and rollback

Each milestone is a separate PR and deployment gate.

### Milestone 0

1. On the exact release candidate, migrate a production-shaped PostgreSQL test
   database from revision 026 and prove the candidate plan uses
   `ix_job_info_schedule_priority` without a `Sort` or outer `Seq Scan`.
2. Build the index concurrently in production through the migration job. If
   Aurora remains saturated, first scale the API deployment to zero while
   leaving existing external load balancers running, then run the migration.
3. Verify revision 027, exact index validity and shape, zero lock waiters, and
   unchanged managed-job state counts before restoring the API deployment.
4. Compare five-minute `job_info` sequential-scan and tuple-read deltas, new
   session rate, Aurora CPU, claim latency, and managed-job outcomes with the
   baseline. The immediate target is to remove about 238 full scans and about
   1.22 million scanned rows per minute.
5. Stop if the migration cannot converge or the plan does not use the index.
   The migration is forward-only; a binary rollback remains compatible with
   the additive index, and a later reviewed migration may remove it if needed.

### Observability gate

1. Deploy the attribution code with API-server metrics disabled and verify the
   API, controllers, session rate, and pool classes remain unchanged.
2. Confirm the canary is the supported monolithic topology: high availability
   is disabled and one API pod owns the executor and controller children. If
   roles are split across pods, stop because the standard metrics flag does not
   yet make their counters visible through the API endpoint. Then enable
   `apiService.metrics.enabled=true` on the API-server replica through Helm.
   Verify that the rendered environment, port 9090, Service port, and scrape
   annotations appear together. Do not assume the annotations prove a scraper
   exists; either verify the installed scraper target or use a controlled
   port-forward scrape.
3. Query:

   ```promql
   sum by (process_role, engine_namespace, mode) (
     rate(sky_postgres_connections_opened_total[5m])
   )
   ```

   Compare its aggregate over one aligned interval with the
   `pg_stat_database.sessions` delta. Investigate any sustained gap before
   changing a pool. A future PgBouncer deployment would change this counter to
   SkyPilot-to-PgBouncer connections and would require separate backend-session
   telemetry. Compare API-process CPU, event-loop lag, request latency, and
   queue wait with the metrics-disabled interval so the counter's own write
   cadence is proven negligible.
4. Inspect Prometheus multiprocess files under `/tmp/metrics` for file count
   and bytes during the canary. Counter files are process-scoped and can remain
   after short-lived children exit until pod restart, so storage growth follows
   unique writer PIDs rather than connection count. Treat any fail-open
   metric-recording warning as an observability-canary failure even though
   database work continues.
5. Roll back by disabling `apiService.metrics.enabled`. Do not change async
   pooling, add PgBouncer, or tune connection limits until the measured role and
   namespace distribution identifies the dominant path.

### Milestone 1

1. Deploy with the environment variable unset and verify nominal cadence and
   expired-claim behavior in a PostgreSQL request-backend environment.
2. Set the interval to 1.0 second on one API-server role only after that role
   uses the PostgreSQL request backend.
3. Observe request queue wait, expired-claim recovery, error rate, and the two
   queue statements in `pg_stat_statements` or Aurora Performance Insights.
4. Expand only if recovery remains bounded and queue latency is unchanged.
5. Roll back cadence exactly by setting zero. Setting 0.1 restores the nominal
   historical idle cadence, and reverting the image removes the feature.

### Milestone 2

1. Deploy with the default 0.1-second cap.
2. Canary 0.5 seconds, then 1.0 second.
3. Gate on `sky_apiserver_queue_wait_seconds`, especially short-request p95 and
   p99, plus request submission-to-running timestamps.
4. Roll back immediately by setting the cap to 0.1.

### Milestone 3

1. Deploy with changed-only persistence disabled.
2. Enable on the single monolithic API pod. The environment variable is
   inherited by every service-controller child, so this canary intentionally
   covers all currently running services rather than claiming a per-service
   rollout that Helm cannot provide.
3. Compare aligned five-minute `replicas.n_tup_upd` and `n_tup_hot_upd`
   deltas, Aurora CPU, WriteIOPS, write throughput, probe-round duration,
   readiness transition latency, teardown classifications, and controller
   errors.
4. Exercise ready, unready, recovery, preemption, initial-delay teardown,
   consecutive-failure teardown, and typed system-OOM recovery transitions.
5. Roll back by setting the kill switch false if any transition is missed,
   teardown classification changes, the LB ready set becomes stale, or probe
   errors increase.

### Milestone 4

1. Confirm after Milestones 0 through 3 that these reads remain material. Defer
   the milestone if they do not.
2. Compare role outcome rates, phase timings, controller query count, cutover
   progression, and owner-change rejection.
3. Exercise a controller ownership handoff and every cutover phase.
4. Roll back with a normal image revert. No data rollback is needed.

## Success and rollback gates

| Milestone | Success | Roll back or stop |
|---|---|---|
| 0 | Idle `job_info` scans fall by about 238/min and tuple reads fall by about 1.22 million/min; claim order and outcomes are unchanged. | Migration or index shape is invalid, the candidate plan retains an outer sequential scan or sort, lock waiters appear, or managed-job behavior differs. |
| Attribution gate | The bounded counter's aligned aggregate explains the material share of `pg_stat_database.sessions`, every emitted label is in its allowlist, and pool behavior plus API CPU and latency are unchanged. | Listener or metric appears while disabled, unknown labels grow, metric recording warns, the aggregate materially diverges without explanation, `/tmp/metrics` grows without a safe bound, API CPU or latency materially regresses, or API/controller behavior changes. |
| 1 | In a PostgreSQL request-backend deployment, empty-queue reads fall from about 40 to about 22 `SELECT`s/s at a 1-second interval; queue latency is unchanged. | An under-cap expiry set recovers later than interval plus normal scheduling tolerance, a capped backlog stops draining immediately, replay outcome changes, or database errors increase. |
| 2 | Combined with 1, empty-queue reads approach 4 `SELECT`s/s at a 1-second cap. | Short-request queue-wait p99 breaches its service objective or shutdown becomes slower. |
| 3 | Stable ordinary replicas approach zero readiness-generated row updates while readiness and typed system-recovery transition counts and outcomes match baseline. | Missed readiness or recovery transition, wrong teardown classification, stale LB ready set, or probe-round error increase. |
| 4 | Role heartbeat authority reads fall from 5 to 4 with identical outcomes. | Any owner mismatch, malformed-state, or cutover outcome differs. |

## Verification plan

Run the narrow tests for each PR, then the combined suite on the exact pushed
SHA. PostgreSQL tests require `SKYPILOT_TEST_POSTGRES_URL` or their repository
fixture environment.

```bash
pytest -n 0 tests/unit_tests/test_batch_recovery.py -k 'schema_027 or waiting_job'
pytest -n 0 tests/unit_tests/test_sky/utils/test_db_utils.py
pytest -n 0 tests/unit_tests/test_parent_death_watchdog.py
pytest -n 0 tests/unit_tests/test_api_requests_pg.py -k 'expired or reap or queue'
pytest -n 0 tests/unit_tests/test_sky/server/requests/test_executor.py
pytest -n 0 tests/unit_tests/test_probe_round_batching.py
pytest -n 0 tests/unit_tests/test_serve_replica_managers.py -k 'probe or readiness or preemption'
pytest -n 0 tests/unit_tests/test_serve_probe_failure_window.py
pytest -n 0 tests/unit_tests/test_serve_system_oom_recovery.py
pytest -n 0 tests/unit_tests/test_serve_system_recovery_route_lease.py
pytest -n 0 tests/unit_tests/test_serve_lb_ha.py
pytest -n 0 tests/unit_tests/test_serve_state.py -k 'BatchReplicaUpsert'
pytest -n 0 tests/unit_tests/test_serve_controller_proxy.py
pytest -n 0 tests/unit_tests/test_serve_lb_cutover_state_contract.py
```

Format only the files changed by the milestone, then run the relevant Serve and
API-server component suites selected by the final diff.

Manual verification:

1. Record five-minute `pg_stat_user_tables` deltas for `job_info`, run the exact
   candidate `EXPLAIN`, apply revision 027, and repeat both measurements.
2. With metrics disabled, confirm engine pool types and the absence of the new
   listener remain unchanged. Confirm high availability is disabled and all
   database-owning child roles share the API pod. Enable metrics on that
   replica, scrape the new counter, compare its aligned rate with
   `pg_stat_database.sessions`, and inspect `/tmp/metrics` count and bytes
   before and after representative executor and controller activity.
3. Record a five-minute idle baseline for queue statement count and queue-wait
   histograms, enable Milestones 1 and 2 independently, and repeat in a
   PostgreSQL request-backend environment.
4. Seed one expired read-only claim and one expired mutating claim. Confirm the
   former receives a new generation and token, while the latter becomes an
   ambiguous cancelled request with `should_retry` set.
5. Run a sustained nonempty queue and prove there is no idle sleep and no
   throughput regression.
6. Across all running services in the monolithic API pod, record replica update
   and HOT-update deltas for ten probe rounds, enable Milestone 3, then force
   ready-to-unready, recovery, preemption, teardown, and typed system-OOM
   transitions.
7. In HA, record one stable heartbeat, each cutover phase, a controller owner
   transfer during a heartbeat, and the exact database statement count before
   and after Milestone 4.

## Changed-path-to-test matrix

| Changed path | Responsibility | Required verification |
|---|---|---|
| `sky/jobs/state_schema.py` and spot-jobs revision 027 | ordered waiting-job access path | SQLite and PostgreSQL migration shape, interrupted-build repair, live plan and scan-rate canary |
| `sky/utils/db/migration_utils.py` | managed-jobs schema target | fresh bootstrap and 026-to-027 upgrade tests |
| `sky/metrics/utils.py` | bounded successful-physical-connection counter | metrics registry and multiprocess collection tests, closed-label review |
| `sky/utils/db/db_utils.py` | one listener per PostgreSQL engine and process-role resolution | disabled-path import test, sync/async and pool reuse tests, cache-hit deduplication, namespace and role fallbacks |
| `sky/server/requests/executor.py`, `sky/jobs/controller.py`, and `sky/serve/controller.py` | exact child-process role override before database initialization | entrypoint ordering tests and existing executor/controller lifecycle suites |
| `sky/server/requests/postgres.py` | expired-claim cadence while preserving queue fencing | PostgreSQL request tests, mixed-consumer race, statement-count observation |
| `sky/server/requests/executor.py` | bounded idle polling and interruptible shutdown | request executor tests, deterministic backoff tests, queue-wait canary |
| `sky/serve/replica_managers.py` | changed-only ordinary readiness bookkeeping while preserving typed system-OOM recovery | probe transition, preemption, recovery, and route-suspension tests; two-service large-fleet write-count canary |
| `sky/serve/lb_cutover_state.py` | one-query typed authority snapshot and shared validation | cutover repository contract, valid and malformed state parity, query-count test |
| `sky/serve/serve_state.py` | historical facade for the combined reader | Serve state and direct alias tests |
| `sky/serve/controller.py` | in-lock combined fence and staged redundant-read removal | full LB HA saga, cancellation, ownership handoff, query ordering |
| `tests/unit_tests/test_serve_controller_proxy.py` | unchanged temporal proxy fence | two-read and changed-owner regression tests |
| `docs/designs/api-server-db-load-relief.md` | canonical behavior and rollout contract | exact-design review before every milestone |
