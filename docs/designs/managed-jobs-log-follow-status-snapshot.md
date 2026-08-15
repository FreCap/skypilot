# Managed jobs log-follow status snapshot

## Behavior contract

`stream_logs_by_id()` waits for the first initialized lifecycle status on the
same log-target snapshot it will follow, so startup performs no separate scalar
latest-task read. Once active log following starts, the unfiltered path uses
`get_latest_log_stream_snapshot()` as the single source for the latest task,
lifecycle status, and routing fields. Those values must not be read in separate
sessions because the reads can observe different recovery epochs. The
remote-log loop retains its existing scalar status refreshes after tail
attempts.

Snapshot carrying is an optimization, never a precondition. Each loop iteration
consumes and clears the snapshot it selected a target from. An iteration that
begins with no carried snapshot must read a fresh one before target selection.
Two re-entries reach the loop top without a replacement: a broken or preempted
remote tail falling through the bottom of the loop, and a failed task waiting
on its configured restart. Following must resume on both.

When the target log is unavailable, the follower waits for the existing poll
interval and reads one new combined snapshot. That post-wait snapshot is
carried into the next loop iteration. It must not be discarded and immediately
reread before target selection.

When an active JobGroup task finishes, the follower waits for the next task by
polling the same combined snapshot. The snapshot that first observes a new task
is carried into the next loop iteration. Task detection and log-target
selection must not be split across two reads because a fast subsequent task can
otherwise replace the detected task before its logs are tailed.

When a task-name filter is present, the follower first reads the task inventory
so an invalid name can fail immediately without waiting for job initialization.
An integer task-ID filter instead reads the exact task row. A hit never
materializes the other task rows; a miss performs one aggregate count so a
missing job remains distinguishable from a missing task and the valid-ID hint
stays available. The active filtered path keeps split reads: lifecycle and
routing come from the explicitly selected task snapshot rather than the latest
whole-job task.

The `spot` schema indexes but does not uniquely constrain
`(spot_job_id, task_id)`, so an exact identity can retain stale rows from an
older incarnation. Task-filtered log lookups must collapse that identity to one
coherent row before materialization. Any non-terminal row wins over terminal
duplicates so a stale completion cannot stop an active retry. Within the
selected active or terminal class, the greatest surrogate `spot.job_id` wins,
so status, task name, routing, log path, and cleanup timestamp all describe the
same newest incarnation. Numeric and task-name filters use the same duplicate
policy after resolving the logical task ID.

An initial task inventory is never authoritative after a terminal latest-task
status is observed. The inventory read precedes the status read, so a job can
become terminal between them without passing through an observed `None`
status. The terminal path therefore refreshes task metadata once before
reading log paths, cleanup timestamps, and final task statuses.

## Lifecycle and liveness

An uninitialized latest-task status is polled once per second until it becomes
non-null. Cancellation and terminal statuses stop active following through the
existing `_should_keep_logging()` policy. A terminal status observed by the
post-wait snapshot stops before handle lookup or remote tailing. A recovered
runnable target observed by that snapshot is used directly on the next
iteration. The remote-log loop continues to refresh scalar status after each
tail attempt. When such a refresh keeps the job non-terminal - the cluster was
preempted, the tail died, or the task is restarting - the follower re-reads the
log target and keeps following the new incarnation. It must not treat a cleared
snapshot as an invariant violation. Active JobGroup transitions use the existing managed-job polling
interval, and cancellation or terminal snapshots stop without an additional
routing read.

Filtered and unfiltered terminal jobs preserve the existing final log and exit
code behavior. A terminal transition that occurs before, during, or after the
initial status wait cannot publish stale task metadata. Invalid task filters
still return before the initial status wait.

## Performance contract

For an unfiltered active job, one combined query selects latest-task status and
routing fields from one recovery snapshot. When the log target remains
unavailable across a poll interval, the post-wait snapshot serves the next
target-selection iteration, so each poll cycle performs one combined database
read instead of two back-to-back reads. Task counts, polling cadence, and
scalar status refreshes after remote tail attempts are unchanged. Startup
discovery drops its separate scalar latest-task read and reuses the first
snapshot it waited on. The re-read after a broken tail or a task restart costs
one combined query per recovery, on a path that already sleeps at least one
managed-job poll interval. Integer task-ID validation changes from an O(tasks) inventory read
to one O(1) point lookup. Only a missing task-ID adds one O(1) aggregate count;
it does not materialize task status rows.

Duplicate collapse stays inside the same statement and returns exactly one
row. It scans only rows sharing the selected indexed task identity and adds no
poll, backend call, session, thread, or timer. Valid unique identities retain
the existing one-row query shape and result.

Waiting for the next JobGroup task performs one combined database read per poll
cycle. The successful handoff reuses that read for routing, reducing the
transition from N scalar polls plus one combined routing read to N combined
polls. The query count therefore falls by one at every observed task handoff,
while polling cadence and asymptotic work remain unchanged.

Terminal paths perform at most one final metadata refresh. A found integer
task-ID path avoids both the inventory and count reads. Polling cadence, remote
backend calls, threads, and timers are unchanged.

## Alternatives

Reusing initial filtered metadata for an immediately terminal status is unsafe
because the reads do not share a transaction or generation token. Reordering
the status wait before task validation would make invalid filters wait behind
job initialization. A point lookup alone cannot distinguish a missing task
from a missing job, so the miss-only aggregate count preserves that boundary
without restoring the whole-task scan.

Treating `(spot_job_id, task_id)` as unique is unsafe because the schema only
provides a non-unique index and older task rows are compatibility data. A plain
`fetchone()` makes insertion order user-visible. Returning every duplicate
would restore the pre-optimization row volume and mix lifecycle incarnations;
selecting one preferred surrogate row preserves both bounded reads and coherent
metadata.

## Rollout and rollback

This changes only process-local read coordination. It adds no schema, API, or
persisted-state change. Rollback is a code rollback.

## Test plan

`tests/unit_tests/test_sky/jobs/test_log_follow_lifecycle.py` covers initial
status polling, no scalar status reads, active follow task selection, immediate
terminal transitions, terminal transitions after a `None` status, stale
initial task inventories, filtered task validation, exact query-call counts,
post-wait snapshot reuse for recovered targets, remote log-call counts, and
resumed following after a broken tail or a failed-task restart.

`tests/unit_tests/test_sky/jobs/test_utils.py` covers integer and name filters,
missing tasks, missing jobs, and exact read counts.
`tests/unit_tests/test_sky/jobs/test_status_refresh_snapshot.py` proves task
counting uses one aggregate query without materializing status rows.
`tests/unit_tests/test_sky/jobs/test_state.py` covers both duplicate insertion
orders, active-over-terminal precedence, newest-terminal selection, coherent
row metadata, one-row materialization, and one-statement query counts for the
legacy terminal helper and the active log lookup gateway. PostgreSQL dialect
compilation pins the same selection shape across supported databases.
Pull-request CI runs all three under `Python Tests - Unit Tests`;
`Python Tests - Jobs & API Tests` and
`Python Tests - Limited Deps - Jobs, Serve & CLI (3.14)` cover the broader
managed-jobs interface. Formatting, mypy, Pylint, and static-analysis workflows
cover the changed Python paths.

## Changed-path-to-test matrix

| Changed path | Invariants | Concrete tests and commands |
| --- | --- | --- |
| `sky/jobs/log_streaming.py` | Exact integer task filters avoid whole-task scans; missing jobs and missing tasks remain distinct; terminal and cancelling snapshots stop without handle lookup; same-task snapshots keep the existing polling cadence; an iteration entered without a carried snapshot re-reads the target instead of failing. | `tests/unit_tests/test_sky/jobs/test_utils.py` task-filter cases and `tests/unit_tests/test_sky/jobs/test_log_follow_lifecycle.py` filtered lifecycle cases plus `test_broken_tail_refetches_routing_snapshot` and `test_failed_task_restart_refetches_routing_snapshot`; run both focused files, then `pytest -n 0 --dist no tests/unit_tests/test_sky/jobs/`. |
| `sky/jobs/state.py` | Task counting is one aggregate query; the legacy exact-task terminal helper prefers an active duplicate and otherwise selects the newest terminal row. | `tests/unit_tests/test_sky/jobs/test_status_refresh_snapshot.py::TestGetJobsToCheckStatusInfo::test_get_num_tasks_uses_one_count_select` and the duplicate exact-task lookup cases in `tests/unit_tests/test_sky/jobs/test_state.py`. |
| `sky/jobs/state_task_lookups.py` | Numeric and name-filtered log lookups collapse duplicate task identities to one coherent preferred incarnation without another statement. | Duplicate insertion-order, newest-terminal, one-row, one-query, and PostgreSQL compilation cases in `tests/unit_tests/test_sky/jobs/test_state.py`; run the focused state and log-follow files, then `pytest -n 0 --dist no tests/unit_tests/test_sky/jobs/`. |
| `docs/designs/managed-jobs-log-follow-status-snapshot.md` | The lifecycle, failure, concurrency, and performance contracts remain synchronized with the implementation. | The focused task-filter and lifecycle tests above plus the one-SQL count assertion. |
