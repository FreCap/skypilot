# Managed jobs log-follow status snapshot

## Behavior contract

`stream_logs_by_id()` uses `get_latest_task_id_status()` to wait for the first
initialized lifecycle status. Once active log following starts, the unfiltered
path uses `get_latest_log_stream_snapshot()` as the single source for the
latest task, lifecycle status, and routing fields. Those values must not be
read in separate sessions because the reads can observe different recovery
epochs. The remote-log loop retains its existing scalar status refreshes after
tail attempts.

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
tail attempt. Active JobGroup transitions use the existing managed-job polling
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
read instead of two back-to-back reads. Initial status discovery, task counts,
polling cadence, and scalar status refreshes after remote tail attempts are
unchanged. Integer task-ID validation changes from an O(tasks) inventory read
to one O(1) point lookup. Only a missing task-ID adds one O(1) aggregate count;
it does not materialize task status rows.

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

## Rollout and rollback

This changes only process-local read coordination. It adds no schema, API, or
persisted-state change. Rollback is a code rollback.

## Test plan

`tests/unit_tests/test_sky/jobs/test_log_follow_lifecycle.py` covers initial
status polling, no scalar status reads, active follow task selection, immediate
terminal transitions, terminal transitions after a `None` status, stale
initial task inventories, filtered task validation, exact query-call counts,
post-wait snapshot reuse for recovered targets, and remote log-call counts.

`tests/unit_tests/test_sky/jobs/test_utils.py` covers integer and name filters,
missing tasks, missing jobs, and exact read counts.
`tests/unit_tests/test_sky/jobs/test_status_refresh_snapshot.py` proves task
counting uses one aggregate query without materializing status rows.
Pull-request CI runs all three under `Python Tests - Unit Tests`;
`Python Tests - Jobs & API Tests` and
`Python Tests - Limited Deps - Jobs, Serve & CLI (3.14)` cover the broader
managed-jobs interface. Formatting, mypy, Pylint, and static-analysis workflows
cover the changed Python paths.

## Changed-path-to-test matrix

| Changed path | Invariants | Concrete tests and commands |
| --- | --- | --- |
| `sky/jobs/log_streaming.py` | Exact integer task filters avoid whole-task scans; missing jobs and missing tasks remain distinct; terminal and cancelling snapshots stop without handle lookup; same-task snapshots keep the existing polling cadence. | `tests/unit_tests/test_sky/jobs/test_utils.py` task-filter cases and `tests/unit_tests/test_sky/jobs/test_log_follow_lifecycle.py` filtered lifecycle cases; run both focused files, then `pytest -n 0 --dist no tests/unit_tests/test_sky/jobs/`. |
| `sky/jobs/state.py` | Task counting is one aggregate query and does not materialize status rows. | `tests/unit_tests/test_sky/jobs/test_status_refresh_snapshot.py::TestGetJobsToCheckStatusInfo::test_get_num_tasks_uses_one_count_select`. |
| `docs/designs/managed-jobs-log-follow-status-snapshot.md` | The lifecycle, failure, concurrency, and performance contracts remain synchronized with the implementation. | The focused task-filter and lifecycle tests above plus the one-SQL count assertion. |
