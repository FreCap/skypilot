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

When a task filter is present, the follower first reads the task inventory so
an invalid task can fail immediately without waiting for job initialization.
The inventory also supplies the task count and avoids a separate count query.
The active filtered path keeps the existing split reads: latest lifecycle
status still comes from the job-level reducer, while routing must use the
explicitly selected task rather than the latest task.

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
filtered-task reads, polling cadence, and scalar status refreshes after remote
tail attempts are unchanged.

Waiting for the next JobGroup task performs one combined database read per poll
cycle. The successful handoff reuses that read for routing, reducing the
transition from N scalar polls plus one combined routing read to N combined
polls. The query count therefore falls by one at every observed task handoff,
while polling cadence and asymptotic work remain unchanged.

Terminal paths perform at most one final inventory refresh. Filtered terminal
paths still avoid the separate task-count read. Polling cadence, asymptotic
work, remote backend calls, threads, and timers are unchanged.

## Alternatives

Reusing the initial filtered inventory for an immediately terminal status is
unsafe because the two reads do not share a transaction or generation token.
Reordering the status wait before task validation would make invalid filters
wait behind job initialization. Deriving the latest job status from the task
inventory would duplicate the state module's lifecycle reducer.

## Rollout and rollback

This changes only process-local read coordination. It adds no schema, API, or
persisted-state change. Rollback is a code rollback.

## Test plan

`tests/unit_tests/test_sky/jobs/test_log_follow_lifecycle.py` covers initial
status polling, no scalar status reads, active follow task selection, immediate
terminal transitions, terminal transitions after a `None` status, stale
initial task inventories, filtered task validation, exact query-call counts,
post-wait snapshot reuse for recovered targets, and remote log-call counts.

`tests/unit_tests/test_sky/jobs/test_utils.py` covers the adjacent task-filter
surface. Pull-request CI runs both under `Python Tests - Unit Tests`;
`Python Tests - Jobs & API Tests` and
`Python Tests - Limited Deps - Jobs, Serve & CLI (3.14)` cover the broader
managed-jobs interface. Formatting, mypy, Pylint, and static-analysis workflows
cover the changed Python paths.

## Changed-path-to-test matrix

| Changed path | Invariants | Concrete tests and commands |
| --- | --- | --- |
| `sky/jobs/utils.py` | The next JobGroup task and its log target come from one recovery snapshot; terminal and cancelling snapshots stop without handle lookup; same-task snapshots keep the existing polling cadence; a fast following transition cannot replace the detected task; each wait poll is one SQL-backed snapshot and the handoff adds no extra read. | `tests/unit_tests/test_sky/jobs/test_log_follow_lifecycle.py`, especially `TestWaitForNextTask` and the fast-transition regression; run the focused lifecycle file, then `pytest -n 0 --dist no tests/unit_tests/test_sky/jobs/`. |
| `docs/designs/managed-jobs-log-follow-status-snapshot.md` | The lifecycle, failure, concurrency, and performance contracts remain synchronized with the implementation. | The lifecycle tests above plus the one-SQL snapshot assertions in `tests/unit_tests/test_sky/jobs/test_state.py`; run both files together before the component suite. |
