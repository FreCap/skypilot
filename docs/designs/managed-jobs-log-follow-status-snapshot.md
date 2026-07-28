# Managed jobs log-follow status snapshot

## Behavior contract

`stream_logs_by_id()` uses `get_latest_task_id_status()` as the single source
for both initial lifecycle status and the task selected when active log
following starts. It must not read the scalar job status and then independently
read the latest task at that boundary, because those reads can observe
different recovery epochs. The later remote-log loop retains its existing
scalar status refreshes.

When a task filter is present, the follower first reads the task inventory so
an invalid task can fail immediately without waiting for job initialization.
The inventory also supplies the task count and avoids a separate count query.

An initial task inventory is never authoritative after a terminal latest-task
status is observed. The inventory read precedes the status read, so a job can
become terminal between them without passing through an observed `None`
status. The terminal path therefore refreshes task metadata once before
reading log paths, cleanup timestamps, and final task statuses.

## Lifecycle and liveness

An uninitialized latest-task status is polled once per second until it becomes
non-null. Cancellation and terminal statuses stop active following through the
existing `_should_keep_logging()` policy. The remote-log loop continues to
refresh scalar status after each tail attempt. Active JobGroup transitions
continue to use `_wait_for_next_task()` and the existing managed-job polling
interval.

Filtered and unfiltered terminal jobs preserve the existing final log and exit
code behavior. A terminal transition that occurs before, during, or after the
initial status wait cannot publish stale task metadata. Invalid task filters
still return before the initial status wait.

## Performance contract

For an unfiltered active job, initial database reads fall from a task count,
scalar job status, and latest-task status to a task count and latest-task
status. For a filtered active job, reads fall from task count, task inventory,
scalar job status, and latest-task status to task inventory and latest-task
status.

Terminal paths perform at most one final inventory refresh. Filtered terminal
paths still save the separate task-count read, while unfiltered terminal paths
retain the original read count. Polling cadence, asymptotic work, remote
backend calls, threads, and timers are unchanged.

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
and remote log-call counts.

`tests/unit_tests/test_sky/jobs/test_utils.py` covers the adjacent task-filter
surface. Pull-request CI runs both under `Python Tests - Unit Tests`;
`Python Tests - Jobs & API Tests` and
`Python Tests - Limited Deps - Jobs, Serve & CLI (3.14)` cover the broader
managed-jobs interface. Formatting, mypy, Pylint, and static-analysis workflows
cover the changed Python paths.
