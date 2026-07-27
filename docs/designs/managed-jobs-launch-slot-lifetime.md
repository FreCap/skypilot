# Managed jobs launch-slot lifetime

## Behavior contract

The consolidated jobs controller limits concurrent initial cluster launches
with a process-local `starting` set guarded by one `asyncio.Lock` and signaled
by one `asyncio.Condition`. Membership represents active initial launch work,
not the lifetime of a managed job.

`ControllerManager.start_job()` preclaims membership before handing execution
to the job coroutine. `scheduler.scheduled_launch()` must treat that job ID as
already admitted, including when its entry fills `LAUNCHES_PER_WORKER`.
Unclaimed later launches and recoveries must still atomically wait for a free
slot and add their job ID under the same lock.

Each claimed job releases its manager-owned entry at the first durable boundary
where no initial launch remains:

1. A normal dedicated-cluster task completes the durable launch transition in
   `scheduler.scheduled_launch()`. Its later STARTED publication observes an
   already released slot.
2. A pool task publishes STARTED after its initial assignment.
3. A Batch coordinator publishes STARTED before its coordinator loop begins.
4. A JobGroup completes its initial launch barrier and publishes STARTED for
   every freshly launched task before networking and monitoring begin.
5. A resumed execution shape that needs no fresh launch releases at the same
   boundary without manufacturing a launch.
6. Any exception or cancellation before that boundary leaves release to
   `ControllerManager.run_job_loop()`'s outer ownership cleanup.

Release is idempotent because alternate success, failure, cancellation, and
outer-cleanup paths can converge. It runs under the shared lock, notifies a
capacity waiter only when membership was removed, and completes under repeated
cancellation.

## Lifecycle and liveness

With `LAUNCHES_PER_WORKER = 1`, the manager's preclaimed job must enter
`scheduled_launch()` rather than waiting on its own membership. A long-running
Batch coordinator or JobGroup must not prevent the monitor loop from claiming
a second waiting job after the first shape has finished durable admission. The
first job continues its orchestration, networking, and monitoring after its
manager entry is removed.

Later single-task recovery attempts continue to use
`scheduler.scheduled_launch()`. The new boundary does not change durable
schedule states, retry backoff, cancellation delivery, cluster cleanup, or
recovery admission.

If a state transition, launch, or barrier snapshot fails before the release
boundary, the manager keeps ownership until the job loop's existing outer
cleanup completes. This prevents an in-flight or partially durable launch from
freeing capacity early. If cancellation lands while release is waiting for the
shared lock, release finishes before cancellation propagates.

## Performance contract

Release performs one O(1) set membership check, at most one O(1) removal, and
at most one condition notification. It adds no database query, provider call,
network request, timer, polling loop, background task, or collection scan.

The deterministic performance regression tests are admission based: a
preclaimed one-slot launch enters without waiting, and while the first Batch or
JobGroup remains long-running, its manager launch-slot count is zero and a
condition waiter wakes without a polling sleep. Before this change, the
preclaimed final slot waits on itself and bypass paths retain the count until
the whole job exits.

## Alternatives

Immediate release in `ControllerManager.start_job()` would allow unbounded
initial launch fanout. Per-shape inline set manipulation would preserve
duplicated ownership logic and make cancellation behavior diverge. A new
semaphore would duplicate the existing shared scheduler primitive and require
cross-cutting recovery changes.

## Rollout and rollback

This is process-local lifecycle coordination with no schema, API, or persisted
state change. Deployment replaces controller workers normally. Rollback is a
code rollback and does not require data migration.

## Test plan

Focused unit tests cover preclaimed and unclaimed scheduler admission, fresh
and resumed Batch coordinators, fresh and resumed JobGroups, durable-state
ordering, long-running phases, waiter
notification, idempotent release, repeated cancellation, launch failure,
barrier failure, and manager fallback cleanup. The relevant controller,
scheduler, Batch recovery, and managed-jobs integration inventories must pass,
along with repository Python formatting and static checks.
