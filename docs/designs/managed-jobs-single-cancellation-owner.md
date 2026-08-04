# Managed Jobs Single-Delivery Cancellation Ownership

_Created: 2026-07-29_

## Problem

`ControllerManager.run_job_loop()` directly awaits `_run_job_loop()`. The
first cancellation enters `_run_job_loop()`'s cancellation handler, which
captures logs and drives durable cleanup. A later cancellation is delivered
to the same coroutine while that finalization is still running. It can skip
the remaining finalization and reach the outer ownership release while log
capture, resource cleanup, terminal state, or scheduler release is incomplete.

The exact `origin/improvements` baseline at
`f06bf706dcfe78ac1fa0ac8d3c2b29d6ba9a589f` reproduces the failure: a second
cancellation during a blocked finalizer ends the owner and releases
bookkeeping without letting the finalizer complete.

## Goals

One cancellation request must reach the inner job loop. Once accepted, later
cancellation requests must not interrupt its cancellation finalization. The
outer owner must stay alive until the inner task reaches a terminal result,
then preserve that result and release launch and job bookkeeping.

Normal completion, inner exceptions, and inner self-cancellation must retain
their current result propagation. The change must add no database query,
provider call, retry, timer, poll, or per-iteration work.

## Background

`start_job()` creates the outer `run_job_loop()` owner. `_run_job_loop()` later
publishes the workload controller task in `job_tasks`, and cancellation signal
delivery cancels that published task. The outer owner still controls the
complete lifecycle and owns the final bookkeeping release.

`asyncio.shield()` prevents cancellation from automatically propagating from
an awaiting owner into a child task. It does not suppress the owner's
`CancelledError`, so the owner can deliver one explicit cancellation to the
child and then keep waiting through later owner cancellation requests.
Child tasks inherit the outer task's context variables.

## Solution

Create one inner task for `_run_job_loop()` inside `run_job_loop()` and await it
through `asyncio.shield()`. On the first outer cancellation, cancel the inner
task exactly once. Continue shielded waits while the inner task finalizes.
Later outer cancellations are recorded by asyncio but are not forwarded.

After the inner task finishes, retrieve its result so normal return, failure,
or cancellation remains authoritative. Keep the existing outer `finally`
release unchanged so bookkeeping is released only after the inner task has
finished.

The owner state is local to one job-loop invocation:

1. Create exactly one inner task.
2. Await it shielded.
3. If the owner is cancelled and the inner task is still live, cancel the
   inner task only if cancellation has not already been delivered.
4. Resume the shielded wait until the inner task is done.
5. Retrieve the inner result and release outer ownership.

## Changed-Path-to-Test Matrix

| Changed production path | Invariant | Test file and case | Local command | CI job |
| --- | --- | --- | --- | --- |
| `sky/jobs/controller.py::ControllerManager.run_job_loop` | Repeated owner cancellation reaches the inner job loop once, cannot interrupt its finalizer, and cannot release ownership early | `tests/unit_tests/test_sky/jobs/test_controller.py::TestRunJobLoopOwnershipCleanup::test_repeated_cancellation_waits_for_inner_finalization` | `pytest -q tests/unit_tests/test_sky/jobs/test_controller.py::TestRunJobLoopOwnershipCleanup` | `Python Tests - Unit Tests` |
| `sky/jobs/controller.py::ControllerManager.run_job_loop` | Normal completion and inner failure remain authoritative, and ownership releases exactly once after the inner result | Existing `TestRunJobLoopOwnershipCleanup` success, initialization-failure, and cleanup-failure cases plus the new finalization-order assertions | `pytest -q tests/unit_tests/test_sky/jobs/test_controller.py::TestRunJobLoopOwnershipCleanup` | `Python Tests - Unit Tests` |
| `sky/jobs/controller.py::ControllerManager.run_job_loop` | The owner creates exactly one O(1) inner task and adds no query, retry, timer, poll, provider call, or loop proportional to job size | `tests/unit_tests/test_sky/jobs/test_controller.py::TestRunJobLoopOwnershipCleanup::test_repeated_cancellation_waits_for_inner_finalization` task-creation and cancel-count assertions | `pytest -q tests/unit_tests/test_sky/jobs/test_controller.py::TestRunJobLoopOwnershipCleanup` | `Python Tests - Unit Tests` |
| Managed-jobs cancellation and controller integration | Cancellation signal delivery, terminal transition, scheduler release, and controller lifecycle remain compatible | `tests/unit_tests/test_sky/jobs/test_controller.py` and `tests/unit_tests/test_batch_recovery.py` | `pytest -q tests/unit_tests/test_sky/jobs/test_controller.py tests/unit_tests/test_batch_recovery.py` | `Python Tests - Unit Tests`; `Python Tests - Jobs & API Tests` |

`.github/workflows/pytest.yml` includes `tests/unit_tests` in the unit-test
matrix without a changed-path exclusion. The broader Jobs and API inventory
also exercises managed-jobs controller behavior. Static workflows cover
formatting, mypy, Pylint, Ruff, basedpyright, async lifecycle, and import
contracts for the changed Python path.

## Performance

The steady job lifecycle adds one `asyncio.Task` and constant-size local state
per managed job. Cancellation adds one explicit `Task.cancel()` and constant
time shielded waiter operations per cancellation request. There is no new I/O
and no loop proportional to DAG size, task count, cluster count, or retry
count. The regression test will assert exactly one inner task creation and one
delivered inner cancellation even when the owner is cancelled repeatedly.

This constant-work ownership cost replaces an unsafe direct await only at the
long-lived job-controller boundary. It does not change scheduling, monitoring,
status polling, log download, or cleanup bodies.

## Alternatives Considered

Decorating cancellation cleanup with `asyncio_utils.shield` would let cleanup
continue in the background after the owner exits. That would still release
ownership before durable finalization finishes and could overlap a replacement
owner, so it does not satisfy the invariant.

Shielding `_run_job_loop()` without explicitly delivering cancellation would
make managed-job cancellation ineffective. Forwarding every outer
cancellation would preserve the current bug. Moving all cancellation and
cleanup code into a new state machine would be a larger carrying cost than the
single owner boundary.

## Rollout and Verification

The change is internal and requires no data migration or compatibility layer.
Before merge, prove the new test fails on the exact parent and passes on the
head, run the focused ownership class, the full controller and batch recovery
suites, repository formatting and static checks, and `git diff --check`.
Require the full relevant CI rollup and review state on the exact pushed SHA.
