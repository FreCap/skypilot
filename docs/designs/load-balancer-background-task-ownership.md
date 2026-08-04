# Load balancer background task ownership

_Created: 2026-07-29_

## Problem

`SkyServeLoadBalancer` has one helper for owning fire-and-forget tasks, but its
three process-lifetime loops bypass that helper during FastAPI startup and
append tasks directly to `_background_tasks`. Completed startup tasks therefore
remain strongly referenced for the rest of the process. An unexpected loop
failure is not retrieved or reported while that reference remains, so a
controller-sync, HA-role, or occupancy loop can stop without a deterministic
event-loop failure report.

The duplicate ownership paths also make lifecycle behavior depend on where a
task was created. Cancellation fallback tasks are removed and consumed by
`_retain_background_task`; startup loops are not.

## Goal and behavior contract

The load balancer must have one ownership path for every background task.

1. Startup creates exactly one controller-sync loop, one HA-role loop, and one
   occupancy-probe loop.
2. Each task remains strongly referenced while pending.
3. Normal completion removes the task.
4. Cancellation removes the task without reporting an error.
5. Unexpected failure removes the task and sends its exception to the running
   event loop's exception handler immediately.
6. Startup adds no request, network call, timer, retry, poll, or steady-state
   scan. Completion handling is constant-time and runs once per task.

## Solution

Add a small `_start_background_loops` lifecycle boundary that creates the three
existing loops and delegates ownership to `_retain_background_task`. Store
owned tasks in a set so both ownership and release are O(1). Update the existing
ownership callback to retrieve terminal failures and route them through
`loop.call_exception_handler`, following the repository's established
shielded-task ownership contract in `sky.utils.asyncio_utils`.

FastAPI startup calls the new boundary after configuring the access logger.
The loop implementations, scheduling order, retry behavior, cancellation
behavior, and request hot paths remain unchanged.

## Alternatives considered

Keeping direct list appends preserves the current silent retention and leaves
two ownership mechanisms.

Adding three bespoke callbacks duplicates lifecycle logic and can drift again.

Replacing FastAPI startup with an `asyncio.TaskGroup` would broaden shutdown
semantics and require the startup context to remain open for the process
lifetime. That is unnecessary for this bounded correction.

## Changed-path-to-test matrix

| Changed production path | Invariant | Test path | Local command | CI job |
|---|---|---|---|---|
| `sky/serve/load_balancer.py::_start_background_loops` | Exactly three existing loops start once and stay owned while pending | `tests/unit_tests/test_serve_load_balancer_rollout.py::test_background_loops_are_owned_until_completion` | `pytest -q tests/unit_tests/test_serve_load_balancer_rollout.py::test_background_loops_are_owned_until_completion` | `Python Tests - Unit Tests` |
| `sky/serve/load_balancer.py::_retain_background_task` | Normal completion removes ownership | `tests/unit_tests/test_serve_load_balancer_rollout.py::test_background_loops_are_owned_until_completion` | same command | `Python Tests - Unit Tests` |
| `sky/serve/load_balancer.py::_retain_background_task` | Unexpected failure is reported once and released | `tests/unit_tests/test_serve_load_balancer_rollout.py::test_background_loop_failure_is_reported_and_released` | `pytest -q tests/unit_tests/test_serve_load_balancer_rollout.py::test_background_loop_failure_is_reported_and_released` | `Python Tests - Unit Tests` |
| `sky/serve/load_balancer.py::_retain_background_task` | Cancellation is quiet and releases ownership | `tests/unit_tests/test_serve_load_balancer_rollout.py::test_background_loop_cancellation_is_quiet_and_released` | `pytest -q tests/unit_tests/test_serve_load_balancer_rollout.py::test_background_loop_cancellation_is_quiet_and_released` | `Python Tests - Unit Tests` |
| `sky/serve/load_balancer.py::_retain_background_task` cancellation-fallback callers | Request-queue cleanup and notification tasks remain owned through repeated cancellation, finish, and release ownership | `tests/unit_tests/test_serve_request_queue.py::{test_cancellation_after_grant_reclaims_slot,test_repeated_cancellation_still_wakes_envelope_waiter}` | `pytest -q tests/unit_tests/test_serve_request_queue.py::test_cancellation_after_grant_reclaims_slot tests/unit_tests/test_serve_request_queue.py::test_repeated_cancellation_still_wakes_envelope_waiter` | `Python Tests - Unit Tests` |
| FastAPI startup registration | No extra loops, timers, retries, or steady-state work | all three tests above, with exact task-count assertions | `pytest -q tests/unit_tests/test_serve_load_balancer_rollout.py` | `Python Tests - Unit Tests` |

The pull request workflow in `.github/workflows/pytest.yml` runs
`tests/unit_tests` for every pull request to `improvements`; there is no path
filter excluding these files. The format, mypy, Pylint, Ruff, basedpyright,
async-lifecycle, and import-contract workflows cover the changed Python path.

## Test and performance plan

First run the three new lifecycle tests on the untouched production
implementation and require failure because `_start_background_loops` does not
exist. After implementation, run those tests, the full rollout test file, the
focused load-balancer unit inventory, and repository format/static checks.

Performance evidence is structural and test-backed: startup still creates
exactly three tasks. Each task gains one O(1) completion callback, which runs
only at terminal completion; request routing and loop iteration bodies are
unchanged. Completed tasks and their terminal frames are released instead of
being retained for process lifetime.

## Rollout

This is process-local and has no schema, protocol, API, configuration, or
mixed-version dependency. A normal load-balancer restart activates it. Revert
the single production commit if event-loop failure reporting is unexpectedly
too noisy; task scheduling and data-plane behavior otherwise remain unchanged.
