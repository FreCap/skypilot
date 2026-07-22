# SkyServe Capacity Refresh Worker Launch Recovery

_Created: 2026-07-22_

## Problem

SkyServe's shared reserved-capacity cache coalesces stale Kubernetes capacity
queries behind the process-local `_DEMAND_REFRESH_RUNNING` flag. The scheduler
sets that flag before constructing and starting its daemon worker. If
`Thread.start()` raises, for example during transient thread exhaustion, no
worker exists to drain `_DEMAND_REFRESH_PENDING_CONTEXTS`, but the flag remains
true. Every later reconciliation therefore treats the missing worker as live
and only appends pending contexts. Capacity observations remain stale until the
controller process restarts.

The untouched implementation deterministically reproduces the failure: after
a mocked launch error, `_DEMAND_REFRESH_RUNNING` is still true, and a second
schedule attempt creates zero additional workers.

## Goals

The launch reservation must represent a worker that was successfully started.
A failed launch must retain every pending context, release the reservation,
avoid failing the caller's reconciliation tick, and allow the next schedule
attempt to start exactly one worker. Successful and concurrent schedules must
retain the existing single-flight behavior and provider-query count.

## Background

`get_cached_free_gpus_by_pool()` performs one batched database read and queues
stale Kubernetes contexts for `_demand_capacity_refresh_worker()`. The worker
drains the pending set in batches and clears the running flag under
`_DEMAND_REFRESH_STATE_LOCK` only after the set is empty. Scheduling performs
no provider I/O inline.

The vulnerable transition is:

```text
pending += contexts -> running = true -> Thread.start()
                                      X launch error
pending remains, running remains true, worker does not exist
```

## Solution

Keep the current lock, pending set, daemon worker, and single-flight protocol.
Construct and start the worker after reserving ownership as today. If launch
raises `RuntimeError`, reacquire `_DEMAND_REFRESH_STATE_LOCK`, clear the running
reservation, retain the pending set untouched, and log the failure. Do not
propagate the launch error into the autoscaler reconciliation path. A later
tick then retries the same pending work together with any contexts coalesced
during the failed launch window.

The success path adds no lock acquisition, worker, provider call, database
query, timer, or collection traversal. The failure path performs one bounded
lock acquisition and log call, replacing a permanent process-local stall with
retry on the next normal reconciliation.

## Alternatives Considered

Storing the `Thread` object instead of a boolean would expose more state but
would not remove the launch reservation race, and checking `is_alive()` can be
false before a newly started thread is scheduled. Starting the worker while
holding `_DEMAND_REFRESH_STATE_LOCK` would serialize launch with schedulers but
would also block the worker's first drain and lengthen the scheduling critical
section. Retrying immediately in a loop risks thread-creation spin during
resource exhaustion. Releasing ownership and using the existing polling cadence
is smaller and naturally bounded.

## Changed-Path-to-Test Matrix

| Changed production path or invariant | Test path | Command |
| --- | --- | --- |
| `sky/serve/reserved_capacity.py::_schedule_demand_capacity_refresh`: launch failure retains all pending contexts, releases ownership, and does not escape into reconciliation | `tests/unit_tests/test_reserved_capacity_fill.py::TestDemandCapacityRefreshScheduling::test_launch_failure_releases_ownership_and_preserves_pending` | `pytest -q tests/unit_tests/test_reserved_capacity_fill.py::TestDemandCapacityRefreshScheduling::test_launch_failure_releases_ownership_and_preserves_pending` |
| Same path: a later schedule after failure starts exactly one worker and includes contexts queued during the failure window | `tests/unit_tests/test_reserved_capacity_fill.py::TestDemandCapacityRefreshScheduling::test_next_schedule_retries_all_pending_contexts` | `pytest -q tests/unit_tests/test_reserved_capacity_fill.py::TestDemandCapacityRefreshScheduling::test_next_schedule_retries_all_pending_contexts` |
| Same path, concurrency and performance boundary: overlapping successful schedules retain one launch and O(1) ownership state | `tests/unit_tests/test_reserved_capacity_fill.py::TestDemandCapacityRefreshScheduling::test_successful_launch_remains_single_flight` | `pytest -q tests/unit_tests/test_reserved_capacity_fill.py::TestDemandCapacityRefreshScheduling::test_successful_launch_remains_single_flight` |
| Adjacent cache, broker, disable, stale-owner, polling lifecycle, and reserved-capacity configuration behavior | `tests/unit_tests/test_reserved_capacity_fill.py` and `tests/unit_tests/test_reserved_capacity_spec.py` | `pytest -q tests/unit_tests/test_reserved_capacity_fill.py tests/unit_tests/test_reserved_capacity_spec.py` |
| Python formatting, typing, lint, and diff integrity for every changed Python path | `format.sh`, `mypy`, `pylint`, `git diff --check` | `bash format.sh --files sky/serve/reserved_capacity.py tests/unit_tests/test_reserved_capacity_fill.py` and `git diff --check origin/improvements...HEAD` |

## CI and Rollout

`.github/workflows/pytest.yml` runs `tests/unit_tests` in the `Python Tests -
Unit Tests` job for pull requests targeting `improvements`, with no changed-path
filter. The repository `format`, `mypy`, `pylint`, `ruff`, `basedpyright`,
`async-lifecycle`, and import-contract jobs also run without a relevant path
filter. No real provider call is required because the regression is entirely
the local worker-launch state transition; adjacent tests retain coverage of
the provider-query and durable-observation boundaries.

Rollout needs no migration or configuration change. On failure, controllers
remain fail-closed on stale capacity as before, but can recover on the next
poll without a process restart.
