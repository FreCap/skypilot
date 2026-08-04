# SkyServe cleanup tail-sleep removal

_Created: 2026-08-01_

## Problem

`sky.serve.service._cleanup()` polls replica termination workers every three
seconds. The loop sleeps unconditionally after each scan, including the scan
that removes the final completed worker or the final worker whose `start()`
failed. That tail sleep cannot observe more cleanup work, but delays scoped
storage cleanup and the caller's final service lifecycle publication by three
seconds.

The delay occurs on every teardown with at least one live replica. It also
widens the interval during which a completed teardown still appears in an
intermediate state. The current focused tests mock the sleep but do not
constrain its call count, so they preserve the delay silently.

## Goals

Replica cleanup must retain its existing termination concurrency gate, owner
checks, failure publication, and three-second polling cadence while workers
remain. Once the pending-worker map is empty, cleanup must proceed immediately
without a final sleep. The success and thread-start-failure boundaries must
both obey this invariant.

## Solution

Guard the polling sleep with the same pending-worker map that controls the
loop. This is the smallest possible change: it leaves worker scheduling,
joining, persistence, ownership fencing, and error handling untouched while
removing only a wait that has no future state to poll.

Add focused deterministic tests using a synchronous fake cleanup thread. One
test covers a successfully started and completed worker: it expects exactly
one polling sleep between start and completion, rather than the current two.
The adjacent failure test makes `start()` raise and expects no sleep after the
only pending entry is removed. Both tests require scoped storage cleanup to run
and verify the returned failure state.

## Changed-path-to-test matrix

| Changed path or invariant | Test file | Local command | CI job |
| --- | --- | --- | --- |
| `sky/serve/service.py`: preserve owner-fenced, concurrency-gated replica cleanup while eliminating the final empty-queue sleep | `tests/unit_tests/test_serve_service.py` | `pytest -n 0 tests/unit_tests/test_serve_service.py -k 'cleanup_skips_tail_sleep'` | `Python Tests - Unit Tests` |
| Success lifecycle: one poll while work remains, immediate storage cleanup after final worker completion | `tests/unit_tests/test_serve_service.py` | Same focused command | `Python Tests - Unit Tests` |
| Failure lifecycle: a final `SafeThread.start()` failure publishes failed cleanup and performs zero tail sleeps | `tests/unit_tests/test_serve_service.py` | Same focused command | `Python Tests - Unit Tests` |
| Performance: remove one fixed three-second wait per nonempty teardown; never add a wait or scan | Call-count assertions in `tests/unit_tests/test_serve_service.py` | Same focused command | `Python Tests - Unit Tests` |
| Formatting, typing, lint, and diff integrity for the changed Python paths | Existing repository checks | `bash format.sh --files sky/serve/service.py tests/unit_tests/test_serve_service.py`; `git diff --check` | `Format`, `MyPy`, `Pylint`, `Ruff` |

## Alternatives considered

Replacing polling with per-thread timed joins or an event-driven coordinator
could reduce the remaining completion-detection latency, but changes
termination scheduling and ownership-check cadence. That broader lifecycle
change is unnecessary to remove the proven tail wait. Reducing the poll
interval would increase wakeups and still retain the unconditional final
sleep.

## Rollout

This changes only the local SkyServe cleanup coordinator and requires no data
migration or compatibility path. The call-count tests are the performance and
lifecycle rollback gate. Reverting the conditional restores the former delay.
