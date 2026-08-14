# Cancel-aware cluster launch waits

## Problem

The legacy Ray autoscaler cluster-launch path uses raw `time.sleep()` calls
between `ray up` retries and between multi-node readiness probes. A request
cancelled during either wait cannot wake until the full delay expires. After
waking, the loop can issue another subprocess or SSH probe even though the
request no longer owns useful work. The retry backoff can reach 180 seconds;
the readiness interval is 10 seconds.

## Goal

Cancellation must wake both waits promptly and raise `asyncio.CancelledError`
before the next remote operation. A live request must retain the existing
backoff values, retry limits, readiness deadlines, and remote call counts.

## Background

`SkyPilotContext` already owns cancellation and exposes race-safe
`register_cancel_callback()` and `unregister_cancel_callback()` methods. The
cluster launch path already propagates this context into synchronous backend
work. What is missing is a small synchronous wait primitive that bridges a
context callback to `threading.Event.wait()`.

## Solution

Add `context_utils.sleep_with_cancellation(seconds)`. Without a request
context it delegates directly to `time.sleep(seconds)`. With a context it
registers a callback that sets a local `threading.Event`, waits for the same
duration, and always unregisters the callback. It then raises
`asyncio.CancelledError` when either the event or context records cancellation.
The register API closes cancel-before-register; checking `is_canceled()` only
after unregistering closes cancellation between the timeout and callback
cleanup.

Use this primitive for the Ray-up retry backoff and the multi-node readiness
poll interval only. Do not alter subprocess execution, readiness parsing,
progress deadlines, or cleanup ownership.

## Alternatives considered

Polling `is_canceled()` with shorter sleeps would increase wakeups and either
retain noticeable cancellation latency or increase CPU activity. Adding a
new cancellation event to `SkyPilotContext` would duplicate state already
represented by its callback API. Reworking the entire launch path as async is
far beyond this bounded lifecycle correction.

## Changed-path-to-test matrix

| Changed production path or invariant | Concrete test file | Command |
| --- | --- | --- |
| `sky/utils/context_utils.py`: no-context waits keep the exact duration; cancellation before and during a wait raises; callbacks are unregistered after timeout | `tests/unit_tests/test_sky/utils/test_context_utils.py` | `pytest -n 0 tests/unit_tests/test_sky/utils/test_context_utils.py` |
| `sky/backends/backend_utils.py`: cancellation during the worker-readiness interval prevents a second SSH status probe; active waits preserve one wait and one probe per round | `tests/unit_tests/test_backend_utils_ray_ready.py` | `pytest -n 0 tests/unit_tests/test_backend_utils_ray_ready.py` |
| `sky/backends/cloud_vm_ray_backend.py`: cancellation during Ray-up backoff prevents a second launch subprocess; active retries preserve the selected backoff and retry count | `tests/unit_tests/test_sky/backends/test_cloud_vm_ray_backend.py` | `pytest -n 0 tests/unit_tests/test_sky/backends/test_cloud_vm_ray_backend.py` |
| Lifecycle, failure, concurrency, and performance boundary across all changed Python paths | all three files above, then backend component coverage | `pytest -n 0 tests/unit_tests/test_sky/utils/test_context_utils.py tests/unit_tests/test_backend_utils_ray_ready.py tests/unit_tests/test_sky/backends/test_cloud_vm_ray_backend.py`; `pytest -n 0 tests/unit_tests/test_backend_utils.py tests/unit_tests/test_backend_utils_ray_ready.py tests/unit_tests/test_sky/backends/test_cloud_vm_ray_backend.py` |

## Performance evidence

Focused tests will assert that an active wait receives exactly the preexisting
delay and that the number of Ray-up subprocesses and SSH probes is unchanged.
The new healthy path adds one local event allocation, callback registration,
and event wait around intervals of at least five seconds; it adds no database,
cloud, SSH, subprocess, timer, thread, or polling operation. Cancellation
removes otherwise wasted remote calls and releases the worker earlier.

## Rollout and rollback

This is an in-process behavior change with no schema or protocol migration.
Rollback is a direct reversion to raw sleeps. Existing teardown and request
executor cleanup remain the owners of partial cluster resources.
