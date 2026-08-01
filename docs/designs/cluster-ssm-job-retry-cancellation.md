# Cluster SSM job retry cancellation

_Created: 2026-08-01_

## Problem

Legacy AWS SSM job-ID operations retry `TargetNotConnected` failures because
the remote mutation has not started yet. The retry backoff currently uses a
raw synchronous sleep. If the owning API request is cancelled during that
sleep, the backend can issue another `run_on_head()` call after cancellation
and create remote job state that no live request owns.

## Goal

Cancellation during SSM reconnect backoff must raise `asyncio.CancelledError`
before the next remote attempt. Active and context-free callers must preserve
the existing retry budget, backoff values, result parsing, failure selection,
and remote call counts.

## Background

`CloudVmRayBackend._run_job_id_command_with_ssm_retries()` is shared by legacy
single-job creation and batch job-info creation. `context_utils` already owns
the race-safe synchronous bridge from a `SkyPilotContext` cancellation callback
to a blocking wait. Reusing that helper avoids a second cancellation protocol.

## Solution

Replace only the SSM reconnect raw sleep with
`context_utils.sleep_with_cancellation()`. No retry condition, backoff, remote
command, parsing path, or public interface changes.

The boundary tests exercise the shared helper directly through `_add_job()`:
a cancelled wait must prevent a second SSH/SSM command, while an active retry
must still wait once and invoke the command twice. Existing tests continue to
pin the maximum-attempt and ambiguous-failure behavior. Shared helper tests pin
pre-cancelled, cancellation-at-timeout, callback cleanup, and no-context
semantics.

## Alternatives considered

Checking cancellation only at the top of the retry loop leaves the backoff
uninterruptible and adds up to eight seconds of cancellation latency. Adding a
new event or polling loop duplicates the established cancellation bridge.
Changing `run_on_head()` or all backend retry sleeps would broaden the blast
radius beyond this observed pre-session mutation boundary.

## Changed-path-to-test matrix

| Changed path or invariant | Test file | Command |
|---|---|---|
| `sky/backends/cloud_vm_ray_backend.py`: cancellation during SSM retry backoff prevents the next remote mutation | `tests/unit_tests/test_sky/backends/test_cloud_vm_ray_backend.py` | `PYTHONPATH=$PWD /Users/fcapponi/projects/skypilot/.venv/bin/python -m pytest -o addopts='' tests/unit_tests/test_sky/backends/test_cloud_vm_ray_backend.py -k 'add_job and target_not_connected' -q` |
| Active retry preserves one wait and two `run_on_head()` calls; maximum retries and ambiguous errors are unchanged | `tests/unit_tests/test_sky/backends/test_cloud_vm_ray_backend.py` | same focused command, then the full file |
| Pre-cancelled, timeout-race, callback-cleanup, and no-context wait behavior | `tests/unit_tests/test_sky/utils/test_context_utils.py` | `PYTHONPATH=$PWD /Users/fcapponi/projects/skypilot/.venv/bin/python -m pytest -o addopts='' tests/unit_tests/test_sky/utils/test_context_utils.py -q` |
| Adjacent cluster backend lifecycle | `tests/unit_tests/test_sky/backends/` | `PYTHONPATH=$PWD /Users/fcapponi/projects/skypilot/.venv/bin/python -m pytest -o addopts='' tests/unit_tests/test_sky/backends -q` |
| Job and API integration guardrail | `tests/test_jobs_and_serve.py tests/test_api.py` | `PYTHONPATH=$PWD /Users/fcapponi/projects/skypilot/.venv/bin/python -m pytest -o addopts='' -n 0 tests/test_jobs_and_serve.py tests/test_api.py -q` |

## Performance and CI proof

The regression tests assert exact wait and remote-call counts. The production
change substitutes one local event-backed wait for one raw wait and adds no
database, provider, SSH, timer, thread, allocation, or polling operation. A
microbenchmark of zero-timeout waits will bound the local helper overhead
relative to the configured one-second initial production backoff.

`.github/workflows/pytest.yml` runs `tests/unit_tests` in `Python Tests - Unit
Tests` and the mapped integration files in `Python Tests - Jobs & API Tests`.
The changed Python paths are also covered by the repository format, mypy,
pylint, Ruff, basedpyright, and async-lifecycle workflows.
