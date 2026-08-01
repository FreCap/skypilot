# Managed-job recovery grace lock probes

_Created: 2026-08-01_

## Problem

The consolidation-mode managed-job refresh thread waits up to 15 seconds after
acquiring the leadership lock before it runs recovery.  This protects rolling
updates whose detached controllers briefly outlive the old API server.  The
thread currently checks the PostgreSQL advisory-lock session only after the
entire grace period.  If that session dies during the wait, another replica can
acquire leadership and recover jobs while this replica's local controllers stay
alive until the delayed check.  That creates a bounded but avoidable split-brain
window.

The current wait is one `time.sleep()` followed by one session probe.  There is
no steady-state query cost during the sleep, but lock loss can take the full
grace duration to trigger fail-stop and API-server termination.

## Goals

Detect lock loss during the recovery grace wait within the existing five-second
leadership-probe interval.  Never run recovery after an unsuccessful probe.
Preserve the configured total grace duration, the signal-file gate, immediate
recovery for backlogs that need no grace period, and the steady-state event-loop
cadence.

The one-time leadership transition may perform at most
`ceil(grace_seconds / lock_probe_seconds)` session probes.  It must add no cloud,
SSH, scheduler, controller, or event-loop work.

## Solution

Add one private method on `ManagedJobRefreshDaemonThread` that divides the
configured grace duration into chunks no larger than
`_LOCK_PROBE_INTERVAL_SECONDS`.  After each chunk it verifies that the exact
PostgreSQL session holding the advisory lock is still alive.  The method returns
false at the first failed probe, allowing the existing lock-loss path to touch
the recovery gate, fail-stop local controllers, and terminate the API server.

The existing final probe is folded into this helper for the grace-wait path.
The no-grace path keeps one probe immediately before recovery.  File locks keep
their existing behavior because `_lock_still_held()` remains true without a
database call.

## Changed-path-to-test matrix

| Changed production path or invariant | Test file | Command |
| --- | --- | --- |
| `sky/jobs/managed_job_refresh_thread.py`: lock loss during grace stops within one probe interval, does not run recovery, and leaves the recovery gate in place | `tests/unit_tests/test_sky/jobs/test_managed_job_refresh_thread.py` | `pytest -o addopts='' tests/unit_tests/test_sky/jobs/test_managed_job_refresh_thread.py -q` |
| Healthy leader waits the full configured duration and performs only `ceil(grace / probe)` session probes | `tests/unit_tests/test_sky/jobs/test_managed_job_refresh_thread.py` | same focused command |
| No-grace backlog performs no sleep and exactly one pre-recovery ownership probe | `tests/unit_tests/test_sky/jobs/test_managed_job_refresh_thread.py` | same focused command |
| Retry, fail-stop ordering, signal-file cleanup, recovery ordering, and steady-state tick boundaries remain intact | `tests/unit_tests/test_sky/jobs/test_managed_job_refresh_thread.py`, `tests/unit_tests/test_server_daemons.py`, `tests/unit_tests/test_sky/jobs/test_scheduler.py` | `pytest -o addopts='' tests/unit_tests/test_sky/jobs/test_managed_job_refresh_thread.py tests/unit_tests/test_server_daemons.py tests/unit_tests/test_sky/jobs/test_scheduler.py -q` |
| Managed-jobs component and API integration behavior | `tests/unit_tests/test_sky/jobs/`, `tests/test_jobs_and_serve.py` | `pytest -o addopts='' tests/unit_tests/test_sky/jobs -q`; `pytest -o addopts='' tests/test_jobs_and_serve.py -q` |
| CI path coverage | `.github/workflows/pytest.yml` | GitHub `Unit Tests`, `Jobs & API Tests`, and `Limited Deps - Jobs, Serve & CLI` on the exact PR head |

## Alternatives considered

Leaving the full-duration sleep unchanged keeps one fewer startup-only database
probe but preserves the split-brain window.  Adding a new watcher thread or a
database notification would expand lifecycle and cleanup surface for a one-time
wait.  Polling every second would detect loss faster but adds unnecessary query
load; the existing five-second ownership cadence is the smallest consistent
bound.

## Rollout

This is an internal lifecycle change with no schema or API migration.  A normal
server restart activates it.  Reverting the production and focused-test commit
restores the previous delayed detection behavior.
