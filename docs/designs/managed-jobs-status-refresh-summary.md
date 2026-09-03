# Managed Jobs Status Refresh Summary

## Problem

Managed-job refresh loaded every task attempt and reconstructed job state in Python. The hot path therefore transferred and allocated data proportional to attempt history, while cancellation and periodic refresh could observe different snapshots.

## Behavioral contract

Each refresh chunk returns at most one aggregate row per requested job and performs one `SELECT`. Explicit job IDs are deduplicated before chunking and returned in caller order; background sweeps remain newest first. Legacy jobs are excluded. A job that becomes terminal during refresh stays terminal, and an inconsistent terminal row with no workspace is terminalized instead of raising. Explicit cancellation reads derive from the same refreshed summary path.

## Solution

`sky.jobs.state` owns the dialect-specific aggregate query and decoding into an immutable refresh summary. `sky.jobs.utils` requests that summary once, applies refresh transitions, and reuses it for cancellation decisions. Direct cancellation lookups also route through the same summary helper, so refresh and cancel share one latest-task implementation. PostgreSQL and SQLite preserve the same ordering and null semantics.

The query returns scalar status, schedule, workspace, controller PID, and latest-attempt fields needed by the transition logic. Adding lifecycle fields must update both dialects, the decoder, and parity tests together.

## Alternatives considered

Keeping the full-row query and caching its Python projection preserves excess transfer and allocation. Issuing a second summary query for cancellation creates a consistency window and doubles database work. A schema migration is unnecessary because the existing task table already contains the authoritative fields.

## Rollout and rollback

The change is internal and requires no data migration. Rollback restores the former full-row refresh path. Query-count, row-count, ordering, cancellation, restart-race, and async/sync parity tests gate rollout.

## Changed-path-to-test matrix

| Changed path | Invariants | Tests | Command |
|---|---|---|---|
| `sky/jobs/state.py` | One aggregate refresh row per job; one `SELECT` per chunk; duplicate IDs deduplicated before chunking; caller order for explicit IDs; newest-first order for background sweeps; legacy rows excluded; explicit cancellation reads reuse the same summary query; no task-row rematerialization to count terminality | `tests/unit_tests/test_sky/jobs/test_status_refresh_snapshot.py`; `tests/test_jobs_state_async_vs_sync.py`; focused status-check and legacy-row cases in `tests/unit_tests/test_sky/jobs/test_state.py` | `PYTHONPATH=$PWD ./.venv/bin/python -m pytest -q -o addopts='' tests/unit_tests/test_sky/jobs/test_status_refresh_snapshot.py`<br>`PYTHONPATH=$PWD ./.venv/bin/python -m pytest -q -o addopts='' tests/unit_tests/test_sky/jobs/test_state.py -k 'status_check or legacy'`<br>`PYTHONPATH=$PWD ./.venv/bin/python -m pytest -q -o addopts='' tests/test_jobs_state_async_vs_sync.py -k 'latest_task_id_status or get_status_same or get_pool_submit_info_same or get_job_schedule_state_same or schedule_state_transitions_same'` |
| `sky/jobs/utils.py` | Refresh and cancellation reuse the same lifecycle snapshot; stale-owner recovery and dead-controller terminalization stay fenced; fixed-slot rows remain observational only; null-workspace DONE rows terminalize instead of crashing; one shared controller-PID probe per refresh sweep | `tests/unit_tests/test_jobs_utils.py`; `tests/unit_tests/test_managed_job_controller_restart_race.py` | `PYTHONPATH=$PWD ./.venv/bin/python -m pytest -q -o addopts='' tests/unit_tests/test_jobs_utils.py tests/unit_tests/test_managed_job_controller_restart_race.py` |
| `docs/designs/managed-jobs-status-refresh-summary.md` | The behavior, lifecycle, rollback, and performance contract stays synchronized with the implementation | Documentation build; branch-local Jobs/API integration coverage | `PYTHONPATH=$PWD ./.venv/bin/python -m pytest -q -o addopts='' tests/unit_tests/test_sky/jobs/test_status_refresh_snapshot.py tests/unit_tests/test_jobs_utils.py tests/unit_tests/test_managed_job_controller_restart_race.py tests/unit_tests/test_sky/jobs/test_state.py`<br>`PYTHONPATH=$PWD ./.venv/bin/python -m pytest -q -o addopts='' tests/test_jobs_state_async_vs_sync.py tests/test_jobs.py tests/test_jobs_and_serve.py tests/test_api.py`<br>`PATH="$PWD/.venv/bin:$PATH" bash format.sh --files sky/jobs/state.py sky/jobs/utils.py tests/unit_tests/test_jobs_utils.py tests/unit_tests/test_managed_job_controller_restart_race.py tests/unit_tests/test_sky/jobs/test_state.py tests/unit_tests/test_sky/jobs/test_status_refresh_snapshot.py` |

## Test plan

`tests/unit_tests/test_sky/jobs/test_status_refresh_snapshot.py` covers aggregation, ordering, deduplication, chunking, legacy exclusion, cancellation parity, and the one-query/one-row performance bounds. `tests/unit_tests/test_jobs_utils.py` and `tests/unit_tests/test_managed_job_controller_restart_race.py` cover transition reuse, cancellation, cleanup, recovery, PID sharing, restart races, and null-workspace terminalization. `tests/test_jobs_state_async_vs_sync.py`, `tests/test_jobs_and_serve.py`, and `tests/test_api.py` provide dialect and component integration coverage.
